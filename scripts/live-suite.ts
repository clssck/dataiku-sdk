#!/usr/bin/env bun
/**
 * Persistent live-test lab lifecycle for the Dataiku SDK (`bun run test:live`).
 *
 * Verbs:
 *   setup      Provision the persistent lab (main project + fixtures) if absent;
 *              verify identity and report ready when already provisioned.
 *   run        Run live case suites against the provisioned lab (never rebuilds).
 *   clean      Delete the entire owned lab INCLUDING the main project; the lab
 *              directory and its history are preserved on disk.
 *   all        setup + run + clean (CI lifecycle; cleans even when setup fails).
 *   status     Report lab state without mutating anything.
 *
 * Design constraints (do not violate):
 * - No CLI production command is created; this is a repo script.
 * - No request transport is reimplemented: SDK client/CLI are reused.
 * - Credentials come from the repo .env via loadEnvFile + resolveCredentials;
 *   .env is never written, secrets never logged.
 * - Only projects created and bound by a lab iteration are ever deleted, and
 *   only through the guarded LiveRunContext cleanup.
 */
import * as fsp from "node:fs/promises";
import { hostname, } from "node:os";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";

import { loadEnvFile, } from "../src/cli/env.js";
import { resolveCredentials, } from "../src/cli/runtime.js";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import { resolveAdminManagedStorageConnection, } from "../src/resources/connections.js";
import { canonicalDssUrl, } from "../src/utils/dss-url.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
import { sha256Hex, stableHash, } from "../src/utils/stable-hash.js";
import { LIVE_CASES, matchesLiveCase, } from "../tests/live-cases.js";
import {
	type CaseResult,
	initializeLiveManifest,
	LIVE_PROFILES,
	LIVE_ROOT,
	type LiveCredentials,
	type LiveManifest,
	LiveRunContext,
	loadLiveManifest,
} from "../tests/live-context.js";
import { checkLiveCoverageCatalogue, liveCoverage, } from "../tests/live-coverage.js";

/** Repo root (parent of scripts/). */
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

type LiveVerb = "setup" | "run" | "clean" | "all" | "status";

export interface LiveSuiteOptions {
	verb: LiveVerb;
	cases: string[];
	profile: (typeof LIVE_PROFILES)[number];
	manifest?: string;
	stateDir?: string;
	/** Show local usage; never mutates anything. */
	help: boolean;
}

class UsageRequestError extends Error {}

function usage(): string {
	return [
		"Usage: bun run test:live <verb> [options]",
		"",
		"Verbs:",
		"  setup     Provision the persistent live lab (idempotent; verifies identity when present).",
		"  run       Run live case suites against the provisioned lab.",
		"  clean     Delete the owned lab including its main project; lab files/history are kept.",
		"  all       setup + run + clean (CI lifecycle; cleans even when setup or run fail).",
		"  status    Report lab state (no mutation, no DSS calls).",
		"",
		"Options:",
		"  --case <id[,id2|prefix*]>   Comma-separated case ids or prefix filters (run/all).",
		"  --profile <name>            core | ml | applications | infrastructure (default core).",
		"  --manifest <PATH>           Override the lab manifest path.",
		"  --state-dir <PATH>          Override the live state root (default .live-tests).",
		"  --help                      Show this usage (no mutation).",
	].join("\n",);
}

export function parseLiveSuiteArgs(argv: string[],): LiveSuiteOptions {
	let verb: LiveVerb | undefined;
	const cases: string[] = [];
	let emptyCase = false;
	let profile: string | undefined;
	let manifest: string | undefined;
	let stateDir: string | undefined;
	let help = false;
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i]!;
		const value = (): string => {
			const next = argv[i + 1];
			if (next === undefined) throw new UsageRequestError(`Option ${arg} requires a value`,);
			i += 1;
			return next;
		};
		switch (arg) {
			case "setup":
			case "run":
			case "clean":
			case "all":
			case "status":
				if (verb !== undefined) throw new UsageRequestError("Exactly one verb is required",);
				verb = arg;
				break;
			case "--case": {
				const count = cases.length;
				for (const part of value().split(",",)) {
					const id = part.trim();
					if (id) cases.push(id,);
				}
				emptyCase ||= cases.length === count;
				break;
			}
			case "--profile": {
				const name = value();
				if (!(LIVE_PROFILES as readonly string[]).includes(name,)) {
					throw new UsageRequestError(
						`Unknown profile ${name}; expected one of ${LIVE_PROFILES.join(", ",)}`,
					);
				}
				profile = name;
				break;
			}
			case "--manifest":
				manifest = value();
				break;
			case "--state-dir":
				stateDir = value();
				break;
			case "--help":
			case "help":
				help = true;
				break;
			default:
				throw new UsageRequestError(`Unknown argument: ${arg}`,);
		}
	}
	if (help) return { verb: "status", cases: [], profile: "core", help, manifest, stateDir, };
	if (emptyCase) throw new UsageRequestError("--case requires at least one case ID or prefix",);
	if (verb === undefined) {
		throw new UsageRequestError("A verb is required (setup|run|clean|all|status)",);
	}
	if (manifest && (verb === "setup" || verb === "all")) {
		throw new UsageRequestError(
			"--manifest selects an existing lab for run, clean or status; use --state-dir for setup/all",
		);
	}
	if (cases.length) {
		if (verb !== "run" && verb !== "all") {
			throw new UsageRequestError("--case is only valid for run/all",);
		}
		const available = Object.entries(LIVE_CASES,).filter(([, c,],) =>
			c.phase === "run" && (c.profile === "core" || c.profile === profile)
		).map(([id,],) => id);
		for (const selected of cases) {
			if (!available.some(id => matchesLiveCase(id, selected,))) {
				throw new UsageRequestError(
					`Unknown or inactive live case ${selected}; available run cases: ${available.join(", ",)}`,
				);
			}
		}
	}
	return {
		verb,
		cases,
		profile: (profile ?? "core") as LiveSuiteOptions["profile"],
		manifest,
		stateDir,
		help,
	};
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/**
 * Load the repo .env (never written) and resolve live credentials. Logs no
 * secrets; failures are actionable.
 */
export function loadLiveCredentials(): LiveCredentials {
	const invocationDirectory = process.cwd();
	try {
		process.chdir(REPO_ROOT,);
		loadEnvFile();
	} finally {
		process.chdir(invocationDirectory,);
	}
	const resolved = resolveCredentials({},);
	const url = resolved.url.trim();
	const apiKey = resolved.apiKey.trim();
	if (!url || !apiKey) {
		throw new Error(
			"Live tests need credentials: set DATAIKU_URL and DATAIKU_API_KEY in the repo .env (never commit it).",
		);
	}
	const credentials: LiveCredentials = { url, apiKey, };
	if (resolved.tlsRejectUnauthorized !== undefined) {
		credentials.tlsRejectUnauthorized = resolved.tlsRejectUnauthorized;
	}
	if (resolved.caCertPath !== undefined && resolved.caCertPath.trim() !== "") {
		credentials.caCertPath = resolved.caCertPath;
	}
	return credentials;
}

// ---------------------------------------------------------------------------
// Lab layout, unique lab directories, and pointer
// ---------------------------------------------------------------------------

/** Pointer file schema. */
export interface LabPointer {
	/** Canonical DSS URL the lab belongs to. */
	dssUrl: string;
	/** Absolute path to the manifest JSON for the current lab. */
	manifest: string;
	/** Absolute path of the lab directory (inside the state root). */
	labDir: string;
	/** ISO timestamp of the last pointer update. */
	updatedAt: string;
}

export interface LabLayout {
	stateRoot: string;
	labDir: string;
	manifestPath: string;
	pointerPath: string;
	lockPath: string;
}

function stateRootFor(credentials: LiveCredentials, stateDir?: string,): string {
	// Relative --state-dir values are anchored to the repo root so the lab
	// state root (e.g. .live-tests/verification) never depends on the caller's
	// working directory.
	return path.resolve(
		stateDir
			? repoAnchoredPath(stateDir,)
			: path.join(REPO_ROOT, ".live-tests", labIdFor(credentials,),),
	);
}

/** Resolve a user-supplied relative path against the repo root. */
function repoAnchoredPath(candidate: string,): string {
	return path.isAbsolute(candidate,) ? candidate : path.join(REPO_ROOT, candidate,);
}

function labIdFor(credentials: LiveCredentials,): string {
	return sha256Hex(canonicalDssUrl(credentials.url,),).slice(0, 16,);
}

export function layoutFor(
	credentials: LiveCredentials,
	stateDir?: string,
	pointer?: LabPointer,
): LabLayout {
	const stateRoot = stateRootFor(credentials, stateDir,);
	const labDir = pointer?.labDir
		? path.resolve(pointer.labDir,)
		: path.join(stateRoot, labIdFor(credentials,),);
	return {
		stateRoot,
		labDir,
		manifestPath: pointer?.manifest
			? path.resolve(pointer.manifest,)
			: path.join(labDir, "manifest.json",),
		pointerPath: path.join(stateRoot, "current.json",),
		lockPath: path.join(stateRoot, "lock",),
	};
}

/** A validated pointer: parses, and the lab dir stays inside the state root. */
export async function readPointer(stateRoot: string,): Promise<LabPointer | undefined> {
	const pointerPath = path.join(stateRoot, "current.json",);
	let raw: string;
	try {
		raw = await fsp.readFile(pointerPath, "utf8",);
	} catch {
		return undefined;
	}
	let parsed: LabPointer;
	try {
		parsed = JSON.parse(raw,) as LabPointer;
	} catch (error) {
		throw new Error(
			`Lab pointer ${pointerPath} is not valid JSON; remove it or run \`clean\` to reset.`,
			{ cause: error, },
		);
	}
	if (
		typeof parsed.dssUrl !== "string"
		|| typeof parsed.manifest !== "string"
		|| typeof parsed.labDir !== "string"
	) {
		throw new Error(`Lab pointer ${pointerPath} is malformed; remove it to reset the lab.`,);
	}
	const resolvedLab = path.resolve(parsed.labDir,);
	if (resolvedLab !== stateRoot && !resolvedLab.startsWith(stateRoot + path.sep,)) {
		throw new Error(
			`Lab pointer escapes the state root (${resolvedLab} not under ${stateRoot}); remove ${pointerPath} to reset.`,
		);
	}
	const resolvedManifest = path.resolve(parsed.manifest,);
	if (!resolvedManifest.startsWith(resolvedLab + path.sep,)) {
		throw new Error("Lab manifest escapes its lab directory",);
	}
	return { ...parsed, labDir: resolvedLab, manifest: resolvedManifest, };
}

async function writePointer(stateRoot: string, pointer: LabPointer,): Promise<void> {
	await fsp.mkdir(stateRoot, { recursive: true, },);
	const pointerPath = path.join(stateRoot, "current.json",);
	const target = `${JSON.stringify(pointer, null, "\t",)}\n`;
	const temporary = `${pointerPath}.tmp`;
	await fsp.writeFile(temporary, target, { mode: 0o600, },);
	await fsp.rename(temporary, pointerPath,);
}

// ---------------------------------------------------------------------------
// Locking
// ---------------------------------------------------------------------------

interface LockHandle {
	path: string;
	pid: number;
	/** Acquired in this process; released on exit. */
	owned: boolean;
}

async function acquireLabLock(stateRoot: string,): Promise<LockHandle> {
	await fsp.mkdir(stateRoot, { recursive: true, },);
	const lockPath = path.join(stateRoot, "lock",);
	const payload = `${
		JSON.stringify({ pid: process.pid, host: hostname(), startedAt: new Date().toISOString(), },)
	}\n`;
	try {
		await fsp.writeFile(lockPath, payload, { flag: "wx", mode: 0o600, },);
		return { path: lockPath, pid: process.pid, owned: true, };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
	}
	// Lock exists: identify the holder. A lock from this pid is reentrant.
	let holder: { pid?: number; startedAt?: string; } = {};
	try {
		holder = JSON.parse(await fsp.readFile(lockPath, "utf8",),) as {
			pid?: number;
			startedAt?: string;
		};
	} catch {
		// Unreadable lock content; treat as foreign.
	}
	if (holder.pid === process.pid) {
		return { path: lockPath, pid: process.pid, owned: true, };
	}
	// Never delete a lock we do not own — not even a provably dead one — silently.
	const detail = typeof holder.pid === "number"
		? `pid ${holder.pid}${holder.startedAt ? ` started ${holder.startedAt}` : ""}`
		: "unknown holder";
	throw new Error(
		`Live lab is locked (${detail}); remove ${lockPath} if no live-suite process is running.`,
	);
}

async function releaseLabLock(lock: LockHandle,): Promise<void> {
	if (!lock.owned) return;
	await fsp.rm(lock.path, { force: true, },);
}

// ---------------------------------------------------------------------------
// Capability preflight
// ---------------------------------------------------------------------------

interface CapabilityProbe {
	name: string;
	ok: boolean;
	reason?: string;
	value?: string;
}

async function probeLiveCapabilities(
	credentials: LiveCredentials,
): Promise<{ owner: string; connection: string; capabilities: Record<string, unknown>; }> {
	const client = new DataikuClient({
		...credentials,
		requestTimeoutMs: 30_000,
		retryMaxAttempts: 2,
	},);
	const capabilities: Record<string, unknown> = {};
	const probes: CapabilityProbe[] = [];

	// Owner: explicit env wins; otherwise the read-only auth-info endpoint.
	// authIdentifier may be an API-key id, not a login: only associatedDSSUser
	// is a usable owner identity here.
	const envOwner = process.env.DATAIKU_TEST_OWNER?.trim();
	let owner = envOwner ?? "";
	if (envOwner) {
		probes.push({ name: "owner_source", ok: true, value: "DATAIKU_TEST_OWNER", },);
	} else {
		try {
			const info = await client.get<Record<string, unknown>>(
				"/public/api/auth/info?withSecrets=false",
			);
			capabilities["authSource"] = info["authSource"] ?? null;
			const associated = info["associatedDSSUser"];
			if (typeof associated === "string" && associated.trim() !== "") {
				owner = associated.trim();
				probes.push({ name: "owner_source", ok: true, value: "auth/info.associatedDSSUser", },);
			} else {
				probes.push({
					name: "owner_source",
					ok: false,
					reason: "GET /public/api/auth/info returned no associatedDSSUser; set DATAIKU_TEST_OWNER",
				},);
			}
		} catch (error) {
			probes.push({
				name: "owner_source",
				ok: false,
				reason: `GET /public/api/auth/info failed (${
					error instanceof Error ? error.message : String(error,)
				}); set DATAIKU_TEST_OWNER`,
			},);
		}
	}

	// Connection: explicit override wins; otherwise resolve a writable managed
	// storage connection with the existing admin helper.
	const envConnection = process.env.DATAIKU_TEST_CONNECTION?.trim();
	let connection = envConnection ?? "";
	if (envConnection) {
		probes.push({ name: "connection_source", ok: true, value: "DATAIKU_TEST_CONNECTION", },);
	} else {
		const datasets = await resolveAdminManagedStorageConnection(client, "allowManagedDatasets",);
		const folders = await resolveAdminManagedStorageConnection(client, "allowManagedFolders",);
		connection = datasets ?? folders ?? "";
		probes.push({
			name: "connection_source",
			ok: connection !== "",
			reason: connection === ""
				? "No admin-managed writable storage connection with allowManagedDatasets/allowManagedFolders; set DATAIKU_TEST_CONNECTION"
				: undefined,
			value: connection || undefined,
		},);
	}

	// Connectivity snapshot with server version headers.
	try {
		const meta = await client.getWithMetadata<unknown>("/public/api/projects/",);
		capabilities["dssVersion"] = meta.meta.dssVersion;
		capabilities["dssApiVersion"] = meta.meta.dssApiVersion;
		probes.push({ name: "connectivity", ok: true, },);
	} catch (error) {
		probes.push({
			name: "connectivity",
			ok: false,
			reason: `Project list failed: ${error instanceof Error ? error.message : String(error,)}`,
		},);
	}

	capabilities["owner"] = owner;
	capabilities["connection"] = connection;
	capabilities["probes"] = probes;
	const blockers = probes.filter((probe,) => !probe.ok);
	if (blockers.length > 0 || owner === "" || connection === "") {
		const reasons = blockers.map((probe,) => `${probe.name}: ${probe.reason ?? "unresolved"}`);
		throw new Error(
			`Live capability preflight failed:\n  - ${
				reasons.join("\n  - ",)
			}\nFix the environment or set the overriding variables and retry.`,
		);
	}
	return { owner, connection, capabilities, };
}

// ---------------------------------------------------------------------------
// Project snapshot / integrity
// ---------------------------------------------------------------------------

/**
 * Full-details snapshot: every visible project is fingerprinted from its
 * complete details so external metadata drift is detected, not just identity.
 */
async function snapshotProjectIdentities(client: DataikuClient,): Promise<Record<string, string>> {
	const snapshot: Record<string, string> = {};
	for (const summary of await client.projects.list()) {
		let details: unknown;
		try {
			details = await client.projects.get(summary.projectKey,);
		} catch (error) {
			// A project that vanished mid-snapshot is not pre-existing state.
			if (error instanceof DataikuError && error.status === 404) continue;
			throw error;
		}
		snapshot[summary.projectKey] = stableHash(details,);
	}
	return snapshot;
}

interface IntegrityReport {
	verified: boolean;
	missing: string[];
	changed: string[];
	unexplained: string[];
	/** Projects deleted by this run as part of owned-lab teardown. */
	deletedOwned: string[];
}

/**
 * Cleanup evidence: owned lab projects are EXPECTED to disappear during
 * teardown, so they are recorded as `deletedOwned` — not counted as external
 * drift. Only a pre-existing (external) project vanishing counts as `missing`.
 */
export function evaluateCleanupIntegrity(
	beforeCleanup: Record<string, string>,
	after: Record<string, string>,
	manifest: LiveManifest,
): IntegrityReport {
	const deletedOwned = new Set(
		manifest.projects
			.filter((project,) => project.state === "deleted")
			.map((project,) => project.key),
	);
	const deleted = (key: string,): boolean => !Object.hasOwn(after, key,);
	const missing = Object.keys(beforeCleanup,).filter((key,) =>
		deleted(key,) && !deletedOwned.has(key,)
	);
	const changed = Object.entries(beforeCleanup,)
		.filter(([key, identity,],) => !deleted(key,) && after[key] !== identity)
		.map(([key,],) => key);
	const unexplained = Object.keys(after,).filter((key,) => !Object.hasOwn(beforeCleanup, key,));
	return {
		verified: missing.length === 0 && changed.length === 0 && unexplained.length === 0,
		missing,
		changed,
		unexplained,
		deletedOwned: [...deletedOwned,].filter((key,) => deleted(key,)),
	};
}

/**
 * Full-details comparison with NO prefix exemption: a project counts as
 * explained only when it appears in beforeProjects or in the manifest's
 * owned-project identities (exact keys).
 */
async function verifyProjectIntegrity(
	client: DataikuClient,
	manifest: LiveManifest,
	beforeProjects: Record<string, string>,
): Promise<IntegrityReport> {
	const ownedKeys = new Set(manifest.projects.map((project,) => project.key),);
	const missing: string[] = [];
	const changed: string[] = [];
	const unexplained: string[] = [];
	const current = new Map<string, string>();
	for (const summary of await client.projects.list()) {
		let details: unknown;
		try {
			details = await client.projects.get(summary.projectKey,);
		} catch {
			details = null;
		}
		const identity = details === null
			? "unreadable"
			: stableHash(details,);
		current.set(summary.projectKey, identity,);
	}
	for (const [key, identity,] of Object.entries(beforeProjects,)) {
		const live = current.get(key,);
		if (live === undefined) missing.push(key,);
		else if (live !== identity) changed.push(key,);
	}
	for (const key of current.keys()) {
		if (Object.hasOwn(beforeProjects, key,)) continue;
		if (ownedKeys.has(key,)) continue;
		unexplained.push(key,);
	}
	return {
		verified: missing.length === 0 && changed.length === 0,
		missing,
		changed,
		unexplained,
		deletedOwned: [],
	};
}

// ---------------------------------------------------------------------------
// Child processes
// ---------------------------------------------------------------------------

interface ChildResult {
	code: number;
	logPath: string;
	durationMs: number;
}

interface ChildSpec {
	name: string;
	logPath: string;
	cwd: string;
	argv: string[];
	env: NodeJS.ProcessEnv;
	/** Wall-clock cap in milliseconds. */
	timeoutMs: number;
}

let activeChild: Bun.Subprocess<"ignore", "pipe", "pipe"> | undefined;

async function spawnLogged(spec: ChildSpec,): Promise<ChildResult> {
	const started = Date.now();
	const child = Bun.spawn(spec.argv, {
		cwd: spec.cwd,
		env: spec.env,
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
	},);
	activeChild = child;
	let timedOut = false;
	const timer = setTimeout(() => {
		timedOut = true;
		child.kill("SIGTERM",);
	}, spec.timeoutMs,);
	let stdout = "";
	let stderr = "";
	try {
		[stdout, stderr,] = await Promise.all([
			new Response(child.stdout,).text(),
			new Response(child.stderr,).text(),
		],);
	} finally {
		clearTimeout(timer,);
		activeChild = undefined;
	}
	const code = await child.exited;
	const body = `${
		[
			`# ${spec.name} — ${new Date(started,).toISOString()} — exit ${code}${
				timedOut ? " (timed out)" : ""
			}`,
			"# --- stdout ---",
			stdout,
			"# --- stderr ---",
			stderr,
		].join("\n",)
	}\n`;
	await fsp.mkdir(path.dirname(spec.logPath,), { recursive: true, },);
	await fsp.writeFile(spec.logPath, body, { mode: 0o600, },);
	return { code, logPath: spec.logPath, durationMs: Date.now() - started, };
}

function liveChildEnv(
	credentials: LiveCredentials,
	manifestPath: string,
	phase: "setup" | "run",
	selection: string[],
): NodeJS.ProcessEnv {
	return {
		...process.env,
		DATAIKU_URL: credentials.url,
		DATAIKU_API_KEY: credentials.apiKey,
		DATAIKU_LIVE_MANIFEST: manifestPath,
		RUN_DATAIKU_LIVE: "1",
		DATAIKU_LIVE_PHASE: phase,
		DATAIKU_LIVE_CASES: selection.join(",",),
		NODE_TLS_REJECT_UNAUTHORIZED: credentials.tlsRejectUnauthorized === false ? "0" : "1",
		...(credentials.caCertPath ? { NODE_EXTRA_CA_CERTS: credentials.caCertPath, } : {}),
	};
}

function legacyChildEnv(
	credentials: LiveCredentials,
	projectKey: string,
	connection: string,
	manifestPath: string,
): NodeJS.ProcessEnv {
	return {
		...process.env,
		DATAIKU_URL: credentials.url,
		DATAIKU_API_KEY: credentials.apiKey,
		DATAIKU_PROJECT_KEY: projectKey,
		DATAIKU_TEST_CONNECTION: connection,
		DATAIKU_LIVE_MANIFEST: manifestPath,
		RUN_DATAIKU_INTEGRATION: "1",
		RUN_DATAIKU_INTEGRATION_MUTATING: "0",
		RUN_DATAIKU_ADMIN_MUTATING: "0",
		RUN_DATAIKU_SQL_LIVE: "0",
		RUN_DATAIKU_INTEGRATION_REPORT: "1",
		NODE_TLS_REJECT_UNAUTHORIZED: credentials.tlsRejectUnauthorized === false ? "0" : "1",
		...(credentials.caCertPath ? { NODE_EXTRA_CA_CERTS: credentials.caCertPath, } : {}),
	};
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

interface SuiteReport {
	runId: string;
	iteration: number;
	profile: string;
	selection: string[];
	versions: Record<string, unknown>;
	phases: Record<
		string,
		{
			status: "passed" | "failed" | "skipped" | "interrupted";
			durationMs: number;
			error?: string;
			log?: string;
		}
	>;
	cases: CaseResult[];
	setupCases: CaseResult[];
	coverage: ReturnType<typeof liveCoverage>;
	capabilities: Record<string, unknown>;
	projects: LiveManifest["projects"];
	commandCount: number;
	timings: Record<string, number>;
	cleanup: LiveManifest["cleanup"];
	integrity: IntegrityReport;
	startedAt: string;
	completedAt: string;
}

function redactText(text: string, credentials: LiveCredentials,): string {
	return credentials.apiKey ? text.split(credentials.apiKey,).join("[redacted]",) : text;
}

async function writeSuiteReport(
	reportPath: string,
	report: SuiteReport,
	credentials: LiveCredentials,
): Promise<void> {
	await fsp.mkdir(path.dirname(reportPath,), { recursive: true, },);
	const serialized = JSON.stringify(report, null, "\t",);
	await fsp.writeFile(reportPath, `${redactText(serialized, credentials,)}\n`, { mode: 0o600, },);
}

function redactLine(text: string, credentials?: LiveCredentials,): string {
	const apiKey = credentials?.apiKey ?? process.env.DATAIKU_API_KEY;
	return apiKey ? text.split(apiKey,).join("[redacted]",) : text;
}

/**
 * Persisted cleanup report: integrity evidence survives the process so a CI
 * job (or the parent harness) can audit external-project safety after the fact.
 */
async function writeCleanupReport(
	state: LabState,
	manifest: LiveManifest,
	cleanup: LiveManifest["cleanup"],
	integrity: IntegrityReport,
	durationMs: number,
): Promise<string> {
	const stateRoot = state.layout.stateRoot;
	const reportPath = path.join(stateRoot, "cleanup-report.json",);
	const report = {
		runId: manifest.runId,
		iteration: manifest.iteration,
		dssUrl: manifest.dssUrl,
		labDir: state.layout.labDir,
		completedAt: new Date().toISOString(),
		durationMs,
		cleanup,
		integrity,
	};
	await fsp.writeFile(reportPath, `${JSON.stringify(report, null, "\t",)}\n`, { mode: 0o600, },);
	return reportPath;
}

// ---------------------------------------------------------------------------
// Verb implementations
// ---------------------------------------------------------------------------

interface LabState {
	credentials: LiveCredentials;
	layout: LabLayout;
	manifest?: LiveManifest;
	pointer?: LabPointer;
}

async function loadLabState(
	credentials: LiveCredentials,
	stateDir?: string,
): Promise<LabState> {
	const stateRoot = stateRootFor(credentials, stateDir,);
	const pointer = await readPointer(stateRoot,);
	const layout = layoutFor(credentials, stateDir, pointer,);
	let manifest: LiveManifest | undefined;
	const manifestPath = pointer?.manifest ?? layout.manifestPath;
	try {
		manifest = await loadLiveManifest(manifestPath,);
	} catch {
		manifest = undefined;
	}
	return { credentials, layout, manifest, pointer, };
}

async function loadManifestState(
	credentials: LiveCredentials,
	options: LiveSuiteOptions,
): Promise<LabState> {
	const manifestPath = repoAnchoredPath(options.manifest!,);
	const manifest = await loadLiveManifest(manifestPath,);
	if (canonicalDssUrl(manifest.dssUrl,) !== canonicalDssUrl(credentials.url,)) {
		throw new Error("Manifest belongs to a different DSS server",);
	}
	const labDir = path.dirname(manifestPath,);
	const stateRoot = options.stateDir
		? stateRootFor(credentials, options.stateDir,)
		: path.dirname(labDir,);
	if (!labDir.startsWith(stateRoot + path.sep,)) {
		throw new Error("Selected manifest is outside the state root",);
	}
	return {
		credentials,
		manifest,
		layout: {
			stateRoot,
			labDir,
			manifestPath,
			pointerPath: path.join(stateRoot, "current.json",),
			lockPath: path.join(stateRoot, "lock",),
		},
	};
}

function printStatus(state: LabState,): void {
	const lines: string[] = [];
	if (!state.manifest) {
		lines.push("status: unprovisioned", `state root: ${state.layout.stateRoot}`,);
	} else {
		const manifest = state.manifest;
		lines.push(
			`status: ${
				manifest.setupComplete
					? "ready"
					: manifest.cleanup.status === "passed"
					? "cleaned"
					: "incomplete"
			}`,
			`run id: ${manifest.runId}`,
			`iteration: ${manifest.iteration}`,
			`dss: ${manifest.dssUrl}`,
			`main project: ${manifest.projectKey || "(none)"}`,
			`connection: ${manifest.connection}`,
			`owner: ${manifest.owner}`,
			`profiles: ${manifest.profiles.join(",",)}`,
			`owned projects: ${manifest.projects.length}`,
			`lab dir: ${state.layout.labDir}`,
			`manifest: ${state.layout.manifestPath}`,
		);
	}
	process.stdout.write(`${lines.join("\n",)}\n`,);
}

/** Unique lab directory for a new setup: <stateRoot>/lab-0001, lab-0002, ... */
async function reserveLabDir(stateRoot: string, credentials: LiveCredentials,): Promise<string> {
	await fsp.mkdir(stateRoot, { recursive: true, },);
	const prefix = `lab-${labIdFor(credentials,)}`;
	// The lab id is derived from the server; uniqueness comes from a sequence
	// suffix while the id keeps labs self-describing per DSS server.
	let index = 1;
	while (true) {
		const candidate = path.join(stateRoot, `${prefix}-${String(index,).padStart(3, "0",)}`,);
		try {
			await fsp.mkdir(candidate, { recursive: false, },);
			return candidate;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
			index += 1;
		}
	}
}

async function runSetupVerb(
	state: LabState,
	options: LiveSuiteOptions,
): Promise<{ manifest: LiveManifest; reused: boolean; durationMs: number; }> {
	const started = Date.now();
	const manifestPath = state.layout.manifestPath;
	if (
		state.manifest?.cleanup.status === "passed"
		&& state.manifest.projects.every(project => project.state === "deleted")
	) {
		state.manifest = undefined;
		state.pointer = undefined;
	}

	// A pointer-backed lab exists: never overwrite it. Ready labs verify
	// identity and return; incomplete labs are rejected unless resumable.
	if (state.manifest) {
		const existing = state.manifest;
		if (!existing.setupComplete || !existing.projectKey) {
			// Failed/interrupted setup: resume only when nothing was created yet
			// (no owned project has been bound), otherwise demand explicit clean.
			const anyOwned = existing.projects.length > 0;
			if (anyOwned) {
				throw new Error(
					`Live lab ${existing.runId} at ${state.layout.labDir} is incomplete but owns projects (${
						existing.projects.map((p,) => p.key).join(", ",)
					}); run \`bun run test:live clean\` before setting up again.`,
				);
			}
			// Safe resume: reuse the same manifest file, no new incarnation.
			process.stdout.write(`resuming incomplete setup for run ${existing.runId}\n`,);
			const resumed = await loadLiveManifest(manifestPath,);
			return finishSetup(state, resumed, options, started,);
		}
		// Complete lab: idempotent repeat setup verifies identity, mutates nothing.
		const client = new DataikuClient({
			...state.credentials,
			requestTimeoutMs: 30_000,
			retryMaxAttempts: 1,
		},);
		const details = await client.projects.get(existing.projectKey,);
		const identity = projectIncarnationHash(existing.projectKey, details,);
		const bound = existing.projects.find((project,) => project.key === existing.projectKey);
		if (!identity || (bound?.incarnation !== undefined && bound.incarnation !== identity)) {
			throw new Error(
				`Lab identity verification failed for ${existing.projectKey}: the main project was rebuilt or replaced. Run \`bun run test:live clean\` then setup again.`,
			);
		}
		process.stdout.write(
			`live lab ready (run ${existing.runId}, iteration ${existing.iteration})\n`,
		);
		return { manifest: existing, reused: true, durationMs: Date.now() - started, };
	}

	// Fresh lab: unique directory, pointer written before any DSS mutation so a
	// failed setup is resumable (or cleanable) by lab path.
	const labDir = await reserveLabDir(state.layout.stateRoot, state.credentials,);
	const layout: LabLayout = {
		...state.layout,
		labDir,
		manifestPath: path.join(labDir, "manifest.json",),
	};
	const { owner, connection, capabilities, } = await probeLiveCapabilities(state.credentials,);
	const client = new DataikuClient({
		...state.credentials,
		requestTimeoutMs: 30_000,
		retryMaxAttempts: 1,
	},);
	const beforeProjects = await snapshotProjectIdentities(client,);
	const manifest = await initializeLiveManifest(
		layout.manifestPath,
		state.credentials,
		owner,
		connection,
		options.profile === "core" ? ["core",] : ["core", options.profile,],
	);
	manifest.beforeProjects = beforeProjects;
	manifest.capabilities = capabilities;
	await new LiveRunContext(layout.manifestPath, manifest, state.credentials, "setup", [],).save();
	await writePointer(layout.stateRoot, {
		dssUrl: manifest.dssUrl,
		manifest: layout.manifestPath,
		labDir,
		updatedAt: new Date().toISOString(),
	},);

	const resumed = await loadLiveManifest(layout.manifestPath,);
	state.layout = layout;
	state.manifest = resumed;
	return finishSetup(state, resumed, options, started,);
}

async function finishSetup(
	state: LabState,
	manifest: LiveManifest,
	options: LiveSuiteOptions,
	started: number,
): Promise<{ manifest: LiveManifest; reused: boolean; durationMs: number; }> {
	const manifestPath = path.join(state.layout.labDir, "manifest.json",);
	const ctx = new LiveRunContext(manifestPath, manifest, state.credentials, "setup", [],);

	// Provision the CLI root project before the child collections run.
	if (!manifest.projectKey) {
		const rootKey = await ctx.createProject("cli_root",);
		manifest.projectKey = rootKey;
		await ctx.save();
	}

	const iterationDir = path.join(state.layout.labDir, "reports", `iteration-${manifest.iteration}`,);
	const child = await spawnLogged({
		name: "setup",
		logPath: path.join(iterationDir, "setup.log",),
		cwd: LIVE_ROOT,
		argv: [process.execPath, "test", "tests/live-suite.test.ts",],
		env: liveChildEnv(state.credentials, manifestPath, "setup", [],),
		timeoutMs: 45 * 60 * 1000,
	},);
	if (child.code !== 0) {
		throw new Error(
			`Live setup failed (exit ${child.code}); lab preserved for debugging at ${state.layout.labDir}. Log: ${child.logPath}`,
		);
	}
	const fresh = await loadLiveManifest(manifestPath,);
	fresh.setupCases = fresh.cases;
	fresh.cases = [];
	fresh.setupComplete = true;
	await new LiveRunContext(manifestPath, fresh, state.credentials, "setup", [],).save();
	await writePointer(state.layout.stateRoot, {
		dssUrl: fresh.dssUrl,
		manifest: manifestPath,
		labDir: state.layout.labDir,
		updatedAt: new Date().toISOString(),
	},);
	process.stdout.write(`live lab provisioned (run ${fresh.runId}, project ${fresh.projectKey})\n`,);
	void options;
	return { manifest: fresh, reused: false, durationMs: Date.now() - started, };
}

interface RunOutcome {
	manifest: LiveManifest;
	report: SuiteReport;
	child: ChildResult;
	legacy: ChildResult[];
	durationMs: number;
}

export async function verifyLiveRoot(
	client: DataikuClient,
	manifest: LiveManifest,
): Promise<void> {
	let details;
	try {
		details = await client.projects.get(manifest.projectKey,);
	} catch (error) {
		if (!(error instanceof DataikuError) || error.status !== 404) throw error;
		throw new Error(
			`Lab main project ${manifest.projectKey} is missing; the baseline cannot be trusted.`,
			{ cause: error, },
		);
	}
	const owned = manifest.projects.find(project =>
		project.key === manifest.projectKey && project.state === "bound"
	);
	if (
		!owned?.incarnation || owned.incarnation !== projectIncarnationHash(manifest.projectKey, details,)
	) {
		throw new Error(
			`Lab main project ${manifest.projectKey} identity changed or is unconfirmed; refusing to run. Inspect the ownership journal before recovery.`,
		);
	}
}

async function runRunVerb(
	state: LabState,
	options: LiveSuiteOptions,
): Promise<RunOutcome> {
	const started = Date.now();
	const manifestPath = state.layout.manifestPath;
	// Reload from disk: a sibling process or prior child may have advanced it.
	const manifest = await loadLiveManifest(manifestPath,);
	if (!manifest.setupComplete) {
		throw new Error(
			`Live lab setup is incomplete (run ${manifest.runId}); rerun \`bun run test:live setup\`. The run verb never rebuilds the baseline.`,
		);
	}
	const client = new DataikuClient({
		...state.credentials,
		requestTimeoutMs: 30_000,
		retryMaxAttempts: 1,
	},);
	await verifyLiveRoot(client, manifest,);

	manifest.profiles = options.profile === "core" ? ["core",] : ["core", options.profile,];
	manifest.iteration += 1;
	manifest.cases = [];
	manifest.commands = [];
	manifest.cleanup = { status: "pending", errors: [], };
	await new LiveRunContext(manifestPath, manifest, state.credentials, "run", options.cases,).save();

	const iterationDir = path.join(state.layout.labDir, "reports", `iteration-${manifest.iteration}`,);
	const child = await spawnLogged({
		name: "run",
		logPath: path.join(iterationDir, "run.log",),
		cwd: LIVE_ROOT,
		argv: [process.execPath, "test", "tests/live-suite.test.ts",],
		env: liveChildEnv(state.credentials, manifestPath, "run", options.cases,),
		timeoutMs: 60 * 60 * 1000,
	},);
	const fresh = await loadLiveManifest(manifestPath,);

	// Optional legacy integration suites: full runs only.
	const legacy: ChildResult[] = [];
	if (options.cases.length === 0) {
		legacy.push(
			await spawnLogged({
				name: "legacy-playground",
				logPath: path.join(iterationDir, "legacy-playground.log",),
				cwd: LIVE_ROOT,
				argv: [process.execPath, "test", "tests/integration-playground.test.ts",],
				env: legacyChildEnv(state.credentials, fresh.projectKey, fresh.connection, manifestPath,),
				timeoutMs: 45 * 60 * 1000,
			},),
		);
		legacy.push(
			await spawnLogged({
				name: "legacy-rigorous",
				logPath: path.join(iterationDir, "legacy-rigorous.log",),
				cwd: LIVE_ROOT,
				argv: [process.execPath, "test", "tests/integration-rigorous.test.ts",],
				env: legacyChildEnv(state.credentials, fresh.projectKey, fresh.connection, manifestPath,),
				timeoutMs: 45 * 60 * 1000,
			},),
		);
	}

	const integrity = await verifyProjectIntegrity(client, fresh, fresh.beforeProjects,);
	// Coverage is a hard dependency: catalogue drift or a missing module fails
	// here rather than degrading the report.
	checkLiveCoverageCatalogue();
	const coverage = liveCoverage([...(fresh.setupCases ?? []), ...fresh.cases,], fresh.profiles,);
	// Mirrors tests/live-suite.test.ts: a required case fails unless it passed.
	// blocked/unsupported are statuses a required case can legitimately end in
	// (missing license, unsupported server); they are recorded but only optional
	// cases may end there without failing the suite.
	const requiredFailures = fresh.cases.filter((c,) =>
		c.status === "failed"
		|| (c.required !== false && c.status !== "passed")
	);
	const report: SuiteReport = {
		runId: fresh.runId,
		iteration: fresh.iteration,
		profile: options.profile,
		selection: options.cases,
		versions: {
			dss: fresh.capabilities["dssVersion"] ?? null,
			dssApi: fresh.capabilities["dssApiVersion"] ?? null,
			sdk: (await import("../package.json", { with: { type: "json", }, }) as {
				default: { version: string; };
			}).default.version,
		},
		phases: {
			setup: { status: "passed", durationMs: 0, },
			run: child.code === 0 && legacy.every(result => result.code === 0) && integrity.verified
				? {
					status: requiredFailures.length === 0 ? "passed" : "failed",
					durationMs: child.durationMs,
					log: child.logPath,
				}
				: {
					status: "failed",
					durationMs: child.durationMs,
					error: `case suite exit ${child.code}; legacy exits ${
						legacy.map(result => result.code).join(",",) || "not selected"
					}; integrity ${integrity.verified ? "verified" : "failed"}`,
					log: child.logPath,
				},
			...Object.fromEntries(
				legacy.map(
					result => [path.basename(result.logPath, ".log",), {
						status: result.code === 0 ? "passed" : "failed",
						durationMs: result.durationMs,
						log: result.logPath,
					},],
				),
			),
		},
		cases: fresh.cases,
		setupCases: fresh.setupCases ?? [],
		coverage,
		capabilities: fresh.capabilities,
		projects: fresh.projects,
		commandCount: fresh.commands.length,
		timings: {
			run: child.durationMs,
			legacy: legacy.reduce((sum, item,) => sum + item.durationMs, 0,),
		},
		cleanup: fresh.cleanup,
		integrity,
		startedAt: new Date(started,).toISOString(),
		completedAt: new Date().toISOString(),
	};
	await writeSuiteReport(path.join(iterationDir, "report.json",), report, state.credentials,);
	process.stdout.write(
		`live run ${report.phases["run"]?.status}: ${
			fresh.cases.filter((c,) => c.status === "passed").length
		} passed, ${requiredFailures.length} required failures — ${
			path.join(iterationDir, "report.json",)
		}\n`,
	);
	return { manifest: fresh, report, child, legacy, durationMs: Date.now() - started, };
}

async function runCleanVerb(
	state: LabState,
): Promise<{ cleanup: LiveManifest["cleanup"]; integrity: IntegrityReport; durationMs: number; }> {
	const started = Date.now();
	// Reload from disk: a child process may have advanced the manifest (bound
	// projects, incarnation hashes) after this parent object was captured.
	const manifest = await loadLiveManifest(state.layout.manifestPath,);
	if (manifest.projects.length === 0) {
		throw new Error(`Lab manifest ${state.layout.manifestPath} owns no projects; nothing to clean.`,);
	}
	const ctx = new LiveRunContext(state.layout.manifestPath, manifest, state.credentials, "run", [],);
	const client = ctx.client;
	const beforeCleanup = await snapshotProjectIdentities(client,);
	// Delete the ENTIRE owned lab: main project included, guarded by the
	// bound-incarnation checks inside LiveRunContext.deleteProject.
	// Once teardown starts, the baseline is no longer reusable, even on partial failure.
	manifest.setupComplete = false;
	await ctx.save();
	let cleanupError: unknown;
	try {
		await ctx.cleanup();
	} catch (error) {
		cleanupError = error;
	}
	const after = await snapshotProjectIdentities(client,);
	const integrity = evaluateCleanupIntegrity(beforeCleanup, after, manifest,);
	// Reload after cleanup so the manifest's own cleanup errors survive: never
	// overwrite a failed cleanup with a success-shaped report.
	const fresh = await loadLiveManifest(state.layout.manifestPath,);
	await writeCleanupReport(state, fresh, fresh.cleanup, integrity, Date.now() - started,);
	process.stdout.write(
		`clean ${fresh.cleanup.status}: ${
			fresh.projects.filter(project => project.state === "deleted").length
		}/${fresh.projects.length} owned projects deleted; external projects unchanged: ${
			integrity.verified ? "yes" : "NO"
		}\n`,
	);
	// The report retains both failures; the deletion error remains the primary diagnostic.
	if (cleanupError) throw cleanupError;
	if (!integrity.verified) {
		throw new Error(
			"External project integrity changed during cleanup; inspect cleanup-report.json",
		);
	}
	if (fresh.cleanup.status === "failed") {
		throw new Error(`Live cleanup failed: ${fresh.cleanup.errors.join("; ",)}`,);
	}
	return { cleanup: fresh.cleanup, integrity, durationMs: Date.now() - started, };
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	const argv = process.argv.slice(2,);
	if (argv.length === 0) {
		process.stdout.write(`${usage()}\n`,);
		return;
	}
	let options: LiveSuiteOptions;
	try {
		options = parseLiveSuiteArgs(argv,);
	} catch (error) {
		if (error instanceof UsageRequestError) {
			process.stderr.write(`${redactLine(error.message,)}\n\n${usage()}\n`,);
			process.exitCode = 2;
			return;
		}
		throw error;
	}
	if (options.help) {
		process.stdout.write(`${usage()}\n`,);
		return;
	}

	const credentials = loadLiveCredentials();
	const state = options.manifest
		? await loadManifestState(credentials, options,)
		: await loadLabState(credentials, options.stateDir,);

	if (options.verb === "status") {
		printStatus(state,);
		return;
	}

	const lock = await acquireLabLock(state.layout.stateRoot,);
	let exitCode = 0;
	try {
		if (options.verb === "setup") {
			await runSetupVerb(state, options,);
		} else if (options.verb === "run") {
			const outcome = await runRunVerb(state, options,);
			if (outcome.report.phases["run"]?.status === "failed") exitCode = 1;
		} else if (options.verb === "clean") {
			await runCleanVerb(state,);
		} else if (options.verb === "all") {
			// CI lifecycle: clean must run even when setup or run fail. Cleanup
			// reloads the persisted pointer, so a mid-setup failure still cleans
			// anything the lab already created.
			try {
				await runSetupVerb(state, options,);
				const outcome = await runRunVerb(state, options,);
				if (outcome.report.phases["run"]?.status === "failed") exitCode = 1;
			} catch (error) {
				process.stderr.write(
					`${redactLine(error instanceof Error ? error.message : String(error,), credentials,)}\n`,
				);
				exitCode = exitCode === 0 ? 1 : exitCode;
			} finally {
				try {
					const labState = await loadLabState(credentials, options.stateDir,);
					if (labState.manifest && labState.manifest.projects.length > 0) {
						await runCleanVerb(labState,);
					}
				} catch (error) {
					process.stderr.write(
						`${redactLine(error instanceof Error ? error.message : String(error,), credentials,)}\n`,
					);
					exitCode = exitCode === 0 ? 1 : exitCode;
				}
			}
		}
	} catch (error) {
		process.stderr.write(
			`${redactLine(error instanceof Error ? error.message : String(error,), credentials,)}\n`,
		);
		exitCode = exitCode === 0 ? 1 : exitCode;
	} finally {
		await releaseLabLock(lock,);
	}
	process.exitCode = exitCode;
}

if (import.meta.main) {
	let interruptCount = 0;
	for (const signal of ["SIGINT", "SIGTERM",] as const) {
		process.on(signal, () => {
			interruptCount += 1;
			if (interruptCount > 1) process.exit(130,);
			process.stderr.write(`\nlive-suite: ${signal} received; stopping child and saving state\n`,);
			const child = activeChild;
			if (child) {
				try {
					child.kill("SIGTERM",);
				} catch { /* already gone */ }
			}
			// The child intercepts the same signals, stops its CLI subprocesses
			// via stopLiveCommands(), saves the manifest, and exits 130/143; give
			// it that window before releasing the process so the lock is not
			// dropped early.
			setTimeout(() => process.exit(130,), 1500,);
		},);
	}

	await main();
}
