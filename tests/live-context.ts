import StreamZip from "node-stream-zip";
import { randomUUID, } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";
import { jsonInput, } from "../src/cli/coerce.js";
import { buildCommandRegistry, } from "../src/cli/contract.js";
import { executionMode, parseArgs, } from "../src/cli/flags.js";
import { futureTargetIdentity, } from "../src/cli/helpers/app-successor.js";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import {
	appendCleanupLedgerEntry,
	reserveCleanupLedgerDssUrl,
} from "../src/utils/cleanup-ledger.js";
import { canonicalDssUrl, } from "../src/utils/dss-url.js";
import { inspectProjectArchive, } from "../src/utils/project-archive.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
import {
	redactUrlUserinfo,
	replaceSecrets,
	sanitizeSecrets,
} from "../src/utils/secret-sanitize.js";

import { type LiveCaseId, matchesLiveCase, } from "./live-cases.js";
import {
	type HostDaemon,
	type HostDirectory,
	hostDirectoryScript,
	type OwnedHostDirectory,
	readBare,
} from "./live-host.js";

export const LIVE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
const commandRegistry = buildCommandRegistry();
const GIT_AUTHORITY = Symbol("owned Git command",);
const activeCommands = new Set<ReturnType<typeof Bun.spawn>>();
export async function stopLiveCommands(): Promise<void> {
	const children = [...activeCommands,];
	for (const child of children) child.kill("SIGTERM",);
	await Promise.allSettled(children.map(child => child.exited),);
}
/** Deterministic uploaded CSV parsing, shared by core and optional fixtures. */
export const LIVE_CSV_FORMAT = {
	formatType: "csv",
	formatParams: {
		style: "excel",
		charset: "utf8",
		separator: ",",
		quoteChar: '"',
		escapeChar: "\\",
		parseHeaderRow: true,
	},
} as const;

/** The infrastructure profile authorizes only this constant, table-free SQL probe. */
export const LIVE_SQL_PROBE = "SELECT 1 AS one";

export const LIVE_PROFILES = ["core", "ml", "applications", "infrastructure",] as const;
export interface LiveFixtures {
	datasets: Record<string, string>;
	expectedRows: Record<string, number>;
	recipes: Record<string, string>;
	dataQualityRuleIds?: string[];
	wikiArticleId?: string;
	folderId?: string;
	scenarioId?: string;
	dashboardId?: string;
	insightId?: string;
	notebookName?: string;
	/** Convenience ids recorded by the disposable-infrastructure module (best-effort). */
	projectFolderId?: string;
	connectionId?: string;
	codeEnvName?: string;
	pluginId?: string;
	ml?: LiveMlFixtures;
}
/** ML fixtures provisioned step-by-step in the run-owned root project; each step persists its id. */
export interface LiveMlFixtures {
	datasetName?: string;
	analysisId?: string;
	mlTaskId?: string;
	trainedModelId?: string;
	savedModelId?: string;
	savedModelVersionId?: string;
	scoringRecipe?: string;
	scoredDataset?: string;
	evaluationStoreId?: string;
	evaluationRecipe?: string;
}
export interface CaseResult {
	id: string;
	status: "passed" | "failed" | "blocked" | "unsupported";
	actions: string[];
	executedActions: string[];
	durationMs: number;
	error?: string;
	capability?: string;
	required?: boolean;
}
export interface LiveCommand {
	action: string;
	args: string[];
	exitCode: number;
	durationMs: number;
	mode: "execute" | "plan" | "dry-run";
}
/**
 * Accurate in-flight record of one generated-key app creation racing on a
 * precursor project. Set just before the creation POST; cleared only by a
 * proven no-POST refusal, a definitive rejection, or a terminal-bound child.
 * An unknown outcome keeps the marker (with `outcome` documenting it) so no
 * cleanup may touch the precursor while a dependent project may exist.
 */
export interface PendingDependentCreation {
	appId: string;
	mode: "direct" | "successor";
	startedAt: string;
	/** Set only once the outcome is known-but-unresolved; absent while racing. */
	outcome?: "indeterminate";
}

export interface OwnedProject {
	key: string;
	state: "pending" | "bound" | "deleted";
	incarnation?: string;
	createdAt: string;
	/** Present while a generated-key app creation may be racing on this precursor. */
	pendingDependentCreation?: PendingDependentCreation;
	/**
	 * Receipt-derived origin of a generated app-instance key: the owned
	 * precursor this creation was verified against. Set only after the trusted
	 * CLI receipt proved the target identity; the manifest validator admits an
	 * APP_ key only when this names a project the same manifest already
	 * validated, so the chain anchors at this run's reserved keys and a bare
	 * foreign APP_ key can never be adopted.
	 */
	generatedFrom?: string;
}
/** Project-less resources with guarded CLI or private harness cleanup paths. */
export const OWNED_GLOBAL_KINDS = [
	"user",
	"group",
	"meaning",
	"workspace",
	"data-collection",
	"project-folder",
	"connection",
	"code-env",
	"plugin",
	"api-deployer-infra",
	"api-deployer-service",
	"api-deployer-deployment",
	"project-deployer-infra",
	"project-deployer-project",
	"project-deployer-deployment",
] as const;
export type OwnedGlobalKind = typeof OWNED_GLOBAL_KINDS[number];
/**
 * One reserved global identity. `name` is the exact creation argument (never a
 * prefix); `id` is the server identity used by follow-up commands and equals
 * `name` unless DSS generates ids (data collections, project folders). Only a
 * `bound` entry (creation confirmed by an identity-matching GET) is ever deleted.
 */
export interface OwnedGlobal {
	kind: OwnedGlobalKind;
	name: string;
	id?: string;
	state: "pending" | "bound" | "conflict" | "unconfirmed" | "deleted";
	/** Reservation nonce; the lab-written marker proves create receipt ownership. */
	nonce?: string;
	identity?: string;
	reason?: string;
	createdAt: string;
}
/** Marker the module must write into the kind's lab-controlled field at create. */
export function globalMarker(runId: string, nonce: string,): string {
	return `SDK_LIVE_MARK_${runId}_${nonce}`;
}
export interface LiveManifest {
	version: 1;
	fixtureVersion: 1;
	runId: string;
	dssUrl: string;
	projectKey: string;
	owner: string;
	connection: string;
	profiles: string[];
	fixtures: LiveFixtures;
	projects: OwnedProject[];
	globals: OwnedGlobal[];
	ownedDirectories: OwnedHostDirectory[];
	/** Future ids started by commands of this lab; only these may be aborted. */
	futures: string[];
	cases: CaseResult[];
	commands: LiveCommand[];
	iteration: number;
	setupComplete?: boolean;
	setupCases?: CaseResult[];
	startedAt: string;
	beforeProjects: Record<string, string>;
	capabilities: Record<string, unknown>;
	cleanup: { status: "pending" | "kept" | "passed" | "failed"; errors: string[]; };
}
type OwnedGitTarget = { kind: "project"; key: string; } | { kind: "plugin"; id: string; };
export interface LiveContext {
	projectKey: string;
	connection: string;
	owner: string;
	dir: string;
	profiles: string[];
	phase: "setup" | "run";
	selection: string[];
	iteration: number;
	runId: string;
	fixtures: LiveFixtures;
	client: DataikuClient;
	run<T = unknown,>(
		args: string[],
		options?: { projectKey?: string; expectedExit?: number; },
	): Promise<T>;
	check(
		id: LiveCaseId,
		actions: readonly string[],
		body: () => Promise<void>,
		options?: { capability?: string; required?: boolean; },
	): Promise<void>;
	withOwnedHostDirectory<T,>(
		label: string,
		body: (directory: HostDirectory,) => Promise<T>,
	): Promise<T>;
	startOwnedSseServer(handle: HostDirectory,): Promise<HostDaemon>;
	withOwnedGitScope<T,>(
		target: OwnedGitTarget,
		directory: HostDirectory,
		repositoryPaths: readonly string[],
		body: (urls: readonly string[],) => Promise<T>,
	): Promise<T>;
	pluginFromGit(
		action: "install" | "update",
		name: string,
		url: string,
		revision: string,
	): Promise<OwnedGlobal>;
	writeFile(name: string, content: string | Uint8Array,): Promise<string>;
	save(): Promise<void>;
	createProject(label: string,): Promise<string>;
	createProjectForGlobal(label: string,): Promise<string>;
	duplicateProject(source: string, label: string,): Promise<string>;
	importProject(archive: string, label: string,): Promise<string>;
	deleteProject(key: string,): Promise<void>;
	reserveProject(label: string,): Promise<string>;
	bindProject(key: string, expectedHash?: string,): Promise<void>;
	/**
	 * Run exactly one generated-key app-instance creation (direct from an owned
	 * APP_TEMPLATE, or successor from an owned APP_INSTANCE predecessor) under a
	 * private one-shot authorization, returning the canonical generated target
	 * key only once the child project is incarnation-verified and bound into the
	 * cleanup ledger. The precursor carries an accurate in-flight
	 * `pendingDependentCreation` marker from before the POST until a proven
	 * no-POST refusal, a definitive rejection, or the bound child clears it; any
	 * unknown outcome retains the marker truthfully. Never relaxes the strict
	 * explicit-key path: an argv naming a target key is refused here.
	 */
	runGeneratedAppCreation(argv: string[], precursorProjectKey: string,): Promise<string>;
	createManagedDataset(name: string,): Promise<void>;
	propagateRecipeSchema(name: string,): Promise<void>;
	globals: OwnedGlobal[];
	/** Read-only view of owned projects for incarnation lookups. */
	projects: OwnedProject[];
	reserveGlobal(
		kind: OwnedGlobalKind,
		label: string,
		options?: { lang?: "PYTHON" | "R"; },
	): Promise<string>;
	createPluginDev(label: string, pluginJson: (name: string, marker: string,) => string,): Promise<
		OwnedGlobal
	>;
	/**
	 * Reserve the managed code environment DSS generates for a BOUND owned
	 * plugin (`plugin_<pluginId>_managed`) before any mutation creates it.
	 * Absence is proven for the exact generated name; returns the reserved
	 * env name for bindGlobal("code-env", name, `PYTHON/${name}`) after the
	 * create future settles.
	 */
	reservePluginCodeEnv(pluginId: string,): Promise<string>;
	installPluginFromZip(
		label: string,
		buildArchive: (name: string, marker: string,) => Promise<string>,
	): Promise<OwnedGlobal>;
	bindGlobal(kind: OwnedGlobalKind, name: string, id?: string,): Promise<OwnedGlobal>;
	createGlobal(
		kind: OwnedGlobalKind,
		label: string,
		argv: (name: string, marker: string,) => string[],
		options?: { lang?: "PYTHON" | "R"; id?: (result: unknown,) => string | undefined; },
	): Promise<OwnedGlobal>;
	deleteGlobal(kind: OwnedGlobalKind, id: string,): Promise<void>;
	recordFuture(id: string,): Promise<void>;
	/** The lab-controlled ownership marker for a reserved global. */
	markerFor(kind: OwnedGlobalKind, name: string,): string;
}
export class LiveCapabilityError extends Error {
	constructor(message: string, readonly status: "blocked" | "unsupported" = "blocked",) {
		super(message,);
		this.name = "LiveCapabilityError";
	}
}
export class LiveCommandError extends Error {
	constructor(message: string, readonly exitCode: number, readonly result: unknown,) {
		super(message,);
		this.name = "LiveCommandError";
	}
}
/**
 * The exact structured CLI refusal that proves app-instance creation was
 * refused BEFORE the creation POST: target-project absence could not be
 * verified under this identity, so no artifact was created. The flag must be
 * exactly `false`; a missing or `true` value is not this shape.
 */
function noPostTargetAbsenceDetails(result: unknown,): Record<string, unknown> | undefined {
	const receipt = asRecord(result,);
	if (receipt?.["code"] !== "target_absence_unverifiable") return undefined;
	const details = asRecord(receipt["details"],);
	return details?.["creationPostAttempted"] === false ? details : undefined;
}
/**
 * Structured CLI proof that a generated-key app creation never created a
 * project. Only the CLI's own error-report envelope is trusted: the exact
 * target-absence refusal shape, a CLI-authored probe `details.creationPostAttempted
 * === false`, or the failed command result the CLI wraps as `details.result`
 * (its handler-authored `state`/`outcome`/`creationPostAttempted` verdict).
 * Remote payloads echoed inside that receipt — its future `result`, `instance`
 * or error bodies — are never scanned for a claim a server could have written,
 * so anything else (5xx, transport failure, unknown state, unparseable output,
 * truncated envelope) leaves the outcome unknown and retains the marker.
 */
function provenNoCreationOutcome(result: unknown,): boolean {
	const envelope = asRecord(result,);
	if (!envelope) return false;
	if (noPostTargetAbsenceDetails(envelope,)) return true;
	const details = asRecord(envelope["details"],);
	if (details?.["creationPostAttempted"] === false) return true;
	const receipt = asRecord(details?.["result"],);
	return receipt?.["creationPostAttempted"] === false
		|| receipt?.["outcome"] === "rejected"
		|| receipt?.["state"] === "CREATE_FAILED";
}
/**
 * The canonical generated target key of a trusted creation receipt: the
 * creation future's own result names the project it created, and DSS echoes
 * `projectKey` there for the *source* template, so `targetProjectKey` always
 * wins (the same precedence the CLI uses before it sets
 * `futureTargetVerified`). The inline POST response is consulted second.
 */
function generatedCreationTarget(receipt: Record<string, unknown>,): string | undefined {
	const result = asRecord(receipt["result"],)
		?? asRecord(asRecord(receipt["instance"],)?.["result"],);
	return result === undefined ? undefined : futureTargetIdentity(result,)?.projectKey;
}
/** The target key an app-creation argv names: JSON `targetProjectKey` or successor `--to`. */
function appCreateTarget(
	action: string,
	flags: Record<string, string | boolean>,
): string | undefined {
	return action === "create-successor-instance"
		? asString(flags["to"],)
		: asString(jsonInput(flags,)?.["targetProjectKey"],);
}
export type LiveCredentials = {
	url: string;
	apiKey: string;
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
};

export async function writeLiveJson(file: string, value: unknown,): Promise<void> {
	const temporary = `${file}.tmp-${randomUUID()}`;
	const handle = await fs.open(temporary, "wx", 0o600,);
	try {
		try {
			await handle.writeFile(`${JSON.stringify(value, null, 2,)}\n`,);
			await handle.sync();
		} finally {
			await handle.close();
		}
		await fs.rename(temporary, file,);
	} finally {
		await fs.rm(temporary, { force: true, },);
	}
}
function isHostDaemon(value: unknown, directory: HostDirectory,): value is HostDaemon {
	const daemon = asRecord(value,);
	if (
		!daemon || !Number.isSafeInteger(daemon.pid,) || (daemon.pid as number) <= 1
		|| typeof daemon.startTime !== "string" || !/^\d+$/.test(daemon.startTime,)
		|| !Number.isInteger(daemon.port,) || (daemon.port as number) < 1024
		|| (daemon.port as number) > 65535
		|| (daemon.service !== undefined && daemon.service !== "sse")
		|| !Array.isArray(daemon.repositories,)
	) return false;
	const repositories = daemon.repositories;
	return (daemon.service === "sse" ? repositories.length === 0 : repositories.length > 0)
		&& new Set(repositories,).size === repositories.length
		&& repositories.every(repo =>
			typeof repo === "string" && path.posix.dirname(repo,) === directory.path
			&& path.posix.normalize(repo,) === repo
		);
}

export async function loadLiveManifest(file: string,): Promise<LiveManifest> {
	const data = JSON.parse(await fs.readFile(file, "utf8",),) as LiveManifest;
	if (
		data.version !== 1 || data.fixtureVersion !== 1 || !/^[a-f0-9]{16}$/.test(data.runId,)
		|| typeof data.dssUrl !== "string" || !Array.isArray(data.projects,)
		|| !Array.isArray(data.profiles,)
		|| !data.fixtures || !Array.isArray(data.cases,) || !Array.isArray(data.commands,)
		|| !data.beforeProjects || !data.cleanup
	) {
		throw new Error("Invalid live-suite manifest",);
	}
	if (data.profiles.some(p => !LIVE_PROFILES.includes(p as typeof LIVE_PROFILES[number],))) {
		throw new Error("Unknown live profile in manifest",);
	}
	if (!Array.isArray(data.globals,)) data.globals = [];
	if (!Array.isArray(data.futures,)) data.futures = [];
	if (data.ownedDirectories === undefined) data.ownedDirectories = [];
	if (!Array.isArray(data.ownedDirectories,)) throw new Error("Invalid owned host directories",);
	const directoryPaths = new Set<string>();
	for (const directory of data.ownedDirectories) {
		if (
			!directory || directory.runId !== data.runId || typeof directory.nonce !== "string"
			|| !/^[a-f0-9]{32}$/.test(directory.nonce,)
			|| directory.path !== `/tmp/sdk_live_${data.runId}_${directory.nonce}`
			|| directoryPaths.has(directory.path,)
			|| !["pending", "bound", "deleted",].includes(directory.state,)
			|| !data.projects.some(p =>
				p.key === directory.projectKey && (directory.state === "deleted" || p.state === "bound")
			)
		) throw new Error("Invalid owned host directory",);
		const daemon = directory.daemon;
		if (
			daemon !== undefined && (directory.state === "deleted" || !isHostDaemon(daemon, directory,))
		) {
			throw new Error("Invalid owned host daemon receipt",);
		}
		directoryPaths.add(directory.path,);
	}
	const seen = new Set<string>();
	for (const project of data.projects) {
		// Two provenances only. A reserved key belongs to this run by its
		// prefix. A generated app-instance key (APP_ + 32 uppercase hex, the
		// CLI's Designer-friendly client-side format) is admitted only with a
		// receipt-derived origin naming a project this same manifest already
		// validated: the chain therefore anchors at this run's reserved keys,
		// and self-references, forward references, cycles and foreign origins
		// are all rejected — a bare foreign APP_ key can never be adopted.
		const reservedKey = project.key.startsWith(`SDK_LIVE_${data.runId.toUpperCase()}_`,);
		const generatedKey = /^APP_[0-9A-F]{32}$/.test(project.key,);
		const origin = project.generatedFrom;
		const originValid = generatedKey
			? typeof origin === "string" && seen.has(origin,)
			: origin === undefined;
		if (
			(!reservedKey && !generatedKey) || seen.has(project.key,) || !originValid
			|| Object.hasOwn(data.beforeProjects, project.key,)
			|| !["pending", "bound", "deleted",].includes(project.state,)
			|| (project.state === "bound" && !/^[a-f0-9]{64}$/.test(project.incarnation ?? "",))
		) throw new Error("Invalid owned-project identity",);
		seen.add(project.key,);
	}
	return data;
}

/** Argv flags whose VALUE is secret material (redact the next token, not the flag). */
const SECRET_VALUE_FLAGS: Record<string, true> = {
	"api-key": true,
	"password": true,
	"password-env": true,
};
/** Live-report surface policy: every credential family, nested at any depth. */
const LIVE_REPORT_SANITIZE_OPTIONS = {
	sensitiveKeys: {
		"apikey": true,
		"apitoken": true,
		"authrealm": true,
		"basicauthpassword": true,
		"key": true,
		"password": true,
		"secret": true,
		"secrets": true,
		"token": true,
		"userpassword": true,
	},
	isSensitiveKey: (normalizedKey: string,) => normalizedKey === "secrets",
} as const;
/**
 * Scrub a report-surface string: exact occurrences of known secret values
 * (API key, plus any secret extracted from JSON payloads on this surface),
 * userinfo in URLs, and credential-bearing JSON fragments.
 */
function redactReportSecrets(text: string, apiKey: string,): string {
	let result = replaceSecrets(text, apiKey ? [apiKey,] : [],);
	result = redactUrlUserinfo(result,);
	return result.replace(
		/("(?:authRealm|key|password|token|secret|apiKey|apiToken|basicAuthPassword|userPassword)"\s*:\s*)"(?:[^"\\]|\\.)*"/gi,
		'$1"[redacted]"',
	);
}
/**
 * Redact secret material from persisted argv before it reaches the report:
 * JSON payloads scrubbed recursively per the live-report key policy (password,
 * key, token, apikey, secrets, authRealm...), values following sensitive flags,
 * and exact known-secret occurrences are all replaced before persisting.
 * Wire payloads are never mutated — only the report surface.
 */
function redactSecretValues(arg: string,): string {
	if (arg.startsWith("{",) || arg.startsWith("[",)) {
		try {
			return JSON.stringify(sanitizeSecrets(JSON.parse(arg,), LIVE_REPORT_SANITIZE_OPTIONS,),);
		} catch {
			return arg;
		}
	}
	return arg;
}
function redactArgv(argv: string[],): string[] {
	const redacted: string[] = [];
	let redactNext = false;
	for (const arg of argv) {
		if (redactNext) {
			redacted.push("[redacted]",);
			redactNext = false;
			continue;
		}
		const bare = arg.replace(/^--/, "",);
		if (SECRET_VALUE_FLAGS[bare] === true) {
			redacted.push(arg,);
			redactNext = true;
			continue;
		}
		redacted.push(redactSecretValues(arg,),);
	}
	return redacted;
}

/** Scope checks also run in the test child; credentials and server/project flags cannot be replaced by a case. */
function safeParseJson(text: string,): unknown {
	try {
		return JSON.parse(text,);
	} catch {
		return undefined;
	}
}
function asRecord(value: unknown,): Record<string, unknown> | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as Record<string, unknown>
		: undefined;
}
/**
 * Verify one GET result proves ownership of a reserved global: the lab-written
 * marker must appear in the kind's lab-controlled field (server-generated id +
 * owner for project folders). Returns true when the object proves ownership.
 */
function globalMarkerVerified(
	kind: OwnedGlobalKind,
	record: Record<string, unknown>,
	marker: string,
	reservedName: string,
	expectedId: string,
): boolean {
	const has = (value: unknown,) => typeof value === "string" && value.includes(marker,);
	switch (kind) {
		case "user":
			return asString(record["email"],) === `${marker}@sdk-live.invalid`;
		case "group":
		case "meaning":
		case "workspace":
		case "data-collection":
			return has(record["description"],);
		case "api-deployer-infra":
		case "api-deployer-service":
		case "api-deployer-deployment":
		case "project-deployer-infra":
		case "project-deployer-project":
		case "project-deployer-deployment": {
			// Deployer create APIs expose caller-supplied IDs, not description fields.
			// The full fresh reservation nonce is carried by that documented ID.
			const infoKey = kind.endsWith("infra",)
				? "infraBasicInfo"
				: kind.endsWith("service",)
				? "serviceBasicInfo"
				: kind.endsWith("project",)
				? "projectBasicInfo"
				: "deploymentBasicInfo";
			const info = asRecord(record[infoKey],) ?? record;
			const id = asString(info["id"],) ?? asString(info["publishedServiceId"],)
				?? asString(info["publishedProjectKey"],) ?? asString(info["deploymentId"],);
			const nonce = marker.slice(marker.lastIndexOf("_",) + 1,);
			return /^[a-f0-9]{32}$/i.test(nonce,) && reservedName.endsWith(`_${nonce.toUpperCase()}`,)
				&& id === reservedName;
		}
		case "project-folder": {
			const nonce = marker.slice(marker.lastIndexOf("_",) + 1,);
			return /^[a-f0-9]{32}$/i.test(nonce,) && reservedName.endsWith(`_${nonce.toUpperCase()}`,)
				&& asString(record["name"],)?.startsWith(reservedName,) === true
				&& asString(record["id"],) === expectedId;
		}
		case "connection": {
			const params = asRecord(record["params"],);
			// Filesystem: marker inside a valid absolute root. JDBC/SQLite: marker
			// inside the jdbc url (in-memory named). Exact-key per type, never
			// invented keys.
			if (asString(record["type"],) === "Filesystem") {
				const root = asString(params?.["root"],) ?? "";
				return root.startsWith("/",) && root.includes(marker,);
			}
			const jdbcUrl = asString(params?.["jdbcurl"],) ?? asString(params?.["URL"],);
			return typeof jdbcUrl === "string" && jdbcUrl.includes(marker,);
		}
		case "plugin":
			return record["id"] === expectedId && typeof record["isDev"] === "boolean"
				&& has(asRecord(record["meta"],)?.["description"],);
		case "code-env":
			// Server incarnation identity (reviewer-confirmed): desc.creationTag
			// is real and nested, versionTag separate. Bind on the EXACT resolved
			// envName and its lang plus desc.creationTag; recheck before every
			// mutation/delete. Two reservations can never share a verified
			// identity: the server echo must equal the reserved name and the
			// lang must match the reservation (plugins envs are PYTHON; plain
			// reservations carry their lowercased lang prefix).
			return asString(record["envName"],) === reservedName
				&& asString(record["envLang"],)?.toUpperCase()
					=== (reservedName.startsWith("plugin_",)
						? "PYTHON"
						: reservedName.split("_",)[0]!.toUpperCase())
				&& Object.keys(asRecord(record["desc"],)?.["creationTag"] ?? {},).length > 0;
	}
}
function stableStringify(value: unknown,): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value,);
	if (Array.isArray(value,)) return `[${value.map(stableStringify,).join(",",)}]`;
	return `{${
		Object.entries(value as Record<string, unknown>,).sort(([a,], [b,],) =>
			a < b ? -1 : a > b ? 1 : 0
		)
			.map(([k, v,],) => `${JSON.stringify(k,)}:${stableStringify(v,)}`).join(",",)
	}}`;
}
function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}
/** The positional/JSON key that carries the reserved identity for a create. */
function createNameArg(
	rule: GlobalRule,
	args: string[],
	flags: Record<string, string | boolean>,
): string | undefined {
	if (rule.extra) {
		// project-folder create-child passes --name NAME as a plain flag.
		return asString(flags[rule.extra],) ?? (asString(jsonInput(flags,)?.[rule.extra],));
	}
	const body = jsonInput(flags,);
	if (rule.kind === "workspace" || rule.kind === "data-collection") {
		return asString(body?.["workspaceKey"],) ?? asString(body?.["id"],)
			?? asString(body?.["displayName"],);
	}
	if (rule.kind === "api-deployer-infra" || rule.kind === "project-deployer-infra") {
		return asString(body?.["id"],);
	}
	if (rule.kind === "api-deployer-service") return asString(body?.["publishedServiceId"],);
	if (rule.kind === "project-deployer-project") return asString(body?.["publishedProjectKey"],);
	if (rule.kind === "api-deployer-deployment" || rule.kind === "project-deployer-deployment") {
		return asString(body?.["deploymentId"],);
	}
	if (rule.kind === "user") return asString(body?.["login"],);
	if (rule.kind === "group") return asString(body?.["name"],);
	if (rule.kind === "connection") return asString(body?.["name"],);
	if (rule.kind === "code-env") return asString(args[3],);
	return asString(args[2],);
}
/** The positional that carries the owned id for a target action. */
function targetNameArg(rule: GlobalRule, action: string, args: string[],): string {
	if (rule.kind === "code-env") {
		const lang = args[2] ?? "";
		const env = args[3] ?? "";
		return `${lang.toUpperCase()}/${env}`;
	}
	return args[2] ?? "";
}
type GlobalRule = {
	kind: OwnedGlobalKind;
	/** Action allowed only while a matching pending reservation exists. */
	create?: boolean;
	/** Action allowed on an existing bound entry; true = args[2] is the id. */
	target?: boolean | "settings";
	/** Extra argv accepted on bound entries before the positional id. */
	extra?: string;
};
type CreationAuthority =
	| { kind: "plugin-json"; name: string; }
	| { kind: "plugin-git"; name: string; repository: string; checkout: string; }
	| { kind: "plugin-archive" | "project-bundle"; name: string; file: string; }
	| { kind: "app-generated"; appId: string; mode: "direct" | "successor"; };
const GLOBAL_RULES: Record<string, GlobalRule> = {
	"user.create": { kind: "user", create: true, },
	"user.update": { kind: "user", target: true, },
	"user.delete": { kind: "user", target: true, },
	"group.create": { kind: "group", create: true, },
	"group.update": { kind: "group", target: true, },
	"group.delete": { kind: "group", target: true, },
	"meaning.create": { kind: "meaning", create: true, },
	"meaning.update": { kind: "meaning", target: true, },
	"meaning.delete": { kind: "meaning", target: true, },
	"workspace.create": { kind: "workspace", create: true, },
	"workspace.update-settings": { kind: "workspace", target: true, },
	"workspace.delete": { kind: "workspace", target: true, },
	"workspace.add-object": { kind: "workspace", target: true, },
	"data-collection.create": { kind: "data-collection", create: true, extra: "displayName", },
	"data-collection.settings-set": { kind: "data-collection", target: true, },
	"data-collection.add-object": { kind: "data-collection", target: true, },
	"data-collection.remove-dataset": { kind: "data-collection", target: true, },
	"data-collection.delete": { kind: "data-collection", target: true, },
	"project-folder.create-child": { kind: "project-folder", create: true, extra: "name", },
	"project-folder.settings-set": { kind: "project-folder", target: true, },
	"project-folder.move": { kind: "project-folder", target: true, },
	"project-folder.delete": { kind: "project-folder", target: true, },
	"connection.create": { kind: "connection", create: true, },
	"connection.update": { kind: "connection", target: true, },
	"connection.delete": { kind: "connection", target: true, },
	"code-env.create": { kind: "code-env", create: true, },
	"code-env.set-definition": { kind: "code-env", target: true, },
	"code-env.set-packages": { kind: "code-env", target: true, },
	"code-env.update-packages": { kind: "code-env", target: true, },
	"code-env.update-images": { kind: "code-env", target: true, },
	"code-env.set-jupyter": { kind: "code-env", target: true, },
	"code-env.delete": { kind: "code-env", target: true, },
	"plugin.create-dev": { kind: "plugin", create: true, },
	"plugin.install-from-zip": { kind: "plugin", create: true, },
	"plugin.install-from-git": { kind: "plugin", create: true, },
	"plugin.update-from-git": { kind: "plugin", target: true, },
	"plugin.delete": { kind: "plugin", target: true, },
	"plugin.settings-set": { kind: "plugin", target: "settings", },
	"plugin.contents-put": { kind: "plugin", target: "settings", },
	"plugin.contents-delete": { kind: "plugin", target: "settings", },
	"plugin.download": { kind: "plugin", target: true, },
	"plugin.update-from-zip": { kind: "plugin", target: true, },
	"plugin.reset-local": { kind: "plugin", target: true, },
	"plugin.move-to-dev": { kind: "plugin", target: true, },
	"plugin.folder-add": { kind: "plugin", target: true, },
	"plugin.rename": { kind: "plugin", target: true, },
	"plugin.move": { kind: "plugin", target: true, },
	"plugin.code-env-create": { kind: "plugin", target: true, },
	"plugin.code-env-update": { kind: "plugin", target: true, },
	"api-deployer.create-infra": { kind: "api-deployer-infra", create: true, },
	"api-deployer.delete-infra": { kind: "api-deployer-infra", target: true, },
	"api-deployer.create-service": { kind: "api-deployer-service", create: true, },
	"api-deployer.delete-service": { kind: "api-deployer-service", target: true, },
	"api-deployer.publish-version": { kind: "api-deployer-service", target: true, },
	"api-deployer.delete-version": { kind: "api-deployer-service", target: true, },
	"api-deployer.create-deployment": { kind: "api-deployer-deployment", create: true, },
	"api-deployer.delete-deployment": { kind: "api-deployer-deployment", target: true, },
	"api-deployer.save-deployment-settings": { kind: "api-deployer-deployment", target: true, },
	"api-deployer.deploy": { kind: "api-deployer-deployment", target: true, },
	"project-deployer.create-infra": { kind: "project-deployer-infra", create: true, },
	"project-deployer.create-project": { kind: "project-deployer-project", create: true, },
	"project-deployer.create-deployment": { kind: "project-deployer-deployment", create: true, },
	"project-deployer.delete-deployment": { kind: "project-deployer-deployment", target: true, },
	"project-deployer.save-deployment-settings": {
		kind: "project-deployer-deployment",
		target: true,
	},
	"project-deployer.deploy": { kind: "project-deployer-deployment", target: true, },
	"plugin.set-git-remote": { kind: "plugin", target: true, },
	"plugin.delete-git-remote": { kind: "plugin", target: true, },
	"plugin.fetch": { kind: "plugin", target: true, },
	"plugin.push": { kind: "plugin", target: true, },
	"plugin.pull": { kind: "plugin", target: true, },
	"plugin.reset-remote": { kind: "plugin", target: true, },
};
function globalEntry(
	kind: OwnedGlobalKind,
	manifest: LiveManifest,
	name: string,
): OwnedGlobal | undefined {
	return manifest.globals.find(g => g.kind === kind && g.name === name);
}
function assertOwnedGlobalScope(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	manifest: LiveManifest,
	authority?: CreationAuthority,
): void {
	const rule = GLOBAL_RULES[`${resource}.${action}`];
	if (!rule) return;
	if (rule.create) {
		if (resource === "plugin" && action === "create-dev" && flags["creation-mode"] !== "EMPTY") {
			throw new Error("Plugin creation from external git sources needs an isolated harness",);
		}
		const archiveInstall = resource === "plugin" && action === "install-from-zip";
		const pluginArchiveArmed = authority?.kind === "plugin-archive" ? authority : undefined;
		if (archiveInstall && (!pluginArchiveArmed || flags["file"] !== pluginArchiveArmed.file)) {
			throw new Error("Plugin archive installation requires a validated private reservation",);
		}
		const gitInstall = resource === "plugin" && action === "install-from-git";
		if (
			gitInstall
			&& (authority?.kind !== "plugin-git" || flags.repository !== authority.repository
				|| flags.checkout !== authority.checkout)
		) {
			throw new Error("Git plugin installation requires an inspected owned reservation",);
		}
		const name = gitInstall
			? authority?.kind === "plugin-git" ? authority.name : undefined
			: archiveInstall
			? pluginArchiveArmed?.name
			: createNameArg(rule, args, flags,);
		const entry = typeof name === "string" ? globalEntry(rule.kind, manifest, name,) : undefined;
		if (!entry || entry.state !== "pending") {
			throw new Error(
				`Global create requires an exact pending reservation: ${resource}.${action} ${name ?? ""}`,
			);
		}
		return;
	}
	const target = targetNameArg(rule, action, args,);
	if (target === "") {
		throw new Error(`Global mutation target missing: ${resource}.${action}`,);
	}
	const entry = manifest.globals.find(g => g.kind === rule.kind && (g.id ?? g.name) === target);
	if (!entry) {
		throw new Error(`Global target is not owned by this run: ${resource}.${action} ${target}`,);
	}
	// Pre-bind writes are denied. The ONLY exception is the atomic plugin
	// bootstrap: a one-shot grant armed by createPluginDev for exactly this
	// plugin id, consumed by run() immediately after this check. A direct
	// ctx.run of the same argv without the grant is refused.
	const isArmedBootstrapWrite = rule.kind === "plugin"
		&& entry.state === "pending"
		&& action === "contents-put"
		&& args[3] === "plugin.json"
		&& authority?.kind === "plugin-json" && authority.name === target;
	if (entry.state !== "bound" && !isArmedBootstrapWrite) {
		throw new Error(
			`Global target creation is not confirmed; refusing mutation: ${resource}.${action} ${target}`,
		);
	}
}
export function assertLiveCommandScope(
	argv: string[],
	manifest: LiveManifest,
	selectedProject: string,
	authority?: CreationAuthority,
	gitAuthority?: typeof GIT_AUTHORITY,
): void {
	const { positional: args, flags, } = parseArgs(argv,);
	const resource = args[0] ?? "";
	const action = args[1] ?? "run";
	const entry = commandRegistry[resource]?.[action];
	if (!entry) throw new Error(`Unregistered live command: ${resource}.${action}`,);
	for (const flag of ["url", "api-key", "insecure", "ca-cert", "record-cleanup", "stdin",]) {
		if (flags[flag] !== undefined) throw new Error(`Live cases cannot override --${flag}`,);
	}
	if (
		resource === "auth" || resource === "cleanup" || resource === "batch"
		|| resource === "install-skill"
	) throw new Error(`Command needs a dedicated isolated harness: ${resource}`,);
	const owned = (key: unknown, pending = false,) =>
		typeof key === "string"
		&& manifest.projects.some(p =>
			p.key === key && (p.state === "bound" || pending && p.state === "pending")
		);
	for (const [flag, value,] of Object.entries(flags,)) {
		const publishedTarget = flag === "published-project-key" && resource === "bundle"
			&& action === "publish"
			&& manifest.globals.some(g =>
				g.kind === "project-deployer-project" && g.name === value && g.state === "bound"
			);
		if (
			flag.endsWith("project-key",) && !publishedTarget
			&& !owned(value, flag === "target-project-key",)
		) {
			throw new Error(`Refusing foreign project flag: --${flag}`,);
		}
	}
	if (entry.requiresProject && !owned(selectedProject,)) {
		throw new Error(`Project is not bound to this run: ${selectedProject}`,);
	}
	if (resource === "project" && ["create", "duplicate", "import", "delete",].includes(action,)) {
		if (flags.data !== undefined || flags["data-file"] !== undefined) {
			throw new Error("Project lifecycle JSON options need a dedicated scoped harness",);
		}
		const target = action === "duplicate"
			? args[3]
			: action === "import"
			? flags["target-project-key"]
			: args[2];
		if (!owned(target, action !== "delete",)) {
			throw new Error("Project lifecycle target is not owned by this run",);
		}
		if (action === "duplicate" && !owned(args[2],)) {
			throw new Error("Cannot duplicate an unowned project",);
		}
		if (action === "delete") {
			const identity = manifest.projects.find(p => p.key === target)!;
			if (!identity.incarnation || flags["expect-project-incarnation"] !== identity.incarnation) {
				throw new Error("Deletion requires the bound project incarnation",);
			}
		} else if (manifest.projects.find(p => p.key === target)?.state !== "pending") {
			throw new Error("Creation requires a reserved new project",);
		}
		return;
	}
	if (resource === "project" && action === "export" && !owned(args[2],)) {
		throw new Error("Cannot export an unowned project",);
	}
	if (resource === "app" && ["create-instance", "create-successor-instance",].includes(action,)) {
		if (action === "create-successor-instance" && !owned(flags.from,)) {
			throw new Error("Cannot create successor from an unowned project",);
		}
		const target = appCreateTarget(action, flags,);
		if (target === undefined) {
			// Generated-key mode: the CLI generates the APP_<uuid> target
			// client-side, so no key exists to reserve and none may be adopted
			// here. The ONLY pass is the private one-shot grant armed by
			// runGeneratedAppCreation for exactly this action and app id; a
			// direct ctx.run of the same shape (without the grant) is refused,
			// and a caller-supplied target key still takes the strict path below.
			const grant = authority?.kind === "app-generated" ? authority : undefined;
			const mode = action === "create-successor-instance" ? "successor" : "direct";
			if (!grant || grant.mode !== mode || grant.appId !== args[2]) {
				throw new Error("Generated-key app creation requires the private one-shot authorization",);
			}
			return;
		}
		if (!owned(target, true,) || manifest.projects.find(p => p.key === target)?.state !== "pending") {
			throw new Error("App creation requires a reserved project target",);
		}
		return;
	}
	if (
		(resource === "future" && action === "abort"
			|| resource === "project-git" && action === "future-abort")
		&& !executionMode(flags,).plan
	) {
		const id = args[2];
		if (typeof id !== "string" || !manifest.futures.includes(id,)) {
			throw new Error(`Future abort requires a future started by this lab: ${id ?? ""}`,);
		}
		return;
	}
	if (
		resource === "plugin"
		&& [
			"install-from-git",
			"update-from-git",
			"set-git-remote",
			"delete-git-remote",
			"fetch",
			"push",
			"pull",
			"reset-remote",
		].includes(action,) && gitAuthority !== GIT_AUTHORITY
	) {
		throw new Error("Plugin Git mutation requires an owned Git scope",);
	}
	if (resource === "project-git" && entry.mutatesDss && gitAuthority !== GIT_AUTHORITY) {
		const local: Record<string, true> = {
			"commit": true,
			"create-branch": true,
			"delete-branch": true,
			"create-tag": true,
			"delete-tag": true,
			"switch": true,
			"revert-to-revision": true,
			"revert-commit": true,
			"reset-to-head": true,
			"reset-all-libraries": true,
			"drop-and-rebuild": true,
		};
		const touchesRemote = flags["remote"] !== undefined || flags["delete-remotely"] !== undefined
			|| flags["target-project-key"] !== undefined
			|| flags["target-project-folder-id"] !== undefined;
		if (!local[action] || touchesRemote) {
			throw new Error(`Git actions reach beyond the owned project checkout: ${action}`,);
		}
	}
	if (resource === "project-folder" && ["move", "move-project", "create-child",].includes(action,)) {
		const ownedFolder = (id: string | undefined,) =>
			manifest.globals.some(g => g.kind === "project-folder" && g.state === "bound" && g.id === id);
		const container = (id: string | undefined,) => id === "ROOT" || ownedFolder(id,);
		if (action === "create-child") {
			if (!container(args[2],)) throw new Error("New folder parent must be ROOT or an owned folder",);
		} else if (action === "move-project") {
			if (!owned(args[3],) || !container(args[2],) || !container(args[4],)) {
				throw new Error(
					"Moving a project requires its owned identity and owned source/destination folders",
				);
			}
			return;
		} else {
			if (!ownedFolder(args[2],) || !container(args[3],)) {
				throw new Error("Moving a folder requires owned source and destination",);
			}
			return;
		}
	}

	if (resource === "project-deployer" && action === "upload-bundle") {
		const grant = authority?.kind === "project-bundle" ? authority : undefined;
		const reserved = grant && globalEntry("project-deployer-project", manifest, grant.name,);
		if (
			!grant || grant.file !== args[2] || !reserved || !["pending", "bound",].includes(reserved.state,)
			|| !owned(grant.name,)
		) {
			throw new Error("Bundle upload requires an inspected archive for the exact owned project",);
		}
		return;
	}
	if (resource === "bundle" && action === "publish") {
		const key = asString(flags["published-project-key"],) ?? selectedProject;
		const target = globalEntry("project-deployer-project", manifest, key,);
		if (target?.state !== "bound" || !target.identity) {
			throw new Error("Bundle publication requires a bound owned published project",);
		}
	}
	assertOwnedGlobalScope(resource, action, args, flags, manifest, authority,);
	if (executionMode(flags,).plan) return;
	// A global rule already verified an exact owned reservation/target above;
	// the project-sandbox fallback below must not reject it again. Read-only
	// plugin commands mislabeled mutatesDss are scoped to owned plugins too.
	const globalRule = GLOBAL_RULES[`${resource}.${action}`];
	if (globalRule) return;
	if (
		resource === "plugin"
		&& ["settings-get", "contents-get", "details", "contents-list", "get-git-remote",].includes(
			action,
		)
	) {
		const pluginId = args[2];
		if (
			typeof pluginId === "string" && pluginId !== ""
			&& manifest.globals.some(g => g.kind === "plugin" && g.name === pluginId && g.state === "bound")
		) return;
	}
	if (
		resource === "sql" && action === "query"
		&& manifest.profiles.includes("infrastructure",)
		&& args.length === 3 && args[2] === LIVE_SQL_PROBE
		&& owned(flags["project-key"],) && flags["project-key"] === selectedProject
		&& Object.keys(flags,).every(flag => ["connection", "dataset", "project-key",].includes(flag,))
		&& [flags.connection, flags.dataset,].filter(value => typeof value === "string" && value.trim())
				.length === 1
		&& (flags.connection === undefined || flags.dataset === undefined)
	) return;
	if (entry.mutatesDss && !entry.requiresProject) {
		throw new Error(
			`Global mutation is not authorized by a project sandbox: ${resource}.${action}`,
		);
	}
}

export class LiveRunContext implements LiveContext {
	readonly dir: string;
	readonly client: DataikuClient;
	/** JobIds harvested from successful future-producing commands, with their origin command. */
	private readonly futureReceipts = new Map<string, string>();
	private creationAuthority: CreationAuthority | undefined;
	private gitScope?: {
		target: OwnedGitTarget;
		directory: OwnedHostDirectory;
		urls: ReadonlySet<string>;
	};

	private async runHost(
		directory: OwnedHostDirectory,
		operation: Parameters<typeof hostDirectoryScript>[1],
		repositories: readonly string[] = [],
		service: "sse" | undefined = directory.daemon?.service,
	): Promise<string> {
		const file = await this.writeFile(
			`host-${directory.nonce}-${operation}.py`,
			hostDirectoryScript(directory, operation, repositories, service,),
		);
		const result = await this.run<{ success: boolean; output: string; }>([
			"code",
			"run",
			"--file",
			file,
			"--timeout",
			"120000",
		], { projectKey: directory.projectKey, },);
		if (!result.success || !result.output.split("\n",).includes("HOST_OK",)) {
			throw new Error(`Owned host operation failed: ${operation}`,);
		}
		return result.output;
	}

	private async startHostDaemon(
		directory: OwnedHostDirectory,
		repositories: readonly string[],
		service?: "sse",
	): Promise<HostDaemon> {
		const output = await this.runHost(directory, "start", repositories, service,);
		const line = output.split("\n",).find(value => value.startsWith("HOST_RESULT=",));
		if (!line) throw new Error("Host daemon returned no ownership receipt",);
		const daemon: unknown = JSON.parse(line.slice("HOST_RESULT=".length,),);
		if (
			!isHostDaemon(daemon, directory,) || daemon.service !== service
			|| JSON.stringify(daemon.repositories,) !== JSON.stringify(repositories,)
		) throw new Error("Invalid host daemon receipt",);
		directory.daemon = daemon;
		await this.save();
		return daemon;
	}

	async startOwnedSseServer(handle: HostDirectory,): Promise<HostDaemon> {
		const directory = this.manifest.ownedDirectories.find(d =>
			d.path === handle.path && d.nonce === handle.nonce && d.projectKey === handle.projectKey
			&& d.state === "bound"
		);
		if (!directory || directory.daemon) {
			throw new Error("SSE server requires an owned directory without an active daemon",);
		}
		return await this.startHostDaemon(directory, [], "sse",);
	}

	async withOwnedHostDirectory<T,>(
		label: string,
		body: (directory: HostDirectory,) => Promise<T>,
	): Promise<T> {
		const projectKey = await this.createProject(`host_${label}`,);
		const nonce = randomUUID().replaceAll("-", "",);
		const directory: OwnedHostDirectory = {
			path: `/tmp/sdk_live_${this.runId}_${nonce}`,
			nonce,
			projectKey,
			runId: this.runId,
			state: "pending",
		};
		this.manifest.ownedDirectories.push(directory,);
		try {
			await this.save();
			await this.runHost(directory, "create",);
			directory.state = "bound";
			await this.save();
			return await body(directory,);
		} finally {
			await this.cleanupHostDirectory(directory,);
			await this.deleteProject(projectKey,);
		}
	}

	private async cleanupHostDirectory(directory: OwnedHostDirectory,): Promise<void> {
		if (directory.state === "deleted") return;
		await this.runHost(directory, "delete",);
		directory.state = "deleted";
		delete directory.daemon;
		await this.save();
	}

	async withOwnedGitScope<T,>(
		target: OwnedGitTarget,
		handle: HostDirectory,
		repositoryPaths: readonly string[],
		body: (urls: readonly string[],) => Promise<T>,
	): Promise<T> {
		if (this.gitScope) throw new Error("Nested Git scopes are not allowed",);
		const directory = this.manifest.ownedDirectories.find(d =>
			d.path === handle.path && d.nonce === handle.nonce && d.projectKey === handle.projectKey
			&& d.state === "bound"
		);
		const targetOwned = target.kind === "project"
			? this.projects.some(p => p.key === target.key && p.state === "bound")
			: this.globals.some(g =>
				g.kind === "plugin" && g.name === target.id && (g.state === "pending" || g.state === "bound")
			);
		if (!directory || !targetOwned) {
			throw new Error("Git scope requires owned directory and reserved target",);
		}
		try {
			const daemon = await this.startHostDaemon(directory, repositoryPaths,);
			const urls = repositoryPaths.map(repo =>
				`http://127.0.0.1:${daemon.port}/${encodeURIComponent(path.posix.basename(repo,),)}`
			);
			this.gitScope = { target, directory, urls: new Set(urls,), };
			return await body(urls,);
		} finally {
			this.gitScope = undefined;
			await this.runHost(directory, "stop",);
			delete directory.daemon;
			await this.save();
		}
	}

	private async authorizeGit(
		argv: string[],
		projectKey: string,
		authority?: CreationAuthority,
	): Promise<typeof GIT_AUTHORITY | undefined> {
		const { positional, flags, } = parseArgs(argv,);
		if (positional[0] === "plugin") return this.authorizePluginGit(positional, flags, authority,);
		if (positional[0] !== "project-git") return;
		const action = positional[1]!;
		const scoped = [
			"set-remote",
			"remove-remote",
			"fetch",
			"push",
			"pull",
			"reset-to-upstream",
			"add-library",
			"set-library",
			"remove-library",
			"reset-library",
			"push-library",
			"push-all-libraries",
			"reset-all-libraries",
		];
		if (!scoped.includes(action,)) return;
		const grant = this.gitScope;
		if (!grant) {
			if (
				action === "reset-all-libraries"
				&& (await this.client.projectGit.listLibraries(projectKey,)).length
			) throw new Error("Resetting attached libraries requires an owned Git scope",);
			return;
		}
		if (
			grant.target.kind !== "project" || grant.target.key !== projectKey
			|| grant.directory.state !== "bound"
		) {
			throw new Error("Git scope belongs to another project",);
		}
		for (
			const flag of [
				"remote",
				"delete-remotely",
				"target-project-key",
				"target-project-folder-id",
				"login",
				"password-env",
			]
		) {
			if (flags[flag] !== undefined) throw new Error(`Unsupported scoped Git flag: ${flag}`,);
		}
		if (action.includes("library",) || action.endsWith("libraries",)) {
			const libraries = await this.client.projectGit.listLibraries(projectKey,);
			if (
				libraries.some(library =>
					!("remote" in library) || typeof library.remote !== "string"
					|| !grant.urls.has(library.remote,)
				)
			) {
				throw new Error("Git scope contains a foreign library",);
			}
			if (action === "add-library" || action === "set-library") {
				if (typeof flags.repository !== "string" || !grant.urls.has(flags.repository,)) {
					throw new Error("Git library URL is not owned",);
				}
			}
		} else {
			const name = typeof flags.name === "string" ? flags.name : "origin";
			const remote = await this.client.projectGit.getRemote(projectKey, name,);
			if (remote.url && !grant.urls.has(remote.url,)) {
				throw new Error("Current Git remote is not owned",);
			}
			if (action === "set-remote") {
				if (typeof flags.repository !== "string" || !grant.urls.has(flags.repository,)) {
					throw new Error("Git remote URL is not owned",);
				}
			} else if (action !== "remove-remote" && !remote.url) {
				throw new Error("Owned Git remote is missing",);
			}
		}
		await this.runHost(grant.directory, "verify",);
		return GIT_AUTHORITY;
	}
	private async authorizePluginGit(
		args: string[],
		flags: Record<string, string | boolean>,
		authority?: CreationAuthority,
	): Promise<typeof GIT_AUTHORITY | undefined> {
		const action = args[1]!;
		if (
			![
				"install-from-git",
				"update-from-git",
				"set-git-remote",
				"delete-git-remote",
				"fetch",
				"push",
				"pull",
				"reset-remote",
			].includes(action,)
		) return;
		const grant = this.gitScope;
		if (!grant || grant.target.kind !== "plugin" || grant.directory.state !== "bound") {
			throw new Error("Plugin Git mutation requires an owned Git scope",);
		}
		if (flags["path-in-repository"] !== undefined) {
			throw new Error("Scoped plugin sources must use the repository root",);
		}
		if (action === "install-from-git" || action === "update-from-git") {
			if (
				authority?.kind !== "plugin-git" || authority.name !== grant.target.id
				|| flags.repository !== authority.repository || flags.checkout !== authority.checkout
				|| !grant.urls.has(authority.repository,)
				|| action === "update-from-git" && args[2] !== authority.name
			) throw new Error("Plugin Git source was not inspected",);
		} else {
			if (args[2] !== grant.target.id) throw new Error("Git scope belongs to another plugin",);
			await this.assertGlobalIdentity("plugin", grant.target.id,);
			const remote = await this.client.plugins.getGitRemote(grant.target.id,);
			if (remote.repositoryUrl && !grant.urls.has(remote.repositoryUrl,)) {
				throw new Error("Plugin Git remote is not owned",);
			}
			if (action === "set-git-remote") {
				if (typeof flags.repository !== "string" || !grant.urls.has(flags.repository,)) {
					throw new Error("Plugin Git URL is not owned",);
				}
			} else if (action !== "delete-git-remote" && !remote.repositoryUrl) {
				throw new Error("Owned plugin Git remote is missing",);
			}
		}
		await this.runHost(grant.directory, "verify",);
		return GIT_AUTHORITY;
	}
	async pluginFromGit(
		action: "install" | "update",
		name: string,
		url: string,
		revision: string,
	): Promise<OwnedGlobal> {
		const grant = this.gitScope;
		if (
			!grant || grant.target.kind !== "plugin" || grant.target.id !== name || !grant.urls.has(url,)
		) throw new Error("Plugin installation requires the exact owned Git scope",);
		const owned = this.globals.find(g => g.kind === "plugin" && g.name === name);
		if (!owned || owned.state !== (action === "install" ? "pending" : "bound")) {
			throw new Error("Plugin Git mutation requires a matching reservation state",);
		}
		const repositoryPath = grant.directory.daemon!.repositories.find(repo =>
			new URL(url,).pathname === `/${encodeURIComponent(path.posix.basename(repo,),)}`
		);
		if (!repositoryPath) throw new Error("Plugin repository is not in the daemon receipt",);
		const checkout = await readBare(this, grant.directory, repositoryPath, [
			"rev-parse",
			"--verify",
			"--end-of-options",
			`${revision}^{commit}`,
		],);
		if (!/^[a-f0-9]{40}$/.test(checkout,)) {
			throw new Error("Plugin revision did not resolve to a commit",);
		}
		const manifest = asRecord(
			safeParseJson(
				await readBare(this, grant.directory, repositoryPath, ["show", `${checkout}:plugin.json`,],),
			),
		);
		const description = asRecord(manifest?.meta,)?.description;
		if (
			manifest?.id !== name || typeof description !== "string"
			|| !description.includes(this.markerFor("plugin", name,),)
		) throw new Error("Plugin Git manifest must match its reserved id and nonce",);
		if (action === "install") await this.assertGlobalAbsent("plugin", name,);
		else await this.assertGlobalIdentity("plugin", name,);
		this.creationAuthority = { kind: "plugin-git", name, repository: url, checkout, };
		try {
			const receipt = asRecord(
				await this.run([
					"plugin",
					`${action}-from-git`,
					...(action === "update" ? [name,] : []),
					"--repository",
					url,
					"--checkout",
					checkout,
				],),
			);
			if (receipt?.[action === "install" ? "installed" : "updated"] !== true) {
				throw new Error("Plugin Git mutation returned no success receipt",);
			}
		} catch (error) {
			if (action === "install") {
				owned.state = "unconfirmed";
				owned.reason = this.redact(error,);
				await this.save();
			}
			throw error;
		} finally {
			this.creationAuthority = undefined;
		}
		return action === "install"
			? this.bindGlobal("plugin", name,)
			: this.assertGlobalIdentity("plugin", name,);
	}
	constructor(
		readonly manifestPath: string,
		readonly manifest: LiveManifest,
		readonly credentials: LiveCredentials,
		readonly phase: "setup" | "run" = "run",
		readonly selection: string[] = [],
	) {
		if (canonicalDssUrl(credentials.url,) !== canonicalDssUrl(manifest.dssUrl,)) {
			throw new Error("Live manifest belongs to a different DSS server",);
		}
		this.dir = path.dirname(path.resolve(manifestPath,),);
		this.client = new DataikuClient({
			...credentials,
			projectKey: manifest.projectKey || undefined,
			requestTimeoutMs: 30000,
			retryMaxAttempts: 1,
		},);
	}
	get iteration() {
		return this.manifest.iteration;
	}
	get runId() {
		return this.manifest.runId;
	}
	get projectKey() {
		return this.manifest.projectKey;
	}
	get connection() {
		return this.manifest.connection;
	}
	get owner() {
		return this.manifest.owner;
	}
	get profiles() {
		return this.manifest.profiles;
	}
	get fixtures() {
		return this.manifest.fixtures;
	}
	redact(value: unknown,): string {
		const text = value instanceof Error ? value.message : String(value,);
		return redactReportSecrets(text, this.credentials.apiKey,);
	}
	async save() {
		await writeLiveJson(this.manifestPath, this.manifest,);
	}
	async writeFile(name: string, content: string | Uint8Array,): Promise<string> {
		const destination = path.resolve(this.dir, name,);
		if (destination === this.dir || !destination.startsWith(this.dir + path.sep,)) {
			throw new Error("Fixture file must stay inside run directory",);
		}
		await fs.mkdir(path.dirname(destination,), { recursive: true, },);
		await fs.writeFile(destination, content, { mode: 0o600, },);
		return destination;
	}
	childEnv(projectKey = this.projectKey,): NodeJS.ProcessEnv {
		return {
			...process.env,
			DATAIKU_URL: this.credentials.url,
			DATAIKU_API_KEY: this.credentials.apiKey,
			DATAIKU_PROJECT_KEY: projectKey,
			DATAIKU_DISABLE_ENV: "0",
			DSS_CONFIG_DIR: path.join(this.dir, "config",),
			NODE_TLS_REJECT_UNAUTHORIZED: this.credentials.tlsRejectUnauthorized === false ? "0" : "1",
			NODE_EXTRA_CA_CERTS: this.credentials.caCertPath,
			DATAIKU_TEST_CONNECTION: this.connection,
		};
	}
	private async assertGlobalIdentity(kind: OwnedGlobalKind, id: string,): Promise<OwnedGlobal> {
		const entry = this.manifest.globals.find(value =>
			value.kind === kind && (value.id ?? value.name) === id
		);
		if (!entry || entry.state !== "bound" || !entry.identity) {
			throw new Error(`Global target is not a confirmed bound identity: ${id}`,);
		}
		const record = asRecord(await this.fetchGlobal(kind, id,),);
		const matches = kind === "code-env"
			? record !== undefined
				&& stableStringify(asRecord(record["desc"],)?.["creationTag"],) === entry.identity
			: record !== undefined
				&& globalMarkerVerified(
					kind,
					record,
					this.markerFor(kind, entry.name,),
					entry.name,
					id,
				);
		if (!matches) throw new Error(`Global identity changed before mutation: ${id}`,);
		return entry;
	}
	private async ownedArchivePath(input: string,): Promise<string> {
		const file = await fs.realpath(input,);
		const relative = path.relative(await fs.realpath(this.dir,), file,);
		if (
			!relative || relative === ".." || relative.startsWith(`..${path.sep}`,)
			|| path.isAbsolute(relative,)
		) {
			throw new Error("Installation archive must be inside this lab directory",);
		}
		return file;
	}

	async run<T = unknown,>(
		argv: string[],
		options: { projectKey?: string; expectedExit?: number; } = {},
	): Promise<T> {
		const parsed = parseArgs(argv,);
		const resource = parsed.positional[0]!;
		const action = parsed.positional[1] ?? "run";
		const selected = typeof parsed.flags["project-key"] === "string"
			? parsed.flags["project-key"]
			: options.projectKey ?? this.projectKey;
		if (
			options.projectKey !== undefined && parsed.flags["project-key"] !== undefined
			&& options.projectKey !== parsed.flags["project-key"]
		) throw new Error("Conflicting live project selection",);
		const entry = commandRegistry[resource]?.[action];
		const args = [...argv,];
		if (entry?.requiresProject && parsed.flags["project-key"] === undefined) {
			args.push("--project-key", selected,);
		}
		let authority = this.creationAuthority;
		this.creationAuthority = undefined;
		if (resource === "project-deployer" && action === "upload-bundle") {
			const file = await this.ownedArchivePath(parsed.positional[2] ?? "",);
			const archive = await inspectProjectArchive(file,);
			if (!archive.valid || !archive.sourceProjectKey) {
				throw new Error("Bundle archive has no valid source-project identity",);
			}
			const key = archive.sourceProjectKey;
			const project = this.manifest.projects.find(value =>
				value.key === key && value.state === "bound"
			);
			const global = globalEntry("project-deployer-project", this.manifest, key,);
			if (!project?.incarnation || !global || !["pending", "bound",].includes(global.state,)) {
				throw new Error(
					"Bundle upload requires the exact owned Design project and published-project reservation",
				);
			}
			if (projectIncarnationHash(key, await this.client.projects.get(key,),) !== project.incarnation) {
				throw new Error("Bundle source-project incarnation changed",);
			}
			if (global.state === "pending") await this.assertGlobalAbsent("project-deployer-project", key,);
			else await this.assertGlobalIdentity("project-deployer-project", key,);
			args[2] = file;
			authority = { kind: "project-bundle", name: key, file, };
		}
		const gitAuthority = await this.authorizeGit(args, selected, authority,);
		assertLiveCommandScope(args, this.manifest, selected, authority, gitAuthority,);
		if (
			entry?.mutatesDss && (entry.requiresProject || resource === "sql" && action === "query")
			&& !executionMode(parsed.flags,).plan
		) {
			const identity = this.manifest.projects.find(p => p.key === selected);
			const details = await this.client.projects.get(selected,);
			if (
				!identity?.incarnation || projectIncarnationHash(selected, details,) !== identity.incarnation
			) throw new Error(`Project incarnation changed before mutation: ${selected}`,);
		}
		if (
			resource === "project-folder" && !executionMode(parsed.flags,).plan
			&& !executionMode(parsed.flags,).dryRun
		) {
			const folderIds = action === "move-project"
				? [args[2], args[4],]
				: action === "move"
				? [args[3],]
				: action === "create-child"
				? [args[2],]
				: [];
			if (action === "move-project") {
				const project = this.manifest.projects.find(p => p.key === args[3] && p.state === "bound");
				if (
					!project?.incarnation
					|| projectIncarnationHash(project.key, await this.client.projects.get(project.key,),)
						!== project.incarnation
				) {
					throw new Error("Project incarnation changed before folder move",);
				}
			}
			for (const id of new Set(folderIds,)) {
				if (id && id !== "ROOT") await this.assertGlobalIdentity("project-folder", id,);
			}
		}

		const globalRule = GLOBAL_RULES[`${resource}.${action}`];
		if (
			globalRule && !globalRule.create && !executionMode(parsed.flags,).plan
			&& !executionMode(parsed.flags,).dryRun
		) {
			const target = targetNameArg(globalRule, action, args,);
			const bootstrap = globalRule.kind === "plugin" && action === "contents-put"
				&& args[3] === "plugin.json"
				&& authority?.kind === "plugin-json" && authority.name === target;
			if (!bootstrap) await this.assertGlobalIdentity(globalRule.kind, target,);
		}
		if (
			resource === "bundle" && action === "publish" && !executionMode(parsed.flags,).plan
			&& !executionMode(parsed.flags,).dryRun
		) {
			await this.assertGlobalIdentity(
				"project-deployer-project",
				asString(parsed.flags["published-project-key"],) ?? selected,
			);
		}
		if (entry?.producesLocalFile) {
			for (const flag of ["output", "output-file",]) {
				const value = parsed.flags[flag];
				if (typeof value !== "string") continue;
				const output = path.resolve(LIVE_ROOT, value,);
				if (!output.startsWith(this.dir + path.sep,)) {
					throw new Error("Live output must stay inside the run directory",);
				}
			}
		}
		const started = Date.now();
		const child = Bun.spawn([
			process.execPath,
			"--no-env-file",
			path.join(LIVE_ROOT, "src/cli.ts",),
			...args,
		], {
			cwd: LIVE_ROOT,
			env: this.childEnv(selected,),
			stdout: "pipe",
			stderr: "pipe",
		},);
		activeCommands.add(child,);
		const timer = setTimeout(() => child.kill("SIGTERM",), 300000,);
		let exit: number;
		let stdout: string;
		let stderr: string;
		try {
			[exit, stdout, stderr,] = await Promise.all([
				child.exited,
				new Response(child.stdout,).text(),
				new Response(child.stderr,).text(),
			],);
		} finally {
			clearTimeout(timer,);
			activeCommands.delete(child,);
		}
		const mode = executionMode(parsed.flags,);
		this.manifest.commands.push({
			action: `${resource}.${action}`,
			args: redactArgv(args.map(s => this.redact(s,)),),
			exitCode: exit,
			durationMs: Date.now() - started,
			mode: mode.plan ? "plan" : mode.dryRun ? "dry-run" : "execute",
		},);
		await this.save();
		let result: unknown;
		try {
			result = JSON.parse(stdout,);
		} catch {
			throw new LiveCommandError(
				`CLI did not return JSON for ${resource}.${action}: ${this.redact(stderr,).slice(0, 2000,)}`,
				exit,
				null,
			);
		}
		if (exit !== (options.expectedExit ?? 0)) {
			await this.retireUnusedAppCreateTarget(resource, action, parsed.flags, result,);
			throw new LiveCommandError(
				`${resource}.${action} exited ${exit}: ${
					this.redact(JSON.stringify(result,),).slice(0, 2500,)
				}`,
				exit,
				result,
			);
		}
		if (
			exit === 0 && resource === "app" && action === "delete-instance"
			&& !mode.plan && !mode.dryRun
			&& typeof parsed.flags["expect-project-incarnation"] === "string"
		) {
			const receipt = asRecord(result,);
			const project = this.manifest.projects.find(p =>
				p.key === selected && p.state === "bound"
				&& p.incarnation === parsed.flags["expect-project-incarnation"]
			);
			if (project && receipt?.["deleted"] === true && receipt["projectKey"] === selected) {
				// A guarded deletion already succeeded; a later GET may mask absence as 403.
				project.state = "deleted";
				await this.save();
			}
		}
		if (
			exit === 0 && entry?.mutatesDss && entry.async === "future"
			&& !executionMode(parsed.flags,).plan && !executionMode(parsed.flags,).dryRun
		) {
			// Successful future-producing mutations in execute mode are the only
			// receipt source; previews (plan/dry-run) never confer abort rights.
			const receipt = asRecord(result,);
			const jobId = ["jobId", "job_id", "futureId",]
				.map(key => receipt?.[key])
				.find(v => typeof v === "string" && v.length > 0);
			if (typeof jobId === "string") this.futureReceipts.set(jobId, `${resource}.${action}`,);
		}
		return result as T;
	}
	async check(
		id: LiveCaseId,
		actions: readonly string[],
		body: () => Promise<void>,
		options: { capability?: string; required?: boolean; } = {},
	): Promise<void> {
		if (
			this.selection.length
			&& !this.selection.some(selected => matchesLiveCase(id, selected,))
		) return;
		const started = Date.now();
		const first = this.manifest.commands.length;
		const result: CaseResult = {
			id,
			status: "passed",
			actions: [...actions,],
			executedActions: [],
			durationMs: 0,
			required: options.required ?? true,
			...(options.capability ? { capability: options.capability, } : {}),
		};
		try {
			await body();
		} catch (error) {
			result.status = error instanceof LiveCapabilityError ? error.status : "failed";
			if (error instanceof LiveCommandError && options.required === false) {
				const payload = error.result as { status?: number; } | null;
				if (payload?.status === 403 || payload?.status === 401) result.status = "blocked";
				if (payload?.status === 501) result.status = "unsupported";
			}
			if (error instanceof LiveCommandError && noPostTargetAbsenceDetails(error.result,)) {
				// The CLI refused BEFORE the creation POST on real-environment
				// grounds (target project-key absence cannot be verified under
				// this identity). No instance exists and creation never ran, so
				// the case is blocked by the environment, not failed.
				result.status = "blocked";
			}
			result.error = this.redact(error,);
		}
		result.durationMs = Date.now() - started;
		result.executedActions = [
			...new Set(
				this.manifest.commands.slice(first,).filter(c => c.mode === "execute" && c.exitCode === 0).map(
					c => c.action,
				),
			),
		];
		this.manifest.cases.push(result,);
		await this.save();
		process.stdout.write(
			`${
				JSON.stringify({
					case: id,
					status: result.status,
					durationMs: result.durationMs,
					...(result.error ? { error: result.error, } : {}),
				},)
			}\n`,
		);
	}
	/**
	 * An app-instance creation the CLI refused BEFORE the creation POST (exact
	 * structured target-absence refusal) leaves its reserved target project
	 * unused: the reservation is an intent, not an owned artifact, so retire
	 * only that receipt from the manifest — never a server DELETE/GET. Every
	 * condition is exact; anything indeterminate or mismatched keeps the
	 * receipt for inspection.
	 */
	private async retireUnusedAppCreateTarget(
		resource: string,
		action: string,
		flags: Record<string, string | boolean>,
		result: unknown,
	): Promise<void> {
		if (
			resource !== "app"
			|| (action !== "create-instance" && action !== "create-successor-instance")
		) return;
		const details = noPostTargetAbsenceDetails(result,);
		const target = appCreateTarget(action, flags,);
		if (!details || target === undefined || details["targetProjectKey"] !== target) return;
		const index = this.manifest.projects.findIndex(project =>
			project.key === target && project.state === "pending"
		);
		if (index < 0) return;
		this.manifest.projects.splice(index, 1,);
		await this.save();
	}
	async reserveProject(label: string,): Promise<string> {
		const suffix = label.toUpperCase().replace(/[^A-Z0-9_]/g, "_",).slice(0, 16,);
		const prefix = `SDK_LIVE_${this.manifest.runId.toUpperCase()}_${suffix}_`;
		// Retiring unused reservations shrinks the array but leaves other receipts.
		// Skip every recorded key, including deleted projects.
		let index = this.manifest.projects.length;
		while (this.manifest.projects.some(project => project.key === `${prefix}${index}`)) {
			index += 1;
		}
		return this.reserveProjectKey(`${prefix}${index}`,);
	}
	private async reserveProjectKey(key: string,): Promise<string> {
		if (this.manifest.projects.some(project => project.key === key)) {
			// A recorded receipt in ANY state already owns this key: re-reserving
			// it would alias a deleted or bound identity onto a new intent.
			throw new Error(`Refusing to re-reserve a recorded project key: ${key}`,);
		}
		// DSS can return 403 for nonexistent keys; creation is exclusive and never updates a collision.
		const visible = await this.client.projects.list();
		if (visible.some(project => project.projectKey === key)) {
			throw new Error(`Refusing to overwrite an existing project: ${key}`,);
		}
		this.manifest.projects.push({ key, state: "pending", createdAt: new Date().toISOString(), },);
		await this.save();
		return key;
	}
	/**
	 * Run exactly one generated-key app-instance creation against an owned
	 * precursor. This is the CLI's own Designer-friendly mode: the target key is
	 * generated inside the CLI (never reserved here), so nothing can be adopted
	 * from the environment and the strict explicit-key path — a reserved pending
	 * receipt plus ctx.run — stays untouched. The private one-shot authority
	 * arms exactly this argv; a direct ctx.run of the same shape without the
	 * grant is refused by the scope check.
	 *
	 * Truthfulness: the precursor carries the in-flight marker from before the
	 * POST. Only a structured no-creation proof, a definitive rejection, or a
	 * fully bound child (canonical future target identity, matching fresh
	 * APP_INSTANCE incarnation, unrecorded key) clears it; every unknown
	 * outcome retains it so deleteProject keeps refusing the precursor.
	 */
	async runGeneratedAppCreation(argv: string[], precursorProjectKey: string,): Promise<string> {
		// Snapshot the caller-owned array at entry: the exact command parsed and
		// validated here is the exact command the one-shot grant authorizes and
		// run() executes, never a mutable alias a caller could retarget later.
		const command = [...argv,];
		const parsed = parseArgs(command,);
		const resource = parsed.positional[0] ?? "";
		const action = parsed.positional[1] ?? "";
		if (
			resource !== "app"
			|| (action !== "create-instance" && action !== "create-successor-instance")
		) {
			throw new Error("runGeneratedAppCreation authorizes only app instance creation",);
		}
		const mode: "direct" | "successor" = action === "create-successor-instance"
			? "successor"
			: "direct";
		// An explicit target key is the strict reserved path, never generated
		// mode: refusing it here keeps that guard unrelaxed.
		if (appCreateTarget(action, parsed.flags,) !== undefined) {
			throw new Error(
				"runGeneratedAppCreation refuses an explicit target key: reserve it and use ctx.run",
			);
		}
		if (executionMode(parsed.flags,).plan || executionMode(parsed.flags,).dryRun) {
			throw new Error("runGeneratedAppCreation binds a real creation; plan/dry-run is refused",);
		}
		if (parsed.flags["record-cleanup"] !== undefined) {
			throw new Error("runGeneratedAppCreation refuses caller-supplied --record-cleanup",);
		}
		if (mode === "direct" && parsed.flags["wait"] !== true) {
			throw new Error("runGeneratedAppCreation requires --wait to bind a terminal creation",);
		}
		const appId = parsed.positional[2] ?? "";
		if (!appId) throw new Error("App creation requires an appId",);
		if (mode === "successor" && parsed.flags["from"] !== precursorProjectKey) {
			throw new Error("Successor creation requires --from to name the owned precursor instance",);
		}
		const precursor = this.manifest.projects.find(p => p.key === precursorProjectKey);
		if (!precursor || precursor.state !== "bound" || !precursor.incarnation) {
			throw new Error(
				`Generated app creation requires a bound owned precursor: ${precursorProjectKey}`,
			);
		}
		if (precursor.pendingDependentCreation) {
			throw new Error(`Precursor already carries an unresolved dependent creation: ${precursor.key}`,);
		}
		const precursorDetails = asRecord(await this.client.projects.get(precursor.key,),);
		if (projectIncarnationHash(precursor.key, precursorDetails,) !== precursor.incarnation) {
			throw new Error(`Precursor incarnation changed before app creation: ${precursor.key}`,);
		}
		const precursorManifest = asRecord(
			await this.client.applications.getInstanceManifest(precursor.key,),
		);
		const precursorType = asString(precursorManifest?.["projectAppType"],)
			?? asString(precursorDetails?.["projectAppType"],);
		if (mode === "direct" && precursorType !== "APP_TEMPLATE") {
			throw new Error(`Direct app creation requires an APP_TEMPLATE precursor: ${precursor.key}`,);
		}
		if (mode === "successor" && precursorType !== "APP_INSTANCE") {
			throw new Error(`Successor creation requires an APP_INSTANCE precursor: ${precursor.key}`,);
		}
		const manifestAppId = asString(precursorManifest?.["id"],);
		if (manifestAppId !== appId) {
			throw new Error(
				`App id ${appId} does not match the precursor manifest id ${manifestAppId ?? "<absent>"}`,
			);
		}
		// Accurate in-flight marker BEFORE the POST: from here a creation may be
		// racing whose target key only the CLI receipt knows.
		precursor.pendingDependentCreation = { appId, mode, startedAt: new Date().toISOString(), };
		await this.save();
		this.creationAuthority = { kind: "app-generated", appId, mode, };
		let receipt: Record<string, unknown> | undefined;
		try {
			receipt = asRecord(await this.run(command,),);
		} catch (error) {
			if (provenNoCreationOutcome(error instanceof LiveCommandError ? error.result : undefined,)) {
				// Proven no-POST or DSS refused the request itself: nothing was
				// created, so the precursor is clean again.
				delete precursor.pendingDependentCreation;
				await this.save();
			} else if (precursor.pendingDependentCreation) {
				precursor.pendingDependentCreation.outcome = "indeterminate";
				await this.save();
			}
			throw error;
		}
		if (!receipt || receipt["success"] !== true || receipt["futureTargetVerified"] !== true) {
			// Exit 0 without a terminal verified future is still an unknown
			// outcome: retain the marker and record no identity.
			precursor.pendingDependentCreation.outcome = "indeterminate";
			await this.save();
			throw new Error(
				`Generated app creation did not prove a verified terminal target: ${
					this.redact(JSON.stringify(receipt ?? null,),).slice(0, 500,)
				}`,
			);
		}
		const target = generatedCreationTarget(receipt,);
		const requested = asString(receipt["projectKey"],);
		const incarnation = asString(receipt["projectIncarnationHash"],);
		if (
			!target || target !== requested || !incarnation || !/^APP_[0-9A-F]{32}$/.test(target,)
		) {
			// No trustworthy generated identity: never adopt the receipt.
			precursor.pendingDependentCreation.outcome = "indeterminate";
			await this.save();
			throw new Error("Generated app creation receipt names no trustworthy target identity",);
		}
		const details = asRecord(await this.client.projects.get(target,),);
		if (asString(details?.["projectAppType"],) !== "APP_INSTANCE") {
			precursor.pendingDependentCreation.outcome = "indeterminate";
			await this.save();
			throw new Error(`Generated target is not an APP_INSTANCE project: ${target}`,);
		}
		if (projectIncarnationHash(target, details,) !== incarnation) {
			precursor.pendingDependentCreation.outcome = "indeterminate";
			await this.save();
			throw new Error(`Fresh APP_INSTANCE incarnation does not match the receipt: ${target}`,);
		}
		if (this.manifest.projects.some(project => project.key === target)) {
			// A recorded receipt in ANY state already owns this key: the CLI
			// naming it again would alias a bound or deleted identity.
			precursor.pendingDependentCreation.outcome = "indeterminate";
			await this.save();
			throw new Error(`Generated target key collides with a recorded receipt: ${target}`,);
		}
		this.manifest.projects.push({
			key: target,
			state: "pending",
			createdAt: new Date().toISOString(),
			// Receipt-derived origin: the verified precursor anchors this
			// generated key to the run's owned chain before it is persisted.
			generatedFrom: precursor.key,
		},);
		await this.save();
		// Canonical cleanup-ledger binding: bindProject re-reads the project and
		// compares the receipt hash before it changes the bound state or writes
		// the single create entry, so an unconfirmed child is never deletable.
		await this.bindProject(target, incarnation,);
		delete precursor.pendingDependentCreation;
		await this.save();
		return target;
	}
	async bindProject(key: string, expectedHash?: string,): Promise<void> {
		const project = this.manifest.projects.find(p => p.key === key && p.state === "pending");
		if (!project) throw new Error("Project was not reserved",);
		const details = await this.client.projects.get(key,);
		const identity = projectIncarnationHash(key, details,);
		if (!identity) throw new Error(`DSS did not return a verifiable project incarnation for ${key}`,);
		if (expectedHash !== undefined && identity !== expectedHash) {
			// Compare BEFORE the bound state or cleanup authority changes: a
			// mismatch proves the project is not the incarnation the creation
			// receipt bound, so the pending receipt stays unconfirmed.
			throw new Error(`Project incarnation does not match the creation receipt: ${key}`,);
		}
		project.incarnation = identity;
		project.state = "bound";
		await this.save();
		await appendCleanupLedgerEntry(path.join(this.dir, "cleanup.jsonl",), {
			ts: new Date().toISOString(),
			action: "create",
			resource: "project",
			projectKey: key,
			name: key,
			cleanup: {
				argv: [
					"project",
					"delete",
					key,
					"--drop-data",
					"--if-exists",
					"--expect-project-incarnation",
					identity,
				],
			},
		}, this.manifest.dssUrl,);
	}
	async propagateRecipeSchema(name: string,): Promise<void> {
		const owned = this.manifest.projects.find(p => p.key === this.projectKey && p.state === "bound");
		if (
			!owned?.incarnation
			|| projectIncarnationHash(this.projectKey, await this.client.projects.get(this.projectKey,),)
				!== owned.incarnation
		) throw new Error("Recipe schema requires matching project incarnation",);
		const endpoint = `/public/api/projects/${encodeURIComponent(this.projectKey,)}/recipes/${
			encodeURIComponent(name,)
		}`;
		const updates = await this.client.get<
			{ computables: Array<{ type: string; datasetName: string; newSchema: unknown; }>; }
		>(`${endpoint}/schema-update`,);
		for (const item of updates.computables) {
			if (item.type !== "DATASET" || !Object.hasOwn(this.fixtures.datasets, item.datasetName,)) {
				throw new Error("Schema update targets an unowned dataset",);
			}
			await this.client.post(`${endpoint}/actions/updateOutputSchema`, {
				computableType: item.type,
				computableId: item.datasetName,
				newSchema: item.newSchema,
				dropAndRecreate: true,
				synchronizeMetastore: true,
			},);
		}
	}
	async createManagedDataset(name: string,): Promise<void> {
		const owned = this.manifest.projects.find(p => p.key === this.projectKey && p.state === "bound");
		if (!owned?.incarnation) throw new Error("Managed dataset requires a bound project",);
		const details = await this.client.projects.get(this.projectKey,);
		if (projectIncarnationHash(this.projectKey, details,) !== owned.incarnation) {
			throw new Error("Project incarnation changed before dataset creation",);
		}
		await this.client.post(
			`/public/api/projects/${encodeURIComponent(this.projectKey,)}/datasets/managed`,
			{ name, creationSettings: { connectionId: this.connection, specificSettings: {}, }, },
		);
	}
	async createProject(label: string,): Promise<string> {
		const key = await this.reserveProject(label,);
		return this.createReservedProject(key, label,);
	}
	async createProjectForGlobal(label: string,): Promise<string> {
		const key = await this.reserveGlobal("project-deployer-project", label,);
		await this.reserveProjectKey(key,);
		return this.createReservedProject(key, label,);
	}
	private async createReservedProject(key: string, label: string,): Promise<string> {
		await this.run([
			"project",
			"create",
			key,
			`Live suite ${this.manifest.runId} ${label}`,
			"--owner",
			this.owner,
		],);
		await this.bindProject(key,);
		return key;
	}
	async duplicateProject(source: string, label: string,): Promise<string> {
		const key = await this.reserveProject(label,);
		await this.run([
			"project",
			"duplicate",
			source,
			key,
			`Live suite ${this.manifest.runId} ${label}`,
		],);
		await this.bindProject(key,);
		return key;
	}
	async importProject(archive: string, label: string,): Promise<string> {
		const key = await this.reserveProject(label,);
		await this.run(["project", "import", archive, "--target-project-key", key,],);
		await this.bindProject(key,);
		return key;
	}
	async deleteProject(key: string,): Promise<void> {
		if (this.manifest.ownedDirectories.some(d => d.projectKey === key && d.state !== "deleted")) {
			throw new Error("Owned host directory must be cleaned before its runner project",);
		}
		const project = this.manifest.projects.find(p => p.key === key);
		if (!project) throw new Error("Project is not owned by this run",);
		if (project.state === "deleted") return;
		if (project.pendingDependentCreation) {
			// A dependent app creation may exist whose key only the creation
			// receipt knows (or never returns): the precursor stays until the
			// marker is cleared by proof, never by assumption.
			throw new Error(`Unresolved dependent app creation; refusing deletion: ${key}`,);
		}
		let details: unknown;
		try {
			details = await this.client.projects.get(key,);
		} catch (error) {
			if (error instanceof DataikuError && error.status === 404) {
				project.state = "deleted";
				await this.save();
				return;
			}
			throw error;
		}
		if (project.state !== "bound" || !project.incarnation) {
			throw new Error(`Unconfirmed creation; refusing deletion without a bound incarnation: ${key}`,);
		}
		if (projectIncarnationHash(key, details,) !== project.incarnation) {
			throw new Error(`Project incarnation changed; refusing deletion: ${key}`,);
		}
		await this.run([
			"project",
			"delete",
			key,
			"--drop-data",
			"--if-exists",
			"--expect-project-incarnation",
			project.incarnation,
		],);
		project.state = "deleted";
		await this.save();
	}
	get globals(): OwnedGlobal[] {
		return this.manifest.globals;
	}
	get projects(): OwnedProject[] {
		return this.manifest.projects;
	}
	/** Deterministic run-scoped global name; every kind reserves one exact identity. */
	globalName(kind: OwnedGlobalKind, label: string,): string {
		const suffix = label.toUpperCase().replace(/[^A-Z0-9_]/g, "_",).slice(0, 16,);
		const index = this.manifest.globals.length;
		return `SDK_LIVE_${this.manifest.runId.toUpperCase()}_${
			kind.toUpperCase().replaceAll("-", "_",)
		}_${suffix}_${index}`;
	}
	/**
	 * Reserve an exact, already-derived global name (duplicate-check + ledger
	 * push with the reservation nonce). Shared by the standard generator and
	 * the plugin code-env path so reservation semantics can never drift.
	 * `nonce` is the reservation nonce; when a kind embeds it in the name
	 * (deployer kinds, project folders) the SAME nonce is recorded so the
	 * marker and the name can never disagree.
	 */
	private async reserveExactName(
		kind: OwnedGlobalKind,
		name: string,
		nonce = randomUUID().replaceAll("-", "",),
	): Promise<string> {
		if (
			this.manifest.globals.some(g => g.kind === kind && g.name === name && g.state !== "deleted")
		) {
			throw new Error(`Global identity is already reserved: ${kind} ${name}`,);
		}
		const entry: OwnedGlobal = {
			kind,
			name,
			state: "pending",
			nonce,
			createdAt: new Date().toISOString(),
		};
		this.manifest.globals.push(entry,);
		await this.save();
		return name;
	}
	/**
	 * Reserve the code environment DSS generates for a managed plugin
	 * (`plugin_<pluginId>_managed`) before any mutation that would create it.
	 * The parent plugin must be a BOUND owned plugin of this run, and absence
	 * is proven against the authoritative code-env surface for the exact
	 * generated name. Returns the reserved env name for use with
	 * bindGlobal("code-env", name, `PYTHON/${name}`) after the create future
	 * settles.
	 */
	async reservePluginCodeEnv(pluginId: string,): Promise<string> {
		const plugin = this.manifest.globals.find(
			g => g.kind === "plugin" && (g.id ?? g.name) === pluginId,
		);
		if (!plugin || plugin.state !== "bound") {
			throw new Error(
				`Plugin code environment requires a bound owned plugin of this run: ${pluginId}`,
			);
		}
		const name = `plugin_${pluginId}_managed`;
		await this.assertGlobalAbsent("code-env", `${"PYTHON"}/${name}`,);
		return await this.reserveExactName("code-env", name,);
	}
	async reserveGlobal(
		kind: OwnedGlobalKind,
		label: string,
		options: { lang?: "PYTHON" | "R"; } = {},
	): Promise<string> {
		const raw = this.globalName(kind, label,);
		const nonce = randomUUID().replaceAll("-", "",);
		const lang = (options.lang ?? "PYTHON").toLowerCase();
		const name = kind === "code-env"
			? `${lang}_${raw.toLowerCase()}`
			: kind === "meaning"
			? raw.toLowerCase()
			: kind.includes("-deployer-",) || kind === "project-folder"
			? `${raw.slice(0, 31,)}_${nonce.toUpperCase()}`
			: raw;
		await this.assertGlobalAbsent(kind, kind === "code-env" ? `${lang}/${name}` : name,);
		return await this.reserveExactName(kind, name, nonce,);
	}
	/** The lab-controlled marker for a reserved entry, verified on every GET. */
	markerFor(kind: OwnedGlobalKind, name: string,): string {
		const entry = this.manifest.globals.find(g => g.kind === kind && g.name === name);
		if (!entry?.nonce) throw new Error(`Global has no reservation nonce: ${kind} ${name}`,);
		return globalMarker(this.manifest.runId, entry.nonce,);
	}
	/**
	 * Confirm a creation by reading the exact server identity. Pass `id` when DSS
	 * generated one (data collections, project folders); it must come from the
	 * create call, never from a name-prefix search.
	 */
	async bindGlobal(kind: OwnedGlobalKind, name: string, id?: string,): Promise<OwnedGlobal> {
		const entry = this.manifest.globals.find(g => g.kind === kind && g.name === name);
		if (!entry) throw new Error(`Global was not reserved: ${kind} ${name}`,);
		const resolved = id ?? name;
		const marker = this.markerFor(kind, name,);
		const details = await this.fetchGlobal(kind, resolved,);
		const record = asRecord(details,);
		const proven = record !== undefined
			&& globalMarkerVerified(kind, record, marker, entry.name, resolved,);
		if (!proven) {
			entry.state = "unconfirmed";
			entry.reason = "GET did not return the lab marker for the reserved identity";
			await this.save();
			throw new Error(`Global creation not proven by marker: ${kind} ${resolved}`,);
		}
		entry.id = resolved;
		entry.identity = kind === "code-env"
			// Exact server incarnation snapshot: a foreign recreated env under the
			// same lang/name has a different creationTag and fails this comparison.
			? stableStringify(asRecord(asRecord(record,)?.["desc"],)?.["creationTag"],)
			: stableStringify({ kind, id: resolved, nonce: entry.nonce, },);
		entry.state = "bound";
		delete entry.reason;
		await this.save();
		const cleanupArgv = this.deleteArgv(kind, resolved,);
		if (cleanupArgv) {
			await appendCleanupLedgerEntry(path.join(this.dir, "cleanup.jsonl",), {
				ts: new Date().toISOString(),
				action: "create",
				resource: kind,
				id: resolved,
				name: entry.name,
				cleanup: { argv: cleanupArgv, },
			}, this.manifest.dssUrl,);
		}
		return entry;
	}
	/**
	 * Reserve → create → confirm lifecycle. A non-zero exit is either a real
	 * conflict (a pre-existing object owns the identity — recorded, never deleted)
	 * or an unconfirmed failure (never adopted, never deleted blind).
	 */
	async createGlobal(
		kind: OwnedGlobalKind,
		label: string,
		argv: (name: string, marker: string,) => string[],
		options: { lang?: "PYTHON" | "R"; id?: (result: unknown,) => string | undefined; } = {},
	): Promise<OwnedGlobal> {
		const name = await this.reserveGlobal(kind, label, options,);
		const marker = this.markerFor(kind, name,);
		let result: unknown;
		try {
			result = await this.run(argv(name, marker,),);
		} catch (error) {
			const entry = this.manifest.globals.find(g => g.kind === kind && g.name === name)!;
			if (error instanceof LiveCommandError && this.isConflictResult(error,)) {
				entry.state = "conflict";
				entry.reason = this.redact(error,).slice(0, 300,);
			} else {
				entry.state = "unconfirmed";
				entry.reason = this.redact(error,).slice(0, 300,);
			}
			await this.save();
			throw error;
		}
		return await this.bindGlobal(kind, name, options.id?.(result,),);
	}
	/**
	 * Atomic plugin bootstrap: reserve → create-dev EMPTY (receipt required) →
	 * exactly ONE nonce-bearing plugin.json contents-put (the builder output)
	 * → bind via the marker-verified GET. Any other pre-bind write stays
	 * denied; a receipt-id echo mismatch or a failed write never binds.
	 */
	async createPluginDev(
		label: string,
		pluginJson: (name: string, marker: string,) => string,
	): Promise<OwnedGlobal> {
		const name = await this.reserveGlobal("plugin", label,);
		const marker = this.markerFor("plugin", name,);
		const entry = this.manifest.globals.find(g => g.kind === "plugin" && g.name === name)!;
		// Validate the builder BEFORE any request so an invalid builder cannot
		// orphan a created plugin: parseable JSON, matching id, marker embedded
		// exactly inside meta.description (not anywhere in the file).
		const content = pluginJson(name, marker,);
		const parsedBuilder = asRecord(safeParseJson(content,),);
		const metaDescription = asString(
			asRecord(parsedBuilder?.["meta"],)?.["description"],
		);
		if (parsedBuilder === undefined || parsedBuilder["id"] !== name) {
			entry.state = "unconfirmed";
			entry.reason = "plugin.json builder output is not valid JSON with the reserved id";
			await this.save();
			throw new Error(`plugin.json must be valid JSON with id === ${name}`,);
		}
		if (metaDescription !== marker) {
			entry.state = "unconfirmed";
			entry.reason = "plugin.json meta.description must be exactly the lab marker";
			await this.save();
			throw new Error(`plugin.json meta.description must embed the lab marker: ${name}`,);
		}
		let result: unknown;
		try {
			result = await this.run(["plugin", "create-dev", name, "--creation-mode", "EMPTY",],);
		} catch (error) {
			entry.state = "unconfirmed";
			entry.reason = `create-dev failed: ${this.redact(error,).slice(0, 300,)}`;
			await this.save();
			throw error;
		}
		const echoed = asRecord(result,)?.["created"];
		if (echoed !== name) {
			entry.state = "unconfirmed";
			entry.reason = `create-dev receipt id ${JSON.stringify(echoed,)} does not match the reservation`;
			await this.save();
			throw new Error(`Create receipt id does not match the reserved plugin: ${name}`,);
		}
		// Arm the PRIVATE one-shot authority: the next run() of this exact
		// contents-put argv is allowed through despite pending state; any direct
		// ctx.run of the same shape (without this grant) is refused.
		this.creationAuthority = { kind: "plugin-json", name, };
		try {
			await this.run(["plugin", "contents-put", name, "plugin.json", "--content", content,],);
		} catch (error) {
			entry.state = "unconfirmed";
			entry.reason = `plugin.json initialization write failed: ${this.redact(error,).slice(0, 300,)}`;
			await this.save();
			throw error;
		} finally {
			this.creationAuthority = undefined;
		}
		return await this.bindGlobal("plugin", name,);
	}
	/** Install only a locally built archive whose manifest carries this reservation's nonce. */
	async installPluginFromZip(
		label: string,
		buildArchive: (name: string, marker: string,) => Promise<string>,
	): Promise<OwnedGlobal> {
		const name = await this.reserveGlobal("plugin", label,);
		const marker = this.markerFor("plugin", name,);
		const file = await fs.realpath(await buildArchive(name, marker,),);
		const relative = path.relative(await fs.realpath(this.dir,), file,);
		if (
			!relative || relative === ".." || relative.startsWith(`..${path.sep}`,)
			|| path.isAbsolute(relative,)
		) {
			throw new Error("Plugin installation archive must be inside this lab directory",);
		}
		const zip = new StreamZip.async({ file, storeEntries: true, },);
		try {
			const entries = await zip.entries();
			const records = Object.values(entries,);
			const prefix = `${name}/`;
			if (
				records.length !== await zip.entriesCount
				|| records.some(entry =>
					!entry.name.startsWith(prefix,) || ((entry.attr >>> 16) & 0o170000) === 0o120000
				)
			) {
				throw new Error(
					"Plugin archive must contain one owned root without duplicate entries or symlinks",
				);
			}
			const member = entries[`${prefix}plugin.json`];
			if (!member || member.isDirectory || member.size > 65536) {
				throw new Error("Plugin archive must contain a bounded plugin.json manifest",);
			}
			const chunks: Buffer[] = [];
			let size = 0;
			for await (const chunk of await zip.stream(member,)) {
				const bytes = Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,);
				size += bytes.length;
				if (size > 65536) throw new Error("Plugin manifest exceeds the inspection limit",);
				chunks.push(bytes,);
			}
			const manifest = asRecord(safeParseJson(Buffer.concat(chunks, size,).toString("utf8",),),);
			const description = asRecord(manifest?.["meta"],)?.["description"];
			if (
				manifest?.["id"] !== name || typeof description !== "string" || !description.includes(marker,)
			) {
				throw new Error("Plugin archive manifest must match the reserved id and nonce",);
			}
		} finally {
			await zip.close();
		}
		const owned = this.manifest.globals.find(entry =>
			entry.kind === "plugin" && entry.name === name
		)!;
		this.creationAuthority = { kind: "plugin-archive", name, file, };
		try {
			const receipt = asRecord(await this.run(["plugin", "install-from-zip", "--file", file,],),);
			if (receipt?.["installed"] !== true) {
				throw new Error("Plugin installation did not return a success receipt",);
			}
		} catch (error) {
			owned.state = "unconfirmed";
			owned.reason = this.redact(error,);
			await this.save();
			throw error;
		} finally {
			this.creationAuthority = undefined;
		}
		return await this.bindGlobal("plugin", name,);
	}

	async deleteGlobal(kind: OwnedGlobalKind, id: string,): Promise<void> {
		const entry = this.manifest.globals.find(g => g.kind === kind && (g.id ?? g.name) === id);
		if (!entry) throw new Error(`Global is not owned by this run: ${kind} ${id}`,);
		if (entry.state === "deleted") return;
		if (entry.state !== "bound" || !entry.identity) {
			throw new Error(`Unconfirmed global creation; refusing deletion: ${kind} ${id}`,);
		}
		let details: unknown;
		try {
			details = await this.fetchGlobal(kind, id,);
		} catch (error) {
			if (error instanceof DataikuError && error.status === 404) {
				entry.state = "deleted";
				await this.save();
				return;
			}
			throw error;
		}
		const record = asRecord(details,);
		// Code-envs verify the EXACT desc.creationTag snapshot captured at bind
		// time; other kinds re-verify their lab marker. A foreign object under
		// the same id fails either path.
		const matches = kind === "code-env"
			? record !== undefined
				&& stableStringify(
						asRecord(asRecord(record,)?.["desc"],)?.["creationTag"],
					) === entry.identity
			: record !== undefined
				&& globalMarkerVerified(
					kind,
					record,
					entry.nonce ? globalMarker(this.manifest.runId, entry.nonce,) : "",
					entry.name,
					id,
				);
		if (!matches) {
			throw new Error(`Global identity changed; refusing deletion: ${kind} ${id}`,);
		}
		const argv = this.deleteArgv(kind, id,);
		if (argv) await this.run(argv,);
		else {
			const resource = kind === "project-deployer-infra" ? "infras" : "projects";
			await this.client.del(
				`/public/api/project-deployer/${resource}/${encodeURIComponent(id,)}`,
			);
		}
		entry.state = "deleted";
		await this.save();
	}
	/**
	 * Record a future id started by this lab. The id is accepted only when it is
	 * exactly a jobId harvested from a successful execute-mode receipt of a
	 * future-producing command of this run; foreign strings are rejected.
	 */
	async recordFuture(id: string,): Promise<void> {
		if (typeof id !== "string" || !this.futureReceipts.has(id,)) {
			throw new Error(`Future id is not a receipt of a lab-started command: ${id}`,);
		}
		if (!this.manifest.futures.includes(id,)) {
			this.manifest.futures.push(id,);
			await this.save();
		}
	}
	private deleteArgv(kind: OwnedGlobalKind, id: string,): string[] | undefined {
		switch (kind) {
			case "user":
				return ["user", "delete", id, "--if-exists",];
			case "group":
				return ["group", "delete", id, "--if-exists",];
			case "meaning":
				return ["meaning", "delete", id, "--if-exists",];
			case "workspace":
				return ["workspace", "delete", id,];
			case "data-collection":
				return ["data-collection", "delete", id,];
			case "project-folder":
				return ["project-folder", "delete", id, "--if-exists",];
			case "connection":
				return ["connection", "delete", id, "--if-exists",];
			case "code-env": {
				const [lang, name,] = id.split("/",);
				return ["code-env", "delete", lang ?? "", name ?? id, "--if-exists",];
			}
			case "plugin":
				return ["plugin", "delete", id,];
			case "api-deployer-infra":
				return ["api-deployer", "delete-infra", id,];
			case "api-deployer-service":
				return ["api-deployer", "delete-service", id,];
			case "api-deployer-deployment":
				return ["api-deployer", "delete-deployment", id,];
			case "project-deployer-deployment":
				return ["project-deployer", "delete-deployment", id,];
			case "project-deployer-project":
			case "project-deployer-infra":
				return undefined;
		}
	}
	/**
	 * Prove absence of a reserved identity before create. Direct probes return
	 * ambiguous errors on some kinds (400 "does not exist", 403 on permission
	 * reads) — absence is only accepted from exact-name evidence or a real 404;
	 * ambiguous errors fall back to an authoritative per-kind list.
	 */
	private async assertGlobalAbsent(kind: OwnedGlobalKind, name: string,): Promise<void> {
		try {
			await this.fetchGlobal(kind, name,);
		} catch (error) {
			if (error instanceof DataikuError && error.status === 404) return;
			if (this.provesAbsenceByError(kind, error, name,)) return;
			if (await this.provesAbsenceByListing(kind, name, error,)) return;
			throw error;
		}
		throw new Error(`Refusing to overwrite an existing global ${kind}: ${name}`,);
	}
	/** Exact-name error shapes that prove absence without a blanket 400/403. */
	private provesAbsenceByError(kind: OwnedGlobalKind, error: unknown, name: string,): boolean {
		if (!(error instanceof DataikuError)) return false;
		if (error.status !== 400) return false;
		// DSS quotes the id in these messages ("Connection 'X' does not exist",
		// "Unknown meaning: X"); match the exact reserved name conservatively
		// against body AND message.
		const text = `${error.body ?? ""}\n${error.message ?? ""}`;
		const quoted = `'${name}'`;
		if (kind === "meaning" && text.includes(`Unknown meaning: ${name}`,)) return true;
		if (kind === "connection" && text.includes(`Connection ${quoted} does not exist`,)) {
			return true;
		}
		if (kind === "connection" && text.includes(`Connection ${name} does not exist`,)) {
			return true;
		}
		return false;
	}
	/**
	 * Permission-shaped failures (403) prove nothing; only an authoritative
	 * list without the exact identity counts as absence.
	 */
	private async provesAbsenceByListing(
		kind: OwnedGlobalKind,
		name: string,
		error: unknown,
	): Promise<boolean> {
		if (!(error instanceof DataikuError)) return false;
		if (kind === "code-env" && (error.status === 500 || error.status === 403)) {
			// Missing DSS environment definitions can return 500, not 404.
			// The error alone proves nothing; require an authoritative typed list.
			try {
				const [lang, envName,] = name.split("/",);
				const envs = await this.client.codeEnvs.list();
				return !!lang && !!envName
					&& envs.every(env =>
						typeof env.envName === "string" && env.envName.length > 0
						&& typeof env.envLang === "string" && env.envLang.length > 0
					)
					&& !envs.some(env =>
						env.envName === envName && env.envLang?.toLowerCase() === lang.toLowerCase()
					);
			} catch {
				return false;
			}
		}
		if (error.status !== 403) return false;
		try {
			if (kind === "workspace") {
				const list = await this.client.workspaces.list();
				return !list.some(w => w.id === name);
			}
			if (kind === "project-folder") {
				// Reservations are named children of ROOT; the id is server-generated,
				// so absence is proven by the root's children, never by name GETs.
				const root = await this.client.projectFolders.root();
				for (const childId of root.childrenIds ?? []) {
					const child = await this.client.projectFolders.get(childId,);
					if (child.name === name) return false;
				}
				return true;
			}
		} catch {
			return false;
		}
		return false;
	}
	private async fetchGlobal(kind: OwnedGlobalKind, id: string,): Promise<unknown> {
		switch (kind) {
			case "user":
				return this.client.users.get(id,);
			case "group":
				return this.client.groups.get(id,);
			case "meaning":
				return this.client.meanings.get(id,);
			case "workspace":
				return this.client.workspaces.get(id,);
			case "data-collection":
				return this.client.dataCollections.get(id,);
			case "project-folder":
				return this.client.projectFolders.get(id,);
			case "connection":
				return this.client.connections.adminGet(id,);
			case "code-env": {
				const [lang, name,] = id.split("/",);
				// getDefinition returns the raw body and preserves desc.creationTag,
				// which the normalized get() drops; the incarnation check needs it.
				return this.client.codeEnvs.getDefinition(lang ?? "", name ?? id,);
			}
			case "plugin": {
				const found = (await this.client.plugins.list()).find(p => p.id === id);
				if (!found) throw new DataikuError(404, "Not Found", `plugin not found: ${id}`,);
				if (!found.isDev) return found;
				// DSS caches loaded metadata; a dev plugin's current nonce lives in its file.
				const manifest = asRecord(
					safeParseJson(await this.client.plugins.getFile(id, "plugin.json",),),
				);
				return { ...found, id: manifest?.["id"], meta: manifest?.["meta"], };
			}
			case "api-deployer-infra":
				return this.client.apiDeployer.getInfra(id,);
			case "api-deployer-service":
				return this.client.apiDeployer.getService(id,);
			case "api-deployer-deployment":
				return this.client.apiDeployer.getDeployment(id,);
			case "project-deployer-project":
				return this.client.projectDeployer.getProjectStatus(id,);
			case "project-deployer-deployment":
				return this.client.projectDeployer.getDeployment(id,);
			case "project-deployer-infra":
				return this.client.get(`/public/api/project-deployer/infras/${encodeURIComponent(id,)}`,);
		}
	}
	private isConflictResult(error: LiveCommandError,): boolean {
		const payload = error.result as { status?: number; errorType?: string; message?: string; } | null;
		if (payload && typeof payload === "object") {
			if (payload.status === 409 || payload.errorType === "Conflict") return true;
			if (typeof payload.message === "string" && /already exists/i.test(payload.message,)) return true;
		}
		return /already exists/i.test(error.message,);
	}
	async cleanup(): Promise<void> {
		const errors: string[] = [];
		const note = (entry: OwnedGlobal, reason: string,) => {
			errors.push(`${entry.kind} ${entry.id ?? entry.name}: ${reason}`,);
		};
		for (let index = this.manifest.globals.length - 1; index >= 0; index--) {
			const entry = this.manifest.globals[index]!;
			try {
				if (entry.state === "conflict") {
					note(entry, "left in place: identity conflicts with a pre-existing resource",);
				} else if (entry.state === "unconfirmed") {
					note(entry, "left in place: creation was never confirmed",);
				} else if (entry.state === "pending") {
					note(entry, "left in place: reservation was never used",);
				} else {
					await this.deleteGlobal(entry.kind, entry.id ?? entry.name,);
				}
			} catch (error) {
				errors.push(this.redact(error,),);
			}
		}
		for (let index = this.manifest.ownedDirectories.length - 1; index >= 0; index--) {
			const directory = this.manifest.ownedDirectories[index]!;
			try {
				await this.cleanupHostDirectory(directory,);
			} catch (error) {
				errors.push(this.redact(error,),);
			}
		}
		for (let index = this.manifest.projects.length - 1; index >= 0; index--) {
			const project = this.manifest.projects[index]!;
			try {
				await this.deleteProject(project.key,);
			} catch (error) {
				errors.push(this.redact(error,),);
			}
		}
		this.manifest.cleanup = { status: errors.length ? "failed" : "passed", errors, };
		await this.save();
		if (errors.length) throw new Error(`Live cleanup failed: ${errors.join("; ",)}`,);
	}
}

export async function initializeLiveManifest(
	file: string,
	credentials: LiveCredentials,
	owner: string,
	connection: string,
	profiles: string[],
): Promise<LiveManifest> {
	if (
		!profiles.length
		|| profiles.some(profile => !LIVE_PROFILES.includes(profile as typeof LIVE_PROFILES[number],))
	) throw new Error("Unknown or empty live profiles",);
	if (!owner.trim() || !connection.trim()) {
		throw new Error("Live owner and managed connection are required",);
	}
	await fs.mkdir(path.dirname(file,), { recursive: true, },);
	const manifest: LiveManifest = {
		version: 1,
		fixtureVersion: 1,
		runId: randomUUID().replaceAll("-", "",).slice(0, 16,),
		dssUrl: canonicalDssUrl(credentials.url,),
		projectKey: "",
		owner,
		connection,
		profiles,
		fixtures: { datasets: {}, expectedRows: {}, recipes: {}, },
		projects: [],
		globals: [],
		ownedDirectories: [],
		futures: [],
		cases: [],
		commands: [],
		iteration: 0,
		startedAt: new Date().toISOString(),
		beforeProjects: {},
		capabilities: {},
		cleanup: { status: "pending", errors: [], },
	};
	await reserveCleanupLedgerDssUrl(
		path.join(path.dirname(file,), "cleanup.jsonl",),
		manifest.dssUrl,
	);
	await writeLiveJson(file, manifest,);
	return manifest;
}
