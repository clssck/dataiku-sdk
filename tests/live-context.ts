import { randomUUID, } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";
import { jsonInput, } from "../src/cli/coerce.js";
import { buildCommandRegistry, } from "../src/cli/contract.js";
import { executionMode, parseArgs, } from "../src/cli/flags.js";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import {
	appendCleanupLedgerEntry,
	reserveCleanupLedgerDssUrl,
} from "../src/utils/cleanup-ledger.js";
import { canonicalDssUrl, } from "../src/utils/dss-url.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";

import { type LiveCaseId, matchesLiveCase, } from "./live-cases.js";

export const LIVE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
const commandRegistry = buildCommandRegistry();
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
export interface OwnedProject {
	key: string;
	state: "pending" | "bound" | "deleted";
	incarnation?: string;
	createdAt: string;
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
	cases: CaseResult[];
	commands: LiveCommand[];
	iteration: number;
	setupComplete?: boolean;
	setupCases?: CaseResult[];
	startedAt: string;
	completedAt?: string;
	beforeProjects: Record<string, string>;
	capabilities: Record<string, unknown>;
	cleanup: { status: "pending" | "kept" | "passed" | "failed"; errors: string[]; };
}
export interface LiveContext {
	projectKey: string;
	connection: string;
	owner: string;
	dir: string;
	profiles: string[];
	phase: "setup" | "run";
	selection: string[];
	iteration: number;
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
	writeFile(name: string, content: string | Uint8Array,): Promise<string>;
	save(): Promise<void>;
	createProject(label: string,): Promise<string>;
	duplicateProject(source: string, label: string,): Promise<string>;
	importProject(archive: string, label: string,): Promise<string>;
	deleteProject(key: string,): Promise<void>;
	reserveProject(label: string,): Promise<string>;
	bindProject(key: string,): Promise<void>;
	createManagedDataset(name: string,): Promise<void>;
	propagateRecipeSchema(name: string,): Promise<void>;
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
	const seen = new Set<string>();
	for (const project of data.projects) {
		if (
			!project.key.startsWith(`SDK_LIVE_${data.runId.toUpperCase()}_`,) || seen.has(project.key,)
			|| Object.hasOwn(data.beforeProjects, project.key,)
			|| !["pending", "bound", "deleted",].includes(project.state,)
			|| (project.state === "bound" && !/^[a-f0-9]{64}$/.test(project.incarnation ?? "",))
		) throw new Error("Invalid owned-project identity",);
		seen.add(project.key,);
	}
	return data;
}

/** Scope checks also run in the test child; credentials and server/project flags cannot be replaced by a case. */
export function assertLiveCommandScope(
	argv: string[],
	manifest: LiveManifest,
	selectedProject: string,
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
		if (flag.endsWith("project-key",) && !owned(value, flag === "target-project-key",)) {
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
		const target = action === "create-successor-instance"
			? flags.to
			: jsonInput(flags,)?.targetProjectKey;
		if (!owned(target, true,) || manifest.projects.find(p => p.key === target)?.state !== "pending") {
			throw new Error("App creation requires a reserved project target",);
		}
		return;
	}
	if (executionMode(flags,).plan) return;
	if (resource === "project-git" && entry.mutatesDss) {
		throw new Error("Git mutations require an isolated remote harness",);
	}
	if (entry.mutatesDss && !entry.requiresProject) {
		throw new Error(
			`Global mutation is not authorized by a project sandbox: ${resource}.${action}`,
		);
	}
}

export class LiveRunContext implements LiveContext {
	readonly dir: string;
	readonly client: DataikuClient;
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
		return this.credentials.apiKey ? text.replaceAll(this.credentials.apiKey, "[redacted]",) : text;
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
		assertLiveCommandScope(args, this.manifest, selected,);
		if (entry?.mutatesDss && entry.requiresProject && !executionMode(parsed.flags,).plan) {
			const identity = this.manifest.projects.find(p => p.key === selected);
			const details = await this.client.projects.get(selected,);
			if (
				!identity?.incarnation || projectIncarnationHash(selected, details,) !== identity.incarnation
			) throw new Error(`Project incarnation changed before mutation: ${selected}`,);
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
			args: args.map(s => this.redact(s,)),
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
			throw new LiveCommandError(
				`${resource}.${action} exited ${exit}: ${
					this.redact(JSON.stringify(result,),).slice(0, 2500,)
				}`,
				exit,
				result,
			);
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
	async reserveProject(label: string,): Promise<string> {
		const suffix = label.toUpperCase().replace(/[^A-Z0-9_]/g, "_",).slice(0, 16,);
		const key =
			`SDK_LIVE_${this.manifest.runId.toUpperCase()}_${suffix}_${this.manifest.projects.length}`;
		// DSS can return 403 for nonexistent keys; creation is exclusive and never updates a collision.
		const visible = await this.client.projects.list();
		if (visible.some(project => project.projectKey === key)) {
			throw new Error(`Refusing to overwrite an existing project: ${key}`,);
		}
		this.manifest.projects.push({ key, state: "pending", createdAt: new Date().toISOString(), },);
		await this.save();
		return key;
	}
	async bindProject(key: string,): Promise<void> {
		const project = this.manifest.projects.find(p => p.key === key && p.state === "pending");
		if (!project) throw new Error("Project was not reserved",);
		const details = await this.client.projects.get(key,);
		const identity = projectIncarnationHash(key, details,);
		if (!identity) throw new Error(`DSS did not return a verifiable project incarnation for ${key}`,);
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
		const project = this.manifest.projects.find(p => p.key === key);
		if (!project) throw new Error("Project is not owned by this run",);
		if (project.state === "deleted") return;
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
	async cleanup(): Promise<void> {
		const errors: string[] = [];
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
