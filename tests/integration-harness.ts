import { describe, } from "bun:test";
import { execFile, } from "node:child_process";
import { randomUUID, } from "node:crypto";
import * as nodeFs from "node:fs";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";
import { DataikuClient, } from "../src/client.js";
import { resolveAdminManagedStorageConnection, } from "../src/resources/connections.js";
import type { ConnectionSummary, } from "../src/schemas.js";
import { type LiveManifest, loadLiveManifest, } from "./live-context.js";

const exec = promisify(execFile,);

export const SDK_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
export const CLI_PATH = path.join(SDK_ROOT, "src/cli.ts",);
export const BUN = process.execPath;

export interface DssRunOptions {
	cwd?: string;
	env?: typeof process.env;
}

export interface DssOutput {
	stdout: string;
	stderr: string;
}

export interface DssRawResult extends DssOutput {
	code: number;
}

export type CleanupFn = () => Promise<void> | void;

export interface CleanupStack {
	push(cleanup: CleanupFn,): void;
	defer(cleanup: CleanupFn,): void;
	run(): Promise<void>;
}

export function loadDotEnv(): void {
	const envPath = path.join(SDK_ROOT, ".env",);
	if (!nodeFs.existsSync(envPath,)) return;
	const content = nodeFs.readFileSync(envPath, "utf-8",);
	for (const line of content.split("\n",)) {
		const trimmed = line.trim();
		if (!trimmed || trimmed.startsWith("#",)) continue;
		const eq = trimmed.indexOf("=",);
		if (eq === -1) continue;
		const key = trimmed.slice(0, eq,).trim();
		const value = trimmed.slice(eq + 1,).trim().replace(/^['"]|['"]$/g, "",);
		if (!process.env[key]) process.env[key] = value;
	}
}

export function parseTlsRejectUnauthorized(value: string | undefined,): boolean | undefined {
	if (value === undefined) return undefined;
	const normalized = value.trim().toLowerCase();
	if (normalized === "0" || normalized === "false" || normalized === "no") return false;
	if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
	return undefined;
}

loadDotEnv();

export const integrationEnabled = process.env.RUN_DATAIKU_INTEGRATION === "1";
export const hasCredentials = Boolean(process.env.DATAIKU_URL && process.env.DATAIKU_API_KEY,);
export const hasProjectKey = Boolean(process.env.DATAIKU_PROJECT_KEY,);
export const describeIntegration = integrationEnabled && hasCredentials ? describe : describe.skip;
export const describeProjectIntegration = integrationEnabled && hasCredentials && hasProjectKey
	? describe
	: describe.skip;
export const liveManifestPath = process.env.DATAIKU_LIVE_MANIFEST?.trim() || undefined;
let liveManifestCache: LiveManifest | undefined;
/** Loaded lazily: the manifest file may be written moments before the child starts. */
export async function getLiveManifest(): Promise<LiveManifest | undefined> {
	if (!liveManifestPath) return undefined;
	liveManifestCache ??= await loadLiveManifest(liveManifestPath,);
	return liveManifestCache;
}
/** True when the suite runs under a parent-provisioned self-provisioned live manifest. */
export const liveMode = liveManifestPath !== undefined;

export const mutatingProjectIntegrationEnabled = integrationEnabled
	&& process.env.RUN_DATAIKU_INTEGRATION_MUTATING === "1"
	&& hasCredentials
	&& hasProjectKey
	&& !liveMode;
export const describeMutatingProjectIntegration = mutatingProjectIntegrationEnabled
	? describe
	: describe.skip;
export const adminMutatingIntegrationEnabled = integrationEnabled
	&& process.env.RUN_DATAIKU_ADMIN_MUTATING === "1"
	&& hasCredentials
	&& !liveMode;
export const describeAdminMutatingIntegration = adminMutatingIntegrationEnabled
	? describe
	: describe.skip;

export const sqlLiveIntegrationEnabled = integrationEnabled
	&& process.env.RUN_DATAIKU_SQL_LIVE === "1"
	&& hasCredentials
	&& !liveMode
	&& Boolean(process.env.DATAIKU_SQL_CONNECTION || process.env.DATAIKU_SQL_DATASET_FULL_NAME,);
export const describeSqlLiveIntegration = sqlLiveIntegrationEnabled
	? describe
	: describe.skip;

/**
 * Resolve the writable storage connection for disposable fixtures without
 * assuming a specific instance configuration: explicit test env first, then the
 * live-suite manifest connection, then an admin-visible managed-storage
 * connection, then connection inference against the project.
 */
export async function resolveStorageConnection(client: DataikuClient,): Promise<string> {
	const explicit = process.env.DATAIKU_TEST_CONNECTION?.trim()
		?? process.env.DATAIKU_TEST_STORAGE_CONNECTION?.trim()
		?? (await getLiveManifest())?.connection;
	if (explicit) return explicit;
	const admin = await resolveAdminManagedStorageConnection(client, "allowManagedDatasets",);
	if (admin) return admin;
	const inferred = await client.connections.infer({ projectKey: process.env.DATAIKU_PROJECT_KEY, },)
		.catch(() => [] as ConnectionSummary[]);
	const managed = inferred.find((connection,) => connection.managed === true);
	return managed?.name ?? inferred[0]?.name ?? "filesystem_managed";
}

/**
 * Pick a safe read-only dataset fixture. Under live mode the manifest-provided
 * fixture name wins so runs stay inside the provisioned project without broad
 * cross-project discovery; otherwise fall back to `dss fixtures` discovery.
 */
export async function selectSafeDataset(): Promise<string | undefined> {
	const manifest = await getLiveManifest();
	const manifestDatasets = Object.values(manifest?.fixtures.datasets ?? {},)
		.filter((name,): name is string => typeof name === "string" && name.length > 0);
	if (manifestDatasets.length > 0) return manifestDatasets[0];
	const projectKey = detectProjectKey(process.env, manifest?.projectKey,);
	if (!projectKey) return undefined;
	const fixturesResult = await dss(["fixtures", "--project-key", projectKey,],)
		.catch(() => undefined);
	if (!fixturesResult) return undefined;
	try {
		const parsed = parseJsonOutput<{ safeDataset?: { name?: unknown; } | null; }>(
			fixturesResult.stdout,
		);
		return fixtureString(parsed.safeDataset, ["name",],);
	} catch {
		return undefined;
	}
}

function fixtureString(value: unknown, fields: string[],): string | undefined {
	if (value === null || typeof value !== "object" || Array.isArray(value,)) return undefined;
	const record = value as Record<string, unknown>;
	for (const field of fields) {
		const candidate = record[field];
		if (typeof candidate === "string" && candidate.length > 0) return candidate;
	}
	return undefined;
}

/**
 * Project key for CLI command injection: child env is authoritative; the
 * caller supplies the manifest project key when env is unset.
 */
export function detectProjectKey(
	env: NodeJS.ProcessEnv,
	manifestProjectKey: string | undefined,
): string {
	return env.DATAIKU_PROJECT_KEY?.trim() || manifestProjectKey || "";
}

function dssExecOptions(
	opts: DssRunOptions,
): { cwd: string; env: typeof process.env; maxBuffer: number; } {
	return {
		cwd: opts.cwd ?? SDK_ROOT,
		env: opts.env ?? process.env,
		maxBuffer: 16 * 1024 * 1024,
	};
}

function exitCodeFromError(error: (Error & { code?: number | string | null; }) | null,): number {
	if (!error) return 0;
	return typeof error.code === "number" ? error.code : 1;
}

export function createClient(): DataikuClient {
	return new DataikuClient({
		url: process.env.DATAIKU_URL!,
		apiKey: process.env.DATAIKU_API_KEY!,
		projectKey: process.env.DATAIKU_PROJECT_KEY,
		caCertPath: process.env.NODE_EXTRA_CA_CERTS,
		tlsRejectUnauthorized: parseTlsRejectUnauthorized(process.env.NODE_TLS_REJECT_UNAUTHORIZED,),
	},);
}

export async function dssRaw(args: string[], opts: DssRunOptions = {},): Promise<DssRawResult> {
	return await new Promise<DssRawResult>((resolveRun,) => {
		try {
			execFile(
				BUN,
				["--no-env-file", "run", CLI_PATH, ...args,],
				dssExecOptions(opts,),
				(error, stdout, stderr,) => {
					resolveRun({
						code: exitCodeFromError(error,),
						stdout: String(stdout,),
						stderr: String(stderr,),
					},);
				},
			);
		} catch (error) {
			resolveRun({
				code: 1,
				stdout: "",
				stderr: error instanceof Error ? error.message : String(error,),
			},);
		}
	},);
}

export async function dss(args: string[], opts: DssRunOptions = {},): Promise<DssOutput> {
	const { stdout, stderr, } = await exec(
		BUN,
		["--no-env-file", "run", CLI_PATH, ...args,],
		dssExecOptions(opts,),
	);
	return { stdout: String(stdout,), stderr: String(stderr,), };
}

export function parseJsonOutput<T = unknown,>(stdout: string,): T {
	return JSON.parse(stdout,) as T;
}

export function uniqueTestName(prefix: string,): string {
	return `${prefix}_${Date.now()}_${randomUUID().replace(/-/g, "",).slice(0, 8,)}`;
}

export function createCleanupStack(): CleanupStack {
	const cleanups: CleanupFn[] = [];
	const push = (cleanup: CleanupFn,): void => {
		cleanups.push(cleanup,);
	};

	return {
		push,
		defer: push,
		async run(): Promise<void> {
			const errors: unknown[] = [];
			while (cleanups.length > 0) {
				const cleanup = cleanups.pop()!;
				try {
					await cleanup();
				} catch (error) {
					errors.push(error,);
				}
			}

			if (errors.length === 1) throw errors[0];
			if (errors.length > 1) throw new AggregateError(errors, "Cleanup stack failed",);
		},
	};
}
