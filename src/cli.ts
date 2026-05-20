#!/usr/bin/env node

import { createHash, } from "node:crypto";
import { readFileSync, } from "node:fs";
import { mkdir, writeFile, } from "node:fs/promises";
import { dirname, join, resolve, } from "node:path";
import { createInterface, } from "node:readline";
import { Writable, } from "node:stream";
import { fileURLToPath, } from "node:url";
import { validateCredentials, } from "./auth.js";
import { DataikuClient, } from "./client.js";
import {
	deleteCredentials,
	type DssCredentials,
	getCredentialsPath,
	loadCredentials,
	maskApiKey,
	saveCredentials,
} from "./config.js";
import { DataikuError, dataikuErrorCode, type StableErrorCode, } from "./errors.js";
import { buildDatasetCloneSettings, } from "./resources/datasets.js";
import type { FlowZoneItemInput, } from "./resources/flow-zones.js";
import {
	type JobBuildTargetType,
	type JobLogFilter,
	parseJobLogProgress,
} from "./resources/jobs.js";
import { scenarioUpdatePreview, } from "./resources/scenarios.js";
import type {
	BuildMode,
	DatasetDetails,
	FlowZone,
	FlowZoneObjectType,
	JobSummary,
} from "./schemas.js";
import { AGENTS, detectAgents, findWorkspaceRoot, installSkill, } from "./skill.js";
import {
	appendCleanupLedgerEntry,
	type CleanupLedgerEntry,
	readCleanupLedger,
} from "./utils/cleanup-ledger.js";
import { deepMerge, } from "./utils/deep-merge.js";
import { sanitizeFileName, } from "./utils/sanitize.js";

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function findPackageRoot(): string | undefined {
	let dir = dirname(fileURLToPath(import.meta.url,),);
	for (let i = 0; i < 5; i++) {
		try {
			readFileSync(resolve(dir, "package.json",), "utf-8",);
			return dir;
		} catch {
			dir = dirname(dir,);
		}
	}
	return undefined;
}

function packageVersion(packageRoot: string | undefined,): string {
	if (!packageRoot) return "unknown";
	try {
		return (JSON.parse(readFileSync(resolve(packageRoot, "package.json",), "utf-8",),) as {
			version: string;
		}).version;
	} catch {
		return "unknown";
	}
}

function gitDirectory(packageRoot: string,): string {
	try {
		const gitFile = readFileSync(resolve(packageRoot, ".git",), "utf-8",).trim();
		if (gitFile.startsWith("gitdir:",)) {
			return resolve(packageRoot, gitFile.slice("gitdir:".length,).trim(),);
		}
	} catch {
		// Normal checkouts have a .git directory, not a .git file.
	}
	return resolve(packageRoot, ".git",);
}

function gitRevision(packageRoot: string | undefined,): string | undefined {
	if (!packageRoot) return undefined;
	try {
		const gitDir = gitDirectory(packageRoot,);
		const head = readFileSync(resolve(gitDir, "HEAD",), "utf-8",).trim();
		if (!head.startsWith("ref:",)) return head.slice(0, 7,);
		const ref = head.slice("ref:".length,).trim();
		const full = readFileSync(resolve(gitDir, ref,), "utf-8",).trim();
		return full.slice(0, 7,);
	} catch {
		return undefined;
	}
}

const PACKAGE_ROOT = findPackageRoot();
const CLI_VERSION = packageVersion(PACKAGE_ROOT,);
const CLI_VERSION_LABEL = (() => {
	const revision = gitRevision(PACKAGE_ROOT,);
	return revision ? `${CLI_VERSION}+g${revision}` : CLI_VERSION;
})();
function num(v: string | boolean | undefined,): number | undefined {
	if (typeof v !== "string") return undefined;
	const n = Number(v,);
	return Number.isFinite(n,) ? n : undefined;
}

function jobBuildTargetType(v: string | boolean | undefined,): JobBuildTargetType {
	if (v === undefined) return "DATASET";
	if (typeof v !== "string") {
		throw new UsageError(
			"Invalid job target type. Use dataset or managed-folder.",
			"invalid_enum",
		);
	}
	const normalized = v.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "DATASET" || normalized === "MANAGED_FOLDER") return normalized;
	throw new UsageError(
		"Invalid job target type. Use dataset or managed-folder.",
		"invalid_enum",
	);
}

function jobBuildTargetTypeFromFlags(flags: Record<string, string | boolean>,): JobBuildTargetType {
	return jobBuildTargetType(flags["target-type"] ?? flags["type"],);
}

function maxLogLinesFromFlags(flags: Record<string, string | boolean>,): number | undefined {
	return num(flags["max-log-lines"] ?? flags["max-lines"],);
}

function jobLogFilterFromFlag(v: string | boolean | undefined,): JobLogFilter | undefined {
	if (v === undefined) return undefined;
	if (typeof v !== "string") {
		throw new UsageError(
			"Invalid --log-filter value. Use stdout, stderr, user, or errors.",
			"invalid_enum",
		);
	}
	const normalized = v.trim().toLowerCase();
	if (
		normalized === "stdout" || normalized === "stderr" || normalized === "user"
		|| normalized === "errors"
	) {
		return normalized;
	}
	throw new UsageError(
		"Invalid --log-filter value. Use stdout, stderr, user, or errors.",
		"invalid_enum",
	);
}

function recipeBackupPath(recipeName: string, backupDir: string,): string {
	const stamp = new Date().toISOString().replace(/[:.]/g, "-",);
	return join(backupDir, `${sanitizeFileName(recipeName, "recipe",)}-${stamp}.recipe-backup.json`,);
}

function sha256Hex(value: string,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

function normalizeLineEndings(value: string,): string {
	return value.replace(/\r\n/g, "\n",);
}

function stableJson(value: unknown,): string {
	if (value === undefined) return "undefined";
	if (value === null || typeof value !== "object") return JSON.stringify(value,);
	if (Array.isArray(value,)) return `[${value.map((item,) => stableJson(item,)).join(",",)}]`;
	const entries = Object.entries(value as Record<string, unknown>,).sort(([a,], [b,],) =>
		a.localeCompare(b,)
	);
	return `{${
		entries.map(([key, item,],) => `${JSON.stringify(key,)}:${stableJson(item,)}`).join(",",)
	}}`;
}

function stableHash(value: unknown,): string {
	return sha256Hex(stableJson(value,),);
}

function recipeCodeEnv(recipe: Record<string, unknown>,): unknown {
	const params = recipe.params;
	if (!params || typeof params !== "object" || Array.isArray(params,)) return undefined;
	return (params as Record<string, unknown>).envSelection;
}

function recipeGraph(recipe: Record<string, unknown>,): Record<string, unknown> {
	return {
		inputs: recipe.inputs,
		outputs: recipe.outputs,
	};
}

function recipeBackupDocument(
	recipeName: string,
	projectKey: string | undefined,
	current: { recipe: Record<string, unknown>; payload?: string; },
): Record<string, unknown> {
	return {
		resource: "recipe",
		recipeName,
		projectKey,
		createdAt: new Date().toISOString(),
		versionTag: current.recipe.versionTag,
		payloadHash: sha256Hex(current.payload ?? "",),
		graphHash: stableHash(recipeGraph(current.recipe,),),
		normalizedPayloadHash: sha256Hex(normalizeLineEndings(current.payload ?? "",),),
		codeEnvHash: stableHash(recipeCodeEnv(current.recipe,),),
		codeEnv: recipeCodeEnv(current.recipe,),
		recipe: current.recipe,
		payload: current.payload ?? "",
	};
}

function readRecipeBackup(backupPath: string,): Record<string, unknown> {
	const raw = readFileSync(backupPath, "utf-8",);
	try {
		const parsed = JSON.parse(raw,) as Record<string, unknown>;
		if (parsed && typeof parsed === "object" && parsed.resource === "recipe") return parsed;
	} catch {
		// Backward-compatible payload-only backups are handled below.
	}
	return {
		resource: "recipe",
		recipeName: "unknown",
		payloadHash: sha256Hex(raw,),
		payload: raw,
	};
}
function recipeRunShouldWait(flags: Record<string, string | boolean>,): boolean {
	if (flags["wait"] === true && flags["no-wait"] === true) {
		throw new UsageError("--wait and --no-wait are mutually exclusive.", "invalid_enum",);
	}
	const waitImplied = flags["include-logs"] === true
		|| flags["summary"] === true
		|| flags["timeout"] !== undefined
		|| flags["poll-interval"] !== undefined;
	if (flags["no-wait"] === true && waitImplied) {
		throw new UsageError(
			"--include-logs, --summary, --timeout, and --poll-interval require waiting; remove --no-wait.",
			"invalid_enum",
		);
	}
	return flags["no-wait"] !== true && (flags["wait"] === true || waitImplied);
}

function splitCsvFlag(v: string | boolean | undefined,): string[] {
	if (typeof v !== "string") return [];
	return v.split(",",).map((item,) => item.trim()).filter((item,) => item.length > 0);
}

function recipeInputDatasetsFromFlags(
	flags: Record<string, string | boolean>,
): string[] | undefined {
	const inputs = splitCsvFlag(flags["input"],);
	return inputs.length > 0 ? inputs : undefined;
}

function rewritePairsFromFlags(
	flags: Record<string, string | boolean>,
	flagName: string,
): Record<string, string> {
	const rewrites: Record<string, string> = {};
	for (const spec of splitCsvFlag(flags[flagName],)) {
		const idx = spec.indexOf("=",);
		if (idx <= 0 || idx === spec.length - 1) {
			throw new UsageError(`--${flagName} values must use FROM=TO.`, "invalid_enum",);
		}
		const from = spec.slice(0, idx,).trim();
		const to = spec.slice(idx + 1,).trim();
		if (!from || !to) throw new UsageError(`--${flagName} values must use FROM=TO.`, "invalid_enum",);
		rewrites[from] = to;
	}
	return rewrites;
}

function plainRecord(value: unknown,): Record<string, unknown> | undefined {
	return value && typeof value === "object" && !Array.isArray(value,)
		? value as Record<string, unknown>
		: undefined;
}

function datasetSourceSummary(details: DatasetDetails,): Record<string, unknown> {
	const params = details.params ?? {};
	return {
		resource: "dataset",
		name: details.name,
		projectKey: details.projectKey,
		type: details.type,
		managed: details.managed,
		connection: params.connection,
		catalog: params.catalog,
		schema: params.schema,
		table: params.table,
		path: params.path,
		folderSmartId: params.folderSmartId,
		formatType: details.formatType,
	};
}

function requiredStringFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string {
	const value = flags[name];
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`--${name} is required. Usage: ${usage}`, "missing_required_flag",);
	}
	return value.trim();
}

function flowZoneId(value: string,): string {
	const trimmed = value.trim();
	if (!trimmed) throw new UsageError("Flow zone id must not be empty.",);
	return trimmed;
}

function flowZoneName(value: string | boolean | undefined,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			"--name is required. Usage: dss flow-zone create --name NAME [--color #RRGGBB]",
		);
	}
	return value.trim();
}

function flowZoneColor(value: string | boolean | undefined,): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !/^#[0-9a-fA-F]{6}$/.test(value.trim(),)) {
		throw new UsageError("--color must be a hex color like #2ab1ac.",);
	}
	return value.trim();
}

function flowZoneObjectType(value: string,): FlowZoneObjectType {
	const normalized = value.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "FOLDER") return "MANAGED_FOLDER";
	if (normalized === "MODEL_EVAL_STORE") return "MODEL_EVALUATION_STORE";
	if (normalized === "KNOWLEDGE_BANK") return "RETRIEVABLE_KNOWLEDGE";
	if (
		normalized === "DATASET"
		|| normalized === "MANAGED_FOLDER"
		|| normalized === "SAVED_MODEL"
		|| normalized === "RECIPE"
		|| normalized === "MODEL_EVALUATION_STORE"
		|| normalized === "STREAMING_ENDPOINT"
		|| normalized === "LABELING_TASK"
		|| normalized === "RETRIEVABLE_KNOWLEDGE"
	) {
		return normalized;
	}
	throw new UsageError(
		`Invalid flow zone object type: ${value}. Use DATASET, RECIPE, MANAGED_FOLDER, SAVED_MODEL, MODEL_EVALUATION_STORE, STREAMING_ENDPOINT, LABELING_TASK, or RETRIEVABLE_KNOWLEDGE.`,
		"invalid_enum",
	);
}

function addFlowZoneFlagItems(
	items: FlowZoneItemInput[],
	flags: Record<string, string | boolean>,
	flagName: string,
	objectType: FlowZoneObjectType,
): void {
	for (const objectId of splitCsvFlag(flags[flagName],)) {
		items.push({ objectId, objectType, },);
	}
}

function parseFlowZoneObject(value: string,): FlowZoneItemInput {
	const parts = value.split(":",).map((part,) => part.trim()).filter((part,) => part.length > 0);
	if (parts.length === 2) {
		return { objectType: flowZoneObjectType(parts[0],), objectId: parts[1], };
	}
	if (parts.length === 3) {
		return { projectKey: parts[0], objectType: flowZoneObjectType(parts[1],), objectId: parts[2], };
	}
	throw new UsageError(
		`Invalid --object value: ${value}. Use TYPE:ID or PROJECT_KEY:TYPE:ID.`,
	);
}

function flowZoneMoveItems(flags: Record<string, string | boolean>,): FlowZoneItemInput[] {
	const items: FlowZoneItemInput[] = [];
	addFlowZoneFlagItems(items, flags, "dataset", "DATASET",);
	addFlowZoneFlagItems(items, flags, "recipe", "RECIPE",);
	addFlowZoneFlagItems(items, flags, "folder", "MANAGED_FOLDER",);
	addFlowZoneFlagItems(items, flags, "saved-model", "SAVED_MODEL",);
	addFlowZoneFlagItems(items, flags, "model-evaluation-store", "MODEL_EVALUATION_STORE",);
	addFlowZoneFlagItems(items, flags, "streaming-endpoint", "STREAMING_ENDPOINT",);
	addFlowZoneFlagItems(items, flags, "labeling-task", "LABELING_TASK",);
	addFlowZoneFlagItems(items, flags, "knowledge-bank", "RETRIEVABLE_KNOWLEDGE",);
	for (const value of splitCsvFlag(flags["object"],)) {
		items.push(parseFlowZoneObject(value,),);
	}
	return items;
}

function flowZoneItems(zone: FlowZone,): FlowZoneItemInput[] {
	return [...(zone.items ?? []), ...(zone.shared ?? []),];
}

function flowZoneContains(zone: FlowZone, object: FlowZoneItemInput,): boolean {
	return flowZoneItems(zone,).some((item,) =>
		item.objectId === object.objectId
		&& item.objectType === object.objectType
		&& (object.projectKey === undefined || item.projectKey === object.projectKey)
	);
}

function flowZoneSummary(zone: FlowZone, object?: FlowZoneItemInput,): Record<string, unknown> {
	const items = flowZoneItems(zone,);
	return {
		id: zone.id,
		name: zone.name,
		itemCount: items.length,
		...(object ? { containsMatchingObject: flowZoneContains(zone, object,), } : {}),
	};
}

function flowZoneDetailSummary(zone: FlowZone,): Record<string, unknown> {
	return {
		...flowZoneSummary(zone,),
		items: flowZoneItems(zone,),
	};
}

async function resolveFlowZoneIdFromFlags(
	client: DataikuClient,
	flags: Record<string, string | boolean>,
	projectKey?: string,
): Promise<string | undefined> {
	const zoneId = typeof flags["zone-id"] === "string" ? flags["zone-id"].trim() : "";
	if (zoneId) return zoneId;
	const zone = typeof flags["zone"] === "string" ? flags["zone"].trim() : "";
	if (!zone) return undefined;
	const zones = await client.flowZones.list(projectKey,);
	const match = zones.find((candidate,) => candidate.id === zone || candidate.name === zone);
	if (!match) throw new UsageError(`Flow zone not found: ${zone}`, "invalid_enum",);
	return match.id;
}

async function moveCreatedItemsToZone(
	client: DataikuClient,
	flags: Record<string, string | boolean>,
	items: FlowZoneItemInput[],
	projectKey?: string,
): Promise<{ zoneId?: string; moved?: FlowZoneItemInput[]; }> {
	const zoneId = await resolveFlowZoneIdFromFlags(client, flags, projectKey,);
	if (!zoneId || items.length === 0) return {};
	await client.flowZones.moveItems(zoneId, items, projectKey,);
	return { zoneId, moved: items, };
}

function nestedValue(value: unknown, path: string[],): unknown {
	let current: unknown = value;
	for (const key of path) {
		const record = plainRecord(current,);
		if (!record) return undefined;
		current = record[key];
	}
	return current;
}

function stringPath(value: unknown, path: string[],): string | undefined {
	const item = nestedValue(value, path,);
	return typeof item === "string" && item.length > 0 ? item : undefined;
}

function numberPath(value: unknown, path: string[],): number | undefined {
	const item = nestedValue(value, path,);
	return typeof item === "number" && Number.isFinite(item,) ? item : undefined;
}

function firstNumberPath(value: unknown, paths: string[][],): number | undefined {
	for (const path of paths) {
		const item = numberPath(value, path,);
		if (item !== undefined) return item;
	}
	return undefined;
}

function jobSummaryId(job: JobSummary | Record<string, unknown>, fallback?: string,): string {
	return stringPath(job, ["baseStatus", "def", "id",],)
		?? stringPath(job, ["def", "id",],)
		?? stringPath(job, ["id",],)
		?? fallback
		?? "unknown";
}

function jobSummaryType(job: JobSummary | Record<string, unknown>,): string {
	return stringPath(job, ["baseStatus", "def", "type",],)
		?? stringPath(job, ["def", "type",],)
		?? stringPath(job, ["type",],)
		?? "unknown";
}

function jobSummaryState(job: JobSummary | Record<string, unknown>,): string {
	return stringPath(job, ["baseStatus", "state",],)
		?? stringPath(job, ["state",],)
		?? "unknown";
}

function filteredJobList(
	jobs: JobSummary[],
	flags: Record<string, string | boolean>,
): JobSummary[] {
	const state = typeof flags["state"] === "string" ? flags["state"].trim().toUpperCase() : "";
	const contains = typeof flags["contains"] === "string"
		? flags["contains"].trim().toLowerCase()
		: "";
	const output = typeof flags["output"] === "string" ? flags["output"].trim().toLowerCase() : "";
	let result = jobs.filter((job,) => {
		if (state && jobSummaryState(job,).toUpperCase() !== state) return false;
		const text = JSON.stringify(job,).toLowerCase();
		if (contains && !text.includes(contains,)) return false;
		if (output && !text.includes(output,)) return false;
		return true;
	},);
	const limit = flags["latest"] === true ? 1 : num(flags["limit"],);
	if (limit !== undefined) result = result.slice(0, Math.max(0, limit,),);
	return result;
}

function maxNumber(values: number[],): number {
	return values.length === 0 ? 0 : Math.max(...values,);
}

function collectWarningCounts(
	value: unknown,
	inActivity: boolean,
	counts: { dss: number[]; activity: number[]; },
): void {
	if (Array.isArray(value,)) {
		for (const item of value) collectWarningCounts(item, inActivity, counts,);
		return;
	}
	const record = plainRecord(value,);
	if (!record) return;
	for (const [key, item,] of Object.entries(record,)) {
		const lower = key.toLowerCase();
		const nextInActivity = inActivity || lower.includes("activit",);
		if (lower.includes("warn",)) {
			const target = nextInActivity ? counts.activity : counts.dss;
			if (typeof item === "number" && Number.isFinite(item,)) target.push(item,);
			else if (Array.isArray(item,)) target.push(item.length,);
		}
		collectWarningCounts(item, nextInActivity, counts,);
	}
}

function jobWarningSummary(
	details: Record<string, unknown>,
	log: string | undefined,
): Record<string, unknown> {
	const counts = { dss: [] as number[], activity: [] as number[], };
	collectWarningCounts(details, false, counts,);
	const warningLines = log
		? log.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) =>
			/\bwarn(?:ing)?\b/i.test(line,)
		)
		: [];
	return {
		dssSummaryWarningCount: maxNumber(counts.dss,),
		activityWarningCount: maxNumber(counts.activity,),
		logWarnLineCount: warningLines.length,
		sampledWarningMessages: warningLines.slice(0, 5,),
	};
}

function jobDurationMs(details: Record<string, unknown>,): number | undefined {
	const started = firstNumberPath(details, [
		["baseStatus", "startTime",],
		["baseStatus", "start",],
		["startTime",],
		["start",],
	],);
	const ended = firstNumberPath(details, [
		["baseStatus", "endTime",],
		["baseStatus", "end",],
		["endTime",],
		["end",],
	],);
	return started !== undefined && ended !== undefined && ended >= started
		? ended - started
		: undefined;
}

async function jobInspectionSummary(
	client: DataikuClient,
	jobId: string,
	flags: Record<string, string | boolean>,
): Promise<Record<string, unknown>> {
	const projectKey = flags["project-key"] as string | undefined;
	const details = await client.jobs.get(jobId, projectKey,);
	let log: string | undefined;
	let logError: string | undefined;
	try {
		log = await client.jobs.log(jobId, {
			activity: flags["activity"] as string | undefined,
			logId: flags["log-id"] as string | undefined,
			maxLogLines: maxLogLinesFromFlags(flags,),
			projectKey,
		},);
	} catch (error: unknown) {
		logError = error instanceof Error ? error.message : String(error,);
	}
	const durationMs = jobDurationMs(details,);
	const progress = log ? parseJobLogProgress(log, durationMs,) : undefined;
	const logLines = log
		? log.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0)
		: [];
	const maxSummaryLines = Math.max(1, maxLogLinesFromFlags(flags,) ?? 20,);
	const outputs = nestedValue(details, ["baseStatus", "def", "outputs",],)
		?? nestedValue(details, ["def", "outputs",],)
		?? details.outputs;
	return {
		resource: "job",
		jobId: jobSummaryId(details, jobId,),
		state: jobSummaryState(details,),
		type: jobSummaryType(details,),
		...(durationMs !== undefined ? { durationMs, } : {}),
		...(outputs !== undefined ? { outputs, } : {}),
		warnings: jobWarningSummary(details, log,),
		...(progress
			? {
				progress,
				latestUsefulProgressLine: progress.lastProgressLine,
				doneLine: progress.doneLine,
			}
			: {}),
		logSummary: {
			lineCount: logLines.length,
			lines: logLines.slice(-maxSummaryLines,),
			...(logError ? { error: logError, } : {}),
		},
	};
}

function stripUtf8Bom(text: string,): string {
	return text.charCodeAt(0,) === 0xfeff ? text.slice(1,) : text;
}

function parseJsonValue(text: string, source: string,): unknown {
	try {
		return JSON.parse(stripUtf8Bom(text,),) as unknown;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error,);
		throw new UsageError(`Invalid JSON in ${source}: ${message}`, "validation_failed",);
	}
}

function expectJsonObject(value: unknown, source: string,): Record<string, unknown> {
	if (value && typeof value === "object" && !Array.isArray(value,)) {
		return value as Record<string, unknown>;
	}
	throw new UsageError(`Expected JSON object in ${source}.`, "validation_failed",);
}

function parseJsonObject(text: string, source: string,): Record<string, unknown> {
	return expectJsonObject(parseJsonValue(text, source,), source,);
}

function json(
	v: string | boolean | undefined,
	source = "JSON flag",
): Record<string, unknown> | undefined {
	if (typeof v !== "string") return undefined;
	return parseJsonObject(v, source,);
}

type TlsSettings = Pick<DssCredentials, "tlsRejectUnauthorized" | "caCertPath">;

const SQL_QUERY_USAGE =
	"dss sql query [SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin] (--connection CONN | --dataset FULL_NAME) [--database DB] [--output PATH|--output-file PATH] [--request-timeout MS] [--project-key KEY]";

function readStdinText(): string {
	return readFileSync(0, "utf-8",);
}

function jsonInput(flags: Record<string, string | boolean>,): Record<string, unknown> | undefined {
	if (flags["stdin"] === true) return parseJsonObject(readStdinText(), "stdin",);
	if (typeof flags["data-file"] === "string") {
		return parseJsonObject(readFileSync(flags["data-file"], "utf-8",), flags["data-file"],);
	}
	if (typeof flags["data"] === "string") return parseJsonObject(flags["data"], "--data",);
	return undefined;
}

function unknownJsonInput(flags: Record<string, string | boolean>,): unknown {
	if (flags["stdin"] === true) return parseJsonValue(readStdinText(), "stdin",);
	if (typeof flags["data-file"] === "string") {
		return parseJsonValue(readFileSync(flags["data-file"], "utf-8",), flags["data-file"],);
	}
	if (typeof flags["data"] === "string") return parseJsonValue(flags["data"], "--data",);
	return undefined;
}

function schemaColumnsInput(flags: Record<string, string | boolean>, usage: string,): Array<{
	comment?: string;
	name: string;
	type: string;
}> {
	const input = unknownJsonInput(flags,);
	if (input === undefined) {
		throw new UsageError(`--data, --data-file, or --stdin is required. Usage: ${usage}`,);
	}
	const columns = Array.isArray(input,)
		? input
		: input && typeof input === "object" && Array.isArray((input as { columns?: unknown; }).columns,)
		? (input as { columns: unknown[]; }).columns
		: undefined;
	if (!columns) {
		throw new UsageError(
			"Schema input must be an array of columns or an object with a columns array.",
		);
	}
	return columns.map((column, index,) => {
		if (!column || typeof column !== "object" || Array.isArray(column,)) {
			throw new UsageError(`Schema column at index ${index} must be an object.`,);
		}
		const record = column as Record<string, unknown>;
		if (typeof record.name !== "string" || record.name.length === 0) {
			throw new UsageError(`Schema column at index ${index} is missing string field "name".`,);
		}
		if (typeof record.type !== "string" || record.type.length === 0) {
			throw new UsageError(`Schema column "${record.name}" is missing string field "type".`,);
		}
		return {
			...record,
			name: record.name,
			type: record.type,
			...(typeof record.comment === "string" ? { comment: record.comment, } : {}),
		};
	},);
}

function textInput(flags: Record<string, string | boolean>,): string | undefined {
	if (typeof flags["content"] === "string") return flags["content"];
	if (typeof flags["file"] === "string") return readFileSync(flags["file"], "utf-8",);
	return undefined;
}

function requiredJsonInput(
	flags: Record<string, string | boolean>,
	message: string,
): Record<string, unknown> {
	const data = jsonInput(flags,);
	if (data === undefined) throw new UsageError(message,);
	return data;
}

function parseBooleanOption(
	value: string | boolean | undefined,
	flagName: string,
): boolean | undefined {
	if (value === undefined) return undefined;
	if (typeof value === "boolean") return value;
	const normalized = value.trim().toLowerCase();
	if (["1", "true", "yes", "y", "on",].includes(normalized,)) return true;
	if (["0", "false", "no", "n", "off",].includes(normalized,)) return false;
	throw new UsageError(`${flagName} must be true or false.`, "invalid_enum",);
}

function codeEnvWait(flags: Record<string, string | boolean>,): boolean {
	return flags["no-wait"] !== true;
}

function codeEnvParams(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const params = json(flags["params"],) ?? jsonInput(flags,) ?? {};
	if (typeof flags["python-interpreter"] === "string") {
		params.pythonInterpreter = flags["python-interpreter"];
	}
	return params;
}

function splitPackageSpec(raw: string,): string[] {
	return raw.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0);
}

function codeEnvPackageList(flags: Record<string, string | boolean>,): string[] {
	const packages: string[] = [];
	if (typeof flags["file"] === "string") {
		packages.push(...splitPackageSpec(readFileSync(flags["file"], "utf-8",),),);
	}
	if (typeof flags["packages"] === "string") {
		packages.push(...splitPackageSpec(flags["packages"],),);
	}
	if (typeof flags["package"] === "string") {
		packages.push(...splitPackageSpec(flags["package"],),);
	}
	if (packages.length === 0) {
		throw new UsageError(
			"--packages, --package, or --file is required. Use newline-separated package specs for version constraints.",
		);
	}
	return packages;
}

function parseTlsRejectUnauthorizedEnv(value: string | undefined,): boolean | undefined {
	if (value === undefined) return undefined;
	const normalized = value.trim().toLowerCase();
	if (normalized === "0" || normalized === "false" || normalized === "no") return false;
	if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
	return undefined;
}

function resolveTlsSettings(
	flags: Record<string, string | boolean>,
	saved?: TlsSettings,
): TlsSettings {
	let tlsRejectUnauthorized = flags["insecure"] === true ? false : undefined;
	let caCertPath = flags["ca-cert"] as string | undefined;

	tlsRejectUnauthorized ??= parseTlsRejectUnauthorizedEnv(process.env.NODE_TLS_REJECT_UNAUTHORIZED,);
	caCertPath ??= process.env.NODE_EXTRA_CA_CERTS;

	if (tlsRejectUnauthorized === undefined) {
		tlsRejectUnauthorized = saved?.tlsRejectUnauthorized;
	}
	caCertPath ??= saved?.caCertPath;

	return { tlsRejectUnauthorized, caCertPath, };
}

function resolveSqlInput(args: string[], flags: Record<string, string | boolean>,): string {
	const sources: Array<{ label: string; read: () => string; }> = [];

	if (typeof flags["sql"] === "string") {
		sources.push({
			label: flags["sql"] === "-" ? "--sql -" : "--sql",
			read: () => flags["sql"] === "-" ? readStdinText() : String(flags["sql"],),
		},);
	}
	if (typeof flags["sql-file"] === "string") {
		sources.push({
			label: "--sql-file",
			read: () => readFileSync(flags["sql-file"] as string, "utf-8",),
		},);
	}
	if (flags["stdin"] === true) {
		sources.push({ label: "--stdin", read: readStdinText, },);
	}
	if (args.length > 1) {
		throw new UsageError(
			`Expected at most one positional SQL argument. Quote the SQL or use --sql-file/--stdin.\nUsage: ${SQL_QUERY_USAGE}`,
		);
	}
	if (args[0] !== undefined) {
		sources.push({ label: "positional SQL", read: () => args[0], },);
	}

	if (sources.length === 0) {
		throw new UsageError(`SQL input is required. Usage: ${SQL_QUERY_USAGE}`,);
	}
	if (sources.length > 1) {
		throw new UsageError(
			`Choose exactly one SQL input source: --sql, --sql-file, --stdin, or one positional SQL argument. Usage: ${SQL_QUERY_USAGE}`,
		);
	}

	const query = stripUtf8Bom(sources[0]!.read(),);
	if (query.trim().length === 0) {
		throw new UsageError(
			`SQL input from ${sources[0]!.label} must not be empty. Usage: ${SQL_QUERY_USAGE}`,
		);
	}
	return query;
}

async function resolveFolderId(
	client: DataikuClient,
	nameOrId: string,
	flags: Record<string, string | boolean>,
): Promise<string> {
	return client.folders.resolveId(nameOrId, flags["project-key"] as string | undefined,);
}

function formatLineDiff(
	remoteName: string,
	localPath: string,
	remoteContent: string,
	localContent: string,
): string {
	if (localContent === remoteContent) {
		return "No differences.";
	}

	const localLines = localContent.split("\n",);
	const remoteLines = remoteContent.split("\n",);
	const lines: string[] = [`--- remote:${remoteName}`, `+++ local:${localPath}`, "",];
	const maxLen = Math.max(localLines.length, remoteLines.length,);

	for (let i = 0; i < maxLen; i++) {
		const remoteLine = remoteLines[i];
		const localLine = localLines[i];
		if (remoteLine === localLine) continue;

		if (remoteLine !== undefined && localLine !== undefined) {
			lines.push(`@@ line ${String(i + 1,)} @@`,);
			lines.push(`- ${remoteLine}`,);
			lines.push(`+ ${localLine}`,);
			continue;
		}

		if (remoteLine !== undefined) {
			lines.push(`- ${remoteLine}`,);
			continue;
		}

		lines.push(`+ ${localLine}`,);
	}

	return lines.join("\n",);
}

function writeCommandResult(result: unknown,): void {
	process.stdout.write(`${JSON.stringify(result ?? { ok: true, }, null, 2,)}\n`,);
}

function transientBodyWithTargetContext(body: string, target: string, elapsedMs: number,): string {
	try {
		const parsed = JSON.parse(body,) as unknown;
		if (parsed && typeof parsed === "object" && !Array.isArray(parsed,)) {
			const record = parsed as Record<string, unknown>;
			const message = typeof record.message === "string" && record.message.length > 0
				? `Target: ${target}\nElapsed: ${elapsedMs}ms\n${record.message}`
				: `Target: ${target}\nElapsed: ${elapsedMs}ms`;
			return JSON.stringify({ ...record, message, target, elapsedMs, },);
		}
	} catch {
		// Non-JSON DSS bodies are wrapped as text below.
	}
	return `Target: ${target}\nElapsed: ${elapsedMs}ms\n${body}`;
}

function addTransientTargetContext(error: unknown, target: string, elapsedMs: number,): never {
	if (error instanceof DataikuError && error.category === "transient") {
		throw new DataikuError(
			error.status,
			error.statusText,
			transientBodyWithTargetContext(error.body, target, elapsedMs,),
			error.retry,
		);
	}
	throw error;
}

function isFailedWaitResult(result: unknown,): boolean {
	if (result === null || typeof result !== "object" || Array.isArray(result,)) return false;
	const record = result as Record<string, unknown>;
	return record.success === false
		&& typeof record.elapsedMs === "number"
		&& typeof record.pollCount === "number"
		&& (typeof record.state === "string" || typeof record.outcome === "string");
}

function commandFailureExitCode(result: unknown,): number | undefined {
	if (isFailedWaitResult(result,)) return 4;
	if (
		result && typeof result === "object" && (result as Record<string, unknown>).unchanged === false
	) return 4;
	return undefined;
}
function isNotFoundError(error: unknown,): boolean {
	if (error instanceof DataikuError) return error.category === "not_found";
	if (error instanceof Error) return /not found|does not exist|unknown/i.test(error.message,);
	return false;
}

async function readIfExists<T,>(reader: () => Promise<T>,): Promise<T | undefined> {
	try {
		return await reader();
	} catch (error) {
		if (isNotFoundError(error,)) return undefined;
		throw error;
	}
}

function skipResult(
	resource: string,
	id: string,
	reason: "exists" | "missing",
	extra: Record<string, unknown> = {},
): Record<string, unknown> {
	return { skipped: id, reason, resource, ...extra, };
}

function planResult(
	resource: string,
	action: string,
	options: {
		asyncKind: string;
		endpoint?: string;
		exitCodesOnFailure: Record<string, number>;
		identifiers?: Record<string, unknown>;
		idempotency: string;
		method?: string;
		payload?: unknown;
		localWrites?: unknown;
		plannedAndDryRun?: boolean;
		wait?: unknown;
	},
): Record<string, unknown> {
	return {
		plan: true,
		action,
		resource,
		...(options.plannedAndDryRun ? { plannedAndDryRun: true, } : {}),
		...options.identifiers,
		...(options.method ? { method: options.method, } : {}),
		...(options.endpoint ? { endpoint: options.endpoint, } : {}),
		...(options.payload !== undefined ? { payload: options.payload, } : {}),
		...(options.localWrites !== undefined ? { localWrites: options.localWrites, } : {}),
		...(options.wait !== undefined ? { wait: options.wait, } : {}),
		idempotency: options.idempotency,
		async: options.asyncKind,
		exitCodesOnFailure: options.exitCodesOnFailure,
	};
}

function encodedProjectEndpoint(
	client: DataikuClient,
	projectKey: string | undefined,
	suffix: string,
): string {
	return `/public/api/projects/${
		encodeURIComponent(client.resolveProjectKey(projectKey,),)
	}${suffix}`;
}

function encodedProjectEndpointForPlan(projectKey: string, suffix: string,): string {
	return `/public/api/projects/${encodeURIComponent(projectKey,)}${suffix}`;
}

function stringField(record: Record<string, unknown>, fields: string[],): string | undefined {
	for (const field of fields) {
		const value = record[field];
		if (typeof value === "string" && value.length > 0) return value;
	}
	return undefined;
}

function projectArg(projectKey: string | undefined,): string[] {
	return projectKey ? ["--project-key", projectKey,] : [];
}

function resultRecord(result: unknown,): Record<string, unknown> {
	return result !== null && typeof result === "object" && !Array.isArray(result,)
		? result as Record<string, unknown>
		: {};
}

function cleanupLedgerEntry(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	result: unknown,
	projectKey: string | undefined,
): CleanupLedgerEntry | undefined {
	if (!(action.startsWith("create",) || action === "clone" || action === "upload")) return undefined;
	const record = resultRecord(result,);
	if (record.skipped !== undefined) return undefined;
	const project = flags["project-key"] as string | undefined ?? projectKey;
	const withProject = projectArg(project,);
	const ts = new Date().toISOString();
	const base = { ts, action, resource, ...(project ? { projectKey: project, } : {}), };
	switch (`${resource}.${action}`) {
		case "dataset.create": {
			const name = stringField(record, ["created", "name",],) ?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["dataset", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "dataset.clone": {
			const name = stringField(record, ["target", "created", "name",],) ?? args[1];
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["dataset", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "recipe.create": {
			const name = stringField(record, ["created", "recipeName", "name",],)
				?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["recipe", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "recipe.clone": {
			const name = stringField(record, ["recipeName", "target", "created", "name",],)
				?? flags["name"] as string | undefined;
			if (!name) return undefined;
			return {
				...base,
				name,
				cleanup: { argv: ["recipe", "delete", name, "--if-exists", ...withProject,], },
			};
		}
		case "scenario.create": {
			const id = args[0];
			return {
				...base,
				id,
				name: args[1],
				cleanup: { argv: ["scenario", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "flow-zone.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["flow-zone", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "folder.create": {
			const id = stringField(record, ["created", "id",],) ?? flags["name"] as string | undefined;
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["folder", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "wiki.create": {
			const article =
				record.article && typeof record.article === "object" && !Array.isArray(record.article,)
					? record.article as Record<string, unknown>
					: {};
			const id = stringField(record, ["created",],) ?? stringField(article, ["id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["wiki", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "dashboard.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["dashboard", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "insight.create": {
			const id = stringField(record, ["created", "id",],);
			if (!id) return undefined;
			return {
				...base,
				id,
				name: flags["name"] as string | undefined,
				cleanup: { argv: ["insight", "delete", id, "--if-exists", ...withProject,], },
			};
		}
		case "data-quality.create-rule": {
			const ruleId = stringField(record, ["id", "created",],);
			if (!ruleId) return undefined;
			return {
				...base,
				id: ruleId,
				name: args[0],
				cleanup: {
					argv: ["data-quality", "delete-rule", args[0], ruleId, "--if-exists", ...withProject,],
				},
			};
		}
		case "code-env.create": {
			const lang = args[0];
			const name = args[1];
			return {
				...base,
				id: `${lang}:${name}`,
				name,
				cleanup: { argv: ["code-env", "delete", lang, name, "--if-exists",], },
			};
		}
		case "folder.upload":
			return {
				...base,
				name: args[0],
				path: args[1],
				cleanup: { argv: ["folder", "delete-file", args[0], args[1], ...withProject,], },
			};
		default:
			return undefined;
	}
}

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

const BOOLEAN_FLAGS = new Set([
	"help",
	"verbose",
	"version",
	"stdin",
	"insecure",
	"global",
	"list-agents",
	"include-raw",
	"raw",
	"include-payload",
	"no-payload",
	"include-logs",
	"summary",
	"replace",
	"dry-run",
	"plan",
	"apply",
	"capabilities",
	"fast",
	"include-all-partitions",
	"wait",
	"if-not-exists",
	"if-exists",
	"json",
	"report-json",
	"no-wait",
	"force-rebuild",
	"latest",
	"copy-output-settings",
	"continue-on-error",
	"no-backup",
	"payload-only",
	"allow-same-path",
],);

const SHORT_FLAGS: Record<string, string> = {
	h: "help",
	v: "verbose",
	V: "version",
	o: "output",
};

/** Long-flag aliases: these are normalized to the canonical name in parseArgs. */
const FLAG_ALIASES: Record<string, string> = {
	project: "project-key",
	dryrun: "dry-run",
	"skip-tls-verify": "insecure",
	"extra-ca-certs": "ca-cert",
	explain: "plan",
	"zone-name": "zone",
};

const VALUE_FLAGS = new Set([
	"activity",
	"agent",
	"api-key",
	"build-mode",
	"backup-dir",
	"backup",
	"ca-cert",
	"catalog",
	"cell-id",
	"allow-types",
	"color",
	"connection",
	"contains",
	"content",
	"content-type",
	"data",
	"active",
	"deployment-mode",
	"env-version",
	"data-file",
	"database",
	"dataset",
	"file",
	"install-core-packages",
	"folder",
	"input",
	"from",
	"knowledge-bank",
	"labeling-task",
	"lang",
	"package",
	"packages",
	"local",
	"max-edges",
	"max-lines",
	"max-log-lines",
	"listed",
	"max-nodes",
	"max-rows",
	"limit",
	"max-timestamp",
	"only-monitored",
	"min-timestamp",
	"mode",
	"log-filter",
	"log-id",
	"model-evaluation-store",
	"name",
	"object",
	"metastore-table",
	"output",
	"output-file",
	"output-connection",
	"output-folder",
	"page",
	"partition",
	"parent",
	"path",
	"project-key",
	"recipe",
	"request-timeout",
	"params",
	"results-per-page",
	"record-cleanup",
	"rule-id",
	"retries",
	"poll-interval",
	"python-interpreter",
	"replace-input",
	"replace-output",
	"replace-payload-text",
	"retain",
	"saved-model",
	"sql",
	"schema",
	"sql-file",
	"standard",
	"state",
	"streaming-endpoint",
	"target",
	"target-type",
	"timeout",
	"table",
	"type",
	"url",
	"until",
	"to",
	"zone",
	"zone-id",
],);

const REPEATABLE_VALUE_FLAGS = new Set([
	"dataset",
	"folder",
	"input",
	"object",
	"package",
	"recipe",
	"replace-input",
	"replace-output",
	"replace-payload-text",
],);

const KNOWN_LONG_FLAGS = new Set([
	...BOOLEAN_FLAGS,
	...VALUE_FLAGS,
	...Object.keys(FLAG_ALIASES,),
	...Object.values(FLAG_ALIASES,),
],);

function normalizeLongFlag(rawFlagName: string,): string {
	const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
	if (!KNOWN_LONG_FLAGS.has(rawFlagName,) && !KNOWN_LONG_FLAGS.has(flagName,)) {
		throw new UsageError(`Unknown flag: --${rawFlagName}`, "unknown_flag",);
	}
	return flagName;
}

function isNegativeNumberToken(value: string,): boolean {
	return value.startsWith("-",) && Number.isFinite(Number(value,),);
}

function requireFlagValue(
	flagLabel: string,
	next: string | undefined,
): string {
	if (next === undefined || (next.startsWith("-",) && !isNegativeNumberToken(next,))) {
		throw new UsageError(`Flag ${flagLabel} requires a value.`, "missing_required_flag",);
	}
	return next;
}

function setParsedFlagValue(
	flags: Record<string, string | boolean>,
	flagName: string,
	value: string,
): void {
	const current = flags[flagName];
	if (REPEATABLE_VALUE_FLAGS.has(flagName,) && typeof current === "string" && current.length > 0) {
		flags[flagName] = `${current},${value}`;
		return;
	}
	flags[flagName] = value;
}

interface ParsedArgs {
	positional: string[];
	flags: Record<string, string | boolean>;
}

function parseArgs(argv: string[],): ParsedArgs {
	const positional: string[] = [];
	const flags: Record<string, string | boolean> = {};
	let i = 0;
	while (i < argv.length) {
		const arg = argv[i];
		if (arg === "--") {
			positional.push(...argv.slice(i + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const eqIdx = arg.indexOf("=",);
			if (eqIdx !== -1) {
				const raw = arg.slice(2, eqIdx,);
				const flagName = normalizeLongFlag(raw,);
				setParsedFlagValue(flags, flagName, arg.slice(eqIdx + 1,),);
			} else {
				const rawFlagName = arg.slice(2,);
				const flagName = normalizeLongFlag(rawFlagName,);
				if (BOOLEAN_FLAGS.has(flagName,)) {
					flags[flagName] = true;
				} else {
					const next = requireFlagValue(`--${rawFlagName}`, argv[i + 1],);
					setParsedFlagValue(flags, flagName, next,);
					i++;
				}
			}
		} else if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long) {
				if (BOOLEAN_FLAGS.has(long,)) {
					flags[long] = true;
				} else {
					const next = requireFlagValue(`-${arg[1]}`, argv[i + 1],);
					setParsedFlagValue(flags, long, next,);
					i++;
				}
			} else {
				throw new UsageError(`Unknown flag: -${arg[1]}`, "unknown_flag",);
			}
		} else {
			positional.push(arg,);
		}
		i++;
	}
	return { positional, flags, };
}

// ---------------------------------------------------------------------------
// Command registry
// ---------------------------------------------------------------------------

type CommandHandler = (
	client: DataikuClient,
	args: string[],
	flags: Record<string, string | boolean>,
) => Promise<unknown>;

interface CommandPayloadSchema {
	stdin?: boolean;
	dataFlag?: boolean;
	dataFileFlag?: boolean;
	jsonShape?: "object" | "array";
}

interface CommandRegistryOverride {
	requiredFlags?: string[];
	optionalFlags?: string[];
	payloadSchema?: CommandPayloadSchema;
	examplePayload?: unknown;
	cleanupCommand?: string;
}

interface CommandMeta extends CommandRegistryOverride {
	handler: CommandHandler;
	usage: string;
	description?: string;
	examples?: string[];
}

const commands: Record<string, Record<string, CommandMeta>> = {
	project: {
		list: {
			handler: (c,) => c.projects.list(),
			usage: "dss project list",
			description: "List all accessible projects.",
			examples: ["dss project list",],
		},
		get: {
			handler: (c, _a, f,) => c.projects.get(f["project-key"] as string | undefined,),
			usage: "dss project get [--project-key KEY]",
			description: "Get project settings and metadata.",
			examples: ["dss project get", "dss project get --project-key MYPROJ",],
		},
		metadata: {
			handler: (c, _a, f,) => c.projects.metadata(f["project-key"] as string | undefined,),
			usage: "dss project metadata [--project-key KEY]",
			description: "Get project-level metadata (tags, labels, custom fields).",
			examples: ["dss project metadata", "dss project metadata --project-key MYPROJ",],
		},
		flow: {
			handler: (c, _a, f,) => c.projects.flow(f["project-key"] as string | undefined,),
			usage: "dss project flow [--project-key KEY]",
			description: "Get the raw flow graph (all datasets, recipes, and edges).",
			examples: ["dss project flow", "dss project flow --project-key MYPROJ",],
		},
		map: {
			handler: (c, _a, f,) =>
				c.projects.map({
					maxNodes: num(f["max-nodes"],),
					maxEdges: num(f["max-edges"],),
					includeRaw: f["include-raw"] === true,
				},),
			usage: "dss project map [--max-nodes N] [--max-edges N] [--include-raw]",
			description: "Get a summarized, truncated flow map.",
			examples: [
				"dss project map",
				"dss project map --max-nodes 50 --max-edges 100",
				"dss project map --include-raw",
			],
		},
	},

	doctor: {
		run: {
			handler: async (_c, _a, f,) => (await runDoctor(f,)).result,
			usage: "dss doctor [--project-key KEY] [--capabilities] [--fast]",
			description: "Run JSON diagnostics for DSS credentials, connectivity, and project access.",
			examples: ["dss doctor", "dss doctor --project-key MYPROJ", "dss doctor --capabilities --fast",],
		},
	},

	wiki: {
		settings: {
			handler: (c, _a, f,) => c.wiki.settings(f["project-key"] as string | undefined,),
			usage: "dss wiki settings [--project-key KEY]",
			description: "Get project wiki settings and taxonomy.",
			examples: ["dss wiki settings",],
		},
		list: {
			handler: (c, _a, f,) => c.wiki.list(f["project-key"] as string | undefined,),
			usage: "dss wiki list [--project-key KEY]",
			description: "List wiki articles by walking the taxonomy.",
			examples: ["dss wiki list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss wiki get <id-or-name>",);
				return c.wiki.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss wiki get <id-or-name> [--project-key KEY]",
			description: "Get a wiki article including markdown body.",
			examples: ["dss wiki get ARTICLE_ID",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const name = f["name"] as string | undefined;
				if (!name) throw new UsageError("--name is required. Usage: dss wiki create --name NAME",);
				const content = textInput(f,);
				const pk = f["project-key"] as string | undefined;
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const existing = (await c.wiki.list(pk,)).find((article,) => article.article.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("wiki", existing.article.id, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "wiki",
							name,
							payload: {
								name,
								parent: f["parent"] as string | undefined,
								content,
							},
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.wiki.create({
					name,
					parent: f["parent"] as string | undefined,
					content,
					projectKey: pk,
				},);
				return { created: created.article.id, resource: "wiki", ...created, };
			},
			usage:
				"dss wiki create --name NAME [--parent ID] [--content TEXT|--file PATH] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a wiki article, optionally with markdown content.",
			examples: [
				"dss wiki create --name 'Agent notes' --content '# Notes'",
				"dss wiki create --name 'Agent notes' --file article.md --dry-run",
			],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss wiki update <id-or-name> [--name NAME] [--content TEXT|--file PATH|--data JSON]",
				);
				const data = jsonInput(f,);
				const content = textInput(f,);
				const name = f["name"] as string | undefined;
				if (!data && content === undefined && name === undefined) {
					throw new UsageError(
						"--name, --content, --file, --data, --data-file, or --stdin is required.",
					);
				}
				if (f["dry-run"] === true) {
					const current = await c.wiki.get(a[0], f["project-key"] as string | undefined,);
					const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
					if (name !== undefined) {
						next.article = {
							...((next.article && typeof next.article === "object" && !Array.isArray(next.article,))
								? next.article as Record<string, unknown>
								: {}),
							name,
						};
					}
					if (content !== undefined) next.payload = content;
					return { dryRun: true, action: "update", resource: "wiki", article: a[0], current, next, };
				}
				return c.wiki.update(a[0], {
					name,
					content,
					data,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss wiki update <id-or-name> [--name NAME] [--content TEXT|--file PATH|--data JSON|--data-file PATH|--stdin] [--dry-run] [--project-key KEY]",
			description: "Update wiki article metadata/body via merge.",
			examples: ["dss wiki update ARTICLE_ID --content '# Updated' --dry-run",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss wiki delete <id-or-name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.wiki.get(a[0], pk,));
					if (!current) return skipResult("wiki", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "wiki", article: a[0], current, };
					}
				}
				await c.wiki.delete(a[0], pk,);
				return { deleted: a[0], resource: "wiki", };
			},
			usage: "dss wiki delete <id-or-name> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a wiki article.",
			examples: ["dss wiki delete ARTICLE_ID --dry-run",],
		},
	},

	dashboard: {
		list: {
			handler: (c, _a, f,) => c.dashboards.list(f["project-key"] as string | undefined,),
			usage: "dss dashboard list [--project-key KEY]",
			description: "List project dashboards.",
			examples: ["dss dashboard list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dashboard get <id>",);
				return c.dashboards.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dashboard get <id> [--project-key KEY]",
			description: "Get dashboard definition.",
			examples: ["dss dashboard get DASHBOARD_ID",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const name = f["name"] as string | undefined;
				if (!name) throw new UsageError("--name is required. Usage: dss dashboard create --name NAME",);
				const settings = jsonInput(f,);
				const pk = f["project-key"] as string | undefined;
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const existing = (await c.dashboards.list(pk,)).find((dashboard,) => dashboard.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("dashboard", existing.id, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "dashboard",
							name,
							payload: settings ?? { pages: [], },
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.dashboards.create({
					name,
					settings,
					projectKey: pk,
				},);
				return { created: created.id, resource: "dashboard", ...created, };
			},
			usage:
				"dss dashboard create --name NAME [--data JSON|--data-file PATH|--stdin] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a dashboard. Defaults to an empty pages array.",
			examples: ["dss dashboard create --name 'Agent dashboard' --dry-run",],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dashboard update <id> [--name NAME|--data JSON]",);
				const name = f["name"] as string | undefined;
				const data = jsonInput(f,);
				if (!name && !data) {
					throw new UsageError("--name, --data, --data-file, or --stdin is required.",);
				}
				if (f["dry-run"] === true) {
					const current = await c.dashboards.get(a[0], f["project-key"] as string | undefined,);
					const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
					if (name !== undefined) next.name = name;
					return { dryRun: true, action: "update", resource: "dashboard", id: a[0], current, next, };
				}
				return c.dashboards.update(a[0], {
					name,
					data,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss dashboard update <id> [--name NAME|--data JSON|--data-file PATH|--stdin] [--dry-run] [--project-key KEY]",
			description: "Update dashboard settings via merge.",
			examples: ["dss dashboard update DASHBOARD_ID --name 'New name' --dry-run",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dashboard delete <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.dashboards.get(a[0], pk,));
					if (!current) return skipResult("dashboard", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "dashboard", id: a[0], current, };
					}
				}
				await c.dashboards.delete(a[0], pk,);
				return { deleted: a[0], resource: "dashboard", };
			},
			usage: "dss dashboard delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a dashboard.",
			examples: ["dss dashboard delete DASHBOARD_ID --dry-run",],
		},
	},

	insight: {
		list: {
			handler: (c, _a, f,) => c.insights.list(f["project-key"] as string | undefined,),
			usage: "dss insight list [--project-key KEY]",
			description: "List project insights.",
			examples: ["dss insight list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss insight get <id>",);
				return c.insights.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss insight get <id> [--project-key KEY]",
			description: "Get insight definition.",
			examples: ["dss insight get INSIGHT_ID",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const data = jsonInput(f,);
				const name = f["name"] as string | undefined;
				const type = f["type"] as string | undefined;
				const params = json(f["params"],);
				const listed = parseBooleanOption(f["listed"], "--listed",);
				const contentType = f["content-type"] as string | undefined;
				const payload = textInput(f,);
				if (!data && (!name || !type)) {
					throw new UsageError(
						"--data or both --name and --type are required. Usage: dss insight create --name NAME --type TYPE",
					);
				}
				const prototype: Record<string, unknown> = { ...data, };
				if (name !== undefined) prototype.name = name;
				if (type !== undefined) prototype.type = type;
				if (listed !== undefined) prototype.listed = listed;
				if (params !== undefined) prototype.params = params;
				const pk = f["project-key"] as string | undefined;
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const existing = name
						? (await c.insights.list(pk,)).find((insight,) => insight.name === name)
						: undefined;
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("insight", existing.id, "exists", { current: existing, },);
					}
					if (f["if-not-exists"] === true && !name && f["dry-run"] !== true) {
						throw new UsageError("--if-not-exists requires --name for insight create.",);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "insight",
							name,
							payload: prototype,
							contentType,
							content: payload,
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.insights.create({
					data,
					name,
					type,
					listed,
					params,
					contentType,
					payload,
					projectKey: pk,
				},);
				return { created: created.id, resource: "insight", ...created, };
			},
			usage:
				"dss insight create (--data JSON|--data-file PATH|--stdin | --name NAME --type TYPE [--params JSON] [--listed true|false]) [--content TEXT|--file PATH --content-type MIME] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create an insight from a raw prototype or minimal name/type fields.",
			examples: [
				"dss insight create --name 'Agent chart' --type chart --params '{\"dataset\":\"orders\"}' --dry-run",
				"dss insight create --data-file insight-prototype.json --dry-run",
			],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss insight update <id> [--name NAME|--params JSON|--data JSON]",);
				const data = jsonInput(f,);
				const name = f["name"] as string | undefined;
				const params = json(f["params"],);
				const listed = parseBooleanOption(f["listed"], "--listed",);
				const contentType = f["content-type"] as string | undefined;
				const payload = textInput(f,);
				if (
					!data && name === undefined && params === undefined && listed === undefined
					&& contentType === undefined && payload === undefined
				) {
					throw new UsageError(
						"--name, --listed, --params, --content, --file, --data, --data-file, or --stdin is required.",
					);
				}
				if (f["dry-run"] === true) {
					const current = await c.insights.get(a[0], f["project-key"] as string | undefined,);
					const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
					if (name !== undefined) next.name = name;
					if (listed !== undefined) next.listed = listed;
					if (params !== undefined) {
						const currentParams = next.params;
						next.params =
							currentParams && typeof currentParams === "object" && !Array.isArray(currentParams,)
								? deepMerge(currentParams as Record<string, unknown>, params,)
								: params;
					}
					return {
						dryRun: true,
						action: "update",
						resource: "insight",
						id: a[0],
						current,
						next,
						contentType,
						payload,
					};
				}
				return c.insights.update(a[0], {
					data,
					name,
					listed,
					params,
					contentType,
					payload,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss insight update <id> [--name NAME] [--listed true|false] [--params JSON] [--content TEXT|--file PATH --content-type MIME] [--data JSON|--data-file PATH|--stdin] [--dry-run] [--project-key KEY]",
			description: "Update an insight using GET-before-POST merge semantics.",
			examples: ["dss insight update INSIGHT_ID --name 'Updated' --dry-run",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss insight delete <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.insights.get(a[0], pk,));
					if (!current) return skipResult("insight", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "insight", id: a[0], current, };
					}
				}
				await c.insights.delete(a[0], pk,);
				return { deleted: a[0], resource: "insight", };
			},
			usage: "dss insight delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete an insight.",
			examples: ["dss insight delete INSIGHT_ID --dry-run",],
		},
	},

	"data-quality": {
		rules: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality rules <dataset>",);
				return c.dataQuality.listRules(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss data-quality rules <dataset> [--project-key KEY]",
			description: "List data quality rules for a dataset.",
			examples: ["dss data-quality rules orders",],
		},
		"get-rule": {
			handler: (c, a, f,) => {
				requireArgs(a, 2, "dss data-quality get-rule <dataset> <rule-id>",);
				return c.dataQuality.getRule(a[0], a[1], f["project-key"] as string | undefined,);
			},
			usage: "dss data-quality get-rule <dataset> <rule-id> [--project-key KEY]",
			description: "Get one data quality rule by id.",
			examples: ["dss data-quality get-rule orders RULE_ID",],
		},
		status: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality status <dataset>",);
				return c.dataQuality.status(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss data-quality status <dataset> [--project-key KEY]",
			description: "Get the aggregate data quality status for a dataset.",
			examples: ["dss data-quality status orders",],
		},
		"create-rule": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality create-rule <dataset> --data JSON",);
				const config = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
				const pk = f["project-key"] as string | undefined;
				const identity = typeof config.id === "string"
					? config.id
					: typeof config.displayName === "string"
					? config.displayName
					: undefined;
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const existing = identity
						? (await c.dataQuality.listRules(a[0], pk,)).find((rule,) =>
							rule.id === identity || rule.displayName === identity
						)
						: undefined;
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("data-quality", identity ?? existing.id, "exists", {
							dataset: a[0],
							current: existing,
						},);
					}
					if (f["if-not-exists"] === true && !identity && f["dry-run"] !== true) {
						throw new UsageError("--if-not-exists requires rule id or displayName in the rule JSON.",);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create-rule",
							resource: "data-quality",
							dataset: a[0],
							payload: config,
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.dataQuality.createRule(a[0], {
					config,
					projectKey: pk,
				},);
				return {
					created: created.id ?? identity ?? "rule",
					dataset: a[0],
					resource: "data-quality",
					...created,
				};
			},
			usage:
				"dss data-quality create-rule <dataset> (--data JSON|--data-file PATH|--stdin) [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a data quality rule from raw rule config.",
			examples: [
				'dss data-quality create-rule orders --data \'{"type":"RecordCountInRangeRule","softMinimum":1,"softMinimumEnabled":true,"displayName":"Has rows"}\' --dry-run',
			],
		},
		"update-rule": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss data-quality update-rule <dataset> <rule-id> --data JSON",);
				const data = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
				if (f["dry-run"] === true) {
					const current = await c.dataQuality.getRule(
						a[0],
						a[1],
						f["project-key"] as string | undefined,
					);
					const next = deepMerge(current as unknown as Record<string, unknown>, data,);
					return {
						dryRun: true,
						action: "update-rule",
						resource: "data-quality",
						dataset: a[0],
						ruleId: a[1],
						current,
						next,
					};
				}
				return c.dataQuality.updateRule(a[0], a[1], {
					data,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss data-quality update-rule <dataset> <rule-id> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
			description: "Update a data quality rule via GET-before-PUT merge.",
			examples: [
				"dss data-quality update-rule orders RULE_ID --data '{\"enabled\":false}' --dry-run",
			],
		},
		"delete-rule": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss data-quality delete-rule <dataset> <rule-id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.dataQuality.getRule(a[0], a[1], pk,));
					if (!current) return skipResult("data-quality", a[1], "missing", { dataset: a[0], },);
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "delete-rule",
							resource: "data-quality",
							dataset: a[0],
							ruleId: a[1],
							current,
						};
					}
				}
				await c.dataQuality.deleteRule(a[0], a[1], pk,);
				return { deleted: a[1], dataset: a[0], resource: "data-quality", };
			},
			usage:
				"dss data-quality delete-rule <dataset> <rule-id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a data quality rule.",
			examples: ["dss data-quality delete-rule orders RULE_ID --dry-run",],
		},
		"status-by-partition": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality status-by-partition <dataset>",);
				return c.dataQuality.statusByPartition(a[0], {
					includeAllPartitions: f["include-all-partitions"] === true,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss data-quality status-by-partition <dataset> [--include-all-partitions] [--project-key KEY]",
			description: "Get data quality status by dataset partition.",
			examples: ["dss data-quality status-by-partition orders --include-all-partitions",],
		},
		"last-results": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality last-results <dataset>",);
				return c.dataQuality.lastResults(a[0], {
					partition: f["partition"] as string | undefined,
					ruleId: f["rule-id"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss data-quality last-results <dataset> [--partition P] [--rule-id ID] [--project-key KEY]",
			description: "Get latest data quality rule results for a dataset.",
			examples: ["dss data-quality last-results orders",],
		},
		history: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality history <dataset>",);
				return c.dataQuality.history(a[0], {
					minTimestamp: num(f["min-timestamp"],),
					maxTimestamp: num(f["max-timestamp"],),
					resultsPerPage: num(f["results-per-page"],),
					page: num(f["page"],),
					ruleId: f["rule-id"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss data-quality history <dataset> [--rule-id ID] [--min-timestamp MS] [--max-timestamp MS] [--results-per-page N] [--page N] [--project-key KEY]",
			description: "Get data quality rule execution history.",
			examples: ["dss data-quality history orders --results-per-page 100",],
		},
		"project-status": {
			handler: (c, _a, f,) =>
				c.dataQuality.projectStatus({
					onlyMonitored: parseBooleanOption(f["only-monitored"], "--only-monitored",),
					projectKey: f["project-key"] as string | undefined,
				},),
			usage: "dss data-quality project-status [--only-monitored true|false] [--project-key KEY]",
			description: "Get project-level data quality status by dataset.",
			examples: ["dss data-quality project-status --only-monitored false",],
		},
		"project-timeline": {
			handler: (c, _a, f,) =>
				c.dataQuality.projectTimeline({
					minTimestamp: num(f["min-timestamp"],),
					maxTimestamp: num(f["max-timestamp"],),
					projectKey: f["project-key"] as string | undefined,
				},),
			usage:
				"dss data-quality project-timeline [--min-timestamp MS] [--max-timestamp MS] [--project-key KEY]",
			description: "Get project-level data quality timeline aggregates.",
			examples: ["dss data-quality project-timeline --min-timestamp 1714521600000",],
		},
		compute: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss data-quality compute <dataset>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					partition: f["partition"] as string | undefined,
					pollIntervalMs: num(f["poll-interval"],),
					ruleId: f["rule-id"] as string | undefined,
					projectKey: pk,
					timeoutMs: num(f["timeout"],),
				};
				if (f["dry-run"] === true) {
					const params = new URLSearchParams();
					params.set("partition", options.partition?.trim() ? options.partition : "NP",);
					if (options.ruleId !== undefined) params.set("ruleId", options.ruleId,);
					return {
						dryRun: true,
						action: "compute",
						resource: "data-quality",
						dataset: a[0],
						...options,
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/datasets/${
								encodeURIComponent(a[0],)
							}/data-quality/actions/compute-rules?${params.toString()}`,
						),
						method: "POST",
					};
				}
				if (f["wait"] === true) return c.dataQuality.computeRulesAndWait(a[0], options,);
				return c.dataQuality.computeRules(a[0], options,);
			},
			usage:
				"dss data-quality compute <dataset> [--partition P] [--rule-id ID] [--wait] [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
			description:
				"Start data quality rule computation, optionally waiting on the returned DSS future.",
			examples: [
				"dss data-quality compute orders --dry-run",
				"dss data-quality compute orders --wait",
			],
		},
	},

	future: {
		get: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss future get <id>",);
				return c.futures.get(a[0],);
			},
			usage: "dss future get <id>",
			description: "Get a DSS future state and retrieve the result if ready.",
			examples: ["dss future get FUTURE_ID",],
		},
		peek: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss future peek <id>",);
				return c.futures.peek(a[0],);
			},
			usage: "dss future peek <id>",
			description: "Peek at a DSS future state without consuming its result.",
			examples: ["dss future peek FUTURE_ID",],
		},
		wait: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss future wait <id>",);
				return c.futures.wait(a[0], {
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
				},);
			},
			usage: "dss future wait <id> [--timeout MS] [--poll-interval MS]",
			description: "Wait for a DSS future to finish.",
			examples: ["dss future wait FUTURE_ID --timeout 60000",],
		},
		abort: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss future abort <id>",);
				if (f["dry-run"] === true) {
					const current = await c.futures.peek(a[0],);
					return { dryRun: true, action: "abort", resource: "future", id: a[0], current, };
				}
				await c.futures.abort(a[0],);
				return { aborted: a[0], resource: "future", };
			},
			usage: "dss future abort <id> [--dry-run]",
			description: "Abort a DSS future.",
			examples: ["dss future abort FUTURE_ID --dry-run",],
		},
	},
	"flow-zone": {
		list: {
			handler: async (c, _a, f,) => {
				const zones = await c.flowZones.list(f["project-key"] as string | undefined,);
				if (f["summary"] !== true) return zones;
				const objects = flowZoneMoveItems(f,);
				const object = objects.length === 1 ? objects[0] : undefined;
				return zones.map((zone,) => flowZoneSummary(zone, object,));
			},
			usage: "dss flow-zone list [--summary] [--object TYPE:ID] [--project-key KEY]",
			description: "List flow zones in a project, optionally as compact summaries.",
			examples: ["dss flow-zone list", "dss flow-zone list --summary --object RECIPE:compute_orders",],
		},
		find: {
			handler: async (c, a, f,) => {
				const zones = await c.flowZones.list(f["project-key"] as string | undefined,);
				const objects = flowZoneMoveItems(f,);
				const query = a[0]?.trim();
				if (query && objects.length === 0) {
					const normalized = query.toLowerCase();
					return zones
						.filter((zone,) =>
							zone.id.toLowerCase().includes(normalized,)
							|| zone.name.toLowerCase().includes(normalized,)
						)
						.map((zone,) => flowZoneDetailSummary(zone,));
				}
				if (objects.length !== 1) {
					throw new UsageError(
						"Exactly one zone name/id or object is required. Use <name>, --object TYPE:ID, --dataset DS, or --recipe R.",
					);
				}
				const object = objects[0]!;
				return zones
					.filter((zone,) => flowZoneContains(zone, object,))
					.map((zone,) => flowZoneSummary(zone, object,));
			},
			usage:
				"dss flow-zone find [name-or-id] [--object TYPE:ID | --dataset DS | --recipe R | --folder F] [--project-key KEY]",
			description: "Find flow zones by name/id or by contained flow object.",
			examples: [
				"dss flow-zone find ATH_SNW_MAP_FRG49",
				"dss flow-zone find --object RECIPE:compute_orders",
				"dss flow-zone find --dataset orders",
			],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss flow-zone get <id>",);
				return c.flowZones.get(flowZoneId(a[0],), f["project-key"] as string | undefined,);
			},
			usage: "dss flow-zone get <id> [--project-key KEY]",
			description: "Get a flow zone by id.",
			examples: ["dss flow-zone get ZONE_ID",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const pk = f["project-key"] as string | undefined;
				const name = flowZoneName(f["name"],);
				const payload = {
					name,
					color: flowZoneColor(f["color"],),
					projectKey: pk,
				};
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const list = await c.flowZones.list(pk,);
					const existing = list.find((zone,) => zone.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("flow-zone", existing.id, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "flow-zone",
							name,
							payload,
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.flowZones.create(payload,);
				return { created: created.id, resource: "flow-zone", ...created, };
			},
			usage:
				"dss flow-zone create --name NAME [--color #RRGGBB] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a flow zone.",
			examples: [
				"dss flow-zone create --name Exports",
				"dss flow-zone create --name Exports --color '#2ab1ac' --dry-run",
			],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss flow-zone update <id> [--name NAME] [--color #RRGGBB]",);
				if (typeof f["name"] !== "string" && typeof f["color"] !== "string") {
					throw new UsageError("--name and/or --color is required.",);
				}
				const zoneId = flowZoneId(a[0],);
				const pk = f["project-key"] as string | undefined;
				const patch = {
					name: typeof f["name"] === "string" ? flowZoneName(f["name"],) : undefined,
					color: flowZoneColor(f["color"],),
					projectKey: pk,
				};
				if (f["dry-run"] === true) {
					const current = await c.flowZones.get(zoneId, pk,);
					const next = deepMerge(current as unknown as Record<string, unknown>, patch,);
					return { dryRun: true, action: "update", resource: "flow-zone", id: zoneId, current, next, };
				}
				return c.flowZones.update(zoneId, patch,);
			},
			usage:
				"dss flow-zone update <id> [--name NAME] [--color #RRGGBB] [--dry-run] [--project-key KEY]",
			description: "Update flow zone settings.",
			examples: ["dss flow-zone update ZONE_ID --name Exports --color '#2ab1ac' --dry-run",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss flow-zone delete <id>",);
				const zoneId = flowZoneId(a[0],);
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() =>
						c.flowZones.get(zoneId, f["project-key"] as string | undefined,)
					);
					if (!current) return skipResult("flow-zone", zoneId, "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "flow-zone", id: zoneId, current, };
					}
				}
				await c.flowZones.delete(zoneId, f["project-key"] as string | undefined,);
				return { deleted: zoneId, resource: "flow-zone", };
			},
			usage: "dss flow-zone delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a flow zone. DSS moves zone items back to the default zone.",
			examples: [
				"dss flow-zone delete ZONE_ID --dry-run",
				"dss flow-zone delete ZONE_ID --if-exists",
			],
		},
		move: {
			handler: async (c, a, f,) => {
				const pk = f["project-key"] as string | undefined;
				const zoneId = a[0] ? flowZoneId(a[0],) : await resolveFlowZoneIdFromFlags(c, f, pk,);
				if (!zoneId) {
					throw new UsageError(
						"A zone id or --zone/--zone-id is required. Usage: dss flow-zone move <id> [--dataset DS] [--recipe R] [--folder F] [--object TYPE:ID]",
					);
				}
				const items = flowZoneMoveItems(f,);
				if (items.length === 0) {
					throw new UsageError(
						"At least one object is required. Use --dataset, --recipe, --folder, or --object TYPE:ID.",
					);
				}

				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "move",
						resource: "flow-zone",
						id: zoneId,
						items,
					};
				}
				return c.flowZones.moveItems(zoneId, items, pk,);
			},
			usage:
				"dss flow-zone move [id] [--zone ZONE|--zone-id ID] [--dataset DS[,DS2]] [--recipe R] [--folder F] [--object TYPE:ID] [--dry-run] [--project-key KEY]",
			description:
				"Move datasets, recipes, managed folders, or other flow objects into a zone by id or --zone name.",
			examples: [
				"dss flow-zone move ZONE_ID --dataset orders --dry-run",
				"dss flow-zone move --zone ATH_SNW_MAP_FRG49 --dataset raw_orders,clean_orders --recipe prepare_orders",
				"dss flow-zone move ZONE_ID --folder FOLDER_ID",
				"dss flow-zone move ZONE_ID --object SAVED_MODEL:model_id",
			],
		},
		graph: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss flow-zone graph <id>",);
				return c.flowZones.graph(flowZoneId(a[0],), f["project-key"] as string | undefined,);
			},
			usage: "dss flow-zone graph <id> [--project-key KEY]",
			description: "Get the graph for a single flow zone.",
			examples: ["dss flow-zone graph ZONE_ID",],
		},
	},

	dataset: {
		list: {
			handler: (c, _a, f,) => c.datasets.list(f["project-key"] as string | undefined,),
			usage: "dss dataset list [--project-key KEY]",
			description: "List all datasets in a project.",
			examples: ["dss dataset list", "dss dataset list --project-key MYPROJ",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset get <name>",);
				return c.datasets.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset get <name> [--project-key KEY]",
			description: "Get full settings for a dataset.",
			examples: ["dss dataset get orders", "dss dataset get orders --project-key MYPROJ",],
		},
		schema: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset schema <name>",);
				return c.datasets.schema(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset schema <name> [--project-key KEY]",
			description: "Show the column schema of a dataset.",
			examples: ["dss dataset schema orders",],
		},
		source: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset source <name>",);
				return datasetSourceSummary(
					await c.datasets.get(a[0], f["project-key"] as string | undefined,),
				);
			},
			usage: "dss dataset source <name> [--project-key KEY]",
			description: "Show backing connection, catalog/schema/table, path, and format for a dataset.",
			examples: ["dss dataset source orders",],
		},
		"refresh-schema": {
			handler: async (c, a, f,) => {
				const usage =
					"dss dataset refresh-schema <name> [--data JSON | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]";
				requireArgs(a, 1, usage,);
				const columns = schemaColumnsInput(f, usage,);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.datasets.schema(a[0], pk,);
					return {
						dryRun: true,
						action: "refresh-schema",
						resource: "dataset",
						name: a[0],
						current,
						next: { columns, },
					};
				}
				await c.datasets.updateSchema(a[0], columns, pk,);
				return { updated: a[0], resource: "dataset", schema: { columns, }, };
			},
			usage:
				"dss dataset refresh-schema <name> [--data JSON | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description: "Replace a dataset schema through the DSS schema endpoint.",
			examples: [
				`dss dataset refresh-schema orders --data '{"columns":[{"name":"id","type":"bigint"}]}' --dry-run`,
			],
		},
		"validate-build": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset validate-build <name>",);
				return c.datasets.validateBuildSettings(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset validate-build <name> [--project-key KEY]",
			description: "Check common dataset settings that can make file-backed builds fail.",
			examples: ["dss dataset validate-build orders",],
		},
		preview: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset preview <name>",);
				return c.datasets.preview(a[0], {
					maxRows: num(f["max-rows"],),
					projectKey: f["project-key"] as string | undefined,
					timeoutMs: num(f["timeout"],),
				},);
			},
			usage: "dss dataset preview <name> [--max-rows N] [--project-key KEY] [--timeout MS]",
			description: "Preview dataset rows.",
			examples: ["dss dataset preview orders", "dss dataset preview orders --max-rows 5",],
		},
		metadata: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset metadata <name>",);
				return c.datasets.metadata(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset metadata <name> [--project-key KEY]",
			description: "Get dataset-level metadata.",
			examples: ["dss dataset metadata orders",],
		},
		download: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset download <name>",);
				return c.datasets.download(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss dataset download <name> [--output PATH] [--project-key KEY]",
			description: "Download dataset contents as CSV.",
			examples: ["dss dataset download orders", "dss dataset download orders --output ./data/",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const pk = f["project-key"] as string | undefined;
				const name = f["name"] as string | undefined;
				const connection = f["connection"] as string | undefined;
				const dsType = f["type"] as string | undefined;
				if (!name || !connection || !dsType) {
					throw new UsageError(
						"--name, --connection, and --type are required. Usage: dss dataset create --name NAME --connection CONN --type TYPE",
					);
				}
				const payload = {
					datasetName: name,
					connection,
					dsType,
					projectKey: pk,
				};
				const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const list = await c.datasets.list(pk,);
					const existing = list.find((d,) => d.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("dataset", name, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "dataset",
							name,
							payload,
							...(existing ? { current: existing, } : {}),
							...(zoneId ? { zoneId, zoneMove: [{ objectId: name, objectType: "DATASET", },], } : {}),
						};
					}
				}
				await c.datasets.create(payload,);
				const moved = await moveCreatedItemsToZone(
					c,
					f,
					[{ objectId: name, objectType: "DATASET", },],
					pk,
				);
				return { created: name, resource: "dataset", ...moved, };
			},
			usage:
				"dss dataset create --name NAME --connection CONN --type TYPE [--zone ZONE|--zone-id ID] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a new dataset.",
			examples: [
				"dss dataset create --name orders --connection filesystem --type Filesystem",
				"dss dataset create --name orders --connection filesystem --type Filesystem --zone Experiments --dry-run",
			],
		},
		clone: {
			handler: async (c, a, f,) => {
				const usage =
					"dss dataset clone <source> <target> [--path PATH] [--table TABLE] [--metastore-table TABLE] [--allow-same-path] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]";
				requireArgs(a, 2, usage,);
				const pk = f["project-key"] as string | undefined;
				const opts = {
					projectKey: pk,
					path: f["path"] as string | undefined,
					table: f["table"] as string | undefined,
					metastoreTableName: f["metastore-table"] as string | undefined,
					allowSamePath: f["allow-same-path"] === true,
				};
				const current = await c.datasets.get(a[0], pk,);
				const next = buildDatasetCloneSettings(current, a[1], pk ?? c.resolveProjectKey(pk,), opts,);
				const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "clone",
						resource: "dataset",
						source: a[0],
						target: a[1],
						current,
						next,
						...(zoneId ? { zoneId, zoneMove: [{ objectId: a[1], objectType: "DATASET", },], } : {}),
					};
				}
				const cloned = await c.datasets.clone(a[0], a[1], opts,);
				const moved = await moveCreatedItemsToZone(
					c,
					f,
					[{ objectId: a[1], objectType: "DATASET", },],
					pk,
				);
				return { ...cloned, resource: "dataset", ...moved, };
			},
			usage:
				"dss dataset clone <source> <target> [--path PATH] [--table TABLE] [--metastore-table TABLE] [--allow-same-path] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]",
			description: "Clone dataset settings into a new dataset, with storage/table overrides.",
			examples: [
				"dss dataset clone source_ds experiment_ds --path /dataiku/TEST/experiment_ds --dry-run",
				"dss dataset clone source_ds experiment_ds --allow-same-path",
			],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset delete <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.datasets.get(a[0], pk,));
					if (!current) return skipResult("dataset", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "dataset", name: a[0], current, };
					}
				}
				await c.datasets.delete(a[0], pk,);
				return { deleted: a[0], resource: "dataset", };
			},
			usage: "dss dataset delete <name> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a dataset.",
			examples: ["dss dataset delete orders", "dss dataset delete orders --if-exists",],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.datasets.get(a[0], pk,);
					const next = deepMerge(current as unknown as Record<string, unknown>, data,);
					return { dryRun: true, action: "update", resource: "dataset", name: a[0], current, next, };
				}
				await c.datasets.update(a[0], data, pk,);
				return { updated: a[0], resource: "dataset", };
			},
			usage:
				"dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description: "Update dataset settings via JSON merge.",
			examples: [
				'dss dataset update orders --data \'{"tags":["production"]}\' --dry-run',
				"echo '{\"tags\":[]}' | dss dataset update orders --stdin",
			],
		},
	},

	recipe: {
		list: {
			handler: (c, _a, f,) => c.recipes.list(f["project-key"] as string | undefined,),
			usage: "dss recipe list [--project-key KEY]",
			description: "List all recipes in a project.",
			examples: ["dss recipe list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe get <name>",);
				return c.recipes.get(a[0], {
					includePayload: f["include-payload"] === true && f["no-payload"] !== true,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe get <name> [--include-payload|--no-payload] [--project-key KEY]",
			description: "Get compact recipe settings unless --include-payload is set.",
			examples: [
				"dss recipe get compute_orders",
				"dss recipe get compute_orders --no-payload",
				"dss recipe get compute_orders --include-payload",
			],
		},
		"validate-graph": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe validate-graph <name>",);
				return c.recipes.validateGraph(a[0], {
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe validate-graph <name> [--project-key KEY]",
			description: "Validate declared recipe input/output graph references before building.",
			examples: ["dss recipe validate-graph compute_orders",],
		},
		run: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe run <name>",);
				const pk = f["project-key"] as string | undefined;
				const wait = recipeRunShouldWait(f,);
				const options = {
					buildMode: f["build-mode"] as BuildMode | undefined,
					includeLogs: f["include-logs"] === true,
					logFilter: jobLogFilterFromFlag(f["log-filter"],),
					maxLogLines: maxLogLinesFromFlags(f,),
					partition: f["partition"] as string | undefined,
					pollIntervalMs: num(f["poll-interval"],),
					projectKey: pk,
					timeoutMs: num(f["timeout"],),
					summary: f["summary"] === true,
					wait,
				};
				if (f["dry-run"] === true) {
					const outputs = await c.recipes.resolveRunOutputs(a[0], {
						partition: options.partition,
						projectKey: pk,
					},);
					return {
						dryRun: true,
						action: "run",
						resource: "recipe",
						recipe: a[0],
						outputs,
						...options,
						endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
						method: "POST",
					};
				}
				return c.recipes.run(a[0], options,);
			},
			usage:
				"dss recipe run <name> [--wait|--no-wait] [--build-mode MODE] [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
			description:
				"Run a recipe by resolving its outputs and submitting the correct dataset or managed-folder build job.",
			examples: [
				"dss recipe run compute_orders --wait",
				"dss recipe run compute_exports --include-logs --log-filter stdout --summary --timeout 600000",
				"dss recipe run compute_exports --dry-run",
			],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe delete <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() =>
						c.recipes.get(a[0], { projectKey: pk, includePayload: true, },)
					);
					if (!current) return skipResult("recipe", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "recipe", name: a[0], current, };
					}
				}
				await c.recipes.delete(a[0], pk,);
				return { deleted: a[0], resource: "recipe", };
			},
			usage: "dss recipe delete <name> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a recipe.",
			examples: ["dss recipe delete compute_orders", "dss recipe delete compute_orders --if-exists",],
		},
		download: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe download <name>",);
				return c.recipes.download(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe download <name> [--output PATH] [--project-key KEY]",
			description: "Download recipe definition as JSON.",
			examples: [
				"dss recipe download compute_orders",
				"dss recipe download compute_orders -o recipe.json",
			],
		},
		"download-code": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe download-code <name>",);
				return c.recipes.downloadCode(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe download-code <name> [--output PATH] [--project-key KEY]",
			description: "Download the code payload of a recipe.",
			examples: [
				"dss recipe download-code compute_orders",
				"dss recipe download-code compute_orders -o code.py",
			],
		},
		create: {
			handler: async (c, _a, f,) => {
				const type = f["type"] as string;
				if (!type) {
					throw new UsageError(
						"--type is required. Usage: dss recipe create --type TYPE --input DS (--output DS | --output-folder FOLDER_ID)",
					);
				}
				const outputDataset = f["output"] as string | undefined;
				const outputFolder = f["output-folder"] as string | undefined;
				if (outputDataset && outputFolder) {
					throw new UsageError("--output and --output-folder are mutually exclusive.",);
				}
				if (!outputDataset && !outputFolder) {
					throw new UsageError(
						"--output or --output-folder is required. Usage: dss recipe create --type TYPE --input DS (--output DS | --output-folder FOLDER_ID)",
					);
				}
				if (outputFolder && !f["output-connection"]) {
					throw new UsageError("--output-connection is required when using --output-folder.",);
				}
				const name = f["name"] as string | undefined;
				const pk = f["project-key"] as string | undefined;
				const inputDatasets = recipeInputDatasetsFromFlags(f,);
				const payload = {
					type,
					name,
					inputDatasets,
					outputDataset,
					outputFolder,
					outputConnection: f["output-connection"] as string | undefined,
					projectKey: pk,
				};
				const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
				const zoneMove = zoneId && name
					? [{ objectId: name, objectType: "RECIPE" as const, },]
					: undefined;
				if ((f["if-not-exists"] === true || f["dry-run"] === true) && name) {
					const list = await c.recipes.list(pk,);
					const existing = list.find((r,) => r.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("recipe", name, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "recipe",
							name,
							payload,
							...(zoneId ? { zoneId, zoneMove, } : {}),
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "recipe",
						payload,
						...(zoneId ? { zoneId, zoneMove, } : {}),
					};
				}
				const created = await c.recipes.create(payload,);
				const createdName = created.recipeName;
				const moved = await moveCreatedItemsToZone(c, f, [{
					objectId: createdName,
					objectType: "RECIPE",
				},], pk,);
				return { created: createdName, resource: "recipe", ...created, ...moved, };
			},
			usage:
				"dss recipe create --type TYPE --input DS[,DS2] (--output DS | --output-folder FOLDER_ID) [--name NAME] [--output-connection CONN] [--zone ZONE|--zone-id ID] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a recipe with one or more inputs and a dataset or managed-folder output.",
			examples: [
				"dss recipe create --type python --input raw_orders,lookup --output orders_clean",
				"dss recipe create --type python --input orders --input customers --output orders_clean --zone Experiments",
				"dss recipe create --type python --input orders --output-folder LT7TUHJ8 --output-connection filesystem --dry-run",
			],
		},
		clone: {
			handler: async (c, a, f,) => {
				const usage =
					"dss recipe clone [source|--from SOURCE] (--name NAME|--to NAME) [--replace-input FROM=TO] [--replace-output FROM=TO] [--replace-payload-text FROM=TO] [--output DATASET] [--copy-output-settings] [--path PATH] [--metastore-table TABLE] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]";
				const fromFlag = typeof f["from"] === "string" ? f["from"].trim() : "";
				const sourceName = a[0] ?? fromFlag;
				if (!sourceName) {
					throw new UsageError(`Source recipe is required. Usage: ${usage}`, "missing_required_flag",);
				}
				if (a[0] && fromFlag && a[0] !== fromFlag) {
					throw new UsageError(
						"Positional source and --from must match when both are provided.",
						"invalid_enum",
					);
				}
				const pk = f["project-key"] as string | undefined;
				const toFlag = typeof f["to"] === "string" ? f["to"].trim() : "";
				const nameFlag = typeof f["name"] === "string" ? f["name"].trim() : "";
				const name = toFlag || nameFlag;
				if (!name) {
					throw new UsageError(`--name or --to is required. Usage: ${usage}`, "missing_required_flag",);
				}
				const inputRewrites = rewritePairsFromFlags(f, "replace-input",);
				const outputRewrites = rewritePairsFromFlags(f, "replace-output",);
				const payloadTextRewrites = rewritePairsFromFlags(f, "replace-payload-text",);
				const opts = {
					projectKey: pk,
					name,
					outputDataset: f["output"] as string | undefined,
					outputRewrites,
					inputRewrites,
					payloadTextRewrites,
					copyOutputSettings: f["copy-output-settings"] === true,
					outputPath: f["path"] as string | undefined,
					metastoreTableName: f["metastore-table"] as string | undefined,
				};
				const source = await c.recipes.get(sourceName, { includePayload: true, projectKey: pk, },);
				const outputItems = Object.values(
					(source.recipe.outputs ?? {}) as Record<
						string,
						{ items?: Array<{ ref?: string; type?: string; }>; }
					>,
				).flatMap((role,) => role.items ?? []).filter((item,) => typeof item.ref === "string");
				const plannedOutputRewrites = { ...outputRewrites, };
				if (opts.outputDataset !== undefined && outputItems.length === 1) {
					plannedOutputRewrites[outputItems[0]!.ref!] = opts.outputDataset;
				}
				if (
					opts.copyOutputSettings === true
					&& Object.keys(plannedOutputRewrites,).length > 1
					&& (opts.outputPath !== undefined || opts.metastoreTableName !== undefined)
				) {
					throw new UsageError(
						"Cannot reuse --path or --metastore-table for multiple cloned output datasets.",
						"invalid_enum",
					);
				}
				const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "clone",
						resource: "recipe",
						source: sourceName,
						target: name,
						inputRewrites,
						outputRewrites: plannedOutputRewrites,
						copyOutputSettings: opts.copyOutputSettings,
						payloadTextRewrites,
						current: source,
						...(zoneId ? { zoneId, zoneMove: [{ objectId: name, objectType: "RECIPE", },], } : {}),
					};
				}
				const cloned = await c.recipes.clone(sourceName, opts,);
				const moved = await moveCreatedItemsToZone(
					c,
					f,
					[{ objectId: name, objectType: "RECIPE", },],
					pk,
				);
				return { ...cloned, resource: "recipe", ...moved, };
			},
			usage:
				"dss recipe clone [source|--from SOURCE] (--name NAME|--to NAME) [--replace-input FROM=TO] [--replace-output FROM=TO] [--replace-payload-text FROM=TO] [--output DATASET] [--copy-output-settings] [--path PATH] [--metastore-table TABLE] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]",
			description: "Clone a recipe graph/settings/payload into a separate experiment recipe.",
			examples: [
				"dss recipe clone compute_orders --name compute_orders_opt --output orders_opt --copy-output-settings --dry-run",
				"dss recipe clone compute_orders --name compute_orders_opt --output orders_opt --zone Experiments",
			],
		},
		diff: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe diff <name> --file PATH",);
				const filePath = f["file"] as string | undefined;
				if (!filePath) {
					throw new UsageError("--file is required. Usage: dss recipe diff <name> --file PATH",);
				}
				const result = await c.recipes.get(a[0], {
					includePayload: true,
					projectKey: f["project-key"] as string | undefined,
				},);
				if (!result.payload) {
					throw new Error(`Recipe "${a[0]}" has no code payload to diff.`,);
				}
				const localContent = readFileSync(filePath, "utf-8",);
				return formatLineDiff(a[0], filePath, result.payload, localContent,);
			},
			usage: "dss recipe diff <name> --file PATH [--project-key KEY]",
			description: "Show differences between local file and remote recipe code.",
			examples: ["dss recipe diff compute_orders --file code.py",],
		},

		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.recipes.get(a[0], { projectKey: pk, includePayload: true, },);
					const currentRecipe = current.recipe as Record<string, unknown>;
					const next = {
						...current,
						...data,
						recipe: deepMerge(
							currentRecipe,
							(data.recipe && typeof data.recipe === "object" && !Array.isArray(data.recipe,))
								? data.recipe as Record<string, unknown>
								: {},
						),
					};
					return { dryRun: true, action: "update", resource: "recipe", name: a[0], current, next, };
				}
				await c.recipes.update(a[0], data, pk,);
				return { updated: a[0], resource: "recipe", };
			},
			usage:
				"dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description:
				"Update recipe settings via JSON merge. Recipe definition fields must be nested under a top-level recipe key.",
			examples: [
				"dss recipe update compute_orders --data-file settings.json --dry-run",
				'dss recipe update compute_orders --data \'{"recipe":{"params":{"envSelection":{"envMode":"EXPLICIT_ENV","envName":"python39"}}}}\'',
				"cat settings.json | dss recipe update compute_orders --stdin",
			],
		},
		"get-payload": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe get-payload <name>",);
				const payload = await c.recipes.getPayload(a[0], {
					projectKey: f["project-key"] as string | undefined,
				},);
				if (typeof f["output"] === "string") {
					await writeFile(f["output"], payload, "utf-8",);
					return f["output"];
				}
				return payload;
			},
			usage: "dss recipe get-payload <name> [--raw] [--output PATH] [--project-key KEY]",
			description: "Print the recipe code payload to stdout; use --raw for pipeable code bytes.",
			examples: [
				"dss recipe get-payload compute_orders --raw",
				"dss recipe get-payload compute_orders -o code.py",
			],
		},
		cat: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe cat <name> [--raw]",);
				return c.recipes.getPayload(a[0], {
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe cat <name> [--raw] [--project-key KEY]",
			description: "Print a recipe code payload; combine with --raw for shell pipes and diffs.",
			examples: ["dss recipe cat compute_orders --raw",],
		},
		"set-payload": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe set-payload <name> --file PATH",);
				const filePath = f["file"] as string;
				if (!filePath) throw new UsageError("--file is required.",);
				const content = readFileSync(filePath, "utf-8",);
				const pk = f["project-key"] as string | undefined;
				const shouldBackup = f["no-backup"] !== true;
				const backupDir = shouldBackup
					? (f["backup-dir"] as string | undefined) ?? join(process.cwd(), ".dss-backups", "recipes",)
					: undefined;
				const backupPath = backupDir ? recipeBackupPath(a[0], backupDir,) : undefined;
				const current = await c.recipes.get(a[0], {
					projectKey: pk,
					includePayload: true,
				},);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "set-payload",
						resource: "recipe",
						name: a[0],
						file: filePath,
						current,
						next: { ...current, payload: content, },
						...(backupPath ? { backupPath, backup: recipeBackupDocument(a[0], pk, current,), } : {}),
					};
				}
				if (backupDir && backupPath) {
					await mkdir(backupDir, { recursive: true, },);
					await writeFile(
						backupPath,
						`${JSON.stringify(recipeBackupDocument(a[0], pk, current,), null, 2,)}\n`,
						"utf-8",
					);
				}
				await c.recipes.replace(a[0], { ...current, payload: content, }, pk,);
				return {
					updated: a[0],
					resource: "recipe",
					file: filePath,
					backupCreated: backupPath !== undefined,
					...(backupPath ? { backupPath, } : {}),
				};
			},
			usage:
				"dss recipe set-payload <name> --file PATH [--backup-dir DIR|--no-backup] [--dry-run] [--project-key KEY]",
			description:
				"Upload recipe code from a local file, backing up payload, graph, settings, and version metadata by default.",
			examples: [
				"dss recipe set-payload compute_orders --file code.py --dry-run",
				"dss recipe set-payload compute_orders --file code.py --backup-dir ./backups",
				"dss recipe set-payload compute_orders --file code.py --no-backup",
			],
		},
		restore: {
			handler: async (c, a, f,) => {
				const usage =
					"dss recipe restore <name> --backup FILE [--payload-only] [--dry-run] [--project-key KEY]";
				requireArgs(a, 1, usage,);
				const backupPath = requiredStringFlag(f, "backup", usage,);
				const backup = readRecipeBackup(backupPath,);
				const payload = typeof backup.payload === "string" ? backup.payload : "";
				const pk = f["project-key"] as string | undefined;
				const current = await c.recipes.get(a[0], { includePayload: true, projectKey: pk, },);
				const backupRecipe =
					backup.recipe && typeof backup.recipe === "object" && !Array.isArray(backup.recipe,)
						? backup.recipe as Record<string, unknown>
						: undefined;
				const restoredRecipe = backupRecipe
					? { ...backupRecipe, name: a[0], ...(pk ? { projectKey: pk, } : {}), }
					: undefined;
				const next = f["payload-only"] === true || !restoredRecipe
					? { ...current, payload, }
					: { ...current, recipe: restoredRecipe, payload, };
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "restore",
						resource: "recipe",
						name: a[0],
						backupPath,
						current,
						next,
					};
				}
				await c.recipes.replace(a[0], next as Record<string, unknown>, pk,);
				return {
					restored: a[0],
					resource: "recipe",
					backupPath,
					payloadOnly: f["payload-only"] === true,
				};
			},
			usage:
				"dss recipe restore <name> --backup FILE [--payload-only] [--dry-run] [--project-key KEY]",
			description: "Restore a recipe from a set-payload backup.",
			examples: [
				"dss recipe restore compute_orders --backup .dss-backups/recipes/backup.recipe-backup.json --dry-run",
			],
		},
		"assert-unchanged": {
			handler: async (c, a, f,) => {
				const usage = "dss recipe assert-unchanged <name> --since BACKUP [--project-key KEY]";
				requireArgs(a, 1, usage,);
				const backupPath = requiredStringFlag(f, "since", usage,);
				const backup = readRecipeBackup(backupPath,);
				const current = await c.recipes.get(a[0], {
					includePayload: true,
					projectKey: f["project-key"] as string | undefined,
				},);
				const payloadHash = sha256Hex(current.payload ?? "",);
				const normalizedPayloadHash = sha256Hex(normalizeLineEndings(current.payload ?? "",),);
				const expectedPayloadHash = typeof backup.payloadHash === "string"
					? backup.payloadHash
					: undefined;
				const expectedNormalizedPayloadHash = typeof backup.normalizedPayloadHash === "string"
					? backup.normalizedPayloadHash
					: typeof backup.payload === "string"
					? sha256Hex(normalizeLineEndings(backup.payload,),)
					: undefined;
				const checks = [
					{
						name: "payload",
						expected: expectedPayloadHash,
						actual: payloadHash,
						unchanged: expectedPayloadHash === payloadHash
							|| (
								expectedNormalizedPayloadHash !== undefined
								&& expectedNormalizedPayloadHash === normalizedPayloadHash
							),
						normalizedExpected: expectedNormalizedPayloadHash,
						normalizedActual: normalizedPayloadHash,
					},
					{
						name: "graph",
						expected: backup.graphHash,
						actual: stableHash(recipeGraph(current.recipe,),),
						unchanged: backup.graphHash === stableHash(recipeGraph(current.recipe,),),
					},
					{
						name: "codeEnv",
						expected: backup.codeEnvHash,
						actual: stableHash(recipeCodeEnv(current.recipe,),),
						unchanged: backup.codeEnvHash === stableHash(recipeCodeEnv(current.recipe,),),
					},
				].filter((check,) => typeof check.expected === "string");
				const failures = checks.filter((check,) => !check.unchanged);
				return {
					unchanged: failures.length === 0,
					resource: "recipe",
					name: a[0],
					backupPath,
					checks,
					failures,
				};
			},
			usage: "dss recipe assert-unchanged <name> --since BACKUP [--project-key KEY]",
			description: "Compare current recipe payload, graph, and code env against a backup.",
			examples: [
				"dss recipe assert-unchanged compute_orders --since .dss-backups/recipes/backup.recipe-backup.json",
			],
		},
	},

	job: {
		list: {
			handler: async (c, _a, f,) =>
				filteredJobList(await c.jobs.list(f["project-key"] as string | undefined,), f,),
			usage:
				"dss job list [--state STATE] [--contains TEXT] [--output ID] [--latest] [--limit N] [--project-key KEY]",
			description: "List recent jobs, optionally filtered for automation.",
			examples: ["dss job list --state DONE --latest", "dss job list --contains WLM225S --limit 10",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job get <id>",);
				return c.jobs.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss job get <id> [--project-key KEY]",
			description: "Get job details.",
			examples: ["dss job get JOB_ID",],
		},
		summary: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job summary <id>",);
				return jobInspectionSummary(c, a[0], f,);
			},
			usage:
				"dss job summary <id> [--activity ACTIVITY_ID] [--log-id LOG_ID] [--max-lines N|--max-log-lines N] [--project-key KEY]",
			description: "Summarize job state, outputs, warnings, progress, and useful terminal log lines.",
			examples: ["dss job summary JOB_ID --max-log-lines 200",],
		},
		log: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job log <id>",);
				return c.jobs.log(a[0], {
					activity: f["activity"] as string | undefined,
					logId: f["log-id"] as string | undefined,
					maxLogLines: maxLogLinesFromFlags(f,),
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss job log <id> [--activity ACTIVITY_ID] [--log-id LOG_ID] [--max-lines N|--max-log-lines N] [--project-key KEY]",
			description:
				"Get public API job log output. --log-id is accepted for UI parity but DSS API-key auth cannot select browser-only cat-activity-log files.",
			examples: [
				"dss job log JOB_ID",
				"dss job log JOB_ID --activity main --max-log-lines 200",
			],
		},
		"log-url": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job log-url <url>",);
				return c.jobs.logFromUrl(a[0], { maxLogLines: maxLogLinesFromFlags(f,), },);
			},
			usage: "dss job log-url <url> [--max-lines N|--max-log-lines N]",
			description: "Fetch a DSS cat-activity-log URL pasted from the UI.",
			examples: [
				'dss job log-url "https://dss/dip/api/flow/jobs/cat-activity-log?projectKey=TEST&jobId=JOB&activityId=A&logId=L"',
			],
		},
		build: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job build <target>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					buildMode: f["build-mode"] as BuildMode | undefined,
					partition: f["partition"] as string | undefined,
					pollIntervalMs: num(f["poll-interval"],),
					targetType: jobBuildTargetTypeFromFlags(f,),
					timeoutMs: num(f["timeout"],),
				};
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "build",
						resource: "job",
						target: a[0],
						...options,
						endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
						method: "POST",
					};
				}
				if (f["wait"] === true) {
					return c.jobs.buildAndWait(a[0], { ...options, projectKey: pk, },);
				}
				return c.jobs.build(a[0], { ...options, projectKey: pk, },);
			},
			usage:
				"dss job build <target> [--target-type dataset|managed-folder] [--type DATASET|MANAGED_FOLDER] [--build-mode MODE] [--wait] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
			description: "Start a dataset or managed-folder build, optionally waiting for completion.",
			examples: [
				"dss job build orders",
				"dss job build orders --build-mode RECURSIVE_BUILD --wait",
				"dss job build LT7TUHJ8 --target-type managed-folder --dry-run",
			],
		},
		"build-and-wait": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job build-and-wait <target>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					buildMode: f["build-mode"] as BuildMode | undefined,
					includeLogs: f["include-logs"] === true,
					logFilter: jobLogFilterFromFlag(f["log-filter"],),
					maxLogLines: maxLogLinesFromFlags(f,),
					partition: f["partition"] as string | undefined,
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
					summary: f["summary"] === true,
					targetType: jobBuildTargetTypeFromFlags(f,),
				};
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "build-and-wait",
						resource: "job",
						target: a[0],
						...options,
						endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
						method: "POST",
					};
				}
				return c.jobs.buildAndWait(a[0], { ...options, projectKey: pk, },);
			},
			usage:
				"dss job build-and-wait <target> [--target-type dataset|managed-folder] [--type DATASET|MANAGED_FOLDER] [--build-mode MODE] [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
			description: "Build a dataset or managed folder and wait for completion.",
			examples: [
				"dss job build-and-wait orders",
				"dss job build-and-wait orders --include-logs --log-filter stdout --summary",
				"dss job build-and-wait orders --timeout 300000",
				"dss job build-and-wait LT7TUHJ8 --target-type managed-folder --dry-run",
			],
		},
		wait: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job wait <id>",);
				return c.jobs.wait(a[0], {
					includeLogs: f["include-logs"] === true,
					logFilter: jobLogFilterFromFlag(f["log-filter"],),
					maxLogLines: maxLogLinesFromFlags(f,),
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
					summary: f["summary"] === true,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss job wait <id> [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS]",
			description: "Wait for an existing job to complete.",
			examples: [
				"dss job wait JOB_ID",
				"dss job wait JOB_ID --include-logs --log-filter stdout --summary --timeout 60000",
			],
		},
		monitor: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job monitor <id...>",);
				const options = {
					includeLogs: f["include-logs"] === true,
					logFilter: jobLogFilterFromFlag(f["log-filter"],),
					maxLogLines: maxLogLinesFromFlags(f,),
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
					summary: f["summary"] !== false,
					projectKey: f["project-key"] as string | undefined,
				};
				const jobs = await Promise.all(a.map((jobId,) => c.jobs.wait(jobId, options,)),);
				return a.length === 1 ? jobs[0] : { jobs, until: f["until"] ?? "all-done", };
			},
			usage:
				"dss job monitor <id...> [--summary] [--include-logs] [--log-filter stdout|stderr|user|errors] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--until all-done] [--project-key KEY]",
			description: "Monitor one or more existing jobs and summarize progress counters from logs.",
			examples: ["dss job monitor JOB_ID --summary", "dss job monitor JOB1 JOB2 --until all-done",],
		},
		watch: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job watch <id...>",);
				const options = {
					includeLogs: f["include-logs"] === true,
					logFilter: jobLogFilterFromFlag(f["log-filter"],),
					maxLogLines: maxLogLinesFromFlags(f,),
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
					summary: true,
					projectKey: f["project-key"] as string | undefined,
				};
				const jobs = await Promise.all(a.map((jobId,) => c.jobs.wait(jobId, options,)),);
				return a.length === 1 ? jobs[0] : { jobs, until: f["until"] ?? "all-done", };
			},
			usage:
				"dss job watch <id...> [--include-logs] [--log-filter stdout|stderr|user|errors] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--until all-done] [--project-key KEY]",
			description: "Watch one or more existing jobs with progress extraction enabled.",
			examples: ["dss job watch JOB_ID", "dss job watch JOB1 JOB2 --until all-done",],
		},
		abort: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job abort <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "abort",
						resource: "job",
						id: a[0],
						endpoint: encodedProjectEndpoint(c, pk, `/jobs/${encodeURIComponent(a[0],)}/abort/`,),
						method: "POST",
					};
				}
				await c.jobs.abort(a[0], pk,);
				return { aborted: a[0], resource: "job", };
			},
			usage: "dss job abort <id> [--dry-run] [--project-key KEY]",
			description: "Abort a running job.",
			examples: ["dss job abort JOB_ID", "dss job abort JOB_ID --dry-run",],
		},
	},

	scenario: {
		list: {
			handler: (c, _a, f,) => c.scenarios.list(f["project-key"] as string | undefined,),
			usage: "dss scenario list [--project-key KEY]",
			description: "List all scenarios in a project.",
			examples: ["dss scenario list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario get <id>",);
				return c.scenarios.get(a[0], { projectKey: f["project-key"] as string | undefined, },);
			},
			usage: "dss scenario get <id> [--project-key KEY]",
			description:
				"Get raw scenario definition. For step-based scenario edits, patch params.steps; rawParams.params is DSS echo data.",
			examples: ["dss scenario get my_scenario",],
		},
		run: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario run <id>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
				};
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "run",
						resource: "scenario",
						id: a[0],
						...options,
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/scenarios/${encodeURIComponent(a[0],)}/run/`,
						),
						method: "POST",
					};
				}
				if (f["wait"] === true) {
					return c.scenarios.runAndWait(a[0], { ...options, projectKey: pk, },);
				}
				return c.scenarios.run(a[0], pk,);
			},
			usage:
				"dss scenario run <id> [--wait] [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
			description: "Trigger a scenario run, optionally waiting for completion.",
			examples: ["dss scenario run my_scenario", "dss scenario run my_scenario --wait",],
		},
		"run-and-wait": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario run-and-wait <id>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					pollIntervalMs: num(f["poll-interval"],),
					timeoutMs: num(f["timeout"],),
				};
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "run-and-wait",
						resource: "scenario",
						id: a[0],
						...options,
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/scenarios/${encodeURIComponent(a[0],)}/run/`,
						),
						method: "POST",
					};
				}
				return c.scenarios.runAndWait(a[0], { ...options, projectKey: pk, },);
			},
			usage:
				"dss scenario run-and-wait <id> [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
			description: "Run a scenario and wait for completion.",
			examples: [
				"dss scenario run-and-wait my_scenario",
				"dss scenario run-and-wait my_scenario --timeout 300000",
			],
		},
		status: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario status <id>",);
				return c.scenarios.status(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario status <id> [--project-key KEY]",
			description: "Get the current run status of a scenario.",
			examples: ["dss scenario status my_scenario",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario delete <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.scenarios.get(a[0], { projectKey: pk, },));
					if (!current) return skipResult("scenario", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "scenario", id: a[0], current, };
					}
				}
				await c.scenarios.delete(a[0], pk,);
				return { deleted: a[0], resource: "scenario", };
			},
			usage: "dss scenario delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a scenario.",
			examples: ["dss scenario delete my_scenario", "dss scenario delete my_scenario --if-exists",],
		},
		create: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss scenario create <id> <name>",);
				const pk = f["project-key"] as string | undefined;
				const payload = {
					scenarioId: a[0],
					name: a[1],
					scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
					projectKey: pk,
				};
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const list = await c.scenarios.list(pk,);
					const existing = list.find((s,) => s.id === a[0]);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("scenario", a[0], "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "scenario",
							id: a[0],
							payload,
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				await c.scenarios.create(a[0], a[1], {
					scenarioType: payload.scenarioType,
					projectKey: pk,
				},);
				return { created: a[0], name: a[1], resource: "scenario", };
			},
			usage:
				"dss scenario create <id> <name> [--type step_based|custom_python] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a new scenario.",
			examples: [
				'dss scenario create my_scenario "My Scenario"',
				'dss scenario create my_scenario "My Scenario" --type custom_python --dry-run',
			],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (data === undefined) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.scenarios.get(a[0], { projectKey: pk, },);
					const preview = scenarioUpdatePreview(current as unknown as Record<string, unknown>, data,);
					return {
						dryRun: true,
						action: "update",
						resource: "scenario",
						id: a[0],
						canonicalEditableFields: preview.canonicalEditableFields,
						normalization: preview.normalization,
						normalizedData: preview.normalizedData,
						changes: preview.changes,
						unchangedPaths: preview.unchangedPaths,
						current: preview.current,
						next: preview.next,
					};
				}
				const result = await c.scenarios.update(a[0], data, pk,);
				return {
					updated: a[0],
					resource: "scenario",
					verified: result.verified,
					changed: result.changes.length > 0,
					canonicalEditableFields: result.canonicalEditableFields,
					normalization: result.normalization,
					...(result.normalization.length > 0 ? { normalizedData: result.normalizedData, } : {}),
					changes: result.changes,
					unchangedPaths: result.unchangedPaths,
				};
			},
			usage:
				"dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description:
				"Update scenario settings via JSON merge; edit step-based scenario steps at params.steps, not rawParams.params.steps.",
			examples: [
				'dss scenario update my_scenario --data \'{"params":{"steps":[]}}\' --dry-run',
				"dss scenario update my_scenario --data-file settings.json --dry-run",
			],
		},
	},

	folder: {
		list: {
			handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
			usage: "dss folder list [--project-key KEY]",
			description: "List managed folders in a project.",
			examples: ["dss folder list",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const name = f["name"] as string | undefined;
				const type = f["type"] as string | undefined;
				const connection = f["connection"] as string | undefined;
				const pk = f["project-key"] as string | undefined;
				if (!name || !type || !connection) {
					throw new UsageError(
						"--name, --type, and --connection are required. Usage: dss folder create --name NAME --type TYPE --connection CONN [--path PATH]",
					);
				}
				const payload = {
					name,
					type,
					connection,
					path: f["path"] as string | undefined,
					projectKey: pk,
				};
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const list = await c.folders.list(pk,);
					const existing = list.find((folder,) => folder.name === name);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("folder", existing.id ?? name, "exists", { current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "folder",
							name,
							payload,
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.folders.create(payload,);
				return { created: created.id ?? name, resource: "folder", ...created, };
			},
			usage:
				"dss folder create --name NAME --type TYPE --connection CONN [--path PATH] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description: "Create a managed folder.",
			examples: [
				"dss folder create --name exports --type S3 --connection s3_connection",
				"dss folder create --name exports --type S3 --connection s3_connection --path /dataiku/MYPROJ/exports --dry-run",
			],
		},
		get: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder get <name-or-id>",);
				return c.folders.get(
					await resolveFolderId(c, a[0], f,),
					f["project-key"] as string | undefined,
				);
			},
			usage: "dss folder get <name-or-id> [--project-key KEY]",
			description: "Get managed folder settings.",
			examples: ["dss folder get my_folder",],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss folder update <name-or-id> [--data '{...}' | --data-file PATH | --stdin]",
				);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss folder update <name-or-id> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				const pk = f["project-key"] as string | undefined;
				const folderId = await resolveFolderId(c, a[0], f,);
				if (f["dry-run"] === true) {
					const current = await c.folders.get(folderId, pk,);
					const next = deepMerge(current as unknown as Record<string, unknown>, data,);
					return {
						dryRun: true,
						action: "update",
						resource: "folder",
						folder: a[0],
						folderId,
						current,
						next,
					};
				}
				await c.folders.update(folderId, data, pk,);
				return { updated: folderId, resource: "folder", };
			},
			usage:
				"dss folder update <name-or-id> [--data JSON | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description: "Update managed folder settings by deep-merging a JSON patch.",
			examples: [
				'dss folder update exports --data \'{"tags":["agent"]}\' --dry-run',
				"dss folder update exports --data-file folder-patch.json",
			],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder delete <name-or-id>",);
				const pk = f["project-key"] as string | undefined;
				const folderId = await resolveFolderId(c, a[0], f,);
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.folders.get(folderId, pk,));
					if (!current) return skipResult("folder", folderId, "missing",);
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "delete",
							resource: "folder",
							folder: a[0],
							folderId,
							current,
						};
					}
				}
				await c.folders.delete(folderId, pk,);
				return { deleted: folderId, resource: "folder", };
			},
			usage: "dss folder delete <name-or-id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a managed folder.",
			examples: ["dss folder delete exports --if-exists",],
		},
		contents: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder contents <name-or-id>",);
				const startedAt = Date.now();
				let folderId = a[0];
				try {
					folderId = await resolveFolderId(c, a[0], f,);
					return await c.folders.contents(folderId, {
						projectKey: f["project-key"] as string | undefined,
					},);
				} catch (error) {
					addTransientTargetContext(error, `folder:${folderId}`, Date.now() - startedAt,);
				}
			},
			usage:
				"dss folder contents <name-or-id> [--retries N] [--request-timeout MS] [--project-key KEY]",
			description: "List files in a managed folder.",
			examples: [
				"dss folder contents my_folder",
				"dss folder contents my_folder --retries 8 --request-timeout 60000",
			],
		},
		download: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder download <name-or-id> <remote-path> [local-path]",);
				const localPath = (a[2] as string | undefined) ?? (f["output"] as string | undefined);
				return c.folders.download(await resolveFolderId(c, a[0], f,), a[1], {
					localPath,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss folder download <name-or-id> <remote-path> [local-path] [--output PATH] [--project-key KEY]",
			description: "Download a file from a managed folder.",
			examples: [
				"dss folder download my_folder /data/report.csv",
				"dss folder download my_folder /data/report.csv ./report.csv",
			],
		},
		upload: {
			handler: async (c, a, f,) => {
				requireArgs(a, 3, "dss folder upload <name-or-id> <path> <localPath>",);
				const pk = f["project-key"] as string | undefined;
				const folderId = await resolveFolderId(c, a[0], f,);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "upload",
						resource: "folder",
						folder: a[0],
						folderId,
						path: a[1],
						localPath: a[2],
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/managedfolders/${encodeURIComponent(folderId,)}/contents/${encodeURIComponent(a[1],)}`,
						),
						method: "POST",
					};
				}
				await c.folders.upload(folderId, a[1], a[2], pk,);
				return { uploaded: a[1], folder: a[0], localPath: a[2], resource: "folder", };
			},
			usage: "dss folder upload <name-or-id> <path> <localPath> [--dry-run] [--project-key KEY]",
			description: "Upload a local file to a managed folder.",
			examples: ["dss folder upload my_folder /data/report.csv ./report.csv --dry-run",],
		},
		"delete-file": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder delete-file <name-or-id> <path>",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete-file", resource: "folder", folder: a[0], path: a[1], };
				}
				await c.folders.deleteFile(
					await resolveFolderId(c, a[0], f,),
					a[1],
					f["project-key"] as string | undefined,
				);
				return { deleted: a[1], folder: a[0], resource: "folder", };
			},
			usage: "dss folder delete-file <name-or-id> <path> [--dry-run] [--project-key KEY]",
			description: "Delete a file from a managed folder.",
			examples: ["dss folder delete-file my_folder /data/report.csv",],
		},
	},

	variable: {
		get: {
			handler: (c, _a, f,) => c.variables.get(f["project-key"] as string | undefined,),
			usage: "dss variable get [--project-key KEY]",
			description: "Get project variables (standard and local).",
			examples: ["dss variable get", "dss variable get --project-key MYPROJ",],
		},
		set: {
			handler: async (c, _a, f,) => {
				const standard = json(f["standard"],);
				const local = json(f["local"],);
				const pk = f["project-key"] as string | undefined;
				if (standard === undefined && local === undefined) {
					throw new UsageError("--standard and/or --local is required.",);
				}
				if (f["dry-run"] === true) {
					const current = await c.variables.get(pk,);
					const next = f["replace"] === true
						? { standard: standard ?? {}, local: local ?? {}, }
						: {
							standard: { ...current.standard, ...standard, },
							local: { ...current.local, ...local, },
						};
					return {
						dryRun: true,
						action: "set",
						resource: "variable",
						projectKey: pk,
						current,
						next,
					};
				}
				return c.variables.set({
					standard,
					local,
					replace: f["replace"] === true,
					projectKey: pk,
				},);
			},
			usage:
				`dss variable set --standard '{"k":"v"}' --local '{"k":"v"}' [--replace] [--dry-run] [--project-key KEY]`,
			description: "Set project variables via JSON merge (or full replace with --replace).",
			examples: [
				'dss variable set --standard \'{"env":"staging"}\' --dry-run',
				"dss variable set --local '{\"debug\":true}' --replace",
			],
		},
	},

	connection: {
		list: {
			handler: (c, _a, f,) =>
				c.connections.list({
					type: f["type"] as string | undefined,
				},),
			usage: "dss connection list [--type TYPE]",
			description: "List all connection names, optionally filtered by connection type.",
			examples: ["dss connection list", "dss connection list --type Filesystem",],
		},
		infer: {
			handler: (c, _a, f,) =>
				c.connections.infer({
					mode: f["mode"] as "fast" | "rich" | undefined,
					projectKey: f["project-key"] as string | undefined,
				},),
			usage: "dss connection infer [--mode fast|rich] [--project-key KEY]",
			description: "List connections with inferred types and metadata.",
			examples: ["dss connection infer", "dss connection infer --mode rich",],
		},
		schemas: {
			handler: (c, _a, f,) => {
				const connection = f["connection"] as string | undefined;
				if (!connection) {
					throw new UsageError(
						"--connection is required. Usage: dss connection schemas --connection CONN",
					);
				}
				return c.connections.schemas({
					connection,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss connection schemas --connection CONN [--project-key KEY]",
			description: "List schemas in a SQL connection.",
			examples: ["dss connection schemas --connection ATHENA_CONN --project-key MYPROJ",],
		},
		tables: {
			handler: (c, _a, f,) => {
				const connection = f["connection"] as string | undefined;
				if (!connection) {
					throw new UsageError(
						"--connection is required. Usage: dss connection tables --connection CONN",
					);
				}
				return c.connections.tables({
					connection,
					catalog: f["catalog"] as string | undefined,
					schema: f["schema"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss connection tables --connection CONN [--catalog CATALOG] [--schema SCHEMA] [--project-key KEY]",
			description:
				"List importable tables in a SQL connection, optionally scoped by catalog and schema.",
			examples: [
				"dss connection tables --connection ATHENA_CONN --schema analytics --project-key MYPROJ",
			],
		},
	},

	"code-env": {
		list: {
			handler: (c, _a, f,) =>
				c.codeEnvs.list({
					envLang: f["lang"] as "PYTHON" | "R" | undefined,
				},),
			usage: "dss code-env list [--lang LANG]",
			description: "List code environments.",
			examples: ["dss code-env list", "dss code-env list --lang PYTHON",],
		},
		get: {
			handler: (c, a,) => {
				requireArgs(a, 2, "dss code-env get <lang> <name>",);
				return c.codeEnvs.get(a[0], a[1],);
			},
			usage: "dss code-env get <lang> <name>",
			description: "Get code environment details.",
			examples: ["dss code-env get PYTHON my_env",],
		},
		"get-definition": {
			handler: (c, a,) => {
				requireArgs(a, 2, "dss code-env get-definition <lang> <name>",);
				return c.codeEnvs.getDefinition(a[0], a[1],);
			},
			usage: "dss code-env get-definition <lang> <name>",
			description: "Get raw code environment definition.",
			examples: ["dss code-env get-definition PYTHON my_env",],
		},
		create: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env create <lang> <name> --deployment-mode MODE",);
				const deploymentMode = f["deployment-mode"] as string | undefined;
				if (!deploymentMode) {
					throw new UsageError(
						"--deployment-mode is required. Usage: dss code-env create <lang> <name> --deployment-mode MODE",
					);
				}
				const params = codeEnvParams(f,);
				const wait = codeEnvWait(f,);
				if (f["if-not-exists"] === true || f["dry-run"] === true) {
					const existing = (await c.codeEnvs.list({ envLang: a[0] as "PYTHON" | "R", },))
						.find((env,) => env.envName === a[1]);
					if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
						return skipResult("code-env", a[1], "exists", { envLang: a[0], current: existing, },);
					}
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "create",
							resource: "code-env",
							envLang: a[0],
							envName: a[1],
							payload: {
								deploymentMode,
								params,
								wait,
							},
							...(existing ? { current: existing, } : {}),
						};
					}
				}
				const created = await c.codeEnvs.create({
					envLang: a[0],
					envName: a[1],
					deploymentMode,
					params,
					wait,
				},);
				return { created: a[1], resource: "code-env", envLang: a[0], ...created, };
			},
			usage:
				"dss code-env create <lang> <name> --deployment-mode MODE [--params JSON|--data JSON|--data-file PATH|--stdin] [--python-interpreter PYTHON311] [--no-wait] [--if-not-exists] [--dry-run]",
			description: "Create a code environment.",
			examples: [
				"dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --python-interpreter PYTHON311",
				'dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --params \'{"pythonInterpreter":"PYTHON311"}\' --dry-run',
			],
		},
		"set-definition": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env set-definition <lang> <name> --data JSON",);
				const definition = jsonInput(f,);
				if (!definition) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss code-env set-definition <lang> <name> --data JSON",
					);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "set-definition",
						resource: "code-env",
						envLang: a[0],
						envName: a[1],
						definition,
					};
				}
				return c.codeEnvs.setDefinition(a[0], a[1], definition,);
			},
			usage:
				"dss code-env set-definition <lang> <name> [--data JSON|--data-file PATH|--stdin] [--dry-run]",
			description: "Replace a code environment definition previously fetched from DSS.",
			examples: ["dss code-env set-definition PYTHON my_env --data-file code-env.json --dry-run",],
		},
		"set-packages": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env set-packages <lang> <name> --packages PKGS",);
				const packages = codeEnvPackageList(f,);
				const installCorePackages = parseBooleanOption(
					f["install-core-packages"],
					"--install-core-packages",
				);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "set-packages",
						resource: "code-env",
						envLang: a[0],
						envName: a[1],
						packages,
						installCorePackages,
					};
				}
				return c.codeEnvs.setPackages(a[0], a[1], packages, { installCorePackages, },);
			},
			usage:
				"dss code-env set-packages <lang> <name> [--packages PKGS|--package PKG|--file PATH] [--install-core-packages true|false] [--dry-run]",
			description: "Update requested package specs without rebuilding packages.",
			examples: [
				"dss code-env set-packages PYTHON my_env --packages 'tabulate\\nnameparser' --dry-run",
				"dss code-env set-packages PYTHON my_env --file requirements.txt",
			],
		},
		"update-packages": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env update-packages <lang> <name>",);
				const wait = codeEnvWait(f,);
				const versionToUpdate = typeof f["env-version"] === "string"
					? f["env-version"]
					: typeof f["version"] === "string"
					? f["version"]
					: undefined;
				const opts = {
					forceRebuildEnv: f["force-rebuild"] === true,
					versionToUpdate,
					wait,
				};
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "update-packages",
						resource: "code-env",
						envLang: a[0],
						envName: a[1],
						...opts,
					};
				}
				return c.codeEnvs.updatePackages(a[0], a[1], opts,);
			},
			usage:
				"dss code-env update-packages <lang> <name> [--force-rebuild] [--env-version VERSION] [--no-wait] [--dry-run]",
			description: "Rebuild or update code environment packages to match the requested specs.",
			examples: ["dss code-env update-packages PYTHON my_env --force-rebuild --dry-run",],
		},
		"set-jupyter": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env set-jupyter <lang> <name> --active true|false",);
				const active = parseBooleanOption(f["active"], "--active",);
				if (active === undefined) {
					throw new UsageError(
						"--active is required. Usage: dss code-env set-jupyter <lang> <name> --active true|false",
					);
				}
				const wait = codeEnvWait(f,);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "set-jupyter",
						resource: "code-env",
						envLang: a[0],
						envName: a[1],
						active,
						wait,
					};
				}
				return c.codeEnvs.setJupyterSupport(a[0], a[1], active, { wait, },);
			},
			usage: "dss code-env set-jupyter <lang> <name> --active true|false [--no-wait] [--dry-run]",
			description: "Enable or disable Jupyter support for a code environment.",
			examples: ["dss code-env set-jupyter PYTHON my_env --active true --dry-run",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss code-env delete <lang> <name>",);
				const wait = codeEnvWait(f,);
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.codeEnvs.get(a[0], a[1],));
					if (!current) return skipResult("code-env", a[1], "missing", { envLang: a[0], },);
					if (f["dry-run"] === true) {
						return {
							dryRun: true,
							action: "delete",
							resource: "code-env",
							envLang: a[0],
							envName: a[1],
							wait,
							current,
						};
					}
				}
				return c.codeEnvs.delete(a[0], a[1], { wait, },);
			},
			usage: "dss code-env delete <lang> <name> [--no-wait] [--if-exists] [--dry-run]",
			description: "Delete a code environment.",
			examples: ["dss code-env delete PYTHON my_env --dry-run",],
		},
		usages: {
			handler: (c, a,) => {
				if (a.length === 0) return c.codeEnvs.listUsages();
				if (a.length === 2) return c.codeEnvs.listUsages(a[0], a[1],);
				throw new UsageError("Usage: dss code-env usages [<lang> <name>]",);
			},
			usage: "dss code-env usages [<lang> <name>]",
			description: "List code environment usages globally or for one environment.",
			examples: ["dss code-env usages", "dss code-env usages PYTHON my_env",],
		},
	},
	sql: {
		query: {
			handler: async (c, a, f,) => {
				const query = resolveSqlInput(a, f,);
				const connection = f["connection"] as string | undefined;
				const datasetFullName = f["dataset"] as string | undefined;
				if ((connection ? 1 : 0) + (datasetFullName ? 1 : 0) !== 1) {
					throw new UsageError(
						`Pass exactly one of --connection or --dataset. Usage: ${SQL_QUERY_USAGE}`,
					);
				}
				const result = await c.sql.query({
					query,
					connection,
					datasetFullName,
					database: f["database"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
				const outputFile = (f["output"] as string | undefined)
					?? (f["output-file"] as string | undefined);
				if (!outputFile) return result;

				const outputPath = resolve(outputFile,);
				await mkdir(dirname(outputPath,), { recursive: true, },);
				await writeFile(outputPath, `${JSON.stringify(result, null, 2,)}\n`, "utf-8",);
				return {
					queryId: result.queryId,
					schema: result.schema,
					columns: result.columns ?? result.schema,
					rowCount: result.rows.length,
					outputPath,
					written: outputPath,
				};
			},
			usage: SQL_QUERY_USAGE,
			description: "Run a SQL query against a DSS connection or dataset.",
			examples: [
				"dss sql query 'SELECT * FROM orders LIMIT 10' --connection my_pg",
				"dss sql query --sql-file query.sql --connection my_pg",
				"echo 'SELECT 1' | dss sql query --stdin --dataset MYPROJ.orders",
				"dss sql query --sql-file query.sql --connection my_pg --output results.json --request-timeout 120000",
			],
		},
	},
	notebook: {
		"list-jupyter": {
			handler: (c, _a, f,) => c.notebooks.listJupyter(f["project-key"] as string | undefined,),
			usage: "dss notebook list-jupyter [--project-key KEY]",
			description: "List Jupyter notebooks.",
			examples: ["dss notebook list-jupyter",],
		},
		"get-jupyter": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook get-jupyter <name>",);
				return c.notebooks.getJupyter(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss notebook get-jupyter <name> [--project-key KEY]",
			description: "Get a Jupyter notebook.",
			examples: ["dss notebook get-jupyter my_notebook",],
		},
		"delete-jupyter": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook delete-jupyter <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.notebooks.getJupyter(a[0], pk,));
					if (!current) return skipResult("jupyter-notebook", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "jupyter-notebook", name: a[0], current, };
					}
				}
				await c.notebooks.deleteJupyter(a[0], pk,);
				return { deleted: a[0], resource: "jupyter-notebook", };
			},
			usage: "dss notebook delete-jupyter <name> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a Jupyter notebook.",
			examples: ["dss notebook delete-jupyter my_notebook --dry-run",],
		},
		"clear-jupyter-outputs": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook clear-jupyter-outputs <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.notebooks.getJupyter(a[0], pk,);
					const next = {
						...current,
						cells: current.cells.map((cell,) => ({ ...cell, outputs: [], execution_count: null, })),
					};
					return {
						dryRun: true,
						action: "clear-jupyter-outputs",
						resource: "jupyter-notebook",
						name: a[0],
						current,
						next,
					};
				}
				await c.notebooks.clearJupyterOutputs(a[0], pk,);
				return { cleared: a[0], resource: "jupyter-notebook", };
			},
			usage: "dss notebook clear-jupyter-outputs <name> [--dry-run] [--project-key KEY]",
			description: "Clear all cell outputs from a Jupyter notebook.",
			examples: ["dss notebook clear-jupyter-outputs my_notebook --dry-run",],
		},
		"sessions-jupyter": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook sessions-jupyter <name>",);
				return c.notebooks.listJupyterSessions(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss notebook sessions-jupyter <name> [--project-key KEY]",
			description: "List active kernel sessions for a Jupyter notebook.",
			examples: ["dss notebook sessions-jupyter my_notebook",],
		},
		"unload-jupyter": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss notebook unload-jupyter <name> <sessionId>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const sessions = await c.notebooks.listJupyterSessions(a[0], pk,);
					const current = sessions.find((session,) => session.sessionId === a[1]);
					return {
						dryRun: true,
						action: "unload-jupyter",
						resource: "jupyter-notebook",
						name: a[0],
						sessionId: a[1],
						current,
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/jupyter-notebooks/${encodeURIComponent(a[0],)}/sessions/${encodeURIComponent(a[1],)}`,
						),
						method: "DELETE",
					};
				}
				await c.notebooks.unloadJupyter(a[0], a[1], pk,);
				return { unloaded: a[0], sessionId: a[1], resource: "jupyter-notebook", };
			},
			usage: "dss notebook unload-jupyter <name> <sessionId> [--dry-run] [--project-key KEY]",
			description: "Unload a Jupyter notebook kernel session.",
			examples: ["dss notebook unload-jupyter my_notebook SESSION_ID --dry-run",],
		},
		"list-sql": {
			handler: (c, _a, f,) => c.notebooks.listSql(f["project-key"] as string | undefined,),
			usage: "dss notebook list-sql [--project-key KEY]",
			description: "List SQL notebooks.",
			examples: ["dss notebook list-sql",],
		},
		"get-sql": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook get-sql <id>",);
				return c.notebooks.getSql(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss notebook get-sql <id> [--project-key KEY]",
			description: "Get a SQL notebook.",
			examples: ["dss notebook get-sql my_sql_notebook",],
		},
		"delete-sql": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook delete-sql <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true || f["if-exists"] === true) {
					const current = await readIfExists(() => c.notebooks.getSql(a[0], pk,));
					if (!current) return skipResult("sql-notebook", a[0], "missing",);
					if (f["dry-run"] === true) {
						return { dryRun: true, action: "delete", resource: "sql-notebook", id: a[0], current, };
					}
				}
				await c.notebooks.deleteSql(a[0], pk,);
				return { deleted: a[0], resource: "sql-notebook", };
			},
			usage: "dss notebook delete-sql <id> [--if-exists] [--dry-run] [--project-key KEY]",
			description: "Delete a SQL notebook.",
			examples: ["dss notebook delete-sql my_sql_notebook --dry-run",],
		},
		"history-sql": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook history-sql <id>",);
				return c.notebooks.getSqlHistory(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss notebook history-sql <id> [--project-key KEY]",
			description: "Get query history for a SQL notebook.",
			examples: ["dss notebook history-sql my_sql_notebook",],
		},
		"save-jupyter": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin]",
				);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required (notebook JSON content).",
					);
				}
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await readIfExists(() => c.notebooks.getJupyter(a[0], pk,));
					return {
						dryRun: true,
						action: "save-jupyter",
						resource: "jupyter-notebook",
						name: a[0],
						current,
						next: data,
					};
				}
				await c.notebooks.saveJupyter(a[0], data as never, pk,);
				return { saved: a[0], resource: "jupyter-notebook", };
			},
			usage:
				"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description: "Save content to a Jupyter notebook.",
			examples: [
				"dss notebook save-jupyter my_notebook --data-file notebook.json --dry-run",
				"cat notebook.json | dss notebook save-jupyter my_notebook --stdin",
			],
		},
		"save-sql": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required (SQL notebook content JSON).",
					);
				}
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await readIfExists(() => c.notebooks.getSql(a[0], pk,));
					return {
						dryRun: true,
						action: "save-sql",
						resource: "sql-notebook",
						id: a[0],
						current,
						next: data,
					};
				}
				await c.notebooks.saveSql(a[0], data as never, pk,);
				return { saved: a[0], resource: "sql-notebook", };
			},
			usage:
				"dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
			description: "Save content to a SQL notebook.",
			examples: ["dss notebook save-sql my_sql_notebook --data-file content.json --dry-run",],
		},
		"clear-sql-history": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook clear-sql-history <id>",);
				const pk = f["project-key"] as string | undefined;
				const options = {
					cellId: f["cell-id"] as string | undefined,
					numRunsToRetain: num(f["retain"],),
					projectKey: pk,
				};
				if (f["dry-run"] === true) {
					const current = await c.notebooks.getSqlHistory(a[0], pk,);
					return {
						dryRun: true,
						action: "clear-sql-history",
						resource: "sql-notebook",
						id: a[0],
						current,
						next: options,
						endpoint: encodedProjectEndpoint(
							c,
							pk,
							`/sql-notebooks/${encodeURIComponent(a[0],)}/history/clear`,
						),
						method: "POST",
					};
				}
				await c.notebooks.clearSqlHistory(a[0], options,);
				return { cleared: a[0], resource: "sql-notebook", };
			},
			usage:
				"dss notebook clear-sql-history <id> [--cell-id CID] [--retain N] [--dry-run] [--project-key KEY]",
			description: "Clear query history for a SQL notebook.",
			examples: [
				"dss notebook clear-sql-history my_sql_notebook --dry-run",
				"dss notebook clear-sql-history my_sql_notebook --cell-id CELL1 --retain 5",
			],
		},
	},
};

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

const RESOURCE_NAMES = [
	...Object.keys(commands,),
	"auth",
	"cleanup",
	"commands",
	"fixtures",
	"install-skill",
]
	.sort();

function printTopLevelHelp(): void {
	const lines = [
		"Usage: dss <resource> <action> [args...] [--flags]",
		"",
		"Global flags:",
		"  -h, --help               Show help",
		"  -v, --verbose            Log HTTP requests to stderr",
		"  -V, --version            Show version",
		"      --json               Emit JSON output (default)",
		"  -o, --output PATH        Write output to file (recipe get-payload)",
		"      --url URL            Dataiku DSS base URL (env: DATAIKU_URL)",
		"      --api-key KEY        API key              (env: DATAIKU_API_KEY)",
		"      --project-key KEY    Default project key   (env: DATAIKU_PROJECT_KEY)",
		"      --timeout MS         Operation timeout (build-and-wait, run-and-wait, recipe run)",
		"      --request-timeout MS HTTP request timeout in ms (default: 30000)",
		"      --dry-run            Preview destructive actions without executing",
		"      --if-not-exists      Skip create if resource already exists",
		"      --if-exists          Skip delete if resource is already missing",
		"      --insecure           Disable TLS certificate verification",
		"      --ca-cert PATH       Extra PEM CA bundle (env: NODE_EXTRA_CA_CERTS)",
		"",
		"Resources:",
		...RESOURCE_NAMES.map((r,) => `  ${r}`),
		"",
		"Quick start:",
		"  dss auth login                         Save DSS credentials",
		"  dss auth status                        Verify connection",
		"  dss doctor                            Run JSON connectivity diagnostics",
		"  dss project list                       List accessible projects",
		"  dss dataset list                       List datasets in default project",
		"  dss dataset preview <name>             Preview dataset rows as CSV",
		"  dss recipe get-payload <name>          Print recipe code to stdout",
		"  dss recipe download-code <name>        Download recipe code to a file",
		"  dss job log <id>                       View job log output",
		"  dss install-skill                      Install agent skill for coding agents",
	];
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

function printResourceHelp(resource: string,): void {
	const actions = commands[resource];
	if (!actions) return;
	const maxName = Math.max(...Object.keys(actions,).map((n,) => n.length),);
	const lines = [
		`Usage: dss ${resource} <action> [args...] [--flags]`,
		"",
		"Actions:",
		...Object.entries(actions,).map(
			([name, meta,],) => `  ${name.padEnd(maxName + 2,)}${meta.description ?? meta.usage}`,
		),
		"",
		`Run 'dss ${resource} <action> --help' for details and examples.`,
	];
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

function printActionHelp(resource: string, action: string,): void {
	const meta = commands[resource]?.[action];
	if (!meta) return;
	const lines: string[] = [];
	if (meta.description) lines.push(meta.description, "",);
	lines.push(`Usage: ${meta.usage}`,);
	if (meta.examples && meta.examples.length > 0) {
		lines.push("", "Examples:",);
		for (const ex of meta.examples) lines.push(`  ${ex}`,);
	}
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

class UsageError extends Error {
	readonly code: StableErrorCode;
	readonly hint?: string;

	constructor(message: string, code: StableErrorCode = "usage_error", hint?: string,) {
		super(message,);
		this.name = "UsageError";
		this.code = code;
		this.hint = hint;
	}
}

function requireArgs(args: string[], count: number, usage: string,): void {
	if (args.length < count) {
		throw new UsageError(
			`Expected ${count} argument(s), got ${args.length}.\nUsage: ${usage}`,
			"missing_required_arg",
		);
	}
}

// ---------------------------------------------------------------------------
// .env auto-loading
// ---------------------------------------------------------------------------

function loadEnvFile(): void {
	if (process.env.DATAIKU_DISABLE_ENV === "1") return;
	const dirs = [
		resolve(dirname(fileURLToPath(import.meta.url,),), "..",),
		process.cwd(),
	];
	for (const dir of dirs) {
		try {
			const content = readFileSync(resolve(dir, ".env",), "utf-8",);
			for (const line of content.split("\n",)) {
				const trimmed = line.trim();
				if (!trimmed || trimmed.startsWith("#",)) continue;
				const eq = trimmed.indexOf("=",);
				if (eq === -1) continue;
				const key = trimmed.slice(0, eq,).trim();
				const val = trimmed.slice(eq + 1,).trim().replace(/^['"]|['"]$/g, "",);
				if (!process.env[key]) process.env[key] = val;
			}
		} catch {
			// no .env file — fine
		}
	}
}

// ---------------------------------------------------------------------------
// Auth commands (run before client creation)
// ---------------------------------------------------------------------------

const AUTH_ACTIONS: Record<string, {
	handler: (flags: Record<string, string | boolean>,) => Promise<void>;
	usage: string;
	description?: string;
	examples?: string[];
}> = {
	login: {
		handler: async (flags,) => {
			const tlsSettings = resolveTlsSettings(flags,);
			let { url, apiKey, projectKey, } = resolveCredentials(flags,);

			if (!url || !apiKey) {
				if (!process.stdin.isTTY) {
					throw new UsageError(
						"Missing --url and/or --api-key. Provide them as flags or run interactively.",
					);
				}
				if (!url) url = await promptLine("DSS URL: ",);
				if (!apiKey) apiKey = await promptSecret("API key: ",);
				if (!projectKey) projectKey = (await promptLine("Project key (optional): ",)) || undefined;
			}

			if (!url) throw new UsageError("URL is required.",);
			if (!apiKey) throw new UsageError("API key is required.",);
			process.stderr.write("Validating credentials... ",);
			const result = await validateCredentials(url, apiKey, tlsSettings,);
			if (!result.valid) {
				process.stderr.write("Failed\n",);
				if (result.dataikuError) throw result.dataikuError;
				throw new DataikuError(
					0,
					"Authentication Failed",
					result.error ?? "Credential validation failed",
				);
			}
			process.stderr.write("Connected\n",);

			saveCredentials({ url, apiKey, projectKey, ...tlsSettings, },);
			process.stderr.write(`Credentials saved to ${getCredentialsPath()}\n`,);
		},
		usage:
			"dss auth login [--url URL] [--api-key KEY] [--project-key KEY] [--insecure] [--ca-cert PATH]",
		description: "Save DSS credentials (interactive or via flags).",
		examples: [
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY",
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ",
		],
	},
	status: {
		handler: async (flags,) => {
			const creds = loadCredentials();
			if (!creds) {
				process.stderr.write("No saved credentials. Run: dss auth login\n",);
				process.exit(1,);
			}
			const tlsSettings = resolveTlsSettings(flags, creds,);
			const lines = [
				`URL:         ${creds.url}`,
				`API key:     ${maskApiKey(creds.apiKey,)}`,
				`Project key: ${creds.projectKey ?? "(not set)"}`,
				`TLS verify:  ${tlsSettings.tlsRejectUnauthorized === false ? "disabled" : "strict"}`,
				`CA cert:     ${tlsSettings.caCertPath ?? "(default trust store)"}`,
			];
			for (const line of lines) process.stderr.write(`${line}\n`,);

			const result = await validateCredentials(creds.url, creds.apiKey, tlsSettings,);
			if (result.valid) {
				process.stderr.write("Connection:  valid\n",);
			} else {
				process.stderr.write(`Connection:  failed (${result.error ?? "unknown error"})\n`,);
				process.stderr.write(`Config:      ${getCredentialsPath()}\n`,);
				process.exit(1,);
			}
			process.stderr.write(`Config:      ${getCredentialsPath()}\n`,);
		},
		usage: "dss auth status [--insecure] [--ca-cert PATH]",
		description: "Show saved credentials and verify the connection.",
		examples: ["dss auth status",],
	},
	logout: {
		handler: async (_flags,) => {
			deleteCredentials();
			process.stderr.write("Credentials removed.\n",);
		},
		usage: "dss auth logout",
		description: "Remove saved credentials.",
		examples: ["dss auth logout",],
	},
};

// ---------------------------------------------------------------------------
// Agent-facing diagnostics and introspection
// ---------------------------------------------------------------------------

interface DoctorCheck {
	name: string;
	ok: boolean;
	message: string;
	details?: Record<string, unknown>;
}

type PermissionStatus = "yes" | "no" | "unknown";

type DoctorPermissionKey =
	| "canListProjects"
	| "canReadProject"
	| "canMutateProject"
	| "canCreateFolder"
	| "canRunJobs"
	| "canCreateScenario"
	| "canSaveJupyter"
	| "canMutateConnection";

type DoctorPermissions = Record<DoctorPermissionKey, PermissionStatus>;

interface DoctorFixtures {
	defaultDataset: string | null;
	defaultRecipe: string | null;
	defaultScenario: string | null;
	defaultFlowZone: string | null;
	defaultManagedFolder: string | null;
	defaultJupyterNotebook: string | null;
}

interface FixtureReject {
	id?: string;
	name?: string;
	type?: string;
	reason: string;
}

interface FixtureDiscoveryResult {
	projectKey: string;
	allowTypes: string[];
	fixtures: DoctorFixtures;
	safeDataset: Record<string, unknown> | null;
	safeManagedFolder: Record<string, unknown> | null;
	safeJupyterNotebook: Record<string, unknown> | null;
	unsafe: {
		datasets: FixtureReject[];
		managedFolders: FixtureReject[];
		jupyterNotebooks: FixtureReject[];
	};
}
interface DoctorEnvironment {
	projectKey?: string;
	dssVersion?: string;
	instanceTime?: string;
	integrationFlags: {
		mutating: boolean;
		adminMutating: boolean;
		variables: boolean;
		sqlLive: boolean;
		bundles: boolean;
		apiServices: boolean;
	};
}

interface DoctorResult {
	ok: boolean;
	checks: DoctorCheck[];
	context: {
		hasUrl: boolean;
		hasApiKey: boolean;
		projectKey?: string;
		tlsVerify: "strict" | "disabled";
		caCert: "default" | "custom";
	};
	permissions?: DoctorPermissions;
	permissionDetails?: Partial<Record<DoctorPermissionKey, Record<string, unknown>>>;
	fixtures?: DoctorFixtures;
	environment?: DoctorEnvironment;
}

function errorDetails(error: unknown,): Record<string, unknown> {
	if (error instanceof DataikuError) {
		return {
			category: error.category,
			retryable: error.retryable,
			status: error.status,
			statusText: error.statusText,
		};
	}
	return { message: error instanceof Error ? error.message : String(error,), };
}

function firstStringField(items: unknown[] | undefined, fields: string[],): string | null {
	for (const item of items ?? []) {
		if (item === null || typeof item !== "object" || Array.isArray(item,)) continue;
		const record = item as Record<string, unknown>;
		for (const field of fields) {
			const value = record[field];
			if (typeof value === "string" && value.trim().length > 0) return value;
		}
	}
	return null;
}

function integrationFlag(name: string,): boolean {
	const value = process.env[name];
	return value === "1" || value?.toLowerCase() === "true";
}

function doctorEnvironment(projectKey?: string,): DoctorEnvironment {
	return {
		...(projectKey ? { projectKey, } : {}),
		integrationFlags: {
			mutating: integrationFlag("RUN_DATAIKU_INTEGRATION_MUTATING",),
			adminMutating: integrationFlag("RUN_DATAIKU_ADMIN_MUTATING",),
			variables: integrationFlag("RUN_DATAIKU_INTEGRATION_VARIABLES",),
			sqlLive: integrationFlag("RUN_DATAIKU_SQL_LIVE",),
			bundles: integrationFlag("RUN_DATAIKU_INTEGRATION_BUNDLES",),
			apiServices: integrationFlag("RUN_DATAIKU_INTEGRATION_API_SERVICES",),
		},
	};
}

function permissionStatusForError(error: unknown,): PermissionStatus {
	if (error instanceof DataikuError) {
		if (error.status === 401 || error.status === 403 || error.status === 404) return "no";
		if (error.status === 0 || error.status >= 500 || error.category === "transient") return "unknown";
		if (error.category === "forbidden" || error.category === "not_found") return "no";
		return "unknown";
	}
	return "unknown";
}

async function probeDoctorPermission(
	probe: () => Promise<unknown>,
): Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }> {
	try {
		await probe();
		return { status: "yes", };
	} catch (error) {
		return { status: permissionStatusForError(error,), details: errorDetails(error,), };
	}
}

async function probeReadOnlyPrerequisiteForMutation(
	probe: () => Promise<unknown>,
	readAction: string,
): Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }> {
	const readProbe = await probeDoctorPermission(probe,);
	if (readProbe.status !== "yes") return readProbe;
	return {
		status: "unknown",
		details: {
			reason: "mutation capability was not verified because doctor capabilities are read-only",
			readAction,
			readStatus: "yes",
		},
	};
}

function missingProjectPermission(): {
	status: PermissionStatus;
	details: Record<string, unknown>;
} {
	return {
		status: "unknown",
		details: { reason: "projectKey is required for this probe", },
	};
}

function recordsFromUnknownArray(items: unknown[],): Array<Record<string, unknown>> {
	return items.filter((item,) =>
		item !== null && typeof item === "object" && !Array.isArray(item,)
	) as Array<
		Record<string, unknown>
	>;
}

function doctorFixturesFromLists(
	datasets: unknown[],
	recipes: unknown[],
	scenarios: unknown[],
	flowZones: unknown[],
	folders: unknown[],
	jupyterNotebooks: unknown[],
): DoctorFixtures {
	return {
		defaultDataset: firstStringField(datasets, ["name",],),
		defaultRecipe: firstStringField(recipes, ["name",],),
		defaultScenario: firstStringField(scenarios, ["id",],),
		defaultFlowZone: firstStringField(flowZones, ["id",],),
		defaultManagedFolder: firstStringField(folders, ["id",],),
		defaultJupyterNotebook: firstStringField(jupyterNotebooks, ["name",],),
	};
}

async function discoverDoctorFixtures(
	client: DataikuClient,
	projectKey: string,
): Promise<DoctorFixtures> {
	const [
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	] = await Promise.all([
		client.datasets.list(projectKey,),
		client.recipes.list(projectKey,),
		client.scenarios.list(projectKey,),
		client.flowZones.list(projectKey,),
		client.folders.list(projectKey,),
		client.notebooks.listJupyter(projectKey,),
	],);
	return doctorFixturesFromLists(
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	);
}

const DEFAULT_FIXTURE_ALLOW_TYPES = ["Filesystem", "Inline",];

function fixtureAllowTypes(flags: Record<string, string | boolean>,): string[] {
	const configured = splitCsvFlag(flags["allow-types"],);
	return configured.length > 0 ? configured : DEFAULT_FIXTURE_ALLOW_TYPES;
}

function isAllowedFixtureType(type: string | undefined, allowTypes: string[],): boolean {
	if (!type) return false;
	const normalized = type.trim().toLowerCase();
	return allowTypes.some((allowed,) => allowed.trim().toLowerCase() === normalized);
}

function fixtureReject(record: Record<string, unknown>, reason: string,): FixtureReject {
	return {
		...(stringField(record, ["id",],) ? { id: stringField(record, ["id",],), } : {}),
		...(stringField(record, ["name",],) ? { name: stringField(record, ["name",],), } : {}),
		...(stringField(record, ["type",],) ? { type: stringField(record, ["type",],), } : {}),
		reason,
	};
}

function firstSafeTypedFixture(
	items: unknown[],
	allowTypes: string[],
): { safe: Record<string, unknown> | null; unsafe: FixtureReject[]; } {
	const unsafe: FixtureReject[] = [];
	for (const record of recordsFromUnknownArray(items,)) {
		const type = stringField(record, ["type",],);
		if (isAllowedFixtureType(type, allowTypes,)) return { safe: record, unsafe, };
		unsafe.push(fixtureReject(record, `type=${type ?? "missing"}`,),);
	}
	return { safe: null, unsafe, };
}

function firstSafeJupyterNotebook(
	items: unknown[],
): { safe: Record<string, unknown> | null; unsafe: FixtureReject[]; } {
	const unsafe: FixtureReject[] = [];
	for (const record of recordsFromUnknownArray(items,)) {
		const name = stringField(record, ["name",],);
		if (name && !name.startsWith("_",)) return { safe: record, unsafe, };
		unsafe.push(fixtureReject(record, name ? "name starts with _" : "missing name",),);
	}
	return { safe: null, unsafe, };
}

async function discoverFixtureReport(
	client: DataikuClient,
	projectKey: string,
	flags: Record<string, string | boolean>,
): Promise<FixtureDiscoveryResult> {
	const allowTypes = fixtureAllowTypes(flags,);
	const [
		datasets,
		recipes,
		scenarios,
		flowZones,
		folders,
		jupyterNotebooks,
	] = await Promise.all([
		client.datasets.list(projectKey,),
		client.recipes.list(projectKey,),
		client.scenarios.list(projectKey,),
		client.flowZones.list(projectKey,),
		client.folders.list(projectKey,),
		client.notebooks.listJupyter(projectKey,),
	],);
	const dataset = firstSafeTypedFixture(datasets, allowTypes,);
	const folder = firstSafeTypedFixture(folders, allowTypes,);
	const notebook = firstSafeJupyterNotebook(jupyterNotebooks,);
	return {
		projectKey,
		allowTypes,
		fixtures: doctorFixturesFromLists(
			datasets,
			recipes,
			scenarios,
			flowZones,
			folders,
			jupyterNotebooks,
		),
		safeDataset: dataset.safe,
		safeManagedFolder: folder.safe,
		safeJupyterNotebook: notebook.safe,
		unsafe: {
			datasets: dataset.unsafe,
			managedFolders: folder.unsafe,
			jupyterNotebooks: notebook.unsafe,
		},
	};
}

async function doctorCapabilities(
	client: DataikuClient,
	projectKey: string | undefined,
	accessibleProjects: unknown[] | undefined,
	flags: Record<string, string | boolean>,
): Promise<Pick<DoctorResult, "permissions" | "permissionDetails" | "fixtures" | "environment">> {
	const probeProjectKey = projectKey ?? firstStringField(accessibleProjects, ["projectKey",],)
		?? undefined;
	const probes: Record<
		DoctorPermissionKey,
		() => Promise<{ status: PermissionStatus; details?: Record<string, unknown>; }>
	> = {
		canListProjects: () =>
			probeDoctorPermission(async () => accessibleProjects ?? await client.projects.list()),
		canReadProject: () =>
			probeProjectKey
				? probeDoctorPermission(() => client.projects.get(probeProjectKey,))
				: Promise.resolve(missingProjectPermission(),),
		canMutateProject: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.variables.get(probeProjectKey,),
					"variables.get",
				)
				: Promise.resolve(missingProjectPermission(),),
		canCreateFolder: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.folders.list(probeProjectKey,),
					"folders.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canRunJobs: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.jobs.list(probeProjectKey,),
					"jobs.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canCreateScenario: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.scenarios.list(probeProjectKey,),
					"scenarios.list",
				)
				: Promise.resolve(missingProjectPermission(),),
		canSaveJupyter: () =>
			probeProjectKey
				? probeReadOnlyPrerequisiteForMutation(
					() => client.notebooks.listJupyter(probeProjectKey,),
					"notebooks.listJupyter",
				)
				: Promise.resolve(missingProjectPermission(),),
		canMutateConnection: () =>
			probeReadOnlyPrerequisiteForMutation(() => client.connections.list(), "connections.list",),
	};
	const permissions = {} as DoctorPermissions;
	const permissionDetails: Partial<Record<DoctorPermissionKey, Record<string, unknown>>> = {};
	for (const key of Object.keys(probes,) as DoctorPermissionKey[]) {
		const probe = await probes[key]();
		permissions[key] = probe.status;
		if (probe.details) permissionDetails[key] = probe.details;
	}

	const capabilityResult: Pick<
		DoctorResult,
		"permissions" | "permissionDetails" | "fixtures" | "environment"
	> = {
		permissions,
		...(Object.keys(permissionDetails,).length > 0 ? { permissionDetails, } : {}),
		environment: doctorEnvironment(projectKey,),
	};

	if (flags["fast"] !== true && probeProjectKey) {
		try {
			capabilityResult.fixtures = await discoverDoctorFixtures(client, probeProjectKey,);
		} catch (error) {
			capabilityResult.fixtures = {
				defaultDataset: null,
				defaultRecipe: null,
				defaultScenario: null,
				defaultFlowZone: null,
				defaultManagedFolder: null,
				defaultJupyterNotebook: null,
			};
			capabilityResult.permissionDetails = {
				...capabilityResult.permissionDetails,
				canReadProject: {
					...capabilityResult.permissionDetails?.canReadProject,
					fixtureDiscovery: errorDetails(error,),
				},
			};
		}
	}

	return capabilityResult;
}

async function runDoctor(flags: Record<string, string | boolean>,): Promise<{
	result: DoctorResult;
	exitCode: number;
}> {
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	const checks: DoctorCheck[] = [];
	const context: DoctorResult["context"] = {
		hasUrl: url.trim().length > 0,
		hasApiKey: apiKey.trim().length > 0,
		...(projectKey ? { projectKey, } : {}),
		tlsVerify: tlsRejectUnauthorized === false ? "disabled" : "strict",
		caCert: caCertPath ? "custom" : "default",
	};

	const credentialsOk = context.hasUrl && context.hasApiKey;
	checks.push({
		name: "credentials_present",
		ok: credentialsOk,
		message: credentialsOk
			? "Dataiku URL and API key are configured."
			: "Missing Dataiku URL and/or API key. Set DATAIKU_URL/DATAIKU_API_KEY, pass flags, or run dss auth login.",
	},);

	let accessibleProjects: unknown[] | undefined;

	if (credentialsOk) {
		const requestTimeoutMs = num(flags["request-timeout"],);
		const retryMaxAttempts = num(flags["retries"],);
		const client = new DataikuClient({
			url,
			apiKey,
			projectKey,
			verbose: flags["verbose"] === true,
			requestTimeoutMs,
			retryMaxAttempts,
			tlsRejectUnauthorized,
			caCertPath,
		},);

		try {
			const projects = await client.projects.list();
			accessibleProjects = projects;
			checks.push({
				name: "connectivity",
				ok: true,
				message: "Connected to DSS and listed accessible projects.",
				details: { projectCount: projects.length, },
			},);
		} catch (error) {
			checks.push({
				name: "connectivity",
				ok: false,
				message: "Could not list accessible projects.",
				details: errorDetails(error,),
			},);
		}

		if (projectKey) {
			try {
				const project = await client.projects.get(projectKey,);
				checks.push({
					name: "default_project",
					ok: true,
					message: `Project ${projectKey} is accessible.`,
					details: {
						projectKey,
						name: typeof project.name === "string" ? project.name : undefined,
					},
				},);
			} catch (error) {
				checks.push({
					name: "default_project",
					ok: false,
					message: `Project ${projectKey} is not accessible.`,
					details: errorDetails(error,),
				},);
			}
		}
	}

	const result: DoctorResult = { ok: checks.every((check,) => check.ok), checks, context, };
	if (flags["capabilities"] === true && credentialsOk) {
		const requestTimeoutMs = num(flags["request-timeout"],);
		const retryMaxAttempts = num(flags["retries"],) ?? 1;
		const client = new DataikuClient({
			url,
			apiKey,
			projectKey,
			verbose: flags["verbose"] === true,
			requestTimeoutMs,
			retryMaxAttempts,
			tlsRejectUnauthorized,
			caCertPath,
		},);
		result.environment = doctorEnvironment(projectKey,);
		Object.assign(result, await doctorCapabilities(client, projectKey, accessibleProjects, flags,),);
	}
	return { result, exitCode: result.ok ? 0 : 2, };
}

async function runFixtures(
	flags: Record<string, string | boolean>,
): Promise<FixtureDiscoveryResult> {
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	if (!url) {
		throw new UsageError("Missing Dataiku URL. Set DATAIKU_URL, pass --url, or run: dss auth login",);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY, pass --api-key, or run: dss auth login",
		);
	}
	if (!projectKey) {
		throw new UsageError(
			"Missing project key. Set DATAIKU_PROJECT_KEY, pass --project-key, or run: dss auth login",
			"missing_required_flag",
		);
	}

	currentCommandContext.projectKey = projectKey;
	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],) ?? 1;
	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);
	return discoverFixtureReport(client, projectKey, flags,);
}

type CommandSideEffect = "read" | "write" | "auth";
type CommandOutputShape = "object" | "array" | "string" | "void";
type CommandDestructiveLevel = "none" | "reversible" | "destructive";
type CommandAsyncKind = "none" | "job" | "future";
type CommandIdempotency = "safe" | "if-not-exists" | "if-exists" | "none";

interface CommandInputContract {
	stdin?: boolean;
	dataFlag?: boolean;
	dataFileFlag?: boolean;
}

interface CommandExitCodes {
	ok: 0;
	usage: 1;
	error: 2;
	transient: 3;
	longRunningFailure?: 4;
}

interface CommandRegistryEntry {
	resource: string;
	action: string;
	usage: string;
	description?: string;
	examples?: string[];
	flags: Array<{ name: string; kind: "boolean" | "value"; }>;
	positionals: string[];
	sideEffect: CommandSideEffect;
	requiresAuth: boolean;
	requiresProject: boolean;
	outputShape: CommandOutputShape;
	inputContract: CommandInputContract;
	destructive: CommandDestructiveLevel;
	producesLocalFile: boolean;
	mutatesDss: boolean;
	async: CommandAsyncKind;
	idempotency: CommandIdempotency;
	dryRun: boolean;
	requiredFlags: string[];
	optionalFlags: string[];
	payloadSchema?: CommandPayloadSchema;
	examplePayload?: unknown;
	cleanupCommand?: string;
	exitCodes: CommandExitCodes;
	cleanupHint?: string;
}

const READ_ACTIONS = new Set([
	"cat",
	"contents",
	"diff",
	"download",
	"download-code",
	"flow",
	"get",
	"get-rule",
	"get-definition",
	"get-jupyter",
	"get-payload",
	"get-sql",
	"graph",
	"history-sql",
	"history",
	"infer",
	"list",
	"last-results",
	"list-jupyter",
	"list-sql",
	"log",
	"log-url",
	"map",
	"metadata",
	"peek",
	"source",
	"summary",
	"wait",
	"watch",
	"preview",
	"query",
	"schema",
	"schemas",
	"sessions-jupyter",
	"status",
	"rules",
	"settings",
	"status-by-partition",
	"usages",
],);

const PROJECT_SCOPED_RESOURCES = new Set([
	"data-quality",
	"dashboard",
	"dataset",
	"flow-zone",
	"insight",
	"folder",
	"fixtures",
	"job",
	"notebook",
	"recipe",
	"scenario",
	"sql",
	"variable",
	"wiki",
],);

const GLOBAL_AGENT_FLAGS = ["help", "json", "report-json", "verbose",];
const AUTHENTICATED_AGENT_FLAGS = [
	"url",
	"api-key",
	"request-timeout",
	"retries",
	"insecure",
	"ca-cert",
];
const COMMANDS_USAGE = "dss commands [--json]";
const COMMANDS_DESCRIPTION = "Print the machine-readable command registry for agent planning.";
const COMMANDS_EXAMPLES = ["dss commands", "dss commands --json",];
const INSTALL_SKILL_USAGE =
	"dss install-skill [--global] [--agent NAME] [--target PATH] [--list-agents] [--dry-run] [--plan]";
const INSTALL_SKILL_DESCRIPTION = "Install the dataiku-dss agent skill for detected coding agents.";
const INSTALL_SKILL_EXAMPLES = [
	"dss install-skill --list-agents",
	"dss install-skill --agent omp --dry-run",
];
const CLEANUP_USAGE = "dss cleanup --file PATH [--dry-run|--apply] [--continue-on-error]";
const CLEANUP_DESCRIPTION = "Replay cleanup ledger entries in reverse order.";
const CLEANUP_EXAMPLES = [
	"dss cleanup --file cleanup.jsonl",
	"dss cleanup --file cleanup.jsonl --apply",
];
const FIXTURES_USAGE = "dss fixtures [--json] [--project-key KEY] [--allow-types CSV]";
const FIXTURES_DESCRIPTION = "Discover safe live-test fixtures for agent workflows.";
const FIXTURES_EXAMPLES = [
	"dss fixtures --json",
	"dss fixtures --json --allow-types Filesystem,Inline",
];

function uniqueStrings(values: string[],): string[] {
	return [...new Set(values,),];
}

function flagKind(name: string,): "boolean" | "value" {
	return BOOLEAN_FLAGS.has(name,) ? "boolean" : "value";
}

function registryKey(resource: string, action: string,): string {
	return `${resource}.${action}`;
}

const EXPLICIT_REGISTRY_OVERRIDES: Record<string, CommandRegistryOverride> = {
	"dashboard.create": {
		examplePayload: { pages: [], },
	},
	"dashboard.update": {
		examplePayload: { name: "Updated dashboard", },
	},
	"data-quality.create-rule": {
		examplePayload: {
			type: "RecordCountInRangeRule",
			softMinimum: 1,
			softMinimumEnabled: true,
			displayName: "Has rows",
		},
	},
	"data-quality.update-rule": {
		examplePayload: { enabled: false, },
	},
	"dataset.update": {
		examplePayload: { tags: ["production",], },
	},
	"insight.create": {
		examplePayload: {
			name: "Agent insight",
			type: "chart",
			listed: false,
			params: {},
		},
	},
	"insight.update": {
		examplePayload: { listed: false, },
	},
	"recipe.update": {
		examplePayload: { recipe: { params: {}, }, },
	},
	"scenario.update": {
		examplePayload: { active: false, },
	},
	"wiki.update": {
		examplePayload: { article: { name: "Updated article", }, },
	},
};

function extractUsageFlags(usage: string,): string[] {
	const flags: string[] = [];
	for (const match of usage.matchAll(/--([a-z0-9-]+)/g,)) {
		flags.push(FLAG_ALIASES[match[1]!] ?? match[1]!,);
	}
	return uniqueStrings(flags,).filter((flag,) => KNOWN_LONG_FLAGS.has(flag,));
}

function extractPositionals(usage: string,): string[] {
	return uniqueStrings([...usage.matchAll(/<([^>]+)>/g,),].map((match,) => match[1]),);
}

function inferSideEffect(resource: string, action: string,): CommandSideEffect {
	if (resource === "auth") return "auth";
	if (resource === "doctor" || resource === "commands" || resource === "fixtures") return "read";
	if (resource === "install-skill") return "write";
	if (resource === "data-quality" && action === "compute") return "write";
	if (READ_ACTIONS.has(action,)) return "read";
	if (
		/^(create|clone|restore|update|delete|set|save|upload|run|build|abort|move|refresh|clear|unload|install|login|logout)/
			.test(action,)
	) {
		return "write";
	}
	return "read";
}

function inferRequiresAuth(resource: string,): boolean {
	return resource !== "auth" && resource !== "commands" && resource !== "install-skill";
}

function inferRequiresProject(resource: string, action: string, usage: string,): boolean {
	if (resource === "doctor" || resource === "commands" || resource === "install-skill") return false;
	if (PROJECT_SCOPED_RESOURCES.has(resource,)) return true;
	if (resource === "project" && action !== "list") return true;
	return usage.includes("--project-key",);
}

const ARRAY_OUTPUT_ACTIONS = new Set([
	"history",
	"find",
	"infer",
	"last-results",
	"list",
	"list-jupyter",
	"list-sql",
	"rules",
	"schemas",
	"sessions-jupyter",
	"usages",
],);

const STRING_OUTPUT_ACTIONS = new Set([
	"diff",
	"download",
	"download-code",
	"get-payload",
	"cat",
	"log",
	"log-url",
	"preview",
],);

function inferOutputShape(resource: string, action: string,): CommandOutputShape {
	if (resource === "auth" || resource === "install-skill") return "void";
	if (ARRAY_OUTPUT_ACTIONS.has(action,)) return "array";
	if (STRING_OUTPUT_ACTIONS.has(action,)) return "string";
	return "object";
}

function inferInputContract(usage: string,): CommandInputContract {
	return {
		...(usage.includes("--stdin",) ? { stdin: true, } : {}),
		...(usage.includes("--data ",) || usage.includes("--data JSON",) ? { dataFlag: true, } : {}),
		...(usage.includes("--data-file",) ? { dataFileFlag: true, } : {}),
	};
}

function stripOptionalUsageGroups(usage: string,): string {
	return usage.replace(/\[[^\]]*\]/g, " ",);
}

function extractRequiredUsageFlags(usage: string,): string[] {
	return extractUsageFlags(stripOptionalUsageGroups(usage,),);
}

function inferPayloadSchema(
	inputContract: CommandInputContract,
): CommandPayloadSchema | undefined {
	if (!inputContract.stdin && !inputContract.dataFlag && !inputContract.dataFileFlag) {
		return undefined;
	}
	return { ...inputContract, jsonShape: "object", };
}

function inferExitCodes(asyncKind: CommandAsyncKind,): CommandExitCodes {
	return {
		ok: 0,
		usage: 1,
		error: 2,
		transient: 3,
		...(asyncKind !== "none" ? { longRunningFailure: 4 as const, } : {}),
	};
}

function cleanupCommandFromDeleteUsage(resource: string, action: string,): string | undefined {
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	const deleteAction = action === "create-rule" ? "delete-rule" : "delete";
	const deleteUsage = commands[resource]?.[deleteAction]?.usage;
	if (!deleteUsage) return undefined;
	const base = stripOptionalUsageGroups(deleteUsage,).replace(/\s+/g, " ",).trim();
	if (deleteUsage.includes("--if-exists",)) return `${base} --if-exists`;
	return base;
}

function supportsCleanupLedger(resource: string, action: string,): boolean {
	return cleanupCommandFromDeleteUsage(resource, action,) !== undefined
		|| `${resource}.${action}` === "folder.upload";
}

function inferDestructiveLevel(
	sideEffect: CommandSideEffect,
	action: string,
): CommandDestructiveLevel {
	if (sideEffect !== "write") return "none";
	if (/^(delete|abort|clear|unload|logout)/.test(action,)) return "destructive";
	return "reversible";
}

function inferAsyncKind(resource: string, action: string,): CommandAsyncKind {
	if (
		resource === "job" && ["build", "build-and-wait", "wait", "monitor", "watch",].includes(action,)
	) {
		return "job";
	}
	if (resource === "recipe" && action === "run") return "job";
	if (resource === "future" && ["get", "peek", "wait", "abort",].includes(action,)) return "future";
	if (resource === "scenario" && ["run", "run-and-wait", "status",].includes(action,)) {
		return "future";
	}
	if (resource === "data-quality" && action === "compute") return "future";
	return "none";
}

function inferIdempotency(
	sideEffect: CommandSideEffect,
	action: string,
	usage: string,
): CommandIdempotency {
	if (sideEffect === "read") return "safe";
	if (action.startsWith("create",) && usage.includes("--if-not-exists",)) return "if-not-exists";
	if (action.startsWith("delete",) && usage.includes("--if-exists",)) return "if-exists";
	return "none";
}

function inferCleanupHint(resource: string, action: string,): string | undefined {
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	if (resource === "code-env") return "Delete with `dss code-env delete <lang> <name> --if-exists`.";
	if (resource === "data-quality") {
		return "Delete with `dss data-quality delete-rule <dataset> <rule-id> --if-exists`.";
	}
	return `Delete with \`dss ${resource} delete <id> --if-exists\` when the created object is disposable.`;
}

function buildRegistryEntry(
	resource: string,
	action: string,
	meta: CommandMeta,
): CommandRegistryEntry {
	const requiresAuth = inferRequiresAuth(resource,);
	const requiresProject = inferRequiresProject(resource, action, meta.usage,);
	const sideEffect = inferSideEffect(resource, action,);
	const destructive = inferDestructiveLevel(sideEffect, action,);
	const asyncKind = inferAsyncKind(resource, action,);
	const mutatesDss = sideEffect === "write" && resource !== "auth" && resource !== "install-skill";
	const supportsPlan = mutatesDss || sideEffect === "write";
	const supportsCleanup = supportsCleanupLedger(resource, action,);
	const usageFlags = extractUsageFlags(meta.usage,);
	const flags = uniqueStrings([
		...usageFlags,
		...(supportsPlan ? ["plan",] : []),
		...(supportsCleanup ? ["record-cleanup",] : []),
		...GLOBAL_AGENT_FLAGS,
		...(requiresAuth ? AUTHENTICATED_AGENT_FLAGS : []),
		...(requiresProject ? ["project-key",] : []),
	],);
	const requiredFlags = meta.requiredFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.requiredFlags
		?? extractRequiredUsageFlags(meta.usage,);
	const optionalFlags = meta.optionalFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.optionalFlags
		?? flags.filter((flag,) => !requiredFlags.includes(flag,));
	const inputContract = inferInputContract(meta.usage,);
	const cleanupHint = inferCleanupHint(resource, action,);
	const payloadSchema = meta.payloadSchema
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.payloadSchema
		?? inferPayloadSchema(inputContract,);
	const examplePayload = meta.examplePayload
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.examplePayload;
	const cleanupCommand = meta.cleanupCommand
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.cleanupCommand
		?? cleanupCommandFromDeleteUsage(resource, action,);
	return {
		resource,
		action,
		usage: meta.usage,
		description: meta.description,
		examples: meta.examples,
		flags: flags.map((name,) => ({ name, kind: flagKind(name,), })),
		positionals: extractPositionals(meta.usage,),
		sideEffect,
		requiresAuth,
		requiresProject,
		outputShape: inferOutputShape(resource, action,),
		inputContract,
		destructive,
		producesLocalFile: meta.usage.includes("--output PATH",)
			|| meta.usage.includes("--output-file PATH",),
		mutatesDss,
		async: asyncKind,
		idempotency: inferIdempotency(sideEffect, action, meta.usage,),
		dryRun: meta.usage.includes("--dry-run",),
		requiredFlags: uniqueStrings(requiredFlags,),
		optionalFlags: uniqueStrings(optionalFlags,),
		...(payloadSchema ? { payloadSchema, } : {}),
		...(examplePayload !== undefined ? { examplePayload, } : {}),
		...(cleanupCommand ? { cleanupCommand, } : {}),
		exitCodes: inferExitCodes(asyncKind,),
		...(cleanupHint ? { cleanupHint, } : {}),
	};
}

function buildCommandRegistry(): Record<string, Record<string, CommandRegistryEntry>> {
	const registry: Record<string, Record<string, CommandRegistryEntry>> = {};
	for (const [resource, actions,] of Object.entries(commands,)) {
		registry[resource] = {};
		for (const [action, meta,] of Object.entries(actions,)) {
			registry[resource][action] = buildRegistryEntry(resource, action, meta,);
		}
	}
	registry.commands = {
		run: buildRegistryEntry("commands", "run", {
			handler: async () => undefined,
			usage: COMMANDS_USAGE,
			description: COMMANDS_DESCRIPTION,
			examples: COMMANDS_EXAMPLES,
		},),
	};
	registry["install-skill"] = {
		run: buildRegistryEntry("install-skill", "run", {
			handler: async () => undefined,
			usage: INSTALL_SKILL_USAGE,
			description: INSTALL_SKILL_DESCRIPTION,
			examples: INSTALL_SKILL_EXAMPLES,
		},),
	};
	registry.cleanup = {
		run: buildRegistryEntry("cleanup", "run", {
			handler: async () => undefined,
			usage: CLEANUP_USAGE,
			description: CLEANUP_DESCRIPTION,
			examples: CLEANUP_EXAMPLES,
		},),
	};
	registry.fixtures = {
		run: buildRegistryEntry("fixtures", "run", {
			handler: async () => undefined,
			usage: FIXTURES_USAGE,
			description: FIXTURES_DESCRIPTION,
			examples: FIXTURES_EXAMPLES,
		},),
	};
	registry.auth = {};
	for (const [action, meta,] of Object.entries(AUTH_ACTIONS,)) {
		registry.auth[action] = buildRegistryEntry("auth", action, {
			handler: async () => undefined,
			usage: meta.usage,
			description: meta.description,
			examples: meta.examples,
		},);
	}
	return registry;
}

function exitCodesOnFailure(entry: CommandRegistryEntry,): Record<string, number> {
	return {
		usage: entry.exitCodes.usage,
		error: entry.exitCodes.error,
		transient: entry.exitCodes.transient,
		...(entry.exitCodes.longRunningFailure !== undefined
			? { longRunningFailure: entry.exitCodes.longRunningFailure, }
			: {}),
	};
}

function projectKeyForPlan(
	entry: CommandRegistryEntry,
	flags: Record<string, string | boolean>,
): string | undefined {
	if (!entry.requiresProject) return undefined;
	const projectKey = resolveCredentials(flags,).projectKey;
	if (projectKey) return projectKey;
	throw new UsageError(
		`Missing project key. Pass --project-key or set DATAIKU_PROJECT_KEY before planning ${entry.resource} ${entry.action}.`,
	);
}

function requiredPlanFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string {
	const value = flags[name];
	if (typeof value === "string" && value.trim().length > 0) return value;
	throw new UsageError(`--${name} is required. Usage: ${usage}`,);
}

function optionalJsonFlag(
	flags: Record<string, string | boolean>,
	name: string,
): Record<string, unknown> | undefined {
	const value = flags[name];
	return typeof value === "string" ? parseJsonObject(value, `--${name}`,) : undefined;
}

function requiredPlanJsonInput(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	return requiredJsonInput(flags, `--data, --data-file, or --stdin is required. Usage: ${usage}`,);
}

function requiredPlanPositionals(usage: string,): string[] {
	return [...stripOptionalUsageGroups(usage,).matchAll(/<([^>]+)>/g,),].map((match,) => match[1]!);
}

function dataQualityEndpoint(projectKey: string, datasetName: string, suffix: string,): string {
	return encodedProjectEndpointForPlan(
		projectKey,
		`/datasets/${encodeURIComponent(datasetName,)}/data-quality${suffix}`,
	);
}

function querySuffix(params: Record<string, string | number | boolean | undefined>,): string {
	const search = new URLSearchParams();
	for (const [key, value,] of Object.entries(params,)) {
		if (value !== undefined) search.set(key, String(value,),);
	}
	const raw = search.toString();
	return raw ? `?${raw}` : "";
}

function jobBuildPayload(
	target: string,
	projectKey: string,
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const targetType = jobBuildTargetTypeFromFlags(flags,);
	const partition = flags["partition"] as string | undefined;
	const output: Record<string, unknown> = { projectKey, id: target, type: targetType, };
	if (targetType === "DATASET") {
		if (partition !== undefined) output.partition = partition;
	} else {
		output.targetManagedFolderProjectKey = projectKey;
		output.targetManagedFolder = target;
		output.targetPartition = partition ?? "NP";
	}
	const payload: Record<string, unknown> = {
		outputs: [output,],
		type: (flags["build-mode"] as string | undefined) ?? "NON_RECURSIVE_FORCED_BUILD",
	};
	if (flags["force-rebuild"] === true && targetType === "DATASET") {
		payload.autoUpdateSchemaBeforeEachRecipeRun = true;
	}
	return payload;
}

function commandPlanShape(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
	projectKey: string | undefined,
): {
	endpoint?: string;
	identifiers?: Record<string, unknown>;
	method?: string;
	payload?: unknown;
	localWrites?: unknown;
	wait?: unknown;
} {
	const projectEndpoint = (suffix: string,) => {
		if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
		return encodedProjectEndpointForPlan(projectKey, suffix,);
	};
	const id = args[0];
	switch (`${resource}.${action}`) {
		case "wiki.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/wiki/",),
				identifiers: { name, },
				payload: {
					projectKey,
					name,
					parent: flags["parent"] as string | undefined ?? null,
					content: textInput(flags,),
				},
			};
		}
		case "wiki.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
				payload: {
					...jsonInput(flags,),
					name: flags["name"] as string | undefined,
					content: textInput(flags,),
				},
			};
		case "wiki.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
			};
		case "dashboard.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/dashboards/",),
				identifiers: { name, },
				payload: { ...(jsonInput(flags,) ?? { pages: [], }), name, },
			};
		}
		case "dashboard.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: { ...jsonInput(flags,), name: flags["name"] as string | undefined, },
			};
		case "dashboard.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "insight.create": {
			const data = jsonInput(flags,);
			const name = flags["name"] as string | undefined;
			const type = flags["type"] as string | undefined;
			if (!data && (!name || !type)) {
				throw new UsageError(
					"--data or both --name and --type are required. Usage: dss insight create --name NAME --type TYPE",
				);
			}
			const prototype: Record<string, unknown> = { ...data, };
			if (name !== undefined) prototype.name = name;
			if (type !== undefined) prototype.type = type;
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			if (listed !== undefined) prototype.listed = listed;
			const params = optionalJsonFlag(flags, "params",);
			if (params !== undefined) prototype.params = params;
			return {
				method: "POST",
				endpoint: projectEndpoint("/insights/",),
				identifiers: { name, type, },
				payload: {
					insightPrototype: prototype,
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		}
		case "insight.update":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: {
					insight: {
						...jsonInput(flags,),
						name: flags["name"] as string | undefined,
						listed: parseBooleanOption(flags["listed"], "--listed",),
						params: optionalJsonFlag(flags, "params",),
					},
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		case "insight.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "data-quality.create-rule":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(projectKey!, args[0], "/rules",),
				identifiers: { dataset: args[0], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.update-rule":
			return {
				method: "PUT",
				endpoint: dataQualityEndpoint(projectKey!, args[0], `/rules/${encodeURIComponent(args[1],)}`,),
				identifiers: { dataset: args[0], ruleId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.delete-rule":
			return {
				method: "DELETE",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/rules/${encodeURIComponent(args[1],)}${querySuffix({ ruleId: args[1], },)}`,
				),
				identifiers: { dataset: args[0], ruleId: args[1], },
			};
		case "data-quality.compute":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/actions/compute-rules${
						querySuffix({
							partition: (flags["partition"] as string | undefined) ?? "NP",
							ruleId: flags["rule-id"] as string | undefined,
						},)
					}`,
				),
				identifiers: { dataset: args[0], ruleId: flags["rule-id"] as string | undefined, },
				wait: flags["wait"] === true,
			};
		case "future.abort":
			return {
				method: "POST",
				endpoint: `/public/api/futures/${encodeURIComponent(id,)}/abort`,
				identifiers: { id, },
			};
		case "flow-zone.create": {
			const name = flowZoneName(flags["name"],);
			const payload = { name, color: flowZoneColor(flags["color"],), projectKey, };
			return {
				method: "POST",
				endpoint: projectEndpoint("/flow/zones",),
				identifiers: { name, },
				payload,
			};
		}
		case "flow-zone.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
				payload: {
					name: typeof flags["name"] === "string" ? flowZoneName(flags["name"],) : undefined,
					color: flowZoneColor(flags["color"],),
					projectKey,
				},
			};
		case "flow-zone.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "flow-zone.move":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}/add-items`,),
				identifiers: { id, },
				payload: flowZoneMoveItems(flags,),
			};
		case "dataset.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			const dsType = requiredPlanFlag(flags, "type", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/",),
				identifiers: { name, },
				payload: { datasetName: name, connection, dsType, projectKey, },
			};
		}
		case "dataset.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "dataset.refresh-schema": {
			const columns = schemaColumnsInput(flags, entry.usage,);
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}/schema`,),
				identifiers: { name: id, },
				payload: { columns, },
			};
		}
		case "dataset.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "recipe.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "recipe.create": {
			const type = requiredPlanFlag(flags, "type", entry.usage,);
			const outputDataset = flags["output"] as string | undefined;
			const outputFolder = flags["output-folder"] as string | undefined;
			if (outputDataset && outputFolder) {
				throw new UsageError("--output and --output-folder are mutually exclusive.",);
			}
			if (!outputDataset && !outputFolder) {
				throw new UsageError("--output or --output-folder is required.",);
			}
			if (outputFolder && !flags["output-connection"]) {
				throw new UsageError("--output-connection is required when using --output-folder.",);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/recipes/",),
				identifiers: { name: flags["name"] as string | undefined, },
				payload: {
					type,
					name: flags["name"] as string | undefined,
					inputDatasets: flags["input"] ? [flags["input"] as string,] : undefined,
					outputDataset,
					outputFolder,
					outputConnection: flags["output-connection"] as string | undefined,
					projectKey,
				},
			};
		}
		case "recipe.run":
			return {
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { recipe: id, },
				payload: {
					recipe: id,
					outputResolution: "dynamic",
					projectKey,
					partition: flags["partition"] as string | undefined,
				},
				wait: recipeRunShouldWait(flags,),
			};
		case "recipe.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "recipe.set-payload": {
			const file = requiredPlanFlag(flags, "file", entry.usage,);
			const backupDir = flags["no-backup"] === true
				? undefined
				: (flags["backup-dir"] as string | undefined)
					?? join(process.cwd(), ".dss-backups", "recipes",);
			const backupPath = backupDir ? recipeBackupPath(id, backupDir,) : undefined;
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: {
					file,
					content: textInput(flags,),
					...(backupPath ? { backupPath, } : {}),
				},
				...(backupPath
					? { localWrites: [{ path: backupPath, source: "remote recipe backup", before: "PUT", },], }
					: {}),
			};
		}
		case "job.build":
		case "job.build-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { target: id, },
				payload: jobBuildPayload(id, projectKey!, flags,),
				wait: action === "build-and-wait" || flags["wait"] === true,
			};
		case "job.abort":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/jobs/${encodeURIComponent(id,)}/abort/`,),
				identifiers: { id, },
			};
		case "scenario.run":
		case "scenario.run-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/run/`,),
				identifiers: { id, },
				payload: {},
				wait: action === "run-and-wait" || flags["wait"] === true,
			};
		case "scenario.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "scenario.create":
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: { id: args[0], name: args[1], },
				payload: {
					id: args[0],
					name: args[1],
					projectKey,
					type: (flags["type"] as string | undefined) ?? "step_based",
				},
			};
		case "scenario.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const type = requiredPlanFlag(flags, "type", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/managedfolders/",),
				identifiers: { name, },
				payload: { name, type, connection, path: flags["path"] as string | undefined, projectKey, },
			};
		}
		case "folder.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
			};
		case "folder.upload":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], localPath: args[2], },
			};
		case "folder.delete-file":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], },
			};
		case "variable.set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/variables/",),
				payload: {
					standard: optionalJsonFlag(flags, "standard",),
					local: optionalJsonFlag(flags, "local",),
					replace: flags["replace"] === true,
				},
			};
		case "code-env.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/code-envs/",
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					envLang: args[0],
					envName: args[1],
					deploymentMode: requiredPlanFlag(flags, "deployment-mode", entry.usage,),
					params: codeEnvParams(flags,),
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.set-definition":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}`,
				identifiers: { lang: args[0], name: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "code-env.set-packages":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}/packages`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					packages: codeEnvPackageList(flags,),
					installCorePackages: parseBooleanOption(
						flags["install-core-packages"],
						"--install-core-packages",
					),
				},
			};
		case "code-env.update-packages":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}/packages/actions/update`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					forceRebuildEnv: flags["force-rebuild"] === true,
					versionToUpdate: flags["env-version"] as string | undefined,
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.set-jupyter":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}/jupyter`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					active: parseBooleanOption(flags["active"], "--active",),
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}`,
				identifiers: { lang: args[0], name: args[1], },
				wait: codeEnvWait(flags,),
			};
		case "notebook.save-jupyter":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "notebook.delete-jupyter":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "notebook.clear-jupyter-outputs":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: { clearOutputs: true, },
			};
		case "notebook.unload-jupyter":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/jupyter-notebooks/${encodeURIComponent(args[0],)}/sessions/${
						encodeURIComponent(args[1],)
					}/unload`,
				),
				identifiers: { name: args[0], sessionId: args[1], },
			};
		case "notebook.save-sql":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "notebook.delete-sql":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "notebook.clear-sql-history":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}/history`,),
				identifiers: { id, cellId: flags["cell-id"] as string | undefined, },
				payload: { retain: num(flags["retain"],), },
			};
		default:
			return {
				method: action.startsWith("delete",) || action === "abort" ? "DELETE" : "POST",
				endpoint: projectKey
					? projectEndpoint(`/${resource}s/${id ? encodeURIComponent(id,) : ""}`,)
					: undefined,
				identifiers: id ? { id, } : undefined,
				payload: jsonInput(flags,),
			};
	}
}

function buildMutationPlan(
	resource: string,
	action: string,
	meta: CommandMeta,
	args: string[],
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const entry = buildRegistryEntry(resource, action, meta,);
	if (!entry.mutatesDss && entry.sideEffect !== "write") {
		throw new UsageError(`--plan is only supported for mutating commands. Usage: ${meta.usage}`,);
	}
	const requiredPositionals = requiredPlanPositionals(meta.usage,);
	requireArgs(args, requiredPositionals.length, meta.usage,);
	const projectKey = projectKeyForPlan(entry, flags,);
	const shape = commandPlanShape(resource, action, args, flags, entry, projectKey,);
	return planResult(resource, action, {
		...shape,
		asyncKind: entry.async,
		exitCodesOnFailure: exitCodesOnFailure(entry,),
		idempotency: entry.idempotency,
		plannedAndDryRun: flags["dry-run"] === true,
	},);
}

async function runCleanup(flags: Record<string, string | boolean>,): Promise<{
	result: Record<string, unknown>;
	exitCode: number;
}> {
	const filePath = flags["file"];
	if (typeof filePath !== "string" || filePath.trim().length === 0) {
		throw new UsageError(`--file is required. Usage: ${CLEANUP_USAGE}`,);
	}
	let entries: CleanupLedgerEntry[];
	try {
		entries = await readCleanupLedger(filePath,);
	} catch (error) {
		throw new UsageError(
			`Could not read cleanup ledger: ${error instanceof Error ? error.message : String(error,)}`,
		);
	}
	const ordered: CleanupLedgerEntry[] = [];
	for (let index = entries.length - 1; index >= 0; index--) ordered.push(entries[index]!,);
	const steps = ordered.map((entry, index,) => ({
		index,
		resource: entry.resource,
		action: entry.action,
		id: entry.id,
		name: entry.name,
		path: entry.path,
		projectKey: entry.projectKey,
		cleanup: entry.cleanup,
	}));
	if (flags["apply"] !== true) {
		return { result: { dryRun: true, steps, }, exitCode: 0, };
	}

	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	if (!url) {
		throw new UsageError("Missing Dataiku URL. Set DATAIKU_URL, pass --url, or run: dss auth login",);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY, pass --api-key, or run: dss auth login",
		);
	}
	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],);
	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);

	const applied: Array<Record<string, unknown>> = [];
	const failures: Array<Record<string, unknown>> = [];
	for (const [index, entry,] of ordered.entries()) {
		try {
			const parsed = parseArgs(entry.cleanup.argv,);
			const [resource, action, ...args] = parsed.positional;
			if (!resource || !action || !commands[resource]?.[action]) {
				throw new UsageError(`Invalid cleanup argv: ${entry.cleanup.argv.join(" ",)}`,);
			}
			const result = await commands[resource][action].handler(client, args, parsed.flags,);
			applied.push({ index, cleanup: entry.cleanup, result, },);
		} catch (error) {
			const failure = {
				index,
				cleanup: entry.cleanup,
				error: error instanceof Error ? error.message : String(error,),
			};
			failures.push(failure,);
			if (flags["continue-on-error"] !== true) {
				return {
					result: { applied: true, steps, results: applied, failures, },
					exitCode: 2,
				};
			}
		}
	}
	return {
		result: { applied: true, steps, results: applied, failures, },
		exitCode: failures.length > 0 ? 2 : 0,
	};
}

// ---------------------------------------------------------------------------
// Interactive prompts
// ---------------------------------------------------------------------------

function promptLine(label: string,): Promise<string> {
	return new Promise((res, rej,) => {
		const rl = createInterface({ input: process.stdin, output: process.stderr, },);
		rl.on("close", () => rej(new UsageError("Input closed before a value was provided.",),),);
		rl.question(label, (answer,) => {
			rl.close();
			res(answer.trim(),);
		},);
	},);
}

function promptSecret(label: string,): Promise<string> {
	return new Promise((res, rej,) => {
		const muted = new Writable({
			write(_chunk, _encoding, cb,) {
				cb();
			},
		},);
		const rl = createInterface({ input: process.stdin, output: muted, terminal: true, },);
		rl.on("close", () => rej(new UsageError("Input closed before a value was provided.",),),);
		process.stderr.write(label,);
		rl.question("", (answer,) => {
			rl.close();
			process.stderr.write("\n",);
			res(answer.trim(),);
		},);
	},);
}

// ---------------------------------------------------------------------------
// Credential resolution
// ---------------------------------------------------------------------------

function resolveCredentials(flags: Record<string, string | boolean>,): {
	url: string;
	apiKey: string;
	projectKey?: string;
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
} {
	const hasUrlFlag = Object.hasOwn(flags, "url",);
	const hasApiKeyFlag = Object.hasOwn(flags, "api-key",);
	const hasProjectKeyFlag = Object.hasOwn(flags, "project-key",);
	let url = hasUrlFlag ? flags["url"] as string | undefined : undefined;
	let apiKey = hasApiKeyFlag ? flags["api-key"] as string | undefined : undefined;
	let projectKey = hasProjectKeyFlag ? flags["project-key"] as string | undefined : undefined;
	const saved = loadCredentials();

	if (!hasUrlFlag) url ??= process.env.DATAIKU_URL;
	if (!hasApiKeyFlag) apiKey ??= process.env.DATAIKU_API_KEY;
	if (!hasProjectKeyFlag) projectKey ??= process.env.DATAIKU_PROJECT_KEY;

	if (saved) {
		if (!hasUrlFlag) url ??= saved.url;
		if (!hasApiKeyFlag) apiKey ??= saved.apiKey;
		if (!hasProjectKeyFlag) projectKey ??= saved.projectKey;
	}

	return {
		url: url ?? "",
		apiKey: apiKey ?? "",
		projectKey,
		...resolveTlsSettings(flags, saved ?? undefined,),
	};
}

interface ErrorReportEnvelope {
	code: StableErrorCode;
	category: "usage" | "dss" | "internal";
	message: string;
	hint?: string;
	resource?: string;
	action?: string;
	projectKey?: string;
	requestId?: string;
	status?: number;
	retryable?: boolean;
	details?: Record<string, unknown>;
}

let currentCommandContext: { resource?: string; action?: string; projectKey?: string; } = {};

function isReportJsonRequested(): boolean {
	return process.env.DSS_REPORT_JSON === "1"
		|| process.argv.slice(2,).some((arg,) =>
			arg === "--report-json" || arg.startsWith("--report-json=",)
		);
}

function rawFlagValue(argv: string[], flagName: string,): string | undefined {
	const longFlag = `--${flagName}`;
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === longFlag) {
			const next = argv[index + 1];
			return next && !next.startsWith("-",) ? next : undefined;
		}
		if (arg.startsWith(`${longFlag}=`,)) return arg.slice(longFlag.length + 1,);
	}
	return undefined;
}

function rawCommandContext(): { resource?: string; action?: string; projectKey?: string; } {
	const argv = process.argv.slice(2,);
	const positionals: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--") {
			positionals.push(...argv.slice(index + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const name = arg.slice(2,).split("=",)[0] ?? "";
			const canonical = FLAG_ALIASES[name] ?? name;
			if (!arg.includes("=",) && VALUE_FLAGS.has(canonical,)) index++;
			continue;
		}
		if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long && VALUE_FLAGS.has(long,)) index++;
			continue;
		}
		positionals.push(arg,);
	}
	return {
		resource: currentCommandContext.resource ?? positionals[0],
		action: currentCommandContext.action ?? positionals[1],
		projectKey: currentCommandContext.projectKey
			?? rawFlagValue(argv, "project-key",)
			?? rawFlagValue(argv, "project",)
			?? process.env.DATAIKU_PROJECT_KEY,
	};
}

function requestIdFromBody(body: string,): string | undefined {
	try {
		const parsed = JSON.parse(body,) as Record<string, unknown>;
		const value = parsed.requestId ?? parsed.request_id ?? parsed.errorId;
		return typeof value === "string" && value.length > 0 ? value : undefined;
	} catch {
		return undefined;
	}
}

function buildErrorReport(err: unknown,): ErrorReportEnvelope {
	const context = rawCommandContext();
	if (err instanceof UsageError) {
		return {
			code: err.code,
			category: "usage",
			message: err.message,
			...(err.hint ? { hint: err.hint, } : {}),
			...context,
		};
	}
	if (err instanceof DataikuError) {
		return {
			code: dataikuErrorCode(err.category,),
			category: "dss",
			message: err.message,
			hint: err.retryHint,
			status: err.status,
			retryable: err.retryable,
			requestId: requestIdFromBody(err.body,),
			details: {
				dssCategory: err.category,
				statusText: err.statusText,
				body: err.body,
				...(err.retry ? { retry: err.retry, } : {}),
			},
			...context,
		};
	}
	const message = err instanceof Error ? err.message : String(err,);
	return {
		code: "internal_error",
		category: "internal",
		message,
		...context,
	};
}

function writeErrorReport(err: unknown,): void {
	process.stderr.write(`${JSON.stringify(buildErrorReport(err,), null, 2,)}\n`,);
}

function commandRegistryEntry(resource: string, action: string,): CommandRegistryEntry | undefined {
	return buildCommandRegistry()[resource]?.[action];
}

function writeReportHelp(resource: string, action: string,): void {
	const entry = commandRegistryEntry(resource, action,);
	if (entry) {
		process.stderr.write(`${JSON.stringify(entry, null, 2,)}\n`,);
		return;
	}
	process.stderr.write(`${
		JSON.stringify(
			{
				code: "usage_error",
				category: "usage",
				message: `No registry entry for ${resource} ${action}.`,
				resource,
				action,
			},
			null,
			2,
		)
	}\n`,);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	loadEnvFile();
	const { positional, flags, } = parseArgs(process.argv.slice(2,),);

	// --version
	if (flags["version"] === true) {
		process.stdout.write(`${CLI_VERSION_LABEL}\n`,);
		process.exit(0,);
	}

	// Top-level help
	if (positional.length === 0 || (positional.length === 0 && flags["help"])) {
		printTopLevelHelp();
		if (flags["help"]) process.exit(0,);
		process.exit(1,);
	}

	const resource = positional[0];
	currentCommandContext = {
		resource,
		action: positional[1],
		projectKey: typeof flags["project-key"] === "string"
			? flags["project-key"]
			: process.env.DATAIKU_PROJECT_KEY,
	};

	if (resource === "doctor") {
		const action = positional[1];
		if (flags["help"] === true) {
			if (flags["report-json"] === true) writeReportHelp("doctor", "run",);
			else printActionHelp("doctor", "run",);
			process.exit(0,);
		}
		if (action !== undefined && action !== "run") {
			throw new UsageError("Usage: dss doctor [--project-key KEY] [--capabilities] [--fast]",);
		}
		const { result, exitCode, } = await runDoctor(flags,);
		writeCommandResult(result,);
		process.exit(exitCode,);
	}

	// Auth commands — dispatched before client creation
	if (resource === "auth") {
		const action = positional[1];
		if (!action) {
			const maxName = Math.max(...Object.keys(AUTH_ACTIONS,).map((n,) => n.length),);
			const lines = [
				"Usage: dss auth <action> [--flags]",
				"",
				"Actions:",
				...Object.entries(AUTH_ACTIONS,).map(
					([name, meta,],) => `  ${name.padEnd(maxName + 2,)}${meta.description ?? meta.usage}`,
				),
				"",
				"Run 'dss auth <action> --help' for details and examples.",
			];
			process.stderr.write(`${lines.join("\n",)}\n`,);
			process.exit(flags["help"] === true ? 0 : 1,);
		}
		const authMeta = AUTH_ACTIONS[action];
		if (!authMeta) {
			if (flags["report-json"] === true) {
				throw new UsageError(
					`Unknown action: auth ${action}. Available: ${Object.keys(AUTH_ACTIONS,).join(", ",)}`,
				);
			}
			process.stderr.write(
				`Unknown action: auth ${action}\nAvailable: ${Object.keys(AUTH_ACTIONS,).join(", ",)}\n`,
			);
			process.exit(1,);
		}
		if (flags["help"] === true) {
			if (flags["report-json"] === true) {
				writeReportHelp("auth", action,);
			} else {
				const lines: string[] = [];
				if (authMeta.description) lines.push(authMeta.description, "",);
				lines.push(`Usage: ${authMeta.usage}`,);
				if (authMeta.examples && authMeta.examples.length > 0) {
					lines.push("", "Examples:",);
					for (const ex of authMeta.examples) lines.push(`  ${ex}`,);
				}
				process.stderr.write(`${lines.join("\n",)}\n`,);
			}
			process.exit(0,);
		}
		await authMeta.handler(flags,);
		return;
	}

	// install-skill — dispatched before client creation
	if (resource === "install-skill") {
		const installSkillAction = positional[1];
		if (flags["help"] === true) {
			if (flags["report-json"] === true) {
				writeReportHelp("install-skill", "run",);
			} else {
				const lines = [
					`Usage: ${INSTALL_SKILL_USAGE}`,
					"",
					INSTALL_SKILL_DESCRIPTION,
					"",
					"Flags:",
					"  --global         Install to user-level global scope (default: project)",
					"  --agent NAME     Target a specific agent: claude, codex, cursor, pi, omp",
					"  --target PATH    Project directory to install into (default: workspace root)",
					"  --list-agents    Print detected agents and exit",
					"  --dry-run        Print planned skill installs without writing files",
					"  --plan           Print planned skill installs without writing files",
				];
				process.stderr.write(`${lines.join("\n",)}\n`,);
			}
			process.exit(0,);
		}
		if (installSkillAction !== undefined && installSkillAction !== "run") {
			throw new UsageError(`Usage: ${INSTALL_SKILL_USAGE}`,);
		}

		const listOnly = flags["list-agents"] === true;
		const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
		const isGlobal = flags["global"] === true;
		const targetDir = typeof flags["target"] === "string" ? flags["target"] : undefined;

		// Resolve target agents
		let targets;
		if (agentFilter) {
			const def = AGENTS[agentFilter];
			if (!def) {
				throw new UsageError(
					`Unknown agent: ${agentFilter}. Available: ${Object.keys(AGENTS,).join(", ",)}`,
				);
			}
			targets = [{ id: agentFilter, def, via: "flag" as const, },];
		} else {
			targets = detectAgents();
		}

		if (listOnly) {
			if (targets.length === 0) {
				process.stderr.write("No coding agents detected.\n",);
			} else {
				process.stderr.write("Detected agents:\n",);
				for (const t of targets) {
					process.stderr.write(`  ${t.id}  (${t.def.name}, via ${t.via})\n`,);
				}
			}
			process.exit(0,);
		}

		if (targets.length === 0) {
			throw new UsageError(
				"No coding agents detected. Install one (claude, codex, cursor, pi, omp) or use --agent NAME.",
			);
		}

		const scope = isGlobal ? "global" : "project";
		const cwd = targetDir ?? (isGlobal ? process.cwd() : findWorkspaceRoot(process.cwd(),));
		if (flags["plan"] === true) {
			writeCommandResult(planResult("install-skill", "run", {
				identifiers: { scope, target: cwd, },
				payload: {
					agents: targets.map((target,) => ({
						id: target.id,
						name: target.def.name,
						via: target.via,
					})),
				},
				idempotency: "none",
				asyncKind: "none",
				exitCodesOnFailure: { usage: 1, error: 2, transient: 3, },
				plannedAndDryRun: flags["dry-run"] === true,
			},),);
			return;
		}
		if (flags["dry-run"] === true) {
			writeCommandResult({
				dryRun: true,
				action: "install-skill",
				resource: "install-skill",
				scope,
				target: cwd,
				agents: targets.map((target,) => ({
					id: target.id,
					name: target.def.name,
					via: target.via,
				})),
			},);
			return;
		}
		process.stderr.write(`Installing dataiku-dss skill (${scope} scope):\n`,);
		const results = installSkill(targets, { global: isGlobal, cwd, },);

		for (const r of results) {
			process.stderr.write(`  ${r.agent}  ->  ${r.path}\n`,);
		}
		if (results.length > 0) {
			process.stderr.write(`\nDone. ${results.length} skill(s) installed.\n`,);
		}
		return;
	}

	// commands — machine-readable introspection (no auth needed)
	if (resource === "commands") {
		const action = positional[1];
		if (flags["help"] === true) {
			if (flags["report-json"] === true) {
				writeReportHelp("commands", "run",);
			} else {
				const lines = [
					`Usage: ${COMMANDS_USAGE}`,
					"",
					COMMANDS_DESCRIPTION,
					"",
					"Examples:",
					...COMMANDS_EXAMPLES.map((example,) => `  ${example}`),
				];
				process.stderr.write(`${lines.join("\n",)}\n`,);
			}
			process.exit(0,);
		}
		if (action !== undefined && action !== "run") {
			throw new UsageError(`Usage: ${COMMANDS_USAGE}`,);
		}
		writeCommandResult(buildCommandRegistry(),);
		return;
	}

	if (resource === "cleanup") {
		const action = positional[1];
		if (flags["help"] === true) {
			if (flags["report-json"] === true) {
				writeReportHelp("cleanup", "run",);
			} else {
				const lines = [
					`Usage: ${CLEANUP_USAGE}`,
					"",
					CLEANUP_DESCRIPTION,
					"",
					"Examples:",
					...CLEANUP_EXAMPLES.map((example,) => `  ${example}`),
				];
				process.stderr.write(`${lines.join("\n",)}\n`,);
			}
			process.exit(0,);
		}
		if (action !== undefined && action !== "run") {
			throw new UsageError(`Usage: ${CLEANUP_USAGE}`,);
		}
		const { result, exitCode, } = await runCleanup(flags,);
		writeCommandResult(result,);
		process.exit(exitCode,);
	}

	if (resource === "fixtures") {
		const action = positional[1];
		currentCommandContext.action = "run";
		if (flags["help"] === true) {
			if (flags["report-json"] === true) {
				writeReportHelp("fixtures", "run",);
			} else {
				const lines = [
					`Usage: ${FIXTURES_USAGE}`,
					"",
					FIXTURES_DESCRIPTION,
					"",
					"Examples:",
					...FIXTURES_EXAMPLES.map((example,) => `  ${example}`),
				];
				process.stderr.write(`${lines.join("\n",)}\n`,);
			}
			process.exit(0,);
		}
		if (action !== undefined && action !== "run") {
			throw new UsageError(`Usage: ${FIXTURES_USAGE}`,);
		}
		const result = await runFixtures(flags,);
		writeCommandResult(result,);
		return;
	}

	// Unknown resource
	if (!commands[resource]) {
		if (flags["help"]) {
			printTopLevelHelp();
			process.exit(0,);
		}
		if (flags["report-json"] === true) {
			throw new UsageError(`Unknown resource: ${resource}. Available: ${RESOURCE_NAMES.join(", ",)}`,);
		}
		process.stderr.write(
			`Unknown resource: ${resource} \nAvailable: ${RESOURCE_NAMES.join(", ",)} \n`,
		);
		process.exit(1,);
	}

	// Resource-level help
	if (positional.length === 1 || flags["help"] === true) {
		if (positional.length === 1) {
			printResourceHelp(resource,);
			if (flags["help"]) process.exit(0,);
			process.exit(1,);
		}
	}

	const action = positional[1];
	const actionMeta = commands[resource][action];

	// Unknown action
	if (!actionMeta) {
		if (flags["report-json"] === true) {
			throw new UsageError(
				`Unknown action: ${resource} ${action}. Available actions for ${resource}: ${
					Object.keys(commands[resource],).join(", ",)
				}`,
			);
		}
		process.stderr.write(
			`Unknown action: ${resource} ${action} \nAvailable actions for ${resource}: ${
				Object.keys(commands[resource],).join(", ",)
			} \n`,
		);
		process.exit(1,);
	}

	// Action-level help
	if (flags["help"] === true) {
		if (flags["report-json"] === true) writeReportHelp(resource, action,);
		else printActionHelp(resource, action,);
		process.exit(0,);
	}

	const args = positional.slice(2,);
	if (flags["plan"] === true) {
		const plan = buildMutationPlan(resource, action, actionMeta, args, flags,);
		writeCommandResult(plan,);
		return;
	}
	// Resolve credentials: flags > env > saved > .env
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	currentCommandContext.projectKey = projectKey;

	if (!url) {
		throw new UsageError("Missing Dataiku URL. Set DATAIKU_URL, pass --url, or run: dss auth login",);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY, pass --api-key, or run: dss auth login",
		);
	}

	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],);

	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);

	if (typeof flags["record-cleanup"] === "string" && flags["dry-run"] !== true) {
		if (!supportsCleanupLedger(resource, action,)) {
			throw new UsageError(`--record-cleanup is not supported for ${resource} ${action}.`,);
		}
	}
	const result = await actionMeta.handler(client, args, flags,);
	if (typeof flags["record-cleanup"] === "string" && flags["dry-run"] !== true) {
		const entry = cleanupLedgerEntry(resource, action, args, flags, result, projectKey,);
		if (entry) await appendCleanupLedgerEntry(flags["record-cleanup"], entry,);
	}
	if (flags["raw"] === true && typeof result === "string") {
		process.stdout.write(result,);
	} else {
		writeCommandResult(result,);
	}
	const failureExitCode = commandFailureExitCode(result,);
	if (failureExitCode !== undefined) process.exit(failureExitCode,);
}

main().catch((err: unknown,) => {
	if (isReportJsonRequested()) {
		writeErrorReport(err,);
		if (err instanceof UsageError) process.exit(1,);
		if (err instanceof DataikuError) process.exit(err.category === "transient" ? 3 : 2,);
		process.exit(2,);
	}
	if (err instanceof UsageError) {
		process.stderr.write(`${JSON.stringify({ error: err.message, code: "usage", }, null, 2,)}\n`,);
		process.exit(1,);
	}
	if (err instanceof DataikuError) {
		const payload: Record<string, unknown> = {
			error: err.message,
			category: err.category,
			retryable: err.retryable,
		};
		if (err.retryHint) payload.retryHint = err.retryHint;
		process.stderr.write(`${JSON.stringify(payload, null, 2,)} \n`,);
		process.exit(err.category === "transient" ? 3 : 2,);
	}
	const message = err instanceof Error ? err.message : String(err,);
	process.stderr.write(`${JSON.stringify({ error: message, }, null, 2,)} \n`,);
	process.exit(1,);
},);
