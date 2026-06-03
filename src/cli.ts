#!/usr/bin/env node

import { createHash, } from "node:crypto";
import { readFileSync, writeFileSync, } from "node:fs";
import { mkdir, writeFile, } from "node:fs/promises";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { validateCredentials, } from "./auth.js";
import { DataikuClient, } from "./client.js";
import {
	type DssCredentials,
	getCredentialsPath,
	loadCredentials,
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
	FlowZonePosition,
	JobSummary,
} from "./schemas.js";
import {
	AGENTS,
	detectAgents,
	findWorkspaceRoot,
	installSkill,
	planSkillInstalls,
} from "./skill.js";
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
const CLI_GIT_REVISION = gitRevision(PACKAGE_ROOT,);
function cliVersionResult(): { version: string; gitRevision: string | null; } {
	return { version: CLI_VERSION, gitRevision: CLI_GIT_REVISION ?? null, };
}

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

interface FlowZoneOrganizeZonePlan {
	id?: string;
	name?: string;
	color?: string;
	position?: FlowZonePosition;
	items: FlowZoneItemInput[];
}

interface FlowZoneOrganizePlan {
	zones: FlowZoneOrganizeZonePlan[];
}

interface FlowZoneOrganizeValidationIssue {
	zone: string;
	objectId: string;
	objectType: FlowZoneObjectType;
	projectKey?: string;
	reason: string;
}

interface FlowZoneOrganizeValidationResult {
	valid: boolean;
	missing: FlowZoneOrganizeValidationIssue[];
}

interface FlowZoneOrganizeValidationIndex {
	projectKey: string;
	all: Set<string>;
	datasets: Set<string>;
	recipes: Set<string>;
	folders: Set<string>;
}

function optionalStringField(
	record: Record<string, unknown>,
	keys: string[],
): string | undefined {
	for (const key of keys) {
		const value = record[key];
		if (typeof value === "string" && value.trim().length > 0) return value.trim();
	}
	return undefined;
}

function requiredStringArray(value: unknown, source: string,): string[] {
	if (!Array.isArray(value,)) {
		throw new UsageError(`${source} must be an array of strings.`, "validation_failed",);
	}
	return value.map((item, index,) => {
		if (typeof item !== "string" || item.trim().length === 0) {
			throw new UsageError(`${source}[${index}] must be a non-empty string.`, "validation_failed",);
		}
		return item.trim();
	},);
}

function finiteNumberField(record: Record<string, unknown>, key: string, source: string,): number {
	const value = record[key];
	if (typeof value !== "number" || !Number.isFinite(value,)) {
		throw new UsageError(`${source}.${key} must be a finite number.`, "validation_failed",);
	}
	return value;
}

function flowZonePlanColor(value: unknown, source: string,): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !/^#[0-9a-fA-F]{6}$/.test(value.trim(),)) {
		throw new UsageError(`${source} must be a hex color like #2ab1ac.`, "validation_failed",);
	}
	return value.trim();
}

function flowZonePlanPosition(value: unknown, source: string,): FlowZonePosition | undefined {
	if (value === undefined) return undefined;
	const record = plainRecord(value,);
	if (!record) {
		throw new UsageError(`${source} must be an object with x and y.`, "validation_failed",);
	}
	return {
		x: finiteNumberField(record, "x", source,),
		y: finiteNumberField(record, "y", source,),
	};
}

function flowZoneCurrentPosition(zone: FlowZone,): FlowZonePosition | undefined {
	const record = zone as unknown as Record<string, unknown>;
	const position = plainRecord(record.position,);
	if (!position) return undefined;
	const x = position.x;
	const y = position.y;
	return typeof x === "number" && Number.isFinite(x,) && typeof y === "number" && Number.isFinite(y,)
		? { x, y, }
		: undefined;
}

function flowZoneSamePosition(
	a: FlowZonePosition | undefined,
	b: FlowZonePosition | undefined,
): boolean {
	if (a === undefined || b === undefined) return a === b;
	return a.x === b.x && a.y === b.y;
}

function parseFlowZonePlanItem(value: unknown, source: string,): FlowZoneItemInput {
	if (typeof value === "string") return parseFlowZoneObject(value,);
	const record = plainRecord(value,);
	if (!record) {
		throw new UsageError(`${source} must be TYPE:ID or an object.`, "validation_failed",);
	}
	const object = optionalStringField(record, ["object",],);
	if (object) return parseFlowZoneObject(object,);
	const objectType = optionalStringField(record, ["objectType", "type",],);
	const objectId = optionalStringField(record, ["objectId", "id", "name",],);
	if (!objectType || !objectId) {
		throw new UsageError(
			`${source} must include objectType/type and objectId/id, or object as TYPE:ID.`,
			"validation_failed",
		);
	}
	const projectKey = optionalStringField(record, ["projectKey", "project",],);
	return {
		objectType: flowZoneObjectType(objectType,),
		objectId,
		...(projectKey ? { projectKey, } : {}),
	};
}

function addFlowZonePlanTypedItems(
	items: FlowZoneItemInput[],
	record: Record<string, unknown>,
	key: string,
	objectType: FlowZoneObjectType,
	source: string,
): void {
	if (record[key] === undefined) return;
	for (const objectId of requiredStringArray(record[key], `${source}.${key}`,)) {
		items.push({ objectType, objectId, },);
	}
}

function flowZoneItemKey(item: FlowZoneItemInput,): string {
	return `${item.projectKey ?? ""}\0${item.objectType}\0${item.objectId}`;
}

function flowZonePlanLabel(plan: FlowZoneOrganizeZonePlan,): string {
	return plan.id ?? plan.name ?? "<unknown>";
}

function dedupeFlowZonePlanItems(items: FlowZoneItemInput[],): FlowZoneItemInput[] {
	const seen = new Set<string>();
	const result: FlowZoneItemInput[] = [];
	for (const item of items) {
		const key = flowZoneItemKey(item,);
		if (seen.has(key,)) continue;
		seen.add(key,);
		result.push(item,);
	}
	return result;
}

function flowZonePlanItemKeys(plan: FlowZoneOrganizePlan,): Set<string> {
	const keys = new Set<string>();
	for (const zone of plan.zones) {
		for (const item of zone.items) keys.add(flowZoneItemKey(item,),);
	}
	return keys;
}

function validateUniqueFlowZoneAssignments(plan: FlowZoneOrganizePlan,): void {
	const seen = new Map<string, string>();
	for (const zone of plan.zones) {
		const label = flowZonePlanLabel(zone,);
		for (const item of zone.items) {
			const key = flowZoneItemKey(item,);
			const previous = seen.get(key,);
			if (previous) {
				throw new UsageError(
					`Flow object ${item.objectType}:${item.objectId} is assigned to both "${previous}" and "${label}".`,
					"validation_failed",
				);
			}
			seen.set(key, label,);
		}
	}
}

function parseFlowZoneOrganizePlan(input: Record<string, unknown>,): FlowZoneOrganizePlan {
	const zones = input.zones;
	if (!Array.isArray(zones,) || zones.length === 0) {
		throw new UsageError(
			"Flow zone organize plan must include a non-empty zones array.",
			"validation_failed",
		);
	}
	const plan = {
		zones: zones.map((value, index,) => {
			const source = `zones[${index}]`;
			const record = plainRecord(value,);
			if (!record) throw new UsageError(`${source} must be an object.`, "validation_failed",);
			const id = optionalStringField(record, ["id", "zoneId",],);
			const name = optionalStringField(record, ["name",],);
			if (!id && !name) {
				throw new UsageError(`${source} must include name or id.`, "validation_failed",);
			}
			const items: FlowZoneItemInput[] = [];
			const rawItems = record.items ?? record.objects;
			if (rawItems !== undefined) {
				if (!Array.isArray(rawItems,)) {
					throw new UsageError(`${source}.items must be an array.`, "validation_failed",);
				}
				rawItems.forEach((item, itemIndex,) => {
					items.push(parseFlowZonePlanItem(item, `${source}.items[${itemIndex}]`,),);
				},);
			}
			addFlowZonePlanTypedItems(items, record, "datasets", "DATASET", source,);
			addFlowZonePlanTypedItems(items, record, "recipes", "RECIPE", source,);
			addFlowZonePlanTypedItems(items, record, "folders", "MANAGED_FOLDER", source,);
			addFlowZonePlanTypedItems(items, record, "savedModels", "SAVED_MODEL", source,);
			addFlowZonePlanTypedItems(
				items,
				record,
				"modelEvaluationStores",
				"MODEL_EVALUATION_STORE",
				source,
			);
			addFlowZonePlanTypedItems(
				items,
				record,
				"streamingEndpoints",
				"STREAMING_ENDPOINT",
				source,
			);
			addFlowZonePlanTypedItems(items, record, "labelingTasks", "LABELING_TASK", source,);
			addFlowZonePlanTypedItems(items, record, "knowledgeBanks", "RETRIEVABLE_KNOWLEDGE", source,);
			return {
				...(id ? { id, } : {}),
				...(name ? { name, } : {}),
				...(record.color !== undefined
					? { color: flowZonePlanColor(record.color, `${source}.color`,), }
					: {}),
				...(record.position !== undefined
					? { position: flowZonePlanPosition(record.position, `${source}.position`,), }
					: {}),
				items: dedupeFlowZonePlanItems(items,),
			};
		},),
	};
	validateUniqueFlowZoneAssignments(plan,);
	return plan;
}

function readFlowZoneOrganizePlan(
	flags: Record<string, string | boolean>,
	usage: string,
): FlowZoneOrganizePlan {
	const data = typeof flags["file"] === "string"
		? parseJsonObject(readFileSync(flags["file"], "utf-8",), flags["file"],)
		: jsonInput(flags,);
	if (!data) {
		throw new UsageError(
			`--data, --data-file, --file, or --stdin is required. Usage: ${usage}`,
			"missing_required_flag",
		);
	}
	return parseFlowZoneOrganizePlan(data,);
}

function findFlowZoneForPlan(
	zones: FlowZone[],
	plan: FlowZoneOrganizeZonePlan,
): FlowZone | undefined {
	if (plan.id) {
		const byId = zones.find((zone,) => zone.id === plan.id);
		if (byId) return byId;
	}
	if (!plan.name) return undefined;
	const byName = zones.filter((zone,) => zone.name === plan.name);
	if (byName.length > 1) {
		throw new UsageError(
			`Multiple flow zones named "${plan.name}" exist; use id.`,
			"validation_failed",
		);
	}
	return byName[0];
}

function ensureFlowZonePlanTarget(
	plan: FlowZoneOrganizeZonePlan,
	existing: FlowZone | undefined,
): void {
	if (existing || plan.name) return;
	throw new UsageError(
		`Flow zone ${plan.id ?? "<unknown>"} was not found and cannot be created without name.`,
		"validation_failed",
	);
}

function flowZoneExplicitItems(zone: FlowZone,): FlowZoneItemInput[] {
	return (zone.items ?? []).map((item,) => ({
		objectId: item.objectId,
		objectType: item.objectType,
		...(item.projectKey ? { projectKey: item.projectKey, } : {}),
	}));
}

function flowZonePruneItems(
	existing: FlowZone | undefined,
	plannedItemKeys: Set<string>,
): FlowZoneItemInput[] {
	if (!existing) return [];
	return flowZoneExplicitItems(existing,).filter((item,) =>
		!plannedItemKeys.has(flowZoneItemKey(item,),)
	);
}

function flowZoneOrganizeStep(
	plan: FlowZoneOrganizeZonePlan,
	existing: FlowZone | undefined,
	sync: boolean,
	plannedItemKeys: Set<string>,
): Record<string, unknown> {
	ensureFlowZonePlanTarget(plan, existing,);
	const update: Record<string, unknown> = {};
	if (existing && plan.name && plan.name !== existing.name) update.name = plan.name;
	if (existing && plan.color && plan.color !== existing.color) update.color = plan.color;
	if (
		existing && plan.position !== undefined
		&& !flowZoneSamePosition(flowZoneCurrentPosition(existing,), plan.position,)
	) {
		update.position = plan.position;
	}
	const pruneItems = sync ? flowZonePruneItems(existing, plannedItemKeys,) : [];
	return {
		target: {
			...(plan.id ? { id: plan.id, } : {}),
			...(plan.name ? { name: plan.name, } : {}),
			...(plan.color ? { color: plan.color, } : {}),
			...(plan.position ? { position: plan.position, } : {}),
		},
		...(existing ? { existing: flowZoneSummary(existing,), } : { create: true, }),
		...(Object.keys(update,).length > 0 ? { update, } : {}),
		moveItems: plan.items,
		...(pruneItems.length > 0 ? { pruneItems, } : {}),
	};
}

function flowZoneValidationBucket(
	index: FlowZoneOrganizeValidationIndex,
	objectType: FlowZoneObjectType,
): Set<string> {
	switch (objectType) {
		case "DATASET":
			return index.datasets;
		case "RECIPE":
			return index.recipes;
		case "MANAGED_FOLDER":
			return index.folders;
		case "SAVED_MODEL":
		case "MODEL_EVALUATION_STORE":
		case "STREAMING_ENDPOINT":
		case "LABELING_TASK":
		case "RETRIEVABLE_KNOWLEDGE":
			return index.all;
	}
}

async function flowZoneValidationIndex(
	client: DataikuClient,
	projectKey: string | undefined,
): Promise<FlowZoneOrganizeValidationIndex> {
	const result = await client.projects.map({
		projectKey,
		maxNodes: 100_000,
		maxEdges: 100_000,
	},);
	const index: FlowZoneOrganizeValidationIndex = {
		projectKey: result.map.projectKey,
		all: new Set(),
		datasets: new Set(),
		recipes: new Set(),
		folders: new Set(),
	};
	for (const node of result.map.nodes) {
		index.all.add(node.id,);
		switch (node.kind) {
			case "dataset":
				index.datasets.add(node.id,);
				break;
			case "recipe":
				index.recipes.add(node.id,);
				break;
			case "folder":
				index.folders.add(node.id,);
				break;
			case "other":
				break;
		}
	}
	return index;
}

async function validateFlowZoneOrganizeObjects(
	client: DataikuClient,
	plan: FlowZoneOrganizePlan,
	projectKey: string | undefined,
): Promise<FlowZoneOrganizeValidationResult> {
	const indexes = new Map<string, FlowZoneOrganizeValidationIndex>();
	const missing: FlowZoneOrganizeValidationIssue[] = [];

	const getIndex = async (itemProjectKey: string | undefined,) => {
		const requestedProjectKey = itemProjectKey ?? projectKey;
		const cacheKey = requestedProjectKey ?? "";
		const cached = indexes.get(cacheKey,);
		if (cached) return cached;
		const index = await flowZoneValidationIndex(client, requestedProjectKey,);
		indexes.set(cacheKey, index,);
		return index;
	};

	for (const zone of plan.zones) {
		for (const item of zone.items) {
			const index = await getIndex(item.projectKey,);
			const bucket = flowZoneValidationBucket(index, item.objectType,);
			if (bucket.has(item.objectId,)) continue;
			missing.push({
				zone: flowZonePlanLabel(zone,),
				objectId: item.objectId,
				objectType: item.objectType,
				...(item.projectKey ? { projectKey: item.projectKey, } : {}),
				reason: `Object not found in project ${item.projectKey ?? index.projectKey}.`,
			},);
		}
	}

	return { valid: missing.length === 0, missing, };
}

function throwFlowZoneValidationError(validation: FlowZoneOrganizeValidationResult,): void {
	if (validation.valid) return;
	const first = validation.missing[0];
	const suffix = validation.missing.length > 1 ? ` and ${validation.missing.length - 1} more` : "";
	throw new UsageError(
		`Flow zone organize validation failed: ${first?.objectType}:${first?.objectId} in zone "${first?.zone}" was not found${suffix}.`,
		"validation_failed",
	);
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
	"dss sql query [SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin] (--connection CONN | --dataset FULL_NAME) [--database DB] [--output PATH|--output-file PATH] [--preview N] [--request-timeout MS] [--project-key KEY]";

const DEFAULT_SQL_PREVIEW_ROWS = 5;

/**
 * Parse `--preview N` into a non-negative row count. Rejects non-integers,
 * negatives, and empty values loudly so a bad flag never silently degrades to a
 * default. `--preview 0` is valid and yields an empty preview (explicit opt-out).
 */
function parseSqlPreviewCount(value: string | boolean | undefined,): number {
	if (typeof value !== "string") {
		throw new UsageError(
			`--preview requires an integer value. Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	const trimmed = value.trim();
	const parsed = Number(trimmed,);
	if (trimmed.length === 0 || !Number.isInteger(parsed,) || parsed < 0) {
		throw new UsageError(
			`--preview must be a non-negative integer (got "${value}"). Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	return parsed;
}

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

const CODE_RUN_USAGE =
	"dss code run (--file PATH | --stdin) [--env ENV] [--timeout MS] [--keep] [--full-log] [--max-log-bytes N] [--project-key KEY]";

function resolveCodeInput(args: string[], flags: Record<string, string | boolean>,): string {
	if (args.length > 0) {
		throw new UsageError(
			`code run takes no positional arguments; pass the script via --file PATH or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	const sources: Array<{ label: string; read: () => string; }> = [];
	if (typeof flags["file"] === "string") {
		sources.push({ label: "--file", read: () => readFileSync(flags["file"] as string, "utf-8",), },);
	}
	if (flags["stdin"] === true) {
		sources.push({ label: "--stdin", read: readStdinText, },);
	}
	if (sources.length === 0) {
		throw new UsageError(
			`Python source is required: pass --file PATH or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	if (sources.length > 1) {
		throw new UsageError(
			`Choose exactly one Python source: --file or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	const script = stripUtf8Bom(sources[0]!.read(),);
	if (script.trim().length === 0) {
		throw new UsageError(
			`Python source from ${sources[0]!.label} must not be empty. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	return script;
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

let outputFieldProjection: string[] | undefined;

/**
 * Non-fatal diagnostics queued during a command and flushed to stderr as one
 * JSON envelope, so agents get a loud, machine-parseable signal (e.g. truncated
 * exports) without polluting the stdout result contract.
 */
let pendingCliWarnings: Record<string, unknown>[] = [];

function enqueueCliWarning(warning: Record<string, unknown>,): void {
	pendingCliWarnings.push(warning,);
}

/** Emit queued warnings as a single `{ warnings: [...] }` object on stderr. Idempotent. */
function flushCliWarnings(): void {
	if (pendingCliWarnings.length === 0) return;
	const warnings = pendingCliWarnings;
	pendingCliWarnings = [];
	process.stderr.write(`${JSON.stringify({ warnings, }, null, 2,)}\n`,);
}

function resolveFieldPath(source: Record<string, unknown>, field: string,): unknown {
	let current: unknown = source;
	for (const segment of field.split(".",)) {
		if (current === null || typeof current !== "object" || Array.isArray(current,)) return null;
		current = (current as Record<string, unknown>)[segment];
	}
	return current ?? null;
}

function pickResultFields(item: unknown, fields: string[],): unknown {
	if (!item || typeof item !== "object" || Array.isArray(item,)) return item;
	const source = item as Record<string, unknown>;
	const picked: Record<string, unknown> = {};
	for (const field of fields) picked[field] = resolveFieldPath(source, field,);
	return picked;
}

/**
 * Project the top-level fields callers asked for via --fields. Arrays are mapped
 * element-wise; scalars and string results pass through untouched. Requested keys
 * that are absent become null so every row keeps a stable, predictable shape.
 */
function projectResultFields(result: unknown, fields: string[],): unknown {
	if (Array.isArray(result,)) return result.map((item,) => pickResultFields(item, fields,));
	return pickResultFields(result, fields,);
}

function writeCommandResult(result: unknown,): void {
	flushCliWarnings();
	const projected = outputFieldProjection
		? projectResultFields(result, outputFieldProjection,)
		: result;
	process.stdout.write(`${JSON.stringify(projected ?? { ok: true, }, null, 2,)}\n`,);
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
			error.requestId,
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

class CommandResultFailure extends Error {
	readonly result: unknown;
	readonly exitCode: number;

	constructor(result: unknown, exitCode: number,) {
		super(commandFailureMessage(result,),);
		this.name = "CommandResultFailure";
		this.result = result;
		this.exitCode = exitCode;
	}
}

function commandFailureMessage(result: unknown,): string {
	if (isFailedWaitResult(result,)) {
		const record = result as Record<string, unknown>;
		const state = typeof record.state === "string" ? record.state : record.outcome;
		return `Command completed with failed long-running result${state ? `: ${state}` : ""}.`;
	}
	if (
		result && typeof result === "object" && (result as Record<string, unknown>).unchanged === false
	) {
		return "Command completed with failed assertion result.";
	}
	return "Command completed with failed result.";
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

function recipeRoleInputItems(recipe: Record<string, unknown>, role: string,): unknown[] {
	const inputs = recipe["inputs"];
	if (!inputs || typeof inputs !== "object") return [];
	const roleEntry = (inputs as Record<string, unknown>)[role];
	if (!roleEntry || typeof roleEntry !== "object") return [];
	const items = (roleEntry as Record<string, unknown>)["items"];
	return Array.isArray(items,) ? items : [];
}

function recipeInputItemRef(item: unknown,): string | undefined {
	if (!item || typeof item !== "object") return undefined;
	const ref = (item as Record<string, unknown>)["ref"];
	return typeof ref === "string" && ref.length > 0 ? ref : undefined;
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
	"no-wait",
	"force-rebuild",
	"latest",
	"copy-output-settings",
	"continue-on-error",
	"no-backup",
	"payload-only",
	"allow-same-path",
	"sync",
	"validate-objects",
	"errors-only",
	"keep",
	"full-log",
],);

const SHORT_FLAGS: Record<string, string> = {
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
	rows: "max-rows",
};

const VALUE_FLAGS = new Set([
	"fields",
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
	"env",
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
	"max-log-bytes",
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
	"preview",
	"project-key",
	"recipe",
	"request-timeout",
	"params",
	"results-per-page",
	"record-cleanup",
	"rule-id",
	"role",
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
	if (rawFlagName === "help") throw unsupportedHelpFlag();
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
				if (arg[1] === "h") throw unsupportedHelpFlag();
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
	requiredOneOf?: CommandFlagChoice[];
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

	app: {
		list: {
			handler: (c,) => c.applications.listApps(),
			usage: "dss app list",
			description: "List all Dataiku App templates.",
			examples: ["dss app list",],
		},
		manifest: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss app manifest <appId>",);
				return c.applications.getAppManifest(a[0],);
			},
			usage: "dss app manifest <appId>",
			description: "Get the manifest of a Dataiku App template.",
			examples: ["dss app manifest my-app",],
		},
		instances: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss app instances <appId>",);
				return c.applications.listInstances(a[0],);
			},
			usage: "dss app instances <appId>",
			description: "List instances created from a Dataiku App template.",
			examples: ["dss app instances my-app",],
		},
		"create-instance": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss app create-instance <appId> (--data JSON|--data-file PATH|--stdin)",);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (instance creation payload).",
				);
				return c.applications.createInstance(a[0], body,);
			},
			usage: "dss app create-instance <appId> (--data JSON|--data-file PATH|--stdin)",
			description: "Create an app instance from a Dataiku App template.",
			examples: ['dss app create-instance my-app --data \'{"targetProjectKey":"NEWPROJ"}\'',],
		},
		"instance-manifest": {
			handler: (c, _a, f,) =>
				c.applications.getInstanceManifest(f["project-key"] as string | undefined,),
			usage: "dss app instance-manifest [--project-key KEY]",
			description: "Get the app manifest of an app-instance project.",
			examples: ["dss app instance-manifest --project-key MYINSTANCE",],
		},
		"save-instance-manifest": {
			handler: (c, _a, f,) => {
				const manifest = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (manifest JSON).",
				);
				return c.applications.saveInstanceManifest(manifest, f["project-key"] as string | undefined,);
			},
			usage:
				"dss app save-instance-manifest (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			description:
				"Save the app manifest of an app-instance project (homepage sections, use-as-recipe settings).",
			examples: ["dss app save-instance-manifest --data-file manifest.json --project-key MYINSTANCE",],
		},
		"delete-instance": {
			handler: async (c, _a, f,) => {
				await c.applications.deleteInstance(f["project-key"] as string | undefined,);
				return { deleted: true, };
			},
			usage: "dss app delete-instance [--project-key KEY]",
			description: "Delete an app-instance project (destructive: removes the instance project).",
			examples: ["dss app delete-instance --project-key MYINSTANCE",],
		},
	},

	"business-app": {
		list: {
			handler: (c,) => c.applications.listBusinessApps(),
			usage: "dss business-app list",
			description: "List all Business Apps.",
			examples: ["dss business-app list",],
		},
		get: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss business-app get <id>",);
				return c.applications.getBusinessApp(a[0],);
			},
			usage: "dss business-app get <id>",
			description: "Get Business App details.",
			examples: ["dss business-app get my-bapp",],
		},
		settings: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss business-app settings <id>",);
				return c.applications.getBusinessAppSettings(a[0],);
			},
			usage: "dss business-app settings <id>",
			description: "Get Business App settings.",
			examples: ["dss business-app settings my-bapp",],
		},
		"save-settings": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss business-app save-settings <id> (--data JSON|--data-file PATH|--stdin)",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (settings JSON).",
				);
				return c.applications.saveBusinessAppSettings(a[0], body,);
			},
			usage: "dss business-app save-settings <id> (--data JSON|--data-file PATH|--stdin)",
			description: "Save Business App settings (admin only; includes connection remapping).",
			examples: ["dss business-app save-settings my-bapp --data-file settings.json",],
		},
		instances: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss business-app instances <id>",);
				return c.applications.listBusinessAppInstances(a[0],);
			},
			usage: "dss business-app instances <id>",
			description: "List instances of a Business App.",
			examples: ["dss business-app instances my-bapp",],
		},
		"create-instance": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss business-app create-instance <id> (--data JSON|--data-file PATH|--stdin)",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (instance payload).",
				);
				return c.applications.createBusinessAppInstance(a[0], body,);
			},
			usage: "dss business-app create-instance <id> (--data JSON|--data-file PATH|--stdin)",
			description: "Create an instance of a Business App.",
			examples: ["dss business-app create-instance my-bapp --data '{}'",],
		},
		"upgrade-instance": {
			handler: (c, a,) => {
				requireArgs(a, 2, "dss business-app upgrade-instance <id> <projectKey>",);
				return c.applications.upgradeBusinessAppInstance(a[0], a[1],);
			},
			usage: "dss business-app upgrade-instance <id> <projectKey>",
			description: "Upgrade a Business App instance to the latest version.",
			examples: ["dss business-app upgrade-instance my-bapp INSTANCEPROJ",],
		},
		"install-from-archive": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss business-app install-from-archive <filePath>",);
				await c.applications.installBusinessAppFromArchive(a[0],);
				return { installed: true, };
			},
			usage: "dss business-app install-from-archive <filePath>",
			description: "Install or upgrade a Business App from a zip archive (admin only).",
			examples: ["dss business-app install-from-archive ./my-bapp.zip",],
		},
	},

	webapp: {
		list: {
			handler: (c, _a, f,) => c.webapps.list(f["project-key"] as string | undefined,),
			usage: "dss webapp list [--project-key KEY]",
			description: "List webapps in a project.",
			examples: ["dss webapp list",],
		},
		"get-settings": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss webapp get-settings <webappId> [--project-key KEY]",);
				return c.webapps.getSettings(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss webapp get-settings <webappId> [--project-key KEY]",
			description: "Get a webapp's settings.",
			examples: ["dss webapp get-settings WEBAPP_ID",],
		},
		create: {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (webapp definition).",
				);
				return c.webapps.create(body, f["project-key"] as string | undefined,);
			},
			usage: "dss webapp create (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			description: "Create a webapp from a JSON definition.",
			examples: ["dss webapp create --data-file webapp.json",],
		},
		"update-settings": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss webapp update-settings <webappId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (webapp settings).",
				);
				return c.webapps.updateSettings(a[0], body, f["project-key"] as string | undefined,);
			},
			usage:
				"dss webapp update-settings <webappId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			description: "Replace a webapp's settings.",
			examples: ["dss webapp update-settings WEBAPP_ID --data-file webapp.json",],
		},
		"stop-backend": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss webapp stop-backend <webappId> [--project-key KEY]",);
				await c.webapps.stopBackend(a[0], f["project-key"] as string | undefined,);
				return { stopped: true, };
			},
			usage: "dss webapp stop-backend <webappId> [--project-key KEY]",
			description: "Stop a webapp's backend.",
			examples: ["dss webapp stop-backend WEBAPP_ID",],
		},
		"restart-backend": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss webapp restart-backend <webappId> [--project-key KEY]",);
				await c.webapps.startOrRestartBackend(a[0], f["project-key"] as string | undefined,);
				return { restarted: true, };
			},
			usage: "dss webapp restart-backend <webappId> [--project-key KEY]",
			description: "Start or restart a webapp's backend.",
			examples: ["dss webapp restart-backend WEBAPP_ID",],
		},
		"backend-state": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss webapp backend-state <webappId> [--project-key KEY]",);
				return c.webapps.getBackendState(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss webapp backend-state <webappId> [--project-key KEY]",
			description: "Get a webapp backend's runtime state.",
			examples: ["dss webapp backend-state WEBAPP_ID",],
		},
	},

	"api-service": {
		list: {
			handler: (c, _a, f,) => c.apiServices.list(f["project-key"] as string | undefined,),
			usage: "dss api-service list [--project-key KEY]",
			description: "List API services in a project.",
			examples: ["dss api-service list",],
		},
		create: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss api-service create <serviceId> [--project-key KEY]",);
				return c.apiServices.create(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service create <serviceId> [--project-key KEY]",
			description: "Create an empty API service.",
			examples: ["dss api-service create my-service",],
		},
		"get-settings": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss api-service get-settings <serviceId> [--project-key KEY]",);
				return c.apiServices.getSettings(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service get-settings <serviceId> [--project-key KEY]",
			description: "Get an API service's settings (endpoint definitions).",
			examples: ["dss api-service get-settings my-service",],
		},
		"save-settings": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss api-service save-settings <serviceId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (service settings).",
				);
				return c.apiServices.saveSettings(a[0], body, f["project-key"] as string | undefined,);
			},
			usage:
				"dss api-service save-settings <serviceId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			description: "Save an API service's settings.",
			examples: ["dss api-service save-settings my-service --data-file service.json",],
		},
		"list-packages": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss api-service list-packages <serviceId> [--project-key KEY]",);
				return c.apiServices.listPackages(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service list-packages <serviceId> [--project-key KEY]",
			description: "List deployable packages of an API service.",
			examples: ["dss api-service list-packages my-service",],
		},
		"package-summary": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss api-service package-summary <serviceId> <packageId> [--project-key KEY]",
				);
				return c.apiServices.getPackageSummary(a[0], a[1], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service package-summary <serviceId> <packageId> [--project-key KEY]",
			description: "Get a package summary.",
			examples: ["dss api-service package-summary my-service v1",],
		},
		"create-package": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss api-service create-package <serviceId> <packageId> [--project-key KEY]",
				);
				return c.apiServices.createPackage(a[0], a[1], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service create-package <serviceId> <packageId> [--project-key KEY]",
			description: "Build a deployable package from the current service state.",
			examples: ["dss api-service create-package my-service v1",],
		},
		"delete-package": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss api-service delete-package <serviceId> <packageId> [--project-key KEY]",
				);
				await c.apiServices.deletePackage(a[0], a[1], f["project-key"] as string | undefined,);
				return { deleted: true, };
			},
			usage: "dss api-service delete-package <serviceId> <packageId> [--project-key KEY]",
			description: "Delete an API service package.",
			examples: ["dss api-service delete-package my-service v1",],
		},
		"download-package": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss api-service download-package <serviceId> <packageId> --output PATH [--project-key KEY]",
				);
				const out = f["output"] as string | undefined;
				if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
				const res = await c.apiServices.downloadPackageArchive(
					a[0],
					a[1],
					f["project-key"] as string | undefined,
				);
				const buf = Buffer.from(await res.arrayBuffer(),);
				writeFileSync(out, buf,);
				return { path: out, bytes: buf.length, };
			},
			usage:
				"dss api-service download-package <serviceId> <packageId> --output PATH [--project-key KEY]",
			description: "Download an API service package archive to a local file.",
			examples: ["dss api-service download-package my-service v1 --output ./pkg.zip",],
		},
		"publish-package": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss api-service publish-package <serviceId> <packageId> [--project-key KEY]",
				);
				return c.apiServices.publishPackage(a[0], a[1], f["project-key"] as string | undefined,);
			},
			usage: "dss api-service publish-package <serviceId> <packageId> [--project-key KEY]",
			description: "Publish a package to the API Deployer.",
			examples: ["dss api-service publish-package my-service v1",],
		},
	},

	"api-deployer": {
		"list-infras": {
			handler: (c,) => c.apiDeployer.listInfras(),
			usage: "dss api-deployer list-infras",
			description: "List API Deployer infrastructures.",
			examples: ["dss api-deployer list-infras",],
		},
		"create-infra": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (infra settings).",
				);
				return c.apiDeployer.createInfra(body,);
			},
			usage: "dss api-deployer create-infra (--data JSON|--data-file PATH|--stdin)",
			description: "Create an API Deployer infrastructure.",
			examples: ["dss api-deployer create-infra --data-file infra.json",],
		},
		"get-infra": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer get-infra <infraId>",);
				return c.apiDeployer.getInfra(a[0],);
			},
			usage: "dss api-deployer get-infra <infraId>",
			description: "Get an API Deployer infrastructure status.",
			examples: ["dss api-deployer get-infra prod-infra",],
		},
		"delete-infra": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss api-deployer delete-infra <infraId>",);
				await c.apiDeployer.deleteInfra(a[0],);
				return { deleted: true, };
			},
			usage: "dss api-deployer delete-infra <infraId>",
			description: "Delete an API Deployer infrastructure.",
			examples: ["dss api-deployer delete-infra prod-infra",],
		},
		"list-stages": {
			handler: (c,) => c.apiDeployer.listStages(),
			usage: "dss api-deployer list-stages",
			description: "List API Deployer lifecycle stages.",
			examples: ["dss api-deployer list-stages",],
		},
		"list-services": {
			handler: (c,) => c.apiDeployer.listServices(),
			usage: "dss api-deployer list-services",
			description: "List published API Deployer services.",
			examples: ["dss api-deployer list-services",],
		},
		"create-service": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (service definition).",
				);
				return c.apiDeployer.createService(body,);
			},
			usage: "dss api-deployer create-service (--data JSON|--data-file PATH|--stdin)",
			description: "Create a published API Deployer service.",
			examples: ['dss api-deployer create-service --data \'{"id":"my-service"}\'',],
		},
		"get-service": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer get-service <serviceId>",);
				return c.apiDeployer.getService(a[0],);
			},
			usage: "dss api-deployer get-service <serviceId>",
			description: "Get a published service's status (versions + deployments).",
			examples: ["dss api-deployer get-service my-service",],
		},
		"delete-service": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss api-deployer delete-service <serviceId>",);
				await c.apiDeployer.deleteService(a[0],);
				return { deleted: true, };
			},
			usage: "dss api-deployer delete-service <serviceId>",
			description: "Delete a published API Deployer service.",
			examples: ["dss api-deployer delete-service my-service",],
		},
		"publish-version": {
			handler: async (c, a,) => {
				requireArgs(a, 2, "dss api-deployer publish-version <serviceId> <archive.zip>",);
				await c.apiDeployer.publishServiceVersion(a[0], a[1],);
				return { published: true, };
			},
			usage: "dss api-deployer publish-version <serviceId> <archive.zip>",
			description: "Publish (upload) a service version package to the API Deployer.",
			examples: ["dss api-deployer publish-version my-service ./pkg.zip",],
		},
		"delete-version": {
			handler: async (c, a,) => {
				requireArgs(a, 2, "dss api-deployer delete-version <serviceId> <version>",);
				await c.apiDeployer.deleteServiceVersion(a[0], a[1],);
				return { deleted: true, };
			},
			usage: "dss api-deployer delete-version <serviceId> <version>",
			description: "Delete a published service version.",
			examples: ["dss api-deployer delete-version my-service v1",],
		},
		"list-deployments": {
			handler: (c,) => c.apiDeployer.listDeployments(),
			usage: "dss api-deployer list-deployments",
			description: "List API Deployer deployments.",
			examples: ["dss api-deployer list-deployments",],
		},
		"create-deployment": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (deployment settings).",
				);
				return c.apiDeployer.createDeployment(body,);
			},
			usage: "dss api-deployer create-deployment (--data JSON|--data-file PATH|--stdin)",
			description: "Create an API Deployer deployment (maps a service version to an infra).",
			examples: ["dss api-deployer create-deployment --data-file deployment.json",],
		},
		"get-deployment": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer get-deployment <deploymentId>",);
				return c.apiDeployer.getDeployment(a[0],);
			},
			usage: "dss api-deployer get-deployment <deploymentId>",
			description: "Get an API Deployer deployment.",
			examples: ["dss api-deployer get-deployment my-deployment",],
		},
		"deployment-status": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer deployment-status <deploymentId>",);
				return c.apiDeployer.getDeploymentStatus(a[0],);
			},
			usage: "dss api-deployer deployment-status <deploymentId>",
			description: "Get an API Deployer deployment's full health/status.",
			examples: ["dss api-deployer deployment-status my-deployment",],
		},
		"deployment-settings": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer deployment-settings <deploymentId>",);
				return c.apiDeployer.getDeploymentSettings(a[0],);
			},
			usage: "dss api-deployer deployment-settings <deploymentId>",
			description: "Get an API Deployer deployment's settings.",
			examples: ["dss api-deployer deployment-settings my-deployment",],
		},
		"save-deployment-settings": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss api-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (deployment settings).",
				);
				await c.apiDeployer.saveDeploymentSettings(a[0], body,);
				return { saved: true, };
			},
			usage:
				"dss api-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
			description: "Save an API Deployer deployment's settings.",
			examples: ["dss api-deployer save-deployment-settings my-deployment --data-file settings.json",],
		},
		deploy: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss api-deployer deploy <deploymentId>",);
				return c.apiDeployer.startDeploymentUpdate(a[0],);
			},
			usage: "dss api-deployer deploy <deploymentId>",
			description: "Apply a deployment's settings to its infrastructure (start update).",
			examples: ["dss api-deployer deploy my-deployment",],
		},
		"delete-deployment": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss api-deployer delete-deployment <deploymentId>",);
				await c.apiDeployer.deleteDeployment(a[0],);
				return { deleted: true, };
			},
			usage: "dss api-deployer delete-deployment <deploymentId>",
			description: "Delete an API Deployer deployment.",
			examples: ["dss api-deployer delete-deployment my-deployment",],
		},
	},

	bundle: {
		"list-exported": {
			handler: (c, _a, f,) => c.bundles.listExported(f["project-key"] as string | undefined,),
			usage: "dss bundle list-exported [--project-key KEY]",
			description: "List bundles exported from a project on the Design node.",
			examples: ["dss bundle list-exported",],
		},
		export: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss bundle export <bundleId> [--project-key KEY]",);
				await c.bundles.exportBundle(a[0], f["project-key"] as string | undefined,);
				return { exported: a[0], };
			},
			usage: "dss bundle export <bundleId> [--project-key KEY]",
			description: "Create (or overwrite) an exported Design-node bundle.",
			examples: ["dss bundle export v1",],
		},
		"delete-exported": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss bundle delete-exported <bundleId> [--project-key KEY]",);
				await c.bundles.deleteExported(a[0], f["project-key"] as string | undefined,);
				return { deleted: true, };
			},
			usage: "dss bundle delete-exported <bundleId> [--project-key KEY]",
			description: "Delete an exported Design-node bundle.",
			examples: ["dss bundle delete-exported v1",],
		},
		"download-exported": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss bundle download-exported <bundleId> --output PATH [--project-key KEY]",);
				const out = f["output"] as string | undefined;
				if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
				const res = await c.bundles.downloadExportedArchive(
					a[0],
					f["project-key"] as string | undefined,
				);
				const buf = Buffer.from(await res.arrayBuffer(),);
				writeFileSync(out, buf,);
				return { path: out, bytes: buf.length, };
			},
			usage: "dss bundle download-exported <bundleId> --output PATH [--project-key KEY]",
			description: "Download an exported Design-node bundle archive to a local file.",
			examples: ["dss bundle download-exported v1 --output ./bundle.zip",],
		},
		publish: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss bundle publish <bundleId> [--project-key KEY]",);
				return c.bundles.publish(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss bundle publish <bundleId> [--project-key KEY]",
			description: "Publish a Design-node bundle to the Project Deployer.",
			examples: ["dss bundle publish v1",],
		},
		"list-imported": {
			handler: (c, _a, f,) => c.bundles.listImported(f["project-key"] as string | undefined,),
			usage: "dss bundle list-imported [--project-key KEY]",
			description: "List bundles imported into a project on the Automation node.",
			examples: ["dss bundle list-imported",],
		},
		"import-from-archive": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss bundle import-from-archive <serverArchivePath> [--project-key KEY]",);
				return c.bundles.importFromArchive(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss bundle import-from-archive <serverArchivePath> [--project-key KEY]",
			description: "Import a server-side bundle archive into an Automation-node project.",
			examples: ["dss bundle import-from-archive /data/bundles/v1.zip",],
		},
		"import-from-stream": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss bundle import-from-stream <filePath> [--project-key KEY]",);
				await c.bundles.importFromStream(a[0], f["project-key"] as string | undefined,);
				return { imported: true, };
			},
			usage: "dss bundle import-from-stream <filePath> [--project-key KEY]",
			description: "Upload and import a local bundle archive into an Automation-node project.",
			examples: ["dss bundle import-from-stream ./v1.zip",],
		},
		activate: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss bundle activate <bundleId> [--project-key KEY]",);
				return c.bundles.activate(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss bundle activate <bundleId> [--project-key KEY]",
			description: "Activate an imported Automation-node bundle.",
			examples: ["dss bundle activate v1",],
		},
		preload: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss bundle preload <bundleId> [--project-key KEY]",);
				return c.bundles.preload(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss bundle preload <bundleId> [--project-key KEY]",
			description: "Preload an imported Automation-node bundle (stage data/models).",
			examples: ["dss bundle preload v1",],
		},
		"delete-imported": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss bundle delete-imported <bundleId> [--project-key KEY]",);
				await c.bundles.deleteImported(a[0], f["project-key"] as string | undefined,);
				return { deleted: true, };
			},
			usage: "dss bundle delete-imported <bundleId> [--project-key KEY]",
			description: "Delete an imported Automation-node bundle.",
			examples: ["dss bundle delete-imported v1",],
		},
	},

	"project-deployer": {
		"list-projects": {
			handler: (c,) => c.projectDeployer.listProjects(),
			usage: "dss project-deployer list-projects",
			description: "List published projects on the Project Deployer.",
			examples: ["dss project-deployer list-projects",],
		},
		"create-project": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (published project settings).",
				);
				return c.projectDeployer.createProject(body,);
			},
			usage: "dss project-deployer create-project (--data JSON|--data-file PATH|--stdin)",
			description: "Create a published project on the Project Deployer.",
			examples: ["dss project-deployer create-project --data-file project.json",],
		},
		"upload-bundle": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss project-deployer upload-bundle <filePath>",);
				await c.projectDeployer.uploadBundle(a[0],);
				return { uploaded: true, };
			},
			usage: "dss project-deployer upload-bundle <filePath>",
			description: "Upload a project bundle archive to the Project Deployer.",
			examples: ["dss project-deployer upload-bundle ./v1.zip",],
		},
		"project-status": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss project-deployer project-status <publishedProjectKey>",);
				return c.projectDeployer.getProjectStatus(a[0],);
			},
			usage: "dss project-deployer project-status <publishedProjectKey>",
			description: "Get a published project's status and available bundles.",
			examples: ["dss project-deployer project-status MYPROJ",],
		},
		"list-deployments": {
			handler: (c,) => c.projectDeployer.listDeployments(),
			usage: "dss project-deployer list-deployments",
			description: "List Project Deployer deployments.",
			examples: ["dss project-deployer list-deployments",],
		},
		"create-deployment": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (deployment settings).",
				);
				return c.projectDeployer.createDeployment(body,);
			},
			usage: "dss project-deployer create-deployment (--data JSON|--data-file PATH|--stdin)",
			description: "Create a Project Deployer deployment (bundle to infra mapping).",
			examples: ["dss project-deployer create-deployment --data-file deployment.json",],
		},
		"get-deployment": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss project-deployer get-deployment <deploymentId>",);
				return c.projectDeployer.getDeployment(a[0],);
			},
			usage: "dss project-deployer get-deployment <deploymentId>",
			description: "Get a Project Deployer deployment.",
			examples: ["dss project-deployer get-deployment my-deployment",],
		},
		"deployment-status": {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss project-deployer deployment-status <deploymentId>",);
				return c.projectDeployer.getDeploymentStatus(a[0],);
			},
			usage: "dss project-deployer deployment-status <deploymentId>",
			description: "Get a Project Deployer deployment's full health/status.",
			examples: ["dss project-deployer deployment-status my-deployment",],
		},
		"save-deployment-settings": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss project-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (deployment settings).",
				);
				await c.projectDeployer.saveDeploymentSettings(a[0], body,);
				return { saved: true, };
			},
			usage:
				"dss project-deployer save-deployment-settings <deploymentId> (--data JSON|--data-file PATH|--stdin)",
			description: "Save a Project Deployer deployment's settings (e.g. bundleId).",
			examples: [
				"dss project-deployer save-deployment-settings my-deployment --data-file settings.json",
			],
		},
		deploy: {
			handler: (c, a,) => {
				requireArgs(a, 1, "dss project-deployer deploy <deploymentId>",);
				return c.projectDeployer.startUpdate(a[0],);
			},
			usage: "dss project-deployer deploy <deploymentId>",
			description: "Apply a deployment to the Automation node (start update).",
			examples: ["dss project-deployer deploy my-deployment",],
		},
		"delete-deployment": {
			handler: async (c, a,) => {
				requireArgs(a, 1, "dss project-deployer delete-deployment <deploymentId>",);
				await c.projectDeployer.deleteDeployment(a[0],);
				return { deleted: true, };
			},
			usage: "dss project-deployer delete-deployment <deploymentId>",
			description: "Delete a Project Deployer deployment.",
			examples: ["dss project-deployer delete-deployment my-deployment",],
		},
		"list-infras": {
			handler: (c,) => c.projectDeployer.listInfras(),
			usage: "dss project-deployer list-infras",
			description: "List Project Deployer infrastructures.",
			examples: ["dss project-deployer list-infras",],
		},
		"create-infra": {
			handler: (c, _a, f,) => {
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (infra settings).",
				);
				return c.projectDeployer.createInfra(body,);
			},
			usage: "dss project-deployer create-infra (--data JSON|--data-file PATH|--stdin)",
			description: "Create a Project Deployer infrastructure.",
			examples: ["dss project-deployer create-infra --data-file infra.json",],
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
		organize: {
			handler: async (c, _a, f,) => {
				const usage =
					"dss flow-zone organize (--data JSON|--data-file PATH|--file PATH|--stdin) [--sync] [--validate-objects] [--dry-run] [--project-key KEY]";
				const pk = f["project-key"] as string | undefined;
				const plan = readFlowZoneOrganizePlan(f, usage,);
				const sync = f["sync"] === true;
				const validateObjects = f["validate-objects"] === true;
				const zones = await c.flowZones.list(pk,);
				const plannedItemKeys = flowZonePlanItemKeys(plan,);
				const planned = plan.zones.map((zonePlan,) =>
					flowZoneOrganizeStep(zonePlan, findFlowZoneForPlan(zones, zonePlan,), sync, plannedItemKeys,)
				);
				const validation = validateObjects
					? await validateFlowZoneOrganizeObjects(c, plan, pk,)
					: undefined;
				if (validation) throwFlowZoneValidationError(validation,);
				const itemCount = plan.zones.reduce((count, zonePlan,) => count + zonePlan.items.length, 0,);
				const pruneItemCount = planned.reduce((count, step,) => {
					const pruneItems = Array.isArray(step.pruneItems,) ? step.pruneItems : [];
					return count + pruneItems.length;
				}, 0,);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "organize",
						resource: "flow-zone",
						projectKey: pk,
						sync,
						validateObjects,
						zoneCount: plan.zones.length,
						itemCount,
						pruneItemCount,
						...(validation ? { validation, } : {}),
						planned,
					};
				}

				const currentZones = [...zones,];
				const created: FlowZone[] = [];
				const updated: FlowZone[] = [];
				const moved: Array<{ zoneId: string; name: string; items: FlowZoneItemInput[]; }> = [];
				const pruned: Array<
					{ zoneId: "default"; fromZoneId: string; name: string; items: FlowZoneItemInput[]; }
				> = [];

				for (const zonePlan of plan.zones) {
					let zone = findFlowZoneForPlan(currentZones, zonePlan,);
					ensureFlowZonePlanTarget(zonePlan, zone,);
					const pruneItems = sync ? flowZonePruneItems(zone, plannedItemKeys,) : [];
					if (!zone) {
						zone = await c.flowZones.create({
							name: zonePlan.name!,
							color: zonePlan.color,
							position: zonePlan.position,
							projectKey: pk,
						},);
						currentZones.push(zone,);
						created.push(zone,);
					} else {
						const patch = {
							...(zonePlan.name && zonePlan.name !== zone.name ? { name: zonePlan.name, } : {}),
							...(zonePlan.color && zonePlan.color !== zone.color ? { color: zonePlan.color, } : {}),
							...(zonePlan.position !== undefined
									&& !flowZoneSamePosition(flowZoneCurrentPosition(zone,), zonePlan.position,)
								? { position: zonePlan.position, }
								: {}),
							projectKey: pk,
						};
						if (patch.name !== undefined || patch.color !== undefined || patch.position !== undefined) {
							zone = await c.flowZones.update(zone.id, patch,);
							const index = currentZones.findIndex((candidate,) => candidate.id === zone!.id);
							if (index !== -1) currentZones[index] = zone;
							updated.push(zone,);
						}
					}

					if (zonePlan.items.length > 0) {
						await c.flowZones.moveItems(zone.id, zonePlan.items, pk,);
						moved.push({ zoneId: zone.id, name: zone.name, items: zonePlan.items, },);
					}
					if (pruneItems.length > 0) {
						await c.flowZones.moveItems("default", pruneItems, pk,);
						pruned.push({ zoneId: "default", fromZoneId: zone.id, name: zone.name, items: pruneItems, },);
					}
				}

				return {
					organized: true,
					action: "organize",
					resource: "flow-zone",
					projectKey: pk,
					sync,
					validateObjects,
					zoneCount: plan.zones.length,
					itemCount,
					pruneItemCount,
					created,
					updated,
					moved,
					pruned,
				};
			},
			usage:
				"dss flow-zone organize (--data JSON|--data-file PATH|--file PATH|--stdin) [--sync] [--validate-objects] [--dry-run] [--project-key KEY]",
			description:
				"Create/update flow zones and move objects from a declarative visual organization plan.",
			examples: [
				"dss flow-zone organize --file flow-zones.json --dry-run",
				`dss flow-zone organize --data '{"zones":[{"name":"Raw","color":"#64748b","datasets":["raw_orders"]}]}'`,
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
			usage: "dss dataset preview <name> [--max-rows N|--rows N] [--project-key KEY] [--timeout MS]",
			description: "Preview dataset rows (--rows is an alias for --max-rows).",
			examples: ["dss dataset preview orders", "dss dataset preview orders --rows 5",],
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
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset download <name>",);
				const result = await c.datasets.download(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
					limit: num(f["limit"],),
				},);
				if (result.truncated) {
					enqueueCliWarning({
						code: "dataset_download_truncated",
						message:
							`Download of '${a[0]}' stopped at the ${result.limit}-row cap; the dataset has more rows. `
							+ "Re-run with --limit N for more, or read inside a recipe (get_dataframe) for the full data.",
						dataset: a[0],
						rows: result.rows,
						limit: result.limit,
						path: result.path,
					},);
				}
				return result;
			},
			usage: "dss dataset download <name> [--output PATH] [--limit N] [--project-key KEY]",
			description:
				"Download up to --limit rows (default 100k) as CSV; returns { path, rows, truncated, limit }. When truncated, a dataset_download_truncated warning is also written to stderr so the cap is never silent.",
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
		"add-input": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss recipe add-input <recipe> <dataset> [--role ROLE] [--if-not-exists] [--dry-run] [--project-key KEY]",
				);
				const role = (f["role"] as string | undefined) ?? "main";
				const pk = f["project-key"] as string | undefined;
				const { recipe, } = await c.recipes.get(a[0], { projectKey: pk, },);
				const items = recipeRoleInputItems(recipe, role,);
				const present = items.some((item,) => recipeInputItemRef(item,) === a[1]);
				if (present) {
					if (f["if-not-exists"] === true) {
						return skipResult("recipe", a[0], "exists", { dataset: a[1], role, },);
					}
					throw new UsageError(
						`Dataset "${a[1]}" is already a "${role}" input of recipe "${a[0]}".`,
						"validation_failed",
					);
				}
				const nextItems = [...items, { ref: a[1], deps: [], },];
				const inputs = nextItems.map(recipeInputItemRef,).filter((ref,): ref is string =>
					Boolean(ref,)
				);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "add-input",
						resource: "recipe",
						recipe: a[0],
						dataset: a[1],
						role,
						inputs,
					};
				}
				await c.recipes.update(
					a[0],
					{ recipe: { inputs: { [role]: { items: nextItems, }, }, }, },
					pk,
				);
				return { updated: a[0], resource: "recipe", action: "add-input", role, dataset: a[1], inputs, };
			},
			usage:
				"dss recipe add-input <recipe> <dataset> [--role ROLE] [--if-not-exists] [--dry-run] [--project-key KEY]",
			description:
				"Add a dataset as a recipe input by appending one item to the current inputs (no need to resend the whole list).",
			examples: [
				"dss recipe add-input compute_orders extra_lookup",
				"dss recipe add-input compute_orders extra_lookup --if-not-exists --dry-run",
			],
		},
		"remove-input": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					2,
					"dss recipe remove-input <recipe> <dataset> [--role ROLE] [--if-exists] [--dry-run] [--project-key KEY]",
				);
				const role = (f["role"] as string | undefined) ?? "main";
				const pk = f["project-key"] as string | undefined;
				const { recipe, } = await c.recipes.get(a[0], { projectKey: pk, },);
				const items = recipeRoleInputItems(recipe, role,);
				const present = items.some((item,) => recipeInputItemRef(item,) === a[1]);
				if (!present) {
					if (f["if-exists"] === true) {
						return skipResult("recipe", a[0], "missing", { dataset: a[1], role, },);
					}
					throw new UsageError(
						`Dataset "${a[1]}" is not a "${role}" input of recipe "${a[0]}".`,
						"validation_failed",
					);
				}
				const nextItems = items.filter((item,) => recipeInputItemRef(item,) !== a[1]);
				const inputs = nextItems.map(recipeInputItemRef,).filter((ref,): ref is string =>
					Boolean(ref,)
				);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "remove-input",
						resource: "recipe",
						recipe: a[0],
						dataset: a[1],
						role,
						inputs,
					};
				}
				await c.recipes.update(
					a[0],
					{ recipe: { inputs: { [role]: { items: nextItems, }, }, }, },
					pk,
				);
				return {
					updated: a[0],
					resource: "recipe",
					action: "remove-input",
					role,
					dataset: a[1],
					inputs,
				};
			},
			usage:
				"dss recipe remove-input <recipe> <dataset> [--role ROLE] [--if-exists] [--dry-run] [--project-key KEY]",
			description:
				"Remove a dataset from a recipe's inputs by dropping one item from the current inputs.",
			examples: [
				"dss recipe remove-input compute_orders stale_lookup",
				"dss recipe remove-input compute_orders stale_lookup --if-exists --dry-run",
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
			description: "Print the recipe code payload as JSON; use --raw for raw bytes, not JSON.",
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
			description: "Print a recipe code payload as JSON; use --raw for raw bytes, not JSON.",
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
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss job log <id>",);
				const logFilter = f["errors-only"] === true
					? "errors"
					: jobLogFilterFromFlag(f["log-filter"],);
				const log = await c.jobs.log(a[0], {
					activity: f["activity"] as string | undefined,
					logId: f["log-id"] as string | undefined,
					logFilter,
					maxLogLines: maxLogLinesFromFlags(f,),
					projectKey: f["project-key"] as string | undefined,
				},);
				const outputFile = (f["output"] as string | undefined)
					?? (f["output-file"] as string | undefined);
				if (!outputFile) return log;
				const outputPath = resolve(outputFile,);
				await mkdir(dirname(outputPath,), { recursive: true, },);
				await writeFile(outputPath, log.endsWith("\n",) ? log : `${log}\n`, "utf-8",);
				return outputPath;
			},
			usage:
				"dss job log <id> [--activity ACTIVITY_ID] [--log-id LOG_ID] [--log-filter stdout|stderr|user|errors] [--errors-only] [--max-lines N|--max-log-lines N] [--output PATH] [--project-key KEY]",
			description:
				"Get public API job log output. Use --errors-only (or --log-filter errors) to surface just error/traceback lines, and --output PATH to write the log to a file (stdout returns the path). --log-id is accepted for UI parity but DSS API-key auth cannot select browser-only cat-activity-log files.",
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
				const connection = f["connection"] as string | undefined;
				const datasetFullName = f["dataset"] as string | undefined;
				if ((connection ? 1 : 0) + (datasetFullName ? 1 : 0) !== 1) {
					throw new UsageError(
						`Pass exactly one of --connection or --dataset. Usage: ${SQL_QUERY_USAGE}`,
					);
				}
				const outputFile = (f["output"] as string | undefined)
					?? (f["output-file"] as string | undefined);
				const previewProvided = f["preview"] !== undefined;
				if (previewProvided && !outputFile) {
					throw new UsageError(
						`--preview requires --output or --output-file. Usage: ${SQL_QUERY_USAGE}`,
						"validation_failed",
					);
				}
				const previewCount = previewProvided
					? parseSqlPreviewCount(f["preview"],)
					: DEFAULT_SQL_PREVIEW_ROWS;
				const query = resolveSqlInput(a, f,);
				const result = await c.sql.query({
					query,
					connection,
					datasetFullName,
					database: f["database"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
				if (!outputFile) return result;

				const outputPath = resolve(outputFile,);
				await mkdir(dirname(outputPath,), { recursive: true, },);
				await writeFile(outputPath, `${JSON.stringify(result, null, 2,)}\n`, "utf-8",);
				return {
					queryId: result.queryId,
					schema: result.schema,
					columns: result.columns ?? result.schema,
					rowCount: result.rows.length,
					preview: result.rows.slice(0, previewCount,),
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
				"dss sql query --sql-file query.sql --connection my_pg --output results.json --preview 10",
			],
		},
	},
	code: {
		run: {
			handler: async (c, a, f,) => {
				const script = resolveCodeInput(a, f,);
				const run = await c.scenarios.runScript(script, {
					envName: f["env"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
					timeoutMs: num(f["timeout"],),
					keepScenario: f["keep"] === true,
					maxLogBytes: num(f["max-log-bytes"],),
				},);
				const result: Record<string, unknown> = {
					outcome: run.outcome,
					success: run.success,
					runId: run.runId,
					elapsedMs: run.elapsedMs,
					pollCount: run.pollCount,
					output: run.output ?? "",
					logTruncated: run.logTruncated,
					maxLogBytes: run.maxLogBytes,
				};
				if (f["full-log"] === true || run.output === undefined) {
					result.log = run.log;
				}
				return result;
			},
			usage: CODE_RUN_USAGE,
			description:
				"Run one-off Python in a DSS code env via a throwaway custom-python scenario; returns the script's captured output (stdout+stderr) plus outcome/success. Log retrieval is capped by --max-log-bytes (default 1048576); pass --full-log to include the capped raw DSS run log. Exits 4 on a non-SUCCESS outcome.",
			examples: [
				"dss code run --file inspect.py",
				"dss code run --file inspect.py --env py39_pandas",
				"cat snippet.py | dss code run --stdin",
				"dss code run --file inspect.py --full-log",
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
// Agent-facing command inventory
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

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

class UsageError extends Error {
	readonly code: StableErrorCode;
	readonly hint?: string;
	readonly details?: Record<string, unknown>;

	constructor(
		message: string,
		code: StableErrorCode = "usage_error",
		hint?: string,
		details?: Record<string, unknown>,
	) {
		super(message,);
		this.name = "UsageError";
		this.code = code;
		this.hint = hint;
		this.details = details;
	}
}

const COMMANDS_RUN_HINT = "Use `dss commands run` for machine-readable command discovery.";

function unsupportedHelpFlag(): UsageError {
	return new UsageError(
		"Help screens are not supported.",
		"usage_error",
		COMMANDS_RUN_HINT,
		{ command: "dss commands run", },
	);
}

function noCommandError(): UsageError {
	return new UsageError(
		"No command provided.",
		"usage_error",
		COMMANDS_RUN_HINT,
		{ command: "dss commands run", resources: RESOURCE_NAMES, },
	);
}

function missingActionError(resource: string, validActions: string[], usage?: string,): UsageError {
	return new UsageError(
		`Missing action for ${resource}.`,
		"usage_error",
		usage ?? COMMANDS_RUN_HINT,
		{ resource, validActions, },
	);
}

function unknownResourceError(resource: string,): UsageError {
	return new UsageError(
		`Unknown resource: ${resource}.`,
		"usage_error",
		COMMANDS_RUN_HINT,
		{ resource, validResources: RESOURCE_NAMES, },
	);
}

function unknownActionError(
	resource: string,
	action: string | undefined,
	validActions: string[],
	hint?: string,
): UsageError {
	return new UsageError(
		`Unknown action: ${resource} ${action ?? ""}`.trim(),
		"usage_error",
		hint ?? COMMANDS_RUN_HINT,
		{ resource, action, validActions, },
	);
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

function dataikuEnvironmentEnabled(): boolean {
	return process.env.DATAIKU_DISABLE_ENV !== "1";
}

function loadEnvFile(): void {
	if (!dataikuEnvironmentEnabled()) return;
	// The invocation cwd takes precedence over the CLI install/root directory, so a
	// project-local .env where `dss` is invoked overrides defaults shipped beside the
	// CLI. First writer wins below, so cwd must be listed first.
	const dirs = [
		process.cwd(),
		resolve(dirname(fileURLToPath(import.meta.url,),), "..",),
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
	handler: (flags: Record<string, string | boolean>,) => Promise<unknown>;
	usage: string;
	description?: string;
	examples?: string[];
	requiredFlags?: string[];
}> = {
	login: {
		handler: async (flags,) => {
			const tlsSettings = resolveTlsSettings(flags,);
			const useEnv = dataikuEnvironmentEnabled();
			const url = typeof flags["url"] === "string"
				? flags["url"]
				: useEnv
				? process.env.DATAIKU_URL ?? ""
				: "";
			const apiKey = typeof flags["api-key"] === "string"
				? flags["api-key"]
				: useEnv
				? process.env.DATAIKU_API_KEY ?? ""
				: "";
			const projectKey = typeof flags["project-key"] === "string"
				? flags["project-key"]
				: useEnv
				? process.env.DATAIKU_PROJECT_KEY
				: undefined;

			if (!url || !apiKey) {
				throw new UsageError(
					"Missing --url and/or --api-key for auth login.",
					"missing_required_flag",
					"Pass --url and --api-key, or set DATAIKU_URL and DATAIKU_API_KEY.",
					{ requiredFlags: ["url", "api-key",], env: ["DATAIKU_URL", "DATAIKU_API_KEY",], },
				);
			}

			const result = await validateCredentials(url, apiKey, tlsSettings,);
			if (!result.valid) {
				if (result.dataikuError) throw result.dataikuError;
				throw new DataikuError(
					0,
					"Authentication Failed",
					result.error ?? "Credential validation failed",
				);
			}

			const path = getCredentialsPath();
			saveCredentials({ url, apiKey, projectKey, ...tlsSettings, },);
			return { saved: true, path, };
		},
		usage: "dss auth login --url URL --api-key KEY [--project-key KEY] [--insecure] [--ca-cert PATH]",
		description: "Validate and save DSS credentials from flags or environment variables.",
		examples: [
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY",
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ",
		],
		requiredFlags: ["url", "api-key",],
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
			: "Missing Dataiku URL and/or API key. Set DATAIKU_URL/DATAIKU_API_KEY or pass --url/--api-key.",
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
		throw new UsageError(
			"Missing Dataiku URL. Set DATAIKU_URL or pass --url.",
			"missing_required_flag",
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY or pass --api-key.",
			"missing_required_flag",
		);
	}
	if (!projectKey) {
		throw new UsageError(
			"Missing project key. Set DATAIKU_PROJECT_KEY or pass --project-key.",
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
type CommandIdempotency = "safe" | "convergent" | "if-not-exists" | "if-exists" | "none";

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

interface CommandFlagChoice {
	oneOf: string[][];
}

interface CommandRegistryEntry {
	resource: string;
	action: string;
	usage: string;
	description?: string;
	examples?: string[];
	flags: Array<
		{ name: string; kind: "boolean" | "value"; valueType?: string; enumValues?: string[]; }
	>;
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
	requiredOneOf?: CommandFlagChoice[];
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

const GLOBAL_AGENT_FLAGS = ["json", "verbose", "fields",];
const AUTHENTICATED_AGENT_FLAGS = [
	"url",
	"api-key",
	"request-timeout",
	"retries",
	"insecure",
	"ca-cert",
];
const COMMANDS_USAGE = "dss commands run [--json]";
const COMMANDS_DESCRIPTION = "Print the machine-readable command registry for agent planning.";
const COMMANDS_EXAMPLES = ["dss commands run", "dss commands run --json",];
const VERSION_USAGE = "dss version";
const VERSION_DESCRIPTION = "Print the CLI version and git revision as JSON.";
const VERSION_EXAMPLES = ["dss version", "dss --version",];
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

const ALLOWED_CLEANUP_ACTIONS: ReadonlySet<string> = new Set([
	// Must mirror every cleanup.argv shape emitted by cleanupLedgerEntry().
	"dataset delete",
	"recipe delete",
	"scenario delete",
	"flow-zone delete",
	"wiki delete",
	"dashboard delete",
	"insight delete",
	"data-quality delete-rule",
	"code-env delete",
	"folder delete-file",
],);

function isAllowedCleanupAction(resource: string, action: string,): boolean {
	return ALLOWED_CLEANUP_ACTIONS.has(`${resource} ${action}`,);
}

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
	if (
		resource === "doctor" || resource === "commands" || resource === "fixtures"
		|| resource === "version"
	) {
		return "read";
	}
	if (resource === "install-skill") return "write";
	if (resource === "data-quality" && action === "compute") return "write";
	if (READ_ACTIONS.has(action,)) return "read";
	if (
		/^(create|clone|restore|update|delete|set|save|upload|run|build|abort|move|refresh|clear|unload|install|login|logout|add|remove|publish|activate|deploy|import|export|preload|upgrade|stop|restart)/
			.test(action,)
	) {
		return "write";
	}
	return "read";
}

function inferRequiresAuth(resource: string,): boolean {
	return resource !== "auth"
		&& resource !== "commands"
		&& resource !== "install-skill"
		&& resource !== "version";
}

function inferRequiresProject(resource: string, action: string, usage: string,): boolean {
	if (
		resource === "auth" || resource === "doctor" || resource === "commands"
		|| resource === "install-skill" || resource === "version"
	) {
		return false;
	}
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
],);

function inferOutputShape(resource: string, action: string,): CommandOutputShape {
	if (
		resource === "auth" || resource === "commands" || resource === "install-skill"
		|| resource === "version"
	) {
		return "object";
	}
	if (ARRAY_OUTPUT_ACTIONS.has(action,)) return "array";
	if (resource === "dataset" && action === "download") return "object";
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

function stripAllUsageGroups(usage: string,): string {
	return usage.replace(/\[[^\]]*\]/g, " ",).replace(/\([^)]*\)/g, " ",);
}

function topLevelParenGroups(usage: string,): string[] {
	const groups: string[] = [];
	let depth = 0;
	let current = "";
	for (const char of usage) {
		if (char === "(") {
			if (depth > 0) current += char;
			else current = "";
			depth++;
		} else if (char === ")") {
			depth--;
			if (depth === 0) groups.push(current,);
			else current += char;
		} else if (depth > 0) {
			current += char;
		}
	}
	return groups;
}

function splitTopLevelChoices(group: string,): string[] {
	const parts: string[] = [];
	let depth = 0;
	let current = "";
	for (const char of group) {
		if (char === "[" || char === "(") depth++;
		else if (char === "]" || char === ")") depth--;
		if (char === "|" && depth === 0) {
			parts.push(current,);
			current = "";
		} else {
			current += char;
		}
	}
	parts.push(current,);
	return parts;
}

function flagsInUsageFragment(fragment: string,): string[] {
	return extractUsageFlags(fragment.replace(/\[[^\]]*\]/g, " ",),);
}

/**
 * Split required usage flags into unconditional flags and mutually-exclusive
 * choice groups. A required `(--a X | --b Y)` group becomes a requiredOneOf entry
 * (pick exactly one alternative; an alternative listing several flags must be
 * supplied together) instead of marking every flag as unconditionally required.
 */
function deriveRequiredUsage(
	usage: string,
): { requiredFlags: string[]; requiredOneOf: CommandFlagChoice[]; } {
	const requiredFlags = extractUsageFlags(stripAllUsageGroups(usage,),);
	const requiredOneOf: CommandFlagChoice[] = [];
	for (const group of topLevelParenGroups(usage,)) {
		const alternatives = splitTopLevelChoices(group,);
		if (alternatives.length <= 1) {
			requiredFlags.push(...flagsInUsageFragment(group,),);
			continue;
		}
		const oneOf = alternatives
			.map((alternative,) => flagsInUsageFragment(alternative,))
			.filter((alternativeFlags,) => alternativeFlags.length > 0);
		if (oneOf.length > 1) requiredOneOf.push({ oneOf, },);
		else if (oneOf.length === 1) requiredFlags.push(...oneOf[0]!,);
	}
	return { requiredFlags: uniqueStrings(requiredFlags,), requiredOneOf, };
}

const GLOBAL_FLAG_VALUE_HINTS: Record<string, { valueType: string; enumValues?: string[]; }> = {
	url: { valueType: "URL", },
	fields: { valueType: "CSV", },
	"api-key": { valueType: "KEY", },
	"request-timeout": { valueType: "MS", },
	retries: { valueType: "N", },
	"ca-cert": { valueType: "PATH", },
	"project-key": { valueType: "KEY", },
	"record-cleanup": { valueType: "PATH", },
};

/** Derive a value placeholder (and enum members) for each value flag from its usage token. */
function extractFlagValueHints(
	usage: string,
): Map<string, { valueType: string; enumValues?: string[]; }> {
	const hints = new Map<string, { valueType: string; enumValues?: string[]; }>();
	for (const match of usage.matchAll(/--([a-z0-9-]+)\s+([a-z]+(?:\|[a-z]+)+)/g,)) {
		const flag = FLAG_ALIASES[match[1]!] ?? match[1]!;
		if (!hints.has(flag,)) {
			hints.set(flag, { valueType: "enum", enumValues: match[2]!.split("|",), },);
		}
	}
	for (const match of usage.matchAll(/--([a-z0-9-]+)\s+(<[^>]+>|[A-Z][A-Za-z0-9_]*)/g,)) {
		const flag = FLAG_ALIASES[match[1]!] ?? match[1]!;
		if (!hints.has(flag,)) hints.set(flag, { valueType: match[2]!, },);
	}
	return hints;
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
	if (resource === "code" && action === "run") return "future";
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
	if (/^(clear|refresh|set|save)/.test(action,)) return "convergent";
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
	const derivedRequired = deriveRequiredUsage(meta.usage,);
	const requiredFlags = meta.requiredFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.requiredFlags
		?? derivedRequired.requiredFlags;
	const requiredOneOf = meta.requiredOneOf
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.requiredOneOf
		?? derivedRequired.requiredOneOf;
	const oneOfFlags = new Set(requiredOneOf.flatMap((choice,) => choice.oneOf.flat()),);
	const optionalFlags = meta.optionalFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.optionalFlags
		?? flags.filter((flag,) => !requiredFlags.includes(flag,) && !oneOfFlags.has(flag,));
	const valueHints = extractFlagValueHints(meta.usage,);
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
		flags: flags.map((name,) => {
			const kind = flagKind(name,);
			if (kind === "boolean") return { name, kind, };
			const hint = valueHints.get(name,) ?? GLOBAL_FLAG_VALUE_HINTS[name];
			if (!hint) return { name, kind, };
			return {
				name,
				kind,
				valueType: hint.valueType,
				...(hint.enumValues ? { enumValues: hint.enumValues, } : {}),
			};
		},),
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
		...(requiredOneOf.length > 0 ? { requiredOneOf, } : {}),
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
	registry.version = {
		run: buildRegistryEntry("version", "run", {
			handler: async () => undefined,
			usage: VERSION_USAGE,
			description: VERSION_DESCRIPTION,
			examples: VERSION_EXAMPLES,
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
	registry.batch = {
		run: buildRegistryEntry("batch", "run", {
			handler: async () => undefined,
			usage: BATCH_USAGE,
			description: BATCH_DESCRIPTION,
			examples: BATCH_EXAMPLES,
			examplePayload: BATCH_EXAMPLE_PAYLOAD,
			payloadSchema: { stdin: true, dataFlag: true, dataFileFlag: true, jsonShape: "array", },
		},),
	};
	registry.auth = {};
	for (const [action, meta,] of Object.entries(AUTH_ACTIONS,)) {
		registry.auth[action] = buildRegistryEntry("auth", action, {
			handler: async () => undefined,
			usage: meta.usage,
			description: meta.description,
			examples: meta.examples,
			requiredFlags: meta.requiredFlags,
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
		throw new UsageError(
			"Missing Dataiku URL. Set DATAIKU_URL or pass --url.",
			"missing_required_flag",
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY or pass --api-key.",
			"missing_required_flag",
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
			if (
				!resource || !action || !isAllowedCleanupAction(resource, action,)
				|| !commands[resource]?.[action]
			) {
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

const BATCH_USAGE =
	"dss batch (--data JSON|--data-file PATH|--stdin) [--continue-on-error] [--dry-run]";
const BATCH_DESCRIPTION =
	"Run a sequence of dss commands from a JSON array of argv arrays. Fail-fast by default; returns one envelope with a per-step ok/result/error and exits non-zero if any step failed.";
const BATCH_HINT =
	'Pass a JSON array of argv arrays, e.g. [["dataset","list"],["recipe","update","r","--data-file","p.json"]].';
const BATCH_EXAMPLE_PAYLOAD: string[][] = [
	["recipe", "set-payload", "compute_orders", "--file", "code.py", "--no-backup",],
	["recipe", "update", "compute_orders", "--data-file", "env.json",],
	["dataset", "update", "orders", "--data-file", "ds.json",],
];
const BATCH_EXAMPLES = [
	"dss batch --data-file steps.json",
	"dss batch --stdin --continue-on-error",
];

interface BatchStepResult {
	index: number;
	args: string[];
	resource?: string;
	action?: string;
	ok: boolean | null;
	result?: unknown;
	error?: ErrorReportEnvelope;
	skipped?: boolean;
}

function parseBatchSteps(payload: unknown,): string[][] {
	if (!Array.isArray(payload,)) {
		throw new UsageError(
			"Batch payload must be a JSON array of command-argument arrays.",
			"validation_failed",
			BATCH_HINT,
			{ example: BATCH_EXAMPLE_PAYLOAD, },
		);
	}
	return payload.map((step, index,) => {
		if (!Array.isArray(step,) || !step.every((token,) => typeof token === "string")) {
			throw new UsageError(
				`Batch step ${index} must be an array of string arguments.`,
				"validation_failed",
				BATCH_HINT,
			);
		}
		return step as string[];
	},);
}

async function runBatch(flags: Record<string, string | boolean>,): Promise<{
	result: Record<string, unknown>;
	exitCode: number;
}> {
	const payload = unknownJsonInput(flags,);
	if (payload === undefined) {
		throw new UsageError(
			`Provide steps via --data, --data-file, or --stdin. Usage: ${BATCH_USAGE}`,
			"missing_required_flag",
			BATCH_HINT,
		);
	}
	const steps = parseBatchSteps(payload,);

	if (flags["dry-run"] === true) {
		const planned = steps.map((argv, index,) => {
			const { positional, } = parseArgs(argv,);
			const resource = positional[0];
			const action = positional[1];
			const runnable = Boolean(resource && action && commands[resource]?.[action],);
			return { index, args: argv, resource, action, runnable, };
		},);
		return {
			result: { dryRun: true, total: steps.length, steps: planned, },
			exitCode: planned.every((step,) => step.runnable) ? 0 : 1,
		};
	}

	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	if (!url) {
		throw new UsageError(
			"Missing Dataiku URL.",
			"missing_required_flag",
			"Set DATAIKU_URL or pass --url.",
			{
				requiredFlags: ["url",],
				env: ["DATAIKU_URL",],
			},
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key.",
			"missing_required_flag",
			"Set DATAIKU_API_KEY or pass --api-key.",
			{
				requiredFlags: ["api-key",],
				env: ["DATAIKU_API_KEY",],
			},
		);
	}
	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs: num(flags["request-timeout"],),
		retryMaxAttempts: num(flags["retries"],),
		tlsRejectUnauthorized,
		caCertPath,
	},);

	const continueOnError = flags["continue-on-error"] === true;
	const results: BatchStepResult[] = [];
	let firstFailureExit: number | undefined;

	for (let index = 0; index < steps.length; index++) {
		const argv = steps[index]!;
		const { positional, flags: stepFlags, } = parseArgs(argv,);
		const resource = positional[0];
		const action = positional[1];
		if (firstFailureExit !== undefined && !continueOnError) {
			results.push({ index, args: argv, resource, action, ok: null, skipped: true, },);
			continue;
		}
		currentCommandContext = {
			resource,
			action,
			projectKey: typeof stepFlags["project-key"] === "string" ? stepFlags["project-key"] : projectKey,
		};
		try {
			if (!resource) throw noCommandError();
			const resourceActions = commands[resource];
			if (!resourceActions) throw unknownResourceError(resource,);
			if (!action) {
				throw missingActionError(resource, Object.keys(resourceActions,), `dss ${resource} <action>`,);
			}
			const meta = resourceActions[action];
			if (!meta) throw unknownActionError(resource, action, Object.keys(resourceActions,),);
			const result = await meta.handler(client, positional.slice(2,), stepFlags,);
			const failureExitCode = commandFailureExitCode(result,);
			if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
			const stepFieldsFlag = stepFlags["fields"];
			const stepFields = typeof stepFieldsFlag === "string"
				? stepFieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) => field.length > 0)
				: [];
			const stepResult = stepFields.length > 0 ? projectResultFields(result, stepFields,) : result;
			results.push({ index, args: argv, resource, action, ok: true, result: stepResult, },);
		} catch (error) {
			const envelope = buildErrorReport(error,);
			results.push({ index, args: argv, resource, action, ok: false, error: envelope, },);
			if (firstFailureExit === undefined) firstFailureExit = envelope.exitCode;
		}
	}

	const ok = firstFailureExit === undefined;
	return {
		result: {
			ok,
			total: steps.length,
			completed: results.filter((step,) => step.ok !== null).length,
			steps: results,
		},
		exitCode: ok ? 0 : firstFailureExit ?? 2,
	};
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
	const useEnv = dataikuEnvironmentEnabled();

	if (useEnv) {
		if (!hasUrlFlag) url ??= process.env.DATAIKU_URL;
		if (!hasApiKeyFlag) apiKey ??= process.env.DATAIKU_API_KEY;
		if (!hasProjectKeyFlag) projectKey ??= process.env.DATAIKU_PROJECT_KEY;
	}

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
	ok: false;
	error: string;
	code: StableErrorCode;
	category: "usage" | "dss" | "internal";
	exitCode: number;
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

function commandIsProjectScoped(
	resource: string | undefined,
	action: string | undefined,
): boolean {
	if (!resource) return false;
	const usage = commands[resource]?.[action ?? ""]?.usage ?? "";
	return inferRequiresProject(resource, action ?? "", usage,);
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
	const resource = currentCommandContext.resource ?? positionals[0];
	const action = currentCommandContext.action ?? positionals[1];
	const explicitProjectKey = rawFlagValue(argv, "project-key",) ?? rawFlagValue(argv, "project",);
	const ambientProjectKey = dataikuEnvironmentEnabled()
		? process.env.DATAIKU_PROJECT_KEY
		: undefined;
	return {
		resource,
		action,
		projectKey: explicitProjectKey
			?? (commandIsProjectScoped(resource, action,)
				? currentCommandContext.projectKey ?? ambientProjectKey
				: undefined),
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

function errorExitCode(err: unknown,): number {
	if (err instanceof CommandResultFailure) return err.exitCode;
	if (err instanceof UsageError) return 1;
	if (err instanceof DataikuError) return err.category === "transient" ? 3 : 2;
	return 2;
}

function buildErrorReport(err: unknown,): ErrorReportEnvelope {
	const context = rawCommandContext();
	const exitCode = errorExitCode(err,);
	if (err instanceof UsageError) {
		return {
			ok: false,
			error: err.message,
			code: err.code,
			category: "usage",
			exitCode,
			...(err.hint ? { hint: err.hint, } : {}),
			...(err.details ? { details: err.details, } : {}),
			...context,
		};
	}
	if (err instanceof CommandResultFailure) {
		return {
			ok: false,
			error: err.message,
			code: "long_running_failure",
			category: "dss",
			exitCode: err.exitCode,
			details: { result: err.result, },
			...context,
		};
	}
	if (err instanceof DataikuError) {
		return {
			ok: false,
			error: err.message,
			code: dataikuErrorCode(err.category,),
			category: "dss",
			exitCode,
			hint: err.retryHint,
			status: err.status,
			retryable: err.retryable,
			requestId: err.requestId ?? requestIdFromBody(err.body,),
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
		ok: false,
		error: message,
		code: "internal_error",
		category: "internal",
		exitCode,
		...context,
	};
}

function writeErrorReport(err: unknown,): void {
	process.stderr.write(`${JSON.stringify(buildErrorReport(err,), null, 2,)}\n`,);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	loadEnvFile();
	const { positional, flags, } = parseArgs(process.argv.slice(2,),);
	const fieldsFlag = flags["fields"];
	if (typeof fieldsFlag === "string") {
		const selected = fieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) =>
			field.length > 0
		);
		if (selected.length > 0) outputFieldProjection = selected;
	}

	if (flags["version"] === true) {
		writeCommandResult(cliVersionResult(),);
		return;
	}

	if (positional.length === 0) throw noCommandError();

	const resource = positional[0]!;
	currentCommandContext = {
		resource,
		action: positional[1],
		projectKey: typeof flags["project-key"] === "string"
			? flags["project-key"]
			: dataikuEnvironmentEnabled()
			? process.env.DATAIKU_PROJECT_KEY
			: undefined,
	};

	if (resource === "doctor") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("doctor", action, ["run",],);
		}
		const { result, exitCode, } = await runDoctor(flags,);
		writeCommandResult(result,);
		if (exitCode !== 0) process.exit(exitCode,);
		return;
	}

	if (resource === "auth") {
		const action = positional[1];
		const validActions = Object.keys(AUTH_ACTIONS,);
		if (!action) {
			throw missingActionError("auth", validActions, "dss auth login --url URL --api-key KEY",);
		}
		currentCommandContext.action = action;
		const authMeta = AUTH_ACTIONS[action];
		if (!authMeta) {
			throw unknownActionError(
				"auth",
				action,
				validActions,
				"auth only supports 'login'. To check credentials/connectivity, run 'dss doctor'.",
			);
		}
		const result = await authMeta.handler(flags,);
		writeCommandResult(result,);
		return;
	}

	if (resource === "install-skill") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("install-skill", action, ["run",],);
		}

		const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
		const isGlobal = flags["global"] === true;
		const targetDir = typeof flags["target"] === "string" ? flags["target"] : undefined;

		const targets = (() => {
			if (!agentFilter) return detectAgents();
			const def = AGENTS[agentFilter];
			if (!def) {
				throw new UsageError(
					`Unknown agent: ${agentFilter}.`,
					"usage_error",
					COMMANDS_RUN_HINT,
					{ agent: agentFilter, validAgents: Object.keys(AGENTS,), },
				);
			}
			return [{ id: agentFilter, def, via: "flag" as const, },];
		})();

		if (flags["list-agents"] === true) {
			writeCommandResult({
				agents: targets.map((target,) => ({
					id: target.id,
					name: target.def.name,
					via: target.via,
				})),
			},);
			return;
		}

		if (targets.length === 0) {
			throw new UsageError(
				"No coding agents detected.",
				"usage_error",
				"Use --agent NAME to choose one of the supported agents.",
				{ validAgents: Object.keys(AGENTS,), },
			);
		}

		const scope = isGlobal ? "global" : "project";
		const cwd = targetDir ?? (isGlobal ? process.cwd() : findWorkspaceRoot(process.cwd(),));
		const installed = planSkillInstalls(targets, { global: isGlobal, cwd, },);

		if (flags["plan"] === true) {
			writeCommandResult(planResult("install-skill", "run", {
				identifiers: { scope, target: cwd, },
				payload: { installed, },
				idempotency: "none",
				asyncKind: "none",
				exitCodesOnFailure: { usage: 1, error: 2, transient: 3, },
				plannedAndDryRun: flags["dry-run"] === true,
			},),);
			return;
		}

		writeCommandResult({
			scope,
			target: cwd,
			installed: flags["dry-run"] === true
				? installed
				: installSkill(targets, { global: isGlobal, cwd, },),
			...(flags["dry-run"] === true ? { dryRun: true, } : {}),
		},);
		return;
	}

	if (resource === "commands") {
		const action = positional[1];
		if (!action) throw missingActionError("commands", ["run",], COMMANDS_USAGE,);
		currentCommandContext.action = action;
		if (action !== "run") throw unknownActionError("commands", action, ["run",],);
		writeCommandResult(buildCommandRegistry(),);
		return;
	}

	if (resource === "version") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("version", action, ["run",],);
		}
		writeCommandResult(cliVersionResult(),);
		return;
	}

	if (resource === "cleanup") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("cleanup", action, ["run",],);
		}
		const { result, exitCode, } = await runCleanup(flags,);
		writeCommandResult(result,);
		if (exitCode !== 0) process.exit(exitCode,);
		return;
	}

	if (resource === "fixtures") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("fixtures", action, ["run",],);
		}
		const result = await runFixtures(flags,);
		writeCommandResult(result,);
		return;
	}

	if (resource === "batch") {
		const action = positional[1];
		currentCommandContext.action = action ?? "run";
		if (action !== undefined && action !== "run") {
			throw unknownActionError("batch", action, ["run",],);
		}
		const { result, exitCode, } = await runBatch(flags,);
		writeCommandResult(result,);
		if (exitCode !== 0) process.exit(exitCode,);
		return;
	}

	if (!commands[resource]) throw unknownResourceError(resource,);

	const resourceActions = commands[resource]!;
	if (positional.length === 1) {
		throw missingActionError(
			resource,
			Object.keys(resourceActions,),
			`dss ${resource} <action> [args...]`,
		);
	}

	const action = positional[1]!;
	currentCommandContext.action = action;
	const actionMeta = resourceActions[action];

	if (!actionMeta) throw unknownActionError(resource, action, Object.keys(resourceActions,),);

	const args = positional.slice(2,);
	if (flags["plan"] === true) {
		const plan = buildMutationPlan(resource, action, actionMeta, args, flags,);
		writeCommandResult(plan,);
		return;
	}

	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	currentCommandContext.projectKey = projectKey;

	if (!url) {
		throw new UsageError(
			"Missing Dataiku URL.",
			"missing_required_flag",
			"Set DATAIKU_URL or pass --url.",
			{ requiredFlags: ["url",], env: ["DATAIKU_URL",], },
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key.",
			"missing_required_flag",
			"Set DATAIKU_API_KEY or pass --api-key.",
			{ requiredFlags: ["api-key",], env: ["DATAIKU_API_KEY",], },
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
	const failureExitCode = commandFailureExitCode(result,);
	if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
	if (flags["raw"] === true && typeof result === "string" && typeof flags["output"] !== "string") {
		process.stdout.write(result,);
	} else {
		writeCommandResult(result,);
	}
}

main().catch((err: unknown,) => {
	writeErrorReport(err,);
	process.exit(errorExitCode(err,),);
},);
