/**
 * Canonical discovery output schemas, keyed by `resource.action`.
 *
 * TypeBox definitions from `packages/types` plus the inline literal schemas
 * that exist only on the CLI side (code-env log envelope, code.run outcome,
 * project-library payloads, notebook saves, sql.query handoff envelope).
 * `scripts/generate-action-output-schemas.mjs` serializes this map into
 * `src/generated/action-output-schemas.json`; discovery (src/cli/contract.ts)
 * reads that generated JSON as plain data, so the TypeBox schema graph never
 * loads on the discovery path. SDK validation keeps using the TypeBox schema
 * objects from `packages/types` directly.
 *
 * Imported by the generator and the discovery parity test only.
 */
import { compactListOutputSchemas, } from "./list-shapes.js";
import {
	compactReadOutputSchemas,
	DATASET_GET_DROPPED_KEYS,
	PROJECT_GET_DROPPED_KEYS,
} from "./read-shapes.js";

const PROJECT_LIBRARY_ITEM_OUTPUT_SCHEMA: Record<string, unknown> = {
	type: "object",
	additionalProperties: false,
	required: ["name",],
	properties: {
		name: { type: "string", },
		path: { type: "string", },
		size: { type: "integer", minimum: 0, },
		mimeType: { type: "string", },
		hasData: { type: "boolean", },
		lastModified: { type: "number", },
		children: { type: "array", items: { type: "object", additionalProperties: true, }, },
	},
};

const NOTEBOOK_SAVE_OUTPUT_SCHEMA: Record<string, unknown> = {
	type: "object",
	additionalProperties: false,
	required: ["saved", "resource", "created", "hash",],
	properties: {
		saved: { type: "string", },
		resource: { enum: ["jupyter-notebook", "sql-notebook",], },
		created: { type: "boolean", },
		hash: { type: "string", pattern: "^[a-f0-9]{64}$", },
	},
};

/**
 * `notebook.save-sql` adds `requested`: on create DSS allocates the persisted
 * id (`saved`), while the requested handle only becomes the display name.
 */
const NOTEBOOK_SAVE_SQL_OUTPUT_SCHEMA: Record<string, unknown> = {
	...NOTEBOOK_SAVE_OUTPUT_SCHEMA,
	required: ["saved", "requested", "resource", "created", "hash",],
	properties: {
		...(NOTEBOOK_SAVE_OUTPUT_SCHEMA["properties"] as Record<string, unknown>),
		requested: { type: "string", },
	},
};

/**
 * Discovery schema of a compact settings object (`dataset get`, `project get`):
 * the SDK TypeBox schema minus the keys the CLI drops, with only the
 * identifiers that survive pruning required. SDK validation keeps the
 * unmodified TypeBox schema.
 */
function settingsOutputSchema(
	schema: Record<string, unknown>,
	droppedKeys: readonly string[],
	extraProperties: Record<string, unknown> = {},
): Record<string, unknown> {
	const properties = Object.fromEntries(
		Object.entries(schema["properties"] as Record<string, unknown>,).filter(([key,],) =>
			!droppedKeys.includes(key,)
		),
	);
	const required = (schema["required"] as string[] | undefined ?? []).filter((key,) =>
		!droppedKeys.includes(key,)
	);
	return {
		...schema,
		properties: { ...properties, ...extraProperties, },
		required: [...required, ...Object.keys(extraProperties,),],
	};
}

let commandOutputSchemas: Record<string, Record<string, unknown>> | undefined;
/**
 * Canonical TypeBox-derived output schemas, keyed by `resource.action`. This is
 * the single source of truth for `src/generated/action-output-schemas.json`,
 * which discovery reads as plain data so no TypeBox schema graph loads on the
 * discovery path. Regenerate with
 * `bun scripts/generate-action-output-schemas.mjs`; `bun run check` fails when
 * the generated file is stale. SDK validation keeps using the TypeBox schema
 * objects from `packages/types` directly and never depends on the generated file.
 */
export function typeBoxCommandOutputSchemas(): Record<string, Record<string, unknown>> {
	if (commandOutputSchemas) return commandOutputSchemas;
	const {
		CodeEnvDetailsSchema,
		CodeEnvLogSummaryArraySchema,
		CodeEnvUsageArraySchema,
		CodeEnvVersionForProjectSchema,
		DatasetDetailsSchema,
		DatasetSchemaSchema,
		FlowZoneArraySchema,
		FlowZoneSchema,
		JobWaitResultSchema,
		JupyterNotebookContentSchema,
		JupyterNotebookSummaryArraySchema,
		NotebookSessionArraySchema,
		ProjectDetailsSchema,
		ProjectMetadataSchema,
		ScenarioDetailsSchema,
		ScenarioStatusSchema,
		SqlNotebookContentSchema,
		SqlNotebookHistorySchema,
		SqlNotebookSummaryArraySchema,
		SqlQueryResponseSchema,
	} = require("../schemas.js",) as typeof import("../schemas.js");
	return commandOutputSchemas = {
		"code-env.get": CodeEnvDetailsSchema,
		"code-env.list-logs": CodeEnvLogSummaryArraySchema,
		"code-env.get-log": {
			oneOf: [
				{
					type: "object",
					required: ["log", "bytes", "truncated", "tailed", "envLang", "envName", "logName",],
					properties: {
						log: { type: "string", },
						bytes: { type: "integer", minimum: 0, },
						truncated: { type: "boolean", },
						tailed: { type: "boolean", },
						envLang: { enum: ["PYTHON", "R",], },
						envName: { type: "string", },
						logName: { type: "string", },
					},
				},
				{
					type: "object",
					required: ["path", "bytes", "truncated", "tailed", "envLang", "envName", "logName",],
					properties: {
						path: { type: "string", },
						bytes: { type: "integer", minimum: 0, },
						truncated: { type: "boolean", },
						tailed: { type: "boolean", },
						envLang: { enum: ["PYTHON", "R",], },
						envName: { type: "string", },
						logName: { type: "string", },
					},
				},
			],
		},
		"code-env.version": CodeEnvVersionForProjectSchema,
		"code-env.usages": CodeEnvUsageArraySchema,
		"code.run": {
			type: "object",
			additionalProperties: false,
			required: [
				"outcome",
				"success",
				"runId",
				"elapsedMs",
				"pollCount",
				"output",
				"logTruncated",
				"maxLogBytes",
				"cleanup",
			],
			properties: {
				outcome: { type: "string", },
				success: { type: "boolean", },
				runId: { type: "string", },
				elapsedMs: { type: "number", minimum: 0, },
				pollCount: { type: "integer", minimum: 0, },
				output: { type: "string", },
				log: { type: "string", },
				logTruncated: { type: "boolean", },
				maxLogBytes: { type: "integer", minimum: 0, },
				timedOut: { const: true, },
				timeoutMs: { type: "integer", minimum: 0, },
				cleanup: {
					type: "object",
					additionalProperties: false,
					required: ["status",],
					properties: {
						status: { enum: ["deleted", "kept", "failed",], },
						error: { type: "string", },
					},
				},
			},
		},
		"notebook.list-jupyter": JupyterNotebookSummaryArraySchema,
		"notebook.get-jupyter": JupyterNotebookContentSchema,
		"notebook.sessions-jupyter": NotebookSessionArraySchema,
		"notebook.list-sql": SqlNotebookSummaryArraySchema,
		"notebook.get-sql": SqlNotebookContentSchema,
		"notebook.history-sql": SqlNotebookHistorySchema,
		"notebook.save-jupyter": NOTEBOOK_SAVE_OUTPUT_SCHEMA,
		"notebook.save-sql": NOTEBOOK_SAVE_SQL_OUTPUT_SCHEMA,
		"project-library.list": { type: "array", items: PROJECT_LIBRARY_ITEM_OUTPUT_SCHEMA, },
		"project-library.get-bytes": {
			type: "object",
			additionalProperties: false,
			required: ["path", "bytes", "sha256",],
			properties: {
				path: { type: "string", },
				bytes: { type: "integer", minimum: 0, },
				sha256: { type: "string", pattern: "^[a-f0-9]{64}$", },
			},
		},
		"project-library.put": {
			type: "object",
			additionalProperties: false,
			required: ["updated", "bytes", "sha256",],
			properties: {
				updated: { type: "string", },
				bytes: { type: "integer", minimum: 0, },
				sha256: { type: "string", pattern: "^[a-f0-9]{64}$", },
				beforeSha256: { type: "string", pattern: "^[a-f0-9]{64}$", },
			},
		},
		"project-library.diff": {
			type: "object",
			additionalProperties: false,
			required: [
				"path",
				"unchanged",
				"added",
				"removed",
				"diff",
				"diffTruncated",
				"localSha256",
				"localBytes",
				"maxLines",
			],
			properties: {
				path: { type: "string", },
				unchanged: { type: "boolean", },
				added: { type: "integer", minimum: 0, },
				removed: { type: "integer", minimum: 0, },
				diff: { type: "string", },
				diffTruncated: { type: "boolean", },
				binary: { type: "boolean", },
				remoteAbsent: { type: "boolean", },
				remoteSha256: { type: "string", pattern: "^[a-f0-9]{64}$", },
				remoteBytes: { type: "integer", minimum: 0, },
				localSha256: { type: "string", pattern: "^[a-f0-9]{64}$", },
				localBytes: { type: "integer", minimum: 0, },
				maxLines: { type: "integer", minimum: 0, },
			},
		},
		"project.get": settingsOutputSchema(ProjectDetailsSchema, PROJECT_GET_DROPPED_KEYS, {
			deniedPermissions: {
				type: "array",
				items: { type: "string", },
				description: "Permission flags DSS reports as false; absent flags are granted.",
			},
		},),
		"project.metadata": ProjectMetadataSchema,
		"dataset.get": settingsOutputSchema(DatasetDetailsSchema, DATASET_GET_DROPPED_KEYS,),
		"dataset.schema": DatasetSchemaSchema,
		"job.wait": JobWaitResultSchema,
		"job.monitor": JobWaitResultSchema,
		"scenario.get": ScenarioDetailsSchema,
		"scenario.status": ScenarioStatusSchema,
		"flow-zone.list": FlowZoneArraySchema,
		"flow-zone.get": FlowZoneSchema,
		"sql.query": {
			anyOf: [
				SqlQueryResponseSchema,
				{
					type: "object",
					required: ["queryId", "rowCount", "preview",],
					additionalProperties: true,
					properties: {
						queryId: { type: "string", },
						rowCount: { type: "number", },
						preview: { type: "array", items: true, },
						truncated: { type: "boolean", },
						outputPath: { type: "string", },
						written: { type: "string", },
					},
				},
			],
		},
		// `*.list` and the four large reads default to compact shapes; `--full`
		// returns the DSS objects.
		...compactListOutputSchemas(),
		...compactReadOutputSchemas(),
	};
}
