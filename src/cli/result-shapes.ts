/**
 * Compact default results of the four large read commands (`job get`,
 * `dataset info`, `dataset get`, `project get`), plus the single dispatch
 * point for compacting command results (`shapeCommandResult`).
 *
 * The SDK resources keep returning the raw DSS objects; only the CLI compacts.
 * `--full` returns the DSS object unchanged, and `--fields` projects DSS paths
 * out of it (exactly like the compact `*.list` shapes in list-output.ts).
 *
 * Imports no schema graph: the discovery schemas below are plain data.
 */
import { isRecord, } from "../utils/records.js";
import { shapeListResult, } from "./list-output.js";
import { DATASET_GET_DROPPED_KEYS, PROJECT_GET_DROPPED_KEYS, } from "./read-shapes.js";

type Item = Record<string, unknown>;

function record(value: unknown,): Item {
	return isRecord(value,) ? value : {};
}

function nonEmptyString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

/** Drop `undefined` members so absent optional fields are omitted, never null. */
function defined(fields: Item,): Item {
	return Object.fromEntries(Object.entries(fields,).filter(([, value,],) => value !== undefined),);
}

/** `{ type, message }` of a DSS error object (`error`, `firstFailure`); undefined when empty. */
function errorSummary(value: unknown,): Item | undefined {
	if (!isRecord(value,)) return undefined;
	const shaped = defined({
		type: nonEmptyString(value["errorType"],),
		message: nonEmptyString(value["detailedMessage"],) ?? nonEmptyString(value["message"],),
	},);
	return Object.keys(shaped,).length > 0 ? shaped : undefined;
}

/**
 * Output id/project of a job output across DSS shapes: `targetDataset` +
 * `targetDatasetProjectKey`, `targetManagedFolder`, `targetSavedModel`, ...
 * and the plain `{ id, projectKey }` form.
 */
function outputTarget(output: Item,): { id: unknown; projectKey: unknown; } {
	const targetKey = Object.keys(output,).find((key,) =>
		/^target[A-Z]/.test(key,) && !key.endsWith("ProjectKey",) && key !== "targetPartition"
		&& typeof output[key] === "string"
	);
	return {
		id: targetKey === undefined ? output["id"] : output[targetKey],
		projectKey: targetKey === undefined ? output["projectKey"] : output[`${targetKey}ProjectKey`],
	};
}

function shapeJobOutput(output: unknown, jobProjectKey: unknown,): Item {
	const fields = record(output,);
	const { id, projectKey, } = outputTarget(fields,);
	const partition = fields["targetPartition"] ?? fields["partition"];
	return defined({
		type: fields["type"],
		id,
		projectKey: typeof projectKey === "string" && projectKey !== jobProjectKey
			? projectKey
			: undefined,
		partition: typeof partition === "string" && partition !== "NP" ? partition : undefined,
	},);
}

function shapeJobActivity(activity: unknown,): Item {
	const fields = record(activity,);
	return defined({
		id: fields["activityId"],
		recipe: fields["recipeName"],
		recipeType: fields["recipeType"],
		state: fields["state"],
		totalTime: fields["totalTime"],
		warnings: record(fields["warnings"],)["totalCount"],
		error: errorSummary(fields["firstFailure"],),
		message: nonEmptyString(fields["message"],),
	},);
}

/** `job get`: state, outputs, progress, and the failure; never the log tail. */
function shapeJobGet(result: unknown,): unknown {
	if (!isRecord(result,)) return result;
	const base = record(result["baseStatus"],);
	const def = record(base["def"],);
	const activities = record(base["activities"],);
	return defined({
		id: def["id"],
		name: def["name"],
		type: def["type"],
		state: base["state"],
		initiator: nonEmptyString(def["initiator"],)
			?? nonEmptyString(record(result["initiator"],)["login"],),
		triggeredFrom: def["triggeredFrom"],
		startTime: base["jobStartTime"],
		endTime: base["jobEndTime"],
		outputs: Array.isArray(def["outputs"],)
			? def["outputs"].map((output,) => shapeJobOutput(output, def["projectKey"],))
			: undefined,
		progress: result["globalState"],
		error: errorSummary(result["error"],),
		activities: Object.values(activities,).map(shapeJobActivity,),
	},);
}

/** Extra column keys worth keeping when set (DSS sends most as ""/defaults). */
const COLUMN_EXTRA_KEYS = ["meaning", "comment",] as const;

function shapeColumn(column: unknown,): Item {
	const fields = record(column,);
	const shaped: Item = { name: fields["name"], type: fields["type"], };
	for (const key of COLUMN_EXTRA_KEYS) {
		const value = nonEmptyString(fields[key],);
		if (value !== undefined) shaped[key] = value;
	}
	return shaped;
}

function shapeLastBuild(value: unknown,): Item | undefined {
	if (!isRecord(value,)) return undefined;
	const shaped = defined({
		jobId: value["jobId"],
		success: value["buildSuccess"],
		startTime: value["buildStartTime"],
		endTime: value["buildEndTime"],
	},);
	return Object.keys(shaped,).length > 0 ? shaped : undefined;
}

/** `dataset info`: what the dataset is, its schema, flow neighbours, and freshness. */
function shapeDatasetInfo(result: unknown,): unknown {
	if (!isRecord(result,)) return result;
	const dataset = record(result["dataset"],);
	const params = record(dataset["params"],);
	const columns = record(dataset["schema"],)["columns"];
	const tags = dataset["tags"];
	const quality = record(result["dataQualityStatus"],);
	const versioning = record(result["versioning"],);
	const timeline = record(result["timeline"],);
	return defined({
		name: result["name"] ?? dataset["name"],
		type: result["type"] ?? dataset["type"],
		managed: dataset["managed"],
		connection: params["connection"] ?? params["uploadConnection"],
		formatType: dataset["formatType"],
		partitioned: result["partitioned"],
		schema: Array.isArray(columns,) ? columns.map(shapeColumn,) : undefined,
		recipes: Array.isArray(result["recipes"],)
			? result["recipes"].map((recipe,) => record(recipe,)["name"])
			: undefined,
		buildable: result["buildable"],
		upstreamBuildable: result["upstreamBuildable"],
		downstreamBuildable: result["downstreamBuildable"],
		tags: Array.isArray(tags,) && tags.length > 0 ? tags : undefined,
		dataQuality: Object.keys(quality,).length > 0 && quality["numberItems"] !== 0
			? result["dataQualityStatus"]
			: undefined,
		lastBuild: shapeLastBuild(result["lastBuild"],),
		lastModifiedOn: versioning["lastModifiedOn"] ?? timeline["lastModifiedOn"],
		lastModifiedBy: record(versioning["lastModifiedBy"] ?? timeline["lastModifiedBy"],)["login"],
	},);
}

/**
 * Recursively remove empty strings, arrays and objects (including objects that
 * become empty). `false`, `0` and `null` are kept. Array positions are
 * preserved: an element that empties out stays as its empty value.
 */
function pruneEmpty(value: unknown,): unknown {
	if (typeof value === "string") return value.length === 0 ? undefined : value;
	if (Array.isArray(value,)) {
		if (value.length === 0) return undefined;
		return value.map((item,) =>
			pruneEmpty(item,) ?? (typeof item === "string" ? "" : Array.isArray(item,) ? [] : {})
		);
	}
	if (isRecord(value,)) {
		const pruned: Item = {};
		for (const [key, member,] of Object.entries(value,)) {
			const kept = pruneEmpty(member,);
			if (kept !== undefined) pruned[key] = kept;
		}
		return Object.keys(pruned,).length === 0 ? undefined : pruned;
	}
	return value;
}

function withoutKeys(source: Item, keys: readonly string[],): Item {
	return Object.fromEntries(Object.entries(source,).filter(([key,],) => !keys.includes(key,)),);
}

/** `dataset get`: the settings object minus metadata and empty/default noise. */
function shapeDatasetGet(result: unknown,): unknown {
	if (!isRecord(result,)) return result;
	const settings = withoutKeys(result, DATASET_GET_DROPPED_KEYS,);
	const formatParams = settings["formatParams"];
	if (isRecord(formatParams,)) {
		settings["formatParams"] = withoutKeys(formatParams, ["hiveSeparators",],);
	}
	return pruneEmpty(settings,) ?? {};
}

const PERMISSION_KEY = /^can[A-Z]/;

/**
 * `project get`: as `dataset get`, with the boolean permission flags
 * (`can*`, `isProjectAdmin`) collapsed to the denied ones. Absent from the
 * list means granted, so nothing is lost.
 */
function shapeProjectGet(result: unknown,): unknown {
	if (!isRecord(result,)) return result;
	const settings = withoutKeys(result, PROJECT_GET_DROPPED_KEYS,);
	const deniedPermissions: string[] = [];
	for (const [key, value,] of Object.entries(settings,)) {
		if (typeof value !== "boolean" || !(PERMISSION_KEY.test(key,) || key === "isProjectAdmin")) {
			continue;
		}
		if (!value) deniedPermissions.push(key,);
		delete settings[key];
	}
	const pruned = record(pruneEmpty(settings,),);
	return { ...pruned, deniedPermissions, };
}

/** Compact read shapers by `resource.action`. */
const READ_SHAPERS: Record<string, (result: unknown,) => unknown> = {
	"job.get": shapeJobGet,
	"dataset.info": shapeDatasetInfo,
	"dataset.get": shapeDatasetGet,
	"project.get": shapeProjectGet,
};

/**
 * Compact a command result for agents: `*.list` items (filter, cap, compact
 * fields) and the four large read commands. `--full` and `--fields` bypass
 * the read shapers so the DSS object (or a projection of it) is returned.
 */
export function shapeCommandResult(
	resource: string,
	action: string,
	result: unknown,
	flags: Record<string, string | boolean>,
): unknown {
	if (action === "list") return shapeListResult(resource, result, flags,);
	const shaper = READ_SHAPERS[`${resource}.${action}`];
	if (!shaper || flags["full"] === true || typeof flags["fields"] === "string") return result;
	return shaper(result,);
}
