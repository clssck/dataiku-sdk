/**
 * Compact default results of the large read commands (`job get`,
 * `dataset info`, `dataset get`, `project get`, `scenario get-run`,
 * `scenario last-runs`), plus the single dispatch point for compacting
 * command results (`shapeCommandResult`).
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

/** Scenario/step/job outcomes that count as a failure worth diagnosing. */
const FAILED_OUTCOMES: Record<string, true> = { FAILED: true, ABORTED: true, };

function isFailedOutcome(value: unknown,): boolean {
	return typeof value === "string" && FAILED_OUTCOMES[value] === true;
}

/** Message of a DSS `thrown` exception object (`{ clazz, message, ... }`). */
function thrownMessage(value: unknown,): string | undefined {
	const thrown = record(value,);
	return nonEmptyString(thrown["message"],) ?? nonEmptyString(thrown["detailedMessage"],);
}

function reportItems(stepRun: Item, type: string,): Item[] {
	const items = stepRun["additionalReportItems"];
	return Array.isArray(items,)
		? items.filter((item,): item is Item => isRecord(item,) && item["type"] === type)
		: [];
}

/**
 * One `stepRuns[]` entry: the step identity and outcome, the datasets it built
 * (`BUILT_DATASET` report items) and the jobs it ran (`JOB_EXECUTED`, with the
 * thrown message). The job log tail DSS embeds in the report is dropped.
 */
function shapeScenarioStep(stepRun: unknown, runProjectKey: unknown,): Item {
	const fields = record(stepRun,);
	const step = record(fields["step"],);
	const result = record(fields["result"],);
	const datasets = reportItems(fields, "BUILT_DATASET",).map((item,) => {
		const target = record(item["target"],);
		const partition = target["partition"];
		return defined({
			dataset: target["datasetName"],
			projectKey: typeof target["projectKey"] === "string" && target["projectKey"] !== runProjectKey
				? target["projectKey"]
				: undefined,
			partition: typeof partition === "string" && partition !== "NP" ? partition : undefined,
			outcome: item["outcome"],
		},);
	},);
	const jobs = reportItems(fields, "JOB_EXECUTED",).map((item,) =>
		defined({
			jobId: item["jobId"],
			outcome: item["outcome"],
			message: thrownMessage(item["thrown"],),
		},)
	);
	return defined({
		id: step["id"],
		name: step["name"],
		type: step["type"],
		outcome: result["outcome"],
		message: thrownMessage(result["thrown"],),
		datasets: datasets.length > 0 ? datasets : undefined,
		jobs: jobs.length > 0 ? jobs : undefined,
	},);
}

/**
 * First failure of a shaped run: the failed step, its failed job and dataset,
 * and the most specific message (job, then step, then scenario).
 */
function firstScenarioFailure(steps: Item[], runThrown: unknown,): Item | undefined {
	for (const step of steps) {
		const jobs = (step["jobs"] as Item[] | undefined) ?? [];
		const job = jobs.find((entry,) => isFailedOutcome(entry["outcome"],));
		if (!isFailedOutcome(step["outcome"],) && !job) continue;
		const datasets = (step["datasets"] as Item[] | undefined) ?? [];
		const dataset = datasets.find((entry,) => isFailedOutcome(entry["outcome"],));
		return defined({
			stepId: step["id"],
			jobId: job?.["jobId"],
			dataset: dataset?.["dataset"],
			message: nonEmptyString(job?.["message"],) ?? nonEmptyString(step["message"],)
				?? thrownMessage(runThrown,),
		},);
	}
	const message = thrownMessage(runThrown,);
	return message === undefined ? undefined : { message, };
}

function projectKeyArgs(projectKey: unknown,): string[] {
	return typeof projectKey === "string" && projectKey.length > 0
		? ["--project-key", projectKey,]
		: [];
}

/** The failed activity of a job: the recipe that failed and the datasets it read. */
export interface FailedActivity {
	recipe: string;
	inputs: string[];
}

/**
 * Next-step argv for a failure. With the failed recipe known: its code and the
 * schemas of its inputs (what a "column not found" or type error needs), then
 * the job's error log; else the job's error log, else the scenario run log.
 */
function failureNextSteps(
	failure: Item,
	scenarioId: unknown,
	runId: unknown,
	projectKey: unknown,
	activity: FailedActivity | undefined,
): string[][] | undefined {
	const pk = projectKeyArgs(projectKey,);
	const inspect = activity
		? [
			["dss", "recipe", "cat", activity.recipe, ...pk,],
			...activity.inputs.slice(0, 3,).map((input,) => ["dss", "dataset", "schema", input, ...pk,]),
		]
		: [];
	if (typeof failure["jobId"] === "string") {
		return [...inspect, ["dss", "job", "log", failure["jobId"], "--errors-only", ...pk,],];
	}
	if (typeof scenarioId !== "string" || typeof runId !== "string") return undefined;
	const stepArgs = typeof failure["stepId"] === "string" ? ["--step-id", failure["stepId"],] : [];
	return [["dss", "scenario", "log", scenarioId, runId, ...stepArgs, ...pk,],];
}

/** Scenario identity of a run object (`scenarioRun` or a last-runs item). */
function runScenarioIdentity(run: Item,): { scenarioId: unknown; projectKey: unknown; } {
	const scenario = record(run["scenario"],);
	const target = record(record(run["result"],)["target"],);
	return {
		scenarioId: scenario["id"] ?? target["scenarioId"],
		projectKey: scenario["projectKey"] ?? target["projectKey"],
	};
}

function runTrigger(run: Item,): Item | undefined {
	const trigger = record(record(run["trigger"],)["trigger"],);
	const shaped = defined({
		type: nonEmptyString(trigger["type"],),
		name: nonEmptyString(trigger["name"],),
	},);
	return Object.keys(shaped,).length > 0 ? shaped : undefined;
}

/**
 * Failure diagnosis of a raw `scenario get-run` report: the first failure
 * (`{ stepId, jobId, dataset, message }`) and the next command(s) to run as
 * argv arrays. With the failed recipe known, `hint` names the edit-and-rerun
 * loop, so the agent does not look up the write commands. Empty when the run did not fail.
 */
export function scenarioRunDiagnosis(
	details: unknown,
	activity?: FailedActivity,
): { failure?: Item; next?: string[][]; hint?: string; } {
	const fields = record(details,);
	const run = record(fields["scenarioRun"],);
	const result = record(run["result"],);
	const { scenarioId, projectKey, } = runScenarioIdentity(run,);
	const steps = Array.isArray(fields["stepRuns"],)
		? fields["stepRuns"].map((stepRun,) => shapeScenarioStep(stepRun, projectKey,))
		: [];
	const failed = isFailedOutcome(result["outcome"],)
		|| steps.some((step,) => isFailedOutcome(step["outcome"],));
	const failure = failed ? firstScenarioFailure(steps, result["thrown"],) : undefined;
	if (!failure) return {};
	if (!activity) {
		const next = failureNextSteps(failure, scenarioId, run["runId"], projectKey, undefined,);
		return next ? { failure, next, } : { failure, };
	}
	const located = { ...failure, recipe: activity.recipe, inputs: activity.inputs, };
	const next = failureNextSteps(located, scenarioId, run["runId"], projectKey, activity,);
	const pk = projectKeyArgs(projectKey,).join(" ",);
	const suffix = pk === "" ? "" : ` ${pk}`;
	const rerun = typeof scenarioId === "string"
		? `; rerun: dss scenario run-and-wait ${scenarioId}${suffix}`
		: "";
	const hint =
		`To fix: dss recipe get-payload ${activity.recipe} --output FILE${suffix}, edit FILE, `
		+ `dss recipe set-payload ${activity.recipe} --file FILE${suffix}${rerun}.`;
	return next ? { failure: located, next, hint, } : { failure: located, hint, };
}

/**
 * `scenario get-run`: run identity, outcome, timing, trigger, per-step
 * datasets/jobs, the first failure, and next-step argv. Never the trigger
 * auth context, run-as user, API key metadata, or job log tails.
 */
function shapeScenarioGetRun(result: unknown,): unknown {
	if (!isRecord(result,) || !isRecord(result["scenarioRun"],)) return result;
	const run = result["scenarioRun"];
	const runResult = record(run["result"],);
	const { scenarioId, projectKey, } = runScenarioIdentity(run,);
	const start = typeof run["start"] === "number" ? run["start"] : undefined;
	const end = typeof run["end"] === "number" ? run["end"] : undefined;
	const steps = Array.isArray(result["stepRuns"],)
		? result["stepRuns"].map((stepRun,) => shapeScenarioStep(stepRun, projectKey,))
		: [];
	const { failure, next, } = scenarioRunDiagnosis(result,);
	return defined({
		runId: run["runId"],
		scenarioId,
		projectKey,
		outcome: runResult["outcome"],
		start,
		end,
		durationMs: start !== undefined && end !== undefined && end >= start ? end - start : undefined,
		trigger: runTrigger(run,),
		steps,
		failure,
		next,
	},);
}

/**
 * One `scenario last-runs` item: identity, outcome, timing, trigger, failure
 * message, and for a failed run the `scenario get-run` argv that shows why.
 */
function shapeScenarioRunSummary(item: unknown,): unknown {
	if (!isRecord(item,)) return item;
	const result = record(item["result"],);
	const message = thrownMessage(result["thrown"],);
	const { scenarioId, projectKey, } = runScenarioIdentity(item,);
	const next = isFailedOutcome(result["outcome"],) && typeof scenarioId === "string"
			&& typeof item["runId"] === "string"
		? [[
			"dss",
			"scenario",
			"get-run",
			scenarioId,
			item["runId"],
			...(typeof projectKey === "string" ? ["--project-key", projectKey,] : []),
		],]
		: undefined;
	// DSS lists an in-progress run with `end: 0` and no result outcome.
	const running = result["outcome"] === undefined && item["end"] === 0;
	return defined({
		runId: item["runId"],
		outcome: running ? "RUNNING" : result["outcome"],
		start: item["start"] ?? result["start"],
		end: item["end"] ?? result["end"],
		trigger: runTrigger(item,),
		failure: message === undefined ? undefined : { message, },
		next,
	},);
}

/** `scenario last-runs`: one small object per run, newest first as DSS returns them. */
function shapeScenarioLastRuns(result: unknown,): unknown {
	return Array.isArray(result,) ? result.map(shapeScenarioRunSummary,) : result;
}

/** Compact read shapers by `resource.action`. */
const READ_SHAPERS: Record<string, (result: unknown,) => unknown> = {
	"job.get": shapeJobGet,
	"dataset.info": shapeDatasetInfo,
	"dataset.get": shapeDatasetGet,
	"project.get": shapeProjectGet,
	"scenario.get-run": shapeScenarioGetRun,
	"scenario.last-runs": shapeScenarioLastRuns,
	"scenario.runs-by-date": shapeScenarioLastRuns,
};

/**
 * Compact a command result for agents: `*.list` items (filter, cap, compact
 * fields) and the large read commands. `--full` and `--fields` bypass
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
