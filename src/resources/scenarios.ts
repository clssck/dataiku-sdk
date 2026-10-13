import { ClientValidationError, DataikuError, } from "../errors.js";
import type {
	ScenarioDetails,
	ScenarioStatus,
	ScenarioStepRun,
	ScenarioSummary,
	ScenarioWaitResult,
} from "../schemas.js";
import {
	ScenarioDetailsSchema,
	ScenarioStatusSchema,
	ScenarioSummaryArraySchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { computeNextPollDelayMs, isRequestDeadlineError, } from "../utils/polling.js";
import { isRecord, } from "../utils/records.js";
import { BaseResource, requireArrayResponse, requireNonEmpty, } from "./base.js";
import { type ScenarioListSelector, selectScenarioListIndex, } from "./scenario-authoring.js";

/** DSS keeps triggers and reporters at the top level of the scenario (as dataikuapi does), not under params. */
export const SCENARIO_CANONICAL_EDITABLE_FIELDS = [
	"params.steps",
	"triggers",
	"reporters",
	"params.customScript",
	"active",
	"name",
] as const;

/** Default byte cap applied when fetching a scenario run log. */
export const DEFAULT_SCENARIO_MAX_LOG_BYTES = 1_048_576;

/** One entry of GET /scenarios/{id}/get-last-runs — a finished (or running) scenario run summary. */
export interface ScenarioRunSummary {
	runId: string;
	start?: number;
	end?: number;
	scenario?: Record<string, unknown>;
	variables?: Record<string, unknown>;
	result?: Record<string, unknown>;
}

/** Status payload returned by the scenario light endpoints: identity plus running/active flags. */
export interface ScenarioLightStatus {
	id?: string;
	running?: boolean;
	active?: boolean;
}

/** Result of a light active toggle: the light status observed after the PUT. */
export interface ScenarioActiveUpdateResult {
	scenarioId: string;
	active: boolean;
	before?: boolean;
	status: ScenarioLightStatus;
}

/** Step-level entry of a scenario run report, sanitized to documented fields. */
export interface ScenarioRunStepReport {
	id?: string;
	name?: string;
	type?: string;
	runId?: string;
	start?: number;
	end?: number;
	outcome?: string;
	warningCount?: number;
}

/** Response of GET /scenarios/{id}/get-run-for-trigger and GET /scenarios/{id}/{runId}. */
export interface ScenarioRunDetails {
	scenarioRun?: {
		runId?: string;
		result?: Record<string, unknown>;
	};
	stepRuns?: ScenarioRunStepReport[];
}

/** One firing of a scenario trigger: what `POST /scenarios/{id}/run` returns (not a scenario run). */
export interface ScenarioTriggerFire {
	triggerId: string;
	triggerRunId: string;
	cancelled: boolean;
}

/** A trigger fire resolved against the scenario run it started (if any). */
export interface ScenarioTriggerRunState extends ScenarioTriggerFire {
	state: "PENDING" | "RUNNING" | "FINISHED" | "CANCELLED";
	/** The scenario run id (what get-run and log take), once DSS started the run. */
	runId?: string;
	outcome?: string;
	start?: number;
}

/** Test-scenario report text (JUnit XML), byte-bounded. */
export interface ScenarioRunReport {
	text: string;
	truncated: boolean;
	maxBytes: number;
}

export type ScenarioListKind = "trigger" | "reporter";

/** Where each list lives on the scenario object (top level, as dataikuapi). */
const SCENARIO_LIST_FIELDS = { trigger: "triggers", reporter: "reporters", } as const;

/** Outcome of adding or removing one trigger/reporter. */
export interface ScenarioListEdit {
	kind: ScenarioListKind;
	/** Position of the added element (as stored) or of the removed one. */
	index: number;
	/** The element as DSS stored it (added) or as it was (removed). */
	item: unknown;
	/** Elements left in the list. */
	count: number;
}

export interface ScenarioRunAsResult {
	scenarioId: string;
	/** The explicit run-as login, or null when the scenario runs as its last editor. */
	runAsUser: string | null;
	/** Who the scenario actually runs as (dataikuapi `effective_run_as`). */
	effectiveRunAs?: string;
	previousRunAsUser?: string;
}

/** Bounded log retrieval result: text plus whether the byte cap cut it. */
export interface ScenarioRunLog {
	text: string;
	truncated: boolean;
	maxLogBytes: number;
}

/** Internal: run state shared by runAndWait/runScript while polling get-run-for-trigger. */
interface TrackedRun {
	runId: string;
	outcome: string;
	timedOut: boolean;
	/** DSS cancelled the trigger fire: no scenario run exists. */
	cancelled: boolean;
	pollCount: number;
}

export interface ScenarioUpdateNormalization {
	from: string;
	to: string;
	action: "promoted" | "ignored";
	message: string;
}

export interface ScenarioFieldChange {
	path: string;
	before: unknown;
	after: unknown;
}

export interface ScenarioFieldMismatch {
	path: string;
	expected: unknown;
	actual: unknown;
}

export interface ScenarioUpdatePreview {
	canonicalEditableFields: typeof SCENARIO_CANONICAL_EDITABLE_FIELDS;
	normalization: ScenarioUpdateNormalization[];
	normalizedData: Record<string, unknown>;
	current: Record<string, unknown>;
	next: Record<string, unknown>;
	changes: ScenarioFieldChange[];
	unchangedPaths: string[];
}

export interface ScenarioUpdateResult extends ScenarioUpdatePreview {
	after: Record<string, unknown>;
	verified: true;
	mismatches: [];
}

export interface ScenarioScriptRunCleanup {
	status: "deleted" | "kept" | "failed";
	error?: string;
}

/** Cleanup failure carried on errors thrown from {@link ScenariosResource.runScript}. */
export interface ScenarioScriptRunCleanupFailure {
	scenarioId: string;
	error: string;
}

/**
 * Wraps a primary code-run failure so the original error (and its taxonomy:
 * category, exit-code semantics) stays intact while the concurrent throwaway
 * cleanup failure remains observable via {@link cleanupFailure}.
 */
export class ScenarioScriptRunWithCleanupFailureError extends Error {
	constructor(
		public readonly cause: unknown,
		public readonly cleanupFailure: ScenarioScriptRunCleanupFailure,
	) {
		super("Code run failed and its throwaway scenario cleanup also failed.",);
		this.name = "ScenarioScriptRunWithCleanupFailureError";
	}
}

export interface ScenarioScriptRunResult {
	scenarioId: string;
	runId: string;
	outcome: string;
	success: boolean;
	elapsedMs: number;
	pollCount: number;
	output?: string;
	/** True when the captured script output was cut (wrapper byte budget or log read cap); `output` then holds the last part only. */
	outputTruncated?: boolean;
	/** Bytes of script output the wrapper dropped from the front; absent when the cut happened in the log read instead. */
	outputOmittedBytes?: number;
	log: string;
	logTruncated: boolean;
	maxLogBytes: number;
	envName?: string;
	timedOut?: boolean;
	timeoutMs?: number;
	/** Observable throwaway-scenario cleanup outcome; never silently swallowed here. */
	cleanup: ScenarioScriptRunCleanup;
}

const DEFAULT_CODE_RUN_MAX_LOG_BYTES = 1_048_576;
const CODE_RUN_OUTPUT_START = "<<<DSS_CODE_RUN_OUTPUT_b7e3a1>>>";
const CODE_RUN_OUTPUT_END = "<<<DSS_CODE_RUN_OUTPUT_END_b7e3a1>>>";
/** Written right after the END marker when the wrapper cut the captured output; carries the omitted byte count. */
const CODE_RUN_OUTPUT_TRUNCATED_RE = /^<<<DSS_CODE_RUN_OUTPUT_TRUNCATED_b7e3a1:(\d+)>>>$/;
/**
 * The wrapper keeps at most `maxLogBytes / CODE_RUN_OUTPUT_BUDGET_DIVISOR` bytes
 * of script output. The remainder of the read cap is headroom for the per-line
 * DSS log prefix, so the markers still fit in what `runScript` reads back.
 */
const CODE_RUN_OUTPUT_BUDGET_DIVISOR = 4;

/** Prefer the current runId field, with the legacy id field as fallback. */
function triggerRunIdOf(trigger: Record<string, unknown>, scenarioId?: string,): string {
	const runId = trigger?.runId ?? trigger?.id;
	if (typeof runId !== "string" || runId.trim().length === 0) {
		throw new DataikuError(
			200,
			"Unexpected Response",
			`Scenario${
				scenarioId ? ` "${scenarioId}"` : ""
			} run response did not contain a usable run identifier.`,
		);
	}
	return runId;
}

/** Trigger id (`manual` for API runs), trigger run id and cancellation flag of a POST /run response. */
function triggerFireOf(
	trigger: Record<string, unknown>,
	scenarioId?: string,
): ScenarioTriggerFire {
	const triggerId = isRecord(trigger.trigger,) && typeof trigger.trigger.id === "string"
		? trigger.trigger.id
		: "manual";
	return {
		triggerId,
		triggerRunId: triggerRunIdOf(trigger, scenarioId,),
		cancelled: trigger.cancelled === true,
	};
}

/** The `triggers`/`reporters` array of a scenario object; absent means empty, any other shape is an error. */
function scenarioListField(scenario: Record<string, unknown>, kind: ScenarioListKind,): unknown[] {
	const items = scenario[SCENARIO_LIST_FIELDS[kind]];
	return items === undefined ? [] : requireArrayResponse<unknown>(items, `scenario ${kind}s`,);
}

/**
 * DSS stamps `params.lastUpdate` on every temporal trigger each time the scenario
 * is saved, so echoing the value it last stored can never be read back (the
 * verification after a trigger edit would report every trigger as unpersisted).
 * Omitting it lets DSS stamp afresh, as it does for the other saved triggers.
 */
function withoutServerStamp(item: unknown,): unknown {
	if (!isRecord(item,) || !isRecord(item.params,) || !("lastUpdate" in item.params)) return item;
	const { lastUpdate: _stamp, ...params } = item.params;
	return { ...item, params, };
}

/** `YYYY-MM-DD` day of a date or ISO date-time (the day DSS filters runs by); anything else is a usage error. */
function scenarioDay(value: string, name: string,): string {
	const match = /^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$/.exec(value,);
	if (match) {
		const day = new Date(Date.UTC(Number(match[1],), Number(match[2],) - 1, Number(match[3],),),);
		if (day.getUTCMonth() === Number(match[2],) - 1 && day.getUTCDate() === Number(match[3],)) {
			return `${match[1]}-${match[2]}-${match[3]}`;
		}
	}
	throw new ClientValidationError(
		`${name} must be a YYYY-MM-DD date or ISO date-time, got ${JSON.stringify(value,)}.`,
		"invalid_flag_value",
	);
}

/** Who a scenario really runs as: its `runAsUser`, else the last editor (dataikuapi `effective_run_as`). */
export function effectiveRunAs(scenario: Record<string, unknown>,): string | undefined {
	if (typeof scenario.runAsUser === "string") return scenario.runAsUser;
	for (const tag of [scenario.versionTag, scenario.creationTag,]) {
		if (
			isRecord(tag,) && isRecord(tag.lastModifiedBy,) && typeof tag.lastModifiedBy.login === "string"
		) {
			return tag.lastModifiedBy.login;
		}
	}
	return undefined;
}

/** A finite number, or undefined for anything else (absent/malformed JSON fields). */
function optionalNumber(value: unknown,): number | undefined {
	return typeof value === "number" && Number.isFinite(value,) ? value : undefined;
}

/** Map one get-last-runs entry to a ScenarioRunSummary, tolerating missing fields. */
function scenarioRunSummaryFromRaw(entry: unknown,): ScenarioRunSummary {
	if (!isRecord(entry,)) return { runId: "unknown", };
	const runId = typeof entry.runId === "string" ? entry.runId : "unknown";
	const start = optionalNumber(entry.start,);
	const end = optionalNumber(entry.end,);
	return {
		runId,
		...(start !== undefined ? { start, } : {}),
		...(end !== undefined ? { end, } : {}),
		...(isRecord(entry.scenario,) ? { scenario: entry.scenario, } : {}),
		...(isRecord(entry.variables,) ? { variables: entry.variables, } : {}),
		...(isRecord(entry.result,) ? { result: entry.result, } : {}),
	};
}

/** Map a light-endpoint response to the documented ScenarioWithStatus shape. */
function scenarioLightStatusFromRaw(raw: Record<string, unknown>,): ScenarioLightStatus {
	return {
		...(typeof raw.id === "string" ? { id: raw.id, } : {}),
		...(typeof raw.running === "boolean" ? { running: raw.running, } : {}),
		...(typeof raw.active === "boolean" ? { active: raw.active, } : {}),
	};
}

/**
 * Wrap a user Python script so its stdout/stderr (and any traceback) are captured
 * into a buffer and re-emitted between unique markers, isolated from DSS scenario
 * wrapper noise. The script is base64-encoded to avoid quoting/escaping issues and
 * exec'd as `__main__`. A failing script re-raises SystemExit(1) so the scenario
 * outcome is FAILED while the captured traceback still lands between the markers.
 * Only the last `maxOutputBytes` UTF-8 bytes of output are re-emitted; when output
 * was cut, a truncation marker with the omitted byte count follows the END marker.
 */
function buildCodeRunScript(script: string, maxOutputBytes: number,): string {
	const encoded = Buffer.from(script, "utf-8",).toString("base64",);
	return [
		"import base64 as _dku_b64, sys as _dku_sys, io as _dku_io, traceback as _dku_tb",
		`_dku_src = _dku_b64.b64decode("${encoded}").decode("utf-8")`,
		"_dku_buf = _dku_io.StringIO()",
		"_dku_out, _dku_err = _dku_sys.stdout, _dku_sys.stderr",
		"_dku_sys.stdout = _dku_sys.stderr = _dku_buf",
		"_dku_code = 0",
		"try:",
		'\texec(compile(_dku_src, "<dss_code_run>", "exec"), {"__name__": "__main__"})',
		"except SystemExit as _dku_e:",
		"\t_dku_code = _dku_e.code if isinstance(_dku_e.code, int) else (0 if _dku_e.code is None else 1)",
		"except BaseException:",
		"\t_dku_code = 1",
		"\t_dku_tb.print_exc()",
		"finally:",
		"\t_dku_sys.stdout, _dku_sys.stderr = _dku_out, _dku_err",
		'\t_dku_bytes = _dku_buf.getvalue().encode("utf-8", "replace")',
		`\t_dku_omitted = max(0, len(_dku_bytes) - ${maxOutputBytes})`,
		'\t_dku_text = _dku_bytes[_dku_omitted:].decode("utf-8", "ignore")',
		`\t_dku_out.write("${CODE_RUN_OUTPUT_START}\\n")`,
		"\t_dku_out.write(_dku_text)",
		`\t_dku_out.write("\\n${CODE_RUN_OUTPUT_END}\\n")`,
		"\tif _dku_omitted:",
		'\t\t_dku_out.write("<<<DSS_CODE_RUN_OUTPUT_TRUNCATED_b7e3a1:%d>>>\\n" % _dku_omitted)',
		"\t_dku_out.flush()",
		"if _dku_code:",
		"\traise SystemExit(_dku_code)",
		"",
	].join("\n",);
}

/**
 * Pull the script's own stdout/stderr back out of the full DSS run log by slicing
 * the `[process]` lines between the markers emitted by {@link buildCodeRunScript}.
 * Returns undefined if the markers are absent (e.g. the harness never ran), in which
 * case callers should fall back to the full log. When the log read itself was cut
 * (`logTruncated`) the START marker may be gone while END survives; the output is
 * then whatever precedes END, reported as truncated.
 */
function extractCodeRunOutput(
	log: string,
	logTruncated: boolean,
): { output: string; truncated: boolean; omittedBytes?: number; } | undefined {
	// `[\s\S]` (not `.`): CR, U+2028 and U+2029 inside a printed line must not drop it.
	const messageRe = /^\[[^\]]*\] \[[^\]]*\] \[[^\]]*\] \[process\]  - ([\s\S]*)$/;
	const contents: string[] = [];
	for (const rawLine of log.split("\n",)) {
		const line = rawLine.endsWith("\r",) ? rawLine.slice(0, -1,) : rawLine;
		const match = messageRe.exec(line,);
		if (match) contents.push(match[1] ?? "",);
	}
	// First start + last end, so a script that prints a marker string stays body content.
	const start = contents.indexOf(CODE_RUN_OUTPUT_START,);
	if (start < 0 && !logTruncated) return undefined;
	let end = -1;
	for (let i = contents.length - 1; i > start; i--) {
		if (contents[i] === CODE_RUN_OUTPUT_END) {
			end = i;
			break;
		}
	}
	if (end < 0) return undefined;
	const body = contents.slice(start + 1, end,);
	// Drop only the single trailing separator the harness writes before the end marker.
	if (body.length > 0 && body[body.length - 1] === "") body.pop();
	const omitted = CODE_RUN_OUTPUT_TRUNCATED_RE.exec(contents[end + 1] ?? "",);
	const omittedBytes = omitted ? Number(omitted[1],) : undefined;
	return {
		output: body.join("\n",),
		truncated: omitted !== null || start < 0,
		...(omittedBytes !== undefined ? { omittedBytes, } : {}),
	};
}

function scenarioStepWarningSummary(stepResult: Record<string, unknown> | undefined,): {
	warningCount?: number;
	warnings?: Array<{ type: string; count?: number; }>;
} {
	const container = stepResult?.warnings;
	if (!isRecord(container,)) return {};
	const rawWarnings = container.warnings;
	const warnings = isRecord(rawWarnings,)
		? Object.entries(rawWarnings,).flatMap(([key, value,],) => {
			if (!isRecord(value,)) return [];
			const rawType = value.type;
			const type = typeof rawType === "string" && rawType.length > 0 ? rawType : key;
			const rawCount = value.count;
			return [{
				type,
				...(typeof rawCount === "number" && Number.isFinite(rawCount,)
					? { count: rawCount, }
					: {}),
			},];
		},)
		: [];
	const rawTotal = container.totalCount;
	return {
		...(typeof rawTotal === "number" && Number.isFinite(rawTotal,)
			? { warningCount: rawTotal, }
			: {}),
		...(warnings.length > 0 ? { warnings, } : {}),
	};
}

function jsonValueEqual(left: unknown, right: unknown,): boolean {
	if (Object.is(left, right,)) return true;
	if (Array.isArray(left,) || Array.isArray(right,)) {
		if (!Array.isArray(left,) || !Array.isArray(right,) || left.length !== right.length) return false;
		return left.every((value, index,) => jsonValueEqual(value, right[index],));
	}
	if (!isRecord(left,) || !isRecord(right,)) return false;
	const leftKeys = Object.keys(left,);
	const rightKeys = Object.keys(right,);
	if (leftKeys.length !== rightKeys.length) return false;
	return leftKeys.every((key,) =>
		Object.hasOwn(right, key,) && jsonValueEqual(left[key], right[key],)
	);
}

function jsonValueContains(actual: unknown, expected: unknown,): boolean {
	if (jsonValueEqual(actual, expected,)) return true;
	if (Array.isArray(actual,) || Array.isArray(expected,)) {
		if (!Array.isArray(actual,) || !Array.isArray(expected,) || actual.length !== expected.length) {
			return false;
		}
		return expected.every((value, index,) => jsonValueContains(actual[index], value,));
	}
	if (!isRecord(actual,) || !isRecord(expected,)) return false;
	return Object.entries(expected,).every(([key, value,],) =>
		Object.hasOwn(actual, key,) && jsonValueContains(actual[key], value,)
	);
}

function collectPatchPaths(value: unknown, prefix = "", paths: string[] = [],): string[] {
	if (!isRecord(value,)) {
		if (prefix) paths.push(prefix,);
		return paths;
	}

	const entries = Object.entries(value,);
	if (entries.length === 0) {
		if (prefix) paths.push(prefix,);
		return paths;
	}

	for (const [key, child,] of entries) {
		collectPatchPaths(child, prefix ? `${prefix}.${key}` : key, paths,);
	}
	return paths;
}

function valueAtPath(value: unknown, path: string,): unknown {
	let current = value;
	for (const part of path.split(".",)) {
		if (!isRecord(current,)) return undefined;
		current = current[part];
	}
	return current;
}

function scenarioFieldChanges(
	before: Record<string, unknown>,
	after: Record<string, unknown>,
	patch: Record<string, unknown>,
): { changes: ScenarioFieldChange[]; unchangedPaths: string[]; } {
	const changes: ScenarioFieldChange[] = [];
	const unchangedPaths: string[] = [];
	for (const path of collectPatchPaths(patch,)) {
		const beforeValue = valueAtPath(before, path,);
		const afterValue = valueAtPath(after, path,);
		if (jsonValueEqual(beforeValue, afterValue,)) {
			unchangedPaths.push(path,);
			continue;
		}
		changes.push({ path, before: beforeValue, after: afterValue, },);
	}
	return { changes, unchangedPaths, };
}

function scenarioFieldMismatches(
	actual: Record<string, unknown>,
	expected: Record<string, unknown>,
	patch: Record<string, unknown>,
): ScenarioFieldMismatch[] {
	const mismatches: ScenarioFieldMismatch[] = [];
	for (const path of collectPatchPaths(patch,)) {
		const actualValue = valueAtPath(actual, path,);
		const expectedValue = valueAtPath(expected, path,);
		if (!jsonValueContains(actualValue, expectedValue,)) {
			mismatches.push({ path, expected: expectedValue, actual: actualValue, },);
		}
	}
	return mismatches;
}

export function normalizeScenarioUpdateData(
	data: Record<string, unknown>,
): { normalizedData: Record<string, unknown>; normalization: ScenarioUpdateNormalization[]; } {
	const normalizedData: Record<string, unknown> = { ...data, };
	const normalization: ScenarioUpdateNormalization[] = [];
	const rawParams = data.rawParams;
	if (!isRecord(rawParams,) || !isRecord(rawParams.params,)) {
		return { normalizedData, normalization, };
	}

	const canonicalParams = isRecord(data.params,) ? data.params : undefined;
	const mergedParams = canonicalParams === undefined
		? rawParams.params
		: deepMerge(rawParams.params, canonicalParams,);
	const promoted = canonicalParams === undefined || !jsonValueEqual(mergedParams, canonicalParams,);
	if (promoted) normalizedData.params = mergedParams;

	const rawParamsWithoutParams = { ...rawParams, };
	delete rawParamsWithoutParams.params;
	if (Object.keys(rawParamsWithoutParams,).length === 0) delete normalizedData.rawParams;
	else normalizedData.rawParams = rawParamsWithoutParams;

	normalization.push({
		from: "rawParams.params",
		to: "params",
		action: promoted ? "promoted" : "ignored",
		message: promoted
			? "rawParams.params is a DSS echo; the editable scenario definition uses params."
			: "rawParams.params was ignored because canonical params already supplied the same editable fields.",
	},);
	return { normalizedData, normalization, };
}

export function scenarioUpdatePreview(
	current: Record<string, unknown>,
	data: Record<string, unknown>,
): ScenarioUpdatePreview {
	const { normalizedData, normalization, } = normalizeScenarioUpdateData(data,);
	const next = deepMerge(current, normalizedData,);
	const { changes, unchangedPaths, } = scenarioFieldChanges(current, next, normalizedData,);
	return {
		canonicalEditableFields: SCENARIO_CANONICAL_EDITABLE_FIELDS,
		normalization,
		normalizedData,
		current,
		next,
		changes,
		unchangedPaths,
	};
}

function scenarioUpdateVerificationError(
	mismatches: ScenarioFieldMismatch[],
	normalization: ScenarioUpdateNormalization[],
): DataikuError {
	const mismatchPaths = mismatches.map((mismatch,) => mismatch.path).join(", ",);
	const body = {
		message: `Scenario update did not persist requested fields after refetch: ${mismatchPaths}`,
		mismatches,
		canonicalEditableFields: SCENARIO_CANONICAL_EDITABLE_FIELDS,
		...(normalization.length > 0 ? { normalization, } : {}),
	};
	const error = new DataikuError(400, "Scenario Update Verification Failed", JSON.stringify(body,),);
	// CLI envelopes show `details`, never the body. Only paths go there: reporter and step
	// values can hold webhook URLs, tokens, or credentials.
	error.details = {
		mismatches: mismatches.map((mismatch,) => ({
			path: mismatch.path,
			differingPaths: differingLeafPaths(mismatch.expected, mismatch.actual, mismatch.path,),
		})),
		canonicalEditableFields: SCENARIO_CANONICAL_EDITABLE_FIELDS,
	};
	return error;
}

/** Paths under `path` where the sent value is missing or different in what DSS stored (e.g. `reporters.0.messaging.channelId`). */
function differingLeafPaths(expected: unknown, actual: unknown, path: string,): string[] {
	if (Array.isArray(expected,)) {
		if (!Array.isArray(actual,)) return [path,];
		return expected.flatMap((item, index,) =>
			differingLeafPaths(item, actual[index], `${path}.${index}`,)
		);
	}
	if (isRecord(expected,)) {
		if (!isRecord(actual,)) return [path,];
		return Object.entries(expected,).flatMap(([key, value,],) =>
			differingLeafPaths(value, actual[key], `${path}.${key}`,)
		);
	}
	return jsonValueEqual(expected, actual,) ? [] : [path,];
}

export class ScenariosResource extends BaseResource {
	/** List all scenarios in a project. */
	async list(projectKey?: string,): Promise<ScenarioSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/`,
		);
		return this.client.safeParse(ScenarioSummaryArraySchema, raw, "scenarios.list",);
	}

	/** Get full scenario details. */
	async get(scenarioId: string, opts?: { projectKey?: string; },): Promise<ScenarioDetails> {
		const scEnc = encodeURIComponent(scenarioId,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/`,
		);
		return this.client.safeParse(ScenarioDetailsSchema, raw, "scenarios.get",);
	}

	/** Create a new scenario. */
	async create(
		scenarioId: string,
		name: string,
		opts?: {
			scenarioType?: "step_based" | "custom_python";
			data?: Record<string, unknown>;
			projectKey?: string;
		},
	): Promise<void> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const scenarioType = opts?.scenarioType ?? "step_based";
		const body: Record<string, unknown> = {
			id: scenarioId,
			name,
			projectKey: pk,
			type: scenarioType,
			...(scenarioType === "step_based"
				? { params: { steps: [], }, triggers: [], reporters: [], }
				: { params: {}, }),
			...opts?.data,
		};
		await this.client.post<void>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/`,
			body,
		);
	}

	/**
	 * Trigger a scenario run. `params` is sent as the POST body (dataikuapi
	 * `run(params)`) and reaches the run as its trigger parameters. The result
	 * identifies the TRIGGER fire, not a scenario run: resolve it with
	 * {@link getRunForTrigger}. `cancelled` is already true when DSS refused the
	 * fire because the scenario is running.
	 */
	async run(
		scenarioId: string,
		opts?: { params?: Record<string, unknown>; projectKey?: string; },
	): Promise<ScenarioTriggerFire> {
		const scEnc = encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),);
		const result = await this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/run/`,
			opts?.params ?? {},
		);
		return triggerFireOf(result, scenarioId,);
	}

	/** Get the light/status view of a scenario. */
	async status(scenarioId: string, projectKey?: string,): Promise<ScenarioStatus> {
		const scEnc = encodeURIComponent(scenarioId,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${scEnc}/light/`,
		);
		return this.client.safeParse(ScenarioStatusSchema, raw, "scenarios.status",);
	}

	/** Merge-update a scenario's definition, then refetch and verify requested fields persisted. */
	async update(
		scenarioId: string,
		data: Record<string, unknown>,
		projectKey?: string,
	): Promise<ScenarioUpdateResult> {
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${encodeURIComponent(scenarioId,)}/`,
		);
		return this.applyUpdate(scenarioId, current, data, projectKey,);
	}

	/** PUT the merge of `data` into an already-read scenario, refetch, and verify it persisted. */
	private async applyUpdate(
		scenarioId: string,
		current: Record<string, unknown>,
		data: Record<string, unknown>,
		projectKey: string | undefined,
	): Promise<ScenarioUpdateResult> {
		const path = `/public/api/projects/${this.enc(projectKey,)}/scenarios/${
			encodeURIComponent(scenarioId,)
		}/`;
		const preview = scenarioUpdatePreview(current, data,);
		await this.client.put<Record<string, unknown>>(path, preview.next,);
		const after = await this.client.get<Record<string, unknown>>(path,);
		const mismatches = scenarioFieldMismatches(after, preview.next, preview.normalizedData,);
		if (mismatches.length > 0) {
			throw scenarioUpdateVerificationError(mismatches, preview.normalization,);
		}

		const verified = scenarioFieldChanges(current, after, preview.normalizedData,);
		return {
			...preview,
			after,
			changes: verified.changes,
			unchangedPaths: verified.unchangedPaths,
			verified: true,
			mismatches: [],
		};
	}

	/** Delete a scenario. */
	async delete(scenarioId: string, projectKey?: string,): Promise<void> {
		const scEnc = encodeURIComponent(scenarioId,);
		await this.client.del(`/public/api/projects/${this.enc(projectKey,)}/scenarios/${scEnc}/`,);
	}

	/**
	 * Run a scenario and poll until it finishes or times out.
	 * Returns `{ success: false, timedOut: true }` on timeout rather than throwing.
	 */
	async runAndWait(
		scenarioId: string,
		opts?: {
			params?: Record<string, unknown>;
			pollIntervalMs?: number;
			timeoutMs?: number;
			projectKey?: string;
		},
	): Promise<ScenarioWaitResult> {
		const pkEnc = this.enc(opts?.projectKey,);
		const base = `/public/api/projects/${pkEnc}/scenarios/${encodeURIComponent(scenarioId,)}`;
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? 2_000,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? 120_000,);
		const startedAt = Date.now();
		// POST /run/ returns a TRIGGER run id, which differs from the actual scenario
		// run id; resolve the real run via get-run-for-trigger so a completed scenario
		// is not misreported as a timeout (the trigger id never matches lastRun).
		const trigger = await this.client.post<Record<string, unknown>>(
			`${base}/run/`,
			opts?.params ?? {},
		);
		const fire = triggerFireOf(trigger, scenarioId,);
		const tracked = await this.trackTriggerRun(scenarioId, opts?.projectKey, trigger, {
			startedAt,
			baseIntervalMs,
			adaptivePolling,
			timeout,
		},);
		const resolvedRunId = tracked.runId || fire.triggerRunId;
		const steps = tracked.timedOut || !tracked.runId
			? undefined
			: await this.runStepSummaries(base, tracked.runId,);
		return {
			scenarioId,
			runId: resolvedRunId,
			outcome: tracked.outcome,
			success: tracked.outcome === "SUCCESS",
			elapsedMs: Date.now() - startedAt,
			pollCount: tracked.pollCount,
			...(fire.triggerRunId !== resolvedRunId || tracked.cancelled
				? { triggerRunId: fire.triggerRunId, }
				: {}),
			...(tracked.cancelled ? { cancelled: true, } : {}),
			...(tracked.timedOut ? { timedOut: true, } : {}),
			...(steps ? { steps, } : {}),
		};
	}

	/**
	 * Run a one-off Python script in a throwaway custom-python scenario and return
	 * its outcome plus the captured run log. The scenario is deleted afterward
	 * unless `keepScenario` is set. This is the only DSS public-API path to execute
	 * ad-hoc code in a code env without a persisted recipe or notebook.
	 */
	async runScript(
		script: string,
		opts?: {
			envName?: string;
			projectKey?: string;
			timeoutMs?: number;
			pollIntervalMs?: number;
			keepScenario?: boolean;
			maxLogBytes?: number;
		},
	): Promise<ScenarioScriptRunResult> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const pkEnc = this.enc(opts?.projectKey,);
		const scenarioId = `dss_cli_code_run_${Date.now()}_${crypto.randomUUID().replace(/-/g, "",)}`;
		const base = `/public/api/projects/${pkEnc}/scenarios/${encodeURIComponent(scenarioId,)}`;
		const envSelection = opts?.envName
			? { envMode: "EXPLICIT_ENV", envName: opts.envName, }
			: { envMode: "INHERIT", };
		const startedAt = Date.now();
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? 2_000,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? 120_000,);
		const maxLogBytes = Math.max(
			0,
			Math.floor(opts?.maxLogBytes ?? DEFAULT_CODE_RUN_MAX_LOG_BYTES,),
		);
		let outcome = "UNKNOWN";
		let runId = "";
		let pollCount = 0;
		let timedOut = false;
		let log = "";
		let logTruncated = false;
		let primaryError: unknown;
		try {
			await this.client.post(`/public/api/projects/${pkEnc}/scenarios/`, {
				id: scenarioId,
				name: `dss code run (${scenarioId})`,
				projectKey: pk,
				type: "custom_python",
				params: { envSelection, },
			},);
			await this.client.putVoid(
				`${base}/payload`,
				{
					script: buildCodeRunScript(
						script,
						Math.floor(maxLogBytes / CODE_RUN_OUTPUT_BUDGET_DIVISOR,),
					),
					extension: "py",
				},
			);

			const trigger = await this.client.post<Record<string, unknown>>(`${base}/run/`, {},);
			const tracked = await this.trackTriggerRun(scenarioId, opts?.projectKey, trigger, {
				startedAt,
				baseIntervalMs,
				adaptivePolling,
				timeout,
			},);
			outcome = tracked.outcome;
			runId = tracked.runId;
			pollCount = tracked.pollCount;
			timedOut = tracked.timedOut;

			if (runId && outcome !== "TIMEOUT") {
				// Tail read: the markers are printed last, so a log over the cap loses
				// DSS start-up noise instead of the END marker.
				const limitedLog = await this.getRunLog(scenarioId, runId, {
					projectKey: opts?.projectKey,
					maxLogBytes,
					tail: true,
				},);
				log = limitedLog.text;
				logTruncated = limitedLog.truncated;
			}
		} catch (error) {
			primaryError = error;
		}
		const cleanup = await this.removeThrowawayScenario(base, opts?.keepScenario === true,);
		if (primaryError !== undefined) {
			if (cleanup.status === "failed") {
				throw new ScenarioScriptRunWithCleanupFailureError(primaryError, {
					scenarioId,
					error: cleanup.error ?? "unknown cleanup error",
				},);
			}
			throw primaryError;
		}
		const extracted = extractCodeRunOutput(log, logTruncated,);
		return {
			scenarioId,
			runId,
			outcome,
			success: outcome === "SUCCESS",
			elapsedMs: Date.now() - startedAt,
			pollCount,
			output: extracted?.output,
			...(extracted?.truncated ? { outputTruncated: true, } : {}),
			...(extracted?.omittedBytes !== undefined
				? { outputOmittedBytes: extracted.omittedBytes, }
				: {}),
			log,
			logTruncated,
			maxLogBytes,
			...(opts?.envName ? { envName: opts.envName, } : {}),
			...(timedOut ? { timedOut: true, timeoutMs: timeout, } : {}),
			cleanup,
		};
	}

	/**
	 * Abort a running scenario (POST /scenarios/{id}/abort). The call returns as
	 * soon as DSS accepts the abort request; the scenario may take some time to
	 * actually stop, so this does not wait for termination.
	 */
	async abort(scenarioId: string, projectKey?: string,): Promise<void> {
		const scEnc = encodeURIComponent(scenarioId,);
		await this.client.post(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${scEnc}/abort`,
		);
	}

	/**
	 * Get the raw last-runs list (GET /scenarios/{id}/get-last-runs/?limit=N).
	 * `onlyFinished` drops runs still in progress (DSS `onlyFinishedRuns`).
	 */
	async getLastRuns(
		scenarioId: string,
		opts?: {
			limit?: number;
			onlyFinished?: boolean;
			projectKey?: string;
		},
	): Promise<ScenarioRunSummary[]> {
		if (opts?.limit !== undefined && (!Number.isInteger(opts.limit,) || opts.limit < 1)) {
			throw new ClientValidationError(`limit must be a positive integer, got ${opts.limit}.`,);
		}
		const scEnc = encodeURIComponent(scenarioId,);
		const query = new URLSearchParams();
		if (opts?.limit !== undefined) query.set("limit", String(opts.limit,),);
		if (opts?.onlyFinished === true) query.set("onlyFinishedRuns", "true",);
		const suffix = query.size > 0 ? `?${query}` : "";
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/get-last-runs/${suffix}`,
		);
		return requireArrayResponse<unknown>(raw, "scenarios.getLastRuns",).map(
			scenarioRunSummaryFromRaw,
		);
	}

	/**
	 * Runs that started between two days (GET /scenarios/{id}/get-runs-by-date).
	 * Dates are `YYYY-MM-DD` or ISO date-times (the time part is dropped, as
	 * dataikuapi does). DSS treats `to` as exclusive (verified live: `from=to=today`
	 * is empty and today's runs need `to=tomorrow`), so it defaults to tomorrow
	 * rather than dataikuapi's today, which would leave today's runs out.
	 */
	async getRunsByDate(
		scenarioId: string,
		opts: { from: string; to?: string; projectKey?: string; },
	): Promise<ScenarioRunSummary[]> {
		const query = new URLSearchParams({
			fromDate: scenarioDay(opts.from, "from",),
			toDate: scenarioDay(
				opts.to ?? new Date(Date.now() + 86_400_000,).toLocaleDateString("en-CA",),
				"to",
			),
		},);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/scenarios/${
				encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),)
			}/get-runs-by-date?${query}`,
		);
		return requireArrayResponse<unknown>(raw, "scenarios.getRunsByDate",).map(
			scenarioRunSummaryFromRaw,
		);
	}

	/**
	 * JUnit XML report of a test scenario run (GET /scenarios/{id}/{runId}/scenario-run-report),
	 * or of one step with `stepId` (…/step-run-report?stepId=). DSS only builds
	 * reports for scenarios marked as test; the text is byte-bounded like the log.
	 */
	async getRunReport(
		scenarioId: string,
		runId: string,
		opts?: { stepId?: string; maxBytes?: number; projectKey?: string; },
	): Promise<ScenarioRunReport> {
		if (opts?.stepId !== undefined) requireNonEmpty(opts.stepId, "stepId",);
		const maxBytes = Math.max(0, Math.floor(opts?.maxBytes ?? DEFAULT_SCENARIO_MAX_LOG_BYTES,),);
		const runPath = `/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${
			encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),)
		}/${encodeURIComponent(requireNonEmpty(runId, "runId",),)}`;
		const { text, truncated, } = await this.client.getTextLimited(
			opts?.stepId === undefined
				? `${runPath}/scenario-run-report`
				: `${runPath}/step-run-report?stepId=${encodeURIComponent(opts.stepId,)}`,
			maxBytes,
		);
		return { text, truncated, maxBytes, };
	}

	/**
	 * Resolve a trigger run (what `run` returns) to its scenario run: the scenario
	 * run id once DSS started one, or `CANCELLED` when DSS cancelled the fire (a
	 * run already in progress suppresses it) and no run will ever exist.
	 */
	async getRunForTrigger(
		scenarioId: string,
		triggerRunId: string,
		opts?: { triggerId?: string; projectKey?: string; },
	): Promise<ScenarioTriggerRunState> {
		const pkEnc = this.enc(opts?.projectKey,);
		const triggerId = requireNonEmpty(opts?.triggerId ?? "manual", "triggerId",);
		const fire = { triggerId, triggerRunId: requireNonEmpty(triggerRunId, "triggerRunId",), };
		const run = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${
				encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),)
			}/get-run-for-trigger?triggerId=${encodeURIComponent(triggerId,)}&triggerRunId=${
				encodeURIComponent(fire.triggerRunId,)
			}`,
		);
		if (isRecord(run.scenarioRun,)) {
			const outcome =
				isRecord(run.scenarioRun.result,) && typeof run.scenarioRun.result.outcome === "string"
					? run.scenarioRun.result.outcome
					: undefined;
			const start = optionalNumber(run.scenarioRun.start,);
			return {
				...fire,
				cancelled: false,
				state: outcome === undefined ? "RUNNING" : "FINISHED",
				...(typeof run.scenarioRun.runId === "string" ? { runId: run.scenarioRun.runId, } : {}),
				...(outcome === undefined ? {} : { outcome, }),
				...(start === undefined ? {} : { start, }),
			};
		}
		const cancelled = await this.isTriggerFireCancelled(pkEnc, scenarioId, fire,);
		return { ...fire, cancelled, state: cancelled ? "CANCELLED" : "PENDING", };
	}

	/** Get the details of a specific run (GET /scenarios/{id}/{runId}). */
	async getRunDetails(
		scenarioId: string,
		runId: string,
		opts?: { projectKey?: string; },
	): Promise<ScenarioRunDetails> {
		return this.client.get<ScenarioRunDetails>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${
				encodeURIComponent(scenarioId,)
			}/${encodeURIComponent(runId,)}/`,
		);
	}

	/**
	 * Get the log of a scenario run (GET /scenarios/{id}/{runId}/log), optionally
	 * scoped to a single step via `stepId`. The text is byte-bounded: at most
	 * `maxLogBytes` (default 1 MiB) of the beginning of the log is retained and
	 * `truncated` reports whether the cap cut the body. With `tail: true` the
	 * last `maxLogBytes` are retained instead (used by code runs, whose output
	 * markers sit at the end of the log).
	 */
	async getRunLog(
		scenarioId: string,
		runId: string,
		opts?: {
			stepId?: string;
			maxLogBytes?: number;
			projectKey?: string;
			tail?: boolean;
		},
	): Promise<ScenarioRunLog> {
		if (opts?.stepId !== undefined) requireNonEmpty(opts.stepId, "stepId",);
		const maxLogBytes = Math.max(
			0,
			Math.floor(opts?.maxLogBytes ?? DEFAULT_SCENARIO_MAX_LOG_BYTES,),
		);
		const scEnc = encodeURIComponent(scenarioId,);
		const query = opts?.stepId !== undefined
			? `?stepId=${encodeURIComponent(opts.stepId,)}`
			: "";
		const readText = opts?.tail === true
			? this.client.getTextTailLimited.bind(this.client,)
			: this.client.getTextLimited.bind(this.client,);
		const { text, truncated, } = await readText(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/${
				encodeURIComponent(runId,)
			}/log${query}`,
			maxLogBytes,
		);
		return { text, truncated, maxLogBytes, };
	}

	/** Get the payload of a (custom) scenario (GET /scenarios/{id}/payload). */
	async getPayload(
		scenarioId: string,
		opts?: { projectKey?: string; },
	): Promise<Record<string, unknown>> {
		const scEnc = encodeURIComponent(scenarioId,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/payload`,
		);
	}

	/** Update the payload of a (custom) scenario (PUT /scenarios/{id}/payload). */
	async setPayload(
		scenarioId: string,
		payload: Record<string, unknown>,
		opts?: { projectKey?: string; },
	): Promise<void> {
		const scEnc = encodeURIComponent(scenarioId,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/payload`,
			payload,
		);
	}

	/**
	 * Toggle the active flag through the light endpoints (GET then PUT
	 * /scenarios/{id}/light) without a full-definition round-trip. Returns the
	 * light status observed after the PUT.
	 */
	async setActive(
		scenarioId: string,
		active: boolean,
		opts?: { projectKey?: string; },
	): Promise<ScenarioActiveUpdateResult> {
		const scEnc = encodeURIComponent(scenarioId,);
		const base = `/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/light`;
		const beforeRaw = await this.client.get<Record<string, unknown>>(base,);
		const before = typeof beforeRaw.active === "boolean" ? beforeRaw.active : undefined;
		// DSS parses the light PUT as a full Scenario object: a minimal {id, active} body 400s with
		// "Could not parse a Scenario from request body" (missing name). Echo the complete GET body
		// with active overridden — the same contract as the python client's set_definition(with_status).
		const nextLight = { ...beforeRaw, active, };
		await this.client.putVoid(base, nextLight,);
		const after = await this.client.get<Record<string, unknown>>(base,);
		return {
			scenarioId,
			active: typeof after.active === "boolean" ? after.active : active,
			...(before !== undefined ? { before, } : {}),
			status: scenarioLightStatusFromRaw(after,),
		};
	}

	/** The scenario's triggers or reporters, in DSS order (GET /scenarios/{id}/). */
	async listItems(
		kind: ScenarioListKind,
		scenarioId: string,
		opts?: { projectKey?: string; },
	): Promise<unknown[]> {
		return scenarioListField(await this.getSettings(scenarioId, opts?.projectKey,), kind,);
	}

	private async getSettings(
		scenarioId: string,
		projectKey: string | undefined,
	): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${
				encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),)
			}/`,
		);
	}

	/**
	 * Append a trigger or reporter. `triggers`/`reporters` are arrays, which a merge
	 * update replaces wholesale, so this reads the current list and writes it back
	 * with the new element last (then verifies the stored result).
	 */
	async addItem(
		kind: ScenarioListKind,
		scenarioId: string,
		item: Record<string, unknown>,
		opts?: { projectKey?: string; },
	): Promise<ScenarioListEdit> {
		const current = await this.getSettings(scenarioId, opts?.projectKey,);
		const items = scenarioListField(current, kind,);
		const result = await this.applyUpdate(
			scenarioId,
			current,
			{
				[SCENARIO_LIST_FIELDS[kind]]: [...items.map(withoutServerStamp,), item,],
			},
			opts?.projectKey,
		);
		const stored = scenarioListField(result.after, kind,);
		return { kind, index: items.length, item: stored[items.length], count: stored.length, };
	}

	/** Remove the trigger or reporter selected by `--index`/`--name`, keeping every other element. */
	async removeItem(
		kind: ScenarioListKind,
		scenarioId: string,
		selector: ScenarioListSelector,
		opts?: { projectKey?: string; },
	): Promise<ScenarioListEdit> {
		const current = await this.getSettings(scenarioId, opts?.projectKey,);
		const items = scenarioListField(current, kind,);
		const index = selectScenarioListIndex(items, selector, kind,);
		const result = await this.applyUpdate(
			scenarioId,
			current,
			{
				[SCENARIO_LIST_FIELDS[kind]]: items.flatMap((existing, position,) =>
					position === index ? [] : [withoutServerStamp(existing,),]
				),
			},
			opts?.projectKey,
		);
		return {
			kind,
			index,
			item: items[index],
			count: scenarioListField(result.after, kind,).length,
		};
	}

	/**
	 * Set (`user`) or clear (`null`) the scenario's `runAsUser` (admin only for a
	 * non-null value). Clearing deletes the key, which a merge update cannot do.
	 */
	async setRunAs(
		scenarioId: string,
		user: string | null,
		opts?: { projectKey?: string; },
	): Promise<ScenarioRunAsResult> {
		const path = `/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${
			encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),)
		}/`;
		const current = await this.client.get<Record<string, unknown>>(path,);
		const { runAsUser: previous, ...withoutRunAs } = current;
		await this.client.put<unknown>(
			path,
			user === null ? withoutRunAs : { ...withoutRunAs, runAsUser: user, },
		);
		const after = await this.client.get<Record<string, unknown>>(path,);
		const stored = typeof after.runAsUser === "string" ? after.runAsUser : null;
		if (stored !== user) {
			const error = new DataikuError(
				400,
				"Scenario Update Verification Failed",
				`Scenario run-as did not persist after refetch: expected ${
					user === null ? "no runAsUser" : "the requested user"
				}.`,
			);
			error.details = { mismatches: [{ path: "runAsUser", },], };
			throw error;
		}
		return {
			scenarioId,
			runAsUser: stored,
			effectiveRunAs: effectiveRunAs(after,),
			...(typeof previous === "string" ? { previousRunAsUser: previous, } : {}),
		};
	}

	/**
	 * Poll get-run-for-trigger until the triggered run reports an outcome, DSS
	 * cancels the trigger fire, or the deadline elapses. Shared by runAndWait and
	 * runScript: POST /run/ returns a TRIGGER run id, which differs from the actual
	 * scenario run id, so the real run is resolved via get-run-for-trigger (the
	 * trigger id never matches lastRun and would misreport a completed scenario as
	 * a timeout). While no scenario run exists yet, the trigger fire itself is read:
	 * DSS cancels a fire when the scenario is already running (or a newer fire
	 * replaces it), and then no run will ever appear.
	 */
	private async trackTriggerRun(
		scenarioId: string,
		projectKey: string | undefined,
		trigger: Record<string, unknown>,
		timing: {
			startedAt: number;
			baseIntervalMs: number;
			adaptivePolling: boolean;
			timeout: number;
		},
	): Promise<TrackedRun> {
		const fire = triggerFireOf(trigger, scenarioId,);
		if (fire.cancelled) {
			return { runId: "", outcome: "CANCELLED", timedOut: false, cancelled: true, pollCount: 0, };
		}
		const pkEnc = this.enc(projectKey,);
		const base = `/public/api/projects/${pkEnc}/scenarios/${encodeURIComponent(scenarioId,)}`;
		const trigQuery = `triggerId=${encodeURIComponent(fire.triggerId,)}&triggerRunId=${
			encodeURIComponent(fire.triggerRunId,)
		}`;
		const remainingMs = () => Math.max(1, timing.timeout - (Date.now() - timing.startedAt),);
		let runId = "";
		let pollCount = 0;
		while (true) {
			if (Date.now() - timing.startedAt >= timing.timeout) {
				return { runId, outcome: "TIMEOUT", timedOut: true, cancelled: false, pollCount, };
			}
			pollCount += 1;
			// Budget the per-poll GETs by the remaining wait so a stalling
			// trigger-status endpoint cannot defeat the overall run deadline.
			let run: Record<string, unknown>;
			let cancelled = false;
			try {
				run = await this.client.get<Record<string, unknown>>(
					`${base}/get-run-for-trigger?${trigQuery}`,
					{ timeoutMs: remainingMs(), },
				);
				if (!isRecord(run.scenarioRun,)) {
					cancelled = await this.isTriggerFireCancelled(pkEnc, scenarioId, fire, remainingMs(),);
				}
			} catch (error) {
				if (!isRequestDeadlineError(error, timing.startedAt + timing.timeout,)) throw error;
				return { runId, outcome: "TIMEOUT", timedOut: true, cancelled: false, pollCount, };
			}
			if (cancelled) {
				return { runId, outcome: "CANCELLED", timedOut: false, cancelled: true, pollCount, };
			}
			const scenarioRun = run.scenarioRun as Record<string, unknown> | undefined;
			if (scenarioRun) {
				runId = (scenarioRun.runId as string | undefined) ?? runId;
				const result = scenarioRun.result as Record<string, unknown> | undefined;
				const finished = result?.outcome as string | undefined;
				if (finished) {
					return { runId, outcome: finished, timedOut: false, cancelled: false, pollCount, };
				}
			}
			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs: timing.baseIntervalMs,
				adaptiveEnabled: timing.adaptivePolling,
			},);
			const { promise, resolve, } = Promise.withResolvers<void>();
			setTimeout(resolve, Math.min(nextDelayMs, remainingMs(),),);
			await promise;
		}
	}

	/** Whether DSS cancelled this trigger fire (GET /scenarios/trigger/{id}/{triggerId}?triggerRunId=). */
	private async isTriggerFireCancelled(
		pkEnc: string,
		scenarioId: string,
		fire: { triggerId: string; triggerRunId: string; },
		timeoutMs?: number,
	): Promise<boolean> {
		const raw = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/trigger/${encodeURIComponent(scenarioId,)}/${
				encodeURIComponent(fire.triggerId,)
			}?triggerRunId=${encodeURIComponent(fire.triggerRunId,)}`,
			timeoutMs === undefined ? undefined : { timeoutMs, },
		);
		return raw.cancelled === true;
	}

	/**
	 * Best-effort step outcome summaries from the run report
	 * (GET /scenarios/{id}/{runId}). Never fails the wait: an unavailable
	 * run report returns undefined.
	 */
	private async runStepSummaries(
		base: string,
		runId: string,
	): Promise<ScenarioStepRun[] | undefined> {
		try {
			const details = await this.client.get<Record<string, unknown>>(
				`${base}/${encodeURIComponent(runId,)}/`,
			);
			const stepRuns = (details.stepRuns as Array<Record<string, unknown>> | undefined) ?? [];
			const mapped = stepRuns.map((entry,) => {
				const stepDef = entry.step as Record<string, unknown> | undefined;
				const stepResult = entry.result as Record<string, unknown> | undefined;
				return {
					name: stepDef?.name as string | undefined,
					type: stepDef?.type as string | undefined,
					outcome: (stepResult?.outcome as string | undefined) ?? "UNKNOWN",
					...scenarioStepWarningSummary(stepResult,),
				};
			},);
			return mapped.length > 0 ? mapped : undefined;
		} catch {
			// Best-effort diagnostics: never fail the wait because the run report is unavailable.
			return undefined;
		}
	}

	/**
	 * Delete the throwaway scenario at the end of a code run, or report the
	 * explicit keep outcome. Never throws: delete failures are surfaced in the
	 * returned cleanup outcome instead of being swallowed. A 404 is idempotent
	 * success — the scenario is already gone.
	 */
	private async removeThrowawayScenario(
		base: string,
		keep: boolean,
	): Promise<ScenarioScriptRunCleanup> {
		if (keep) return { status: "kept", };
		try {
			await this.client.del(base,);
			return { status: "deleted", };
		} catch (error) {
			if (error instanceof DataikuError && error.category === "not_found") {
				return { status: "deleted", };
			}
			return {
				status: "failed",
				error: error instanceof Error ? error.message : String(error,),
			};
		}
	}
}
