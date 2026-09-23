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

export const SCENARIO_CANONICAL_EDITABLE_FIELDS = [
	"params.steps",
	"params.triggers",
	"params.reporters",
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

function triggerQueryOf(trigger: Record<string, unknown>,): string {
	const triggerObj = trigger.trigger as Record<string, unknown> | undefined;
	const triggerId = (triggerObj?.id as string | undefined) ?? "manual";
	const triggerRunId = triggerRunIdOf(trigger,);
	return `triggerId=${encodeURIComponent(triggerId,)}&triggerRunId=${
		encodeURIComponent(triggerRunId,)
	}`;
}

/** A finite number, or undefined for anything else (absent/malformed JSON fields). */
function optionalNumber(value: unknown,): number | undefined {
	return typeof value === "number" && Number.isFinite(value,) ? value : undefined;
}

/** Map one get-last-runs entry to a ScenarioRunSummary, tolerating missing fields. */
function scenarioRunSummaryFromRaw(entry: unknown,): ScenarioRunSummary {
	if (!isRecord(entry,)) return { runId: "unknown", };
	const runId = typeof entry.runId === "string" ? entry.runId : "unknown";
	const optionalRecord = (value: unknown,): Record<string, unknown> | undefined =>
		isRecord(value,) ? value : undefined;
	return {
		runId,
		...(optionalNumber(entry.start,) !== undefined ? { start: optionalNumber(entry.start,), } : {}),
		...(optionalNumber(entry.end,) !== undefined ? { end: optionalNumber(entry.end,), } : {}),
		...(optionalRecord(entry.scenario,) !== undefined
			? { scenario: optionalRecord(entry.scenario,), }
			: {}),
		...(optionalRecord(entry.variables,) !== undefined
			? { variables: optionalRecord(entry.variables,), }
			: {}),
		...(optionalRecord(entry.result,) !== undefined
			? { result: optionalRecord(entry.result,), }
			: {}),
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
 */
function buildCodeRunScript(script: string,): string {
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
		`\t_dku_out.write("${CODE_RUN_OUTPUT_START}\\n")`,
		"\t_dku_out.write(_dku_buf.getvalue())",
		`\t_dku_out.write("\\n${CODE_RUN_OUTPUT_END}\\n")`,
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
 * case callers should fall back to the full log.
 */
function extractCodeRunOutput(log: string,): string | undefined {
	const messageRe = /^\[[^\]]*\] \[[^\]]*\] \[[^\]]*\] \[process\]  - (.*)$/;
	const contents: string[] = [];
	for (const rawLine of log.split("\n",)) {
		const line = rawLine.endsWith("\r",) ? rawLine.slice(0, -1,) : rawLine;
		const match = messageRe.exec(line,);
		if (match) contents.push(match[1] ?? "",);
	}
	// First start + last end, so a script that prints a marker string stays body content.
	const start = contents.indexOf(CODE_RUN_OUTPUT_START,);
	if (start < 0) return undefined;
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
	return body.join("\n",);
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
	return new DataikuError(
		400,
		"Scenario Update Verification Failed",
		JSON.stringify({
			message: `Scenario update did not persist requested fields after refetch: ${mismatchPaths}`,
			mismatches,
			canonicalEditableFields: SCENARIO_CANONICAL_EDITABLE_FIELDS,
			...(normalization.length > 0 ? { normalization, } : {}),
		},),
	);
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
			params: scenarioType === "step_based" ? { steps: [], triggers: [], reporters: [], } : {},
			...opts?.data,
		};
		await this.client.post<void>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/`,
			body,
		);
	}

	/** Trigger a scenario run. */
	async run(scenarioId: string, projectKey?: string,): Promise<{ runId: string; }> {
		const scEnc = encodeURIComponent(requireNonEmpty(scenarioId, "scenarioId",),);
		const result = await this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${scEnc}/run/`,
			{},
		);
		return { runId: triggerRunIdOf(result, scenarioId,), };
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
		const scEnc = encodeURIComponent(scenarioId,);
		const pkEnc = this.enc(projectKey,);
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${scEnc}/`,
		);
		const preview = scenarioUpdatePreview(current, data,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${scEnc}/`,
			preview.next,
		);
		const after = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${scEnc}/`,
		);
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
		const trigger = await this.client.post<Record<string, unknown>>(`${base}/run/`, {},);
		const tracked = await this.trackTriggerRun(scenarioId, base, trigger, {
			startedAt,
			baseIntervalMs,
			adaptivePolling,
			timeout,
		},);
		const resolvedRunId = tracked.runId || triggerRunIdOf(trigger,);
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
			...(triggerRunIdOf(trigger,) !== resolvedRunId
				? { triggerRunId: triggerRunIdOf(trigger,), }
				: {}),
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
				{ script: buildCodeRunScript(script,), extension: "py", },
			);

			const trigger = await this.client.post<Record<string, unknown>>(`${base}/run/`, {},);
			const tracked = await this.trackTriggerRun(scenarioId, base, trigger, {
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
				const limitedLog = await this.getRunLog(scenarioId, runId, {
					projectKey: opts?.projectKey,
					maxLogBytes,
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
		const output = extractCodeRunOutput(log,);
		return {
			scenarioId,
			runId,
			outcome,
			success: outcome === "SUCCESS",
			elapsedMs: Date.now() - startedAt,
			pollCount,
			output,
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

	/** Get the raw last-runs list (GET /scenarios/{id}/get-last-runs/?limit=N). */
	async getLastRuns(
		scenarioId: string,
		opts?: {
			limit?: number;
			projectKey?: string;
		},
	): Promise<ScenarioRunSummary[]> {
		if (opts?.limit !== undefined && (!Number.isInteger(opts.limit,) || opts.limit < 1)) {
			throw new ClientValidationError(`limit must be a positive integer, got ${opts.limit}.`,);
		}
		const scEnc = encodeURIComponent(scenarioId,);
		const query = opts?.limit !== undefined ? `?limit=${opts.limit}` : "";
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/scenarios/${scEnc}/get-last-runs/${query}`,
		);
		return requireArrayResponse<unknown>(raw, "scenarios.getLastRuns",).map(
			scenarioRunSummaryFromRaw,
		);
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
	 * `truncated` reports whether the cap cut the body.
	 */
	async getRunLog(
		scenarioId: string,
		runId: string,
		opts?: {
			stepId?: string;
			maxLogBytes?: number;
			projectKey?: string;
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
		const { text, truncated, } = await this.client.getTextLimited(
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

	/**
	 * Poll get-run-for-trigger until the triggered run reports an outcome or the
	 * deadline elapses. Shared by runAndWait and runScript: POST /run/ returns a
	 * TRIGGER run id, which differs from the actual scenario run id, so the real
	 * run is resolved via get-run-for-trigger (the trigger id never matches
	 * lastRun and would misreport a completed scenario as a timeout).
	 */
	private async trackTriggerRun(
		scenarioId: string,
		base: string,
		trigger: Record<string, unknown>,
		timing: {
			startedAt: number;
			baseIntervalMs: number;
			adaptivePolling: boolean;
			timeout: number;
		},
	): Promise<TrackedRun> {
		triggerRunIdOf(trigger, scenarioId,);
		const trigQuery = triggerQueryOf(trigger,);
		let runId = "";
		let pollCount = 0;
		while (true) {
			if (Date.now() - timing.startedAt >= timing.timeout) {
				return { runId, outcome: "TIMEOUT", timedOut: true, pollCount, };
			}
			pollCount += 1;
			// Budget the per-poll GET by the remaining wait so a stalling
			// trigger-status endpoint cannot defeat the overall run deadline.
			const remainingMs = Math.max(1, timing.timeout - (Date.now() - timing.startedAt),);
			let run: Record<string, unknown>;
			try {
				run = await this.client.get<Record<string, unknown>>(
					`${base}/get-run-for-trigger?${trigQuery}`,
					{ timeoutMs: remainingMs, },
				);
			} catch (error) {
				if (!isRequestDeadlineError(error, timing.startedAt + timing.timeout,)) throw error;
				return { runId, outcome: "TIMEOUT", timedOut: true, pollCount, };
			}
			const scenarioRun = run.scenarioRun as Record<string, unknown> | undefined;
			if (scenarioRun) {
				runId = (scenarioRun.runId as string | undefined) ?? runId;
				const result = scenarioRun.result as Record<string, unknown> | undefined;
				const finished = result?.outcome as string | undefined;
				if (finished) {
					return { runId, outcome: finished, timedOut: false, pollCount, };
				}
			}
			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs: timing.baseIntervalMs,
				adaptiveEnabled: timing.adaptivePolling,
			},);
			const { promise, resolve, } = Promise.withResolvers<void>();
			setTimeout(
				resolve,
				Math.min(
					nextDelayMs,
					Math.max(1, timing.timeout - (Date.now() - timing.startedAt),),
				),
			);
			await promise;
		}
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
