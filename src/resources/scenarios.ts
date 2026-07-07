import { randomUUID, } from "node:crypto";
import { DataikuError, } from "../errors.js";
import type {
	ScenarioDetails,
	ScenarioStatus,
	ScenarioSummary,
	ScenarioWaitResult,
} from "../schemas.js";
import {
	ScenarioDetailsSchema,
	ScenarioStatusSchema,
	ScenarioSummaryArraySchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";
import { computeNextPollDelayMs, } from "./jobs.js";

export const SCENARIO_CANONICAL_EDITABLE_FIELDS = [
	"params.steps",
	"params.triggers",
	"params.reporters",
	"params.customScript",
	"active",
	"name",
] as const;

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
}

const DEFAULT_CODE_RUN_MAX_LOG_BYTES = 1_048_576;
const CODE_RUN_OUTPUT_START = "<<<DSS_CODE_RUN_OUTPUT_b7e3a1>>>";
const CODE_RUN_OUTPUT_END = "<<<DSS_CODE_RUN_OUTPUT_END_b7e3a1>>>";

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

function isRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
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
		const scEnc = encodeURIComponent(scenarioId,);
		const result = await this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/scenarios/${scEnc}/run/`,
			{},
		);
		return {
			runId: (result.id as string | undefined) ?? (result.runId as string | undefined) ?? "unknown",
		};
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
		const triggerObj = trigger.trigger as Record<string, unknown> | undefined;
		const triggerId = (triggerObj?.id as string | undefined) ?? "manual";
		const triggerRunId = String(trigger.runId ?? trigger.id ?? "",);
		if (!triggerRunId) {
			throw new DataikuError(
				500,
				"Scenario run not started",
				`Scenario "${scenarioId}" run trigger returned no run id; cannot track the run.`,
			);
		}
		const trigQuery = `triggerId=${encodeURIComponent(triggerId,)}&triggerRunId=${
			encodeURIComponent(triggerRunId,)
		}`;

		let runId = "";
		let outcome = "UNKNOWN";
		let pollCount = 0;
		let timedOut = false;
		while (true) {
			if (Date.now() - startedAt >= timeout) {
				outcome = "TIMEOUT";
				timedOut = true;
				break;
			}
			pollCount += 1;
			const run = await this.client.get<Record<string, unknown>>(
				`${base}/get-run-for-trigger?${trigQuery}`,
			);
			const scenarioRun = run.scenarioRun as Record<string, unknown> | undefined;
			if (scenarioRun) {
				runId = (scenarioRun.runId as string | undefined) ?? runId;
				const result = scenarioRun.result as Record<string, unknown> | undefined;
				const finished = result?.outcome as string | undefined;
				if (finished) {
					outcome = finished;
					break;
				}
			}
			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled: adaptivePolling,
			},);
			const { promise, resolve, } = Promise.withResolvers<void>();
			setTimeout(resolve, Math.min(nextDelayMs, Math.max(1, timeout - (Date.now() - startedAt),),),);
			await promise;
		}

		const resolvedRunId = runId || triggerRunId;
		let steps: Array<{ name?: string; type?: string; outcome: string; }> | undefined;
		if (!timedOut && runId) {
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
					};
				},);
				if (mapped.length > 0) steps = mapped;
			} catch {
				// Best-effort diagnostics: never fail the wait because the run report is unavailable.
			}
		}
		return {
			scenarioId,
			runId: resolvedRunId,
			outcome,
			success: outcome === "SUCCESS",
			elapsedMs: Date.now() - startedAt,
			pollCount,
			...(triggerRunId !== resolvedRunId ? { triggerRunId, } : {}),
			...(timedOut ? { timedOut: true, } : {}),
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
		const scenarioId = `dss_cli_code_run_${Date.now()}_${randomUUID().replace(/-/g, "",)}`;
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
			const triggerObj = trigger.trigger as Record<string, unknown> | undefined;
			const triggerId = (triggerObj?.id as string | undefined) ?? "manual";
			const triggerRunId = String(trigger.runId ?? "",);
			if (!triggerRunId) {
				throw new DataikuError(
					500,
					"Scenario run not started",
					`Scenario "${scenarioId}" run trigger returned no run id; cannot track the run.`,
				);
			}
			const trigQuery = `triggerId=${encodeURIComponent(triggerId,)}&triggerRunId=${
				encodeURIComponent(triggerRunId,)
			}`;

			let runId = "";
			let outcome = "UNKNOWN";
			let pollCount = 0;
			while (true) {
				if (Date.now() - startedAt >= timeout) {
					outcome = "TIMEOUT";
					break;
				}
				pollCount += 1;
				const run = await this.client.get<Record<string, unknown>>(
					`${base}/get-run-for-trigger?${trigQuery}`,
				);
				const scenarioRun = run.scenarioRun as Record<string, unknown> | undefined;
				if (scenarioRun) {
					runId = (scenarioRun.runId as string | undefined) ?? runId;
					const result = scenarioRun.result as Record<string, unknown> | undefined;
					const finished = result?.outcome as string | undefined;
					if (finished) {
						outcome = finished;
						break;
					}
				}
				const nextDelayMs = computeNextPollDelayMs({
					pollCount,
					baseIntervalMs,
					adaptiveEnabled: adaptivePolling,
				},);
				await new Promise((r,) =>
					setTimeout(r, Math.min(nextDelayMs, Math.max(1, timeout - (Date.now() - startedAt),),),)
				);
			}

			let log = "";
			let logTruncated = false;
			if (runId && outcome !== "TIMEOUT") {
				const limitedLog = await this.client.getTextLimited(
					`${base}/${encodeURIComponent(runId,)}/log`,
					maxLogBytes,
				);
				log = limitedLog.text;
				logTruncated = limitedLog.truncated;
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
			};
		} finally {
			if (opts?.keepScenario !== true) {
				try {
					await this.client.del(base,);
				} catch {
					// Best-effort cleanup of the throwaway scenario.
				}
			}
		}
	}
}
