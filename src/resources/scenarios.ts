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
	log: string;
	envName?: string;
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
		const { runId, } = await this.run(scenarioId, opts?.projectKey,);
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? 2_000,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? 120_000,);
		const startedAt = Date.now();
		let pollCount = 0;

		while (true) {
			pollCount += 1;
			const st = await this.status(scenarioId, opts?.projectKey,);
			const elapsedMs = Date.now() - startedAt;

			// Scenario finished when it's no longer running and lastRun matches our runId
			if (!st.running && st.lastRun?.runId === runId) {
				const outcome = st.lastRun?.outcome ?? "UNKNOWN";
				return {
					scenarioId,
					runId,
					outcome,
					success: outcome === "SUCCESS",
					elapsedMs,
					pollCount,
				};
			}

			if (elapsedMs >= timeout) {
				return {
					scenarioId,
					runId,
					outcome: "TIMEOUT",
					success: false,
					elapsedMs,
					pollCount,
					timedOut: true,
				};
			}

			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled: adaptivePolling,
			},);
			await new Promise((r,) => setTimeout(r, Math.min(nextDelayMs, timeout - elapsedMs,),));
		}
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
		},
	): Promise<ScenarioScriptRunResult> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const pkEnc = this.enc(opts?.projectKey,);
		const scenarioId = `dss_cli_code_run_${Date.now()}`;
		const base = `/public/api/projects/${pkEnc}/scenarios/${encodeURIComponent(scenarioId,)}`;
		const envSelection = opts?.envName
			? { envMode: "EXPLICIT_ENV", envName: opts.envName, }
			: { envMode: "INHERIT", };
		const startedAt = Date.now();
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? 2_000,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? 120_000,);
		let created = false;
		try {
			await this.client.post(`/public/api/projects/${pkEnc}/scenarios/`, {
				id: scenarioId,
				name: `dss code run (${scenarioId})`,
				projectKey: pk,
				type: "custom_python",
				params: { envSelection, },
			},);
			created = true;
			await this.client.putVoid(`${base}/payload`, { script, extension: "py", },);

			const trigger = await this.client.post<Record<string, unknown>>(`${base}/run/`, {},);
			const triggerObj = trigger.trigger as Record<string, unknown> | undefined;
			const triggerId = (triggerObj?.id as string | undefined) ?? "manual";
			const triggerRunId = String(trigger.runId ?? "",);
			const trigQuery = `triggerId=${encodeURIComponent(triggerId,)}&triggerRunId=${
				encodeURIComponent(triggerRunId,)
			}`;

			let runId = "";
			let outcome = "UNKNOWN";
			let pollCount = 0;
			while (true) {
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
				if (Date.now() - startedAt >= timeout) {
					outcome = "TIMEOUT";
					break;
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
			if (runId) {
				try {
					log = await this.client.getText(`${base}/${encodeURIComponent(runId,)}/log`,);
				} catch {
					// Log may be unavailable for an early failure or timeout; leave it empty.
				}
			}
			return {
				scenarioId,
				runId,
				outcome,
				success: outcome === "SUCCESS",
				elapsedMs: Date.now() - startedAt,
				pollCount,
				log,
				...(opts?.envName ? { envName: opts.envName, } : {}),
			};
		} finally {
			if (created && opts?.keepScenario !== true) {
				try {
					await this.client.del(base,);
				} catch {
					// Best-effort cleanup of the throwaway scenario.
				}
			}
		}
	}
}
