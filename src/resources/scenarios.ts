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

	/** Merge-update a scenario's definition. */
	async update(
		scenarioId: string,
		data: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const scEnc = encodeURIComponent(scenarioId,);
		const pkEnc = this.enc(projectKey,);
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${scEnc}/`,
		);
		const merged = deepMerge(current, data,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/scenarios/${scEnc}/`,
			merged,
		);
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
}
