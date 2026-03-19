import { JobSummaryArraySchema, } from "../schemas.js";
import type { BuildMode, JobSummary, JobWaitResult, } from "../schemas.js";
import { BaseResource, } from "./base.js";

const DEFAULT_POLL_INTERVAL_MS = 2_000;
const MAX_POLL_INTERVAL_MS = 10_000;
const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_MAX_LOG_LINES = 500;

const TERMINAL_STATES = new Set([
	"DONE",
	"FAILED",
	"ABORTED",
	"KILLED",
	"CANCELED",
	"CANCELLED",
	"ERROR",
],);

function isTerminalState(state: string | undefined,): boolean {
	return TERMINAL_STATES.has((state ?? "").toUpperCase(),);
}

function isSuccessfulTerminalState(state: string | undefined,): boolean {
	return (state ?? "").toUpperCase() === "DONE";
}

interface ComputeNextPollDelayMsOptions {
	pollCount: number;
	baseIntervalMs: number;
	adaptiveEnabled: boolean;
}

/**
 * Compute the next poll delay.
 * When adaptive polling is enabled, the interval doubles every 3 polls,
 * capped at MAX_POLL_INTERVAL_MS (or baseIntervalMs if it's larger).
 */
export function computeNextPollDelayMs({
	pollCount,
	baseIntervalMs,
	adaptiveEnabled,
}: ComputeNextPollDelayMsOptions,): number {
	if (!adaptiveEnabled) {
		return baseIntervalMs;
	}
	const step = Math.max(0, Math.floor((pollCount - 1) / 3,),);
	const interval = baseIntervalMs * 2 ** step;
	return Math.min(interval, Math.max(baseIntervalMs, MAX_POLL_INTERVAL_MS,),);
}

function sleep(ms: number,): Promise<void> {
	return new Promise((resolve,) => setTimeout(resolve, ms,));
}

export class JobsResource extends BaseResource {
	/** List jobs in a project. */
	async list(projectKey?: string,): Promise<JobSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/jobs/`,
		);
		return this.client.safeParse(JobSummaryArraySchema, raw, "jobs.list",);
	}

	/** Get full details for a single job. */
	async get(jobId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const jobEnc = encodeURIComponent(jobId,);
		// Trailing slash required — DSS Cloud proxy misroutes URLs ending in .NNN (job ID timestamps)
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/jobs/${jobEnc}/`,
		);
	}

	/**
	 * Retrieve job log text.
	 * Returns the last `maxLogLines` lines (default 500) from the tail.
	 * Use `0` or `-1` to return the full log without truncation.
	 */
	async log(
		jobId: string,
		opts?: { activity?: string; maxLogLines?: number; projectKey?: string; },
	): Promise<string> {
		const jobEnc = encodeURIComponent(jobId,);
		const query = opts?.activity ? `?activity=${encodeURIComponent(opts.activity,)}` : "";
		const log = await this.client.getText(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/jobs/${jobEnc}/log/${query}`,
		);
		if (!log) return "";

		const limit = opts?.maxLogLines ?? DEFAULT_MAX_LOG_LINES;
		if (limit === 0 || limit === -1) {
			return log;
		}

		const lines = log.split("\n",);
		if (lines.length > limit) {
			return lines.slice(-limit,).join("\n",);
		}
		return log;
	}

	/**
	 * Start a dataset build job.
	 * Returns the new job's ID.
	 */
	async build(
		datasetName: string,
		opts?: {
			buildMode?: BuildMode;
			autoUpdateSchema?: boolean;
			projectKey?: string;
		},
	): Promise<{ jobId: string; }> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const enc = encodeURIComponent(pk,);
		const jobDef: Record<string, unknown> = {
			outputs: [{ projectKey: pk, id: datasetName, type: "DATASET", },],
			type: opts?.buildMode ?? "NON_RECURSIVE_FORCED_BUILD",
		};
		if (opts?.autoUpdateSchema) {
			jobDef.autoUpdateSchemaBeforeEachRecipeRun = true;
		}
		const job = await this.client.post<{ id: string; }>(`/public/api/projects/${enc}/jobs/`, jobDef,);
		return { jobId: job.id, };
	}

	/**
	 * Build a dataset and wait for the job to reach a terminal state.
	 * Combines {@link build} then {@link wait}.
	 */
	async buildAndWait(
		datasetName: string,
		opts?: {
			buildMode?: BuildMode;
			autoUpdateSchema?: boolean;
			activity?: string;
			includeLogs?: boolean;
			maxLogLines?: number;
			pollIntervalMs?: number;
			timeoutMs?: number;
			projectKey?: string;
		},
	): Promise<JobWaitResult> {
		const { jobId, } = await this.build(datasetName, {
			buildMode: opts?.buildMode,
			autoUpdateSchema: opts?.autoUpdateSchema,
			projectKey: opts?.projectKey,
		},);
		return this.wait(jobId, {
			activity: opts?.activity,
			includeLogs: opts?.includeLogs,
			maxLogLines: opts?.maxLogLines,
			pollIntervalMs: opts?.pollIntervalMs,
			timeoutMs: opts?.timeoutMs,
			projectKey: opts?.projectKey,
		},);
	}

	/**
	 * Poll a job until it reaches a terminal state or times out.
	 *
	 * Adaptive polling doubles the interval every 3 polls when
	 * `pollIntervalMs` is not explicitly set.
	 *
	 * On timeout, returns `{ success: false, ... }` rather than throwing.
	 */
	async wait(
		jobId: string,
		opts?: {
			activity?: string;
			includeLogs?: boolean;
			maxLogLines?: number;
			pollIntervalMs?: number;
			timeoutMs?: number;
			projectKey?: string;
		},
	): Promise<JobWaitResult> {
		const projectEnc = this.enc(opts?.projectKey,);
		const jobEnc = encodeURIComponent(jobId,);
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS,);
		const startedAt = Date.now();
		let pollCount = 0;

		while (true) {
			pollCount += 1;

			const j = await this.client.get<{
				baseStatus?: {
					def?: { id?: string; type?: string; };
					state?: string;
				};
				globalState?: {
					done?: number;
					failed?: number;
					running?: number;
					total?: number;
				};
			}>(`/public/api/projects/${projectEnc}/jobs/${jobEnc}/`,);

			const bs = j.baseStatus ?? {};
			const def = bs.def ?? {};
			const gs = j.globalState ?? {};
			const state = bs.state ?? "unknown";
			const elapsedMs = Date.now() - startedAt;

			if (isTerminalState(state,)) {
				const success = isSuccessfulTerminalState(state,);

				let log: string | undefined;
				if (opts?.includeLogs) {
					log = await this.log(jobId, {
						activity: opts.activity,
						maxLogLines: opts.maxLogLines,
						projectKey: opts.projectKey,
					},);
				}

				return {
					success,
					jobId: def.id ?? jobId,
					state,
					type: def.type ?? "unknown",
					elapsedMs,
					pollCount,
					progress: {
						done: gs.done ?? 0,
						failed: gs.failed ?? 0,
						running: gs.running ?? 0,
						total: gs.total ?? null,
					},
					...(log !== undefined ? { log, } : {}),
				};
			}

			// Timeout — return failure result, don't throw
			if (elapsedMs >= timeout) {
				return {
					success: false,
					jobId,
					state,
					type: def.type ?? "unknown",
					elapsedMs,
					pollCount,
					timedOut: true,
					progress: {
						done: gs.done ?? 0,
						failed: gs.failed ?? 0,
						running: gs.running ?? 0,
						total: gs.total ?? null,
					},
				};
			}

			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled: adaptivePolling,
			},);
			await sleep(Math.min(nextDelayMs, timeout - elapsedMs,),);
		}
	}

	/** Request a job abort. */
	async abort(jobId: string, projectKey?: string,): Promise<void> {
		const jobEnc = encodeURIComponent(jobId,);
		await this.client.post(`/public/api/projects/${this.enc(projectKey,)}/jobs/${jobEnc}/abort/`,);
	}
}
