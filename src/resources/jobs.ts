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

export type JobBuildTargetType = "DATASET" | "MANAGED_FOLDER";
export type JobLogFilter = "stdout" | "stderr" | "user" | "errors";

export interface JobLogProgress {
	lastProgressLine?: string;
	doneLine?: string;
	counters: Record<string, number>;
	rowsPerMinute?: number;
}

export interface JobLogSummary {
	state: string;
	lineCount: number;
	lines: string[];
	progress?: JobLogProgress;
}

export interface JobBuildTarget {
	id: string;
	type?: JobBuildTargetType;
	projectKey?: string;
	partition?: string;
}

export interface JobBuildOptions {
	buildMode?: BuildMode;
	autoUpdateSchema?: boolean;
	projectKey?: string;
	targetType?: JobBuildTargetType;
	partition?: string;
}

export interface JobBuildAndWaitOptions extends JobBuildOptions {
	activity?: string;
	includeLogs?: boolean;
	maxLogLines?: number;
	pollIntervalMs?: number;
	timeoutMs?: number;
	logFilter?: JobLogFilter;
	logId?: string;
	summary?: boolean;
}

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

const DEFAULT_TARGET_PARTITION = "NP";

function jobBuildOutput(
	target: JobBuildTarget,
	defaultProjectKey: string,
	defaultPartition: string | undefined,
	defaultTargetType: JobBuildTargetType | undefined,
): Record<string, unknown> {
	const targetType = target.type ?? defaultTargetType ?? "DATASET";
	const projectKey = target.projectKey ?? defaultProjectKey;
	const partition = target.partition ?? defaultPartition;
	const output: Record<string, unknown> = { projectKey, id: target.id, type: targetType, };
	if (targetType === "DATASET") {
		if (partition !== undefined) output.partition = partition;
	} else {
		output.targetManagedFolderProjectKey = projectKey;
		output.targetManagedFolder = target.id;
		output.targetPartition = partition ?? DEFAULT_TARGET_PARTITION;
	}
	return output;
}

function jobBuildDefinition(
	targets: JobBuildTarget[],
	defaultProjectKey: string,
	opts: JobBuildOptions | undefined,
): Record<string, unknown> {
	if (targets.length === 0) {
		throw new Error("At least one build target is required.",);
	}
	const payload: Record<string, unknown> = {
		outputs: targets.map((target,) =>
			jobBuildOutput(target, defaultProjectKey, opts?.partition, opts?.targetType,)
		),
		type: opts?.buildMode ?? "NON_RECURSIVE_FORCED_BUILD",
	};
	if (
		opts?.autoUpdateSchema
		&& targets.every((target,) => (target.type ?? opts?.targetType ?? "DATASET") === "DATASET")
	) {
		payload.autoUpdateSchemaBeforeEachRecipeRun = true;
	}
	return payload;
}

function jobLogLines(log: string,): string[] {
	return log.split(/\r?\n/,).map((line,) => line.trimEnd());
}

function lineMatchesLogFilter(line: string, filter: JobLogFilter,): boolean {
	const normalized = line.toLowerCase();
	switch (filter) {
		case "stdout":
			return normalized.includes("stdout",) || line.startsWith(">>> ",);
		case "stderr":
			return normalized.includes("stderr",);
		case "errors":
			return /\b(error|failed|failure|exception|traceback)\b/i.test(line,);
		case "user":
			return !/^\d{4}[-/]\d{2}[-/]\d{2}/.test(line,)
				&& !normalized.includes("backend-log",)
				&& !normalized.includes("debug",);
	}
}

function filterJobLog(log: string, filter: JobLogFilter | undefined,): string {
	if (!filter) return log;
	return jobLogLines(log,).filter((line,) => lineMatchesLogFilter(line, filter,)).join("\n",);
}

function limitJobLog(log: string, maxLines: number | undefined,): string {
	if (!log) return "";
	const limit = maxLines ?? DEFAULT_MAX_LOG_LINES;
	if (limit === 0 || limit === -1) return log;

	const lines = log.split(/\r?\n/,);
	const hasTrailingLineBreak = lines.length > 0 && lines[lines.length - 1] === "";
	if (hasTrailingLineBreak) lines.pop();
	if (lines.length <= limit) return log;

	const tail = lines.slice(-Math.max(1, limit,),).join("\n",);
	return hasTrailingLineBreak ? `${tail}\n` : tail;
}

function parsedCounterValue(value: string,): number {
	return Number(value.replace(/,/g, "",),);
}

export function parseJobLogProgress(log: string, elapsedMs?: number,): JobLogProgress | undefined {
	const counters: Record<string, number> = {};
	let lastProgressLine: string | undefined;
	let doneLine: string | undefined;
	for (const line of jobLogLines(log,)) {
		const normalized = line.trim();
		if (!normalized) continue;
		const lower = normalized.toLowerCase();
		let matched = false;
		for (
			const match of normalized.matchAll(
				/\b(scanned|matched|joined|written|emitted)\s+([0-9][0-9,]*)/gi,
			)
		) {
			counters[match[1]!.toLowerCase()] = parsedCounterValue(match[2]!,);
			matched = true;
		}
		const written = normalized.match(/\b([0-9][0-9,]*)\s+rows\s+successfully\s+written\b/i,);
		if (written) {
			counters.written = parsedCounterValue(written[1]!,);
			doneLine = normalized;
			matched = true;
		}
		if (lower.includes("done!",)) {
			doneLine = normalized;
			matched = true;
		}
		if (matched) lastProgressLine = normalized;
	}
	if (lastProgressLine === undefined && doneLine === undefined) return undefined;
	const writtenRows = counters.written ?? counters.emitted;
	const rowsPerMinute = writtenRows !== undefined && elapsedMs !== undefined && elapsedMs > 0
		? writtenRows / (elapsedMs / 60_000)
		: undefined;
	return {
		...(lastProgressLine ? { lastProgressLine, } : {}),
		...(doneLine ? { doneLine, } : {}),
		counters,
		...(rowsPerMinute !== undefined ? { rowsPerMinute, } : {}),
	};
}

function summarizeJobLog(
	state: string,
	log: string,
	maxLines: number,
	elapsedMs?: number,
): JobLogSummary {
	const lines = jobLogLines(log,).map((line,) => line.trim()).filter((line,) => line.length > 0);
	const summaryLines = lines.slice(-Math.max(1, maxLines,),);
	const progress = parseJobLogProgress(log, elapsedMs,);
	return {
		state,
		lineCount: lines.length,
		lines: summaryLines,
		...(progress ? { progress, } : {}),
	};
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
		opts?: { activity?: string; logId?: string; maxLogLines?: number; projectKey?: string; },
	): Promise<string> {
		let path: string;
		if (opts?.logId) {
			if (!opts.activity) throw new Error("activity is required when logId is provided.",);
			const params = new URLSearchParams({
				projectKey: this.resolveProjectKey(opts.projectKey,),
				jobId,
				activityId: opts.activity,
				logId: opts.logId,
			},);
			path = `/dip/api/flow/jobs/cat-activity-log?${params.toString()}`;
		} else {
			const jobEnc = encodeURIComponent(jobId,);
			const query = opts?.activity ? `?activity=${encodeURIComponent(opts.activity,)}` : "";
			path = `/public/api/projects/${this.enc(opts?.projectKey,)}/jobs/${jobEnc}/log/${query}`;
		}
		const log = await this.client.getText(path,);
		return limitJobLog(log, opts?.maxLogLines,);
	}

	async logFromUrl(logUrl: string, opts?: { maxLogLines?: number; },): Promise<string> {
		const parsed = new URL(logUrl, "http://dss.local",);
		const projectKey = parsed.searchParams.get("projectKey",) ?? undefined;
		const jobId = parsed.searchParams.get("jobId",) ?? undefined;
		const activity = parsed.searchParams.get("activityId",) ?? undefined;
		const logId = parsed.searchParams.get("logId",) ?? undefined;
		if (!projectKey || !jobId || !activity || !logId) {
			throw new Error(
				"Log URL must include projectKey, jobId, activityId, and logId query parameters.",
			);
		}
		return this.log(jobId, { activity, logId, projectKey, maxLogLines: opts?.maxLogLines, },);
	}

	/**
	 * Start a build job for one or more dataset or managed-folder outputs.
	 * Returns the new job's ID.
	 */
	async buildOutputs(
		targets: JobBuildTarget[],
		opts?: JobBuildOptions,
	): Promise<{ jobId: string; }> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const enc = encodeURIComponent(pk,);
		const jobDef = jobBuildDefinition(targets, pk, opts,);
		const job = await this.client.post<{ id: string; }>(`/public/api/projects/${enc}/jobs/`, jobDef,);
		return { jobId: job.id, };
	}

	/**
	 * Start a build job for a single dataset or managed folder.
	 * Returns the new job's ID.
	 */
	async build(
		targetId: string,
		opts?: JobBuildOptions,
	): Promise<{ jobId: string; }> {
		return this.buildOutputs([{
			id: targetId,
			type: opts?.targetType,
			partition: opts?.partition,
		},], opts,);
	}

	/**
	 * Build one or more dataset or managed-folder outputs and wait for a terminal state.
	 * Combines {@link buildOutputs} then {@link wait}.
	 */
	async buildAndWaitOutputs(
		targets: JobBuildTarget[],
		opts?: JobBuildAndWaitOptions,
	): Promise<JobWaitResult & { logSummary?: JobLogSummary; }> {
		const { jobId, } = await this.buildOutputs(targets, opts,);
		return this.wait(jobId, {
			activity: opts?.activity,
			includeLogs: opts?.includeLogs,
			logFilter: opts?.logFilter,
			logId: opts?.logId,
			maxLogLines: opts?.maxLogLines,
			pollIntervalMs: opts?.pollIntervalMs,
			summary: opts?.summary,
			timeoutMs: opts?.timeoutMs,
			projectKey: opts?.projectKey,
		},);
	}

	/**
	 * Build a dataset or managed folder and wait for the job to reach a terminal state.
	 * Combines {@link build} then {@link wait}.
	 */
	async buildAndWait(
		targetId: string,
		opts?: JobBuildAndWaitOptions,
	): Promise<JobWaitResult & { logSummary?: JobLogSummary; }> {
		return this.buildAndWaitOutputs([{
			id: targetId,
			type: opts?.targetType,
			partition: opts?.partition,
		},], opts,);
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
			logFilter?: JobLogFilter;
			logId?: string;
			maxLogLines?: number;
			pollIntervalMs?: number;
			summary?: boolean;
			timeoutMs?: number;
			projectKey?: string;
		},
	): Promise<JobWaitResult & { logSummary?: JobLogSummary; }> {
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
				let logSummary: JobLogSummary | undefined;
				if (opts?.includeLogs || opts?.summary) {
					const rawLog = await this.log(jobId, {
						activity: opts.activity,
						maxLogLines: opts.summary ? 0 : opts.maxLogLines,
						logId: opts.logId,
						projectKey: opts.projectKey,
					},);
					const filteredLog = filterJobLog(rawLog, opts.logFilter,);
					if (opts.includeLogs) log = limitJobLog(filteredLog, opts.maxLogLines,);
					if (opts.summary) {
						logSummary = summarizeJobLog(state, filteredLog, opts.maxLogLines ?? 20, elapsedMs,);
					}
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
					...(logSummary !== undefined ? { logSummary, } : {}),
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
