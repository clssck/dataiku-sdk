import { ClientValidationError, DataikuError, } from "../errors.js";
import { JobSummaryArraySchema, } from "../schemas.js";
import type { BuildMode, JobSummary, JobWaitResult, } from "../schemas.js";
import {
	computeNextPollDelayMs,
	DEFAULT_POLL_INTERVAL_MS,
	DEFAULT_TIMEOUT_MS,
	isRequestDeadlineError,
} from "../utils/polling.js";
import { asRecord, } from "../utils/records.js";
import { redactCredentialPairs, } from "../utils/secret-sanitize.js";
import { BaseResource, } from "./base.js";

const DEFAULT_MAX_LOG_LINES = 500;
/**
 * Default bytes retained from a job log body (default 10 MiB). Logs larger
 * than this are downloaded bounded: only the most recent `maxLogBytes` bytes
 * are kept, so a collaborator-influenced log cannot exhaust client memory.
 */
const DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024;

const TERMINAL_STATES = new Set([
	"DONE",
	"FAILED",
	"ABORTED",
	"KILLED",
	"CANCELED",
	"CANCELLED",
	"ERROR",
],);

/**
 * Buildable target kinds. DATASET and MANAGED_FOLDER are user-selectable;
 * MODEL_EVALUATION_STORE appears as a recipe output role and is a valid job
 * target per the official client (recipe.run object_type_map maps
 * COMPUTABLE_MODEL_EVALUATION_STORE to MODEL_EVALUATION_STORE before
 * JobDefinition.with_output).
 */
export type JobBuildTargetType = "DATASET" | "MANAGED_FOLDER" | "MODEL_EVALUATION_STORE";
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

/** Why a requested job log could not be retrieved. */
export type JobLogUnavailableReason = "not_found" | "error";

/**
 * A requested build target that no flow object can ever produce: a managed
 * dataset with no recipe writing it.
 */
export interface JobUnbuildableTarget {
	type: "DATASET";
	id: string;
	projectKey: string;
	reason: "no_producing_recipe";
}

/** Why a job that ended DONE is reported as failed because it built nothing. */
export interface JobNothingToBuildFailure {
	code: "nothing_to_build";
	message: string;
	targets: JobUnbuildableTarget[];
}

/** Result of a completed job wait, including log-unavailable metadata. */
export type JobWaitOutcome = JobWaitResult & {
	logSummary?: JobLogSummary;
	logUnavailable?: JobLogUnavailableReason;
	/** True when the job itself no longer exists on the server. */
	removed?: boolean;
	/** Set (with `success: false`) when a DONE job ran no activity and a target has no producer. */
	failure?: JobNothingToBuildFailure;
};

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
	if (targetType === "DATASET" || targetType === "MODEL_EVALUATION_STORE") {
		// MES targets follow the plain {projectKey, id, type} shape (official
		// JobDefinition.with_output sends the name/id plus object type, with no
		// managed-folder indirection).
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
		throw new ClientValidationError("At least one build target is required.", "validation_failed",);
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

/**
 * DSS tags recipe subprocess output by reader thread: `[null-out-N]` carries
 * the child's stdout and `[null-err-N]` its stderr. The legacy fallback is an
 * explicit `stdout:` / `stderr:` record prefix (hand-written or non-DSS logs);
 * a bare substring match would also select backend metadata such as the
 * process wrapper's `pipes {"stdout": ...}` line and crowd out real output.
 */
const LEGACY_STDOUT_RECORD = /^\s*stdout:/i;
const LEGACY_STDERR_RECORD = /^\s*stderr:/i;

/**
 * A DSS backend log record starts with a timestamped header line
 * (`[YYYY/MM/DD-HH:MM:SS.mmm]`); every following non-timestamped line is a
 * continuation of that record (exception chain, JVM frames, ...).
 */
const DSS_RECORD_HEADER = /^\[\d{4}\/\d{2}\/\d{2}-\d{2}:\d{2}:\d{2}\.\d{3}\]/;
const ERROR_KEYWORDS = /\b(error|failed|failure|exception|traceback)\b/i;
/**
 * Exception class names ending in the `Exception`/`Error` suffix (e.g.
 * `java.lang.ClassCastException`, `ValueError`): no word boundary precedes the
 * suffix inside a compound name, so the keyword regex alone misses them.
 */
const ERROR_CLASS_NAME = /\b\w*(?:Exception|Error)\b/;
const PY_TRACEBACK_HEAD = /^\s*Traceback \(most recent call last\):$/;
const PY_FILE_FRAME = /^\s*File "(?:[^"\\]|\\.)*", line \d+/;
const JVM_STACK_FRAME = /^\s+at\s/;
const JVM_MORE_FRAMES = /^\s*\.\.\. \d+ more\b/;

/**
 * Message payload of a DSS record line: timestamp header and the
 * `[thread] [LEVEL] [logger] -` prefix stripped, so continuation logic can be
 * shared between timestamped records and raw log lines.
 */
function dssRecordMessage(line: string,): string {
	return line
		.replace(/^\[\d{4}\/\d{2}\/\d{2}-\d{2}:\d{2}:\d{2}\.\d{3}\]\s?/, "",)
		.replace(/^\[[^\]]*\] \[[^\]]*\] \[[^\]]*\]\s+-\s?/, "",);
}

function isErrorContent(line: string,): boolean {
	return ERROR_KEYWORDS.test(line,) || ERROR_CLASS_NAME.test(line,) || PY_FILE_FRAME.test(line,);
}

/**
 * Remove embedded JSON object payloads (`{"...": ...}`) from a DSS record
 * line. Structured payloads such as `SQL status {"statusWarnLevel":"ERROR",...}`
 * carry level/class words as data, not as a failure of the record itself.
 * An unbalanced payload (truncated line) is dropped to the end of the line.
 */
function stripJsonPayloads(line: string,): string {
	let out = "";
	let from = 0;
	for (let start = line.indexOf('{"',); start !== -1; start = line.indexOf('{"', from,)) {
		out += line.slice(from, start,);
		let depth = 0;
		let inString = false;
		let end = line.length;
		for (let i = start; i < line.length; i++) {
			const ch = line[i];
			if (inString) {
				if (ch === "\\") i++;
				else if (ch === '"') inString = false;
			} else if (ch === '"') inString = true;
			else if (ch === "{") depth++;
			else if (ch === "}" && --depth === 0) {
				end = i + 1;
				break;
			}
		}
		from = end;
	}
	return out + line.slice(from,);
}

function lineMatchesLogFilter(line: string, filter: JobLogFilter,): boolean {
	const normalized = line.toLowerCase();
	switch (filter) {
		case "stdout":
			return normalized.includes("[null-out-",) || LEGACY_STDOUT_RECORD.test(line,)
				|| line.startsWith(">>> ",);
		case "stderr":
			return normalized.includes("[null-err-",) || LEGACY_STDERR_RECORD.test(line,);
		case "errors":
			return isErrorContent(line,);
		case "user":
			return !/^\d{4}[-/]\d{2}[-/]\d{2}/.test(line,)
				&& !normalized.includes("backend-log",)
				&& !normalized.includes("debug",);
	}
}

/**
 * Errors filter over DSS records. A record's exception spans lines: the
 * timestamped header carries the level/message, untimestamped continuations
 * carry the exception chain. A record whose header matches the error keywords
 * keeps its cause-bearing continuation lines (exception headers like
 * `pkg.FooException: msg`, `Caused by:` chains, Python `Traceback` essentials)
 * and drops bare JVM stack frames. A Python traceback body is tracked as state,
 * so `File "..."` frames and the user-code source line survive even though they
 * carry no error keyword — including when DSS re-logs each child-stderr line as
 * its own timestamped record.
 */
function filterJobLogToErrors(lines: string[],): string {
	const kept: string[] = [];
	let recordMatches = false;
	let pyTraceback = false;
	for (const line of lines) {
		const isHeader = DSS_RECORD_HEADER.test(line,);
		const message = dssRecordMessage(line,);
		if (pyTraceback) {
			if (JVM_STACK_FRAME.test(line,) || JVM_MORE_FRAMES.test(line,)) {
				if (isHeader) recordMatches = false;
				continue;
			}
			if (/^\s+\S/.test(message,)) {
				kept.push(line,); // `File "..."` frame or user-code source line
				if (isHeader) recordMatches = false;
				continue;
			}
			if (ERROR_CLASS_NAME.test(message,)) {
				kept.push(line,); // terminating exception line
				pyTraceback = false;
				if (isHeader) recordMatches = false;
				continue;
			}
			pyTraceback = false;
		}
		if (isHeader) {
			recordMatches = isErrorContent(stripJsonPayloads(line,),) || PY_FILE_FRAME.test(message,);
			if (recordMatches && (PY_TRACEBACK_HEAD.test(message,) || PY_FILE_FRAME.test(message,))) {
				pyTraceback = true;
			}
			if (recordMatches) kept.push(line,);
			continue;
		}
		if (PY_TRACEBACK_HEAD.test(message,)) {
			kept.push(line,);
			pyTraceback = true;
			continue;
		}
		if (
			recordMatches
			&& !JVM_STACK_FRAME.test(line,)
			&& !JVM_MORE_FRAMES.test(line,)
			&& isErrorContent(stripJsonPayloads(line,),)
		) kept.push(line,);
	}
	return kept.join("\n",);
}

/**
 * Filter a job log. DSS logs (any timestamped record) use the record-aware
 * errors pass; untimestamped (non-DSS) logs keep the per-line behaviour, since
 * no record structure can be inferred.
 */
function filterJobLog(log: string, filter: JobLogFilter | undefined,): string {
	if (!filter) return log;
	const lines = jobLogLines(log,);
	if (filter !== "errors" || !lines.some((line,) => DSS_RECORD_HEADER.test(line,))) {
		return lines.filter((line,) => lineMatchesLogFilter(line, filter,)).join("\n",);
	}
	return filterJobLogToErrors(lines,);
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
/** Shape of the job status payload returned by the jobs status endpoint. */
interface JobStatusPayload {
	baseStatus?: {
		def?: { id?: string; type?: string; projectKey?: string; outputs?: unknown; };
		state?: string;
	};
	globalState?: {
		done?: number;
		failed?: number;
		running?: number;
		total?: number;
	};
}

function nothingToBuildMessage(targets: JobUnbuildableTarget[],): string {
	const names = targets.map((target,) => `${target.projectKey}.${target.id}`).join(", ",);
	return `The job ran no activity: ${names} ${
		targets.length === 1 ? "is a managed dataset" : "are managed datasets"
	} that no recipe produces, so nothing was built. Create a recipe that writes ${
		targets.length === 1 ? "it" : "them"
	} or build the recipe's output instead.`;
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
	 * Use `0` or `-1` to return the log without line truncation. Credential-like
	 * JSON pairs (`"jobTicketSecret":"..."`) are redacted.
	 *
	 * The download is byte-bounded and deadline-covered: at most `maxLogBytes`
	 * (default 10 MiB) of the *most recent* log output is retained, so a
	 * collaborator-influenced oversized log cannot exhaust client memory while
	 * the line limit previously only applied after a full unbounded download.
	 * Logs up to the cap are returned in full.
	 */
	async log(
		jobId: string,
		opts?: {
			activity?: string;
			logId?: string;
			logFilter?: JobLogFilter;
			maxLogLines?: number;
			maxLogBytes?: number;
			projectKey?: string;
		},
	): Promise<string> {
		const jobEnc = encodeURIComponent(jobId,);
		const query = opts?.activity ? `?activity=${encodeURIComponent(opts.activity,)}` : "";
		// DSS cat-activity-log URLs require a browser session; API-key callers must use the public log endpoint.
		const path = `/public/api/projects/${this.enc(opts?.projectKey,)}/jobs/${jobEnc}/log/${query}`;
		const maxLogBytes = Math.max(1, opts?.maxLogBytes ?? DEFAULT_MAX_LOG_BYTES,);
		const { text, } = await this.client.getTextTailLimited(path, maxLogBytes,);
		// Single download choke point (logFromUrl and wait/summary go through here):
		// DSS logs embed the job ticket secret in start_session JSON.
		return limitJobLog(
			filterJobLog(redactCredentialPairs(text,), opts?.logFilter,),
			opts?.maxLogLines,
		);
	}

	async logFromUrl(
		logUrl: string,
		opts?: { maxLogLines?: number; maxLogBytes?: number; },
	): Promise<string> {
		const parsed = new URL(logUrl, "http://dss.local",);
		const projectKey = parsed.searchParams.get("projectKey",) ?? undefined;
		const jobId = parsed.searchParams.get("jobId",) ?? undefined;
		const activity = parsed.searchParams.get("activityId",) ?? undefined;
		if (!projectKey || !jobId || !activity) {
			throw new ClientValidationError(
				"Log URL must include projectKey, jobId, and activityId query parameters.",
				"validation_failed",
			);
		}
		return this.log(jobId, {
			activity,
			projectKey,
			maxLogLines: opts?.maxLogLines,
			maxLogBytes: opts?.maxLogBytes,
		},);
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
	): Promise<JobWaitOutcome> {
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
	): Promise<JobWaitOutcome> {
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
	 * A terminal state observed from the status endpoint is authoritative:
	 * when log retrieval fails afterwards (e.g. `not_found` because logs were
	 * purged or the job was removed), `wait` still returns the terminal
	 * outcome instead of throwing, with `logUnavailable` holding the machine-
	 * readable reason and `removed` set once the job itself is probed and
	 * confirmed gone. Explicit `includeLogs`/`summary` requests stay honest:
	 * `log`/`logSummary` are only present when logs were actually retrieved.
	 *
	 * A DONE job that ran no activity (`progress.total === 0`) is also what DSS
	 * reports for an already-up-to-date build, so it stays successful unless an
	 * output is a managed dataset that no recipe writes: then nothing can ever
	 * build it, and the outcome is `success: false` with
	 * `failure.code === "nothing_to_build"`.
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
	): Promise<JobWaitOutcome> {
		const projectEnc = this.enc(opts?.projectKey,);
		const jobEnc = encodeURIComponent(jobId,);
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		// Pre-existing contract: the effective budget is never below one poll
		// interval, so a single observation always fits inside it.
		const timeout = Math.max(baseIntervalMs, opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS,);
		const startedAt = Date.now();
		let pollCount = 0;
		let lastJ: JobStatusPayload | undefined;

		while (true) {
			const elapsedBeforeMs = Date.now() - startedAt;
			// The first observation always happens, even when the budget is
			// already spent. A later poll is never started after the deadline:
			// the loop reports the structured timeout from the last observed
			// state instead of issuing a request the budget cannot cover.
			if (lastJ !== undefined && elapsedBeforeMs >= timeout) {
				const bs = lastJ.baseStatus ?? {};
				const def = bs.def ?? {};
				const gs = lastJ.globalState ?? {};
				return {
					success: false,
					jobId,
					state: bs.state ?? "unknown",
					type: def.type ?? "unknown",
					elapsedMs: elapsedBeforeMs,
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
			pollCount += 1;

			// Requests issued while budget remains are bounded by the remaining
			// time; a spent budget still issues exactly one transport attempt
			// (no retries, client requestTimeoutMs cap) so the first observation
			// reaches the server instead of failing before any attempt.
			const remainingMs = timeout - elapsedBeforeMs;
			let j: JobStatusPayload;
			try {
				j = await this.client.get(
					`/public/api/projects/${projectEnc}/jobs/${jobEnc}/`,
					remainingMs > 0
						? { timeoutMs: remainingMs, }
						: { noRetry: true, },
				);
			} catch (error) {
				// The poll budget ran out mid-request: report the documented
				// structured timeout instead of letting the transport deadline
				// error escape the loop.
				if (!isRequestDeadlineError(error, startedAt + timeout,)) throw error;
				return {
					success: false,
					jobId,
					state: "unknown",
					type: "unknown",
					elapsedMs: Date.now() - startedAt,
					pollCount,
					timedOut: true,
					progress: {
						done: 0,
						failed: 0,
						running: 0,
						total: null,
					},
				};
			}

			lastJ = j;
			const bs = j.baseStatus ?? {};
			const def = bs.def ?? {};
			const gs = j.globalState ?? {};
			const state = bs.state ?? "unknown";
			const elapsedMs = Date.now() - startedAt;

			if (isTerminalState(state,)) {
				const success = isSuccessfulTerminalState(state,);

				let log: string | undefined;
				let logSummary: JobLogSummary | undefined;
				let logUnavailable: JobLogUnavailableReason | undefined;
				let removed: boolean | undefined;

				if (opts?.includeLogs || opts?.summary) {
					try {
						// Fetch unlimited lines (still byte-bounded by log()) so the
						// filter runs over the whole retained log; the line limit is
						// applied to the filtered result below.
						const rawLog = await this.log(jobId, {
							activity: opts.activity,
							maxLogLines: 0,
							logId: opts.logId,
							projectKey: opts.projectKey,
						},);
						const filteredLog = filterJobLog(rawLog, opts.logFilter,);
						if (opts.includeLogs) log = limitJobLog(filteredLog, opts.maxLogLines,);
						if (opts.summary) {
							logSummary = summarizeJobLog(
								state,
								filteredLog,
								opts.maxLogLines ?? 20,
								elapsedMs,
							);
						}
					} catch (error) {
						if (error instanceof DataikuError) {
							// A terminal outcome observed from the status endpoint is
							// authoritative; failed log retrieval must not fail the wait.
							logUnavailable = error.category === "not_found" ? "not_found" : "error";
							if (logUnavailable === "not_found") {
								removed = await this.probeJobRemoved(projectEnc, jobEnc,);
							}
						} else {
							throw error;
						}
					}
				}

				const unbuildable = success && gs.total === 0
					? await this.unbuildableTargets(def.outputs, def.projectKey ?? opts?.projectKey,)
					: [];
				const failure: JobNothingToBuildFailure | undefined = unbuildable.length > 0
					? {
						code: "nothing_to_build",
						message: nothingToBuildMessage(unbuildable,),
						targets: unbuildable,
					}
					: undefined;

				return {
					success: success && failure === undefined,
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
					...(failure !== undefined ? { failure, } : {}),
					...(log !== undefined ? { log, } : {}),
					...(logSummary !== undefined ? { logSummary, } : {}),
					...(logUnavailable !== undefined ? { logUnavailable, } : {}),
					...(removed !== undefined ? { removed, } : {}),
				};
			}

			// Timeout — the guard at the top of the loop returns the failure
			// result from this observation without issuing another request.
			if (elapsedMs >= timeout) continue;

			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled: adaptivePolling,
			},);
			await sleep(Math.min(nextDelayMs, timeout - elapsedMs,),);
		}
	}

	/**
	 * Dataset outputs of a job that ran no activity which nothing can produce.
	 * DSS ends such a job DONE exactly like an already-up-to-date build, so the
	 * job status cannot tell them apart; the flow graph can: a target is
	 * unbuildable when it is a managed dataset (DSS owns its storage, so a
	 * recipe is the only way to fill it) that no recipe writes. Unmanaged
	 * datasets (uploads, external tables) are sources and legitimately build
	 * nothing. Lookups that fail leave the DONE outcome untouched.
	 */
	private async unbuildableTargets(
		outputs: unknown,
		fallbackProjectKey: string | undefined,
	): Promise<JobUnbuildableTarget[]> {
		if (!Array.isArray(outputs,)) return [];
		const writtenByProject = new Map<string, Set<string>>();
		const unbuildable: JobUnbuildableTarget[] = [];
		for (const output of outputs) {
			const record = asRecord(output,);
			if (record?.type !== "DATASET" || typeof record.targetDataset !== "string") continue;
			const id = record.targetDataset;
			const projectKey = typeof record.targetDatasetProjectKey === "string"
				? record.targetDatasetProjectKey
				: fallbackProjectKey;
			if (projectKey === undefined) continue;
			try {
				const details = await this.client.datasets.get(id, projectKey,);
				if (details.managed !== true) continue;
				let written = writtenByProject.get(projectKey,);
				if (written === undefined) {
					const { graph, } = await this.client.projects.flowTopology(projectKey,);
					written = new Set(
						graph.edges.filter((edge,) => edge.relation === "writes").map((edge,) => edge.to),
					);
					writtenByProject.set(projectKey, written,);
				}
				if (!written.has(id,)) {
					unbuildable.push({ type: "DATASET", id, projectKey, reason: "no_producing_recipe", },);
				}
			} catch (error) {
				if (!(error instanceof DataikuError || error instanceof ClientValidationError)) throw error;
			}
		}
		return unbuildable;
	}

	/**
	 * Probe whether a job still exists. Returns `true` when the server reports
	 * it as not found, `false` when it is still visible, and `undefined` when
	 * the probe outcome is ambiguous.
	 */
	private async probeJobRemoved(projectEnc: string, jobEnc: string,): Promise<boolean | undefined> {
		try {
			await this.client.get(`/public/api/projects/${projectEnc}/jobs/${jobEnc}/`,);
			return false;
		} catch (error) {
			if (error instanceof DataikuError && error.category === "not_found") return true;
			return undefined;
		}
	}

	/** Request a job abort. */
	async abort(jobId: string, projectKey?: string,): Promise<void> {
		const jobEnc = encodeURIComponent(jobId,);
		await this.client.post(`/public/api/projects/${this.enc(projectKey,)}/jobs/${jobEnc}/abort/`,);
	}
}
