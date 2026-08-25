import { mkdir, writeFile, } from "node:fs/promises";
import { dirname, resolve, } from "node:path";
import type { DataikuClient, } from "../../client.js";
import { parseJobLogProgress, } from "../../resources/jobs.js";
import type { BuildMode, JobSummary, } from "../../schemas.js";
import {
	jobBuildTargetTypeFromFlags,
	jobLogFilterFromFlag,
	maxLogLinesFromFlags,
	num,
	plainRecord,
} from "../coerce.js";
import { encodedProjectEndpoint, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function nestedValue(value: unknown, path: string[],): unknown {
	let current: unknown = value;
	for (const key of path) {
		const record = plainRecord(current,);
		if (!record) return undefined;
		current = record[key];
	}
	return current;
}

function stringPath(value: unknown, path: string[],): string | undefined {
	const item = nestedValue(value, path,);
	return typeof item === "string" && item.length > 0 ? item : undefined;
}

function numberPath(value: unknown, path: string[],): number | undefined {
	const item = nestedValue(value, path,);
	return typeof item === "number" && Number.isFinite(item,) ? item : undefined;
}

function firstNumberPath(value: unknown, paths: string[][],): number | undefined {
	for (const path of paths) {
		const item = numberPath(value, path,);
		if (item !== undefined) return item;
	}
	return undefined;
}

function jobSummaryId(job: JobSummary | Record<string, unknown>, fallback?: string,): string {
	return stringPath(job, ["baseStatus", "def", "id",],)
		?? stringPath(job, ["def", "id",],)
		?? stringPath(job, ["id",],)
		?? fallback
		?? "unknown";
}

function jobSummaryType(job: JobSummary | Record<string, unknown>,): string {
	return stringPath(job, ["baseStatus", "def", "type",],)
		?? stringPath(job, ["def", "type",],)
		?? stringPath(job, ["type",],)
		?? "unknown";
}

function jobSummaryState(job: JobSummary | Record<string, unknown>,): string {
	return stringPath(job, ["baseStatus", "state",],)
		?? stringPath(job, ["state",],)
		?? "unknown";
}

function filteredJobList(
	jobs: JobSummary[],
	flags: Record<string, string | boolean>,
): JobSummary[] {
	const state = typeof flags["state"] === "string" ? flags["state"].trim().toUpperCase() : "";
	const contains = typeof flags["contains"] === "string"
		? flags["contains"].trim().toLowerCase()
		: "";
	const output = typeof flags["output"] === "string" ? flags["output"].trim().toLowerCase() : "";
	let result = jobs.filter((job,) => {
		if (state && jobSummaryState(job,).toUpperCase() !== state) return false;
		const text = JSON.stringify(job,).toLowerCase();
		if (contains && !text.includes(contains,)) return false;
		if (output && !text.includes(output,)) return false;
		return true;
	},);
	const limit = flags["latest"] === true ? 1 : num(flags["limit"], "--limit",);
	if (limit !== undefined) result = result.slice(0, Math.max(0, limit,),);
	return result;
}

function maxNumber(values: number[],): number {
	return values.length === 0 ? 0 : Math.max(...values,);
}

function collectWarningCounts(
	value: unknown,
	inActivity: boolean,
	counts: { dss: number[]; activity: number[]; },
): void {
	if (Array.isArray(value,)) {
		for (const item of value) collectWarningCounts(item, inActivity, counts,);
		return;
	}
	const record = plainRecord(value,);
	if (!record) return;
	for (const [key, item,] of Object.entries(record,)) {
		const lower = key.toLowerCase();
		const nextInActivity = inActivity || lower.includes("activit",);
		if (lower.includes("warn",)) {
			const target = nextInActivity ? counts.activity : counts.dss;
			if (typeof item === "number" && Number.isFinite(item,)) target.push(item,);
			else if (Array.isArray(item,)) target.push(item.length,);
		}
		collectWarningCounts(item, nextInActivity, counts,);
	}
}

function jobWarningSummary(
	details: Record<string, unknown>,
	log: string | undefined,
): Record<string, unknown> {
	const counts = { dss: [] as number[], activity: [] as number[], };
	collectWarningCounts(details, false, counts,);
	const warningLines = log
		? log.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) =>
			/\bwarn(?:ing)?\b/i.test(line,)
		)
		: [];
	return {
		dssSummaryWarningCount: maxNumber(counts.dss,),
		activityWarningCount: maxNumber(counts.activity,),
		logWarnLineCount: warningLines.length,
		sampledWarningMessages: warningLines.slice(0, 5,),
	};
}

function jobDurationMs(details: Record<string, unknown>,): number | undefined {
	const started = firstNumberPath(details, [
		["baseStatus", "startTime",],
		["baseStatus", "start",],
		["startTime",],
		["start",],
	],);
	const ended = firstNumberPath(details, [
		["baseStatus", "endTime",],
		["baseStatus", "end",],
		["endTime",],
		["end",],
	],);
	return started !== undefined && ended !== undefined && ended >= started
		? ended - started
		: undefined;
}

async function jobInspectionSummary(
	client: DataikuClient,
	jobId: string,
	flags: Record<string, string | boolean>,
): Promise<Record<string, unknown>> {
	const projectKey = flags["project-key"] as string | undefined;
	const details = await client.jobs.get(jobId, projectKey,);
	let log: string | undefined;
	let logError: string | undefined;
	try {
		log = await client.jobs.log(jobId, {
			activity: flags["activity"] as string | undefined,
			logId: flags["log-id"] as string | undefined,
			maxLogLines: maxLogLinesFromFlags(flags,),
			projectKey,
		},);
	} catch (error: unknown) {
		logError = error instanceof Error ? error.message : String(error,);
	}
	const durationMs = jobDurationMs(details,);
	const progress = log ? parseJobLogProgress(log, durationMs,) : undefined;
	const logLines = log
		? log.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0)
		: [];
	const maxSummaryLines = Math.max(1, maxLogLinesFromFlags(flags,) ?? 20,);
	const outputs = nestedValue(details, ["baseStatus", "def", "outputs",],)
		?? nestedValue(details, ["def", "outputs",],)
		?? details.outputs;
	return {
		resource: "job",
		jobId: jobSummaryId(details, jobId,),
		state: jobSummaryState(details,),
		type: jobSummaryType(details,),
		...(durationMs !== undefined ? { durationMs, } : {}),
		...(outputs !== undefined ? { outputs, } : {}),
		warnings: jobWarningSummary(details, log,),
		...(progress
			? {
				progress,
				latestUsefulProgressLine: progress.lastProgressLine,
				doneLine: progress.doneLine,
			}
			: {}),
		logSummary: {
			lineCount: logLines.length,
			lines: logLines.slice(-maxSummaryLines,),
			...(logError ? { error: logError, } : {}),
		},
	};
}

export const jobCommands: Record<string, CommandMeta> = {
	list: {
		handler: async (c, _a, f,) =>
			filteredJobList(await c.jobs.list(f["project-key"] as string | undefined,), f,),
		usage:
			"dss job list [--state STATE] [--contains TEXT] [--output ID] [--latest] [--limit N] [--project-key KEY]",
		description: "List recent jobs, optionally filtered for automation.",
		examples: ["dss job list --state DONE --latest", "dss job list --contains WLM225S --limit 10",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss job get <id>",);
			return c.jobs.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss job get <id> [--project-key KEY]",
		description: "Get job details.",
		examples: ["dss job get JOB_ID",],
	},
	summary: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss job summary <id>",);
			return jobInspectionSummary(c, a[0], f,);
		},
		usage:
			"dss job summary <id> [--activity ACTIVITY_ID] [--log-id LOG_ID] [--max-lines N|--max-log-lines N] [--project-key KEY]",
		description: "Summarize job state, outputs, warnings, progress, and useful terminal log lines.",
		examples: ["dss job summary JOB_ID --max-log-lines 200",],
	},
	log: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job log <id>",);
			const logFilter = f["errors-only"] === true
				? "errors"
				: jobLogFilterFromFlag(f["log-filter"],);
			const log = await c.jobs.log(a[0], {
				activity: f["activity"] as string | undefined,
				logId: f["log-id"] as string | undefined,
				logFilter,
				maxLogLines: maxLogLinesFromFlags(f,),
				projectKey: f["project-key"] as string | undefined,
			},);
			const outputFile = (f["output"] as string | undefined)
				?? (f["output-file"] as string | undefined);
			if (!outputFile) return log;
			const outputPath = resolve(outputFile,);
			await mkdir(dirname(outputPath,), { recursive: true, },);
			await writeFile(outputPath, log.endsWith("\n",) ? log : `${log}\n`, "utf-8",);
			return outputPath;
		},
		usage:
			"dss job log <id> [--activity ACTIVITY_ID] [--log-id LOG_ID] [--log-filter stdout|stderr|user|errors] [--errors-only] [--max-lines N|--max-log-lines N] [--output PATH] [--project-key KEY]",
		description:
			"Get public API job log output. Use --errors-only (or --log-filter errors) to surface just error/traceback lines, and --output PATH to write the log to a file (stdout returns the path). --log-id is accepted for UI parity but DSS API-key auth cannot select browser-only cat-activity-log files.",
		examples: [
			"dss job log JOB_ID",
			"dss job log JOB_ID --activity main --max-log-lines 200",
		],
	},
	"log-url": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss job log-url <url>",);
			let parsed: URL;
			try {
				parsed = new URL(a[0], "http://dss.local",);
			} catch {
				throw new UsageError("Log URL must be a valid URL.",);
			}
			const projectKey = parsed.searchParams.get("projectKey",);
			const jobId = parsed.searchParams.get("jobId",);
			const activity = parsed.searchParams.get("activityId",);
			if (!projectKey || !jobId || !activity) {
				throw new UsageError(
					"Log URL must include projectKey, jobId, and activityId query parameters.",
				);
			}
			return c.jobs.logFromUrl(a[0], { maxLogLines: maxLogLinesFromFlags(f,), },);
		},
		usage: "dss job log-url <url> [--max-lines N|--max-log-lines N]",
		description: "Fetch a DSS cat-activity-log URL pasted from the UI.",
		examples: [
			'dss job log-url "https://dss/dip/api/flow/jobs/cat-activity-log?projectKey=TEST&jobId=JOB&activityId=A&logId=L"',
		],
	},
	build: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job build <target>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				buildMode: f["build-mode"] as BuildMode | undefined,
				partition: f["partition"] as string | undefined,
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				targetType: jobBuildTargetTypeFromFlags(f,),
				timeoutMs: num(f["timeout"], "--timeout",),
			};
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "build",
					resource: "job",
					target: a[0],
					...options,
					endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
					method: "POST",
				};
			}
			if (f["wait"] === true) {
				return c.jobs.buildAndWait(a[0], { ...options, projectKey: pk, },);
			}
			return c.jobs.build(a[0], { ...options, projectKey: pk, },);
		},
		usage:
			"dss job build <target> [--target-type dataset|managed-folder] [--type DATASET|MANAGED_FOLDER] [--build-mode MODE] [--wait] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
		description: "Start a dataset or managed-folder build, optionally waiting for completion.",
		examples: [
			"dss job build orders",
			"dss job build orders --build-mode RECURSIVE_BUILD --wait",
			"dss job build LT7TUHJ8 --target-type managed-folder --dry-run",
		],
	},
	"build-and-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job build-and-wait <target>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				buildMode: f["build-mode"] as BuildMode | undefined,
				includeLogs: f["include-logs"] === true,
				logFilter: jobLogFilterFromFlag(f["log-filter"],),
				maxLogLines: maxLogLinesFromFlags(f,),
				partition: f["partition"] as string | undefined,
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
				summary: f["summary"] === true,
				targetType: jobBuildTargetTypeFromFlags(f,),
			};
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "build-and-wait",
					resource: "job",
					target: a[0],
					...options,
					endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
					method: "POST",
				};
			}
			return c.jobs.buildAndWait(a[0], { ...options, projectKey: pk, },);
		},
		usage:
			"dss job build-and-wait <target> [--target-type dataset|managed-folder] [--type DATASET|MANAGED_FOLDER] [--build-mode MODE] [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
		description: "Build a dataset or managed folder and wait for completion.",
		examples: [
			"dss job build-and-wait orders",
			"dss job build-and-wait orders --include-logs --log-filter stdout --summary",
			"dss job build-and-wait orders --timeout 300000",
			"dss job build-and-wait LT7TUHJ8 --target-type managed-folder --dry-run",
		],
	},
	wait: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss job wait <id>",);
			return c.jobs.wait(a[0], {
				includeLogs: f["include-logs"] === true,
				logFilter: jobLogFilterFromFlag(f["log-filter"],),
				maxLogLines: maxLogLinesFromFlags(f,),
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
				summary: f["summary"] === true,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss job wait <id> [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--project-key KEY]",
		description: "Wait for an existing job to complete.",
		examples: [
			"dss job wait JOB_ID",
			"dss job wait JOB_ID --include-logs --log-filter stdout --summary --timeout 60000",
		],
	},
	monitor: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job monitor <id...>",);
			const options = {
				includeLogs: f["include-logs"] === true,
				logFilter: jobLogFilterFromFlag(f["log-filter"],),
				maxLogLines: maxLogLinesFromFlags(f,),
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
				summary: f["summary"] !== false,
				projectKey: f["project-key"] as string | undefined,
			};
			const jobs = await Promise.all(a.map((jobId,) => c.jobs.wait(jobId, options,)),);
			return a.length === 1 ? jobs[0] : { jobs, until: f["until"] ?? "all-done", };
		},
		usage:
			"dss job monitor <id...> [--summary] [--include-logs] [--log-filter stdout|stderr|user|errors] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--until all-done] [--project-key KEY]",
		description: "Monitor one or more existing jobs and summarize progress counters from logs.",
		examples: ["dss job monitor JOB_ID --summary", "dss job monitor JOB1 JOB2 --until all-done",],
	},
	watch: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job watch <id...>",);
			const options = {
				includeLogs: f["include-logs"] === true,
				logFilter: jobLogFilterFromFlag(f["log-filter"],),
				maxLogLines: maxLogLinesFromFlags(f,),
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
				summary: true,
				projectKey: f["project-key"] as string | undefined,
			};
			const jobs = await Promise.all(a.map((jobId,) => c.jobs.wait(jobId, options,)),);
			return a.length === 1 ? jobs[0] : { jobs, until: f["until"] ?? "all-done", };
		},
		usage:
			"dss job watch <id...> [--include-logs] [--log-filter stdout|stderr|user|errors] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--until all-done] [--project-key KEY]",
		description: "Watch one or more existing jobs with progress extraction enabled.",
		examples: ["dss job watch JOB_ID", "dss job watch JOB1 JOB2 --until all-done",],
	},
	abort: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss job abort <id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "abort",
					resource: "job",
					id: a[0],
					endpoint: encodedProjectEndpoint(c, pk, `/jobs/${encodeURIComponent(a[0],)}/abort/`,),
					method: "POST",
				};
			}
			await c.jobs.abort(a[0], pk,);
			return { aborted: a[0], resource: "job", };
		},
		usage: "dss job abort <id> [--dry-run] [--project-key KEY]",
		description: "Abort a running job.",
		examples: ["dss job abort JOB_ID", "dss job abort JOB_ID --dry-run",],
	},
};
