#!/usr/bin/env bun
/**
 * Agent-navigability eval (`bun run eval:agents`).
 *
 * Provisions a self-contained case on the trial DSS, runs a headless coding
 * agent (`omp -p`) in an isolated workspace with only the installed skill and
 * `bin/dss`, verifies the outcome against DSS state and the final answer, parses
 * the agent's session log, and writes `.agent-eval/<runId>/report.{json,md}` plus
 * the session jsonl. Failed `dss` calls in the report show where the CLI is hard
 * for an agent to navigate.
 *
 * Credentials come from the repo .env via the live-suite loader; nothing secret
 * is logged, and the API key is redacted from reports and the copied session.
 * Only projects this run created (`AGENTEVAL_*`) are ever deleted.
 */
import type { Subprocess, } from "bun";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import { DataikuClient, } from "../src/client.js";
import { resolveAdminManagedStorageConnection, } from "../src/resources/connections.js";
import {
	type CaseContext,
	EVAL_CASES,
	type EvalCase,
	type RunOutcome,
	type Verdict,
} from "./agent-eval/cases.js";
import {
	type CaseRecord,
	findMutatingCalls,
	renderReportMarkdown,
	type RunReport,
} from "./agent-eval/report.js";
import { dssActionOf, parseSessionLog, type SessionMetrics, } from "./agent-eval/session-log.js";
import { loadLiveCredentials, } from "./live-suite.js";

const REPO_ROOT = path.resolve(import.meta.dir, "..",);
const DSS_BIN = path.join(REPO_ROOT, "bin", "dss",);
const REPORT_ROOT = path.join(REPO_ROOT, ".agent-eval",);
const PROJECT_PREFIX = "AGENTEVAL_";
const DEFAULT_MAX_TIME = "20m";

// ---------------------------------------------------------------------------
// CLI arguments
// ---------------------------------------------------------------------------

export interface AgentEvalOptions {
	cases: string[];
	model: string;
	maxTime: string;
	keep: boolean;
	list: boolean;
	help: boolean;
}

export class AgentEvalUsageError extends Error {}

const USAGE = [
	"Usage: bun run eval:agents [--case ID[,ID]] [--model M] [--max-time 20m] [--keep] [--list]",
	"",
	"  --case ID[,ID]   cases to run (default: all); repeatable",
	"  --model M        model passed to omp (default: omp's configured model)",
	"  --max-time T     per-case agent time limit, e.g. 600, 10m, 1h (default 20m)",
	"  --keep           keep the case project and temp dirs (default: delete them)",
	"  --list           list cases and exit (offline)",
].join("\n",);

export function parseMaxTimeMs(value: string,): number {
	const match = /^(\d+)([smh])?$/.exec(value,);
	if (!match) {
		throw new AgentEvalUsageError(`--max-time must look like 600, 10m or 1h, got "${value}"`,);
	}
	const unit = { s: 1000, m: 60_000, h: 3_600_000, }[match[2] ?? "s"] ?? 1000;
	return Number(match[1],) * unit;
}

export function parseAgentEvalArgs(argv: string[],): AgentEvalOptions {
	const options: AgentEvalOptions = {
		cases: [],
		model: "",
		maxTime: DEFAULT_MAX_TIME,
		keep: false,
		list: false,
		help: false,
	};
	for (let index = 0; index < argv.length; index += 1) {
		const raw = argv[index] ?? "";
		const eq = raw.startsWith("--",) ? raw.indexOf("=",) : -1;
		const flag = eq === -1 ? raw : raw.slice(0, eq,);
		const value = (): string => {
			if (eq !== -1) return raw.slice(eq + 1,);
			index += 1;
			const next = argv[index];
			if (next === undefined) throw new AgentEvalUsageError(`${flag} needs a value`,);
			return next;
		};
		if (flag === "--case") {
			options.cases.push(...value().split(",",).map((id,) => id.trim()).filter(Boolean,),);
		} else if (flag === "--model") options.model = value();
		else if (flag === "--max-time") options.maxTime = value();
		else if (flag === "--keep") options.keep = true;
		else if (flag === "--list") options.list = true;
		else if (flag === "--help" || flag === "-h") options.help = true;
		else throw new AgentEvalUsageError(`Unknown argument: ${raw}`,);
	}
	parseMaxTimeMs(options.maxTime,);
	const known = EVAL_CASES.map((evalCase,) => evalCase.id);
	const unknown = options.cases.filter((id,) => !known.includes(id,));
	if (unknown.length > 0) {
		throw new AgentEvalUsageError(
			`Unknown case: ${unknown.join(", ",)} (known: ${known.join(", ",)})`,
		);
	}
	return options;
}

// ---------------------------------------------------------------------------
// Process helpers
// ---------------------------------------------------------------------------

interface Credentials {
	url: string;
	apiKey: string;
}

/** Environment for every child: inherited (loadEnvFile already put DATAIKU_* in it) minus ambient overrides. */
function childEnv(
	credentials: Credentials,
	configDir: string,
	extra: Record<string, string> = {},
) {
	const env: Record<string, string | undefined> = { ...process.env, };
	delete env["DATAIKU_DISABLE_ENV"];
	delete env["DATAIKU_PROJECT_KEY"];
	return {
		...env,
		DATAIKU_URL: credentials.url,
		DATAIKU_API_KEY: credentials.apiKey,
		DSS_CONFIG_DIR: configDir,
		...extra,
	};
}

let activeChild: Subprocess<"ignore", "pipe", "pipe"> | undefined;
let interrupted = false;

async function run(
	cmd: string[],
	options: { cwd: string; env: Record<string, string | undefined>; timeoutMs: number; },
): Promise<{ exitCode: number | null; stdout: string; stderr: string; timedOut: boolean; }> {
	const child = Bun.spawn(cmd, {
		cwd: options.cwd,
		env: options.env,
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
		timeout: options.timeoutMs,
		killSignal: "SIGKILL",
	},);
	activeChild = child;
	try {
		const [stdout, stderr,] = await Promise.all([
			new Response(child.stdout,).text(),
			new Response(child.stderr,).text(),
		],);
		await child.exited;
		return {
			exitCode: child.exitCode,
			stdout,
			stderr,
			timedOut: child.signalCode === "SIGKILL" && !interrupted,
		};
	} finally {
		activeChild = undefined;
	}
}

function makeDss(credentials: Credentials, configDir: string, cwd: string,): CaseContext["dss"] {
	return async (args, options = {},) => {
		const result = await run([DSS_BIN, ...args,], {
			cwd,
			env: childEnv(credentials, configDir,),
			timeoutMs: options.timeoutMs ?? 120_000,
		},);
		let json: unknown;
		try {
			json = JSON.parse(result.stdout,);
		} catch { /* non-JSON output */ }
		const exitCode = result.exitCode ?? -1;
		if (exitCode !== 0 && !options.allowFailure) {
			const detail = (result.stdout.trim() || result.stderr.trim()).slice(0, 400,);
			throw new Error(`dss ${args.slice(0, 2,).join(" ",)} failed (exit ${exitCode}): ${detail}`,);
		}
		return { exitCode, stdout: result.stdout, stderr: result.stderr, json, };
	};
}

// ---------------------------------------------------------------------------
// Case lifecycle
// ---------------------------------------------------------------------------

interface Environment {
	credentials: Credentials;
	owner: string;
	connection: string;
}

async function resolveEnvironment(): Promise<Environment> {
	const credentials = loadLiveCredentials();
	const client = new DataikuClient({
		...credentials,
		requestTimeoutMs: 30_000,
		retryMaxAttempts: 2,
	},);
	let owner = process.env["DATAIKU_TEST_OWNER"]?.trim() ?? "";
	if (!owner) {
		const info = await client.get<Record<string, unknown>>(
			"/public/api/auth/info?withSecrets=false",
		);
		const associated = info["associatedDSSUser"];
		if (typeof associated === "string") owner = associated.trim();
	}
	if (!owner) {
		throw new Error("Cannot resolve a project owner: set DATAIKU_TEST_OWNER in the repo .env",);
	}
	let connection = process.env["DATAIKU_TEST_CONNECTION"]?.trim() ?? "";
	if (!connection) {
		connection = await resolveAdminManagedStorageConnection(client, "allowManagedDatasets",) ?? "";
	}
	if (!connection) {
		throw new Error(
			"Cannot resolve a managed storage connection: set DATAIKU_TEST_CONNECTION in the repo .env",
		);
	}
	return { credentials, owner, connection, };
}

function newProjectKey(caseId: string,): string {
	const suffix = crypto.randomUUID().replace(/-/g, "",).slice(0, 6,).toUpperCase();
	return `${PROJECT_PREFIX}${caseId.toUpperCase().replace(/[^A-Z0-9]+/g, "_",)}_${suffix}`;
}

async function findSessionFile(sessionDir: string,): Promise<string | undefined> {
	const files = [...new Bun.Glob("*.jsonl",).scanSync(sessionDir,),].filter((file,) =>
		!file.startsWith("__",)
	);
	files.sort();
	return files.at(-1,);
}

async function classifyMutations(ctx: CaseContext, metrics: SessionMetrics,): Promise<string[][]> {
	const known = new Map<string, boolean>();
	for (const { argv, } of metrics.dssInvocations) {
		const action = dssActionOf(argv,);
		if (action === undefined || known.has(action,)) continue;
		const entry = (await ctx.dss(["commands", "run", "--fields", action,], { allowFailure: true, },))
			.json as
				| Record<string, { mutatesDss?: boolean; } | undefined>
				| undefined;
		known.set(action, entry?.[action]?.mutatesDss === true,);
	}
	return findMutatingCalls(metrics.dssInvocations, (action,) => known.get(action,) === true,);
}

async function runCase(
	evalCase: EvalCase,
	environment: Environment,
	options: AgentEvalOptions,
	outDir: string,
): Promise<CaseRecord> {
	const { credentials, } = environment;
	const projectKey = newProjectKey(evalCase.id,);
	const root = await fsp.mkdtemp(path.join(os.tmpdir(), "agent-eval-",),);
	const dirs = {
		work: path.join(root, "work",),
		workspace: path.join(root, "workspace",),
		sessions: path.join(root, "sessions",),
		setupConfig: path.join(root, "cfg-setup",),
		agentConfig: path.join(root, "cfg-agent",),
	};
	await Promise.all(Object.values(dirs,).map((dir,) => fsp.mkdir(dir, { recursive: true, },)),);
	const ctx: CaseContext = {
		projectKey,
		owner: environment.owner,
		connection: environment.connection,
		workDir: dirs.work,
		dss: makeDss(credentials, dirs.setupConfig, dirs.work,),
	};
	const record: CaseRecord = {
		id: evalCase.id,
		projectKey,
		success: false,
		reason: "",
		durationMs: 0,
		agentExitCode: null,
		timedOut: false,
		toolCalls: 0,
		toolCallsByTool: {},
		dssInvocationCount: 0,
		dssInvocations: [],
		failedDssCalls: [],
		usageErrors: 0,
		mutatingCalls: [],
		tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, cost: 0, },
		finalAnswer: "",
		cleanedUp: false,
	};
	let projectMayExist = false;
	try {
		log(`[${evalCase.id}] provisioning ${projectKey}`,);
		projectMayExist = true;
		await evalCase.setup(ctx,);

		log(`[${evalCase.id}] installing skill into the workspace`,);
		await run([DSS_BIN, "install-skill", "--agent", "omp",], {
			cwd: dirs.workspace,
			env: childEnv(credentials, dirs.setupConfig, { DATAIKU_DISABLE_ENV: "1", },),
			timeoutMs: 60_000,
		},);

		const prompt = evalCase.prompt(ctx,);
		const maxTimeMs = parseMaxTimeMs(options.maxTime,);
		const command = [
			"omp",
			"-p",
			"--auto-approve",
			"--max-time",
			options.maxTime,
			"--no-title",
			"--session-dir",
			dirs.sessions,
			"--cwd",
			dirs.workspace,
			...(options.model ? ["--model", options.model,] : []),
			prompt,
		];
		log(`[${evalCase.id}] running agent (max ${options.maxTime})`,);
		const started = Date.now();
		const agent = await run(command, {
			cwd: dirs.workspace,
			env: childEnv(credentials, dirs.agentConfig, {
				PATH: `${path.join(REPO_ROOT, "bin",)}${path.delimiter}${process.env["PATH"] ?? ""}`,
			},),
			timeoutMs: maxTimeMs + 120_000,
		},);
		record.durationMs = Date.now() - started;
		record.agentExitCode = agent.exitCode;
		record.timedOut = agent.timedOut;

		const sessionName = await findSessionFile(dirs.sessions,);
		let metrics: SessionMetrics | undefined;
		if (sessionName) {
			const rawSession = await Bun.file(path.join(dirs.sessions, sessionName,),).text();
			metrics = parseSessionLog(rawSession,);
			record.sessionFile = `${evalCase.id}.session.jsonl`;
			await fsp.writeFile(
				path.join(outDir, record.sessionFile,),
				rawSession.replaceAll(credentials.apiKey, "[redacted]",),
			);
		}
		if (metrics) {
			record.model = metrics.model;
			record.toolCalls = metrics.toolCalls;
			record.toolCallsByTool = metrics.toolCallsByTool;
			record.dssInvocationCount = metrics.dssInvocations.length;
			record.dssInvocations = metrics.dssInvocations.map(({ argv, },) => argv.join(" ",));
			record.failedDssCalls = metrics.failedDssCalls;
			record.usageErrors = metrics.usageErrors;
			record.tokens = metrics.tokens;
			record.mutatingCalls = await classifyMutations(ctx, metrics,);
		}
		record.finalAnswer = agent.stdout.trim() || metrics?.finalAnswer || "";

		log(`[${evalCase.id}] verifying`,);
		const outcome: RunOutcome = {
			answer: record.finalAnswer,
			metrics,
			mutatingCalls: record.mutatingCalls,
		};
		let verdict: Verdict;
		if (agent.timedOut) verdict = { ok: false, reason: "agent run timed out", };
		else if (!metrics) verdict = { ok: false, reason: "no session log was produced", };
		else verdict = await evalCase.verify(ctx, outcome,);
		record.success = verdict.ok;
		record.reason = verdict.reason;
	} catch (error) {
		record.error = error instanceof Error ? error.message : String(error,);
		record.reason ||= "case aborted before verification";
	} finally {
		if (options.keep) {
			log(`[${evalCase.id}] --keep: project ${projectKey} and ${root} left in place`,);
		} else {
			record.cleanedUp = await cleanup(ctx, projectKey, projectMayExist, root,);
		}
	}
	return record;
}

/** Delete the case project (only an `AGENTEVAL_` key this run created) and the temp root. */
async function cleanup(
	ctx: CaseContext,
	projectKey: string,
	projectMayExist: boolean,
	root: string,
): Promise<boolean> {
	let ok = true;
	if (projectMayExist && projectKey.startsWith(PROJECT_PREFIX,)) {
		try {
			await ctx.dss(["project", "delete", projectKey, "--drop-data", "--if-exists",],);
		} catch (error) {
			ok = false;
			log(`cleanup of ${projectKey} FAILED: ${(error as Error).message}`,);
		}
	}
	try {
		await fsp.rm(root, { recursive: true, force: true, },);
	} catch (error) {
		ok = false;
		log(`removing ${root} failed: ${(error as Error).message}`,);
	}
	return ok;
}

function log(message: string,): void {
	process.stderr.write(`agent-eval: ${message}\n`,);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	let options: AgentEvalOptions;
	try {
		options = parseAgentEvalArgs(process.argv.slice(2,),);
	} catch (error) {
		if (!(error instanceof AgentEvalUsageError)) throw error;
		process.stderr.write(`${error.message}\n\n${USAGE}\n`,);
		process.exitCode = 2;
		return;
	}
	if (options.help) {
		process.stdout.write(`${USAGE}\n`,);
		return;
	}
	if (options.list) {
		for (const evalCase of EVAL_CASES) {
			process.stdout.write(
				`${evalCase.id}${evalCase.readOnly ? " (read-only)" : ""}\t${evalCase.title}\n`,
			);
		}
		return;
	}

	const selected = options.cases.length > 0
		? EVAL_CASES.filter((evalCase,) => options.cases.includes(evalCase.id,))
		: EVAL_CASES;
	const environment = await resolveEnvironment();
	const startedAt = new Date();
	const runId = `${startedAt.toISOString().replace(/[-:]/g, "",).replace(/\.\d+Z$/, "Z",)}-${
		crypto.randomUUID().slice(0, 4,)
	}`;
	const outDir = path.join(REPORT_ROOT, runId,);
	await fsp.mkdir(outDir, { recursive: true, },);

	const records: CaseRecord[] = [];
	for (const evalCase of selected) {
		if (interrupted) break;
		records.push(await runCase(evalCase, environment, options, outDir,),);
	}

	const report: RunReport = {
		runId,
		startedAt: startedAt.toISOString(),
		finishedAt: new Date().toISOString(),
		model: options.model || records.find((record,) => record.model)?.model || "",
		maxTime: options.maxTime,
		cases: records,
	};
	const redact = (text: string,) => text.replaceAll(environment.credentials.apiKey, "[redacted]",);
	const markdown = renderReportMarkdown(report,);
	await fsp.writeFile(
		path.join(outDir, "report.json",),
		redact(`${JSON.stringify(report, null, 2,)}\n`,),
	);
	await fsp.writeFile(path.join(outDir, "report.md",), redact(markdown,),);
	process.stdout.write(redact(markdown,),);
	log(`report: ${path.relative(process.cwd(), outDir,) || outDir}`,);
	if (
		records.some((record,) =>
			!record.success || record.error !== undefined || !record.cleanedUp && !options.keep
		)
	) {
		process.exitCode = 1;
	}
}

if (import.meta.main) {
	for (const signal of ["SIGINT", "SIGTERM",] as const) {
		process.on(signal, () => {
			interrupted = true;
			log(`${signal} received; stopping the agent and cleaning up`,);
			activeChild?.kill("SIGTERM",);
		},);
	}
	await main();
	if (interrupted) process.exitCode = 130;
}
