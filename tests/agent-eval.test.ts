import { describe, expect, it, } from "bun:test";
import * as path from "node:path";

import { AgentEvalUsageError, parseAgentEvalArgs, parseMaxTimeMs, } from "../scripts/agent-eval.js";
import { EVAL_CASES, mentionsNumber, } from "../scripts/agent-eval/cases.js";
import {
	findMutatingCalls,
	renderReportMarkdown,
	type RunReport,
} from "../scripts/agent-eval/report.js";
import {
	dssActionOf,
	extractCodeDssInvocations,
	extractDssErrors,
	extractShellDssInvocations,
	parseSessionLog,
} from "../scripts/agent-eval/session-log.js";

const fixture = await Bun.file(
	path.join(import.meta.dir, "fixtures", "agent-eval", "session.jsonl",),
).text();

describe("parseSessionLog", () => {
	const metrics = parseSessionLog(fixture,);

	it("counts tool calls by tool and reads model, timing and the final answer", () => {
		expect(metrics.toolCalls,).toBe(5,);
		expect(metrics.toolCallsByTool,).toEqual({ bash: 2, eval: 2, read: 1, },);
		expect(metrics.model,).toBe("anthropic/test-model",);
		expect(metrics.startedAt,).toBe("2026-10-12T10:00:00.000Z",);
		expect(metrics.endedAt,).toBe("2026-10-12T10:01:10.000Z",);
		expect(metrics.finalAnswer,).toBe("Fixed. DE 54.9, FR 107.685, US 19.8.",);
	});

	it("sums token usage over assistant messages", () => {
		expect(metrics.tokens,).toMatchObject({
			input: 18,
			output: 56,
			cacheRead: 1000,
			cacheWrite: 20,
			total: 1094,
		},);
		expect(metrics.tokens.cost,).toBeCloseTo(0.08, 6,);
	});

	it("extracts dss invocations from bash commands and eval code", () => {
		expect(metrics.dssInvocations.map(({ tool, argv, },) => [tool, argv.join(" ",),]),).toEqual([
			["bash", "commands run --fields scenario.get.usage,flow.map.usage",],
			["bash", "scenario get nightly --project-key P1",],
			["eval", "recipe list --project-key P1",],
			["eval", "recipe cat compute_totals --project-key P1",],
			["eval", "dataset get {d} --project-key P1",],
			["bash", "scenario run-and-wait nightly --project-key P1",],
			["bash", "dataset preview orders --project-key P1",],
			["eval", "dataset preview {name} --rows 3",],
		],);
	});

	it("attributes failed dss calls to the invocation that failed and counts usage errors", () => {
		expect(metrics.failedDssCalls.map(({ code, exitCode, argv, },) => ({ code, exitCode, argv, })),)
			.toEqual([
				{
					code: "usage_error",
					exitCode: 1,
					argv: ["commands", "run", "--fields", "scenario.get.usage,flow.map.usage",],
				},
				{
					code: "not_found",
					exitCode: 2,
					argv: ["recipe", "cat", "compute_totals", "--project-key", "P1",],
				},
			],);
		expect(metrics.failedDssCalls[1]?.error,).toBe(
			'404 Not Found: Recipe "compute_totals" not found',
		);
		expect(metrics.usageErrors,).toBe(1,);
	});

	it("keeps candidates when a failure cannot be attributed", () => {
		const log = [
			JSON.stringify({
				type: "message",
				message: {
					role: "assistant",
					content: [{
						type: "toolCall",
						id: "x",
						name: "bash",
						arguments: { command: "dss project list; dss dataset list", },
					},],
				},
			},),
			JSON.stringify({
				type: "message",
				message: {
					role: "toolResult",
					toolCallId: "x",
					toolName: "bash",
					content: [{
						type: "text",
						text: '{"type":"error","error":"boom","code":"internal","exitCode":2}',
					},],
				},
			},),
		].join("\n",);
		const [failed,] = parseSessionLog(log,).failedDssCalls;
		expect(failed?.argv,).toBeNull();
		expect(failed?.candidates,).toEqual([["project", "list",], ["dataset", "list",],],);
	});

	it("ignores logs without tool calls", () => {
		const empty = parseSessionLog('{"type":"session"}\nnot json\n',);
		expect(empty.toolCalls,).toBe(0,);
		expect(empty.finalAnswer,).toBe("",);
		expect(empty.tokens.total,).toBe(0,);
	});
});

describe("extractShellDssInvocations", () => {
	it("handles prefixes, pipes, redirections, quoting and non-dss commands", () => {
		expect(
			extractShellDssInvocations(
				"FOO=1 dss dataset get 'my ds' --project-key \"P 1\" 2>&1 | head -3",
			),
		).toEqual([
			["dataset", "get", "my ds", "--project-key", "P 1",],
		],);
		expect(
			extractShellDssInvocations("cd /tmp && /repo/bin/dss recipe list > out.json; echo dss done",),
		).toEqual([
			["recipe", "list",],
		],);
		expect(extractShellDssInvocations("for r in a b; do dss recipe cat $r; done",),).toEqual([
			["recipe", "cat", "$r",],
		],);
		expect(extractShellDssInvocations("git status && ls dss-notes",),).toEqual([],);
	});
});

describe("extractCodeDssInvocations", () => {
	it("remembers a helper across eval calls and renders dynamic arguments", () => {
		const helpers = new Set<string>();
		extractCodeDssInvocations('def dss(*a):\n    return subprocess.run(["dss", *a])\n', helpers,);
		expect(extractCodeDssInvocations('dss("job", "build", ds, "--wait")', helpers,),).toEqual([
			["job", "build", "{ds}", "--wait",],
		],);
	});

	it("reads subprocess list literals and Bun shell templates", () => {
		const code =
			'subprocess.run(["dss", "dataset", "list", *P])\nawait $`dss recipe list --project-key X`';
		expect(extractCodeDssInvocations(code,),).toEqual([["dataset", "list", "*P",], [
			"recipe",
			"list",
			"--project-key",
			"X",
		],],);
	});
});

describe("extractDssErrors", () => {
	it("reads compact, pretty-printed and repr error envelopes", () => {
		const text = '{"type":"error","error":"a","code":"usage_error","exitCode":1}\n'
			+ '{\n  "type": "error",\n  "error": "b",\n  "code": "not_found",\n  "exitCode": 2\n}\n'
			+ "{'type': 'error', 'error': 'it\\'s c', 'code': 'forbidden', 'exitCode': 2}";
		expect(extractDssErrors(text,),).toEqual([
			{ code: "usage_error", error: "a", exitCode: 1, },
			{ code: "not_found", error: "b", exitCode: 2, },
			{ code: "forbidden", error: "it's c", exitCode: 2, },
		],);
	});
});

const MUTATING_ACTIONS = ["scenario.run", "recipe.set-payload",];

describe("mutating calls", () => {
	const mutating = MUTATING_ACTIONS.includes.bind(MUTATING_ACTIONS,);

	it("flags registry-mutating calls and skips dry-run, plan and unresolved actions", () => {
		const invocations = [
			["scenario", "run", "nightly",],
			["scenario", "run", "nightly", "--dry-run",],
			["recipe", "set-payload", "r", "--plan",],
			["recipe", "list",],
			["{resource}", "{action}",],
		].map((argv,) => ({ tool: "bash", toolCallId: "t", argv, }));
		expect(findMutatingCalls(invocations, mutating,),).toEqual([["scenario", "run", "nightly",],],);
	});

	it("resolves resource.action from argv", () => {
		expect(dssActionOf(["recipe", "cat", "x",],),).toBe("recipe.cat",);
		expect(dssActionOf(["--help",],),).toBeUndefined();
		expect(dssActionOf(["{r}", "get",],),).toBeUndefined();
	});
});

describe("agent-eval CLI", () => {
	it("defines the three required cases with unique ids", () => {
		const ids = EVAL_CASES.map((evalCase,) => evalCase.id);
		expect(ids,).toEqual(["scenario-fix", "data-question", "build-flow",],);
		expect(EVAL_CASES.find((evalCase,) => evalCase.id === "data-question")?.readOnly,).toBe(true,);
	});

	it("parses flags in both spellings", () => {
		expect(
			parseAgentEvalArgs([
				"--case",
				"scenario-fix,data-question",
				"--model=opus",
				"--max-time",
				"10m",
				"--keep",
			],),
		).toMatchObject({
			cases: ["scenario-fix", "data-question",],
			model: "opus",
			maxTime: "10m",
			keep: true,
			list: false,
		},);
		expect(parseAgentEvalArgs(["--list",],).list,).toBe(true,);
		expect(parseAgentEvalArgs([],),).toMatchObject({ cases: [], maxTime: "20m", keep: false, },);
	});

	it("rejects unknown flags, unknown cases and bad durations", () => {
		expect(() => parseAgentEvalArgs(["--bogus",],)).toThrow(AgentEvalUsageError,);
		expect(() => parseAgentEvalArgs(["--case", "nope",],)).toThrow(/Unknown case: nope/,);
		expect(() => parseAgentEvalArgs(["--max-time", "soon",],)).toThrow(AgentEvalUsageError,);
		expect(() => parseAgentEvalArgs(["--model",],)).toThrow(/needs a value/,);
	});

	it("converts durations", () => {
		expect(parseMaxTimeMs("90",),).toBe(90_000,);
		expect(parseMaxTimeMs("10m",),).toBe(600_000,);
		expect(parseMaxTimeMs("1h",),).toBe(3_600_000,);
	});
});

describe("renderReportMarkdown", () => {
	it("renders the result table and the friction list", () => {
		const metrics = parseSessionLog(fixture,);
		const report: RunReport = {
			runId: "r1",
			startedAt: "2026-10-12T10:00:00.000Z",
			finishedAt: "2026-10-12T10:02:00.000Z",
			model: "anthropic/test-model",
			maxTime: "20m",
			cases: [{
				id: "scenario-fix",
				projectKey: "AGENTEVAL_SCENARIO_FIX_ABC123",
				success: true,
				reason: "ok",
				durationMs: 70_000,
				agentExitCode: 0,
				timedOut: false,
				toolCalls: metrics.toolCalls,
				toolCallsByTool: metrics.toolCallsByTool,
				dssInvocationCount: metrics.dssInvocations.length,
				dssInvocations: metrics.dssInvocations.map(({ argv, },) => argv.join(" ",)),
				failedDssCalls: metrics.failedDssCalls,
				usageErrors: metrics.usageErrors,
				mutatingCalls: [],
				tokens: metrics.tokens,
				finalAnswer: metrics.finalAnswer,
				cleanedUp: true,
			},],
		};
		const markdown = renderReportMarkdown(report,);
		expect(markdown,).toContain("| scenario-fix | PASS | 70s | 5 | 8 | 2 | 1 | 1.1k / 0.1k |",);
		expect(markdown,).toContain("[scenario-fix] `usage_error` (exit 1) `dss commands run --fields",);
		expect(markdown,).toContain(
			"`not_found` (exit 2) `dss recipe cat compute_totals --project-key P1`",
		);
	});
});

describe("answer number matching", () => {
	it("matches a stated value by number, not by spelling, and rejects fragments", () => {
		const answer =
			"| DE | 54.90 |\n| FR | 107.685 |\n| US | 19.80 |\nrun 2026-10-12-13-32-08-085, total 1,234.5";
		expect(mentionsNumber(answer, "54.9",),).toBe(true,);
		expect(mentionsNumber(answer, "19.8",),).toBe(true,);
		expect(mentionsNumber(answer, "107.685",),).toBe(true,);
		expect(mentionsNumber(answer, "1234.5",),).toBe(true,);
		expect(mentionsNumber(answer, "107.68",),).toBe(false,);
		expect(mentionsNumber(answer, "4.9",),).toBe(false,);
		expect(mentionsNumber(answer, "13",),).toBe(false,);
	});
});
