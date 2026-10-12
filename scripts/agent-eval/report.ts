/** Pure report model + Markdown rendering + mutating-call classification for the agent eval. */
import {
	dssActionOf,
	type DssInvocation,
	type FailedDssCall,
	type SessionTokens,
} from "./session-log.js";

export interface CaseRecord {
	id: string;
	projectKey: string;
	success: boolean;
	reason: string;
	/** Wall time of the agent run, in milliseconds. */
	durationMs: number;
	agentExitCode: number | null;
	timedOut: boolean;
	model?: string;
	toolCalls: number;
	toolCallsByTool: Record<string, number>;
	dssInvocationCount: number;
	dssInvocations: string[];
	failedDssCalls: FailedDssCall[];
	usageErrors: number;
	mutatingCalls: string[][];
	tokens: SessionTokens;
	/** Model turns and context sizes; see SessionMetrics.turns. */
	turns: { count: number; firstTurnContextTokens: number; lastTurnContextTokens: number; };
	/** Characters of tool output from calls that ran `dss` (what the CLI added to the context). */
	dssOutputChars: number;
	finalAnswer: string;
	sessionFile?: string;
	/** Setup/run error that kept the case from reaching verification. */
	error?: string;
	cleanedUp: boolean;
}

export interface RunReport {
	runId: string;
	startedAt: string;
	finishedAt: string;
	model: string;
	maxTime: string;
	cases: CaseRecord[];
}

/**
 * Invocations that change DSS state. `isMutating` answers for a `resource.action`
 * (the CLI registry's `mutatesDss`); `--dry-run` and `--plan` previews never mutate.
 * Unresolvable argv (dynamic resource/action in eval code) is not counted.
 */
export function findMutatingCalls(
	invocations: DssInvocation[],
	isMutating: (action: string,) => boolean,
): string[][] {
	return invocations
		.filter(({ argv, },) => !argv.includes("--dry-run",) && !argv.includes("--plan",))
		.filter(({ argv, },) => {
			const action = dssActionOf(argv,);
			return action !== undefined && isMutating(action,);
		},)
		.map(({ argv, },) => argv);
}

export function formatArgv(argv: string[] | null,): string {
	return argv === null ? "(unknown call)" : `dss ${argv.join(" ",)}`;
}

function cell(value: string,): string {
	return value.replace(/\|/g, "\\|",).replace(/\s+/g, " ",).trim();
}

function oneLine(value: string, limit: number,): string {
	const flat = value.replace(/\s+/g, " ",).trim();
	return flat.length > limit ? `${flat.slice(0, limit - 1,)}…` : flat;
}

export function renderReportMarkdown(report: RunReport,): string {
	const lines = [
		`# Agent eval ${report.runId}`,
		"",
		`Model: ${report.model || "default"} · max time: ${report.maxTime} · started ${report.startedAt}`,
		"",
		"| Case | Result | Time | Turns | Tool calls | dss calls | Failed dss | Tokens (total / output) | Agent base context | CLI output |",
		"| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
	];
	for (const record of report.cases) {
		// ~4 chars per token: the CLI's share of what every later turn re-reads.
		const cliTokens = Math.round(record.dssOutputChars / 4,);
		lines.push(
			`| ${record.id} | ${record.success ? "PASS" : "FAIL"} | ${
				Math.round(record.durationMs / 1000,)
			}s | ${record.turns.count} | ${record.toolCalls} `
				+ `| ${record.dssInvocationCount} | ${record.failedDssCalls.length} `
				+ `| ${(record.tokens.total / 1000).toFixed(1,)}k / ${
					(record.tokens.output / 1000).toFixed(1,)
				}k `
				+ `| ${(record.turns.firstTurnContextTokens / 1000).toFixed(1,)}k `
				+ `| ~${(cliTokens / 1000).toFixed(1,)}k |`,
		);
	}
	lines.push(
		"",
		"Total tokens ≈ turns × context: each turn re-reads the agent's base context (system prompt, tools, skill) plus everything so far. CLI output is the part the CLI controls; fewer turns cut the total most.",
	);
	lines.push("", "## Verdicts", "",);
	for (const record of report.cases) {
		const tools = Object.entries(record.toolCallsByTool,).map(([name, count,],) => `${name} ${count}`)
			.join(", ",);
		lines.push(
			`- **${record.id}** (${record.projectKey}): ${record.success ? "PASS" : "FAIL"} — ${
				cell(record.reason,)
			}`
				+ `${record.timedOut ? " [timed out]" : ""}${
					record.error ? ` [error: ${cell(oneLine(record.error, 200,),)}]` : ""
				}`,
			`  - tools: ${
				tools || "none"
			}; tokens in/out/cacheRead/cacheWrite: ${record.tokens.input}/${record.tokens.output}/${record.tokens.cacheRead}/${record.tokens.cacheWrite}`,
		);
		if (record.mutatingCalls.length > 0) {
			lines.push(`  - mutating dss calls: ${record.mutatingCalls.length}`,);
		}
	}
	lines.push("", "## Friction (failed dss calls)", "",);
	const friction = report.cases.flatMap((record,) =>
		record.failedDssCalls.map((failed,) => ({ record, failed, }))
	);
	if (friction.length === 0) lines.push("None.",);
	for (const { record, failed, } of friction) {
		const call = failed.argv === null && failed.candidates
			? `one of ${failed.candidates.length} calls in a ${failed.tool} step`
			: `\`${oneLine(formatArgv(failed.argv,), 220,)}\``;
		lines.push(
			`- [${record.id}] \`${failed.code}\`${
				failed.exitCode === undefined ? "" : ` (exit ${failed.exitCode})`
			} ${call} — ${oneLine(failed.error, 200,)}`,
		);
	}
	lines.push("", "## Final answers", "",);
	for (const record of report.cases) {
		lines.push(`### ${record.id}`, "", oneLine(record.finalAnswer, 1200,) || "(no answer)", "",);
	}
	return `${lines.join("\n",)}\n`;
}
