import { readFileSync, } from "node:fs";
import { num, readStdinText, stripUtf8Bom, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

const CODE_RUN_USAGE =
	"dss code run (--file PATH | --stdin) [--env ENV] [--timeout MS] [--keep] [--full-log] [--max-log-bytes N] [--project-key KEY]";

function resolveCodeInput(args: string[], flags: Record<string, string | boolean>,): string {
	if (args.length > 0) {
		throw new UsageError(
			`code run takes no positional arguments; pass the script via --file PATH or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	const sources: Array<{ label: string; read: () => string; }> = [];
	if (typeof flags["file"] === "string") {
		sources.push({ label: "--file", read: () => readFileSync(flags["file"] as string, "utf-8",), },);
	}
	if (flags["stdin"] === true) {
		sources.push({ label: "--stdin", read: readStdinText, },);
	}
	if (sources.length === 0) {
		throw new UsageError(
			`Python source is required: pass --file PATH or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	if (sources.length > 1) {
		throw new UsageError(
			`Choose exactly one Python source: --file or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	const script = stripUtf8Bom(sources[0]!.read(),);
	if (script.trim().length === 0) {
		throw new UsageError(
			`Python source from ${sources[0]!.label} must not be empty. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	return script;
}

export const codeCommands: Record<string, CommandMeta> = {
	run: {
		handler: async (c, a, f,) => {
			const script = resolveCodeInput(a, f,);
			const run = await c.scenarios.runScript(script, {
				envName: f["env"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
				timeoutMs: num(f["timeout"],),
				keepScenario: f["keep"] === true,
				maxLogBytes: num(f["max-log-bytes"],),
			},);
			const result: Record<string, unknown> = {
				outcome: run.outcome,
				success: run.success,
				runId: run.runId,
				elapsedMs: run.elapsedMs,
				pollCount: run.pollCount,
				output: run.output ?? "",
				logTruncated: run.logTruncated,
				maxLogBytes: run.maxLogBytes,
			};
			if (f["full-log"] === true || run.output === undefined) {
				result.log = run.log;
			}
			return result;
		},
		usage: CODE_RUN_USAGE,
		description:
			"Run one-off Python in a DSS code env via a throwaway custom-python scenario; returns the script's captured output (stdout+stderr) plus outcome/success. Log retrieval is capped by --max-log-bytes (default 1048576); pass --full-log to include the capped raw DSS run log. Exits 4 on a non-SUCCESS outcome.",
		examples: [
			"dss code run --file inspect.py",
			"dss code run --file inspect.py --env py39_pandas",
			"cat snippet.py | dss code run --stdin",
			"dss code run --file inspect.py --full-log",
		],
	},
};
