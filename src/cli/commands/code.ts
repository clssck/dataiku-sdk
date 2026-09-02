import { readFileSync, } from "node:fs";
import { ScenarioScriptRunWithCleanupFailureError, } from "../../resources/scenarios.js";
import type {
	ScenarioScriptRunCleanupFailure,
	ScenarioScriptRunResult,
} from "../../resources/scenarios.js";
import { num, readStdinText, stripUtf8Bom, } from "../coerce.js";
import { enqueueCliWarning, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

const CODE_RUN_USAGE =
	"dss code run (--file PATH | --stdin) [--env ENV] [--timeout MS (default 120000)] [--keep] [--full-log] [--max-log-bytes N (default 1048576)] [--project-key KEY]";

export interface CodeInputSource {
	kind: "file" | "stdin";
	path?: string;
}

export interface ResolvedCodeInput {
	script: string;
	source: CodeInputSource;
}

function previewFlagValue(value: unknown,): string {
	const text = typeof value === "string" ? value : String(value,);
	return text.length <= 32 ? text : `${text.slice(0, 32,)}…`;
}

/** Read a local Python source file, mapping read failures to usage validation. */
function readCodeSourceFile(filePath: string,): string {
	try {
		return readFileSync(filePath, "utf-8",);
	} catch (error) {
		throw new UsageError(
			`Could not read --file ${filePath}: ${error instanceof Error ? error.message : String(error,)}.`,
			"not_found",
			"Verify the file path and that it is readable.",
		);
	}
}

/**
 * Canonical validation of `code run` input: rejects positional args and
 * conflicting sources, reads the chosen source, strips a UTF-8 BOM, and
 * rejects empty scripts. Returns the script plus source metadata so plan or
 * replay payloads can hash the script and reference the source without
 * embedding its contents.
 */
export function resolveCodeInputWithSource(
	args: string[],
	flags: Record<string, string | boolean>,
): ResolvedCodeInput {
	if (args.length > 0) {
		throw new UsageError(
			`code run takes no positional arguments; pass the script via --file PATH or --stdin. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	const sources: Array<{
		kind: "file" | "stdin";
		label: string;
		path?: string;
		read: () => string;
	}> = [];
	if (typeof flags["file"] === "string") {
		sources.push({
			kind: "file",
			label: "--file",
			path: flags["file"],
			read: () => readCodeSourceFile(flags["file"] as string,),
		},);
	}
	if (flags["stdin"] === true) {
		sources.push({ kind: "stdin", label: "--stdin", read: readStdinText, },);
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
	const source = sources[0]!;
	const script = stripUtf8Bom(source.read(),);
	if (script.trim().length === 0) {
		throw new UsageError(
			`Python source from ${source.label} must not be empty. Usage: ${CODE_RUN_USAGE}`,
		);
	}
	return {
		script,
		source: source.kind === "file" ? { kind: "file", path: source.path, } : { kind: "stdin", },
	};
}

export function resolveCodeInput(args: string[], flags: Record<string, string | boolean>,): string {
	return resolveCodeInputWithSource(args, flags,).script;
}

/** Parse an integer flag value, rejecting negative and fractional values before any DSS request. */
export function parseCodeRunIntegerFlag(
	value: string | boolean | undefined,
	flagName: string,
): number | undefined {
	const parsed = num(value, flagName,);
	if (parsed === undefined) return undefined;
	if (!Number.isInteger(parsed,) || parsed < 0) {
		const valuePreview = previewFlagValue(value,);
		throw new UsageError(
			`Invalid value for ${flagName}: expected a non-negative integer, got ${valuePreview}.`,
			"invalid_flag_value",
			undefined,
			{ flag: flagName, value: valuePreview, },
		);
	}
	return parsed;
}

function reportCleanupFailure(cleanupFailure: ScenarioScriptRunCleanupFailure,): void {
	enqueueCliWarning({
		type: "code_run_cleanup_failed",
		scenarioId: cleanupFailure.scenarioId,
		error: cleanupFailure.error,
	},);
}

function unwrapCodeRunError(error: unknown,): unknown {
	if (error instanceof ScenarioScriptRunWithCleanupFailureError) {
		reportCleanupFailure(error.cleanupFailure,);
		return error.cause;
	}
	return error;
}

export const codeCommands: Record<string, CommandMeta> = {
	run: {
		handler: async (c, a, f,) => {
			const timeoutMs = parseCodeRunIntegerFlag(f["timeout"], "--timeout",);
			const maxLogBytes = parseCodeRunIntegerFlag(f["max-log-bytes"], "--max-log-bytes",);
			const script = resolveCodeInput(a, f,);
			let run: ScenarioScriptRunResult;
			try {
				run = await c.scenarios.runScript(script, {
					envName: f["env"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
					timeoutMs,
					keepScenario: f["keep"] === true,
					maxLogBytes,
				},);
			} catch (error) {
				throw unwrapCodeRunError(error,);
			}
			const result: Record<string, unknown> = {
				outcome: run.outcome,
				success: run.success,
				runId: run.runId,
				elapsedMs: run.elapsedMs,
				pollCount: run.pollCount,
				output: run.output ?? "",
				logTruncated: run.logTruncated,
				maxLogBytes: run.maxLogBytes,
				cleanup: run.cleanup,
				...(run.timedOut === true ? { timedOut: true, timeoutMs: run.timeoutMs, } : {}),
			};
			if (f["full-log"] === true || run.output === undefined) {
				result.log = run.log;
			}
			if (run.cleanup.status === "failed") {
				reportCleanupFailure({ scenarioId: run.scenarioId, error: run.cleanup.error ?? "unknown", },);
			}
			return result;
		},
		usage: CODE_RUN_USAGE,
		description:
			"Run Python through a throwaway custom-python scenario and return captured output plus outcome. --timeout caps waiting; --max-log-bytes caps logs; --full-log returns the capped raw log. Exits 4 unless outcome is SUCCESS.",
		examples: [
			"dss code run --file inspect.py",
			"dss code run --file inspect.py --env py39_pandas",
			"cat snippet.py | dss code run --stdin",
			"dss code run --file inspect.py --full-log",
		],
	},
};
