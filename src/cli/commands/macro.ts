import type { MacroRunOptions, } from "../../resources/macros.js";
import { jsonInput, num, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/**
 * Flags accepted by run/run-and-wait; validated identically for live, dry-run,
 * and --plan so a plan that would fail cannot be "fixed" by dropping flags.
 * Returns undefined rather than a partial options object so callers can spread
 * it only when the macro actually has parameters.
 */
function macroWaitOptionsFromFlags(
	flags: Record<string, string | boolean>,
): { timeoutMs?: number; pollIntervalMs?: number; } {
	const timeoutMs = num(flags["timeout"], "--timeout",);
	const pollIntervalMs = num(flags["poll-interval"], "--poll-interval",);
	if (timeoutMs !== undefined && timeoutMs < 0) {
		throw new UsageError("--timeout must be >= 0.", "invalid_flag_value", undefined, {
			flag: "--timeout",
		},);
	}
	if (pollIntervalMs !== undefined && pollIntervalMs < 1) {
		throw new UsageError(
			"--poll-interval must be >= 1.",
			"invalid_flag_value",
			undefined,
			{ flag: "--poll-interval", },
		);
	}
	return { timeoutMs, pollIntervalMs, };
}

/**
 * Macro parameter payload from --data/--data-file/--stdin. The JSON object is
 * passed through as the POST body: `{ "params": {...}, "adminParams": {...} }`.
 * Values may carry credentials; the plan redacts nothing beyond DSS's own
 * response echo, so callers must not log payloads.
 */
function macroRunParamsFromFlags(
	flags: Record<string, string | boolean>,
): Pick<MacroRunOptions, "params" | "adminParams"> {
	const input = jsonInput(flags,);
	if (input === undefined) return {};
	const body: Pick<MacroRunOptions, "params" | "adminParams"> = {};
	if (input["params"] !== undefined) {
		if (input["params"] === null || typeof input["params"] !== "object") {
			throw new UsageError(
				"Macro run payload params must be an object keyed by macro parameter names.",
				"invalid_flag_value",
				undefined,
				{ flag: "--data", },
			);
		}
		body.params = input["params"] as Record<string, unknown>;
	}
	if (input["adminParams"] !== undefined) {
		if (input["adminParams"] === null || typeof input["adminParams"] !== "object") {
			throw new UsageError(
				"Macro run payload adminParams must be an object.",
				"invalid_flag_value",
				undefined,
				{ flag: "--data", },
			);
		}
		body.adminParams = input["adminParams"] as Record<string, unknown>;
	}
	return body;
}

/** Exact POST body a run will send; empty object when no parameters. */
function macroRunDryRunPayload(
	runParams: Pick<MacroRunOptions, "params" | "adminParams">,
): Record<string, unknown> {
	const payload: Record<string, unknown> = {};
	if (runParams.params !== undefined) payload["params"] = runParams.params;
	if (runParams.adminParams !== undefined) payload["adminParams"] = runParams.adminParams;
	return payload;
}

/**
 * Reject blank/whitespace positional ids as a usage error BEFORE any request:
 * an empty macro/run id would hit the list route or an unresolvable path.
 */
function requireNotBlankId(value: string, usage: string,): void {
	if (value.trim().length === 0) {
		throw new UsageError(`Macro/run id must be non-empty. Usage: ${usage}`,);
	}
}

export const macroCommands: Record<string, CommandMeta> = withUsage("macro", {
	list: {
		handler: (c, _a, f,) => c.macros.list(f["project-key"] as string | undefined,),
		description: "List the macros (runnables) available in a project.",
		examples: ["dss macro list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("macro", "get",),);
			requireNotBlankId(a[0], "dss macro get <macro_id>",);
			return c.macros.definition(a[0], {
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get a macro's definition: parameters, result type, owning plugin, labels.",
		examples: ["dss macro get compute_orders",],
	},
	run: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("macro", "run",),);
			const pk = f["project-key"] as string | undefined;
			const waitOptions = macroWaitOptionsFromFlags(f,);
			const runParams = macroRunParamsFromFlags(f,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "run",
					resource: "macro",
					id: a[0],
					...waitOptions,
					...(runParams.params !== undefined ? { params: runParams.params, } : {}),
					...(runParams.adminParams !== undefined
						? { adminParams: runParams.adminParams, }
						: {}),
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/runnables/${encodeURIComponent(a[0],)}?wait=false`,
					),
					method: "POST",
					payload: macroRunDryRunPayload(runParams,),
					warning:
						"Macro runs execute arbitrary plugin code on the DSS backend; review the macro definition before running.",
				};
			}
			if (f["wait"] === true) {
				return c.macros.runAndWait(a[0], {
					...waitOptions,
					...runParams,
					projectKey: pk,
				},);
			}
			return c.macros.run(a[0], { ...runParams, projectKey: pk, },);
		},
		description:
			"Start a macro run. Macros execute arbitrary plugin code and are classified destructive. --wait polls to completion and exits 4 on failure; without it only the runId is returned. --data JSON may carry { params: {...}, adminParams: {...} } for parameterized macros.",
		examples: [
			"dss macro run compute_orders",
			"dss macro run compute_orders --wait",
			'dss macro run compute_orders --data \'{"params":{"region":"EU"}}\'',
			"dss macro run compute_orders --wait --timeout 300000",
		],
	},
	"run-and-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("macro", "run-and-wait",),);
			const pk = f["project-key"] as string | undefined;
			const waitOptions = macroWaitOptionsFromFlags(f,);
			const runParams = macroRunParamsFromFlags(f,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "run-and-wait",
					resource: "macro",
					id: a[0],
					...waitOptions,
					...(runParams.params !== undefined ? { params: runParams.params, } : {}),
					...(runParams.adminParams !== undefined
						? { adminParams: runParams.adminParams, }
						: {}),
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/runnables/${encodeURIComponent(a[0],)}?wait=false`,
					),
					method: "POST",
					payload: macroRunDryRunPayload(runParams,),
					warning:
						"Macro runs execute arbitrary plugin code on the DSS backend; review the macro definition before running.",
				};
			}
			return c.macros.runAndWait(a[0], {
				...waitOptions,
				...runParams,
				projectKey: pk,
			},);
		},
		description:
			"Run a macro and poll its state until it finishes. Exits 4 on failure; returns { success: false, timedOut: true } on timeout.",
		examples: [
			"dss macro run-and-wait compute_orders",
			"dss macro run-and-wait compute_orders --timeout 60000 --poll-interval 500",
		],
	},
	abort: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("macro", "abort",),);
			requireNotBlankId(a[0], "dss macro abort <macro_id> <run_id>",);
			requireNotBlankId(a[1], "dss macro abort <macro_id> <run_id>",);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "abort",
					resource: "macro",
					id: a[0],
					runId: a[1],
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/runnables/${encodeURIComponent(a[0],)}/abort/${encodeURIComponent(a[1],)}`,
					),
					method: "POST",
				};
			}
			await c.macros.abort(a[0], a[1], pk,);
			return { aborted: a[1], runId: a[1], resource: "macro", };
		},
		description: "Request abort of a running macro run.",
		examples: ["dss macro abort compute_orders run_42",],
	},
	state: {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("macro", "state",),);
			requireNotBlankId(a[0], "dss macro state <macro_id> <run_id>",);
			requireNotBlankId(a[1], "dss macro state <macro_id> <run_id>",);
			return c.macros.state(a[0], a[1], {
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get the poll state of a macro run: running flag, progress stack, failure details.",
		examples: ["dss macro state compute_orders run_42",],
	},
	result: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("macro", "result",),);
			requireNotBlankId(a[0], "dss macro result <macro_id> <run_id>",);
			requireNotBlankId(a[1], "dss macro result <macro_id> <run_id>",);
			const pk = f["project-key"] as string | undefined;
			const maxBytes = num(f["max-bytes"], "--max-bytes",);
			if (maxBytes !== undefined && (maxBytes < 1 || !Number.isInteger(maxBytes,))) {
				throw new UsageError(
					"--max-bytes must be a positive integer.",
					"invalid_flag_value",
					undefined,
					{ flag: "--max-bytes", },
				);
			}
			return c.macros.result(a[0], a[1], {
				projectKey: pk,
				maxBytes: (require("../../resources/macros.js",) as typeof import("../../resources/macros.js"))
					.macroResultMaxBytes(maxBytes,),
			},);
		},
		description:
			"Download a macro run's result. Text/HTML bodies print as a JSON string (stdout stays one JSON document, embedded newlines escaped); JSON bodies print as the parsed object.",
		examples: ["dss macro result compute_orders run_42", "dss macro result m r --max-bytes 1024",],
	},
},);
