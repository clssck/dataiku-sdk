import { num, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const futureCommands: Record<string, CommandMeta> = withUsage("future", {
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("future", "get",),);
			return c.futures.get(a[0],);
		},
		description: "Get a DSS future state and retrieve the result if ready.",
		examples: ["dss future get FUTURE_ID",],
	},
	peek: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("future", "peek",),);
			return c.futures.peek(a[0],);
		},
		description: "Peek at a DSS future state without consuming its result.",
		examples: ["dss future peek FUTURE_ID",],
	},
	wait: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("future", "wait",),);
			return c.futures.wait(a[0], {
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
			},);
		},
		description: "Wait for a DSS future to finish.",
		examples: ["dss future wait FUTURE_ID --timeout 60000",],
	},
	abort: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("future", "abort",),);
			const id = a[0];
			if (executionMode(f,).dryRun) {
				return planResult("future", "abort", {
					method: "DELETE",
					endpoint: `/public/api/futures/${encodeURIComponent(id,)}`,
					identifiers: { id, },
					idempotency: "none",
					asyncKind: "future",
					exitCodesOnFailure: { usage: 1, error: 2, transient: 3, longRunningFailure: 4, },
					plannedAndDryRun: true,
				},);
			}
			await c.futures.abort(id,);
			return { aborted: id, resource: "future", };
		},
		description: "Abort a DSS future.",
		examples: ["dss future abort FUTURE_ID --dry-run",],
	},
},);
