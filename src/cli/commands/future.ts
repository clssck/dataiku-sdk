import { num, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const futureCommands: Record<string, CommandMeta> = {
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss future get <id>",);
			return c.futures.get(a[0],);
		},
		usage: "dss future get <id>",
		description: "Get a DSS future state and retrieve the result if ready.",
		examples: ["dss future get FUTURE_ID",],
	},
	peek: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss future peek <id>",);
			return c.futures.peek(a[0],);
		},
		usage: "dss future peek <id>",
		description: "Peek at a DSS future state without consuming its result.",
		examples: ["dss future peek FUTURE_ID",],
	},
	wait: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss future wait <id>",);
			return c.futures.wait(a[0], {
				pollIntervalMs: num(f["poll-interval"],),
				timeoutMs: num(f["timeout"],),
			},);
		},
		usage: "dss future wait <id> [--timeout MS] [--poll-interval MS]",
		description: "Wait for a DSS future to finish.",
		examples: ["dss future wait FUTURE_ID --timeout 60000",],
	},
	abort: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss future abort <id>",);
			if (f["dry-run"] === true) {
				const current = await c.futures.peek(a[0],);
				return { dryRun: true, action: "abort", resource: "future", id: a[0], current, };
			}
			await c.futures.abort(a[0],);
			return { aborted: a[0], resource: "future", };
		},
		usage: "dss future abort <id> [--dry-run]",
		description: "Abort a DSS future.",
		examples: ["dss future abort FUTURE_ID --dry-run",],
	},
};
