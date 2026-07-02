import { jsonInput, requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const meaningCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.meanings.list(),
		usage: "dss meaning list",
		description: "List user-defined meanings (column semantic types) on the instance.",
		examples: ["dss meaning list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss meaning get <meaningId>",);
			return c.meanings.get(a[0],);
		},
		usage: "dss meaning get <meaningId>",
		description: "Get a user-defined meaning definition.",
		examples: ["dss meaning get customer_type",],
	},
	create: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				"dss meaning create <id> <label> <type> [--data JSON|--data-file PATH|--stdin]",
			);
			return c.meanings.create(a[0], a[1], a[2], jsonInput(f,) ?? {},);
		},
		usage: "dss meaning create <id> <label> <type> [--data JSON|--data-file PATH|--stdin]",
		description: "Create a user-defined meaning (type e.g. VALUES_LIST, VALUES_MAPPING, PATTERN).",
		examples: ["dss meaning create vip VIP VALUES_LIST",],
	},
	update: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss meaning update <meaningId> (--data JSON|--data-file PATH|--stdin)",);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (meaning definition).",
			);
			return c.meanings.update(a[0], body,);
		},
		usage: "dss meaning update <meaningId> (--data JSON|--data-file PATH|--stdin)",
		description: "Replace a user-defined meaning definition.",
		examples: ["dss meaning update vip --data-file meaning.json",],
	},
};
