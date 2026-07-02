import { jsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const continuousActivityCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.continuousActivities.list(f["project-key"] as string | undefined,),
		usage: "dss continuous-activity list [--project-key KEY]",
		description: "List continuous activities in a project.",
		examples: ["dss continuous-activity list",],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss continuous-activity status <recipeId> [--project-key KEY]",);
			return c.continuousActivities.getStatus(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss continuous-activity status <recipeId> [--project-key KEY]",
		description: "Get a continuous recipe's desired and effective state.",
		examples: ["dss continuous-activity status compute_stream",],
	},
	start: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss continuous-activity start <recipeId> [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
			);
			return c.continuousActivities.start(
				a[0],
				jsonInput(f,),
				f["project-key"] as string | undefined,
			);
		},
		usage:
			"dss continuous-activity start <recipeId> [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
		description: "Start a continuous recipe (optional loop restart params via JSON).",
		examples: ["dss continuous-activity start compute_stream",],
	},
	stop: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss continuous-activity stop <recipeId> [--project-key KEY]",);
			await c.continuousActivities.stop(a[0], f["project-key"] as string | undefined,);
			return { stopped: a[0], };
		},
		usage: "dss continuous-activity stop <recipeId> [--project-key KEY]",
		description: "Stop a continuous recipe.",
		examples: ["dss continuous-activity stop compute_stream",],
	},
};
