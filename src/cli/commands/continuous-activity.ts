import { jsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const continuousActivityCommands: Record<string, CommandMeta> = withUsage(
	"continuous-activity",
	{
		list: {
			handler: (c, _a, f,) => c.continuousActivities.list(f["project-key"] as string | undefined,),
			description: "List continuous activities in a project.",
			examples: ["dss continuous-activity list",],
		},
		status: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, commandUsage("continuous-activity", "status",),);
				return c.continuousActivities.getStatus(a[0], f["project-key"] as string | undefined,);
			},
			description: "Get a continuous recipe's desired and effective state.",
			examples: ["dss continuous-activity status compute_stream",],
		},
		start: {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					commandUsage("continuous-activity", "start",),
				);
				return c.continuousActivities.start(
					a[0],
					jsonInput(f,),
					f["project-key"] as string | undefined,
				);
			},
			description: "Start a continuous recipe (optional loop restart params via JSON).",
			examples: ["dss continuous-activity start compute_stream",],
		},
		stop: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, commandUsage("continuous-activity", "stop",),);
				await c.continuousActivities.stop(a[0], f["project-key"] as string | undefined,);
				return { stopped: a[0], };
			},
			description: "Stop a continuous recipe.",
			examples: ["dss continuous-activity stop compute_stream",],
		},
	},
);
