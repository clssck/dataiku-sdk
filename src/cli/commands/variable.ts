import { json, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

export const variableCommands: Record<string, CommandMeta> = {
	get: {
		handler: (c, _a, f,) => c.variables.get(f["project-key"] as string | undefined,),
		usage: "dss variable get [--project-key KEY]",
		description: "Get project variables (standard and local).",
		examples: ["dss variable get", "dss variable get --project-key MYPROJ",],
	},
	set: {
		handler: async (c, _a, f,) => {
			const standard = json(f["standard"],);
			const local = json(f["local"],);
			const pk = f["project-key"] as string | undefined;
			if (standard === undefined && local === undefined) {
				throw new UsageError("--standard and/or --local is required.",);
			}
			if (f["dry-run"] === true) {
				const current = await c.variables.get(pk,);
				const next = f["replace"] === true
					? { standard: standard ?? {}, local: local ?? {}, }
					: {
						standard: { ...current.standard, ...standard, },
						local: { ...current.local, ...local, },
					};
				return {
					dryRun: true,
					action: "set",
					resource: "variable",
					projectKey: pk,
					current,
					next,
				};
			}
			return c.variables.set({
				standard,
				local,
				replace: f["replace"] === true,
				projectKey: pk,
			},);
		},
		usage:
			`dss variable set (--standard '{"k":"v"}'|--local '{"k":"v"}') [--replace] [--dry-run] [--project-key KEY]`,
		description: "Set project variables via JSON merge (or full replace with --replace).",
		examples: [
			'dss variable set --standard \'{"env":"staging"}\' --dry-run',
			"dss variable set --local '{\"debug\":true}' --replace",
		],
	},
};
