import { requiredJsonInput, requiredStringFlag, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const analysisCommands: Record<string, CommandMeta> = withUsage("analysis", {
	list: {
		handler: (c, _a, f,) => c.analyses.list(f["project-key"] as string | undefined,),
		description: "List Visual ML analyses in a project.",
		examples: ["dss analysis list --project-key PROJECT",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("analysis", "get",),);
			return c.analyses.get(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a Visual ML analysis.",
		examples: ["dss analysis get ANALYSIS_ID --project-key PROJECT",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("analysis", "create",);
			const inputDataset = requiredStringFlag(f, "input-dataset", usage,);
			const projectKey = f["project-key"] as string | undefined;
			const options = { inputDataset, projectKey, };
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "create",
					resource: "analysis",
					payload: options,
				};
			}
			const created = await c.analyses.create(options,);
			return { created: created.id, resource: "analysis", ...created, };
		},
		description: "Create a Visual ML analysis for an input dataset.",
		examples: [
			"dss analysis create --input-dataset customers --project-key PROJECT",
			"dss analysis create --input-dataset customers --dry-run --project-key PROJECT",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("analysis", "delete",),
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.analyses.get(a[0], projectKey,));
				if (!current) return skipResult("analysis", a[0], "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "analysis", id: a[0], current, };
				}
			}
			await c.analyses.delete(a[0], projectKey,);
			return { deleted: a[0], resource: "analysis", };
		},
		description: "Delete a Visual ML analysis.",
		examples: [
			"dss analysis delete ANALYSIS_ID --if-exists --project-key PROJECT",
			"dss analysis delete ANALYSIS_ID --dry-run --project-key PROJECT",
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("analysis", "update",),);
			const definition = requiredJsonInput(
				f,
				"The analysis definition is required via --data, --data-file, or --stdin (dss analysis get, edited).",
			);
			await c.analyses.update(a[0]!, definition, f["project-key"] as string | undefined,);
			return { updated: a[0], resource: "analysis", };
		},
		description:
			"Replace a visual analysis definition (name, script steps, charts, tags) with the object from dss analysis get, edited.",
		examples: ["dss analysis update ANALYSIS_ID --data-file analysis.json",],
	},
	"list-ml-tasks": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("analysis", "list-ml-tasks",),);
			return c.analyses.listMlTasks(a[0]!, f["project-key"] as string | undefined,);
		},
		description: "List the ML tasks of one visual analysis (whole project: dss ml-task list).",
		examples: ["dss analysis list-ml-tasks ANALYSIS_ID",],
	},
},);
