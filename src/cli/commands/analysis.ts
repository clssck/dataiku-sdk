import { requiredStringFlag, } from "../coerce.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const analysisCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.analyses.list(f["project-key"] as string | undefined,),
		usage: "dss analysis list [--project-key KEY]",
		description: "List Visual ML analyses in a project.",
		examples: ["dss analysis list --project-key PROJECT",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss analysis get <analysisId> [--project-key KEY]",);
			return c.analyses.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss analysis get <analysisId> [--project-key KEY]",
		description: "Get a Visual ML analysis.",
		examples: ["dss analysis get ANALYSIS_ID --project-key PROJECT",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = "dss analysis create --input-dataset DS [--dry-run] [--project-key KEY]";
			const inputDataset = requiredStringFlag(f, "input-dataset", usage,);
			const projectKey = f["project-key"] as string | undefined;
			const options = { inputDataset, projectKey, };
			if (f["dry-run"] === true) {
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
		usage: "dss analysis create --input-dataset DS [--dry-run] [--project-key KEY]",
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
				"dss analysis delete <analysisId> [--if-exists] [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.analyses.get(a[0], projectKey,));
				if (!current) return skipResult("analysis", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "analysis", id: a[0], current, };
				}
			}
			await c.analyses.delete(a[0], projectKey,);
			return { deleted: a[0], resource: "analysis", };
		},
		usage: "dss analysis delete <analysisId> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a Visual ML analysis.",
		examples: [
			"dss analysis delete ANALYSIS_ID --if-exists --project-key PROJECT",
			"dss analysis delete ANALYSIS_ID --dry-run --project-key PROJECT",
		],
	},
};
