import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const savedModelCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.savedModels.list(f["project-key"] as string | undefined,),
		usage: "dss saved-model list [--project-key KEY]",
		description: "List saved models in a project.",
		examples: ["dss saved-model list --project-key PROJECT",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss saved-model get <modelId> [--project-key KEY]",);
			return c.savedModels.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss saved-model get <modelId> [--project-key KEY]",
		description: "Get a saved model.",
		examples: ["dss saved-model get MODEL_ID --project-key PROJECT",],
	},
	"list-versions": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss saved-model list-versions <modelId> [--project-key KEY]",);
			return c.savedModels.listVersions(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss saved-model list-versions <modelId> [--project-key KEY]",
		description: "List versions of a saved model.",
		examples: ["dss saved-model list-versions MODEL_ID --project-key PROJECT",],
	},
	"version-details": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss saved-model version-details <modelId> <versionId> [--project-key KEY]",
			);
			return c.savedModels.versionDetails(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
			);
		},
		usage: "dss saved-model version-details <modelId> <versionId> [--project-key KEY]",
		description: "Get details for one saved-model version.",
		examples: [
			"dss saved-model version-details MODEL_ID VERSION_ID --project-key PROJECT",
		],
	},
	"set-active": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss saved-model set-active <modelId> <versionId> [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-active",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: { versionId: a[1], },
				};
			}
			return c.savedModels.setActiveVersion(a[0], a[1], projectKey,);
		},
		usage: "dss saved-model set-active <modelId> <versionId> [--dry-run] [--project-key KEY]",
		description: "Set the active version of a saved model.",
		examples: [
			"dss saved-model set-active MODEL_ID VERSION_ID --project-key PROJECT",
			"dss saved-model set-active MODEL_ID VERSION_ID --dry-run --project-key PROJECT",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss saved-model delete <modelId> [--if-exists] [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.savedModels.get(a[0], projectKey,));
				if (!current) return skipResult("saved-model", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "saved-model", id: a[0], current, };
				}
			}
			await c.savedModels.delete(a[0], projectKey,);
			return { deleted: a[0], resource: "saved-model", };
		},
		usage: "dss saved-model delete <modelId> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a saved model.",
		examples: [
			"dss saved-model delete MODEL_ID --if-exists --project-key PROJECT",
			"dss saved-model delete MODEL_ID --dry-run --project-key PROJECT",
		],
	},
};
