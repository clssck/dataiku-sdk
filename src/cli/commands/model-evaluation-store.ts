import { requiredStringFlag, } from "../coerce.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const modelEvaluationStoreCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.modelEvaluationStores.list(f["project-key"] as string | undefined,),
		usage: "dss model-evaluation-store list [--project-key KEY]",
		description: "List model evaluation stores in a project.",
		examples: ["dss model-evaluation-store list --project-key PROJECT",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss model-evaluation-store get <storeId> [--project-key KEY]",
			);
			return c.modelEvaluationStores.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss model-evaluation-store get <storeId> [--project-key KEY]",
		description: "Get a model evaluation store.",
		examples: ["dss model-evaluation-store get STORE_ID --project-key PROJECT",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = "dss model-evaluation-store create --name NAME [--dry-run] [--project-key KEY]";
			const name = requiredStringFlag(f, "name", usage,);
			const projectKey = f["project-key"] as string | undefined;
			const options = { name, projectKey, };
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "create",
					resource: "model-evaluation-store",
					payload: options,
				};
			}
			const created = await c.modelEvaluationStores.create(options,);
			return { created: created.id, resource: "model-evaluation-store", ...created, };
		},
		usage: "dss model-evaluation-store create --name NAME [--dry-run] [--project-key KEY]",
		description: "Create an empty model evaluation store.",
		examples: [
			"dss model-evaluation-store create --name production-evaluations --project-key PROJECT",
			"dss model-evaluation-store create --name production-evaluations --dry-run --project-key PROJECT",
		],
	},
	"list-evaluations": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss model-evaluation-store list-evaluations <storeId> [--project-key KEY]",
			);
			return c.modelEvaluationStores.listEvaluations(
				a[0],
				f["project-key"] as string | undefined,
			);
		},
		usage: "dss model-evaluation-store list-evaluations <storeId> [--project-key KEY]",
		description: "List evaluations in a model evaluation store.",
		examples: [
			"dss model-evaluation-store list-evaluations STORE_ID --project-key PROJECT",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss model-evaluation-store delete <storeId> [--if-exists] [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.modelEvaluationStores.get(a[0], projectKey,));
				if (!current) return skipResult("model-evaluation-store", a[0], "missing",);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "delete",
						resource: "model-evaluation-store",
						id: a[0],
						current,
					};
				}
			}
			await c.modelEvaluationStores.delete(a[0], projectKey,);
			return { deleted: a[0], resource: "model-evaluation-store", };
		},
		usage:
			"dss model-evaluation-store delete <storeId> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a model evaluation store.",
		examples: [
			"dss model-evaluation-store delete STORE_ID --if-exists --project-key PROJECT",
			"dss model-evaluation-store delete STORE_ID --dry-run --project-key PROJECT",
		],
	},
};
