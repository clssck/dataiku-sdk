import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const PLAN_FAILURE_CODES = { usage: 1, error: 2, transient: 3, };

function collectionEndpoint(dataCollectionId: string, suffix = "",): string {
	return `/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}${suffix}`;
}

export const dataCollectionCommands: Record<string, CommandMeta> = withUsage("data-collection", {
	list: {
		handler: (c,) => c.dataCollections.list(),
		description: "List data collections on which the API key has READ privilege.",
		examples: ["dss data-collection list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("data-collection", "get",),);
			return c.dataCollections.get(a[0],);
		},
		description: "Get collection settings. Permissions are omitted for non-admins.",
		examples: ["dss data-collection get OjVsTQ3O",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (collection definition).",
			);
			if (executionMode(f,).dryRun) {
				return planResult("data-collection", "create", {
					asyncKind: "none",
					method: "POST",
					endpoint: "/public/api/data-collections/",
					payload: body,
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.dataCollections.create(
				body as Parameters<typeof c.dataCollections.create>[0],
			);
		},
		description: "Create a collection (creator becomes admin; requires mayCreateDataCollections).",
		examples: [
			'dss data-collection create --data \'{"displayName":"My Collection"}\'',
		],
	},
	"settings-set": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("data-collection", "settings-set",),
			);
			const settings = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (collection settings).",
			);
			if (executionMode(f,).dryRun) {
				return planResult("data-collection", "settings-set", {
					asyncKind: "none",
					method: "PUT",
					endpoint: collectionEndpoint(a[0],),
					identifiers: { dataCollectionId: a[0], },
					payload: settings,
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			const updated = await c.dataCollections.update(
				a[0],
				settings as Parameters<typeof c.dataCollections.update>[1],
			);
			return { updated: a[0], resource: "data-collection", ...updated, };
		},
		description:
			"Update collection settings; only send settings obtained through get. Admin required.",
		examples: [
			"dss data-collection settings-set OjVsTQ3O --data-file settings.json",
		],
	},
	delete: {
		validate: (a,) => {
			const usage = commandUsage("data-collection", "delete",);
			requireArgs(a, 1, usage,);
			if (a[0].trim().length === 0) {
				throw new UsageError(
					`<dataCollectionId> must be a non-empty identifier. Usage: ${usage}`,
				);
			}
		},
		handler: async (c, a, f,) => {
			if (executionMode(f,).dryRun) {
				return planResult("data-collection", "delete", {
					asyncKind: "none",
					method: "DELETE",
					endpoint: collectionEndpoint(a[0],),
					identifiers: { dataCollectionId: a[0], },
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.dataCollections.delete(a[0],);
			return { deleted: a[0], resource: "data-collection", };
		},
		description: "Permanently delete a collection. Admin required.",
		examples: ["dss data-collection delete OjVsTQ3O --dry-run",],
	},
	"list-objects": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("data-collection", "list-objects",),);
			return c.dataCollections.listObjects(a[0],);
		},
		description: "List the objects in a collection.",
		examples: ["dss data-collection list-objects OjVsTQ3O",],
	},
	"add-object": {
		handler: async (c, a, f,) => {
			const object = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (collection object reference).",
			);
			const collectionId = a[0]
				?? (typeof object.dataCollectionId === "string" ? object.dataCollectionId : undefined);
			if (!collectionId) {
				throw new UsageError(
					`Usage: ${commandUsage("data-collection", "add-object",)}`,
				);
			}
			const reference = { ...object, };
			delete (reference as Record<string, unknown>).dataCollectionId;
			if (executionMode(f,).dryRun) {
				return planResult("data-collection", "add-object", {
					asyncKind: "none",
					method: "POST",
					endpoint: `${collectionEndpoint(collectionId,)}/objects`,
					identifiers: { dataCollectionId: collectionId, },
					payload: reference,
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.dataCollections.addObject(
				collectionId,
				reference as Parameters<typeof c.dataCollections.addObject>[1],
			);
			return { added: reference.id, dataCollectionId: collectionId, resource: "data-collection", };
		},
		description:
			"Add an object (e.g. {type:'DATASET',projectKey:'K',id:'name'}) to a collection. Contributor required.",
		examples: [
			'dss data-collection add-object OjVsTQ3O --data \'{"type":"DATASET","projectKey":"MYPROJECT","id":"orders"}\'',
		],
	},
	"remove-dataset": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				3,
				commandUsage("data-collection", "remove-dataset",),
			);
			if (executionMode(f,).dryRun) {
				return planResult("data-collection", "remove-dataset", {
					asyncKind: "none",
					method: "DELETE",
					endpoint: `${collectionEndpoint(a[0],)}/objects/dataset/${encodeURIComponent(a[1],)}/${
						encodeURIComponent(a[2],)
					}`,
					identifiers: {
						dataCollectionId: a[0],
						projectKey: a[1],
						datasetName: a[2],
					},
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.dataCollections.removeDataset(a[0], a[1], a[2],);
			return { removed: a[2], projectKey: a[1], dataCollectionId: a[0], resource: "data-collection", };
		},
		description: "Remove a dataset from a collection. Contributor required.",
		examples: ["dss data-collection remove-dataset OjVsTQ3O MYPROJECT DATASET1",],
	},
},);
