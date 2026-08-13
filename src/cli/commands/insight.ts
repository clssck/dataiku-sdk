import { deepMerge, } from "../../utils/deep-merge.js";
import { json, jsonInput, parseBooleanOption, textInput, } from "../coerce.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const insightCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.insights.list(f["project-key"] as string | undefined,),
		usage: "dss insight list [--project-key KEY]",
		description: "List project insights.",
		examples: ["dss insight list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss insight get <id>",);
			return c.insights.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss insight get <id> [--project-key KEY]",
		description: "Get insight definition.",
		examples: ["dss insight get INSIGHT_ID",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const data = jsonInput(f,);
			const name = f["name"] as string | undefined;
			const type = f["type"] as string | undefined;
			const params = json(f["params"],);
			const listed = parseBooleanOption(f["listed"], "--listed",);
			const contentType = f["content-type"] as string | undefined;
			const payload = textInput(f,);
			if (!data && (!name || !type)) {
				throw new UsageError(
					"--data or both --name and --type are required. Usage: dss insight create --name NAME --type TYPE",
				);
			}
			const prototype: Record<string, unknown> = { ...data, };
			if (name !== undefined) prototype.name = name;
			if (type !== undefined) prototype.type = type;
			if (listed !== undefined) prototype.listed = listed;
			if (params !== undefined) prototype.params = params;
			const pk = f["project-key"] as string | undefined;
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = name
					? (await c.insights.list(pk,)).find((insight,) => insight.name === name)
					: undefined;
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("insight", existing.id, "exists", { current: existing, },);
				}
				if (f["if-not-exists"] === true && !name && f["dry-run"] !== true) {
					throw new UsageError("--if-not-exists requires --name for insight create.",);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "insight",
						name,
						payload: prototype,
						contentType,
						content: payload,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.insights.create({
				data,
				name,
				type,
				listed,
				params,
				contentType,
				payload,
				projectKey: pk,
			},);
			return { created: created.id, resource: "insight", ...created, };
		},
		usage:
			"dss insight create (--data JSON|--data-file PATH|--stdin | --name NAME --type TYPE [--params JSON] [--listed true|false]) [--content TEXT|--file PATH --content-type MIME] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create an insight from a raw prototype or minimal name/type fields.",
		examples: [
			"dss insight create --name 'Agent chart' --type chart --params '{\"dataset\":\"orders\"}' --dry-run",
			"dss insight create --data-file insight-prototype.json --dry-run",
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss insight update <id> (--name NAME|--listed true|false|--params JSON|--content TEXT|--file PATH|--content-type MIME|--data JSON|--data-file PATH|--stdin)",
			);
			const data = jsonInput(f,);
			const name = f["name"] as string | undefined;
			const params = json(f["params"],);
			const listed = parseBooleanOption(f["listed"], "--listed",);
			const contentType = f["content-type"] as string | undefined;
			const payload = textInput(f,);
			if (
				!data && name === undefined && params === undefined && listed === undefined
				&& contentType === undefined && payload === undefined
			) {
				throw new UsageError(
					"--name, --listed, --params, --content, --file, --data, --data-file, or --stdin is required.",
				);
			}
			if (f["dry-run"] === true) {
				const current = await c.insights.get(a[0], f["project-key"] as string | undefined,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
				if (name !== undefined) next.name = name;
				if (listed !== undefined) next.listed = listed;
				if (params !== undefined) {
					const currentParams = next.params;
					next.params =
						currentParams && typeof currentParams === "object" && !Array.isArray(currentParams,)
							? deepMerge(currentParams as Record<string, unknown>, params,)
							: params;
				}
				return {
					dryRun: true,
					action: "update",
					resource: "insight",
					id: a[0],
					current,
					next,
					contentType,
					payload,
				};
			}
			return c.insights.update(a[0], {
				data,
				name,
				listed,
				params,
				contentType,
				payload,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss insight update <id> (--name NAME|--listed true|false|--params JSON|--content TEXT|--file PATH|--content-type MIME|--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Update an insight using GET-before-POST merge semantics.",
		examples: ["dss insight update INSIGHT_ID --name 'Updated' --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss insight delete <id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.insights.get(a[0], pk,));
				if (!current) return skipResult("insight", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "insight", id: a[0], current, };
				}
			}
			await c.insights.delete(a[0], pk,);
			return { deleted: a[0], resource: "insight", };
		},
		usage: "dss insight delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete an insight.",
		examples: ["dss insight delete INSIGHT_ID --dry-run",],
	},
};
