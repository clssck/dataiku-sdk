import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, textInput, } from "../coerce.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const wikiCommands: Record<string, CommandMeta> = {
	settings: {
		handler: (c, _a, f,) => c.wiki.settings(f["project-key"] as string | undefined,),
		usage: "dss wiki settings [--project-key KEY]",
		description: "Get project wiki settings and taxonomy.",
		examples: ["dss wiki settings",],
	},
	list: {
		handler: (c, _a, f,) => c.wiki.list(f["project-key"] as string | undefined,),
		usage: "dss wiki list [--project-key KEY]",
		description: "List wiki articles by walking the taxonomy.",
		examples: ["dss wiki list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss wiki get <id-or-name>",);
			return c.wiki.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss wiki get <id-or-name> [--project-key KEY]",
		description: "Get a wiki article including markdown body.",
		examples: ["dss wiki get ARTICLE_ID",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const name = f["name"] as string | undefined;
			if (!name) throw new UsageError("--name is required. Usage: dss wiki create --name NAME",);
			const content = textInput(f,);
			const pk = f["project-key"] as string | undefined;
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = (await c.wiki.list(pk,)).find((article,) => article.article.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("wiki", existing.article.id, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "wiki",
						name,
						payload: {
							name,
							parent: f["parent"] as string | undefined,
							content,
						},
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.wiki.create({
				name,
				parent: f["parent"] as string | undefined,
				content,
				projectKey: pk,
			},);
			return { created: created.article.id, resource: "wiki", ...created, };
		},
		usage:
			"dss wiki create --name NAME [--parent ID] [--content TEXT|--file PATH] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a wiki article, optionally with markdown content.",
		examples: [
			"dss wiki create --name 'Agent notes' --content '# Notes'",
			"dss wiki create --name 'Agent notes' --file article.md --dry-run",
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss wiki update <id-or-name> [--name NAME] [--content TEXT|--file PATH|--data JSON]",
			);
			const data = jsonInput(f,);
			const content = textInput(f,);
			const name = f["name"] as string | undefined;
			if (!data && content === undefined && name === undefined) {
				throw new UsageError(
					"--name, --content, --file, --data, --data-file, or --stdin is required.",
				);
			}
			if (f["dry-run"] === true) {
				const current = await c.wiki.get(a[0], f["project-key"] as string | undefined,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
				if (name !== undefined) {
					next.article = {
						...((next.article && typeof next.article === "object" && !Array.isArray(next.article,))
							? next.article as Record<string, unknown>
							: {}),
						name,
					};
				}
				if (content !== undefined) next.payload = content;
				return { dryRun: true, action: "update", resource: "wiki", article: a[0], current, next, };
			}
			return c.wiki.update(a[0], {
				name,
				content,
				data,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss wiki update <id-or-name> (--name NAME | --content TEXT|--file PATH|--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Update wiki article metadata/body via merge.",
		examples: ["dss wiki update ARTICLE_ID --content '# Updated' --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss wiki delete <id-or-name>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.wiki.get(a[0], pk,));
				if (!current) return skipResult("wiki", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "wiki", article: a[0], current, };
				}
			}
			await c.wiki.delete(a[0], pk,);
			return { deleted: a[0], resource: "wiki", };
		},
		usage: "dss wiki delete <id-or-name> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a wiki article.",
		examples: ["dss wiki delete ARTICLE_ID --dry-run",],
	},
};
