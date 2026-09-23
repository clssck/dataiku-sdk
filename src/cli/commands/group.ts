import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/** Group identifiers must be non-empty before any request or dry-run preview. */
function requireGroupName(value: string | undefined, usage: string,): string {
	const trimmed = value?.trim() ?? "";
	if (trimmed.length === 0) {
		throw new UsageError(
			`group name must be a non-empty string.\nUsage: ${usage}`,
			"validation_failed",
		);
	}
	return trimmed;
}

export const groupCommands: Record<string, CommandMeta> = withUsage("group", {
	list: {
		handler: (c,) => c.groups.list(),
		description: "List DSS groups (admin).",
		examples: ["dss group list",],
	},
	get: {
		handler: (c, a,) => {
			const usage = commandUsage("group", "get",);
			requireArgs(a, 1, usage,);
			return c.groups.get(requireGroupName(a[0], usage,),);
		},
		description: "Get a DSS group (admin).",
		examples: ["dss group get administrators",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("group", "create",);
			const body = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (group definition). Usage: ${usage}`,
			);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "create",
					resource: "group",
					name: typeof body.name === "string" ? body.name : undefined,
					method: "POST",
					endpoint: "/public/api/admin/groups",
					payload: body,
				};
			}
			return c.groups.create(body as Parameters<typeof c.groups.create>[0],);
		},
		description:
			'Create a DSS group (admin). Body follows the DSS Group schema, e.g. {"name":"analysts","admin":false,"description":"..."}.',
		examples: [
			`dss group create --data '{"name":"analysts","admin":false,"description":"Business analysts"}'`,
		],
	},
	update: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("group", "update",);
			requireArgs(a, 1, usage,);
			const name = requireGroupName(a[0], usage,);
			const body = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (group definition). Usage: ${usage}`,
			);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "update",
					resource: "group",
					name,
					method: "PUT",
					endpoint: `/public/api/admin/groups/${encodeURIComponent(name,)}`,
					payload: body,
					note:
						"PUT replaces the whole Group object; the body must come from `dss group get`. No GET was issued in dry-run.",
				};
			}
			await c.groups.update(name, body as Parameters<typeof c.groups.update>[1],);
			return { updated: name, };
		},
		description:
			"Update a DSS group (admin). The body MUST be the Group object obtained from `dss group get` (PUT semantics); pass undocumented attributes through unchanged. Use --dry-run to preview the merged result without writing.",
		examples: [
			`dss group update analysts --data '{"name":"ANALYSTS","admin":false,"description":"Business analysts"}'`,
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("group", "delete",);
			requireArgs(a, 1, usage,);
			const name = requireGroupName(a[0], usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "delete",
					resource: "group",
					name,
					method: "DELETE",
					endpoint: `/public/api/admin/groups/${encodeURIComponent(name,)}`,
					...(f["if-exists"] === true
						? { note: "Existence not checked in dry-run; live --if-exists issues a GET probe first.", }
						: {}),
				};
			}
			if (f["if-exists"] === true) {
				const current = await readIfExists(() => c.groups.get(name,));
				if (!current) return skipResult("group", name, "missing",);
			}
			return c.groups.delete(name,);
		},
		description: "Delete a DSS group (admin). Destructive and irreversible.",
		examples: ["dss group delete analysts", "dss group delete analysts --if-exists",],
	},
},);
