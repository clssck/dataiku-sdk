import { requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const workspaceCommands: Record<string, CommandMeta> = withUsage("workspace", {
	list: {
		handler: (c,) => c.workspaces.list(),
		description: "List collaboration workspaces on the instance.",
		examples: ["dss workspace list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("workspace", "get",),);
			return c.workspaces.get(a[0],);
		},
		description: "Get a workspace's settings.",
		examples: ["dss workspace get MY_WS",],
	},
	create: {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (workspace definition).",
			);
			return c.workspaces.create(body as Parameters<typeof c.workspaces.create>[0],);
		},
		description: "Create a collaboration workspace.",
		examples: ["dss workspace create --data-file ws.json",],
	},
	"update-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("workspace", "update-settings",),
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (workspace settings).",
			);
			await c.workspaces.updateSettings(a[0], body,);
			return { updated: a[0], };
		},
		description: "Replace a workspace's settings (admin).",
		examples: ["dss workspace update-settings MY_WS --data-file ws.json",],
	},
	delete: {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("workspace", "delete",),);
			await c.workspaces.delete(a[0],);
			return { deleted: a[0], };
		},
		description: "Delete a workspace (admin).",
		examples: ["dss workspace delete MY_WS",],
	},
	"list-objects": {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("workspace", "list-objects",),);
			return c.workspaces.listObjects(a[0],);
		},
		description: "List objects shared in a workspace.",
		examples: ["dss workspace list-objects MY_WS",],
	},
	"add-object": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("workspace", "add-object",),
			);
			const object = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (object definition).",
			);
			return c.workspaces.addObject(a[0], object,);
		},
		description: "Add an object (link or DSS object) to a workspace.",
		examples: ["dss workspace add-object MY_WS --data-file object.json",],
	},
},);
