import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const workspaceCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.workspaces.list(),
		usage: "dss workspace list",
		description: "List collaboration workspaces on the instance.",
		examples: ["dss workspace list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss workspace get <workspaceKey>",);
			return c.workspaces.get(a[0],);
		},
		usage: "dss workspace get <workspaceKey>",
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
		usage: "dss workspace create (--data JSON|--data-file PATH|--stdin)",
		description: "Create a collaboration workspace.",
		examples: ["dss workspace create --data-file ws.json",],
	},
	"update-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss workspace update-settings <workspaceKey> (--data JSON|--data-file PATH|--stdin)",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (workspace settings).",
			);
			await c.workspaces.updateSettings(a[0], body,);
			return { updated: a[0], };
		},
		usage: "dss workspace update-settings <workspaceKey> (--data JSON|--data-file PATH|--stdin)",
		description: "Replace a workspace's settings (admin).",
		examples: ["dss workspace update-settings MY_WS --data-file ws.json",],
	},
	delete: {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss workspace delete <workspaceKey>",);
			await c.workspaces.delete(a[0],);
			return { deleted: a[0], };
		},
		usage: "dss workspace delete <workspaceKey>",
		description: "Delete a workspace (admin).",
		examples: ["dss workspace delete MY_WS",],
	},
	"list-objects": {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss workspace list-objects <workspaceKey>",);
			return c.workspaces.listObjects(a[0],);
		},
		usage: "dss workspace list-objects <workspaceKey>",
		description: "List objects shared in a workspace.",
		examples: ["dss workspace list-objects MY_WS",],
	},
	"add-object": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss workspace add-object <workspaceKey> (--data JSON|--data-file PATH|--stdin)",
			);
			const object = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (object definition).",
			);
			return c.workspaces.addObject(a[0], object,);
		},
		usage: "dss workspace add-object <workspaceKey> (--data JSON|--data-file PATH|--stdin)",
		description: "Add an object (link or DSS object) to a workspace.",
		examples: ["dss workspace add-object MY_WS --data-file object.json",],
	},
};
