import { jsonInput, requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const streamingEndpointCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.streamingEndpoints.list(f["project-key"] as string | undefined,),
		usage: "dss streaming-endpoint list [--project-key KEY]",
		description: "List streaming endpoints in a project.",
		examples: ["dss streaming-endpoint list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss streaming-endpoint get <id> [--project-key KEY]",);
			return c.streamingEndpoints.getSettings(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss streaming-endpoint get <id> [--project-key KEY]",
		description: "Get a streaming endpoint's settings.",
		examples: ["dss streaming-endpoint get my-stream",],
	},
	create: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss streaming-endpoint create <id> <type> [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
			);
			const body = jsonInput(f,) ?? {};
			return c.streamingEndpoints.create(a[0], a[1], body, f["project-key"] as string | undefined,);
		},
		usage:
			"dss streaming-endpoint create <id> <type> [--data JSON|--data-file PATH|--stdin] [--project-key KEY]",
		description: "Create a streaming endpoint of the given type.",
		examples: ["dss streaming-endpoint create my-stream kafka",],
	},
	"update-settings": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss streaming-endpoint update-settings <id> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (endpoint settings).",
			);
			await c.streamingEndpoints.updateSettings(a[0], body, f["project-key"] as string | undefined,);
			return { updated: a[0], };
		},
		usage:
			"dss streaming-endpoint update-settings <id> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a streaming endpoint's settings.",
		examples: ["dss streaming-endpoint update-settings my-stream --data-file settings.json",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss streaming-endpoint delete <id> [--project-key KEY]",);
			await c.streamingEndpoints.delete(a[0], f["project-key"] as string | undefined,);
			return { deleted: a[0], };
		},
		usage: "dss streaming-endpoint delete <id> [--project-key KEY]",
		description: "Delete a streaming endpoint.",
		examples: ["dss streaming-endpoint delete my-stream",],
	},
};
