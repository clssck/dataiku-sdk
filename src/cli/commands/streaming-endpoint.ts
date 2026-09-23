import { jsonInput, requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const streamingEndpointCommands: Record<string, CommandMeta> = withUsage(
	"streaming-endpoint",
	{
		list: {
			handler: (c, _a, f,) => c.streamingEndpoints.list(f["project-key"] as string | undefined,),
			description: "List streaming endpoints in a project.",
			examples: ["dss streaming-endpoint list",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, commandUsage("streaming-endpoint", "get",),);
				return c.streamingEndpoints.getSettings(a[0], f["project-key"] as string | undefined,);
			},
			description: "Get a streaming endpoint's settings.",
			examples: ["dss streaming-endpoint get my-stream",],
		},
		create: {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					2,
					commandUsage("streaming-endpoint", "create",),
				);
				const body = jsonInput(f,) ?? {};
				return c.streamingEndpoints.create(a[0], a[1], body, f["project-key"] as string | undefined,);
			},
			description: "Create a streaming endpoint of the given type.",
			examples: ["dss streaming-endpoint create my-stream kafka",],
		},
		"update-settings": {
			handler: async (c, a, f,) => {
				requireArgs(
					a,
					1,
					commandUsage("streaming-endpoint", "update-settings",),
				);
				const body = requiredJsonInput(
					f,
					"--data, --data-file, or --stdin is required (endpoint settings).",
				);
				await c.streamingEndpoints.updateSettings(a[0], body, f["project-key"] as string | undefined,);
				return { updated: a[0], };
			},
			description: "Replace a streaming endpoint's settings.",
			examples: ["dss streaming-endpoint update-settings my-stream --data-file settings.json",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, commandUsage("streaming-endpoint", "delete",),);
				await c.streamingEndpoints.delete(a[0], f["project-key"] as string | undefined,);
				return { deleted: a[0], };
			},
			description: "Delete a streaming endpoint.",
			examples: ["dss streaming-endpoint delete my-stream",],
		},
	},
);
