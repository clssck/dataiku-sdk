import { jsonInput, requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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
		"create-managed": {
			handler: async (c, a, f,) => {
				const connectionId = f["connection"];
				if (typeof connectionId !== "string") {
					throw new UsageError("--connection CONN is required.", "missing_required_flag",);
				}
				const formatOptionId = f["format-option"];
				await c.streamingEndpoints.createManaged(a[0]!, {
					connectionId,
					...(typeof formatOptionId === "string" ? { formatOptionId, } : {}),
				}, f["project-key"] as string | undefined,);
				return { created: a[0], resource: "streaming-endpoint", };
			},
			description:
				"Create a managed streaming endpoint on a connection; DSS picks type and params from the connection (--format-option for the message format).",
			examples: ["dss streaming-endpoint create-managed orders_stream --connection kafka_main",],
		},
		schema: {
			handler: (c, a, f,) =>
				c.streamingEndpoints.getSchema(a[0]!, f["project-key"] as string | undefined,),
			description: "Get a streaming endpoint's schema ({columns: [{name, type}]}).",
			examples: ["dss streaming-endpoint schema orders_stream",],
		},
		"set-schema": {
			handler: async (c, a, f,) => {
				await c.streamingEndpoints.setSchema(
					a[0]!,
					requiredJsonInput(f, '--data, --data-file, or --stdin is required ({"columns": [...]}).',),
					f["project-key"] as string | undefined,
				);
				return { updated: a[0], resource: "streaming-endpoint", };
			},
			description:
				"Replace a streaming endpoint's schema with an object from the schema action, edited.",
			examples: ["dss streaming-endpoint set-schema orders_stream --data-file schema.json",],
		},
	},
);
