import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

export const connectionCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) =>
			c.connections.list({
				type: f["type"] as string | undefined,
			},),
		usage: "dss connection list [--type TYPE]",
		description: "List all connection names, optionally filtered by connection type.",
		examples: ["dss connection list", "dss connection list --type Filesystem",],
	},
	infer: {
		handler: (c, _a, f,) =>
			c.connections.infer({
				mode: f["mode"] as "fast" | "rich" | undefined,
				projectKey: f["project-key"] as string | undefined,
			},),
		usage: "dss connection infer [--mode fast|rich] [--project-key KEY]",
		description: "List connections with inferred types and metadata.",
		examples: ["dss connection infer", "dss connection infer --mode rich",],
	},
	schemas: {
		handler: (c, _a, f,) => {
			const connection = f["connection"] as string | undefined;
			if (!connection) {
				throw new UsageError(
					"--connection is required. Usage: dss connection schemas --connection CONN",
				);
			}
			return c.connections.schemas({
				connection,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: "dss connection schemas --connection CONN [--project-key KEY]",
		description: "List schemas in a SQL connection.",
		examples: ["dss connection schemas --connection ATHENA_CONN --project-key MYPROJ",],
	},
	tables: {
		handler: (c, _a, f,) => {
			const connection = f["connection"] as string | undefined;
			if (!connection) {
				throw new UsageError(
					"--connection is required. Usage: dss connection tables --connection CONN",
				);
			}
			return c.connections.tables({
				connection,
				catalog: f["catalog"] as string | undefined,
				schema: f["schema"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss connection tables --connection CONN [--catalog CATALOG] [--schema SCHEMA] [--project-key KEY]",
		description:
			"List importable tables in a SQL connection, optionally scoped by catalog and schema.",
		examples: [
			"dss connection tables --connection ATHENA_CONN --schema analytics --project-key MYPROJ",
		],
	},
};
