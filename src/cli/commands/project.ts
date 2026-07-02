import { jsonInput, num, requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const projectCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.projects.list(),
		usage: "dss project list",
		description: "List all accessible projects.",
		examples: ["dss project list",],
	},
	get: {
		handler: (c, _a, f,) => c.projects.get(f["project-key"] as string | undefined,),
		usage: "dss project get [--project-key KEY]",
		description: "Get project settings and metadata.",
		examples: ["dss project get", "dss project get --project-key MYPROJ",],
	},
	metadata: {
		handler: (c, _a, f,) => c.projects.metadata(f["project-key"] as string | undefined,),
		usage: "dss project metadata [--project-key KEY]",
		description: "Get project-level metadata (tags, labels, custom fields).",
		examples: ["dss project metadata", "dss project metadata --project-key MYPROJ",],
	},
	flow: {
		handler: (c, _a, f,) => c.projects.flow(f["project-key"] as string | undefined,),
		usage: "dss project flow [--project-key KEY]",
		description: "Get the raw flow graph (all datasets, recipes, and edges).",
		examples: ["dss project flow", "dss project flow --project-key MYPROJ",],
	},
	map: {
		handler: (c, _a, f,) =>
			c.projects.map({
				maxNodes: num(f["max-nodes"],),
				maxEdges: num(f["max-edges"],),
				includeRaw: f["include-raw"] === true,
			},),
		usage: "dss project map [--max-nodes N] [--max-edges N] [--include-raw]",
		description: "Get a summarized, truncated flow map.",
		examples: [
			"dss project map",
			"dss project map --max-nodes 50 --max-edges 100",
			"dss project map --include-raw",
		],
	},
	create: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss project create <projectKey> <name> [--owner LOGIN] [--data JSON|--data-file PATH|--stdin]",
			);
			return c.projects.createProject(a[0], a[1], f["owner"] as string | undefined, jsonInput(f,),);
		},
		usage:
			"dss project create <projectKey> <name> [--owner LOGIN] [--data JSON|--data-file PATH|--stdin]",
		description: "Create a new project (optionally with owner login and settings).",
		examples: ["dss project create MY_PROJ MyProject",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project delete <projectKey> [--drop-data]",);
			await c.projects.deleteProject(a[0], f["drop-data"] === true,);
			return { deleted: a[0], };
		},
		usage: "dss project delete <projectKey> [--drop-data]",
		description: "Delete a project (destructive). --drop-data also clears managed datasets.",
		examples: ["dss project delete MY_PROJ",],
	},
	duplicate: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				"dss project duplicate <sourceKey> <targetKey> <targetName> [--data JSON|--data-file PATH|--stdin]",
			);
			return c.projects.duplicate(a[0], a[1], a[2], jsonInput(f,),);
		},
		usage:
			"dss project duplicate <sourceKey> <targetKey> <targetName> [--data JSON|--data-file PATH|--stdin]",
		description: "Duplicate a project into a new project key.",
		examples: ["dss project duplicate SRC NEW NewProject",],
	},
	export: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project export <projectKey> --output PATH [--data JSON|--data-file PATH|--stdin]",
			);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			await c.projects.exportArchive(a[0], out, jsonInput(f,),);
			return { path: out, };
		},
		usage: "dss project export <projectKey> --output PATH [--data JSON|--data-file PATH|--stdin]",
		description: "Export a project to a local archive (.zip).",
		examples: ["dss project export MY_PROJ --output ./my_proj.zip",],
	},
	import: {
		handler: async (c, a,) => {
			requireArgs(a, 1, "dss project import <filePath>",);
			return c.projects.importProjectFromArchive(a[0],);
		},
		usage: "dss project import <filePath>",
		description: "Upload a project archive to prepare an import (returns a temporary import id).",
		examples: ["dss project import ./my_proj.zip",],
	},
	"permissions-get": {
		handler: (c, _a, f,) => c.projects.getPermissions(f["project-key"] as string | undefined,),
		usage: "dss project permissions-get [--project-key KEY]",
		description: "Get a project's permissions.",
		examples: ["dss project permissions-get --project-key MY_PROJ",],
	},
	"permissions-set": {
		handler: async (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (permissions object).",
			);
			await c.projects.setPermissions(f["project-key"] as string | undefined, body,);
			return { updated: true, };
		},
		usage: "dss project permissions-set (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a project's permissions.",
		examples: ["dss project permissions-set --data-file perms.json --project-key MY_PROJ",],
	},
	"settings-get": {
		handler: (c, _a, f,) => c.projects.getSettings(f["project-key"] as string | undefined,),
		usage: "dss project settings-get [--project-key KEY]",
		description: "Get a project's settings.",
		examples: ["dss project settings-get --project-key MY_PROJ",],
	},
	"settings-set": {
		handler: async (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (settings object).",
			);
			await c.projects.setSettings(f["project-key"] as string | undefined, body,);
			return { updated: true, };
		},
		usage: "dss project settings-set (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a project's settings.",
		examples: ["dss project settings-set --data-file settings.json --project-key MY_PROJ",],
	},
};
