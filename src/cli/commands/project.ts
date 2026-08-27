import { ClientValidationError, DataikuError, } from "../../errors.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { inspectProjectArchive, } from "../../utils/project-archive.js";
import type { ProjectArchiveInspection, } from "../../utils/project-archive.js";
import { projectIncarnationHash, } from "../../utils/project-incarnation.js";
import { jsonInput, num, requiredJsonInput, requiredStringFlag, } from "../coerce.js";
import { CommandResultFailure, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const INSPECT_ARCHIVE_USAGE = "dss project inspect-archive <file>";

/** Local, DSS-free archive inspection shared by the local handler and the command handler. */
function inspectArchiveCommand(args: string[],): Promise<ProjectArchiveInspection> {
	requireArgs(args, 1, INSPECT_ARCHIVE_USAGE,);
	return inspectProjectArchive(args[0]!,);
}

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
				projectKey: f["project-key"] as string | undefined,
				maxNodes: num(f["max-nodes"], "--max-nodes",),
				maxEdges: num(f["max-edges"], "--max-edges",),
				includeRaw: f["include-raw"] === true,
			},),
		usage: "dss project map [--max-nodes N] [--max-edges N] [--include-raw] [--project-key KEY]",
		description: "Get a summarized, truncated flow map.",
		examples: [
			"dss project map",
			"dss project map --max-nodes 50 --max-edges 100",
			"dss project map --include-raw",
		],
	},
	create: {
		handler: (c, a, f,) => {
			const usage =
				"dss project create <projectKey> <name> --owner LOGIN [--data JSON|--data-file PATH|--stdin]";
			requireArgs(a, 2, usage,);
			const owner = f["owner"] as string | undefined;
			if (!owner) throw new UsageError(`--owner is required. Usage: ${usage}`,);
			return c.projects.createProject(a[0], a[1], owner, jsonInput(f,),);
		},
		usage:
			"dss project create <projectKey> <name> --owner LOGIN [--data JSON|--data-file PATH|--stdin]",
		description: "Create a new project with owner login and optional settings.",
		examples: ["dss project create MY_PROJ MyProject --owner alice",],
	},
	delete: {
		handler: async (c, a, f,) => {
			const usage =
				"dss project delete <projectKey> [--drop-data] [--if-exists] [--dry-run] [--expect-project-incarnation HASH]";
			requireArgs(a, 1, usage,);
			const projectKey = a[0];
			const expectedIncarnation = f["expect-project-incarnation"] === undefined
				? undefined
				: requiredStringFlag(f, "expect-project-incarnation", usage,);
			if (
				expectedIncarnation !== undefined
				&& !/^[0-9a-f]{64}$/.test(expectedIncarnation,)
			) {
				throw new UsageError(
					"--expect-project-incarnation must be a 64-character lowercase SHA-256 hash.",
					"validation_failed",
				);
			}
			const guarded = f["if-exists"] === true
				|| f["dry-run"] === true
				|| expectedIncarnation !== undefined;
			if (!guarded) {
				// Purely user-requested delete without any guard flag: keep the
				// historical direct DELETE, with no preflight probe.
				await c.projects.deleteProject(projectKey, f["drop-data"] === true,);
				return { deleted: projectKey, };
			}

			// Guarded path: existence and identity must be verified before any
			// DELETE. A missing creationTag or an incarnation mismatch refuses
			// before the DELETE under possible project-key reuse.
			let currentIncarnation: string | undefined;
			try {
				const details = await c.projects.get(projectKey,);
				if (expectedIncarnation !== undefined) {
					currentIncarnation = projectIncarnationHash(projectKey, details,);
					if (currentIncarnation === undefined) {
						throw new ClientValidationError(
							`DSS did not return creationTag identity for project ${projectKey}. Refusing to delete without project-incarnation verification.`,
							"validation_failed",
							"Project-key reuse cannot be excluded. Refusing the DELETE; re-read the project and retry with a hash captured from that incarnation.",
							{ projectKey, },
						);
					}
					if (currentIncarnation !== expectedIncarnation) {
						throw new ClientValidationError(
							`Project ${projectKey} is not the same project incarnation recorded for this operation. Refusing to delete after project-key reuse.`,
							"validation_failed",
							"Re-read the current project and retry only with an artifact captured from that incarnation.",
							{
								projectKey,
								expectedProjectIncarnationHash: expectedIncarnation,
								currentProjectIncarnationHash: currentIncarnation,
							},
						);
					}
				}
			} catch (error) {
				if (error instanceof DataikuError && error.status === 404) {
					// Already absent: converge only when the caller asked for it.
					if (f["if-exists"] !== true) throw error;
					return { deleted: false, alreadyAbsent: true, projectKey, };
				}
				throw error;
			}
			if (f["dry-run"] === true) {
				return {
					deleted: false,
					dryRun: true,
					projectKey,
					...(currentIncarnation !== undefined
						? { projectIncarnationHash: currentIncarnation, }
						: {}),
				};
			}
			await c.projects.deleteProject(projectKey, f["drop-data"] === true,);
			return {
				deleted: true,
				projectKey,
				...(currentIncarnation !== undefined
					? { projectIncarnationHash: currentIncarnation, }
					: {}),
			};
		},
		usage:
			"dss project delete <projectKey> [--drop-data] [--if-exists] [--dry-run] [--expect-project-incarnation HASH]",
		description:
			"Delete a project (destructive). --drop-data also clears managed datasets; --if-exists treats an absent project as already deleted; --dry-run verifies without deleting; --expect-project-incarnation HASH refuses deletion unless the project still matches the recorded incarnation.",
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
		handler: async (c, a, f,) => {
			const usage =
				"dss project import <filePath> [--target-project-key KEY] [--data JSON|--data-file PATH|--stdin]";
			requireArgs(a, 1, usage,);
			const settings = jsonInput(f,) ?? {};
			const rawTarget = f["target-project-key"] as string | undefined;
			const targetProjectKey = rawTarget?.trim();
			if (rawTarget !== undefined && targetProjectKey === "") {
				throw new UsageError(`--target-project-key must not be empty. Usage: ${usage}`,);
			}
			const settingsTarget = settings.targetProjectKey;
			if (
				settingsTarget !== undefined
				&& (typeof settingsTarget !== "string" || settingsTarget.trim() === "")
			) {
				throw new UsageError(
					`targetProjectKey in import settings must be a non-empty string. Usage: ${usage}`,
				);
			}
			if (
				targetProjectKey !== undefined
				&& settingsTarget !== undefined
				&& targetProjectKey !== settingsTarget.trim()
			) {
				throw new UsageError(
					`--target-project-key conflicts with targetProjectKey in import settings. Usage: ${usage}`,
				);
			}
			const result = await c.projects.importProjectFromArchive(
				a[0],
				targetProjectKey === undefined ? settings : { ...settings, targetProjectKey, },
			);
			if (result.success !== true) {
				throw new CommandResultFailure(
					{ ...result, resource: "project", action: "import", stage: "process", },
					2,
				);
			}
			return result;
		},
		usage:
			"dss project import <filePath> [--target-project-key KEY] [--data JSON|--data-file PATH|--stdin]",
		description:
			"Upload and process a project archive, optionally overriding its project key. The archive is validated locally before upload; verified success reports the used project key, explicit request/actual remapping, and the landed incarnation hash.",
		examples: [
			"dss project import ./my_proj.zip",
			"dss project import ./my_proj.zip --target-project-key MY_PROJ",
		],
	},
	"inspect-archive": {
		handler: async (_c, a,) => inspectArchiveCommand(a,),
		localHandler: inspectArchiveCommand,
		usage: INSPECT_ARCHIVE_USAGE,
		description:
			"Read-only local inspection of a project export archive: member names, sizes, manifest validity, archive issues, and the source project key. Never contacts DSS.",
		examples: ["dss project inspect-archive ./my_proj.zip",],
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
			const projectKey = f["project-key"] as string | undefined;
			const current = await c.projects.getSettings(projectKey,);
			const next = deepMerge(current as unknown as Record<string, unknown>, body,);
			await c.projects.setSettings(projectKey, next,);
			return { updated: true, };
		},
		usage: "dss project settings-set (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Update a project's settings via JSON merge.",
		examples: ["dss project settings-set --data-file settings.json --project-key MY_PROJ",],
	},
};
