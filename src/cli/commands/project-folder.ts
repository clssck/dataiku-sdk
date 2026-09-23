import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { isNotFoundError, planResult, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const PLAN_FAILURE_CODES = { usage: 1, error: 2, transient: 3, };

function folderEndpoint(folderId: string, suffix = "",): string {
	return `/public/api/project-folders/${encodeURIComponent(folderId,)}${suffix}`;
}

/**
 * Positional ids are server keys: an empty string would silently target the
 * parent collection (e.g. DELETE /public/api/project-folders/ = the root
 * folder) instead of failing as a usage error. Runs for live, --plan, and
 * --dry-run alike with zero network.
 */
function nonEmptyPositionals(
	args: string[],
	names: string[],
	usage: string,
): void {
	args.forEach((value, index,) => {
		if (value.trim().length === 0) {
			throw new UsageError(
				`<${
					names[index] ?? `arg${String(index + 1,)}`
				}> must be a non-empty identifier. Usage: ${usage}`,
			);
		}
	},);
}

const GET_USAGE = commandUsage("project-folder", "get",);
const SETTINGS_GET_USAGE = commandUsage("project-folder", "settings-get",);
const SETTINGS_SET_USAGE = commandUsage("project-folder", "settings-set",);
const MOVE_USAGE = commandUsage("project-folder", "move",);
const DELETE_USAGE = commandUsage("project-folder", "delete",);
const CREATE_CHILD_USAGE = commandUsage("project-folder", "create-child",);
const MOVE_PROJECT_USAGE = commandUsage("project-folder", "move-project",);

export const projectFolderCommands: Record<string, CommandMeta> = withUsage("project-folder", {
	root: {
		handler: (c,) => c.projectFolders.root(),
		description: "Get the root project folder definition (visible children and projects).",
		examples: ["dss project-folder root",],
	},
	get: {
		validate: (a,) => {
			requireArgs(a, 1, GET_USAGE,);
			nonEmptyPositionals(a, ["folderId",], GET_USAGE,);
		},
		handler: (c, a,) => c.projectFolders.get(a[0],),
		description: "Get a project folder definition (children and project keys).",
		examples: ["dss project-folder get KdLmPU6",],
	},
	"settings-get": {
		validate: (a,) => {
			requireArgs(a, 1, SETTINGS_GET_USAGE,);
			nonEmptyPositionals(a, ["folderId",], SETTINGS_GET_USAGE,);
		},
		handler: (c, a,) => c.projectFolders.getSettings(a[0],),
		description: "Get project folder settings (name, owner, permissions). Admin required.",
		examples: ["dss project-folder settings-get KdLmPU6",],
	},
	"settings-set": {
		validate: (a,) => {
			requireArgs(a, 1, SETTINGS_SET_USAGE,);
			nonEmptyPositionals(a, ["folderId",], SETTINGS_SET_USAGE,);
		},
		handler: async (c, a, f,) => {
			const settings = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (project folder settings).",
			);
			if (executionMode(f,).dryRun) {
				return planResult("project-folder", "settings-set", {
					asyncKind: "none",
					method: "PUT",
					endpoint: folderEndpoint(a[0], "/settings",),
					identifiers: { folderId: a[0], },
					payload: settings,
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.projectFolders.updateSettings(
				a[0],
				settings as Parameters<typeof c.projectFolders.updateSettings>[1],
			);
			return { updated: a[0], resource: "project-folder", };
		},
		description: "Replace project folder settings (name, owner, permissions). Admin required.",
		examples: [
			"dss project-folder settings-set KdLmPU6 --data-file settings.json --dry-run",
		],
	},
	move: {
		validate: (a,) => {
			requireArgs(a, 2, MOVE_USAGE,);
			nonEmptyPositionals(a, ["folderId", "destinationFolderId",], MOVE_USAGE,);
		},
		handler: async (c, a, f,) => {
			if (executionMode(f,).dryRun) {
				return planResult("project-folder", "move", {
					asyncKind: "none",
					method: "POST",
					endpoint: `${folderEndpoint(a[0], "/move",)}?destination=${encodeURIComponent(a[1],)}`,
					identifiers: { folderId: a[0], destination: a[1], },
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.projectFolders.move(a[0], a[1],);
			return { moved: a[0], destination: a[1], resource: "project-folder", };
		},
		description:
			"Move a project folder (and its content) into another project folder. Cannot move into a sub-folder of itself.",
		examples: ["dss project-folder move KdLmPU6 dgKywsx",],
	},
	delete: {
		validate: (a,) => {
			requireArgs(a, 1, DELETE_USAGE,);
			nonEmptyPositionals(a, ["folderId",], DELETE_USAGE,);
		},
		handler: async (c, a, f,) => {
			// Dry-run must be a validated plan with zero HTTP: no existence GET.
			if (executionMode(f,).dryRun) {
				return planResult("project-folder", "delete", {
					asyncKind: "none",
					method: "DELETE",
					endpoint: folderEndpoint(a[0],),
					identifiers: { folderId: a[0], },
					idempotency: "if-exists",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			// --if-exists (live only) pays one existence GET; a 404 becomes a skip.
			if (f["if-exists"] === true) {
				const current = await c.projectFolders.get(a[0],).catch((error: unknown,) => {
					if (isNotFoundError(error,)) return undefined;
					throw error;
				},);
				if (!current) return skipResult("project-folder", a[0], "missing",);
			}
			await c.projectFolders.delete(a[0],);
			return { deleted: a[0], resource: "project-folder", };
		},
		description: "Delete an empty project folder (no sub-folders, no projects). Admin required.",
		examples: ["dss project-folder delete KdLmPU6 --dry-run",],
	},
	"create-child": {
		validate: (a, f,) => {
			const name = f["name"];
			if (typeof name === "string" && name.trim().length === 0) {
				throw new UsageError(
					`--name must be a non-empty folder name. Usage: ${CREATE_CHILD_USAGE}`,
				);
			}
			if (!a[0] || typeof name !== "string" || name.length === 0) {
				throw new UsageError(`Usage: ${CREATE_CHILD_USAGE}`,);
			}
		},
		handler: async (c, a, f,) => {
			const name = f["name"] as string;
			if (executionMode(f,).dryRun) {
				return planResult("project-folder", "create-child", {
					asyncKind: "none",
					method: "POST",
					endpoint: `${folderEndpoint(a[0], "/children",)}?name=${encodeURIComponent(name,)}`,
					identifiers: { parentFolderId: a[0], name, },
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			const created = await c.projectFolders.createChild(a[0], name,);
			return { created: created.id ?? name, resource: "project-folder", ...created, };
		},
		description: "Create a sub-project folder. WRITE_CONTENTS required on the parent.",
		examples: ["dss project-folder create-child KdLmPU6 --name my_sub_folder",],
	},
	"move-project": {
		validate: (a,) => {
			requireArgs(a, 3, MOVE_PROJECT_USAGE,);
			nonEmptyPositionals(
				a,
				["folderId", "projectKey", "destinationFolderId",],
				MOVE_PROJECT_USAGE,
			);
		},
		handler: async (c, a, f,) => {
			if (executionMode(f,).dryRun) {
				return planResult("project-folder", "move-project", {
					asyncKind: "none",
					method: "POST",
					endpoint: `${
						folderEndpoint(a[0], `/projects/${encodeURIComponent(a[1],)}/move`,)
					}?destination=${encodeURIComponent(a[2],)}`,
					identifiers: { folderId: a[0], projectKey: a[1], destination: a[2], },
					idempotency: "none",
					exitCodesOnFailure: PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.projectFolders.moveProject(a[0], a[1], a[2],);
			return { moved: a[1], destination: a[2], resource: "project-folder", };
		},
		description:
			"Move a project from its project folder to another project folder. WRITE_CONTENTS on destination and project admin required.",
		examples: ["dss project-folder move-project KdLmPU6 MYPROJECT dgKywsx",],
	},
},);
