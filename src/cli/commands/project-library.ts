import * as fs from "node:fs";
import type { DataikuClient, } from "../../client.js";
import { readStdinText, } from "../coerce.js";
import { encodedProjectEndpoint, planResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const PROJECT_LIBRARY_EXIT_CODES: Record<string, number> = { usage: 1, error: 2, transient: 3, };

function normalizeLibraryPath(path: string,): string {
	const normalized = path.replace(/^\/+/, "",);
	if (!normalized) throw new Error("Project library path is required",);
	return normalized;
}

function normalizeLibraryDestinationPath(path: string,): string {
	if (path === "/") return "/";
	return `/${normalizeLibraryPath(path,)}`;
}

function encodeLibraryPath(path: string,): string {
	return normalizeLibraryPath(path,)
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

function projectLibraryContentsEndpoint(
	client: DataikuClient,
	projectKey: string | undefined,
	path: string,
): string {
	return encodedProjectEndpoint(
		client,
		projectKey,
		`/libraries/contents/${encodeLibraryPath(path,)}`,
	);
}

function projectLibraryFolderEndpoint(
	client: DataikuClient,
	projectKey: string | undefined,
	path: string,
): string {
	return encodedProjectEndpoint(
		client,
		projectKey,
		`/libraries/folders/${encodeLibraryPath(path,)}`,
	);
}

function projectLibraryActionEndpoint(
	client: DataikuClient,
	projectKey: string | undefined,
	action: string,
): string {
	return encodedProjectEndpoint(client, projectKey, `/libraries/contents-actions/${action}`,);
}

function projectLibraryPutPayload(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	if (typeof flags["content"] === "string") return { contentSource: "flag", };
	if (typeof flags["file"] === "string") return { contentSource: "file", file: flags["file"], };
	return { contentSource: "stdin", };
}

function projectLibraryPlan(
	action: string,
	options: {
		method: string;
		endpoint: string;
		identifiers: Record<string, unknown>;
		payload?: unknown;
	},
): Record<string, unknown> {
	return planResult("project-library", action, {
		...options,
		asyncKind: "none",
		exitCodesOnFailure: PROJECT_LIBRARY_EXIT_CODES,
		idempotency: "none",
		plannedAndDryRun: true,
	},);
}

export const projectLibraryCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.projectLibrary.listContents(f["project-key"] as string | undefined,),
		usage: "dss project-library list [--project-key KEY]",
		description: "List the project code library (lib/) contents.",
		examples: ["dss project-library list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss project-library get <path> [--project-key KEY]",);
			return c.projectLibrary.getFile(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss project-library get <path> [--project-key KEY]",
		description: "Print the text content of a project library file.",
		examples: ["dss project-library get python/mylib/utils.py",],
	},
	"get-bytes": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library get-bytes <path> --output PATH [--project-key KEY]",);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const bytes = await c.projectLibrary.getFileBytes(
				a[0],
				f["project-key"] as string | undefined,
			);
			const written = await Bun.write(out, bytes, { createPath: false, },);
			return { path: out, bytes: written, };
		},
		usage: "dss project-library get-bytes <path> --output PATH [--project-key KEY]",
		description: "Download a project library file's raw bytes to a local file.",
		examples: ["dss project-library get-bytes static/logo.png --output ./logo.png",],
	},
	"create-file": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library create-file <path> [--dry-run] [--project-key KEY]",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("create-file", {
					method: "POST",
					endpoint: projectLibraryContentsEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
				},);
			}
			await c.projectLibrary.addFile(a[0], pk,);
			return { created: a[0], };
		},
		usage: "dss project-library create-file <path> [--dry-run] [--project-key KEY]",
		description: "Create an empty file in the project library.",
		examples: ["dss project-library create-file python/mylib/new.py",],
	},
	"create-folder": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library create-folder <path> [--dry-run] [--project-key KEY]",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("create-folder", {
					method: "POST",
					endpoint: projectLibraryFolderEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
				},);
			}
			await c.projectLibrary.addFolder(a[0], pk,);
			return { created: a[0], };
		},
		usage: "dss project-library create-folder <path> [--dry-run] [--project-key KEY]",
		description: "Create a folder in the project library.",
		examples: ["dss project-library create-folder python/mylib",],
	},
	put: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("put", {
					method: "POST",
					endpoint: projectLibraryContentsEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
					payload: projectLibraryPutPayload(f,),
				},);
			}
			const content = typeof f["content"] === "string"
				? f["content"] as string
				: typeof f["file"] === "string"
				? fs.readFileSync(f["file"] as string, "utf-8",)
				: await readStdinText();
			await c.projectLibrary.addOrUpdateFile(a[0], content, pk,);
			return { updated: a[0], };
		},
		usage:
			"dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Create or overwrite a project library file with raw text content.",
		examples: ["dss project-library put python/mylib/utils.py --file ./utils.py",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library delete <path> [--dry-run] [--project-key KEY]",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("delete", {
					method: "DELETE",
					endpoint: projectLibraryContentsEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
				},);
			}
			await c.projectLibrary.deleteFile(a[0], pk,);
			return { deleted: a[0], };
		},
		usage: "dss project-library delete <path> [--dry-run] [--project-key KEY]",
		description: "Delete a project library file or folder.",
		examples: ["dss project-library delete python/mylib/old.py",],
	},
	rename: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss project-library rename <path> <new-name> [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("rename", {
					method: "POST",
					endpoint: projectLibraryActionEndpoint(c, pk, "rename/",),
					identifiers: { path: a[0], newName: a[1], },
					payload: { oldPath: `/${normalizeLibraryPath(a[0],)}`, newName: a[1], },
				},);
			}
			await c.projectLibrary.rename(a[0], a[1], pk,);
			return { renamed: a[0], to: a[1], };
		},
		usage: "dss project-library rename <path> <new-name> [--dry-run] [--project-key KEY]",
		description: "Rename a project library file or folder within its parent.",
		examples: ["dss project-library rename python/mylib/old.py new.py",],
	},
	move: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss project-library move <path> <destination-folder> [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return projectLibraryPlan("move", {
					method: "POST",
					endpoint: projectLibraryActionEndpoint(c, pk, "move",),
					identifiers: { path: a[0], destinationFolder: a[1], },
					payload: {
						oldPath: `/${normalizeLibraryPath(a[0],)}`,
						newPath: normalizeLibraryDestinationPath(a[1],),
					},
				},);
			}
			await c.projectLibrary.move(a[0], a[1], pk,);
			return { moved: a[0], to: a[1], };
		},
		usage: "dss project-library move <path> <destination-folder> [--dry-run] [--project-key KEY]",
		description: "Move a project library file or folder into another folder.",
		examples: ["dss project-library move python/old.py python/mylib",],
	},
};
