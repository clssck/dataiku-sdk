import { readFileSync, writeFileSync, } from "node:fs";
import { readStdinText, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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
			writeFileSync(out, bytes,);
			return { path: out, bytes: bytes.length, };
		},
		usage: "dss project-library get-bytes <path> --output PATH [--project-key KEY]",
		description: "Download a project library file's raw bytes to a local file.",
		examples: ["dss project-library get-bytes static/logo.png --output ./logo.png",],
	},
	"create-file": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library create-file <path> [--project-key KEY]",);
			await c.projectLibrary.addFile(a[0], f["project-key"] as string | undefined,);
			return { created: a[0], };
		},
		usage: "dss project-library create-file <path> [--project-key KEY]",
		description: "Create an empty file in the project library.",
		examples: ["dss project-library create-file python/mylib/new.py",],
	},
	"create-folder": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library create-folder <path> [--project-key KEY]",);
			await c.projectLibrary.addFolder(a[0], f["project-key"] as string | undefined,);
			return { created: a[0], };
		},
		usage: "dss project-library create-folder <path> [--project-key KEY]",
		description: "Create a folder in the project library.",
		examples: ["dss project-library create-folder python/mylib",],
	},
	put: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--project-key KEY]",
			);
			const content = typeof f["content"] === "string"
				? f["content"] as string
				: typeof f["file"] === "string"
				? readFileSync(f["file"] as string, "utf-8",)
				: await readStdinText();
			await c.projectLibrary.addOrUpdateFile(a[0], content, f["project-key"] as string | undefined,);
			return { updated: a[0], };
		},
		usage: "dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--project-key KEY]",
		description: "Create or overwrite a project library file with raw text content.",
		examples: ["dss project-library put python/mylib/utils.py --file ./utils.py",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library delete <path> [--project-key KEY]",);
			await c.projectLibrary.deleteFile(a[0], f["project-key"] as string | undefined,);
			return { deleted: a[0], };
		},
		usage: "dss project-library delete <path> [--project-key KEY]",
		description: "Delete a project library file or folder.",
		examples: ["dss project-library delete python/mylib/old.py",],
	},
	rename: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss project-library rename <path> <new-name> [--project-key KEY]",);
			await c.projectLibrary.rename(a[0], a[1], f["project-key"] as string | undefined,);
			return { renamed: a[0], to: a[1], };
		},
		usage: "dss project-library rename <path> <new-name> [--project-key KEY]",
		description: "Rename a project library file or folder within its parent.",
		examples: ["dss project-library rename python/mylib/old.py new.py",],
	},
	move: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss project-library move <path> <destination-folder> [--project-key KEY]",);
			await c.projectLibrary.move(a[0], a[1], f["project-key"] as string | undefined,);
			return { moved: a[0], to: a[1], };
		},
		usage: "dss project-library move <path> <destination-folder> [--project-key KEY]",
		description: "Move a project library file or folder into another folder.",
		examples: ["dss project-library move python/old.py python/mylib",],
	},
};
