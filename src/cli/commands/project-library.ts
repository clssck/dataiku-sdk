import * as fs from "node:fs";
import type { DataikuClient, } from "../../client.js";
import {
	encodeLibraryPath,
	EXPECT_SHA256_PATTERN,
	type ProjectLibraryDiffResult,
	validateLibraryDestinationPath,
	validateLibraryName,
	validateLibraryPath,
} from "../../resources/project-library.js";
import { numFlag, readStdinText, sha256Hex, } from "../coerce.js";
import { encodedProjectEndpoint, planResult, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const PROJECT_LIBRARY_EXIT_CODES: Record<string, number> = { usage: 1, error: 2, transient: 3, };
const PUT_FLAG_ERROR_HINT = "Pass a 64-character SHA-256 hex digest (lowercase or uppercase).";

function sha256BytesHex(bytes: Uint8Array,): string {
	return new Bun.CryptoHasher("sha256",).update(bytes,).digest("hex",);
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

export interface ProjectLibraryPutPayload {
	/** Where the written bytes come from: --content flag, --file, or --stdin. */
	contentSource: "flag" | "file" | "stdin";
	/** Local file path when contentSource is "file". */
	file?: string;
	/** Byte count of the content when it is available without consuming stdin. */
	bytes?: number;
	/** SHA-256 hex digest of the exact bytes to write, when safely available. */
	sha256?: string;
	/** SHA-256 hex digest the caller expects the remote file to carry before the write. */
	expectSha256?: string;
}

/**
 * Put-input metadata for plans and reports. Plans must never include content
 * bytes: this helper reports only the source, byte count, and SHA-256 digest —
 * and only when they are available without consuming stdin (dry-run keeps
 * stdin unread, so the stdin source carries no size/hash).
 */
export function projectLibraryPutPayload(
	flags: Record<string, string | boolean>,
): ProjectLibraryPutPayload {
	const expectSha256 = expectSha256FromFlags(flags,);
	if (typeof flags["content"] === "string") {
		const text = flags["content"] as string;
		return {
			contentSource: "flag",
			bytes: Buffer.byteLength(text, "utf8",),
			sha256: sha256Hex(text,),
			...(expectSha256 ? { expectSha256, } : {}),
		};
	}
	if (typeof flags["file"] === "string") {
		const bytes = fs.readFileSync(flags["file"] as string,);
		return {
			contentSource: "file",
			file: flags["file"] as string,
			bytes: bytes.length,
			sha256: sha256BytesHex(bytes,),
			...(expectSha256 ? { expectSha256, } : {}),
		};
	}
	return { contentSource: "stdin", ...(expectSha256 ? { expectSha256, } : {}), };
}

function projectLibraryPlan(
	action: string,
	options: {
		method: string;
		endpoint: string;
		identifiers: Record<string, unknown>;
		payload?: unknown;
		idempotency?: string;
	},
): Record<string, unknown> {
	return planResult("project-library", action, {
		...options,
		idempotency: options.idempotency ?? "none",
		asyncKind: "none",
		exitCodesOnFailure: PROJECT_LIBRARY_EXIT_CODES,
		plannedAndDryRun: true,
	},);
}

function expectSha256FromFlags(flags: Record<string, string | boolean>,): string | undefined {
	const value = flags["expect-sha256"];
	if (value === undefined || value === false) return undefined;
	if (typeof value !== "string" || !EXPECT_SHA256_PATTERN.test(value,)) {
		throw new UsageError(
			"--expect-sha256 must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			PUT_FLAG_ERROR_HINT,
		);
	}
	return value;
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
			validateLibraryPath(a[0],);
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
			return { path: out, bytes: written, sha256: sha256BytesHex(bytes,), };
		},
		usage: "dss project-library get-bytes <path> --output PATH [--project-key KEY]",
		description: "Download a project library file's raw bytes to a local file.",
		examples: ["dss project-library get-bytes static/logo.png --output ./logo.png",],
	},
	"create-file": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library create-file <path> [--if-not-exists] [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			validateLibraryPath(a[0],);
			if (f["dry-run"] === true) {
				return projectLibraryPlan("create-file", {
					method: "POST",
					endpoint: projectLibraryContentsEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
					idempotency: "if-not-exists",
				},);
			}
			if (f["if-not-exists"] === true) {
				const exists = await c.projectLibrary.hasLibraryItem(a[0], pk,);
				if (exists) {
					return skipResult("project-library", a[0], "exists", { kind: "file", },);
				}
			}
			await c.projectLibrary.addFile(a[0], pk,);
			return { created: a[0], };
		},
		usage: "dss project-library create-file <path> [--if-not-exists] [--dry-run] [--project-key KEY]",
		description:
			"Create an empty file in the project library; refuses to overwrite an existing item.",
		examples: ["dss project-library create-file python/mylib/new.py --if-not-exists",],
	},
	"create-folder": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library create-folder <path> [--if-not-exists] [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			validateLibraryPath(a[0],);
			if (f["dry-run"] === true) {
				return projectLibraryPlan("create-folder", {
					method: "POST",
					endpoint: projectLibraryFolderEndpoint(c, pk, a[0],),
					identifiers: { path: a[0], },
					idempotency: "if-not-exists",
				},);
			}
			if (f["if-not-exists"] === true) {
				const exists = await c.projectLibrary.hasLibraryItem(a[0], pk,);
				if (exists) {
					return skipResult("project-library", a[0], "exists", { kind: "folder", },);
				}
			}
			await c.projectLibrary.addFolder(a[0], pk,);
			return { created: a[0], };
		},
		usage:
			"dss project-library create-folder <path> [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a folder in the project library; refuses to overwrite an existing item.",
		examples: ["dss project-library create-folder python/mylib --if-not-exists",],
	},
	put: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--expect-sha256 SHA256] [--dry-run] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			const expectSha256 = expectSha256FromFlags(f,);
			validateLibraryPath(a[0],);
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
				? fs.readFileSync(f["file"] as string,)
				: await readStdinText();
			const result = await c.projectLibrary.addOrUpdateFile(
				a[0],
				content,
				pk,
				expectSha256 !== undefined ? { expectSha256, } : undefined,
			);
			return {
				updated: result.path,
				bytes: result.bytes,
				sha256: result.sha256,
				...(result.beforeSha256 !== undefined ? { beforeSha256: result.beforeSha256, } : {}),
			};
		},
		usage:
			"dss project-library put <path> (--content TEXT|--file PATH|--stdin) [--expect-sha256 SHA256] [--dry-run] [--project-key KEY]",
		description:
			"Create or overwrite a project library file with text or binary content; reports the written byte count and sha256.",
		examples: [
			"dss project-library put python/mylib/utils.py --file ./utils.py",
			"dss project-library put python/mylib/utils.py --file ./utils.py --expect-sha256 <previous-sha256>",
		],
	},
	diff: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss project-library diff <path> (--content TEXT|--file PATH|--stdin) [--max-lines N] [--project-key KEY]",
			);
			const pk = f["project-key"] as string | undefined;
			validateLibraryPath(a[0],);
			const maxLines = numFlag(f, ["max-lines",],);
			const local: string | Uint8Array = typeof f["content"] === "string"
				? f["content"] as string
				: typeof f["file"] === "string"
				? fs.readFileSync(f["file"] as string,)
				: await readStdinText();
			return c.projectLibrary.diffFile(
				a[0],
				local,
				pk,
				maxLines !== undefined ? { maxLines, } : {},
			);
		},
		usage:
			"dss project-library diff <path> (--content TEXT|--file PATH|--stdin) [--max-lines N] [--project-key KEY]",
		description:
			"Diff a project library file against local content; capped unified text diff, binary files detected instead of dumped.",
		examples: [
			"dss project-library diff python/mylib/utils.py --file ./utils.py",
			"dss project-library diff python/mylib/utils.py --content 'print(1)' --max-lines 50",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss project-library delete <path> [--dry-run] [--project-key KEY]",);
			const pk = f["project-key"] as string | undefined;
			validateLibraryPath(a[0],);
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
			const validPath = validateLibraryPath(a[0],);
			const validName = validateLibraryName(a[1],);
			if (f["dry-run"] === true) {
				return projectLibraryPlan("rename", {
					method: "POST",
					endpoint: projectLibraryActionEndpoint(c, pk, "rename/",),
					identifiers: { path: validPath, newName: validName, },
					payload: { oldPath: `/${validPath}`, newName: validName, },
				},);
			}
			await c.projectLibrary.rename(validPath, validName, pk,);
			return { renamed: validPath, to: validName, };
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
			const validPath = validateLibraryPath(a[0],);
			const destination = validateLibraryDestinationPath(a[1],);
			if (f["dry-run"] === true) {
				return projectLibraryPlan("move", {
					method: "POST",
					endpoint: projectLibraryActionEndpoint(c, pk, "move",),
					identifiers: { path: validPath, destinationFolder: destination, },
					payload: {
						oldPath: `/${validPath}`,
						newPath: destination,
					},
				},);
			}
			await c.projectLibrary.move(validPath, destination, pk,);
			return { moved: validPath, to: destination, };
		},
		usage: "dss project-library move <path> <destination-folder> [--dry-run] [--project-key KEY]",
		description: "Move a project library file or folder into another folder.",
		examples: ["dss project-library move python/old.py python/mylib",],
	},
};

export type { ProjectLibraryDiffResult, };
