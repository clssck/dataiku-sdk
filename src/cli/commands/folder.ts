import type { DataikuClient, } from "../../client.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, } from "../coerce.js";
import {
	addTransientTargetContext,
	encodedProjectEndpoint,
	readIfExists,
	skipResult,
} from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

async function resolveFolderId(
	client: DataikuClient,
	nameOrId: string,
	flags: Record<string, string | boolean>,
): Promise<string> {
	return client.folders.resolveId(nameOrId, flags["project-key"] as string | undefined,);
}

export const folderCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
		usage: "dss folder list [--project-key KEY]",
		description: "List managed folders in a project.",
		examples: ["dss folder list",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const name = f["name"] as string | undefined;
			const type = f["type"] as string | undefined;
			const connection = f["connection"] as string | undefined;
			const pk = f["project-key"] as string | undefined;
			if (!name || !type || !connection) {
				throw new UsageError(
					"--name, --type, and --connection are required. Usage: dss folder create --name NAME --type TYPE --connection CONN [--path PATH]",
				);
			}
			const payload = {
				name,
				type,
				connection,
				path: f["path"] as string | undefined,
				projectKey: pk,
			};
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const list = await c.folders.list(pk,);
				const existing = list.find((folder,) => folder.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("folder", existing.id ?? name, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "folder",
						name,
						payload,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.folders.create(payload,);
			return { created: created.id ?? name, resource: "folder", ...created, };
		},
		usage:
			"dss folder create --name NAME --type TYPE --connection CONN [--path PATH] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a managed folder.",
		examples: [
			"dss folder create --name exports --type S3 --connection s3_connection",
			"dss folder create --name exports --type S3 --connection s3_connection --path /dataiku/MYPROJ/exports --dry-run",
		],
	},
	get: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss folder get <name-or-id>",);
			return c.folders.get(
				await resolveFolderId(c, a[0], f,),
				f["project-key"] as string | undefined,
			);
		},
		usage: "dss folder get <name-or-id> [--project-key KEY]",
		description: "Get managed folder settings.",
		examples: ["dss folder get my_folder",],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss folder update <name-or-id> [--data '{...}' | --data-file PATH | --stdin]",
			);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required. Usage: dss folder update <name-or-id> [--data '{...}' | --data-file PATH | --stdin]",
				);
			}
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (f["dry-run"] === true) {
				const current = await c.folders.get(folderId, pk,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data,);
				return {
					dryRun: true,
					action: "update",
					resource: "folder",
					folder: a[0],
					folderId,
					current,
					next,
				};
			}
			await c.folders.update(folderId, data, pk,);
			return { updated: folderId, resource: "folder", };
		},
		usage:
			"dss folder update <name-or-id> [--data JSON | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
		description: "Update managed folder settings by deep-merging a JSON patch.",
		examples: [
			'dss folder update exports --data \'{"tags":["agent"]}\' --dry-run',
			"dss folder update exports --data-file folder-patch.json",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss folder delete <name-or-id>",);
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.folders.get(folderId, pk,));
				if (!current) return skipResult("folder", folderId, "missing",);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "delete",
						resource: "folder",
						folder: a[0],
						folderId,
						current,
					};
				}
			}
			await c.folders.delete(folderId, pk,);
			return { deleted: folderId, resource: "folder", };
		},
		usage: "dss folder delete <name-or-id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a managed folder.",
		examples: ["dss folder delete exports --if-exists",],
	},
	contents: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss folder contents <name-or-id>",);
			const startedAt = Date.now();
			let folderId = a[0];
			try {
				folderId = await resolveFolderId(c, a[0], f,);
				return await c.folders.contents(folderId, {
					projectKey: f["project-key"] as string | undefined,
				},);
			} catch (error) {
				addTransientTargetContext(error, `folder:${folderId}`, Date.now() - startedAt,);
			}
		},
		usage:
			"dss folder contents <name-or-id> [--retries N] [--request-timeout MS] [--project-key KEY]",
		description: "List files in a managed folder.",
		examples: [
			"dss folder contents my_folder",
			"dss folder contents my_folder --retries 8 --request-timeout 60000",
		],
	},
	download: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss folder download <name-or-id> <remote-path> [local-path]",);
			const localPath = (a[2] as string | undefined) ?? (f["output"] as string | undefined);
			return c.folders.download(await resolveFolderId(c, a[0], f,), a[1], {
				localPath,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss folder download <name-or-id> <remote-path> [local-path] [--output PATH] [--project-key KEY]",
		description: "Download a file from a managed folder.",
		examples: [
			"dss folder download my_folder /data/report.csv",
			"dss folder download my_folder /data/report.csv ./report.csv",
		],
	},
	upload: {
		handler: async (c, a, f,) => {
			requireArgs(a, 3, "dss folder upload <name-or-id> <path> <localPath>",);
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "upload",
					resource: "folder",
					folder: a[0],
					folderId,
					path: a[1],
					localPath: a[2],
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/managedfolders/${encodeURIComponent(folderId,)}/contents/${encodeURIComponent(a[1],)}`,
					),
					method: "POST",
				};
			}
			await c.folders.upload(folderId, a[1], a[2], pk,);
			return { uploaded: a[1], folder: a[0], localPath: a[2], resource: "folder", };
		},
		usage: "dss folder upload <name-or-id> <path> <localPath> [--dry-run] [--project-key KEY]",
		description: "Upload a local file to a managed folder.",
		examples: ["dss folder upload my_folder /data/report.csv ./report.csv --dry-run",],
	},
	"delete-file": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss folder delete-file <name-or-id> <path>",);
			if (f["dry-run"] === true) {
				return { dryRun: true, action: "delete-file", resource: "folder", folder: a[0], path: a[1], };
			}
			await c.folders.deleteFile(
				await resolveFolderId(c, a[0], f,),
				a[1],
				f["project-key"] as string | undefined,
			);
			return { deleted: a[1], folder: a[0], resource: "folder", };
		},
		usage: "dss folder delete-file <name-or-id> <path> [--dry-run] [--project-key KEY]",
		description: "Delete a file from a managed folder.",
		examples: ["dss folder delete-file my_folder /data/report.csv",],
	},
};
