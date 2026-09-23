import type { DataikuClient, } from "../../client.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import {
	addTransientTargetContext,
	encodedProjectEndpoint,
	readIfExists,
	skipResult,
} from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

async function resolveFolderId(
	client: DataikuClient,
	nameOrId: string,
	flags: Record<string, string | boolean>,
): Promise<string> {
	return client.folders.resolveId(nameOrId, flags["project-key"] as string | undefined,);
}

export const folderCommands: Record<string, CommandMeta> = withUsage("folder", {
	list: {
		handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
		description: "List managed folders in a project.",
		examples: ["dss folder list",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const name = f["name"] as string | undefined;
			const type = f["type"] as string | undefined;
			const connection = f["connection"] as string | undefined;
			const pk = f["project-key"] as string | undefined;
			if (!name) {
				throw new UsageError(
					`--name is required. Usage: ${commandUsage("folder", "create",)}`,
				);
			}
			const payload = {
				name,
				type,
				connection,
				path: f["path"] as string | undefined,
				projectKey: pk,
			};
			if (f["if-not-exists"] === true || executionMode(f,).dryRun) {
				const list = await c.folders.list(pk,);
				const existing = list.find((folder,) => folder.name === name);
				if (existing && f["if-not-exists"] === true && !executionMode(f,).dryRun) {
					return skipResult("folder", existing.id ?? name, "exists", { current: existing, },);
				}
				if (executionMode(f,).dryRun) {
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
		description:
			"Create a managed folder; when omitted, the connection is selected from writable managed-folder storage and DSS infers the folder type.",
		examples: [
			"dss folder create --name exports",
			"dss folder create --name exports --type S3 --connection s3_connection",
			"dss folder create --name exports --type S3 --connection s3_connection --path /dataiku/MYPROJ/exports --dry-run",
		],
	},
	get: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("folder", "get",),);
			return c.folders.get(
				await resolveFolderId(c, a[0], f,),
				f["project-key"] as string | undefined,
			);
		},
		description: "Get managed folder settings.",
		examples: ["dss folder get my_folder",],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("folder", "update",),
			);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					`--data, --data-file, or --stdin is required. Usage: ${commandUsage("folder", "update",)}`,
				);
			}
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (executionMode(f,).dryRun) {
				const current = await c.folders.get(folderId, pk,);
				const next = deepMerge(current, data,);
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
		description: "Update managed folder settings by deep-merging a JSON patch.",
		examples: [
			'dss folder update exports --data \'{"tags":["agent"]}\' --dry-run',
			"dss folder update exports --data-file folder-patch.json",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("folder", "delete",),);
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.folders.get(folderId, pk,));
				if (!current) return skipResult("folder", folderId, "missing",);
				if (executionMode(f,).dryRun) {
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
		description: "Delete a managed folder.",
		examples: ["dss folder delete exports --if-exists",],
	},
	contents: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("folder", "contents",),);
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
		description: "List files in a managed folder.",
		examples: [
			"dss folder contents my_folder",
			"dss folder contents my_folder --retries 8 --request-timeout 60000",
		],
	},
	download: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("folder", "download",),);
			const localPath = (a[2] as string | undefined) ?? (f["output"] as string | undefined);
			return c.folders.download(await resolveFolderId(c, a[0], f,), a[1], {
				localPath,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Download a file from a managed folder.",
		examples: [
			"dss folder download my_folder /data/report.csv",
			"dss folder download my_folder /data/report.csv ./report.csv",
		],
	},
	upload: {
		handler: async (c, a, f,) => {
			requireArgs(a, 3, commandUsage("folder", "upload",),);
			const pk = f["project-key"] as string | undefined;
			const folderId = await resolveFolderId(c, a[0], f,);
			if (executionMode(f,).dryRun) {
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
		description: "Upload a local file to a managed folder.",
		examples: ["dss folder upload my_folder /data/report.csv ./report.csv --dry-run",],
	},
	"delete-file": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("folder", "delete-file",),);
			if (executionMode(f,).dryRun) {
				return { dryRun: true, action: "delete-file", resource: "folder", folder: a[0], path: a[1], };
			}
			await c.folders.deleteFile(
				await resolveFolderId(c, a[0], f,),
				a[1],
				f["project-key"] as string | undefined,
			);
			return { deleted: a[1], folder: a[0], resource: "folder", };
		},
		description: "Delete a file from a managed folder.",
		examples: ["dss folder delete-file my_folder /data/report.csv",],
	},
},);
