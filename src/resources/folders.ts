import { createWriteStream, } from "node:fs";
import { resolve, } from "node:path";
import { Readable, } from "node:stream";
import { pipeline, } from "node:stream/promises";
import type { FolderCreateOptions, FolderDetails, FolderItem, FolderSummary, } from "../schemas.js";
import {
	FolderDetailsSchema,
	FolderItemArraySchema,
	FolderSummaryArraySchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";
import { resolveAdminManagedStorageConnection, } from "./connections.js";

function normalizeRemotePath(path: string,): string {
	return path.replace(/\\/g, "/",);
}

function inferDownloadFileName(remotePath: string,): string {
	const segments = remotePath.split("/",).filter(Boolean,);
	const last = segments[segments.length - 1] ?? "file";
	return sanitizeFileName(last, "file",);
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

export class FoldersResource extends BaseResource {
	async create(opts: FolderCreateOptions,): Promise<FolderDetails> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const path = opts.path?.trim() || "/${projectKey}/${odbId}";
		const connection = opts.connection
			?? await resolveAdminManagedStorageConnection(
				this.client,
				"allowManagedFolders",
				"filesystem_folders",
			)
			?? "filesystem_folders";
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/managedfolders/`,
			{
				name: opts.name,
				projectKey: pk,
				type: opts.type ?? null,
				params: {
					...opts.params,
					connection: connection,
					path,
				},
			},
		);
		return this.client.safeParse(FolderDetailsSchema, raw, "folders.create",);
	}

	async list(projectKey?: string,): Promise<FolderSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/managedfolders/`,
		);
		return this.client.safeParse(FolderSummaryArraySchema, raw, "folders.list",);
	}

	async resolveId(nameOrId: string, projectKey?: string,): Promise<string> {
		const folders = await this.list(projectKey,);
		if (folders.some((folder,) => folder.id === nameOrId)) {
			return nameOrId;
		}
		const match = folders.find((folder,) => folder.name === nameOrId);
		return match?.id ?? nameOrId;
	}

	async get(folderId: string, projectKey?: string,): Promise<FolderDetails> {
		const fEnc = encodeURIComponent(folderId,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/managedfolders/${fEnc}`,
		);
		return this.client.safeParse(FolderDetailsSchema, raw, "folders.get",);
	}

	async update(
		folderId: string,
		data: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const fEnc = encodeURIComponent(folderId,);
		const pkEnc = this.enc(projectKey,);
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/managedfolders/${fEnc}`,
		);
		const merged = deepMerge(current, data,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/managedfolders/${fEnc}`,
			merged,
		);
	}

	async contents(folderId: string, opts?: { projectKey?: string; },): Promise<FolderItem[]> {
		const fEnc = encodeURIComponent(folderId,);
		const response = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/managedfolders/${fEnc}/contents/`,
		);
		const items = (response as Record<string, unknown>).items ?? [];
		return this.client.safeParse(FolderItemArraySchema, items, "folders.contents",);
	}

	async download(
		folderId: string,
		path: string,
		opts?: { localPath?: string; projectKey?: string; },
	): Promise<string> {
		const fEnc = encodeURIComponent(folderId,);
		const normalizedPath = normalizeRemotePath(path,);
		const pEnc = encodeURIComponent(normalizedPath,);
		const res = await this.client.stream(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/managedfolders/${fEnc}/contents/${pEnc}`,
		);
		const dest = opts?.localPath ?? resolve(process.cwd(), inferDownloadFileName(normalizedPath,),);
		const nodeStream = Readable.fromWeb(res.body as unknown as import("stream/web").ReadableStream,);
		const fileOut = createWriteStream(dest,);
		await pipeline(nodeStream, fileOut,);
		return dest;
	}

	upload(folderId: string, path: string, localPath: string, projectKey?: string,): Promise<void> {
		const fEnc = encodeURIComponent(folderId,);
		const normalizedPath = normalizeRemotePath(path,);
		const pEnc = encodeURIComponent(normalizedPath,);
		return this.client.upload(
			`/public/api/projects/${this.enc(projectKey,)}/managedfolders/${fEnc}/contents/${pEnc}`,
			localPath,
		);
	}

	deleteFile(folderId: string, path: string, projectKey?: string,): Promise<void> {
		const fEnc = encodeURIComponent(folderId,);
		const normalizedPath = normalizeRemotePath(path,);
		const pEnc = encodeURIComponent(normalizedPath,);
		return this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/managedfolders/${fEnc}/contents/${pEnc}`,
		);
	}

	delete(folderId: string, projectKey?: string,): Promise<void> {
		const fEnc = encodeURIComponent(folderId,);
		return this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/managedfolders/${fEnc}`,
		);
	}
}
