import { createWriteStream, } from "node:fs";
import { resolve, } from "node:path";
import { Readable, } from "node:stream";
import { pipeline, } from "node:stream/promises";
import type { FolderDetails, FolderItem, FolderSummary, } from "../schemas.js";
import {
	FolderDetailsSchema,
	FolderItemArraySchema,
	FolderSummaryArraySchema,
} from "../schemas.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";

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
}
