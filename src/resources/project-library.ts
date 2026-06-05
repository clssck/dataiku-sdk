import { BaseResource, } from "./base.js";

export interface ProjectLibraryItem {
	name: string;
	children?: ProjectLibraryItem[];
}

export interface ProjectLibraryFileContent {
	data: string;
}

export interface ProjectLibraryRenameRequest {
	oldPath: string;
	newName: string;
}

export interface ProjectLibraryMoveRequest {
	oldPath: string;
	newPath: string;
}

type RawBodyClient = {
	baseUrl: string;
	fetchWithRetry(url: string, init: RequestInit,): Promise<Response>;
	getAnyHeaders(): Record<string, string>;
};

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
	const normalized = normalizeLibraryPath(path,);
	return normalized
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

export class ProjectLibraryResource extends BaseResource {
	/** List the full project code-library contents tree. */
	async listContents(projectKey?: string,): Promise<ProjectLibraryItem[]> {
		return this.client.get<ProjectLibraryItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents`,
		);
	}

	/** Read a project code-library file as text. */
	async getFile(path: string, projectKey?: string,): Promise<string> {
		const res = await this.client.get<ProjectLibraryFileContent>(
			this.contentsPath(path, projectKey,),
		);
		return res.data;
	}

	/** Read a project code-library file as bytes. */
	async getFileBytes(path: string, projectKey?: string,): Promise<Uint8Array> {
		const res = await this.client.get<ProjectLibraryFileContent>(
			`${this.contentsPath(path, projectKey,)}?dataEncoding=base64`,
		);
		return Buffer.from(res.data, "base64",);
	}

	/** Create an empty project code-library file. */
	async addFile(path: string, projectKey?: string,): Promise<void> {
		await this.client.postText(this.contentsPath(path, projectKey,),);
	}

	/** Create a project code-library folder. */
	async addFolder(path: string, projectKey?: string,): Promise<void> {
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/folders/${encodeLibraryPath(path,)}`,
		);
	}

	/** Create or replace a project code-library file with raw text content. */
	async addOrUpdateFile(path: string, content: string, projectKey?: string,): Promise<void> {
		await this.postRawText(this.contentsPath(path, projectKey,), content,);
	}

	/** Delete a project code-library file. */
	async deleteFile(path: string, projectKey?: string,): Promise<void> {
		await this.client.del(this.contentsPath(path, projectKey,),);
	}

	/** Rename a project code-library file or folder within its current parent folder. */
	async rename(path: string, newName: string, projectKey?: string,): Promise<void> {
		const body: ProjectLibraryRenameRequest = {
			oldPath: `/${normalizeLibraryPath(path,)}`,
			newName,
		};
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents-actions/rename/`,
			body,
		);
	}

	/** Move a project code-library file or folder into another library folder. */
	async move(path: string, destinationFolderPath: string, projectKey?: string,): Promise<void> {
		const body: ProjectLibraryMoveRequest = {
			oldPath: `/${normalizeLibraryPath(path,)}`,
			newPath: normalizeLibraryDestinationPath(destinationFolderPath,),
		};
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents-actions/move`,
			body,
		);
	}

	private contentsPath(path: string, projectKey?: string,): string {
		const projectKeyPart = this.enc(projectKey,);
		return `/public/api/projects/${projectKeyPart}/libraries/contents/${encodeLibraryPath(path,)}`;
	}

	private async postRawText(path: string, content: string,): Promise<void> {
		// DSS library writes use dataikuapi's raw_body; route through the client's
		// transport so auth, retries, TLS options, and DSS error handling stay aligned.
		const rawClient = this.client as unknown as RawBodyClient;
		const res = await rawClient.fetchWithRetry(`${rawClient.baseUrl}${path}`, {
			method: "POST",
			headers: rawClient.getAnyHeaders(),
			body: content,
		},);
		await res.text();
	}
}
