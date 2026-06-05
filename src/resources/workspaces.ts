import { BaseResource, } from "./base.js";

export type WorkspaceObjectReferenceType = "DATASET" | "ARTICLE" | "DASHBOARD" | (string & {});

export interface WorkspacePermissionItem extends Record<string, unknown> {
	group?: string;
	user?: string;
	admin?: boolean;
	write?: boolean;
	read?: boolean;
}

export interface WorkspaceListItem extends Record<string, unknown> {
	workspaceKey?: string;
	displayName?: string;
	color?: string | null;
	description?: string | null;
}

export interface WorkspaceSettings extends WorkspaceListItem {
	permissions?: WorkspacePermissionItem[] | null;
}

export interface WorkspaceCreateRequest extends Record<string, unknown> {
	workspaceKey: string;
	displayName: string;
	color?: string | null;
	description?: string | null;
	permissions?: WorkspacePermissionItem[] | null;
}

export interface WorkspaceObjectReference extends Record<string, unknown> {
	projectKey?: string;
	type?: WorkspaceObjectReferenceType;
	id?: string;
	workspaceKey?: string;
}

export interface WorkspaceHtmlLinkObject extends Record<string, unknown> {
	name: string;
	url: string;
	description?: string | null;
}

export interface WorkspaceObject extends Record<string, unknown> {
	id?: string;
	reference?: WorkspaceObjectReference;
	appId?: string;
	htmlLink?: WorkspaceHtmlLinkObject;
}

export type WorkspaceObjectInput = WorkspaceObject;

export class WorkspacesResource extends BaseResource {
	/** List workspaces visible to the API key. */
	async list(): Promise<WorkspaceListItem[]> {
		return this.client.get<WorkspaceListItem[]>("/public/api/workspaces/",);
	}

	/** Get workspace settings. */
	async get(workspaceKey: string,): Promise<WorkspaceSettings> {
		return this.client.get<WorkspaceSettings>(
			`/public/api/workspaces/${encodeURIComponent(workspaceKey,)}`,
		);
	}

	/** Create a workspace. DSS returns a text confirmation. */
	async create(body: WorkspaceCreateRequest,): Promise<string> {
		return this.client.postText("/public/api/workspaces/", body,);
	}

	/** Replace workspace settings. */
	async updateSettings(workspaceKey: string, body: WorkspaceSettings,): Promise<void> {
		await this.client.putVoid(
			`/public/api/workspaces/${encodeURIComponent(workspaceKey,)}`,
			body,
		);
	}

	/** Delete a workspace. */
	async delete(workspaceKey: string,): Promise<void> {
		await this.client.del(`/public/api/workspaces/${encodeURIComponent(workspaceKey,)}`,);
	}

	/** List objects in a workspace. */
	async listObjects(workspaceKey: string,): Promise<WorkspaceObject[]> {
		return this.client.get<WorkspaceObject[]>(
			`/public/api/workspaces/${encodeURIComponent(workspaceKey,)}/objects`,
		);
	}

	/** Add an object to a workspace. */
	async addObject(workspaceKey: string, object: WorkspaceObjectInput,): Promise<WorkspaceObject> {
		return this.client.post<WorkspaceObject>(
			`/public/api/workspaces/${encodeURIComponent(workspaceKey,)}/objects`,
			object,
		);
	}
}
