import { BaseResource, } from "./base.js";

export interface AppListItem extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	name?: string;
}

export interface AppInstanceRef extends Record<string, unknown> {
	appId?: string;
	projectKey?: string;
	jobId?: string;
}

export interface BusinessAppListItem extends Record<string, unknown> {
	id?: string;
	projectKey?: string;
	name?: string;
}

export interface BusinessAppInstanceUserPermissions extends Record<string, unknown> {
	login?: string;
	admin?: boolean;
	readProjectContent?: boolean;
	writeProjectContent?: boolean;
}

export class ApplicationsResource extends BaseResource {
	/** List all Dataiku Apps. */
	async listApps(): Promise<AppListItem[]> {
		return this.client.get<AppListItem[]>("/public/api/apps/",);
	}

	/** Get the manifest for a Dataiku App template. */
	async getAppManifest(appId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/apps/${encodeURIComponent(appId,)}/`,
		);
	}

	/** List instances created from a Dataiku App template. */
	async listInstances(appId: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances/`,
		);
	}

	/** Create an instance from a Dataiku App template. */
	async createInstance(appId: string, body: Record<string, unknown>,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/apps/${encodeURIComponent(appId,)}/instances`,
			body,
		);
	}

	/** Get the manifest for an app instance project. */
	async getInstanceManifest(projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
		);
	}

	/** Save the manifest for an app instance project. */
	async saveInstanceManifest(
		manifest: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
			manifest,
		);
	}

	/** Delete an app instance project. */
	async deleteInstance(projectKey?: string,): Promise<void> {
		await this.client.del(`/public/api/projects/${this.enc(projectKey,)}`,);
	}

	/** List all Business Apps. */
	async listBusinessApps(): Promise<BusinessAppListItem[]> {
		return this.client.get<BusinessAppListItem[]>("/public/api/business-apps/",);
	}

	/** Get Business App details. */
	async getBusinessApp(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}`,
		);
	}

	/** Get Business App settings. */
	async getBusinessAppSettings(id: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
		);
	}

	/** Save Business App settings. */
	async saveBusinessAppSettings(id: string, body: Record<string, unknown>,): Promise<void> {
		await this.client.putVoid(
			`/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
			body,
		);
	}

	/** List instances of a Business App. */
	async listBusinessAppInstances(id: string,): Promise<AppInstanceRef[]> {
		return this.client.get<AppInstanceRef[]>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
		);
	}

	/** Create an instance of a Business App. */
	async createBusinessAppInstance(
		id: string,
		body: Record<string, unknown>,
	): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
			body,
		);
	}

	/** Upgrade a Business App instance to the latest version. */
	async upgradeBusinessAppInstance(id: string, projectKey: string,): Promise<AppInstanceRef> {
		return this.client.post<AppInstanceRef>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/upgrade`,
			{},
		);
	}

	/** Get a user's effective permissions on a Business App instance. */
	async getBusinessAppInstanceUserPermissions(
		id: string,
		projectKey: string,
		user: string,
	): Promise<BusinessAppInstanceUserPermissions> {
		return this.client.get<BusinessAppInstanceUserPermissions>(
			`/public/api/business-apps/${encodeURIComponent(id,)}/instances/${
				encodeURIComponent(projectKey,)
			}/permissions/${encodeURIComponent(user,)}`,
		);
	}

	/** Install or upgrade a Business App from an archive. */
	async installBusinessAppFromArchive(filePath: string,): Promise<void> {
		await this.client.upload("/public/api/business-apps/install-from-archive", filePath,);
	}
}
