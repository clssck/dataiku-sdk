import { ClientValidationError, DataikuError, } from "../errors.js";
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

	/** Save the manifest for a Dataiku App template project (rejects classic app-instance projects). */
	async saveInstanceManifest(
		manifest: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const currentManifest = await this.getInstanceManifest(projectKey,);
		if (currentManifest.projectAppType === "APP_INSTANCE") {
			throw new ClientValidationError(
				"Classic Dataiku App instance manifests cannot be saved through the app-manifest endpoint; only Dataiku App template project manifests can be updated.",
				"validation_failed",
				"Save against the Dataiku App template project instead; existing classic app-instance manifests are read-only through this endpoint.",
				{ projectAppType: "APP_INSTANCE", projectKey: this.resolveProjectKey(projectKey,), },
			);
		}
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/app-manifest`,
			manifest,
		);
	}

	/** Delete an app instance project. */
	async deleteInstance(projectKey?: string,): Promise<void> {
		let manifest: Record<string, unknown> | undefined;
		try {
			manifest = await this.getInstanceManifest(projectKey,);
		} catch (error) {
			// DSS answers the app-manifest probe with 400 "neither an app template nor an app
			// instance" for ordinary projects; treat only that as "not an app instance". Rethrow
			// everything else (404 not-found, transient, permission) so real failures are not masked.
			if (error instanceof DataikuError && error.status === 400) {
				manifest = undefined;
			} else {
				throw error;
			}
		}
		if (manifest?.projectAppType !== "APP_INSTANCE") {
			throw new ClientValidationError(
				"Only classic Dataiku App instance projects can be deleted through app delete-instance.",
				"validation_failed",
				"Use `dss app delete-instance` only for projects whose app manifest has projectAppType=APP_INSTANCE; use `dss project delete` for ordinary projects.",
				{
					projectAppType: manifest?.projectAppType ?? null,
					projectKey: this.resolveProjectKey(projectKey,),
				},
			);
		}
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
