import { BaseResource, } from "./base.js";

export interface WebappListItem {
	id?: string;
	webAppId?: string;
	name?: string;
	type?: string;
	projectKey?: string;
}

export interface WebappCreateResult {
	webAppId: string;
}

export class WebappsResource extends BaseResource {
	/** List all webapps in a project. */
	async list(projectKey?: string,): Promise<WebappListItem[]> {
		return this.client.get<WebappListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/`,
		);
	}

	/** Get one webapp's settings. */
	async getSettings(webappId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}`,
		);
	}

	/** Create a webapp. */
	async create(body: Record<string, unknown>, projectKey?: string,): Promise<WebappCreateResult> {
		return this.client.post<WebappCreateResult>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/`,
			body,
		);
	}

	/** Update one webapp's settings. */
	async updateSettings(
		webappId: string,
		body: Record<string, unknown>,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		return this.client.put<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}`,
			body,
		);
	}

	/** Stop a webapp backend. */
	async stopBackend(webappId: string, projectKey?: string,): Promise<void> {
		const id = encodeURIComponent(webappId,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/actions/stop`,
			{},
		);
	}

	/** Start or restart a webapp backend. */
	async startOrRestartBackend(webappId: string, projectKey?: string,): Promise<void> {
		const id = encodeURIComponent(webappId,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/actions/restart`,
			{},
		);
	}

	/** Get a webapp backend's state. */
	async getBackendState(webappId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/state`,
		);
	}
}
