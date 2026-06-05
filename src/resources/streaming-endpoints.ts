import { BaseResource, } from "./base.js";

export interface StreamingEndpointSchemaColumn {
	name: string;
	type: string;
	length?: number;
	comment?: string;
	[key: string]: unknown;
}

export interface StreamingEndpointSchema {
	columns: StreamingEndpointSchemaColumn[];
	[key: string]: unknown;
}

export interface StreamingEndpointListItem {
	id: string;
	projectKey: string;
	type: string;
	schema?: StreamingEndpointSchema;
	params?: Record<string, unknown>;
	[key: string]: unknown;
}

export interface StreamingEndpointSettings {
	id: string;
	projectKey?: string;
	type: string;
	params?: Record<string, unknown>;
	schema?: StreamingEndpointSchema;
	[key: string]: unknown;
}

export interface StreamingEndpointCreateParams {
	[key: string]: unknown;
}

export class StreamingEndpointsResource extends BaseResource {
	/** List all streaming endpoints in a project. */
	async list(projectKey?: string,): Promise<StreamingEndpointListItem[]> {
		return this.client.get<StreamingEndpointListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/streamingendpoints/`,
		);
	}

	/** Get one streaming endpoint's settings. */
	async get(id: string, projectKey?: string,): Promise<StreamingEndpointSettings> {
		return this.getSettings(id, projectKey,);
	}

	/** Get one streaming endpoint's settings. */
	async getSettings(id: string, projectKey?: string,): Promise<StreamingEndpointSettings> {
		const endpointId = encodeURIComponent(id,);
		return this.client.get<StreamingEndpointSettings>(
			`/public/api/projects/${this.enc(projectKey,)}/streamingendpoints/${endpointId}`,
		);
	}

	/** Create a streaming endpoint with type-specific params. */
	async create(
		id: string,
		type: string,
		body: StreamingEndpointCreateParams,
		projectKey?: string,
	): Promise<StreamingEndpointSettings> {
		const pk = this.resolveProjectKey(projectKey,);
		return this.client.post<StreamingEndpointSettings>(
			`/public/api/projects/${this.enc(pk,)}/streamingendpoints/`,
			{
				id,
				projectKey: pk,
				type,
				params: body,
			},
		);
	}

	/** Update one streaming endpoint's raw settings. */
	async updateSettings(
		id: string,
		body: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const endpointId = encodeURIComponent(id,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/streamingendpoints/${endpointId}`,
			body,
		);
	}

	/** Delete a streaming endpoint from the flow without deleting underlying streaming data. */
	async delete(id: string, projectKey?: string,): Promise<void> {
		const endpointId = encodeURIComponent(id,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/streamingendpoints/${endpointId}`,
		);
	}
}
