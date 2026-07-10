import { UsageError, } from "../cli/usage.js";
import { BaseResource, } from "./base.js";

export interface ModelEvaluationStoreListItem extends Record<string, unknown> {
	id?: string;
	name?: string;
	projectKey?: string;
	mesFlavor?: string;
}

export interface ModelEvaluationStore extends Record<string, unknown> {
	id?: string;
	name?: string;
	projectKey?: string;
	mesFlavor?: string;
}

export interface ModelEvaluationStoreCreateOptions {
	name: string;
	projectKey?: string;
}

export interface ModelEvaluationStoreCreateResult extends Record<string, unknown> {
	id?: string;
	name?: string;
	projectKey?: string;
}

export interface ModelEvaluationListItem extends Record<string, unknown> {
	id?: string;
	evaluationId?: string;
	createdOn?: number;
	label?: string;
}

function requireNonEmpty(value: string, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`${name} must be a non-empty string.`, "validation_failed",);
	}
	return value;
}

export class ModelEvaluationStoresResource extends BaseResource {
	/** List model evaluation stores in a project. */
	async list(projectKey?: string,): Promise<ModelEvaluationStoreListItem[]> {
		return this.client.get<ModelEvaluationStoreListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/evaluationstores/`,
		);
	}

	/** Get model evaluation store settings and metadata. */
	async get(
		modelEvaluationStoreId: string,
		projectKey?: string,
	): Promise<ModelEvaluationStore> {
		return this.client.get<ModelEvaluationStore>(
			this.storePath(modelEvaluationStoreId, projectKey,),
		);
	}

	/** Create a tabular model evaluation store. */
	async create(
		opts: ModelEvaluationStoreCreateOptions,
	): Promise<ModelEvaluationStoreCreateResult> {
		const name = requireNonEmpty(opts.name, "name",);
		const projectKey = this.resolveProjectKey(opts.projectKey,);
		return this.client.post<ModelEvaluationStoreCreateResult>(
			`/public/api/projects/${encodeURIComponent(projectKey,)}/evaluationstores/?flavor=TABULAR`,
			{ projectKey, name, },
		);
	}

	/** List evaluations in a model evaluation store. */
	async listEvaluations(
		modelEvaluationStoreId: string,
		projectKey?: string,
	): Promise<ModelEvaluationListItem[]> {
		return this.client.get<ModelEvaluationListItem[]>(
			`${this.storePath(modelEvaluationStoreId, projectKey,)}/evaluations/`,
		);
	}

	/** Delete a model evaluation store and its evaluations. */
	async delete(modelEvaluationStoreId: string, projectKey?: string,): Promise<void> {
		await this.client.del(this.storePath(modelEvaluationStoreId, projectKey,),);
	}

	private storePath(modelEvaluationStoreId: string, projectKey?: string,): string {
		const id = encodeURIComponent(
			requireNonEmpty(modelEvaluationStoreId, "modelEvaluationStoreId",),
		);
		return `/public/api/projects/${this.enc(projectKey,)}/evaluationstores/${id}`;
	}
}
