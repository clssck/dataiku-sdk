import { UsageError, } from "../cli/usage.js";
import { BaseResource, } from "./base.js";

export interface SavedModelListItem extends Record<string, unknown> {
	id?: string;
	name?: string;
	projectKey?: string;
	activeVersion?: string;
}

export interface SavedModel extends Record<string, unknown> {
	id?: string;
	name?: string;
	projectKey?: string;
	activeVersion?: string;
}

export interface SavedModelVersionListItem extends Record<string, unknown> {
	id?: string;
	active?: boolean;
	createdOn?: number;
}

export interface SavedModelVersionDetails extends Record<string, unknown> {
	id?: string;
	algorithm?: string;
	predictionType?: string;
}
export interface SavedModelActionResult extends Record<string, unknown> {
	message?: string;
}

function requireNonEmpty(value: string, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`${name} must be a non-empty string.`, "validation_failed",);
	}
	return value;
}

export class SavedModelsResource extends BaseResource {
	/** List saved models in a project. */
	async list(projectKey?: string,): Promise<SavedModelListItem[]> {
		return this.client.get<SavedModelListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/savedmodels/`,
		);
	}

	/** Get saved-model settings and metadata. */
	async get(savedModelId: string, projectKey?: string,): Promise<SavedModel> {
		return this.client.get<SavedModel>(this.savedModelPath(savedModelId, projectKey,),);
	}

	/** List all versions of a saved model. */
	async listVersions(
		savedModelId: string,
		projectKey?: string,
	): Promise<SavedModelVersionListItem[]> {
		return this.client.get<SavedModelVersionListItem[]>(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions`,
		);
	}

	/** Get full details for one saved-model version. */
	async versionDetails(
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<SavedModelVersionDetails> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		return this.client.get<SavedModelVersionDetails>(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/details`,
		);
	}

	/** Make one saved-model version active. */
	async setActiveVersion(
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<SavedModelActionResult | undefined> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		return this.client.post<SavedModelActionResult | undefined>(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/actions/setActive`,
			{},
		);
	}

	/** Delete a saved model and all of its versions. */
	async delete(savedModelId: string, projectKey?: string,): Promise<void> {
		await this.client.del(this.savedModelPath(savedModelId, projectKey,),);
	}

	private savedModelPath(savedModelId: string, projectKey?: string,): string {
		const id = encodeURIComponent(requireNonEmpty(savedModelId, "savedModelId",),);
		return `/public/api/projects/${this.enc(projectKey,)}/savedmodels/${id}`;
	}
}
