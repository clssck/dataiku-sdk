import { BaseResource, } from "./base.js";

/**
 * Data collections are instance-level curated groupings of DSS objects
 * (datasets, saved models, agents, fine-tuned models, agent tools, semantic
 * models) shared across projects.
 *
 * @see https://doc.dataiku.com/dss/api/15/rest/ (Data Collections section)
 */

export type DataCollectionObjectReferenceType =
	| "DATASET"
	| "SAVED_MODEL"
	| "FINE_TUNED_MODEL"
	| "AGENT"
	| "AGENT_TOOL"
	| "SEMANTIC_MODEL"
	| (string & {});

export interface DataCollectionPermission extends Record<string, unknown> {
	/** Name of the user this permission applies to (exclusive with group). */
	user?: string;
	/** Name of the group this permission applies to (exclusive with user). */
	group?: string;
	/** true if the permission grants administrator access. */
	admin?: boolean;
	/** true if the permission grants contributor access. */
	write?: boolean;
	/** true if the permission grants member access. */
	read?: boolean;
}

export interface DataCollectionSummary extends Record<string, unknown> {
	/** Name of the collection. */
	displayName?: string;
	/** Color of the collection (#RRGBB syntax). */
	color?: string | null;
	/** The description of the collection (markdown). */
	description?: string | null;
	/** List of tags of the collection. */
	tags?: string[] | null;
	/** Id of the collection. */
	id?: string;
	/** Number of objects in the collection. */
	itemCount?: number;
	/** Number of datasets in the collection. */
	datasetsCount?: number;
	/** Number of saved models in the collection, excluding agents and fine tuned models. */
	savedModelsCount?: number;
	/** Number of fine tuned models in the collection. */
	fineTunedModelsCount?: number;
	/** Number of agents in the collection. */
	agentsCount?: number;
	/** Number of agent tools in the collection. */
	agentToolsCount?: number;
	/** Number of semantic models in the collection. */
	semanticModelsCount?: number;
	/** Timestamp of the last collection modification. */
	lastModifiedOn?: number;
}

export interface DataCollectionSettings extends Record<string, unknown> {
	/** Id of the collection. */
	id?: string;
	/** Name of the collection. */
	displayName: string;
	/** Color of the collection (#RRGGBB syntax). */
	color?: string | null;
	/** The description of the collection (markdown). */
	description?: string | null;
	/** List of tags of the collection. */
	tags?: string[] | null;
	/** List of permissions of the collection (READER, CONTRIBUTOR or ADMIN). */
	permissions?: DataCollectionPermission[] | null;
}

export interface DataCollectionCreateRequest extends Omit<DataCollectionSettings, "id"> {
	/**
	 * Id of the Collection (a random id is generated if not provided).
	 * Must match the [A-Za-z0-9]{8} server-side format when provided.
	 */
	id?: string;
}

/**
 * An object inside a collection: a reference to a DSS object hosted by a
 * project. The documented shape covers datasets; `type` accepts the other
 * collection member kinds the instance may report.
 */
export interface DataCollectionObject extends Record<string, unknown> {
	/** The type of this object (e.g. "DATASET"). */
	type: DataCollectionObjectReferenceType;
	/** The key of the project hosting the object. */
	projectKey: string;
	/** The id (name) of the object in its project. */
	id: string;
}

export class DataCollectionsResource extends BaseResource {
	/** List the collections on which the API key has READ privilege. */
	async list(): Promise<DataCollectionSummary[]> {
		return this.client.get<DataCollectionSummary[]>("/public/api/data-collections/",);
	}

	/**
	 * Create a collection. The creating user is added as an administrator.
	 * Requires the mayCreateDataCollections global permission. DSS returns
	 * `{ msg, id }`; the created id is surfaced to the caller.
	 */
	async create(
		body: DataCollectionCreateRequest,
	): Promise<{ msg?: string; id?: string; }> {
		return this.client.post<{ msg?: string; id?: string; }>(
			"/public/api/data-collections/",
			body,
		);
	}

	/**
	 * Get the settings of a collection. Requires READ on the collection; the
	 * permissions array is omitted unless the caller is an ADMIN.
	 */
	async get(dataCollectionId: string,): Promise<DataCollectionSettings> {
		return this.client.get<DataCollectionSettings>(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}`,
		);
	}

	/**
	 * Update the settings of a collection. Only set a settings object obtained
	 * through {@link get}. Requires ADMIN on the collection.
	 */
	async update(
		dataCollectionId: string,
		settings: DataCollectionSettings,
	): Promise<DataCollectionSettings> {
		return this.client.put<DataCollectionSettings>(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}`,
			settings,
		);
	}

	/** Permanently delete a collection. Requires ADMIN on the collection. */
	async delete(dataCollectionId: string,): Promise<void> {
		await this.client.del(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}`,
		);
	}

	/** List the objects in a collection. Requires READ on the collection. */
	async listObjects(
		dataCollectionId: string,
	): Promise<DataCollectionObject[]> {
		return this.client.get<DataCollectionObject[]>(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}/objects`,
		);
	}

	/**
	 * Add an object to a collection. Requires WRITE (Contributor) on the
	 * collection, the mayPublishToDataCollections global group permission, and
	 * publishToDataCollections on the project hosting the object (if relevant).
	 */
	async addObject(
		dataCollectionId: string,
		object: DataCollectionObject,
	): Promise<void> {
		await this.client.post(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}/objects`,
			object,
		);
	}

	/** Remove a dataset from a collection. Requires WRITE on the collection. */
	async removeDataset(
		dataCollectionId: string,
		projectKey: string,
		datasetName: string,
	): Promise<void> {
		await this.client.del(
			`/public/api/data-collections/${encodeURIComponent(dataCollectionId,)}/objects/dataset/${
				encodeURIComponent(projectKey,)
			}/${encodeURIComponent(datasetName,)}`,
		);
	}
}
