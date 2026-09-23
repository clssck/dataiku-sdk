import { basename, } from "node:path";
import type { UploadFormPart, } from "../client.js";
import { ClientValidationError, } from "../errors.js";
import { BaseResource, requireNonEmpty, requireNonEmptyArray, requireObject, } from "./base.js";

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

/**
 * Body of POST /projects/{pkey}/savedmodels/ (official Python client
 * `create_mlflow_model` / `create_external_model`; the REST reference's
 * `/savedmodels/create-external` path returns 405 on DSS 15.0.1).
 * `savedModelType` is only "MLFLOW_PYFUNC" or "PROXY_MODEL"; `proxyModelConfiguration`
 * carries the protocol settings for PROXY_MODEL creation (sagemaker, azure-ml,
 * vertex-ai, databricks — per the official Python client `create_external_model`).
 */
export interface SavedModelCreateRequest extends Record<string, unknown> {
	savedModelType: "MLFLOW_PYFUNC" | "PROXY_MODEL";
	name: string;
	predictionType?: "BINARY_CLASSIFICATION" | "MULTICLASS" | "REGRESSION" | null;
	/** PROXY_MODEL only: e.g. { protocol: "sagemaker", region: "eu-west-1" }. */
	proxyModelConfiguration?: Record<string, unknown>;
}

/** Full saved-model settings document as returned by GET, sent back by PUT. */
export type SavedModelSettings = Record<string, unknown>;

/** Body of POST .../actions/delete-versions (official Python client, see docs note below). */
export interface SavedModelDeleteVersionsRequest extends Record<string, unknown> {
	versions: string[];
	removeIntermediate: boolean;
}

/** Query options for MLflow version import (POST .../versions/{versionId}). */
export interface SavedModelVersionImportOptions extends Record<string, unknown> {
	codeEnvName?: string;
	containerExecConfigName?: string;
	setActive?: boolean;
	binaryClassificationThreshold?: number;
}

/** Body of POST .../external-ml/actions/evaluate. */
export interface ExternalModelVersionEvaluateRequest extends Record<string, unknown> {
	datasetRef: string;
	containerExecConfigName?: string;
	samplingParam?: Record<string, unknown> | null;
}

/** Query flags for .../external-ml/actions/evaluate. */
export interface ExternalModelVersionEvaluateOptions extends Record<string, unknown> {
	useOptimalThreshold?: boolean;
	skipExpensiveReports?: boolean;
}

/** Query options for the scoring JAR export (GET .../scoring-jar). */
export interface SavedModelScoringJarOptions extends Record<string, unknown> {
	fullClassName?: string;
	includeLibs?: boolean;
}

function boolQuery(value: boolean | undefined,): string {
	return value === undefined ? "true" : String(value,);
}

function numberQuery(value: number | undefined, fallback: number,): string {
	return String(value ?? fallback,);
}

export class SavedModelsResource extends BaseResource {
	/** List saved models in a project. */
	async list(projectKey?: string,): Promise<SavedModelListItem[]> {
		return this.client.get<SavedModelListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/savedmodels/`,
		);
	}

	/**
	 * Create a saved model for storing and managing MLflow or external
	 * (proxy) models. POST /projects/{pkey}/savedmodels/ (the official Python
	 * client's collection-root creation endpoint; `/create-external` 405s).
	 */
	async createExternal(
		request: SavedModelCreateRequest,
		projectKey?: string,
	): Promise<SavedModel> {
		const body = requireObject(request, "request",);
		if (typeof body.name !== "string" || body.name.trim().length === 0) {
			throw new ClientValidationError(
				"request.name must be a non-empty string.",
				"validation_failed",
			);
		}
		if (body.savedModelType !== "MLFLOW_PYFUNC" && body.savedModelType !== "PROXY_MODEL") {
			throw new ClientValidationError(
				'request.savedModelType must be "MLFLOW_PYFUNC" or "PROXY_MODEL".',
				"validation_failed",
			);
		}
		return this.client.post<SavedModel>(
			`/public/api/projects/${this.enc(projectKey,)}/savedmodels/`,
			body,
		);
	}

	/**
	 * Get saved-model settings and metadata.
	 * GET /projects/{pkey}/savedmodels/{savedModelId}
	 */
	async get(savedModelId: string, projectKey?: string,): Promise<SavedModel> {
		return this.client.get<SavedModel>(this.savedModelPath(savedModelId, projectKey,),);
	}

	/**
	 * Save the full settings document of a saved model. The body must be the
	 * complete settings document as returned by {@link get} (GET-then-PUT
	 * workflow), per the official Python client's DSSSavedModelSettings.save().
	 */
	async updateSettings(
		savedModelId: string,
		settings: SavedModelSettings,
		projectKey?: string,
	): Promise<void> {
		requireObject(settings, "settings",);
		await this.client.putVoid(this.savedModelPath(savedModelId, projectKey,), settings,);
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

	/** Get the short snippet (summary) for one saved-model version. */
	async versionSnippet(
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		return this.client.get<Record<string, unknown>>(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/snippet`,
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

	/**
	 * Delete one or more versions of a saved model.
	 *
	 * Transport note: the DSS 15 REST reference documents this action as GET
	 * `/projects/{pkey}/savedmodels/{savedModelId}/delete-versions{?versions}{?removeIntermediate}`,
	 * but the official Python client (dataikuapi/dss/savedmodel.py,
	 * DSSSavedModel.delete_versions, verified against dataiku-api-client-python
	 * master) performs `POST .../actions/delete-versions` with a JSON body
	 * `{versions, removeIntermediate}`. The REST doc is erroneous; this SDK
	 * follows the official client. The request is destructive and must never be
	 * retried automatically: retryMaxAttempts is pinned to 1 regardless of the
	 * client-level retry configuration.
	 */
	async deleteVersions(
		versions: string[],
		options?: { removeIntermediate?: boolean; },
		savedModelId?: string,
		projectKey?: string,
	): Promise<void> {
		const id = requireNonEmpty(savedModelId ?? "", "savedModelId",);
		const versionList = requireNonEmptyArray(versions, "versions",);
		const body: SavedModelDeleteVersionsRequest = {
			versions: versionList,
			removeIntermediate: options?.removeIntermediate ?? true,
		};
		await this.client.post<SavedModelActionResult | undefined>(
			`${this.savedModelPath(id, projectKey,)}/actions/delete-versions`,
			body,
			// Mutating action: a retried attempt could double-delete after a lost
			// response. One attempt only, explicitly overriding any future default.
			{ retryMaxAttempts: 1, },
		);
	}

	/**
	 * Import a new MLflow version from a local MLflow model archive
	 * (zip) uploaded as multipart/form-data.
	 * POST /projects/{pkey}/savedmodels/{savedModelId}/versions/{versionId}
	 * with query codeEnvName, containerExecConfigName, setActive,
	 * binaryClassificationThreshold.
	 */
	async importMlflowVersion(
		archivePath: string,
		versionId: string,
		options?: SavedModelVersionImportOptions,
		savedModelId?: string,
		projectKey?: string,
	): Promise<Record<string, unknown> | undefined> {
		const id = requireNonEmpty(savedModelId ?? "", "savedModelId",);
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		const archive = requireNonEmpty(archivePath, "archivePath",);
		const exists = await Bun.file(archive,).exists();
		if (!exists) {
			throw new ClientValidationError(
				`MLflow archive file not found: ${archive}.`,
				"validation_failed",
			);
		}
		const query = this.mlflowImportQuery(options,);
		const parts: UploadFormPart[] = [
			{ name: "file", blob: Bun.file(archive,), fileName: basename(archive,), },
		];
		return this.client.uploadForm<Record<string, unknown>>(
			`${this.savedModelPath(id, projectKey,)}/versions/${version}`,
			parts,
			query,
		);
	}

	/**
	 * Import a new MLflow version from a path inside a managed folder. The
	 * server-side backend mandates a multipart request, so the part set is the
	 * same as the archive upload with a zero-byte "file" part, and the model
	 * location is carried by the folderRef (PROJECT_KEY.FOLDER_ID) and path
	 * query parameters — matching the official Python client, which sends
	 * `files={"file": (None, None)}`.
	 */
	async importMlflowVersionFromFolder(
		folderRef: string,
		folderPath: string,
		versionId: string,
		options?: SavedModelVersionImportOptions,
		savedModelId?: string,
		projectKey?: string,
	): Promise<Record<string, unknown> | undefined> {
		const id = requireNonEmpty(savedModelId ?? "", "savedModelId",);
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		const ref = requireNonEmpty(folderRef, "folderRef",);
		const path = requireNonEmpty(folderPath, "folderPath",);
		const query = this.mlflowImportQuery(options,);
		query.set("folderRef", ref,);
		query.set("path", path,);
		// Match the official client exactly: files={"file": (None, None)} — a
		// filename-less, zero-length field part (a Blob part would carry a
		// `filename=` parameter, which the endpoint rejects).
		const parts: UploadFormPart[] = [{ name: "file", },];
		return this.client.uploadForm<Record<string, unknown>>(
			`${this.savedModelPath(id, projectKey,)}/versions/${version}`,
			parts,
			query,
		);
	}

	/**
	 * Get the external-ml metadata of a saved-model version (MLflow or proxy
	 * model). Opaque server document; no strict schema is enforced.
	 */
	async externalMetadataGet(
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		return this.client.get<Record<string, unknown>>(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/external-ml/metadata`,
		);
	}

	/**
	 * PUT the external-ml metadata of a saved-model version. The body must be
	 * the full metadata document as returned by {@link externalMetadataGet}
	 * (GET-then-PUT). DSS requires the containerExecConfigName query parameter
	 * on this endpoint (live: 400 "Required request parameter
	 * 'containerExecConfigName' not present" without it); for an external API
	 * caller the container exec config resolves as LOCAL-CONFIG -> NONE, so an
	 * omitted option sends NONE rather than dropping the parameter.
	 */
	async externalMetadataPut(
		savedModelId: string,
		versionId: string,
		metadata: Record<string, unknown>,
		options?: { containerExecConfigName?: string; },
		projectKey?: string,
	): Promise<void> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		requireObject(metadata, "metadata",);
		const containerExecConfigName = options?.containerExecConfigName ?? "NONE";
		const suffix = `?containerExecConfigName=${encodeURIComponent(containerExecConfigName,)}`;
		await this.client.putVoid(
			`${
				this.savedModelPath(savedModelId, projectKey,)
			}/versions/${version}/external-ml/metadata${suffix}`,
			metadata,
		);
	}

	/**
	 * Evaluate the performance of an external/MLflow model version on a
	 * dataset. POST .../external-ml/actions/evaluate with
	 * useOptimalThreshold/skipExpensiveReports as query parameters and the
	 * dataset/container/sampling payload in the JSON body.
	 */
	async evaluateVersion(
		request: ExternalModelVersionEvaluateRequest,
		options?: ExternalModelVersionEvaluateOptions,
		savedModelId?: string,
		versionId?: string,
		projectKey?: string,
	): Promise<void> {
		const id = requireNonEmpty(savedModelId ?? "", "savedModelId",);
		const version = encodeURIComponent(requireNonEmpty(versionId ?? "", "versionId",),);
		const requestBody = requireObject(request, "request",);
		if (typeof requestBody.datasetRef !== "string" || requestBody.datasetRef.trim().length === 0) {
			throw new ClientValidationError(
				"request.datasetRef must be a non-empty string.",
				"validation_failed",
			);
		}
		// DSS 15 official external-caller semantics: the container exec config
		// resolves as LOCAL-CONFIG -> NONE, so an omitted value sends NONE
		// rather than being left out; an explicit value (including INHERIT)
		// passes through unchanged. The caller's request object is never
		// mutated — the default is applied to a copy.
		const body = {
			...requestBody,
			containerExecConfigName: requestBody.containerExecConfigName ?? "NONE",
		};
		const useOptimal = boolQuery(options?.useOptimalThreshold,);
		const skipExpensive = boolQuery(options?.skipExpensiveReports,);
		await this.client.post<SavedModelActionResult | undefined>(
			`${this.savedModelPath(id, projectKey,)}/versions/${version}/external-ml/actions/evaluate`
				+ `?useOptimalThreshold=${useOptimal}&skipExpensiveReports=${skipExpensive}`,
			body,
		);
	}

	/**
	 * GET the scoring JAR of a version as a binary stream. Caller is
	 * responsible for consuming the response body (e.g. writeResponseToFile);
	 * the stream is read under the client body deadline.
	 */
	async downloadScoringJar(
		options: SavedModelScoringJarOptions | undefined,
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<Response> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		const query = new URLSearchParams();
		if (options?.fullClassName !== undefined) {
			query.set("fullClassName", options.fullClassName,);
		}
		if (options?.includeLibs !== undefined) {
			query.set("includeLibs", String(options.includeLibs,),);
		}
		const suffix = [...query.keys(),].length > 0 ? `?${query.toString()}` : "";
		return this.client.stream(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/scoring-jar${suffix}`,
		);
	}

	/** GET the scoring PMML of a version as a binary/XML stream. */
	async downloadScoringPmml(
		savedModelId: string,
		versionId: string,
		projectKey?: string,
	): Promise<Response> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		return this.client.stream(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/scoring-pmml`,
		);
	}

	/**
	 * Update the user metadata of a model version. The body must only be the
	 * "userMeta" field of a previously-retrieved version-details object.
	 */
	async setUserMeta(
		savedModelId: string,
		versionId: string,
		userMeta: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const version = encodeURIComponent(requireNonEmpty(versionId, "versionId",),);
		requireObject(userMeta, "userMeta",);
		await this.client.putVoid(
			`${this.savedModelPath(savedModelId, projectKey,)}/versions/${version}/user-meta`,
			userMeta,
		);
	}

	/** Delete a saved model and all of its versions. */
	async delete(savedModelId: string, projectKey?: string,): Promise<void> {
		await this.client.del(this.savedModelPath(savedModelId, projectKey,),);
	}

	private mlflowImportQuery(
		options: SavedModelVersionImportOptions | undefined,
	): URLSearchParams {
		// DSS 15 official semantics: both import endpoints resolve the
		// container exec config as LOCAL-CONFIG, i.e. NONE for an external
		// caller, and DSS validates the parameter's presence (400 when
		// omitted) — so an omitted option sends NONE. An explicit option
		// (including INHERIT) passes through unchanged.
		const query = new URLSearchParams();
		query.set("codeEnvName", options?.codeEnvName ?? "INHERIT",);
		query.set(
			"containerExecConfigName",
			options?.containerExecConfigName ?? "NONE",
		);
		query.set("setActive", boolQuery(options?.setActive,),);
		query.set(
			"binaryClassificationThreshold",
			numberQuery(options?.binaryClassificationThreshold, 0.5,),
		);
		return query;
	}

	private savedModelPath(savedModelId: string, projectKey?: string,): string {
		const id = encodeURIComponent(requireNonEmpty(savedModelId, "savedModelId",),);
		return `/public/api/projects/${this.enc(projectKey,)}/savedmodels/${id}`;
	}
}
