import { ClientValidationError, } from "../errors.js";
import { BaseResource, } from "./base.js";

/** DSS hosts its MLflow tracking API next to the public API (dataikuapi `/dip/publicapi/api/2.0/mlflow`). */
const MLFLOW_EXTENSION_PATH = "/public/api/api/2.0/mlflow/extension";

export type MlflowPredictionType = "REGRESSION" | "BINARY_CLASSIFICATION" | "MULTICLASS" | "OTHER";

const PREDICTION_TYPES: Record<string, true> = {
	REGRESSION: true,
	BINARY_CLASSIFICATION: true,
	MULTICLASS: true,
	OTHER: true,
};

export interface MlflowInferenceInfo {
	runId: string;
	predictionType: MlflowPredictionType;
	/** Ordered classes; required for and only allowed with classification types. */
	classes?: string[];
	codeEnvName?: string;
	target?: string;
}

export interface MlflowDeployRunOptions {
	runId: string;
	savedModelId: string;
	/** Version id; DSS overwrites an existing one. Defaults to a timestamp, as in dataikuapi. */
	versionId?: string;
	/** Model name, needed when the run logged several models (see `listModels`). */
	modelName?: string;
	/** Evaluation dataset; without it the version is deployed but not evaluated. */
	evaluationDataset?: string;
	targetColumnName?: string;
	classLabels?: string[];
	codeEnvName?: string;
	/** Use the run's inference info (`setRunInferenceInfo`); default true. */
	useInferenceInfo?: boolean;
	activate?: boolean;
	binaryClassificationThreshold?: number;
	useOptimalThreshold?: boolean;
	skipExpensiveReports?: boolean;
}

export interface MlflowExperimentsDatasetOptions {
	datasetName: string;
	experimentIds?: string[];
	viewType?: "ACTIVE_ONLY" | "DELETED_ONLY" | "ALL";
	/** MLflow search expression. */
	filter?: string;
	orderBy?: string[];
	format?: "LONG" | "JSON";
}

/** Body of POST set-run-inference-info, validated as dataikuapi does. */
export function inferenceInfoBody(info: MlflowInferenceInfo,): Record<string, string> {
	if (PREDICTION_TYPES[info.predictionType] !== true) {
		throw new ClientValidationError(
			`predictionType must be one of ${Object.keys(PREDICTION_TYPES,).join(", ",)}.`,
			"invalid_enum",
		);
	}
	const classification = info.predictionType === "BINARY_CLASSIFICATION"
		|| info.predictionType === "MULTICLASS";
	if (classification !== (info.classes !== undefined && info.classes.length > 0)) {
		throw new ClientValidationError(
			"classes are required for BINARY_CLASSIFICATION and MULTICLASS, and only allowed for them.",
			"validation_failed",
		);
	}
	return {
		run_id: info.runId,
		prediction_type: info.predictionType,
		...(info.classes ? { classes: JSON.stringify(info.classes,), } : {}),
		...(info.codeEnvName ? { code_env_name: info.codeEnvName, } : {}),
		...(info.target ? { target: info.target, } : {}),
	};
}

/**
 * Query of POST deploy-run, as dataikuapi `deploy_run_model` sends it. The
 * version id must be resolved first (`deployRun` defaults it to a timestamp).
 */
export function deployRunQuery(
	opts: MlflowDeployRunOptions & { versionId: string; },
	projectKey: string,
): URLSearchParams {
	const modelVersionInfo = {
		...(opts.evaluationDataset ? { gatherFeaturesFromDataset: opts.evaluationDataset, } : {}),
		...(opts.targetColumnName ? { targetColumnName: opts.targetColumnName, } : {}),
		classLabels: (opts.classLabels ?? []).map((label,) => ({ label, })),
		...(opts.codeEnvName ? { pythonCodeEnvName: opts.codeEnvName, } : {}),
	};
	const query = new URLSearchParams({
		projectKey,
		runId: opts.runId,
		smId: opts.savedModelId,
		versionId: opts.versionId,
		modelVersionInfo: JSON.stringify(modelVersionInfo,),
		activate: String(opts.activate ?? true,),
		binaryClassificationThreshold: String(opts.binaryClassificationThreshold ?? 0.5,),
		useOptimalThreshold: String(opts.useOptimalThreshold ?? true,),
		skipExpensiveReports: String(opts.skipExpensiveReports ?? false,),
		useInferenceInfo: String(opts.useInferenceInfo ?? true,),
	},);
	if (opts.modelName) query.set("modelName", opts.modelName,);
	return query;
}

/** Body of POST create-project-experiments-dataset with the documented defaults. */
export function experimentsDatasetBody(
	opts: MlflowExperimentsDatasetOptions,
): Record<string, unknown> {
	return {
		datasetName: opts.datasetName,
		experimentIds: opts.experimentIds ?? [],
		viewType: opts.viewType ?? "ACTIVE_ONLY",
		filter: opts.filter ?? "",
		orderBy: opts.orderBy ?? [],
		format: opts.format ?? "LONG",
	};
}

/**
 * DSS MLflow extension endpoints (`/api/2.0/mlflow/extension/*`): models of a
 * run, inference info, deploying a run as a saved-model version, the
 * experiments virtual dataset, and cleanup. Every call except `cleanDb`
 * carries the `x-dku-mlflow-project-key` header naming the tracking project.
 */
export class MlflowExtensionResource extends BaseResource {
	private projectHeader(projectKey?: string,): { headers: Record<string, string>; } {
		return { headers: { "x-dku-mlflow-project-key": this.resolveProjectKey(projectKey,), }, };
	}

	/** Models logged by a run (`[{runId, artifactPath}]`). */
	async listModels(runId: string, projectKey?: string,): Promise<Array<Record<string, unknown>>> {
		return this.client.get<Array<Record<string, unknown>>>(
			`${MLFLOW_EXTENSION_PATH}/models/${encodeURIComponent(runId,)}`,
			this.projectHeader(projectKey,),
		);
	}

	/** Record a run's prediction type (and classes, code env, target) for deployment and evaluation. */
	async setRunInferenceInfo(info: MlflowInferenceInfo, projectKey?: string,): Promise<void> {
		await this.client.post(
			`${MLFLOW_EXTENSION_PATH}/set-run-inference-info`,
			inferenceInfoBody(info,),
			this.projectHeader(projectKey,),
		);
	}

	/** Deploy a run's model as a version of an existing saved model. */
	async deployRun(
		opts: MlflowDeployRunOptions,
		projectKey?: string,
	): Promise<{ savedModelId: string; versionId: string; }> {
		const pk = this.resolveProjectKey(projectKey,);
		// dataikuapi defaults to a %Y_%m_%dT%H_%M_%S timestamp; version ids allow no dashes.
		const versionId = opts.versionId ?? new Date().toISOString().slice(0, 19,).replace(/[-:]/g, "_",);
		await this.client.post(
			`${MLFLOW_EXTENSION_PATH}/deploy-run?${deployRunQuery({ ...opts, versionId, }, pk,)}`,
			undefined,
			this.projectHeader(pk,),
		);
		return { savedModelId: opts.savedModelId, versionId, };
	}

	/** Create a virtual dataset exposing the project's experiment tracking data. */
	async createExperimentsDataset(
		opts: MlflowExperimentsDatasetOptions,
		projectKey?: string,
	): Promise<void> {
		await this.client.post(
			`${MLFLOW_EXTENSION_PATH}/create-project-experiments-dataset`,
			experimentsDatasetBody(opts,),
			this.projectHeader(projectKey,),
		);
	}

	/** Permanently delete the project's experiments and runs marked as deleted. */
	async garbageCollect(projectKey?: string,): Promise<void> {
		await this.client.post(
			`${MLFLOW_EXTENSION_PATH}/garbage-collect`,
			undefined,
			this.projectHeader(projectKey,),
		);
	}

	/** Delete all experiment tracking data (experiments, runs, metrics, ...) of a project (admin). */
	async cleanDb(projectKey?: string,): Promise<void> {
		await this.client.del(`${MLFLOW_EXTENSION_PATH}/clean-db/${this.enc(projectKey,)}`,);
	}
}
