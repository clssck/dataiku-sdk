import { UsageError, } from "../cli/usage.js";
import { BaseResource, } from "./base.js";

const TRAIN_POLL_INTERVAL_MS = 5_000;

export type MlTaskType = "PREDICTION" | "CLUSTERING";

export interface MlTaskCreateOptions {
	analysisId: string;
	taskType: MlTaskType;
	targetVariable?: string;
	predictionType?: string;
	backendType?: string;
	guessPolicy?: string;
	projectKey?: string;
}

export interface MlTaskCreateResult extends Record<string, unknown> {
	analysisId?: string;
	mlTaskId?: string;
}

export interface MlTaskFullModelId extends Record<string, unknown> {
	id?: string;
	fullModelId?: {
		sessionId?: string;
		[key: string]: unknown;
	};
}

export interface MlTaskStatus extends Record<string, unknown> {
	guessing?: boolean;
	training?: boolean;
	fullModelIds?: MlTaskFullModelId[];
}

export interface MlTaskSettings extends Record<string, unknown> {
	taskType?: MlTaskType;
	predictionType?: string;
}

export interface MlTaskActionResult extends Record<string, unknown> {
	message?: string;
}

export interface MlTaskTrainOptions {
	analysisId: string;
	mlTaskId: string;
	sessionName?: string;
	sessionDescription?: string;
	runQueue?: boolean;
	wait?: boolean;
	projectKey?: string;
}

export interface MlTaskTrainingSession extends Record<string, unknown> {
	sessionId: string;
}

export interface MlTaskTrainCompletedResult extends Record<string, unknown> {
	sessionId: string;
	trainedModelIds: string[];
}

export type MlTaskTrainResult = MlTaskTrainingSession | MlTaskTrainCompletedResult;

export interface MlTrainedModelDetails extends Record<string, unknown> {
	id?: string;
	algorithm?: string;
	predictionType?: string;
}

export interface MlTaskDeployOptions {
	analysisId: string;
	mlTaskId: string;
	modelId: string;
	trainDatasetRef: string;
	testDatasetRef?: string;
	modelName: string;
	redoOptimization?: boolean;
	projectKey?: string;
}

export interface MlTaskDeployResult extends Record<string, unknown> {
	savedModelId?: string;
	trainRecipeName?: string;
}

function requireNonEmpty(value: string, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`${name} must be a non-empty string.`, "validation_failed",);
	}
	return value;
}

function delay(ms: number,): Promise<void> {
	const { promise, resolve, } = Promise.withResolvers<void>();
	setTimeout(resolve, ms,);
	return promise;
}

function modelIdsFromStatus(status: MlTaskStatus,): string[] {
	if (!Array.isArray(status.fullModelIds,)) return [];
	const modelIds: string[] = [];
	for (const model of status.fullModelIds) {
		if (typeof model?.id === "string" && model.id.length > 0) modelIds.push(model.id,);
	}
	return modelIds;
}

export class MlTasksResource extends BaseResource {
	/** Create a prediction or clustering task in an existing visual analysis. */
	async create(opts: MlTaskCreateOptions,): Promise<MlTaskCreateResult> {
		if (opts.taskType !== "PREDICTION" && opts.taskType !== "CLUSTERING") {
			throw new UsageError(
				"taskType must be PREDICTION or CLUSTERING.",
				"invalid_enum",
			);
		}
		if (
			opts.taskType === "PREDICTION"
			&& (typeof opts.targetVariable !== "string" || opts.targetVariable.trim().length === 0)
		) {
			throw new UsageError(
				"targetVariable is required for PREDICTION ML tasks.",
				"missing_required_arg",
			);
		}

		const analysisId = encodeURIComponent(requireNonEmpty(opts.analysisId, "analysisId",),);
		const backendType = opts.backendType ?? "PY_MEMORY";
		const guessPolicy = opts.guessPolicy
			?? (opts.taskType === "CLUSTERING" ? "KMEANS" : "DEFAULT");
		requireNonEmpty(backendType, "backendType",);
		requireNonEmpty(guessPolicy, "guessPolicy",);
		if (opts.targetVariable !== undefined) requireNonEmpty(opts.targetVariable, "targetVariable",);
		if (opts.predictionType !== undefined) requireNonEmpty(opts.predictionType, "predictionType",);

		return this.client.post<MlTaskCreateResult>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/lab/${analysisId}/models/`,
			{
				taskType: opts.taskType,
				...(opts.targetVariable !== undefined
					? { targetVariable: opts.targetVariable, }
					: {}),
				...(opts.predictionType !== undefined
					? { predictionType: opts.predictionType, }
					: {}),
				backendType,
				guessPolicy,
			},
		);
	}

	/** Get the current guessing, training, and trained-model status of a task. */
	async status(analysisId: string, mlTaskId: string, projectKey?: string,): Promise<MlTaskStatus> {
		return this.client.get<MlTaskStatus>(
			`${this.taskPath(analysisId, mlTaskId, projectKey,)}/status`,
		);
	}

	/** Get the editable settings of an ML task. */
	async getSettings(
		analysisId: string,
		mlTaskId: string,
		projectKey?: string,
	): Promise<MlTaskSettings> {
		return this.client.get<MlTaskSettings>(
			`${this.taskPath(analysisId, mlTaskId, projectKey,)}/settings`,
		);
	}

	/** Save the full settings object of an ML task. */
	async saveSettings(
		analysisId: string,
		mlTaskId: string,
		settings: MlTaskSettings,
		projectKey?: string,
	): Promise<MlTaskActionResult | undefined> {
		if (settings === null || typeof settings !== "object" || Array.isArray(settings,)) {
			throw new UsageError("settings must be an object.", "validation_failed",);
		}
		return this.client.post<MlTaskActionResult | undefined>(
			`${this.taskPath(analysisId, mlTaskId, projectKey,)}/settings`,
			settings,
		);
	}

	/** Start training, optionally waiting for the task to finish. */
	async train(opts: MlTaskTrainOptions,): Promise<MlTaskTrainResult> {
		if (opts.sessionName !== undefined) requireNonEmpty(opts.sessionName, "sessionName",);
		if (opts.sessionDescription !== undefined) {
			requireNonEmpty(opts.sessionDescription, "sessionDescription",);
		}
		const taskPath = this.taskPath(opts.analysisId, opts.mlTaskId, opts.projectKey,);
		const session = await this.client.post<MlTaskTrainingSession>(
			`${taskPath}/train`,
			{
				sessionName: opts.sessionName,
				sessionDescription: opts.sessionDescription,
				runQueue: opts.runQueue ?? false,
			},
		);
		if (opts.wait !== true) return session;

		let status = await this.status(opts.analysisId, opts.mlTaskId, opts.projectKey,);
		while (status.training !== false) {
			await delay(TRAIN_POLL_INTERVAL_MS,);
			status = await this.status(opts.analysisId, opts.mlTaskId, opts.projectKey,);
		}
		return {
			sessionId: session.sessionId,
			trainedModelIds: modelIdsFromStatus(status,),
		};
	}

	/** List identifiers for every trained model currently present on the task. */
	async listTrainedModels(
		analysisId: string,
		mlTaskId: string,
		projectKey?: string,
	): Promise<string[]> {
		return modelIdsFromStatus(await this.status(analysisId, mlTaskId, projectKey,),);
	}

	/** Get full details for a trained model. */
	async trainedModelDetails(
		analysisId: string,
		mlTaskId: string,
		modelId: string,
		projectKey?: string,
	): Promise<MlTrainedModelDetails> {
		const id = encodeURIComponent(requireNonEmpty(modelId, "modelId",),);
		return this.client.get<MlTrainedModelDetails>(
			`${this.taskPath(analysisId, mlTaskId, projectKey,)}/models/${id}/details`,
		);
	}

	/** Deploy a trained model to the Flow as a saved model and training recipe. */
	async deployToFlow(opts: MlTaskDeployOptions,): Promise<MlTaskDeployResult> {
		const modelId = encodeURIComponent(requireNonEmpty(opts.modelId, "modelId",),);
		const trainDatasetRef = requireNonEmpty(opts.trainDatasetRef, "trainDatasetRef",);
		const modelName = requireNonEmpty(opts.modelName, "modelName",);
		if (opts.testDatasetRef !== undefined) {
			requireNonEmpty(opts.testDatasetRef, "testDatasetRef",);
		}
		return this.client.post<MlTaskDeployResult>(
			`${
				this.taskPath(opts.analysisId, opts.mlTaskId, opts.projectKey,)
			}/models/${modelId}/actions/deployToFlow`,
			{
				trainDatasetRef,
				testDatasetRef: opts.testDatasetRef,
				modelName,
				redoOptimization: opts.redoOptimization ?? true,
			},
		);
	}

	/** Delete an ML task and its trained models. */
	async delete(analysisId: string, mlTaskId: string, projectKey?: string,): Promise<void> {
		await this.client.del(`${this.taskPath(analysisId, mlTaskId, projectKey,)}/`,);
	}

	private taskPath(analysisId: string, mlTaskId: string, projectKey?: string,): string {
		const analysis = encodeURIComponent(requireNonEmpty(analysisId, "analysisId",),);
		const task = encodeURIComponent(requireNonEmpty(mlTaskId, "mlTaskId",),);
		return `/public/api/projects/${this.enc(projectKey,)}/models/lab/${analysis}/${task}`;
	}
}
