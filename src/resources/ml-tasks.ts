import { ClientValidationError, } from "../errors.js";
import { computeNextPollDelayMs, isRequestDeadlineError, } from "../utils/polling.js";
import { BaseResource, requireNonEmpty, } from "./base.js";

const TRAIN_POLL_INTERVAL_MS = 5_000;
const TRAIN_DEFAULT_TIMEOUT_MS = 120_000;

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
	/**
	 * Deadline in milliseconds for the whole `wait` phase after DSS has
	 * accepted the training request. Validated before any request is sent:
	 * an invalid deadline must not start training work on the server.
	 * `timeoutMs: 0` disables waiting (single status check). Requires `wait`.
	 */
	timeoutMs?: number;
	/**
	 * Fixed poll interval in milliseconds for the training status loop.
	 * When omitted the loop backs off adaptively (doubles every 3 polls, capped).
	 * Requires `wait`.
	 */
	pollIntervalMs?: number;
	projectKey?: string;
}

export interface MlTaskTrainingSession extends Record<string, unknown> {
	sessionId: string;
}

export interface MlTaskTrainCompletedResult extends Record<string, unknown> {
	sessionId: string;
	trainedModelIds: string[];
}

/** Result returned when the training wait budget was exhausted. */
export interface MlTaskTrainTimedOutResult extends Record<string, unknown> {
	sessionId: string;
	trainedModelIds: string[];
	timedOut: true;
	success: false;
	state: "RUNNING";
	elapsedMs: number;
	pollCount: number;
}

export type MlTaskTrainResult =
	| MlTaskTrainingSession
	| MlTaskTrainCompletedResult
	| MlTaskTrainTimedOutResult;

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
			throw new ClientValidationError(
				"taskType must be PREDICTION or CLUSTERING.",
				"invalid_enum",
			);
		}
		if (
			opts.taskType === "PREDICTION"
			&& (typeof opts.targetVariable !== "string" || opts.targetVariable.trim().length === 0)
		) {
			throw new ClientValidationError(
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
			throw new ClientValidationError("settings must be an object.", "validation_failed",);
		}
		return this.client.post<MlTaskActionResult | undefined>(
			`${this.taskPath(analysisId, mlTaskId, projectKey,)}/settings`,
			settings,
		);
	}

	/**
	 * Start training, optionally waiting for the task to finish.
	 *
	 * `timeoutMs` is validated BEFORE the training POST: an invalid deadline
	 * never reaches the server, so bad input cannot start training work. Once
	 * DSS has accepted the training request, the deadline scopes only the
	 * wait phase — each status poll is issued with the remaining budget as a
	 * total request deadline, so a hung status request cannot outlast it.
	 *
	 * When the budget is exhausted the returned result keeps the documented
	 * success shape plus `timedOut: true` and `success: false` instead of
	 * throwing, so a caller can distinguish "still training" from "finished".
	 */
	async train(opts: MlTaskTrainOptions,): Promise<MlTaskTrainResult> {
		if (opts.sessionName !== undefined) requireNonEmpty(opts.sessionName, "sessionName",);
		if (opts.sessionDescription !== undefined) {
			requireNonEmpty(opts.sessionDescription, "sessionDescription",);
		}
		// Deadline validation happens before any network request: a malformed
		// budget must not start training work on the server.
		const explicitIntervalMs = opts.pollIntervalMs;
		if (explicitIntervalMs !== undefined && opts.wait !== true) {
			throw new ClientValidationError(
				"pollIntervalMs requires wait: true.",
				"validation_failed",
			);
		}
		if (
			explicitIntervalMs !== undefined
			&& (!Number.isFinite(explicitIntervalMs,) || explicitIntervalMs <= 0)
		) {
			throw new ClientValidationError(
				"pollIntervalMs must be a finite positive number.",
				"validation_failed",
			);
		}
		let timeoutMs: number | undefined;
		if (opts.timeoutMs !== undefined) {
			if (opts.wait !== true) {
				throw new ClientValidationError("timeoutMs requires wait: true.", "validation_failed",);
			}
			if (!Number.isFinite(opts.timeoutMs,) || opts.timeoutMs < 0) {
				throw new ClientValidationError(
					"timeoutMs must be a finite non-negative number.",
					"validation_failed",
				);
			}
			timeoutMs = opts.timeoutMs;
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

		const deadlineMs = timeoutMs ?? TRAIN_DEFAULT_TIMEOUT_MS;
		const baseIntervalMs = Math.max(1, explicitIntervalMs ?? TRAIN_POLL_INTERVAL_MS,);
		const adaptiveEnabled = explicitIntervalMs === undefined;
		const startedAt = Date.now();
		let pollCount = 0;
		let lastStatus: MlTaskStatus | undefined;

		while (true) {
			const elapsedBeforeMs = Date.now() - startedAt;
			// The first observation always happens, even when the budget is
			// already spent. A later poll is never started after the deadline:
			// the loop reports the structured timeout from the last observed
			// status instead of issuing a request the budget cannot cover.
			if (lastStatus !== undefined && elapsedBeforeMs >= deadlineMs) {
				return {
					sessionId: session.sessionId,
					trainedModelIds: modelIdsFromStatus(lastStatus,),
					timedOut: true,
					success: false,
					state: "RUNNING",
					elapsedMs: elapsedBeforeMs,
					pollCount,
				} as MlTaskTrainResult;
			}
			pollCount += 1;
			// Requests issued while budget remains are bounded by the remaining
			// time; a spent budget still issues exactly one transport attempt
			// (no retries, client requestTimeoutMs cap) so the first observation
			// reaches the server instead of failing before any attempt.
			const remainingMs = deadlineMs - elapsedBeforeMs;
			let status: MlTaskStatus;
			try {
				status = await this.client.get<MlTaskStatus>(
					`${this.taskPath(opts.analysisId, opts.mlTaskId, opts.projectKey,)}/status`,
					remainingMs > 0
						? { timeoutMs: remainingMs, }
						: { noRetry: true, },
				);
			} catch (error) {
				// The poll budget ran out: report the structured timeout instead
				// of letting the transport deadline error escape the loop.
				if (!isRequestDeadlineError(error, startedAt + deadlineMs,)) throw error;
				return {
					sessionId: session.sessionId,
					trainedModelIds: [],
					timedOut: true,
					success: false,
					state: "RUNNING",
					elapsedMs: Date.now() - startedAt,
					pollCount,
				};
			}
			lastStatus = status;
			const elapsedMs = Date.now() - startedAt;

			if (status.training !== false) {
				// Deadline reached: the guard at the top of the loop reports the
				// timeout from this status without issuing another request.
				if (elapsedMs >= deadlineMs) continue;
				const nextDelayMs = computeNextPollDelayMs({
					pollCount,
					baseIntervalMs,
					adaptiveEnabled,
				},);
				await delay(Math.min(nextDelayMs, deadlineMs - elapsedMs,),);
				continue;
			}

			return {
				sessionId: session.sessionId,
				trainedModelIds: modelIdsFromStatus(status,),
			};
		}
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
