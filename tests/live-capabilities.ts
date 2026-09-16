import { mkdir, stat, } from "node:fs/promises";
import path from "node:path";
import { matchesLiveCase, } from "./live-cases.js";
import type { LiveContext, LiveFixtures, } from "./live-context.js";
import {
	LIVE_CSV_FORMAT,
	LIVE_SQL_PROBE,
	LiveCapabilityError,
	LiveCommandError,
} from "./live-context.js";
import { withOwnedCodeEnv, withOwnedSqlConnection, } from "./live-infrastructure-disposable.js";

/**
 * Optional explicit SQL targets for the infrastructure SQL case (matches the
 * legacy integration harness gate in tests/integration-harness.ts). The
 * dataset variant is accepted as an alternative. When neither is set, the case
 * self-provisions an owned disposable SQLite connection through the canonical
 * withOwnedSqlConnection helper instead of reporting a configuration blocker.
 */
const SQL_CONNECTION_ENV = "DATAIKU_SQL_CONNECTION";
const SQL_DATASET_ENV = "DATAIKU_SQL_DATASET_FULL_NAME";

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown,): JsonRecord | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as JsonRecord
		: undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function errorMessage(error: unknown,): string {
	return error instanceof Error ? error.message : String(error,);
}

function capabilityBlocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

function requireId(value: unknown, what: string,): string {
	const id = asString(value,);
	if (!id) throw new Error(`DSS did not return a ${what} identifier.`,);
	return id;
}

// ---------------------------------------------------------------------------
// Case-owned ML training data
// ---------------------------------------------------------------------------

/**
 * Deterministic 0/1 churn table used by the ML cases. The schema mirrors the
 * agreed core synthetic contract (churn_flag bigint target + numeric bigint /
 * float features, no nulls), but the table is uploaded into the case-owned
 * project itself: no case ever references another project's dataset without
 * cloning, and DSS datasets are project-scoped.
 */
const ML_COLUMNS: Array<{ name: string; type: string; }> = [
	{ name: "id", type: "bigint", },
	{ name: "age", type: "bigint", },
	{ name: "tenure_months", type: "bigint", },
	{ name: "monthly_spend", type: "float", },
	{ name: "support_tickets", type: "bigint", },
	{ name: "churn_flag", type: "bigint", },
];

/** Churn is correlated with support load and tenure so training is meaningful. */
function mlRow(id: number,): [number, number, number, number, number, number,] {
	const supportTickets = (id * 7) % 5;
	const tenureMonths = 3 + (id * 11) % 36;
	const churn = supportTickets >= 3 || tenureMonths < 6 ? 1 : 0;
	return [id, 20 + (id * 13) % 45, tenureMonths, 10 + (id * 17) % 90, supportTickets, churn,];
}

function mlCsv(): string {
	const rows: string[] = [ML_COLUMNS.map((column,) => column.name).join(",",),];
	for (let id = 1; id <= 24; id++) {
		rows.push(mlRow(id,).join(",",),);
	}
	return `${rows.join("\n",)}\n`;
}

/**
 * Provision an ML training dataset inside the given (run-owned) project:
 * create an UploadedFiles dataset, upload the deterministic CSV written via
 * ctx.writeFile, and pin the schema explicitly so the ML task sees a stable
 * numeric target.
 */
async function provisionMlDataset(
	ctx: LiveContext,
	projectKey: string,
	datasetName: string,
): Promise<string> {
	await ctx.run([
		"dataset",
		"create",
		"--name",
		datasetName,
		"--type",
		"UploadedFiles",
		"--project-key",
		projectKey,
	],);
	const csvPath = await ctx.writeFile(`ml_train_${ctx.iteration}.csv`, mlCsv(),);
	await ctx.run([
		"dataset",
		"upload-file",
		datasetName,
		csvPath,
		"--file-name",
		"train.csv",
		"--project-key",
		projectKey,
	],);
	// UploadedFiles datasets default to zero parsed rows until the CSV format
	// is pinned explicitly (per the core fixtures contract): header row,
	// UTF-8, comma separator, double-quote quoting with backslash escapes.
	await ctx.run([
		"dataset",
		"update",
		datasetName,
		"--data",
		JSON.stringify(LIVE_CSV_FORMAT,),
		"--project-key",
		projectKey,
	],);
	await ctx.run([
		"dataset",
		"refresh-schema",
		datasetName,
		"--data",
		JSON.stringify({ columns: ML_COLUMNS, },),
		"--project-key",
		projectKey,
	],);
	await assertMlDataset(ctx, projectKey, datasetName,);
	return datasetName;
}

/** The fixture contract: 24 parsed rows carrying both target classes. */
async function assertMlDataset(
	ctx: LiveContext,
	projectKey: string,
	datasetName: string,
): Promise<void> {
	const preview = await ctx.run<{ rowCount: number; rows: string[][]; }>([
		"dataset",
		"preview",
		datasetName,
		"--max-rows",
		"100",
		"--project-key",
		projectKey,
	],);
	if (preview.rowCount !== 24 || new Set(preview.rows.map(row => row[5]),).size !== 2) {
		throw new Error("ML fixture must contain 24 rows and both target classes",);
	}
}

/** Case-owned variant used by the throwaway lifecycle/clustering cases. */
function provisionCaseDataset(ctx: LiveContext, projectKey: string,): Promise<string> {
	return provisionMlDataset(ctx, projectKey, `live_cap_train_${ctx.iteration}`,);
}

// ---------------------------------------------------------------------------
// ML scenario: analysis → prediction task → train → deploy → saved model
// ---------------------------------------------------------------------------

interface MlTaskTrainShape {
	trainedModelIds?: unknown;
}
interface DeployShape {
	savedModelId?: unknown;
}

/**
 * Full low-cost Visual ML lifecycle in a case-owned project: create the
 * training dataset, create an analysis over it, create a binary-classification
 * task on churn_flag, train with --wait, inspect trained models, deploy to the
 * Flow (dry-run proof first), and read the resulting saved model. Every
 * created object is deleted in finally and the project itself is removed via
 * ctx.deleteProject; all cleanup failures are reported.
 */
async function exerciseMlLifecycle(ctx: LiveContext,): Promise<void> {
	const projectKey = await ctx.createProject("mlcap",);
	let datasetName: string | undefined;
	let analysisId: string | undefined;
	let mlTaskId: string | undefined;
	let savedModelId: string | undefined;
	const failures: unknown[] = [];
	try {
		datasetName = await provisionCaseDataset(ctx, projectKey,);

		const analysis = await ctx.run<JsonRecord>([
			"analysis",
			"create",
			"--input-dataset",
			datasetName,
			"--project-key",
			projectKey,
		],);
		analysisId = requireId(analysis["created"] ?? analysis["id"], "analysis",);

		const task = await ctx.run<JsonRecord>([
			"ml-task",
			"create",
			analysisId,
			"--task-type",
			"PREDICTION",
			"--target",
			"churn_flag",
			"--prediction-type",
			"BINARY_CLASSIFICATION",
			"--guess-policy",
			"DECISION_TREE",
			"--project-key",
			projectKey,
		],);
		mlTaskId = requireId(task["created"] ?? task["mlTaskId"], "ML task",);

		const settings = await ctx.run<JsonRecord>([
			"ml-task",
			"get-settings",
			analysisId,
			mlTaskId,
			"--project-key",
			projectKey,
		],);
		if (asRecord(settings,) === undefined) {
			throw new Error("ML task settings response was not a JSON object.",);
		}

		const trained = await ctx.run<MlTaskTrainShape>([
			"ml-task",
			"train",
			analysisId,
			mlTaskId,
			"--session-name",
			`live_cap_${ctx.iteration}`,
			"--wait",
			"--project-key",
			projectKey,
		],);
		const modelIds = Array.isArray(trained["trainedModelIds"],)
			? trained["trainedModelIds"].map((id,) => asString(id,)).filter((id,): id is string =>
				Boolean(id,)
			)
			: [];
		if (modelIds.length === 0) {
			throw new Error("ML training finished without reporting any trained model IDs.",);
		}

		const listed = await ctx.run<unknown[]>([
			"ml-task",
			"list-models",
			analysisId,
			mlTaskId,
			"--project-key",
			projectKey,
		],);
		if (!Array.isArray(listed,) || listed.length === 0) {
			throw new Error("ml-task list-models returned no trained models after --wait training.",);
		}
		const details = await ctx.run<JsonRecord>([
			"ml-task",
			"model-details",
			analysisId,
			mlTaskId,
			modelIds[0]!,
			"--project-key",
			projectKey,
		],);
		if (asRecord(details,) === undefined) {
			throw new Error("ml-task model-details returned no object.",);
		}

		const dryRun = await ctx.run<JsonRecord>([
			"ml-task",
			"deploy",
			analysisId,
			mlTaskId,
			modelIds[0]!,
			"--model-name",
			`live_cap_model_${ctx.iteration}`,
			"--train-dataset",
			datasetName,
			"--dry-run",
			"--project-key",
			projectKey,
		],);
		if (dryRun["dryRun"] !== true) {
			throw new Error("ml-task deploy --dry-run did not report dryRun=true.",);
		}
		const deployed = await ctx.run<DeployShape>([
			"ml-task",
			"deploy",
			analysisId,
			mlTaskId,
			modelIds[0]!,
			"--model-name",
			`live_cap_model_${ctx.iteration}`,
			"--train-dataset",
			datasetName,
			"--project-key",
			projectKey,
		],);
		savedModelId = requireId(deployed["savedModelId"], "saved model",);

		const savedList = await ctx.run<unknown[]>([
			"saved-model",
			"list",
			"--project-key",
			projectKey,
		],);
		if (
			!Array.isArray(savedList,)
			|| !savedList.some((item,) => asRecord(item,)?.["id"] === savedModelId)
		) {
			throw new Error(`Deployed saved model ${savedModelId} missing from saved-model list.`,);
		}
		const savedGet = await ctx.run<JsonRecord>([
			"saved-model",
			"get",
			savedModelId,
			"--project-key",
			projectKey,
		],);
		if (asString(savedGet["id"],) !== savedModelId) {
			throw new Error("saved-model get returned a different saved model.",);
		}
		const versions = await ctx.run<unknown[]>([
			"saved-model",
			"list-versions",
			savedModelId,
			"--project-key",
			projectKey,
		],);
		const versionId = Array.isArray(versions,)
			? versions.map((item,) => asRecord(item,)).find(Boolean,)?.["id"]
			: undefined;
		const versionIdString = asString(versionId,);
		if (!versionIdString) throw new Error("saved-model list-versions returned no version id.",);
		await ctx.run([
			"saved-model",
			"version-details",
			savedModelId,
			versionIdString,
			"--project-key",
			projectKey,
		],);
	} catch (error) {
		failures.push(error,);
	} finally {
		if (savedModelId) {
			try {
				await ctx.run([
					"saved-model",
					"delete",
					savedModelId,
					"--if-exists",
					"--project-key",
					projectKey,
				],);
			} catch (error) {
				failures.push(`saved-model delete: ${errorMessage(error,).slice(0, 200,)}`,);
			}
		}
		if (mlTaskId) {
			try {
				await ctx.run(["ml-task", "delete", analysisId!, mlTaskId, "--project-key", projectKey,],);
			} catch (error) {
				failures.push(`ml-task delete: ${errorMessage(error,).slice(0, 200,)}`,);
			}
		}
		if (analysisId) {
			try {
				await ctx.run(["analysis", "delete", analysisId, "--if-exists", "--project-key", projectKey,],);
			} catch (error) {
				failures.push(`analysis delete: ${errorMessage(error,).slice(0, 200,)}`,);
			}
		}
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			failures.push(`project delete ${projectKey}: ${errorMessage(error,).slice(0, 200,)}`,);
		}
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length) throw new AggregateError(failures, "Live case or cleanup failed",);
}

/**
 * Clustering path: exercises the non-prediction task type on the same
 * case-owned dataset without a target. Train-only proof is not a deployment
 * fallback — this case exists to cover CLUSTERING task creation and status.
 */
async function exerciseMlClustering(ctx: LiveContext,): Promise<void> {
	const projectKey = await ctx.createProject("mlclus",);
	let datasetName: string | undefined;
	let analysisId: string | undefined;
	let mlTaskId: string | undefined;
	const failures: unknown[] = [];
	try {
		datasetName = await provisionCaseDataset(ctx, projectKey,);
		const analysis = await ctx.run<JsonRecord>([
			"analysis",
			"create",
			"--input-dataset",
			datasetName,
			"--project-key",
			projectKey,
		],);
		analysisId = requireId(analysis["created"] ?? analysis["id"], "analysis",);
		const task = await ctx.run<JsonRecord>([
			"ml-task",
			"create",
			analysisId,
			"--task-type",
			"CLUSTERING",
			"--project-key",
			projectKey,
		],);
		mlTaskId = requireId(task["created"] ?? task["mlTaskId"], "ML task",);
		await ctx.run(["ml-task", "status", analysisId, mlTaskId, "--project-key", projectKey,],);
	} catch (error) {
		failures.push(error,);
	} finally {
		if (mlTaskId) {
			try {
				await ctx.run(["ml-task", "delete", analysisId!, mlTaskId, "--project-key", projectKey,],);
			} catch (error) {
				failures.push(`ml-task delete: ${errorMessage(error,).slice(0, 200,)}`,);
			}
		}
		if (analysisId) {
			try {
				await ctx.run(["analysis", "delete", analysisId, "--if-exists", "--project-key", projectKey,],);
			} catch (error) {
				failures.push(`analysis delete: ${errorMessage(error,).slice(0, 200,)}`,);
			}
		}
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			failures.push(`project delete ${projectKey}: ${errorMessage(error,).slice(0, 200,)}`,);
		}
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length) throw new AggregateError(failures, "Live case or cleanup failed",);
}

// ---------------------------------------------------------------------------
// Retained ML fixtures + expanded surface in the persistent root project
// ---------------------------------------------------------------------------

const MLFLOW_CODE_ENV_ENV = "DATAIKU_LIVE_MLFLOW_CODE_ENV";

const RETAINED_DATASET_NAME = "ml_train_root";
const RETAINED_ANALYSIS_NAME = "live_ml_retained_analysis";
const RETAINED_MODEL_NAME = "ml_retained_model";
const RETAINED_SCORED_DATASET = "ml_scored_root";
const RETAINED_SCORE_RECIPE = "live_ml_score_recipe";
const RETAINED_EVAL_STORE_NAME = "live_ml_eval_store";
const RETAINED_EVAL_RECIPE = "live_ml_eval_recipe";
const MLFLOW_MODEL_NAME = "mlflow_root_model";
const MLFLOW_VERSIONS = ["v1", "v2",] as const;

interface TrainedSavedModel {
	datasetName: string;
	analysisId: string;
	mlTaskId: string;
	trainedModelId: string;
	savedModelId: string;
	versionId: string;
}

type LiveMlFixtures = NonNullable<LiveFixtures["ml"]>;

/** Require the shared ml fixture object (provisioning initializes it first). */
function mlFixtures(ctx: LiveContext,): LiveMlFixtures {
	ctx.fixtures.ml ??= {};
	return ctx.fixtures.ml;
}

/** Read one retained fixture id, failing loudly when provisioning did not record it. */
function requireMlId(
	ctx: LiveContext,
	field: keyof LiveMlFixtures,
): string {
	const value = ctx.fixtures.ml?.[field];
	if (typeof value !== "string" || value.length === 0) {
		throw new LiveCapabilityError(
			`Retained ML fixture missing: fixtures.ml.${field} was not recorded by provisioning.`,
			"blocked",
		);
	}
	return value;
}

/** Persist a provisioned id before depending on it, so re-runs resume instead of duplicating. */
function rememberMlId(
	ctx: LiveContext,
	field: keyof LiveMlFixtures,
	value: string | undefined,
): string {
	const id = requireId(value, field,);
	ctx.fixtures.ml ??= {};
	ctx.fixtures.ml[field] = id;
	return id;
}

function firstListItemId(items: unknown,): string | undefined {
	const first = Array.isArray(items,)
		? items.map((item,) => asRecord(item,)).find(Boolean,)
		: undefined;
	return asString(first?.["id"],);
}

function activeVersionId(versions: unknown,): string | undefined {
	const active = Array.isArray(versions,)
		? versions.map((item,) => asRecord(item,)).find(item => item?.["active"] === true)
		: undefined;
	return asString(active?.["id"],) ?? firstListItemId(versions,);
}

/** First ref of one recipe role, e.g. inputs.model or outputs.evaluationStore. */
function roleItemRef(
	recipe: JsonRecord,
	direction: "inputs" | "outputs",
	role: string,
): string | undefined {
	const items = asRecord(asRecord(recipe[direction],)?.[role],)?.["items"];
	const first = Array.isArray(items,) ? asRecord(items[0],) : undefined;
	return asString(first?.["ref"],);
}

/**
 * Find the single lab-owned recipe whose full graph matches by readback.
 * Zero matches: undefined (caller creates). Exactly one: its name (caller
 * reuses it, repairing any stale recorded name). More than one: an error that
 * preserves every candidate — an ambiguous match is never deleted, since a
 * same-graph clone may be user-owned.
 */
async function findOwnedRecipe(
	ctx: LiveContext,
	type: string,
	matches: (recipe: JsonRecord,) => boolean,
): Promise<string | undefined> {
	const recipes = await ctx.run<Array<{ name?: string; type?: string; }>>(["recipe", "list",],);
	const matched: string[] = [];
	for (const candidate of Array.isArray(recipes,) ? recipes : []) {
		if (!candidate?.name || candidate.type !== type) continue;
		const detail = await ctx.run<JsonRecord>(["recipe", "get", candidate.name,],);
		const recipe = asRecord(detail["recipe"],);
		if (recipe && matches(recipe,)) matched.push(candidate.name,);
	}
	if (matched.length > 1) {
		throw new Error(
			`Multiple ${type} recipes match the lab fixture graph (${
				matched.join(", ",)
			}); refusing to touch any of them.`,
		);
	}
	return matched[0];
}

/**
 * Ensure a lab-owned managed ML output dataset exists, then register its exact
 * name (name -> name) in the canonical ownership registry `fixtures.datasets`
 * that ctx.propagateRecipeSchema validates against. Registration happens only
 * after existence is confirmed — freshly created by this run, or verified by
 * the dataset-list readback (reuse path). No other dataset names are ever
 * registered here.
 */
async function ensureManagedMlOutput(ctx: LiveContext, name: string,): Promise<void> {
	const datasets = await ctx.run<Array<{ name?: string; }>>(["dataset", "list",],);
	const exists = Array.isArray(datasets,) && datasets.some(entry => entry?.name === name);
	if (!exists) await ctx.createManagedDataset(name,);
	ctx.fixtures.datasets[name] = name;
}

/**
 * Retained run-project training assets. Idempotent and resumable: every step
 * looks for its object by the stable retained name first, reuses it, and
 * records its id in ctx.fixtures.ml before moving on, so repeated runs enrich
 * the same root example without ever rebuilding or deleting it.
 */
export async function provisionMlFixtures(ctx: LiveContext,): Promise<void> {
	const ml = mlFixtures(ctx,);

	// --- Training dataset -----------------------------------------------------
	if (typeof ml.datasetName !== "string") {
		const datasets = await ctx.run<Array<{ name?: string; }>>([
			"dataset",
			"list",
		],);
		const existing = Array.isArray(datasets,)
			&& datasets.some(entry => entry?.name === RETAINED_DATASET_NAME);
		ml.datasetName = existing
			? RETAINED_DATASET_NAME
			: await provisionMlDataset(ctx, ctx.projectKey, RETAINED_DATASET_NAME,);
	} else {
		await assertMlDataset(ctx, ctx.projectKey, ml.datasetName,);
	}

	// --- Analysis + prediction task ------------------------------------------
	if (typeof ml.analysisId !== "string") {
		const analyses = await ctx.run<Array<{ id?: string; name?: string; }>>([
			"analysis",
			"list",
		],);
		const existing = (Array.isArray(analyses,) ? analyses : [])
			.find(entry => entry?.name === RETAINED_ANALYSIS_NAME);
		if (existing) {
			ml.analysisId = requireId(existing["id"], "retained analysis id",);
		} else {
			const created = await ctx.run<JsonRecord>([
				"analysis",
				"create",
				"--input-dataset",
				ml.datasetName,
			],);
			ml.analysisId = rememberMlId(ctx, "analysisId", asString(created["created"] ?? created["id"],),);
		}
	}
	if (typeof ml.mlTaskId !== "string") {
		const created = await ctx.run<JsonRecord>([
			"ml-task",
			"create",
			ml.analysisId,
			"--task-type",
			"PREDICTION",
			"--target",
			"churn_flag",
			"--prediction-type",
			"BINARY_CLASSIFICATION",
			"--guess-policy",
			"DECISION_TREE",
		],);
		ml.mlTaskId = rememberMlId(
			ctx,
			"mlTaskId",
			asString(created["created"] ?? created["mlTaskId"],),
		);
	}

	// --- Train + deploy a REAL model (never an empty/external substitute) -----
	if (typeof ml.trainedModelId !== "string") {
		const trained = await ctx.run<MlTaskTrainShape>([
			"ml-task",
			"train",
			ml.analysisId,
			ml.mlTaskId,
			"--session-name",
			`live_ml_retained_${ctx.iteration}`,
			"--wait",
		],);
		const modelIds = Array.isArray(trained["trainedModelIds"],)
			? trained["trainedModelIds"].map((id,) => asString(id,)).filter((id,): id is string =>
				Boolean(id,)
			)
			: [];
		if (modelIds.length === 0) {
			throw new Error("Retained ML training finished without trained model IDs.",);
		}
		ml.trainedModelId = modelIds[0]!;
	}
	if (typeof ml.savedModelId !== "string") {
		const savedModels = await ctx.run<Array<{ id?: string; name?: string; }>>([
			"saved-model",
			"list",
		],);
		const existing = (Array.isArray(savedModels,) ? savedModels : [])
			.find(entry => entry?.name === RETAINED_MODEL_NAME);
		if (existing) {
			ml.savedModelId = requireId(existing["id"], "retained saved model id",);
		} else {
			const deployed = await ctx.run<DeployShape>([
				"ml-task",
				"deploy",
				ml.analysisId,
				ml.mlTaskId,
				ml.trainedModelId,
				"--model-name",
				RETAINED_MODEL_NAME,
				"--train-dataset",
				ml.datasetName,
			],);
			ml.savedModelId = requireId(deployed["savedModelId"], "retained saved model",);
		}
	}
	if (typeof ml.savedModelVersionId !== "string") {
		const versions = await ctx.run<unknown[]>([
			"saved-model",
			"list-versions",
			ml.savedModelId,
		],);
		const versionId = activeVersionId(versions,);
		if (!versionId) throw new Error("Retained saved model has no versions.",);
		ml.savedModelVersionId = versionId;
	}
}

/**
 * Check-shaped retained provisioning for the run phase: fresh runs provision
 * the root example on demand; established labs reuse it without rebuilding.
 */
export async function provisionRetainedMl(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
}

/**
 * Train a real visual-ML model and deploy it into the given bound project.
 * Caller owns all cleanup (typically project teardown); nothing is recorded
 * in shared fixtures.
 */
export async function provisionTrainedSavedModel(
	ctx: LiveContext,
	projectKey: string,
): Promise<TrainedSavedModel> {
	const datasetName = await provisionMlDataset(
		ctx,
		projectKey,
		`ml_train_${ctx.iteration}_${randomNameSuffix()}`,
	);
	const analysis = await ctx.run<JsonRecord>([
		"analysis",
		"create",
		"--input-dataset",
		datasetName,
		"--project-key",
		projectKey,
	],);
	const analysisId = requireId(analysis["created"] ?? analysis["id"], "analysis",);
	const mlTaskId = await createPredictionTask(ctx, analysisId, projectKey,);
	const trainedModelId = await trainPredictionTask(ctx, analysisId, mlTaskId, projectKey,);
	const savedModelId = await deployPredictionModel(
		ctx,
		analysisId,
		mlTaskId,
		trainedModelId,
		datasetName,
		projectKey,
	);
	const versions = await ctx.run<unknown[]>([
		"saved-model",
		"list-versions",
		savedModelId,
		"--project-key",
		projectKey,
	],);
	const versionId = activeVersionId(versions,);
	if (!versionId) throw new Error("Deployed saved model has no versions.",);
	return { datasetName, analysisId, mlTaskId, trainedModelId, savedModelId, versionId, };
}

/** Deploy a trained prediction model into the Flow as a named saved model. */
async function deployPredictionModel(
	ctx: LiveContext,
	analysisId: string,
	mlTaskId: string,
	trainedModelId: string,
	trainDataset: string,
	projectKey: string | undefined,
): Promise<string> {
	const projectArgs = projectKey === undefined ? [] : ["--project-key", projectKey,];
	const deployed = await ctx.run<DeployShape>([
		"ml-task",
		"deploy",
		analysisId,
		mlTaskId,
		trainedModelId,
		"--model-name",
		`ml_model_${ctx.iteration}_${randomNameSuffix()}`,
		"--train-dataset",
		trainDataset,
		...projectArgs,
	],);
	return requireId(deployed["savedModelId"], "saved model",);
}

/** Create the retained BINARY_CLASSIFICATION prediction task. */
async function createPredictionTask(
	ctx: LiveContext,
	analysisId: string,
	projectKey: string,
): Promise<string> {
	const task = await ctx.run<JsonRecord>([
		"ml-task",
		"create",
		analysisId,
		"--task-type",
		"PREDICTION",
		"--target",
		"churn_flag",
		"--prediction-type",
		"BINARY_CLASSIFICATION",
		"--guess-policy",
		"DECISION_TREE",
		"--project-key",
		projectKey,
	],);
	return requireId(task["created"] ?? task["mlTaskId"], "ML task",);
}

/** Train with --wait and return the first trained model id. */
async function trainPredictionTask(
	ctx: LiveContext,
	analysisId: string,
	mlTaskId: string,
	projectKey: string,
): Promise<string> {
	const trained = await ctx.run<MlTaskTrainShape>([
		"ml-task",
		"train",
		analysisId,
		mlTaskId,
		"--session-name",
		`live_ml_${ctx.iteration}`,
		"--wait",
		"--project-key",
		projectKey,
	],);
	const modelIds = Array.isArray(trained["trainedModelIds"],)
		? trained["trainedModelIds"].map((id,) => asString(id,)).filter((id,): id is string =>
			Boolean(id,)
		)
		: [];
	if (modelIds.length === 0) {
		throw new Error("ML training finished without reporting any trained model IDs.",);
	}
	return modelIds[0]!;
}

/** Short lowercase suffix keeping concurrent retained names unique per run. */
function randomNameSuffix(): string {
	return Math.random().toString(36,).slice(2, 7,);
}

/**
 * Expanded run-phase ML surface over the retained root example. Each granular
 * case is independent: a license- or runtime-gated case reports an explicit
 * blocker without hiding the independent behaviors beside it. Setup
 * provisioning is exposed through provisionMlFixtures/provisionRetainedMl;
 * retained root fixtures are never deleted here.
 */
export async function exerciseExpandedMl(ctx: LiveContext,): Promise<void> {
	await ctx.check(
		"ml.retained-fixtures",
		[
			"dataset.list",
			"dataset.create",
			"dataset.upload-file",
			"dataset.refresh-schema",
			"analysis.list",
			"analysis.create",
			"ml-task.create",
			"ml-task.train",
			"saved-model.list",
			"ml-task.deploy",
			"saved-model.list-versions",
		],
		async () => {
			await provisionMlFixtures(ctx,);
		},
	);
	await ctx.check(
		"ml.analysis.read",
		["analysis.list", "analysis.get",],
		async () => {
			await exerciseAnalysisRead(ctx,);
		},
	);
	await ctx.check(
		"ml.task.settings",
		["ml-task.status", "ml-task.get-settings", "ml-task.set-settings",],
		async () => {
			await exerciseTaskSettings(ctx,);
		},
	);
	await ctx.check(
		"ml.saved-model.metadata",
		[
			"saved-model.get",
			"saved-model.update-settings",
			"saved-model.list-versions",
			"saved-model.version-details",
			"saved-model.version-snippet",
			"saved-model.set-active",
			"saved-model.set-user-meta",
		],
		async () => {
			await exerciseSavedModelMetadata(ctx,);
		},
	);
	await ctx.check(
		"ml.saved-model.scoring-jar",
		["saved-model.download-scoring-jar",],
		async () => {
			await downloadScoringArtifact(ctx, "jar",);
		},
		{ required: false, },
	);
	await ctx.check(
		"ml.saved-model.scoring-pmml",
		["saved-model.download-scoring-pmml",],
		async () => {
			await downloadScoringArtifact(ctx, "pmml",);
		},
		{ required: false, },
	);
	await ctx.check(
		"ml.scoring.retained-recipe",
		[
			"recipe.validate-graph",
			"recipe.run",
			"dataset.preview",
			"dataset.download",
		],
		async () => {
			await provisionRetainedScoring(ctx,);
		},
	);
	await ctx.check(
		"ml.mes.retained-evaluation",
		[
			"model-evaluation-store.list",
			"model-evaluation-store.create",
			"recipe.validate-graph",
			"recipe.run",
			"model-evaluation-store.list-evaluations",
			"model-evaluation-store.get",
		],
		async () => {
			await provisionRetainedEvaluation(ctx,);
		},
	);
	await ctx.check(
		"ml.mes.lifecycle",
		[
			"project.create",
			"model-evaluation-store.create",
			"model-evaluation-store.list",
			"model-evaluation-store.get",
			"model-evaluation-store.list-evaluations",
			"model-evaluation-store.delete",
			"project.delete",
		],
		async () => {
			await exerciseMesLifecycle(ctx,);
		},
	);
	// ---- MLflow cases: one runtime per run, shared by both selected checks ----
	// Lifecycle: an existing eligible runtime is used directly; otherwise an
	// owned env is provisioned through demo-project's canonical helper and its
	// callback encloses BOTH checks, so the environment is deleted only after
	// both complete (never cached beyond the callback). Provisioning errors are
	// recorded INSIDE each selected check as its own blocked result and never
	// abort the remaining matrix; the helper is never invoked when neither
	// MLflow case is selected.
	const mlflowSelected = ctx.selection.length === 0
		|| ctx.selection.some(selected =>
			matchesLiveCase("ml.mlflow.import", selected,)
			|| matchesLiveCase("ml.mlflow.evaluate", selected,)
		);
	if (mlflowSelected) {
		const runtime = await resolveMlflowRuntime(ctx,);
		if (runtime.envName !== undefined) {
			await runMlflowChecks(ctx, runtime,);
		} else {
			let checksRan = false;
			try {
				await withOwnedCodeEnv(
					ctx,
					"mlflow",
					{
						packages: MLFLOW_RUNTIME_PACKAGES,
						corePackagesSet: "PANDAS22",
						installJupyterSupport: true,
					},
					async ({ envName, },) => {
						// Gate first: an inspection failure here must still land as a
						// per-case blocked result below, not as an aborting error.
						const ownedRuntime = await verifyOwnedMlflowRuntime(ctx, envName,);
						checksRan = true;
						await runMlflowChecks(ctx, ownedRuntime,);
					},
				);
			} catch (error) {
				// Provisioning/build failure before any check ran: both selected
				// checks report the recorded reason as their own blocked result.
				if (checksRan) throw error;
				const reason = `Owned MLflow runtime provisioning failed: ${
					errorMessage(error,).slice(0, 500,)
				}`;
				await runMlflowChecks(ctx, { blocked: reason, },);
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Expanded ML exercise cases (run phase, root project)
// ---------------------------------------------------------------------------

/** Read-only analysis surface of the retained example (self-provisioning). */
async function exerciseAnalysisRead(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
	const analysisId = requireMlId(ctx, "analysisId",);
	const listed = await ctx.run<Array<{ analysisId?: string; }>>(["analysis", "list",],);
	// GET /lab list items carry analysisId/analysisName/inputDataset (DSS 15
	// REST reference), not id/name.
	if (!Array.isArray(listed,) || !listed.some(item => item?.analysisId === analysisId)) {
		throw new Error(`analysis list did not include the retained analysis ${analysisId}.`,);
	}
	const analysis = await ctx.run<JsonRecord>(["analysis", "get", analysisId,],);
	if (asString(analysis["analysisId"] ?? analysis["id"],) !== analysisId) {
		throw new Error("analysis get returned a different analysis.",);
	}
}

/** Task status + full settings GET-then-PUT round-trip (self-provisioning). */
async function exerciseTaskSettings(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
	const analysisId = requireMlId(ctx, "analysisId",);
	const mlTaskId = requireMlId(ctx, "mlTaskId",);
	const status = await ctx.run<JsonRecord>(["ml-task", "status", analysisId, mlTaskId,],);
	if (asRecord(status,) === undefined) {
		throw new Error("ml-task status returned no object.",);
	}
	const settings = await ctx.run<JsonRecord>(["ml-task", "get-settings", analysisId, mlTaskId,],);
	if (asRecord(settings,) === undefined) {
		throw new Error("ml-task get-settings returned no object.",);
	}
	await ctx.run([
		"ml-task",
		"set-settings",
		analysisId,
		mlTaskId,
		"--data",
		JSON.stringify(settings,),
	],);
	const reread = await ctx.run<JsonRecord>(["ml-task", "get-settings", analysisId, mlTaskId,],);
	if (JSON.stringify(reread,) !== JSON.stringify(settings,)) {
		throw new Error("ml-task set-settings round-trip changed the settings document.",);
	}
}

/** Non-destructive saved-model surface over the retained example. */
async function exerciseSavedModelMetadata(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
	const savedModelId = requireMlId(ctx, "savedModelId",);
	const saved = await ctx.run<JsonRecord>(["saved-model", "get", savedModelId,],);
	if (asString(saved["id"],) !== savedModelId) {
		throw new Error("saved-model get returned a different saved model.",);
	}
	await ctx.run<JsonRecord>([
		"saved-model",
		"update-settings",
		savedModelId,
		"--data",
		JSON.stringify(saved,),
	],);

	const versions = await ctx.run<unknown[]>(["saved-model", "list-versions", savedModelId,],);
	const versionId = activeVersionId(versions,);
	if (!versionId) throw new Error("Retained saved model has no versions.",);
	const details = await ctx.run<JsonRecord>([
		"saved-model",
		"version-details",
		savedModelId,
		versionId,
	],);
	if (asRecord(details,) === undefined) {
		throw new Error("saved-model version-details returned no object.",);
	}
	await ctx.run(["saved-model", "version-snippet", savedModelId, versionId,],);
	await ctx.run(["saved-model", "set-active", savedModelId, versionId,],);

	// DSS userMeta has a fixed key set; arbitrary markers belong in the
	// documented "custom fields" container userMeta.customMeta.kv (the official
	// client round-trips the whole userMeta document).
	const userMeta = asRecord(details["userMeta"],) ?? {};
	const customMeta = asRecord(userMeta["customMeta"],) ?? { kv: {}, };
	const marker = `retained_${ctx.iteration}`;
	const nextMeta = {
		...userMeta,
		customMeta: {
			...customMeta,
			kv: { ...asRecord(customMeta["kv"],), liveSuiteMarker: marker, },
		},
	};
	await ctx.run<JsonRecord>([
		"saved-model",
		"set-user-meta",
		savedModelId,
		versionId,
		"--data",
		JSON.stringify(nextMeta,),
	],);
	const rereadDetails = await ctx.run<JsonRecord>([
		"saved-model",
		"version-details",
		savedModelId,
		versionId,
	],);
	const rereadKv = asRecord(asRecord(rereadDetails["userMeta"],)?.["customMeta"],)?.["kv"];
	if (asRecord(rereadKv,)?.["liveSuiteMarker"] !== marker) {
		throw new Error("saved-model set-user-meta did not persist the customMeta marker.",);
	}
	rememberMlId(ctx, "savedModelVersionId", versionId,);
}

/**
 * Download the version scoring artifact (`kind`: "jar" | "pmml"). Status
 * codes (401/403/501) are classified by the runner; a non-status license
 * gate maps to an explicit blocker, everything else stays a real failure.
 */
async function downloadScoringArtifact(
	ctx: LiveContext,
	kind: "jar" | "pmml",
): Promise<void> {
	await provisionMlFixtures(ctx,);
	const savedModelId = requireMlId(ctx, "savedModelId",);
	const versionId = requireMlId(ctx, "savedModelVersionId",);
	const output = path.join(
		ctx.dir,
		`ml_scoring_${kind}_${ctx.iteration}.${kind === "jar" ? "jar" : "xml"}`,
	);
	await mkdir(path.dirname(output,), { recursive: true, },);
	try {
		const result = await ctx.run<{ path?: string; bytes?: number; }>([
			"saved-model",
			kind === "jar" ? "download-scoring-jar" : "download-scoring-pmml",
			savedModelId,
			versionId,
			"--output",
			output,
		],);
		if (asString(result["path"],) !== output) {
			throw new Error(`saved-model download-scoring-${kind} wrote an unexpected path.`,);
		}
		const bytes = (await stat(output,)).size;
		if (bytes <= 0) {
			throw new Error(`saved-model download-scoring-${kind} produced an empty file.`,);
		}
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error,);
		// Status codes (401/403/501) are already classified by the runner for
		// required:false cases; only a non-status license gate maps here.
		if (/licen[cs]e/iu.test(message,)) {
			capabilityBlocked(
				`saved-model download-scoring-${kind} is license-gated on this DSS: ${message}`,
			);
		}
		throw error;
	}
}

/**
 * Retained scoring recipe: score the full training table through the retained
 * saved model, run the recipe, then download the real scored dataset rows.
 */
async function provisionRetainedScoring(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
	const ml = mlFixtures(ctx,);

	// The scoring recipe input is the *saved model*. Retention invariant: a
	// recipe is lab-owned only when its full graph matches this lab's fixtures
	// by readback (model + dataset inputs, scored output). Exactly one match is
	// REUSED (repairing any stale recorded name); zero matches create a new
	// recipe; more than one is ambiguous and errors without touching anything
	// (a same-graph clone may be user-owned). Names alone are never trusted.
	const scored = asString(ml.scoredDataset,) ?? RETAINED_SCORED_DATASET;
	ml.scoredDataset = scored;
	await ensureManagedMlOutput(ctx, scored,);
	let recipe = await findOwnedRecipe(
		ctx,
		"prediction_scoring",
		(recipeRecord,) =>
			roleItemRef(recipeRecord, "inputs", "model",) === ml.savedModelId
			&& roleItemRef(recipeRecord, "inputs", "main",) === ml.datasetName
			&& roleItemRef(recipeRecord, "outputs", "main",) === scored,
	);
	if (!recipe) {
		// DSS 15 rejects (500) a prediction_scoring creation whose prototype
		// lacks the model input role: the official client's
		// PredictionScoringRecipeCreator always sets inputs {main, model} BEFORE
		// the POST (recipe.py with_input_model/_with_input + create_recipe). DSS
		// renames the recipe server-side (observed: score_<inputDataset>), so the
		// name from the creation receipt is authoritative.
		const createdRecipe = await ctx.client.recipes.create({
			type: "prediction_scoring",
			name: RETAINED_SCORE_RECIPE,
			inputs: {
				main: { items: [{ ref: ml.datasetName, deps: [], },], },
				model: { items: [{ ref: ml.savedModelId, deps: [], },], },
			},
			outputs: { main: { items: [{ ref: scored, appendMode: false, },], }, },
		},);
		recipe = createdRecipe.recipeName;
	}
	ml.scoringRecipe = recipe;
	// The scoring output dataset starts with an empty schema (POST
	// /datasets/managed creates {columns: []}); without a materialized schema
	// the scoring stream writer emits a headerless empty file (observed: 23-byte
	// gzip) even though the job reports DONE. Materialize the output schema with
	// the canonical context helper (official RequiredSchemaUpdates flow:
	// GET .../schema-update + POST .../actions/updateOutputSchema) BEFORE the
	// CLI run so the job writes the real 24 scored rows.
	await ctx.propagateRecipeSchema(recipe,);
	await ctx.run(["recipe", "validate-graph", recipe,],);
	await ctx.run(["recipe", "run", recipe, "--wait", "--timeout", "180000",],);

	const preview = await ctx.run<{ rowCount: number; }>([
		"dataset",
		"preview",
		scored,
		"--max-rows",
		"100",
	],);
	if (preview.rowCount !== 24) {
		throw new Error(`Scoring recipe output expected 24 rows, got ${preview.rowCount}.`,);
	}
	const download = await ctx.run<{ path: string; rows: number; }>([
		"dataset",
		"download",
		scored,
		"--limit",
		"100",
		"--output",
		path.join(ctx.dir, `ml_scored_${ctx.iteration}.csv`,),
	],);
	if (download.rows !== 24) {
		throw new Error(`Scored dataset download expected 24 rows, got ${download.rows}.`,);
	}
}

/**
 * Retained model evaluation: create an evaluation store, wire an evaluation
 * recipe (dataset + saved model -> store), run it, then read the
 * evaluations back from the store.
 */
async function provisionRetainedEvaluation(ctx: LiveContext,): Promise<void> {
	await provisionMlFixtures(ctx,);
	const ml = mlFixtures(ctx,);
	const storeName = RETAINED_EVAL_STORE_NAME;

	if (typeof ml.evaluationStoreId !== "string") {
		const stores = await ctx.run<Array<{ id?: string; name?: string; }>>([
			"model-evaluation-store",
			"list",
		],);
		const existing = (Array.isArray(stores,) ? stores : []).find(item => item?.name === storeName);
		if (existing) {
			ml.evaluationStoreId = requireId(existing["id"], "retained evaluation store id",);
		} else {
			const created = await ctx.run<JsonRecord>([
				"model-evaluation-store",
				"create",
				"--name",
				storeName,
			],);
			ml.evaluationStoreId = rememberMlId(
				ctx,
				"evaluationStoreId",
				asString(created["created"] ?? created["id"],),
			);
		}
	}

	const output = asString(ml.scoredDataset,) ?? RETAINED_SCORED_DATASET;
	const evalOutput = `${output}_eval`;
	await ensureManagedMlOutput(ctx, evalOutput,);
	// Same reuse-or-create invariant as scoring: exactly one full-graph match
	// (readback-verified) is reused; ambiguity errors without touching anything.
	let recipe = await findOwnedRecipe(
		ctx,
		"evaluation",
		(recipeRecord,) =>
			roleItemRef(recipeRecord, "inputs", "model",) === ml.savedModelId
			&& roleItemRef(recipeRecord, "inputs", "main",) === ml.datasetName
			&& roleItemRef(recipeRecord, "outputs", "main",) === evalOutput
			&& roleItemRef(recipeRecord, "outputs", "evaluationStore",) === ml.evaluationStoreId,
	);
	if (!recipe) {
		// Evaluation recipes also need the model input role at creation time (the
		// official EvaluationRecipeCreator sets inputs {main, model} and outputs
		// {main, metrics?, evaluationStore} before POST /recipes/). Metrics output
		// is optional; the evaluation store must already exist. As with scoring,
		// the server-final name from the receipt is authoritative.
		const createdRecipe = await ctx.client.recipes.create({
			type: "evaluation",
			name: RETAINED_EVAL_RECIPE,
			inputs: {
				main: { items: [{ ref: ml.datasetName, deps: [], },], },
				model: { items: [{ ref: ml.savedModelId, deps: [], },], },
			},
			outputs: {
				main: { items: [{ ref: evalOutput, appendMode: false, },], },
				evaluationStore: { items: [{ ref: ml.evaluationStoreId, appendMode: false, },], },
			},
		},);
		recipe = createdRecipe.recipeName;
	}
	ml.evaluationRecipe = recipe;
	// No schema propagation here: the MES case passes on its store semantics and
	// Main ruled out broadening MES behavior; only the scoring case needs the
	// propagation (its scored dataset rows are asserted). The output dataset is
	// still ensured and registered in the canonical ownership registry above.
	await ctx.run(["recipe", "validate-graph", recipe,],);
	await ctx.run(["recipe", "run", recipe, "--wait", "--timeout", "180000",],);

	const evaluations = await ctx.run<unknown[]>([
		"model-evaluation-store",
		"list-evaluations",
		ml.evaluationStoreId,
	],);
	if (!Array.isArray(evaluations,) || evaluations.length === 0) {
		throw new Error("Evaluation store has no evaluations after running the evaluation recipe.",);
	}
	await ctx.run(["model-evaluation-store", "get", ml.evaluationStoreId,],);
}

/** MES CRUD in a disposable project: dry-run proof then real lifecycle. */
async function exerciseMesLifecycle(ctx: LiveContext,): Promise<void> {
	const projectKey = await ctx.createProject("mescap",);
	const failures: unknown[] = [];
	try {
		const dryRun = await ctx.run<JsonRecord>([
			"model-evaluation-store",
			"create",
			"--name",
			`live_mes_${ctx.iteration}`,
			"--dry-run",
			"--project-key",
			projectKey,
		],);
		if (dryRun["dryRun"] !== true) {
			throw new Error("model-evaluation-store create --dry-run did not report dryRun=true.",);
		}
		const created = await ctx.run<JsonRecord>([
			"model-evaluation-store",
			"create",
			"--name",
			`live_mes_${ctx.iteration}`,
			"--project-key",
			projectKey,
		],);
		const storeId = requireId(created["created"] ?? created["id"], "model evaluation store",);

		const listed = await ctx.run<unknown[]>([
			"model-evaluation-store",
			"list",
			"--project-key",
			projectKey,
		],);
		if (
			!Array.isArray(listed,)
			|| !listed.some(item => asRecord(item,)?.["id"] === storeId)
		) {
			throw new Error(`Created evaluation store ${storeId} missing from list.`,);
		}
		const got = await ctx.run<JsonRecord>([
			"model-evaluation-store",
			"get",
			storeId,
			"--project-key",
			projectKey,
		],);
		if (asString(got["id"],) !== storeId) {
			throw new Error("model-evaluation-store get returned a different store.",);
		}
		const evaluations = await ctx.run<unknown[]>([
			"model-evaluation-store",
			"list-evaluations",
			storeId,
			"--project-key",
			projectKey,
		],);
		if (!Array.isArray(evaluations,)) {
			throw new Error("model-evaluation-store list-evaluations returned no array.",);
		}
		await ctx.run([
			"model-evaluation-store",
			"delete",
			storeId,
			"--if-exists",
			"--project-key",
			projectKey,
		],);
	} catch (error) {
		failures.push(error,);
	} finally {
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			failures.push(`project delete ${projectKey}: ${errorMessage(error,).slice(0, 200,)}`,);
		}
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length) throw new AggregateError(failures, "Live case or cleanup failed",);
}

// ---------------------------------------------------------------------------
// MLflow pyfunc import + evaluation in a disposable project
// ---------------------------------------------------------------------------

/** One built MLflow model folder + archive. */
interface MlflowModelBuild {
	archivePath: string;
	codeDir: string;
	versionId: string;
}

function mlflowLoaderCode(): string {
	return `"""Pure-python pyfunc loader for the live-suite example model."""
import json
import os


class ChurnThresholdModel:
    def __init__(self, positive_class, threshold):
        self.positive_class = positive_class
        self.threshold = threshold

    def predict(self, dataframe, params=None):
        import pandas as pd

        support = dataframe["support_tickets"].astype(float)
        tenure = dataframe["tenure_months"].astype(float)
        positive = ((support >= 3.0) | (tenure < 6.0)).astype(float) * 0.98 + 0.01
        return pd.DataFrame({"1": positive, "0": 1.0 - positive})


def _load_pyfunc(path):
    with open(os.path.join(path, "model_spec.json"), encoding="utf-8") as handle:
        spec = json.load(handle)
    return ChurnThresholdModel(spec["positive_class"], spec["decision_threshold"])
`;
}

const MLFLOW_MODEL_FILES: Record<string, string> = {
	// Canonical python_function flavor per the mlflow upstream contract
	// (mlflow/pyfunc/__init__.py "MLModel configuration"): `code` is added to
	// sys.path before importing loader_module, `data` is the path passed to
	// _load_pyfunc, `env` names the environment file. `model_path` is not a
	// pyfunc key — the raw live probe failed with ModuleNotFoundError
	// 'mlflow_loader' until these were correct.
	"MLmodel": `${
		JSON.stringify(
			{
				artifact_path: "model",
				flavors: {
					python_function: {
						code: "code",
						data: "model",
						env: "conda.yaml",
						loader_module: "mlflow_loader",
						python_version: "3.10.0",
					},
				},
				mlflow_version: "2.0.1",
				run_id: "live-suite-retained",
				utc_time_created: "1970-01-01T00:00:00.000000Z",
			},
			null,
			2,
		)
	}\n`,
	"model/model_spec.json": `${
		JSON.stringify(
			{
				positive_class: 1,
				decision_threshold: 0.5,
			},
			null,
			2,
		)
	}\n`,
	"code/mlflow_loader.py": mlflowLoaderCode(),
	"requirements.txt": "",
	"python_env.yaml": "python: 3.10.0\nbuild_dependencies:\n- pip:\n  - mlflow==2.0.1\n",
	"conda.yaml": "channels:\n- defaults\ndependencies:\n- python=3.10\nname: live-suite-example\n",
};

/**
 * Official external-ml metadata mutation (dataikuapi DSSSavedModelVersion
 * `_init_model_version_info`): GET the metadata document, then set FLAT keys —
 * `targetColumnName`, `classLabels` as [{"label": ...}] objects, and either
 * `gatherFeaturesFromDataset` (a dataset ref) or `features` as
 * [{"name", "type"}] — then PUT the whole document back. Nested documents
 * (e.g. a coreMetadata/versionTag wrapper) are not part of the contract and
 * are rejected with 400.
 */
function withCoreExternalMetadata(
	metadata: JsonRecord,
	datasetRef?: string,
): JsonRecord {
	return {
		...metadata,
		targetColumnName: "churn_flag",
		classLabels: [{ label: "0", }, { label: "1", },],
		...(datasetRef !== undefined ? { gatherFeaturesFromDataset: datasetRef, } : {}),
	};
}

/** Version ids of a saved model (the server list is the source of truth). */
async function mlflowVersionIds(
	ctx: LiveContext,
	savedModelId: string,
	projectKey: string,
): Promise<string[]> {
	const versions = await ctx.run<unknown[]>([
		"saved-model",
		"list-versions",
		savedModelId,
		"--project-key",
		projectKey,
	],);
	return (Array.isArray(versions,) ? versions : [])
		.map((entry,) => asString(asRecord(entry,)?.["id"],))
		.filter((id,): id is string => Boolean(id,));
}

function projectFiles(
	files: Record<string, string>,
): Array<{ relative: string; content: string; }> {
	return Object.entries(files,).map(([relative, content,],) => ({ relative, content, }));
}

/** Single-line, redaction-safe message for a CLI failure. */
function commandErrorSummary(error: LiveCommandError,): string {
	return error.message.replace(/\s+/gu, " ",).slice(0, 400,);
}
/**
 * True only for module-missing evidence (the code env ships no mlflow
 * package), never generic code-env or payload text.
 */
function isMlflowEnvironmentError(message: string,): boolean {
	return /no module named ['"]?[\w.]*mlflow/iu.test(message,)
		|| /importerror:[^\n]*mlflow/iu.test(message,)
		|| /mlflow[^\n]{0,40}(is not installed|not installed)/iu.test(message,);
}

/** Map missing-MLflow-runtime failures to an explicit capability blocker. */
function classifyMlflowFailure(error: unknown,): never {
	if (error instanceof LiveCommandError) {
		const summary = commandErrorSummary(error,);
		if (isMlflowEnvironmentError(summary,)) {
			const configured = process.env[MLFLOW_CODE_ENV_ENV];
			const override = configured
				? ` (using ${configured})`
				: ` (set ${MLFLOW_CODE_ENV_ENV} to override)`;
			capabilityBlocked(
				`MLflow runtime unavailable on DSS (code env must ship mlflow): ${summary}${override}`,
			);
		}
	}
	throw error;
}

export interface MlflowCodeEnvCandidate {
	envName?: string;
	deploymentMode?: string;
}

/**
 * Pure selection rule for the MLflow import runtime: the first candidate whose
 * packages include mlflow, skipping every DSS-internal deploymentMode env
 * (deploymentMode "DSS_INTERNAL" — its rows carry arbitrary envName values,
 * e.g. INTERNAL_retrieval_augmented_generation_v1). Internal runtimes are
 * never auto-selected even when they ship mlflow; one can only be chosen
 * deliberately through DATAIKU_LIVE_MLFLOW_CODE_ENV.
 */
export function selectMlflowCodeEnvironment(
	environments: MlflowCodeEnvCandidate[],
	packagesByEnvName: Record<string, string[]>,
): string | undefined {
	for (const environment of environments) {
		const name = asString(environment?.envName,);
		if (!name || environment?.deploymentMode === "DSS_INTERNAL") continue;
		const packages = packagesByEnvName[name] ?? [];
		if (packages.some((entry,) => /^mlflow(\W|$)/u.test(String(entry,).trim().toLowerCase(),))) {
			return name;
		}
	}
	return undefined;
}

/**
 * MLflow runtime resolution for one run. envName set  => both MLflow cases use
 * it; blocked set => each selected case reports that evidence as its own
 * blocked result (never aborting the rest of the matrix). Resolution order:
 * explicit DATAIKU_LIVE_MLFLOW_CODE_ENV -> existing eligible PYTHON env with
 * mlflow evidence -> (owned env provisioning, pending canonical helper) ->
 * blocked. Never creates or modifies a code environment here; DSS rejects
 * INHERIT when no project/instance default exists (live evidence: 400 "Please
 * specify a code environment or define a default code environment at the
 * project or instance level.") and an explicit non-MLflow env import was
 * observed to fail at model load (500 "Interactive Model Python kernel failed
 * to start. Check that code environment ... contains core packages and visual
 * ML packages.").
 */
interface MlflowRuntimeResolution {
	envName?: string;
	blocked?: string;
}

async function resolveMlflowRuntime(ctx: LiveContext,): Promise<MlflowRuntimeResolution> {
	const explicit = process.env[MLFLOW_CODE_ENV_ENV]?.trim();
	if (explicit) return { envName: explicit, };
	let environments: MlflowCodeEnvCandidate[];
	try {
		environments = await ctx.run(["code-env", "list", "--lang", "PYTHON",],);
	} catch (error) {
		return {
			blocked: `Cannot inspect code environments to select an MLflow runtime: ${
				errorMessage(error,).slice(0, 200,)
			}. Set ${MLFLOW_CODE_ENV_ENV} to a PYTHON code environment with mlflow installed.`,
		};
	}
	const eligible = (Array.isArray(environments,) ? environments : [])
		.filter(environment => asString(environment?.envName,) !== undefined)
		.filter(environment => environment?.deploymentMode !== "DSS_INTERNAL");
	const packagesByEnvName: Record<string, string[]> = {};
	for (const environment of eligible) {
		const name = asString(environment.envName,)!;
		const details = await ctx.run<{
			requestedPackages?: unknown[];
			installedPackages?: unknown[];
		}>(["code-env", "get", "PYTHON", name,],);
		packagesByEnvName[name] = [
			...(Array.isArray(details.requestedPackages,) ? details.requestedPackages : []),
			...(Array.isArray(details.installedPackages,) ? details.installedPackages : []),
		].map((entry,) => String(entry,).trim());
	}
	const selected = selectMlflowCodeEnvironment(eligible, packagesByEnvName,);
	if (selected) return { envName: selected, };
	return {
		blocked: `No usable MLflow runtime available. Checked eligible PYTHON code environment(s): ${
			eligible.map(environment => asString(environment.envName,)).filter(Boolean,).join(", ",)
			|| "none"
		} — none lists mlflow in requested or installed packages; an explicit import attempt with a non-MLflow environment was observed to fail at model load (500: "Interactive Model Python kernel failed to start. Check that code environment ... contains core packages and visual ML packages."), and DSS rejects INHERIT when no project/instance default code environment exists. Set ${MLFLOW_CODE_ENV_ENV} to an environment shipping mlflow (or provision an owned compatible code environment) before importing MLflow models.`,
	};
}

/**
 * Requested packages for the owned MLflow runtime, pinned to the baseline that
 * was proven live in the controlled probe (PROBE97): with these exact versions
 * in an owned env created on the instance's existing interpreter, the custom
 * pure-python pyfunc model both imported AND evaluated successfully, and the
 * generated sklearn control model started its kernel. Core packages come from
 * the helper's create params, with the core package set pinned to PANDAS22 so
 * the pandas==2.2.3 spec never fights the server-default core set.
 */
const MLFLOW_RUNTIME_PACKAGES = [
	"mlflow==3.10.1",
	"scikit-learn==1.6.1",
	"scipy==1.13.1",
	"statsmodels==0.14.6",
	"pandas==2.2.3",
] as const;

/**
 * Readback gate for a freshly provisioned owned runtime: never import blind.
 * The awaited package build has already completed, so mlflow must appear in
 * the environment's INSTALLED packages (requested-only is not proof the
 * runtime actually has it); otherwise both checks report the observed package
 * state as blocked evidence.
 */
async function verifyOwnedMlflowRuntime(
	ctx: LiveContext,
	envName: string,
): Promise<MlflowRuntimeResolution> {
	const details = await ctx.run<{
		requestedPackages?: unknown[];
		installedPackages?: unknown[];
	}>(["code-env", "get", "PYTHON", envName,],);
	const requested = Array.isArray(details.requestedPackages,) ? details.requestedPackages : [];
	const installed = Array.isArray(details.installedPackages,) ? details.installedPackages : [];
	const verified = selectMlflowCodeEnvironment(
		[{ envName, },],
		{ [envName]: installed.map((entry,) => String(entry,)), },
	);
	if (verified !== envName) {
		return {
			blocked:
				`Owned MLflow runtime ${envName} was provisioned but mlflow is not among its INSTALLED packages (installed: ${installed.length}, requested: ${requested.length}); refusing a blind import.`,
		};
	}
	return { envName, };
}

/**
 * Run both MLflow checks against the one runtime resolved for this run. A
 * missing runtime is reported as each selected check's own blocked result, so
 * neither case hides the other and the rest of the matrix never aborts.
 */
async function runMlflowChecks(
	ctx: LiveContext,
	resolution: MlflowRuntimeResolution,
): Promise<void> {
	await ctx.check(
		"ml.mlflow.import",
		[
			"project.create",
			"saved-model.create-external",
			"saved-model.import-mlflow-version",
			"saved-model.import-mlflow-version-from-folder",
			"saved-model.list-versions",
			"saved-model.version-details",
			"saved-model.external-metadata-get",
			"saved-model.external-metadata-put",
			"saved-model.delete-versions",
			"saved-model.delete",
			"project.delete",
		],
		async () => {
			if (resolution.envName === undefined) {
				capabilityBlocked(resolution.blocked ?? "MLflow runtime unavailable.",);
			}
			await exerciseMlflowImport(ctx, resolution.envName,);
		},
		{ required: false, },
	);
	await ctx.check(
		"ml.mlflow.evaluate",
		[
			"project.create",
			"saved-model.create-external",
			"saved-model.import-mlflow-version",
			"saved-model.external-metadata-put",
			"saved-model.evaluate-version",
			"saved-model.version-details",
			"saved-model.delete",
			"project.delete",
		],
		async () => {
			if (resolution.envName === undefined) {
				capabilityBlocked(resolution.blocked ?? "MLflow runtime unavailable.",);
			}
			await exerciseMlflowEvaluate(ctx, resolution.envName,);
		},
		{ required: false, },
	);
}

/** Create the MLFLOW_PYFUNC saved model backing the imported versions. */
async function createMlflowModel(ctx: LiveContext, projectKey: string,): Promise<string> {
	const created = await ctx.run<JsonRecord>([
		"saved-model",
		"create-external",
		MLFLOW_MODEL_NAME,
		"--type",
		"MLFLOW_PYFUNC",
		"--prediction-type",
		"BINARY_CLASSIFICATION",
		"--project-key",
		projectKey,
	],);
	return requireId(created["created"] ?? created["id"], "MLflow saved model",);
}

/**
 * Real local MLflow pyfunc import: two versions through both documented
 * transports (local zip archive + managed folder), version listing, external
 * metadata GET-then-PUT, then full cleanup of the case-owned model.
 */
async function exerciseMlflowImport(ctx: LiveContext, codeEnvName: string,): Promise<void> {
	const projectKey = await ctx.createProject("mlflowcap",);
	const failures: unknown[] = [];
	let savedModelId: string | undefined;
	try {
		savedModelId = await createMlflowModel(ctx, projectKey,);
		const v1 = await buildMlflowModel(ctx,);
		await ctx.run([
			"saved-model",
			"import-mlflow-version",
			savedModelId,
			v1.versionId,
			"--archive",
			v1.archivePath,
			"--set-active",
			"true",
			"--code-env",
			codeEnvName,
			"--project-key",
			projectKey,
		],);
		// Assert per import step against the SERVER's returned ids (the version
		// list is the source of truth), not against the requested path segment.
		const idsAfterV1 = await mlflowVersionIds(ctx, savedModelId, projectKey,);
		if (idsAfterV1.length !== 1) {
			throw new Error(
				`Expected exactly one version after the archive import, got ${idsAfterV1.length}.`,
			);
		}
		const v1Id = idsAfterV1[0]!;
		await ctx.run([
			"saved-model",
			"version-details",
			savedModelId,
			v1Id,
			"--project-key",
			projectKey,
		],);
		// External-ml metadata GET-then-PUT round-trip (official flat mutation;
		// no dataset in this case, so only target + class labels are set).
		const metadata = await ctx.run<JsonRecord>([
			"saved-model",
			"external-metadata-get",
			savedModelId,
			v1Id,
			"--project-key",
			projectKey,
		],);
		await ctx.run<JsonRecord>([
			"saved-model",
			"external-metadata-put",
			savedModelId,
			v1Id,
			"--data",
			JSON.stringify(withCoreExternalMetadata(metadata,),),
			"--project-key",
			projectKey,
		],);

		const staged = await stageMlflowFolder(ctx, projectKey,);
		await ctx.run([
			"saved-model",
			"import-mlflow-version-from-folder",
			savedModelId,
			"v2",
			"--folder",
			staged.folderRef,
			"--path",
			staged.path,
			"--set-active",
			"false",
			"--code-env",
			codeEnvName,
			"--project-key",
			projectKey,
		],);
		const idsAfterV2 = await mlflowVersionIds(ctx, savedModelId, projectKey,);
		const v2Id = idsAfterV2.find(id => id !== v1Id);
		if (v2Id === undefined || idsAfterV2.length !== 2) {
			throw new Error(
				`Expected two distinct versions after the managed-folder import, got ${idsAfterV2.length}: ${
					idsAfterV2.join(", ",)
				}.`,
			);
		}

		await ctx.run([
			"saved-model",
			"delete-versions",
			savedModelId,
			v2Id,
			"--remove-intermediate",
			"true",
			"--project-key",
			projectKey,
		],);
	} catch (error) {
		failures.push(classifyMlflowFailure(error,),);
	} finally {
		// Cleanup by receipt id, never by name: the model is known-created, so
		// delete WITHOUT --if-exists must actually remove it, and the list
		// readback proves the exact id is gone (no skip/false coverage).
		// project.delete below stays the last-resort fallback.
		if (savedModelId !== undefined) {
			try {
				await ctx.run([
					"saved-model",
					"delete",
					savedModelId,
					"--project-key",
					projectKey,
				],);
				const remaining = await ctx.run<unknown[]>([
					"saved-model",
					"list",
					"--project-key",
					projectKey,
				],);
				const stillPresent = (Array.isArray(remaining,) ? remaining : [])
					.some(entry => asRecord(entry,)?.["id"] === savedModelId);
				if (stillPresent) {
					failures.push(`saved-model ${savedModelId} still listed after delete`,);
				}
			} catch (error) {
				failures.push(
					`saved-model delete ${savedModelId}: ${errorMessage(error,).slice(0, 200,)}`,
				);
			}
		}
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			failures.push(`project delete ${projectKey}: ${errorMessage(error,).slice(0, 200,)}`,);
		}
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length) throw new AggregateError(failures, "Live case or cleanup failed",);
}

/** Build the local MLflow model folder + zip once per run. */
async function buildMlflowModel(ctx: LiveContext,): Promise<MlflowModelBuild> {
	const versionId = MLFLOW_VERSIONS[0]!;
	const codeDir = path.join(ctx.dir, `mlflow_model_${ctx.iteration}_${versionId}`,);
	await mkdir(codeDir, { recursive: true, },);
	for (const { relative, content, } of projectFiles(MLFLOW_MODEL_FILES,)) {
		const destination = path.join(codeDir, relative,);
		await mkdir(path.dirname(destination,), { recursive: true, },);
		await Bun.write(destination, content,);
	}
	const archivePath = path.join(ctx.dir, `mlflow_model_${ctx.iteration}_${versionId}.zip`,);
	const proc = Bun.spawn(["zip", "-q", "-r", archivePath, ".",], {
		cwd: codeDir,
		stdout: "pipe",
		stderr: "pipe",
	},);
	const exit = await proc.exited;
	if (exit !== 0) {
		const stderr = await new Response(proc.stderr,).text();
		throw new Error(`zip failed for MLflow model archive: ${stderr.slice(0, 300,).trim()}`,);
	}
	if ((await stat(archivePath,)).size === 0) {
		throw new Error("MLflow model archive is empty.",);
	}
	return { archivePath, codeDir, versionId, };
}

/** Build the local MLflow model and upload it to a managed folder. */
async function stageMlflowFolder(
	ctx: LiveContext,
	projectKey: string,
): Promise<{ folderRef: string; path: string; }> {
	await ctx.run([
		"folder",
		"create",
		"--name",
		"mlflow_model_folder",
		"--project-key",
		projectKey,
	],);
	const folders = await ctx.run<Array<{ id: string; name?: string; }>>([
		"folder",
		"list",
		"--project-key",
		projectKey,
	],);
	const folder = folders.find(item => item?.name === "mlflow_model_folder");
	if (!folder) throw new Error("MLflow staging folder was not created.",);
	for (const { relative, content, } of projectFiles(MLFLOW_MODEL_FILES,)) {
		const localPath = await ctx.writeFile(`mlflow_model_${ctx.iteration}/${relative}`, content,);
		await ctx.run([
			"folder",
			"upload",
			folder.id,
			`/model/${relative}`,
			localPath,
			"--project-key",
			projectKey,
		],);
	}
	return { folderRef: `${projectKey}.${folder.id}`, path: "/model", };
}

/**
 * Real evaluation of the imported pyfunc model on the training dataset:
 * external metadata first, then evaluate-version with per-class probability
 * outputs matching the fixture schema.
 */
async function exerciseMlflowEvaluate(ctx: LiveContext, codeEnvName: string,): Promise<void> {
	const projectKey = await ctx.createProject("mlfloweval",);
	const failures: unknown[] = [];
	let savedModelId: string | undefined;
	try {
		const datasetName = await provisionMlDataset(
			ctx,
			projectKey,
			`mlflow_eval_train_${ctx.iteration}`,
		);
		savedModelId = await createMlflowModel(ctx, projectKey,);
		const model = await buildMlflowModel(ctx,);
		await ctx.run([
			"saved-model",
			"import-mlflow-version",
			savedModelId,
			model.versionId,
			"--archive",
			model.archivePath,
			"--set-active",
			"true",
			"--code-env",
			codeEnvName,
			"--project-key",
			projectKey,
		],);
		// Official metadata flow for evaluation: GET the metadata document,
		// set flat keys (targetColumnName, classLabels as [{label}], and
		// gatherFeaturesFromDataset pointing at the evaluation dataset — the
		// sanctioned alternative to inventing a features list), then PUT it
		// back; evaluate-version follows. The evaluation dataset ref uses the
		// same qualified name as evaluate-version.
		const metadata = await ctx.run<JsonRecord>([
			"saved-model",
			"external-metadata-get",
			savedModelId,
			model.versionId,
			"--project-key",
			projectKey,
		],);
		await ctx.run<JsonRecord>([
			"saved-model",
			"external-metadata-put",
			savedModelId,
			model.versionId,
			"--data",
			JSON.stringify(withCoreExternalMetadata(metadata, `${projectKey}.${datasetName}`,),),
			"--project-key",
			projectKey,
		],);
		await ctx.run([
			"saved-model",
			"evaluate-version",
			savedModelId,
			model.versionId,
			"--dataset",
			`${projectKey}.${datasetName}`,
			"--sampling",
			JSON.stringify({ samplingMethod: "HEAD_SEQUENTIAL", maxRecords: 24, },),
			"--skip-expensive-reports",
			"true",
			"--project-key",
			projectKey,
		],);
		const details = await ctx.run<JsonRecord>([
			"saved-model",
			"version-details",
			savedModelId,
			model.versionId,
			"--project-key",
			projectKey,
		],);
		if (asRecord(details,) === undefined) {
			throw new Error("version-details returned no object after evaluation.",);
		}
	} catch (error) {
		failures.push(classifyMlflowFailure(error,),);
	} finally {
		// Receipt-id cleanup, identical to the import case: delete WITHOUT
		// --if-exists (the model is known-created), then prove the exact id is
		// absent from the project list; project.delete stays the fallback.
		if (savedModelId !== undefined) {
			try {
				await ctx.run([
					"saved-model",
					"delete",
					savedModelId,
					"--project-key",
					projectKey,
				],);
				const remaining = await ctx.run<unknown[]>([
					"saved-model",
					"list",
					"--project-key",
					projectKey,
				],);
				const stillPresent = (Array.isArray(remaining,) ? remaining : [])
					.some(entry => asRecord(entry,)?.["id"] === savedModelId);
				if (stillPresent) {
					failures.push(`saved-model ${savedModelId} still listed after delete`,);
				}
			} catch (error) {
				failures.push(
					`saved-model delete ${savedModelId}: ${errorMessage(error,).slice(0, 200,)}`,
				);
			}
		}
		try {
			await ctx.deleteProject(projectKey,);
		} catch (error) {
			failures.push(`project delete ${projectKey}: ${errorMessage(error,).slice(0, 200,)}`,);
		}
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length) throw new AggregateError(failures, "Live case or cleanup failed",);
}

// ---------------------------------------------------------------------------
// Infrastructure scenario: read-only SQL constant assertion
// ---------------------------------------------------------------------------

/**
 * Read-only SQL smoke: an explicitly configured connection/dataset target wins
 * (DATAIKU_SQL_CONNECTION / DATAIKU_SQL_DATASET_FULL_NAME); otherwise the case
 * self-provisions an owned disposable SQLite connection through demo-project's
 * canonical helper. The probe never mutates data and never touches foreign
 * connections or code environments; the owned connection is ledger-bound for
 * the body only and deleted by the helper's own finally.
 */
async function exerciseInfrastructureSql(ctx: LiveContext,): Promise<void> {
	const connection = process.env[SQL_CONNECTION_ENV]?.trim();
	const datasetFullName = process.env[SQL_DATASET_ENV]?.trim();
	const projectKey = await ctx.createProject("sqlsel",);
	// Shared assertion for every path: exactly one row whose first cell is 1.
	const runProbe = async (target: { connection?: string; dataset?: string; },): Promise<void> => {
		const targetArgs = target.connection
			? ["--connection", target.connection,]
			: ["--dataset", target.dataset!,];
		const result = await ctx.run<JsonRecord>([
			"sql",
			"query",
			LIVE_SQL_PROBE,
			...targetArgs,
			"--project-key",
			projectKey,
		],);
		const rows = Array.isArray(result["rows"],) ? result["rows"] as unknown[] : [];
		if (rows.length !== 1) {
			throw new Error(`Expected exactly one row from SELECT 1, got ${rows.length}.`,);
		}
		const firstRow: unknown = rows[0];
		const firstCell = Array.isArray(firstRow,) ? firstRow[0] : firstRow;
		const numeric = typeof firstCell === "number"
			? firstCell
			: typeof firstCell === "string"
			? Number(firstCell,)
			: Number.NaN;
		if (numeric !== 1) {
			throw new Error(`SELECT 1 returned an unexpected first cell: ${JSON.stringify(firstCell,)}.`,);
		}
	};
	try {
		if (connection) {
			await runProbe({ connection, },);
		} else if (datasetFullName) {
			await runProbe({ dataset: datasetFullName, },);
		} else {
			await withOwnedSqlConnection(ctx, "sqlselect", async connectionName => {
				await runProbe({ connection: connectionName, },);
			},);
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

/**
 * Exercise the optional ML and infrastructure capability modules. The
 * application-profile cases live in the applications module (live-applications)
 * so their case ids keep one owning module.
 *
 * Phase contract (parent LiveContext): this module creates no persistent
 * fixtures in either phase — setup is a no-op for capability cases and every
 * mutating scenario runs in the run phase on case-owned projects deleted in
 * finally, so repeated selected runs stay idempotent. ctx.selection filtering
 * is applied automatically by ctx.check.
 *
 * Profiles: ML cases require the "ml" profile, the SQL assertion the
 * "infrastructure" profile (explicit target or an owned disposable SQLite
 * connection). Missing prerequisites produce explicit blocked/unsupported
 * CaseResults via LiveCapabilityError — never mock passes; genuine command
 * defects stay failures.
 */
export async function exerciseCapabilities(ctx: LiveContext,): Promise<void> {
	if (ctx.profiles.includes("ml",)) {
		await ctx.check(
			"ml.lifecycle",
			[
				"project.create",
				"dataset.create",
				"dataset.upload-file",
				"dataset.refresh-schema",
				"analysis.create",
				"ml-task.create",
				"ml-task.get-settings",
				"ml-task.train",
				"ml-task.list-models",
				"ml-task.model-details",
				"ml-task.deploy",
				"saved-model.list",
				"saved-model.get",
				"saved-model.list-versions",
				"saved-model.version-details",
				"analysis.delete",
				"project.delete",
			],
			async () => {
				await exerciseMlLifecycle(ctx,);
			},
			{ capability: "ml.visual-ml-lifecycle", },
		);
		await ctx.check(
			"ml.clustering-task",
			[
				"project.create",
				"dataset.create",
				"dataset.upload-file",
				"dataset.refresh-schema",
				"analysis.create",
				"ml-task.create",
				"ml-task.status",
				"ml-task.delete",
				"project.delete",
			],
			async () => {
				await exerciseMlClustering(ctx,);
			},
			{ capability: "ml.visual-ml-clustering", required: false, },
		);
		await exerciseExpandedMl(ctx,);
	}

	if (ctx.profiles.includes("infrastructure",)) {
		await ctx.check(
			"infrastructure.sql-select",
			["project.create", "sql.query", "project.delete",],
			async () => {
				await exerciseInfrastructureSql(ctx,);
			},
			{ capability: "infrastructure.sql-select-readonly", required: false, },
		);
	}
}
