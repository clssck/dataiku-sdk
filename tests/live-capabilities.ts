// Live self-provisioning capability suite: optional ML, Dataiku Application,
// and infrastructure scenario modules. Every mutating case operates on
// case-owned, run-scoped projects created through the parent LiveContext and
// torn down in `finally`, so repeated runs are idempotent and no baseline
// fixture or external/global resource is ever touched. Missing prerequisites
// are reported as explicit "blocked" (or "unsupported") results via
// LiveCapabilityError — never as a mock pass — while genuine command defects
// remain ordinary failures.
import type { LiveContext, } from "./live-context.js";
import { LIVE_CSV_FORMAT, LiveCapabilityError, } from "./live-context.js";

/**
 * Read-only prerequisite for the Application scenario. The template id must be
 * provided explicitly: this module never guesses a template from `dss app
 * list` and never writes to an external template's manifest or version.
 */
const APP_TEMPLATE_ID_ENV = "DATAIKU_LIVE_APP_TEMPLATE_ID";

/**
 * Explicit opt-in for the infrastructure SQL case (matches the legacy
 * integration harness gate in tests/integration-harness.ts). The dataset
 * variant is accepted as an alternative target.
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
 * Provision the case-owned training dataset inside the case-owned project:
 * create an UploadedFiles dataset, upload the deterministic CSV written via
 * ctx.writeFile, and pin the schema explicitly so the ML task sees a stable
 * numeric target.
 */
async function provisionCaseDataset(ctx: LiveContext, projectKey: string,): Promise<string> {
	const datasetName = `live_cap_train_${ctx.iteration}`;
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
		throw new Error("ML fixture must contain24 rows and both target classes",);
	}
	return datasetName;
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
// Application scenario: read-only template prerequisite + instance lifecycle
// ---------------------------------------------------------------------------

/**
 * Resolve the read-only app-template prerequisite. Only an explicit
 * DATAIKU_LIVE_APP_TEMPLATE_ID is accepted: guessing a template from
 * `dss app list` would exercise an unrelated tenant artifact, and no supported
 * CLI path exists to create an owned template.
 */
function appTemplateId(): string {
	const appId = process.env[APP_TEMPLATE_ID_ENV]?.trim();
	if (!appId) {
		capabilityBlocked(
			`No Dataiku App template prerequisite: set ${APP_TEMPLATE_ID_ENV} to an app template this key can read. The applications profile does not guess templates from app list and never writes to external template manifests.`,
		);
	}
	return appId;
}

/** Read-only manifest + instance surface of the prerequisite template. */
async function readTemplateSurface(ctx: LiveContext, appId: string,): Promise<void> {
	const manifest = await ctx.run<JsonRecord>(["app", "manifest", appId,],);
	if (asRecord(manifest,) === undefined) throw new Error("app manifest returned no object.",);
	const instances = await ctx.run<unknown[]>(["app", "instances", appId,],);
	if (!Array.isArray(instances,)) throw new Error("app instances returned no array.",);
}

/**
 * Full instance lifecycle against the prerequisite template: reserve a pending
 * project key owned by this run, create the instance with --wait, bind the
 * project (records its incarnation), verify the instance through the API
 * readiness surface, compare the instance manifest against the template, and
 * delete the instance project in finally via ctx.deleteProject
 * (incarnation-gated). The template itself is never modified, and a generic
 * CREATE_FAILED stays a failure — it is never converted into a blocked result
 * without exact license/capability evidence.
 */
async function exerciseAppInstanceLifecycle(ctx: LiveContext, appId: string,): Promise<void> {
	const reservedKey = await ctx.reserveProject("appinst",);
	const label = `Live capability instance ${ctx.iteration}`;
	let created = false;
	try {
		const creation = await ctx.run<JsonRecord>([
			"app",
			"create-instance",
			appId,
			"--data",
			JSON.stringify({ targetProjectKey: reservedKey, targetProjectName: label, },),
			"--wait",
			"--timeout",
			"180000",
			"--poll-interval",
			"3000",
		],);
		if (creation["success"] !== true) {
			// CREATE_FAILED / INDETERMINATE / VERIFICATION_FAILED are genuine
			// defects or ambiguous outcomes: reported as failures, never masked.
			const state = asString(creation["state"],) ?? "UNKNOWN";
			throw new Error(
				`App instance creation did not complete (state=${state}): ${
					JSON.stringify(creation,).slice(0, 300,)
				}`,
			);
		}
		const echoed = asString(creation["projectKey"],);
		if (echoed !== undefined && echoed !== reservedKey) {
			throw new Error(`App instance creation named a different target project: ${echoed}.`,);
		}
		created = true;
		await ctx.bindProject(reservedKey,);

		const verification = await ctx.run<JsonRecord>([
			"app",
			"verify-instance",
			appId,
			"--project-key",
			reservedKey,
		],);
		if (verification["valid"] !== true || verification["apiReady"] !== true) {
			throw new Error(
				`App instance verification failed: ${JSON.stringify(verification,).slice(0, 300,)}`,
			);
		}
		const comparison = await ctx.run<JsonRecord>([
			"app",
			"compare-manifest",
			appId,
			"--project-key",
			reservedKey,
		],);
		if (asRecord(comparison,) === undefined) {
			throw new Error("app compare-manifest returned no object.",);
		}
	} finally {
		if (created) await ctx.deleteProject(reservedKey,);
	}
}

// ---------------------------------------------------------------------------
// Infrastructure scenario: read-only SQL constant assertion
// ---------------------------------------------------------------------------

/**
 * Read-only SQL smoke against an explicitly configured SQL connection (or
 * dataset). Never mutates data, never lists or changes connections or code
 * envs: a fresh trial core cannot cover external systems unless the operator
 * explicitly points the suite at one. Missing configuration is an explicit
 * blocker, not an "unsupported" blanket.
 */
async function exerciseInfrastructureSql(ctx: LiveContext,): Promise<void> {
	const connection = process.env[SQL_CONNECTION_ENV]?.trim();
	const datasetFullName = process.env[SQL_DATASET_ENV]?.trim();
	if (!connection && !datasetFullName) {
		capabilityBlocked(
			`No SQL target is explicitly configured: set ${SQL_CONNECTION_ENV} (a SQL-type connection id) or ${SQL_DATASET_ENV} (a full dataset name) to exercise the read-only SELECT assertion. The infrastructure profile never mutates global connections or code environments.`,
		);
	}
	const projectKey = await ctx.createProject("sqlsel",);
	try {
		const target = connection
			? ["--connection", connection,]
			: ["--dataset", datasetFullName!,];
		const result = await ctx.run<JsonRecord>([
			"sql",
			"query",
			"SELECT 1 AS one",
			...target,
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
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

/**
 * Exercise the optional ML, application, and infrastructure capability
 * modules.
 *
 * Phase contract (parent LiveContext): this module creates no persistent
 * fixtures in either phase — setup is a no-op for capability cases and every
 * mutating scenario runs in the run phase on case-owned projects deleted in
 * finally, so repeated selected runs stay idempotent. ctx.selection filtering
 * is applied automatically by ctx.check.
 *
 * Profiles: ML cases require the "ml" profile, application cases the
 * "applications" profile, the SQL assertion the "infrastructure" profile with
 * an explicitly configured SQL target. Missing prerequisites produce explicit
 * blocked/unsupported CaseResults via LiveCapabilityError — never mock passes;
 * genuine command defects stay failures.
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
	}

	if (ctx.profiles.includes("applications",)) {
		await ctx.check(
			"applications.template-prerequisite",
			["app.manifest", "app.instances",],
			async () => {
				await readTemplateSurface(ctx, appTemplateId(),);
			},
			{ capability: "applications.template-read", required: false, },
		);
		await ctx.check(
			"applications.instance-lifecycle",
			[
				"project.create",
				"app.create-instance",
				"app.verify-instance",
				"app.compare-manifest",
				"project.delete",
			],
			async () => {
				const appId = appTemplateId();
				await readTemplateSurface(ctx, appId,);
				await exerciseAppInstanceLifecycle(ctx, appId,);
			},
			{ capability: "applications.instance-lifecycle", },
		);
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
