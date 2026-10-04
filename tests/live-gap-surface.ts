import { expect, } from "bun:test";
import { readFile, } from "node:fs/promises";
import path from "node:path";
import { DataikuError, } from "../src/errors.js";
import type { DssTask, } from "../src/resources/futures.js";
import type { FutureWaitResult, } from "../src/schemas.js";
import { provisionMlFixtures, } from "./live-capabilities.js";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";
import { withChildProject, } from "./live-project-surface.js";

/**
 * DSS 15 REST gap actions: column/partition dataset reads, dataset checks,
 * flow schema propagation and documentation, the MLflow extension, and Visual
 * ML diagnostics, documentation, re-guessing, and time series forecasting.
 * Mutations run in disposable child projects or on lab-owned ML fixtures;
 * the root project's data is only read.
 */

type JsonRecord = Record<string, unknown>;

const TASK_TIMEOUT_MS = "600000";
const MLFLOW_API = "/public/api/api/2.0/mlflow";

const SOURCE_SCRIPT = `import dataiku, pandas as pd
pd.DataFrame({
    "region": ["north", "south", "east", "west"],
    "units": [3, 5, 8, 13],
    "price": [1.5, 2.5, 3.5, 4.5],
}).pipe(dataiku.Dataset("gap_source").write_with_schema)
`;

const SALES_SCRIPT = `import dataiku, numpy as np, pandas as pd
rng = np.random.default_rng(3)
days = pd.date_range("2025-01-01", periods=120, freq="D")
frames = []
for store, base in (("a", 100.0), ("b", 50.0)):
    t = np.arange(len(days))
    frames.append(pd.DataFrame({
        "day": days,
        "store": store,
        "sales": base + 10 * np.sin(t / 7 * 2 * np.pi) + rng.normal(0, 2, len(days)),
    }))
dataiku.Dataset("gap_sales").write_with_schema(pd.concat(frames, ignore_index=True))
`;

function requireValue<T,>(value: T | undefined | null, label: string,): T {
	if (value === undefined || value === null || value === "") {
		throw new Error(`${label} is missing`,);
	}
	return value;
}

function firstFixtureDataset(ctx: LiveContext,): string {
	const dataset = Object.keys(ctx.fixtures.datasets,)[0];
	if (!dataset) throw new LiveCapabilityError("the lab has no baseline dataset",);
	return dataset;
}

function schemaNames(dataset: JsonRecord,): string[] {
	const schema = dataset["schema"] as { columns?: Array<{ name?: string; }>; } | undefined;
	return (schema?.columns ?? []).map(column => column.name ?? "");
}

/** Create a python recipe writing `output` from `script`, then build it. */
async function buildPythonDataset(
	ctx: LiveContext,
	projectKey: string,
	recipe: string,
	output: string,
	script: string,
): Promise<void> {
	await ctx.run([
		"recipe",
		"create",
		"--type",
		"python",
		"--name",
		recipe,
		"--output",
		output,
		"--output-connection",
		ctx.connection,
	], { projectKey, },);
	const file = await ctx.writeFile(`${recipe}_${ctx.iteration}.py`, script,);
	await ctx.run(["recipe", "set-payload", recipe, "--file", file, "--no-backup",], { projectKey, },);
	const run = await ctx.run<{ state?: string; }>(["recipe", "run", recipe, "--wait",], {
		projectKey,
	},);
	expect(run.state,).toBe("DONE",);
}

/** A finished DSS task: --wait results are terminal and successful. */
function expectDone(result: FutureWaitResult | DssTask,): void {
	expect(result.hasResult,).toBe(true,);
	if ("success" in result) expect(result.success,).toBe(true,);
}

async function exerciseDatasetSelection(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.dataset.selection", ["dataset.preview", "dataset.download",], async () => {
		const dataset = firstFixtureDataset(ctx,);
		const full = await ctx.run<{ columns: Array<{ name: string; }>; }>([
			"dataset",
			"preview",
			dataset,
			"--max-rows",
			"5",
		],);
		const picked = full.columns.slice(0, 2,).map(column => column.name);
		expect(picked.length,).toBeGreaterThan(0,);
		const selected = await ctx.run<{ columns: Array<{ name: string; }>; rows: string[][]; }>([
			"dataset",
			"preview",
			dataset,
			"--max-rows",
			"5",
			"--columns",
			picked.join(",",),
		],);
		expect(selected.columns.map(column => column.name),).toEqual(picked,);
		expect(selected.rows.every(row => row.length === picked.length),).toBe(true,);
		const output = path.join(ctx.dir, `gap_selection_${ctx.iteration}.csv`,);
		await ctx.run([
			"dataset",
			"download",
			dataset,
			"--columns",
			picked.join(",",),
			"--output",
			output,
		],);
		const header = (await readFile(output, "utf8",)).split("\n",)[0]!;
		expect(header.split(",",).map(name => name.replace(/^"|"$/g, "",)),).toEqual(picked,);
	},);
}

async function exerciseDatasetChecks(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.metrics.run-checks", ["metrics.dataset-run-checks",], async () => {
		const result = await ctx.run<unknown>([
			"metrics",
			"dataset-run-checks",
			firstFixtureDataset(ctx,),
		],);
		expect(typeof result,).toBe("object",);
	},);
}

async function exerciseDataQualityHistoryDeletion(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.data-quality.delete-history", [
		"recipe.create",
		"recipe.set-payload",
		"recipe.run",
		"data-quality.create-rule",
		"data-quality.compute",
		"data-quality.history",
		"data-quality.delete-history",
	], async () => {
		await withChildProject(ctx, "dqhistory", async key => {
			await buildPythonDataset(ctx, key, "gap_make_source", "gap_source", SOURCE_SCRIPT,);
			await ctx.run([
				"data-quality",
				"create-rule",
				"gap_source",
				"--data",
				JSON.stringify({
					type: "RecordCountInRangeRule",
					displayName: "gap rows",
					softMinimum: 1,
					softMinimumEnabled: true,
				},),
			], { projectKey: key, },);
			await ctx.run(
				["data-quality", "compute", "gap_source", "--wait", "--timeout", TASK_TIMEOUT_MS,],
				{
					projectKey: key,
				},
			);
			const history = (): Promise<unknown[]> =>
				ctx.run<unknown[]>(["data-quality", "history", "gap_source",], { projectKey: key, },);
			expect((await history()).length,).toBeGreaterThan(0,);
			// The dataset is unpartitioned: its history lives under partition NP.
			await ctx.run(["data-quality", "delete-history", "gap_source", "--partition", "NP",], {
				projectKey: key,
			},);
			expect(await history(),).toEqual([],);
		},);
	},);
}

async function exerciseSchemaPropagation(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.flow.propagate-schema", [
		"recipe.create",
		"recipe.set-payload",
		"recipe.run",
		"dataset.refresh-schema",
		"flow.propagate-schema",
		"dataset.get",
	], async () => {
		await withChildProject(ctx, "flowprop", async key => {
			await buildPythonDataset(ctx, key, "gap_make_source", "gap_source", SOURCE_SCRIPT,);
			await ctx.run([
				"recipe",
				"create",
				"--type",
				"sync",
				"--input",
				"gap_source",
				"--output",
				"gap_copy",
				"--output-connection",
				ctx.connection,
			], { projectKey: key, },);
			const source = schemaNames(
				await ctx.run<JsonRecord>(["dataset", "get", "gap_source",], {
					projectKey: key,
				},),
			);
			expect(source,).toEqual(["region", "units", "price",],);
			// Break the downstream schema, then let propagation restore it from the source.
			await ctx.run([
				"dataset",
				"refresh-schema",
				"gap_copy",
				"--data",
				JSON.stringify({ columns: [{ name: "stale", type: "string", },], },),
			], { projectKey: key, },);
			const propagated = await ctx.run<FutureWaitResult | DssTask>([
				"flow",
				"propagate-schema",
				"gap_source",
				"--wait",
				"--timeout",
				TASK_TIMEOUT_MS,
			], { projectKey: key, },);
			expectDone(propagated,);
			const copy = await ctx.run<JsonRecord>(["dataset", "get", "gap_copy",], { projectKey: key, },);
			expect(schemaNames(copy,),).toEqual(source,);
		},);
	},);
}

async function exerciseFlowDocumentation(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.flow.documentation", [
		"flow.generate-documentation",
		"flow.download-documentation",
	], async () => {
		const generated = path.join(ctx.dir, `gap_flow_${ctx.iteration}.docx`,);
		const result = await ctx.run<{ path: string; bytes: number; exportId: string; }>([
			"flow",
			"generate-documentation",
			"--output",
			generated,
			"--timeout",
			TASK_TIMEOUT_MS,
		],);
		expect(result.path,).toBe(generated,);
		expect(result.bytes,).toBeGreaterThan(0,);
		const again = path.join(ctx.dir, `gap_flow_${ctx.iteration}_again.docx`,);
		const downloaded = await ctx.run<{ bytes: number; }>([
			"flow",
			"download-documentation",
			requireValue(result.exportId, "flow documentation exportId",),
			"--output",
			again,
		],);
		expect(downloaded.bytes,).toBe(result.bytes,);
		// A docx is a zip archive.
		expect((await readFile(again,)).subarray(0, 2,).toString(),).toBe("PK",);
	}, { required: false, },);
}

async function exerciseMlflowExtension(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.mlflow.extension", [
		"folder.create",
		"folder.upload",
		"mlflow.list-models",
		"mlflow.set-inference-info",
		"mlflow.create-experiments-dataset",
		"dataset.list",
		"mlflow.garbage-collect",
		"mlflow.clean-db",
	], async () => {
		await withChildProject(ctx, "mlflowext", async key => {
			const folder = await ctx.run<{ id?: string; }>([
				"folder",
				"create",
				"--name",
				"mlflow_artifacts",
			], {
				projectKey: key,
			},);
			const folderId = requireValue(folder.id, "artifact folder id",);
			const headers = {
				"x-dku-mlflow-project-key": key,
				"x-dku-mlflow-managed-folder-id": `${key}.${folderId}`,
			};
			// The tracking API itself (experiments, runs) is plain MLflow; only the
			// extension endpoints are wrapped by the CLI under test.
			const experiment = await ctx.client.post<{ experimentId?: string; experiment_id?: string; }>(
				`${MLFLOW_API}/experiments/create`,
				{ name: `gap_experiment_${ctx.iteration}`, },
				{ headers, },
			);
			const experimentId = requireValue(
				experiment.experimentId ?? experiment.experiment_id,
				"experiment id",
			);
			const created = await ctx.client.post<{ run?: { info?: { runId?: string; }; }; }>(
				`${MLFLOW_API}/runs/create`,
				{
					experiment_id: experimentId,
					start_time: Date.now(),
					run_name: "gap_run",
					tags: [],
					user_id: ctx.owner,
				},
				{ headers, },
			);
			const runId = requireValue(created.run?.info?.runId, "run id",);
			const mlmodel = await ctx.writeFile(
				`gap_mlmodel_${ctx.iteration}.yaml`,
				`artifact_path: model\nflavors:\n  python_function:\n    loader_module: mlflow.sklearn\nrun_id: ${runId}\n`,
			);
			await ctx.run([
				"folder",
				"upload",
				folderId,
				`${experimentId}/${runId}/artifacts/model/MLmodel`,
				mlmodel,
			], { projectKey: key, },);
			await ctx.client.post(
				`${MLFLOW_API}/runs/log-model`,
				{
					run_id: runId,
					model_json: JSON.stringify({
						artifact_path: "model",
						run_id: runId,
						flavors: { python_function: {}, },
					},),
				},
				{ headers, },
			);
			const models = await ctx.run<Array<{ artifactPath?: string; }>>([
				"mlflow",
				"list-models",
				runId,
			], {
				projectKey: key,
			},);
			expect(models.map(model => model.artifactPath),).toContain("model",);

			await ctx.run([
				"mlflow",
				"set-inference-info",
				runId,
				"--prediction-type",
				"BINARY_CLASSIFICATION",
				"--classes",
				"0,1",
				"--target",
				"churn_flag",
			], { projectKey: key, },);
			const run = await ctx.client.get<
				{ run?: { data?: { tags?: Array<{ key: string; value: string; }>; }; }; }
			>(
				`${MLFLOW_API}/runs/get?run_id=${encodeURIComponent(runId,)}`,
				{ headers, },
			);
			const tags = Object.fromEntries((run.run?.data?.tags ?? []).map(tag => [tag.key, tag.value,]),);
			expect(tags["dku-ext.predictionType"],).toBe("BINARY_CLASSIFICATION",);
			expect(tags["dku-ext.target"],).toBe("churn_flag",);
			expect(JSON.parse(tags["dku-ext.targetClasses"] ?? "[]",),).toEqual(["0", "1",],);

			await ctx.run([
				"mlflow",
				"create-experiments-dataset",
				"gap_mlflow_runs",
				"--experiments",
				experimentId,
			], { projectKey: key, },);
			const datasets = await ctx.run<Array<{ name?: string; type?: string; }>>(["dataset", "list",], {
				projectKey: key,
			},);
			expect(datasets.find(dataset => dataset.name === "gap_mlflow_runs")?.type,).toBe(
				"ExperimentsDB",
			);

			// Garbage collection permanently removes runs marked deleted.
			await ctx.client.post(`${MLFLOW_API}/runs/delete`, { run_id: runId, }, { headers, },);
			await ctx.run(["mlflow", "garbage-collect",], { projectKey: key, },);
			let collected = false;
			try {
				await ctx.client.get(`${MLFLOW_API}/runs/get?run_id=${encodeURIComponent(runId,)}`, {
					headers,
				},);
			} catch (error) {
				collected = error instanceof DataikuError;
			}
			expect(collected,).toBe(true,);

			// clean-db drops every experiment of the project.
			await ctx.run(["mlflow", "clean-db",], { projectKey: key, },);
			const remaining = await ctx.client.post<{ experiments?: unknown[]; }>(
				`${MLFLOW_API}/experiments/search`,
				{ max_results: 10, view_type: "ALL", },
				{ headers, },
			);
			expect(remaining.experiments ?? [],).toEqual([],);
		},);
	},);
}

function mlIds(ctx: LiveContext,): {
	datasetName: string;
	analysisId: string;
	mlTaskId: string;
	modelId: string;
	savedModelId: string;
	versionId: string;
} {
	const ml = ctx.fixtures.ml ?? {};
	return {
		datasetName: requireValue(ml.datasetName, "fixtures.ml.datasetName",),
		analysisId: requireValue(ml.analysisId, "fixtures.ml.analysisId",),
		mlTaskId: requireValue(ml.mlTaskId, "fixtures.ml.mlTaskId",),
		modelId: requireValue(ml.trainedModelId, "fixtures.ml.trainedModelId",),
		savedModelId: requireValue(ml.savedModelId, "fixtures.ml.savedModelId",),
		versionId: requireValue(ml.savedModelVersionId, "fixtures.ml.savedModelVersionId",),
	};
}

async function exerciseModelDiagnostics(ctx: LiveContext,): Promise<void> {
	await ctx.check("ml.model.diagnostics", [
		"ml-task.list",
		"analysis.list-ml-tasks",
		"ml-task.compute-diagnostics",
		"ml-task.diagnostics",
		"ml-task.model-details",
		"ml-task.set-user-meta",
		"saved-model.compute-diagnostics",
		"saved-model.diagnostics",
		"ml-task.get-settings",
	], async () => {
		await provisionMlFixtures(ctx,);
		const ids = mlIds(ctx,);
		const lab = [ids.analysisId, ids.mlTaskId, ids.modelId,];
		const version = [ids.savedModelId, ids.versionId,];
		const tasks = await ctx.run<Array<{ mlTaskId?: string; }>>(["ml-task", "list",],);
		expect(tasks.some(task => task.mlTaskId === ids.mlTaskId),).toBe(true,);
		const ofAnalysis = await ctx.run<Array<{ mlTaskId?: string; }>>([
			"analysis",
			"list-ml-tasks",
			ids.analysisId,
		],);
		expect(ofAnalysis.map(task => task.mlTaskId),).toContain(ids.mlTaskId,);
		// Diagnostics only run on features the task uses; the guess policy decides which.
		const taskSettings = await ctx.run<
			{ preprocessing?: { per_feature?: Record<string, { role?: string; }>; }; }
		>([
			"ml-task",
			"get-settings",
			ids.analysisId,
			ids.mlTaskId,
		],);
		const inputs = Object.entries(taskSettings.preprocessing?.per_feature ?? {},)
			.filter(([, feature,],) => feature.role === "INPUT")
			.map(([name,],) => name);
		const [dependencyFeature, subpopulationFeature = dependencyFeature,] = inputs;
		if (dependencyFeature === undefined) {
			throw new Error("the retained ML task uses no input feature",);
		}

		const dependencies = await ctx.run<FutureWaitResult>([
			"ml-task",
			"compute-diagnostics",
			...lab,
			"--kind",
			"partial-dependencies",
			"--features",
			dependencyFeature,
			"--wait",
			"--timeout",
			TASK_TIMEOUT_MS,
		],);
		expectDone(dependencies,);
		const labDependencies = await ctx.run<{ partialDependencies?: Array<{ feature?: string; }>; }>([
			"ml-task",
			"diagnostics",
			...lab,
			"--kind",
			"partial-dependencies",
		],);
		expect(labDependencies.partialDependencies?.map(entry => entry.feature),).toContain(
			dependencyFeature,
		);

		const subpopulations = await ctx.run<FutureWaitResult>([
			"saved-model",
			"compute-diagnostics",
			...version,
			"--kind",
			"subpopulation-analyses",
			"--features",
			subpopulationFeature,
			"--wait",
			"--timeout",
			TASK_TIMEOUT_MS,
		],);
		expectDone(subpopulations,);
		const versionSubpopulations = await ctx.run<
			{ subpopulationAnalyses?: Array<{ feature?: string; }>; }
		>([
			"saved-model",
			"diagnostics",
			...version,
			"--kind",
			"subpopulation-analyses",
		],);
		expect(versionSubpopulations.subpopulationAnalyses?.map(entry => entry.feature),).toContain(
			subpopulationFeature,
		);

		// User metadata: only the userMeta field, with a marker in its custom key/values.
		const details = await ctx.run<{ userMeta?: JsonRecord; }>(["ml-task", "model-details", ...lab,],);
		const userMeta = details.userMeta ?? {};
		const customMeta = (userMeta["customMeta"] as { kv?: JsonRecord; } | undefined) ?? { kv: {}, };
		const marker = `lab_${ctx.iteration}`;
		await ctx.run([
			"ml-task",
			"set-user-meta",
			...lab,
			"--data",
			JSON.stringify({
				...userMeta,
				customMeta: { ...customMeta, kv: { ...customMeta.kv, liveSuiteMarker: marker, }, },
			},),
		],);
		const reread = await ctx.run<{ userMeta?: { customMeta?: { kv?: JsonRecord; }; }; }>([
			"ml-task",
			"model-details",
			...lab,
		],);
		expect(reread.userMeta?.customMeta?.kv?.["liveSuiteMarker"],).toBe(marker,);
	},);
}

async function exerciseModelDocumentation(ctx: LiveContext,): Promise<void> {
	await ctx.check("ml.model.documentation", [
		"ml-task.generate-documentation",
		"ml-task.download-documentation",
		"saved-model.generate-documentation",
		"saved-model.download-documentation",
	], async () => {
		await provisionMlFixtures(ctx,);
		const ids = mlIds(ctx,);
		for (
			const [resource, target,] of [
				["ml-task", [ids.analysisId, ids.mlTaskId, ids.modelId,],],
				["saved-model", [ids.savedModelId, ids.versionId,],],
			] as const
		) {
			const generated = path.join(ctx.dir, `gap_${resource}_doc_${ctx.iteration}.docx`,);
			const result = await ctx.run<{ path: string; bytes: number; exportId: string; }>([
				resource,
				"generate-documentation",
				...target,
				"--output",
				generated,
				"--timeout",
				TASK_TIMEOUT_MS,
			],);
			expect(result.bytes,).toBeGreaterThan(0,);
			const again = path.join(ctx.dir, `gap_${resource}_doc_${ctx.iteration}_again.docx`,);
			const downloaded = await ctx.run<{ bytes: number; }>([
				resource,
				"download-documentation",
				requireValue(result.exportId, `${resource} documentation exportId`,),
				"--output",
				again,
			],);
			expect(downloaded.bytes,).toBe(result.bytes,);
		}
	}, { required: false, },);
}

async function exerciseLabScoringExports(ctx: LiveContext,): Promise<void> {
	await ctx.check("ml.model.scoring-exports", [
		"ml-task.download-scoring-jar",
		"ml-task.download-scoring-pmml",
	], async () => {
		await provisionMlFixtures(ctx,);
		const ids = mlIds(ctx,);
		const lab = [ids.analysisId, ids.mlTaskId, ids.modelId,];
		for (
			const [action, extension,] of [["download-scoring-jar", "jar",], [
				"download-scoring-pmml",
				"pmml",
			],]
		) {
			const output = path.join(ctx.dir, `gap_lab_scoring_${ctx.iteration}.${extension}`,);
			const result = await ctx.run<{ bytes: number; }>([
				"ml-task",
				action,
				...lab,
				"--output",
				output,
			],);
			expect(result.bytes,).toBeGreaterThan(0,);
		}
	}, { required: false, },);
}

async function exerciseTaskReguess(ctx: LiveContext,): Promise<void> {
	await ctx.check("ml.task.reguess", [
		"ml-task.create-for-dataset",
		"ml-task.status",
		"ml-task.reguess",
		"ml-task.get-settings",
		"analysis.get",
		"analysis.update",
		"analysis.delete",
	], async () => {
		await provisionMlFixtures(ctx,);
		const { datasetName, } = mlIds(ctx,);
		const created = await ctx.run<{ analysisId?: string; mlTaskId?: string; }>([
			"ml-task",
			"create-for-dataset",
			datasetName,
			"--task-type",
			"PREDICTION",
			"--target",
			"churn_flag",
			"--prediction-type",
			"BINARY_CLASSIFICATION",
		],);
		const analysisId = requireValue(created.analysisId, "new analysis id",);
		const mlTaskId = requireValue(created.mlTaskId, "new ML task id",);
		try {
			await waitForGuess(ctx, analysisId, mlTaskId,);
			await ctx.run(["ml-task", "reguess", analysisId, mlTaskId, "--target", "support_tickets",],);
			await waitForGuess(ctx, analysisId, mlTaskId,);
			const settings = await ctx.run<{ targetVariable?: string; }>([
				"ml-task",
				"get-settings",
				analysisId,
				mlTaskId,
			],);
			expect(settings.targetVariable,).toBe("support_tickets",);

			const analysis = await ctx.run<JsonRecord>(["analysis", "get", analysisId,],);
			const renamed = `gap analysis ${ctx.iteration}`;
			await ctx.run([
				"analysis",
				"update",
				analysisId,
				"--data",
				JSON.stringify({ ...analysis, name: renamed, },),
			],);
			const reread = await ctx.run<JsonRecord>(["analysis", "get", analysisId,],);
			expect(reread["name"],).toBe(renamed,);
		} finally {
			await ctx.run(["analysis", "delete", analysisId,],);
		}
	},);
}

async function waitForGuess(
	ctx: LiveContext,
	analysisId: string,
	mlTaskId: string,
	projectKey?: string,
): Promise<void> {
	for (let attempt = 0; attempt < 60; attempt++) {
		const status = await ctx.run<{ guessing?: boolean; }>(
			["ml-task", "status", analysisId, mlTaskId,],
			projectKey === undefined ? {} : { projectKey, },
		);
		if (status.guessing !== true) return;
		await Bun.sleep(2_000,);
	}
	throw new Error(`ML task ${mlTaskId} is still guessing after 2 minutes`,);
}

async function exerciseTimeseriesForecasting(ctx: LiveContext,): Promise<void> {
	await ctx.check("ml.timeseries.diagnostics", [
		"recipe.create",
		"recipe.set-payload",
		"recipe.run",
		"ml-task.create-for-dataset",
		"ml-task.status",
		"ml-task.reguess-forecasting",
		"ml-task.get-settings",
		"ml-task.train",
		"ml-task.compute-diagnostics",
		"ml-task.diagnostics",
	], async () => {
		await withChildProject(ctx, "forecast", async key => {
			await buildPythonDataset(ctx, key, "gap_make_sales", "gap_sales", SALES_SCRIPT,);
			const created = await ctx.run<{ analysisId?: string; mlTaskId?: string; }>([
				"ml-task",
				"create-for-dataset",
				"gap_sales",
				"--task-type",
				"PREDICTION",
				"--target",
				"sales",
				"--prediction-type",
				"TIMESERIES_FORECAST",
				"--time-variable",
				"day",
				"--timeseries-ids",
				"store",
			], { projectKey: key, },);
			const analysisId = requireValue(created.analysisId, "forecast analysis id",);
			const mlTaskId = requireValue(created.mlTaskId, "forecast ML task id",);
			await waitForGuess(ctx, analysisId, mlTaskId, key,);
			await ctx.run([
				"ml-task",
				"reguess-forecasting",
				analysisId,
				mlTaskId,
				"--data",
				JSON.stringify({ forecastHorizon: 5, updateAlgorithmSettings: true, },),
			], { projectKey: key, },);
			const settings = await ctx.run<{ predictionLength?: number; timeseriesIdentifiers?: string[]; }>(
				[
					"ml-task",
					"get-settings",
					analysisId,
					mlTaskId,
				],
				{ projectKey: key, },
			);
			expect(settings.predictionLength,).toBe(5,);
			expect(settings.timeseriesIdentifiers,).toEqual(["store",],);
			const trained = await ctx.run<{ trainedModelIds?: string[]; }>([
				"ml-task",
				"train",
				analysisId,
				mlTaskId,
				"--wait",
				"--timeout",
				TASK_TIMEOUT_MS,
			], { projectKey: key, },);
			const models = trained.trainedModelIds ?? [];
			expect(models.length,).toBeGreaterThan(0,);
			// Some forecasting algorithms can fail to train; one model with every
			// per-series diagnostic is the contract.
			const failures: string[] = [];
			for (const modelId of models) {
				const model = [analysisId, mlTaskId, modelId,];
				try {
					expectDone(
						await ctx.run<FutureWaitResult>([
							"ml-task",
							"compute-diagnostics",
							...model,
							"--kind",
							"timeseries-residuals",
							"--wait",
							"--timeout",
							TASK_TIMEOUT_MS,
						], { projectKey: key, },),
					);
					const residuals = await ctx.run<JsonRecord>([
						"ml-task",
						"diagnostics",
						...model,
						"--kind",
						"timeseries-residuals",
					], { projectKey: key, },);
					expect(Object.keys(residuals,).length,).toBe(2,);
					for (const kind of ["per-timeseries-metrics", "per-timeseries-evaluation-forecasts",]) {
						const perSeries = await ctx.run<{ perTimeseries?: JsonRecord; }>([
							"ml-task",
							"diagnostics",
							...model,
							"--kind",
							kind,
						], { projectKey: key, },);
						expect(Object.keys(perSeries.perTimeseries ?? {},).length,).toBe(2,);
					}
					return;
				} catch (error) {
					failures.push(`${modelId}: ${error instanceof Error ? error.message : String(error,)}`,);
				}
			}
			throw new Error(`No forecasting model served its diagnostics: ${failures.join("; ",)}`,);
		},);
	},);
}

export async function exerciseGapSurface(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	await exerciseDatasetSelection(ctx,);
	await exerciseDatasetChecks(ctx,);
	await exerciseDataQualityHistoryDeletion(ctx,);
	await exerciseSchemaPropagation(ctx,);
	await exerciseFlowDocumentation(ctx,);
	await exerciseMlflowExtension(ctx,);
	if (!ctx.profiles.includes("ml",)) return;
	await exerciseModelDiagnostics(ctx,);
	await exerciseModelDocumentation(ctx,);
	await exerciseLabScoringExports(ctx,);
	await exerciseTaskReguess(ctx,);
	await exerciseTimeseriesForecasting(ctx,);
}
