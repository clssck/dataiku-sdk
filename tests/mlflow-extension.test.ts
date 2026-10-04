import { describe, expect, it, } from "bun:test";
import { createServer, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";

interface Seen {
	request: string;
	projectHeader: string | undefined;
	body: unknown;
}

async function withServer(run: (client: DataikuClient,) => Promise<void>,): Promise<Seen[]> {
	const seen: Seen[] = [];
	const server = createServer(async (req, res,) => {
		let text = "";
		for await (const chunk of req) text += chunk.toString();
		const header = req.headers["x-dku-mlflow-project-key"];
		seen.push({
			request: `${req.method} ${req.url}`,
			projectHeader: typeof header === "string" ? header : undefined,
			body: text ? JSON.parse(text,) : undefined,
		},);
		res.setHeader("Content-Type", "application/json",);
		res.end(req.url?.includes("/models/",) ? '[{"runId":"r1","artifactPath":"model"}]' : "{}",);
	},);
	const listening = Promise.withResolvers<void>();
	server.listen(0, "127.0.0.1", () => listening.resolve(),);
	await listening.promise;
	const { port, } = server.address() as AddressInfo;
	try {
		await run(
			new DataikuClient({
				url: `http://127.0.0.1:${port}`,
				apiKey: "k",
				projectKey: "P",
				retryMaxAttempts: 1,
			},),
		);
	} finally {
		const closed = Promise.withResolvers<void>();
		server.close(() => closed.resolve());
		await closed.promise;
	}
	return seen;
}

describe("MlflowExtensionResource", () => {
	it("sends the tracking project header, bodies, and deploy query dataikuapi sends", async () => {
		const seen = await withServer(async (client,) => {
			const mlflow = client.mlflowExtension;
			expect(await mlflow.listModels("r1",),).toEqual([{ runId: "r1", artifactPath: "model", },],);
			await mlflow.setRunInferenceInfo({
				runId: "r1",
				predictionType: "MULTICLASS",
				classes: ["a", "b",],
				target: "y",
			},);
			const deployed = await mlflow.deployRun({
				runId: "r1",
				savedModelId: "sm1",
				versionId: "v2",
				modelName: "model",
				evaluationDataset: "test",
				classLabels: ["a", "b",],
				codeEnvName: "py",
				activate: false,
			},);
			expect(deployed,).toEqual({ savedModelId: "sm1", versionId: "v2", },);
			await mlflow.createExperimentsDataset({ datasetName: "runs", experimentIds: ["e1",], },);
			await mlflow.garbageCollect("OTHER",);
			await mlflow.cleanDb();
		},);
		const base = "/public/api/api/2.0/mlflow/extension";
		const deployQuery = new URLSearchParams({
			projectKey: "P",
			runId: "r1",
			smId: "sm1",
			versionId: "v2",
			modelVersionInfo: JSON.stringify({
				gatherFeaturesFromDataset: "test",
				classLabels: [{ label: "a", }, { label: "b", },],
				pythonCodeEnvName: "py",
			},),
			activate: "false",
			binaryClassificationThreshold: "0.5",
			useOptimalThreshold: "true",
			skipExpensiveReports: "false",
			useInferenceInfo: "true",
			modelName: "model",
		},);
		expect(seen,).toEqual([
			{ request: `GET ${base}/models/r1`, projectHeader: "P", body: undefined, },
			{
				request: `POST ${base}/set-run-inference-info`,
				projectHeader: "P",
				body: { run_id: "r1", prediction_type: "MULTICLASS", classes: '["a","b"]', target: "y", },
			},
			{ request: `POST ${base}/deploy-run?${deployQuery}`, projectHeader: "P", body: undefined, },
			{
				request: `POST ${base}/create-project-experiments-dataset`,
				projectHeader: "P",
				body: {
					datasetName: "runs",
					experimentIds: ["e1",],
					viewType: "ACTIVE_ONLY",
					filter: "",
					orderBy: [],
					format: "LONG",
				},
			},
			{ request: `POST ${base}/garbage-collect`, projectHeader: "OTHER", body: undefined, },
			{ request: `DELETE ${base}/clean-db/P`, projectHeader: undefined, body: undefined, },
		],);
	});

	it("rejects inference info whose classes do not match the prediction type before any request", async () => {
		const seen = await withServer(async (client,) => {
			await expect(
				client.mlflowExtension.setRunInferenceInfo({ runId: "r1", predictionType: "MULTICLASS", },),
			)
				.rejects.toBeInstanceOf(ClientValidationError,);
			await expect(
				client.mlflowExtension.setRunInferenceInfo({
					runId: "r1",
					predictionType: "REGRESSION",
					classes: ["a",],
				},),
			).rejects.toBeInstanceOf(ClientValidationError,);
		},);
		expect(seen,).toEqual([],);
	});
});
