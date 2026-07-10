import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { AnalysesResource, } from "../src/resources/analyses.js";
import { MlTasksResource, } from "../src/resources/ml-tasks.js";
import { ModelEvaluationStoresResource, } from "../src/resources/model-evaluation-stores.js";
import { SavedModelsResource, } from "../src/resources/saved-models.js";

type RecordedRequest = {
	method: string;
	path: string;
	body?: unknown;
};

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) body += chunk.toString();
	return body;
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "DEFAULT",
	},);
}

async function recordRequests(run: (url: string,) => Promise<void>,): Promise<RecordedRequest[]> {
	const requests: RecordedRequest[] = [];
	const server = createServer((req, res,) => {
		void (async () => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const text = await readBody(req,);
			requests.push({
				method: req.method ?? "GET",
				path: `${url.pathname}${url.search}`,
				...(text.length > 0 ? { body: JSON.parse(text,), } : {}),
			},);
			if (req.method === "DELETE") {
				res.statusCode = 204;
				res.end();
				return;
			}
			sendJson(res, { ok: true, },);
		})().catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);

	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) rejectPromise(error,);
			else resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	try {
		await run(`http://127.0.0.1:${String(port,)}`,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) rejectPromise(error,);
				else resolvePromise();
			},);
		},);
	}
	return requests;
}

describe("Visual ML SDK endpoints", () => {
	it("composes analysis endpoints and create payloads", async () => {
		const requests = await recordRequests(async (url,) => {
			const resource = new AnalysesResource(createClient(url,),);
			await resource.list("PROJECT KEY",);
			await resource.get("analysis/1", "PROJECT KEY",);
			await resource.create({ inputDataset: "input/name", projectKey: "PROJECT KEY", },);
			await resource.delete("analysis/1", "PROJECT KEY",);
		},);

		expect(requests,).toEqual([
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/lab/", },
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/lab/analysis%2F1/", },
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/lab/",
				body: { inputDataset: "input/name", },
			},
			{ method: "DELETE", path: "/public/api/projects/PROJECT%20KEY/lab/analysis%2F1/", },
		],);
	});

	it("validates that prediction tasks declare a non-blank target", async () => {
		const resource = new MlTasksResource(createClient("http://127.0.0.1:1",),);

		await expect(resource.create({
			analysisId: "analysis",
			taskType: "PREDICTION",
			targetVariable: "   ",
		},),).rejects.toThrow("targetVariable is required for PREDICTION ML tasks.",);
	});

	it("composes ML-task lifecycle endpoints and request bodies", async () => {
		const requests = await recordRequests(async (url,) => {
			const resource = new MlTasksResource(createClient(url,),);
			await resource.create({
				analysisId: "analysis/1",
				taskType: "PREDICTION",
				targetVariable: "churn flag",
				predictionType: "BINARY_CLASSIFICATION",
				guessPolicy: "DEFAULT",
				projectKey: "PROJECT KEY",
			},);
			await resource.status("analysis/1", "task 1", "PROJECT KEY",);
			await resource.getSettings("analysis/1", "task 1", "PROJECT KEY",);
			await resource.saveSettings(
				"analysis/1",
				"task 1",
				{ algorithms: { RANDOM_FOREST_CLASSIFICATION: { enabled: true, }, }, },
				"PROJECT KEY",
			);
			await resource.train({
				analysisId: "analysis/1",
				mlTaskId: "task 1",
				sessionName: "First session",
				sessionDescription: "Deterministic test",
				projectKey: "PROJECT KEY",
			},);
			await resource.listTrainedModels("analysis/1", "task 1", "PROJECT KEY",);
			await resource.trainedModelDetails("analysis/1", "task 1", "model/1", "PROJECT KEY",);
			await resource.deployToFlow({
				analysisId: "analysis/1",
				mlTaskId: "task 1",
				modelId: "model/1",
				trainDatasetRef: "train/data",
				modelName: "Churn model",
				projectKey: "PROJECT KEY",
			},);
			await resource.deployToFlow({
				analysisId: "analysis/1",
				mlTaskId: "task 1",
				modelId: "model/2",
				trainDatasetRef: "train/data",
				testDatasetRef: "test/data",
				modelName: "Churn model with holdout",
				redoOptimization: false,
				projectKey: "PROJECT KEY",
			},);
			await resource.delete("analysis/1", "task 1", "PROJECT KEY",);
		},);

		const taskPath = "/public/api/projects/PROJECT%20KEY/models/lab/analysis%2F1/task%201";
		expect(requests,).toEqual([
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/lab/analysis%2F1/models/",
				body: {
					taskType: "PREDICTION",
					targetVariable: "churn flag",
					predictionType: "BINARY_CLASSIFICATION",
					backendType: "PY_MEMORY",
					guessPolicy: "DEFAULT",
				},
			},
			{ method: "GET", path: `${taskPath}/status`, },
			{ method: "GET", path: `${taskPath}/settings`, },
			{
				method: "POST",
				path: `${taskPath}/settings`,
				body: { algorithms: { RANDOM_FOREST_CLASSIFICATION: { enabled: true, }, }, },
			},
			{
				method: "POST",
				path: `${taskPath}/train`,
				body: {
					sessionName: "First session",
					sessionDescription: "Deterministic test",
					runQueue: false,
				},
			},
			{ method: "GET", path: `${taskPath}/status`, },
			{ method: "GET", path: `${taskPath}/models/model%2F1/details`, },
			{
				method: "POST",
				path: `${taskPath}/models/model%2F1/actions/deployToFlow`,
				body: {
					trainDatasetRef: "train/data",
					modelName: "Churn model",
					redoOptimization: true,
				},
			},
			{
				method: "POST",
				path: `${taskPath}/models/model%2F2/actions/deployToFlow`,
				body: {
					trainDatasetRef: "train/data",
					testDatasetRef: "test/data",
					modelName: "Churn model with holdout",
					redoOptimization: false,
				},
			},
			{ method: "DELETE", path: `${taskPath}/`, },
		],);
	});

	it("composes saved-model and version action endpoints", async () => {
		const requests = await recordRequests(async (url,) => {
			const resource = new SavedModelsResource(createClient(url,),);
			await resource.list("PROJECT KEY",);
			await resource.get("saved/1", "PROJECT KEY",);
			await resource.listVersions("saved/1", "PROJECT KEY",);
			await resource.versionDetails("saved/1", "v 1", "PROJECT KEY",);
			await resource.setActiveVersion("saved/1", "v 1", "PROJECT KEY",);
			await resource.delete("saved/1", "PROJECT KEY",);
		},);

		expect(requests,).toEqual([
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/savedmodels/", },
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1", },
			{
				method: "GET",
				path: "/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1/versions",
			},
			{
				method: "GET",
				path: "/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1/versions/v%201/details",
			},
			{
				method: "POST",
				path:
					"/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1/versions/v%201/actions/setActive",
				body: {},
			},
			{ method: "DELETE", path: "/public/api/projects/PROJECT%20KEY/savedmodels/saved%2F1", },
		],);
	});

	it("composes model-evaluation-store endpoints and create payloads", async () => {
		const requests = await recordRequests(async (url,) => {
			const resource = new ModelEvaluationStoresResource(createClient(url,),);
			await resource.list("PROJECT KEY",);
			await resource.get("store/1", "PROJECT KEY",);
			await resource.create({ name: "Evaluation store", projectKey: "PROJECT KEY", },);
			await resource.listEvaluations("store/1", "PROJECT KEY",);
			await resource.delete("store/1", "PROJECT KEY",);
		},);

		expect(requests,).toEqual([
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/evaluationstores/", },
			{ method: "GET", path: "/public/api/projects/PROJECT%20KEY/evaluationstores/store%2F1", },
			{
				method: "POST",
				path: "/public/api/projects/PROJECT%20KEY/evaluationstores/?flavor=TABULAR",
				body: { projectKey: "PROJECT KEY", name: "Evaluation store", },
			},
			{
				method: "GET",
				path: "/public/api/projects/PROJECT%20KEY/evaluationstores/store%2F1/evaluations/",
			},
			{ method: "DELETE", path: "/public/api/projects/PROJECT%20KEY/evaluationstores/store%2F1", },
		],);
	});
});
