import { describe, expect, it, } from "bun:test";
import { writeFileSync, } from "node:fs";
import { createServer, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";
import type { ComputedModelDiagnostic, ModelDiagnostic, } from "../src/resources/trained-model.js";

interface Seen {
	request: string;
	body: unknown;
}

async function withServer(run: (client: DataikuClient,) => Promise<void>,): Promise<Seen[]> {
	const seen: Seen[] = [];
	const server = createServer(async (req, res,) => {
		let text = "";
		for await (const chunk of req) text += chunk.toString();
		const json = req.headers["content-type"]?.includes("application/json",) === true;
		seen.push({
			request: `${req.method} ${req.url}`,
			body: text ? (json ? JSON.parse(text,) : "multipart") : undefined,
		},);
		res.setHeader("Content-Type", "application/json",);
		if (req.url?.endsWith("/models/lab/",) || req.url?.endsWith("/models/",)) {
			res.end(
				req.method === "GET"
					? '{"mlTasks":[{"analysisId":"a1","mlTaskId":"t1"}]}'
					: '{"analysisId":"a2","mlTaskId":"t2"}',
			);
			return;
		}
		res.end('{"jobId":"j1","hasResult":false}',);
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

const LAB = "/public/api/projects/P/models/lab";
const MODEL = `${LAB}/a1/t1/models/A-P-a1-t1-s1-pp1-m1`;
const VERSION = "/public/api/projects/P/savedmodels/sm1/versions/v1";

/** Requests the first test issues against one trained-model base, in order. */
function expectedModelRoutes(base: string,): Seen[] {
	return [
		{ request: `PUT ${base}/user-meta`, body: { name: "n", }, },
		{ request: `GET ${base}/scoring-jar?fullClassName=a.B&includeLibs=false`, body: undefined, },
		{ request: `GET ${base}/scoring-pmml`, body: undefined, },
		{
			request: `POST ${base}/subpopulation-analyses`,
			body: { features: ["color",], computationParams: { sample_size: 200, random_state: 1, }, },
		},
		{
			request: `POST ${base}/partial-dependencies`,
			body: { features: ["age",], computationParams: {}, },
		},
		{ request: `POST ${base}/timeseries-residuals`, body: undefined, },
		{ request: `GET ${base}/subpopulation-analyses`, body: undefined, },
		{ request: `GET ${base}/partial-dependencies`, body: undefined, },
		{ request: `GET ${base}/timeseries-residuals`, body: undefined, },
		{ request: `GET ${base}/per-timeseries-metrics`, body: undefined, },
		{ request: `GET ${base}/per-timeseries-evaluation-forecasts`, body: undefined, },
		{ request: `POST ${base}/generate-documentation-from-default-template`, body: undefined, },
		{
			request:
				`POST ${base}/generate-documentation-from-template-in-folder?folderId=f1&path=%2Ft.docx`,
			body: undefined,
		},
		{ request: `POST ${base}/generate-documentation-from-custom-template`, body: "multipart", },
	];
}

describe("TrainedModel (lab models and saved-model versions)", () => {
	it("serves diagnostics, documentation, scoring exports, and user meta on both bases", async () => {
		const template = join(tmpdir(), `dss-template-${Date.now()}.docx`,);
		writeFileSync(template, "docx",);
		const seen = await withServer(async (client,) => {
			for (
				const model of [
					client.mlTasks.model("a1", "t1", "A-P-a1-t1-s1-pp1-m1",),
					client.savedModels.version("sm1", "v1",),
				]
			) {
				await model.setUserMeta({ name: "n", },);
				await (await model.downloadScoringJar({ fullClassName: "a.B", includeLibs: false, },)).text();
				await (await model.downloadScoringPmml()).text();
				expect(
					await model.compute("subpopulation-analyses", ["color",], {
						sampleSize: 200,
						randomState: 1,
					},),
				)
					.toEqual({ jobId: "j1", hasResult: false, },);
				await model.compute("partial-dependencies", ["age",],);
				await model.compute("timeseries-residuals",);
				for (
					const kind of [
						"subpopulation-analyses",
						"partial-dependencies",
						"timeseries-residuals",
						"per-timeseries-metrics",
						"per-timeseries-evaluation-forecasts",
					] as ModelDiagnostic[]
				) {
					await model.get(kind,);
				}
				await model.generateDocumentation();
				await model.generateDocumentation({ kind: "folder", folderId: "f1", path: "/t.docx", },);
				await model.generateDocumentation({ kind: "file", filePath: template, },);
			}
			await (await client.mlTasks.downloadModelDocumentation("e1",)).text();
			await (await client.savedModels.downloadDocumentation("e2",)).text();
		},);
		expect(seen,).toEqual([
			...expectedModelRoutes(MODEL,),
			...expectedModelRoutes(VERSION,),
			{ request: `GET ${LAB}/documentations/e1`, body: undefined, },
			{ request: "GET /public/api/projects/P/savedmodels/documentations/e2", body: undefined, },
		],);
	});

	it("rejects unknown diagnostics and feature-less analyses before any request", async () => {
		const seen = await withServer(async (client,) => {
			const model = client.mlTasks.model("a1", "t1", "m1",);
			await expect(model.compute("partial-dependencies",),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
			await expect(model.compute("feature-importance" as ComputedModelDiagnostic,),).rejects
				.toBeInstanceOf(
					ClientValidationError,
				);
			await expect(model.get("../details" as ModelDiagnostic,),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
		},);
		expect(seen,).toEqual([],);
	});
});

describe("Visual ML lab tasks and analyses", () => {
	it("lists, creates, and re-guesses tasks on the documented routes", async () => {
		const seen = await withServer(async (client,) => {
			expect(await client.mlTasks.list(),).toEqual([{ analysisId: "a1", mlTaskId: "t1", },],);
			expect(
				await client.mlTasks.createForDataset({
					inputDataset: "sales",
					taskType: "PREDICTION",
					targetVariable: "amount",
					predictionType: "TIMESERIES_FORECAST",
					timeVariable: "day",
					timeseriesIdentifiers: ["store",],
				},),
			).toEqual({ analysisId: "a2", mlTaskId: "t2", },);
			await client.mlTasks.reguess("a1", "t1", {
				targetVariable: "y",
				timeseriesIdentifiers: ["s1", "s2",],
				fullReguess: false,
			},);
			await client.mlTasks.reguess("a1", "t1",);
			await client.mlTasks.reguessForecasting("a1", "t1", { forecastHorizon: 5, },);
			await client.analyses.update("a1", { id: "a1", name: "renamed", },);
			expect(await client.analyses.listMlTasks("a1",),).toEqual([{
				analysisId: "a1",
				mlTaskId: "t1",
			},],);
		},);
		expect(seen,).toEqual([
			{ request: `GET ${LAB}/`, body: undefined, },
			{
				request: `POST ${LAB}/`,
				body: {
					inputDataset: "sales",
					taskType: "PREDICTION",
					targetVariable: "amount",
					predictionType: "TIMESERIES_FORECAST",
					timeVariable: "day",
					timeseriesIdentifiers: ["store",],
					backendType: "PY_MEMORY",
					guessPolicy: "TIMESERIES_DEFAULT",
				},
			},
			{
				request:
					`POST ${LAB}/a1/t1/guess?targetVariable=y&timeseriesIdentifiers=s1&timeseriesIdentifiers=s2&fullReguess=false`,
				body: undefined,
			},
			{ request: `POST ${LAB}/a1/t1/guess`, body: undefined, },
			{ request: `POST ${LAB}/a1/t1/reguess-with-forecasting-params`, body: { forecastHorizon: 5, }, },
			{ request: "PUT /public/api/projects/P/lab/a1/", body: { id: "a1", name: "renamed", }, },
			{ request: "GET /public/api/projects/P/lab/a1/models/", body: undefined, },
		],);
	});

	it("requires a time variable for forecasting tasks before any request", async () => {
		const seen = await withServer(async (client,) => {
			await expect(
				client.mlTasks.createForDataset({
					inputDataset: "sales",
					taskType: "PREDICTION",
					targetVariable: "amount",
					predictionType: "TIMESERIES_FORECAST",
				},),
			).rejects.toBeInstanceOf(ClientValidationError,);
		},);
		expect(seen,).toEqual([],);
	});
});
