import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { StatisticsResource, } from "../src/resources/statistics.js";

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) {
		body += chunk.toString();
	}
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
		projectKey: "TEST",
	},);
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);

	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

describe("StatisticsResource", () => {
	it("lists dataset worksheets from the bare array endpoint", async () => {
		const worksheets = [{ id: "worksheet-1", name: "EDA", }, { id: "worksheet-2", },];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, worksheets,);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(resource.listWorksheets("orders/table",),).resolves.toEqual(worksheets,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders%2Ftable/statistics/worksheets/",
		],);
	});

	it("gets worksheets with encoded dataset, worksheet, and project ids", async () => {
		const worksheet = { id: "worksheet/slash", name: "Stats", rootCard: { type: "container", }, };
		const requests: string[] = [];
		const expectedRequest = "GET /public/api/projects/ALT%2FPROJECT/datasets/orders%20table"
			+ "/statistics/worksheets/worksheet%2Fslash";

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, worksheet,);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.getWorksheet("orders table", "worksheet/slash", "ALT/PROJECT",),
			).resolves.toEqual(worksheet,);
		},);

		expect(requests,).toEqual([expectedRequest,],);
	});

	it("creates worksheets by posting the supplied definition", async () => {
		const body = {
			name: "My worksheet",
			dataSpec: {
				inputDatasetSmartName: "orders",
				datasetSelection: {
					partitionSelectionMethod: "ALL",
					maxRecords: 30000,
					samplingMethod: "FULL",
				},
			},
		};
		const created = { id: "worksheet-1", ...body, };
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, created,);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(resource.createWorksheet("orders", body,),).resolves.toEqual(created,);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/datasets/orders/statistics/worksheets/",
		);
		expect(observedBody,).toEqual(body,);
	});

	it("rejects worksheet definitions missing dataSpec.datasetSelection before POSTing", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 500;
			res.end("unexpected request",);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.createWorksheet("orders", {
					name: "Missing dataset selection",
					dataSpec: { inputDatasetSmartName: "orders", },
				},),
			).rejects.toThrow("dataSpec.datasetSelection",);
		},);

		expect(requests,).toEqual([],);
	});

	it("updates worksheet definitions through PUT", async () => {
		const body = { id: "worksheet-1", name: "Renamed", rootCard: { cards: [], }, };
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, body,);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.updateWorksheet("orders", "worksheet-1", body,),
			).resolves.toEqual(body,);
		},);

		expect(observedMethod,).toBe("PUT",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/datasets/orders/statistics/worksheets/worksheet-1",
		);
		expect(observedBody,).toEqual(body,);
	});

	it("deletes worksheets", async () => {
		const requests: string[] = [];
		const expectedRequest = "DELETE /public/api/projects/TEST/datasets/orders/statistics"
			+ "/worksheets/worksheet%2Fdelete";

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.deleteWorksheet("orders", "worksheet/delete",),
			).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([expectedRequest,],);
	});

	it("runs cards and computations through worksheet action endpoints", async () => {
		const requests: string[] = [];
		const bodies: unknown[] = [];
		const runCardRequest = "POST /public/api/projects/TEST/datasets/orders/statistics"
			+ "/worksheets/worksheet-1/actions/run-card";
		const runComputationRequest = "POST /public/api/projects/TEST/datasets/orders/statistics"
			+ "/worksheets/worksheet-1/actions/run-computation";

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			bodies.push(JSON.parse(await readBody(req,),),);
			if (req.url?.endsWith("/actions/run-card",)) {
				sendJson(res, { jobId: "job-card", },);
				return;
			}
			sendJson(res, { jobId: "job-computation", },);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.runCard("orders", "worksheet-1", { type: "univariate_header", },),
			).resolves.toEqual({
				jobId: "job-card",
			},);
			await expect(
				resource.runComputation("orders", "worksheet-1", { type: "compute-card", },),
			).resolves.toEqual({
				jobId: "job-computation",
			},);
		},);

		expect(requests,).toEqual([runCardRequest, runComputationRequest,],);
		expect(bodies,).toEqual([{ type: "univariate_header", }, { type: "compute-card", },],);
	});

	it("runs a worksheet by first loading and submitting its root card", async () => {
		const requests: string[] = [];
		let observedBody: unknown;
		const runCardRequest = "POST /public/api/projects/TEST/datasets/orders/statistics"
			+ "/worksheets/worksheet-1/actions/run-card";

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "GET") {
				sendJson(res, {
					id: "worksheet-1",
					rootCard: { type: "ks_test", columns: ["amount",], },
				},);
				return;
			}
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { jobId: "job-root", },);
		}, async (url,) => {
			const resource = new StatisticsResource(createClient(url,),);
			await expect(
				resource.runWorksheet("orders", "worksheet-1",),
			).resolves.toEqual({ jobId: "job-root", },);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders/statistics/worksheets/worksheet-1",
			runCardRequest,
		],);
		expect(observedBody,).toEqual({ type: "ks_test", columns: ["amount",], },);
	});
});
