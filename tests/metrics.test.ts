import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { MetricsResource, } from "../src/resources/metrics.js";

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

describe("MetricsResource", () => {
	it("gets dataset metric values from the global partition", async () => {
		const payload = {
			metrics: [{
				metric: { id: "records:COUNT_RECORDS", },
				lastValues: [{ partition: "NP", value: "12", dataType: "BIGINT", },],
			},],
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, payload,);
		}, async (url,) => {
			const resource = new MetricsResource(createClient(url,),);
			await expect(resource.getDatasetMetrics("orders/2026",),).resolves.toEqual(payload,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders%2F2026/metrics/last/NP",
		],);
	});

	it(
		"computes configured dataset metrics with the Python client's default partition query",
		async () => {
			const report = {
				jobId: "job-1",
				computed: [{ metricId: "records:COUNT_RECORDS", value: 12, },],
			};
			let observedMethod = "";
			let observedPath = "";
			let observedBody = "";

			await withServer(async (req, res,) => {
				observedMethod = req.method ?? "";
				observedPath = req.url ?? "";
				observedBody = await readBody(req,);
				sendJson(res, report,);
			}, async (url,) => {
				const resource = new MetricsResource(createClient(url,),);
				await expect(
					resource.computeDatasetMetrics("orders 2026", "ALT PROJECT",),
				).resolves.toEqual(report,);
			},);

			expect(observedMethod,).toBe("POST",);
			expect(observedPath,).toBe(
				"/public/api/projects/ALT%20PROJECT/datasets/orders%202026/actions/computeMetrics?partition=",
			);
			expect(observedBody,).toBe("",);
		},
	);

	it("gets managed folder metric values", async () => {
		const payload = {
			metrics: [{
				metric: { id: "basic:COUNT_FILES", },
				lastValues: [{ value: "3", dataType: "BIGINT", },],
			},],
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, payload,);
		}, async (url,) => {
			const resource = new MetricsResource(createClient(url,),);
			await expect(resource.getFolderMetrics("folder/id",),).resolves.toEqual(payload,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/managedfolders/folder%2Fid/metrics/last",
		],);
	});

	it("gets dataset metric history with metricLookup as a query parameter", async () => {
		const history = {
			metricId: "records:COUNT_RECORDS",
			values: [{ time: 1_716_000_000_000, value: 12, dataType: "BIGINT", },],
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, history,);
		}, async (url,) => {
			const resource = new MetricsResource(createClient(url,),);
			await expect(
				resource.getDatasetMetricHistory("orders 2026", "records:COUNT_RECORDS",),
			).resolves.toEqual(history,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders%202026/metrics/history/NP?metricLookup=records%3ACOUNT_RECORDS",
		],);
	});
});
