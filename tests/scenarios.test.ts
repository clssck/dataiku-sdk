import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, type DataikuClientConfig, } from "../src/client.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient,) => Promise<void>,
	config?: Partial<DataikuClientConfig>,
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
	const client = new DataikuClient({
		url: `http://127.0.0.1:${port}`,
		apiKey: "test",
		projectKey: "TEST",
		...config,
	},);

	try {
		await run(client,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error?: Error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

describe("ScenariosResource.runAndWait", () => {
	it("resolves the finished scenario run when DSS reports a different trigger run id", async () => {
		const requests: string[] = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/nightly/run/"
			) {
				sendJson(res, { trigger: { id: "manual", }, runId: "trig-188", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/nightly/get-run-for-trigger"
			) {
				expect(url.searchParams.get("triggerId",),).toBe("manual",);
				expect(url.searchParams.get("triggerRunId",),).toBe("trig-188",);
				sendJson(res, {
					scenarioRun: {
						runId: "run-191",
						result: { outcome: "SUCCESS", },
					},
				},);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("nightly", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result,).toMatchObject({
				scenarioId: "nightly",
				runId: "run-191",
				triggerRunId: "trig-188",
				outcome: "SUCCESS",
				success: true,
				pollCount: 1,
			},);
			expect(result.timedOut,).toBeUndefined();
		},);

		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/scenarios/nightly/run/",
			"GET /public/api/projects/TEST/scenarios/nightly/get-run-for-trigger?triggerId=manual&triggerRunId=trig-188",
			"GET /public/api/projects/TEST/scenarios/nightly/run-191/",
		],);
	});

	it("uses legacy trigger id as the trigger run id when runId is absent", async () => {
		const requests: string[] = [];
		const observedTriggerRunIds: Array<string | null> = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/legacy/run/"
			) {
				sendJson(res, { id: "legacy-trigger-9", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/legacy/get-run-for-trigger"
			) {
				observedTriggerRunIds.push(url.searchParams.get("triggerRunId",),);
				sendJson(res, {
					scenarioRun: {
						runId: "run-legacy-9",
						result: { outcome: "SUCCESS", },
					},
				},);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("legacy", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result,).toMatchObject({
				scenarioId: "legacy",
				runId: "run-legacy-9",
				triggerRunId: "legacy-trigger-9",
				outcome: "SUCCESS",
				success: true,
				pollCount: 1,
			},);
			expect(result.timedOut,).toBeUndefined();
		},);

		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/scenarios/legacy/run/",
			"GET /public/api/projects/TEST/scenarios/legacy/get-run-for-trigger",
			"GET /public/api/projects/TEST/scenarios/legacy/run-legacy-9/",
		],);
		expect(observedTriggerRunIds,).toEqual(["legacy-trigger-9",],);
	});

	it("returns sanitized step outcomes from run details after a failed scenario run", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/nightly/run/"
			) {
				sendJson(res, { trigger: { id: "manual", }, runId: "trigger-steps-1", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/nightly/get-run-for-trigger"
			) {
				sendJson(res, {
					scenarioRun: {
						runId: "run-steps-1",
						result: { outcome: "FAILED", },
					},
				},);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/nightly/run-steps-1/"
			) {
				sendJson(res, {
					stepRuns: [
						{
							step: { name: "Prepare data", type: "compute_metrics", id: "step-prepare", },
							result: { outcome: "SUCCESS", logTail: "prepared", data: { rows: 10, }, },
						},
						{
							step: { name: "Validate data", type: "check_dataset", id: "step-validate", },
							result: { outcome: "FAILED", logTail: "failed validation", data: { failedRows: 2, }, },
						},
					],
				},);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("nightly", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result,).toMatchObject({
				scenarioId: "nightly",
				runId: "run-steps-1",
				triggerRunId: "trigger-steps-1",
				outcome: "FAILED",
				success: false,
			},);
			expect(result.steps,).toEqual([
				{ name: "Prepare data", type: "compute_metrics", outcome: "SUCCESS", },
				{ name: "Validate data", type: "check_dataset", outcome: "FAILED", },
			],);
		},);
	});

	it("does not leak raw step result or data fields in runAndWait steps", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/sanitized/run/"
			) {
				sendJson(res, { trigger: { id: "manual", }, runId: "trigger-sanitize-1", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/sanitized/get-run-for-trigger"
			) {
				sendJson(res, {
					scenarioRun: {
						runId: "run-sanitize-1",
						result: { outcome: "FAILED", },
					},
				},);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/sanitized/run-sanitize-1/"
			) {
				sendJson(res, {
					stepRuns: [
						{
							step: {
								id: "raw-step-id",
								name: "Only safe fields",
								type: "custom_python",
								params: { script: "secret", },
							},
							result: {
								outcome: "FAILED",
								log: "stack trace",
								data: { secret: "token", },
								startTime: 123,
							},
							rawTopLevel: "should-not-leak",
						},
					],
				},);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("sanitized", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result.steps,).toEqual([
				{ name: "Only safe fields", type: "custom_python", outcome: "FAILED", },
			],);
			expect(Object.keys(result.steps?.[0] ?? {},).sort(),).toEqual(["name", "outcome", "type",],);
		},);
	});

	it("keeps the terminal outcome when run details are unavailable", async () => {
		const requests: string[] = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/report-down/run/"
			) {
				sendJson(res, { trigger: { id: "manual", }, runId: "trigger-report-down-1", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/report-down/get-run-for-trigger"
			) {
				sendJson(res, {
					scenarioRun: {
						runId: "run-report-down-1",
						result: { outcome: "SUCCESS", },
					},
				},);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/report-down/run-report-down-1/"
			) {
				sendJson(res, { message: "run details temporarily unavailable", }, 500,);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("report-down", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result,).toMatchObject({
				scenarioId: "report-down",
				runId: "run-report-down-1",
				triggerRunId: "trigger-report-down-1",
				outcome: "SUCCESS",
				success: true,
			},);
			expect(result.steps,).toBeUndefined();
		}, { retryMaxAttempts: 1, },);

		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/scenarios/report-down/run/",
			"GET /public/api/projects/TEST/scenarios/report-down/get-run-for-trigger",
			"GET /public/api/projects/TEST/scenarios/report-down/run-report-down-1/",
		],);
	});

	it("surfaces successful step outcomes from run details", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (
				req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/successful/run/"
			) {
				sendJson(res, { trigger: { id: "manual", }, runId: "trigger-success-1", },);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/successful/get-run-for-trigger"
			) {
				sendJson(res, {
					scenarioRun: {
						runId: "run-success-1",
						result: { outcome: "SUCCESS", },
					},
				},);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/scenarios/successful/run-success-1/"
			) {
				sendJson(res, {
					stepRuns: [
						{
							step: { name: "Refresh dataset", type: "build_dataset", },
							result: { outcome: "SUCCESS", },
						},
					],
				},);
				return;
			}

			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.scenarios.runAndWait("successful", {
				pollIntervalMs: 1,
				timeoutMs: 50,
			},);

			expect(result,).toMatchObject({
				scenarioId: "successful",
				runId: "run-success-1",
				triggerRunId: "trigger-success-1",
				outcome: "SUCCESS",
				success: true,
			},);
			expect(result.steps,).toEqual([
				{ name: "Refresh dataset", type: "build_dataset", outcome: "SUCCESS", },
			],);
		},);
	});
});
