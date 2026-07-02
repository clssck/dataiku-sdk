import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient,) => Promise<void>,
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
		],);
		expect(observedTriggerRunIds,).toEqual(["legacy-trigger-9",],);
	});
});
