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

describe("ScenariosResource script payload helpers", () => {
	it("gets custom Python scenario scripts from the payload endpoint", async () => {
		const requests: string[] = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/GENERATE/payload") {
				sendJson(res, { script: "print('hello')\n", },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await expect(client.scenarios.getScript("GENERATE",),).resolves.toBe("print('hello')\n",);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/TEST/scenarios/GENERATE/payload",],);
	});

	it("treats a missing script key as empty DSS Python .get script semantics", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/EMPTY/payload") {
				sendJson(res, { other: true, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await expect(client.scenarios.getScript("EMPTY",),).resolves.toBe("",);
		},);
	});

	it("validates scenario payload and script field shapes", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/BAD_OBJECT/payload") {
				sendJson(res, ["not", "object",],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/BAD_SCRIPT/payload") {
				sendJson(res, { script: 42, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await expect(client.scenarios.getScript("BAD_OBJECT",),)
				.rejects.toMatchObject({ code: "validation_failed", });
			await expect(client.scenarios.getScript("BAD_SCRIPT",),)
				.rejects.toMatchObject({ code: "validation_failed", });
		},);
	});

	it("sets custom Python scenario scripts with the Python API payload shape", async () => {
		let observedBody: unknown;
		const requests: string[] = [];

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/scenarios/GENERATE/payload") {
				let body = "";
				for await (const chunk of req) body += chunk.toString();
				observedBody = JSON.parse(body,);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await expect(client.scenarios.setScript("GENERATE", "print('done')\n",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual(["PUT /public/api/projects/TEST/scenarios/GENERATE/payload",],);
		expect(observedBody,).toEqual({ script: "print('done')\n", });
	});
});

