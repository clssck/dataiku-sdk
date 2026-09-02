import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";
import { WebappsResource, } from "../src/resources/webapps.js";
import { stableHash, } from "../src/utils/stable-hash.js";

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

describe("WebappsResource", () => {
	it("lists webapps", async () => {
		let requestedPath = "";

		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestedPath = `${req.method ?? "GET"} ${url.pathname}`;
			sendJson(res, [{ id: "webapp-1", name: "Main", type: "STANDARD", projectKey: "TEST", },],);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(resource.list(),).resolves.toEqual([
				{ id: "webapp-1", name: "Main", type: "STANDARD", projectKey: "TEST", },
			],);
		},);

		expect(requestedPath,).toBe("GET /public/api/projects/TEST/webapps/",);
	});

	it("gets settings and backend state", async () => {
		const requests: string[] = [];
		const settings = { id: "webapp-1", name: "Main", type: "STANDARD", params: {}, };
		const state = { projectKey: "TEST", webAppId: "webapp-1", futureInfo: { alive: true, }, };

		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (url.pathname.endsWith("/backend/state",)) {
				sendJson(res, state,);
				return;
			}
			sendJson(res, settings,);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(resource.getSettings("webapp-1",),).resolves.toEqual(settings,);
			await expect(resource.getBackendState("webapp-1",),).resolves.toEqual(state,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/webapps/webapp-1",
			"GET /public/api/projects/TEST/webapps/webapp-1/backend/state",
		],);
	});

	it("creates webapps with the supplied body", async () => {
		let requestedMethod = "";
		let requestedPath = "";
		let requestedBody: unknown;
		const payload = { name: "Main", type: "STANDARD", };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestedMethod = req.method ?? "";
			requestedPath = url.pathname;
			requestedBody = JSON.parse(await readBody(req,),) as unknown;
			sendJson(res, { webAppId: "webapp-1", },);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(resource.create(payload,),).resolves.toEqual({ webAppId: "webapp-1", },);
		},);

		expect(requestedMethod,).toBe("POST",);
		expect(requestedPath,).toBe("/public/api/projects/TEST/webapps/",);
		expect(requestedBody,).toEqual(payload,);
	});

	it("updates webapp settings via GET-merge-PUT, preserving untouched fields", async () => {
		const requests: Array<{ method: string; path: string; body: unknown; }> = [];
		const stored = {
			id: "webapp/1",
			name: "Main",
			type: "STANDARD",
			params: { html: "<main />", backend: "python", },
			tags: ["prod",],
		};
		const patch = { name: "Updated", params: { html: "<section />", }, };
		const merged = {
			id: "webapp/1",
			name: "Updated",
			type: "STANDARD",
			params: { html: "<main />", backend: "python", },
			tags: ["prod",],
		};
		const response = { ...merged, saved: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const method = req.method ?? "";
			const body = await readBody(req,);
			requests.push({ method, path: url.pathname, body: body ? JSON.parse(body,) : undefined, },);
			if (method === "GET") {
				sendJson(res, stored,);
				return;
			}
			sendJson(res, response,);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(
				resource.updateSettings("webapp/1", { name: "Updated", params: { html: "<main />", }, },),
			).resolves.toEqual(response,);
		},);

		expect(requests,).toEqual([
			{ method: "GET", path: "/public/api/projects/TEST/webapps/webapp%2F1", body: undefined, },
			{ method: "PUT", path: "/public/api/projects/TEST/webapps/webapp%2F1", body: merged, },
		],);
	});

	it("refuses update-settings when the expectHash guard sees a stale object", async () => {
		const stored = { id: "webapp-1", name: "Main", type: "STANDARD", params: {}, };
		let sawPut = false;

		await withServer(async (req, res,) => {
			if ((req.method ?? "") === "PUT") sawPut = true;
			sendJson(res, stored,);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			const staleHash = "0".repeat(64,);
			await expect(
				resource.updateSettings("webapp-1", { name: "Updated", }, undefined, {
					expectHash: staleHash,
				},),
			).rejects.toThrow(ClientValidationError,);
		},);

		expect(sawPut,).toBe(false,);
	});

	it("accepts update-settings when the expectHash guard matches", async () => {
		const stored = { id: "webapp-1", name: "Main", type: "STANDARD", params: {}, };
		const response = { ...stored, name: "Updated", };
		let putBody: unknown;

		await withServer(async (req, res,) => {
			if ((req.method ?? "") === "PUT") {
				putBody = JSON.parse(await readBody(req,),) as unknown;
			}
			sendJson(res, (req.method ?? "") === "GET" ? stored : response,);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(
				resource.updateSettings("webapp-1", { name: "Updated", }, undefined, {
					expectHash: stableHash(stored,),
				},),
			).resolves.toEqual(response,);
		},);

		expect(putBody,).toEqual(response,);
	});

	it("stops and restarts webapp backends", async () => {
		const requests: Array<{ method: string; path: string; body: unknown; }> = [];

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const body = await readBody(req,);
			requests.push({
				method: req.method ?? "",
				path: url.pathname,
				body: JSON.parse(body,) as unknown,
			},);
			if (url.pathname.endsWith("/actions/restart",)) {
				sendJson(res, { jobId: "restart-future-1", hasResult: false, alive: true, },);
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await resource.stopBackend("webapp-1",);
			await expect(resource.startOrRestartBackend("webapp-1",),).resolves.toEqual({
				jobId: "restart-future-1",
				hasResult: false,
				alive: true,
			},);
		},);

		expect(requests,).toEqual([
			{
				method: "PUT",
				path: "/public/api/projects/TEST/webapps/webapp-1/backend/actions/stop",
				body: {},
			},
			{
				method: "PUT",
				path: "/public/api/projects/TEST/webapps/webapp-1/backend/actions/restart",
				body: {},
			},
		],);
	});

	it("waits on the restart future and surfaces the settled wait result", async () => {
		const requests: Array<{ method: string; path: string; }> = [];
		let polls = 0;

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push({ method: req.method ?? "", path: url.pathname, },);
			if (url.pathname.endsWith("/actions/restart",)) {
				sendJson(res, { jobId: "restart-future-9", hasResult: false, alive: true, },);
				return;
			}
			polls += 1;
			sendJson(
				res,
				polls >= 2
					? { jobId: "restart-future-9", hasResult: true, alive: false, result: { started: true, }, }
					: { jobId: "restart-future-9", hasResult: false, alive: true, },
			);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(
				resource.restartBackendAndWait("webapp-1", undefined, {
					pollIntervalMs: 1,
					timeoutMs: 5_000,
				},),
			).resolves.toMatchObject({
				futureId: "restart-future-9",
				state: "DONE",
				success: true,
				hasResult: true,
			},);
		},);

		expect(requests[0],).toEqual({
			method: "PUT",
			path: "/public/api/projects/TEST/webapps/webapp-1/backend/actions/restart",
		},);
		expect(requests.slice(1,).every((request,) => request.method === "GET"),).toBe(true,);
		expect(polls,).toBeGreaterThanOrEqual(2,);
	});
});
