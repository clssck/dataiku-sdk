import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { WebappsResource, } from "../src/resources/webapps.js";

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

	it("updates webapp settings with the supplied body", async () => {
		let requestedMethod = "";
		let requestedPath = "";
		let requestedBody: unknown;
		const payload = {
			id: "webapp/1",
			name: "Updated",
			type: "STANDARD",
			params: { html: "<main />", },
		};
		const response = { ...payload, saved: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestedMethod = req.method ?? "";
			requestedPath = url.pathname;
			requestedBody = JSON.parse(await readBody(req,),) as unknown;
			sendJson(res, response,);
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await expect(resource.updateSettings("webapp/1", payload,),).resolves.toEqual(response,);
		},);

		expect(requestedMethod,).toBe("PUT",);
		expect(requestedPath,).toBe("/public/api/projects/TEST/webapps/webapp%2F1",);
		expect(requestedBody,).toEqual(payload,);
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
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new WebappsResource(createClient(url,),);

			await resource.stopBackend("webapp-1",);
			await resource.startOrRestartBackend("webapp-1",);
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
});
