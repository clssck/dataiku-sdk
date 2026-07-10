import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ApiServicesResource, } from "../src/resources/api-services.js";

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

describe("ApiServicesResource", () => {
	it("lists API services and gets settings", async () => {
		const requests: string[] = [];
		const services = [{ serviceId: "svc 1/v1", name: "Fraud API", },];
		const settings = { id: "svc 1/v1", endpoints: [], };

		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (url.pathname.endsWith("/settings",)) {
				sendJson(res, settings,);
				return;
			}
			sendJson(res, services,);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			await expect(resource.list(),).resolves.toEqual(services,);
			await expect(resource.getSettings("svc 1/v1",),).resolves.toEqual(settings,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/apiservices/",
			"GET /public/api/projects/TEST/apiservices/svc%201%2Fv1/settings",
		],);
	});

	it("creates API services at the service path", async () => {
		let requestPath = "";
		let requestMethod = "";
		let requestBody: unknown;
		const response = { serviceId: "svc 1/v1", created: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestPath = url.pathname;
			requestMethod = req.method ?? "";
			requestBody = JSON.parse(await readBody(req,),);
			sendJson(res, response,);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			await expect(resource.create("svc 1/v1",),).resolves.toEqual(response,);
		},);

		expect(requestMethod,).toBe("POST",);
		expect(requestPath,).toBe("/public/api/projects/TEST/apiservices/svc%201%2Fv1",);
		expect(requestBody,).toEqual({},);
	});

	it("saves API service settings with the provided body", async () => {
		let requestPath = "";
		let requestMethod = "";
		let requestBody: unknown;
		const settings = {
			endpoints: [{ id: "predict", type: "PY_FUNCTION", },],
			versionTag: { lastModifiedBy: { login: "admin", }, },
		};
		const response = { saved: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestPath = url.pathname;
			requestMethod = req.method ?? "";
			requestBody = JSON.parse(await readBody(req,),);
			sendJson(res, response,);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			await expect(resource.saveSettings("svc", settings, "OTHER PROJECT",),).resolves.toEqual(
				response,
			);
		},);

		expect(requestMethod,).toBe("PUT",);
		expect(requestPath,).toBe("/public/api/projects/OTHER%20PROJECT/apiservices/svc/settings",);
		expect(requestBody,).toEqual(settings,);
	});

	it("adds a prediction endpoint without dropping existing service settings", async () => {
		const requests: string[] = [];
		let savedBody: unknown;
		const existingSettings = {
			authRealm: "project-default",
			endpoints: [{ id: "existing", type: "PY_FUNCTION", config: { enabled: true, }, },],
			versionTag: { versionNumber: 17, },
		};
		const response = { saved: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (req.method === "GET") {
				sendJson(res, existingSettings,);
				return;
			}
			savedBody = JSON.parse(await readBody(req,),);
			sendJson(res, response,);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			await expect(
				resource.addPredictionEndpoint("svc 1", "predict churn", "model/1", "OTHER PROJECT",),
			).resolves.toEqual(response,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/OTHER%20PROJECT/apiservices/svc%201/settings",
			"PUT /public/api/projects/OTHER%20PROJECT/apiservices/svc%201/settings",
		],);
		expect(savedBody,).toEqual({
			authRealm: "project-default",
			endpoints: [
				{ id: "existing", type: "PY_FUNCTION", config: { enabled: true, }, },
				{ id: "predict churn", type: "STD_PREDICTION", modelRef: "model/1", },
			],
			versionTag: { versionNumber: 17, },
		},);
	});

	it("manages API service packages", async () => {
		const requests: string[] = [];
		const requestBodies: unknown[] = [];
		const packages = [{ packageId: "pkg/1", serviceId: "svc id", },];
		const summary = { packageId: "pkg/1", status: "BUILT", };
		const createdMessage = "Created package pkg/1";
		const published = { packageId: "pkg/1", published: true, };

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname.endsWith("/packages",)) {
				sendJson(res, packages,);
				return;
			}
			if (req.method === "GET" && url.pathname.endsWith("/summary",)) {
				sendJson(res, summary,);
				return;
			}
			if (req.method === "DELETE") {
				res.statusCode = 204;
				res.end();
				return;
			}
			requestBodies.push(JSON.parse(await readBody(req,),),);
			if (url.pathname.endsWith("/publish",)) {
				sendJson(res, published,);
				return;
			}
			res.setHeader("Content-Type", "text/plain",);
			res.end(createdMessage,);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			await expect(resource.listPackages("svc id",),).resolves.toEqual(packages,);
			await expect(resource.getPackageSummary("svc id", "pkg/1",),).resolves.toEqual(summary,);
			await expect(resource.createPackage("svc id", "pkg/1",),).resolves.toEqual({
				message: createdMessage,
			},);
			await resource.deletePackage("svc id", "pkg/1",);
			await expect(resource.publishPackage("svc id", "pkg/1",),).resolves.toEqual(published,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/apiservices/svc%20id/packages",
			"GET /public/api/projects/TEST/apiservices/svc%20id/packages/pkg%2F1/summary",
			"POST /public/api/projects/TEST/apiservices/svc%20id/packages/pkg%2F1",
			"DELETE /public/api/projects/TEST/apiservices/svc%20id/packages/pkg%2F1",
			"POST /public/api/projects/TEST/apiservices/svc%20id/packages/pkg%2F1/publish",
		],);
		expect(requestBodies,).toEqual([{}, {},],);
	});

	it("downloads package archives as responses", async () => {
		let requestPath = "";
		let requestMethod = "";

		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestPath = url.pathname;
			requestMethod = req.method ?? "";
			res.statusCode = 200;
			res.setHeader("Content-Type", "application/zip",);
			res.end("archive bytes",);
		}, async (url,) => {
			const resource = new ApiServicesResource(createClient(url,),);
			const response = await resource.downloadPackageArchive("svc", "pkg",);
			expect(response,).toBeInstanceOf(Response,);
			await expect(response.text(),).resolves.toBe("archive bytes",);
		},);

		expect(requestMethod,).toBe("GET",);
		expect(requestPath,).toBe("/public/api/projects/TEST/apiservices/svc/packages/pkg/archive",);
	});
});
