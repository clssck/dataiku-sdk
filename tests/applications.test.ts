import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import { type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";
import { ApplicationsResource, } from "../src/resources/applications.js";

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
	const server = http.createServer((req, res,) => {
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

describe("ApplicationsResource", () => {
	it("lists Dataiku Apps", async () => {
		const apps = [
			{ appId: "APP_ONE", projectKey: "APP_TEMPLATE", name: "One", },
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, apps,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.listApps(),).resolves.toEqual(apps,);
		},);

		expect(requests,).toEqual(["GET /public/api/apps/",],);
	});

	it("creates a Dataiku App instance", async () => {
		const body = {
			targetProjectKey: "APP_INSTANCE",
			targetProjectName: "App Instance",
			isTemporaryAppInstance: false,
		};
		let observedBody: unknown;
		let observedMethod = "";
		let observedPath = "";

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { appId: "APP_ONE", projectKey: "APP_INSTANCE", jobId: "job-1", },);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.createInstance("APP/ONE", body,),).resolves.toEqual({
				appId: "APP_ONE",
				projectKey: "APP_INSTANCE",
				jobId: "job-1",
			},);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/apps/APP%2FONE/instances",);
		expect(observedBody,).toEqual(body,);
	});

	it("saves the instance manifest", async () => {
		const manifest = {
			homepageSections: [{ type: "STATIC", title: "Overview", },],
			useAsRecipeSettings: { enabled: true, },
		};
		let observedBody: unknown;
		const requests: string[] = [];

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === "/public/api/projects/TEST/app-manifest") {
				sendJson(res, { homepageSections: [], projectAppType: "APP_TEMPLATE", },);
				return;
			}
			if (req.method === "PUT" && req.url === "/public/api/projects/TEST/app-manifest") {
				observedBody = JSON.parse(await readBody(req,),);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.saveInstanceManifest(manifest,),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
		],);
		expect(observedBody,).toEqual(manifest,);
	});

	it("rejects saving an app-instance manifest without sending a PUT", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === "/public/api/projects/TEST/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", homepageSections: [], },);
				return;
			}
			if (req.method === "PUT" && req.url === "/public/api/projects/TEST/app-manifest") {
				res.statusCode = 500;
				res.end("unexpected PUT",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			let caught: unknown;
			try {
				await resource.saveInstanceManifest({ homepageSections: [], },);
			} catch (error) {
				caught = error;
			}
			expect(caught,).toBeInstanceOf(ClientValidationError,);
			expect((caught as ClientValidationError).name,).toBe("ClientValidationError",);
			expect((caught as ClientValidationError).code,).toBe("validation_failed",);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("deletes an app instance project", async () => {
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.deleteInstance("APP INSTANCE",),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("DELETE",);
		expect(observedPath,).toBe("/public/api/projects/APP%20INSTANCE",);
	});

	it("upgrades a Business App instance", async () => {
		let observedBody: unknown;
		let observedMethod = "";
		let observedPath = "";

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { projectKey: "BA_INSTANCE", jobId: "job-2", },);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.upgradeBusinessAppInstance("business/app", "BA INSTANCE",),).resolves
				.toEqual({
					projectKey: "BA_INSTANCE",
					jobId: "job-2",
				},);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/business-apps/business%2Fapp/instances/BA%20INSTANCE/upgrade",
		);
		expect(observedBody,).toEqual({},);
	});

	it("installs a Business App archive as multipart upload", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-business-app-",),);
		const archivePath = path.join(tempDir, "business-app.zip",);
		await fs.writeFile(archivePath, "zip contents",);
		let observedMethod = "";
		let observedPath = "";
		let observedContentType = "";
		let observedBody = "";

		try {
			await withServer(async (req, res,) => {
				observedMethod = req.method ?? "";
				observedPath = req.url ?? "";
				observedContentType = req.headers["content-type"] ?? "";
				observedBody = await readBody(req,);
				sendJson(res, { jobId: "job-1", },);
			}, async (url,) => {
				const resource = new ApplicationsResource(createClient(url,),);
				await expect(resource.installBusinessAppFromArchive(archivePath,),).resolves.toBeUndefined();
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/business-apps/install-from-archive",);
		expect(observedContentType.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(observedBody,).toContain('name="file"',);
		expect(observedBody,).toContain('filename="business-app.zip"',);
		expect(observedBody,).toContain("zip contents",);
	});

	it("gets Business App instance user permissions", async () => {
		let observedMethod = "";
		let observedPath = "";
		const permissions = {
			login: "alice@example.com",
			admin: true,
			readProjectContent: true,
			writeProjectContent: false,
			extra: "kept",
		};

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			sendJson(res, permissions,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(
				resource.getBusinessAppInstanceUserPermissions(
					"business/app",
					"BA INSTANCE",
					"alice@example.com",
				),
			).resolves.toEqual(permissions,);
		},);

		expect(observedMethod,).toBe("GET",);
		expect(observedPath,).toBe(
			"/public/api/business-apps/business%2Fapp/instances/BA%20INSTANCE/permissions/alice%40example.com",
		);
	});
});
