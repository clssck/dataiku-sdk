import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { DataikuClient, } from "../src/client.js";
import { BundlesResource, ProjectDeployerResource, } from "../src/resources/bundles.js";

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

describe("BundlesResource", () => {
	it("lists exported bundles", async () => {
		const bundles = [{ id: "bundle-1", }, { id: "bundle-2", },];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, { bundles, },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.listExported(),).resolves.toEqual(bundles,);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/TEST/bundles/exported",],);
	});

	it("exports bundles through PUT with encoded ids", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.exportBundle("bundle/slash",),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("PUT",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/exported/bundle%2Fslash?evaluateProjectStandardsChecks=true",
		);
		expect(observedBody,).toEqual({},);
	});

	it("forwards bundle export release notes and standards-check opt-out", async () => {
		let observedPath = "";

		await withServer(async (req, res,) => {
			await readBody(req,);
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(
				resource.exportBundle("v2", undefined, {
					releaseNotes: "Adds churn model",
					evaluateProjectStandardsChecks: false,
				},),
			).resolves.toBeUndefined();
		},);

		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/exported/v2?releaseNotes=Adds+churn+model&evaluateProjectStandardsChecks=false",
		);
	});

	it("deletes exported and imported bundles with encoded ids", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.deleteExported("bundle/delete",),).resolves.toBeUndefined();
			await expect(resource.deleteImported("bundle/delete",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"DELETE /public/api/projects/TEST/bundles/exported/bundle%2Fdelete",
			"DELETE /public/api/projects/TEST/bundles/imported/bundle%2Fdelete",
		],);
	});

	it("activates imported bundles through the action endpoint", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { jobId: "job-1", },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.activate("bundle action",),).resolves.toEqual({ jobId: "job-1", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/imported/bundle%20action/actions/activate",
		);
		expect(observedBody,).toEqual({},);
	});

	it("forwards activation scenarios on activate", async () => {
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedBody = JSON.parse(await readBody(req,),) as unknown;
			sendJson(res, { report: "ok", },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(
				resource.activate("v1", undefined, {
					scenariosToEnable: { daily_build: true, hourly_retrain: false, },
				},),
			).resolves.toEqual({ report: "ok", },);
		},);

		expect(observedBody,).toEqual({
			scenariosActiveOnActivation: { daily_build: true, hourly_retrain: false, },
		},);
	});

	it("forwards publishedProjectKey on bundle publish", async () => {
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			observedPath = `${url.pathname}${url.search}`;
			observedBody = JSON.parse(await readBody(req,),) as unknown;
			sendJson(res, { publishedProjectKey: "PROD-CHURN", publishedOn: 1, publishedBy: "admin", },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(
				resource.publish("v1", undefined, { publishedProjectKey: "PROD-CHURN", },),
			).resolves.toEqual({
				publishedProjectKey: "PROD-CHURN",
				publishedOn: 1,
				publishedBy: "admin",
			},);
		},);

		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/v1/publish?publishedProjectKey=PROD-CHURN",
		);
		expect(observedBody,).toEqual({},);
	});

	it("imports bundle streams as multipart uploads", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-bundles-",),);
		const archivePath = path.join(tempDir, "project-bundle.zip",);
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
				sendJson(res, { imported: true, },);
			}, async (url,) => {
				const resource = new BundlesResource(createClient(url,),);
				await expect(resource.importFromStream(archivePath,),).resolves.toBeUndefined();
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/imported/actions/importFromStream",
		);
		expect(observedContentType.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(observedBody,).toContain('name="file"',);
		expect(observedBody,).toContain('filename="project-bundle.zip"',);
		expect(observedBody,).toContain("zip contents",);
	});
	it("imports server-side archives with archivePath as a query parameter", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { imported: true, },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.importFromArchive("/data/bundles/v1.zip",),).resolves.toEqual({
				imported: true,
			},);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/bundles/imported/actions/importFromArchive?archivePath=%2Fdata%2Fbundles%2Fv1.zip",
		);
		expect(observedBody,).toEqual({},);
	});

	it("unwraps the bundles wrapper when listing imported bundles", async () => {
		const bundles = [{ bundleId: "imp-1", },];

		await withServer((req, res,) => {
			expect(req.url,).toBe("/public/api/projects/TEST/bundles/imported",);
			sendJson(res, { bundles, },);
		}, async (url,) => {
			const resource = new BundlesResource(createClient(url,),);
			await expect(resource.listImported(),).resolves.toEqual(bundles,);
		},);
	});
});

describe("ProjectDeployerResource", () => {
	it("lists published projects", async () => {
		const projects = [{ projectKey: "PROJECT_ONE", }, { projectKey: "PROJECT_TWO", },];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, projects,);
		}, async (url,) => {
			const resource = new ProjectDeployerResource(createClient(url,),);
			await expect(resource.listProjects(),).resolves.toEqual(projects,);
		},);

		expect(requests,).toEqual(["GET /public/api/project-deployer/projects",],);
	});

	it("creates deployments with the supplied settings body", async () => {
		const body = {
			deploymentId: "dep-1",
			publishedProjectKey: "PROJECT_ONE",
			infraId: "infra-1",
			bundleId: "v1",
		};
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { id: "dep-1", },);
		}, async (url,) => {
			const resource = new ProjectDeployerResource(createClient(url,),);
			await expect(resource.createDeployment(body,),).resolves.toEqual({ id: "dep-1", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/project-deployer/deployments",);
		expect(observedBody,).toEqual(body,);
	});

	it("deletes deployments with encoded ids", async () => {
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectDeployerResource(createClient(url,),);
			await expect(resource.deleteDeployment("dep/delete",),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("DELETE",);
		expect(observedPath,).toBe("/public/api/project-deployer/deployments/dep%2Fdelete",);
	});

	it("starts deployment updates through the action endpoint", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { jobId: "job-1", },);
		}, async (url,) => {
			const resource = new ProjectDeployerResource(createClient(url,),);
			await expect(resource.startUpdate("dep/action",),).resolves.toEqual({ jobId: "job-1", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/project-deployer/deployments/dep%2Faction/actions/update",
		);
		expect(observedBody,).toEqual({},);
	});
});
