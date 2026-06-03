import { describe, expect, it, } from "bun:test";
import { mkdtemp, rm, writeFile, } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { ApiDeployerResource, } from "../src/resources/api-deployer.js";

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

describe("ApiDeployerResource", () => {
	it("lists deployments", async () => {
		const deployments = [{ id: "dep-1", version: "v1", },];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, deployments,);
		}, async (url,) => {
			const resource = new ApiDeployerResource(createClient(url,),);
			await expect(resource.listDeployments(),).resolves.toEqual(deployments,);
		},);

		expect(requests,).toEqual(["GET /public/api/api-deployer/deployments",],);
	});

	it("creates deployments with the supplied settings body", async () => {
		const body = {
			deploymentId: "dep-1",
			publishedServiceId: "svc-1",
			infraId: "infra-1",
			version: "v1",
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
			const resource = new ApiDeployerResource(createClient(url,),);
			await expect(resource.createDeployment(body,),).resolves.toEqual({ id: "dep-1", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/api-deployer/deployments",);
		expect(observedBody,).toEqual(body,);
	});

	it("gets and deletes deployments with encoded ids", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			if (req.method === "GET") {
				sendJson(res, { id: "dep one", },);
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ApiDeployerResource(createClient(url,),);
			await expect(resource.getDeployment("dep one",),).resolves.toEqual({ id: "dep one", },);
			await expect(resource.deleteDeployment("dep one",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"GET /public/api/api-deployer/deployments/dep%20one",
			"DELETE /public/api/api-deployer/deployments/dep%20one",
		],);
	});

	it("saves deployment settings with PUT", async () => {
		const body = {
			enabled: true,
			generationsMapping: { mode: "SINGLE_GENERATION", generation: "v2", },
		};
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
			const resource = new ApiDeployerResource(createClient(url,),);
			await expect(resource.saveDeploymentSettings("dep/settings", body,),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("PUT",);
		expect(observedPath,).toBe("/public/api/api-deployer/deployments/dep%2Fsettings/settings",);
		expect(observedBody,).toEqual(body,);
	});

	it("starts deployment updates through the actions endpoint", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { jobId: "job-1", },);
		}, async (url,) => {
			const resource = new ApiDeployerResource(createClient(url,),);
			await expect(resource.startDeploymentUpdate("dep/action",),).resolves.toEqual({
				jobId: "job-1",
			},);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/api-deployer/deployments/dep%2Faction/actions/update",
		);
		expect(observedBody,).toEqual({},);
	});

	it("publishes service versions as multipart uploads", async () => {
		const tempDir = await mkdtemp(join(tmpdir(), "dataiku-api-deployer-",),);
		const archivePath = join(tempDir, "service-version.zip",);
		await writeFile(archivePath, "zip contents",);
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
				const resource = new ApiDeployerResource(createClient(url,),);
				await expect(resource.publishServiceVersion("svc/upload", archivePath,),)
					.resolves.toBeUndefined();
			},);
		} finally {
			await rm(tempDir, { recursive: true, force: true, },);
		}

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/api-deployer/services/svc%2Fupload/versions",);
		expect(observedContentType.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(observedBody,).toContain('name="file"',);
		expect(observedBody,).toContain('filename="service-version.zip"',);
		expect(observedBody,).toContain("zip contents",);
	});
});
