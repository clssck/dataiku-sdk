import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import { type IncomingMessage, type ServerResponse, } from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, DataikuError, } from "../src/errors.js";
import {
	APP_MANIFEST_CONCURRENCY_CONTROL,
	ApplicationsResource,
	type AppManifestVersionState,
	type AppManifestVersionUpdate,
	type AppManifestVersionWriteResult,
} from "../src/resources/applications.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
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

	const address = server.address();
	if (!address || typeof address === "string") {
		throw new Error("Test server did not bind to a TCP address.",);
	}
	const url = `http://127.0.0.1:${String(address.port,)}`;
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

interface ManifestVersionServerState {
	requests: string[];
	manifest: Record<string, unknown>;
}

/**
 * Fake DSS app-manifest endpoint: serves the current manifest on GET, stores
 * the PUT body, and applies an optional mutation between PUT and the follow-up
 * verification GET so post-write drift is observable.
 */
function createManifestVersionHandler(
	initialManifest: Record<string, unknown>,
	onPut?: (body: Record<string, unknown>,) => void,
): {
	handler: (req: IncomingMessage, res: ServerResponse,) => void;
	state: ManifestVersionServerState;
} {
	const state: ManifestVersionServerState = {
		requests: [],
		manifest: { ...initialManifest, },
	};
	const handler = (req: IncomingMessage, res: ServerResponse,): void => {
		const request = `${req.method ?? ""} ${req.url ?? ""}`;
		state.requests.push(request,);
		if (request === "GET /public/api/projects/TEST/app-manifest") {
			sendJson(res, state.manifest,);
			return;
		}
		if (request === "PUT /public/api/projects/TEST/app-manifest") {
			void readBody(req,).then((body,) => {
				state.manifest = JSON.parse(body,) as Record<string, unknown>;
				onPut?.(state.manifest,);
				res.statusCode = 204;
				res.end();
			},).catch(() => {
				res.statusCode = 500;
				res.end("failed to read request body",);
			},);
			return;
		}
		res.statusCode = 404;
		res.end(`unexpected ${request}`,);
	};
	return { handler, state, };
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
			if (!(caught instanceof ClientValidationError)) throw caught;
			expect(caught.name,).toBe("ClientValidationError",);
			expect(caught.code,).toBe("validation_failed",);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("uses project details when a live app manifest omits its project type", async () => {
		const requests: string[] = [];
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/TEST/app-manifest") {
				sendJson(res, { homepageSections: [], },);
				return;
			}
			if (request === "GET /public/api/projects/TEST/") {
				sendJson(res, {
					projectKey: "TEST",
					name: "Live app instance",
					projectAppType: "APP_INSTANCE",
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			const error = await resource.saveInstanceManifest({ homepageSections: [], },).catch(
				(caught: unknown,) => caught,
			);
			expect(error,).toBeInstanceOf(ClientValidationError,);
			if (!(error instanceof ClientValidationError)) throw error;
			expect(error.details,).toEqual({ projectAppType: "APP_INSTANCE", projectKey: "TEST", },);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/",
		],);
	});

	it("deletes an app instance project after verifying its manifest type", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/APP%20INSTANCE/app-manifest"
			) {
				sendJson(res, { projectAppType: "APP_INSTANCE", homepageSections: [], },);
				return;
			}
			if (req.method === "DELETE" && req.url === "/public/api/projects/APP%20INSTANCE") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.deleteInstance("APP INSTANCE",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/APP%20INSTANCE/app-manifest",
			"DELETE /public/api/projects/APP%20INSTANCE",
		],);
	});

	it("rejects replacement between the manifest probe and the incarnation-bound DELETE", async () => {
		const requests: string[] = [];
		const expectedHash = projectIncarnationHash("APP INSTANCE", {
			projectKey: "APP INSTANCE",
			creationTag: { versionNumber: 1, lastModifiedOn: 1_700_000_000_000, },
		},)!;
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/APP%20INSTANCE/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", homepageSections: [], },);
				return;
			}
			if (request === "GET /public/api/projects/APP%20INSTANCE/") {
				sendJson(res, {
					projectKey: "APP INSTANCE",
					projectAppType: "APP_INSTANCE",
					creationTag: { versionNumber: 1, lastModifiedOn: 1_800_000_000_000, },
				},);
				return;
			}
			if (request === "DELETE /public/api/projects/APP%20INSTANCE") {
				res.statusCode = 500;
				res.end("unexpected DELETE",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			const error = await resource.deleteInstance("APP INSTANCE", {
				expectedProjectIncarnationHash: expectedHash,
			},).catch((caught: unknown,) => caught);
			expect(error,).toBeInstanceOf(ClientValidationError,);
			if (!(error instanceof ClientValidationError)) throw error;
			expect(error.message,).toContain("not the project incarnation authorized for deletion",);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/APP%20INSTANCE/app-manifest",
			"GET /public/api/projects/APP%20INSTANCE/",
		],);
	});

	it("deletes a live-shape app instance whose manifest omits projectAppType", async () => {
		const requests: string[] = [];
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/LIVE%20INSTANCE/app-manifest") {
				sendJson(res, { homepageSections: [], },);
				return;
			}
			if (request === "GET /public/api/projects/LIVE%20INSTANCE/") {
				sendJson(res, {
					projectKey: "LIVE INSTANCE",
					name: "Live app instance",
					projectAppType: "APP_INSTANCE",
				},);
				return;
			}
			if (request === "DELETE /public/api/projects/LIVE%20INSTANCE") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			await expect(resource.deleteInstance("LIVE INSTANCE",),).resolves.toBeUndefined();
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/LIVE%20INSTANCE/app-manifest",
			"GET /public/api/projects/LIVE%20INSTANCE/",
			"DELETE /public/api/projects/LIVE%20INSTANCE",
		],);
	});

	it("rejects deleting a non-app-instance project without sending a DELETE", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/ORDINARY%20PROJECT/app-manifest"
			) {
				sendJson(res, { projectAppType: "APP_TEMPLATE", homepageSections: [], },);
				return;
			}
			if (req.method === "DELETE" && req.url === "/public/api/projects/ORDINARY%20PROJECT") {
				res.statusCode = 500;
				res.end("unexpected DELETE",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			const error = await resource.deleteInstance("ORDINARY PROJECT",).catch((caught: unknown,) =>
				caught
			);
			expect(error,).toBeInstanceOf(ClientValidationError,);
			if (!(error instanceof ClientValidationError)) throw error;
			expect(error.code,).toBe("validation_failed",);
			expect(error.details,).toEqual({
				projectAppType: "APP_TEMPLATE",
				projectKey: "ORDINARY PROJECT",
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/ORDINARY%20PROJECT/app-manifest",
		],);
	});

	it("rejects deleting an ordinary project when the manifest probe returns DSS validation", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/ORDINARY%20PROJECT/app-manifest"
			) {
				sendJson(res, { message: "Project ORDINARY PROJECT has no app manifest.", }, 400,);
				return;
			}
			if (req.method === "DELETE" && req.url === "/public/api/projects/ORDINARY%20PROJECT") {
				res.statusCode = 500;
				res.end("unexpected DELETE",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			const error = await resource.deleteInstance("ORDINARY PROJECT",).catch((caught: unknown,) =>
				caught
			);
			expect(error,).toBeInstanceOf(ClientValidationError,);
			if (!(error instanceof ClientValidationError)) throw error;
			expect(error.code,).toBe("validation_failed",);
			expect(error.details,).toEqual({
				projectAppType: null,
				projectKey: "ORDINARY PROJECT",
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/ORDINARY%20PROJECT/app-manifest",
		],);
	});

	it("rethrows transient manifest probe errors when deleting an app instance", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (
				req.method === "GET"
				&& req.url === "/public/api/projects/APP%20INSTANCE/app-manifest"
			) {
				sendJson(res, { message: "DSS is temporarily unavailable.", }, 500,);
				return;
			}
			if (req.method === "DELETE" && req.url === "/public/api/projects/APP%20INSTANCE") {
				res.statusCode = 500;
				res.end("unexpected DELETE",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(
				new DataikuClient({
					url,
					apiKey: "test-key",
					projectKey: "TEST",
					retryMaxAttempts: 1,
				},),
			);
			const error = await resource.deleteInstance("APP INSTANCE",).catch((caught: unknown,) => caught);
			expect(error,).toBeInstanceOf(DataikuError,);
			expect(error,).not.toBeInstanceOf(ClientValidationError,);
			if (!(error instanceof DataikuError)) throw error;
			expect(error.status,).toBe(500,);
			expect(error.category,).toBe("transient",);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/APP%20INSTANCE/app-manifest",
		],);
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
	it("validates manifest references and reports every malformed or missing value deterministically", async () => {
		const requests: string[] = [];
		const manifest = {
			homepageSections: [{
				tiles: [
					{ type: "SCENARIO_RUN", scenarioId: "SCENARIO_OK", },
					{ type: "SCENARIO_RUN", scenarioId: "scenario-missing", },
					{ type: "SCENARIO_RUN", scenarioId: ["not-singular",], },
					{ type: "DOWNLOAD_FILE", managedFolderId: "folder-ok", },
					{ type: "DOWNLOAD_FILE", folderId: "folder-missing", },
					{ type: "DOWNLOAD_FILE", folderId: ["not-singular",], },
					{
						type: "RUNTIME_FORM",
						params: [
							{ name: "localVar", type: "STRING", },
							{ name: "standardVar", type: "STRING", },
							{ name: "missingVar", type: "STRING", },
						],
					},
					{ type: "CUSTOM_FORM", config: { scenarioId: "display-only", folderId: "opaque", }, },
				],
			},],
			unknownGovernedField: { untouched: true, },
		};

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/TEST/scenarios/") {
				sendJson(res, [{ id: "SCENARIO_OK", name: "Known scenario", },],);
				return;
			}
			if (request === "GET /public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder-ok", name: "Known folder", },],);
				return;
			}
			if (request === "GET /public/api/projects/TEST/variables/") {
				sendJson(res, {
					standard: { standardVar: 1, },
					local: { localVar: 2, },
				},);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const result = await new ApplicationsResource(createClient(url,),)
				.validateAppManifest(manifest,);
			expect(result.valid,).toBe(false,);
			expect(result.projectKey,).toBe("TEST",);
			expect(result.manifestHash,).toMatch(/^[a-f0-9]{64}$/,);
			expect(result.checks,).toEqual([
				{ kind: "scenario", status: "failed", checked: 2, malformed: 1, missing: 1, },
				{ kind: "folder", status: "failed", checked: 2, malformed: 1, missing: 1, },
				{ kind: "variable", status: "failed", checked: 3, malformed: 0, missing: 1, },
			],);
			expect(result.errors.map(({ code, path, },) => ({ code, path, })),).toEqual([
				{
					code: "MISSING_SCENARIO",
					path: '$["homepageSections"][0]["tiles"][1]["scenarioId"]',
				},
				{
					code: "INVALID_REFERENCE_VALUE",
					path: '$["homepageSections"][0]["tiles"][2]["scenarioId"]',
				},
				{
					code: "MISSING_FOLDER",
					path: '$["homepageSections"][0]["tiles"][4]["folderId"]',
				},
				{
					code: "INVALID_REFERENCE_VALUE",
					path: '$["homepageSections"][0]["tiles"][5]["folderId"]',
				},
				{
					code: "MISSING_VARIABLE",
					path: '$["homepageSections"][0]["tiles"][6]["params"][2]["name"]',
				},
			],);
			expect(result.references.map(({ kind, path, value, exists, },) => ({
				kind,
				path,
				value,
				exists,
			})),).toEqual([
				{
					kind: "scenario",
					path: '$["homepageSections"][0]["tiles"][0]["scenarioId"]',
					value: "SCENARIO_OK",
					exists: true,
				},
				{
					kind: "scenario",
					path: '$["homepageSections"][0]["tiles"][1]["scenarioId"]',
					value: "scenario-missing",
					exists: false,
				},
				{
					kind: "folder",
					path: '$["homepageSections"][0]["tiles"][3]["managedFolderId"]',
					value: "folder-ok",
					exists: true,
				},
				{
					kind: "folder",
					path: '$["homepageSections"][0]["tiles"][4]["folderId"]',
					value: "folder-missing",
					exists: false,
				},
				{
					kind: "variable",
					path: '$["homepageSections"][0]["tiles"][6]["params"][0]["name"]',
					value: "localVar",
					exists: true,
				},
				{
					kind: "variable",
					path: '$["homepageSections"][0]["tiles"][6]["params"][1]["name"]',
					value: "standardVar",
					exists: true,
				},
				{
					kind: "variable",
					path: '$["homepageSections"][0]["tiles"][6]["params"][2]["name"]',
					value: "missingVar",
					exists: false,
				},
			],);
		},);

		expect(requests.sort(),).toEqual([
			"GET /public/api/projects/TEST/managedfolders/",
			"GET /public/api/projects/TEST/scenarios/",
			"GET /public/api/projects/TEST/variables/",
		],);
	});

	it("rejects a non-object manifest without querying reference APIs", async () => {
		const requests: string[] = [];
		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 500;
			res.end("unexpected request",);
		}, async (url,) => {
			const result = await new ApplicationsResource(createClient(url,),)
				.validateAppManifest(["not", "an", "object",],);
			expect(result,).toMatchObject({
				valid: false,
				projectKey: "TEST",
				references: [],
				checks: [
					{ kind: "scenario", status: "skipped", checked: 0, malformed: 0, missing: 0, },
					{ kind: "folder", status: "skipped", checked: 0, malformed: 0, missing: 0, },
					{ kind: "variable", status: "skipped", checked: 0, malformed: 0, missing: 0, },
				],
				errors: [{
					code: "INVALID_MANIFEST_ROOT",
					path: "$",
					message: "App manifest root must be a JSON object.",
				},],
			},);
		},);
		expect(requests,).toEqual([],);
	});

	it("enforces reference value shapes without inspecting custom-form config", async () => {
		const requests: string[] = [];
		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 500;
			res.end("unexpected request",);
		}, async (url,) => {
			const result = await new ApplicationsResource(createClient(url,),).validateAppManifest({
				homepageSections: [{
					tiles: [
						{ type: "SCENARIO_RUN", scenarioId: ["SCENARIO",], },
						{ type: "DOWNLOAD_FILE", folderId: ["FOLDER",], },
						{ type: "RUNTIME_FORM", params: [{ name: ["VARIABLE",], type: "STRING", },], },
						{
							type: "CUSTOM_FORM",
							config: { scenarioId: "opaque", folderId: "opaque", variableName: "opaque", },
						},
					],
				},],
			},);
			expect(result.valid,).toBe(false,);
			expect(result.references,).toEqual([],);
			expect(result.checks,).toEqual([
				{ kind: "scenario", status: "failed", checked: 0, malformed: 1, missing: 0, },
				{ kind: "folder", status: "failed", checked: 0, malformed: 1, missing: 0, },
				{ kind: "variable", status: "failed", checked: 0, malformed: 1, missing: 0, },
			],);
			expect(result.errors.map(({ path, },) => path),).toEqual([
				'$["homepageSections"][0]["tiles"][0]["scenarioId"]',
				'$["homepageSections"][0]["tiles"][1]["folderId"]',
				'$["homepageSections"][0]["tiles"][2]["params"][0]["name"]',
			],);
		},);
		expect(requests,).toEqual([],);
	});

	it("compares every governed manifest field while omitting only project identity", async () => {
		const requests: string[] = [];
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/apps/APP%2FONE/") {
				sendJson(res, {
					projectKey: "TEMPLATE",
					projectAppType: "APP_TEMPLATE",
					id: "release-v2",
					versionTag: "2",
					governed: {
						"é": "template-accent",
						"é": "template-combining",
						"a.b": "template-dotted",
						a: { b: "template-nested", },
						nested: { same: true, },
						alpha: 1,
					},
					sections: [{ title: "same", },],
				},);
				return;
			}
			if (request === "GET /public/api/projects/INSTANCE/app-manifest") {
				sendJson(res, {
					sections: [{ title: "same", },],
					governed: {
						unknown: "live-only",
						alpha: 2,
						nested: { same: true, },
						a: { b: "instance-nested", },
						"a.b": "instance-dotted",
						"é": "instance-combining",
						"é": "instance-accent",
					},
					versionTag: "1",
					id: "release-v1",
					projectAppType: "APP_INSTANCE",
					projectKey: "INSTANCE",
				},);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const result = await new ApplicationsResource(createClient(url,),)
				.compareAppManifest("APP/ONE", "INSTANCE",);
			expect(result.appId,).toBe("APP/ONE",);
			expect(result.projectKey,).toBe("INSTANCE",);
			expect(result.omittedFields,).toEqual(["projectKey", "projectAppType",],);
			expect(result.identical,).toBe(false,);
			expect(result.templateHash,).toMatch(/^[a-f0-9]{64}$/,);
			expect(result.instanceHash,).toMatch(/^[a-f0-9]{64}$/,);
			expect(result.templateHash,).not.toBe(result.instanceHash,);
			expect(result.differences,).toEqual([
				{
					path: '$["governed"]["a"]["b"]',
					kind: "changed",
					template: "template-nested",
					instance: "instance-nested",
				},
				{
					path: '$["governed"]["a.b"]',
					kind: "changed",
					template: "template-dotted",
					instance: "instance-dotted",
				},
				{ path: '$["governed"]["alpha"]', kind: "changed", template: 1, instance: 2, },
				{
					path: '$["governed"]["é"]',
					kind: "changed",
					template: "template-combining",
					instance: "instance-combining",
				},
				{ path: '$["governed"]["unknown"]', kind: "added", instance: "live-only", },
				{
					path: '$["governed"]["é"]',
					kind: "changed",
					template: "template-accent",
					instance: "instance-accent",
				},
				{ path: '$["id"]', kind: "changed", template: "release-v2", instance: "release-v1", },
				{ path: '$["versionTag"]', kind: "changed", template: "2", instance: "1", },
			],);
		},);
		expect(requests.sort(),).toEqual([
			"GET /public/api/apps/APP%2FONE/",
			"GET /public/api/projects/INSTANCE/app-manifest",
		],);
	});

	it("hashes adversarial object keys independently of insertion order", () => {
		const left = { "é": 1, "é": 2, "a.b": 3, a: { b: 4, }, };
		const right = { a: { b: 4, }, "a.b": 3, "é": 2, "é": 1, };
		expect(stableHash(left,),).toBe(stableHash(right,),);
	});
	it("reads authoritative app-manifest version markers as strings only", async () => {
		const manifest = {
			projectAppType: "APP_TEMPLATE",
			version: "2.3.0",
			versionNotes: "Faster dashboard tiles.",
			appVersion: "legacy-value",
			homepageSections: [{ type: "STATIC", title: "Overview", },],
		};
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let result: AppManifestVersionState | undefined;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			result = await resource.getManifestVersion();
		},);

		expect(result,).toEqual({
			projectKey: "TEST",
			projectAppType: "APP_TEMPLATE",
			version: "2.3.0",
			versionNotes: "Faster dashboard tiles.",
			manifestHash: stableHash(manifest,),
		},);
		expect(result,).not.toHaveProperty("appVersion",);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("reports non-string version markers as null and reads app instances", async () => {
		const manifest = { projectAppType: "APP_INSTANCE", version: 3, versionNotes: null, };
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let result: AppManifestVersionState | undefined;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			result = await resource.getManifestVersion();
		},);

		expect(result,).toEqual({
			projectKey: "TEST",
			projectAppType: "APP_INSTANCE",
			version: null,
			versionNotes: null,
			manifestHash: stableHash(manifest,),
		},);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("treats an identical version update as a no-op without a PUT", async () => {
		const manifest = {
			projectAppType: "APP_TEMPLATE",
			version: "1.0.0",
			versionNotes: "",
			homepageSections: [],
		};
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let result: AppManifestVersionWriteResult | undefined;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			result = await resource.setManifestVersion({ version: "1.0.0", versionNotes: "", },);
		},);

		expect(result?.changed,).toBe(false,);
		expect(result?.persisted,).toBe(false,);
		expect(result?.dryRun,).toBe(false,);
		expect(result?.concurrencyControl,).toBe(APP_MANIFEST_CONCURRENCY_CONTROL,);
		expect(result?.before.manifestHash,).toBe(result?.desired.manifestHash,);
		expect(result?.after.manifestHash,).toBe(result?.before.manifestHash,);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("computes a dry-run version change without a PUT", async () => {
		const manifest = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let result: AppManifestVersionWriteResult | undefined;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			result = await resource.setManifestVersion({ version: "2.0.0", dryRun: true, },);
		},);

		expect(result?.dryRun,).toBe(true,);
		expect(result?.concurrencyControl,).toBe(APP_MANIFEST_CONCURRENCY_CONTROL,);
		expect(result?.changed,).toBe(true,);
		expect(result?.persisted,).toBe(false,);
		expect(result?.desired.version,).toBe("2.0.0",);
		expect(result?.after.version,).toBe("1.0.0",);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("rejects a stale expected hash before any PUT", async () => {
		const manifest = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let caught: unknown;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			caught = await resource.setManifestVersion({
				version: "2.0.0",
				expectHash: "0".repeat(64,),
			},).catch((error: unknown,) => error);
		},);

		expect(caught,).toBeInstanceOf(ClientValidationError,);
		if (!(caught instanceof ClientValidationError)) throw caught;
		expect(caught.code,).toBe("validation_failed",);
		expect(caught.details,).toMatchObject({
			projectKey: "TEST",
			expectedHash: "0".repeat(64,),
			actualHash: stableHash(manifest,),
		},);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("rejects a version write on an app-instance manifest without a PUT", async () => {
		const manifest = { projectAppType: "APP_INSTANCE", version: "1.0.0", homepageSections: [], };
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let caught: unknown;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			caught = await resource.setManifestVersion({ version: "2.0.0", },).catch(
				(error: unknown,) => error,
			);
		},);

		expect(caught,).toBeInstanceOf(ClientValidationError,);
		if (!(caught instanceof ClientValidationError)) throw caught;
		expect(caught.code,).toBe("validation_failed",);
		expect(caught.details,).toEqual({ projectAppType: "APP_INSTANCE", projectKey: "TEST", },);
		expect(state.requests,).toEqual(["GET /public/api/projects/TEST/app-manifest",],);
	});

	it("writes only the version fields and verifies the persisted manifest", async () => {
		const manifest = {
			projectAppType: "APP_TEMPLATE",
			version: "1.0.0",
			versionNotes: "Initial release.",
			homepageSections: [{
				type: "STATIC",
				title: "Overview",
				params: [{ name: "keep", },],
			},],
			custom: { nested: { untouched: true, }, },
		};
		const { handler, state, } = createManifestVersionHandler(manifest,);

		let result: AppManifestVersionWriteResult | undefined;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			result = await resource.setManifestVersion({
				version: "2.0.0",
				versionNotes: "Faster tiles.",
				expectHash: stableHash(manifest,).toUpperCase(),
			},);
		},);

		expect(result?.changed,).toBe(true,);
		expect(result?.persisted,).toBe(true,);
		expect(result?.dryRun,).toBe(false,);
		expect(result?.concurrencyControl,).toBe(APP_MANIFEST_CONCURRENCY_CONTROL,);
		expect(result?.desired.version,).toBe("2.0.0",);
		expect(result?.after.version,).toBe("2.0.0",);
		expect(result?.after.versionNotes,).toBe("Faster tiles.",);
		expect(result?.after.manifestHash,).toBe(result?.desired.manifestHash,);
		expect(state.requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
		],);
		expect(state.manifest,).toEqual({
			...manifest,
			version: "2.0.0",
			versionNotes: "Faster tiles.",
		},);
	});

	it("fails validation when the persisted manifest differs from the desired state", async () => {
		const manifest = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		const { handler, state, } = createManifestVersionHandler(manifest, (body,) => {
			body["version"] = "9.9.9";
		},);

		let caught: unknown;
		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			caught = await resource.setManifestVersion({ version: "2.0.0", },).catch(
				(error: unknown,) => error,
			);
		},);

		expect(caught,).toBeInstanceOf(ClientValidationError,);
		if (!(caught instanceof ClientValidationError)) throw caught;
		expect(caught.code,).toBe("validation_failed",);
		expect(caught.details,).toMatchObject({ projectKey: "TEST", after: { version: "9.9.9", }, },);
		expect(state.requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
		],);
	});

	it("reports an indeterminate outcome when the PUT lands but the response is lost", async () => {
		const initial = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		const state: { manifest: Record<string, unknown>; requests: string[]; } = {
			manifest: { ...initial, },
			requests: [],
		};
		let result: AppManifestVersionWriteResult | undefined;
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			state.requests.push(request,);
			if (request === "GET /public/api/projects/TEST/app-manifest") {
				sendJson(res, state.manifest,);
				return;
			}
			if (request === "PUT /public/api/projects/TEST/app-manifest") {
				void readBody(req,).then((body,) => {
					// The server commits the write, then the connection dies
					// before any response bytes reach the client.
					state.manifest = JSON.parse(body,) as Record<string, unknown>;
					res.destroy();
				},);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(
				new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", retryMaxAttempts: 1, },),
			);
			result = await resource.setManifestVersion({ version: "2.0.0", },);
		},);

		expect(state.manifest.version,).toBe("2.0.0",);
		expect(result?.outcome,).toBe("indeterminate",);
		expect(result?.persisted,).toBeNull();
		expect(result?.after,).toBeNull();
		expect(result?.before.version,).toBe("1.0.0",);
		expect(result?.desired.version,).toBe("2.0.0",);
		expect(result?.concurrencyControl,).toBe(APP_MANIFEST_CONCURRENCY_CONTROL,);
		expect(result?.error?.errorStatus,).toBe(0,);
		expect(result?.error?.error,).toBeTruthy();
		expect(state.requests[0],).toBe("GET /public/api/projects/TEST/app-manifest",);
		expect(state.requests.length,).toBeGreaterThanOrEqual(2,);
		expect(
			state.requests.slice(1,).every((request,) =>
				request === "PUT /public/api/projects/TEST/app-manifest"
			),
		).toBe(true,);
	});

	it("reports an indeterminate outcome when the post-PUT verification read fails", async () => {
		const initial = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		let getCount = 0;
		const requests: string[] = [];
		let result: AppManifestVersionWriteResult | undefined;
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/TEST/app-manifest") {
				getCount += 1;
				if (getCount === 2) {
					sendJson(res, { message: "post-write read failure", }, 500,);
					return;
				}
				sendJson(res, initial,);
				return;
			}
			if (request === "PUT /public/api/projects/TEST/app-manifest") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(
				new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", retryMaxAttempts: 1, },),
			);
			result = await resource.setManifestVersion({ version: "2.0.0", },);
		},);

		expect(result?.outcome,).toBe("indeterminate",);
		expect(result?.persisted,).toBeNull();
		expect(result?.after,).toBeNull();
		expect(result?.before.version,).toBe("1.0.0",);
		expect(result?.desired.version,).toBe("2.0.0",);
		expect(result?.error?.errorStatus,).toBe(500,);
		expect(result?.error?.errorCategory,).toBe("transient",);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
		],);
	});

	it("keeps definitive 4xx PUT rejections on the normal DataikuError path", async () => {
		const initial = { projectAppType: "APP_TEMPLATE", version: "1.0.0", homepageSections: [], };
		const requests: string[] = [];
		let caught: unknown;
		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (request === "GET /public/api/projects/TEST/app-manifest") {
				sendJson(res, initial,);
				return;
			}
			if (request === "PUT /public/api/projects/TEST/app-manifest") {
				sendJson(res, { message: "rejected version", }, 422,);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const resource = new ApplicationsResource(
				new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", retryMaxAttempts: 1, },),
			);
			caught = await resource.setManifestVersion({ version: "2.0.0", },).catch(
				(error: unknown,) => error,
			);
		},);

		expect(caught,).toBeInstanceOf(DataikuError,);
		if (!(caught instanceof DataikuError)) throw caught;
		expect(caught.status,).toBe(422,);
		expect(caught.category,).toBe("validation",);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
		],);
	});

	it("validates version update arguments before any request", async () => {
		const { handler, state, } = createManifestVersionHandler({
			projectAppType: "APP_TEMPLATE",
			version: "1.0.0",
			homepageSections: [],
		},);

		await withServer(handler, async (url,) => {
			const resource = new ApplicationsResource(createClient(url,),);
			const cases: AppManifestVersionUpdate[] = [
				{},
				{ version: "   ", },
				{ version: "2.0.0", expectHash: "not-a-hash", },
				{ version: "2.0.0", expectHash: "abcd1234", },
			];
			for (const update of cases) {
				const caught = await resource.setManifestVersion(update,).catch(
					(error: unknown,) => error,
				);
				expect(caught,).toBeInstanceOf(ClientValidationError,);
				if (!(caught instanceof ClientValidationError)) throw caught;
				expect(caught.code,).toBe("validation_failed",);
			}
		},);
		expect(state.requests,).toEqual([],);
	});
	it("exposes a machine-checkable non-atomic concurrency contract without optimistic claims", async () => {
		expect(APP_MANIFEST_CONCURRENCY_CONTROL,).toBe("client-side-non-atomic-stale-read-check",);
		const sourceFiles = [
			new URL("../src/resources/applications.ts", import.meta.url,),
			new URL("../README.md", import.meta.url,),
		];
		for (const file of sourceFiles) {
			const source = await fs.readFile(file, "utf-8",);
			expect(source.toLowerCase(),).not.toContain("optimistic",);
			expect(source,).toContain(APP_MANIFEST_CONCURRENCY_CONTROL,);
			const withoutNegations = source.replace(/non-atomic|not\s+atomic/gi, "",);
			expect(withoutNegations.toLowerCase(),).not.toContain("atomic",);
		}
	});
});
