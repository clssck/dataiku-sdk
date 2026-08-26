import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { projectCommands, } from "../src/cli/commands/project.js";
import { DataikuClient, } from "../src/client.js";
import { ProjectsResource, } from "../src/resources/projects.js";

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

describe("ProjectsResource lifecycle", () => {
	it("creates projects and returns the parsed response", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { msg: "Created project NEW_PROJECT", },);
		}, async (url,) => {
			const resource = new ProjectsResource(createClient(url,),);
			await expect(
				resource.createProject("NEW_PROJECT", "New Project", "owner-login", { codeEnv: "py", },),
			).resolves.toEqual({ msg: "Created project NEW_PROJECT", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/projects/",);
		expect(observedBody,).toEqual({
			projectKey: "NEW_PROJECT",
			name: "New Project",
			owner: "owner-login",
			settings: { codeEnv: "py", },
			description: null,
			permissions: [],
			tags: [],
		},);
	});

	it("requires an owner when creating projects through the command handler", () => {
		const client = createClient("http://127.0.0.1:1",);
		expect(
			() => projectCommands.create.handler(client, ["NEW_PROJECT", "New Project",], {},),
		).toThrow("--owner is required",);
	});

	it("deletes projects with source-verified lifecycle query parameters", async () => {
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectsResource(createClient(url,),);
			await expect(resource.deleteProject("OLD/PROJECT", true,),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("DELETE",);
		expect(observedPath,).toBe(
			"/public/api/projects/OLD%2FPROJECT?clearManagedDatasets=true&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
		);
	});

	it("duplicates projects through the duplicate endpoint", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { sourceProjectKey: "SRC/PROJECT", targetProjectKey: "TARGET", },);
		}, async (url,) => {
			const resource = new ProjectsResource(createClient(url,),);
			await expect(
				resource.duplicate("SRC/PROJECT", "TARGET", "Target Project", {
					duplicationMode: "FULL",
					exportGitRepository: false,
					remapping: { connections: [{ source: "old", target: "new", },], },
					targetProjectFolderId: "folder-1",
				},),
			).resolves.toEqual({ sourceProjectKey: "SRC/PROJECT", targetProjectKey: "TARGET", },);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/projects/SRC%2FPROJECT/duplicate/",);
		expect(observedBody,).toEqual({
			targetProjectName: "Target Project",
			targetProjectKey: "TARGET",
			duplicationMode: "FULL",
			exportAnalysisModels: true,
			exportSavedModels: true,
			exportGitRepository: false,
			exportInsightsData: true,
			remapping: { connections: [{ source: "old", target: "new", },], },
			targetProjectFolderId: "folder-1",
		},);
	});

	it("exports project archives by POSTing options and streaming the response to disk", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-export-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		try {
			await withServer(async (req, res,) => {
				observedMethod = req.method ?? "";
				observedPath = req.url ?? "";
				observedBody = JSON.parse(await readBody(req,),);
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/zip",);
				res.end("zip bytes",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.exportArchive("SRC/PROJECT", archivePath, { exportUploads: true, },),
				).resolves.toBeUndefined();
			},);

			await expect(fs.readFile(archivePath, "utf8",),).resolves.toBe("zip bytes",);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/projects/SRC%2FPROJECT/export",);
		expect(observedBody,).toEqual({ exportUploads: true, },);
	});

	it("uploads and processes project import archives", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-",),);
		const archivePath = path.join(tempDir, "source-name.zip",);
		await fs.writeFile(archivePath, "import zip contents",);
		let uploadContentType = "";
		let uploadBody = "";
		let processBody: unknown;
		const requests: string[] = [];

		try {
			await withServer(async (req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.url === "/public/api/projects/import/upload") {
					uploadContentType = req.headers["content-type"] ?? "";
					uploadBody = await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					processBody = JSON.parse(await readBody(req,),);
					sendJson(res, {
						success: true,
						usedProjectKey: "TARGET",
						messages: [],
					},);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).resolves.toEqual({
					success: true,
					usedProjectKey: "TARGET",
					messages: [],
					importId: "tmp-import-1",
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toEqual([
			"POST /public/api/projects/import/upload",
			"POST /public/api/projects/import/tmp-import-1/process",
		],);
		expect(uploadContentType.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(uploadBody,).toContain('name="file"',);
		expect(uploadBody,).toContain('filename="tmp-import.zip"',);
		expect(uploadBody,).toContain("import zip contents",);
		expect(processBody,).toEqual({ targetProjectKey: "TARGET", },);
	});

	it("uses the DSS sentinel payload for default project import settings", async () => {
		let body: unknown;
		await withServer(async (req, res,) => {
			body = JSON.parse(await readBody(req,),);
			sendJson(res, { success: true, usedProjectKey: "ARCHIVE_KEY", },);
		}, async (url,) => {
			const resource = new ProjectsResource(createClient(url,),);
			await expect(resource.processProjectImport("tmp/import",),).resolves.toEqual({
				success: true,
				usedProjectKey: "ARCHIVE_KEY",
			},);
		},);

		expect(body,).toEqual({ _: "_", },);
	});
	it("refuses to process an upload response without a temporary import id", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-empty-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await fs.writeFile(archivePath, "archive",);
		let requests = 0;

		try {
			await withServer(async (_req, res,) => {
				requests++;
				res.statusCode = 204;
				res.end();
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(resource.importProjectFromArchive(archivePath,),).rejects.toMatchObject({
					code: "ambiguous_outcome",
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toBe(1,);
	});

	it("gets and sets project permissions and settings", async () => {
		const requests: Array<{ method: string; url: string; body?: unknown; }> = [];
		const permissions = {
			owner: "owner-login",
			permissions: [{ group: "analysts", readProjectContent: true, },],
		};
		const settings = { settings: { codeEnvs: { python: { mode: "USE_BUILTIN_MODE", }, }, }, };

		await withServer(async (req, res,) => {
			const method = req.method ?? "";
			const url = req.url ?? "";
			if (method === "GET" && url === "/public/api/projects/PERM/permissions") {
				requests.push({ method, url, },);
				sendJson(res, permissions,);
				return;
			}
			if (method === "PUT" && url === "/public/api/projects/PERM/permissions") {
				requests.push({ method, url, body: JSON.parse(await readBody(req,),), },);
				res.statusCode = 204;
				res.end();
				return;
			}
			if (method === "GET" && url === "/public/api/projects/TEST/settings") {
				requests.push({ method, url, },);
				sendJson(res, settings,);
				return;
			}
			if (method === "PUT" && url === "/public/api/projects/TEST/settings") {
				requests.push({ method, url, body: JSON.parse(await readBody(req,),), },);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`${method} ${url}`,);
		}, async (url,) => {
			const resource = new ProjectsResource(createClient(url,),);
			await expect(resource.getPermissions("PERM",),).resolves.toEqual(permissions,);
			await expect(resource.setPermissions("PERM", permissions,),).resolves.toBeUndefined();
			await expect(resource.getSettings(),).resolves.toEqual(settings,);
			await expect(resource.setSettings(undefined, settings,),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{ method: "GET", url: "/public/api/projects/PERM/permissions", },
			{ method: "PUT", url: "/public/api/projects/PERM/permissions", body: permissions, },
			{ method: "GET", url: "/public/api/projects/TEST/settings", },
			{ method: "PUT", url: "/public/api/projects/TEST/settings", body: settings, },
		],);
	});

	it("deep-merges partial settings-set payloads before replacing settings", async () => {
		const requests: Array<{ method: string; url: string; body?: unknown; }> = [];
		const currentSettings = {
			settings: {
				shortDesc: "old",
				codeEnvs: { python: { mode: "USE_BUILTIN_MODE", }, },
				nested: { keep: true, },
			},
			permissions: { owner: "owner-login", },
		};
		const partialSettings = { settings: { shortDesc: "new", }, };

		await withServer(async (req, res,) => {
			const method = req.method ?? "";
			const url = req.url ?? "";
			if (method === "GET" && url === "/public/api/projects/TEST/settings") {
				requests.push({ method, url, },);
				sendJson(res, currentSettings,);
				return;
			}
			if (method === "PUT" && url === "/public/api/projects/TEST/settings") {
				requests.push({ method, url, body: JSON.parse(await readBody(req,),), },);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`${method} ${url}`,);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands["settings-set"].handler(client, [], {
					"project-key": "TEST",
					data: JSON.stringify(partialSettings,),
				},),
			).resolves.toEqual({ updated: true, },);
		},);

		expect(requests,).toEqual([
			{ method: "GET", url: "/public/api/projects/TEST/settings", },
			{
				method: "PUT",
				url: "/public/api/projects/TEST/settings",
				body: {
					settings: {
						shortDesc: "new",
						codeEnvs: { python: { mode: "USE_BUILTIN_MODE", }, },
						nested: { keep: true, },
					},
					permissions: { owner: "owner-login", },
				},
			},
		],);
	});
});
