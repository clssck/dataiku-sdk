import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { projectCommands, } from "../src/cli/commands/project.js";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, DataikuError, } from "../src/errors.js";
import { ProjectsResource, } from "../src/resources/projects.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
import { writeProjectArchive, } from "./cli/_archive-fixtures.js";

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
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		let uploadContentType = "";
		let uploadBody = "";
		let processBody: unknown;
		const requests: string[] = [];
		const landedDetails = {
			projectKey: "TARGET",
			name: "Target Project",
			creationTag: { lastModifiedOn: 1234567, },
		};
		const landedHash = projectIncarnationHash("TARGET", landedDetails,);

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
				if (req.method === "GET" && req.url === "/public/api/projects/TARGET/") {
					sendJson(res, landedDetails,);
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
					requestedProjectKey: "TARGET",
					remapped: false,
					projectIncarnationHash: landedHash,
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toEqual([
			"POST /public/api/projects/import/upload",
			"POST /public/api/projects/import/tmp-import-1/process",
			"GET /public/api/projects/TARGET/",
		],);
		expect(uploadContentType.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(uploadBody,).toContain('name="file"',);
		expect(uploadBody,).toContain('filename="tmp-import.zip"',);
		expect(uploadBody,).toContain("export-manifest.json",);
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
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
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

describe("Project import hardening", () => {
	it("rejects provably unusable archives before any upload", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-invalid-",),);
		const archivePath = path.join(tempDir, "broken.zip",);
		await fs.writeFile(archivePath, "this is not a zip archive",);
		let requests = 0;

		try {
			await withServer(async (_req, _res,) => {
				requests++;
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(resource.importProjectFromArchive(archivePath,),).rejects.toMatchObject({
					code: "validation_failed",
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toBe(0,);
	});

	it("treats a process response without a boolean success field as ambiguous", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-msgs-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const requests: string[] = [];

		try {
			await withServer(async (req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					sendJson(res, { messages: [], },);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toEqual([
			"POST /public/api/projects/import/upload",
			"POST /public/api/projects/import/tmp-import-1/process",
		],);
	});

	it("treats a non-boolean success field as ambiguous", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-msgstr-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					sendJson(res, { success: "yes", },);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("wraps process 5xx as ambiguous while preserving the import id", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-5xx-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const requests: string[] = [];

		try {
			await withServer(async (req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					res.statusCode = 503;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ message: "import server hiccup", },),);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toHaveLength(2,);
	});

	it("wraps transport failures after process as ambiguous while preserving the import id", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-drop-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const requests: string[] = [];

		try {
			await withServer(async (req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					req.socket.destroy();
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requests.length,).toBeGreaterThanOrEqual(2,);
	});

	it("does not label a definitive 4xx process response ambiguous", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-4xx-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					res.statusCode = 400;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ message: "target project key conflict", },),);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				try {
					await resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },);
					expect.unreachable("4xx process response must reject",);
				} catch (error) {
					expect(error,).toBeInstanceOf(DataikuError,);
					expect(error,).toMatchObject({ status: 400, },);
					expect(error instanceof ClientValidationError,).toBe(false,);
				}
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("classifies an empty process 2xx response as ambiguous with the import id", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-emptybody-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					res.statusCode = 200;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				try {
					await resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },);
					expect.unreachable("empty 2xx process response must reject",);
				} catch (error) {
					expect(error,).toBeInstanceOf(ClientValidationError,);
					expect(error,).toMatchObject({
						code: "ambiguous_outcome",
						details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
					},);
				}
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("classifies a non-JSON 2xx process response as ambiguous with the import id", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-nonjson-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/html",);
					res.end("<html>maintenance window</html>",);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				try {
					await resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },);
					expect.unreachable("non-JSON 2xx process response must reject",);
				} catch (error) {
					expect(error,).toBeInstanceOf(ClientValidationError,);
					expect(error,).toMatchObject({
						code: "ambiguous_outcome",
						details: { importId: "tmp-import-1", targetProjectKey: "TARGET", },
					},);
				}
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("reports explicit remapping when the used key differs from the requested key", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-remap-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const landedDetails = {
			projectKey: "ACTUAL",
			name: "Landed Project",
			creationTag: { lastModifiedOn: 999, },
		};
		const landedHash = projectIncarnationHash("ACTUAL", landedDetails,);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					sendJson(res, { success: true, usedProjectKey: "ACTUAL", },);
					return;
				}
				if (req.method === "GET" && req.url === "/public/api/projects/ACTUAL/") {
					sendJson(res, landedDetails,);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "REQUESTED", },),
				).resolves.toEqual({
					success: true,
					usedProjectKey: "ACTUAL",
					importId: "tmp-import-1",
					requestedProjectKey: "REQUESTED",
					remapped: true,
					projectIncarnationHash: landedHash,
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("treats post-import verification 404 as ambiguous", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-miss-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					sendJson(res, { success: true, usedProjectKey: "TARGET", },);
					return;
				}
				if (req.method === "GET" && req.url === "/public/api/projects/TARGET/") {
					res.statusCode = 404;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ message: "project not found", },),);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", usedProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});

	it("treats a landed project without creationTag identity as ambiguous", async () => {
		const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "dataiku-project-import-notag-",),);
		const archivePath = path.join(tempDir, "project.zip",);
		await writeProjectArchive(archivePath, "ARCHIVE_KEY",);

		try {
			await withServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					sendJson(res, { id: "tmp-import-1", },);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					sendJson(res, { success: true, usedProjectKey: "TARGET", },);
					return;
				}
				if (req.method === "GET" && req.url === "/public/api/projects/TARGET/") {
					sendJson(res, { projectKey: "TARGET", name: "No Tag", },);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const resource = new ProjectsResource(createClient(url,),);
				await expect(
					resource.importProjectFromArchive(archivePath, { targetProjectKey: "TARGET", },),
				).rejects.toMatchObject({
					code: "ambiguous_outcome",
					details: { importId: "tmp-import-1", usedProjectKey: "TARGET", },
				},);
			},);
		} finally {
			await fs.rm(tempDir, { recursive: true, force: true, },);
		}
	});
});

describe("Project delete guards", () => {
	const GUARDED_DETAILS = {
		projectKey: "GUARDED",
		name: "Guarded Project",
		creationTag: { lastModifiedOn: 42, },
	};
	const GUARDED_HASH = projectIncarnationHash("GUARDED", GUARDED_DETAILS,);

	it("deletes with a matching incarnation guard", async () => {
		let deletes = 0;
		const requests: string[] = [];
		await withServer(async (req, res,) => {
			const url = req.url ?? "";
			requests.push(`${req.method ?? ""} ${url}`,);
			if (req.method === "GET" && url === "/public/api/projects/GUARDED/") {
				sendJson(res, GUARDED_DETAILS,);
				return;
			}
			if (req.method === "DELETE" && url.startsWith("/public/api/projects/GUARDED",)) {
				deletes++;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"if-exists": true,
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).resolves.toEqual({
				deleted: true,
				projectKey: "GUARDED",
				projectIncarnationHash: GUARDED_HASH,
			},);
		},);

		expect(deletes,).toBe(1,);
		expect(requests,).toEqual([
			"GET /public/api/projects/GUARDED/",
			"DELETE /public/api/projects/GUARDED?clearManagedDatasets=false&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
		],);
	});

	it("refuses an incarnation mismatch before any DELETE", async () => {
		let deletes = 0;
		await withServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/GUARDED/") {
				sendJson(res, {
					...GUARDED_DETAILS,
					creationTag: { lastModifiedOn: 9999, },
				},);
				return;
			}
			if (req.method === "DELETE") {
				deletes++;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).rejects.toMatchObject({
				code: "validation_failed",
				details: {
					projectKey: "GUARDED",
					expectedProjectIncarnationHash: GUARDED_HASH,
				},
			},);
		},);

		expect(deletes,).toBe(0,);
	});

	it("refuses a missing creationTag before any DELETE", async () => {
		let deletes = 0;
		await withServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/GUARDED/") {
				sendJson(res, { projectKey: "GUARDED", name: "No Tag", },);
				return;
			}
			if (req.method === "DELETE") {
				deletes++;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).rejects.toMatchObject({
				code: "validation_failed",
				details: { projectKey: "GUARDED", },
			},);
		},);

		expect(deletes,).toBe(0,);
	});

	it("treats an absent project as already deleted with --if-exists", async () => {
		let deletes = 0;
		await withServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/GUARDED/") {
				res.statusCode = 404;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ message: "project not found", },),);
				return;
			}
			if (req.method === "DELETE") {
				deletes++;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"if-exists": true,
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).resolves.toEqual({
				deleted: false,
				alreadyAbsent: true,
				projectKey: "GUARDED",
			},);
		},);

		expect(deletes,).toBe(0,);
	});

	it("rethrows absence without --if-exists", async () => {
		await withServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/GUARDED/") {
				res.statusCode = 404;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ message: "project not found", },),);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).rejects.toMatchObject({ status: 404, },);
		},);
	});

	it("verifies identity under --dry-run without deleting", async () => {
		let deletes = 0;
		await withServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/GUARDED/") {
				sendJson(res, GUARDED_DETAILS,);
				return;
			}
			if (req.method === "DELETE") {
				deletes++;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["GUARDED",], {
					"dry-run": true,
					"expect-project-incarnation": GUARDED_HASH,
				},),
			).resolves.toEqual({
				deleted: false,
				dryRun: true,
				projectKey: "GUARDED",
				projectIncarnationHash: GUARDED_HASH,
			},);
		},);

		expect(deletes,).toBe(0,);
	});

	it("rejects a malformed incarnation hash", async () => {
		const client = createClient("http://127.0.0.1:1",);
		await expect(
			projectCommands.delete.handler(client, ["GUARDED",], {
				"expect-project-incarnation": "not-a-hash",
			},),
		).rejects.toThrow("64-character lowercase SHA-256",);
		await expect(
			projectCommands.delete.handler(client, ["GUARDED",], {
				"expect-project-incarnation": "not-a-hash",
			},),
		).rejects.toMatchObject({ code: "validation_failed", },);
	});

	it("keeps the direct unguarded delete when no guard flag is supplied", async () => {
		const requests: string[] = [];
		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "DELETE" && (req.url ?? "").startsWith("/public/api/projects/OLD",)) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				projectCommands.delete.handler(client, ["OLD",], {},),
			).resolves.toEqual({ deleted: "OLD", },);
		},);

		expect(requests,).toEqual([
			"DELETE /public/api/projects/OLD?clearManagedDatasets=false&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
		],);
	});
});
