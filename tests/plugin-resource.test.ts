import { describe, expect, it, } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { PluginsResource, } from "../src/resources/plugins.js";
import { writeResponseToFile, } from "../src/utils/response-file.js";

/* ------------------------------------------------------------------ */
/*  Loopback fake DSS                                                  */
/* ------------------------------------------------------------------ */

type Handler = (req: IncomingMessage, res: ServerResponse,) => void | Promise<void>;

function json(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

/**
 * Route table keyed by `METHOD /path`. Values are either a fixed JSON body, a
 * status-code response, or a handler for body-inspecting assertions. Any
 * unmatched route records a failure so tests cannot silently hit the wrong
 * endpoint.
 */
function buildFakeDss() {
	const routes = new Map<string, Handler>();
	const misses: string[] = [];
	const server = createServer(async (req, res,) => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		const key = `${req.method ?? "GET"} ${url.pathname}`;
		const route = routes.get(key,);
		if (!route) {
			misses.push(key,);
			json(res, { reason: "no route", key, }, 404,);
			return;
		}
		await route(req, res,);
	},);
	return {
		routes,
		misses,
		async start(): Promise<string> {
			await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", resolve,));
			const { port, } = server.address() as AddressInfo;
			return `http://127.0.0.1:${String(port,)}`;
		},
		stop(): void {
			server.close();
		},
	};
}

function readBody(req: IncomingMessage,): Promise<Buffer> {
	return new Promise((resolve,) => {
		const chunks: Buffer[] = [];
		req.on("data", (chunk,) => chunks.push(chunk as Buffer,),);
		req.on("end", () => resolve(Buffer.concat(chunks,),),);
	},);
}

function client(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
}

async function withFakeDss(
	setup: (routes: Map<string, Handler>, misses: string[],) => void,
	run: (url: string, plugins: PluginsResource, misses: string[],) => Promise<void>,
): Promise<void> {
	const fake = buildFakeDss();
	setup(fake.routes, fake.misses,);
	const url = await fake.start();
	try {
		await run(url, new PluginsResource(client(url,),), fake.misses,);
		expect(fake.misses,).toEqual([],);
	} finally {
		fake.stop();
	}
}

/* ------------------------------------------------------------------ */
/*  Endpoint-by-endpoint proofs through the real resource              */
/* ------------------------------------------------------------------ */

describe("PluginsResource against a fake DSS (all documented endpoints)", () => {
	it("lists installed plugins", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/", (_req, res,) => {
					json(res, [{ id: "mini-audit", version: "v1.0", isDev: false, },],);
				},);
			},
			async (_url, plugins,) => {
				const list = await plugins.list();
				expect(list,).toEqual([{ id: "mini-audit", version: "v1.0", isDev: false, },],);
			},
		);
	});

	it("installs from zip via multipart with the file part", async () => {
		const dir = mkdtempSync(join(tmpdir(), "plugin-zip-",),);
		const zipPath = join(dir, "p.zip",);
		writeFileSync(zipPath, Buffer.from([0x50, 0x4b, 0x03, 0x04, 0x00, 0x01, 0xff,],),);
		try {
			let sawMultipart = false;
			let sawBytes = false;
			await withFakeDss(
				(routes,) => {
					routes.set("POST /public/api/plugins/actions/installFromZip", async (req, res,) => {
						const body = await readBody(req,);
						sawMultipart = (req.headers["content-type"] ?? "").includes("multipart/form-data",);
						sawBytes = body.includes(Buffer.from([0x50, 0x4b, 0x03, 0x04,],),);
						res.statusCode = 204;
						res.end();
					},);
				},
				async (_url, plugins,) => {
					await plugins.installFromZip(zipPath,);
					expect(sawMultipart,).toBe(true,);
					expect(sawBytes,).toBe(true,);
				},
			);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("installs and updates from store with the documented body", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/actions/installFromStore", async (req, res,) => {
					const body = JSON.parse((await readBody(req,)).toString("utf8",) || "{}",);
					expect(body,).toEqual({ pluginId: "my-plugin", },);
					res.statusCode = 204;
					res.end();
				},);
				routes.set("POST /public/api/plugins/my-plugin/actions/updateFromStore", (_req, res,) => {
					res.statusCode = 204;
					res.end();
				},);
			},
			async (_url, plugins,) => {
				await plugins.installFromStore("my-plugin",);
				await plugins.updateFromStore("my-plugin",);
			},
		);
	});

	it("updates only the selected installed plugin from Git", async () => {
		const installed = [{ id: "my-plugin", version: "1.0.0", }, {
			id: "unrelated",
			version: "1.0.0",
		},];
		await withFakeDss(routes => {
			routes.set("POST /public/api/plugins/my-plugin/actions/updateFromGit", async (req, res,) => {
				const body = JSON.parse((await readBody(req,)).toString("utf8",),);
				if (body.gitRepositoryUrl !== "git@github.com:acme/p.git" || body.gitCheckout !== "release") {
					json(res, { errorType: "InvalidGitSource", }, 400,);
					return;
				}
				installed[0]!.version = "2.0.0";
				res.statusCode = 204;
				res.end();
			},);
			routes.set("GET /public/api/plugins/", (_req, res,) => json(res, installed,),);
		}, async (_url, plugins,) => {
			await plugins.updateFromGit("my-plugin", {
				gitRepositoryUrl: "git@github.com:acme/p.git",
				gitCheckout: "release",
			},);
			expect(await plugins.list(),).toEqual([{ id: "my-plugin", version: "2.0.0", }, {
				id: "unrelated",
				version: "1.0.0",
			},],);
		},);
	});

	it("downloads a dev plugin zip as a binary stream", async () => {
		const zipBytes = Buffer.from([0x50, 0x4b, 0x01, 0x02, 0xde, 0xad, 0xbe, 0xef,],);
		const dir = mkdtempSync(join(tmpdir(), "plugin-dl-",),);
		try {
			await withFakeDss(
				(routes,) => {
					routes.set("GET /public/api/plugins/dev-p/download", (_req, res,) => {
						res.setHeader("Content-Type", "application/zip",);
						res.end(zipBytes,);
					},);
				},
				async (url, plugins,) => {
					const res = await plugins.download("dev-p",);
					const outPath = join(dir, "plugin.zip",);
					const bytes = await writeResponseToFile(outPath, res,);
					expect(bytes,).toBe(zipBytes.length,);
					expect(readFileSync(outPath,).equals(zipBytes,),).toBe(true,);
					void url;
				},
			);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("moves to dev and deletes with the force body", async () => {
		let deleteBody: unknown;
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/actions/moveToDev", (_req, res,) => {
					res.statusCode = 204;
					res.end();
				},);
				routes.set("POST /public/api/plugins/p/actions/delete", async (req, res,) => {
					deleteBody = JSON.parse((await readBody(req,)).toString("utf8",) || "{}",);
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end("{}",);
				},);
			},
			async (_url, plugins,) => {
				await plugins.moveToDev("p",);
				await plugins.delete("p", { force: true, },);
				expect(deleteBody,).toEqual({ force: true, },);
			},
		);
	});

	it("settles the delete future before resolving", async () => {
		const order: string[] = [];
		let polls = 0;
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/actions/delete", (_req, res,) => {
					order.push("post",);
					json(res, { jobId: "job-del", },);
				},);
				routes.set("GET /public/api/futures/job-del", (_req, res,) => {
					polls += 1;
					order.push(`poll${String(polls,)}`,);
					if (polls === 1) {
						json(res, { jobId: "job-del", alive: true, hasResult: false, },);
					} else {
						json(res, { jobId: "job-del", hasResult: true, result: { ok: true, }, },);
					}
				},);
			},
			async (_url, plugins,) => {
				await plugins.delete("p",);
				order.push("resolved",);
			},
		);
		// Fire-and-forget would resolve right after the POST; the call must
		// only resolve once the future reported its result.
		expect(order,).toEqual(["post", "poll1", "poll2", "resolved",],);
	});

	it("does not poll anything when the action receipt carries no future", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/actions/delete", (_req, res,) => {
					json(res, {},);
				},);
			},
			async (_url, plugins,) => {
				// No /futures route is registered: an invented poll would 404
				// (recorded as a miss) instead of passing silently.
				await plugins.delete("p",);
			},
		);
	});

	it("settles install/update/move/reset action futures before resolving", async () => {
		const order: string[] = [];
		const dir = mkdtempSync(join(tmpdir(), "plugin-settle-",),);
		const zipPath = join(dir, "p.zip",);
		writeFileSync(zipPath, Buffer.from([0x50, 0x4b, 0x03, 0x04,],),);
		try {
			await withFakeDss(
				(routes,) => {
					routes.set("POST /public/api/plugins/actions/installFromZip", async (req, res,) => {
						await readBody(req,);
						order.push("install",);
						json(res, { jobId: "job-install", },);
					},);
					routes.set("GET /public/api/futures/job-install", (_req, res,) => {
						order.push("poll-install",);
						json(res, { jobId: "job-install", hasResult: true, result: {}, },);
					},);
					routes.set("POST /public/api/plugins/p/actions/updateFromZip", async (req, res,) => {
						await readBody(req,);
						order.push("update",);
						json(res, { jobId: "job-update", },);
					},);
					routes.set("GET /public/api/futures/job-update", (_req, res,) => {
						order.push("poll-update",);
						json(res, { jobId: "job-update", hasResult: true, result: {}, },);
					},);
					routes.set("POST /public/api/plugins/p/actions/moveToDev", (_req, res,) => {
						order.push("move",);
						json(res, { jobId: "job-move", },);
					},);
					routes.set("GET /public/api/futures/job-move", (_req, res,) => {
						order.push("poll-move",);
						json(res, { jobId: "job-move", hasResult: true, result: {}, },);
					},);
					routes.set("POST /public/api/plugins/p/actions/resetToRemoteHeadState", (_req, res,) => {
						order.push("reset",);
						json(res, { jobId: "job-reset", },);
					},);
					routes.set("GET /public/api/futures/job-reset", (_req, res,) => {
						order.push("poll-reset",);
						json(res, { jobId: "job-reset", hasResult: true, result: {}, },);
					},);
				},
				async (_url, plugins,) => {
					await plugins.installFromZip(zipPath,);
					order.push("install-done",);
					await plugins.updateFromZip("p", zipPath,);
					order.push("update-done",);
					await plugins.moveToDev("p",);
					order.push("move-done",);
					await plugins.resetToRemoteHeadState("p",);
					order.push("reset-done",);
				},
			);
			expect(order,).toEqual([
				"install",
				"poll-install",
				"install-done",
				"update",
				"poll-update",
				"update-done",
				"move",
				"poll-move",
				"move-done",
				"reset",
				"poll-reset",
				"reset-done",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reports a failed action future instead of reporting success", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/actions/delete", (_req, res,) => {
					json(res, { jobId: "job-aborted", },);
				},);
				routes.set("GET /public/api/futures/job-aborted", (_req, res,) => {
					json(res, { jobId: "job-aborted", aborted: true, },);
				},);
			},
			async (_url, plugins,) => {
				await expect(plugins.delete("p",),).rejects.toThrow(/ABORTED/,);
			},
		);
	});

	it("gets and sets settings with projectKey query", async () => {
		let setPath = "";
		let setBody: unknown;
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/settings", (req, res,) => {
					const url = new URL(req.url ?? "/", "http://localhost",);
					expect(url.searchParams.get("projectKey",),).toBe("PROJ",);
					json(res, { config: { a: 1, }, codeEnvName: "py", },);
				},);
				routes.set("POST /public/api/plugins/p/settings", async (req, res,) => {
					setPath = req.url ?? "";
					setBody = JSON.parse((await readBody(req,)).toString("utf8",),);
					res.statusCode = 204;
					res.end();
				},);
			},
			async (_url, plugins,) => {
				const settings = await plugins.getSettings("p", { projectKey: "PROJ", },);
				expect(settings,).toEqual({ config: { a: 1, }, codeEnvName: "py", },);
				await plugins.setSettings("p", settings, { projectKey: "PROJ", },);
				expect(setPath,).toContain("projectKey=PROJ",);
				expect(setBody,).toEqual({ config: { a: 1, }, codeEnvName: "py", },);
			},
		);
	});

	it("creates and updates the plugin code env returning futures", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/code-env/actions/create", async (req, res,) => {
					const body = JSON.parse((await readBody(req,)).toString("utf8",),);
					expect(body,).toEqual({
						deploymentMode: "PLUGIN_MANAGED",
						conda: true,
						pythonInterpreter: "PYTHON36",
					},);
					json(res, { jobId: "future-1", },);
				},);
				routes.set("POST /public/api/plugins/p/code-env/actions/update", (_req, res,) => {
					json(res, { jobId: "future-2", },);
				},);
			},
			async (_url, plugins,) => {
				const created = await plugins.createCodeEnv("p", {
					conda: true,
					pythonInterpreter: "PYTHON36",
				},);
				expect(created.jobId,).toBe("future-1",);
				const updated = await plugins.updateCodeEnv("p",);
				expect(updated.jobId,).toBe("future-2",);
			},
		);
	});

	it("lists usages with missingTypes passthrough", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/actions/listUsages", (_req, res,) => {
					json(res, {
						usages: [{ elementKind: "webapps", objectId: "o1", projectKey: "PROJ", },],
						missingTypes: [{ missingType: "kind.t", objectId: "o2", },],
					},);
				},);
			},
			async (_url, plugins,) => {
				const report = await plugins.listUsages("p",);
				expect((report.usages ?? [])[0]?.elementKind,).toBe("webapps",);
				expect((report.missingTypes ?? [])[0]?.missingType,).toBe("kind.t",);
			},
		);
	});

	it("creates a dev plugin (EMPTY) with null git fields", async () => {
		let body: unknown;
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/actions/createDev", async (req, res,) => {
					body = JSON.parse((await readBody(req,)).toString("utf8",),);
					res.statusCode = 204;
					res.end();
				},);
			},
			async (_url, plugins,) => {
				await plugins.createDev({ pluginId: "dev-p", creationMode: "EMPTY", },);
				expect(body,).toEqual({
					pluginId: "dev-p",
					creationMode: "EMPTY",
					gitRepository: null,
					gitCheckout: null,
					gitSubpath: null,
				},);
			},
		);
	});

	it("manages the dev plugin git remote (get/set/delete)", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/gitRemote", (_req, res,) => {
					json(res, { repositoryUrl: null, },);
				},);
				routes.set("POST /public/api/plugins/p/gitRemote", async (req, res,) => {
					const body = JSON.parse((await readBody(req,)).toString("utf8",),);
					json(res, { repositoryUrl: body.repositoryUrl, },);
				},);
				routes.set("DELETE /public/api/plugins/p/gitRemote", (_req, res,) => {
					res.statusCode = 200;
					res.end();
				},);
			},
			async (_url, plugins,) => {
				expect((await plugins.getGitRemote("p",)).repositoryUrl,).toBeNull();
				const set = await plugins.setGitRemote("p", "git@github.com:acme/p.git",);
				expect(set.repositoryUrl,).toBe("git@github.com:acme/p.git",);
				await plugins.deleteGitRemote("p",);
			},
		);
	});

	it("lists git branches through the documented POST", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/gitBranches", (_req, res,) => {
					json(res, ["main", "feature/x",],);
				},);
			},
			async (_url, plugins,) => {
				expect(await plugins.listGitBranches("p",),).toEqual(["main", "feature/x",],);
			},
		);
	});

	it("runs push/pull/fetch/reset git actions", async () => {
		const seen: string[] = [];
		await withFakeDss(
			(routes,) => {
				for (
					const action of [
						"push",
						"pullRebase",
						"fetch",
						"resetToLocalHeadState",
						"resetToRemoteHeadState",
					]
				) {
					routes.set(`POST /public/api/plugins/p/actions/${action}`, (_req, res,) => {
						seen.push(action,);
						json(res, { success: true, },);
					},);
				}
			},
			async (_url, plugins,) => {
				await plugins.push("p",);
				await plugins.pull("p",);
				await plugins.fetch("p",);
				await plugins.resetToLocalHeadState("p",);
				await plugins.resetToRemoteHeadState("p",);
				expect(seen,).toEqual([
					"push",
					"pullRebase",
					"fetch",
					"resetToLocalHeadState",
					"resetToRemoteHeadState",
				],);
			},
		);
	});

	it("lists contents and reads file details", async () => {
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/contents/", (_req, res,) => {
					json(res, [
						{ name: "a.txt", path: "/a.txt", mimeType: "text/plain", },
						{ name: "lib", path: "/lib", children: [{ name: "b.py", path: "/lib/b.py", },], },
					],);
				},);
				routes.set("GET /public/api/plugins/p/details/python%2Fx.py", (_req, res,) => {
					json(res, { name: "x.py", size: 10, mimeType: "text/x-python", readOnly: false, },);
				},);
				routes.set("GET /public/api/plugins/p/details/python/lib.py", (_req, res,) => {
					json(res, { name: "lib.py", size: 3, },);
				},);
			},
			async (_url, plugins,) => {
				const items = await plugins.listContents("p",);
				expect(items,).toHaveLength(2,);
				const details = await plugins.getFileDetails("p", "python/lib.py",);
				expect(details.size,).toBe(3,);
			},
		);
	});

	it("reads and writes plugin file contents binary-safely", async () => {
		const binary = Uint8Array.from([0x00, 0x01, 0xff, 0xfe, 0x7f,],);
		const notes = "plain text notes\n";
		const queries: string[] = [];
		let uploaded: Buffer = Buffer.alloc(0,);
		await withFakeDss(
			(routes,) => {
				// DSS answers plugin contents reads with the raw file body: no
				// JSON envelope and no honored dataEncoding parameter.
				routes.set("GET /public/api/plugins/p/contents/static/data.bin", (req, res,) => {
					queries.push(new URL(req.url ?? "/", "http://localhost",).search,);
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/octet-stream",);
					res.end(Buffer.from(binary,),);
				},);
				routes.set("GET /public/api/plugins/p/contents/static/notes.txt", (req, res,) => {
					queries.push(new URL(req.url ?? "/", "http://localhost",).search,);
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/plain",);
					res.end(notes,);
				},);
				routes.set("POST /public/api/plugins/p/contents/static/blob.bin", async (req, res,) => {
					uploaded = await readBody(req,);
					res.statusCode = 204;
					res.end();
				},);
			},
			async (_url, plugins,) => {
				const bytes = await plugins.getFileBytes("p", "static/data.bin",);
				expect(Array.from(bytes,),).toEqual(Array.from(binary,),);
				const text = await plugins.getFile("p", "static/notes.txt",);
				expect(text,).toBe(notes,);
				expect(queries,).toEqual(["", "",],);
				await plugins.putFile("p", "static/blob.bin", binary,);
				expect(uploaded.equals(Buffer.from(binary,),),).toBe(true,);
			},
		);
	});

	it("reads a raw JSON plugin manifest verbatim with both readers", async () => {
		const manifest = JSON.stringify({
			id: "p",
			version: "1.0.0",
			metaVersion: 1,
			meta: { label: "Live plugin", description: "SDK_LIVE_MARK_run_nonce", },
		},);
		await withFakeDss(
			(routes,) => {
				routes.set("GET /public/api/plugins/p/contents/plugin.json", (_req, res,) => {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end(manifest,);
				},);
			},
			async (_url, plugins,) => {
				// The old base64-envelope reader would fail here; both readers
				// must return the raw manifest byte-for-byte.
				expect(await plugins.getFile("p", "plugin.json",),).toBe(manifest,);
				const bytes = await plugins.getFileBytes("p", "plugin.json",);
				expect(Buffer.from(bytes,).toString("utf8",),).toBe(manifest,);
			},
		);
	});

	it("deletes contents and adds folders", async () => {
		const seen: string[] = [];
		await withFakeDss(
			(routes,) => {
				routes.set("DELETE /public/api/plugins/p/contents/python/old%20lib/x.py", (_req, res,) => {
					seen.push("DELETE /public/api/plugins/p/contents/python/old%20lib/x.py",);
					res.statusCode = 204;
					res.end();
				},);
				routes.set("POST /public/api/plugins/p/folders/python/my%20lib", (_req, res,) => {
					seen.push("POST /public/api/plugins/p/folders/python/my%20lib",);
					json(res, {},);
				},);
			},
			async (_url, plugins,) => {
				await plugins.deleteFile("p", "python/old lib/x.py",);
				await plugins.addFolder("p", "python/my lib",);
				expect(seen,).toEqual([
					"DELETE /public/api/plugins/p/contents/python/old%20lib/x.py",
					"POST /public/api/plugins/p/folders/python/my%20lib",
				],);
			},
		);
	});

	it("renames and moves contents with documented bodies", async () => {
		const bodies: Array<Record<string, string>> = [];
		await withFakeDss(
			(routes,) => {
				routes.set("POST /public/api/plugins/p/contents-actions/rename", async (req, res,) => {
					bodies.push(JSON.parse((await readBody(req,)).toString("utf8",),),);
					json(res, {},);
				},);
				routes.set("POST /public/api/plugins/p/contents-actions/move", async (req, res,) => {
					bodies.push(JSON.parse((await readBody(req,)).toString("utf8",),),);
					json(res, {},);
				},);
			},
			async (_url, plugins,) => {
				await plugins.rename("p", "python/old.py", "new.py",);
				await plugins.move("p", "python/old.py", "python/mylib",);
				expect(bodies,).toEqual([
					{ oldPath: "/python/old.py", newName: "new.py", },
					{ oldPath: "/python/old.py", newPath: "/python/mylib", },
				],);
			},
		);
	});
});
