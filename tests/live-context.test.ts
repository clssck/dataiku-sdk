import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
import { sendJson, withCliServer, } from "./cli/_harness.js";
import {
	assertLiveCommandScope,
	initializeLiveManifest,
	LiveRunContext,
	loadLiveManifest,
	stopLiveCommands,
} from "./live-context.js";

async function fixture(url: string, body: (ctx: LiveRunContext,) => Promise<void>,) {
	const dir = await fs.mkdtemp(join(tmpdir(), "dss-live-guard-",),);
	try {
		const credentials = { url, apiKey: "offline-unit-key", };
		const manifest = await initializeLiveManifest(
			join(dir, "manifest.json",),
			credentials,
			"tester",
			"managed",
			["core",],
		);
		const key = `SDK_LIVE_${manifest.runId.toUpperCase()}_PRIMARY_0`;
		manifest.projectKey = key;
		manifest.projects.push({
			key,
			state: "bound",
			incarnation: projectIncarnationHash(key, details(key, 1,),),
			createdAt: new Date().toISOString(),
		},);
		const ctx = new LiveRunContext(join(dir, "manifest.json",), manifest, credentials,);
		await ctx.save();
		await body(ctx,);
	} finally {
		await fs.rm(dir, { recursive: true, force: true, },);
	}
}
function details(key: string, tag: number,) {
	return {
		projectKey: key,
		name: "owned lab",
		owner: "tester",
		creationTag: { lastModifiedOn: tag, },
	};
}

describe("live sandbox ownership", () => {
	it("checks lifecycle ownership even for nonexecuting plans", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			expect(() =>
				assertLiveCommandScope(
					["project", "delete", "USER_WORK", "--plan",],
					ctx.manifest,
					ctx.projectKey,
				)
			).toThrow();
			expect(() =>
				assertLiveCommandScope(
					[
						"project",
						"delete",
						ctx.projectKey,
						"--expect-project-incarnation",
						ctx.manifest.projects[0]!.incarnation!,
						"--plan",
					],
					ctx.manifest,
					ctx.projectKey,
				)
			).not.toThrow();
		},);
	});
	it("confines the SQL output-file spelling before launching even a plan", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			await expect(
				ctx.run([
					"sql",
					"query",
					"SELECT 1",
					"--connection",
					"offline",
					"--output-file",
					join(ctx.dir, "..", "outside.json",),
					"--plan",
				],),
			).rejects.toThrow();
			expect(ctx.manifest.commands,).toEqual([],);
		},);
	});
	it("stops an active CLI child and preserves its failure journal without credentials", async () => {
		let started!: () => void;
		const pending = new Promise<void>(resolve => {
			started = resolve;
		},);
		await withCliServer((_req, res,) => {
			res.writeHead(200, { "Content-Type": "application/json", },);
			res.write("{",);
			started();
		}, async url => {
			await fixture(url, async ctx => {
				const result = ctx.run(["project", "get",],).then(() => false, () => true,);
				await pending;
				await stopLiveCommands();
				expect(await result,).toBe(true,);
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(manifest.commands[0]!.exitCode,).not.toBe(0,);
				expect(JSON.stringify(manifest,),).not.toContain(ctx.credentials.apiKey,);
			},);
		},);
	});
	it("rejects foreign projects, global writes, and credential overrides before any request", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(req.method!,);
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.run(["dataset", "delete", "x",], { projectKey: "USER_WORK", },),).rejects
					.toThrow();
				await expect(ctx.run(["meaning", "delete", "x",],),).rejects.toThrow();
				await expect(ctx.run(["project", "list", "--url", url,],),).rejects.toThrow();
				await expect(ctx.run(["project", "delete", ctx.projectKey, "--if-exists",],),).rejects
					.toThrow();
				await expect(ctx.run(["project-git", "push",],),).rejects.toThrow();
				const target = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_TARGET_1`;
				ctx.manifest.projects.push({
					key: target,
					state: "pending",
					createdAt: new Date().toISOString(),
				},);
				await expect(
					ctx.run([
						"project",
						"duplicate",
						ctx.projectKey,
						target,
						"copy",
						"--data",
						JSON.stringify({ targetProjectKey: "USER_WORK", },),
					],),
				).rejects.toThrow();
				expect(requests,).toEqual([],);
			},);
		},);
	});
	it("checks the explicitly selected project incarnation before a write", async () => {
		const methods: string[] = [];
		await withCliServer((req, res,) => {
			methods.push(req.method!,);
			const key = decodeURIComponent(req.url!.split("/",)[4]!,);
			sendJson(res, details(key, 2,),);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.run(["dataset", "delete", "x", "--project-key", ctx.projectKey,],),).rejects
					.toThrow();
				expect(methods,).toEqual(["GET",],);
			},);
		},);
	});
	it("refuses cleanup on another server and on a replaced or unconfirmed project", async () => {
		const methods: string[] = [];
		await withCliServer((req, res,) => {
			methods.push(req.method!,);
			const key = decodeURIComponent(req.url!.split("/",)[4]!,);
			sendJson(res, details(key, 2,),);
		}, async url => {
			await fixture(url, async ctx => {
				expect(() =>
					new LiveRunContext(ctx.manifestPath, ctx.manifest, {
						...ctx.credentials,
						url: "http://127.0.0.1:1",
					},)
				).toThrow();
				await expect(ctx.cleanup(),).rejects.toThrow();
				ctx.manifest.projects[0]!.state = "pending";
				delete ctx.manifest.projects[0]!.incarnation;
				await expect(ctx.cleanup(),).rejects.toThrow();
				expect(methods,).toEqual(["GET", "GET",],);
				expect(ctx.manifest.cleanup.status,).toBe("failed",);
			},);
		},);
	});
	it("resumes guarded cleanup from disk and treats a completed deletion as idempotent", async () => {
		let present = true;
		let deletes = 0;
		await withCliServer((req, res,) => {
			const key = decodeURIComponent(req.url!.split("/",)[4]!.split("?",)[0]!,);
			if (req.method === "DELETE") {
				present = false;
				deletes++;
				sendJson(res, {},);
			} else if (present) sendJson(res, details(key, 1,),);
			else sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				const resumed = new LiveRunContext(
					ctx.manifestPath,
					await loadLiveManifest(ctx.manifestPath,),
					ctx.credentials,
				);
				await resumed.cleanup();
				await resumed.cleanup();
				expect(present,).toBe(false,);
				expect(deletes,).toBe(1,);
				expect((await loadLiveManifest(ctx.manifestPath,)).projects[0]!.state,).toBe("deleted",);
			},);
		},);
	});
	it("does not execute a destructive case outside the selection", async () => {
		const methods: string[] = [];
		await withCliServer((req, res,) => {
			methods.push(req.method!,);
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				const selected = new LiveRunContext(ctx.manifestPath, ctx.manifest, ctx.credentials, "run", [
					"dataset.*",
				],);
				await selected.check(
					"project.lifecycle",
					["project.delete",],
					() => selected.deleteProject(selected.projectKey,),
				);
				expect(methods,).toEqual([],);
				expect(selected.manifest.projects[0]!.state,).toBe("bound",);
			},);
		},);
	});
});
