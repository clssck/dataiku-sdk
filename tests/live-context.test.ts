import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { readCleanupLedger, } from "../src/utils/cleanup-ledger.js";
import { projectIncarnationHash, } from "../src/utils/project-incarnation.js";
import {
	type IncomingMessage,
	readBody,
	sendJson,
	type ServerResponse,
	withCliServer,
} from "./cli/_harness.js";
import type { LiveCaseId, } from "./live-cases.js";
import {
	assertLiveCommandScope,
	initializeLiveManifest,
	LiveCommandError,
	LiveRunContext,
	loadLiveManifest,
	stopLiveCommands,
	writeLiveJson,
} from "./live-context.js";
import { exerciseInfrastructureDisposable, } from "./live-infrastructure-disposable.js";
import { buildStoredZip, writePluginArchive, } from "./live-plugins.js";

const generated = (suffix: string,) => `APP_${suffix.repeat(32,).slice(0, 32,)}`;

/**
 * Minimal stub context driving the PUBLIC exercise entry: check invokes only
 * the selected case's body and collects its error; every wire call is
 * scripted through run/createGlobal/deleteGlobal overrides.
 */
function stubContext(
	selected: string,
	overrides: Record<string, unknown>,
): { ctx: unknown; errors: unknown[]; events: string[]; } {
	const errors: unknown[] = [];
	const events: string[] = [];
	const ctx = {
		iteration: 0,
		runId: "0123456789ABCDEF",
		fixtures: {},
		profiles: ["infrastructure",],
		markerFor: () => "SDK_LIVE_MARK_STUB",
		check: async (id: string, _actions: readonly string[], body: () => Promise<void>,) => {
			if (id !== selected) return;
			events.push(`check:${id}`,);
			try {
				await body();
			} catch (error) {
				errors.push(error,);
			}
		},
		...overrides,
	};
	return { ctx, errors, events, };
}

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
	it("runs the fixed infrastructure probe through the CLI without authorizing arbitrary SQL", async () => {
		const queries: unknown[] = [];
		await withCliServer(async (req, res,) => {
			if (req.url?.startsWith("/public/api/projects/",)) {
				sendJson(res, details(req.url.match(/\/projects\/([^/]+)/,)![1]!, 1,),);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/sql/queries/") {
				const chunks: Buffer[] = [];
				for await (const chunk of req) chunks.push(Buffer.from(chunk,),);
				queries.push(JSON.parse(Buffer.concat(chunks,).toString(),),);
				sendJson(res, {
					queryId: "probe",
					hasResults: true,
					schema: [{ name: "one", type: "int", },],
				},);
				return;
			}
			if (req.url === "/public/api/sql/queries/probe/stream?format=json") {
				sendJson(res, [[1,],],);
				return;
			}
			if (req.url === "/public/api/sql/queries/probe/finish-streaming") {
				res.end();
				return;
			}
			res.writeHead(500,);
			res.end();
		}, async url => {
			await fixture(url, async ctx => {
				const target = ["--connection", "configured-sql", "--project-key", ctx.projectKey,];
				await expect(ctx.run(["sql", "query", "SELECT 1 AS one", ...target,],),).rejects.toThrow();
				ctx.manifest.profiles.push("infrastructure",);
				for (
					const args of [
						["sql", "query", "DROP TABLE important", ...target,],
						["sql", "query", "SELECT 1 AS one", ...target, "--sql", "DELETE FROM important",],
						["sql", "query", "SELECT 1 AS one", ...target, "--sql-file", "query.sql",],
						["sql", "query", "SELECT 1 AS one", ...target, "--dataset", "FOREIGN.table",],
						["sql", "query", "SELECT 1 AS one", "--connection", "configured-sql",],
					]
				) await expect(ctx.run(args,),).rejects.toThrow();
				expect(queries,).toEqual([],);
				const result = await ctx.run<{ rows: number[][]; }>([
					"sql",
					"query",
					"SELECT 1 AS one",
					...target,
				],);
				expect(result.rows,).toEqual([[1,],],);
				expect(queries,).toEqual([{
					query: "SELECT 1 AS one",
					type: "sql",
					connection: "configured-sql",
					projectKey: ctx.projectKey,
				},],);
			},);
		},);
	});
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
				await expect(
					ctx.run([
						"plugin",
						"set-git-remote",
						"owned",
						"--repository",
						"http://127.0.0.1:1234/fixture.git",
					],),
				).rejects.toThrow();
				await expect(
					ctx.pluginFromGit("install", "owned", "http://127.0.0.1:1234/fixture.git", "main",),
				).rejects.toThrow();
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
	it("refuses update and delete of a same-name object whose marker is missing", async () => {
		const updates: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "PUT") updates.push(req.url!,);
			if (req.method === "DELETE") {
				sendJson(res, {},);
				return;
			}
			// GET always returns a foreign object with the reserved name but no
			// lab marker: it must never prove ownership.
			sendJson(res, {
				name: "SDK_LIVE_X_GROUP_GROUP_0",
				type: "NORMAL",
				description: "not the marker",
			},);
		}, async url => {
			await fixture(url, async ctx => {
				ctx.manifest.globals.push({
					kind: "group",
					name: "SDK_LIVE_X_GROUP_GROUP_0",
					state: "bound",
					nonce: "abc123",
					identity: "snapshot-of-our-object",
					createdAt: new Date().toISOString(),
				},);
				await expect(ctx.run(["group", "update", "SDK_LIVE_X_GROUP_GROUP_0", "--data", "{}",],),)
					.rejects.toThrow();
				await expect(ctx.deleteGlobal("group", "SDK_LIVE_X_GROUP_GROUP_0",),).rejects.toThrow();
				expect(updates,).toEqual([],);
				expect(ctx.manifest.globals[0]!.state,).toBe("bound",);
			},);
		},);
	});
	it("never binds after a failed create even when a same-name object exists", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET") {
				// Pre-create absence probe sees a free slot; post-create GET (only
				// reachable after a successful create, which never happens here)
				// would return the object.
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			// The create exits non-zero without a conflict classification (500).
			sendJson(res, { errorType: "Internal", message: "boom", status: 500, }, 500,);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.createGlobal(
						"group",
						"g",
						(
							name,
						) => [
							"group",
							"create",
							"--data",
							JSON.stringify({ name, description: `x ${ctx.markerFor("group", name,)}`, },),
						],
					),
				).rejects.toThrow();
				expect(ctx.manifest.globals[0]!.state,).toBe("unconfirmed",);
			},);
		},);
	});
	it("classifies a real 409 create as conflict and never deletes it", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET") {
				// Absence probe must see a free slot; only the POST is a 409.
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			sendJson(res, { errorType: "Conflict", message: "name already exists", status: 409, }, 409,);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.createGlobal("group", "g", (name,) => [
						"group",
						"create",
						"--data",
						JSON.stringify({ name, description: `x ${ctx.markerFor("group", name,)}`, },),
					],),
				).rejects.toThrow();
				expect(ctx.manifest.globals[0]!.state,).toBe("conflict",);
				// Cleanup never deletes a conflict entry.
				await expect(ctx.cleanup(),).rejects.toThrow();
				expect(ctx.manifest.globals[0]!.state,).toBe("conflict",);
				expect(ctx.manifest.cleanup.errors.join(" ",),).toContain("conflicts with a pre-existing",);
			},);
		},);
	});
	it("aborts only futures harvested from successful receipts of this run", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET") {
				// reserveGlobal's absence probe must see a free slot; code-env
				// targets are POST-only in this fixture.
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			if (
				req.method === "POST" && req.url === "/public/api/admin/code-envs/PYTHON/labenv?wait=false"
			) {
				sendJson(res, { jobId: "receipt-job-1", },);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				// A foreign string read from a list can never be registered.
				await expect(ctx.recordFuture("foreign-future-id",),).rejects.toThrow();
				expect(ctx.manifest.futures,).toEqual([],);
				// The guard requires an exact pending reservation before create.
				const name = await ctx.reserveGlobal("code-env", "env",);
				await ctx.run([
					"code-env",
					"create",
					"PYTHON",
					name,
					"--deployment-mode",
					"DEFAULT",
					"--params",
					"{}",
					"--no-wait",
				],);
				// A fabricated id from another command surface is still rejected.
				await expect(ctx.recordFuture("unrelated-job-2",),).rejects.toThrow();
			},);
		},);
	});
	it("aborts a project-git future by recorded receipt but rejects foreign ids", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/projects/",)) {
				// Mutating project commands re-check the bound incarnation first.
				const key = decodeURIComponent(req.url.split("/",)[4] ?? "",);
				sendJson(res, details(key, 1,),);
				return;
			}
			if (req.method === "POST" && req.url?.includes("/actions/git-refs/reset-all",)) {
				sendJson(res, { jobId: "git-receipt-1", },);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				// The harvest only accepts execute-mode future-producing receipts;
				// registration stays explicit via recordFuture.
				await ctx.run(["project-git", "reset-all-libraries", "--project-key", ctx.projectKey,],);
				await ctx.recordFuture("git-receipt-1",);
				// The alias surface authorizes the recorded receipt id...
				await ctx.run(["project-git", "future-abort", "git-receipt-1",],);
				// ...but a foreign id is refused on either surface.
				await expect(ctx.run(["project-git", "future-abort", "foreign-x",],),).rejects.toThrow();
				await expect(ctx.run(["future", "abort", "foreign-x",],),).rejects.toThrow();
			},);
		},);
	});
	it("never persists password material from JSON payloads into the manifest", async () => {
		const password = "S3cret-p4ss!";
		const realmKey = "realm-key-9f8e7d";
		await withCliServer((req, res,) => {
			if (req.method === "GET") {
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/admin/users") {
				// The error path echoes the credential-bearing payload back.
				sendJson(res, {
					errorType: "Forbidden",
					status: 403,
					message: `denied: ${JSON.stringify({ authRealm: { key: realmKey, }, password, },)}`,
				}, 403,);
				return;
			}
			sendJson(res, { created: true, },);
		}, async url => {
			await fixture(url, async ctx => {
				const name = await ctx.reserveGlobal("user", "user",);
				const payload = {
					login: name,
					sourceType: "LOCAL",
					password,
					authRealm: { key: realmKey, },
					settings: { nested: { password, }, },
				};
				// Create fails (403): the thrown LiveCommandError's message must be
				// scrubbed before it lands in case.error.
				await expect(
					ctx.run(["user", "create", "--data", JSON.stringify(payload,),],),
				).rejects.toThrow();
				await ctx.check("infrastructure.user", ["user.create",], async () => {
					await ctx.run(["user", "create", "--data", JSON.stringify(payload,),],);
				},);
				const persisted = JSON.stringify(ctx.manifest,);
				expect(persisted,).not.toContain(password,);
				expect(persisted,).not.toContain(realmKey,);
				expect(persisted,).toContain("[redacted]",);
				// The report surface stays parseable JSON with the login intact.
				expect(persisted,).toContain(name,);
			},);
		},);
	});
	it("bootstraps a plugin atomically via receipt-gated createPluginDev", async () => {
		let wrote: string | undefined;
		let pluginId = "SDK_LIVE_X_PLUGIN_PLUGIN_0";
		await withCliServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				// The REAL regression this pins: DSS caches loaded metadata, so
				// the listing carries a STALE EMPTY description for the freshly
				// created dev plugin. Ownership must therefore come from the raw
				// plugin.json file, not the cached list meta.
				if (wrote) {
					sendJson(res, [{ id: pluginId, isDev: true, meta: { description: "", }, },],);
				} else sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.includes("/contents/plugin.json",)) {
				// The dev-plugin file route returns the RAW file body.
				res.writeHead(200, { "Content-Type": "application/json", },);
				res.end(wrote ?? "",);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/createDev") {
				const body = await readBody(req,);
				// The CLI wraps the requested pluginId; echo the requested id.
				pluginId = JSON.parse(body,).pluginId;
				sendJson(res, { created: pluginId, creationMode: "EMPTY", },);
				return;
			}
			if (req.method === "POST" && req.url?.endsWith("/plugin.json",)) {
				wrote = await readBody(req,);
				sendJson(res, {},);
				return;
			}
			sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				const entry = await ctx.createPluginDev(
					"plugin",
					(name, marker,) => JSON.stringify({ id: name, meta: { description: marker, }, },),
				);
				// Bound despite the stale empty list description: the raw file's
				// marker was verified, proving the file is the identity source.
				expect(entry.state,).toBe("bound",);
				expect(entry.id,).toBe(entry.name,);
				expect(wrote,).toContain("meta",);
				// Pre-bind writes other than the single plugin.json are refused by
				// the guard; post-bind writes flow through normal bound rules.
			},);
		},);
	});
	it("create-dev failure produces no adoption and no plugin.json write", async () => {
		const requests: string[] = [];
		await withCliServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/createDev") {
				// A real wire failure: the create request itself is rejected.
				sendJson(res, { errorType: "BadRequest", message: "invalid plugin", }, 400,);
				return;
			}
			sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.createPluginDev(
						"plugin",
						(name, marker,) => JSON.stringify({ id: name, meta: { description: marker, }, },),
					),
				).rejects.toThrow();
				// No plugin.json write was ever issued; the entry is never adopted
				// into a deletable state despite the ledger recording it.
				expect(requests.filter(r => r.includes("plugin.json",)),).toEqual([],);
				expect(ctx.manifest.globals[0]!.state,).toBe("unconfirmed",);
				await expect(
					ctx.deleteGlobal("plugin", ctx.manifest.globals[0]!.name,),
				).rejects.toThrow();
			},);
		},);
	});
	it("refuses a plugin.json builder output without the lab marker", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.createPluginDev(
						"plugin",
						(name,) => JSON.stringify({ id: name, meta: { description: "no marker", }, },),
					),
				).rejects.toThrow(/must embed the lab marker/,);
				expect(ctx.manifest.globals[0]!.state,).toBe("unconfirmed",);
			},);
		},);
	});
	it("binds a code-env on the exact server creationTag snapshot", async () => {
		const creationTag = { lastModifiedOn: 1700000000123, version: 1, };
		const foreignTag = { lastModifiedOn: 1900000000456, version: 1, };
		let requestedName = "";
		let created = false;
		let replaced = false;
		await withCliServer((req, res,) => {
			if (req.method === "POST") {
				created = true;
				// The create URL carries the reserved env name; the receipt
				// echoes it (the tightened verifier requires the EXACT name).
				requestedName = decodeURIComponent(req.url!.split("/",)[6]!.split("?",)[0]!,);
				sendJson(res, { envName: requestedName, envLang: "PYTHON", },);
				return;
			}
			if (!created) {
				// reserveGlobal's absence probe must see a free slot.
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			// GET returns the definition body; after bind the lab simulates a
			// foreign recreated env under the same lang/name with a new tag.
			sendJson(res, {
				envName: requestedName,
				envLang: "PYTHON",
				desc: { creationTag: replaced ? foreignTag : creationTag, },
			},);
		}, async url => {
			await fixture(url, async ctx => {
				const entry = await ctx.createGlobal(
					"code-env",
					"env",
					(
						name,
					) => [
						"code-env",
						"create",
						"PYTHON",
						name,
						"--deployment-mode",
						"DEFAULT",
						"--params",
						"{}",
						"--no-wait",
					],
					{
						id: result => {
							const envName = result && typeof result === "object" && "envName" in result
									&& typeof result.envName === "string"
								? result.envName
								: undefined;
							return envName === undefined ? undefined : `PYTHON/${envName}`;
						},
					},
				);
				expect(entry.state,).toBe("bound",);
				expect(entry.identity,).toContain("lastModifiedOn",);
				expect(entry.id,).toBe(`PYTHON/${requestedName}`,);
				// A foreign env recreated under the same name must be refused.
				replaced = true;
				await expect(ctx.deleteGlobal("code-env", entry.id!,),).rejects.toThrow(/identity changed/,);
			},);
		},);
	});
	it("refuses a plugin code-env reservation without a bound owned plugin parent", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.reservePluginCodeEnv("FOREIGN_PLUGIN",),).rejects.toThrow();
				// No reservation was created for a foreign or pending parent.
				expect(ctx.manifest.globals.filter(g => g.kind === "code-env"),).toEqual([],);
			},);
		},);
	});
	it("refuses a plugin code-env reservation when the generated env already exists", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				// The generated env exists: the exact definition answers 200.
				const name = decodeURIComponent(req.url.split("/",)[6]!.split("?",)[0]!,);
				sendJson(res, {
					envName: name,
					envLang: "PYTHON",
					desc: { creationTag: { lastModifiedOn: 1, }, },
				},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				ctx.manifest.globals.push({
					kind: "plugin",
					name: "PLUG",
					id: "PLUG",
					state: "bound",
					nonce: "a".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				},);
				await expect(ctx.reservePluginCodeEnv("PLUG",),).rejects.toThrow();
				// Only the parent remains; no code-env entry was pushed.
				expect(ctx.manifest.globals.filter(g => g.kind === "code-env"),).toEqual([],);
			},);
		},);
	});
	it("reserves and strictly binds the plugin generated code env", async () => {
		let probe = 0;
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/code-envs/") {
				// Authoritative list proves absence for the pending reservation.
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				probe += 1;
				if (probe === 1) {
					// Absence probe: DSS answers 500 "Not a file" for missing envs.
					sendJson(res, { errorType: "InternalError", message: "Not a file", }, 500,);
					return;
				}
				// Bind readback: exact generated identity + plugin deployment mode.
				const name = decodeURIComponent(req.url.split("/",)[6]!.split("?",)[0]!,);
				sendJson(res, {
					envName: name,
					envLang: "PYTHON",
					deploymentMode: "PLUGIN_MANAGED",
					desc: { creationTag: { lastModifiedOn: 2, }, },
				},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				ctx.manifest.globals.push({
					kind: "plugin",
					name: "PLUG",
					id: "PLUG",
					state: "bound",
					nonce: "a".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				},);
				const name = await ctx.reservePluginCodeEnv("PLUG",);
				expect(name,).toBe("plugin_PLUG_managed",);
				expect(ctx.manifest.globals.find(g => g.kind === "code-env")!.state,).toBe("pending",);
				const entry = await ctx.bindGlobal("code-env", name, `PYTHON/${name}`,);
				// Strict identity: the echoed envName/envLang matched the
				// reservation exactly; the plugin fixture asserts its deployment
				// mode after binding.
				expect(entry.state,).toBe("bound",);
				expect(entry.id,).toBe(`PYTHON/${name}`,);
			},);
		},);
	});
	it("refuses a code-env bind when the server echoes a different envName", async () => {
		let probe = 0;
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/code-envs/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				probe += 1;
				if (probe === 1) {
					sendJson(res, { errorType: "InternalError", message: "Not a file", }, 500,);
					return;
				}
				// The readback carries a DIFFERENT env name: never our identity.
				sendJson(res, {
					envName: "SOMETHING_ELSE",
					envLang: "PYTHON",
					desc: { creationTag: { lastModifiedOn: 3, }, },
				},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				const name = await ctx.reserveGlobal("code-env", "env",);
				await expect(ctx.bindGlobal("code-env", name, `PYTHON/${name}`,),).rejects.toThrow();
				expect(ctx.manifest.globals[0]!.state,).toBe("unconfirmed",);
			},);
		},);
	});
	it("treats the exact quoted 400 absence shape as a free reservation slot", async () => {
		let probed = 0;
		let requestedName = "";
		await withCliServer((req, res,) => {
			if (
				req.method === "GET"
				&& /^\/public\/api\/admin\/connections\/[^/]+/.test(req.url ?? "",)
			) {
				probed += 1;
				// DSS quotes the REQUESTED id in the absence message; the fixture
				// must echo the exact requested name, not a hardcoded one.
				requestedName = decodeURIComponent(req.url!.split("/",)[5]!.split("?",)[0]!,);
				sendJson(res, {
					errorType: "BadRequest",
					status: 400,
					message: `Connection '${requestedName}' does not exist`,
				}, 400,);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				// The absence probe must classify the quoted 400 as absence and
				// complete the reservation instead of failing the create.
				const name = await ctx.reserveGlobal("connection", "connection",);
				expect(name,).toBe(requestedName,);
				expect(probed,).toBeGreaterThanOrEqual(1,);
				expect(ctx.manifest.globals[0]!.state,).toBe("pending",);
			},);
		},);
	});
	it("never treats a blanket 400 as absence", async () => {
		let probed = 0;
		await withCliServer((req, res,) => {
			if (req.method === "GET") {
				probed += 1;
				sendJson(res, { errorType: "BadRequest", status: 400, message: "bad payload", }, 400,);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.reserveGlobal("connection", "connection",),).rejects.toThrow();
				expect(ctx.manifest.globals,).toEqual([],);
			},);
		},);
	});
	it("refuses archives whose manifest id does not match the reservation without any install request", async () => {
		const installs: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				// Absence probe: no plugin with the reserved id exists yet.
				sendJson(res, [],);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/installFromZip") {
				installs.push("install",);
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.installPluginFromZip(
						"zipplugin",
						(name, marker,) => writePluginArchive(ctx, `${name}_WRONG`, marker,),
					),
				).rejects.toThrow();
				// The archive was rejected before the install could be attempted.
				expect(installs,).toEqual([],);
			},);
		},);
	});
	it("refuses archives without the reservation marker without any install request", async () => {
		const installs: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/installFromZip") {
				installs.push("install",);
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.installPluginFromZip(
						"zipplugin",
						(name,) => writePluginArchive(ctx, name, "no-marker-here",),
					),
				).rejects.toThrow();
				expect(installs,).toEqual([],);
			},);
		},);
	});
	it("installs a valid archive once and binds the non-dev plugin via the marker", async () => {
		let marker = "";
		let servedId = "";
		let installs = 0;
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				// No plugin before the install; the installed (non-dev) plugin is
				// listed afterwards under its real reserved id with the
				// marker-bearing meta.description the archive carried.
				sendJson(
					res,
					installs === 0 ? [] : [{
						id: servedId,
						isDev: false,
						meta: { description: marker, },
					},],
				);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/installFromZip") {
				installs += 1;
				sendJson(res, { installed: true, },);
				return;
			}
			sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				const name = `SDK_LIVE_${ctx.runId.toUpperCase()}_PLUGIN_ZIPPLUGIN_0`;
				const entry = await ctx.installPluginFromZip("zipplugin", (reserved, value,) => {
					servedId = reserved;
					marker = value;
					return writePluginArchive(ctx, reserved, value,);
				},);
				// Exactly one install request crossed the wire and the non-dev
				// plugin bound through the marker verification.
				expect(installs,).toBe(1,);
				expect(entry.state,).toBe("bound",);
				expect(entry.id,).toBe(name,);
			},);
		},);
	});
	it("denies a direct pending install-from-zip without the private grant", async () => {
		const installs: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/plugins/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/plugins/actions/installFromZip") {
				installs.push("install",);
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await ctx.reserveGlobal("plugin", "direct",);
				await expect(
					ctx.run(
						["plugin", "install-from-zip", "--file", join(ctx.dir, "plugin-x.zip",),],
						{ projectKey: ctx.projectKey, },
					),
				).rejects.toThrow();
				expect(installs,).toEqual([],);
				expect(ctx.manifest.globals[0]!.state,).toBe("pending",);
			},);
		},);
	});
	it("refuses a foreign-source bundle archive without any project-deployer request", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
		}, async url => {
			await fixture(url, async ctx => {
				// Another owned global entry exists, but the archive's source
				// project is foreign: the upload must be refused before any wire
				// request to the deployer.
				await ctx.reserveGlobal("project-deployer-project", "pub",);
				const manifest = new TextEncoder().encode(
					JSON.stringify({ projectKey: "USER_FOREIGN", },),
				);
				const archive = await ctx.writeFile(
					"foreign-bundle.zip",
					buildStoredZip([
						{ name: "export-manifest.json", data: manifest, },
					],),
				);
				await expect(
					ctx.run(["project-deployer", "upload-bundle", archive,], { projectKey: ctx.projectKey, },),
				).rejects.toThrow();
				expect(requests.filter(r => r.startsWith("POST",) && r.includes("project-deployer",)),)
					.toEqual([],);
			},);
		},);
	});
	it("blocks binding when the deployer readback id mismatches the nonce reservation", async () => {
		let created = false;
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/project-deployer/infras/",)) {
				if (!created) {
					sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
					return;
				}
				sendJson(res, { infraBasicInfo: { id: "DIFFERENT_ID", }, },);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/project-deployer/infras") {
				created = true;
				sendJson(res, {},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(
					ctx.createGlobal("project-deployer-infra", "infra", (name,) => [
						"project-deployer",
						"create-infra",
						"--data",
						JSON.stringify({ id: name, },),
					],),
				).rejects.toThrow();
				// The mismatched readback never confers ownership: no adoption,
				// no deletable state.
				expect(ctx.manifest.globals[0]!.state,).toBe("unconfirmed",);
				await expect(
					ctx.deleteGlobal("project-deployer-infra", ctx.manifest.globals[0]!.name,),
				).rejects.toThrow();
			},);
		},);
	});
	it("binds and deletes an owned project-deployer infra through the nonce id", async () => {
		const deletes: string[] = [];
		let created = false;
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/project-deployer/infras/",)) {
				if (!created) {
					sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
					return;
				}
				const id = decodeURIComponent(req.url.split("/",)[5]!.split("?",)[0]!,);
				sendJson(res, { infraBasicInfo: { id, }, },);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/project-deployer/infras") {
				created = true;
				sendJson(res, {},);
				return;
			}
			if (req.method === "DELETE" && req.url?.startsWith("/public/api/project-deployer/infras/",)) {
				deletes.push(req.url,);
				sendJson(res, {},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				const entry = await ctx.createGlobal("project-deployer-infra", "infra", (name,) => [
					"project-deployer",
					"create-infra",
					"--data",
					JSON.stringify({ id: name, },),
				],);
				// The reservation id carries the full nonce and the readback id
				// matched it exactly before binding.
				expect(entry.state,).toBe("bound",);
				expect(entry.name.endsWith(`_${ctx.manifest.globals[0]!.nonce!.toUpperCase()}`,),).toBe(true,);
				await ctx.deleteGlobal("project-deployer-infra", entry.id!,);
				expect(entry.state,).toBe("deleted",);
				expect(deletes.length,).toBe(1,);
				expect(deletes[0],).toContain(encodeURIComponent(entry.name,),);
			},);
		},);
	});
	it("keeps a renamed folder owned when its name begins with the nonce reservation", async () => {
		const mutations: string[] = [];
		const nonce = "a".repeat(32,);
		const folderId = "FOLDER_GEN_1";
		let reserved = "";
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith(`/public/api/project-folders/${folderId}`,)) {
				// The server renamed the folder after creation: the GET name still
				// begins with the full nonce reservation name.
				sendJson(res, { id: folderId, name: `${reserved}_renamed`, },);
				return;
			}
			if (req.method !== "GET" && req.url?.includes("/project-folders/",)) {
				mutations.push(`${req.method} ${req.url}`,);
				sendJson(res, {},);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				reserved = `SDK_LIVE_${ctx.runId.toUpperCase()}_FOLDER_${nonce.toUpperCase()}`;
				ctx.manifest.globals.push({
					kind: "project-folder",
					name: reserved,
					id: folderId,
					state: "bound",
					nonce,
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				},);
				// The renamed suffix remains owned, so the mutation crosses the wire.
				await ctx.run(["project-folder", "settings-set", folderId, "--data", "{}",],);
				expect(mutations.length,).toBe(1,);
			},);
		},);
	});
	it("denies a wrong generated folder id before any wire request", async () => {
		const mutations: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/project-folders/",)) {
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			if (req.method !== "GET" && req.url?.includes("/project-folders/",)) {
				mutations.push(`${req.method} ${req.url}`,);
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await ctx.reserveGlobal("project-folder", "folder",);
				const entry = ctx.manifest.globals[0]!;
				entry.id = "FOLDER_GEN_OK";
				entry.state = "bound";
				entry.identity = "snapshot";
				await expect(
					ctx.run(["project-folder", "settings-set", "FOLDER_GEN_OTHER", "--data", "{}",],),
				).rejects.toThrow();
				expect(mutations,).toEqual([],);
			},);
		},);
	});
	it("denies a move-project with a foreign destination before any wire request", async () => {
		const mutations: string[] = [];
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url?.startsWith("/public/api/project-folders/",)) {
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			if (req.method !== "GET" && req.url?.includes("/project-folders/",)) {
				mutations.push(`${req.method} ${req.url}`,);
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				ctx.manifest.globals.push({
					kind: "project-folder",
					name: `SDK_LIVE_${ctx.runId.toUpperCase()}_FOLDER_${"b".repeat(32,).toUpperCase()}`,
					id: "FOLDER_GEN_SRC",
					state: "bound",
					nonce: "b".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				},);
				await expect(
					ctx.run([
						"project-folder",
						"move-project",
						"FOLDER_GEN_SRC",
						ctx.projectKey,
						"FOREIGN_DESTINATION",
					],),
				).rejects.toThrow();
				expect(mutations,).toEqual([],);
			},);
		},);
	});
	it("surfaces a failed teardown even when the body succeeded", async () => {
		const { ctx, errors, events, } = stubContext("infrastructure.group", {
			createGlobal: async () => {
				events.push("create",);
				return {
					kind: "group",
					name: "GROUP_1",
					id: "GROUP_1",
					state: "bound",
					nonce: "a".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				};
			},
			run: async (argv: string[],) => {
				events.push(`${argv[0]}.${argv[1]}`,);
				return ["GROUP_1",];
			},
			deleteGlobal: async () => {
				events.push("delete",);
				throw new Error("teardown refused",);
			},
		},);
		await exerciseInfrastructureDisposable(ctx as never,);
		// The selected case executed, the delete was attempted, and the failed
		// cleanup surfaced as the case error instead of a silent pass.
		expect(events.includes("check:infrastructure.group",),).toBe(true,);
		expect(events.filter(e => e === "delete").length,).toBe(1,);
		expect(errors.length,).toBe(1,);
		expect(String(errors[0],),).toContain("teardown refused",);
	});
	it("preserves the resource when an async mutation receipt carries no jobId", async () => {
		const { ctx, errors, events, } = stubContext("infrastructure.code-env", {
			reserveGlobal: async () => "python_stub_env_0",
			bindGlobal: async (kind: string, name: string, id?: string,) => {
				events.push(`bind:${kind}`,);
				return {
					kind,
					name,
					id: id ?? name,
					state: "bound",
					nonce: "a".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				};
			},
			recordFuture: async (id: string,) => {
				events.push(`record:${id}`,);
			},
			deleteGlobal: async () => {
				events.push("delete",);
			},
			run: async (argv: string[],) => {
				events.push(`${argv[0]}.${argv[1]}`,);
				if (argv[0] === "code-env" && argv[1] === "get" && argv[3] === "default_v1") {
					return { deploymentMode: "DESIGN_MANAGED", pythonInterpreter: "PYTHON311", };
				}
				if (argv[0] === "code-env" && argv[1] === "create") {
					// The migrated flow reads the create jobId straight from the
					// run receipt, records it, then binds only after the wait.
					return { jobId: "CREATE_JOB_1", };
				}
				if (argv[0] === "future" && argv[1] === "wait") return { success: true, };
				if (argv[0] === "code-env" && argv[1] === "list") {
					return [{ envLang: "PYTHON", envName: "python_stub_env_0", },];
				}
				if (argv[0] === "code-env" && argv[1] === "get") {
					return {
						definitionHash: "hash-1",
						installedPackages: ["pkg1",],
						requestedPackages: ["pkg1",],
					};
				}
				if (argv[0] === "code-env" && argv[1] === "get-definition") return { desc: {}, };
				if (argv[0] === "code-env" && argv[1] === "update-packages") {
					// Known-async mutation whose receipt carries NO jobId.
					return {};
				}
				return {};
			},
		},);
		await exerciseInfrastructureDisposable(ctx as never,);
		// The create receipt carried a jobId (recorded + waited + bound); the
		// async update-packages receipt did not, so the case failed closed with
		// the resource preserved — never a delete mid-build.
		expect(events.includes("record:CREATE_JOB_1",),).toBe(true,);
		expect(events.includes("bind:code-env",),).toBe(true,);
		const messages = errors.map(e => String(e,)).join(" | ",);
		expect(messages,).toContain("carried no jobId",);
		expect(events.includes("delete",),).toBe(false,);
	});
	it("preserves the resource when a failed wait ends in an unknown future state", async () => {
		const { ctx, errors, events, } = stubContext("infrastructure.code-env", {
			reserveGlobal: async () => "python_stub_env_0",
			bindGlobal: async (kind: string, name: string, id?: string,) => {
				events.push(`bind:${kind}`,);
				return {
					kind,
					name,
					id: id ?? name,
					state: "bound",
					nonce: "a".repeat(32,),
					identity: "snapshot",
					createdAt: new Date().toISOString(),
				};
			},
			recordFuture: async (id: string,) => {
				events.push(`record:${id}`,);
			},
			deleteGlobal: async () => {
				events.push("delete",);
			},
			run: async (argv: string[],) => {
				events.push(`${argv[0]}.${argv[1]}`,);
				if (argv[0] === "code-env" && argv[1] === "get" && argv[3] === "default_v1") {
					return { deploymentMode: "DESIGN_MANAGED", pythonInterpreter: "PYTHON311", };
				}
				if (argv[0] === "code-env" && argv[1] === "create") {
					return { jobId: "CREATE_JOB_1", };
				}
				if (argv[0] === "future" && argv[1] === "wait") {
					return { success: false, timedOut: true, state: "RUNNING", };
				}
				if (argv[0] === "future" && argv[1] === "peek") {
					// The dangerous combined shape the schema allows: unknown
					// TOGETHER with alive:false. Unknown must win — it is never
					// terminal, so terminality stays unconfirmed.
					return { unknown: true, alive: false, };
				}
				return {};
			},
		},);
		await exerciseInfrastructureDisposable(ctx as never,);
		// Terminality was never confirmed (unknown ≠ terminal): the create-future
		// failure must preserve the env instead of deleting it mid-build, and no
		// bind may run against a half-built env.
		const messages = errors.map(e => String(e,)).join(" | ",);
		expect(messages,).toContain("preserved for inspection",);
		expect(events.includes("delete",),).toBe(false,);
		expect(events.includes("bind:code-env",),).toBe(false,);
		expect(events.filter(e => e === "future.abort").length,).toBe(1,);
	});
	it("permits a code-env reservation when the definition 500s and the authoritative list lacks it", async () => {
		let requested = "";
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/code-envs/") {
				// Authoritative list: no row carries the requested env.
				sendJson(res, [{ envName: "default_v1", envLang: "PYTHON", },],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				// Absent getDefinition answers 500 "Not a file" on DSS.
				requested = decodeURIComponent(req.url.split("/",)[6]!.split("?",)[0]!,);
				sendJson(res, { errorType: "InternalError", message: "Not a file", }, 500,);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				const name = await ctx.reserveGlobal("code-env", "env",);
				// The absence was proven by the list, so the reservation is legal.
				expect(name,).toBe(requested,);
				expect(ctx.manifest.globals[0]!.state,).toBe("pending",);
			},);
		},);
	});
	it("refuses a code-env reservation when the list already carries the exact lang and name", async () => {
		let requested = "";
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/code-envs/") {
				// The authoritative list contains the exact lang/name pair.
				sendJson(res, [{ envName: requested, envLang: "PYTHON", },],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				requested = decodeURIComponent(req.url.split("/",)[6]!.split("?",)[0]!,);
				sendJson(res, { errorType: "InternalError", message: "Not a file", }, 500,);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.reserveGlobal("code-env", "env",),).rejects.toThrow();
				// The refusal left no phantom ledger entry behind.
				expect(ctx.manifest.globals,).toEqual([],);
			},);
		},);
	});
	it("never lets a malformed code-env list prove absence", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/code-envs/") {
				// Rows missing envName/envLang cannot prove anything.
				sendJson(res, [{},],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/admin/code-envs/",)) {
				sendJson(res, { errorType: "InternalError", message: "Not a file", }, 500,);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				await expect(ctx.reserveGlobal("code-env", "env",),).rejects.toThrow();
				expect(ctx.manifest.globals,).toEqual([],);
			},);
		},);
	});
});
describe("live owned host receipts", () => {
	it("rejects directory targets outside the reservation and inactive runners", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const nonce = "a".repeat(32,);
			const directory = {
				path: `/tmp/sdk_live_${ctx.runId}_${nonce}`,
				nonce,
				runId: ctx.runId,
				projectKey: ctx.projectKey,
				state: "bound",
			};
			for (
				const changes of [{ path: "/tmp/unrelated", }, { nonce: [nonce,], }, {
					projectKey: "FOREIGN",
				},]
			) {
				await writeLiveJson(ctx.manifestPath, {
					...ctx.manifest,
					ownedDirectories: [{ ...directory, ...changes, },],
				},);
				await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(Error,);
			}
			ctx.manifest.projects[0]!.state = "deleted";
			await writeLiveJson(ctx.manifestPath, { ...ctx.manifest, ownedDirectories: [directory,], },);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(Error,);
		},);
	});
	it("rejects unsafe process identities and foreign repository receipts", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const nonce = "a".repeat(32,);
			const directory = {
				path: `/tmp/sdk_live_${ctx.runId}_${nonce}`,
				nonce,
				runId: ctx.runId,
				projectKey: ctx.projectKey,
				state: "bound",
			};
			const daemon = {
				pid: 4242,
				startTime: "1234",
				port: 20001,
				repositories: [`${directory.path}/fixture.git`,],
			};
			for (
				const changes of [{ pid: 1, }, { startTime: 1234, }, { repositories: ["/tmp/foreign.git",], },]
			) {
				await writeLiveJson(ctx.manifestPath, {
					...ctx.manifest,
					ownedDirectories: [{ ...directory, daemon: { ...daemon, ...changes, }, },],
				},);
				await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(Error,);
			}
		},);
	});
	it("rejects unknown or mismatched daemon services and accepts canonical receipts", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const nonce = "a".repeat(32,);
			const directory = {
				path: `/tmp/sdk_live_${ctx.runId}_${nonce}`,
				nonce,
				runId: ctx.runId,
				projectKey: ctx.projectKey,
				state: "bound",
			};
			const base = { pid: 4242, startTime: "1234", port: 20001, };
			const malformed: Array<Record<string, unknown>> = [
				{ ...base, service: "ftp", repositories: [], },
				{ ...base, service: "sse", repositories: [`${directory.path}/fixture.git`,], },
				{ ...base, repositories: [], },
			];
			for (const daemon of malformed) {
				await writeLiveJson(ctx.manifestPath, {
					...ctx.manifest,
					ownedDirectories: [{ ...directory, daemon, },],
				},);
				await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(Error,);
			}
			// Canonical receipts round-trip: Git carries repositories and no
			// service marker; SSE carries an empty repositories array.
			const canonical: Array<{ daemon: Record<string, unknown>; service: "sse" | undefined; }> = [
				{
					daemon: { ...base, repositories: [`${directory.path}/fixture.git`,], },
					service: undefined,
				},
				{ daemon: { ...base, service: "sse", repositories: [], }, service: "sse", },
			];
			for (const { daemon, service, } of canonical) {
				await writeLiveJson(ctx.manifestPath, {
					...ctx.manifest,
					ownedDirectories: [{ ...directory, daemon, },],
				},);
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(manifest.ownedDirectories[0]?.daemon?.service,).toBe(service,);
			}
		},);
	});
});

describe("live app-create reservation safety", () => {
	it("retires the unused reservation when creation refuses before the POST", async () => {
		const requests: string[] = [];
		let boundKey = "";
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "GET" && req.url === "/public/api/projects/") {
				// The accessible list cannot see the target: absence is unverifiable.
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/apps/",)) {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/projects/",)) {
				const key = decodeURIComponent(req.url.split("/",)[4] ?? "",);
				if (key === boundKey) {
					sendJson(res, details(key, 1,),);
					return;
				}
				// The direct probe is forbidden and the project stays hidden.
				sendJson(res, { errorType: "Forbidden", message: "denied", }, 403,);
				return;
			}
			res.writeHead(500,);
			res.end();
		}, async url => {
			await fixture(url, async ctx => {
				boundKey = ctx.projectKey;
				const runId = ctx.manifest.runId.toUpperCase();
				const targetKey = `SDK_LIVE_${runId}_APPTARGET_2`;
				const otherKey = `SDK_LIVE_${runId}_APPTARGET_3`;
				ctx.manifest.projects.push(
					{ key: targetKey, state: "pending", createdAt: new Date().toISOString(), },
					{ key: otherKey, state: "pending", createdAt: new Date().toISOString(), },
				);
				await ctx.save();
				await ctx.check(
					"applications.app.instance-ops" as LiveCaseId,
					["app.create-instance",],
					async () => {
						await ctx.run([
							"app",
							"create-instance",
							"template-app",
							"--data",
							JSON.stringify({ targetProjectKey: targetKey, },),
							"--project-key",
							ctx.projectKey,
						],);
					},
				);
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(manifest.cases.at(-1,)?.status,).toBe("blocked",);
				expect(manifest.commands.at(-1,)?.exitCode,).not.toBe(0,);
				expect(manifest.projects.some(project => project.key === targetKey),).toBe(false,);
				expect(manifest.projects.find(project => project.key === otherKey)?.state,).toBe("pending",);
				expect(manifest.projects.find(project => project.key === ctx.projectKey)?.state,).toBe(
					"bound",
				);
				expect(requests.filter(request => request.startsWith("DELETE",)),).toEqual([],);
				// Exactly one target read: the CLI preflight probe, never a cleanup readback.
				expect(requests.filter(request => request.includes(targetKey,)),).toEqual([
					`GET /public/api/projects/${targetKey}/`,
				],);
			},);
		},);
	});
	it("preserves the pending receipt when creation is indeterminate after the POST", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "POST" && req.url?.startsWith("/public/api/apps/",)) {
				// The POST ran; its outcome is unknown, so cleanup cannot be authorized.
				sendJson(res, { errorType: "InternalError", message: "boom", }, 500,);
				return;
			}
			if (req.method === "GET" && req.url === "/public/api/projects/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/projects/",)) {
				// The direct probe proves absence: the preflight passes and the POST runs.
				sendJson(res, { errorType: "NotFound", message: "missing", }, 404,);
				return;
			}
			res.writeHead(500,);
			res.end();
		}, async url => {
			await fixture(url, async ctx => {
				const targetKey = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_APPINDET_2`;
				ctx.manifest.projects.push({
					key: targetKey,
					state: "pending",
					createdAt: new Date().toISOString(),
				},);
				await ctx.save();
				await expect(
					ctx.run([
						"app",
						"create-instance",
						"template-app",
						"--data",
						JSON.stringify({ targetProjectKey: targetKey, },),
					],),
				).rejects.toThrow();
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(requests.some(request => request.startsWith("POST",)),).toBe(true,);
				expect(manifest.projects.find(project => project.key === targetKey)?.state,).toBe("pending",);
				expect(requests.filter(request => request.startsWith("DELETE",)),).toEqual([],);
			},);
		},);
	});
	it("preserves the pending receipt when the target already exists", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "GET" && req.url === "/public/api/projects/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && req.url?.startsWith("/public/api/projects/",)) {
				const key = decodeURIComponent(req.url.split("/",)[4] ?? "",);
				sendJson(res, details(key, 1,),);
				return;
			}
			res.writeHead(500,);
			res.end();
		}, async url => {
			await fixture(url, async ctx => {
				const targetKey = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_APPEXISTS_2`;
				ctx.manifest.projects.push({
					key: targetKey,
					state: "pending",
					createdAt: new Date().toISOString(),
				},);
				await ctx.save();
				await expect(
					ctx.run([
						"app",
						"create-instance",
						"template-app",
						"--data",
						JSON.stringify({ targetProjectKey: targetKey, },),
					],),
				).rejects.toThrow();
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(manifest.projects.find(project => project.key === targetKey)?.state,).toBe("pending",);
				expect(requests.filter(request => request.startsWith("DELETE",)),).toEqual([],);
			},);
		},);
	});
	it("classifies only the exact no-POST refusal as blocked", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const samples = [
				{
					expected: "blocked",
					result: { code: "target_absence_unverifiable", details: { creationPostAttempted: false, }, },
				},
				{ expected: "failed", result: { code: "target_absence_unverifiable", details: {}, }, },
				{
					expected: "failed",
					result: { code: "target_absence_unverifiable", details: { creationPostAttempted: true, }, },
				},
				{
					expected: "failed",
					result: { code: "validation_failed", details: { creationPostAttempted: false, }, },
				},
				{ expected: "failed", result: null, },
			] as const;
			for (const [index, sample,] of samples.entries()) {
				await ctx.check("applications.app.instance-ops" as LiveCaseId, [], async () => {
					throw new LiveCommandError(`sample ${index}`, 1, sample.result,);
				},);
			}
			const manifest = await loadLiveManifest(ctx.manifestPath,);
			expect(manifest.cases.map(kase => kase.status),).toEqual(
				samples.map(sample => sample.expected),
			);
		},);
	});
	it("never reuses a recorded project key after a reservation was retired", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/") {
				sendJson(res, [],);
				return;
			}
			sendJson(res, {},);
		}, async url => {
			await fixture(url, async ctx => {
				const prefix = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_APPINST_`;
				// Cancellation can shrink the array past an already recorded project suffix.
				const retired = `${prefix}${ctx.manifest.projects.length + 1}`;
				const incarnation = "a".repeat(64,);
				ctx.manifest.projects.push({
					key: retired,
					state: "deleted",
					incarnation,
					createdAt: new Date().toISOString(),
				},);
				await ctx.save();
				const next = await ctx.reserveProject("appinst",);
				expect(next,).not.toBe(retired,);
				expect(ctx.manifest.projects.filter(project => project.key === next),).toHaveLength(1,);
				const retiredEntry = ctx.manifest.projects.find(project => project.key === retired);
				expect(retiredEntry?.state,).toBe("deleted",);
				expect(retiredEntry?.incarnation,).toBe(incarnation,);
				expect(new Set(ctx.manifest.projects.map(project => project.key),).size,).toBe(
					ctx.manifest.projects.length,
				);
				expect(() =>
					assertLiveCommandScope(
						[
							"app",
							"create-instance",
							"template-app",
							"--data",
							JSON.stringify({ targetProjectKey: next, },),
						],
						ctx.manifest,
						ctx.projectKey,
					)
				).not.toThrow();
			},);
		},);
	});
});

/**
 * Bind an APP_INSTANCE project receipt owned by this run: the target of the
 * guarded deletion bookkeeping regressions.
 */
function withOwnedAppInstance(ctx: LiveRunContext,): string {
	const key = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_APPINSTANCE_0`;
	ctx.manifest.projects.push({
		key,
		state: "bound",
		incarnation: projectIncarnationHash(key, details(key, 1,),),
		createdAt: new Date().toISOString(),
	},);
	return key;
}

/**
 * Mock DSS for guarded project deletion: the CLI pre-delete probes (app
 * manifest, project details), the incarnation-bound project DELETE (first
 * attempt optionally failing), and the masked-absence behavior — once a
 * project DELETE succeeded, later reads of that key answer 403 like live DSS,
 * so a re-probe can never confirm the deletion.
 */
function appInstanceDeleteHandler(
	requests: string[],
	options: { readonly firstDeleteStatus?: number; } = {},
) {
	const deletedKeys = new Set<string>();
	let deletes = 0;
	return (req: IncomingMessage, res: ServerResponse,) => {
		requests.push(`${req.method} ${req.url}`,);
		const url = req.url ?? "";
		if (req.method === "GET" && url.endsWith("/app-manifest",)) {
			sendJson(res, { id: "template-app", projectAppType: "APP_INSTANCE", version: "1.0.0", },);
			return;
		}
		if (req.method === "GET" && url.startsWith("/public/api/projects/",)) {
			const key = decodeURIComponent(url.split("/",)[4] ?? "",);
			if (deletedKeys.has(key,)) {
				sendJson(res, { errorType: "Forbidden", message: "Project is not visible", }, 403,);
				return;
			}
			sendJson(res, details(key, 1,),);
			return;
		}
		if (req.method === "DELETE" && url.startsWith("/public/api/projects/",)) {
			deletes += 1;
			const key = decodeURIComponent(url.split("/",)[4] ?? "",);
			if (options.firstDeleteStatus !== undefined && deletes === 1) {
				// The DELETE ran and failed: the target is still alive, so
				// reads keep proving its identity.
				sendJson(res, { errorType: "InternalError", message: "boom", }, options.firstDeleteStatus,);
				return;
			}
			deletedKeys.add(key,);
			sendJson(res, { deleted: true, projectKey: key, },);
			return;
		}
		res.writeHead(500,);
		res.end();
	};
}

describe("live app delete bookkeeping", () => {
	it("books the guarded app deletion from the CLI receipt without re-probing the deleted project", async () => {
		const requests: string[] = [];
		await withCliServer(appInstanceDeleteHandler(requests,), async url => {
			await fixture(url, async ctx => {
				const key = withOwnedAppInstance(ctx,);
				const incarnation = ctx.manifest.projects.find(project => project.key === key)!.incarnation!;
				await ctx.run([
					"app",
					"delete-instance",
					"--project-key",
					key,
					"--expect-project-incarnation",
					incarnation,
				],);
				// The verified receipt alone retires the owned entry and
				// persists the retirement.
				expect(ctx.manifest.projects.find(project => project.key === key)?.state,).toBe("deleted",);
				expect(
					(await loadLiveManifest(ctx.manifestPath,)).projects.find(project => project.key === key)
						?.state,
				).toBe("deleted",);
				// Exactly one guarded DELETE, and no request of any kind after
				// it: a later probe would only ever see the masked 403.
				expect(
					requests.filter(request => request.startsWith("DELETE",))
						.map(request => request.split("?",)[0]!),
				).toEqual([
					`DELETE /public/api/projects/${key}`,
				],);
				const deleteAt = requests.findIndex(request => request.startsWith("DELETE",));
				expect(requests.slice(deleteAt + 1,),).toEqual([],);
				// Cleanup after the booked deletion is a no-op, not a re-probe
				// that would hit the masked 403.
				await ctx.deleteProject(key,);
				expect(requests,).toHaveLength(deleteAt + 1,);
				// The mock stands in for live DSS: once deleted, the project
				// answers only a masked 403, so a re-probe could never confirm
				// the deletion it is asked about.
				expect((await fetch(`${url}/public/api/projects/${key}/`,)).status,).toBe(403,);
			},);
		},);
	});
	it("keeps ownership after a failed guarded delete and deletes only after an explicit identity check", async () => {
		const requests: string[] = [];
		await withCliServer(
			appInstanceDeleteHandler(requests, { firstDeleteStatus: 500, },),
			async url => {
				await fixture(url, async ctx => {
					const key = withOwnedAppInstance(ctx,);
					const incarnation = ctx.manifest.projects.find(project => project.key === key)!.incarnation!;
					await expect(
						ctx.run([
							"app",
							"delete-instance",
							"--project-key",
							key,
							"--expect-project-incarnation",
							incarnation,
						],),
					).rejects.toThrow(LiveCommandError,);
					// A failed delete is ambiguous, never a retirement: the
					// receipt stays bound with its recorded incarnation.
					expect(ctx.manifest.projects.find(project => project.key === key)?.state,).toBe("bound",);
					expect(ctx.manifest.projects.find(project => project.key === key)?.incarnation,)
						.toBe(incarnation,);
					expect(
						(await loadLiveManifest(ctx.manifestPath,)).projects.find(project => project.key === key)
							?.state,
					).toBe("bound",);
					const firstDeleteAt = requests.findIndex(request => request.startsWith("DELETE",));
					expect(firstDeleteAt,).toBeGreaterThanOrEqual(0,);
					expect(requests.slice(firstDeleteAt + 1,),).toEqual([],);
					// The target is still alive: the explicit delete verifies the
					// live identity with a GET, then deletes the actual target.
					await ctx.deleteProject(key,);
					expect(
						requests.filter(request => request.startsWith("DELETE",))
							.map(request => request.split("?",)[0]!),
					).toEqual([
						`DELETE /public/api/projects/${key}`,
						`DELETE /public/api/projects/${key}`,
					],);
					const secondDeleteAt = requests.findIndex(
						(request, index,) => index > firstDeleteAt && request.startsWith("DELETE",),
					);
					expect(
						requests.slice(firstDeleteAt + 1, secondDeleteAt,).filter(request =>
							request.startsWith(`GET /public/api/projects/${key}`,)
						),
					).not.toEqual([],);
					expect(ctx.manifest.projects.find(project => project.key === key)?.state,).toBe("deleted",);
				},);
			},
		);
	});
});
/**
 * Mock DSS for generated-key app creation: the precursor app manifest, project
 * reads (counted per key so a diverging fresh incarnation read can be staged),
 * and the creation POST (inline success with the CLI's generated key, or 500).
 */
function generatedAppHandler(options: {
	readonly post: "inline" | "error" | "rejected";
	readonly requests?: string[];
	readonly divergeAt?: number;
	readonly precursorType?: string;
	readonly manifestId?: string;
	readonly appId?: string;
	/** Runs while the creation POST is in flight, before any response is sent. */
	readonly onPost?: () => void | Promise<void>;
	/** Runs when the precursor project is read, before the response is sent. */
	readonly onPrecursorRead?: () => void;
},) {
	const reads = new Map<string, number>();
	const appId = options.appId ?? "template-app";
	return (req: IncomingMessage, res: ServerResponse,) => {
		options.requests?.push(`${req.method} ${req.url}`,);
		const url = req.url ?? "";
		if (req.method === "GET" && url.endsWith("/app-manifest",)) {
			sendJson(res, {
				id: options.manifestId ?? appId,
				projectAppType: options.precursorType ?? "APP_TEMPLATE",
				version: "1.0.0",
			},);
			return;
		}
		if (req.method === "GET" && url.startsWith("/public/api/projects/",)) {
			const key = decodeURIComponent(url.split("/",)[4] ?? "",);
			if (!key.startsWith("APP_",)) options.onPrecursorRead?.();
			const count = (reads.get(key,) ?? 0) + 1;
			reads.set(key, count,);
			const tag = options.divergeAt !== undefined && count >= options.divergeAt ? 2 : 1;
			sendJson(res, {
				...details(key, tag,),
				projectAppType: key.startsWith("APP_",)
					? "APP_INSTANCE"
					: options.precursorType ?? "APP_TEMPLATE",
			},);
			return;
		}
		if (req.method === "POST" && url.endsWith("/instances",)) {
			void readBody(req,).then(async body => {
				// Observe the pre-POST state (the marker must already be on
				// disk) before this POST is answered.
				await options.onPost?.();
				if (options.post === "error") {
					// The POST ran; its outcome is unknown, so nothing may be adopted.
					sendJson(res, { errorType: "InternalError", message: "boom", }, 500,);
					return;
				}
				if (options.post === "rejected") {
					// A definitive 4xx refusal: DSS rejected the request itself.
					sendJson(res, {
						errorType: "ValidationError",
						message: "invalid instance creation request",
					}, 400,);
					return;
				}
				const target = (JSON.parse(body,) as { targetProjectKey?: string; }).targetProjectKey ?? "";
				sendJson(res, {
					jobId: "future-1",
					hasResult: true,
					targetProjectKey: target,
					result: { projectKey: appId, targetProjectKey: target, },
				},);
			},);
			return;
		}
		res.writeHead(500,);
		res.end();
	};
}

/** Bind an APP_TEMPLATE precursor receipt owned by this run. */
async function withAppPrecursor(ctx: LiveRunContext,): Promise<string> {
	const key = `SDK_LIVE_${ctx.manifest.runId.toUpperCase()}_APPTEMPLATE_0`;
	ctx.manifest.projects.push({
		key,
		state: "bound",
		incarnation: projectIncarnationHash(key, details(key, 1,),),
		createdAt: new Date().toISOString(),
	},);
	await ctx.save();
	return key;
}

/** Ledger entries, treating a never-created ledger file as empty. */
async function cleanupEntries(ctx: LiveRunContext,) {
	try {
		return await readCleanupLedger(join(ctx.dir, "cleanup.jsonl",),);
	} catch (error) {
		if ((error as { code?: string; }).code === "ENOENT") return [];
		throw error;
	}
}

const GENERATED_DIRECT = [
	"app",
	"create-instance",
	"template-app",
	"--data",
	JSON.stringify({ targetProjectName: "generated", },),
	"--wait",
];

describe("live generated app creation", () => {
	it("denies every generated-shape run that lacks the private one-shot authorization", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			await expect(ctx.run(GENERATED_DIRECT,),).rejects.toThrow(
				/private one-shot authorization/,
			);
			await expect(
				ctx.run([
					"app",
					"create-successor-instance",
					"template-app",
					"--from",
					ctx.projectKey,
					"--name",
					"generated",
				],),
			).rejects.toThrow(/private one-shot authorization/,);
			// The strict explicit-key guard is untouched: a caller-named target
			// still needs its reserved pending receipt.
			await expect(
				ctx.run([
					"app",
					"create-instance",
					"template-app",
					"--data",
					JSON.stringify({ targetProjectKey: "NEWPROJ", },),
				],),
			).rejects.toThrow(/requires a reserved project target/,);
			// The private method itself never accepts an explicit target.
			await expect(
				ctx.runGeneratedAppCreation([
					"app",
					"create-instance",
					"template-app",
					"--data",
					JSON.stringify({ targetProjectKey: "NEWPROJ", },),
				], ctx.projectKey,),
			).rejects.toThrow(/refuses an explicit target key/,);
			await expect(
				ctx.runGeneratedAppCreation(
					[...GENERATED_DIRECT, "--record-cleanup", "ledger.jsonl",],
					ctx.projectKey,
				),
			).rejects.toThrow(/refuses caller-supplied --record-cleanup/,);
			await expect(
				ctx.runGeneratedAppCreation([
					"app",
					"create-successor-instance",
					"template-app",
					"--from",
					"FOREIGN",
					"--name",
					"generated",
				], ctx.projectKey,),
			).rejects.toThrow(/--from to name the owned precursor instance/,);
			expect(ctx.manifest.commands,).toEqual([],);
			expect(ctx.manifest.projects[0]?.pendingDependentCreation,).toBeUndefined();
		},);
	});
	it("binds the one-shot authorization to the exact app id and mode", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const grant = { kind: "app-generated", appId: "template-app", mode: "direct", } as const;
			const successor = [
				"app",
				"create-successor-instance",
				"template-app",
				"--from",
				ctx.projectKey,
				"--name",
				"generated",
			];
			expect(() => assertLiveCommandScope(GENERATED_DIRECT, ctx.manifest, ctx.projectKey, grant,))
				.not.toThrow();
			// Without the grant, the same shape is refused.
			expect(() => assertLiveCommandScope(GENERATED_DIRECT, ctx.manifest, ctx.projectKey,))
				.toThrow(/private one-shot authorization/,);
			// The grant cannot ride a different app id...
			expect(() =>
				assertLiveCommandScope(
					["app", "create-instance", "other-app", "--data", JSON.stringify({},),],
					ctx.manifest,
					ctx.projectKey,
					grant,
				)
			).toThrow(/private one-shot authorization/,);
			// ...nor a different creation mode.
			expect(() => assertLiveCommandScope(successor, ctx.manifest, ctx.projectKey, grant,))
				.toThrow(/private one-shot authorization/,);
			// ...and it never retargets an explicit key onto the strict path.
			expect(() =>
				assertLiveCommandScope(
					[
						"app",
						"create-instance",
						"template-app",
						"--data",
						JSON.stringify({ targetProjectKey: "NEWPROJ", },),
					],
					ctx.manifest,
					ctx.projectKey,
					grant,
				)
			).toThrow(/requires a reserved project target/,);
		},);
	});
	it("records no identity and keeps the precursor when the outcome is indeterminate", async () => {
		const requests: string[] = [];
		let handler: ((req: IncomingMessage, res: ServerResponse,) => void) | undefined;
		await withCliServer((req, res,) => handler?.(req, res,), async url => {
			await fixture(url, async ctx => {
				const templateKey = await withAppPrecursor(ctx,);
				let markerAtPost: unknown;
				handler = generatedAppHandler({
					post: "error",
					requests,
					onPost: async () => {
						markerAtPost = (await loadLiveManifest(ctx.manifestPath,)).projects
							.find(project => project.key === templateKey)?.pendingDependentCreation;
					},
				},);
				await expect(ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,),)
					.rejects.toThrow();
				expect(markerAtPost,).toMatchObject({ appId: "template-app", mode: "direct", },);
				expect(
					ctx.manifest.projects.find(project => project.key === templateKey)
						?.pendingDependentCreation?.outcome,
				).toBe("indeterminate",);
				// No future identity was adopted, and cleanup was never authorized.
				expect(ctx.manifest.projects.filter(project => project.key.startsWith("APP_",)),)
					.toEqual([],);
				expect(requests.some(request => request.startsWith("DELETE",)),).toBe(false,);
				expect(await cleanupEntries(ctx,),).toEqual([],);
				// The precursor itself stays until the marker is cleared.
				await expect(ctx.deleteProject(templateKey,),).rejects.toThrow(
					/Unresolved dependent app creation/,
				);
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(
					manifest.projects.find(project => project.key === templateKey)
						?.pendingDependentCreation?.appId,
				).toBe("template-app",);
			},);
		},);
	});
	it("binds the verified child and clears the precursor marker only on terminal binding", async () => {
		const requests: string[] = [];
		await withCliServer(generatedAppHandler({ post: "inline", requests, },), async url => {
			await fixture(url, async ctx => {
				const templateKey = await withAppPrecursor(ctx,);
				const target = await ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,);
				expect(target,).toMatch(/^APP_[0-9A-F]{32}$/,);
				const child = ctx.manifest.projects.find(project => project.key === target);
				expect(child?.state,).toBe("bound",);
				const incarnation = child?.incarnation;
				expect(incarnation,).toBe(projectIncarnationHash(target, details(target, 1,),),);
				expect(
					ctx.manifest.projects.find(project => project.key === templateKey)
						?.pendingDependentCreation,
				).toBeUndefined();
				const entries = await cleanupEntries(ctx,);
				const created = entries.filter(entry =>
					entry.resource === "project" && entry.projectKey === target
				);
				expect(created,).toHaveLength(1,);
				expect(created[0]?.cleanup.argv,).toEqual([
					"project",
					"delete",
					target,
					"--drop-data",
					"--if-exists",
					"--expect-project-incarnation",
					incarnation!,
				],);
				expect(requests.some(request => request.startsWith("DELETE",)),).toBe(false,);
				// The one-shot grant is consumed by that single execution.
				await expect(ctx.run(GENERATED_DIRECT,),).rejects.toThrow(
					/private one-shot authorization/,
				);
				// The bound generated key round-trips through manifest validation.
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(manifest.projects.find(project => project.key === target)?.state,).toBe("bound",);
			},);
		},);
	});
	it("refuses to bind a child whose fresh incarnation does not match the receipt", async () => {
		const requests: string[] = [];
		await withCliServer(
			generatedAppHandler({ post: "inline", divergeAt: 2, requests, },),
			async url => {
				await fixture(url, async ctx => {
					const templateKey = await withAppPrecursor(ctx,);
					await expect(ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,),)
						.rejects.toThrow(/does not match the receipt/,);
					// Nothing was recorded: no child receipt, no cleanup authority.
					expect(ctx.manifest.projects.filter(project => project.key.startsWith("APP_",)),)
						.toEqual([],);
					expect(
						ctx.manifest.projects.find(project => project.key === templateKey)
							?.pendingDependentCreation?.outcome,
					).toBe("indeterminate",);
					expect(await cleanupEntries(ctx,),).toEqual([],);
					expect(requests.some(request => request.startsWith("DELETE",)),).toBe(false,);
				},);
			},
		);
	});
	it("admits generated keys only with a receipt-derived origin anchoring to this run", async () => {
		await fixture("http://127.0.0.1:1", async ctx => {
			const templateKey = await withAppPrecursor(ctx,);
			const incarnation = "b".repeat(64,);
			const child = (key: string, extra: Record<string, unknown> = {},) => ({
				key,
				state: "bound",
				incarnation,
				createdAt: new Date().toISOString(),
				...extra,
			});
			const write = (projects: Array<Record<string, unknown>>,) =>
				writeLiveJson(ctx.manifestPath, { ...ctx.manifest, projects, },);
			const anchor = ctx.manifest.projects.map(project => ({ ...project, }));
			// A foreign APP_ key with a valid shape and incarnation but no
			// receipt-derived origin is rejected outright.
			await write([...anchor, child(generated("A",),),],);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(
				"Invalid owned-project identity",
			);
			// An origin that was never validated in this manifest (foreign or
			// forward reference) is rejected just the same.
			await write([
				...anchor,
				child(generated("A",), { generatedFrom: generated("B",), },),
			],);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(
				"Invalid owned-project identity",
			);
			// A self-reference is not an origin either.
			const selfKey = generated("C",);
			await write([...anchor, child(selfKey, { generatedFrom: selfKey, },),],);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(
				"Invalid owned-project identity",
			);
			// A reserved key must not carry an origin at all.
			await write([
				...anchor,
				child(`SDK_LIVE_${ctx.runId.toUpperCase()}_FOREIGN_0`, { generatedFrom: templateKey, },),
			],);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(
				"Invalid owned-project identity",
			);
			// The receipt-derived chain is accepted: template -> generated
			// instance -> successor instance, each anchored on its precursor.
			const instance = generated("D",);
			const successor = generated("E",);
			await write([
				...anchor,
				child(instance, { generatedFrom: templateKey, },),
				child(successor, { generatedFrom: instance, },),
			],);
			const manifest = await loadLiveManifest(ctx.manifestPath,);
			expect(manifest.projects.find(project => project.key === successor)?.generatedFrom,).toBe(
				instance,
			);
			// A forward reference to a project validated later in the same
			// array is not an origin either: the anchor must already be
			// validated when the entry is read.
			await write([
				...anchor,
				child(instance, { generatedFrom: successor, },),
				child(successor, { generatedFrom: templateKey, },),
			],);
			await expect(loadLiveManifest(ctx.manifestPath,),).rejects.toThrow(
				"Invalid owned-project identity",
			);
		},);
	});
	it("refuses a precursor that is not the template the argv names", async () => {
		await withCliServer(
			generatedAppHandler({ post: "inline", precursorType: "APP_INSTANCE", },),
			async url => {
				await fixture(url, async ctx => {
					const templateKey = await withAppPrecursor(ctx,);
					await expect(ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,),)
						.rejects.toThrow(/requires an APP_TEMPLATE precursor/,);
					expect(
						ctx.manifest.projects.find(project => project.key === templateKey)
							?.pendingDependentCreation,
					).toBeUndefined();
				},);
			},
		);
		await withCliServer(
			generatedAppHandler({ post: "inline", manifestId: "other-app", },),
			async url => {
				await fixture(url, async ctx => {
					const templateKey = await withAppPrecursor(ctx,);
					await expect(ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,),)
						.rejects.toThrow(/does not match the precursor manifest id/,);
					expect(
						ctx.manifest.projects.find(project => project.key === templateKey)
							?.pendingDependentCreation,
					).toBeUndefined();
				},);
			},
		);
	});
	it("clears the precursor marker on a definitive CLI rejection but records nothing", async () => {
		const requests: string[] = [];
		let handler: ((req: IncomingMessage, res: ServerResponse,) => void) | undefined;
		await withCliServer((req, res,) => handler?.(req, res,), async url => {
			await fixture(url, async ctx => {
				const templateKey = await withAppPrecursor(ctx,);
				let markerAtPost: unknown;
				handler = generatedAppHandler({
					post: "rejected",
					requests,
					onPost: async () => {
						// Read the persisted manifest while the POST is in
						// flight: the in-flight marker must already be on disk.
						markerAtPost = (await loadLiveManifest(ctx.manifestPath,)).projects
							.find(project => project.key === templateKey)?.pendingDependentCreation;
					},
				},);
				await expect(ctx.runGeneratedAppCreation(GENERATED_DIRECT, templateKey,),)
					.rejects.toThrow();
				expect(markerAtPost,).toMatchObject({ appId: "template-app", mode: "direct", },);
				// DSS refused the request itself: no project exists, so the
				// precursor is clean again and nothing was adopted.
				expect(
					ctx.manifest.projects.find(project => project.key === templateKey)
						?.pendingDependentCreation,
				).toBeUndefined();
				expect(ctx.manifest.projects.filter(project => project.key.startsWith("APP_",)),)
					.toEqual([],);
				expect(await cleanupEntries(ctx,),).toEqual([],);
				expect(requests.some(request => request.startsWith("DELETE",)),).toBe(false,);
				const manifest = await loadLiveManifest(ctx.manifestPath,);
				expect(
					manifest.projects.find(project => project.key === templateKey)
						?.pendingDependentCreation,
				).toBeUndefined();
			},);
		},);
	});
	it("keeps the entry-time command when a caller retargets its argv mid-flight", async () => {
		const requests: string[] = [];
		let handler: ((req: IncomingMessage, res: ServerResponse,) => void) | undefined;
		await withCliServer((req, res,) => handler?.(req, res,), async url => {
			await fixture(url, async ctx => {
				const templateKey = await withAppPrecursor(ctx,);
				const argv: string[] = [...GENERATED_DIRECT,];
				let retargeted = false;
				handler = generatedAppHandler({
					post: "inline",
					requests,
					onPrecursorRead: () => {
						if (retargeted) return;
						retargeted = true;
						// Mutate the caller-owned array after the method parsed
						// it but before the creation runs: a live alias would
						// now retarget the awarded app id and be refused by the
						// grant binding, while the entry-time snapshot creates
						// the originally authorized app.
						argv[2] = "other-app";
					},
				},);
				const target = await ctx.runGeneratedAppCreation(argv, templateKey,);
				expect(retargeted,).toBe(true,);
				expect(target,).toMatch(/^APP_[0-9A-F]{32}$/,);
				expect(ctx.manifest.projects.find(project => project.key === target)?.state,).toBe("bound",);
				expect(requests,).toContain("POST /public/api/apps/template-app/instances",);
				expect(requests.some(request => request.includes("other-app",)),).toBe(false,);
			},);
		},);
	});
});
