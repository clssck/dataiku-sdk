import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI managed folder commands", () => {
	it("folder create posts managed folder payload", async () => {
		let requestBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "folder-id", name: "exports", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"folder",
				"create",
				"--name",
				"exports",
				"--type",
				"S3",
				"--connection",
				"s3_conn",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				created: "folder-id",
				resource: "folder",
				id: "folder-id",
				name: "exports",
			},);
		},);

		expect(requestBody,).toMatchObject({
			name: "exports",
			projectKey: "TEST",
			type: "S3",
			params: {
				connection: "s3_conn",
				path: "/${projectKey}/${odbId}",
			},
		},);
	});
	it("folder create resolves managed storage when only name is supplied", async () => {
		let requestBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/admin/connections/") {
				sendJson(res, {
					filesystem_folders: {
						allowWrite: true,
						allowManagedFolders: false,
					},
					"dataiku-managed-storage": {
						allowWrite: true,
						allowManagedFolders: true,
					},
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "folder-id", name: "exports", },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"folder",
				"create",
				"--name",
				"exports",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				created: "folder-id",
				resource: "folder",
			},);
		},);

		expect(requestBody,).toEqual({
			name: "exports",
			projectKey: "TEST",
			type: null,
			params: {
				connection: "dataiku-managed-storage",
				path: "/${projectKey}/${odbId}",
			},
		},);
	});

	it("folder update dry-run previews a deep merge", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder-id", name: "exports", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-id"
			) {
				sendJson(res, {
					id: "folder-id",
					name: "exports",
					type: "Filesystem",
					params: { connection: "filesystem", path: "/dataiku/TEST/exports", },
					tags: ["old",],
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss([
					"folder",
					"update",
					"exports",
					"--data",
					'{"tags":["agent"],"params":{"custom":true}}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as {
				dryRun?: boolean;
				folderId?: string;
				next?: Record<string, unknown>;
			};
			expect(result.dryRun,).toBe(true,);
			expect(result.folderId,).toBe("folder-id",);
			expect(result.next,).toMatchObject({
				id: "folder-id",
				tags: ["agent",],
				params: { connection: "filesystem", path: "/dataiku/TEST/exports", custom: true, },
			},);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/managedfolders/",
			"GET /public/api/projects/TEST/managedfolders/folder-id",
		],);
	});

	it("folder delete supports if-exists and resolved names", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder-id", name: "exports", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-id"
			) {
				sendJson(res, { id: "folder-id", name: "exports", type: "Filesystem", },);
				return;
			}
			if (
				req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-id"
			) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["folder", "delete", "exports", "--if-exists",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(result,).toEqual({ deleted: "folder-id", resource: "folder", },);
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/managedfolders/",
			"GET /public/api/projects/TEST/managedfolders/folder-id",
			"DELETE /public/api/projects/TEST/managedfolders/folder-id",
		],);
	});

	it("folder delete plan is local and uses the managedfolders endpoint", async () => {
		const result = JSON.parse(
			(await dss(["folder", "delete", "folder-id", "--plan", "--project-key", "TEST",], {
				env: { PATH: process.env.PATH, HOME: process.env.HOME, },
			},)).stdout,
		) as Record<string, unknown>;
		expect(result,).toMatchObject({
			plan: true,
			resource: "folder",
			action: "delete",
			method: "DELETE",
			endpoint: "/public/api/projects/TEST/managedfolders/folder-id",
		},);
	});

	it("connection infer rich honors --project-key", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/OTHER/datasets/") {
				sendJson(res, [
					{ type: "Filesystem", managed: true, params: { connection: "filesystem_managed", }, },
				],);
				return;
			}
			expect(url.pathname,).toBe("/public/api/projects/OTHER/managedfolders/",);
			sendJson(res, [],);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["connection", "infer", "--mode", "rich", "--project-key", "OTHER",], {
					env: cliEnv(url,),
				},)).stdout,
			) as unknown[];
			expect(result,).toEqual([
				{ name: "filesystem_managed", types: ["Filesystem",], managed: true, dbSchemas: [], },
			],);
		},);
	});

	it("connection schema and table inspection uses public import endpoints", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/TEST/datasets/tables-import/actions/list-schemas") {
				expect(url.searchParams.get("connectionName",),).toBe("ATHENA_CONN",);
				sendJson(res, ["analytics",],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/datasets/tables-import/actions/list-tables") {
				expect(url.searchParams.get("connectionName",),).toBe("ATHENA_CONN",);
				expect(url.searchParams.get("schemaName",),).toBe("analytics",);
				sendJson(res, {
					hasResult: true,
					alive: false,
					aborted: false,
					unknown: false,
					jobId: "future-1",
					result: [{ table: "orders", schema: "analytics", },],
				},);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			expect(JSON.parse(
				(await dss(["connection", "schemas", "--connection", "ATHENA_CONN",], { env: cliEnv(url,), },))
					.stdout,
			),).toEqual(["analytics",],);
			expect(JSON.parse(
				(
					await dss([
						"connection",
						"tables",
						"--connection",
						"ATHENA_CONN",
						"--schema",
						"analytics",
					], { env: cliEnv(url,), },)
				).stdout,
			),).toMatchObject({
				hasResult: true,
				result: [{ table: "orders", schema: "analytics", },],
			},);
		},);
	});

	it("job build supports managed folder targets", async () => {
		let requestBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "job-folder", },);
		}, async (url,) => {
			const { stdout, } = await dss(
				["job", "build", "folder-id", "--target-type", "managed-folder",],
				{
					env: cliEnv(url,),
				},
			);
			expect(JSON.parse(stdout,),).toEqual({ jobId: "job-folder", },);
		},);

		expect(requestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "folder-id",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "folder-id",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
	});

	it("job build-and-wait supports managed folder targets", async () => {
		const requests: string[] = [];
		let requestBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-folder-wait", },);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder-wait/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-folder-wait", type: "MANAGED_FOLDER_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"job",
				"build-and-wait",
				"folder-id",
				"--target-type",
				"managed-folder",
				"--timeout",
				"5000",
			], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toMatchObject({
				success: true,
				jobId: "job-folder-wait",
				state: "DONE",
				type: "MANAGED_FOLDER_BUILD",
			},);
		},);

		expect(requestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "folder-id",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "folder-id",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-folder-wait/",
		],);
	});
});

describe("CLI flow zone commands", () => {
	it("flow-zone create posts zone payload", async () => {
		let requestBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "zone-1", name: "Exports", color: "#cc0000", items: [], },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"flow-zone",
				"create",
				"--name",
				"Exports",
				"--color",
				"#cc0000",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				created: "zone-1",
				resource: "flow-zone",
				id: "zone-1",
				name: "Exports",
				color: "#cc0000",
				items: [],
			},);
		},);

		expect(requestBody,).toEqual({ name: "Exports", color: "#cc0000", },);
	});

	it("flow-zone move posts comma-delimited object refs", async () => {
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones/zone-1/add-items",);
			requestBody = JSON.parse(await readBody(req,),);
			sendJson(res, { id: "zone-1", name: "Exports", items: requestBody, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"flow-zone",
				"move",
				"zone-1",
				"--dataset",
				"raw_orders,clean_orders",
				"--recipe",
				"prepare_orders",
				"--folder",
				"folder-id",
				"--object",
				"OTHER:SAVED_MODEL:model-id",
			], { env: cliEnv(url,), },);
			const payload = JSON.parse(stdout,) as Record<string, unknown>;
			expect(payload.items,).toEqual(requestBody,);
		},);

		expect(requestBody,).toEqual([
			{ objectId: "raw_orders", objectType: "DATASET", },
			{ objectId: "clean_orders", objectType: "DATASET", },
			{ objectId: "prepare_orders", objectType: "RECIPE", },
			{ objectId: "folder-id", objectType: "MANAGED_FOLDER", },
			{ projectKey: "OTHER", objectType: "SAVED_MODEL", objectId: "model-id", },
		],);
	});

	it("flow-zone organize plans declarative visual grouping without mutating", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones",);
			sendJson(res, [
				{ id: "zone-raw", name: "Raw", color: "#111111", items: [], },
			],);
		}, async (url,) => {
			const plan = {
				zones: [
					{
						name: "Raw",
						color: "#64748b",
						position: { x: 100, y: 200, },
						datasets: ["raw_orders",],
						recipes: ["prepare_orders",],
					},
					{
						name: "Prepared",
						color: "#2ab1ac",
						items: [
							"DATASET:clean_orders",
							{ objectType: "MANAGED_FOLDER", objectId: "folder-id", },
						],
					},
				],
			};
			const { stdout, stderr, } = await dss([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--dry-run",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				dryRun: true;
				itemCount: number;
				planned: Array<{ create?: boolean; update?: Record<string, unknown>; moveItems: unknown[]; }>;
			};
			expect(result.dryRun,).toBe(true,);
			expect(result.itemCount,).toBe(4,);
			expect(result.planned[0].update,).toEqual({ color: "#64748b", position: { x: 100, y: 200, }, },);
			expect(result.planned[0].moveItems,).toEqual([
				{ objectType: "DATASET", objectId: "raw_orders", },
				{ objectType: "RECIPE", objectId: "prepare_orders", },
			],);
			expect(result.planned[1].create,).toBe(true,);
		},);
	});

	it("flow-zone organize creates missing zones and moves grouped objects", async () => {
		const requests: string[] = [];
		const moveBodies: Array<{ path: string; body: unknown; }> = [];
		let createBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [
					{
						id: "zone-raw",
						name: "Raw",
						color: "#64748b",
						items: [{ objectType: "DATASET", objectId: "old_extra", },],
					},
				],);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/flow/zones") {
				createBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "zone-prepared", name: "Prepared", color: "#2ab1ac", items: [], },);
				return;
			}
			if (req.method === "POST" && url.pathname.endsWith("/add-items",)) {
				const body = JSON.parse(await readBody(req,),);
				moveBodies.push({ path: url.pathname, body, },);
				const zoneId = url.pathname.includes("zone-raw",)
					? "zone-raw"
					: url.pathname.includes("zone-prepared",)
					? "zone-prepared"
					: "default";
				const name = zoneId === "zone-raw" ? "Raw" : "Prepared";
				sendJson(res, { id: zoneId, name, items: body, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const plan = {
				zones: [
					{ name: "Raw", color: "#64748b", datasets: ["raw_orders",], },
					{ name: "Prepared", color: "#2ab1ac", recipes: ["prepare_orders",], },
				],
			};
			const { stdout, stderr, } = await dss([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--sync",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				organized: boolean;
				created: Array<{ id: string; name: string; }>;
				moved: Array<{ zoneId: string; items: unknown[]; }>;
				pruned: Array<{ zoneId: string; items: unknown[]; }>;
			};
			expect(result.organized,).toBe(true,);
			expect(result.created,).toEqual([{
				id: "zone-prepared",
				name: "Prepared",
				color: "#2ab1ac",
				items: [],
			},],);
			expect(result.moved.map((move,) => move.zoneId),).toEqual(["zone-raw", "zone-prepared",],);
			expect(result.pruned,).toEqual([{
				zoneId: "default",
				fromZoneId: "zone-raw",
				name: "Raw",
				items: [{ objectId: "old_extra", objectType: "DATASET", },],
			},],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/flow/zones",
			"POST /public/api/projects/TEST/flow/zones/zone-raw/add-items",
			"POST /public/api/projects/TEST/flow/zones/default/add-items",
			"POST /public/api/projects/TEST/flow/zones",
			"POST /public/api/projects/TEST/flow/zones/zone-prepared/add-items",
		],);
		expect(createBody,).toEqual({ name: "Prepared", color: "#2ab1ac", },);
		expect(moveBodies,).toEqual([
			{
				path: "/public/api/projects/TEST/flow/zones/zone-raw/add-items",
				body: [{ objectId: "raw_orders", objectType: "DATASET", },],
			},
			{
				path: "/public/api/projects/TEST/flow/zones/default/add-items",
				body: [{ objectId: "old_extra", objectType: "DATASET", },],
			},
			{
				path: "/public/api/projects/TEST/flow/zones/zone-prepared/add-items",
				body: [{ objectId: "prepare_orders", objectType: "RECIPE", },],
			},
		],);
	});

	it("flow-zone organize validates objects before moving", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [
					{ id: "zone-raw", name: "Raw", color: "#64748b", items: [], },
				],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/graph/") {
				sendJson(res, { datasets: ["known_orders",], recipes: [], folders: [], nodes: {}, },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "known_orders", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, [],);
				return;
			}
			res.statusCode = 500;
			res.end("unexpected mutation",);
		}, async (url,) => {
			const plan = { zones: [{ name: "Raw", datasets: ["missing_orders",], },], };
			const failure = await dssFailure([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--validate-objects",
				"--dry-run",
			], { env: cliEnv(url,), },);

			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("Flow zone organize validation failed",);
			expect(failure.stdout,).toContain("DATASET:missing_orders",);
		},);

		expect(requests.every((request,) => request.startsWith("GET ",)),).toBe(true,);
	});

	it("flow-zone create rejects invalid colors before calling DSS", async () => {
		await withCliServer(() => {
			throw new Error("server should not be called for invalid flow-zone color",);
		}, async (url,) => {
			const failure = await dssFailure([
				"flow-zone",
				"create",
				"--name",
				"Exports",
				"--color",
				"red",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("--color must be a hex color",);
		},);
	});

	it("flow-zone get rejects empty zone ids before calling DSS", async () => {
		await withCliServer(() => {
			throw new Error("server should not be called for empty flow-zone id",);
		}, async (url,) => {
			const failure = await dssFailure(["flow-zone", "get", "",], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("Flow zone id must not be empty",);
		},);
	});

	it("rejects unknown long flags", async () => {
		const failure = await dssFailure(["flow-zone", "list", "--wat", "yes",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(failure.stdout,).toContain("Unknown flag: --wat",);
	});

	it("flow-zone delete accepts common dryrun alias", async () => {
		const methods: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			methods.push(`${req.method ?? "GET"} ${url.pathname}`,);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones/zone-1",);
			sendJson(res, { id: "zone-1", name: "Exports", items: [], },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"flow-zone",
				"delete",
				"zone-1",
				"--dryrun",
			], { env: cliEnv(url,), },);
			const payload = JSON.parse(stdout,) as Record<string, unknown>;
			expect(payload.dryRun,).toBe(true,);
		},);
		expect(methods,).toEqual(["GET /public/api/projects/TEST/flow/zones/zone-1",],);
	});

	it("prints empty arrays as JSON", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones",);
			sendJson(res, [],);
		}, async (url,) => {
			const { stdout, } = await dss(["flow-zone", "list",], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual([],);
		},);
	});

	it("flow-zone move requires at least one object", async () => {
		await withCliServer(() => {
			throw new Error("server should not be called for invalid flow-zone move usage",);
		}, async (url,) => {
			const failure = await dssFailure(["flow-zone", "move", "zone-1",], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("At least one object is required",);
		},);
	});
});
