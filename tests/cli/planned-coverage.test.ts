import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	readBody,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI planned command coverage", () => {
	it("fails recipe create without --output", async () => {
		const failure = await dssFailure([
			"recipe",
			"create",
			"--type",
			"python",
			"--input",
			"source_ds",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(failure.stdout,).toContain("--output or --output-folder is required",);
		expect(failure.stdout,).toContain(
			"dss recipe create --type TYPE [--input DS] (--output DS | --output-folder FOLDER_ID)",
		);
	});

	it("dataset clone plans the target creation without contacting DSS", async () => {
		const { stdout, stderr, } = await dss([
			"dataset",
			"clone",
			"source_ds",
			"target_ds",
			"--path",
			"/dataiku/TEST/target_ds",
			"--plan",
			"--project-key",
			"TEST",
		], { env: cliEnv("http://127.0.0.1:1",), },);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			plan: true,
			resource: "dataset",
			action: "clone",
			method: "POST",
			endpoint: "/public/api/projects/TEST/datasets/",
			source: "source_ds",
			target: "target_ds",
			payload: {
				sourceDataset: "source_ds",
				targetDataset: "target_ds",
				path: "/dataiku/TEST/target_ds",
				allowSamePath: false,
				projectKey: "TEST",
			},
		},);
	});

	it("dataset rename plans the actual rename action and both names", async () => {
		const { stdout, stderr, } = await dss([
			"dataset",
			"rename",
			"source_ds",
			"target_ds",
			"--plan",
			"--project-key",
			"TEST",
		], { env: cliEnv("http://127.0.0.1:1",), },);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			plan: true,
			resource: "dataset",
			action: "rename",
			method: "POST",
			endpoint: "/public/api/projects/TEST/actions/renameDataset",
			oldName: "source_ds",
			newName: "target_ds",
			payload: { oldName: "source_ds", newName: "target_ds", },
		},);
	});

	it("recipe input edit plans expose the dataset, role, and update operation", async () => {
		for (
			const [action, operation,] of [
				["add-input", "append",],
				["remove-input", "remove",],
			] as const
		) {
			const { stdout, stderr, } = await dss([
				"recipe",
				action,
				"compute_orders",
				"lookup",
				"--role",
				"reference",
				"--plan",
				"--project-key",
				"TEST",
			], { env: cliEnv("http://127.0.0.1:1",), },);

			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toMatchObject({
				plan: true,
				resource: "recipe",
				action,
				method: "PUT",
				endpoint: "/public/api/projects/TEST/recipes/compute_orders",
				recipe: "compute_orders",
				dataset: "lookup",
				role: "reference",
				payload: { operation, dataset: "lookup", role: "reference", projectKey: "TEST", },
			},);
		}
	});

	it("recipe clone plans the target and rewrites without contacting DSS", async () => {
		const { stdout, stderr, } = await dss([
			"recipe",
			"clone",
			"source_recipe",
			"--name",
			"target_recipe",
			"--replace-input",
			"old_input=new_input",
			"--replace-output",
			"old_output=new_output",
			"--plan",
			"--project-key",
			"TEST",
		], { env: cliEnv("http://127.0.0.1:1",), },);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			plan: true,
			resource: "recipe",
			action: "clone",
			method: "POST",
			endpoint: "/public/api/projects/TEST/recipes/",
			source: "source_recipe",
			target: "target_recipe",
			payload: {
				sourceRecipe: "source_recipe",
				targetRecipe: "target_recipe",
				inputRewrites: { old_input: "new_input", },
				outputRewrites: { old_output: "new_output", },
				copyOutputSettings: false,
				projectKey: "TEST",
			},
		},);
	});

	it("recipe create dry-run expands repeated and comma-separated inputs", async () => {
		const { stdout, stderr, } = await dss([
			"recipe",
			"create",
			"--type",
			"python",
			"--input",
			"source_a",
			"--input",
			"source_b,source_c",
			"--output",
			"target_ds",
			"--dry-run",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(stderr,).toBe("",);
		const result = JSON.parse(stdout,) as { payload: { inputDatasets: string[]; }; };
		expect(result.payload.inputDatasets,).toEqual(["source_a", "source_b", "source_c",],);
	});

	it("recipe create plan expands repeated and comma-separated inputs", async () => {
		const { stdout, stderr, } = await dss([
			"recipe",
			"create",
			"--type",
			"python",
			"--input",
			"source_a",
			"--input",
			"source_b,source_c",
			"--output",
			"target_ds",
			"--plan",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(stderr,).toBe("",);
		const result = JSON.parse(stdout,) as { payload: { inputDatasets: string[]; }; };
		expect(result.payload.inputDatasets,).toEqual(["source_a", "source_b", "source_c",],);
	});

	it("recipe clone dry-run accepts from/to and input/output rewrites", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/source_recipe",);
			expect(url.searchParams.get("includePayload",),).toBe("true",);
			sendJson(res, {
				recipe: {
					name: "source_recipe",
					type: "python",
					inputs: { main: { items: [{ ref: "old_input", },], }, },
					outputs: { main: { items: [{ ref: "old_output", },], }, },
				},
				payload: "dataiku.Dataset('old_input').get_dataframe()\n",
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"recipe",
				"clone",
				"--from",
				"source_recipe",
				"--to",
				"target_recipe",
				"--replace-input",
				"old_input=new_input",
				"--replace-output",
				"old_output=new_output",
				"--dry-run",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				inputRewrites: Record<string, string>;
				outputRewrites: Record<string, string>;
				source: string;
				target: string;
			};
			expect(result.source,).toBe("source_recipe",);
			expect(result.target,).toBe("target_recipe",);
			expect(result.inputRewrites,).toEqual({ old_input: "new_input", },);
			expect(result.outputRewrites,).toEqual({ old_output: "new_output", },);
		},);
	});

	it("recipe clone rejects one storage override for multiple copied outputs", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/source_recipe",);
			sendJson(res, {
				recipe: {
					name: "source_recipe",
					type: "python",
					outputs: {
						main: {
							items: [
								{ ref: "old_output_a", },
								{ ref: "old_output_b", },
							],
						},
					},
				},
				payload: "",
			},);
		}, async (url,) => {
			const failure = await dssFailure([
				"recipe",
				"clone",
				"source_recipe",
				"--to",
				"target_recipe",
				"--replace-output",
				"old_output_a=new_output_a",
				"--replace-output",
				"old_output_b=new_output_b",
				"--copy-output-settings",
				"--path",
				"/dataiku/TEST/reused",
				"--dry-run",
			], { env: cliEnv(url,), },);

			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("Cannot reuse --path or --metastore-table",);
		},);
	});

	it("dataset clone dry-run preserves settings with storage overrides", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/source_ds",);
			sendJson(res, {
				name: "source_ds",
				id: "read-only-id",
				type: "S3",
				managed: true,
				params: {
					connection: "s3_conn",
					path: "/dataiku/TEST/source_ds",
					metastoreTableName: "source_ds",
					internalState: "server-managed",
				},
				formatType: "csv",
				formatParams: { separator: "\t", parseHeaderRow: true, },
				schema: { columns: [{ name: "id", type: "bigint", },], },
				versionTag: { versionNumber: 3, },
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"clone",
				"source_ds",
				"target_ds",
				"--path",
				"/dataiku/TEST/target_ds",
				"--metastore-table",
				"target_ds",
				"--dry-run",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				next: {
					id?: string;
					versionTag?: unknown;
					name: string;
					params: {
						connection: string;
						path: string;
						metastoreTableName: string;
						internalState?: unknown;
					};
					schema: unknown;
				};
			};
			expect(result.next.name,).toBe("target_ds",);
			expect(result.next.params.connection,).toBe("s3_conn",);
			expect(result.next.params.path,).toBe("/dataiku/TEST/target_ds",);
			expect(result.next.params.metastoreTableName,).toBe("target_ds",);
			expect(result.next.params.internalState,).toBeUndefined();
			expect(result.next.schema,).toEqual({ columns: [{ name: "id", type: "bigint", },], },);
			expect(result.next.id,).toBeUndefined();
			expect(result.next.versionTag,).toBeUndefined();
		},);
	});

	it("dataset source returns compact backing storage details", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/source_ds",);
			sendJson(res, {
				name: "source_ds",
				type: "PostgreSQL",
				projectKey: "TEST",
				managed: false,
				params: {
					connection: "warehouse",
					catalog: "prod",
					schema: "public",
					table: "orders",
				},
				formatType: "csv",
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "source", "source_ds",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toMatchObject({
				resource: "dataset",
				name: "source_ds",
				connection: "warehouse",
				catalog: "prod",
				schema: "public",
				table: "orders",
			},);
		},);
	});

	it("dataset clone refuses to reuse managed storage paths by default", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				name: "source_ds",
				type: "S3",
				managed: true,
				params: { connection: "s3_conn", path: "/dataiku/TEST/source_ds", },
			},);
		}, async (url,) => {
			const failure = await dssFailure([
				"dataset",
				"clone",
				"source_ds",
				"target_ds",
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("Refusing to clone managed dataset",);
		},);
	});

	it("flow-zone summary and find expose compact object membership", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones",);
			sendJson(res, [
				{
					id: "zone-1",
					name: "Raw",
					items: [{ objectType: "DATASET", objectId: "orders", },],
				},
				{
					id: "zone-2",
					name: "Recipes",
					items: [{ objectType: "RECIPE", objectId: "compute_orders", },],
				},
			],);
		}, async (url,) => {
			const summary = JSON.parse(
				(await dss(["flow-zone", "list", "--summary", "--object", "RECIPE:compute_orders",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Array<{ id: string; itemCount: number; containsMatchingObject: boolean; }>;
			expect(summary,).toEqual([
				{ id: "zone-1", name: "Raw", itemCount: 1, containsMatchingObject: false, },
				{ id: "zone-2", name: "Recipes", itemCount: 1, containsMatchingObject: true, },
			],);

			const found = JSON.parse(
				(await dss(["flow-zone", "find", "--recipe", "compute_orders",], { env: cliEnv(url,), },))
					.stdout,
			) as Array<{ id: string; containsMatchingObject: boolean; }>;
			expect(found,).toEqual([{
				id: "zone-2",
				name: "Recipes",
				itemCount: 1,
				containsMatchingObject: true,
			},],);

			const foundByName = JSON.parse(
				(await dss(["flow-zone", "find", "Recipes",], { env: cliEnv(url,), },)).stdout,
			) as Array<{ id: string; items: unknown[]; }>;
			expect(foundByName,).toEqual([{
				id: "zone-2",
				name: "Recipes",
				itemCount: 1,
				items: [{ objectType: "RECIPE", objectId: "compute_orders", },],
			},],);
		},);
	});

	it("job list filters and summary normalize progress and warnings", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/") {
				sendJson(res, [
					{
						baseStatus: {
							def: { id: "job-1", type: "DATASET_BUILD", outputs: [{ id: "target_ds", },], },
							state: "DONE",
						},
					},
					{
						baseStatus: {
							def: { id: "job-2", type: "DATASET_BUILD", outputs: [{ id: "other_ds", },], },
							state: "FAILED",
						},
					},
				],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-1", type: "DATASET_BUILD", outputs: [{ id: "target_ds", },], },
						state: "DONE",
						startTime: 1000,
						endTime: 61_000,
						warningCount: 7,
					},
					activities: [{ warningCount: 2, },],
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/log/") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end([
					"WARN first warning",
					"Scanned 10, written 5",
					"5 rows successfully written",
					"Done! completed",
				].join("\n",),);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const list = JSON.parse(
				(await dss(["job", "list", "--state", "DONE", "--output", "target_ds", "--latest",], {
					env: cliEnv(url,),
				},)).stdout,
			) as unknown[];
			expect(list,).toHaveLength(1,);

			const summary = JSON.parse(
				(await dss(["job", "summary", "job-1", "--max-log-lines", "10",], { env: cliEnv(url,), },))
					.stdout,
			) as {
				durationMs: number;
				warnings: {
					dssSummaryWarningCount: number;
					activityWarningCount: number;
					logWarnLineCount: number;
				};
				doneLine: string;
				progress: { counters: { written: number; }; rowsPerMinute: number; };
			};
			expect(summary.durationMs,).toBe(60_000,);
			expect(summary.warnings,).toMatchObject({
				dssSummaryWarningCount: 7,
				activityWarningCount: 2,
				logWarnLineCount: 1,
			},);
			expect(summary.progress.counters.written,).toBe(5,);
			expect(summary.progress.rowsPerMinute,).toBe(5,);
			expect(summary.doneLine,).toBe("Done! completed",);
		},);
	});
	it("uses replace mode for variable set without fetching existing values", async () => {
		let sawGet = false;
		let capturedBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/variables/") {
				sawGet = true;
				sendJson(res, { standard: { stale: true, }, local: {}, },);
				return;
			}

			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/variables/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, }, 204,);
				return;
			}

			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"variable",
				"set",
				"--standard",
				'{"fresh":true}',
				"--local",
				'{"note":"set"}',
				"--replace",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			expect(stdout,).toBe('{"standard":{"fresh":true},"local":{"note":"set"}}\n',);
		},);

		expect(sawGet,).toBe(false,);
		expect(capturedBody,).toEqual({
			standard: { fresh: true, },
			local: { note: "set", },
		},);
	});

	it("resolves folder names before calling folder commands", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/ALT/managedfolders/") {
				sendJson(res, [{ id: "fld-123", name: "Named folder", },],);
				return;
			}

			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/ALT/managedfolders/fld-123/contents/"
			) {
				sendJson(res, { items: [{ path: "sub/file.txt", size: 12, },], },);
				return;
			}

			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"folder",
				"contents",
				"Named folder",
				"--project-key",
				"ALT",
			], { env: cliEnv(url,), },);

			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toEqual([{ path: "sub/file.txt", size: 12, },],);
		},);
	});

	it("adds folder target context to transient contents failures", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "fld-123", name: "Named folder", },],);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/managedfolders/fld-123/contents/"
			) {
				res.setHeader("X-Request-Id", "req-123",);
				sendJson(res, { message: "temporary gateway failure", }, 503,);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const failure = await dssFailure([
				"folder",
				"contents",
				"Named folder",
				"--retries",
				"1",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(3,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				requestId?: string;
				details: { body: string; };
			};
			const body = JSON.parse(report.details.body,) as { elapsedMs: number; target: string; };
			expect(report.requestId,).toBe("req-123",);
			expect(body.target,).toBe("folder:fld-123",);
			expect(body.elapsedMs,).toEqual(expect.any(Number,),);
		},);
	});

	it("adds folder target context when contents resolution is transient", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, { message: "gateway timeout", requestId: "req-list", }, 503,);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const failure = await dssFailure([
				"folder",
				"contents",
				"Named folder",
				"--retries",
				"1",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(3,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				requestId?: string;
				details: { body: string; };
			};
			const body = JSON.parse(report.details.body,) as { target: string; };
			expect(report.requestId,).toBe("req-list",);
			expect(body.target,).toBe("folder:Named folder",);
		},);
	});

	it("downloads recipe code to a file and prints the file path", async () => {
		const outputPath = join(tmpdir(), `dss-cli-recipe-code-${Date.now()}.py`,);

		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				expect(req.method,).toBe("GET",);
				expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/sample_recipe",);
				expect(url.searchParams.get("includePayload",),).toBe("true",);
				sendJson(res, {
					recipe: { type: "python", },
					payload: "print('hello from recipe')\n",
				},);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"recipe",
					"download-code",
					"sample_recipe",
					"--output",
					outputPath,
				], { env: cliEnv(url,), },);

				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toBe(outputPath,);
			},);

			expect(readFileSync(outputPath, "utf-8",),).toBe("print('hello from recipe')\n",);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	it("shows a line-based diff for modified local recipe code", async () => {
		const filePath = join(tmpdir(), `dss-cli-recipe-diff-${Date.now()}.py`,);
		writeFileSync(filePath, "print('remote')\nprint('local')\n", "utf-8",);

		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				expect(req.method,).toBe("GET",);
				expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/sample_recipe",);
				expect(url.searchParams.get("includePayload",),).toBe("true",);
				sendJson(res, {
					recipe: { type: "python", },
					payload: "print('remote')\nprint('server')\n",
				},);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"recipe",
					"diff",
					"sample_recipe",
					"--file",
					filePath,
				], { env: cliEnv(url,), },);

				expect(stderr,).toBe("",);
				const diff = JSON.parse(stdout,) as string;
				expect(diff,).toContain("--- remote:sample_recipe",);
				expect(diff,).toContain(`+++ local:${filePath}`,);
				expect(diff,).toContain("@@ line 2 @@",);
				expect(diff,).toContain("- print('server')",);
				expect(diff,).toContain("+ print('local')",);
			},);
		} finally {
			rmSync(filePath, { force: true, },);
		}
	});
});
