import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";
import {
	cliEnv,
	dss,
	dssFailure,
	dssWithInput,
	join,
	readBody,
	readFileExists,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI execution behavior", () => {
	it("prints { ok: true } for void commands", async () => {
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("DELETE",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/sample",);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "delete", "sample",], { env: cliEnv(url,), },);
			expect(stdout,).toContain('"deleted": "sample"',);
			expect(stdout,).toContain('"resource": "dataset"',);
			expect(stderr,).toBe("",);
		},);
	});

	it("prints string results as JSON strings", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("line 1\nline 2\n",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["job", "log", "job-1",], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toBe("line 1\nline 2\n",);
			expect(stderr,).toBe("",);
		},);
	});

	it("routes --log-id through the public activity log endpoint", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			expect(url.searchParams.get("activity",),).toBe("activity-1",);
			expect(url.searchParams.has("logId",),).toBe(false,);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("activity stdout\n",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"job",
				"log",
				"job-1",
				"--activity",
				"activity-1",
				"--log-id",
				"/python-recipe/python-output.log",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toBe("activity stdout\n",);
			expect(stderr,).toBe("",);
		},);
	});

	it("filters to error and traceback lines with --errors-only", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("starting build\nERROR: boom\nprocessing\nTraceback (most recent call last):\n",);
		}, async (url,) => {
			const { stdout, } = await dss(["job", "log", "job-1", "--errors-only",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toBe("ERROR: boom\nTraceback (most recent call last):",);
		},);
	});

	it("writes the log to --output and returns the path", async () => {
		const tmpFile = join(tmpdir(), `dss-cli-job-log-${Date.now()}.txt`,);
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("line 1\nline 2\n",);
		}, async (url,) => {
			try {
				const { stdout, } = await dss(["job", "log", "job-1", "--output", tmpFile,], {
					env: cliEnv(url,),
				},);
				expect(JSON.parse(stdout,),).toBe(tmpFile,);
				expect(readFileSync(tmpFile, "utf-8",),).toBe("line 1\nline 2\n",);
			} finally {
				rmSync(tmpFile, { force: true, },);
			}
		},);
	});

	it("supports --data-file JSON input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		const tmpFile = join(tmpdir(), `dss-cli-data-file-${Date.now()}.json`,);
		writeFileSync(
			tmpFile,
			`\ufeff${JSON.stringify({ nested: { added: "from-file", }, },)}`,
			"utf-8",
		);
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/sample") {
					sendJson(res, { nested: { preserved: true, }, },);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/datasets/sample") {
					capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { updated: true, },);
					return;
				}
				res.statusCode = 404;
				res.end("not found",);
			}, async (url,) => {
				const { stdout, } = await dss(
					["dataset", "update", "sample", "--data-file", tmpFile,],
					{ env: cliEnv(url,), },
				);
				expect(stdout,).toContain('"updated": "sample"',);
			},);
			const nested = capturedBody?.nested as Record<string, unknown> | undefined;
			expect(nested?.preserved,).toBe(true,);
			expect(nested?.added,).toBe("from-file",);
		} finally {
			rmSync(tmpFile, { force: true, },);
		}
	});

	it("normalizes scenario rawParams dry-run to canonical params", async () => {
		const step = { type: "build_flowitem", name: "Build FRG", };
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/BUILD_FRG/") {
				sendJson(res, {
					id: "BUILD_FRG",
					type: "step_based",
					params: { steps: [], },
					rawParams: { params: { steps: [], }, },
				},);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"scenario",
				"update",
				"BUILD_FRG",
				"--data",
				JSON.stringify({ rawParams: { params: { steps: [step,], }, }, },),
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.normalization,).toEqual([{
				from: "rawParams.params",
				to: "params",
				action: "promoted",
				message: expect.stringContaining("editable scenario definition uses params",),
			},],);
			expect(result.normalizedData,).toEqual({ params: { steps: [step,], }, },);
			expect(result.changes,).toEqual([{
				path: "params.steps",
				before: [],
				after: [step,],
			},],);
			expect(result.next,).toHaveProperty("params.steps", [step,],);
		},);
	});

	it("fails scenario update when refetch does not include requested fields", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/BUILD_FRG/") {
				sendJson(res, { id: "BUILD_FRG", type: "step_based", params: { steps: [], }, },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/scenarios/BUILD_FRG/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, }, 204,);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const failure = await dssFailure([
				"scenario",
				"update",
				"BUILD_FRG",
				"--data",
				JSON.stringify({ params: { steps: [{ type: "build_flowitem", },], }, },),
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			expect(capturedBody,).toHaveProperty("params.steps.0.type", "build_flowitem",);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "validation_failed",
				category: "dss",
				status: 400,
				resource: "scenario",
				action: "update",
			},);
			expect(report.error,).toContain("Scenario update did not persist requested fields",);
			expect(report.error,).toContain("params.steps",);
		},);
	});

	it("supports --stdin JSON input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/sample") {
				sendJson(res, { nested: { preserved: true, }, },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/datasets/sample") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { updated: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, } = await dssWithInput(
				["dataset", "update", "sample", "--stdin",],
				JSON.stringify({ nested: { added: "from-stdin", }, },),
				{ env: cliEnv(url,), },
			);
			expect(stdout,).toContain('"updated": "sample"',);
		},);
		const nested = capturedBody?.nested as Record<string, unknown> | undefined;
		expect(nested?.preserved,).toBe(true,);
		expect(nested?.added,).toBe("from-stdin",);
	});

	it("supports positional SQL input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { queryId: "q-positional", hasResults: true, schema: [], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-positional/stream") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/sql/queries/q-positional/finish-streaming"
			) {
				res.statusCode = 200;
				res.end("",);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, } = await dss(["sql", "query", "SELECT 1", "--connection", "CONN",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({
				queryId: "q-positional",
				schema: [],
				columns: [],
				rows: [],
			},);
		},);
		expect(capturedBody,).toMatchObject({
			query: "SELECT 1",
			connection: "CONN",
			projectKey: "TEST",
		},);
	});

	it("supports --sql-file input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		const tmpFile = join(tmpdir(), `dss-cli-sql-${Date.now()}.sql`,);
		writeFileSync(tmpFile, 'SELECT * FROM "analytics".orders LIMIT 5', "utf-8",);
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
					capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { queryId: "q-file", hasResults: true, schema: [], },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-file/stream") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end("[]",);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/sql/queries/q-file/finish-streaming"
				) {
					res.statusCode = 200;
					res.end("",);
					return;
				}
				res.statusCode = 404;
				res.end("not found",);
			}, async (url,) => {
				const { stdout, } = await dss([
					"sql",
					"query",
					"--sql-file",
					tmpFile,
					"--connection",
					"CONN",
				], { env: cliEnv(url,), },);
				expect(JSON.parse(stdout,),).toEqual({
					queryId: "q-file",
					schema: [],
					columns: [],
					rows: [],
				},);
			},);
			expect(capturedBody?.query,).toBe('SELECT * FROM "analytics".orders LIMIT 5',);
		} finally {
			rmSync(tmpFile, { force: true, },);
		}
	});

	it("strips a UTF-8 BOM from --sql-file input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		const tmpFile = join(tmpdir(), `dss-cli-sql-bom-${Date.now()}.sql`,);
		writeFileSync(
			tmpFile,
			'\ufeffSELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',
			"utf-8",
		);
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
					capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { queryId: "q-file-bom", hasResults: true, schema: [], },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-file-bom/stream") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end("[]",);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/sql/queries/q-file-bom/finish-streaming"
				) {
					res.statusCode = 200;
					res.end("",);
					return;
				}
				res.statusCode = 404;
				res.end("not found",);
			}, async (url,) => {
				await dss(["sql", "query", "--sql-file", tmpFile, "--connection", "CONN",], {
					env: cliEnv(url,),
				},);
			},);
			expect(capturedBody?.query,).toBe(
				'SELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',
			);
		} finally {
			rmSync(tmpFile, { force: true, },);
		}
	});

	it("writes SQL query results to --output", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-output-${Date.now()}.json`,);
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
					sendJson(res, {
						queryId: "q-output",
						hasResults: true,
						schema: [{ name: "id", type: "bigint", },],
					},);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-output/stream") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end("[[1],[2]]",);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/sql/queries/q-output/finish-streaming"
				) {
					res.statusCode = 200;
					res.end("",);
					return;
				}
				res.statusCode = 404;
				res.end("not found",);
			}, async (url,) => {
				const { stdout, } = await dss([
					"sql",
					"query",
					"SELECT id FROM orders",
					"--connection",
					"CONN",
					"--output",
					outputPath,
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as {
					outputPath: string;
					rowCount: number;
					written: string;
				};
				expect(result,).toMatchObject({
					outputPath,
					written: outputPath,
					rowCount: 2,
				},);
				expect(JSON.parse(readFileSync(outputPath, "utf-8",),),).toEqual({
					queryId: "q-output",
					schema: [{ name: "id", type: "bigint", },],
					columns: [{ name: "id", type: "bigint", },],
					rows: [[1,], [2,],],
				},);
			},);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	function sqlPreviewServer(queryId: string, rows: number[][],) {
		return (req: IncomingMessage, res: ServerResponse,): void => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
				sendJson(res, { queryId, hasResults: true, schema: [{ name: "id", type: "bigint", },], },);
				return;
			}
			if (req.method === "GET" && url.pathname === `/public/api/sql/queries/${queryId}/stream`) {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(rows,),);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === `/public/api/sql/queries/${queryId}/finish-streaming`
			) {
				res.statusCode = 200;
				res.end("",);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		};
	}

	it("includes a default 5-row preview in the --output summary", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-preview-${Date.now()}.json`,);
		const rows = [[1,], [2,], [3,], [4,], [5,], [6,], [7,],];
		try {
			await withCliServer(sqlPreviewServer("q-preview", rows,), async (url,) => {
				const { stdout, } = await dss([
					"sql",
					"query",
					"SELECT id FROM orders",
					"--connection",
					"CONN",
					"--output",
					outputPath,
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { rowCount: number; preview: number[][]; };
				expect(result.rowCount,).toBe(7,);
				expect(result.preview,).toEqual([[1,], [2,], [3,], [4,], [5,],],);
				// The file keeps every row; the preview is only the stdout summary.
				expect((JSON.parse(readFileSync(outputPath, "utf-8",),) as { rows: number[][]; }).rows,)
					.toEqual(rows,);
			},);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	it("honors --preview N for the preview row count", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-preview-n-${Date.now()}.json`,);
		const rows = [[1,], [2,], [3,], [4,],];
		try {
			await withCliServer(sqlPreviewServer("q-preview-n", rows,), async (url,) => {
				const { stdout, } = await dss([
					"sql",
					"query",
					"SELECT id FROM orders",
					"--connection",
					"CONN",
					"--output",
					outputPath,
					"--preview",
					"2",
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { rowCount: number; preview: number[][]; };
				expect(result.rowCount,).toBe(4,);
				expect(result.preview,).toEqual([[1,], [2,],],);
			},);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	it("treats --preview 0 as an explicit empty preview", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-preview-zero-${Date.now()}.json`,);
		const rows = [[1,], [2,],];
		try {
			await withCliServer(sqlPreviewServer("q-preview-zero", rows,), async (url,) => {
				const { stdout, } = await dss([
					"sql",
					"query",
					"SELECT id FROM orders",
					"--connection",
					"CONN",
					"--output",
					outputPath,
					"--preview",
					"0",
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { rowCount: number; preview: number[][]; };
				expect(result.rowCount,).toBe(2,);
				expect(result.preview,).toEqual([],);
			},);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	it("rejects --preview without --output", async () => {
		const failure = await dssFailure([
			"sql",
			"query",
			"SELECT 1",
			"--connection",
			"CONN",
			"--preview",
			"5",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("--preview requires --output",);
	});

	it("rejects a non-integer --preview value before querying", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-preview-bad-${Date.now()}.json`,);
		const failure = await dssFailure([
			"sql",
			"query",
			"SELECT 1",
			"--connection",
			"CONN",
			"--output",
			outputPath,
			"--preview",
			"abc",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("non-negative integer",);
		// Validation runs before the query, so no output file is created.
		expect(readFileExists(outputPath,),).toBe(false,);
	});

	it("rejects a negative --preview value", async () => {
		const outputPath = join(tmpdir(), `dss-cli-sql-preview-neg-${Date.now()}.json`,);
		const failure = await dssFailure([
			"sql",
			"query",
			"SELECT 1",
			"--connection",
			"CONN",
			"--output",
			outputPath,
			"--preview",
			"-1",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("non-negative integer",);
	});

	it("supports SQL from stdin without losing double quotes", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { queryId: "q-stdin", hasResults: true, schema: [], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-stdin/stream") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/sql/queries/q-stdin/finish-streaming"
			) {
				res.statusCode = 200;
				res.end("",);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const { stdout, } = await dssWithInput(
				["sql", "query", "--stdin", "--connection", "CONN",],
				'SELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',
				{ env: cliEnv(url,), },
			);
			expect(JSON.parse(stdout,),).toEqual({
				queryId: "q-stdin",
				schema: [],
				columns: [],
				rows: [],
			},);
		},);
		expect(capturedBody?.query,).toBe('SELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',);
	});

	it("strips a UTF-8 BOM from stdin SQL input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { queryId: "q-stdin-bom", hasResults: true, schema: [], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-stdin-bom/stream") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/sql/queries/q-stdin-bom/finish-streaming"
			) {
				res.statusCode = 200;
				res.end("",);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			await dssWithInput(
				["sql", "query", "--stdin", "--connection", "CONN",],
				'\ufeffSELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',
				{ env: cliEnv(url,), },
			);
		},);
		expect(capturedBody?.query,).toBe('SELECT * FROM "prod-icmc-dg-ilab-db".workbook LIMIT 5',);
	});

	it("requires exactly one SQL execution target", async () => {
		const failure = await dssFailure(["sql", "query", "SELECT 1",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Pass exactly one of --connection or --dataset",);
	});

	it("outputs JSON by default and with explicit --json", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [
				{ projectKey: "P1", name: "One", },
				{ projectKey: "P2", name: "Two", },
			],);
		}, async (url,) => {
			const implicitJson = await dss(["project", "list",], { env: cliEnv(url,), },);
			expect(JSON.parse(implicitJson.stdout,),).toEqual([
				{ projectKey: "P1", name: "One", },
				{ projectKey: "P2", name: "Two", },
			],);
		},);

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [{ projectKey: "P1", name: "One", },],);
		}, async (url,) => {
			const explicitJson = await dss(["project", "list", "--json",], { env: cliEnv(url,), },);
			expect(JSON.parse(explicitJson.stdout,),).toEqual([{ projectKey: "P1", name: "One", },],);
		},);
	});

	it("projects only the requested top-level fields with --fields", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [
				{ projectKey: "P1", name: "One", projectStatus: "Sandbox", tags: ["a",], },
				{ projectKey: "P2", name: "Two", projectStatus: "Production", tags: [], },
			],);
		}, async (url,) => {
			const projected = await dss(["project", "list", "--fields", "projectKey,name",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(projected.stdout,),).toEqual([
				{ projectKey: "P1", name: "One", },
				{ projectKey: "P2", name: "Two", },
			],);
		},);

		await withCliServer((_req, res,) => {
			sendJson(res, [{ projectKey: "P1", name: "One", },],);
		}, async (url,) => {
			const projected = await dss(["project", "list", "--fields", "name, missingField",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(projected.stdout,),).toEqual([{ name: "One", missingField: null, },],);
		},);

		await withCliServer((_req, res,) => {
			sendJson(res, [{ name: "P1", formatParams: { separator: "\t", charset: "utf8", }, },],);
		}, async (url,) => {
			const projected = await dss([
				"project",
				"list",
				"--fields",
				"name,formatParams.separator,formatParams.missing",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(projected.stdout,),).toEqual([
				{ "name": "P1", "formatParams.separator": "\t", "formatParams.missing": null, },
			],);
		},);
	});

	it("rejects removed output format flags", async () => {
		const longFailure = await dssFailure(["project", "list", "--format", "table",],);
		expect(longFailure.code,).toBe(1,);
		expect(longFailure.stderr,).toContain("Unknown flag: --format",);

		const shortFailure = await dssFailure(["-f", "table",],);
		expect(shortFailure.code,).toBe(1,);
		expect(shortFailure.stderr,).toContain("Unknown flag: -f",);
	});

	it("doctor reports connectivity and default project access as JSON", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/") {
				sendJson(res, [{ projectKey: "TEST", name: "Test Project", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/") {
				sendJson(res, { projectKey: "TEST", name: "Test Project", },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["doctor", "--project-key", "TEST",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				ok: boolean;
				checks: Array<{ name: string; ok: boolean; }>;
				context: Record<string, unknown>;
				permissions?: unknown;
			};
			expect(result.ok,).toBe(true,);
			expect(result.context.hasUrl,).toBe(true,);
			expect(result.context.hasApiKey,).toBe(true,);
			expect(result.context.projectKey,).toBe("TEST",);
			expect(result.permissions,).toBeUndefined();
			expect(result.checks.map((check,) => [check.name, check.ok,]),).toEqual([
				["credentials_present", true,],
				["connectivity", true,],
				["default_project", true,],
			],);
		},);
	});

	it("doctor capabilities report permissions, fixtures, and environment", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/") {
				sendJson(res, [{ projectKey: "TEST", name: "Test Project", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/") {
				sendJson(res, { projectKey: "TEST", name: "Test Project", },);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/variables/") {
				res.statusCode = 500;
				res.end("temporary variable probe failure",);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "orders", type: "Filesystem", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, [{ name: "prepare_orders", type: "python", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [{ id: "daily_build", name: "Daily build", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [{ id: "zone_a", name: "Zone A", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder_a", name: "Folder A", type: "Filesystem", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jobs/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jupyter-notebooks/") {
				sendJson(res, [{ name: "analysis", projectKey: "TEST", language: "python", },],);
				return;
			}
			if (url.pathname === "/public/api/connections/get-names/") {
				sendJson(res, ["filesystem_managed",],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected request ${url.pathname}`,);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["doctor", "--project-key", "TEST", "--capabilities",], {
				env: {
					...cliEnv(url,),
					RUN_DATAIKU_INTEGRATION_MUTATING: "1",
					RUN_DATAIKU_INTEGRATION_VARIABLES: "true",
					RUN_DATAIKU_ADMIN_MUTATING: "1",
					RUN_DATAIKU_SQL_LIVE: "true",
				},
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				ok: boolean;
				permissions: Record<string, string>;
				permissionDetails?: Record<string, unknown>;
				fixtures: Record<string, string | null>;
				environment: {
					projectKey?: string;
					integrationFlags: Record<string, boolean>;
				};
			};
			expect(result.ok,).toBe(true,);
			expect(result.permissions.canListProjects,).toBe("yes",);
			expect(result.permissions.canReadProject,).toBe("yes",);
			expect(result.permissions.canMutateProject,).toBe("unknown",);
			expect(result.permissionDetails?.canMutateProject,).toBeDefined();
			expect(result.permissions.canCreateFolder,).toBe("unknown",);
			expect(result.permissionDetails?.canCreateFolder,).toMatchObject({
				reason: "mutation capability was not verified because doctor capabilities are read-only",
				readAction: "folders.list",
				readStatus: "yes",
			},);
			expect(result.permissions.canRunJobs,).toBe("unknown",);
			expect(result.permissions.canCreateScenario,).toBe("unknown",);
			expect(result.permissions.canSaveJupyter,).toBe("unknown",);
			expect(result.permissions.canMutateConnection,).toBe("unknown",);
			expect(result.fixtures,).toEqual({
				defaultDataset: "orders",
				defaultRecipe: "prepare_orders",
				defaultScenario: "daily_build",
				defaultFlowZone: "zone_a",
				defaultManagedFolder: "folder_a",
				defaultJupyterNotebook: "analysis",
			},);
			expect(result.environment.projectKey,).toBe("TEST",);
			expect(result.environment.integrationFlags.mutating,).toBe(true,);
			expect(result.environment.integrationFlags.variables,).toBe(true,);
			expect(result.environment.integrationFlags.adminMutating,).toBe(true,);
			expect(result.environment.integrationFlags.sqlLive,).toBe(true,);
			expect(result.environment.integrationFlags.bundles,).toBe(false,);
		},);
	});

	it("doctor capabilities fast mode omits fixture discovery", async () => {
		let datasetFixtureRequested = false;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/") {
				sendJson(res, [{ projectKey: "TEST", name: "Test Project", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/") {
				sendJson(res, { projectKey: "TEST", name: "Test Project", },);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/variables/") {
				sendJson(res, { standard: {}, local: {}, },);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jobs/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jupyter-notebooks/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/connections/get-names/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/datasets/") {
				datasetFixtureRequested = true;
			}
			res.statusCode = 404;
			res.end(`unexpected request ${url.pathname}`,);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"doctor",
				"--project-key",
				"TEST",
				"--capabilities",
				"--fast",
			], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.permissions,).toBeDefined();
			expect(result.fixtures,).toBeUndefined();
			expect(datasetFixtureRequested,).toBe(false,);
		},);
	});

	it("fixtures discovers safe defaults and rejected candidates", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [
					{ name: "warehouse", type: "BigQuery", },
					{ name: "orders", type: "Filesystem", },
				],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, [{ name: "prepare_orders", type: "python", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [{ id: "daily_build", name: "Daily build", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [{ id: "zone_a", name: "Zone A", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [
					{ id: "folder_remote", name: "Remote", type: "S3", },
					{ id: "folder_safe", name: "Safe", type: "Inline", },
				],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jupyter-notebooks/") {
				sendJson(res, [
					{ name: "_system", projectKey: "TEST", language: "python", },
					{ name: "analysis", projectKey: "TEST", language: "python", },
				],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected request ${url.pathname}`,);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["fixtures", "--json", "--project-key", "TEST",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				projectKey: string;
				allowTypes: string[];
				fixtures: Record<string, string | null>;
				safeDataset: { name?: string; type?: string; } | null;
				safeManagedFolder: { id?: string; type?: string; } | null;
				safeJupyterNotebook: { name?: string; } | null;
				unsafe: Record<string, Array<Record<string, unknown>>>;
			};
			expect(result.projectKey,).toBe("TEST",);
			expect(result.allowTypes,).toEqual(["Filesystem", "Inline",],);
			expect(result.fixtures,).toEqual({
				defaultDataset: "warehouse",
				defaultRecipe: "prepare_orders",
				defaultScenario: "daily_build",
				defaultFlowZone: "zone_a",
				defaultManagedFolder: "folder_remote",
				defaultJupyterNotebook: "_system",
			},);
			expect(result.safeDataset,).toMatchObject({ name: "orders", type: "Filesystem", },);
			expect(result.safeManagedFolder,).toMatchObject({ id: "folder_safe", type: "Inline", },);
			expect(result.safeJupyterNotebook,).toMatchObject({ name: "analysis", },);
			expect(result.unsafe.datasets,).toEqual([{
				name: "warehouse",
				type: "BigQuery",
				reason: "type=BigQuery",
			},],);
			expect(result.unsafe.managedFolders,).toEqual([{
				id: "folder_remote",
				name: "Remote",
				type: "S3",
				reason: "type=S3",
			},],);
			expect(result.unsafe.jupyterNotebooks,).toEqual([{
				name: "_system",
				reason: "name starts with _",
			},],);
		},);
	});

	it("fixtures honors the allow-types filter", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "warehouse", type: "BigQuery", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder_safe", name: "Safe", type: "Filesystem", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jupyter-notebooks/") {
				sendJson(res, [{ name: "analysis", projectKey: "TEST", language: "python", },],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected request ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["fixtures", "--json", "--project-key", "TEST", "--allow-types", "BigQuery",], {
					env: cliEnv(url,),
				},)).stdout,
			) as {
				allowTypes: string[];
				safeDataset: { name?: string; } | null;
				safeManagedFolder: { id?: string; } | null;
				unsafe: { managedFolders?: Array<Record<string, unknown>>; };
			};
			expect(result.allowTypes,).toEqual(["BigQuery",],);
			expect(result.safeDataset,).toMatchObject({ name: "warehouse", },);
			expect(result.safeManagedFolder,).toBeNull();
			expect(result.unsafe.managedFolders,).toEqual([
				{ id: "folder_safe", name: "Safe", type: "Filesystem", reason: "type=Filesystem", },
			],);
		},);
	});

	it("fixtures returns null when no safe candidate exists", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			if (url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "warehouse", type: "BigQuery", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/flow/zones") {
				sendJson(res, [],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder_remote", name: "Remote", type: "S3", },],);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jupyter-notebooks/") {
				sendJson(res, [{ name: "_system", projectKey: "TEST", language: "python", },],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected request ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["fixtures", "--json", "--project-key", "TEST",], { env: cliEnv(url,), },)).stdout,
			) as {
				safeDataset: unknown;
				safeManagedFolder: unknown;
				safeJupyterNotebook: unknown;
			};
			expect(result.safeDataset,).toBeNull();
			expect(result.safeManagedFolder,).toBeNull();
			expect(result.safeJupyterNotebook,).toBeNull();
		},);
	});

	it("doctor returns JSON and nonzero exit for missing credentials", async () => {
		const failure = await dssFailure(["doctor",], {
			env: {
				PATH: process.env.PATH,
				HOME: process.env.HOME,
				DATAIKU_URL: " ",
				DATAIKU_API_KEY: " ",
			},
		},);
		expect(failure.code,).toBe(2,);
		expect(failure.stderr,).toBe("",);
		const result = JSON.parse(failure.stdout,) as {
			ok: boolean;
			checks: Array<{ name: string; ok: boolean; }>;
			context: Record<string, unknown>;
		};
		expect(result.ok,).toBe(false,);
		expect(result.context.hasUrl,).toBe(false,);
		expect(result.context.hasApiKey,).toBe(false,);
		expect(result.checks,).toEqual([
			expect.objectContaining({ name: "credentials_present", ok: false, },),
		],);
	});

	it("uses distinct exit codes for API and transient errors", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/missing") {
				res.statusCode = 404;
				res.end("dataset not found",);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/datasets/transient") {
				res.statusCode = 503;
				res.end("service unavailable",);
				return;
			}
			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const apiError = await dssFailure(["dataset", "get", "missing",], { env: cliEnv(url,), },);
			expect(apiError.code,).toBe(2,);
			expect(apiError.stderr,).toContain('"code":"not_found"',);

			const transientError = await dssFailure(["dataset", "delete", "transient",], {
				env: cliEnv(url,),
			},);
			expect(transientError.code,).toBe(3,);
			expect(transientError.stderr,).toContain('"code":"transient"',);
		},);
	});

	it("explains unavailable Business Apps API root 404s", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/business-apps/",);
			res.statusCode = 404;
			res.statusMessage = "Not Found";
			res.end("Not Found: /dip/publicapi/business-apps/",);
		}, async (url,) => {
			const failure = await dssFailure(["business-app", "list",], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report.code,).toBe("not_found",);
			expect(report.resource,).toBe("business-app",);
			expect(report.action,).toBe("list",);
			const hint = String(report.hint,);
			expect(hint,).toContain("Business Apps API is not available",);
			expect(hint,).toContain("classic app commands",);
			expect(hint,).not.toContain("projectKey and object identifiers",);
			expect(String(report.error,),).toContain("Business Apps API is not available",);
		},);
	});

	it("emits HTTP request JSONL traces with --verbose", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [],);
		}, async (url,) => {
			const { stderr, } = await dss(["project", "list", "--verbose",], { env: cliEnv(url,), },);
			const events = stderr.trim().split("\n",).map((line,) =>
				JSON.parse(line,) as Record<string, unknown>
			);
			expect(events,).toContainEqual(expect.objectContaining({ type: "trace", phase: "request", },),);
			expect(events,).toContainEqual(
				expect.objectContaining({ type: "trace", phase: "response", status: 200, },),
			);
		},);
	});
});
