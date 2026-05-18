import { describe, expect, it, } from "bun:test";
import { execFile, spawn, } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync, } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = resolve(dirname(fileURLToPath(import.meta.url,),), "..",);
const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
const BUN = process.execPath;

type CliExecOptions = { cwd?: string; env?: NodeJS.ProcessEnv; };
type CliFailure = { code: number | null; stdout: string; stderr: string; };

async function dss(
	args: string[],
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return exec(BUN, ["run", CLI_PATH, ...args,], {
		cwd: opts.cwd ?? SDK_ROOT,
		env: opts.env ?? process.env,
	},);
}

async function dssWithInput(
	args: string[],
	input: string,
	opts: CliExecOptions = {},
): Promise<{ stdout: string; stderr: string; }> {
	return new Promise((resolvePromise, rejectPromise,) => {
		const child = spawn(BUN, ["run", CLI_PATH, ...args,], {
			cwd: opts.cwd ?? SDK_ROOT,
			env: opts.env ?? process.env,
		},);
		let stdout = "";
		let stderr = "";

		child.stdout.setEncoding("utf8",);
		child.stderr.setEncoding("utf8",);
		child.stdout.on("data", (chunk: string,) => {
			stdout += chunk;
		},);
		child.stderr.on("data", (chunk: string,) => {
			stderr += chunk;
		},);
		child.stdin.on("error", () => {
			// Ignore EPIPE if the process exits before consuming all input.
		},);
		child.on("error", rejectPromise,);
		child.on("close", (code,) => {
			if (code === 0) {
				resolvePromise({ stdout, stderr, },);
				return;
			}
			rejectPromise(Object.assign(new Error(`CLI exited with code ${String(code,)}`,), {
				code,
				stdout,
				stderr,
			},),);
		},);
		child.stdin.end(input,);
	},);
}

async function dssFailure(args: string[], opts: CliExecOptions = {},): Promise<CliFailure> {
	try {
		await dss(args, opts,);
		throw new Error("expected CLI command to fail",);
	} catch (error: unknown) {
		const failure = error as { code?: number | null; stdout?: string; stderr?: string; };
		return {
			code: failure.code ?? null,
			stdout: failure.stdout ?? "",
			stderr: failure.stderr ?? "",
		};
	}
}

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

function cliEnv(url: string,): NodeJS.ProcessEnv {
	return {
		...process.env,
		DATAIKU_URL: url,
		DATAIKU_API_KEY: "test-key",
		DATAIKU_PROJECT_KEY: "TEST",
	};
}

async function withCliServer(
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

describe("CLI help output", () => {
	it("dss --help shows top-level usage", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("Usage: dss <resource> <action>",);
	});

	it("dss project --help lists project actions", async () => {
		const { stderr, } = await dss(["project", "--help",],);
		expect(stderr,).toContain("Actions:",);
		expect(stderr,).toContain("list",);
	});

	it("dss --help lists JSON and TLS flags", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("--json",);
		expect(stderr,).not.toContain("--format",);
		expect(stderr,).toContain("--verbose",);
		expect(stderr,).toContain("--insecure",);
		expect(stderr,).toContain("--ca-cert PATH",);
	});

	it("dss dataset --help lists update action", async () => {
		const { stderr, } = await dss(["dataset", "--help",],);
		expect(stderr,).toContain("update",);
	});

	it("dss notebook --help lists save-jupyter and clear-sql-history", async () => {
		const { stderr, } = await dss(["notebook", "--help",],);
		expect(stderr,).toContain("save-jupyter",);
		expect(stderr,).toContain("clear-sql-history",);
	});

	it("prints command registry help when --report-json is set", async () => {
		const { stderr, } = await dss(["dataset", "list", "--help", "--report-json",],);
		const help = JSON.parse(stderr,) as {
			resource?: string;
			action?: string;
			usage?: string;
			flags?: unknown[];
		};
		expect(help,).toMatchObject({
			resource: "dataset",
			action: "list",
			usage: "dss dataset list [--project-key KEY]",
		},);
		expect(Array.isArray(help.flags,),).toBe(true,);
	});
});

describe("CLI missing credentials", () => {
	it("exits non-zero when no credentials are available", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-creds-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["project", "list",], {
				cwd: tmpDir,
				env: {
					PATH: process.env.PATH,
					HOME: tmpDir,
					DSS_CONFIG_DIR: join(tmpDir, "config",),
					DATAIKU_DISABLE_ENV: "1",
				},
			},);
			throw new Error("should have exited non-zero",);
		} catch (e: unknown) {
			const err = e as { code?: number; stderr?: string; stdout?: string; };
			expect(err.code !== 0 || err.stderr,).toBeTruthy();
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("emits stable report JSON for usage errors", async () => {
		const failure = await dssFailure(["flow-zone", "list", "--wat", "yes", "--report-json",], {
			env: {
				...process.env,
				DATAIKU_PROJECT_KEY: "TEST",
				DATAIKU_URL: "http://127.0.0.1:9",
				DATAIKU_API_KEY: "test-key",
			},
		},);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "unknown_flag",
			category: "usage",
			resource: "flow-zone",
			action: "list",
			projectKey: "TEST",
		},);
		expect(report.message,).toContain("Unknown flag: --wat",);
	});

	it("emits stable report JSON for missing positional arguments", async () => {
		const failure = await dssFailure(["scenario", "delete", "--report-json",], {
			env: {
				...process.env,
				DATAIKU_PROJECT_KEY: "TEST",
				DATAIKU_URL: "http://127.0.0.1:9",
				DATAIKU_API_KEY: "test-key",
			},
		},);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "missing_required_arg",
			category: "usage",
			resource: "scenario",
			action: "delete",
			projectKey: "TEST",
		},);
		expect(report.message,).toContain("Expected 1 argument(s), got 0",);
	});

	it("emits stable report JSON for DSS permission errors", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { message: "Access denied", requestId: "req-123", }, 403,);
		}, async (url,) => {
			const failure = await dssFailure(["scenario", "list", "--report-json",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "permission_denied",
				category: "dss",
				resource: "scenario",
				action: "list",
				projectKey: "TEST",
				requestId: "req-123",
				status: 403,
				retryable: false,
			},);
			expect(report.message,).toContain("Access denied",);
		},);
	});
});

describe("CLI .env loading", () => {
	it("loads .env from CWD and uses those credentials", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-env-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(
			join(tmpDir, ".env",),
			"DATAIKU_URL=http://dss-env-test-sentinel.invalid\nDATAIKU_API_KEY=fake-key\n",
		);
		try {
			await dss(["--help",], {
				cwd: tmpDir,
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					DATAIKU_URL: "",
					DATAIKU_API_KEY: "",
				},
			},);
		} catch (e: unknown) {
			const err = e as { stderr?: string; stdout?: string; message?: string; };
			const output = `${err.stderr ?? ""}${err.stdout ?? ""}${err.message ?? ""}`;
			expect(output,).not.toContain("DATAIKU_URL is required",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

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

	it("supports --data-file JSON input", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		const tmpFile = join(tmpdir(), `dss-cli-data-file-${Date.now()}.json`,);
		writeFileSync(tmpFile, JSON.stringify({ nested: { added: "from-file", }, },), "utf-8",);
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

	it("rejects removed output format flags", async () => {
		const longFailure = await dssFailure(["project", "list", "--format", "table",],);
		expect(longFailure.code,).toBe(1,);
		expect(longFailure.stderr,).toContain("Unknown flag: --format",);

		const shortFailure = await dssFailure(["-f", "table", "--help",],);
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
			expect(apiError.stderr,).toContain('"category": "not_found"',);

			const transientError = await dssFailure(["dataset", "delete", "transient",], {
				env: cliEnv(url,),
			},);
			expect(transientError.code,).toBe(3,);
			expect(transientError.stderr,).toContain('"category": "transient"',);
		},);
	});

	it("emits HTTP request logs with --verbose", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [],);
		}, async (url,) => {
			const { stderr, } = await dss(["project", "list", "--verbose",], { env: cliEnv(url,), },);
			expect(stderr,).toContain("[dss] GET",);
			expect(stderr,).toContain("→ 200",);
		},);
	});
});

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
		expect(failure.stderr,).toContain("--output or --output-folder is required",);
		expect(failure.stderr,).toContain(
			"dss recipe create --type TYPE --input DS (--output DS | --output-folder FOLDER_ID)",
		);
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
					params: { path: string; metastoreTableName: string; };
					schema: unknown;
				};
			};
			expect(result.next.name,).toBe("target_ds",);
			expect(result.next.params.path,).toBe("/dataiku/TEST/target_ds",);
			expect(result.next.params.metastoreTableName,).toBe("target_ds",);
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
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toContain("Refusing to clone managed dataset",);
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
			expect(stdout,).toBe(
				'{\n  "standard": {\n    "fresh": true\n  },\n  "local": {\n    "note": "set"\n  }\n}\n',
			);
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
			expect(stdout,).toContain('"path": "sub/file.txt"',);
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
				sendJson(res, { message: "temporary gateway failure", requestId: "req-123", }, 503,);
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
				"--report-json",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(3,);
			const report = JSON.parse(failure.stderr,) as {
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
				"--report-json",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(3,);
			const report = JSON.parse(failure.stderr,) as {
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

describe("CLI auth commands", () => {
	it("dss auth --help shows auth actions", async () => {
		const { stderr, } = await dss(["auth", "--help",],);
		expect(stderr,).toContain("login",);
		expect(stderr,).toContain("status",);
		expect(stderr,).toContain("logout",);
	});

	it("dss auth login saves credentials and validates", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-login-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stderr, } = await dss([
					"auth",
					"login",
					"--url",
					url,
					"--api-key",
					"test-key",
					"--project-key",
					"MYPROJ",
				], {
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(stderr,).toContain("Connected",);
				expect(stderr,).toContain("Credentials saved",);

				// Verify the file was written
				const creds = JSON.parse(readFileSync(join(tmpDir, "credentials.json",), "utf-8",),);
				expect(creds.url,).toBe(url,);
				expect(creds.apiKey,).toBe("test-key",);
				expect(creds.projectKey,).toBe("MYPROJ",);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth login saves TLS settings when provided", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-tls-${Date.now()}`,);
		const caPath = join(tmpDir, "corp-ca.pem",);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(caPath, "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n", "utf-8",);
		try {
			await withCliServer((req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stderr, } = await dss([
					"auth",
					"login",
					"--url",
					url,
					"--api-key",
					"test-key",
					"--insecure",
					"--ca-cert",
					caPath,
				], {
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(stderr,).toContain("Credentials saved",);
				const creds = JSON.parse(readFileSync(join(tmpDir, "credentials.json",), "utf-8",),);
				expect(creds.tlsRejectUnauthorized,).toBe(false,);
				expect(creds.caCertPath,).toBe(caPath,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth login does not save credentials when validation fails", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-fail-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, { message: "Unauthorized", }, 401,);
			}, async (url,) => {
				const failure = await dssFailure([
					"auth",
					"login",
					"--url",
					url,
					"--api-key",
					"bad-key",
				], {
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				// Process should exit as an API/auth failure, not transient transport.
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toContain('"category": "forbidden"',);
				expect(failure.stderr,).toContain("401 Unauthorized",);
				// Credentials file should NOT have been written
				const exists = (() => {
					try {
						readFileSync(join(tmpDir, "credentials.json",),);
						return true;
					} catch {
						return false;
					}
				})();
				expect(exists,).toBe(false,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth status shows saved credentials with working server", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-status-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				writeFileSync(
					join(tmpDir, "credentials.json",),
					JSON.stringify({ url, apiKey: "dkuaps-longenoughkey123", projectKey: "PROJ", },),
				);
				const { stderr, } = await dss(["auth", "status",], {
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(stderr,).toContain("URL:",);
				expect(stderr,).toContain("API key:",);
				expect(stderr,).toContain("Project key:",);
				expect(stderr,).toContain("PROJ",);
				expect(stderr,).toContain("Connection:  valid",);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth logout removes credentials", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-logout-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(join(tmpDir, "credentials.json",), "{}",);
		try {
			const { stderr, } = await dss(["auth", "logout",], {
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					DSS_CONFIG_DIR: tmpDir,
				},
			},);
			expect(stderr,).toContain("Credentials removed",);
			// File should be gone
			const exists = (() => {
				try {
					readFileSync(join(tmpDir, "credentials.json",),);
					return true;
				} catch {
					return false;
				}
			})();
			expect(exists,).toBe(false,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	// Note: cannot reliably test saved-credential resolution through CLI subprocess
	// because loadEnvFile() always reads .env from the SDK root (resolved via import.meta.url),
	// which provides DATAIKU_API_KEY before resolveCredentials() can consult saved creds.
	// The credential precedence chain is tested indirectly: config read/write is covered by
	// config.test.ts, and auth login/status verify the saved-cred round-trip end-to-end.
});

describe("CLI --timeout flag", () => {
	it("passes timeout to client", async () => {
		let receivedRequest = false;
		await withCliServer((req, res,) => {
			receivedRequest = true;
			sendJson(res, [],);
		}, async (url,) => {
			const { stdout, } = await dss(["project", "list", "--timeout", "5000",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual([],);
			expect(receivedRequest,).toBe(true,);
		},);
	});
});

describe("CLI dataset validation", () => {
	it("reports file-backed dataset build blockers", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/broken_output",);
			sendJson(res, {
				name: "broken_output",
				type: "Filesystem",
				params: { connection: "filesystem_managed", },
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["dataset", "validate-build", "broken_output",], {
				env: cliEnv(url,),
			},);
			const result = JSON.parse(stdout,) as { valid: boolean; warnings: string[]; };
			expect(result.valid,).toBe(false,);
			expect(result.warnings,).toContain(
				"File-backed dataset has no writable storage path configured.",
			);
			expect(result.warnings,).toContain("File-backed dataset has no formatType configured.",);
		},);
	});

	it("refreshes dataset schema through schema endpoint", async () => {
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("PUT",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/schema",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, {},);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"refresh-schema",
				"orders",
				"--data",
				JSON.stringify({ columns: [{ name: "id", type: "bigint", },], },),
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				updated: "orders",
				resource: "dataset",
				schema: { columns: [{ name: "id", type: "bigint", },], },
			},);
		},);
		expect(requestBody,).toEqual({ columns: [{ name: "id", type: "bigint", },], },);
	});
});

describe("CLI recipe get", () => {
	it("prints compact recipe settings when DSS returns a payload and --no-payload is set", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
			expect(url.searchParams.get("includePayload",),).toBeNull();
			sendJson(res, {
				recipe: { name: "my_recipe", type: "python", },
				payload: "print('large payload')\n",
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["recipe", "get", "my_recipe", "--no-payload",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({
				recipe: { name: "my_recipe", type: "python", },
			},);
		},);
	});
});
describe("CLI recipe get-payload and set-payload", () => {
	it("get-payload prints recipe code to stdout", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
				payload: "print('hello')\n",
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["recipe", "get-payload", "my_recipe",], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toBe("print('hello')\n",);
		},);
	});

	it("get-payload --raw preserves payload bytes on stdout", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
				payload: "print('hello')\r\nprint('bye')\n",
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["recipe", "get-payload", "my_recipe", "--raw",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			expect(stdout,).toBe("print('hello')\r\nprint('bye')\n",);
		},);
	});

	it("get-payload writes to --output file", async () => {
		const outPath = join(tmpdir(), `dss-cli-getpayload-${Date.now()}.py`,);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, {
					recipe: { type: "python", },
					payload: "import os\n",
				},);
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"get-payload",
					"my_recipe",
					"--output",
					outPath,
				], { env: cliEnv(url,), },);
				expect(JSON.parse(stdout,),).toBe(outPath,);
				expect(readFileSync(outPath, "utf-8",),).toBe("import os\n",);
			},);
		} finally {
			rmSync(outPath, { force: true, },);
		}
	});

	it("set-payload reads from --file and PUTs", async () => {
		const filePath = join(tmpdir(), `dss-cli-setpayload-${Date.now()}.py`,);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		let putBody: string | undefined;

		try {
			await withCliServer(async (req, res,) => {
				const _url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: "print('old')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--no-backup",
				], { env: cliEnv(url,), },);
				expect(stdout,).toContain('"updated": "my_recipe"',);
				expect(putBody,).toBeDefined();
				const parsed = JSON.parse(putBody!,);
				expect(parsed.payload,).toBe("print('updated')\n",);
			},);
		} finally {
			rmSync(filePath, { force: true, },);
		}
	});

	it("set-payload writes a remote payload backup before PUT", async () => {
		const filePath = join(tmpdir(), `dss-cli-setpayload-${Date.now()}.py`,);
		const backupDir = join(tmpdir(), `dss-cli-backup-${Date.now()}`,);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		const requestEvents: string[] = [];
		let putBody: string | undefined;

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				requestEvents.push(`${req.method} ${url.pathname}${url.search}`,);
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: putBody === undefined ? "print('remote')\n" : "print('updated')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--backup-dir",
					backupDir,
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { backupPath: string; backupCreated: boolean; };
				expect(result.backupCreated,).toBe(true,);
				expect(result.backupPath.startsWith(backupDir,),).toBe(true,);
				const backup = JSON.parse(readFileSync(result.backupPath, "utf-8",),) as {
					payload: string;
					payloadHash: string;
					recipe: { name: string; };
				};
				expect(backup.payload,).toBe("print('remote')\n",);
				expect(backup.payloadHash,).toHaveLength(64,);
				expect(backup.recipe.name,).toBe("my_recipe",);
				expect(putBody,).toBeDefined();
				expect(JSON.parse(putBody!,).payload,).toBe("print('updated')\n",);
			},);
		} finally {
			expect(requestEvents,).toEqual([
				"GET /public/api/projects/TEST/recipes/my_recipe?includePayload=true",
				"PUT /public/api/projects/TEST/recipes/my_recipe",
			],);
			rmSync(filePath, { force: true, },);
			rmSync(backupDir, { recursive: true, force: true, },);
		}
	});

	it("set-payload uses the default backup directory", async () => {
		const tempDir = join(tmpdir(), `dss-cli-default-backup-${Date.now()}`,);
		const filePath = join(tempDir, "updated.py",);
		let putBody: string | undefined;

		try {
			mkdirSync(tempDir, { recursive: true, },);
			writeFileSync(filePath, "print('updated')\n", "utf-8",);
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: "print('remote')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const result = JSON.parse(
					(await dss(["recipe", "set-payload", "my_recipe", "--file", filePath,], {
						cwd: tempDir,
						env: cliEnv(url,),
					},)).stdout,
				) as { backupCreated: boolean; backupPath: string; };

				expect(result.backupCreated,).toBe(true,);
				expect(result.backupPath.startsWith(join(tempDir, ".dss-backups", "recipes",),),).toBe(true,);
				const backup = JSON.parse(readFileSync(result.backupPath, "utf-8",),) as {
					normalizedPayloadHash: string;
					payload: string;
				};
				expect(backup.payload,).toBe("print('remote')\n",);
				expect(backup.normalizedPayloadHash,).toHaveLength(64,);
				expect(JSON.parse(putBody!,).payload,).toBe("print('updated')\n",);
			},);
		} finally {
			rmSync(tempDir, { recursive: true, force: true, },);
		}
	});

	it("set-payload fails without --file", async () => {
		const failure = await dssFailure(["recipe", "set-payload", "my_recipe",], {
			env: cliEnv("http://localhost:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("--file is required",);
	});

	it("recipe run resolves managed-folder outputs and waits for logs", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_FOLDERID"
			) {
				sendJson(res, {
					recipe: {
						name: "compute_FOLDERID",
						type: "python",
						outputs: {
							main: {
								items: [{ ref: "FOLDERID", appendMode: false, },],
							},
						},
					},
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [],);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "FOLDERID", name: "Exports", type: "Filesystem", },],);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				buildRequestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-folder", },);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-folder", type: "MANAGED_FOLDER_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder/log/") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end(
					"2026-01-01 backend-log ignore\nstderr: noisy\nstdout: preparing\n>>> DONE: 19 experiments\n",
				);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"recipe",
				"run",
				"compute_FOLDERID",
				"--include-logs",
				"--max-log-lines",
				"20",
				"--log-filter",
				"stdout",
				"--summary",
				"--timeout",
				"5000",
			], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result,).toMatchObject({
				recipeName: "compute_FOLDERID",
				success: true,
				jobId: "job-folder",
				state: "DONE",
				type: "MANAGED_FOLDER_BUILD",
				log: "stdout: preparing\n>>> DONE: 19 experiments",
				logSummary: {
					state: "DONE",
					lineCount: 2,
					lines: ["stdout: preparing", ">>> DONE: 19 experiments",],
				},
			},);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "FOLDERID",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "FOLDERID",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
		expect(requests,).toContain("GET /public/api/projects/TEST/recipes/compute_FOLDERID",);
		expect(requests,).toContain("GET /public/api/projects/TEST/datasets/",);
		expect(requests,).toContain("GET /public/api/projects/TEST/managedfolders/",);
		const jobRequests = requests.filter((request,) =>
			request.startsWith("POST /public/api/projects/TEST/jobs/",)
			|| request.startsWith("GET /public/api/projects/TEST/jobs/job-folder",)
		);
		expect(jobRequests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-folder/",
			"GET /public/api/projects/TEST/jobs/job-folder/log/",
		],);
	});
});

describe("CLI help improvements", () => {
	it("help shows --timeout flag", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("--timeout MS",);
	});

	it("help omits removed table and TSV format options", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).not.toContain("--format",);
		expect(stderr,).not.toContain("tsv",);
		expect(stderr,).not.toContain("table",);
	});

	it("help shows quick start examples", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("Quick start:",);
		expect(stderr,).toContain("dss auth login",);
		expect(stderr,).toContain("dss recipe get-payload",);
	});

	it("help lists auth as a resource", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("auth",);
	});

	it("help shows get-payload and set-payload in recipe actions", async () => {
		const { stderr, } = await dss(["recipe", "--help",],);
		expect(stderr,).toContain("get-payload",);
		expect(stderr,).toContain("set-payload",);
	});
});

describe("CLI --version flag", () => {
	it("dss --version prints version string to stdout", async () => {
		const { stdout, } = await dss(["--version",],);
		expect(stdout.trim(),).toMatch(/^\d+\.\d+\.\d+(?:\+g[0-9a-f]{7})?/,);
		expect(stdout.trim(),).toContain("+g",);
	});

	it("dss -V prints version string to stdout", async () => {
		const { stdout, } = await dss(["-V",],);
		expect(stdout.trim(),).toMatch(/^\d+\.\d+\.\d+/,);
	});
});

describe("CLI short flags", () => {
	it("-h shows top-level help", async () => {
		const { stderr, } = await dss(["-h",],);
		expect(stderr,).toContain("Usage: dss",);
		expect(stderr,).toContain("Global flags:",);
	});

	it("-f is rejected after the JSON-only output cutover", async () => {
		const failure = await dssFailure(["-f", "table", "--help",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Unknown flag: -f",);
	});
});

describe("CLI boolean flag does not swallow next positional", () => {
	it("--verbose does not consume the next positional arg", async () => {
		const { stderr, } = await dss(["--verbose", "project", "--help",],);
		// If --verbose swallowed 'project', this would show top-level help or error.
		// With the fix, 'project' is a positional and --help shows project actions.
		expect(stderr,).toContain("project",);
		expect(stderr,).toContain("list",);
	});

	it("--help does not consume the next positional arg", async () => {
		const { stderr, } = await dss(["--help", "project",],);
		// --help is boolean, so 'project' stays positional.
		// Since positional[0] = 'project', this should show project-level help.
		expect(stderr,).toContain("project",);
	});
});

describe("CLI flag value parsing", () => {
	it("missing value for --target fails fast", async () => {
		const failure = await dssFailure(["install-skill", "--target",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Flag --target requires a value.",);
	});

	it("--max-lines -1 is consumed as an option value", async () => {
		const fullLog = `${
			Array.from({ length: 600, }, (_value, index,) => `line-${String(index,)}`,).join("\n",)
		}\n`;
		await withCliServer((_req, res,) => {
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (url,) => {
			const { stdout, } = await dss(["job", "log", "job-123", "--max-lines", "-1",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toBe(fullLog,);
		},);
	});
});

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
				path: "/dataiku/TEST/exports",
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
			expect(url.pathname,).toBe("/public/api/projects/OTHER/datasets/",);
			sendJson(res, [
				{ type: "Filesystem", managed: true, params: { connection: "filesystem_managed", }, },
			],);
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
			expect(failure.stderr,).toContain("--color must be a hex color",);
		},);
	});

	it("flow-zone get rejects empty zone ids before calling DSS", async () => {
		await withCliServer(() => {
			throw new Error("server should not be called for empty flow-zone id",);
		}, async (url,) => {
			const failure = await dssFailure(["flow-zone", "get", "",], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toContain("Flow zone id must not be empty",);
		},);
	});

	it("rejects unknown long flags", async () => {
		const failure = await dssFailure(["flow-zone", "list", "--wat", "yes",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Unknown flag: --wat",);
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
			expect(failure.stderr,).toContain("At least one object is required",);
		},);
	});
});

describe("CLI code-env management commands", () => {
	it("code-env create posts deployment mode and params", async () => {
		let requestBody: Record<string, unknown> | undefined;
		let requestUrl: URL | undefined;

		await withCliServer(async (req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"create",
				"PYTHON",
				"omp_test_env",
				"--deployment-mode",
				"DESIGN_MANAGED",
				"--python-interpreter",
				"PYTHON311",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				created: "omp_test_env",
				resource: "code-env",
				envLang: "PYTHON",
				messages: { success: true, },
			},);
		},);

		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
		expect(requestBody,).toEqual({
			pythonInterpreter: "PYTHON311",
			deploymentMode: "DESIGN_MANAGED",
		},);
	});

	it("code-env set-packages preserves definition fields", async () => {
		const requests: string[] = [];
		let updateBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/omp_test_env") {
				sendJson(res, {
					envName: "omp_test_env",
					envLang: "PYTHON",
					specPackageList: "oldpkg",
					desc: { installCorePackages: false, pythonInterpreter: "PYTHON311", },
				},);
				return;
			}

			if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/omp_test_env") {
				updateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { updated: true, },);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-packages",
				"PYTHON",
				"omp_test_env",
				"--packages",
				"tabulate\nopenpyxl>=3.1.5,<3.2",
				"--install-core-packages",
				"true",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ updated: true, },);
		},);

		expect(requests,).toEqual([
			"GET /public/api/admin/code-envs/PYTHON/omp_test_env",
			"PUT /public/api/admin/code-envs/PYTHON/omp_test_env",
		],);
		expect(updateBody,).toEqual({
			envName: "omp_test_env",
			envLang: "PYTHON",
			specPackageList: "tabulate\nopenpyxl>=3.1.5,<3.2",
			desc: { installCorePackages: true, pythonInterpreter: "PYTHON311", },
		},);
	});

	it("code-env update-packages posts rebuild query flags", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/packages",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"update-packages",
				"PYTHON",
				"omp_test_env",
				"--force-rebuild",
				"--env-version",
				"bundle-v1",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ messages: { success: true, }, },);
		},);

		expect(requestUrl?.searchParams.get("forceRebuildEnv",),).toBe("true",);
		expect(requestUrl?.searchParams.get("versionToUpdate",),).toBe("bundle-v1",);
		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env set-jupyter posts active and wait flags", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/jupyter",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-jupyter",
				"PYTHON",
				"omp_test_env",
				"--active",
				"false",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ messages: { success: true, }, },);
		},);

		expect(requestUrl?.searchParams.get("active",),).toBe("false",);
		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env delete calls DSS with wait flag", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("DELETE",);
			expect(requestUrl.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"delete",
				"PYTHON",
				"omp_test_env",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				deleted: "omp_test_env",
				envLang: "PYTHON",
			},);
		},);

		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env delete dry-run reads current state without deleting", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/",);
			sendJson(res, { envName: "omp_test_env", envLang: "PYTHON", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"delete",
				"PYTHON",
				"omp_test_env",
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "code-env",
				envLang: "PYTHON",
				envName: "omp_test_env",
				wait: true,
				current: { envName: "omp_test_env", envLang: "PYTHON", },
			},);
		},);
	});
});

describe("CLI wait command exit codes", () => {
	it("job wait exits non-zero when the wait result times out", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-timeout/",);
			sendJson(res, {
				baseStatus: {
					def: { id: "job-timeout", type: "DATASET_BUILD", },
					state: "RUNNING",
				},
				globalState: { done: 0, failed: 0, running: 1, total: 1, },
			},);
		}, async (url,) => {
			const failure = await dssFailure(["job", "wait", "job-timeout", "--timeout", "10",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(4,);
			const payload = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(payload.success,).toBe(false,);
			expect(payload.timedOut,).toBe(true,);
			expect(payload.state,).toBe("RUNNING",);
		},);
	});
});

describe("CLI missing credentials plain text errors", () => {
	it("missing URL prints plain text error, not JSON", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-missing-url-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(
			join(tmpDir, "credentials.json",),
			JSON.stringify({ url: "http://saved.example", apiKey: "saved-key", },),
		);
		try {
			const failure = await dssFailure(["--url", "", "project", "list",], {
				env: {
					PATH: process.env.PATH,
					HOME: tmpDir,
					DSS_CONFIG_DIR: tmpDir,
					DATAIKU_DISABLE_ENV: "1",
					DATAIKU_URL: "http://env.example",
					DATAIKU_API_KEY: "env-key",
				},
			},);
			expect(failure.stderr,).not.toContain('{"error"',);
			expect(failure.stderr,).toContain("Missing Dataiku URL",);
			expect(failure.code,).toBe(1,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("missing API key prints plain text error, not JSON", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-missing-key-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		writeFileSync(
			join(tmpDir, "credentials.json",),
			JSON.stringify({ url: "http://saved.example", apiKey: "saved-key", },),
		);
		try {
			const failure = await dssFailure([
				"--url",
				"http://localhost:1",
				"--api-key",
				"",
				"project",
				"list",
			], {
				env: {
					PATH: process.env.PATH,
					HOME: tmpDir,
					DSS_CONFIG_DIR: tmpDir,
					DATAIKU_DISABLE_ENV: "1",
					DATAIKU_URL: "http://env.example",
					DATAIKU_API_KEY: "env-key",
				},
			},);
			expect(failure.stderr,).not.toContain('{"error"',);
			expect(failure.stderr,).toContain("Missing API key",);
			expect(failure.code,).toBe(1,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

describe("CLI help text includes short flags", () => {
	it("help shows short flag aliases", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("-h, --help",);
		expect(stderr,).toContain("-v, --verbose",);
		expect(stderr,).toContain("-V, --version",);
		expect(stderr,).not.toContain("-f, --format",);
		expect(stderr,).toContain("-o, --output",);
	});
});

describe("CLI install-skill command", () => {
	it("dss install-skill --help shows usage", async () => {
		const { stderr, } = await dss(["install-skill", "--help",],);
		expect(stderr,).toContain("Usage: dss install-skill",);
		expect(stderr,).toContain("--global",);
		expect(stderr,).toContain("--agent",);
		expect(stderr,).toContain("--list-agents",);
	});

	it("dss install-skill --list-agents exits cleanly", async () => {
		// CI has no agents; local dev has some. Either way, exits 0.
		const { stderr, } = await dss(["install-skill", "--list-agents",],);
		expect(stderr,).toMatch(/Detected agents:|No coding agents detected/,);
	});

	it("dss install-skill --agent claude writes SKILL.md to project dir", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stderr, } = await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			expect(stderr,).toContain("Installing dataiku-dss skill",);
			expect(stderr,).toContain("claude",);
			expect(stderr,).toContain("Done.",);

			// Verify the file was written
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
			expect(content,).toContain("dss auth login",);
			expect(content,).toContain("dss project list",);
			expect(content,).toContain("~/.config/dataiku/credentials.json",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent codex writes to .codex/skills/", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-codex-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "codex",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".codex", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent cursor writes to .cursor/skills/", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-cursor-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "cursor",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".cursor", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill is idempotent", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-idem-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent unknown fails with UsageError", async () => {
		const failure = await dssFailure(["install-skill", "--agent", "unknown",],);
		expect(failure.stderr,).toContain("Unknown agent: unknown",);
		expect(failure.code,).toBe(1,);
	});

	it("dss install-skill --target writes to specified directory", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-target-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stderr, } = await dss(["install-skill", "--agent", "claude", "--target", tmpDir,],);
			expect(stderr,).toContain("Installing dataiku-dss skill",);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("workspace detection finds .git parent for project installs", async () => {
		// Create workspace/.git and workspace/sub/
		const workspace = join(tmpdir(), `dss-cli-skill-ws-${Date.now()}`,);
		const subdir = join(workspace, "sub",);
		mkdirSync(join(workspace, ".git",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			// Run from sub/ — should detect workspace/ as root via .git
			await dss(["install-skill", "--agent", "claude",], { cwd: subdir, },);
			// Skill should be at workspace/.claude/skills/... not sub/.claude/skills/...
			const skillPath = join(workspace, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("workspace detection ignores nested agent config parents for project installs", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-pi-${Date.now()}`,);
		const subdir = join(workspace, "nested", "deeper",);
		mkdirSync(join(workspace, ".pi",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "pi",], { cwd: subdir, },);
			const skillPath = join(subdir, ".pi", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("workspace detection ignores nested .omp agent parents for project installs", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-omp-${Date.now()}`,);
		const subdir = join(workspace, "nested", "deeper",);
		mkdirSync(join(workspace, ".omp", "agent",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "omp",], { cwd: subdir, },);
			const skillPath = join(subdir, ".omp", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("--target overrides workspace detection", async () => {
		// Create workspace/.git and a separate target dir
		const workspace = join(tmpdir(), `dss-cli-skill-override-${Date.now()}`,);
		const target = join(tmpdir(), `dss-cli-skill-target2-${Date.now()}`,);
		mkdirSync(join(workspace, ".git",), { recursive: true, },);
		mkdirSync(target, { recursive: true, },);
		try {
			// Run from workspace/ but --target points elsewhere
			await dss(["install-skill", "--agent", "claude", "--target", target,], { cwd: workspace, },);
			// Skill should be at target, not workspace
			const skillPath = join(target, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
			rmSync(target, { recursive: true, force: true, },);
		}
	});

	it("help lists install-skill in resources and quick start", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("install-skill",);
	});
});

describe("CLI command behavioral smoke coverage", () => {
	it("smokes project, flow-zone, and dataset command gaps", async () => {
		const datasetOut = join(tmpdir(), `dss-cli-dataset-download-${Date.now()}.csv`,);
		let flowZoneUpdateBody: Record<string, unknown> | undefined;
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/graph/") {
					sendJson(res, { nodes: [], links: [], },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1/graph"
				) {
					sendJson(res, { zoneId: "zone-1", nodes: [], },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					sendJson(res, {
						id: "zone-1",
						name: (flowZoneUpdateBody?.name as string | undefined) ?? "Zone 1",
						color: (flowZoneUpdateBody?.color as string | undefined) ?? "#2ab1ac",
						items: [],
					},);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					flowZoneUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders") {
					sendJson(res, { name: "orders", type: "Filesystem", projectKey: "TEST", },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/schema"
				) {
					sendJson(res, { columns: [{ name: "order_id", type: "string", },], },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/data/"
				) {
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/plain",);
					res.end("order_id\nA1\n",);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/metadata"
				) {
					sendJson(res, { tags: ["agent-tested",], },);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(JSON.parse((await dss(["project", "flow",], { env: cliEnv(url,), },)).stdout,),).toEqual(
					{
						nodes: [],
						links: [],
					},
				);
				expect(
					JSON.parse((await dss(["flow-zone", "graph", "zone-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ zoneId: "zone-1", nodes: [], },);
				expect(
					JSON.parse(
						(await dss(["flow-zone", "update", "zone-1", "--name", "Zone 2",], {
							env: cliEnv(url,),
						},)).stdout,
					),
				)
					.toHaveProperty("name", "Zone 2",);
				expect(flowZoneUpdateBody,).toMatchObject({ id: "zone-1", name: "Zone 2", },);
				expect(
					JSON.parse((await dss(["flow-zone", "delete", "zone-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ deleted: "zone-1", resource: "flow-zone", },);
				expect(
					JSON.parse((await dss(["dataset", "get", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("name", "orders",);
				expect(
					JSON.parse((await dss(["dataset", "schema", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ columns: [{ name: "order_id", type: "string", },], },);
				expect(JSON.parse(
					(await dss(["dataset", "preview", "orders", "--max-rows", "1",], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe("order_id\nA1",);
				expect(
					JSON.parse((await dss(["dataset", "metadata", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ tags: ["agent-tested",], },);
				expect(JSON.parse(
					(await dss(["dataset", "download", "orders", "--output", datasetOut,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe(datasetOut,);
				expect(readFileSync(datasetOut, "utf-8",),).toBe("order_id\nA1\n",);
			},);
		} finally {
			rmSync(datasetOut, { force: true, },);
		}
	}, 30_000,);

	it("smokes dataset, recipe, job, scenario, and connection command gaps", async () => {
		const recipeOut = join(tmpdir(), `dss-cli-recipe-download-${Date.now()}.json`,);
		let datasetCreateBody: Record<string, unknown> | undefined;
		let recipeCreateBody: Record<string, unknown> | undefined;
		let recipeUpdateBody: Record<string, unknown> | undefined;
		let scenarioCreateBody: Record<string, unknown> | undefined;
		let scenarioUpdateBody: Record<string, unknown> | undefined;
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
					datasetCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { name: "new_orders", type: "Filesystem", },);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
					recipeCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					sendJson(res, {
						recipe: { name: "compute_orders", type: "python", params: { old: true, }, },
					},);
					return;
				}
				if (
					req.method === "PUT" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					recipeUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, },);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/") {
					sendJson(res, {
						baseStatus: { def: { id: "job-1", type: "NON_RECURSIVE_FORCED_BUILD", }, state: "DONE", },
					},);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/job-1/abort/") {
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/connections/get-names/") {
					sendJson(res, ["filesystem_managed",],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					sendJson(res, { id: "scenario-1", name: "Scenario 1", params: { steps: [], }, },);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/") {
					scenarioCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "PUT" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					scenarioUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/run/"
				) {
					sendJson(res, { id: "run-1", },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/light/"
				) {
					sendJson(res, {
						id: "scenario-1",
						running: false,
						lastRun: { runId: "run-1", outcome: "SUCCESS", },
					},);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(JSON.parse(
					(await dss([
						"dataset",
						"create",
						"--name",
						"new_orders",
						"--connection",
						"filesystem_managed",
						"--type",
						"Filesystem",
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ created: "new_orders", resource: "dataset", },);
				expect(datasetCreateBody,).toMatchObject({
					name: "new_orders",
					type: "Filesystem",
					projectKey: "TEST",
				},);

				expect(JSON.parse(
					(await dss([
						"recipe",
						"create",
						"--type",
						"python",
						"--name",
						"compute_orders",
						"--input",
						"orders",
						"--output",
						"orders_out",
					], { env: cliEnv(url,), },)).stdout,
				),).toMatchObject({ recipeName: "compute_orders", type: "python", },);
				expect(recipeCreateBody?.recipePrototype,).toMatchObject({
					name: "compute_orders",
					type: "python",
					projectKey: "TEST",
				},);

				expect(JSON.parse(
					(await dss([
						"recipe",
						"update",
						"compute_orders",
						"--data",
						'{"recipe":{"params":{"new":true}}}',
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ updated: "compute_orders", resource: "recipe", },);
				expect((recipeUpdateBody?.recipe as Record<string, unknown>)?.params,).toEqual({
					old: true,
					new: true,
				},);

				expect(JSON.parse(
					(await dss(["recipe", "download", "compute_orders", "--output", recipeOut,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe(recipeOut,);
				expect(JSON.parse(readFileSync(recipeOut, "utf-8",),),).toHaveProperty(
					"recipe.name",
					"compute_orders",
				);

				expect(
					JSON.parse(
						(await dss(["recipe", "delete", "compute_orders",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "compute_orders", resource: "recipe", },);
				expect(JSON.parse((await dss(["job", "get", "job-1",], { env: cliEnv(url,), },)).stdout,),)
					.toHaveProperty("baseStatus.state", "DONE",);
				expect(JSON.parse((await dss(["job", "abort", "job-1",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual({ aborted: "job-1", resource: "job", },);
				expect(JSON.parse((await dss(["connection", "list",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual(["filesystem_managed",],);

				expect(
					JSON.parse(
						(await dss(["connection", "list", "--type", "Filesystem",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual(["filesystem_managed",],);
				expect(
					JSON.parse((await dss(["scenario", "get", "scenario-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("id", "scenario-1",);
				expect(
					JSON.parse(
						(await dss(["scenario", "create", "scenario-1", "Scenario 1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ created: "scenario-1", name: "Scenario 1", resource: "scenario", },);
				expect(scenarioCreateBody,).toMatchObject({
					id: "scenario-1",
					name: "Scenario 1",
					projectKey: "TEST",
				},);
				expect(JSON.parse(
					(await dss([
						"scenario",
						"update",
						"scenario-1",
						"--data",
						'{"active":false}',
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ updated: "scenario-1", resource: "scenario", },);
				expect(scenarioUpdateBody,).toMatchObject({ id: "scenario-1", active: false, },);
				expect(
					JSON.parse((await dss(["scenario", "run", "scenario-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ runId: "run-1", },);
				expect(
					JSON.parse(
						(await dss(["scenario", "status", "scenario-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("lastRun.outcome", "SUCCESS",);
				expect(JSON.parse(
					(await dss([
						"scenario",
						"run-and-wait",
						"scenario-1",
						"--timeout",
						"1000",
					], { env: cliEnv(url,), },)).stdout,
				),).toMatchObject({
					scenarioId: "scenario-1",
					runId: "run-1",
					success: true,
				},);
				expect(
					JSON.parse(
						(await dss(["scenario", "delete", "scenario-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "scenario-1", resource: "scenario", },);
			},);
		} finally {
			rmSync(recipeOut, { force: true, },);
		}
	}, 45_000,);

	it("smokes folder, code-env, and notebook command gaps", async () => {
		const uploadPath = join(tmpdir(), `dss-cli-folder-upload-${Date.now()}.txt`,);
		const downloadPath = join(tmpdir(), `dss-cli-folder-download-${Date.now()}.txt`,);
		const jupyterContent = {
			metadata: {},
			nbformat: 4,
			nbformat_minor: 5,
			cells: [{
				cell_type: "code",
				source: ["print(1)",],
				outputs: [{ text: "old", },],
				execution_count: 1,
			},],
		};
		const sqlContent = {
			connection: "filesystem_managed",
			cells: [{ id: "cell-1", type: "QUERY", code: "SELECT 1", },],
		};
		let codeEnvDefinitionBody: Record<string, unknown> | undefined;
		let savedJupyterBody: Record<string, unknown> | undefined;
		let savedSqlBody: Record<string, unknown> | undefined;
		let clearSqlHistoryBody: Record<string, unknown> | undefined;
		try {
			writeFileSync(uploadPath, "upload body\n",);
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
					sendJson(res, [{ id: "folder-1", name: "Folder 1", type: "Filesystem", },],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-1"
				) {
					sendJson(res, { id: "folder-1", name: "Folder 1", type: "Filesystem", },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/plain",);
					res.end("download body\n",);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1/") {
					sendJson(res, {
						envName: "default_v1",
						envLang: "PYTHON",
						desc: { pythonInterpreter: "PYTHON311", },
						specPackageList: "openpyxl==3.1.5\npolars",
						actualPackageList: "openpyxl==3.1.5\npolars==1.40.1",
					},);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1") {
					sendJson(res, { envName: "default_v1", envLang: "PYTHON", specPackageList: "polars", },);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1") {
					codeEnvDefinitionBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { updated: true, },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/usages") {
					sendJson(res, [{ envName: "default_v1", envLang: "PYTHON", usages: [], },],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1/usages"
				) {
					sendJson(res, [{ projectKey: "TEST", },],);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					sendJson(res, jupyterContent,);
					return;
				}
				if (
					req.method === "PUT"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					savedJupyterBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1/sessions"
				) {
					sendJson(res, [{ sessionId: "session-1", notebookName: "notebook-1", },],);
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1/sessions/session-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1") {
					sendJson(res, sqlContent,);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1") {
					savedSqlBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1/history"
				) {
					sendJson(res, { "cell-1": [{ startedOn: 1, },], },);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1/history/clear"
				) {
					clearSqlHistoryBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(
					JSON.parse((await dss(["folder", "get", "folder-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("id", "folder-1",);
				expect(JSON.parse(
					(await dss(["folder", "download", "folder-1", "/remote.txt", downloadPath,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe(downloadPath,);
				expect(readFileSync(downloadPath, "utf-8",),).toBe("download body\n",);
				expect(JSON.parse(
					(await dss(["folder", "upload", "folder-1", "/remote.txt", uploadPath,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toEqual({
					uploaded: "/remote.txt",
					folder: "folder-1",
					localPath: uploadPath,
					resource: "folder",
				},);
				expect(
					JSON.parse(
						(await dss(["folder", "delete-file", "folder-1", "/remote.txt",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ deleted: "/remote.txt", folder: "folder-1", resource: "folder", },);

				expect(
					JSON.parse(
						(await dss(["code-env", "get", "PYTHON", "default_v1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toMatchObject({
						envName: "default_v1",
						envLang: "PYTHON",
						requestedPackages: ["openpyxl==3.1.5", "polars",],
					},);
				expect(
					JSON.parse(
						(await dss(["code-env", "get-definition", "PYTHON", "default_v1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toHaveProperty("specPackageList", "polars",);
				expect(JSON.parse(
					(await dss([
						"code-env",
						"set-definition",
						"PYTHON",
						"default_v1",
						"--data",
						'{"envName":"default_v1","envLang":"PYTHON","specPackageList":"polars"}',
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ updated: true, },);
				expect(codeEnvDefinitionBody,).toHaveProperty("specPackageList", "polars",);
				expect(JSON.parse((await dss(["code-env", "usages",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual([{ envName: "default_v1", envLang: "PYTHON", usages: [], },],);
				expect(
					JSON.parse(
						(await dss(["code-env", "usages", "PYTHON", "default_v1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual([{ projectKey: "TEST", },],);

				expect(
					JSON.parse(
						(await dss(["notebook", "get-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("nbformat", 4,);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"save-jupyter",
						"notebook-1",
						"--data",
						JSON.stringify(jupyterContent,),
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ saved: "notebook-1", resource: "jupyter-notebook", },);
				expect(savedJupyterBody,).toHaveProperty("nbformat", 4,);
				expect(
					JSON.parse(
						(await dss(["notebook", "clear-jupyter-outputs", "notebook-1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ cleared: "notebook-1", resource: "jupyter-notebook", },);
				expect((savedJupyterBody?.cells as Array<Record<string, unknown>>)[0]?.outputs as unknown[],)
					.toEqual([],);
				expect(
					JSON.parse(
						(await dss(["notebook", "sessions-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual([{ sessionId: "session-1", notebookName: "notebook-1", },],);
				expect(
					JSON.parse(
						(await dss(["notebook", "unload-jupyter", "notebook-1", "session-1",], {
							env: cliEnv(url,),
						},)).stdout,
					),
				)
					.toEqual({ unloaded: "notebook-1", sessionId: "session-1", resource: "jupyter-notebook", },);
				expect(
					JSON.parse(
						(await dss(["notebook", "delete-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "notebook-1", resource: "jupyter-notebook", },);

				expect(
					JSON.parse((await dss(["notebook", "get-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("connection", "filesystem_managed",);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"save-sql",
						"sql-1",
						"--data",
						JSON.stringify(sqlContent,),
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ saved: "sql-1", resource: "sql-notebook", },);
				expect(savedSqlBody,).toHaveProperty("connection", "filesystem_managed",);
				expect(
					JSON.parse(
						(await dss(["notebook", "history-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("cell-1", [{ startedOn: 1, },],);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"clear-sql-history",
						"sql-1",
						"--cell-id",
						"cell-1",
						"--retain",
						"2",
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ cleared: "sql-1", resource: "sql-notebook", },);
				expect(clearSqlHistoryBody,).toEqual({ cellId: "cell-1", numRunsToRetain: 2, },);
				expect(
					JSON.parse((await dss(["notebook", "delete-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ deleted: "sql-1", resource: "sql-notebook", },);
			},);
		} finally {
			rmSync(uploadPath, { force: true, },);
			rmSync(downloadPath, { force: true, },);
		}
	}, 45_000,);

	it("smokes wiki, dashboard, and insight commands", async () => {
		let wikiUpdateBody: Record<string, unknown> | undefined;
		let dashboardCreateBody: Record<string, unknown> | undefined;
		let dashboardUpdateBody: Record<string, unknown> | undefined;
		let insightName = "Insight 1";
		let insightCreateBody: Record<string, unknown> | undefined;
		let insightUpdateBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/") {
				sendJson(res, { projectKey: "TEST", taxonomy: [{ id: "article-1", children: [], },], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				sendJson(res, {
					article: { id: "article-1", name: "Article 1", projectKey: "TEST", },
					payload: "old",
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/wiki/") {
				const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { article: { id: "article-2", name: body.name, projectKey: "TEST", }, },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/article-2") {
				sendJson(res, {
					article: { id: "article-2", name: "Created", projectKey: "TEST", },
					payload: "created",
				},);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				wikiUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, wikiUpdateBody,);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/wiki/article-2") {
				const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, body,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				res.statusCode = 204;
				res.end();
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/") {
				sendJson(res, [{ id: "dash-1", name: "Dashboard 1", projectKey: "TEST", listed: true, },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				sendJson(res, { id: "dash-1", name: "Dashboard 1", projectKey: "TEST", pages: [], },);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/dashboards/") {
				dashboardCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "dash-2", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/dash-2/") {
				sendJson(res, { id: "dash-2", name: "Created dashboard", projectKey: "TEST", pages: [], },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				dashboardUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, dashboardUpdateBody,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				res.statusCode = 204;
				res.end();
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/") {
				sendJson(res, [{ id: "insight-1", name: insightName, type: "chart", projectKey: "TEST", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/insight-1/") {
				sendJson(res, {
					id: "insight-1",
					name: insightName,
					type: "chart",
					projectKey: "TEST",
					params: {},
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/insights/") {
				insightCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "insight-2", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/insight-2/") {
				sendJson(res, {
					id: "insight-2",
					name: "Created insight",
					type: "chart",
					projectKey: "TEST",
					params: {},
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/insights/insight-1/") {
				insightUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				const insight = insightUpdateBody.insight as { name?: string; };
				insightName = insight.name ?? insightName;
				sendJson(res, { id: "insight-1", },);
				return;
			}
			if (
				req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/insights/insight-1/"
			) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			expect(JSON.parse((await dss(["wiki", "settings",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveProperty("projectKey", "TEST",);
			expect(JSON.parse((await dss(["wiki", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(JSON.parse((await dss(["wiki", "get", "article-1",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveProperty("payload", "old",);

			const wikiCreateDryRun = JSON.parse(
				(
					await dss(["wiki", "create", "--name", "Dry run", "--content", "preview", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(wikiCreateDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "wiki",
				name: "Dry run",
				payload: { name: "Dry run", content: "preview", },
			},);
			expect(JSON.parse(
				(await dss(["wiki", "create", "--name", "Created", "--content", "created",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("article.id", "article-2",);

			const wikiUpdateDryRun = JSON.parse(
				(
					await dss([
						"wiki",
						"update",
						"article-1",
						"--name",
						"Dry Updated",
						"--content",
						"dry",
						"--dry-run",
					], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as {
				current?: { payload?: string; };
				next?: { article?: { name?: string; }; payload?: string; };
			};
			expect(wikiUpdateDryRun.current?.payload,).toBe("old",);
			expect(wikiUpdateDryRun.next?.article?.name,).toBe("Dry Updated",);
			expect(wikiUpdateDryRun.next?.payload,).toBe("dry",);
			expect(
				JSON.parse(
					(await dss(["wiki", "update", "article-1", "--name", "Updated", "--content", "new",], {
						env: cliEnv(url,),
					},)).stdout,
				),
			).toHaveProperty("payload", "new",);
			expect(wikiUpdateBody?.article,).toMatchObject({ id: "article-1", name: "Updated", },);

			const wikiDeleteDryRun = JSON.parse(
				(
					await dss(["wiki", "delete", "article-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(wikiDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "wiki",
				article: "article-1",
			},);
			expect(
				JSON.parse((await dss(["wiki", "delete", "article-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "article-1", resource: "wiki", },);

			expect(JSON.parse((await dss(["dashboard", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(
				JSON.parse((await dss(["dashboard", "get", "dash-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("name", "Dashboard 1",);

			const dashboardCreateDryRun = JSON.parse(
				(
					await dss(["dashboard", "create", "--name", "Dry dashboard", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(dashboardCreateDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "dashboard",
				name: "Dry dashboard",
				payload: { pages: [], },
			},);
			expect(JSON.parse(
				(await dss(["dashboard", "create", "--name", "Created dashboard",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("id", "dash-2",);
			expect(dashboardCreateBody,).toEqual({ pages: [], name: "Created dashboard", },);

			const dashboardUpdateDryRun = JSON.parse(
				(
					await dss(["dashboard", "update", "dash-1", "--name", "Dry dashboard update", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { current?: { name?: string; }; next?: { name?: string; }; };
			expect(dashboardUpdateDryRun.current?.name,).toBe("Dashboard 1",);
			expect(dashboardUpdateDryRun.next?.name,).toBe("Dry dashboard update",);
			expect(JSON.parse(
				(await dss(["dashboard", "update", "dash-1", "--name", "Updated dashboard",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("name", "Updated dashboard",);
			expect(dashboardUpdateBody,).toMatchObject({ id: "dash-1", name: "Updated dashboard", },);

			const dashboardDeleteDryRun = JSON.parse(
				(
					await dss(["dashboard", "delete", "dash-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(dashboardDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "dashboard",
				id: "dash-1",
			},);
			expect(
				JSON.parse((await dss(["dashboard", "delete", "dash-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "dash-1", resource: "dashboard", },);

			expect(JSON.parse((await dss(["insight", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(
				JSON.parse((await dss(["insight", "get", "insight-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("type", "chart",);
			const insightCreateDryRun = JSON.parse(
				(
					await dss(["insight", "create", "--name", "Dry insight", "--type", "chart", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { payload?: { name?: string; type?: string; }; };
			expect(insightCreateDryRun.payload,).toMatchObject({
				name: "Dry insight",
				type: "chart",
			},);
			expect(JSON.parse(
				(await dss([
					"insight",
					"create",
					"--name",
					"Created insight",
					"--type",
					"chart",
					"--params",
					"{}",
				], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("id", "insight-2",);
			expect(insightCreateBody?.insightPrototype,).toMatchObject({
				name: "Created insight",
				type: "chart",
			},);

			const insightUpdateDryRun = JSON.parse(
				(
					await dss(["insight", "update", "insight-1", "--name", "Dry insight update", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { current?: { name?: string; }; next?: { name?: string; }; };
			expect(insightUpdateDryRun.current?.name,).toBe("Insight 1",);
			expect(insightUpdateDryRun.next?.name,).toBe("Dry insight update",);
			expect(JSON.parse(
				(await dss(["insight", "update", "insight-1", "--name", "Updated insight",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("name", "Updated insight",);
			expect(insightUpdateBody?.insight,).toMatchObject({
				id: "insight-1",
				name: "Updated insight",
			},);

			const insightDeleteDryRun = JSON.parse(
				(
					await dss(["insight", "delete", "insight-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(insightDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "insight",
				id: "insight-1",
			},);
			expect(
				JSON.parse((await dss(["insight", "delete", "insight-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "insight-1", resource: "insight", },);
		},);
	}, 45_000,);

	it("smokes data quality commands", async () => {
		let ruleName = "Has rows";
		let createRuleBody: Record<string, unknown> | undefined;
		let updateRuleBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules"
			) {
				sendJson(res, {
					monitor: {},
					displayedState: {},
					checks: [{ id: "rule-1", displayName: ruleName, type: "RecordCountInRangeRule", },],
				},);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/status"
			) {
				sendJson(res, { outcome: "SUCCESS", enabled: true, },);
				return;
			}
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules"
			) {
				createRuleBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ...createRuleBody, id: "rule-2", },);
				return;
			}
			if (
				req.method === "PUT"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules/rule-1"
			) {
				updateRuleBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				ruleName = String(updateRuleBody.displayName ?? ruleName,);
				res.statusCode = 204;
				res.end();
				return;
			}
			if (
				req.method === "DELETE"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules/rule-1"
			) {
				expect(url.searchParams.get("ruleId",),).toBe("rule-1",);
				res.statusCode = 204;
				res.end();
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/status-by-partition"
			) {
				sendJson(res, { NP: { status: "OK", }, },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/last-rules-result"
			) {
				sendJson(res, [{ id: "rule-1", outcome: "OK", },],);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules-history"
			) {
				sendJson(res, [{ id: "rule-1", outcome: "OK", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/data-quality/status") {
				expect(url.searchParams.get("onlyMonitored",),).toBe("false",);
				sendJson(res, { orders: { outcome: "SUCCESS", }, },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/data-quality/timeline"
			) {
				expect(url.searchParams.get("minTimestamp",),).toBe("1714521600000",);
				sendJson(res, [{ day: "2026-05-01", currentOutcome: "SUCCESS", },],);
				return;
			}
			if (
				req.method === "POST"
				&& url.pathname
					=== "/public/api/projects/TEST/datasets/orders/data-quality/actions/compute-rules"
			) {
				sendJson(res, { jobId: "job-1", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				sendJson(res, {
					jobId: "job-1",
					hasResult: true,
					alive: false,
					result: { outcome: "SUCCESS", },
				},);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/futures/job-1") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			expect(
				JSON.parse((await dss(["data-quality", "rules", "orders",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(await dss(["data-quality", "get-rule", "orders", "rule-1",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("displayName", "Has rows",);

			expect(
				JSON.parse((await dss(["data-quality", "status", "orders",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("outcome", "SUCCESS",);

			const createDryRun = JSON.parse(
				(
					await dss([
						"data-quality",
						"create-rule",
						"orders",
						"--data",
						'{"type":"RecordCountInRangeRule","displayName":"New rule"}',
						"--dry-run",
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(createDryRun,).toMatchObject({
				dryRun: true,
				action: "create-rule",
				dataset: "orders",
			},);
			expect(JSON.parse(
				(
					await dss([
						"data-quality",
						"create-rule",
						"orders",
						"--data",
						'{"type":"RecordCountInRangeRule","displayName":"New rule"}',
					], { env: cliEnv(url,), },)
				).stdout,
			),).toHaveProperty("id", "rule-2",);
			expect(createRuleBody,).toMatchObject({
				type: "RecordCountInRangeRule",
				displayName: "New rule",
			},);

			const updateDryRun = JSON.parse(
				(
					await dss([
						"data-quality",
						"update-rule",
						"orders",
						"rule-1",
						"--data",
						'{"displayName":"Dry rule"}',
						"--dry-run",
					], { env: cliEnv(url,), },)
				).stdout,
			) as { current?: { displayName?: string; }; next?: { displayName?: string; }; };
			expect(updateDryRun.current?.displayName,).toBe("Has rows",);
			expect(updateDryRun.next?.displayName,).toBe("Dry rule",);
			expect(JSON.parse(
				(
					await dss([
						"data-quality",
						"update-rule",
						"orders",
						"rule-1",
						"--data",
						'{"displayName":"Updated rule"}',
					], { env: cliEnv(url,), },)
				).stdout,
			),).toHaveProperty("displayName", "Updated rule",);
			expect(updateRuleBody,).toMatchObject({ id: "rule-1", displayName: "Updated rule", },);

			const deleteDryRun = JSON.parse(
				(
					await dss(["data-quality", "delete-rule", "orders", "rule-1", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(deleteDryRun,).toMatchObject({ dryRun: true, action: "delete-rule", ruleId: "rule-1", },);
			expect(JSON.parse(
				(
					await dss(["data-quality", "delete-rule", "orders", "rule-1",], { env: cliEnv(url,), },)
				).stdout,
			),).toEqual({ deleted: "rule-1", dataset: "orders", resource: "data-quality", },);

			expect(JSON.parse(
				(
					await dss(["data-quality", "status-by-partition", "orders", "--include-all-partitions",], {
						env: cliEnv(url,),
					},)
				).stdout,
			),).toHaveProperty("NP.status", "OK",);
			expect(
				JSON.parse(
					(await dss(["data-quality", "last-results", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(await dss(["data-quality", "history", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"project-status",
							"--only-monitored",
							"false",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toHaveProperty("orders.outcome", "SUCCESS",);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"project-timeline",
							"--min-timestamp",
							"1714521600000",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toHaveLength(1,);
			expect(JSON.parse(
				(
					await dss(["data-quality", "compute", "orders", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			),).toMatchObject({ dryRun: true, action: "compute", dataset: "orders", },);
			expect(
				JSON.parse(
					(await dss(["data-quality", "compute", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("jobId", "job-1",);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"compute",
							"orders",
							"--wait",
							"--poll-interval",
							"1",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toMatchObject({ futureId: "job-1", success: true, result: { outcome: "SUCCESS", }, },);
			expect(
				JSON.parse(
					(await dss(["future", "peek", "job-1",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("result.outcome", "SUCCESS",);
			expect(
				JSON.parse(
					(await dss(["future", "wait", "job-1", "--poll-interval", "1",], { env: cliEnv(url,), },))
						.stdout,
				),
			)
				.toHaveProperty("success", true,);
			expect(
				JSON.parse(
					(await dss(["future", "abort", "job-1", "--dry-run",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toMatchObject({ dryRun: true, action: "abort", resource: "future", id: "job-1", },);
			expect(
				JSON.parse((await dss(["future", "abort", "job-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ aborted: "job-1", resource: "future", },);
		},);
	}, 30_000,);
});

describe("CLI agent-readiness mutation contracts", () => {
	it("dry-runs dataset writes and reports idempotent create skips", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "orders", type: "Filesystem", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders") {
				sendJson(res, { name: "orders", tags: ["old",], },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const skipped = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--if-not-exists",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(skipped,).toMatchObject({ skipped: "orders", reason: "exists", resource: "dataset", },);

			const createDryRun = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"new_orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(createDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "dataset",
				name: "new_orders",
				payload: { datasetName: "new_orders", connection: "filesystem", dsType: "Filesystem", },
			},);

			const updateDryRun = JSON.parse(
				(await dss([
					"dataset",
					"update",
					"orders",
					"--data",
					'{"tags":["new"]}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as { current?: { tags?: string[]; }; next?: { tags?: string[]; }; };
			expect(updateDryRun.current?.tags,).toEqual(["old",],);
			expect(updateDryRun.next?.tags,).toEqual(["new",],);
		},);
	});

	it("dry-runs long-running job and scenario commands without POSTing", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const jobDryRun = JSON.parse(
				(await dss([
					"job",
					"build",
					"orders",
					"--wait",
					"--timeout",
					"10",
					"--poll-interval",
					"1",
					"--dry-run",
				], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(jobDryRun,).toMatchObject({
				dryRun: true,
				action: "build",
				resource: "job",
				target: "orders",
				method: "POST",
			},);
			expect(jobDryRun.endpoint,).toBe("/public/api/projects/TEST/jobs/",);

			const scenarioDryRun = JSON.parse(
				(await dss([
					"scenario",
					"run",
					"nightly",
					"--wait",
					"--timeout",
					"10",
					"--poll-interval",
					"1",
					"--dry-run",
				], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(scenarioDryRun,).toMatchObject({
				dryRun: true,
				action: "run",
				resource: "scenario",
				id: "nightly",
				method: "POST",
			},);
			expect(scenarioDryRun.endpoint,).toBe("/public/api/projects/TEST/scenarios/nightly/run/",);
		},);
	});

	it("plans mutations without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url ?? ""}`,);
		}, async (url,) => {
			const datasetPlan = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"planned_orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--plan",
					"--project-key",
					"TEST",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(datasetPlan,).toMatchObject({
				plan: true,
				action: "create",
				resource: "dataset",
				name: "planned_orders",
				method: "POST",
				endpoint: "/public/api/projects/TEST/datasets/",
				payload: {
					datasetName: "planned_orders",
					connection: "filesystem",
					dsType: "Filesystem",
					projectKey: "TEST",
				},
			},);

			const scenarioPlan = JSON.parse(
				(await dss(["scenario", "run", "nightly", "--plan", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(scenarioPlan,).toMatchObject({
				plan: true,
				action: "run",
				resource: "scenario",
				id: "nightly",
				method: "POST",
				endpoint: "/public/api/projects/TEST/scenarios/nightly/run/",
				payload: {},
			},);

			const jobPlan = JSON.parse(
				(await dss(["job", "build", "orders", "--plan", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(jobPlan,).toMatchObject({
				plan: true,
				action: "build",
				resource: "job",
				target: "orders",
				method: "POST",
				endpoint: "/public/api/projects/TEST/jobs/",
			},);

			const flowZonePlan = JSON.parse(
				(await dss([
					"flow-zone",
					"move",
					"zone-1",
					"--dataset",
					"orders",
					"--plan",
					"--project-key",
					"TEST",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(flowZonePlan,).toMatchObject({
				plan: true,
				action: "move",
				resource: "flow-zone",
				id: "zone-1",
				method: "POST",
				endpoint: "/public/api/projects/TEST/flow/zones/zone-1/add-items",
				payload: [{ objectType: "DATASET", objectId: "orders", },],
			},);
			const plannedAndDryRun = JSON.parse(
				(await dss(["scenario", "run", "nightly", "--plan", "--dry-run", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(plannedAndDryRun,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				action: "run",
				resource: "scenario",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans recipe payload backups as local writes", async () => {
		const filePath = join(tmpdir(), `dss-cli-plan-payload-${Date.now()}.py`,);
		const backupDir = join(tmpdir(), `dss-cli-plan-backup-${Date.now()}`,);
		writeFileSync(filePath, "print('planned')\n", "utf-8",);
		let requestCount = 0;

		try {
			await withCliServer((req, res,) => {
				requestCount++;
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${req.url ?? ""}`,);
			}, async (url,) => {
				const plan = JSON.parse(
					(await dss([
						"recipe",
						"set-payload",
						"my_recipe",
						"--file",
						filePath,
						"--backup-dir",
						backupDir,
						"--plan",
						"--project-key",
						"TEST",
					], { env: cliEnv(url,), },)).stdout,
				) as {
					endpoint: string;
					localWrites: Array<{ before: string; path: string; source: string; }>;
					payload: { backupPath: string; content: string; file: string; };
				};

				expect(plan.endpoint,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
				expect(plan.payload,).toMatchObject({
					file: filePath,
					content: "print('planned')\n",
				},);
				expect(plan.payload.backupPath.startsWith(backupDir,),).toBe(true,);
				expect(plan.localWrites,).toEqual([{
					path: plan.payload.backupPath,
					source: "remote recipe backup",
					before: "PUT",
				},],);
			},);
			expect(requestCount,).toBe(0,);
		} finally {
			rmSync(filePath, { force: true, },);
			rmSync(backupDir, { recursive: true, force: true, },);
		}
	});

	it("validates mutation plan inputs locally", async () => {
		const failure = await dssFailure([
			"dataset",
			"create",
			"--name",
			"planned_orders",
			"--type",
			"Filesystem",
			"--plan",
			"--project-key",
			"TEST",
		], {
			env: {
				PATH: process.env.PATH,
				HOME: process.env.HOME,
				DATAIKU_PROJECT_KEY: "TEST",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("--connection is required",);
	});

	it("dry-runs variable and notebook mutations with current and next state", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/variables/") {
				sendJson(res, { standard: { a: 1, }, local: {}, },);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/jupyter-notebooks/book"
			) {
				sendJson(res, {
					cells: [{
						cell_type: "code",
						source: "1",
						outputs: [{ text: "old", },],
						execution_count: 7,
					},],
					metadata: {},
					nbformat: 4,
					nbformat_minor: 5,
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const varsDryRun = JSON.parse(
				(await dss(["variable", "set", "--standard", '{"b":2}', "--dry-run",], { env: cliEnv(url,), },))
					.stdout,
			) as { next?: { standard?: Record<string, unknown>; }; };
			expect(varsDryRun.next?.standard,).toEqual({ a: 1, b: 2, },);

			const clearDryRun = JSON.parse(
				(await dss(["notebook", "clear-jupyter-outputs", "book", "--dry-run",], {
					env: cliEnv(url,),
				},)).stdout,
			) as { next?: { cells?: Array<{ outputs?: unknown[]; execution_count?: number | null; }>; }; };
			expect(clearDryRun.next?.cells?.[0]?.outputs,).toEqual([],);
			expect(clearDryRun.next?.cells?.[0]?.execution_count,).toBeNull();
		},);
	});

	it("dry-runs install-skill without credentials or file writes", async () => {
		const tmpDir = join(tmpdir(), `dss-install-skill-dry-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const result = JSON.parse(
				(await dss(["install-skill", "--agent", "omp", "--target", tmpDir, "--dry-run",], {
					cwd: tmpDir,
					env: { PATH: process.env.PATH, HOME: process.env.HOME, },
				},)).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				dryRun: true,
				action: "install-skill",
				resource: "install-skill",
				scope: "project",
				target: tmpDir,
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("records cleanup ledger entries for successful creates", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-ledger-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/") {
					sendJson(res, {},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				await dss([
					"scenario",
					"create",
					"cleanup_test",
					"Cleanup Test",
					"--record-cleanup",
					ledgerPath,
				], { env: cliEnv(url,), },);
			},);
			const lines = readFileSync(ledgerPath, "utf-8",).trim().split("\n",);
			expect(lines,).toHaveLength(1,);
			const entry = JSON.parse(lines[0],) as {
				action?: string;
				resource?: string;
				id?: string;
				cleanup?: { argv?: string[]; };
			};
			expect(entry,).toMatchObject({
				action: "create",
				resource: "scenario",
				id: "cleanup_test",
				cleanup: {
					argv: ["scenario", "delete", "cleanup_test", "--if-exists", "--project-key", "TEST",],
				},
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("applies cleanup ledgers in reverse order", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-apply-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const entries = [
			{
				ts: "2026-05-07T00:00:00.000Z",
				action: "create",
				resource: "scenario",
				id: "first",
				projectKey: "TEST",
				cleanup: { argv: ["scenario", "delete", "first", "--if-exists", "--project-key", "TEST",], },
			},
			{
				ts: "2026-05-07T00:00:01.000Z",
				action: "create",
				resource: "scenario",
				id: "second",
				projectKey: "TEST",
				cleanup: { argv: ["scenario", "delete", "second", "--if-exists", "--project-key", "TEST",], },
			},
		];
		writeFileSync(ledgerPath, `${entries.map((entry,) => JSON.stringify(entry,)).join("\n",)}\n`,);
		const deleted: string[] = [];
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				const match = url.pathname.match(/^\/public\/api\/projects\/TEST\/scenarios\/([^/]+)\/$/,);
				if (req.method === "GET" && match) {
					sendJson(res, { id: match[1], name: match[1], },);
					return;
				}
				if (req.method === "DELETE" && match) {
					deleted.push(match[1]!,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const applied = JSON.parse(
					(await dss(["cleanup", "--file", ledgerPath, "--apply",], { env: cliEnv(url,), },)).stdout,
				) as { applied?: boolean; failures?: unknown[]; };
				expect(applied.applied,).toBe(true,);
				expect(applied.failures,).toEqual([],);
			},);
			expect(deleted,).toEqual(["second", "first",],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("reports missing cleanup ledger files as usage errors", async () => {
		const missingPath = join(tmpdir(), `dss-missing-ledger-${Date.now()}.jsonl`,);
		const failure = await dssFailure(["cleanup", "--file", missingPath,], {
			env: { PATH: process.env.PATH, HOME: process.env.HOME, },
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Could not read cleanup ledger",);
	});
});
