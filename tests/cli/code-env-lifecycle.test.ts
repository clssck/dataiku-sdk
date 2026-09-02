import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readBody,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
} from "./_harness.js";

import { stableHash, } from "../../src/utils/stable-hash.js";

const LOG_DEFINITION = {
	envName: "omp_test_env",
	envLang: "PYTHON",
	specPackageList: "polars",
	desc: { pythonInterpreter: "PYTHON311", },
};

describe("CLI code-env lifecycle additions", () => {
	it("code-env list-logs fetches the logs listing", async () => {
		await withCliServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
				"/public/api/admin/code-envs/PYTHON/omp_test_env/logs",
			);
			sendJson(res, [{ name: "install.log", }, { name: "update-1.log", size: 42, },],);
		}, async (url,) => {
			const { stdout, } = await dss(["code-env", "list-logs", "PYTHON", "omp_test_env",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual([
				{ name: "install.log", },
				{ name: "update-1.log", size: 42, },
			],);
		},);
	});

	it("code-env get-log returns bounded metadata and tails lines", async () => {
		const lines = ["l1", "l2", "l3", "l4", "l5",].join("\n",);

		await withCliServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			res.setHeader("Content-Type", "text/plain",);
			res.end(lines,);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"get-log",
				"PYTHON",
				"omp_test_env",
				"install.log",
				"--max-lines",
				"2",
				"--max-log-bytes",
				"0",
			], { env: cliEnv(url,), },);
			const parsed = JSON.parse(stdout,) as Record<string, unknown>;
			expect(parsed,).toMatchObject({
				log: "l4\nl5",
				truncated: false,
				tailed: true,
				envLang: "PYTHON",
				envName: "omp_test_env",
				logName: "install.log",
			},);
		},);
	});

	it("code-env get-log --output writes the kept content to a file", async () => {
		const longLog = Array.from(
			{ length: 40, },
			(_, i,) => `line-${String(i,).padStart(3, "0",)}`,
		).join("\n",);
		const dir = join(tmpdir(), `dss-code-env-log-${crypto.randomUUID()}`,);
		mkdirSync(dir, { recursive: true, },);
		const outputPath = join(dir, "output.txt",);

		try {
			await withCliServer((req, res,) => {
				expect(req.method,).toBe("GET",);
				res.setHeader("Content-Type", "text/plain",);
				res.end(longLog,);
			}, async (url,) => {
				const { stdout, } = await dss([
					"code-env",
					"get-log",
					"PYTHON",
					"omp_test_env",
					"install.log",
					"--output",
					outputPath,
				], { env: cliEnv(url,), },);
				const parsed = JSON.parse(stdout,) as Record<string, unknown>;
				expect(parsed,).toMatchObject({
					path: outputPath,
					bytes: Buffer.byteLength(longLog,),
					truncated: false,
					tailed: false,
					envLang: "PYTHON",
					envName: "omp_test_env",
					logName: "install.log",
				},);
				const written = await Bun.file(outputPath,).text();
				expect(written,).toBe(longLog,);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("code-env get-log --output warns when the byte cap truncates", async () => {
		const longLog = Array.from({ length: 2000, }, (_, i,) => `line-${String(i,)}`,).join("\n",);
		const dir = join(tmpdir(), `dss-code-env-log-${crypto.randomUUID()}`,);
		mkdirSync(dir, { recursive: true, },);
		const outputPath = join(dir, "truncated.txt",);

		try {
			await withCliServer((req, res,) => {
				expect(req.method,).toBe("GET",);
				res.setHeader("Content-Type", "text/plain",);
				res.end(longLog,);
			}, async (url,) => {
				const { stdout, } = await dss([
					"code-env",
					"get-log",
					"PYTHON",
					"omp_test_env",
					"install.log",
					"--output",
					outputPath,
					"--max-log-bytes",
					"64",
					"--max-lines",
					"0",
				], { env: cliEnv(url,), },);
				const parsed = JSON.parse(stdout,) as Record<string, unknown>;
				expect(parsed.truncated,).toBe(true,);
				expect(parsed.tailed,).toBe(false,);
				const written = await Bun.file(outputPath,).text();
				expect(Buffer.byteLength(written,),).toBeLessThanOrEqual(64,);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("code-env update-images --dry-run sends no HTTP request", async () => {
		await withCliServer(() => {
			throw new Error("dry-run must not reach the server",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"update-images",
				"PYTHON",
				"omp_test_env",
				"--dry-run",
			], { env: cliEnv(url,), },);
			const parsed = JSON.parse(stdout,) as Record<string, unknown>;
			expect(parsed,).toMatchObject({
				dryRun: true,
				action: "update-images",
				resource: "code-env",
				envLang: "PYTHON",
				envName: "omp_test_env",
				wait: true,
			},);
		},);
	});

	it("code-env update-images posts envVersion and wait query params", async () => {
		let requestUrl: URL | undefined;

		await withCliServer((req, res,) => {
			requestUrl = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			sendJson(res, { messages: { success: true, }, },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"update-images",
				"PYTHON",
				"omp_test_env",
				"--env-version",
				"v3",
				"--no-wait",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ messages: { success: true, }, },);
		},);

		expect(requestUrl?.pathname,).toBe("/public/api/admin/code-envs/PYTHON/omp_test_env/images",);
		expect(requestUrl?.searchParams.get("envVersion",),).toBe("v3",);
		expect(requestUrl?.searchParams.get("wait",),).toBe("false",);
	});

	it("code-env version resolves the project-pinned version", async () => {
		await withCliServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
				"/public/api/admin/code-envs/PYTHON/omp_test_env/MY_PROJ/version",
			);
			sendJson(res, { version: "bundle-v9", bundleId: "b-77", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"version",
				"PYTHON",
				"omp_test_env",
				"MY_PROJ",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ version: "bundle-v9", bundleId: "b-77", },);
		},);
	});

	it("code-env set-definition --dry-run reports definition hashes without writing", async () => {
		const requests: string[] = [];

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? ""} ${url.pathname}`,);
			expect(req.method,).toBe("GET",);
			sendJson(res, LOG_DEFINITION,);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-definition",
				"PYTHON",
				"omp_test_env",
				"--data",
				'{"envName":"omp_test_env","envLang":"PYTHON","specPackageList":"polars>=1.0"}',
				"--dry-run",
			], { env: cliEnv(url,), },);
			const parsed = JSON.parse(stdout,) as Record<string, unknown>;
			expect(parsed,).toMatchObject({
				dryRun: true,
				action: "set-definition",
				resource: "code-env",
				envLang: "PYTHON",
				envName: "omp_test_env",
				changed: true,
			},);
			expect(parsed.definitionHash,).toBeString();
			expect(parsed.currentDefinitionHash,).toBeString();
			expect(parsed.definitionHash,).not.toBe(parsed.currentDefinitionHash,);
		},);

		expect(requests,).toEqual(["GET /public/api/admin/code-envs/PYTHON/omp_test_env",],);
	});

	it("code-env set-definition --expect-hash refuses a stale definition without PUT", async () => {
		const requests: string[] = [];

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? ""} ${url.pathname}`,);
			expect(req.method,).toBe("GET",);
			sendJson(res, { ...LOG_DEFINITION, specPackageList: "polars>=1.0", },);
		}, async (url,) => {
			const staleHash = "a".repeat(64,);
			const failure = await dssFailure([
				"code-env",
				"set-definition",
				"PYTHON",
				"omp_test_env",
				"--data",
				'{"envName":"omp_test_env","envLang":"PYTHON","specPackageList":"polars"}',
				"--expect-hash",
				staleHash,
			], { env: cliEnv(url,), },);
			expect([1, 2,],).toContain(failure.code,);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report.code,).toBe("validation_failed",);
			expect(report.details,).toMatchObject({
				envLang: "PYTHON",
				envName: "omp_test_env",
				expectedDefinitionHash: staleHash,
			},);
		},);

		expect(requests,).toEqual(["GET /public/api/admin/code-envs/PYTHON/omp_test_env",],);
	});

	it("code-env set-definition --expect-hash PUTs when the hash matches", async () => {
		const requests: string[] = [];
		let putBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? ""} ${url.pathname}`,);
			if (req.method === "GET") {
				sendJson(res, LOG_DEFINITION,);
				return;
			}
			if (req.method === "PUT") {
				const rawBody = await readBody(req,);
				putBody = JSON.parse(rawBody,) as Record<string, unknown>;
				sendJson(res, { updated: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"code-env",
				"set-definition",
				"PYTHON",
				"omp_test_env",
				"--data",
				'{"envName":"omp_test_env","envLang":"PYTHON","specPackageList":"polars"}',
				"--expect-hash",
				stableHash(LOG_DEFINITION,),
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({ updated: true, },);
		},);

		expect(putBody,).toEqual({
			envName: "omp_test_env",
			envLang: "PYTHON",
			specPackageList: "polars",
		},);
		expect(requests,).toEqual([
			"GET /public/api/admin/code-envs/PYTHON/omp_test_env",
			"PUT /public/api/admin/code-envs/PYTHON/omp_test_env",
		],);
	});

	it("code-env rejects an invalid language with invalid_enum", async () => {
		await withCliServer(() => {
			throw new Error("invalid lang must be rejected before any request",);
		}, async (url,) => {
			const failure = await dssFailure(["code-env", "get", "python", "omp_test_env",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report.code,).toBe("invalid_enum",);
		},);
	});

	it("registry metadata derives dry-run and flags for the new commands", async () => {
		await withCliServer(() => {
			throw new Error("commands run is a meta command and must not hit DSS",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"commands",
				"run",
				"--fields",
				"code-env.update-images.dryRun,code-env.update-images.sideEffect,code-env.get-log.sideEffect,code-env.set-definition.usage",
			], { env: cliEnv(url,), },);
			const registry = JSON.parse(stdout,) as Record<string, unknown>;
			expect(registry["code-env.update-images.dryRun"],).toBe(true,);
			expect(registry["code-env.update-images.sideEffect"],).toBe("write",);
			expect(registry["code-env.get-log.sideEffect"],).toBe("read",);
			expect(registry["code-env.set-definition.usage"],).toContain("--expect-hash SHA256",);
		},);
	});
});
