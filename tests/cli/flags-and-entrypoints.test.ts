import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "node:http";
import {
	cliEnv,
	dss,
	dssFailure,
	exec,
	join,
	mkdirSync,
	readBody,
	readFileSync,
	rmSync,
	SDK_ROOT,
	sendJson,
	statSync,
	tmpdir,
	withCliServer,
} from "./_harness.js";

describe("CLI --version flag", () => {
	for (const flag of ["--version", "-V",]) {
		it(`dss ${flag} prints version JSON to stdout`, async () => {
			const { stdout, stderr, } = await dss([flag,],);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.version,).toEqual(expect.any(String,),);
			expect(result,).toHaveProperty("gitRevision",);
		});
	}
});

describe("CLI bin entrypoints", () => {
	const binShim = join(SDK_ROOT, "bin", "dss",);
	const binJs = join(SDK_ROOT, "bin", "dss.js",);

	it("package metadata exposes the executable dss bin", () => {
		const pkg = JSON.parse(readFileSync(join(SDK_ROOT, "package.json",), "utf-8",),) as {
			bin?: Record<string, string>;
		};
		expect(pkg.bin?.dss,).toBe("bin/dss.js",);
		if (process.platform !== "win32") {
			expect((statSync(binJs,).mode & 0o111) !== 0,).toBe(true,);
		}
	});
	it("rejects Bun invocations that bypass the no-env-file launcher guard", async () => {
		try {
			await exec(process.execPath, [binJs, "version",], { cwd: tmpdir(), },);
			throw new Error("expected the unguarded Bun invocation to fail",);
		} catch (error: unknown) {
			const failure = error as { code?: number; stdout?: string; stderr?: string; };
			expect(failure.code,).toBe(1,);
			expect(failure.stderr ?? "",).toBe("",);
			expect(JSON.parse(failure.stdout ?? "",),).toMatchObject({
				type: "error",
				code: "env_autoload_enabled",
				category: "usage",
				exitCode: 1,
			},);
		}
	});

	it("source checkout entrypoints emit version JSON", async () => {
		const entrypoints: Array<[string, string[],]> = [
			[process.execPath, ["--no-env-file", binJs, "version",],],
		];
		if (process.platform !== "win32") {
			entrypoints.unshift([binShim, ["version",],], [binJs, ["version",],],);
		}
		for (const [cmd, args,] of entrypoints) {
			const { stdout, stderr, } = await exec(cmd, args, {
				cwd: SDK_ROOT,
				env: {
					...process.env,
					DATAIKU_URL: "",
					DATAIKU_API_KEY: "",
					DATAIKU_PROJECT_KEY: "",
					DATAIKU_DISABLE_ENV: "1",
				},
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.version,).toEqual(expect.any(String,),);
			expect(result,).toHaveProperty("gitRevision",);
		}
	});

	it("bin launcher preserves JSON error envelopes under DATAIKU_DISABLE_ENV", async () => {
		const tmpDir = join(tmpdir(), `dss-bin-entrypoint-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await exec(process.execPath, ["--no-env-file", binJs, "project", "list",], {
				cwd: tmpDir,
				env: {
					...process.env,
					DSS_CONFIG_DIR: join(tmpDir, "config",),
					DATAIKU_URL: "",
					DATAIKU_API_KEY: "",
					DATAIKU_PROJECT_KEY: "",
					DATAIKU_DISABLE_ENV: "1",
				},
			},);
			throw new Error("expected bin command to fail",);
		} catch (error: unknown) {
			const failure = error as { code?: number; stdout?: string; stderr?: string; };
			expect(failure.code,).toBe(1,);
			expect(failure.stderr ?? "",).toBe("",);
			const report = JSON.parse(failure.stdout ?? "",) as Record<string, unknown>;
			expect(report,).toMatchObject({
				error: "Missing Dataiku URL.",
				code: "missing_required_flag",
				exitCode: 1,
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

describe("CLI short flags", () => {
	it("-h fails with the unsupported help JSON envelope", async () => {
		const failure = await dssFailure(["-h",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "usage_error",
			error: "Help screens are not supported.",
			exitCode: 1,
		},);
	});

	it("-f is rejected after the JSON-only output cutover", async () => {
		const failure = await dssFailure(["-f", "table",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			error: "Unknown flag: -f",
			code: "unknown_flag",
			category: "usage",
			exitCode: 1,
		},);
	});

	it("removed --json and --raw flags are rejected as unknown flags", async () => {
		for (const flag of ["--json", "--raw",]) {
			const failure = await dssFailure([flag,],);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				error: `Unknown flag: ${flag}`,
				code: "unknown_flag",
				category: "usage",
				exitCode: 1,
			},);
		}
	});
});

describe("CLI boolean flag does not swallow next positional", () => {
	it("--verbose does not consume the next positional arg", async () => {
		const { stdout, stderr, } = await dss(["--verbose", "commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<string, unknown>;
		expect(registry,).toHaveProperty("project",);
	});
});

const sqlServer = (seen: { sql?: string; },) =>
async (
	req: IncomingMessage,
	res: ServerResponse,
) => {
	const url = new URL(req.url ?? "/", "http://localhost",);
	if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
		const body: unknown = JSON.parse(await readBody(req,),);
		if (body && typeof body === "object" && "query" in body && typeof body.query === "string") {
			seen.sql = body.query;
		}
		sendJson(res, { queryId: "q1", schema: [{ name: "one", type: "int", },], },);
		return;
	}
	if (url.pathname === "/public/api/sql/queries/q1/stream") {
		res.setHeader("Content-Type", "application/json",);
		res.end("[[1]]",);
		return;
	}
	if (url.pathname === "/public/api/sql/queries/q1/finish-streaming") {
		res.end("",);
		return;
	}
	res.statusCode = 404;
	res.end("not found",);
};

describe("CLI flag value parsing", () => {
	it("missing value for --target fails fast", async () => {
		const failure = await dssFailure(["install-skill", "--target",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			error: "Flag --target requires a value.",
			code: "missing_required_flag",
			category: "usage",
			exitCode: 1,
		},);
	});

	describe("SQL values starting with a -- comment", () => {
		const comment = "-- fetch one row\nselect 1 as one";

		it("accepts --sql VALUE when the value is a -- comment, not a flag", async () => {
			const seen: { sql?: string; } = {};
			await withCliServer(sqlServer(seen,), async (url,) => {
				const { stdout, } = await dss(["sql", "query", "--sql", comment, "--connection", "CONN",], {
					env: cliEnv(url,),
				},);
				expect((JSON.parse(stdout,) as { rows: unknown[][]; }).rows,).toEqual([[1,],],);
				expect(seen.sql,).toBe(comment,);
			},);
		});

		it("accepts --sql=VALUE when the value starts with --", async () => {
			const seen: { sql?: string; } = {};
			await withCliServer(sqlServer(seen,), async (url,) => {
				await dss(["sql", "query", `--sql=${comment}`, "--connection", "CONN",], {
					env: cliEnv(url,),
				},);
				expect(seen.sql,).toBe(comment,);
			},);
		});

		it("still treats a flag-shaped token as a missing value and points to --sql=, --sql-file, --stdin", async () => {
			const failure = await dssFailure(["sql", "query", "--sql", "--connection", "CONN",],);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stdout,) as { error: string; code: string; hint: string; };
			expect(report.error,).toBe("Flag --sql requires a value.",);
			expect(report.code,).toBe("missing_required_flag",);
			expect(report.hint,).toContain("--sql=",);
			expect(report.hint,).toContain("--sql-file",);
			expect(report.hint,).toContain("--stdin",);
		});

		it("keeps rejecting unknown long flags after a value flag", async () => {
			const failure = await dssFailure(["sql", "query", "SELECT 1", "--bogus-flag", "x",],);
			expect(failure.code,).toBe(1,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				error: "Unknown flag: --bogus-flag",
				code: "unknown_flag",
			},);
		});

		it("points to --flag=VALUE for other value flags whose value starts with -", async () => {
			const failure = await dssFailure(["install-skill", "--target", "--agent=x",],);
			const report = JSON.parse(failure.stdout,) as { hint?: string; };
			expect(report.hint,).toContain("--target=VALUE",);
		});
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

describe("explicit boolean execution modes", () => {
	it("plans direct and batch operations without sending mutation requests", async () => {
		const calls: string[] = [];
		await withCliServer((req, res,) => {
			calls.push(req.method!,);
			sendJson(res, {},);
		}, async (url,) => {
			const env = cliEnv(url,);
			const direct = await dss(["project", "delete", "TEST", "--explain=true", "--drop-data=true",], {
				env,
			},);
			const plan = JSON.parse(direct.stdout,);
			expect(plan.plan,).toBe(true,);
			const combined = await dss(["project", "delete", "TEST", "--plan=true", "--dryrun=true",], {
				env,
			},);
			expect(JSON.parse(combined.stdout,),).toMatchObject({ plan: true, plannedAndDryRun: true, },);
			expect(plan.endpoint,).toContain("clearManagedDatasets=true",);
			const batch = await dss([
				"batch",
				"--dry-run=true",
				"--data",
				JSON.stringify([["project", "delete", "TEST",],],),
			], { env, },);
			expect(JSON.parse(batch.stdout,).dryRun,).toBe(true,);
			expect(calls,).toEqual([],);
		},);
	});
	it("honors explicit false and rejects invalid booleans before sending requests", async () => {
		const calls: string[] = [];
		await withCliServer((req, res,) => {
			calls.push(`${req.method} ${req.url}`,);
			sendJson(res, {},);
		}, async (url,) => {
			const env = cliEnv(url,);
			const invalid = await dssFailure(["project", "delete", "TEST", "--plan=maybe",], { env, },);
			expect(invalid.code,).toBe(1,);
			expect(JSON.parse(invalid.stdout,),).toMatchObject({
				code: "invalid_enum",
				category: "usage",
			},);
			expect(calls,).toEqual([],);
			await dss([
				"project",
				"delete",
				"TEST",
				"--plan",
				"--plan=false",
				"--dry-run=false",
				"--drop-data=false",
			], { env, },);
			expect(calls,).toEqual([
				"DELETE /public/api/projects/TEST?clearManagedDatasets=false&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
			],);
		},);
	});
});

describe("unreadable payload sources", () => {
	it("classifies missing JSON and text files as correctable input errors", async () => {
		const dir = join(tmpdir(), `dss-input-errors-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		try {
			const file = join(dir, "missing",);
			const json = await dssFailure(["batch", "--data-file", file,],);
			const text = await dssFailure([
				"wiki",
				"create",
				"--name",
				"Article",
				"--file",
				file,
				"--plan",
				"--project-key",
				"TEST",
			],);
			for (const failure of [json, text,]) {
				expect(failure.code,).toBe(1,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "validation_failed",
					category: "usage",
					details: { path: file, cause: "ENOENT", },
				},);
			}
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});
