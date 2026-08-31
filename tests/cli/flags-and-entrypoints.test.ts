import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	exec,
	join,
	mkdirSync,
	readFileSync,
	rmSync,
	SDK_ROOT,
	sendJson,
	statSync,
	tmpdir,
	withCliServer,
} from "./_harness.js";

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
				ok: false,
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
				ok: false,
				error: "Missing Dataiku URL.",
				code: "missing_required_flag",
				exitCode: 1,
				resource: "project",
				action: "list",
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
