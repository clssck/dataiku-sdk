import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI missing credentials", () => {
	it("exits with a JSON envelope when no credentials are available", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-creds-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const failure = await dssFailure(["project", "list",], {
				cwd: tmpDir,
				env: {
					PATH: process.env.PATH,
					HOME: tmpDir,
					DSS_CONFIG_DIR: join(tmpDir, "config",),
					DATAIKU_DISABLE_ENV: "1",
				},
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "missing_required_flag",
				category: "usage",
				error: "Missing Dataiku URL.",
				resource: "project",
				action: "list",
				exitCode: 1,
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("emits stable report JSON for usage errors", async () => {
		const failure = await dssFailure(["flow-zone", "list", "--wat", "yes",], {
			env: {
				...process.env,
				DATAIKU_PROJECT_KEY: "TEST",
				DATAIKU_URL: "http://127.0.0.1:9",
				DATAIKU_API_KEY: "test-key",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "unknown_flag",
			category: "usage",
			resource: "flow-zone",
			action: "list",
			projectKey: "TEST",
		},);
		expect(report.error,).toContain("Unknown flag: --wat",);
	});

	it("emits stable report JSON for missing positional arguments", async () => {
		const failure = await dssFailure(["scenario", "delete",], {
			env: {
				...process.env,
				DATAIKU_PROJECT_KEY: "TEST",
				DATAIKU_URL: "http://127.0.0.1:9",
				DATAIKU_API_KEY: "test-key",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "missing_required_arg",
			category: "usage",
			resource: "scenario",
			action: "delete",
			projectKey: "TEST",
		},);
		expect(report.error,).toContain("Expected 1 argument(s), got 0",);
	});

	it("emits stable report JSON for DSS permission errors", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { message: "Access denied", requestId: "req-123", }, 403,);
		}, async (url,) => {
			const failure = await dssFailure(["scenario", "list",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(2,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
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
			expect(report.error,).toContain("403 Forbidden",);
			expect(report.error,).toContain("Check API key validity and project permissions",);
			expect(report.error,).not.toContain("Access denied",);
		},);
	});

	it("uses DSS request id headers when response body omits requestId", async () => {
		await withCliServer((_req, res,) => {
			res.setHeader("X-Request-Id", "rid-header-only",);
			sendJson(res, { message: "Temporary failure", }, 500,);
		}, async (url,) => {
			const failure = await dssFailure(["project", "list", "--retries", "1",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(3,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				code: "transient",
				category: "dss",
				resource: "project",
				action: "list",
				requestId: "rid-header-only",
				status: 500,
				retryable: true,
				exitCode: 3,
			},);
		},);
	});
});

describe("CLI .env loading", () => {
	it("loads .env from CWD and uses those credentials", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-env-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [{ projectKey: "ENV", name: "From env", },],);
			}, async (url,) => {
				writeFileSync(
					join(tmpDir, ".env",),
					`DATAIKU_URL=${url}\nDATAIKU_API_KEY=fake-key\n`,
				);
				const { stdout, stderr, } = await dss(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
					},
				},);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toEqual([{ projectKey: "ENV", name: "From env", },],);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("DATAIKU_DISABLE_ENV skips .env and falls back to saved credentials", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-env-disabled-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [{ projectKey: "DOTENV", name: "From dotenv", },],);
			}, async (dotenvUrl,) => {
				await withCliServer((_req, res,) => {
					sendJson(res, [{ projectKey: "ENVVAR", name: "From process env", },],);
				}, async (envUrl,) => {
					await withCliServer((_req, res,) => {
						sendJson(res, [{ projectKey: "SAVED", name: "From saved credentials", },],);
					}, async (savedUrl,) => {
						writeFileSync(
							join(tmpDir, ".env",),
							`DATAIKU_URL=${dotenvUrl}\nDATAIKU_API_KEY=dotenv-key\n`,
						);
						const env = {
							PATH: process.env.PATH,
							HOME: process.env.HOME,
							DSS_CONFIG_DIR: join(tmpDir, "config",),
							DATAIKU_DISABLE_ENV: "1",
							DATAIKU_URL: envUrl,
							DATAIKU_API_KEY: "env-key",
						};
						await dss([
							"auth",
							"login",
							"--url",
							savedUrl,
							"--api-key",
							"saved-key",
							"--project-key",
							"SAVED",
						], { cwd: tmpDir, env, },);
						const { stdout, stderr, } = await dss(["project", "list",], { cwd: tmpDir, env, },);
						expect(stderr,).toBe("",);
						expect(JSON.parse(stdout,),).toEqual([
							{ projectKey: "SAVED", name: "From saved credentials", },
						],);
					},);
				},);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

describe("CLI credential provenance binding", () => {
	it("refuses to pair a project .env URL with a saved API key", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-provenance-saved-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let attackerHits = 0;
			await withCliServer((_req, res,) => {
				attackerHits += 1;
				sendJson(res, [{ projectKey: "ATTACKER", name: "Never reached", },],);
			}, async (attackerUrl,) => {
				mkdirSync(join(tmpDir, "config",), { recursive: true, },);
				writeFileSync(
					join(tmpDir, "config", "credentials.json",),
					`${
						JSON.stringify(
							{ url: "https://user-dss.example", apiKey: "saved-key", projectKey: "SAVED", },
							null,
							2,
						)
					}\n`,
				);
				writeFileSync(join(tmpDir, ".env",), `DATAIKU_URL=${attackerUrl}\n`,);
				const failure = await dssFailure(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
					},
				},);
				expect(failure.code,).toBe(1,);
				expect(attackerHits,).toBe(0,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({
					type: "error",
					ok: false,
					code: "conflicting_input_sources",
					category: "usage",
					resource: "project",
					action: "list",
					exitCode: 1,
				},);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses to pair a project .env URL with an API key from the environment", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-provenance-env-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let attackerHits = 0;
			await withCliServer((_req, res,) => {
				attackerHits += 1;
				sendJson(res, [{ projectKey: "ATTACKER", name: "Never reached", },],);
			}, async (attackerUrl,) => {
				writeFileSync(join(tmpDir, ".env",), `DATAIKU_URL=${attackerUrl}\n`,);
				const failure = await dssFailure(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
						DATAIKU_API_KEY: "user-key",
					},
				},);
				expect(failure.code,).toBe(1,);
				expect(attackerHits,).toBe(0,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({ code: "conflicting_input_sources", category: "usage", },);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses to pair a project .env TLS setting with credentials from elsewhere", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-provenance-tls-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let hits = 0;
			await withCliServer((_req, res,) => {
				hits += 1;
				sendJson(res, [{ projectKey: "TLS", name: "Never reached", },],);
			}, async (serverUrl,) => {
				writeFileSync(join(tmpDir, ".env",), "NODE_TLS_REJECT_UNAUTHORIZED=0\n",);
				const failure = await dssFailure(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
						DATAIKU_URL: serverUrl,
						DATAIKU_API_KEY: "user-key",
					},
				},);
				expect(failure.code,).toBe(1,);
				expect(hits,).toBe(0,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({ code: "conflicting_input_sources", category: "usage", },);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("keeps URL+API key from the same project .env working", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-provenance-pair-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [{ projectKey: "PAIR", name: "Both from .env", },],);
			}, async (url,) => {
				writeFileSync(
					join(tmpDir, ".env",),
					`DATAIKU_URL=${url}\nDATAIKU_API_KEY=pair-key\n`,
				);
				const { stdout, stderr, } = await dss(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
					},
				},);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toEqual([{ projectKey: "PAIR", name: "Both from .env", },],);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("DATAIKU_DISABLE_ENV ignores a URL-only project .env and uses saved credentials", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-provenance-disabled-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [{ projectKey: "SAVED", name: "From saved credentials", },],);
			}, async (savedUrl,) => {
				mkdirSync(join(tmpDir, "config",), { recursive: true, },);
				writeFileSync(
					join(tmpDir, "config", "credentials.json",),
					`${
						JSON.stringify(
							{ url: savedUrl, apiKey: "saved-key", projectKey: "SAVED", },
							null,
							2,
						)
					}\n`,
				);
				writeFileSync(join(tmpDir, ".env",), "DATAIKU_URL=http://attacker.invalid\n",);
				const { stdout, stderr, } = await dss(["project", "list",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: join(tmpDir, "config",),
						DATAIKU_DISABLE_ENV: "1",
					},
				},);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toEqual([
					{ projectKey: "SAVED", name: "From saved credentials", },
				],);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

describe("CLI explicit empty credential errors", () => {
	it("empty --url emits a JSON error envelope", async () => {
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
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				code: "missing_required_flag",
				error: "Missing Dataiku URL.",
				exitCode: 1,
				resource: "project",
				action: "list",
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("empty --api-key emits a JSON error envelope", async () => {
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
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				ok: false,
				code: "missing_required_flag",
				error: "Missing API key.",
				exitCode: 1,
				resource: "project",
				action: "list",
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
