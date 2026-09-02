import { describe, expect, it, } from "bun:test";
import {
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileExists,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI auth commands", () => {
	it("dss auth login requires URL and API key without prompting", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-missing-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const failure = await dssFailure(["auth", "login",], {
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					DSS_CONFIG_DIR: tmpDir,
					DATAIKU_DISABLE_ENV: "1",
				},
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "missing_required_flag",
				category: "usage",
				resource: "auth",
				action: "login",
				exitCode: 1,
			},);
			expect((report.details as Record<string, unknown>).requiredFlags,).toEqual(["url", "api-key",],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth login --plan returns a redacted plan without network or credential writes", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-plan-${Date.now()}`,);
		const sentinelApiKey = "SENTINEL_API_KEY_SHOULD_NOT_APPEAR";
		let requestCount = 0;
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((req, res,) => {
				requestCount++;
				res.statusCode = 500;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"auth",
					"login",
					"--url",
					`${url}/`,
					"--api-key",
					sentinelApiKey,
					"--project-key",
					"PLANPROJ",
					"--plan",
				], {
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
						DATAIKU_DISABLE_ENV: "1",
					},
				},);

				expect(`${stdout}${stderr}`,).not.toContain(sentinelApiKey,);
				expect(stderr,).toBe("",);
				const plan = JSON.parse(stdout,) as Record<string, unknown>;
				expect(plan,).toMatchObject({
					plan: true,
					action: "login",
					resource: "auth",
					url,
					configTarget: join(tmpDir, "credentials.json",),
					payload: {
						apiKeyProvided: true,
						projectKey: "PLANPROJ",
					},
					localWrites: [{
						path: join(tmpDir, "credentials.json",),
						target: "credentials",
						redacted: ["apiKey",],
					},],
				},);
			},);
			expect(requestCount,).toBe(0,);
			expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(false,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss auth status and logout are rejected", async () => {
		for (const action of ["status", "logout",]) {
			const failure = await dssFailure(["auth", action,],);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "usage_error",
				category: "usage",
				resource: "auth",
				action,
				exitCode: 1,
			},);
			expect((report.details as Record<string, unknown>).validActions,).toEqual(["login",],);
		}
	});

	it("dss auth login saves credentials and returns JSON", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-login-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
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
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as Record<string, unknown>;
				expect(result,).toEqual({ saved: true, path: join(tmpDir, "credentials.json",), },);

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
			await withCliServer((_req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
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
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toMatchObject({
					saved: true,
					path: join(tmpDir, "credentials.json",),
				},);
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
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({
					category: "dss",
					resource: "auth",
					action: "login",
					status: 401,
					exitCode: 2,
				},);
				expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(false,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});

describe("CLI auth login credential provenance", () => {
	it("refuses a project .env URL paired with an environment API key, without a request", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-provenance-url-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let attackerHits = 0;
			await withCliServer((_req, res,) => {
				attackerHits += 1;
				sendJson(res, {},);
			}, async (attackerUrl,) => {
				writeFileSync(join(tmpDir, ".env",), `DATAIKU_URL=${attackerUrl}\n`,);
				const failure = await dssFailure(["auth", "login",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
						DATAIKU_API_KEY: "user-key",
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
					resource: "auth",
					action: "login",
					exitCode: 1,
				},);
				expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(false,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses a project .env URL paired with a --api-key flag, without a request", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-provenance-flag-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let attackerHits = 0;
			await withCliServer((_req, res,) => {
				attackerHits += 1;
				sendJson(res, {},);
			}, async (attackerUrl,) => {
				writeFileSync(join(tmpDir, ".env",), `DATAIKU_URL=${attackerUrl}\n`,);
				const failure = await dssFailure(["auth", "login", "--api-key", "flag-key",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(failure.code,).toBe(1,);
				expect(attackerHits,).toBe(0,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({ code: "conflicting_input_sources", category: "usage", },);
				expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(false,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses a project .env TLS setting with explicit flags, without a request", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-provenance-tls-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			let hits = 0;
			await withCliServer((_req, res,) => {
				hits += 1;
				sendJson(res, {},);
			}, async (url,) => {
				writeFileSync(join(tmpDir, ".env",), "NODE_TLS_REJECT_UNAUTHORIZED=0\n",);
				const failure = await dssFailure(["auth", "login", "--url", url, "--api-key", "flag-key",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(failure.code,).toBe(1,);
				expect(hits,).toBe(0,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({ code: "conflicting_input_sources", category: "usage", },);
				expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(false,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("allows auth login when the project .env supplies URL and API key together", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-auth-provenance-pair-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, [{ projectKey: "LOGIN", name: "Login target", },],);
			}, async (url,) => {
				writeFileSync(
					join(tmpDir, ".env",),
					`DATAIKU_URL=${url}\nDATAIKU_API_KEY=login-key\n`,
				);
				const { stdout, stderr, } = await dss(["auth", "login",], {
					cwd: tmpDir,
					env: {
						PATH: process.env.PATH,
						HOME: process.env.HOME,
						DSS_CONFIG_DIR: tmpDir,
					},
				},);
				expect(stderr,).toBe("",);
				const report = JSON.parse(stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({ saved: true, },);
				expect(readFileExists(join(tmpDir, "credentials.json",),),).toBe(true,);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
