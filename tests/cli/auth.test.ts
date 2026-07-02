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
			expect(failure.stdout,).toBe("",);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
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

	it("dss auth status and logout are rejected", async () => {
		for (const action of ["status", "logout",]) {
			const failure = await dssFailure(["auth", action,],);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
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
				expect(failure.stdout,).toBe("",);
				const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
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
