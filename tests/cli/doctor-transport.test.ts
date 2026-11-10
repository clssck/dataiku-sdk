import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

interface DoctorCheck {
	name: string;
	ok: boolean;
	message: string;
	details?: Record<string, unknown>;
}

interface DoctorReport {
	ok: boolean;
	checks: DoctorCheck[];
	context: { tlsVerify: string; };
}

interface CliWarning {
	code: string;
	[key: string]: unknown;
}

const API_KEY = "FAKE-TRANSPORT-KEY-0123456789";

/** Hermetic child: temp config dir and cwd, loopback-only mocks, fake key. */
async function withSandbox(
	run: (dir: string, env: (extra: NodeJS.ProcessEnv,) => NodeJS.ProcessEnv,) => Promise<void>,
): Promise<void> {
	const dir = mkdtempSync(join(tmpdir(), "dss-transport-",),);
	try {
		await run(dir, (extra,) => ({
			PATH: process.env.PATH,
			HOME: process.env.HOME,
			DSS_CONFIG_DIR: dir,
			DATAIKU_API_KEY: API_KEY,
			...extra,
		}),);
	} finally {
		rmSync(dir, { recursive: true, force: true, },);
	}
}

function warnings(stderr: string,): CliWarning[] {
	return stderr.split("\n",).filter((line,) => line.trim() !== "",).flatMap((line,) => {
		const event = JSON.parse(line,) as { type?: string; warnings?: CliWarning[]; };
		return event.type === "warning" ? event.warnings ?? [] : [];
	},);
}

/** `project list` against an unresolvable host: it fails fast, after credential resolution. */
async function failingList(
	url: string,
	dir: string,
	env: (extra: NodeJS.ProcessEnv,) => NodeJS.ProcessEnv,
	extra: NodeJS.ProcessEnv = {},
	args: string[] = [],
) {
	const failure = await dssFailure([
		"project",
		"list",
		"--request-timeout",
		"2000",
		"--retries",
		"0",
		...args,
	], {
		cwd: dir,
		env: env({ DATAIKU_URL: url, ...extra, },),
	},);
	return { failure, warnings: warnings(failure.stderr,), };
}

describe("transport warnings", () => {
	it("warns once, naming only the host, for a plain-HTTP non-loopback URL", async () => {
		await withSandbox(async (dir, env,) => {
			const { failure, warnings: emitted, } = await failingList(
				"http://example.invalid:9/secret-path?token=abc",
				dir,
				env,
			);
			expect(emitted.filter((w,) => w.code === "insecure_transport",).length,).toBe(1,);
			const warning = emitted.find((w,) => w.code === "insecure_transport",)!;
			expect(warning,).toEqual({
				code: "insecure_transport",
				host: "example.invalid:9",
				hint: "API key is sent in cleartext; use https.",
			},);
			expect(failure.stderr,).not.toContain("secret-path",);
			expect(failure.stderr,).not.toContain("token=abc",);
			expect(failure.stderr,).not.toContain(API_KEY,);
		},);
	});

	it("never repeats the warning in one process, even though credentials resolve again for error scrubbing", async () => {
		await withSandbox(async (dir, env,) => {
			const { failure, } = await failingList("http://example.invalid:9", dir, env,);
			expect(failure.stderr.match(/insecure_transport/g,)?.length,).toBe(1,);
		},);
	});

	it("does not leak URL userinfo into the warning", async () => {
		await withSandbox(async (dir, env,) => {
			const { failure, warnings: emitted, } = await failingList(
				"http://alice:hunter2@example.invalid:9",
				dir,
				env,
			);
			expect(emitted.map((w,) => w.code,),).toContain("insecure_transport",);
			expect(failure.stderr,).not.toContain("alice",);
			expect(failure.stderr,).not.toContain("hunter2",);
		},);
	});

	it("stays silent for loopback HTTP", async () => {
		await withSandbox(async (dir, env,) => {
			await withCliServer((_req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stderr, } = await dss(["project", "list",], {
					cwd: dir,
					env: env({ DATAIKU_URL: url, },),
				},);
				expect(warnings(stderr,),).toEqual([],);
			},);
			for (const url of ["http://localhost:9", "http://[::1]:9", "http://127.9.9.9:9",]) {
				const { warnings: emitted, } = await failingList(url, dir, env,);
				expect(emitted.map((w,) => w.code,),).not.toContain("insecure_transport",);
			}
		},);
	});

	it("stays silent for https", async () => {
		await withSandbox(async (dir, env,) => {
			const { warnings: emitted, } = await failingList("https://example.invalid", dir, env,);
			expect(emitted,).toEqual([],);
		},);
	});

	it("names --insecure as the source when verification is disabled by flag", async () => {
		await withSandbox(async (dir, env,) => {
			const { failure, warnings: emitted, } = await failingList(
				"https://example.invalid",
				dir,
				env,
				{},
				["--insecure",],
			);
			const warning = emitted.find((w,) => w.code === "tls_verification_disabled",);
			expect(warning,).toEqual({
				code: "tls_verification_disabled",
				source: "flag",
				hint: "Server identity is not checked; unset it or use --ca-cert.",
			},);
			expect(emitted.filter((w,) => w.code === "tls_verification_disabled",).length,).toBe(1,);
			expect(failure.stderr,).not.toContain(API_KEY,);
		},);
	});

	it("names the environment variable as the source when it disables verification", async () => {
		await withSandbox(async (dir, env,) => {
			const { warnings: emitted, } = await failingList("https://example.invalid", dir, env, {
				NODE_TLS_REJECT_UNAUTHORIZED: "0",
			},);
			expect(emitted.find((w,) => w.code === "tls_verification_disabled",),).toEqual({
				code: "tls_verification_disabled",
				source: "NODE_TLS_REJECT_UNAUTHORIZED",
				hint: "Server identity is not checked; unset it or use --ca-cert.",
			},);
		},);
	});

	it("names the saved credentials as the source when they disable verification", async () => {
		await withSandbox(async (dir, env,) => {
			writeFileSync(
				join(dir, "credentials.json",),
				JSON.stringify({
					url: "https://example.invalid",
					apiKey: API_KEY,
					tlsRejectUnauthorized: false,
				},),
			);
			const failure = await dssFailure([
				"project",
				"list",
				"--request-timeout",
				"2000",
				"--retries",
				"0",
			], {
				cwd: dir,
				env: env({ DATAIKU_API_KEY: undefined, },),
			},);
			expect(warnings(failure.stderr,).find((w,) => w.code === "tls_verification_disabled",),)
				.toEqual({
					code: "tls_verification_disabled",
					source: "saved",
					hint: "Server identity is not checked; re-enable with `dss auth login` without --insecure.",
				},);
		},);
	});

	it("does not warn about verification when it is left on", async () => {
		await withSandbox(async (dir, env,) => {
			const { warnings: emitted, } = await failingList("https://example.invalid", dir, env,);
			expect(emitted.map((w,) => w.code,),).not.toContain("tls_verification_disabled",);
		},);
	});
});

describe("doctor transport context", () => {
	it("reports tlsVerify none for plain HTTP, without a transport finding for loopback", async () => {
		await withSandbox(async (dir, env,) => {
			await withCliServer((_req, res,) => {
				sendJson(res, [],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(["doctor",], {
					cwd: dir,
					env: env({ DATAIKU_URL: url, },),
				},);
				const report = JSON.parse(stdout,) as DoctorReport;
				expect(report.ok,).toBe(true,);
				expect(report.context.tlsVerify,).toBe("none",);
				expect(report.checks.map((c,) => c.name,),).not.toContain("transport_security",);
				expect(warnings(stderr,),).toEqual([],);
			},);
		},);
	});

	it.skipIf(process.platform === "win32",)(
		"records cleartext transport as a passing check and keeps a working plain-HTTP setup green",
		async () => {
			await withSandbox(async (dir, env,) => {
				await withCliServer((_req, res,) => {
					sendJson(res, [],);
				}, async (url,) => {
					// 0.0.0.0 reaches the loopback mock but is not a loopback host to the policy.
					const port = new URL(url,).port;
					const { stdout, stderr, } = await dss(["doctor",], {
						cwd: dir,
						env: env({ DATAIKU_URL: `http://0.0.0.0:${port}`, },),
					},);
					const report = JSON.parse(stdout,) as DoctorReport;
					expect(report.ok,).toBe(true,);
					expect(report.context.tlsVerify,).toBe("none",);
					const check = report.checks.find((c,) => c.name === "transport_security",);
					expect(check,).toMatchObject({ ok: true, details: { scheme: "http", cleartext: true, }, },);
					expect(warnings(stderr,).map((w,) => w.code,),).toEqual(["insecure_transport",],);
				},);
			},);
		},
	);

	it("keeps strict and disabled for https", async () => {
		await withSandbox(async (dir, env,) => {
			const strict = await dssFailure(["doctor", "--request-timeout", "2000", "--retries", "0",], {
				cwd: dir,
				env: env({ DATAIKU_URL: "https://example.invalid", },),
			},);
			expect((JSON.parse(strict.stdout,) as DoctorReport).context.tlsVerify,).toBe("strict",);
			const disabled = await dssFailure(
				["doctor", "--insecure", "--request-timeout", "2000", "--retries", "0",],
				{ cwd: dir, env: env({ DATAIKU_URL: "https://example.invalid", },), },
			);
			expect((JSON.parse(disabled.stdout,) as DoctorReport).context.tlsVerify,).toBe("disabled",);
		},);
	});
});

describe("doctor error text", () => {
	it("never prints the API key a proxy echoes back in an error body", async () => {
		await withSandbox(async (dir, env,) => {
			await withCliServer((req, res,) => {
				res.statusCode = 500;
				res.setHeader("Content-Type", "text/plain",);
				res.end(`proxy debug: Authorization: ${req.headers.authorization ?? ""}`,);
			}, async (url,) => {
				const failure = await dssFailure(["doctor", "--capabilities", "--retries", "0",], {
					cwd: dir,
					env: env({ DATAIKU_URL: url, DATAIKU_PROJECT_KEY: "TEST", },),
				},);
				expect(failure.code,).toBe(2,);
				const report = JSON.parse(failure.stdout,) as DoctorReport;
				expect(report.ok,).toBe(false,);
				const connectivity = report.checks.find((c,) => c.name === "connectivity",);
				expect(String(connectivity?.details?.message,),).toContain("proxy debug",);
				expect(failure.stdout,).not.toContain(API_KEY,);
				expect(failure.stderr,).not.toContain(API_KEY,);
			},);
		},);
	});
});
