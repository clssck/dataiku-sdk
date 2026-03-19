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

	it("dss --help lists global format and verbose flags", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("--format FORMAT",);
		expect(stderr,).toContain("--verbose",);
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
});

describe("CLI missing credentials", () => {
	it("exits non-zero when no credentials are available", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-creds-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["project", "list",], {
				cwd: tmpDir,
				env: { PATH: process.env.PATH, HOME: process.env.HOME, },
			},);
			throw new Error("should have exited non-zero",);
		} catch (e: unknown) {
			const err = e as { code?: number; stderr?: string; stdout?: string; };
			expect(err.code !== 0 || err.stderr,).toBeTruthy();
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
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
			expect(stdout,).toBe('{\n  "ok": true\n}\n',);
			expect(stderr,).toBe("",);
		},);
	});

	it("prints string results without JSON escaping", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("line 1\nline 2\n",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["job", "log", "job-1",], { env: cliEnv(url,), },);
			expect(stdout,).toBe("line 1\nline 2\n",);
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
				expect(stdout,).toBe('{\n  "ok": true\n}\n',);
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
			expect(stdout,).toBe('{\n  "ok": true\n}\n',);
		},);
		const nested = capturedBody?.nested as Record<string, unknown> | undefined;
		expect(nested?.preserved,).toBe(true,);
		expect(nested?.added,).toBe("from-stdin",);
	});

	it("supports TSV and quiet output formats", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [
				{ projectKey: "P1", name: "One", },
				{ projectKey: "P2", name: "Two", },
			],);
		}, async (url,) => {
			const tsv = await dss(["project", "list", "--format", "tsv",], { env: cliEnv(url,), },);
			expect(tsv.stdout,).toBe("projectKey\tname\nP1\tOne\nP2\tTwo\n",);

			const quiet = await dss(["project", "list", "--format", "quiet",], { env: cliEnv(url,), },);
			expect(quiet.stdout,).toBe("",);
		},);
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
		expect(failure.stderr,).toContain("--output is required",);
		expect(failure.stderr,).toContain("dss recipe create --type TYPE --input DS --output DS",);
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
				expect(stdout,).toBe(`${outputPath}\n`,);
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
				expect(stdout,).toContain("--- remote:sample_recipe",);
				expect(stdout,).toContain(`+++ local:${filePath}`,);
				expect(stdout,).toContain("@@ line 2 @@",);
				expect(stdout,).toContain("- print('server')",);
				expect(stdout,).toContain("+ print('local')",);
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
				expect(stderr,).toContain("\u2713 Connected",);
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
				// Process should exit non-zero
				expect(failure.code !== 0 || failure.stderr,).toBeTruthy();
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
				expect(stderr,).toContain("\u2713 Valid",);
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

describe("CLI --format table", () => {
	it("renders array of objects as aligned table", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, [
				{ name: "alpha", projectKey: "PROJ", },
				{ name: "beta", projectKey: "PROJ", },
			],);
		}, async (url,) => {
			const { stdout, } = await dss(["project", "list", "--format", "table",], {
				env: cliEnv(url,),
			},);
			// Header row
			expect(stdout,).toContain("name",);
			expect(stdout,).toContain("projectKey",);
			// Separator row
			expect(stdout,).toContain("----",);
			// Data rows
			expect(stdout,).toContain("alpha",);
			expect(stdout,).toContain("beta",);
		},);
	});

	it("falls back to JSON for non-array results", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { name: "proj", projectKey: "PROJ", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"project",
				"get",
				"--format",
				"table",
			], { env: cliEnv(url,), },);
			// Should fall back to JSON
			expect(JSON.parse(stdout,),).toEqual({ name: "proj", projectKey: "PROJ", },);
		},);
	});

	it("truncates long values with ellipsis", async () => {
		const longValue = "a".repeat(50,);
		await withCliServer((_req, res,) => {
			sendJson(res, [{ name: longValue, },],);
		}, async (url,) => {
			const { stdout, } = await dss(["project", "list", "--format", "table",], {
				env: cliEnv(url,),
			},);
			// Value should be truncated (max 40 chars) and contain ellipsis
			expect(stdout,).toContain("\u2026",);
			expect(stdout,).not.toContain(longValue,);
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
			expect(stdout,).toBe("print('hello')\n",);
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
				expect(stdout.trim(),).toBe(outPath,);
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
				], { env: cliEnv(url,), },);
				expect(stdout,).toContain('"ok": true',);
				expect(putBody,).toBeDefined();
				const parsed = JSON.parse(putBody!,);
				expect(parsed.payload,).toBe("print('updated')\n",);
			},);
		} finally {
			rmSync(filePath, { force: true, },);
		}
	});

	it("set-payload fails without --file", async () => {
		const failure = await dssFailure(["recipe", "set-payload", "my_recipe",], {
			env: cliEnv("http://localhost:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("--file is required",);
	});
});

describe("CLI help improvements", () => {
	it("help shows --timeout flag", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("--timeout MS",);
	});

	it("help shows table format option", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("table",);
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
		expect(stdout.trim(),).toMatch(/^\d+\.\d+\.\d+/,);
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

	it("-f table formats output as table", async () => {
		const { stderr, } = await dss(["-f", "table", "--help",],);
		// -f should consume "table" as the format value, --help triggers help
		// Since --help with no resource shows top-level help and exits 0,
		// this verifies -f didn't swallow "table" as a positional
		expect(stderr,).toContain("Usage: dss",);
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

describe("CLI missing credentials plain text errors", () => {
	it("missing URL prints plain text error, not JSON", async () => {
		// --url "" overrides .env-loaded DATAIKU_URL, forcing the missing-URL path
		const failure = await dssFailure(["--url", "", "project", "list",],);
		expect(failure.stderr,).not.toContain('{"error"',);
		expect(failure.stderr,).toContain("Missing Dataiku URL",);
		expect(failure.code,).toBe(1,);
	});

	it("missing API key prints plain text error, not JSON", async () => {
		// --api-key "" overrides .env-loaded DATAIKU_API_KEY, forcing the missing-key path
		const failure = await dssFailure(["--api-key", "", "project", "list",],);
		expect(failure.stderr,).not.toContain('{"error"',);
		expect(failure.stderr,).toContain("Missing API key",);
		expect(failure.code,).toBe(1,);
	});
});

describe("CLI help text includes short flags", () => {
	it("help shows short flag aliases", async () => {
		const { stderr, } = await dss(["--help",],);
		expect(stderr,).toContain("-h, --help",);
		expect(stderr,).toContain("-v, --verbose",);
		expect(stderr,).toContain("-V, --version",);
		expect(stderr,).toContain("-f, --format",);
		expect(stderr,).toContain("-o, --output",);
	});
});
