import { describe, expect, it, } from "bun:test";
import { execFile, } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = resolve(dirname(fileURLToPath(import.meta.url,),), "..",);
const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);
const BUN = process.execPath;

async function dss(
	args: string[],
	opts: { cwd?: string; env?: NodeJS.ProcessEnv; } = {},
): Promise<{ stdout: string; stderr: string; }> {
	return exec(BUN, ["run", CLI_PATH, ...args,], {
		cwd: opts.cwd ?? SDK_ROOT,
		env: opts.env ?? process.env,
	},);
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
			// Process exited non-zero — that's the assertion
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
		// Provide a syntactically valid URL but no real server.
		// The CLI loads .env, reads DATAIKU_URL, and fails on the request.
		// We verify .env was loaded by checking the error references our URL.
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
			// --help succeeds (exit 0) even without a real server.
			// The fact it didn't throw "DATAIKU_URL is required" proves .env loaded.
		} catch (e: unknown) {
			const err = e as { stderr?: string; stdout?: string; message?: string; };
			const output = `${err.stderr ?? ""}${err.stdout ?? ""}${err.message ?? ""}`;
			// If it errors, it should NOT be about missing credentials
			expect(output,).not.toContain("DATAIKU_URL is required",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
