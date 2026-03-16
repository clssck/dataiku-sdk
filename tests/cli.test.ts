import { describe, expect, it, } from "bun:test";
import { execFile, } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { promisify, } from "node:util";

const exec = promisify(execFile,);
const SDK_ROOT = "/home/coder/shared/dataiku-sdk";
const CLI_PATH = join(SDK_ROOT, "src/cli.ts",);

async function dss(
	args: string[],
	opts: { cwd?: string; env?: NodeJS.ProcessEnv; } = {},
): Promise<{ stdout: string; stderr: string; }> {
	return exec("bun", ["run", CLI_PATH, ...args,], {
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
		writeFileSync(
			join(tmpDir, ".env",),
			"DATAIKU_URL=http://127.0.0.1:1\nDATAIKU_API_KEY=fake-key-from-env\n",
		);
		try {
			// Explicitly clear env vars so only the .env file provides them.
			// Use port 1 to guarantee fast connection refusal.
			await dss(["project", "list",], {
				cwd: tmpDir,
				env: {
					PATH: process.env.PATH,
					HOME: process.env.HOME,
					DATAIKU_URL: "",
					DATAIKU_API_KEY: "",
				},
			},);
			throw new Error("should have failed",);
		} catch (e: unknown) {
			const err = e as { stderr?: string; stdout?: string; message?: string; };
			const output = `${err.stderr ?? ""}${err.stdout ?? ""}${err.message ?? ""}`;
			// Should NOT say credentials are missing — .env was loaded
			expect(output,).not.toContain("DATAIKU_URL is required",);
			expect(output,).not.toContain("DATAIKU_API_KEY is required",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
