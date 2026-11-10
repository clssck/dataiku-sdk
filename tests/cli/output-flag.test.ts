import { afterAll, beforeAll, describe, expect, it, } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, statSync, writeFileSync, } from "node:fs";
import { join, } from "node:path";
import { cliEnv, dss, dssFailure, rmSync, sendJson, tmpdir, withCliServer, } from "./_harness.js";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";

const COLUMN_COUNT = 120;
const columns = Array.from({ length: COLUMN_COUNT, }, (_, index,) => ({
	name: `column_${String(index,).padStart(3, "0",)}`,
	type: "string",
}),);

type Handler = (req: IncomingMessage, res: ServerResponse,) => void;

/** One mock covering the reads, a 404, a running job, and a job log. */
function mockDss(requests: string[] = [],): Handler {
	return (req, res,) => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		requests.push(`${req.method ?? ""} ${url.pathname}`,);
		const base = "/public/api/projects/TEST";
		if (req.method === "GET" && url.pathname === `${base}/datasets/orders/schema`) {
			sendJson(res, { columns, },);
			return;
		}
		if (req.method === "GET" && url.pathname === `${base}/datasets/orders/data/`) {
			res.setHeader("Content-Type", "text/plain",);
			res.end("order_id\nA1\nA2\n",);
			return;
		}
		if (req.method === "GET" && url.pathname === `${base}/datasets/orders`) {
			sendJson(res, { name: "orders", type: "Filesystem", projectKey: "TEST", },);
			return;
		}
		if (req.method === "GET" && url.pathname === `${base}/jobs/job-running/`) {
			sendJson(res, {
				baseStatus: { def: { id: "job-running", type: "DATASET_BUILD", }, state: "RUNNING", },
				globalState: { done: 0, failed: 0, running: 1, total: 1, },
			},);
			return;
		}
		if (req.method === "GET" && url.pathname === `${base}/jobs/job-1/log/`) {
			res.setHeader("Content-Type", "text/plain",);
			res.end("line 1\nline 2\n",);
			return;
		}
		if (req.method === "GET" && url.pathname === `${base}/datasets/`) {
			sendJson(res, [],);
			return;
		}
		res.statusCode = 404;
		res.setHeader("Content-Type", "text/plain",);
		res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
	};
}

function env(url: string, extra: Record<string, string | undefined> = {},): NodeJS.ProcessEnv {
	return { ...cliEnv(url,), DSS_PRETTY: undefined, ...extra, };
}

const hermetic: NodeJS.ProcessEnv = {
	PATH: process.env.PATH ?? "",
	HOME: process.env.HOME ?? "",
	DATAIKU_DISABLE_ENV: "1",
};

type WrittenResult = { path: string; bytes: number; };

function parseWritten(stdout: string,): WrittenResult {
	return JSON.parse(stdout,) as WrittenResult;
}

function lineLengths(text: string,): number[] {
	return text.split("\n",).map((line,) => Buffer.byteLength(line,));
}

let dir: string;

beforeAll(() => {
	dir = mkdtempSync(join(tmpdir(), "dss-output-flag-",),);
},);

afterAll(() => {
	rmSync(dir, { recursive: true, force: true, },);
},);

describe("global --output on commands that do not declare it", () => {
	it("writes the printed value to the file and reports {path, bytes}", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const args = ["dataset", "preview", "orders", "--max-rows", "2",];
			const expected = JSON.parse((await dss(args, { env: env(url,), },)).stdout,) as unknown;
			expect(expected,).toMatchObject({ rowCount: 2, },);

			const file = join(dir, "preview.json",);
			const { stdout, stderr, } = await dss([...args, "--output", file,], { env: env(url,), },);
			expect(stderr,).toBe("",);
			expect(Object.keys(JSON.parse(stdout,) as object,),).toEqual(["path", "bytes",],);
			const written = parseWritten(stdout,);
			expect(written.path,).toBe(file,);
			expect(written.bytes,).toBe(statSync(file,).size,);
			expect(JSON.parse(readFileSync(file, "utf8",),),).toEqual(expected,);

			const shortFile = join(dir, "preview-short.json",);
			const short = await dss([...args, "-o", shortFile,], { env: env(url,), },);
			expect(parseWritten(short.stdout,).path,).toBe(shortFile,);
			expect(JSON.parse(readFileSync(shortFile, "utf8",),),).toEqual(expected,);

			const inlineFile = join(dir, "preview-inline.json",);
			await dss([...args, `--output=${inlineFile}`,], { env: env(url,), },);
			expect(JSON.parse(readFileSync(inlineFile, "utf8",),),).toEqual(expected,);
		},);
	});

	it("applies --fields projection before writing", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const args = [
				"dataset",
				"preview",
				"orders",
				"--max-rows",
				"2",
				"--fields",
				"columns,rowCount",
			];
			const projected = JSON.parse((await dss(args, { env: env(url,), },)).stdout,) as unknown;
			expect(Object.keys(projected as object,),).toEqual(["columns", "rowCount",],);

			const file = join(dir, "projected.json",);
			const { stdout, } = await dss([...args, "-o", file,], { env: env(url,), },);
			expect(JSON.parse(readFileSync(file, "utf8",),),).toEqual(projected,);
			expect(parseWritten(stdout,).bytes,).toBe(statSync(file,).size,);
		},);
	});

	it("creates missing parent directories and treats `-` as an ordinary file name", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const nested = join(dir, "a", "b", "c", "schema.json",);
			await dss(["dataset", "schema", "orders", "--output", nested,], { env: env(url,), },);
			expect(existsSync(nested,),).toBe(true,);

			const cwd = mkdtempSync(join(dir, "cwd-",),);
			const { stdout, } = await dss(["dataset", "schema", "orders", "--output", "-",], {
				env: env(url,),
				cwd,
			},);
			expect(parseWritten(stdout,).path,).toBe("-",);
			expect(
				(JSON.parse(readFileSync(join(cwd, "-",), "utf8",),) as { columns: unknown[]; }).columns,
			).toHaveLength(COLUMN_COUNT,);
		},);
	});

	it("line-wraps large results like stdout, and DSS_PRETTY=0 writes one line", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const args = ["dataset", "schema", "orders",];
			const printed = (await dss(args, { env: env(url,), },)).stdout;
			expect(Math.max(...lineLengths(printed,),),).toBeLessThanOrEqual(760,);

			const wrapped = join(dir, "wrapped.json",);
			await dss([...args, "--output", wrapped,], { env: env(url,), },);
			const text = readFileSync(wrapped, "utf8",);
			expect(text,).toBe(printed,);
			expect(text.trimEnd().split("\n",).length,).toBeGreaterThan(1,);
			for (const length of lineLengths(text,)) expect(length,).toBeLessThanOrEqual(760,);

			const compact = join(dir, "compact.json",);
			await dss([...args, "--output", compact,], { env: env(url, { DSS_PRETTY: "0", },), },);
			const compactText = readFileSync(compact, "utf8",);
			expect(compactText.endsWith("\n",),).toBe(true,);
			expect(compactText.trimEnd().includes("\n",),).toBe(false,);
			expect(JSON.parse(compactText,),).toEqual(JSON.parse(text,),);
		},);
	});

	it("prints the error envelope and writes nothing when the command fails", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const file = join(dir, "never.json",);
			const failure = await dssFailure(["dataset", "get", "MISSING", "--output", file,], {
				env: env(url,),
			},);
			expect(failure.code,).not.toBe(0,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({ type: "error", },);
			expect(existsSync(file,),).toBe(false,);
		},);
	});

	it("writes a failed long-running result and keeps exit code 4", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const file = join(dir, "job-wait.json",);
			const failure = await dssFailure(
				["job", "wait", "job-running", "--timeout", "10", "--output", file,],
				{ env: env(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const written = parseWritten(failure.stdout,);
			expect(written.path,).toBe(file,);
			expect(written.bytes,).toBe(statSync(file,).size,);
			expect(JSON.parse(readFileSync(file, "utf8",),),).toMatchObject({
				success: false,
				timedOut: true,
				state: "RUNNING",
			},);
		},);
	});

	it("reports an unwritable path as an error envelope", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const blocker = join(dir, "blocker",);
			writeFileSync(blocker, "x",);
			const failure = await dssFailure(
				["dataset", "schema", "orders", "--output", join(blocker, "out.json",),],
				{ env: env(url,), },
			);
			expect(failure.code,).toBe(2,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({ type: "error", exitCode: 2, },);
		},);
	});

	it("rejects an empty or blank path before running anything", async () => {
		const requests: string[] = [];
		await withCliServer(mockDss(requests,), async (url,) => {
			for (const value of ["", "   ",]) {
				const failure = await dssFailure(["dataset", "schema", "orders", "--output", value,], {
					env: env(url,),
				},);
				expect(failure.code,).toBe(1,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					type: "error",
					code: "invalid_flag_value",
					category: "usage",
				},);
			}
			const failure = await dssFailure(["version", "-o", "",], { env: hermetic, },);
			expect(JSON.parse(failure.stdout,),).toMatchObject({ code: "invalid_flag_value", },);
		},);
		expect(requests,).toEqual([],);
	});

	it("writes --plan and --dry-run results", async () => {
		const plan = join(dir, "plan.json",);
		const planned = await dss(["project", "delete", "OLD", "--plan", "--output", plan,], {
			env: hermetic,
		},);
		expect(parseWritten(planned.stdout,).bytes,).toBe(statSync(plan,).size,);
		expect(JSON.parse(readFileSync(plan, "utf8",),),).toMatchObject({
			plan: true,
			resource: "project",
			action: "delete",
		},);

		await withCliServer(mockDss(), async (url,) => {
			const args = ["dataset", "delete", "orders", "--dry-run",];
			const expected = JSON.parse((await dss(args, { env: env(url,), },)).stdout,) as unknown;
			expect(expected,).toMatchObject({ dryRun: true, },);
			const file = join(dir, "dry-run.json",);
			const { stdout, } = await dss([...args, "-o", file,], { env: env(url,), },);
			expect(parseWritten(stdout,).path,).toBe(file,);
			expect(JSON.parse(readFileSync(file, "utf8",),),).toEqual(expected,);
		},);
	});

	it("writes the result of meta commands", async () => {
		const printed = JSON.parse((await dss(["version",], { env: hermetic, },)).stdout,) as unknown;
		const file = join(dir, "version.json",);
		const { stdout, } = await dss(["version", "--output", file,], { env: hermetic, },);
		expect(parseWritten(stdout,).bytes,).toBe(statSync(file,).size,);
		expect(JSON.parse(readFileSync(file, "utf8",),),).toEqual(printed,);

		const viaFlag = join(dir, "version-flag.json",);
		await dss(["--version", "-o", viaFlag,], { env: hermetic, },);
		expect(JSON.parse(readFileSync(viaFlag, "utf8",),),).toEqual(printed,);

		const contractFile = join(dir, "contract.json",);
		const contract = await dss(
			["agent", "contract", "--fields", "commands.globalFlags", "--output", contractFile,],
			{ env: hermetic, },
		);
		expect(parseWritten(contract.stdout,).path,).toBe(contractFile,);
		expect(JSON.parse(readFileSync(contractFile, "utf8",),),).toHaveProperty(
			["commands.globalFlags",],
		);
	});
});

describe("declared --output is unchanged", () => {
	it("job log and recipe get-payload keep their own --output behavior", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.endsWith("/recipes/my_recipe",)) {
				sendJson(res, { recipe: { type: "python", }, payload: "import os\n", },);
				return;
			}
			mockDss()(req, res,);
		}, async (url,) => {
			const log = join(dir, "job.log",);
			const logRun = await dss(["job", "log", "job-1", "--output", log,], { env: env(url,), },);
			expect(JSON.parse(logRun.stdout,),).toBe(log,);
			expect(readFileSync(log, "utf8",),).toBe("line 1\nline 2\n",);

			const payload = join(dir, "payload.py",);
			const payloadRun = await dss(
				["recipe", "get-payload", "my_recipe", "-o", payload,],
				{ env: env(url,), },
			);
			expect(JSON.parse(payloadRun.stdout,),).toBe(payload,);
			expect(readFileSync(payload, "utf8",),).toBe("import os\n",);
		},);
	});
});

describe("global --output in batches", () => {
	it("rejects a step carrying the global flag before any request", async () => {
		const requests: string[] = [];
		await withCliServer(mockDss(requests,), async (url,) => {
			const file = join(dir, "step.json",);
			for (const flag of ["--output", "-o",]) {
				const failure = await dssFailure([
					"batch",
					"--data",
					JSON.stringify([["dataset", "list",], ["dataset", "schema", "orders", flag, file,],],),
				], { env: env(url,), },);
				expect(failure.code,).toBe(1,);
				const report = JSON.parse(failure.stdout,) as { code: string; error: string; hint: string; };
				expect(report.code,).toBe("usage_error",);
				expect(report.error,).toContain("Batch step 1",);
				expect(report.hint,).toContain("dss batch",);
			}
			expect(existsSync(file,),).toBe(false,);
		},);
		expect(requests,).toEqual([],);

		const dry = await dssFailure([
			"batch",
			"--dry-run",
			"--data",
			JSON.stringify([["dataset", "list", "--output", "x.json",],],),
		], { env: hermetic, },);
		expect(
			(JSON.parse(dry.stdout,) as { steps: Array<{ runnable: boolean; }>; }).steps[0].runnable,
		).toBe(false,);
	});

	it("still runs steps whose command declares --output", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const log = join(dir, "batch-job.log",);
			const { stdout, } = await dss([
				"batch",
				"--data",
				JSON.stringify([["job", "log", "job-1", "--output", log,],],),
			], { env: env(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({ ok: true, },);
			expect(readFileSync(log, "utf8",),).toBe("line 1\nline 2\n",);
		},);
	});

	it("writes the batch envelope when --output is on dss batch", async () => {
		await withCliServer(mockDss(), async (url,) => {
			const args = ["batch", "--data", JSON.stringify([["dataset", "list",],],),];
			const expected = JSON.parse((await dss(args, { env: env(url,), },)).stdout,) as unknown;
			expect(expected,).toMatchObject({ ok: true, },);
			const file = join(dir, "batch.json",);
			const { stdout, } = await dss([...args, "--output", file,], { env: env(url,), },);
			expect(parseWritten(stdout,).bytes,).toBe(statSync(file,).size,);
			expect(JSON.parse(readFileSync(file, "utf8",),),).toEqual(expected,);
		},);
	});
});

describe("contract", () => {
	it("lists output among the global flags", async () => {
		const flag = { name: "output", kind: "value", valueType: "PATH", };
		const { stdout, } = await dss(["agent", "contract", "--fields", "commands.globalFlags",], {
			env: hermetic,
		},);
		expect(
			(JSON.parse(stdout,) as { "commands.globalFlags": unknown[]; })["commands.globalFlags"],
		).toContainEqual(flag,);
	});
});
