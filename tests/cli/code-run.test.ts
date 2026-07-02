import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";
import {
	cliEnv,
	dssFailure,
	dssWithInput,
	execProcessLogLine,
	join,
	readBody,
	rmSync,
	sendJson,
	shortProcessLogLine,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("code run command", () => {
	// Markers must match buildCodeRunScript in src/resources/scenarios.ts.
	const OUT_START = "<<<DSS_CODE_RUN_OUTPUT_b7e3a1>>>";
	const OUT_END = "<<<DSS_CODE_RUN_OUTPUT_END_b7e3a1>>>";

	type CodeRunCapture = {
		created: boolean;
		deleted: boolean;
		createBody?: Record<string, unknown>;
		script?: string;
	};

	function processLog(outputLines: string[],): string {
		return [
			"[2026/06/01-11:30:11.894] [Exec-1] [INFO] [dip.scenario.custompython]  - start scenario",
			execProcessLogLine(OUT_START,),
			...outputLines.map(execProcessLogLine,),
			execProcessLogLine(OUT_END,),
			"[2026/06/01-11:30:12.000] [wrapper-stderr-3] [INFO] [dku.utils]  - done",
		].join("\n",);
	}

	function codeRunServer(
		opts: { outcome: string; log: string; neverFinish?: boolean; failRunStep?: boolean; },
		capture: CodeRunCapture,
	) {
		return async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const p = url.pathname;
			if (req.method === "POST" && p === "/public/api/projects/TEST/scenarios/") {
				capture.created = true;
				capture.createBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "scn", },);
				return;
			}
			if (req.method === "PUT" && p.endsWith("/payload",)) {
				capture.script = (JSON.parse(await readBody(req,),) as { script?: string; }).script;
				sendJson(res, {},);
				return;
			}
			if (req.method === "POST" && p.endsWith("/run/",)) {
				if (opts.failRunStep) {
					res.statusCode = 500;
					res.end(`{"message":"run boom"}`,);
					return;
				}
				sendJson(res, { trigger: { id: "manual", }, runId: "trig-1", },);
				return;
			}
			if (req.method === "GET" && p.endsWith("/get-run-for-trigger",)) {
				const scenarioRun = opts.neverFinish
					? { runId: "run-1", }
					: { runId: "run-1", result: { outcome: opts.outcome, }, };
				sendJson(res, { scenarioRun, },);
				return;
			}
			if (req.method === "GET" && p.endsWith("/log",)) {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end(opts.log,);
				return;
			}
			if (req.method === "DELETE" && /\/scenarios\/[^/]+$/.test(p,)) {
				capture.deleted = true;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${p}`,);
		};
	}

	it("returns the script's clean output without DSS noise and deletes the scenario", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const log = processLog(["line one", "MARKER_OUT",],);
		await withCliServer(codeRunServer({ outcome: "SUCCESS", log, }, capture,), async (url,) => {
			const { stdout, } = await dssWithInput(["code", "run", "--stdin",], "print('hi')\n", {
				env: cliEnv(url,),
			},);
			const r = JSON.parse(stdout,) as {
				outcome: string;
				success: boolean;
				runId: string;
				output: string;
				log?: string;
			};
			expect(r.outcome,).toBe("SUCCESS",);
			expect(r.success,).toBe(true,);
			expect(r.runId,).toBe("run-1",);
			expect(r.output,).toBe("line one\nMARKER_OUT",);
			expect(r.output,).not.toContain("[process]",);
			expect(r.log,).toBeUndefined();
		},);
		expect(capture.created,).toBe(true,);
		expect(capture.script,).toContain("base64",);
		expect(capture.deleted,).toBe(true,);
	});

	it("includes the full DSS log with --full-log", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		await withCliServer(
			codeRunServer({ outcome: "SUCCESS", log: processLog(["only line",],), }, capture,),
			async (url,) => {
				const { stdout, } = await dssWithInput(
					["code", "run", "--stdin", "--full-log",],
					"print(1)\n",
					{ env: cliEnv(url,), },
				);
				const r = JSON.parse(stdout,) as { output: string; log?: string; };
				expect(r.output,).toBe("only line",);
				expect(r.log,).toContain("dip.scenario.custompython",);
			},
		);
	});

	it("caps code-run log retrieval with --max-log-bytes", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const log = processLog(["first", "second", "third",],);
		await withCliServer(
			codeRunServer({ outcome: "SUCCESS", log, }, capture,),
			async (url,) => {
				const { stdout, } = await dssWithInput(
					["code", "run", "--stdin", "--full-log", "--max-log-bytes", "64",],
					"print(1)\n",
					{ env: cliEnv(url,), },
				);
				const r = JSON.parse(stdout,) as { log: string; logTruncated: boolean; maxLogBytes: number; };
				expect(Buffer.byteLength(r.log, "utf-8",),).toBeLessThanOrEqual(64,);
				expect(r.logTruncated,).toBe(true,);
				expect(r.maxLogBytes,).toBe(64,);
			},
		);
	});

	it("falls back to the raw log when output markers are absent", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		await withCliServer(
			codeRunServer({ outcome: "SUCCESS", log: "no markers here\n", }, capture,),
			async (url,) => {
				const { stdout, } = await dssWithInput(["code", "run", "--stdin",], "print(1)\n", {
					env: cliEnv(url,),
				},);
				const r = JSON.parse(stdout,) as { output: string; log?: string; };
				expect(r.output,).toBe("",);
				expect(r.log,).toContain("no markers here",);
			},
		);
	});

	it("exits 4 with the captured traceback when the script run fails", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const scriptPath = join(tmpdir(), `dss-coderun-fail-${Date.now()}.py`,);
		writeFileSync(scriptPath, "raise ValueError('boom')\n",);
		const log = processLog(["Traceback (most recent call last):", "ValueError: boom",],);
		try {
			await withCliServer(codeRunServer({ outcome: "FAILED", log, }, capture,), async (url,) => {
				const failure = await dssFailure(["code", "run", "--file", scriptPath,], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toContain("long_running_failure",);
				expect(failure.stderr,).toContain("ValueError: boom",);
			},);
		} finally {
			rmSync(scriptPath, { force: true, },);
		}
		expect(capture.deleted,).toBe(true,);
	});

	it("maps --env to an explicit code-env selection", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		await withCliServer(
			codeRunServer({ outcome: "SUCCESS", log: processLog(["x",],), }, capture,),
			async (url,) => {
				await dssWithInput(["code", "run", "--stdin", "--env", "py39_pandas",], "print(1)\n", {
					env: cliEnv(url,),
				},);
			},
		);
		const params = (capture.createBody?.params ?? {}) as {
			envSelection?: { envMode?: string; envName?: string; };
		};
		expect(params.envSelection,).toEqual({ envMode: "EXPLICIT_ENV", envName: "py39_pandas", },);
	});

	it("leaves the scenario in place with --keep", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		await withCliServer(
			codeRunServer({ outcome: "SUCCESS", log: processLog(["x",],), }, capture,),
			async (url,) => {
				await dssWithInput(["code", "run", "--stdin", "--keep",], "print(1)\n", {
					env: cliEnv(url,),
				},);
			},
		);
		expect(capture.created,).toBe(true,);
		expect(capture.deleted,).toBe(false,);
	});

	it("requires a Python source", async () => {
		const failure = await dssFailure(["code", "run",], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("Python source is required",);
	});

	it("times out to exit 4 and still cleans up the scenario", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const scriptPath = join(tmpdir(), `dss-coderun-timeout-${Date.now()}.py`,);
		writeFileSync(scriptPath, "print(1)\n",);
		try {
			await withCliServer(
				codeRunServer({ outcome: "SUCCESS", log: "", neverFinish: true, }, capture,),
				async (url,) => {
					const failure = await dssFailure(
						["code", "run", "--file", scriptPath, "--timeout", "2000",],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toContain("TIMEOUT",);
				},
			);
		} finally {
			rmSync(scriptPath, { force: true, },);
		}
		expect(capture.deleted,).toBe(true,);
	});

	it("cleans up the scenario when the run step errors", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const scriptPath = join(tmpdir(), `dss-coderun-midfail-${Date.now()}.py`,);
		writeFileSync(scriptPath, "print(1)\n",);
		try {
			await withCliServer(
				codeRunServer({ outcome: "SUCCESS", log: "", failRunStep: true, }, capture,),
				async (url,) => {
					const failure = await dssFailure(["code", "run", "--file", scriptPath,], {
						env: cliEnv(url,),
					},);
					expect(failure.code,).not.toBe(0,);
					expect(failure.code,).not.toBe(1,);
				},
			);
		} finally {
			rmSync(scriptPath, { force: true, },);
		}
		expect(capture.created,).toBe(true,);
		expect(capture.deleted,).toBe(true,);
	});

	it("extracts clean output across CRLF logs and user-printed marker lines", async () => {
		const capture: CodeRunCapture = { created: false, deleted: false, };
		const log = [
			"[t] [E] [INFO] [dip.scenario]  - start",
			shortProcessLogLine(OUT_START,),
			shortProcessLogLine("real output",),
			shortProcessLogLine(OUT_END,),
			shortProcessLogLine("after fake end",),
			shortProcessLogLine(OUT_END,),
			"[t] [W] [INFO] [dku.utils]  - done",
		].join("\r\n",);
		await withCliServer(codeRunServer({ outcome: "SUCCESS", log, }, capture,), async (url,) => {
			const { stdout, } = await dssWithInput(["code", "run", "--stdin",], "print(1)\n", {
				env: cliEnv(url,),
			},);
			const r = JSON.parse(stdout,) as { output: string; };
			expect(r.output,).toBe(`real output\n${OUT_END}\nafter fake end`,);
		},);
	});

	it("rejects positional args and conflicting sources", async () => {
		const f1 = await dssFailure(["code", "run", "extra.py",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(f1.code,).toBe(1,);
		expect(f1.stderr,).toContain("no positional arguments",);
		const f2 = await dssFailure(["code", "run", "--file", "x.py", "--stdin",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(f2.code,).toBe(1,);
		expect(f2.stderr,).toContain("exactly one",);
	});
});
