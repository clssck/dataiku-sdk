import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

const RUNNABLES_BASE = "/public/api/projects/TEST/runnables";

function macroServer(
	opts: {
		outcome?: "success" | "failure" | "never-finish";
		resultBody?: string;
		resultContentType?: string;
	},
	capture: {
		runBodies: string[];
		abortBodies: string[];
		stateHits: number;
	},
) {
	return async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		const p = url.pathname;
		if (req.method === "GET" && p === `${RUNNABLES_BASE}/`) {
			sendJson(res, [
				{ runnableType: "m1", meta: { label: "Macro One", }, },
				{ runnableType: "m2", meta: { label: "Macro Two", }, },
			],);
			return;
		}
		if (req.method === "GET" && p === `${RUNNABLES_BASE}/m1`) {
			sendJson(res, {
				runnableType: "m1",
				ownerPluginId: "p",
				meta: { label: "Macro One", },
				resultType: "HTML",
				params: [{ name: "q", type: "STRING", },],
				adminParams: [],
			},);
			return;
		}
		if (req.method === "POST" && p === `${RUNNABLES_BASE}/m1`) {
			capture.runBodies.push(await readBody(req,),);
			sendJson(res, { runId: "run-42", },);
			return;
		}
		if (req.method === "POST" && p === `${RUNNABLES_BASE}/m1/abort/run-42`) {
			capture.abortBodies.push(await readBody(req,),);
			res.statusCode = 204;
			res.end();
			return;
		}
		if (req.method === "GET" && p === `${RUNNABLES_BASE}/m1/state/run-42`) {
			capture.stateHits += 1;
			if (opts.outcome === "never-finish") {
				// Always running: the bounded client wait must time out.
				sendJson(res, { running: true, progress: [], },);
				return;
			}
			if (capture.stateHits < 2 && opts.outcome !== "success") {
				sendJson(res, { running: true, progress: [], },);
				return;
			}
			if (opts.outcome === "failure") {
				sendJson(res, { running: false, resultError: { message: "boom", }, },);
				return;
			}
			sendJson(res, { running: false, type: "HTML", },);
			return;
		}
		if (req.method === "GET" && p === `${RUNNABLES_BASE}/m1/result/run-42`) {
			res.statusCode = 200;
			res.setHeader("Content-Type", opts.resultContentType ?? "text/html",);
			res.end(opts.resultBody ?? "<html>ok</html>",);
			return;
		}
		res.statusCode = 404;
		res.end(`unexpected ${req.method} ${p}`,);
	};
}

describe("macro CLI commands", () => {
	it("lists macros", async () => {
		await withCliServer(
			macroServer({}, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const { stdout, } = await dss(["macro", "list",], { env: cliEnv(url,), },);
				const list = JSON.parse(stdout,) as Array<{ runnableType?: string; }>;
				expect(list,).toHaveLength(2,);
				expect(list[0]?.runnableType,).toBe("m1",);
			},
		);
	});

	it("gets a macro definition", async () => {
		await withCliServer(
			macroServer({}, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const { stdout, } = await dss(["macro", "get", "m1",], { env: cliEnv(url,), },);
				const def = JSON.parse(stdout,) as { runnableType: string; resultType: string; };
				expect(def.runnableType,).toBe("m1",);
				expect(def.resultType,).toBe("HTML",);
			},
		);
	});

	it("runs a macro without --wait and prints only the runId envelope", async () => {
		const capture = { runBodies: [], abortBodies: [], stateHits: 0, };
		await withCliServer(macroServer({}, capture,), async (url,) => {
			const { stdout, } = await dss(["macro", "run", "m1",], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as { runId: string; };
			expect(result.runId,).toBe("run-42",);
		},);
		expect(capture.runBodies,).toEqual(["{}",],);
		expect(capture.stateHits,).toBe(0,);
	});

	it("run --wait polls to completion and exits 0 on success", async () => {
		await withCliServer(
			macroServer({ outcome: "success", }, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const { stdout, } = await dss([
					"macro",
					"run",
					"m1",
					"--wait",
					"--poll-interval",
					"1",
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { success: boolean; runId: string; };
				expect(result.success,).toBe(true,);
				expect(result.runId,).toBe("run-42",);
			},
		);
	});

	it("run-and-wait exits 4 when the macro run fails", async () => {
		await withCliServer(
			macroServer({ outcome: "failure", }, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const failure = await dssFailure([
					"macro",
					"run-and-wait",
					"m1",
					"--poll-interval",
					"1",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const parsed = JSON.parse(failure.stdout,) as {
					error: string;
					code: string;
					details?: { result?: { success?: boolean; timedOut?: boolean; }; };
				};
				expect(parsed.code,).toBe("long_running_failure",);
				expect(parsed.details?.result?.success,).toBe(false,);
			},
		);
	});

	it("aborts a run with an empty POST body and 204", async () => {
		const capture = { runBodies: [], abortBodies: [], stateHits: 0, };
		await withCliServer(macroServer({}, capture,), async (url,) => {
			const { stdout, } = await dss(["macro", "abort", "m1", "run-42",], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as { aborted: string; };
			expect(result.aborted,).toBe("run-42",);
		},);
		expect(capture.abortBodies,).toEqual(["{}",],);
	});

	it("prints the poll state", async () => {
		await withCliServer(
			macroServer({ outcome: "success", }, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const { stdout, } = await dss(["macro", "state", "m1", "run-42",], { env: cliEnv(url,), },);
				const state = JSON.parse(stdout,) as { running: boolean; };
				expect(state.running,).toBe(false,);
			},
		);
	});

	it("returns raw text as a JSON string for non-JSON results and parsed JSON otherwise", async () => {
		await withCliServer(
			macroServer({ resultBody: "<html>Report</html>", resultContentType: "text/html", }, {
				runBodies: [],
				abortBodies: [],
				stateHits: 0,
			},),
			async (url,) => {
				const { stdout, } = await dss(["macro", "result", "m1", "run-42",], {
					env: cliEnv(url,),
				},);
				// D8: stdout stays a single JSON document — the text body is
				// JSON-string encoded, not printed verbatim.
				const text = JSON.parse(stdout,) as string;
				expect(text,).toBe("<html>Report</html>",);
			},
		);

		await withCliServer(
			macroServer({ resultBody: '{"n":1}', resultContentType: "application/json", }, {
				runBodies: [],
				abortBodies: [],
				stateHits: 0,
			},),
			async (url,) => {
				const { stdout, } = await dss(["macro", "result", "m1", "run-42",], {
					env: cliEnv(url,),
				},);
				const parsed = JSON.parse(stdout,) as { n: number; };
				expect(parsed.n,).toBe(1,);
			},
		);
	});

	it("dry-run for run, run-and-wait, and abort makes zero network requests", async () => {
		await withCliServer((req, res,) => {
			res.statusCode = 500;
			res.end("dry-run must not contact DSS",);
		}, async (url,) => {
			const runDry = JSON.parse(
				(await dss(["macro", "run", "m1", "--dry-run",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(runDry,).toMatchObject({
				dryRun: true,
				resource: "macro",
				action: "run",
				id: "m1",
				method: "POST",
				endpoint: "/public/api/projects/TEST/runnables/m1?wait=false",
			},);

			const waitDry = JSON.parse(
				(await dss(["macro", "run-and-wait", "m1", "--dry-run",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(waitDry,).toMatchObject({
				dryRun: true,
				resource: "macro",
				action: "run-and-wait",
				id: "m1",
			},);

			const abortDry = JSON.parse(
				(await dss(["macro", "abort", "m1", "run-42", "--dry-run",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(abortDry,).toMatchObject({
				dryRun: true,
				resource: "macro",
				action: "abort",
				id: "m1",
				runId: "run-42",
				method: "POST",
				endpoint: "/public/api/projects/TEST/runnables/m1/abort/run-42",
			},);
		},);
	});

	it("validates wait flags before any network request", async () => {
		const negativeTimeout = await dssFailure([
			"macro",
			"run-and-wait",
			"m1",
			"--timeout",
			"-5",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(negativeTimeout.code,).toBe(1,);
		expect(negativeTimeout.stderr,).toBe("",);
		expect(negativeTimeout.stdout,).toContain("--timeout",);

		const badPoll = await dssFailure([
			"macro",
			"run-and-wait",
			"m1",
			"--poll-interval",
			"0",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(badPoll.code,).toBe(1,);
		expect(badPoll.stderr,).toBe("",);
		expect(badPoll.stdout,).toContain("--poll-interval",);
	});

	it("requires positional arguments", async () => {
		const missing = await dssFailure(["macro", "state", "m1",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(missing.code,).toBe(1,);
		expect(missing.stderr,).toBe("",);
		expect(missing.stdout,).toContain("Expected 2 argument(s)",);
	});

	it("times out the bounded wait to exit 4 with timedOut flag", async () => {
		await withCliServer(
			macroServer({ outcome: "never-finish", }, { runBodies: [], abortBodies: [], stateHits: 0, },),
			async (url,) => {
				const failure = await dssFailure([
					"macro",
					"run-and-wait",
					"m1",
					"--timeout",
					"50",
					"--poll-interval",
					"1",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain('"timedOut":true',);
				expect(failure.stdout,).toContain('"success":false',);
			},
		);
	});
});
