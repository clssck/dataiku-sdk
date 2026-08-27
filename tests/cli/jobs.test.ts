import { describe, expect, it, } from "bun:test";
import { cliEnv, dssFailure, sendJson, withCliServer, } from "./_harness.js";

describe("CLI job aggregation", () => {
	it("fails monitor and watch when any job fails", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.endsWith("/log",)) {
				res.statusCode = 200;
				res.setHeader("content-type", "text/plain",);
				res.end("",);
				return;
			}
			const jobId = decodeURIComponent(url.pathname.split("/",).at(-2,) ?? "",);
			const state = jobId === "JOB_FAILED" ? "FAILED" : "DONE";
			sendJson(res, {
				baseStatus: { def: { id: jobId, type: "RECURSIVE_BUILD", }, state, },
				globalState: {
					done: state === "DONE" ? 1 : 0,
					failed: state === "FAILED" ? 1 : 0,
					running: 0,
					total: 1,
				},
			},);
		}, async (url,) => {
			for (const action of ["monitor", "watch",]) {
				const failure = await dssFailure([
					"job",
					action,
					"JOB_DONE",
					"JOB_FAILED",
					"--poll-interval",
					"1",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "long_running_failure",
					details: {
						result: {
							state: "FAILED",
							success: false,
							jobs: [
								{ jobId: "JOB_DONE", success: true, },
								{ jobId: "JOB_FAILED", success: false, },
							],
						},
					},
				},);
			}
		},);
	});
});
