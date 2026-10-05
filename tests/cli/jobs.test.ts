import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "node:http";
import { cliEnv, dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

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

	it("reports DONE when every watched job succeeds", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.endsWith("/log",)) {
				res.statusCode = 200;
				res.setHeader("content-type", "text/plain",);
				res.end("",);
				return;
			}
			const jobId = decodeURIComponent(url.pathname.split("/",).at(-2,) ?? "",);
			sendJson(res, {
				baseStatus: { def: { id: jobId, type: "RECURSIVE_BUILD", }, state: "DONE", },
				globalState: { done: 1, failed: 0, running: 0, total: 1, },
			},);
		}, async (url,) => {
			for (const action of ["monitor", "watch",]) {
				const { stdout, stderr, } = await dss([
					"job",
					action,
					"JOB_DONE",
					"JOB_DONE_2",
					"--poll-interval",
					"1",
				], { env: cliEnv(url,), },);
				expect(stderr,).toBe("",);
				const aggregate = JSON.parse(stdout,) as {
					state: string;
					success: boolean;
					jobs: Array<{ jobId: string; success: boolean; }>;
				};
				expect(aggregate,).toMatchObject({
					state: "DONE",
					success: true,
					jobs: [
						{ jobId: "JOB_DONE", success: true, },
						{ jobId: "JOB_DONE_2", success: true, },
					],
				},);
			}
		},);
	});

	it("keeps monitor success when a terminal job's log endpoint returns not found", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.endsWith("/log/",)) {
				res.statusCode = 404;
				res.setHeader("content-type", "text/plain",);
				res.end("Job log not found",);
				return;
			}
			const jobId = decodeURIComponent(url.pathname.split("/",).at(-2,) ?? "",);
			sendJson(res, {
				baseStatus: { def: { id: jobId, type: "RECURSIVE_BUILD", }, state: "DONE", },
				globalState: { done: 1, failed: 0, running: 0, total: 1, },
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"job",
				"monitor",
				"JOB_LOG_404",
				"--poll-interval",
				"1",
			], { env: cliEnv(url,), },);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toMatchObject({
				success: true,
				state: "DONE",
				logUnavailable: "not_found",
				removed: false,
			},);
		},);
	});
});

describe("CLI job build-and-wait zero-activity builds", () => {
	const FLOW_GRAPH = {
		nodes: {
			sync_out: {
				type: "RUNNABLE_RECIPE",
				ref: "sync_out",
				predecessors: ["src",],
				successors: ["built",],
			},
			src: { type: "COMPUTABLE_DATASET", ref: "src", predecessors: [], successors: ["sync_out",], },
			built: {
				type: "COMPUTABLE_DATASET",
				ref: "built",
				predecessors: ["sync_out",],
				successors: [],
			},
			orphan: { type: "COMPUTABLE_DATASET", ref: "orphan", predecessors: [], successors: [], },
		},
	};

	function serveBuild(
		target: string,
		opts: { total: number; schemaStatus?: number; },
	): (req: IncomingMessage, res: ServerResponse,) => void {
		return (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				sendJson(res, { id: "job-zero", },);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jobs/job-zero/") {
				sendJson(res, {
					baseStatus: {
						def: {
							id: "job-zero",
							type: "RECURSIVE_FORCED_BUILD",
							projectKey: "TEST",
							outputs: [{
								type: "DATASET",
								targetDatasetProjectKey: "TEST",
								targetDataset: target,
								targetPartition: "NP",
							},],
						},
						state: "DONE",
					},
					globalState: { done: opts.total, failed: 0, running: 0, total: opts.total, },
				},);
				return;
			}
			if (url.pathname === `/public/api/projects/TEST/datasets/${target}/schema`) {
				if (opts.schemaStatus !== undefined) {
					sendJson(res, { message: "denied", }, opts.schemaStatus,);
					return;
				}
				sendJson(res, { columns: [], userModified: true, },);
				return;
			}
			if (url.pathname === `/public/api/projects/TEST/datasets/${target}`) {
				sendJson(res, { name: target, managed: true, },);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/flow/graph/") {
				sendJson(res, FLOW_GRAPH,);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		};
	}

	it("fails with long_running_failure when nothing can produce the target", async () => {
		await withCliServer(serveBuild("orphan", { total: 0, },), async (url,) => {
			const failure = await dssFailure([
				"job",
				"build-and-wait",
				"orphan",
				"--build-mode",
				"RECURSIVE_FORCED_BUILD",
				"--poll-interval",
				"1",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(4,);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report["code"],).toBe("long_running_failure",);
			expect(report["exitCode"],).toBe(4,);
			expect(report["error"],).toContain("TEST.orphan",);
			expect(report["error"],).toContain("no recipe produces",);
			const result = (report["details"] as { result: Record<string, unknown>; }).result;
			expect(result["success"],).toBe(false,);
			expect(result["state"],).toBe("DONE",);
			expect((result["failure"] as Record<string, unknown>)["code"],).toBe("nothing_to_build",);
		},);
	});

	it("keeps an up-to-date build of a produced dataset successful", async () => {
		await withCliServer(serveBuild("built", { total: 0, },), async (url,) => {
			const { stdout, } = await dss([
				"job",
				"build-and-wait",
				"built",
				"--build-mode",
				"RECURSIVE_BUILD",
				"--poll-interval",
				"1",
			], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result["success"],).toBe(true,);
			expect(result["state"],).toBe("DONE",);
			expect(result["failure"],).toBeUndefined();
		},);
	});

	it("warns built_dataset_has_no_columns after a build that ran and left an empty schema", async () => {
		await withCliServer(serveBuild("built", { total: 1, },), async (url,) => {
			const { stdout, stderr, } = await dss([
				"job",
				"build-and-wait",
				"built",
				"--poll-interval",
				"1",
			], { env: cliEnv(url,), },);
			expect((JSON.parse(stdout,) as Record<string, unknown>)["success"],).toBe(true,);
			const warnings = (JSON.parse(stderr.trim(),) as { warnings: Array<Record<string, unknown>>; })
				.warnings;
			expect(warnings.length,).toBe(1,);
			expect(warnings[0]?.["code"],).toBe("built_dataset_has_no_columns",);
			expect(warnings[0]?.["dataset"],).toBe("built",);
		},);
	});

	it("warns built_dataset_schema_unchecked instead of staying silent when the schema cannot be read", async () => {
		await withCliServer(serveBuild("built", { total: 1, schemaStatus: 403, },), async (url,) => {
			const { stdout, stderr, } = await dss([
				"job",
				"build-and-wait",
				"built",
				"--poll-interval",
				"1",
			], { env: cliEnv(url,), },);
			expect((JSON.parse(stdout,) as Record<string, unknown>)["success"],).toBe(true,);
			const warnings = (JSON.parse(stderr.trim(),) as { warnings: Array<Record<string, unknown>>; })
				.warnings;
			expect(warnings.length,).toBe(1,);
			expect(warnings[0]?.["code"],).toBe("built_dataset_schema_unchecked",);
			expect(warnings[0]?.["dataset"],).toBe("built",);
		},);
	});
});
