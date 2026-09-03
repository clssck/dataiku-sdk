import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient,) => Promise<void>,
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
	const client = new DataikuClient({
		url: `http://127.0.0.1:${port}`,
		apiKey: "test",
		projectKey: "TEST",
	},);

	try {
		await run(client,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error?: Error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

async function readRequestBody(req: IncomingMessage,): Promise<string> {
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

describe("FoldersResource.resolveId", () => {
	it("prefers exact IDs, resolves exact names, and preserves missing values", async () => {
		let requestCount = 0;

		await withDataikuServer((req, res,) => {
			requestCount += 1;
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/",);
			sendJson(res, [
				{ id: "folder-id", name: "Shared data", },
				{ id: "folder-by-name", name: "Exports", },
			],);
		}, async (client,) => {
			expect(await client.folders.resolveId("folder-id",),).toBe("folder-id",);
			expect(await client.folders.resolveId("Exports",),).toBe("folder-by-name",);
			expect(await client.folders.resolveId("missing-folder",),).toBe("missing-folder",);
		},);

		expect(requestCount,).toBe(3,);
	});
});

describe("FoldersResource.create", () => {
	it("posts managed folder creation payload with trailing slash and the official project and folder template path", async () => {
		let createBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/",);
			createBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "folder-id", name: "exports", },);
		}, async (client,) => {
			const result = await client.folders.create({
				name: "exports",
				type: "S3",
				connection: "s3_conn",
			},);
			expect(result,).toEqual({ id: "folder-id", name: "exports", },);
		},);

		expect(createBody,).toEqual({
			name: "exports",
			projectKey: "TEST",
			type: "S3",
			params: {
				connection: "s3_conn",
				path: "/${projectKey}/${odbId}",
			},
		},);
	});
	it("selects writable managed-folder storage when connection and type are omitted", async () => {
		let createBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/admin/connections/") {
				sendJson(res, {
					filesystem_folders: {
						allowWrite: true,
						allowManagedFolders: false,
					},
					"dataiku-managed-storage": {
						allowWrite: true,
						allowManagedFolders: true,
					},
				},);
				return;
			}
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/",);
			createBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "folder-id", name: "exports", },);
		}, async (client,) => {
			await client.folders.create({ name: "exports", },);
		},);

		expect(createBody,).toEqual({
			name: "exports",
			projectKey: "TEST",
			type: null,
			params: {
				connection: "dataiku-managed-storage",
				path: "/${projectKey}/${odbId}",
			},
		},);
	});
});

describe("FoldersResource.update", () => {
	it("deep-merges current managed folder settings and PUTs the result", async () => {
		const requests: string[] = [];
		let updateBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET") {
				expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/folder-id",);
				sendJson(res, {
					id: "folder-id",
					name: "exports",
					type: "Filesystem",
					params: { connection: "filesystem", path: "/dataiku/TEST/exports", },
					tags: ["old",],
				},);
				return;
			}
			expect(req.method,).toBe("PUT",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/folder-id",);
			updateBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			sendJson(res, { ok: true, },);
		}, async (client,) => {
			await client.folders.update("folder-id", { tags: ["agent",], params: { custom: true, }, },);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/managedfolders/folder-id",
			"PUT /public/api/projects/TEST/managedfolders/folder-id",
		],);
		expect(updateBody,).toEqual({
			id: "folder-id",
			name: "exports",
			type: "Filesystem",
			params: { connection: "filesystem", path: "/dataiku/TEST/exports", custom: true, },
			tags: ["agent",],
		},);
	});
});

describe("FoldersResource.delete", () => {
	it("deletes a managed folder by id", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("DELETE",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/folder-id",);
			res.statusCode = 204;
			res.end();
		}, async (client,) => {
			await client.folders.delete("folder-id",);
		},);
	});
});

describe("JobsResource.log", () => {
	it("tails 500 lines by default and preserves activity filtering", async () => {
		const fullLog = Array.from({ length: 600, }, (_value, index,) => `line ${index + 1}`,).join(
			"\n",
		);

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			expect(url.searchParams.get("activity",),).toBe("build",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (client,) => {
			const log = await client.jobs.log("job-1", { activity: "build", },);
			const lines = log.split("\n",);
			expect(lines,).toHaveLength(500,);
			expect(lines[0],).toBe("line 101",);
			expect(lines[lines.length - 1],).toBe("line 600",);
		},);
	});

	it("returns the full log when maxLogLines is 0 or -1", async () => {
		const fullLog = Array.from({ length: 600, }, (_value, index,) => `line ${index + 1}`,).join(
			"\n",
		);
		let requestCount = 0;

		await withDataikuServer((req, res,) => {
			requestCount += 1;
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-2/log/",);
			expect(url.search,).toBe("",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (client,) => {
			expect(await client.jobs.log("job-2", { maxLogLines: 0, },),).toBe(fullLog,);
			expect(await client.jobs.log("job-2", { maxLogLines: -1, },),).toBe(fullLog,);
		},);

		expect(requestCount,).toBe(2,);
	});

	it("fetches selected activity logs through the public API", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-3/log/",);
			expect(url.searchParams.get("activity",),).toBe("activity-1",);
			expect(url.searchParams.has("logId",),).toBe(false,);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("python stdout\n",);
		}, async (client,) => {
			await expect(client.jobs.log("job-3", {
				activity: "activity-1",
				logId: "/python-recipe/python-output.log",
			},),).resolves.toBe("python stdout\n",);
		},);
	});

	it("translates a pasted UI activity log URL to the public job log endpoint", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-4/log/",);
			expect(url.searchParams.get("activity",),).toBe("activity-1",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("public api stdout\n",);
		}, async (client,) => {
			await expect(client.jobs.logFromUrl(
				"https://dss/dip/api/flow/jobs/cat-activity-log?projectKey=TEST&jobId=job-4&activityId=activity-1&logId=/python-recipe/python-output.log",
			),).resolves.toBe("public api stdout\n",);
		},);
	});

	it("bounds oversized logs to the tail and line-limits from the retained bytes", async () => {
		const fullLog = Array.from({ length: 600, }, (_value, index,) => `line ${index + 1}`,).join(
			"\n",
		);
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-tail/log/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (client,) => {
			const log = await client.jobs.log("job-tail", { maxLogBytes: 200, maxLogLines: 2, },);
			const lines = log.split("\n",);
			expect(log.length,).toBeLessThan(fullLog.length,);
			expect(lines,).toHaveLength(2,);
			expect(lines[0],).toBe("line 599",);
			expect(lines[1],).toBe("line 600",);
		},);
	});

	it("selects recipe subprocess stdout and stderr by DSS reader-thread tags", async () => {
		// The wrapper metadata line mentions both pipe names but is neither
		// child stdout nor stderr; it must not be selected by either filter.
		const wrapperLine =
			'[2026/09/02-14:34:00.900] [FRT-76-FlowRunnable] [INFO] [dku.security.process] act.x - Data read from wrapper: {"pid":1,"pipes":{"stdin":"/j/.stdin","stdout":"/j/.stdout","stderr":"/j/.stderr"}}';
		const dssLog = [
			"[2026/09/02-14:34:01.000] [FRT-76-FlowRunnable] [INFO] [dku.flow.python] act.x - Starting execution of user's Python code",
			wrapperLine,
			"[2026/09/02-14:34:01.500] [null-err-85] [INFO] [dku.utils]  - 2026-09-02 14:34:01,499 INFO Setup complete, ready to execute Python code",
			"[2026/09/02-14:34:01.801] [null-out-84] [INFO] [dku.utils]  - OK polars 1.44.1 rows 3",
			"stdout: legacy record",
			"stderr: legacy record",
			"[2026/09/02-14:34:02.000] [null-err-85] [INFO] [dku.utils]  - Traceback (most recent call last):",
			"[2026/09/02-14:34:02.466] [ActivityExecutor-66] [INFO] [dku.flow.activity] running x - Activity is successful",
		].join("\n",);
		await withDataikuServer((_req, res,) => {
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(dssLog,);
		}, async (client,) => {
			expect((await client.jobs.log("job-py", { logFilter: "stdout", },)).split("\n",),).toEqual([
				"[2026/09/02-14:34:01.801] [null-out-84] [INFO] [dku.utils]  - OK polars 1.44.1 rows 3",
				"stdout: legacy record",
			],);
			const stderr = (await client.jobs.log("job-py", { logFilter: "stderr", },)).split("\n",);
			expect(stderr,).toHaveLength(3,);
			expect(stderr,).not.toContain(wrapperLine,);
			expect(stderr,).toContain("stderr: legacy record",);
		},);
	});
});

describe("JobsResource.wait", () => {
	it("returns terminal success details and includes logs when requested", async () => {
		const requests: string[] = [];
		let statusRequests = 0;

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/") {
				statusRequests += 1;
				sendJson(res, {
					baseStatus: {
						def: { id: "job-1", type: "DATASET_BUILD", },
						state: statusRequests === 1 ? "RUNNING" : "DONE",
					},
					globalState: statusRequests === 1
						? { done: 0, failed: 0, running: 1, total: 1, }
						: { done: 1, failed: 0, running: 0, total: 1, },
				}, 200,);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/log/") {
				expect(url.searchParams.get("activity",),).toBe("build",);
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end("started\nfinished",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-1", {
				activity: "build",
				includeLogs: true,
				maxLogLines: -1,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toEqual({
				success: true,
				jobId: "job-1",
				state: "DONE",
				type: "DATASET_BUILD",
				elapsedMs: expect.any(Number,),
				pollCount: 2,
				progress: { done: 1, failed: 0, running: 0, total: 1, },
				log: "started\nfinished",
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/jobs/job-1/",
			"GET /public/api/projects/TEST/jobs/job-1/",
			"GET /public/api/projects/TEST/jobs/job-1/log/?activity=build",
		],);
	});

	it("applies the log filter before the line limit so early stdout survives a small tail", async () => {
		const dssLog = [
			"[2026/09/02-14:34:01.801] [null-out-84] [INFO] [dku.utils]  - OK polars 1.44.1 rows 3",
			...Array.from(
				{ length: 20, },
				(_v, i,) =>
					`[2026/09/02-14:34:02.${
						String(i,).padStart(3, "0",)
					}] [ActivityExecutor-66] [INFO] [dku.flow.activity] backend line ${i}`,
			),
		].join("\n",);
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname === "/public/api/projects/TEST/jobs/job-filter/") {
				sendJson(res, {
					baseStatus: { def: { id: "job-filter", type: "DATASET_BUILD", }, state: "DONE", },
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(dssLog,);
		}, async (client,) => {
			const result = await client.jobs.wait("job-filter", {
				includeLogs: true,
				logFilter: "stdout",
				maxLogLines: 5,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);
			expect(result.log,).toBe(
				"[2026/09/02-14:34:01.801] [null-out-84] [INFO] [dku.utils]  - OK polars 1.44.1 rows 3",
			);
		},);
	});

	it("summarizes Dataiku Python progress counters from terminal logs", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname === "/public/api/projects/TEST/jobs/job-progress/") {
				sendJson(res, {
					baseStatus: { def: { id: "job-progress", type: "DATASET_BUILD", }, state: "DONE", },
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jobs/job-progress/log/") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end([
					"Scanned 39,520,498, matched 825,328,253",
					"Scanned 39,520,498, joined 303,833,978, written 15,969,743",
					"825328253 rows successfully written",
					"Done! everything committed",
				].join("\n",),);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-progress", {
				summary: true,
				maxLogLines: 10,
				pollIntervalMs: 1,
			},);

			expect(result.logSummary?.progress?.counters.scanned,).toBe(39_520_498,);
			expect(result.logSummary?.progress?.counters.matched,).toBe(825_328_253,);
			expect(result.logSummary?.progress?.counters.joined,).toBe(303_833_978,);
			expect(result.logSummary?.progress?.counters.written,).toBe(825_328_253,);
			expect(result.logSummary?.progress?.doneLine,).toBe("Done! everything committed",);
		},);
	});

	it("returns a timeout result for non-terminal jobs without fetching logs", async () => {
		const originalDateNow = Date.now;
		Date.now = (() => {
			let callCount = 0;
			return () => {
				callCount += 1;
				return callCount === 1 ? 0 : 2;
			};
		})();

		let logRequested = false;

		try {
			await withDataikuServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);

				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-timeout/") {
					sendJson(res, {
						baseStatus: {
							def: { type: "DATASET_BUILD", },
							state: "RUNNING",
						},
						globalState: { done: 0, failed: 0, running: 1, total: 1, },
					}, 200,);
					return;
				}

				if (url.pathname === "/public/api/projects/TEST/jobs/job-timeout/log/") {
					logRequested = true;
				}

				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (client,) => {
				const result = await client.jobs.wait("job-timeout", {
					includeLogs: true,
					pollIntervalMs: 1,
					timeoutMs: 1,
				},);

				expect(result,).toEqual({
					success: false,
					jobId: "job-timeout",
					state: "RUNNING",
					type: "DATASET_BUILD",
					elapsedMs: 2,
					pollCount: 1,
					timedOut: true,
					progress: { done: 0, failed: 0, running: 1, total: 1, },
				},);
			},);
		} finally {
			Date.now = originalDateNow;
		}

		expect(logRequested,).toBe(false,);
	});

	it("keeps terminal success and reports logUnavailable when the log endpoint returns not found", async () => {
		let statusRequests = 0;

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-purged/") {
				statusRequests += 1;
				sendJson(res, {
					baseStatus: { def: { id: "job-purged", type: "DATASET_BUILD", }, state: "DONE", },
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				}, 200,);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-purged/log/") {
				res.statusCode = 404;
				res.end("Job log not found",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-purged", {
				includeLogs: true,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toEqual({
				success: true,
				jobId: "job-purged",
				state: "DONE",
				type: "DATASET_BUILD",
				elapsedMs: expect.any(Number,),
				pollCount: 1,
				progress: { done: 1, failed: 0, running: 0, total: 1, },
				logUnavailable: "not_found",
				removed: false,
			},);
		},);

		expect(statusRequests,).toBe(2,); // terminal poll + removal probe
	});

	it("marks a removed terminal job with logUnavailable not_found and removed true", async () => {
		let statusRequests = 0;

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-removed/") {
				statusRequests += 1;
				if (statusRequests === 1) {
					sendJson(res, {
						baseStatus: { def: { id: "job-removed", type: "DATASET_BUILD", }, state: "DONE", },
						globalState: { done: 1, failed: 0, running: 0, total: 1, },
					}, 200,);
				} else {
					res.statusCode = 404;
					res.end("Job not found",);
				}
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-removed/log/") {
				res.statusCode = 404;
				res.end("Job not found",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-removed", {
				summary: true,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toEqual({
				success: true,
				jobId: "job-removed",
				state: "DONE",
				type: "DATASET_BUILD",
				elapsedMs: expect.any(Number,),
				pollCount: 1,
				progress: { done: 1, failed: 0, running: 0, total: 1, },
				logUnavailable: "not_found",
				removed: true,
			},);
		},);

		expect(statusRequests,).toBe(2,); // terminal poll + removal probe
	});

	it("keeps a failed terminal outcome when logs are not found", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-failed-log/") {
				sendJson(res, {
					baseStatus: { def: { id: "job-failed-log", type: "RECURSIVE_BUILD", }, state: "FAILED", },
					globalState: { done: 0, failed: 1, running: 0, total: 1, },
				},);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/jobs/job-failed-log/log/"
			) {
				res.statusCode = 404;
				res.end("Job log not found",);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-failed-log", {
				includeLogs: true,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toEqual({
				success: false,
				jobId: "job-failed-log",
				state: "FAILED",
				type: "RECURSIVE_BUILD",
				elapsedMs: expect.any(Number,),
				pollCount: 1,
				progress: { done: 0, failed: 1, running: 0, total: 1, },
				logUnavailable: "not_found",
				removed: false,
			},);
		},);
	});

	it("reports logUnavailable error without claiming removal for other log failures", async () => {
		let logRequests = 0;

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname === "/public/api/projects/TEST/jobs/job-log-403/") {
				sendJson(res, {
					baseStatus: { def: { id: "job-log-403", type: "DATASET_BUILD", }, state: "DONE", },
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}
			if (url.pathname === "/public/api/projects/TEST/jobs/job-log-403/log/") {
				logRequests += 1;
				res.statusCode = 403;
				res.end("access denied to job log",);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.wait("job-log-403", {
				summary: true,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toMatchObject({
				success: true,
				state: "DONE",
				logUnavailable: "error",
			},);
			expect(result,).not.toHaveProperty("removed",);
			expect(result,).not.toHaveProperty("logSummary",);
		},);

		expect(logRequests,).toBe(1,); // 403 is not retried; single failed attempt
	});
});

describe("JobsResource.buildAndWait", () => {
	it("builds the dataset and waits for the resulting job", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				buildRequestBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-2", }, 200,);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-2/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-2", type: "DATASET_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				}, 200,);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-2/log/") {
				expect(url.searchParams.get("activity",),).toBe("prepare",);
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end("full build log",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.buildAndWait("target_dataset", {
				buildMode: "RECURSIVE_BUILD",
				autoUpdateSchema: true,
				activity: "prepare",
				includeLogs: true,
				maxLogLines: 0,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(buildRequestBody,).toEqual({
				outputs: [{ projectKey: "TEST", id: "target_dataset", type: "DATASET", },],
				type: "RECURSIVE_BUILD",
				autoUpdateSchemaBeforeEachRecipeRun: true,
			},);
			expect(result,).toEqual({
				success: true,
				jobId: "job-2",
				state: "DONE",
				type: "DATASET_BUILD",
				elapsedMs: expect.any(Number,),
				pollCount: 1,
				progress: { done: 1, failed: 0, running: 0, total: 1, },
				log: "full build log",
			},);
		},);

		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-2/",
			"GET /public/api/projects/TEST/jobs/job-2/log/?activity=prepare",
		],);
	});
});

describe("JobsResource.build", () => {
	it("starts managed-folder builds with MANAGED_FOLDER output type", async () => {
		let buildRequestBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/",);
			buildRequestBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "job-folder", }, 200,);
		}, async (client,) => {
			const result = await client.jobs.build("folder-id", {
				targetType: "MANAGED_FOLDER",
				autoUpdateSchema: true,
			},);
			expect(result,).toEqual({ jobId: "job-folder", },);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "folder-id",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "folder-id",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
	});

	it("includes dataset partitions in build payloads", async () => {
		let buildRequestBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/",);
			buildRequestBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			sendJson(res, { id: "job-partition", }, 200,);
		}, async (client,) => {
			const result = await client.jobs.build("target_dataset", {
				partition: "2026-05-13",
			},);
			expect(result,).toEqual({ jobId: "job-partition", },);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "target_dataset",
				type: "DATASET",
				partition: "2026-05-13",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
	});

	it("builds and waits for managed-folder targets", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				buildRequestBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-folder-wait", }, 200,);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder-wait/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-folder-wait", type: "MANAGED_FOLDER_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				}, 200,);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (client,) => {
			const result = await client.jobs.buildAndWait("folder-id", {
				targetType: "MANAGED_FOLDER",
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);
			expect(result,).toMatchObject({
				success: true,
				jobId: "job-folder-wait",
				state: "DONE",
				type: "MANAGED_FOLDER_BUILD",
			},);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "folder-id",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "folder-id",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-folder-wait/",
		],);
	});
});
