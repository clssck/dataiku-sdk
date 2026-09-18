import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import { FuturesResource, } from "../src/resources/futures.js";
import { JobsResource, } from "../src/resources/jobs.js";
import { MlTasksResource, } from "../src/resources/ml-tasks.js";
import { ProjectGitResource, } from "../src/resources/project-git.js";
import { computeNextPollDelayMs, MAX_POLL_INTERVAL_MS, } from "../src/utils/polling.js";

type RecordedRequest = {
	method: string;
	path: string;
};

interface Loopback {
	url: string;
	requests: RecordedRequest[];
	close(): Promise<void>;
}

/** Minimal DSS-shaped loopback server driven by per-path handlers. */
async function startLoopback(
	handler: (req: IncomingMessage, res: ServerResponse, path: string,) => void | Promise<void>,
): Promise<Loopback> {
	const requests: RecordedRequest[] = [];
	const server = createServer((req, res,) => {
		void (async () => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push({
				method: req.method ?? "GET",
				path: `${url.pathname}${url.search}`,
			},);
			await handler(req, res, `${url.pathname}${url.search}`,);
		})().catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);
	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) rejectPromise(error,);
			else resolvePromise();
		},);
	},);
	const { port, } = server.address() as AddressInfo;
	return {
		url: `http://127.0.0.1:${String(port,)}`,
		requests,
		close() {
			return new Promise<void>((resolvePromise, rejectPromise,) => {
				server.close((error,) => {
					if (error) rejectPromise(error,);
					else resolvePromise();
				},);
			},);
		},
	};
}

function client(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "DEFAULT", },);
}

const TRAIN = "/public/api/projects/DEFAULT/models/lab/A1/T1/train";
const STATUS = "/public/api/projects/DEFAULT/models/lab/A1/T1/status";

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

describe("ML train wait deadline", () => {
	it("validates invalid timeoutMs BEFORE the train POST (zero requests)", async () => {
		const loopback = await startLoopback((_req, res,) => {
			sendJson(res, { sessionId: "s-1", },);
		},);
		try {
			for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -1,]) {
				await expect(
					new MlTasksResource(client(loopback.url,),).train({
						analysisId: "A1",
						mlTaskId: "T1",
						wait: true,
						timeoutMs: bad,
					},),
				).rejects.toThrow(/timeoutMs/,);
			}
			// timeoutMs/pollIntervalMs without wait are rejected pre-flight too.
			await expect(
				new MlTasksResource(client(loopback.url,),).train({
					analysisId: "A1",
					mlTaskId: "T1",
					timeoutMs: 1_000,
				},),
			).rejects.toThrow(/timeoutMs requires wait/,);
			await expect(
				new MlTasksResource(client(loopback.url,),).train({
					analysisId: "A1",
					mlTaskId: "T1",
					pollIntervalMs: 500,
				},),
			).rejects.toThrow(/pollIntervalMs requires wait/,);
			await expect(
				new MlTasksResource(client(loopback.url,),).train({
					analysisId: "A1",
					mlTaskId: "T1",
					wait: true,
					pollIntervalMs: Number.POSITIVE_INFINITY,
				},),
			).rejects.toThrow(/pollIntervalMs/,);
			// No POST /train and no GET /status ever reached the wire.
			expect(loopback.requests,).toEqual([],);
		} finally {
			await loopback.close();
		}
	});

	it("returns the structured timeout result when training never finishes", async () => {
		const loopback = await startLoopback((req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-timeout", },);
				return;
			}
			if (path === STATUS) {
				sendJson(res, { training: true, fullModelIds: [], },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const started = Date.now();
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: 300,
				pollIntervalMs: 60,
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.state,).toBe("RUNNING",);
			expect(result.sessionId,).toBe("s-timeout",);
			expect(result.trainedModelIds,).toEqual([],);
			expect(result.pollCount,).toBeGreaterThan(1,);
			// Bounded: no full-interval overshoot beyond generous slack.
			expect(Date.now() - started,).toBeLessThan(1_500,);
			expect(loopback.requests[0].method,).toBe("POST",);
		} finally {
			await loopback.close();
		}
	});

	it("keeps the documented success shape when training finishes in budget", async () => {
		const loopback = await startLoopback((req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-ok", },);
				return;
			}
			if (path === STATUS) {
				sendJson(res, { training: false, fullModelIds: [{ id: "m-1", },], },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: 5_000,
				pollIntervalMs: 20,
			},);
			expect(result.timedOut,).toBeUndefined();
			expect(result.success,).toBeUndefined();
			expect(result.sessionId,).toBe("s-ok",);
			expect(result.trainedModelIds,).toEqual(["m-1",],);
		} finally {
			await loopback.close();
		}
	});

	it("timeoutMs 0 still performs exactly one status observation then times out", async () => {
		let statusCalls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-zero", },);
				return;
			}
			if (path === STATUS) {
				statusCalls += 1;
				sendJson(res, { training: true, fullModelIds: [], },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: 0,
				pollIntervalMs: 10,
			},);
			expect(statusCalls,).toBe(1,);
			expect(result.timedOut,).toBe(true,);
			expect(result.sessionId,).toBe("s-zero",);
		} finally {
			await loopback.close();
		}
	});

	it("a stalled status response cannot defeat the wait budget", async () => {
		let statusCalls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-stall", },);
				return;
			}
			if (path === STATUS) {
				statusCalls += 1;
				if (statusCalls === 1) {
					// Stall headers AND body far beyond the budget.
					setTimeout(() => sendJson(res, { training: true, },), 30_000,);
					return;
				}
				sendJson(res, { training: true, },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const started = Date.now();
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: 500,
				pollIntervalMs: 50,
			},);
			expect(result.timedOut,).toBe(true,);
			// The 30s stall was cut near the 500ms budget (slack for retries).
			expect(Date.now() - started,).toBeLessThan(5_000,);
		} finally {
			await loopback.close();
		}
	});

	it("transient 503s with backoff stay inside the total wait budget", async () => {
		let statusCalls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-503", },);
				return;
			}
			if (path === STATUS) {
				statusCalls += 1;
				// Always transient: retries + backoff must still fit the budget.
				res.statusCode = 503;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ error: "transient", },),);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			expect(statusCalls,).toBe(0,);
			const started = Date.now();
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: 400,
				pollIntervalMs: 40,
			},);
			const elapsed = Date.now() - started;
			// The total-budget deadline converts exhausted retries into a wait
			// timeout instead of hanging past the caller's budget.
			expect(elapsed,).toBeLessThan(5_000,);
			expect(result.timedOut,).toBe(true,);
		} finally {
			await loopback.close();
		}
	});
});

describe("futures.wait", () => {
	it("preserves the timeoutMs 0 first-observation contract", async () => {
		let polls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path.startsWith("/public/api/futures/f-1",)) {
				polls += 1;
				sendJson(res, { alive: true, hasResult: false, },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const result = await new FuturesResource(client(loopback.url,),).wait("f-1", {
				timeoutMs: 0,
				pollIntervalMs: 10,
			},);
			expect(polls,).toBe(1,);
			expect(result.pollCount,).toBe(1,);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
		} finally {
			await loopback.close();
		}
	});

	it("keeps an explicit interval fixed instead of adapting", async () => {
		let polls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path.startsWith("/public/api/futures/f-2",)) {
				polls += 1;
				sendJson(
					res,
					polls >= 4
						? { hasResult: true, result: { done: true, }, }
						: { alive: true, hasResult: false, },
				);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const started = Date.now();
			const result = await new FuturesResource(client(loopback.url,),).wait("f-2", {
				timeoutMs: 10_000,
				pollIntervalMs: 30,
			},);
			expect(result.success,).toBe(true,);
			expect(result.state,).toBe("DONE",);
			expect(result.result,).toEqual({ done: true, },);
			// Four fixed 30ms polls; adaptive doubling would overshoot 120ms.
			expect(Date.now() - started,).toBeLessThan(600,);
		} finally {
			await loopback.close();
		}
	});

	it("adapts the delay by default and still respects the deadline", async () => {
		let polls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path.startsWith("/public/api/futures/f-3",)) {
				polls += 1;
				sendJson(res, { alive: true, hasResult: false, },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const result = await new FuturesResource(client(loopback.url,),).wait("f-3", {
				timeoutMs: 500,
				pollIntervalMs: 200,
			},);
			expect(result.timedOut,).toBe(true,);
			// ~3 polls at 200ms fit inside 500ms.
			expect(polls,).toBeLessThanOrEqual(4,);
		} finally {
			await loopback.close();
		}
	});
});

describe("project-git.waitForFuture", () => {
	it("returns the future result once available", async () => {
		let polls = 0;
		const loopback = await startLoopback((req, res, path,) => {
			if (path.startsWith("/dip/publicapi/futures/g-1",)) {
				polls += 1;
				if (polls >= 2) sendJson(res, { hasResult: true, result: { ok: 1, }, },);
				else sendJson(res, { alive: true, hasResult: false, },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const result = await new ProjectGitResource(client(loopback.url,),).waitForFuture("g-1", {
				timeoutMs: 5_000,
				pollIntervalMs: 20,
			},);
			expect(polls,).toBe(2,);
			expect(result,).toEqual({ ok: 1, },);
		} finally {
			await loopback.close();
		}
	});

	it("throws on exhausted budget (existing contract) without overshooting", async () => {
		const loopback = await startLoopback((req, res, path,) => {
			if (path.startsWith("/dip/publicapi/futures/g-2",)) {
				sendJson(res, { alive: true, hasResult: false, },);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const started = Date.now();
			await expect(
				new ProjectGitResource(client(loopback.url,),).waitForFuture("g-2", {
					timeoutMs: 150,
					pollIntervalMs: 60_000,
				},),
			).rejects.toThrow(/Timed out after/,);
			// A 60s interval must never be slept in full.
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});
});

describe("shared computeNextPollDelayMs", () => {
	it("keeps the documented adaptive table from the moved helper", () => {
		expect(computeNextPollDelayMs({ pollCount: 1, baseIntervalMs: 2_000, adaptiveEnabled: false, },),)
			.toBe(2_000,);
		expect(computeNextPollDelayMs({ pollCount: 4, baseIntervalMs: 2_000, adaptiveEnabled: true, },),)
			.toBe(4_000,);
		expect(
			computeNextPollDelayMs({ pollCount: 100, baseIntervalMs: 2_000, adaptiveEnabled: true, },),
		).toBe(10_000,);
		expect(MAX_POLL_INTERVAL_MS,).toBe(10_000,);
	});
});

describe("wait request limits", () => {
	for (const phase of ["headers", "body",] as const) {
		it(`preserves the per-request ${phase} cap inside a longer wait`, async () => {
			const server = Bun.serve({
				hostname: "127.0.0.1",
				port: 0,
				fetch: () =>
					phase === "headers"
						? new Promise<Response>(() => {},)
						: new Response(
							new ReadableStream({
								start(controller,) {
									controller.enqueue(new TextEncoder().encode("{",),);
								},
							},),
						),
			},);
			try {
				const c = new DataikuClient({
					url: server.url.href,
					apiKey: "test",
					requestTimeoutMs: 50,
					retryMaxAttempts: 1,
				},);
				const started = Date.now();
				await expect(c.futures.wait("F", { timeoutMs: 1000, },),).rejects.toBeInstanceOf(DataikuError,);
				expect(Date.now() - started,).toBeLessThan(750,);
			} finally {
				server.stop(true,);
			}
		});
	}

	it("returns the wait timeout when a stalled body exhausts the overall deadline", async () => {
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: () =>
				new Response(
					new ReadableStream({
						start(controller,) {
							controller.enqueue(new TextEncoder().encode("{",),);
						},
					},),
				),
		},);
		try {
			const c = new DataikuClient({
				url: server.url.href,
				apiKey: "test",
				requestTimeoutMs: 1000,
				retryMaxAttempts: 1,
			},);
			await expect(c.futures.wait("F", { timeoutMs: 50, },),).resolves.toMatchObject({
				timedOut: true,
			},);
		} finally {
			server.stop(true,);
		}
	});
});

/**
 * Answer a request after a delay unless the client already hung up.
 *
 * Real timers are intentional: the delay must race the client's own fetch
 * aborts and deadline timers on the platform clock, which is exactly what the
 * loopback tests exercise; fake timers cannot drive an HTTP socket stall.
 */
function delayedJson(res: ServerResponse, delayMs: number, body: unknown,): void {
	const timer = setTimeout(() => {
		if (!res.destroyed) sendJson(res, body,);
	}, delayMs,);
	res.on("close", () => clearTimeout(timer,),);
}

/**
 * Answer the first `fastPolls` requests immediately, then stall every later
 * request well past the caller's budget — the stall a spent budget must never
 * reach. Real timers are intentional (see {@link delayedJson}).
 */
function stallAfter(
	fastPolls: number,
	stallMs: number,
	body: unknown,
): (res: ServerResponse,) => void {
	let polls = 0;
	return (res,) => {
		polls += 1;
		if (polls <= fastPolls) {
			sendJson(res, body,);
			return;
		}
		delayedJson(res, stallMs, body,);
	};
}

describe("post-budget polling", () => {
	// A 5 s stall dwarfs the 250 ms budget, so "returns before the stall"
	// tolerates wide scheduling jitter while still failing the old behavior.
	const BUDGET_MS = 250;
	const INTERVAL_MS = 100;
	const STALL_MS = 5_000;

	it("futures.wait reports the timeout from the last observation on a spent budget", async () => {
		let polls = 0;
		const loopback = await startLoopback((_req, res, path,) => {
			if (!path.startsWith("/public/api/futures/f-clock",)) {
				sendJson(res, {},);
				return;
			}
			polls += 1;
			if (polls === 1) {
				sendJson(res, { alive: true, hasResult: false, },);
				return;
			}
			// A post-budget observation would succeed here — the guard must
			// ensure it is never requested.
			delayedJson(res, STALL_MS, { alive: true, hasResult: true, result: { done: true, }, },);
		},);
		const realNow = Date.now;
		let offsetMs = 0;
		Date.now = () => realNow() + offsetMs;
		// Real timer on purpose: the wait loop sleeps on a platform-clock timer
		// after its first observation, so a fake timer here would not fire in
		// lockstep with the socket and the loop's own sleep.
		const advance = setTimeout(() => {
			offsetMs = 60_000;
		}, 20,);
		try {
			// Controlled clock: move past the budget while the loop sleeps after
			// its first observation, so the pre-request guard is what ends the
			// wait. No second request may leave the client.
			const result = await new FuturesResource(client(loopback.url,),).wait("f-clock", {
				timeoutMs: 100,
				pollIntervalMs: 100,
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.pollCount,).toBe(1,);
			expect(loopback.requests.length,).toBe(1,);
		} finally {
			clearTimeout(advance,);
			Date.now = realNow;
			await loopback.close();
		}
	});

	it("futures.wait stops at the deadline instead of stalling past it", async () => {
		const respond = stallAfter(3, STALL_MS, { alive: true, hasResult: false, },);
		const loopback = await startLoopback((_req, res,) => respond(res,));
		try {
			const started = Date.now();
			const result = await new FuturesResource(client(loopback.url,),).wait("f-post", {
				timeoutMs: BUDGET_MS,
				pollIntervalMs: INTERVAL_MS,
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.state,).toBe("RUNNING",);
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});

	it("jobs.wait stops at the deadline instead of stalling past it", async () => {
		const respond = stallAfter(
			3,
			STALL_MS,
			{ baseStatus: { state: "RUNNING", }, globalState: {}, },
		);
		const loopback = await startLoopback((_req, res,) => respond(res,));
		try {
			const started = Date.now();
			const result = await new JobsResource(client(loopback.url,),).wait("j-post", {
				timeoutMs: BUDGET_MS,
				pollIntervalMs: INTERVAL_MS,
				projectKey: "DEFAULT",
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.state,).toBe("RUNNING",);
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});

	it("ml-tasks.train stops polling at the deadline instead of stalling past it", async () => {
		const respond = stallAfter(3, STALL_MS, { training: true, fullModelIds: [], },);
		const loopback = await startLoopback((_req, res, path,) => {
			if (path === TRAIN) {
				sendJson(res, { sessionId: "s-post", },);
				return;
			}
			if (path === STATUS) {
				respond(res,);
				return;
			}
			sendJson(res, {},);
		},);
		try {
			const started = Date.now();
			const result = await new MlTasksResource(client(loopback.url,),).train({
				analysisId: "A1",
				mlTaskId: "T1",
				wait: true,
				timeoutMs: BUDGET_MS,
				pollIntervalMs: INTERVAL_MS,
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.sessionId,).toBe("s-post",);
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});

	it("project-git.waitForFuture stops at the deadline instead of stalling past it", async () => {
		const respond = stallAfter(3, STALL_MS, { alive: true, hasResult: false, },);
		const loopback = await startLoopback((_req, res,) => respond(res,));
		try {
			const started = Date.now();
			await expect(
				new ProjectGitResource(client(loopback.url,),).waitForFuture("g-post", {
					timeoutMs: BUDGET_MS,
					pollIntervalMs: INTERVAL_MS,
				},),
			).rejects.toThrow(/Timed out after/,);
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});
});

describe("DataikuClient.get noRetry", () => {
	it("issues exactly one transport attempt", async () => {
		const loopback = await startLoopback((_req, res,) => {
			res.statusCode = 503;
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify({ error: "transient", },),);
		},);
		try {
			const failure = await client(loopback.url,).get("/retry-once", { noRetry: true, },).catch(
				(error: unknown,) => error,
			);
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as DataikuError).retry?.maxAttempts,).toBe(1,);
			expect(loopback.requests.length,).toBe(1,);
		} finally {
			await loopback.close();
		}
	});

	it("still enforces the total timeout when combined with noRetry", async () => {
		const loopback = await startLoopback((_req, res,) => {
			delayedJson(res, 5_000, { ok: true, },);
		},);
		try {
			const started = Date.now();
			const failure = await client(loopback.url,).get("/slow", {
				timeoutMs: 100,
				noRetry: true,
			},).catch((error: unknown,) => error);
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as DataikuError).retry?.timedOut,).toBe(true,);
			expect(loopback.requests.length,).toBe(1,);
			expect(Date.now() - started,).toBeLessThan(2_000,);
		} finally {
			await loopback.close();
		}
	});
});
