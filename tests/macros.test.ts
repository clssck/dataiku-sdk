import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, type DataikuClientConfig, } from "../src/client.js";
import { macroResultMaxBytes, } from "../src/resources/macros.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient, requests: string[],) => Promise<void>,
	config?: Partial<DataikuClientConfig>,
): Promise<void> {
	const requests: string[] = [];
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
		...config,
	},);

	try {
		await run(client, requests,);
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

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function sendText(res: ServerResponse, body: string, contentType: string,): void {
	res.statusCode = 200;
	res.setHeader("Content-Type", contentType,);
	res.end(body,);
}

describe("MacrosResource", () => {
	it("lists macros with URL-encoded project key", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/runnables") {
				sendJson(res, [
					{ runnableType: "compute_orders", meta: { label: "Compute Orders", }, },
					{ runnableType: "cleanup", meta: { label: "Cleanup", }, },
				],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const list = await client.macros.list();
			expect(list,).toHaveLength(2,);
			expect(list[0]!.runnableType,).toBe("compute_orders",);
			expect(list[1]!.meta?.label,).toBe("Cleanup",);
		},);
	});

	it("fetches a macro definition with encoded runnable type", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/My%20Macro"
			) {
				sendJson(res, {
					runnableType: "My Macro",
					ownerPluginId: "orders-plugin",
					meta: { label: "my awesome macro", },
					resultType: "HTML",
					extension: "html",
					mimeType: "application/html",
					params: [
						{ name: "param1", type: "STRING", },
						{ name: "param2", type: "INT", },
					],
					adminParams: [],
				},);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const def = await client.macros.definition("My Macro",);
			expect(def.runnableType,).toBe("My Macro",);
			expect(def.ownerPluginId,).toBe("orders-plugin",);
			expect(def.resultType,).toBe("HTML",);
			expect(def.params,).toHaveLength(2,);
		},);
	});

	it("definition rejects blank runnable types with zero HTTP requests", async () => {
		let httpHits = 0;
		await withDataikuServer((req, res,) => {
			httpHits += 1;
			const url = new URL(req.url ?? "/", "http://localhost",);
			// A blank id would encode to the bare /runnables list route; the
			// guard must fire before any request so this stays 404-free.
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/runnables") {
				sendJson(res, [],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await expect(client.macros.definition("",),).rejects.toThrow(
				/runnableType must be a non-empty string/,
			);
			await expect(client.macros.definition("   ",),).rejects.toThrow(
				/runnableType must be a non-empty string/,
			);
		},);
		expect(httpHits,).toBe(0,);
	});

	it("starts a run with wait=false and returns the runId", async () => {
		const seen: string[] = [];
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			seen.push(`${req.method} ${url.pathname}${url.search}`,);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1"
				&& url.searchParams.get("wait",) === "false"
			) {
				sendJson(res, { runId: "run-77", },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}${url.search}`,);
		}, async (client,) => {
			const run = await client.macros.run("m1",);
			expect(run.runId,).toBe("run-77",);
			expect(seen,).toEqual(["POST /public/api/projects/TEST/runnables/m1?wait=false",],);
		},);
	});

	it("posts an empty JSON body on run and abort", async () => {
		const bodies: string[] = [];
		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/runnables/m1") {
				let body = "";
				for await (const chunk of req) body += chunk.toString();
				bodies.push(body,);
				sendJson(res, { runId: "r1", },);
				return;
			}
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/abort/r1"
			) {
				let body = "";
				for await (const chunk of req) body += chunk.toString();
				bodies.push(body,);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			await client.macros.run("m1",);
			await client.macros.abort("m1", "r1",);
			expect(bodies,).toEqual(["{}", "{}",],);
		},);
	});

	it("returns the poll state with running flag and failure details", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/state/run-9"
			) {
				sendJson(res, {
					exists: true,
					running: false,
					empty: false,
					storedError: { message: "boom", },
				},);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const state = await client.macros.state("m1", "run-9",);
			expect(state.running,).toBe(false,);
			expect(state.exists,).toBe(true,);
			expect(state.storedError,).toMatchObject({ message: "boom", },);
		},);
	});

	it("returns raw text when the result body is not JSON", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/result/run-9"
			) {
				sendText(res, "<html><body>Report</body></html>", "text/html",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.result("m1", "run-9",);
			expect(result,).toBe("<html><body>Report</body></html>",);
		},);
	});

	it("parses JSON result bodies into objects", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/result/run-9"
			) {
				sendText(res, '{"rows": 12, "status": "ok"}', "application/json",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.result("m1", "run-9",) as { rows: number; };
			expect(result.rows,).toBe(12,);
			expect(result.status,).toBe("ok",);
		},);
	});

	it("honors --max-bytes style caps and rejects truncation", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/result/run-9"
			) {
				sendText(res, "x".repeat(100,), "text/plain",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			// Within the cap: full body, no error.
			const ok = await client.macros.result("m1", "run-9", { maxBytes: 200, },);
			expect(ok,).toBe("x".repeat(100,),);
			// Below the size: truncated body must throw rather than return partial data.
			await expect(
				client.macros.result("m1", "run-9", { maxBytes: 10, },),
			).rejects.toThrow(/exceeded/,);
		},);
	});

	it("runAndWait polls until the run finishes and reports success", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1"
			) {
				sendJson(res, { runId: "run-1", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/state/run-1"
			) {
				sendJson(res, { exists: true, running: false, type: "HTML", },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("m1", {
				pollIntervalMs: 1,
				timeoutMs: 2_000,
			},);
			expect(result,).toMatchObject({
				runnableType: "m1",
				runId: "run-1",
				running: false,
				success: true,
			},);
			expect(result.timedOut,).toBeUndefined();
		},);
	});

	it("runAndWait polls multiple times while the macro is still running", async () => {
		let stateHits = 0;
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1"
			) {
				sendJson(res, { runId: "run-1", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m1/state/run-1"
			) {
				stateHits += 1;
				if (stateHits < 3) {
					sendJson(res, {
						running: true,
						progress: [{ name: "step", unit: "SIZE", target: 10, cur: 4, },],
					},);
					return;
				}
				sendJson(res, { running: false, type: "HTML", },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("m1", {
				pollIntervalMs: 1,
				timeoutMs: 2_000,
			},);
			expect(result.pollCount,).toBe(3,);
			expect(result.success,).toBe(true,);
		},);
	});

	it("runAndWait reports failed runs via resultError without throwing", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/bad"
			) {
				sendJson(res, { runId: "run-x", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/bad/state/run-x"
			) {
				sendJson(res, { running: false, resultError: { message: "macro exploded", }, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("bad", {
				pollIntervalMs: 1,
				timeoutMs: 2_000,
			},);
			expect(result.success,).toBe(false,);
			expect(result.running,).toBe(false,);
			expect(result.timedOut,).toBeUndefined();
		},);
	});

	it("runAndWait returns { success: false, timedOut: true } on timeout", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/slow"
			) {
				sendJson(res, { runId: "run-slow", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/slow/state/run-slow"
			) {
				sendJson(res, { running: true, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("slow", {
				pollIntervalMs: 1,
				timeoutMs: 30,
			},);
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.running,).toBe(true,);
			expect(result.runId,).toBe("run-slow",);
		},);
	});

	it("runAndWait bounds a stalled state endpoint with the wait budget", async () => {
		// HARD STALL: the state endpoint accepts the connection and never
		// responds. The run POST answers normally. The wait budget (300ms)
		// must bound the second poll via the client { timeoutMs } pass-through:
		// the call ends (structured timeout result or thrown transport timeout)
		// around the budget, NOT at the ~30s client default per-attempt timeout.
		//
		// Real timers are intentional: this test exercises the platform fetch
		// deadline against a genuinely unresponsive socket, which fake timers
		// cannot simulate.
		let stateHits = 0;
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/stall"
			) {
				sendJson(res, { runId: "run-stall", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/stall/state/run-stall"
			) {
				stateHits += 1;
				if (stateHits === 1) {
					sendJson(res, { running: true, },);
					return;
				}
				// Second poll: never respond (stalled server).
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const startedAt = Date.now();
			const result = await client.macros.runAndWait("stall", {
				pollIntervalMs: 1,
				timeoutMs: 300,
			},);
			const wallMs = Date.now() - startedAt;
			// STRICT: budget expiry MUST surface as the structured timeout
			// result (newcode contract), never as a thrown DataikuError, and
			// the wall clock must stay near the 300ms budget — not the ~30s
			// client default per-attempt timeout, and not a 2s-style overshoot.
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.running,).toBe(true,);
			expect(wallMs,).toBeLessThan(1_000,);
			expect(stateHits,).toBeGreaterThanOrEqual(2,);
		},);
	});

	it("runAndWait times out with no completed state observation (state omitted)", async () => {
		// The FIRST state poll stalls: no observation ever completes, so the
		// structured timeout result must omit `state` rather than fabricate one.
		// Real timers are intentional: this exercises the fetch deadline against
		// a genuinely unresponsive socket, which fake timers cannot simulate.
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/dead"
			) {
				sendJson(res, { runId: "run-dead", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/dead/state/run-dead"
			) {
				// Never respond.
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const startedAt = Date.now();
			const result = await client.macros.runAndWait("dead", {
				pollIntervalMs: 1,
				timeoutMs: 300,
			},);
			const wallMs = Date.now() - startedAt;
			expect(result.timedOut,).toBe(true,);
			expect(result.success,).toBe(false,);
			expect(result.running,).toBe(true,);
			// No observation completed: no fabricated state.
			expect(result.state,).toBeUndefined();
			expect(wallMs,).toBeLessThan(1_000,);
		},);
	});

	it("treats exists=false terminal states as unsuccessful", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/gone"
			) {
				sendJson(res, { runId: "run-g", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/gone/state/run-g"
			) {
				sendJson(res, { exists: false, running: false, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("gone", {
				pollIntervalMs: 1,
				timeoutMs: 2_000,
			},);
			expect(result.success,).toBe(false,);
			expect(result.timedOut,).toBeUndefined();
		},);
	});

	it("surfaces storedError when resultError is absent", async () => {
		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/runnables/m2"
			) {
				sendJson(res, { runId: "r2", },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/runnables/m2/state/r2"
			) {
				sendJson(res, { running: false, storedError: { code: "E42", }, },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (client,) => {
			const result = await client.macros.runAndWait("m2", {
				pollIntervalMs: 1,
				timeoutMs: 2_000,
			},);
			expect(result.success,).toBe(false,);
		},);
	});
});

describe("macroResultMaxBytes", () => {
	it("defaults to 8 MiB and clamps to the 2 GiB ceiling", () => {
		expect(macroResultMaxBytes(undefined,),).toBe(8 * 1024 * 1024,);
		expect(macroResultMaxBytes(1_000_000_000_000,),).toBe(2 * 1024 * 1024 * 1024,);
	});

	it("rejects non-positive and non-integer caps", () => {
		expect(() => macroResultMaxBytes(0,)).toThrow(/maxBytes/,);
		expect(() => macroResultMaxBytes(-5,)).toThrow(/maxBytes/,);
		expect(() => macroResultMaxBytes(1.5,)).toThrow(/maxBytes/,);
	});
});
