import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient,) => Promise<void>,
	config: { requestTimeoutMs?: number; maxResponseBodyBytes?: number; } = {},
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch(() => {
			// A cancelled body may make the server-side write fail; that is
			// expected in timeout/overflow tests and must not crash the test.
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
		url: `http://127.0.0.1:${String(port,)}`,
		apiKey: "test",
		projectKey: "TEST",
		...(config.requestTimeoutMs !== undefined ? { requestTimeoutMs: config.requestTimeoutMs, } : {}),
		...(config.maxResponseBodyBytes !== undefined
			? { maxResponseBodyBytes: config.maxResponseBodyBytes, }
			: {}),
	},);
	try {
		await run(client,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

describe("DataikuClient bounded response bodies", () => {
	for (const format of ["seconds", "date",] as const) {
		it(`honors Retry-After expressed as ${format}`, async () => {
			let attempts = 0;
			let earliestRetry = 0;
			let actualRetry = 0;
			await withDataikuServer((_req, res,) => {
				attempts++;
				if (attempts === 1) {
					const header = format === "seconds" ? "3" : new Date(Date.now() + 4_000,).toUTCString();
					earliestRetry = format === "seconds" ? Date.now() + 3_000 : Date.parse(header,);
					res.writeHead(429, { "Retry-After": header, },);
					res.end("rate limited",);
				} else {
					actualRetry = Date.now();
					res.end('{"recovered":true}',);
				}
			}, async client => {
				expect(await client.get("/server-backoff",),).toEqual({ recovered: true, },);
				expect(attempts,).toBe(2,);
				expect(actualRetry,).toBeGreaterThanOrEqual(earliestRetry - 20,);
			},);
		}, 10_000,);
	}

	it("does not extend a total deadline to honor Retry-After", async () => {
		let attempts = 0;
		await withDataikuServer((_req, res,) => {
			attempts++;
			res.writeHead(503, { "Retry-After": "30", },);
			res.end("busy",);
		}, async client => {
			const failure = await client.get("/deadline", { timeoutMs: 100, },).catch((error: unknown,) =>
				error
			);
			if (!(failure instanceof DataikuError)) throw new Error("Expected timeout",);
			expect(failure.retry?.timedOut,).toBe(true,);
			expect(attempts,).toBe(1,);
		},);
	});

	it("declines excessive server waits instead of overflowing the timer into an early retry", async () => {
		let attempts = 0;
		await withDataikuServer((_req, res,) => {
			attempts++;
			res.writeHead(429, { "Retry-After": "999999999999999999999", },);
			res.end("rate limited",);
		}, async client => {
			const failure = await client.get("/excessive-backoff",).catch((error: unknown,) => error);
			if (!(failure instanceof DataikuError)) throw new Error("Expected original error",);
			expect(failure.status,).toBe(429,);
			expect(failure.retry?.delaysMs,).toEqual([],);
			expect(attempts,).toBe(1,);
		},);
	});

	it("bounds mutation errors at UTF-8 boundaries without retrying or losing request identity", async () => {
		let attempts = 0;
		await withDataikuServer((_req, res,) => {
			attempts++;
			res.writeHead(503, { "x-request-id": "bounded-error-request", "Retry-After": "1", },);
			res.end("😀".repeat(2_000,),);
		}, async (client,) => {
			const failure = await client.post("/oversized-mutation-error", {},).catch((error: unknown,) =>
				error
			);
			if (!(failure instanceof DataikuError)) throw new Error("Expected a DataikuError",);
			expect(failure.status,).toBe(503,);
			expect(failure.requestId,).toBe("bounded-error-request",);
			expect(failure.body,).toBe("😀".repeat(128,),);
			expect(Buffer.byteLength(failure.body,),).toBeLessThanOrEqual(513,);
			expect(failure.bodyTruncated,).toBe(true,);
			expect(failure.retry?.enabled,).toBe(false,);
			expect(attempts,).toBe(1,);
		}, { maxResponseBodyBytes: 513, },);
	});

	it("still retries an idempotent request after a bounded transient error body", async () => {
		let attempts = 0;
		await withDataikuServer((_req, res,) => {
			attempts++;
			res.statusCode = attempts === 1 ? 503 : 200;
			res.end(attempts === 1 ? "busy ".repeat(2_000,) : JSON.stringify({ recovered: true, },),);
		}, async (client,) => {
			expect(await client.get("/retry-large-error",),).toEqual({ recovered: true, },);
			expect(attempts,).toBe(2,);
		}, { maxResponseBodyBytes: 512, },);
	});

	it("retains transport retries when an error response stalls after headers", async () => {
		let attempts = 0;
		await withDataikuServer((_req, res,) => {
			attempts++;
			if (attempts === 1) {
				res.writeHead(500,);
				res.write("partial error",);
			} else {
				res.end(JSON.stringify({ recovered: true, },),);
			}
		}, async (client,) => {
			expect(await client.get("/retry-stalled-error",),).toEqual({ recovered: true, },);
			expect(attempts,).toBe(2,);
		}, { requestTimeoutMs: 80, },);
	});

	it("getText rejects a body exceeding maxResponseBodyBytes", async () => {
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end("x".repeat(2_000,),);
		}, async (client,) => {
			const failure = await client.getText("/big",).catch((error: unknown,) => error);
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as DataikuError).status,).toBe(200,);
			expect((failure as Error).message,).toContain("exceeded the 512-byte limit",);
		}, { maxResponseBodyBytes: 512, },);
	});

	it("getText returns bodies within the configured cap unchanged", async () => {
		const body = "hello bounded world";
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end(body,);
		}, async (client,) => {
			expect(await client.getText("/ok",),).toBe(body,);
		}, { maxResponseBodyBytes: 512, },);
	});

	it("postText rejects an oversized body instead of buffering it unbounded", async () => {
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end("y".repeat(2_000,),);
		}, async (client,) => {
			const failure = await client.postText("/big-post", {},).catch((error: unknown,) => error);
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as Error).message,).toContain("exceeded the 512-byte limit",);
		}, { maxResponseBodyBytes: 512, },);
	});

	it("JSON consumers reject oversized bodies as DataikuError, not a JSON parse error", async () => {
		const oversizedJson = JSON.stringify({ items: Array.from({ length: 500, }, (_v, i,) => i,), },);
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.setHeader("content-type", "application/json",);
			res.end(oversizedJson,);
		}, async (client,) => {
			const failure = await client.get("/big-json",).catch((error: unknown,) => error);
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as Error).message,).toContain("exceeded the 512-byte limit",);
		}, { maxResponseBodyBytes: 512, },);
	});

	it("times out a stalled body after headers instead of waiting forever", async () => {
		// Genuine platform-clock integration: the client's deadline races a real
		// TCP connection, so deterministic fake timers cannot model the socket
		// stall. Server delays are kept far above the client budget to bound
		// runtime and avoid flake.
		await withDataikuServer((_req, res,) => {
			res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", },);
			res.flushHeaders();
			setTimeout(() => res.end("late body",), 500,);
		}, async (client,) => {
			const started = Date.now();
			const failure = await client.getText("/stalled",).catch((error: unknown,) => error);
			const elapsed = Date.now() - started;
			expect(failure,).toBeInstanceOf(DataikuError,);
			expect((failure as DataikuError).status,).toBe(0,);
			expect((failure as Error).message,).toContain("timed out after 80ms",);
			expect(elapsed,).toBeLessThan(400,);
		}, { requestTimeoutMs: 80, },);
	});

	it("getTextLimited still truncates within the deadline", async () => {
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end("abcdefghij".repeat(100,),);
		}, async (client,) => {
			const { text, truncated, } = await client.getTextLimited("/limited", 64,);
			expect(truncated,).toBe(true,);
			expect(text,).toBe("abcdefghij".repeat(6,).concat("abcd",),); // 64 bytes = 6×10 + 4
		},);
	});

	it("getTextTailLimited keeps the last bytes of an oversized body", async () => {
		const body = "0123456789".repeat(200,); // 2 000 bytes
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end(body,);
		}, async (client,) => {
			const { text, truncated, } = await client.getTextTailLimited("/log", 64,);
			expect(truncated,).toBe(true,);
			expect(text,).toBe(body.slice(-64,),);
		},);
	});

	it("getTextTailLimited keeps the tail when the body arrives in multiple chunks", async () => {
		const body = "chunked-log-line-".repeat(200,); // 3 200 bytes
		await withDataikuServer((_req, res,) => {
			res.statusCode = 200;
			res.write(body.slice(0, 1_000,),);
			res.write(body.slice(1_000, 2_000,),);
			res.end(body.slice(2_000,),);
		}, async (client,) => {
			const { text, truncated, } = await client.getTextTailLimited("/log", 100,);
			expect(truncated,).toBe(true,);
			expect(text,).toBe(body.slice(-100,),);
		},);
	});

	it("getTextTailLimited stays bounded across many one-byte chunks", async () => {
		const body = Array.from(
			{ length: 20_000, },
			(_, index,) => String.fromCharCode(97 + index % 26,),
		).join("",);
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.flushHeaders();
			for (const character of body) {
				res.write(character,);
				await new Promise<void>((resolvePromise,) => setImmediate(resolvePromise,));
			}
			res.end();
		}, async (client,) => {
			const { text, truncated, } = await client.getTextTailLimited("/tiny-chunks", 64,);
			expect(truncated,).toBe(true,);
			expect(text,).toBe(body.slice(-64,),);
		},);
	});

	it("getTextTailLimited never emits replacement characters when cutting multibyte text", async () => {
		// é = 2 bytes, € = 3 bytes, 😀 = 4 bytes (9-byte multibyte prefix).
		const body = "é€😀".repeat(200,);
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end(body,);
		}, async (client,) => {
			for (let cap = 1; cap <= 32; cap++) {
				const { text, truncated, } = await client.getTextTailLimited("/log", cap,);
				expect(truncated, `cap ${cap} truncated`,).toBe(true,);
				expect(text.includes("\uFFFD",), `cap ${cap} replacement char`,).toBe(false,);
				expect(Buffer.byteLength(text, "utf-8",), `cap ${cap} byte budget`,).toBeLessThanOrEqual(
					cap,
				);
				// A byte-based cut may split a character; the kept text is still a
				// suffix of the original (characters may only be dropped at the cut).
				expect(body.endsWith(text,), `cap ${cap} suffix`,).toBe(true,);
			}
		},);
	});

	it("getTextTailLimited returns the whole body below the cap", async () => {
		const body = "small log";
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end(body,);
		}, async (client,) => {
			const { text, truncated, } = await client.getTextTailLimited("/log", 1_024,);
			expect(text,).toBe(body,);
			expect(truncated,).toBe(false,);
		},);
	});

	it("stream() preserves raw Response semantics", async () => {
		const body = "raw streaming bytes";
		await withDataikuServer(async (_req, res,) => {
			res.statusCode = 200;
			res.end(body,);
		}, async (client,) => {
			const res = await client.stream("/raw",);
			expect(res,).toBeInstanceOf(Response,);
			expect(await res.text(),).toBe(body,);
		},);
	});

	for (const method of ["stream", "postStream",] as const) {
		it(method + " rejects a stalled body after headers", async () => {
			await withDataikuServer((_req, res,) => {
				res.writeHead(200, { "Content-Type": "application/octet-stream", },);
				res.write("first chunk",);
			}, async (client,) => {
				const res = await client[method]("/stalled",);
				try {
					await res.arrayBuffer();
					throw new Error("expected stalled read to fail",);
				} catch (error) {
					expect(error,).toBeInstanceOf(DataikuError,);
					expect((error as DataikuError).status,).toBe(0,);
				}
			}, { requestTimeoutMs: 100, },);
		},);
	}

	it("streams a healthy transfer beyond the timeout without buffering or truncation", async () => {
		await withDataikuServer(async (_req, res,) => {
			res.writeHead(206, { "Content-Type": "application/octet-stream", "X-Stream": "intact", },);
			res.write("one",);
			await Bun.sleep(120,);
			res.write("two",);
			await Bun.sleep(120,);
			res.end("three",);
		}, async (client,) => {
			const res = await client.stream("/long",);
			expect(res.status,).toBe(206,);
			expect(res.headers.get("x-stream",),).toBe("intact",);
			expect(res.url,).toBe(client.getBaseUrl() + "/long",);
			expect(await res.text(),).toBe("onetwothree",);
		}, { requestTimeoutMs: 200, maxResponseBodyBytes: 1, },);
	});
});
