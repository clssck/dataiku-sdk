import { afterAll, beforeAll, describe, expect, it, } from "bun:test";
import { createServer, type Server, } from "node:http";
import { DataikuClient, } from "../src/client.js";

// Prefix is deliberately multibyte so byte caps fall *inside* characters:
// é = 2 bytes, € = 3 bytes, 😀 = 4 bytes (9-byte multibyte prefix).
const BODY = "é€😀ABCDEFGHIJ".repeat(40,);
const TOTAL_BYTES = Buffer.byteLength(BODY, "utf-8",);

let server: Server;
let baseUrl = "";

beforeAll(async () => {
	server = createServer((_req, res,) => {
		res.writeHead(200, { "content-type": "text/plain; charset=utf-8", },);
		res.end(Buffer.from(BODY, "utf-8",),);
	},);
	await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", () => resolve(),));
	const addr = server.address();
	baseUrl = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
},);

afterAll(() => {
	server.close();
},);

function client(): DataikuClient {
	return new DataikuClient({ url: baseUrl, apiKey: "test-key", },);
}

describe("DataikuClient.getTextLimited()", () => {
	it("never exceeds the byte cap when it splits a multibyte character", async () => {
		// Sweep across the multibyte prefix so several caps land mid-character.
		for (let limit = 0; limit <= 16; limit++) {
			const { text, truncated, } = await client().getTextLimited("/log", limit,);
			expect(Buffer.byteLength(text, "utf-8",), `limit ${limit} byte cap`,).toBeLessThanOrEqual(
				limit,
			);
			expect(text.includes("\uFFFD",), `limit ${limit} replacement char`,).toBe(false,);
			expect(truncated, `limit ${limit} truncated`,).toBe(true,);
		}
	});

	it("returns whole multibyte characters up to the boundary below the cap", async () => {
		// Cap of 2 fits exactly one é; a cap of 1 cannot fit any char.
		expect((await client().getTextLimited("/log", 1,)).text,).toBe("",);
		expect((await client().getTextLimited("/log", 2,)).text,).toBe("é",);
		// 5 bytes = é(2) + €(3); the 😀(4) does not fit.
		expect((await client().getTextLimited("/log", 5,)).text,).toBe("é€",);
	});

	it("returns the full body untruncated when the cap exceeds its size", async () => {
		const { text, truncated, } = await client().getTextLimited("/log", TOTAL_BYTES + 100,);
		expect(text,).toBe(BODY,);
		expect(truncated,).toBe(false,);
	});

	it("treats a cap equal to the body size as untruncated", async () => {
		const { text, truncated, } = await client().getTextLimited("/log", TOTAL_BYTES,);
		expect(text,).toBe(BODY,);
		expect(truncated,).toBe(false,);
	});

	it("decodes split UTF-8 while preserving an interior byte order mark", async () => {
		const originalFetch = globalThis.fetch;
		const bytes = new TextEncoder().encode("\uFEFFé€😀abc\uFEFF東京",);
		try {
			globalThis.fetch = (async () =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(controller,) {
							for (const byte of bytes) controller.enqueue(Uint8Array.of(byte,),);
							controller.close();
						},
					},),
				)) as typeof fetch;
			expect(await client().getTextLimited("/log", bytes.length,),).toEqual({
				text: "é€😀abc\uFEFF東京",
				truncated: false,
			},);
		} finally {
			globalThis.fetch = originalFetch;
		}
	});

	it("trims a truncated malformed UTF-8 tail but replaces it in a complete body", async () => {
		const originalFetch = globalThis.fetch;
		const prefix = new TextEncoder().encode("prefix",);
		const bytes = Uint8Array.of(...prefix, 0xF5, 0x80, 0x5A,);
		try {
			globalThis.fetch = (async () =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(controller,) {
							for (const byte of bytes) controller.enqueue(Uint8Array.of(byte,),);
							controller.close();
						},
					},),
				)) as typeof fetch;
			expect(await client().getTextLimited("/log", bytes.length - 1,),).toEqual({
				text: "prefix",
				truncated: true,
			},);
			expect(await client().getTextLimited("/log", bytes.length,),).toEqual({
				text: "prefix\uFFFD\uFFFDZ",
				truncated: false,
			},);
		} finally {
			globalThis.fetch = originalFetch;
		}
	});
	it("does not flush a split character past a truncated byte cap", async () => {
		const originalFetch = globalThis.fetch;
		const chunks = [Uint8Array.of(0x61, 0xF0,), Uint8Array.of(0x9F,), Uint8Array.of(0x98, 0x80,),];
		try {
			globalThis.fetch = (async () =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(controller,) {
							for (const chunk of chunks) controller.enqueue(chunk,);
							controller.close();
						},
					},),
				)) as typeof fetch;
			expect(await client().getTextLimited("/log", 3,),).toEqual({ text: "a", truncated: true, },);
		} finally {
			globalThis.fetch = originalFetch;
		}
	});
});
