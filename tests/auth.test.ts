import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { validateCredentials, } from "../src/auth.js";
import { DataikuError, } from "../src/errors.js";

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer(handler,);
	await new Promise<void>((resolve, reject,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				reject(error,);
				return;
			}
			resolve();
		},);
	},);
	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolve, reject,) => {
			server.close((err,) => {
				if (err) reject(err,);
				else resolve();
			},);
		},);
	}
}

describe("validateCredentials", () => {
	it("returns valid: true when API responds with 200", async () => {
		await withServer(
			(_req, res,) => {
				res.writeHead(200, { "Content-Type": "application/json", },);
				res.end("[]",);
			},
			async (url,) => {
				const result = await validateCredentials(url, "test-key",);
				expect(result,).toEqual({ valid: true, },);
			},
		);
	});

	it("returns valid: false on 401 unauthorized", async () => {
		await withServer(
			(_req, res,) => {
				res.writeHead(401, { "Content-Type": "application/json", },);
				res.end(JSON.stringify({ message: "Unauthorized", },),);
			},
			async (url,) => {
				const result = await validateCredentials(url, "bad-key",);
				expect(result.valid,).toBe(false,);
				expect(typeof result.error,).toBe("string",);
				expect(result.error,).toBeTruthy();
				expect(result.dataikuError,).toBeInstanceOf(DataikuError,);
				expect(result.dataikuError?.status,).toBe(401,);
				expect(result.dataikuError?.category,).toBe("forbidden",);
			},
		);
	});

	it("returns valid: false on 403 forbidden", async () => {
		await withServer(
			(_req, res,) => {
				res.writeHead(403, { "Content-Type": "application/json", },);
				res.end(JSON.stringify({ message: "Forbidden", },),);
			},
			async (url,) => {
				const result = await validateCredentials(url, "test-key",);
				expect(result.valid,).toBe(false,);
			},
		);
	});

	it("returns valid: false on network error", async () => {
		const result = await validateCredentials("http://127.0.0.1:1", "test-key",);
		expect(result.valid,).toBe(false,);
		expect(typeof result.error,).toBe("string",);
		expect(result.dataikuError,).toBeInstanceOf(DataikuError,);
		expect(result.dataikuError?.status,).toBe(0,);
		expect(result.dataikuError?.category,).toBe("transient",);
	});

	it("returns valid: false on server error", async () => {
		await withServer(
			(_req, res,) => {
				res.writeHead(500, { "Content-Type": "application/json", },);
				res.end(JSON.stringify({ message: "Internal Server Error", },),);
			},
			async (url,) => {
				const result = await validateCredentials(url, "test-key",);
				expect(result.valid,).toBe(false,);
			},
		);
	});
});
