import { describe, expect, it, } from "bun:test";
import { execSync, } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, } from "node:fs";
import {
	createServer as createHttpServer,
	type IncomingMessage,
	type ServerResponse,
} from "node:http";
import { createServer as createHttpsServer, } from "node:https";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { validateCredentials, } from "../src/auth.js";
import { DataikuError, } from "../src/errors.js";

/** Generate a fresh self-signed localhost key+cert pair at test time so no private key lives in source. */
function generateTlsMaterial(): { key: string; cert: string; } {
	const dir = mkdtempSync(join(tmpdir(), "dataiku-tls-gen-",),);
	const keyPath = join(dir, "key.pem",);
	const certPath = join(dir, "cert.pem",);
	try {
		execSync(
			[
				"openssl",
				"req",
				"-x509",
				"-newkey",
				"rsa:2048",
				"-keyout",
				keyPath,
				"-out",
				certPath,
				"-days",
				"1",
				"-nodes",
				"-subj",
				"/CN=localhost",
				"-addext",
				"subjectAltName=DNS:localhost,IP:127.0.0.1",
			].join(" ",),
			{ stdio: "pipe", },
		);
		return {
			key: readFileSync(keyPath, "utf-8",),
			cert: readFileSync(certPath, "utf-8",),
		};
	} finally {
		rmSync(dir, { recursive: true, force: true, },);
	}
}

const { key: TEST_TLS_KEY, cert: TEST_TLS_CERT, } = generateTlsMaterial();

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createHttpServer(handler,);
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

async function withHttpsServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createHttpsServer({ key: TEST_TLS_KEY, cert: TEST_TLS_CERT, }, handler,);
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
	const url = `https://127.0.0.1:${String(port,)}`;
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

	it("supports disabling TLS verification for self-signed servers", async () => {
		await withHttpsServer(
			(_req, res,) => {
				res.writeHead(200, { "Content-Type": "application/json", },);
				res.end("[]",);
			},
			async (url,) => {
				const result = await validateCredentials(url, "test-key", {
					tlsRejectUnauthorized: false,
				},);
				expect(result,).toEqual({ valid: true, },);
			},
		);
	});

	it("supports trusting a custom CA bundle for self-signed servers", async () => {
		const tmpDir = mkdtempSync(join(tmpdir(), "dataiku-auth-ca-",),);
		const caPath = join(tmpDir, "ca.pem",);
		writeFileSync(caPath, `${TEST_TLS_CERT}\n`, "utf-8",);
		try {
			await withHttpsServer(
				(_req, res,) => {
					res.writeHead(200, { "Content-Type": "application/json", },);
					res.end("[]",);
				},
				async (url,) => {
					const result = await validateCredentials(url, "test-key", { caCertPath: caPath, },);
					expect(result,).toEqual({ valid: true, },);
				},
			);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
