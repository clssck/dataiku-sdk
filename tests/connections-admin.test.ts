import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";

async function readJsonBody(req: IncomingMessage,): Promise<unknown> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) {
		chunks.push(Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,),);
	}
	const body = Buffer.concat(chunks,).toString("utf8",);
	return body.length > 0 ? JSON.parse(body,) : undefined;
}

async function withTestServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
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
	try {
		await run(`http://127.0.0.1:${String(port,)}`,);
	} finally {
		await new Promise<void>((resolvePromise,) => {
			server.close(() => resolvePromise());
		},);
	}
}

function client(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
}

function json(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

describe("ConnectionsResource admin CRUD", () => {
	it("lists all connections as the documented name→definition dictionary", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/admin/connections",);
			json(res, {
				"my-connection": {
					name: "my-connection",
					type: "Vertica",
					allowWrite: true,
					params: { host: "127.0.0.1", password: "thedbpassword", },
				},
			},);
		}, async (url,) => {
			const all = await client(url,).connections.adminList();
			expect(all["my-connection"]?.params,).toEqual({
				host: "127.0.0.1",
				password: "thedbpassword",
			},);
		},);
	});

	it("performs the full connection lifecycle with encoded names and empty bodies", async () => {
		await withTestServer(async (req, res,) => {
			if (req.method === "GET") {
				expect(req.url,).toBe("/public/api/admin/connections/weird%2Fname",);
				json(res, { name: "weird/name", type: "PostgreSQL", params: { password: "p", }, },);
				return;
			}
			if (req.method === "POST") {
				expect(req.url,).toBe("/public/api/admin/connections",);
				expect(await readJsonBody(req,),).toEqual({
					name: "new-connection",
					type: "PostgreSQL",
					params: { db: "dbname", password: "s3cret", },
				},);
				res.statusCode = 200;
				res.end();
				return;
			}
			if (req.method === "PUT") {
				expect(req.url,).toBe("/public/api/admin/connections/new-connection",);
				expect(await readJsonBody(req,),).toEqual({
					name: "new-connection",
					type: "PostgreSQL",
					allowWrite: true,
				},);
				res.statusCode = 204;
				res.end();
				return;
			}
			expect(req.method,).toBe("DELETE",);
			expect(req.url,).toBe("/public/api/admin/connections/old",);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const c = client(url,);
			const got = await c.connections.adminGet("weird/name",);
			expect(got.name,).toBe("weird/name",);
			await expect(c.connections.adminCreate({
				name: "new-connection",
				type: "PostgreSQL",
				params: { db: "dbname", password: "s3cret", },
			},),).resolves.toEqual({},);
			await expect(c.connections.adminUpdate("new-connection", {
				name: "new-connection",
				type: "PostgreSQL",
				allowWrite: true,
			},),).resolves.toEqual({},);
			await expect(c.connections.adminDelete("old",),).resolves.toEqual({ deleted: "old", },);
		},);
	});

	it("tests availability via the official /connections/{name}/test route", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/connections/postgres/test",);
			json(res, { connectionOK: true, },);
		}, async (url,) => {
			await expect(client(url,).connections.adminTest("postgres",),).resolves.toEqual({
				connectionOK: true,
			},);
		},);
	});

	it("rejects empty connection names before any request", async () => {
		await withTestServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			await expect(client(url,).connections.adminGet("  ",),).rejects.toThrow(
				ClientValidationError,
			);
			await expect(client(url,).connections.adminDelete("",),).rejects.toThrow(
				ClientValidationError,
			);
		},);
	});
});

describe("ConnectionsResource tables import", () => {
	it("prepares and executes table imports on the project endpoints", async () => {
		await withTestServer(async (req, res,) => {
			if (req.url === "/public/api/projects/TEST/datasets/tables-import/actions/prepare-from-keys") {
				expect(await readJsonBody(req,),).toEqual({
					keys: [{ connectionName: "postgres", name: "my_table", },],
				},);
				json(res, { hasResult: false, alive: true, jobId: "26S2LeJw", },);
				return;
			}
			expect(req.url,).toBe(
				"/public/api/projects/MY%20PROJ/datasets/tables-import/actions/execute-from-candidates",
			);
			expect(await readJsonBody(req,),).toEqual({
				sqlImportCandidates: [{ connectionName: "pgsql", table: "t", },],
			},);
			json(res, { hasResult: true, alive: false, },);
		}, async (url,) => {
			const c = client(url,);
			await expect(c.connections.prepareTablesImport({
				keys: [{ connectionName: "postgres", name: "my_table", },],
			},),).resolves.toHaveProperty("jobId", "26S2LeJw",);
			await expect(c.connections.executeTablesImport({
				sqlImportCandidates: [{ connectionName: "pgsql", table: "t", },],
				projectKey: "MY PROJ",
			},),).resolves.toHaveProperty("alive", false,);
		},);
	});

	it("rejects empty keys and missing candidates before any request", async () => {
		await withTestServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			const c = client(url,);
			await expect(c.connections.prepareTablesImport({ keys: [], },),).rejects.toThrow(
				"non-empty array of key objects",
			);
			await expect(c.connections.executeTablesImport({},),).rejects.toThrow(
				"sqlImportCandidates or hiveImportCandidates",
			);
			await expect(
				c.connections.executeTablesImport({ sqlImportCandidates: [], },),
			).rejects.toThrow("non-empty array",);
		},);
	});
});
