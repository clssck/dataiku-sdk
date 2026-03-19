import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuError, } from "../src/errors.js";
import { SqlResource, } from "../src/resources/sql.js";

class TestHttpClient {
	constructor(private readonly baseUrl: string,) {}

	async post<T = unknown,>(path: string, body?: unknown,): Promise<T> {
		const res = await fetch(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: { "content-type": "application/json", },
			body: body === undefined ? undefined : JSON.stringify(body,),
		},);
		return this.parseJsonResponse<T>(res,);
	}

	async getText(path: string,): Promise<string> {
		const res = await fetch(`${this.baseUrl}${path}`,);
		if (!res.ok) {
			throw new DataikuError(res.status, res.statusText, await res.text(),);
		}
		return res.text();
	}

	private async parseJsonResponse<T,>(res: Response,): Promise<T> {
		const text = await res.text();
		if (!res.ok) {
			throw new DataikuError(res.status, res.statusText, text,);
		}
		return JSON.parse(text,) as T;
	}
}

async function readJsonBody(req: IncomingMessage,): Promise<unknown> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) {
		chunks.push(Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,),);
	}
	const text = Buffer.concat(chunks,).toString("utf8",);
	return text ? JSON.parse(text,) : undefined;
}

async function withSqlServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (sql: SqlResource,) => Promise<void>,
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
	const sql = new SqlResource(new TestHttpClient(`http://127.0.0.1:${String(port,)}`,) as never,);
	try {
		await run(sql,);
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

describe("SqlResource", () => {
	it("startQuery uses the trailing-slash endpoint and defaults type to sql", async () => {
		await withSqlServer(async (req, res,) => {
			expect(req.method,).toBe("POST",);
			expect(req.url,).toBe("/public/api/sql/queries/",);
			expect(await readJsonBody(req,),).toEqual({
				query: "SELECT 1",
				type: "sql",
			},);
			res.setHeader("content-type", "application/json",);
			res.end(JSON.stringify({ queryId: "q-1", hasResults: true, schema: [], },),);
		}, async (sql,) => {
			const result = await sql.startQuery({ query: "SELECT 1", },);
			expect(result,).toEqual({ queryId: "q-1", hasResults: true, schema: [], },);
		},);
	});

	it("startQuery preserves a caller-provided type", async () => {
		await withSqlServer(async (req, res,) => {
			expect(req.url,).toBe("/public/api/sql/queries/",);
			expect(await readJsonBody(req,),).toEqual({
				query: "SELECT * FROM files",
				type: "hdfs",
			},);
			res.setHeader("content-type", "application/json",);
			res.end(JSON.stringify({ queryId: "q-2", hasResults: true, schema: [], },),);
		}, async (sql,) => {
			await sql.startQuery({ query: "SELECT * FROM files", type: "hdfs", },);
		},);
	});

	it("query rewrites the unsupported dataset-connection error with the dataset name", async () => {
		await withSqlServer(async (req, res,) => {
			expect(req.url,).toBe("/public/api/sql/queries/",);
			res.statusCode = 400;
			res.statusMessage = "Bad Request";
			res.end("Connection is neither of SQL nor HDFS type",);
		}, async (sql,) => {
			await expect(sql.query({
				query: "SELECT 1",
				datasetFullName: "PROJECT.dataset_orders",
			},),).rejects.toThrow(
				'Dataset "PROJECT.dataset_orders" uses a connection that DSS does not support for direct SQL queries. Use --connection with a SQL-compatible connection instead.',
			);
		},);
	});

	it("query rethrows unrelated errors unchanged", async () => {
		await withSqlServer(async (_req, res,) => {
			res.statusCode = 400;
			res.statusMessage = "Bad Request";
			res.end("Some other DSS validation failure",);
		}, async (sql,) => {
			await expect(sql.query({ query: "SELECT 1", },),).rejects.toThrow(
				"Some other DSS validation failure",
			);
		},);
	});
});
