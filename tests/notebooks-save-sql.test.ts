import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { notebookCommands, } from "../src/cli/commands/notebook.js";
import { DataikuClient, } from "../src/client.js";
import { stableHash, } from "../src/utils/stable-hash.js";

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

async function withServer(
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
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const address = server.address();
	if (!address || typeof address === "string") {
		throw new Error("Test server did not bind to a TCP address.",);
	}
	const url = `http://127.0.0.1:${String(address.port,)}`;
	try {
		await run(url,);
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

describe("notebook save-sql command", () => {
	const saveSql = notebookCommands["save-sql"];
	const nextNotebook = {
		connection: "postgres",
		cells: [{ id: "cell-1", type: "QUERY", code: "select 1", },],
	};
	const notebookPath = "/public/api/projects/TEST/sql-notebooks/sql%20notebook";

	it("creates a missing SQL notebook, exposes created=true and the persisted hash", async () => {
		const requests: string[] = [];
		let createdBody: unknown;
		const state: { stored?: Record<string, unknown>; } = {};
		const generatedPath = "/public/api/projects/TEST/sql-notebooks/gen-1";

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				if (state.stored === undefined) {
					sendJson(res, { message: "SQL notebook not found", }, 404,);
					return;
				}
				sendJson(res, state.stored,);
				return;
			}
			if (req.method === "GET" && req.url === generatedPath) {
				sendJson(res, state.stored ?? {},);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/projects/TEST/sql-notebooks/") {
				createdBody = JSON.parse(await readRequestBody(req,),);
				// The server allocates the id and returns the full receipt.
				state.stored = { id: "gen-1", ...(createdBody as Record<string, unknown>), };
				sendJson(res, state.stored, 201,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const result = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(nextNotebook,),
			},) as {
				saved?: string;
				requested?: string;
				resource?: string;
				created?: boolean;
				hash?: string;
			};
			expect(result.saved,).toBe("gen-1",);
			expect(result.requested,).toBe("sql notebook",);
			expect(result.resource,).toBe("sql-notebook",);
			expect(result.created,).toBe(true,);
			expect(result.hash,).toBe(stableHash(state.stored,),);
		},);

		// Fresh read decides create vs update, then POST (no id in the body),
		// then a confirming read by the server-allocated id.
		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			"POST /public/api/projects/TEST/sql-notebooks/",
			`GET ${generatedPath}`,
		],);
		expect(createdBody,).toEqual({
			...nextNotebook,
			name: "sql notebook",
			projectKey: "TEST",
		},);
	});

	it("saves an existing SQL notebook, exposes created=false and a stable hash", async () => {
		const requests: string[] = [];
		let savedBody: unknown;
		const currentNotebook = {
			connection: "postgres",
			cells: [{ id: "cell-1", type: "QUERY", code: "select 0", },],
		};
		const state: { stored?: Record<string, unknown>; } = { stored: currentNotebook, };

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, state.stored,);
				return;
			}
			if (req.method === "PUT" && req.url === notebookPath) {
				savedBody = JSON.parse(await readRequestBody(req,),);
				state.stored = savedBody as Record<string, unknown>;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const first = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(nextNotebook,),
			},) as { created?: boolean; hash?: string; };
			const second = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(nextNotebook,),
			},) as { created?: boolean; hash?: string; };
			expect(first.created,).toBe(false,);
			expect(second.created,).toBe(false,);
			// Deterministic: identical persisted content yields the same hash.
			expect(first.hash,).toBe(second.hash,);
			expect(first.hash,).toBe(stableHash(state.stored,),);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
			`GET ${notebookPath}`,
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
			`GET ${notebookPath}`,
		],);
		// Identity is positional: the URL id and resolved project key are
		// injected into the PUT body.
		expect(savedBody,).toEqual({
			...nextNotebook,
			id: "sql notebook",
			projectKey: "TEST",
		},);
	});

	it("cannot retarget a notebook from stale content identity", async () => {
		const requests: string[] = [];
		let savedBody: Record<string, unknown> | undefined;
		const currentNotebook = {
			connection: "postgres",
			cells: [{ id: "cell-1", type: "QUERY", code: "select 0", },],
		};
		const state: { stored?: Record<string, unknown>; } = { stored: currentNotebook, };

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, state.stored,);
				return;
			}
			if (req.method === "PUT" && req.url === notebookPath) {
				savedBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				state.stored = savedBody;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			// Stale content tries to carry a foreign identity; the positional
			// arguments must win and the write must stay on the requested notebook.
			const stale = { ...nextNotebook, id: "other-target", projectKey: "OTHER", };
			const result = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(stale,),
			},) as { saved?: string; };
			expect(result.saved,).toBe("sql notebook",);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
			`GET ${notebookPath}`,
		],);
		expect(savedBody,).toEqual({
			...nextNotebook,
			id: "sql notebook",
			projectKey: "TEST",
		},);
	});

	it("rejects a stale --expect-hash without writing", async () => {
		const requests: string[] = [];
		const currentNotebook = {
			connection: "postgres",
			cells: [{ id: "cell-1", type: "QUERY", code: "select 0", },],
		};

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, currentNotebook,);
				return;
			}
			if (req.method === "PUT" || req.method === "POST") {
				res.statusCode = 500;
				res.end(`guard must prevent mutation: ${request}`,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(
				saveSql.handler(client, ["sql notebook",], {
					data: JSON.stringify(nextNotebook,),
					"expect-hash": "0".repeat(64,),
				},),
			).rejects.toThrow(/changed since it was read/,);
		},);

		expect(requests,).toEqual([`GET ${notebookPath}`,],);
	});

	it("saves when --expect-hash matches the stored content hash", async () => {
		const requests: string[] = [];
		const currentNotebook = {
			connection: "postgres",
			cells: [{ id: "cell-1", type: "QUERY", code: "select 0", },],
		};
		const state: { stored?: Record<string, unknown>; } = { stored: currentNotebook, };

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, state.stored,);
				return;
			}
			if (req.method === "PUT" && req.url === notebookPath) {
				state.stored = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const result = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(nextNotebook,),
				"expect-hash": stableHash(currentNotebook,),
			},) as { created?: boolean; hash?: string; };
			expect(result.created,).toBe(false,);
			expect(result.hash,).toBe(stableHash(state.stored,),);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
			`GET ${notebookPath}`,
		],);
	});

	it("rejects a malformed --expect-hash before any request", async () => {
		const requests: string[] = [];

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(
				saveSql.handler(client, ["sql notebook",], {
					data: JSON.stringify(nextNotebook,),
					"expect-hash": "not-a-hash",
				},),
			).rejects.toThrow(/64-character SHA-256 hex digest/,);
		},);

		expect(requests,).toEqual([],);
	});

	it("dry-runs a missing SQL notebook save without creating it", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, { message: "SQL notebook not found", }, 404,);
				return;
			}
			if (req.method === "POST" || req.method === "PUT") {
				res.statusCode = 500;
				res.end(`unexpected mutation ${request}`,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const result = await saveSql.handler(client, ["sql notebook",], {
				data: JSON.stringify(nextNotebook,),
				"dry-run": true,
			},);
			expect(result,).toEqual({
				dryRun: true,
				action: "save-sql",
				resource: "sql-notebook",
				id: "sql notebook",
				current: undefined,
				next: nextNotebook,
			},);
		},);

		expect(requests,).toEqual([`GET ${notebookPath}`,],);
	});
});
