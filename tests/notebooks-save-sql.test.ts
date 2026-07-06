import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { notebookCommands, } from "../src/cli/commands/notebook.js";
import { DataikuClient, } from "../src/client.js";

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

	it("creates a missing SQL notebook after probing get-sql", async () => {
		const requests: string[] = [];
		let createdBody: unknown;

		await withServer(async (req, res,) => {
			const request = `${req.method ?? ""} ${req.url ?? ""}`;
			requests.push(request,);
			if (req.method === "GET" && req.url === notebookPath) {
				sendJson(res, { message: "SQL notebook not found", }, 404,);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/projects/TEST/sql-notebooks/") {
				createdBody = JSON.parse(await readRequestBody(req,),);
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
			},);
			expect(result,).toEqual({ saved: "sql notebook", resource: "sql-notebook", },);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			"POST /public/api/projects/TEST/sql-notebooks/",
		],);
		expect(createdBody,).toEqual({
			...nextNotebook,
			id: "sql notebook",
			projectKey: "TEST",
		},);
	});

	it("saves an existing SQL notebook after probing get-sql", async () => {
		const requests: string[] = [];
		let savedBody: unknown;
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
			if (req.method === "PUT" && req.url === notebookPath) {
				savedBody = JSON.parse(await readRequestBody(req,),);
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
			},);
			expect(result,).toEqual({ saved: "sql notebook", resource: "sql-notebook", },);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
		],);
		expect(savedBody,).toEqual(nextNotebook,);
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
