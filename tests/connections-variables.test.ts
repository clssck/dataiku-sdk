import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

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
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;

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

describe("ConnectionsResource.list", () => {
	it("filters connection names by type with the public non-admin endpoint", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/connections/get-names/?type=Filesystem",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(["filesystem_managed",],),);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
			},);

			await expect(client.connections.list({ type: "Filesystem", },),).resolves.toEqual([
				"filesystem_managed",
			],);
		},);

		expect(requests,).toEqual(["GET /public/api/connections/get-names/?type=Filesystem",],);
	});
});

describe("ConnectionsResource.infer", () => {
	it("falls back to rich inference when fast mode returns an empty list", async () => {
		const requests: string[] = [];

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);

			if (req.method === "GET" && req.url === "/public/api/connections/get-names/") {
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}

			if (req.method === "GET" && req.url === "/public/api/projects/TEST/datasets/") {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([
					{
						type: "Snowflake",
						managed: true,
						params: { connection: "warehouse", schema: "analytics", },
					},
					{
						type: "Filesystem",
						managed: false,
						params: { connection: "archive", },
					},
					{
						type: "Snowflake",
						managed: false,
						params: { connection: "warehouse", schema: "raw", },
					},
					{
						type: "Ignored",
						managed: false,
						params: {},
					},
				],),);
				return;
			}

			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
			},);

			await expect(client.connections.infer({ mode: "fast", },),).resolves.toEqual([
				{ name: "archive", types: ["Filesystem",], managed: false, dbSchemas: [], },
				{ name: "warehouse", types: ["Snowflake",], managed: true, dbSchemas: ["analytics", "raw",], },
			],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/connections/get-names/",
			"GET /public/api/projects/TEST/datasets/",
		],);
	});

	it("uses the requested project for rich inference", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/projects/OTHER/datasets/",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify([
				{ type: "Filesystem", managed: true, params: { connection: "filesystem_managed", }, },
			],),);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
			},);

			await expect(client.connections.infer({ mode: "rich", projectKey: "OTHER", },),).resolves
				.toEqual([
					{ name: "filesystem_managed", types: ["Filesystem",], managed: true, dbSchemas: [], },
				],);
		},);

		expect(requests,).toEqual(["GET /public/api/projects/OTHER/datasets/",],);
	});
});

describe("ConnectionsResource schema inspection", () => {
	it("lists SQL schemas for a connection", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe(
				"/public/api/projects/TEST/datasets/tables-import/actions/list-schemas?connectionName=ATHENA_CONN",
			);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(["analytics", "raw",],),);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
			},);

			await expect(client.connections.schemas({ connection: "ATHENA_CONN", },),).resolves.toEqual([
				"analytics",
				"raw",
			],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/tables-import/actions/list-schemas?connectionName=ATHENA_CONN",
		],);
	});

	it("lists SQL tables with catalog and schema filters", async () => {
		const requests: string[] = [];
		const response = {
			hasResult: true,
			alive: false,
			aborted: false,
			unknown: false,
			jobId: "future-1",
			result: [{
				connectionName: "ATHENA_CONN",
				catalog: "prod",
				schema: "analytics",
				table: "orders",
			},],
		};

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe(
				"/public/api/projects/TEST/datasets/tables-import/actions/list-tables?connectionName=ATHENA_CONN&catalogName=prod&schemaName=analytics",
			);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(response,),);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
			},);

			await expect(client.connections.tables({
				connection: "ATHENA_CONN",
				catalog: "prod",
				schema: "analytics",
			},),).resolves.toEqual(response,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/tables-import/actions/list-tables?connectionName=ATHENA_CONN&catalogName=prod&schemaName=analytics",
		],);
	});
});

describe("VariablesResource.set", () => {
	it("merges with existing variables by default", async () => {
		const requests: string[] = [];
		let putBody: unknown;

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);

			if (req.method === "GET" && req.url === "/public/api/projects/TEST/variables/") {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({
					standard: { existing: 1, },
					local: { secret: true, },
				},),);
				return;
			}

			if (req.method === "PUT" && req.url === "/public/api/projects/TEST/variables/") {
				putBody = await readJsonBody(req,);
				res.statusCode = 204;
				res.end();
				return;
			}

			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", },);

			const result = await client.variables.set({
				projectKey: "TEST",
				standard: { added: 2, },
			},);

			expect(result,).toEqual({
				standard: { existing: 1, added: 2, },
				local: { secret: true, },
			},);
		},);

		expect(putBody,).toEqual({
			standard: { existing: 1, added: 2, },
			local: { secret: true, },
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/variables/",
			"PUT /public/api/projects/TEST/variables/",
		],);
	});

	it("replaces variables without fetching existing values when replace is true", async () => {
		const requests: string[] = [];
		let putBody: unknown;

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);

			if (req.method === "GET" && req.url === "/public/api/projects/TEST/variables/") {
				res.statusCode = 500;
				res.end("replace mode should not fetch existing variables",);
				return;
			}

			if (req.method === "PUT" && req.url === "/public/api/projects/TEST/variables/") {
				putBody = await readJsonBody(req,);
				res.statusCode = 204;
				res.end();
				return;
			}

			res.statusCode = 404;
			res.end("not found",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", },);

			const result = await client.variables.set({
				projectKey: "TEST",
				standard: { fresh: 1, },
				replace: true,
			},);

			expect(result,).toEqual({
				standard: { fresh: 1, },
				local: {},
			},);
		},);

		expect(putBody,).toEqual({
			standard: { fresh: 1, },
			local: {},
		},);
		expect(requests,).toEqual([
			"PUT /public/api/projects/TEST/variables/",
		],);
	});
});
