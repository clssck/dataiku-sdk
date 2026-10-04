import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

async function withDataikuServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (client: DataikuClient,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);
	const listening = Promise.withResolvers<void>();
	server.listen(0, "127.0.0.1", () => listening.resolve(),);
	await listening.promise;
	const { port, } = server.address() as AddressInfo;
	try {
		await run(
			new DataikuClient({ url: `http://127.0.0.1:${port}`, apiKey: "test", projectKey: "TEST", },),
		);
	} finally {
		const closed = Promise.withResolvers<void>();
		server.close(() => closed.resolve());
		await closed.promise;
	}
}

async function readJson(req: IncomingMessage,): Promise<unknown> {
	let body = "";
	for await (const chunk of req) body += chunk.toString();
	return body.length > 0 ? JSON.parse(body,) : undefined;
}

function sendJson(res: ServerResponse, body: unknown,): void {
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

describe("FlowResource", () => {
	it("propagates a schema with dataikuapi's request body and waits on the returned future", async () => {
		const requests: Array<{ method: string; path: string; body: unknown; }> = [];
		await withDataikuServer(async (req, res,) => {
			const path = new URL(req.url ?? "/", "http://localhost",).pathname;
			requests.push({ method: req.method ?? "", path, body: await readJson(req,), },);
			if (path === "/public/api/projects/TEST/flow/tools/propagate-schema/") {
				sendJson(res, { jobId: "job-1", hasResult: false, alive: true, },);
				return;
			}
			if (path === "/public/api/futures/job-1") {
				sendJson(res, { hasResult: true, alive: false, result: { success: true, }, },);
				return;
			}
			res.statusCode = 404;
			res.end();
		}, async (client,) => {
			const waited = await client.flow.propagateSchemaAndWait("orders", {
				autoRebuild: false,
				excludedRecipes: ["compute_report",],
				pollIntervalMs: 1,
			},);
			expect(waited,).toMatchObject({ success: true, result: { success: true, }, },);
		},);
		expect(requests[0],).toEqual({
			method: "POST",
			path: "/public/api/projects/TEST/flow/tools/propagate-schema/",
			body: {
				options: {
					recipeUpdateOptions: { byType: {}, byName: {}, },
					defaultPartitionValuesByDimension: {},
					partitionsByComputable: {},
					excludedRecipes: ["compute_report",],
					markAsOkRecipes: [],
					autoRebuild: false,
				},
				sources: [{ projectKey: "TEST", id: "orders", },],
			},
		},);
	});

	it("returns a task DSS finished inline without polling", async () => {
		const paths: string[] = [];
		const inline = { hasResult: true, alive: false, result: { messages: [], }, };
		await withDataikuServer((req, res,) => {
			paths.push(new URL(req.url ?? "/", "http://localhost",).pathname,);
			sendJson(res, inline,);
		}, async (client,) => {
			await expect(client.flow.propagateSchemaAndWait("orders",),).resolves.toEqual(inline,);
		},);
		expect(paths,).toEqual(["/public/api/projects/TEST/flow/tools/propagate-schema/",],);
	});

	it("generates documentation from each template source", async () => {
		const requests: string[] = [];
		await withDataikuServer((req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			sendJson(res, { jobId: "doc-1", },);
		}, async (client,) => {
			await client.flow.generateDocumentation();
			await client.flow.generateDocumentation({ kind: "folder", folderId: "F1", path: "/t.docx", },);
		},);
		expect(requests,).toEqual([
			"POST /public/api/projects/TEST/flow/documentation/generate",
			"POST /public/api/projects/TEST/flow/documentation/generate-with-template-in-folder?folderId=F1&path=%2Ft.docx",
		],);
	});
});
