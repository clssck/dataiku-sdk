import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

async function readBody(req: IncomingMessage,): Promise<string> {
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

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
	},);
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

describe("KnowledgeBanksResource", () => {
	it("searches a knowledge bank and returns the documented document envelope", async () => {
		let capturedPath = "";
		let capturedBody = "";
		await withServer(async (req, res,) => {
			capturedPath = req.url ?? "";
			capturedBody = await readBody(req,);
			sendJson(res, {
				documents: [
					{ text: "Sample text", metadata: { Category: "General News", }, score: 0.912, },
					{ text: "Another doc", metadata: { Category: "Technology", }, score: 0.856, },
				],
			},);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.knowledgeBanks.search("my-kb", {
				query: "Hello, world!",
				params: { maxDocuments: 2, searchType: "SIMILARITY", },
			},);
			expect(capturedPath,).toBe(
				"/public/api/projects/TEST/knowledge-banks/my-kb/search",
			);
			const parsed = JSON.parse(capturedBody,) as Record<string, unknown>;
			expect(parsed["query"],).toBe("Hello, world!",);
			expect(parsed["params"],).toEqual({ maxDocuments: 2, searchType: "SIMILARITY", },);
			expect(result.documents,).toHaveLength(2,);
			expect(result.documents![0]!.score,).toBe(0.912,);
			expect(result.documents![1]!.metadata,).toEqual({ Category: "Technology", },);
		},);
	});

	it("URL-encodes knowledge bank ids containing slashes and colons", async () => {
		let capturedPath = "";
		await withServer(async (req, res,) => {
			capturedPath = req.url ?? "";
			sendJson(res, { documents: [], },);
		}, async (url,) => {
			const client = createClient(url,);
			await client.knowledgeBanks.search("team/kb:v2", { query: "q", },);
			expect(capturedPath,).toBe(
				"/public/api/projects/TEST/knowledge-banks/team%2Fkb%3Av2/search",
			);
		},);
	});

	it("omits params entirely when not supplied", async () => {
		let capturedBody = "";
		await withServer(async (req, res,) => {
			capturedBody = await readBody(req,);
			sendJson(res, { documents: [], },);
		}, async (url,) => {
			const client = createClient(url,);
			await client.knowledgeBanks.search("kb", { query: "q", },);
			const parsed = JSON.parse(capturedBody,) as Record<string, unknown>;
			expect(parsed,).toEqual({ query: "q", },);
		},);
	});

	it("clears a knowledge bank with POST and an empty JSON body", async () => {
		let capturedPath = "";
		let capturedBody = "";
		await withServer(async (req, res,) => {
			capturedPath = req.url ?? "";
			capturedBody = await readBody(req,);
			sendJson(res, {},);
		}, async (url,) => {
			const client = createClient(url,);
			await client.knowledgeBanks.clear("my-kb",);
			expect(capturedPath,).toBe(
				"/public/api/projects/TEST/knowledge-banks/my-kb/clear",
			);
			expect(capturedBody,).toBe("{}",);
		},);
	});

	it("rejects empty ids, empty query, and non-object params without contacting DSS", async () => {
		await withServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			const client = createClient(url,);
			expect(client.knowledgeBanks.search("", { query: "q", },),).rejects.toThrow(
				"knowledgeBankId must be a non-empty string.",
			);
			expect(client.knowledgeBanks.clear("  ",),).rejects.toThrow(
				"knowledgeBankId must be a non-empty string.",
			);
			expect(client.knowledgeBanks.search("kb", { query: "", },),).rejects.toThrow(
				"query must be a non-empty string.",
			);
			expect(
				client.knowledgeBanks.search("kb", {
					query: "q",
					params: { maxDocuments: "many" as never, },
				},),
			).rejects.toThrow("params.maxDocuments must be a finite number.",);
			expect(
				client.knowledgeBanks.search("kb", {
					query: "q",
					params: { useAdvancedReranking: "yes" as never, },
				},),
			).rejects.toThrow("params.useAdvancedReranking must be a boolean.",);
		},);
	});
});
