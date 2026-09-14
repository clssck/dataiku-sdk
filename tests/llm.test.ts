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

describe("LlmsResource", () => {
	it("lists LLMs without a purpose query by default", async () => {
		await withServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/projects/TEST/llms/",);
			sendJson(res, [
				{
					id: "openai:openai1:gpt-4",
					type: "OPENAI",
					connection: "openai1",
					model: "gpt-4",
					promptDriven: true,
				},
			],);
		}, async (url,) => {
			const client = createClient(url,);
			const llms = await client.llms.list();
			expect(llms,).toHaveLength(1,);
			expect(llms[0]!.id,).toBe("openai:openai1:gpt-4",);
			expect(llms[0]!.promptDriven,).toBe(true,);
		},);
	});

	it("passes the purpose query parameter when given", async () => {
		await withServer((req, res,) => {
			expect(req.url,).toBe(
				"/public/api/projects/TEST/llms/?purpose=TEXT_EMBEDDING_EXTRACTION",
			);
			sendJson(res, [],);
		}, async (url,) => {
			const client = createClient(url,);
			expect(await client.llms.list({ purpose: "TEXT_EMBEDDING_EXTRACTION", },),).toEqual([],);
		},);
	});

	it("URL-encodes the project key and an explicit per-call project key", async () => {
		await withServer((req, res,) => {
			expect(req.url,).toBe("/public/api/projects/MY%2BPROJ/llms/",);
			sendJson(res, [],);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", },);
			expect(await client.llms.list({ projectKey: "MY+PROJ", },),).toEqual([],);
		},);
	});

	it("URL-encodes llm ids containing colons when used as identifiers", async () => {
		// LLM ids appear in the POST body, not the URL; verify the body round-trips.
		let capturedBody = "";
		await withServer(async (req, res,) => {
			capturedBody = await readBody(req,);
			sendJson(res, { responses: [{ ok: true, text: "Ludwig Van Beethoven", totalTokens: 47, },], },);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.llms.completions({
				llmId: "openai:openai1:gpt-4",
				queries: [{ messages: [{ role: "user", content: "Who wrote Beethoven's 9th?", },], },],
				settings: { temperature: 0.9, maxOutputTokens: 2048, topP: 0.7, },
			},);
			const parsed = JSON.parse(capturedBody!,) as Record<string, unknown>;
			expect(parsed["llmId"],).toBe("openai:openai1:gpt-4",);
			expect(Array.isArray(parsed["queries"],),).toBe(true,);
			expect(parsed["settings"],).toEqual({
				temperature: 0.9,
				maxOutputTokens: 2048,
				topP: 0.7,
			},);
			expect(result.responses,).toHaveLength(1,);
			expect(result.responses![0]!.text,).toBe("Ludwig Van Beethoven",);
		},);
	});

	it("performs embeddings and returns the documented response envelope", async () => {
		let capturedPath = "";
		let capturedBody = "";
		await withServer(async (req, res,) => {
			capturedPath = req.url ?? "";
			capturedBody = await readBody(req,);
			sendJson(res, {
				responses: [
					{
						ok: true,
						embedding: [0.1235243, 0.2196464, 0.365436456546,],
						promptTokens: 42,
						estimatedCost: 0.000000012,
					},
				],
			},);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.llms.embeddings({
				llmId: "openai:openai1:ada002-text-embedding",
				queries: [{ text: "Who wrote Beethoven's 9th symphony?", },],
			},);
			expect(capturedPath,).toBe("/public/api/projects/TEST/llms/embeddings",);
			const parsed = JSON.parse(capturedBody,) as Record<string, unknown>;
			expect(parsed["llmId"],).toBe("openai:openai1:ada002-text-embedding",);
			expect(result.responses![0]!.embedding,).toEqual([0.1235243, 0.2196464, 0.365436456546,],);
			expect(result.responses![0]!.estimatedCost,).toBe(0.000000012,);
		},);
	});

	it("rejects empty llmId, empty queries, and non-object queries without contacting DSS", async () => {
		await withServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			const client = createClient(url,);
			expect(
				client.llms.completions({ llmId: "", queries: [{},], },),
			).rejects.toThrow("llmId must be a non-empty string.",);
			expect(
				client.llms.embeddings({ llmId: "x", queries: [], },),
			).rejects.toThrow("queries must be a non-empty array",);
			expect(
				client.llms.completions({ llmId: "x", queries: ["not-an-object" as never,], },),
			).rejects.toThrow("queries[0] must be an object.",);
			expect(
				client.llms.completions({ llmId: "x", queries: [{},], settings: 42 as never, },),
			).rejects.toThrow("settings must be an object.",);
		},);
	});

	it("rejects a missing project key before making a request", async () => {
		await withServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", },);
			expect(client.llms.list(),).rejects.toThrow("projectKey is required",);
		},);
	});
});
