import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DiscussionsResource, } from "../src/resources/discussions.js";

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

describe("DiscussionsResource", () => {
	it("lists object discussions without unwrapping", async () => {
		const discussions = [{ id: "discussion-1", topic: "First", }, { id: "discussion-2", },];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, discussions,);
		}, async (url,) => {
			const resource = new DiscussionsResource(createClient(url,),);
			await expect(resource.list("DATASET", "folder/dataset",),).resolves.toEqual(discussions,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/discussions/DATASET/folder%2Fdataset/",
		],);
	});

	it("gets one object discussion with encoded ids", async () => {
		const discussion = {
			id: "discussion/1",
			topic: "Detailed",
			replies: [{ text: "Reply", author: "admin", time: 123, editedOn: 456, },],
		};
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			sendJson(res, discussion,);
		}, async (url,) => {
			const resource = new DiscussionsResource(createClient(url,),);
			await expect(resource.get("DASHBOARD", "dash board", "discussion/1",),).resolves.toEqual(
				discussion,
			);
		},);

		expect(observedMethod,).toBe("GET",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/discussions/DASHBOARD/dash%20board/discussion%2F1",
		);
	});

	it("creates a discussion with topic and first reply", async () => {
		const discussion = {
			id: "discussion-3",
			topic: "Release",
			replies: [{ text: "Ship it", },],
		};
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, discussion,);
		}, async (url,) => {
			const resource = new DiscussionsResource(createClient(url,),);
			await expect(resource.create("SAVEDMODEL", "model id", "Release", "Ship it",),).resolves
				.toEqual(discussion,);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/discussions/SAVEDMODEL/model%20id/",
		);
		expect(observedBody,).toEqual({ topic: "Release", reply: "Ship it", },);
	});

	it("adds a reply to a discussion", async () => {
		const discussion = {
			id: "discussion-4",
			topic: "Follow-up",
			replies: [{ text: "Initial", }, { text: "Second reply", },],
		};
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, discussion,);
		}, async (url,) => {
			const resource = new DiscussionsResource(createClient(url,),);
			await expect(resource.reply("DATASET", "dataset", "discussion-4", "Second reply",),)
				.resolves.toEqual(discussion,);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/discussions/DATASET/dataset/discussion-4/replies/",
		);
		expect(observedBody,).toEqual({ reply: "Second reply", },);
	});
});
