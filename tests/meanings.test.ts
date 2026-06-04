import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { MeaningsResource, } from "../src/resources/meanings.js";

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

describe("MeaningsResource", () => {
	it("lists user-defined meanings at the instance level", async () => {
		const meanings = [
			{ id: "vip", label: "VIP", type: "VALUES_LIST", },
			{ id: "sku", label: "SKU", type: "PATTERN", },
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, meanings,);
		}, async (url,) => {
			const resource = new MeaningsResource(createClient(url,),);
			await expect(resource.list(),).resolves.toEqual(meanings,);
		},);

		expect(requests,).toEqual(["GET /public/api/meanings/",],);
	});

	it("gets a meaning definition with encoded ids", async () => {
		const definition = {
			id: "meaning/slash",
			label: "Meaning Slash",
			type: "PATTERN",
			pattern: "^x$",
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, definition,);
		}, async (url,) => {
			const resource = new MeaningsResource(createClient(url,),);
			await expect(resource.get("meaning/slash",),).resolves.toEqual(definition,);
		},);

		expect(requests,).toEqual(["GET /public/api/meanings/meaning%2Fslash",],);
	});

	it("creates a meaning through POST with a source-compatible body", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;
		const body = {
			description: "Matches VIP values",
			entries: [{ value: "vip", color: "green", },],
			normalizationMode: "LOWERCASE",
			detectable: true,
		};

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("Created",);
		}, async (url,) => {
			const resource = new MeaningsResource(createClient(url,),);
			await expect(resource.create("vip", "VIP", "VALUES_LIST", body,),).resolves.toBe("Created",);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/meanings/",);
		expect(observedBody,).toEqual({
			...body,
			id: "vip",
			label: "VIP",
			type: "VALUES_LIST",
			mappings: null,
			pattern: null,
		},);
	});

	it("updates a meaning definition through PUT with encoded ids", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;
		const body = {
			id: "meaning/slash",
			label: "Meaning Slash",
			type: "PATTERN",
			pattern: "^updated$",
		};
		const updated = { ...body, detectable: false, };

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, updated,);
		}, async (url,) => {
			const resource = new MeaningsResource(createClient(url,),);
			await expect(resource.update("meaning/slash", body,),).resolves.toEqual(updated,);
		},);

		expect(observedMethod,).toBe("PUT",);
		expect(observedPath,).toBe("/public/api/meanings/meaning%2Fslash",);
		expect(observedBody,).toEqual(body,);
	});
});
