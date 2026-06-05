import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type Server, type ServerResponse, } from "node:http";
import { DataikuClient, } from "../src/client.js";
import { DatasetsResource, } from "../src/resources/datasets.js";

function createClient(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
}

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) body += chunk.toString();
	return body;
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => void | Promise<void>,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server: Server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch(() => {
			res.statusCode = 500;
			res.end();
		},);
	},);
	await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", () => resolve(),));
	const addr = server.address();
	const url = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
	try {
		await run(url,);
	} finally {
		server.close();
	}
}

describe("DatasetsResource flow ops", () => {
	it("renames a dataset via the project rename action", async () => {
		let method = "";
		let path = "";
		let body: unknown;
		await withServer(async (req, res,) => {
			method = req.method ?? "";
			path = req.url ?? "";
			body = JSON.parse(await readBody(req,),);
			res.statusCode = 200;
			res.end();
		}, async (url,) => {
			await new DatasetsResource(createClient(url,),).rename("old ds", "new ds",);
		},);
		expect(method,).toBe("POST",);
		expect(path,).toBe("/public/api/projects/TEST/actions/renameDataset",);
		expect(body,).toEqual({ oldName: "old ds", newName: "new ds", },);
	});

	it("lists partitions as a bare array", async () => {
		const partitions = ["2024-01", "2024-02",];
		await withServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/projects/TEST/datasets/events/partitions",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(partitions,),);
		}, async (url,) => {
			await expect(new DatasetsResource(createClient(url,),).listPartitions("events",),)
				.resolves.toEqual(partitions,);
		},);
	});

	it("clears the whole dataset when no partitions are given", async () => {
		let method = "";
		let path = "";
		await withServer((req, res,) => {
			method = req.method ?? "";
			path = req.url ?? "";
			res.statusCode = 200;
			res.end("{}",);
		}, async (url,) => {
			await new DatasetsResource(createClient(url,),).clear("staging",);
		},);
		expect(method,).toBe("DELETE",);
		expect(path,).toBe("/public/api/projects/TEST/datasets/staging/data",);
	});

	it("clears a specific partition spec via query parameter", async () => {
		let path = "";
		await withServer((req, res,) => {
			path = req.url ?? "";
			res.statusCode = 200;
			res.end("{}",);
		}, async (url,) => {
			await new DatasetsResource(createClient(url,),).clear("events", "2024-01",);
		},);
		expect(path,).toBe("/public/api/projects/TEST/datasets/events/data?partitions=2024-01",);
	});
});
