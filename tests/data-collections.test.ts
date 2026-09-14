import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DataCollectionsResource, } from "../src/resources/data-collections.js";

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

describe("DataCollectionsResource", () => {
	it("lists collections", async () => {
		const collections = [
			{
				displayName: "My first Collection",
				color: "#FF0000",
				description: "Description of the collection",
				tags: ["some", "tags",],
				id: "OjVsTQ3O",
				itemCount: 12,
				datasetsCount: 5,
				lastModifiedOn: 1680307200000,
			},
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, collections,);
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.list(),).resolves.toEqual(collections,);
		},);

		expect(requests,).toEqual(["GET /public/api/data-collections/",],);
	});

	it("creates a collection and surfaces the server id", async () => {
		const body = {
			displayName: "My first Collection",
			color: "#FF0000",
			description: "Description",
			tags: ["some", "tags",],
			permissions: [{ user: "user_login", admin: true, write: true, read: true, },],
		};
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			sendJson(res, { msg: "Created Collection OjVsTQ3O", id: "OjVsTQ3O", }, 201,);
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.create(body,),).resolves.toEqual({
				msg: "Created Collection OjVsTQ3O",
				id: "OjVsTQ3O",
			},);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/data-collections/",);
		expect(observedBody,).toEqual(body,);
	});

	it("gets collection settings with an encoded id", async () => {
		const settings = {
			id: "OjVsTQ3O",
			displayName: "My first Collection",
			color: "#FF0000",
			description: "Description",
			tags: [],
			permissions: [{ user: "user_login", admin: true, write: true, read: true, },],
		};
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			sendJson(res, settings,);
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.get("OjVsTQ3O",),).resolves.toEqual(settings,);
		},);

		expect(observedPath,).toBe("/public/api/data-collections/OjVsTQ3O",);
	});

	it("updates settings with PUT and deletes the collection", async () => {
		const settings = {
			displayName: "Renamed Collection",
			color: "#00FF00",
			description: "updated",
			tags: ["x",],
			permissions: [],
		};
		const requests: string[] = [];
		const bodies: unknown[] = [];

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "PUT") {
				bodies.push(JSON.parse(await readBody(req,),),);
			}
			res.statusCode = 200;
			res.end();
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.update("OjVsTQ3O", settings,),).resolves.toBeUndefined();
			await expect(resource.delete("OjVsTQ3O",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"PUT /public/api/data-collections/OjVsTQ3O",
			"DELETE /public/api/data-collections/OjVsTQ3O",
		],);
		expect(bodies,).toEqual([settings,],);
	});

	it("lists collection objects", async () => {
		const objects = [
			{ type: "DATASET", projectKey: "PROJECT_KEY", id: "dataset_name", },
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, objects,);
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.listObjects("OjVsTQ3O",),).resolves.toEqual(objects,);
		},);

		expect(requests,).toEqual(["GET /public/api/data-collections/OjVsTQ3O/objects",],);
	});

	it("adds a collection object with the documented body", async () => {
		const object = { type: "DATASET", projectKey: "PROJECT_KEY", id: "dataset_name", };
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			res.statusCode = 200;
			res.end();
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.addObject("OjVsTQ3O", object,),).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/data-collections/OjVsTQ3O/objects",);
		expect(observedBody,).toEqual(object,);
	});

	it("removes a dataset with encoded path segments", async () => {
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			sendJson(res, { ok: true, },);
		}, async (url,) => {
			const resource = new DataCollectionsResource(createClient(url,),);
			await expect(resource.removeDataset("OjVsTQ3O", "MY PROJECT", "dataset/one",),).resolves
				.toBeUndefined();
		},);

		expect(observedPath,).toBe(
			"/public/api/data-collections/OjVsTQ3O/objects/dataset/MY%20PROJECT/dataset%2Fone",
		);
	});
});
