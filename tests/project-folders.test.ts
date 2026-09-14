import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ProjectFoldersResource, } from "../src/resources/project-folders.js";

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

describe("ProjectFoldersResource", () => {
	it("gets the root project folder", async () => {
		const root = {
			id: "root",
			name: null,
			parentId: null,
			childrenIds: ["KdLmPU6",],
			projectKeys: ["MYPROJECT",],
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, root,);
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.root(),).resolves.toEqual(root,);
		},);

		expect(requests,).toEqual(["GET /public/api/project-folders/",],);
	});

	it("gets a project folder with an encoded id", async () => {
		const folder = { id: "space id", name: "Space", childrenIds: [], projectKeys: [], };
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			sendJson(res, folder,);
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.get("space id",),).resolves.toEqual(folder,);
		},);

		expect(observedPath,).toBe("/public/api/project-folders/space%20id",);
	});

	it("gets and saves settings with PUT", async () => {
		const settings = {
			name: "my folder",
			owner: "admin",
			permissions: [{ group: "data_scientists", read: true, writeContents: true, admin: false, },],
		};
		const requests: string[] = [];
		const bodies: unknown[] = [];

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "PUT") {
				bodies.push(JSON.parse(await readBody(req,),),);
			}
			if (req.method === "GET") {
				sendJson(res, settings,);
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.getSettings("KdLmPU6",),).resolves.toEqual(settings,);
			await expect(resource.updateSettings("KdLmPU6", settings,),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"GET /public/api/project-folders/KdLmPU6/settings",
			"PUT /public/api/project-folders/KdLmPU6/settings",
		],);
		expect(bodies,).toEqual([settings,],);
	});

	it("moves a folder with an encoded destination query", async () => {
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.move("KdLmPU6", "dest/id",),).resolves.toBeUndefined();
		},);

		expect(observedPath,).toBe("/public/api/project-folders/KdLmPU6/move?destination=dest%2Fid",);
	});

	it("deletes an empty project folder", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.delete("KdLmPU6",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual(["DELETE /public/api/project-folders/KdLmPU6",],);
	});

	it("creates a child folder with an encoded name query", async () => {
		const created = {
			id: "child01",
			name: "my sub folder",
			parentId: "KdLmPU6",
			childrenIds: [],
			projectKeys: [],
		};
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			sendJson(res, created, 200,);
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.createChild("KdLmPU6", "my sub folder",),).resolves.toEqual(
				created,
			);
		},);

		expect(observedPath,).toBe(
			"/public/api/project-folders/KdLmPU6/children?name=my%20sub%20folder",
		);
	});

	it("moves a project with encoded key and destination", async () => {
		let observedPath = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectFoldersResource(createClient(url,),);
			await expect(resource.moveProject("KdLmPU6", "MY PROJECT", "dgKywsx",),).resolves
				.toBeUndefined();
		},);

		expect(observedPath,).toBe(
			"/public/api/project-folders/KdLmPU6/projects/MY%20PROJECT/move?destination=dgKywsx",
		);
	});
});
