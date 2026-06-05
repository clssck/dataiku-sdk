import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { WorkspacesResource, } from "../src/resources/workspaces.js";

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

describe("WorkspacesResource", () => {
	it("lists workspaces", async () => {
		const workspaces = [
			{ workspaceKey: "SPACE1", displayName: "Space 1", },
			{ workspaceKey: "SPACE2", displayName: "Space 2", },
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? "GET"} ${req.url ?? ""}`,);
			sendJson(res, workspaces,);
		}, async (url,) => {
			const resource = new WorkspacesResource(createClient(url,),);
			await expect(resource.list(),).resolves.toEqual(workspaces,);
		},);

		expect(requests,).toEqual(["GET /public/api/workspaces/",],);
	});

	it("gets workspace settings with encoded keys", async () => {
		const settings = {
			workspaceKey: "space/key",
			displayName: "Space Key",
			color: "#123456",
			description: "workspace description",
			permissions: [{ group: "readers", read: true, write: false, admin: false, },],
		};
		let observedMethod = "";
		let observedPath = "";

		await withServer((req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			sendJson(res, settings,);
		}, async (url,) => {
			const resource = new WorkspacesResource(createClient(url,),);
			await expect(resource.get("space/key",),).resolves.toEqual(settings,);
		},);

		expect(observedMethod,).toBe("GET",);
		expect(observedPath,).toBe("/public/api/workspaces/space%2Fkey",);
	});

	it("creates workspaces through the text endpoint", async () => {
		const body = {
			workspaceKey: "NEW_SPACE",
			displayName: "New Space",
			color: "#abcdef",
			description: "new workspace",
			permissions: [{ user: "alice", admin: true, write: true, read: true, },],
		};
		let observedMethod = "";
		let observedPath = "";
		let observedBody: unknown;

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = JSON.parse(await readBody(req,),);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end("Workspace created",);
		}, async (url,) => {
			const resource = new WorkspacesResource(createClient(url,),);
			await expect(resource.create(body,),).resolves.toBe("Workspace created",);
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe("/public/api/workspaces/",);
		expect(observedBody,).toEqual(body,);
	});

	it("updates settings and deletes workspaces with encoded keys", async () => {
		const settings = {
			displayName: "Updated Space",
			color: "#654321",
			description: "updated workspace",
			permissions: [{ group: "admins", admin: true, write: true, read: true, },],
		};
		const requests: string[] = [];
		const bodies: unknown[] = [];

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "PUT") {
				bodies.push(JSON.parse(await readBody(req,),),);
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new WorkspacesResource(createClient(url,),);
			await expect(resource.updateSettings("space/delete", settings,),).resolves.toBeUndefined();
			await expect(resource.delete("space/delete",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			"PUT /public/api/workspaces/space%2Fdelete",
			"DELETE /public/api/workspaces/space%2Fdelete",
		],);
		expect(bodies,).toEqual([settings,],);
	});

	it("lists and adds workspace objects", async () => {
		const objects = [
			{
				id: "dataset-object",
				reference: {
					projectKey: "TEST",
					type: "DATASET",
					id: "input_dataset",
					workspaceKey: "team space",
				},
			},
		];
		const object = {
			htmlLink: {
				name: "Docs",
				url: "https://example.invalid/docs",
				description: "Documentation",
			},
		};
		const created = { id: "html-link-object", ...object, };
		const requests: string[] = [];
		const bodies: unknown[] = [];

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "GET") {
				sendJson(res, objects,);
				return;
			}
			bodies.push(JSON.parse(await readBody(req,),),);
			sendJson(res, created,);
		}, async (url,) => {
			const resource = new WorkspacesResource(createClient(url,),);
			await expect(resource.listObjects("team space",),).resolves.toEqual(objects,);
			await expect(resource.addObject("team space", object,),).resolves.toEqual(created,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/workspaces/team%20space/objects",
			"POST /public/api/workspaces/team%20space/objects",
		],);
		expect(bodies,).toEqual([object,],);
	});
});
