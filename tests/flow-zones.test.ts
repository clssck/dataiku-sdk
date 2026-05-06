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
	const client = new DataikuClient({
		url: `http://127.0.0.1:${port}`,
		apiKey: "test",
		projectKey: "TEST",
	},);

	try {
		await run(client,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error?: Error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

async function readRequestBody(req: IncomingMessage,): Promise<string> {
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

describe("FlowZonesResource", () => {
	it("lists and fetches flow zones", async () => {
		const requests: string[] = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (url.pathname.endsWith("/zones/zone-1",)) {
				sendJson(res, { id: "zone-1", name: "Exports", color: "#2ab1ac", items: [], },);
				return;
			}
			sendJson(res, [{ id: "zone-1", name: "Exports", color: "#2ab1ac", items: [], },],);
		}, async (client,) => {
			await expect(client.flowZones.list(),).resolves.toEqual([
				{ id: "zone-1", name: "Exports", color: "#2ab1ac", items: [], },
			],);
			await expect(client.flowZones.get("zone-1",),).resolves.toEqual({
				id: "zone-1",
				name: "Exports",
				color: "#2ab1ac",
				items: [],
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/flow/zones",
			"GET /public/api/projects/TEST/flow/zones/zone-1",
		],);
	});

	it("creates flow zones with default and explicit colors", async () => {
		const requestBodies: unknown[] = [];

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones",);
			requestBodies.push(JSON.parse(await readRequestBody(req,),),);
			sendJson(res, {
				id: `zone-${requestBodies.length}`,
				name: "Exports",
				color: "#2ab1ac",
				items: [],
			},);
		}, async (client,) => {
			await client.flowZones.create({ name: "Exports", },);
			await client.flowZones.create({ name: "Exports", color: "#cc0000", },);
		},);

		expect(requestBodies,).toEqual([
			{ name: "Exports", color: "#2ab1ac", },
			{ name: "Exports", color: "#cc0000", },
		],);
	});

	it("updates flow zones by preserving DSS-managed fields", async () => {
		let putBody: Record<string, unknown> | undefined;
		let step = 0;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET") {
				step += 1;
				expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones/zone-1",);
				sendJson(res, {
					id: "zone-1",
					name: step === 1 ? "Exports" : "Curated exports",
					color: step === 1 ? "#2ab1ac" : "#cc0000",
					items: [{ objectType: "DATASET", objectId: "orders", },],
					position: { x: 10, y: 20, },
				},);
				return;
			}
			expect(req.method,).toBe("PUT",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones/zone-1",);
			putBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (client,) => {
			const updated = await client.flowZones.update("zone-1", {
				name: "Curated exports",
				color: "#cc0000",
			},);
			expect(updated.name,).toBe("Curated exports",);
			expect(updated.color,).toBe("#cc0000",);
		},);

		expect(putBody,).toEqual({
			id: "zone-1",
			name: "Curated exports",
			color: "#cc0000",
			items: [{ objectType: "DATASET", objectId: "orders", },],
			position: { x: 10, y: 20, },
		},);
	});

	it("moves multiple flow items into a zone", async () => {
		let moveBody: unknown;

		await withDataikuServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("POST",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/flow/zones/zone-1/add-items",);
			moveBody = JSON.parse(await readRequestBody(req,),);
			sendJson(res, {
				id: "zone-1",
				name: "Exports",
				items: moveBody,
			},);
		}, async (client,) => {
			const result = await client.flowZones.moveItems("zone-1", [
				{ objectType: "DATASET", objectId: "orders", },
				{ objectType: "MANAGED_FOLDER", objectId: "exports", projectKey: "OTHER", },
			],);
			expect(result.items,).toEqual(moveBody,);
		},);

		expect(moveBody,).toEqual([
			{ objectId: "orders", objectType: "DATASET", },
			{ objectId: "exports", objectType: "MANAGED_FOLDER", projectKey: "OTHER", },
		],);
	});

	it("deletes flow zones and exposes zone graphs", async () => {
		const requests: string[] = [];

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "GET"} ${url.pathname}`,);
			if (req.method === "DELETE") {
				res.statusCode = 204;
				res.end();
				return;
			}
			sendJson(res, { nodes: [], edges: [], },);
		}, async (client,) => {
			await client.flowZones.delete("zone-1",);
			await expect(client.flowZones.graph("zone-1",),).resolves.toEqual({ nodes: [], edges: [], },);
		},);

		expect(requests,).toEqual([
			"DELETE /public/api/projects/TEST/flow/zones/zone-1",
			"GET /public/api/projects/TEST/flow/zones/zone-1/graph",
		],);
	});

	it("rejects empty move requests before calling DSS", async () => {
		await withDataikuServer(() => {
			throw new Error("server should not be called for empty moveItems",);
		}, async (client,) => {
			await expect(client.flowZones.moveItems("zone-1", [],),).rejects.toThrow(
				"flowZones.moveItems requires at least one item",
			);
		},);
	});
});
