import { describe, expect, it, } from "bun:test";
import { createServer, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

describe("remaining DSS 15 endpoints", () => {
	it("call the documented method, path, query, and body", async () => {
		const seen: Array<{ request: string; body: unknown; }> = [];
		const server = createServer(async (req, res,) => {
			let text = "";
			for await (const chunk of req) text += chunk.toString();
			seen.push({ request: `${req.method} ${req.url}`, body: text ? JSON.parse(text,) : undefined, },);
			if (req.url?.startsWith("/public/api/futures",)) {
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}
			res.setHeader("Content-Type", "application/json",);
			res.end("{}",);
		},);
		const listening = Promise.withResolvers<void>();
		server.listen(0, "127.0.0.1", () => listening.resolve(),);
		await listening.promise;
		const { port, } = server.address() as AddressInfo;
		const client = new DataikuClient({
			url: `http://127.0.0.1:${port}`,
			apiKey: "k",
			projectKey: "P",
			retryMaxAttempts: 1,
		},);
		try {
			await client.workspaces.removeObject("WS", "obj/1",);
			await client.discussions.update("DATASET", "orders", "D1", { id: "D1", topic: "t", },);
			await client.wiki.updateSettings({ homeArticleId: "Home", taxonomy: [], },);
			await client.streamingEndpoints.createManaged("s1", {
				connectionId: "kafka",
				formatOptionId: "json",
			},);
			await client.streamingEndpoints.getSchema("s1",);
			await client.streamingEndpoints.setSchema("s1", { columns: [], },);
			await client.futures.list({ allUsers: true, },);
			await client.webapps.trust("W1", { trustForEverybody: true, },);
			await client.projects.pushToGitRemote("origin",);
		} finally {
			const closed = Promise.withResolvers<void>();
			server.close(() => closed.resolve());
			await closed.promise;
		}
		expect(seen,).toEqual([
			{ request: "DELETE /public/api/workspaces/WS/objects/obj%2F1", body: undefined, },
			{
				request: "PUT /public/api/projects/P/discussions/DATASET/orders/D1",
				body: { id: "D1", topic: "t", },
			},
			{ request: "PUT /public/api/projects/P/wiki/", body: { homeArticleId: "Home", taxonomy: [], }, },
			{
				request: "POST /public/api/projects/P/streamingendpoints/managed",
				body: { id: "s1", creationSettings: { connectionId: "kafka", formatOptionId: "json", }, },
			},
			{ request: "GET /public/api/projects/P/streamingendpoints/s1/schema", body: undefined, },
			{ request: "PUT /public/api/projects/P/streamingendpoints/s1/schema", body: { columns: [], }, },
			{ request: "GET /public/api/futures/?allUsers=true&withScenarios=false", body: undefined, },
			{
				request: "POST /public/api/projects/P/webapps/W1/actions/trust?trustForEverybody=true",
				body: undefined,
			},
			{
				request: "POST /public/api/projects/P/actions/push-to-git-remote?remote=origin",
				body: undefined,
			},
		],);
	});
});
