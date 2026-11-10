import { afterEach, describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type Server, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";

type Handler = (req: IncomingMessage, res: ServerResponse,) => void;

const servers: Server[] = [];

async function listen(handler: Handler,): Promise<string> {
	const server = createServer(handler,);
	servers.push(server,);
	await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", resolve,));
	return `http://127.0.0.1:${String((server.address() as AddressInfo).port,)}`;
}

afterEach(async () => {
	await Promise.all(
		servers.splice(0,).map((server,) =>
			new Promise<void>((resolve,) => {
				server.closeAllConnections();
				server.close(() => resolve());
			},)
		),
	);
},);

function clientFor(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", requestTimeoutMs: 2_000, },);
}

describe("DataikuClient redirect policy", () => {
	const statuses = [301, 302, 307, 308,] as const;
	const mutations = [
		["POST", (c: DataikuClient,) => c.post("/things", { a: 1, },),],
		["PUT", (c: DataikuClient,) => c.put("/things", { a: 1, },),],
		["DELETE", (c: DataikuClient,) => c.del("/things",),],
	] as const;

	for (const status of statuses) {
		for (const [method, call,] of mutations) {
			it(`rejects a ${status} answer to ${method} and never contacts the target`, async () => {
				let targetRequests = 0;
				const target = await listen((_req, res,) => {
					targetRequests++;
					res.end('{"applied":"elsewhere"}',);
				},);
				const source = await listen((_req, res,) => {
					res.writeHead(status, { Location: `${target}/user:pw/path?token=secret`, },);
					res.end("moved",);
				},);

				const failure = await call(clientFor(source,),).catch((error: unknown,) => error);

				expect(failure,).toBeInstanceOf(DataikuError,);
				const error = failure as DataikuError;
				expect(error.status,).toBe(status,);
				expect(error.category,).toBe("unexpected_response",);
				expect(error.retryable,).toBe(false,);
				expect(error.retryHint,).toContain("DATAIKU_URL",);
				expect(error.retryHint,).toContain("was not applied",);
				expect(error.details,).toEqual({ redirectOrigin: target, },);
				expect(error.message,).not.toContain("secret",);
				expect(error.message,).not.toContain("/path",);
				expect(error.retry?.method,).toBe(method,);
				expect(targetRequests,).toBe(0,);
			});
		}
	}

	it("does not retry a redirected mutation even when retries are enabled", async () => {
		let sourceRequests = 0;
		const source = await listen((_req, res,) => {
			sourceRequests++;
			res.writeHead(307, { Location: "/elsewhere", },);
			res.end();
		},);

		const failure = await clientFor(source,).post("/things", {}, { retryMaxAttempts: 3, },).catch((
			error: unknown,
		) => error);

		expect(failure,).toBeInstanceOf(DataikuError,);
		expect((failure as DataikuError).status,).toBe(307,);
		expect(sourceRequests,).toBe(1,);
	});

	it("reports a redirect without a usable Location without an origin", async () => {
		const source = await listen((_req, res,) => {
			res.writeHead(302,);
			res.end();
		},);

		const failure = await clientFor(source,).post("/things", {},).catch((error: unknown,) => error);

		expect(failure,).toBeInstanceOf(DataikuError,);
		expect((failure as DataikuError).category,).toBe("unexpected_response",);
		expect((failure as DataikuError).details,).toBeUndefined();
	});

	it("still follows a same-origin GET redirect", async () => {
		const source = await listen((req, res,) => {
			if (req.url === "/old") {
				res.writeHead(302, { Location: "/new", },);
				res.end();
				return;
			}
			res.end('{"ok":true}',);
		},);

		expect(await clientFor(source,).get("/old",),).toEqual({ ok: true, },);
	});

	it("does not deliver Authorization to another origin when a GET redirect is followed", async () => {
		const seen: Array<string | undefined> = [];
		const target = await listen((req, res,) => {
			seen.push(req.headers.authorization,);
			res.end('{"ok":true}',);
		},);
		const source = await listen((_req, res,) => {
			res.writeHead(302, { Location: `${target}/landing`, },);
			res.end();
		},);

		await clientFor(source,).get("/old",);

		expect(seen,).toEqual([undefined,],);
	});
});
