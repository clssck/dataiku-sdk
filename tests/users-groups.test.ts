import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, } from "../src/errors.js";

async function readJsonBody(req: IncomingMessage,): Promise<unknown> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) {
		chunks.push(Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,),);
	}
	const body = Buffer.concat(chunks,).toString("utf8",);
	return body.length > 0 ? JSON.parse(body,) : undefined;
}

async function withTestServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string, requests: string[],) => Promise<void>,
): Promise<void> {
	const requests: string[] = [];
	const server = createServer((req, res,) => {
		requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);
	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) rejectPromise(error,);
			else resolvePromise();
		},);
	},);
	const { port, } = server.address() as AddressInfo;
	try {
		await run(`http://127.0.0.1:${String(port,)}`, requests,);
	} finally {
		await new Promise<void>((resolvePromise,) => {
			server.close(() => resolvePromise());
		},);
	}
}

function client(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
}

function json(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

const FUTURE_STATE = { hasResult: true, aborted: false, alive: false, result: { success: true, }, };

describe("UsersResource", () => {
	it("URL-encodes logins and forwards the connected flag", async () => {
		await withTestServer((req, res,) => {
			if (req.url === "/public/api/admin/users/?connected=true") {
				json(res, [{ login: "admin", groups: ["administrators",], },],);
				return;
			}
			if (req.url === "/public/api/admin/users/") {
				json(res, [],);
				return;
			}
			expect(req.url,).toBe("/public/api/admin/users/weird%2Fuser",);
			json(res, { login: "weird/user", groups: [], },);
		}, async (url,) => {
			await expect(client(url,).users.list({ connected: true, },),).resolves.toEqual([
				{ login: "admin", groups: ["administrators",], },
			],);
			await expect(client(url,).users.list(),).resolves.toEqual([],);
			await expect(client(url,).users.get("weird/user",),).resolves.toEqual({
				login: "weird/user",
				groups: [],
			},);
		},);
	});

	it("creates a user with the documented body and returns the server record", async () => {
		await withTestServer(async (req, res,) => {
			expect(req.method,).toBe("POST",);
			expect(req.url,).toBe("/public/api/admin/users",);
			expect(await readJsonBody(req,),).toEqual({
				login: "UserA",
				sourceType: "LOCAL",
				userProfile: "DATA_ANALYST",
				password: "s3cret",
			},);
			res.statusCode = 201;
			json(res, { msg: "Created user UserA", },);
		}, async (url,) => {
			await expect(
				client(url,).users.create({
					login: "UserA",
					sourceType: "LOCAL",
					userProfile: "DATA_ANALYST",
					password: "s3cret",
				},),
			).resolves.toEqual({ msg: "Created user UserA", },);
		},);
	});

	it("updates, deletes, and reports the deleted login", async () => {
		await withTestServer(async (req, res,) => {
			if (req.method === "PUT") {
				expect(req.url,).toBe("/public/api/admin/users/UserA",);
				expect(await readJsonBody(req,),).toEqual({ login: "UserA", displayName: "Renamed", },);
				json(res, { msg: "Edited user UserA", },);
				return;
			}
			expect(req.method,).toBe("DELETE",);
			expect(req.url,).toBe("/public/api/admin/users/UserA",);
			json(res, { msg: "Deleted user UserA", },);
		}, async (url,) => {
			await expect(client(url,).users.update("UserA", { login: "UserA", displayName: "Renamed", },),)
				.resolves.toEqual({ msg: "Edited user UserA", },);
			await expect(client(url,).users.delete("UserA",),).resolves.toEqual({ deleted: "UserA", },);
		},);
	});

	it("mutation actions return futures parsed as FutureState", async () => {
		await withTestServer(async (req, res,) => {
			if (req.method === "POST" && req.url === "/public/api/admin/users/UserA/actions/resync") {
				expect(await readJsonBody(req,),).toBeUndefined();
				json(res, FUTURE_STATE,);
				return;
			}
			if (req.method === "POST" && req.url === "/public/api/admin/users/actions/resync-multi") {
				expect(await readJsonBody(req,),).toEqual(["u1", "u2",],);
				json(res, FUTURE_STATE,);
				return;
			}
			expect(req.method,).toBe("POST",);
			expect(req.url,).toBe("/public/api/admin/users/actions/provision",);
			expect(await readJsonBody(req,),).toEqual({
				userSourceType: "AZURE_AD",
				users: [{ login: "bob", sourceGroupNames: ["externalGroupA",], },],
			},);
			json(res, FUTURE_STATE,);
		}, async (url,) => {
			await expect(client(url,).users.resync("UserA",),).resolves.toHaveProperty("hasResult", true,);
			await expect(client(url,).users.resyncMulti(["u1", "u2",],),).resolves.toHaveProperty(
				"alive",
				false,
			);
			await expect(
				client(url,).users.provision({
					userSourceType: "AZURE_AD",
					users: [{ login: "bob", sourceGroupNames: ["externalGroupA",], },],
				},),
			).resolves.toHaveProperty("alive", false,);
		},);
	});

	it("external users/groups reads surface the future result payloads", async () => {
		await withTestServer((req, res,) => {
			if (req.url === "/public/api/admin/users/actions/external-users") {
				json(res, {
					hasResult: true,
					alive: false,
					result: [{ status: "SYNCED", profile: "READER", },],
				},);
				return;
			}
			expect(req.url,).toBe("/public/api/admin/users/actions/external-groups",);
			json(res, { hasResult: true, alive: false, result: ["externalGroupA",], },);
		}, async (url,) => {
			await expect(client(url,).users.externalUsers(),).resolves.toHaveProperty(
				"result",
				[{ status: "SYNCED", profile: "READER", },],
			);
			await expect(client(url,).users.externalGroups(),).resolves.toHaveProperty(
				"result",
				["externalGroupA",],
			);
		},);
	});

	it("activity reads cover all users and one login", async () => {
		await withTestServer((req, res,) => {
			if (req.url === "/public/api/admin/users-activity") {
				json(res, [{ login: "user1", lastSuccessfulLogin: 1647448027194, },],);
				return;
			}
			expect(req.url,).toBe("/public/api/admin/users/UserA/activity",);
			json(res, { login: "UserA", lastLoaded: 1, },);
		}, async (url,) => {
			await expect(client(url,).users.activityAll(),).resolves.toEqual([
				{ login: "user1", lastSuccessfulLogin: 1647448027194, },
			],);
			await expect(client(url,).users.activity("UserA",),).resolves.toEqual({
				login: "UserA",
				lastLoaded: 1,
			},);
		},);
	});

	it("rejects invalid mutations before any request", async () => {
		await withTestServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url, requests,) => {
			const c = client(url,);
			await expect(c.users.create({ password: "x", } as never,),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
			await expect(c.users.resyncMulti([],),).rejects.toBeInstanceOf(ClientValidationError,);
			await expect(c.users.resyncMulti([" ",],),).rejects.toBeInstanceOf(ClientValidationError,);
			await expect(c.users.provision({ users: [], } as never,),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
			await expect(c.users.get("  ",),).rejects.toBeInstanceOf(ClientValidationError,);
			expect(requests,).toEqual([],);
		},);
	});
});

describe("GroupsResource", () => {
	it("performs the full group lifecycle with encoded names", async () => {
		await withTestServer(async (req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/admin/groups") {
				json(res, [{ name: "administrators", admin: true, sourceType: "LOCAL", },],);
				return;
			}
			if (req.method === "GET") {
				expect(req.url,).toBe("/public/api/admin/groups/g%20r",);
				json(res, { name: "g r", admin: false, },);
				return;
			}
			if (req.method === "POST") {
				expect(req.url,).toBe("/public/api/admin/groups",);
				expect(await readJsonBody(req,),).toEqual({ name: "analysts", admin: false, },);
				json(res, { msg: "Created group analysts", },);
				return;
			}
			if (req.method === "PUT") {
				expect(req.url,).toBe("/public/api/admin/groups/analysts",);
				json(res, { msg: "Edited group analysts", },);
				return;
			}
			expect(req.method,).toBe("DELETE",);
			expect(req.url,).toBe("/public/api/admin/groups/analysts",);
			json(res, { msg: "Deleted group analysts", },);
		}, async (url,) => {
			await expect(client(url,).groups.list(),).resolves.toEqual([
				{ name: "administrators", admin: true, sourceType: "LOCAL", },
			],);
			await expect(client(url,).groups.get("g r",),).resolves.toEqual({ name: "g r", admin: false, },);
			await expect(client(url,).groups.create({ name: "analysts", admin: false, },),)
				.resolves.toEqual({ msg: "Created group analysts", },);
			await expect(client(url,).groups.update("analysts", { name: "analysts", },),)
				.resolves.toEqual({ msg: "Edited group analysts", },);
			await expect(client(url,).groups.delete("analysts",),).resolves.toEqual({
				deleted: "analysts",
			},);
		},);
	});

	it("rejects group create without a name before any request", async () => {
		await withTestServer((_req, res,) => {
			res.statusCode = 500;
			res.end("must not be called",);
		}, async (url,) => {
			await expect(client(url,).groups.create({ admin: true, } as never,),).rejects.toThrow(
				"body.name is required",
			);
		},);
	});
});
