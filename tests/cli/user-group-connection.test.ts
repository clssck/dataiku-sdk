import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import {
	sanitizeConnectionError,
	sanitizeConnectionSecrets,
} from "../../src/cli/commands/connection.js";
import { commands, } from "../../src/cli/commands/index.js";
import { sanitizeUserSecrets, } from "../../src/cli/commands/user.js";
import { buildMutationPlan, } from "../../src/cli/plans.js";
import { DataikuClient, } from "../../src/client.js";

const USER = commands["user"]!;
const GROUP = commands["group"]!;
const CONNECTION = commands["connection"]!;

function plan(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	return buildMutationPlan(resource, action, commands[resource]![action]!, args, flags,);
}

function cliSecret(): string {
	return "S3CRET-VALUE";
}

describe("CLI user/group/connection actions", () => {
	it("registers the full documented action surface", () => {
		expect(Object.keys(USER,).sort(),).toEqual([
			"activity",
			"activity-get",
			"create",
			"delete",
			"external-groups",
			"external-users",
			"get",
			"list",
			"provision",
			"resync",
			"resync-multi",
			"update",
		],);
		expect(Object.keys(GROUP,).sort(),).toEqual(["create", "delete", "get", "list", "update",],);
		for (
			const action of [
				"get",
				"create",
				"update",
				"delete",
				"test",
				"prepare-import",
				"execute-import",
			]
		) {
			expect(CONNECTION[action],).toBeDefined();
		}
	});

	it("plans user create with zero requests and redacted payload", () => {
		const p = plan("user", "create", [], {
			data: JSON.stringify({ login: "UserA", password: cliSecret(), },),
		},);
		expect(p,).toMatchObject({ resource: "user", action: "create", method: "POST", },);
		expect(p.endpoint,).toBe("/public/api/admin/users",);
		const payload = JSON.stringify(p.payload,);
		expect(payload,).not.toContain(cliSecret(),);
	});

	it("plans user delete as destructive DELETE with encoded login", () => {
		const p = plan("user", "delete", ["weird/user",], {},);
		expect(p,).toMatchObject({ resource: "user", action: "delete", method: "DELETE", },);
		expect(p.endpoint,).toBe("/public/api/admin/users/weird%2Fuser",);
	});

	it("plans user resync as a future POST", () => {
		const p = plan("user", "resync", ["UserA",], {},);
		expect(p.method,).toBe("POST",);
		expect(p.endpoint,).toBe("/public/api/admin/users/UserA/actions/resync",);
	});

	it("plans user resync-multi with the logins array payload", () => {
		const p = plan("user", "resync-multi", [], { logins: "u1,u2", },);
		expect(p.method,).toBe("POST",);
		expect(p.endpoint,).toBe("/public/api/admin/users/actions/resync-multi",);
		expect(p.payload,).toEqual(["u1", "u2",],);
	});

	it("plans user provision with body identifiers", () => {
		const p = plan("user", "provision", [], {
			data: JSON.stringify({ userSourceType: "AZURE_AD", users: [{ login: "bob", },], },),
		},);
		expect(p.method,).toBe("POST",);
		expect(p.endpoint,).toBe("/public/api/admin/users/actions/provision",);
	});

	it("plans group create/update/delete", () => {
		const create = plan("group", "create", [], {
			data: JSON.stringify({ name: "analysts", admin: false, },),
		},);
		expect(create,).toMatchObject({ method: "POST", endpoint: "/public/api/admin/groups", },);
		const update = plan("group", "update", ["analysts",], { data: '{"name":"analysts"}', },);
		expect(update,).toMatchObject({
			method: "PUT",
			endpoint: "/public/api/admin/groups/analysts",
		},);
		const del = plan("group", "delete", ["analysts",], {},);
		expect(del,).toMatchObject({
			method: "DELETE",
			endpoint: "/public/api/admin/groups/analysts",
		},);
	});

	it("plans connection create/update/delete with redacted params", () => {
		const create = plan("connection", "create", [], {
			data: JSON.stringify({
				name: "conn",
				type: "PostgreSQL",
				params: { password: cliSecret(), },
			},),
		},);
		expect(create,).toMatchObject({ method: "POST", endpoint: "/public/api/admin/connections", },);
		expect(JSON.stringify(create.payload,),).not.toContain(cliSecret(),);

		const update = plan("connection", "update", ["conn",], {
			data: JSON.stringify({ name: "conn", type: "PostgreSQL", params: { password: cliSecret(), }, },),
		},);
		expect(update,).toMatchObject({
			method: "PUT",
			endpoint: "/public/api/admin/connections/conn",
		},);
		expect(JSON.stringify(update.payload,),).not.toContain(cliSecret(),);

		const del = plan("connection", "delete", ["old",], {},);
		expect(del,).toMatchObject({
			method: "DELETE",
			endpoint: "/public/api/admin/connections/old",
		},);
	});

	it("plans connection prepare/execute import on the project endpoint", () => {
		const prepare = plan("connection", "prepare-import", [], {
			data: JSON.stringify({ keys: [{ connectionName: "postgres", name: "t", },], },),
			"project-key": "MYPROJ",
		},);
		expect(prepare,).toMatchObject({
			method: "POST",
			endpoint: "/public/api/projects/MYPROJ/datasets/tables-import/actions/prepare-from-keys",
		},);
		const execute = plan("connection", "execute-import", [], {
			data: JSON.stringify({ sqlImportCandidates: [{ connectionName: "pgsql", table: "t", },], },),
			"project-key": "MYPROJ",
		},);
		expect(execute,).toMatchObject({
			method: "POST",
			endpoint: "/public/api/projects/MYPROJ/datasets/tables-import/actions/execute-from-candidates",
		},);
	});

	it("rejects plans without required JSON input", () => {
		expect(() => plan("user", "create", [], {},)).toThrow();
		expect(() => plan("group", "create", [], {},)).toThrow();
		expect(() => plan("connection", "create", [], {},)).toThrow();
		expect(() => plan("connection", "prepare-import", [], {},)).toThrow();
		expect(() => plan("connection", "execute-import", [], {},)).toThrow();
		expect(() => plan("user", "resync-multi", [], {},)).toThrow();
	});

	it("dry-runs mutators with zero HTTP and redacted payloads", async () => {
		const requests: string[] = [];
		const server = createServer((req: IncomingMessage, res: ServerResponse,) => {
			requests.push(`${String(req.method,)} ${String(req.url,)}`,);
			res.statusCode = 500;
			res.end("dry-run must not hit the server",);
		},);
		await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", () => resolve(),));
		const { port, } = server.address() as AddressInfo;
		const c = new DataikuClient({
			url: `http://127.0.0.1:${String(port,)}`,
			apiKey: "k",
			projectKey: "TEST",
		},);
		try {
			const createResult = await commands["user"]!["create"]!.handler(c, [], {
				data: '{"login":"X","password":"PW-SECRET"}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(createResult["dryRun"],).toBe(true,);
			expect(JSON.stringify(createResult,),).not.toContain("PW-SECRET",);

			const updateResult = await commands["user"]!["update"]!.handler(c, ["UserA",], {
				data: '{"displayName":"N"}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(updateResult["dryRun"],).toBe(true,);

			const deleteResult = await commands["user"]!["delete"]!.handler(c, ["UserA",], {
				"dry-run": true,
				"if-exists": true,
			},) as Record<string, unknown>;
			expect(deleteResult["dryRun"],).toBe(true,);

			const resyncMulti = await commands["user"]!["resync-multi"]!.handler(c, [], {
				logins: "a,b",
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(resyncMulti["dryRun"],).toBe(true,);

			const provision = await commands["user"]!["provision"]!.handler(c, [], {
				data: '{"userSourceType":"AZURE_AD","users":[]}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(provision["dryRun"],).toBe(true,);

			const groupUpdate = await commands["group"]!["update"]!.handler(c, ["g",], {
				data: '{"name":"g"}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(groupUpdate["dryRun"],).toBe(true,);

			const connCreate = await commands["connection"]!["create"]!.handler(c, [], {
				data: '{"name":"c","params":{"password":"CPW"}}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(connCreate["dryRun"],).toBe(true,);
			expect(JSON.stringify(connCreate,),).not.toContain("CPW",);

			const connDelete = await commands["connection"]!["delete"]!.handler(c, ["c",], {
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(connDelete["dryRun"],).toBe(true,);

			const prepare = await commands["connection"]!["prepare-import"]!.handler(c, [], {
				data: '{"keys":[{"connectionName":"pg","name":"t"}]}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(prepare["dryRun"],).toBe(true,);

			const execute = await commands["connection"]!["execute-import"]!.handler(c, [], {
				data: '{"sqlImportCandidates":[{"connectionName":"pg","table":"t"}]}',
				"dry-run": true,
			},) as Record<string, unknown>;
			expect(execute["dryRun"],).toBe(true,);

			expect(requests,).toEqual([],);
		} finally {
			await new Promise<void>((resolve,) => server.close(() => resolve()));
		}
	});

	it("masks the full connection credential key family (AK-LEAK repro)", () => {
		const out = sanitizeConnectionSecrets({
			params: {
				apiKey: "AK-LEAK",
				apiToken: "T-LEAK",
				accessToken: "AT-LEAK",
				refreshToken: "RT-LEAK",
				privateKey: "PK-LEAK",
				secretKey: "SK-LEAK",
				clientSecret: "CS-LEAK",
				ordinary: "keep me",
			},
		},) as { params: Record<string, unknown>; };
		const json = JSON.stringify(out,);
		for (
			const leak of ["AK-LEAK", "T-LEAK", "AT-LEAK", "RT-LEAK", "PK-LEAK", "SK-LEAK", "CS-LEAK",]
		) {
			expect(json,).not.toContain(leak,);
		}
		expect(out.params.ordinary,).toBe("keep me",);
	});

	it("redacts user secrets arrays and scrubs array entries from errors", () => {
		const out = sanitizeUserSecrets({
			login: "UserA",
			secrets: ["S1", "S2",],
			nested: { secrets: { k: "S3", }, profile: { password: "P1", }, },
		},) as Record<string, unknown>;
		const json = JSON.stringify(out,);
		expect(json,).not.toContain("S1",);
		expect(json,).not.toContain("S2",);
		expect(json,).not.toContain("S3",);
		expect(json,).not.toContain("P1",);
	});
});

describe("secret redaction", () => {
	it("masks password keys and exact secret occurrences in user output", () => {
		const secret = cliSecret();
		const input = {
			login: "UserA",
			password: secret,
			nested: { "user-password": secret, deep: [{ password: secret, },], },
			photo: "data:image/png;base64,AAAA",
		};
		const out = sanitizeUserSecrets(input,) as Record<string, unknown>;
		expect(out.password,).toBe("[redacted]",);
		expect(JSON.stringify(out,),).not.toContain(secret,);
	});

	it("masks credential keys anywhere in connection payloads", () => {
		const secret = cliSecret();
		const out = sanitizeConnectionSecrets({
			name: "conn",
			params: { password: secret, token: secret, apiKeyNote: "keep me", },
			list: [{ clientSecret: secret, },],
		},) as Record<string, unknown>;
		const json = JSON.stringify(out,);
		expect(json,).not.toContain(secret,);
		expect(json,).toContain("keep me",);
	});

	it("scrubs secrets from error message, body, and stack", () => {
		const secret = cliSecret();
		const error = new Error(`validation failed for ${secret}`,);
		const withBody = error as Error & { body?: string; };
		withBody.body = `{"params":{"password":"${secret}"}}`;
		sanitizeConnectionError(error, [secret,],);
		expect(error.message,).not.toContain(secret,);
		expect(withBody.body,).not.toContain(secret,);
		expect(error.stack ?? "",).not.toContain(secret,);
	});
});
