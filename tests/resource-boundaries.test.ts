import { describe, expect, it, } from "bun:test";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, DataikuError, } from "../src/errors.js";

describe("SDK response and identifier boundaries", () => {
	it("does not turn malformed directory or run-history responses into empty lists", async () => {
		let response: unknown = { error: "degraded", };
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: () => Response.json(response,),
		},);
		try {
			const client = new DataikuClient({ url: server.url.href, apiKey: "test", projectKey: "P", },);
			const reads = [
				() => client.users.list(),
				() => client.users.activityAll(),
				() => client.groups.list(),
				() => client.scenarios.getLastRuns("S",),
			];
			for (const read of reads) await expect(read(),).rejects.toBeInstanceOf(DataikuError,);
			response = [];
			for (const read of reads) await expect(read(),).resolves.toEqual([],);
		} finally {
			server.stop(true,);
		}
	});

	it("rejects missing run identities without inventing handles or attempting status reads", async () => {
		let response: unknown = {};
		const methods: string[] = [];
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: request => {
				methods.push(request.method,);
				return Response.json(response,);
			},
		},);
		try {
			const client = new DataikuClient({ url: server.url.href, apiKey: "test", projectKey: "P", },);
			await expect(client.macros.runAndWait("M", { timeoutMs: 100, },),).rejects.toMatchObject({
				status: 200,
				retryable: false,
			},);
			await expect(client.scenarios.run("S",),).rejects.toMatchObject({
				status: 200,
				retryable: false,
			},);
			await expect(client.scenarios.runAndWait("S",),).rejects.toMatchObject({
				status: 200,
				retryable: false,
			},);
			expect(methods,).toEqual(["POST", "POST", "POST",],);
			response = { runId: "R", id: "legacy", };
			await expect(client.macros.run("M",),).resolves.toMatchObject({ runId: "R", },);
			await expect(client.scenarios.run("S",),).resolves.toEqual({ runId: "R", },);
		} finally {
			server.stop(true,);
		}
	});

	it("rejects invalid SDK identifiers as caller errors before any HTTP", async () => {
		let requests = 0;
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: () => {
				requests++;
				return Response.json({},);
			},
		},);
		try {
			const client = new DataikuClient({ url: server.url.href, apiKey: "test", projectKey: "P", },);
			for (
				const invoke of [
					() => client.users.get(null as never,),
					() => client.groups.get(" ",),
					() => client.connections.adminGet(undefined as never,),
					() => client.connections.prepareTablesImport({ keys: [{ connectionName: "pg", },], },),
					() => client.macros.definition("",),
					() => client.scenarios.run(" ",),
				]
			) {
				await expect(invoke(),).rejects.toBeInstanceOf(ClientValidationError,);
			}
			expect(requests,).toBe(0,);
		} finally {
			server.stop(true,);
		}
	});
});
