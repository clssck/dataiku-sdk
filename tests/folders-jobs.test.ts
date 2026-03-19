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

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

describe("FoldersResource.resolveId", () => {
	it("prefers exact IDs, resolves exact names, and preserves missing values", async () => {
		let requestCount = 0;

		await withDataikuServer((req, res,) => {
			requestCount += 1;
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/managedfolders/",);
			sendJson(res, [
				{ id: "folder-id", name: "Shared data", },
				{ id: "folder-by-name", name: "Exports", },
			],);
		}, async (client,) => {
			expect(await client.folders.resolveId("folder-id",),).toBe("folder-id",);
			expect(await client.folders.resolveId("Exports",),).toBe("folder-by-name",);
			expect(await client.folders.resolveId("missing-folder",),).toBe("missing-folder",);
		},);

		expect(requestCount,).toBe(3,);
	});
});

describe("JobsResource.log", () => {
	it("tails 500 lines by default and preserves activity filtering", async () => {
		const fullLog = Array.from({ length: 600, }, (_value, index,) => `line ${index + 1}`,).join(
			"\n",
		);

		await withDataikuServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-1/log/",);
			expect(url.searchParams.get("activity",),).toBe("build",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (client,) => {
			const log = await client.jobs.log("job-1", { activity: "build", },);
			const lines = log.split("\n",);
			expect(lines,).toHaveLength(500,);
			expect(lines[0],).toBe("line 101",);
			expect(lines.at(-1,),).toBe("line 600",);
		},);
	});

	it("returns the full log when maxLogLines is 0 or -1", async () => {
		const fullLog = Array.from({ length: 600, }, (_value, index,) => `line ${index + 1}`,).join(
			"\n",
		);
		let requestCount = 0;

		await withDataikuServer((req, res,) => {
			requestCount += 1;
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/jobs/job-2/log/",);
			expect(url.search,).toBe("",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (client,) => {
			expect(await client.jobs.log("job-2", { maxLogLines: 0, },),).toBe(fullLog,);
			expect(await client.jobs.log("job-2", { maxLogLines: -1, },),).toBe(fullLog,);
		},);

		expect(requestCount,).toBe(2,);
	});
});
