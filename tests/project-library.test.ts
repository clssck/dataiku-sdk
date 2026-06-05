import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ProjectLibraryResource, } from "../src/resources/project-library.js";

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

describe("ProjectLibraryResource", () => {
	it("lists the project library contents tree", async () => {
		const contents = [
			{
				name: "python",
				children: [{ name: "utils.py", },],
			},
		];
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, contents,);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.listContents("ALT PROJECT",),).resolves.toEqual(contents,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/ALT%20PROJECT/libraries/contents",
		],);
	});

	it("reads text and byte file contents with encoded paths", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if ((req.url ?? "").endsWith("?dataEncoding=base64",)) {
				sendJson(res, {
					data: Buffer.from("raw bytes", "utf8",).toString("base64",),
				},);
				return;
			}
			sendJson(res, { data: "print('hello')\n", },);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.getFile("python scripts/module name.py",),).resolves.toBe(
				"print('hello')\n",
			);
			const bytes = await resource.getFileBytes("python scripts/raw data.bin",);
			expect(Array.from(bytes,),).toEqual(
				Array.from(Buffer.from("raw bytes", "utf8",),),
			);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/libraries/contents/python%20scripts/module%20name.py",
			"GET /public/api/projects/TEST/libraries/contents/python%20scripts/raw%20data.bin?dataEncoding=base64",
		],);
	});

	it("posts raw text when adding or updating files", async () => {
		let observedMethod = "";
		let observedPath = "";
		let observedBody = "";
		const content = 'line 1\n"not json"\n';

		await withServer(async (req, res,) => {
			observedMethod = req.method ?? "";
			observedPath = req.url ?? "";
			observedBody = await readBody(req,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(
				resource.addOrUpdateFile("python/lib file.py", content,),
			).resolves.toBeUndefined();
		},);

		expect(observedMethod,).toBe("POST",);
		expect(observedPath,).toBe(
			"/public/api/projects/TEST/libraries/contents/python/lib%20file.py",
		);
		expect(observedBody,).toBe(content,);
	});

	it("creates empty files and folders and deletes contents", async () => {
		const requests: Array<{ method: string; url: string; body: string; }> = [];

		await withServer(async (req, res,) => {
			requests.push({
				method: req.method ?? "",
				url: req.url ?? "",
				body: await readBody(req,),
			},);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFile("python/new.py",),).resolves.toBeUndefined();
			await expect(resource.addFolder("python/package",),).resolves.toBeUndefined();
			await expect(resource.deleteFile("python/old.py",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/contents/python/new.py",
				body: "",
			},
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/folders/python/package",
				body: "",
			},
			{
				method: "DELETE",
				url: "/public/api/projects/TEST/libraries/contents/python/old.py",
				body: "",
			},
		],);
	});

	it("renames and moves library items through the action endpoints", async () => {
		const requests: Array<{ method: string; url: string; body: unknown; }> = [];

		await withServer(async (req, res,) => {
			const body = await readBody(req,);
			requests.push({
				method: req.method ?? "",
				url: req.url ?? "",
				body: body ? JSON.parse(body,) : undefined,
			},);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.rename("python/old.py", "new.py",),).resolves.toBeUndefined();
			await expect(resource.move("python/new.py", "archive",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/contents-actions/rename/",
				body: { oldPath: "/python/old.py", newName: "new.py", },
			},
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/contents-actions/move",
				body: { oldPath: "/python/new.py", newPath: "/archive", },
			},
		],);
	});
});
