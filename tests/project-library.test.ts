import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { ClientValidationError, DataikuError, } from "../src/errors.js";
import {
	encodeLibraryPath,
	type ProjectLibraryItem,
	ProjectLibraryResource,
	validateLibraryDestinationPath,
	validateLibraryName,
	validateLibraryPath,
} from "../src/resources/project-library.js";

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

interface RecordedRequest {
	method: string;
	url: string;
	body: Uint8Array;
	text: string;
}

/**
 * A DataikuClient double that records every HTTP-level request the resource
 * attempts (method, URL, exact body bytes) without touching the network, and
 * answers from a scripted handler.
 */
function recordingClient(
	handler: (req: RecordedRequest,) => unknown | undefined,
): { client: DataikuClient; requests: RecordedRequest[]; } {
	const requests: RecordedRequest[] = [];
	const raw: Record<string, unknown> = {
		baseUrl: "http://recording.test",
		resolveProjectKey: (pk?: string,) => pk ?? "TEST",
		get: async <T,>(path: string,) => {
			const self = raw as unknown as {
				fetchWithRetry(url: string, init: RequestInit,): Promise<Response>;
			};
			const res = await self.fetchWithRetry(`http://recording.test${path}`, { method: "GET", },);
			if (res.status === 404) {
				throw new DataikuError(res.status, res.statusText, "", undefined, undefined,);
			}
			return await res.json() as T;
		},
		getAnyHeaders: () => ({ Authorization: "Bearer test-key", Accept: "*/*", }),
		fetchWithRetry: async (url: string, init: RequestInit,) => {
			const body = init.body;
			const bytes = typeof body === "string" ? Buffer.from(body, "utf8",) : new Uint8Array(0,);
			const request: RecordedRequest = {
				method: init.method ?? "GET",
				url,
				body: bytes,
				text: typeof body === "string" ? body : "",
			};
			requests.push(request,);
			const scripted = handler(request,);
			const status = scripted === undefined ? 404 : 200;
			const responseText = scripted === undefined ? "" : JSON.stringify(scripted,);
			return new Response(responseText, {
				status,
				headers: { "Content-Type": "application/json", },
			},);
		},
	};
	return { client: raw as unknown as DataikuClient, requests, };
}

function assertClientValidationError(error: unknown,): ClientValidationError {
	expect(error,).toBeInstanceOf(ClientValidationError,);
	return error as ClientValidationError;
}

const TRAVERSAL_PATHS = [
	"../escape.py",
	"a/../../escape.py",
	"a/..",
	"..",
	".",
	"",
	"   ",
	"a//b",
	"/a//b/",
	"a/.",
	"./a",
	"a/b/",
	"a\\b",
	"a/b.",
	"a/ b",
	"a/b ",
	"a/  ",
	"/",
	"///",
	"a/\u0001b",
	"a/\u007fb",
];

describe("project library path validation", () => {
	it("rejects traversal, empty, control, and ambiguous paths without any request", async () => {
		for (const path of TRAVERSAL_PATHS) {
			const { client, requests, } = recordingClient(() => undefined);
			const resource = new ProjectLibraryResource(client,);
			expect(() => validateLibraryPath(path,), `validateLibraryPath(${JSON.stringify(path,)}`,)
				.toThrow(
					ClientValidationError,
				);
			for (
				const action of ["getFile", "getFileBytes", "addFile", "addFolder", "deleteFile",] as const
			) {
				// biome-ignore lint/suspicious/noExplicitAny: exercising one validator across every path-taking method
				await expect(resource[action](path,),).rejects.toBeInstanceOf(ClientValidationError,);
			}
			expect(requests, `no request for ${JSON.stringify(path,)}`,).toEqual([],);
		}
	});

	it("normalizes a leading slash and encodes segments", () => {
		expect(validateLibraryPath("/python/mylib/utils.py",),).toBe("python/mylib/utils.py",);
		expect(encodeLibraryPath("python scripts/raw data.bin",),).toBe(
			"python%20scripts/raw%20data.bin",
		);
	});

	it("rejects invalid rename names before any request", async () => {
		for (const name of ["", ".", "..", "a/b", "a\\b", "a.", "a ", " a", "\u0001a", "  ",]) {
			const { client, requests, } = recordingClient(() => undefined);
			const resource = new ProjectLibraryResource(client,);
			await expect(resource.rename("python/old.py", name,),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
			expect(requests, `no request for rename target ${JSON.stringify(name,)}`,).toEqual([],);
			expect(() => validateLibraryName(name,)).toThrow(ClientValidationError,);
		}
	});

	it("accepts single-segment rename targets with dots and spaces inside", () => {
		expect(validateLibraryName("my.file.v2",),).toBe("my.file.v2",);
		expect(validateLibraryName("my file",),).toBe("my file",);
	});

	it("normalizes move destinations and rejects traversals", () => {
		expect(validateLibraryDestinationPath("/",),).toBe("/",);
		expect(validateLibraryDestinationPath("python/mylib",),).toBe("/python/mylib",);
		expect(() => validateLibraryDestinationPath("../escape",)).toThrow(ClientValidationError,);
		expect(() => validateLibraryDestinationPath("a/..",)).toThrow(ClientValidationError,);
	});
});

describe("ProjectLibraryResource reads", () => {
	it("lists the project library contents tree", async () => {
		const contents: ProjectLibraryItem[] = [
			{
				name: "python",
				path: "python",
				children: [
					{
						name: "utils.py",
						path: "python/utils.py",
						size: 42,
						mimeType: "text/x-python",
						hasData: true,
						lastModified: 1755000000000,
					},
				],
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
});

describe("ProjectLibraryResource create overwrite guard", () => {
	it("never posts when the file already exists", async () => {
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			sendJson(res, { data: "existing", },);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFile("python/exists.py",),).rejects.toThrow(/already exists/,);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET",],);
		expect(requests[0]!.url,).toBe(
			"/public/api/projects/TEST/libraries/contents/python/exists.py",
		);
	});

	it("posts only after the file existence probe reports absence", async () => {
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			if (req.method === "GET") {
				res.statusCode = 404;
				res.end();
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFile("python/new.py",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{ method: "GET", url: "/public/api/projects/TEST/libraries/contents/python/new.py", },
			{ method: "POST", url: "/public/api/projects/TEST/libraries/contents/python/new.py", },
		],);
	});

	it("never posts when the folder already exists in the tree", async () => {
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			sendJson(res, [{ name: "python", children: [{ name: "mylib", },], },],);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFolder("python/mylib",),).rejects.toThrow(/already exists/,);
			await expect(resource.hasLibraryItem("python/mylib",),).resolves.toBe(true,);
			await expect(resource.hasLibraryItem("python/absent.py",),).resolves.toBe(false,);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET", "GET", "GET",],);
		expect(
			requests.every((request,) => request.url === "/public/api/projects/TEST/libraries/contents"),
		).toBe(true,);
	});

	it("posts the folder create when the tree reports absence", async () => {
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			sendJson(res, [{ name: "python", children: [], },],);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFolder("python/package",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{ method: "GET", url: "/public/api/projects/TEST/libraries/contents", },
			{ method: "POST", url: "/public/api/projects/TEST/libraries/folders/python/package", },
		],);
	});
});

describe("ProjectLibraryResource binary put round-trip", () => {
	it("sends exact bytes for binary content and reports digest and count", async () => {
		const binary = Uint8Array.from([0x00, 0x01, 0x1e, 0x7f, 0x80, 0xfe, 0xff, 0x00, 0x0a, 0xff,],);
		let observedBody = Buffer.alloc(0,);
		let observedUrl = "";

		await withServer(async (req, res,) => {
			observedUrl = req.url ?? "";
			const chunks: Buffer[] = [];
			for await (const chunk of req) {
				chunks.push(chunk as Buffer,);
			}
			observedBody = Buffer.concat(chunks,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			const result = await resource.addOrUpdateFile("static/blob.bin", binary,);
			expect(result.bytes,).toBe(binary.length,);
			expect(result.sha256,).toBe(
				new Bun.CryptoHasher("sha256",).update(binary,).digest("hex",),
			);
		},);

		expect(observedUrl,).toBe("/public/api/projects/TEST/libraries/contents/static/blob.bin",);
		expect(observedBody.equals(Buffer.from(binary,),),).toBe(true,);
	});

	it("keeps text content byte-identical over the wire", async () => {
		const text = 'line 1\n"not json"\n';
		let observedBody = "";

		await withServer(async (req, res,) => {
			observedBody = await readBody(req,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			const result = await resource.addOrUpdateFile("python/lib file.py", text,);
			expect(result.bytes,).toBe(Buffer.byteLength(text, "utf8",),);
			expect(result.sha256,).toBe(
				new Bun.CryptoHasher("sha256",).update(Buffer.from(text, "utf8",),).digest("hex",),
			);
		},);

		expect(observedBody,).toBe(text,);
	});
});

describe("ProjectLibraryResource expectSha256 precondition", () => {
	it("refuses to write when the remote hash differs and posts nothing", async () => {
		const remoteBytes = Buffer.from("remote content", "utf8",);
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			sendJson(res, { data: remoteBytes.toString("base64",), },);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			let error: unknown;
			try {
				await resource.addOrUpdateFile("python/a.py", "local content", undefined, {
					expectSha256: "0".repeat(64,),
				},);
			} catch (caught) {
				error = caught;
			}
			const validation = assertClientValidationError(error,);
			expect(validation.code,).toBe("assertion_failed",);
			expect(validation.details,).toMatchObject({
				path: "python/a.py",
				expectedSha256: "0".repeat(64,),
				actualSha256: new Bun.CryptoHasher("sha256",).update(remoteBytes,).digest("hex",),
			},);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET",],);
	});

	it("writes when the remote hash matches and reports beforeSha256", async () => {
		const remoteBytes = Buffer.from("same", "utf8",);
		const expectedHash = new Bun.CryptoHasher("sha256",).update(remoteBytes,).digest("hex",);
		const requests: Array<{ method: string; url: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", url: req.url ?? "", },);
			if (req.method === "GET") {
				sendJson(res, { data: remoteBytes.toString("base64",), },);
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(
				resource.addOrUpdateFile("python/a.py", "same", undefined, {
					expectSha256: expectedHash.toUpperCase(),
				},),
			).resolves.toMatchObject({
				path: "python/a.py",
				bytes: 4,
				sha256: expectedHash,
				beforeSha256: expectedHash,
			},);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET", "POST",],);
	});

	it("fails the precondition when the remote file is absent", async () => {
		const requests: Array<{ method: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", },);
			res.statusCode = 404;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			let error: unknown;
			try {
				await resource.addOrUpdateFile("python/gone.py", "content", undefined, {
					expectSha256: "a".repeat(64,),
				},);
			} catch (caught) {
				error = caught;
			}
			const validation = assertClientValidationError(error,);
			expect(validation.code,).toBe("assertion_failed",);
			expect(validation.details,).toMatchObject({ actualSha256: null, },);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET",],);
	});

	it("rejects a malformed precondition hash before any request", async () => {
		const { client, requests, } = recordingClient(() => undefined);
		const resource = new ProjectLibraryResource(client,);
		await expect(
			resource.addOrUpdateFile("python/a.py", "content", undefined, { expectSha256: "nothex", },),
		).rejects.toBeInstanceOf(ClientValidationError,);
		expect(requests,).toEqual([],);
	});
});

function bytesHandler(bytes: Uint8Array,): (req: RecordedRequest,) => unknown {
	return (req,) => {
		if (req.url.includes("dataEncoding=base64",)) {
			return { data: Buffer.from(bytes,).toString("base64",), };
		}
		return { data: Buffer.from(bytes,).toString("utf8",), };
	};
}

describe("ProjectLibraryResource diffFile", () => {
	it("renders a unified diff with counts for changed text", async () => {
		const { client, requests, } = recordingClient(bytesHandler(Buffer.from("a\nb\nc\n", "utf8",),),);
		const resource = new ProjectLibraryResource(client,);
		const result = await resource.diffFile("python/a.py", "line 1\nline 2\nline 3\n",);
		expect(result.unchanged,).toBe(false,);
		expect(result.binary,).toBeUndefined();
		expect(result.added,).toBe(3,);
		expect(result.removed,).toBe(3,);
		expect(result.diff,).toContain("--- a/python/a.py",);
		expect(result.diff,).toContain("+++ b/python/a.py",);
		// Disjoint files share only the trailing '' line from the final newline,
		// so the single hunk covers 4 lines on both sides.
		expect(result.diff,).toContain("@@ -1,4 +1,4 @@",);
		expect(result.diff,).toContain("-a\n-b\n-c",);
		expect(result.diff,).toContain("+line 1\n+line 2\n+line 3",);
		expect(result.diffTruncated,).toBe(false,);
		expect(result.remoteSha256,).toBe(
			new Bun.CryptoHasher("sha256",).update(Buffer.from("a\nb\nc\n", "utf8",),).digest("hex",),
		);
		expect(result.localSha256,).toBe(
			new Bun.CryptoHasher("sha256",).update(
				Buffer.from("line 1\nline 2\nline 3\n", "utf8",),
			).digest("hex",),
		);
		expect(requests,).toHaveLength(1,);
	});

	it("reports byte-identical content as unchanged without a diff body", async () => {
		const { client, } = recordingClient(bytesHandler(Buffer.from("same\n", "utf8",),),);
		const resource = new ProjectLibraryResource(client,);
		const result = await resource.diffFile("python/a.py", "same\n",);
		expect(result.unchanged,).toBe(true,);
		expect(result.diff,).toBe("",);
		expect(result.added,).toBe(0,);
		expect(result.removed,).toBe(0,);
	});

	it("reports binary content without dumping bytes", async () => {
		const binary = Uint8Array.from([0x00, 0x01, 0xff, 0xfe,],);
		const { client, } = recordingClient(bytesHandler(binary,),);
		const resource = new ProjectLibraryResource(client,);
		const result = await resource.diffFile("static/logo.png", "text content",);
		expect(result.binary,).toBe(true,);
		expect(result.unchanged,).toBe(false,);
		expect(result.diff,).toBe("",);
		expect(result.remoteBytes,).toBe(binary.length,);
	});

	it("caps the diff text at maxLines and reports truncation", async () => {
		const remote = Array.from({ length: 30, }, (_v, i,) => `old ${String(i,)}`,).join("\n",);
		const local = Array.from({ length: 30, }, (_v, i,) => `new ${String(i,)}`,).join("\n",);
		const { client, } = recordingClient(bytesHandler(Buffer.from(remote, "utf8",),),);
		const resource = new ProjectLibraryResource(client,);
		const result = await resource.diffFile("python/a.py", local, undefined, { maxLines: 10, },);
		expect(result.diffTruncated,).toBe(true,);
		expect(result.diff.split("\n",).length,).toBeLessThanOrEqual(11,);
		expect(result.diff,).toContain("…",);
		expect(result.added,).toBe(30,);
		expect(result.removed,).toBe(30,);
	});

	it("reports remote absence as an all-added diff", async () => {
		const { client, } = recordingClient(() => undefined);
		const resource = new ProjectLibraryResource(client,);
		const result = await resource.diffFile("python/absent.py", "one\ntwo\n",);
		expect(result.remoteAbsent,).toBe(true,);
		expect(result.unchanged,).toBe(false,);
		expect(result.added,).toBe(2,);
		expect(result.removed,).toBe(0,);
		expect(result.diff,).toContain("+one",);
		expect(result.diff,).toContain("+two",);
	});
});

describe("ProjectLibraryResource rename and move validation", () => {
	it("rejects multi-segment rename names before any request", async () => {
		const { client, requests, } = recordingClient(() => undefined);
		const resource = new ProjectLibraryResource(client,);
		await expect(resource.rename("python/old.py", "sub/new.py",),).rejects.toThrow(
			/single segment/,
		);
		expect(requests,).toEqual([],);
	});

	it("posts rename and move with validated bodies", async () => {
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
			await expect(resource.move("python/new.py", "/",),).resolves.toBeUndefined();
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
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/contents-actions/move",
				body: { oldPath: "/python/new.py", newPath: "/", },
			},
		],);
	});
});

describe("ProjectLibraryResource legacy behavior", () => {
	it("creates empty files and folders and deletes contents when absent", async () => {
		const requests: Array<{ method: string; url: string; body: string; }> = [];

		await withServer(async (req, res,) => {
			requests.push({
				method: req.method ?? "",
				url: req.url ?? "",
				body: await readBody(req,),
			},);
			if (req.method === "GET") {
				res.statusCode = 404;
				res.end();
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new ProjectLibraryResource(createClient(url,),);
			await expect(resource.addFile("python/new.py",),).resolves.toBeUndefined();
			await expect(resource.deleteFile("python/old.py",),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([
			{
				method: "GET",
				url: "/public/api/projects/TEST/libraries/contents/python/new.py",
				body: "",
			},
			{
				method: "POST",
				url: "/public/api/projects/TEST/libraries/contents/python/new.py",
				body: "",
			},
			{
				method: "DELETE",
				url: "/public/api/projects/TEST/libraries/contents/python/old.py",
				body: "",
			},
		],);
	});

	it("propagates non-404 existence-probe failures instead of creating", async () => {
		const requests: Array<{ method: string; }> = [];

		await withServer((req, res,) => {
			requests.push({ method: req.method ?? "", },);
			sendJson(res, { message: "boom", }, 500,);
		}, async (url,) => {
			const resource = new ProjectLibraryResource(
				new DataikuClient({
					url,
					apiKey: "test-key",
					projectKey: "TEST",
					retryMaxAttempts: 1,
					requestTimeoutMs: 2000,
				},),
			);
			await expect(resource.addFile("python/new.py",),).rejects.toBeInstanceOf(DataikuError,);
		},);

		expect(requests.map((request,) => request.method),).toEqual(["GET",],);
	});
});

describe("ProjectLibraryItem observed DSS fields", () => {
	it("types carry the observed DSS listing fields", () => {
		const item: ProjectLibraryItem = {
			name: "utils.py",
			path: "python/utils.py",
			size: 42,
			mimeType: "text/x-python",
			hasData: true,
			lastModified: 1755000000000,
		};
		expect(item.path,).toBe("python/utils.py",);
		expect(item.size,).toBe(42,);
		expect(item.mimeType,).toBe("text/x-python",);
		expect(item.hasData,).toBe(true,);
		expect(item.lastModified,).toBe(1755000000000,);
	});
});
