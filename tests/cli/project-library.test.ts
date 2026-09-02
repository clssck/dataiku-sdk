import { describe, expect, it, } from "bun:test";
import { commands, } from "../../src/cli/commands/index.js";
import { projectLibraryCommands, } from "../../src/cli/commands/project-library.js";
import { KNOWN_LONG_FLAGS, } from "../../src/cli/flags.js";
import { RESOURCE_NAMES, } from "../../src/cli/usage.js";
import type { DataikuClient, } from "../../src/client.js";
import { ClientValidationError, } from "../../src/errors.js";
import { cliEnv, dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

const ACTIONS = [
	"list",
	"get",
	"get-bytes",
	"create-file",
	"create-folder",
	"put",
	"diff",
	"delete",
	"rename",
	"move",
];

interface RecordedCall {
	method: string;
	args: unknown[];
}

/**
 * A client whose `projectLibrary` resource records every call instead of
 * issuing HTTP, so each test asserts the exact method and arguments the CLI
 * forwards.
 */
function recordingClient(
	results: Record<string, unknown> = {},
): { client: DataikuClient; calls: RecordedCall[]; } {
	const calls: RecordedCall[] = [];
	const projectLibrary = new Proxy({}, {
		get: (_target, method: string,) => async (...args: unknown[]): Promise<unknown> => {
			calls.push({ method, args, },);
			if (method in results && results[method] instanceof Error) throw results[method];
			return Promise.resolve(method in results ? results[method] : {},);
		},
	},);
	return {
		client: {
			projectLibrary,
			resolveProjectKey: (pk?: string,) => pk ?? "P",
		} as unknown as DataikuClient,
		calls,
	};
}

async function run(
	action: string,
	client: DataikuClient,
	args: string[] = [],
	flags: Record<string, string | boolean> = {},
): Promise<unknown> {
	return await projectLibraryCommands[action]!.handler(client, args, flags,);
}

describe("project-library CLI registration", () => {
	it("registers the resource and every action", () => {
		expect(RESOURCE_NAMES,).toContain("project-library",);
		expect(commands["project-library"],).toBe(projectLibraryCommands,);
		expect(Object.keys(projectLibraryCommands,).sort(),).toEqual([...ACTIONS,].sort(),);
	});

	it("declares only flags the parser accepts", () => {
		const unknownFlags: string[] = [];
		for (const meta of Object.values(projectLibraryCommands,)) {
			for (const match of meta.usage.matchAll(/--([a-z][a-z0-9-]*)/g,)) {
				if (!KNOWN_LONG_FLAGS.has(match[1]!,)) unknownFlags.push(match[1]!,);
			}
		}
		expect(unknownFlags,).toEqual([],);
	});

	it("keeps put --file binary-safe by never decoding the file as text", async () => {
		const binary = Uint8Array.from([0x00, 0x01, 0xff, 0xfe,],);
		const tmpFile = `/tmp/dss-bsafe-${String(Math.random(),).slice(2,)}.bin`;
		await Bun.write(tmpFile, binary,);
		try {
			const calls: Array<{ path: string; content: unknown; }> = [];
			const client = {
				projectLibrary: {
					addOrUpdateFile: async (path: string, content: unknown,) => {
						calls.push({ path, content, },);
						return { path, bytes: binary.length, sha256: "a".repeat(64,), };
					},
				},
			} as unknown as DataikuClient;
			await run("put", client, ["python/lib.py",], { file: tmpFile, "project-key": "P", },);
			expect(calls[0]!.path,).toBe("python/lib.py",);
			const content = calls[0]!.content as Uint8Array;
			expect(content,).toBeInstanceOf(Uint8Array,);
			expect(Array.from(content,),).toEqual(Array.from(binary,),);
		} finally {
			await Bun.write(tmpFile, "",);
		}
	});
});

describe("project-library path validation before endpoint construction", () => {
	it("rejects traversal and ambiguous paths without contacting the library", async () => {
		const badPaths = [
			"../escape.py",
			"a/../../escape.py",
			"a/..",
			"..",
			".",
			"",
			"a//b",
			"a\\b",
			"a/b.",
			"a/b ",
			"/a//b/",
			"a/\u0001b",
		];
		for (const badPath of badPaths) {
			for (
				const action of ["get", "create-file", "create-folder", "put", "delete", "diff",] as const
			) {
				const { client, calls, } = recordingClient();
				await expect(
					run(action, client, [badPath,], { "project-key": "P", },),
				).rejects.toBeInstanceOf(ClientValidationError,);
				expect(
					calls,
					`no request for ${action} ${JSON.stringify(badPath,)}`,
				).toEqual([],);
			}
		}
	});

	it("rejects traversal on dry-run plans too", async () => {
		const { client, calls, } = recordingClient();
		await expect(
			run("create-file", client, ["../escape.py",], { "dry-run": true, "project-key": "P", },),
		).rejects.toBeInstanceOf(ClientValidationError,);
		expect(calls,).toEqual([],);
	});

	it("rejects invalid rename and move targets before any request", async () => {
		const { client, calls, } = recordingClient();
		await expect(run("rename", client, ["python/old.py", "sub/new.py",], { "project-key": "P", },),)
			.rejects.toThrow(/single segment/,);
		await expect(run("rename", client, ["python/old.py", "..",], { "project-key": "P", },),)
			.rejects.toBeInstanceOf(ClientValidationError,);
		await expect(run("move", client, ["python/old.py", "../escape",], { "project-key": "P", },),)
			.rejects.toBeInstanceOf(ClientValidationError,);
		expect(calls,).toEqual([],);
	});
});

describe("project-library create overwrite guard", () => {
	it("skips with --if-not-exists when the item exists and never posts", async () => {
		const { client, calls, } = recordingClient({ hasLibraryItem: true, },);
		const result = await run("create-file", client, ["python/exists.py",], {
			"if-not-exists": true,
			"project-key": "P",
		},);
		expect(result,).toMatchObject({ skipped: "python/exists.py", reason: "exists", },);
		expect(calls.map((call,) => call.method),).toEqual(["hasLibraryItem",],);
	});

	it("creates when the item is absent with --if-not-exists", async () => {
		const { client, calls, } = recordingClient({ hasLibraryItem: false, },);
		const result = await run("create-file", client, ["python/new.py",], {
			"if-not-exists": true,
			"project-key": "P",
		},);
		expect(result,).toEqual({ created: "python/new.py", },);
		expect(calls.map((call,) => call.method),).toEqual(["hasLibraryItem", "addFile",],);
	});

	it("surfaces the resource already-exists error without --if-not-exists", async () => {
		const { client, calls, } = recordingClient({
			addFile: new ClientValidationError(
				'Project library item "python/exists.py" already exists.',
				"validation_failed",
			),
		},);
		await expect(
			run("create-file", client, ["python/exists.py",], { "project-key": "P", },),
		).rejects.toThrow(/already exists/,);
		expect(calls.map((call,) => call.method),).toEqual(["addFile",],);
	});

	it("skips create-folder with --if-not-exists for an existing folder", async () => {
		const { client, calls, } = recordingClient({ hasLibraryItem: true, },);
		const result = await run("create-folder", client, ["python/mylib",], {
			"if-not-exists": true,
			"project-key": "P",
		},);
		expect(result,).toMatchObject({ skipped: "python/mylib", reason: "exists", kind: "folder", },);
		expect(calls.map((call,) => call.method),).toEqual(["hasLibraryItem",],);
	});
});

describe("project-library put", () => {
	it("reports bytes and sha256 from the write result", async () => {
		const { client, calls, } = recordingClient({
			addOrUpdateFile: { path: "python/a.py", bytes: 11, sha256: "b".repeat(64,), },
		},);
		const result = await run("put", client, ["python/a.py",], {
			content: "hello world",
			"project-key": "P",
		},);
		expect(result,).toEqual({
			updated: "python/a.py",
			bytes: 11,
			sha256: "b".repeat(64,),
		},);
		expect(calls[0]!.method,).toBe("addOrUpdateFile",);
		expect(calls[0]!.args,).toEqual(["python/a.py", "hello world", "P", undefined,],);
	});

	it("passes --expect-sha256 through to the resource precondition", async () => {
		const { client, calls, } = recordingClient({
			addOrUpdateFile: {
				path: "python/a.py",
				bytes: 4,
				sha256: "c".repeat(64,),
				beforeSha256: "d".repeat(64,),
			},
		},);
		const result = await run("put", client, ["python/a.py",], {
			content: "test",
			"expect-sha256": "d".repeat(64,),
			"project-key": "P",
		},);
		expect(result,).toMatchObject({ beforeSha256: "d".repeat(64,), },);
		expect(calls[0]!.args[3],).toEqual({ expectSha256: "d".repeat(64,), },);
	});

	it("rejects a malformed --expect-sha256 before any request", async () => {
		const { client, calls, } = recordingClient();
		await expect(
			run("put", client, ["python/a.py",], {
				content: "test",
				"expect-sha256": "xyz",
				"project-key": "P",
			},),
		).rejects.toThrow("--expect-sha256 must be a 64-character SHA-256 hex digest.",);
		expect(calls,).toEqual([],);
	});

	it("dry-run plans carry source metadata without content bytes", async () => {
		const { client, calls, } = recordingClient();
		const result = await run("put", client, ["python/a.py",], {
			content: "secret body",
			"dry-run": true,
			"project-key": "P",
		},);
		expect(result,).toMatchObject({
			plan: true,
			resource: "project-library",
			action: "put",
			payload: { contentSource: "flag", bytes: 11, sha256: expect.any(String,), },
		},);
		expect(JSON.stringify(result,),).not.toContain("secret body",);
		expect(calls,).toEqual([],);
	});

	it("dry-run --file plans report the file's size and hash without reading stdin", async () => {
		const { client, calls, } = recordingClient();
		const tmpFile = `/tmp/dss-pl-test-${String(Math.random(),).slice(2,)}.txt`;
		await Bun.write(tmpFile, "file body\n",);
		try {
			const result = await run("put", client, ["python/a.py",], {
				file: tmpFile,
				"dry-run": true,
				"project-key": "P",
			},);
			expect(result,).toMatchObject({
				payload: { contentSource: "file", file: tmpFile, bytes: 10, sha256: expect.any(String,), },
			},);
			expect(JSON.stringify(result,),).not.toContain("file body",);
			expect(calls,).toEqual([],);
		} finally {
			await Bun.write(tmpFile, "",);
		}
	});
});

describe("project-library diff", () => {
	it("forwards local text and max-lines to diffFile", async () => {
		const diffResult = {
			path: "python/a.py",
			unchanged: false,
			added: 1,
			removed: 1,
			diff: "@@ -1 +1 @@\n-old\n+new\n",
			diffTruncated: false,
			localSha256: "e".repeat(64,),
			localBytes: 8,
			maxLines: 50,
		};
		const { client, calls, } = recordingClient({ diffFile: diffResult, },);
		const result = await run("diff", client, ["python/a.py",], {
			content: "new\n",
			"max-lines": "50",
			"project-key": "P",
		},);
		expect(result,).toMatchObject({ unchanged: false, added: 1, removed: 1, },);
		expect(calls[0]!.method,).toBe("diffFile",);
		expect(calls[0]!.args[0],).toBe("python/a.py",);
		expect(calls[0]!.args[1],).toBe("new\n",);
		expect(calls[0]!.args[3],).toEqual({ maxLines: 50, },);
	});
});

describe("project-library CLI end-to-end against a fake DSS", () => {
	it("rejects a traversal path with exit code 1 and no request", async () => {
		let sawRequest = false;
		await withCliServer((req, res,) => {
			sawRequest = true;
			sendJson(res, { unexpected: true, },);
		}, async (url,) => {
			const failure = await dssFailure(
				["project-library", "put", "../../etc/passwd", "--content", "x", "--project-key", "TEST",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report.ok,).toBe(false,);
			expect(String(report.error,),).toContain("must not contain '.' or '..'",);
		},);
		expect(sawRequest,).toBe(false,);
	});

	it("exits 4 with assertion_failed when --expect-sha256 mismatches", async () => {
		const remoteHash = new Bun.CryptoHasher("sha256",).update(Buffer.from("remote", "utf8",),).digest(
			"hex",
		);
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			if (req.method === "GET") {
				sendJson(res, { data: Buffer.from("remote", "utf8",).toString("base64",), },);
				return;
			}
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"project-library",
					"put",
					"python/a.py",
					"--content",
					"local",
					"--expect-sha256",
					"0".repeat(64,),
					"--project-key",
					"TEST",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stdout,).toContain("assertion_failed",);
		},);
		// Only the precondition read happened; no write request reached DSS.
		expect(requests.map((request,) => request.split(" ",)[0]),).toEqual(["GET",],);
		expect(remoteHash,).not.toBe("0".repeat(64,),);
	});

	it("puts binary file bytes end-to-end and reports the digest", async () => {
		const binary = Uint8Array.from([0x00, 0x01, 0xff, 0xfe, 0x00,],);
		const tmpFile = `/tmp/dss-bin-${String(Math.random(),).slice(2,)}.bin`;
		await Bun.write(tmpFile, binary,);
		let observedBody = Buffer.alloc(0,);
		try {
			await withCliServer(async (req, res,) => {
				const chunks: Buffer[] = [];
				for await (const chunk of req) chunks.push(chunk as Buffer,);
				observedBody = Buffer.concat(chunks,);
				res.statusCode = 204;
				res.end();
			}, async (url,) => {
				const { stdout, } = await dss(
					[
						"project-library",
						"put",
						"static/blob.bin",
						"--file",
						tmpFile,
						"--project-key",
						"TEST",
					],
					{ env: cliEnv(url,), },
				);
				const result = JSON.parse(stdout,) as Record<string, unknown>;
				expect(result.bytes,).toBe(binary.length,);
				expect(result.sha256,).toBe(
					new Bun.CryptoHasher("sha256",).update(binary,).digest("hex",),
				);
			},);
			expect(observedBody.equals(Buffer.from(binary,),),).toBe(true,);
		} finally {
			await Bun.write(tmpFile, "",);
		}
	});

	it("skips create-file with --if-not-exists on an existing item without posting", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, [{ name: "python", children: [{ name: "exists.py", },], },],);
		}, async (url,) => {
			const { stdout, } = await dss(
				[
					"project-library",
					"create-file",
					"python/exists.py",
					"--if-not-exists",
					"--project-key",
					"TEST",
				],
				{ env: cliEnv(url,), },
			);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.skipped,).toBe("python/exists.py",);
		},);
		expect(requests.map((request,) => request.split(" ",)[0]),).toEqual(["GET",],);
	});
});
