import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import { ProjectGitResource, } from "../src/resources/project-git.js";

interface RecordedRequest {
	method: string;
	url: string;
	headers: Record<string, string | string[] | undefined>;
	body: string;
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string, records: RecordedRequest[],) => Promise<void>,
): Promise<void> {
	const records: RecordedRequest[] = [];
	const server = createServer((req, res,) => {
		void (async (): Promise<void> => {
			const chunks: Buffer[] = [];
			for await (const chunk of req) {
				chunks.push(Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,),);
			}
			records.push({
				method: req.method ?? "",
				url: req.url ?? "",
				headers: req.headers,
				body: Buffer.concat(chunks,).toString("utf8",),
			},);
			try {
				await handler(req, res,);
			} catch (error: unknown) {
				res.statusCode = 500;
				res.end(error instanceof Error ? error.message : String(error,),);
			}
		})();
	},);

	await new Promise<void>((resolvePromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				server.close();
				return;
			}
			resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
	try {
		await run(url, records,);
	} finally {
		await new Promise<void>((resolvePromise,) => {
			server.close(() => resolvePromise());
		},);
	}
}

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
	},);
}

function assertBasicAuth(request: RecordedRequest,): void {
	const authorization = request.headers.authorization;
	expect(typeof authorization,).toBe("string",);
	const auth = authorization as string;
	expect(auth.startsWith("Basic ",),).toBe(true,);
	const decoded = Buffer.from(auth.slice(6,), "base64",).toString("utf8",);
	expect(decoded,).toBe("test-key:",);
}

function last(records: RecordedRequest[],): RecordedRequest {
	const record = records[records.length - 1];
	expect(record,).toBeDefined();
	return record!;
}

const API = "/dip/publicapi";

describe("ProjectGitResource remotes", () => {
	it("reads status with Basic auth and an exact encoded path", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { currentBranch: "master", clean: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const status = await git.status("A B/C#1",);
				expect(status.currentBranch,).toBe("master",);
				const request = last(records,);
				expect(request.method,).toBe("GET",);
				expect(request.url,).toBe("/dip/publicapi/projects/A%20B%2FC%231/git/status",);
				assertBasicAuth(request,);
			},
		);
	});

	it("reads origin remote by default and a named one on request", async () => {
		let requestedPath = "";
		await withServer(
			(req, res,) => {
				requestedPath = req.url ?? "";
				sendJson(
					res,
					req.url?.includes("origin",)
						? { url: "ssh://git@host/a.git", }
						: { url: "ssh://git@host/b.git", },
				);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.getRemote("PROJ",),).resolves.toEqual({ url: "ssh://git@host/a.git", },);
				expect(requestedPath,).toBe(
					"/dip/publicapi/projects/PROJ/git/remotes/origin",
				);
				await expect(git.getRemote("PROJ", "upstream",),).resolves.toEqual({
					url: "ssh://git@host/b.git",
				},);
				expect(requestedPath,).toBe(
					"/dip/publicapi/projects/PROJ/git/remotes/upstream",
				);
			},
		);
	});

	it("returns an empty remote object when DSS reports none", async () => {
		await withServer(
			(_req, res,) => sendJson(res, {},),
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.getRemote("PROJ",),).resolves.toEqual({},);
			},
		);
	});

	it("sets and removes remotes with matching method, path and body", async () => {
		await withServer(
			(req, res,) => {
				sendJson(res, { success: true, },);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.setRemote("PROJ", {
					url: "ssh://git@host:22/org/repo.git",
					name: "upstream",
				},);
				expect(result,).toEqual({ success: true, },);
				expect(last(records,).method,).toBe("POST",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/remotes/upstream",
				);
				expect(JSON.parse(last(records,).body,),).toEqual({
					url: "ssh://git@host:22/org/repo.git",
				},);

				await git.setRemote("PROJ", { url: "ssh://git@host/org/repo.git", },);
				expect(last(records,).url,).toContain("/remotes/origin",);

				await git.removeRemote("PROJ",);
				expect(last(records,).method,).toBe("DELETE",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/remotes/origin",
				);
				await git.removeRemote("PROJ", "upstream",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/remotes/upstream",
				);
			},
		);
	});

	it("rejects HTTP(S) remotes with embedded credentials and allows SSH/scp forms", async () => {
		let requests = 0;
		await withServer(
			(req, res,) => {
				requests += 1;
				sendJson(res, { success: true, },);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(
					git.setRemote("PROJ", { url: "https://user:token@github.com/a/b.git", },),
				).rejects.toThrow("must not embed credentials",);
				await expect(
					git.setRemote("PROJ", { url: "https://user@github.com/a/b.git", },),
				).rejects.toThrow("must not embed credentials",);
				await expect(
					git.setRemote("PROJ", { url: "https://[invalid", },),
				).rejects.toThrow("must be a valid HTTP(S) URL",);
				await expect(
					git.setRemote("PROJ", { url: "https:/user:token@github.com/a/b.git", },),
				).rejects.toThrow("must be a valid HTTP(S) URL",);
				await expect(
					git.setRemote("PROJ", { url: "https://github.com\\org/repo.git", },),
				).rejects.toThrow("must be a valid HTTP(S) URL",);
				await expect(
					git.setRemote("PROJ", { url: "https://github.com/\trepo.git", },),
				).rejects.toThrow("must not contain control characters",);
				expect(requests,).toBe(0,);

				await expect(
					git.setRemote("PROJ", { url: "git@github.com:org/repo.git", },),
				).resolves.toEqual({ success: true, },);
				await expect(
					git.setRemote("PROJ", { url: "ssh://git@github.com/org/repo.git", },),
				).resolves.toEqual({ success: true, },);
				await expect(
					git.setRemote("PROJ", { url: " https://github.com/org/repo.git ", },),
				).resolves.toEqual({ success: true, },);
				expect(JSON.parse(last(records,).body,),).toEqual({
					url: "https://github.com/org/repo.git",
				},);
				expect(requests,).toBe(3,);
			},
		);
	});
});

describe("ProjectGitResource branches", () => {
	it("lists local branches by default and remote branches on demand", async () => {
		await withServer(
			(req, res,) => {
				sendJson(res, req.url?.includes("remote=true",) ? ["origin/master",] : ["master",],);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.listBranches("PROJ",),).resolves.toEqual(["master",],);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/branches?remote=false",
				);
				await expect(git.listBranches("PROJ", true,),).resolves.toEqual(["origin/master",],);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/branches?remote=true",
				);
			},
		);
	});

	it("creates a branch with the full DSS body shape, nulls for absent fields", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.createBranch("PROJ", "feat/x",);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe("/dip/publicapi/projects/PROJ/git/branches/",);
				expect(JSON.parse(request.body,),).toEqual({
					name: "feat/x",
					commit: null,
					duplicateProject: false,
					targetProjectKey: null,
					targetProjectFolderId: null,
				},);

				await git.createBranch("PROJ", "feat/y", {
					commit: "abc123",
					duplicateProject: true,
					targetProjectKey: "OTHER",
					targetProjectFolderId: "f1",
				},);
				expect(JSON.parse(last(records,).body,),).toEqual({
					name: "feat/y",
					commit: "abc123",
					duplicateProject: true,
					targetProjectKey: "OTHER",
					targetProjectFolderId: "f1",
				},);
			},
		);
	});

	it("deletes a branch with exact body and passthroughs {success:false}", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: false, logs: ["failed",], },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.deleteBranch("PROJ", "feat/x", { forceDelete: true, },);
				expect(result,).toEqual({ success: false, logs: ["failed",], },);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe("/dip/publicapi/projects/PROJ/git/actions/deleteBranch",);
				expect(JSON.parse(request.body,),).toEqual({
					name: "feat/x",
					remote: false,
					deleteRemotely: false,
					forceDelete: true,
				},);
			},
		);
	});

	it("reads the current branch and reports null when DSS has none", async () => {
		let call = 0;
		await withServer(
			(_req, res,) => {
				call += 1;
				sendJson(res, call === 1 ? { name: "dev", } : {},);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.currentBranch("PROJ",),).resolves.toBe("dev",);
				await expect(git.currentBranch("PROJ",),).resolves.toBeNull();
			},
		);
	});

	it("switches branch with an encoded branchName query param", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.switchBranch("PROJ", "feat/x",);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/switchBranch?branchName=feat%2Fx",
				);
			},
		);
	});
});

describe("ProjectGitResource tags", () => {
	it("lists, creates and deletes tags with the exact DSS shapes", async () => {
		await withServer(
			(req, res,) => {
				if (req.url?.endsWith("/git/tags",)) {
					sendJson(res, [{ name: "v1", commit: "abc", },],);
				} else {
					sendJson(res, { success: true, },);
				}
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.listTags("PROJ",),).resolves.toEqual([
					{ name: "v1", commit: "abc", },
				],);
				expect(last(records,).url,).toBe("/dip/publicapi/projects/PROJ/git/tags",);

				await git.createTag("PROJ", "v2",);
				expect(JSON.parse(last(records,).body,),).toEqual({
					name: "v2",
					reference: "HEAD",
					message: "",
				},);
				expect(last(records,).url,).toBe("/dip/publicapi/projects/PROJ/git/tags/",);

				await git.createTag("PROJ", "v3", { reference: "abc123", message: "release", },);
				expect(JSON.parse(last(records,).body,),).toEqual({
					name: "v3",
					reference: "abc123",
					message: "release",
				},);

				await git.deleteTag("PROJ", "v2",);
				expect(last(records,).method,).toBe("POST",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/deleteTag",
				);
				expect(JSON.parse(last(records,).body,),).toEqual({ name: "v2", },);
			},
		);
	});
});

describe("ProjectGitResource sync actions", () => {
	it("fetches and returns a false-success payload verbatim", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: false, output: "cannot fetch", },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.fetch("PROJ",);
				expect(result,).toEqual({ success: false, output: "cannot fetch", },);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe("/dip/publicapi/projects/PROJ/git/actions/fetch",);
				expect(request.body,).toBe("",);
			},
		);
	});

	it("pulls and pushes with optional branchName queries", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.pull("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/pullRebase",
				);
				await git.pull("PROJ", { branchName: "main", },);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/pullRebase?branchName=main",
				);

				await git.push("PROJ", { branchName: "feat/x", },);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/push?branchName=feat%2Fx",
				);
				await git.push("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/push",
				);
			},
		);
	});

	it("logs with count default 1000 and pagination cursor", async () => {
		await withServer(
			(req, res,) => {
				if (req.url?.includes("startCommit=",)) {
					sendJson(res, { entries: [], nextCommit: undefined, },);
				} else {
					sendJson(res, {
						entries: [{ hash: "abc", },],
						nextCommit: "abc",
					},);
				}
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const page = await git.log("PROJ",);
				expect(page.entries,).toHaveLength(1,);
				expect(page.nextCommit,).toBe("abc",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/log?count=1000",
				);

				await git.log("PROJ", {
					path: "libs/py",
					startCommit: "abc",
					count: 50,
				},);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/log?path=libs%2Fpy&startCommit=abc&count=50",
				);
			},
		);
	});

	it("diffs with encoded commitFrom/commitTo queries", async () => {
		await withServer(
			(req, res,) => {
				sendJson(res, { entries: [], changedFiles: 1, },);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.diff("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/diff",
				);
				await git.diff("PROJ", { commitFrom: "a/b", commitTo: "c#1", },);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/diff?commitFrom=a%2Fb&commitTo=c%231",
				);
			},
		);
	});

	it("commits with an exact message body", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.commit("PROJ", { message: "feat: done", },);
				const request = last(records,);
				expect(request.url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/commit",
				);
				expect(JSON.parse(request.body,),).toEqual({ message: "feat: done", },);
			},
		);
	});

	it("reverts to a revision and reverts a commit with encoded commit params", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.revertToRevision("PROJ", "a/b",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/revertToRevision?commit=a%2Fb",
				);
				await git.revertCommit("PROJ", "c#1",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/revertCommit?commit=c%231",
				);
			},
		);
	});

	it("resets to head and upstream on the source-exact endpoints", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { success: true, },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.resetToHead("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/resetToLocalHeadState",
				);
				await git.resetToUpstream("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/resetToRemoteHeadState",
				);
			},
		);
	});

	it("requires acknowledgement before dropAndRebuild sends anything", async () => {
		let requests = 0;
		await withServer(
			(req, res,) => {
				requests += 1;
				sendJson(res, { success: true, },);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.dropAndRebuild("PROJ", {},),).rejects.toThrow(
					"permanently destroys",
				);
				await expect(git.dropAndRebuild("PROJ", { confirmed: false, },),).rejects.toThrow(
					"permanently destroys",
				);
				expect(requests,).toBe(0,);

				await git.dropAndRebuild("PROJ", { confirmed: true, },);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/dropAndRebuild?iKnowWhatIAmDoing=true",
				);
				expect(request.body,).toBe("",);
			},
		);
	});
});

describe("ProjectGitResource libraries", () => {
	it("lists attached libraries", async () => {
		await withServer(
			(_req, res,) => sendJson(res, [{ repository: "ssh://git@host/lib.git", },],),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const libraries = await git.listLibraries("PROJ",);
				expect(libraries,).toEqual([{ repository: "ssh://git@host/lib.git", },],);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/",
				);
			},
		);
	});

	it("adds a library with defaults: addToPythonPath true, empty path, null login", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { jobId: "j1", },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.addLibrary("PROJ", {
					repository: "ssh://git@host/lib.git",
					localTargetPath: "libs/python",
					checkout: "main",
				},);
				expect(result,).toEqual({ jobId: "j1", },);
				const request = last(records,);
				expect(request.method,).toBe("POST",);
				expect(request.url,).toBe("/dip/publicapi/projects/PROJ/git/lib-git-refs/",);
				expect(JSON.parse(request.body,),).toEqual({
					repository: "ssh://git@host/lib.git",
					login: null,
					password: null,
					pathInGitRepository: "",
					localTargetPath: "libs/python",
					checkout: "main",
					addToPythonPath: true,
				},);

				await git.addLibrary("PROJ", {
					repository: "ssh://git@host/lib.git",
					localTargetPath: "libs/other",
					checkout: "v1",
					pathInGitRepository: "src/lib",
					addToPythonPath: false,
					login: "user",
					password: "secret1",
				},);
				expect(JSON.parse(last(records,).body,),).toEqual({
					repository: "ssh://git@host/lib.git",
					login: "user",
					password: "secret1",
					pathInGitRepository: "src/lib",
					localTargetPath: "libs/other",
					checkout: "v1",
					addToPythonPath: false,
				},);
			},
		);
	});

	it("rejects libraries with embedded credential URLs before sending", async () => {
		let requests = 0;
		await withServer(
			(req, res,) => {
				requests += 1;
				sendJson(res, { jobId: "j1", },);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(
					git.addLibrary("PROJ", {
						repository: "https://user:token@host/lib.git",
						localTargetPath: "libs/x",
						checkout: "main",
					},),
				).rejects.toThrow("must not embed credentials",);
				await expect(
					git.setLibrary("PROJ", "libs/x", {
						repository: "https://user:token@host/lib.git",
						checkout: "main",
					},),
				).rejects.toThrow("must not embed credentials",);
				expect(requests,).toBe(0,);
			},
		);
	});

	it("redacts a password that DSS echoes in an error response", async () => {
		await withServer(
			(_req, res,) => {
				sendJson(
					res,
					{ message: "authentication failed for supersecret", },
					400,
				);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				let caughtError: unknown;
				try {
					await git.addLibrary("PROJ", {
						repository: "https://host/lib.git",
						localTargetPath: "libs/x",
						checkout: "main",
						password: "supersecret",
					},);
				} catch (caught: unknown) {
					caughtError = caught;
				}
				expect(caughtError,).toBeInstanceOf(DataikuError,);
				const error = caughtError as DataikuError;
				expect(error.message,).not.toContain("supersecret",);
				expect(error.body,).not.toContain("supersecret",);
				expect(error.message,).toContain("[redacted]",);
			},
		);
	});

	it("updates a library with the exact set body and segment-encoded path", async () => {
		await withServer(
			(_req, res,) => sendJson(res, "libs/py lib",),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.setLibrary("PROJ", "libs/py lib", {
					repository: "ssh://git@host/lib.git",
					checkout: "main",
				},);
				expect(result,).toBe("libs/py lib",);
				const request = last(records,);
				expect(request.method,).toBe("PUT",);
				expect(request.url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/libs/py%20lib",
				);
				expect(JSON.parse(request.body,),).toEqual({
					repository: "ssh://git@host/lib.git",
					login: null,
					password: null,
					pathInGitRepository: "",
					checkout: "main",
				},);

				await git.setLibrary("PROJ", "libs/py lib", {
					repository: "ssh://git@host/lib.git",
					pathInGitRepository: "src/lib",
					checkout: "v1",
					login: "user",
					password: "secret1",
				},);
				expect(JSON.parse(last(records,).body,),).toEqual({
					repository: "ssh://git@host/lib.git",
					login: "user",
					password: "secret1",
					pathInGitRepository: "src/lib",
					checkout: "v1",
				},);
			},
		);
	});

	it("removes a library with the deleteDirectory query default", async () => {
		await withServer(
			(_req, res,) => sendJson(res, {},),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.removeLibrary("PROJ", "libs/py lib",);
				expect(last(records,).method,).toBe("DELETE",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/libs/py%20lib?deleteDirectory=false",
				);
				await git.removeLibrary("PROJ", "libs/py lib", true,);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/libs/py%20lib?deleteDirectory=true",
				);
			},
		);
	});

	it("rejects dot segments, empty segments, and empty library reference paths before sending", async () => {
		let requests = 0;
		await withServer(
			(req, res,) => {
				requests += 1;
				sendJson(res, {},);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const options = {
					repository: "ssh://git@host/lib.git",
					checkout: "main",
				};
				for (
					const path of [
						"../lib",
						"libs/../other",
						"libs/./py",
						"..",
						"libs//py",
						"",
						"/",
						"///",
					]
				) {
					await expect(git.setLibrary("PROJ", path, options,),).rejects.toThrow(
						"Git library reference path",
					);
					await expect(git.removeLibrary("PROJ", path,),).rejects.toThrow(
						"Git library reference path",
					);
					await expect(git.resetLibrary("PROJ", path,),).rejects.toThrow(
						"Git library reference path",
					);
					await expect(git.pushLibrary("PROJ", path, "update lib",),).rejects.toThrow(
						"Git library reference path",
					);
				}
				expect(requests,).toBe(0,);
			},
		);
	});

	it("strips only outer slashes from a library reference path before encoding", async () => {
		await withServer(
			(_req, res,) => sendJson(res, {},),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.setLibrary("PROJ", "/libs/py lib/", {
					repository: "ssh://git@host/lib.git",
					checkout: "main",
				},);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/libs/py%20lib",
				);
				await git.removeLibrary("PROJ", "/libs/py lib/",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/libs/py%20lib?deleteDirectory=false",
				);
			},
		);
	});

	it("resets and pushes single libraries with gitRef and commitMessage bodies", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { jobId: "j2", },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(git.resetLibrary("PROJ", "libs/py",),).resolves.toEqual({ jobId: "j2", },);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/action/reset",
				);
				expect(JSON.parse(last(records,).body,),).toEqual({ gitRef: "libs/py", },);

				await git.pushLibrary("PROJ", "libs/py", "update lib",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/lib-git-refs/action/push",
				);
				expect(JSON.parse(last(records,).body,),).toEqual({
					gitRef: "libs/py",
					commitMessage: "update lib",
				},);
			},
		);
	});

	it("pushes and resets all libraries on the git-refs action endpoints", async () => {
		await withServer(
			(_req, res,) => sendJson(res, { jobId: "j3", },),
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.pushAllLibraries("PROJ", "sync all",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/git-refs/push-all",
				);
				expect(JSON.parse(last(records,).body,),).toEqual({ commitMessage: "sync all", },);

				await git.resetAllLibraries("PROJ",);
				expect(last(records,).url,).toBe(
					"/dip/publicapi/projects/PROJ/git/actions/git-refs/reset-all",
				);
				expect(last(records,).body,).toBe("",);
			},
		);
	});
});

describe("ProjectGitResource futures", () => {
	it("reads future state with peek=false by default and true on demand", async () => {
		await withServer(
			(req, res,) => {
				sendJson(res, { jobId: "j1", hasResult: false, alive: true, },);
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await git.getFutureState("j1",);
				expect(last(records,).url,).toBe(`${API}/futures/j1?peek=false`,);
				await git.getFutureState("j1", { peek: true, },);
				expect(last(records,).url,).toBe(`${API}/futures/j1?peek=true`,);
				await git.abortFuture("j1",);
				expect(last(records,).method,).toBe("DELETE",);
				expect(last(records,).url,).toBe(`${API}/futures/j1`,);
			},
		);
	});

	it("waits for a future and returns its result when hasResult arrives", async () => {
		let polls = 0;
		await withServer(
			(_req, res,) => {
				polls += 1;
				if (polls >= 3) {
					sendJson(res, { jobId: "j1", hasResult: true, result: { success: true, }, },);
				} else {
					sendJson(res, { jobId: "j1", hasResult: false, alive: true, },);
				}
			},
			async (url, records,) => {
				const git = new ProjectGitResource(createClient(url,),);
				const result = await git.waitForFuture("j1", {
					pollIntervalMs: 1,
					timeoutMs: 5_000,
				},);
				expect(result,).toEqual({ success: true, },);
				expect(polls,).toBe(3,);
				expect(records.every((record,) => record.url.endsWith("?peek=false",)),).toBe(true,);
			},
		);
	});

	it("throws on server error, aborted and unknown future states", async () => {
		await withServer(
			(req, res,) => {
				if (req.url?.includes("/futures/j1",)) {
					sendJson(res, { jobId: "j1", hasResult: false, error: "repo exploded", },);
				} else if (req.url?.includes("/futures/j2",)) {
					sendJson(res, { jobId: "j2", aborted: true, },);
				} else {
					sendJson(res, { jobId: "j3", unknown: true, },);
				}
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(
					git.waitForFuture("j1", { pollIntervalMs: 1, timeoutMs: 100, },),
				).rejects.toThrow("repo exploded",);
				await expect(
					git.waitForFuture("j2", { pollIntervalMs: 1, timeoutMs: 100, },),
				).rejects.toThrow("was aborted",);
				await expect(
					git.waitForFuture("j3", { pollIntervalMs: 1, timeoutMs: 100, },),
				).rejects.toThrow("unknown to the server",);
			},
		);
	});

	it("throws a timeout error after the budget elapses", async () => {
		await withServer(
			(_req, res,) => {
				sendJson(res, { jobId: "j1", hasResult: false, alive: true, },);
			},
			async (url,) => {
				const git = new ProjectGitResource(createClient(url,),);
				await expect(
					git.waitForFuture("j1", { pollIntervalMs: 1, timeoutMs: 25, },),
				).rejects.toThrow("Timed out",);
			},
		);
	});
	it("rejects non-finite future wait timing options", async () => {
		const git = new ProjectGitResource(createClient("http://127.0.0.1:1",),);
		await expect(git.waitForFuture("j1", { pollIntervalMs: Number.NaN, },),).rejects.toThrow(
			"pollIntervalMs must be a finite positive number",
		);
		await expect(git.waitForFuture("j1", { timeoutMs: Number.POSITIVE_INFINITY, },),).rejects
			.toThrow("timeoutMs must be a finite non-negative number",);
	});
});
