import { describe, expect, it, } from "bun:test";
import { commands, } from "../../src/cli/commands/index.js";
import { projectGitCommands, } from "../../src/cli/commands/project-git.js";
import { KNOWN_LONG_FLAGS, } from "../../src/cli/flags.js";
import { CommandResultFailure, } from "../../src/cli/output.js";
import { RESOURCE_NAMES, } from "../../src/cli/usage.js";
import type { DataikuClient, } from "../../src/client.js";
import type { ProjectGitAddLibraryOptions, } from "../../src/resources/project-git.js";
import { cliEnv, dss, sendJson, withCliServer, } from "./_harness.js";

const ACTIONS = [
	"status",
	"get-remote",
	"set-remote",
	"remove-remote",
	"branches",
	"create-branch",
	"delete-branch",
	"current-branch",
	"tags",
	"create-tag",
	"delete-tag",
	"switch",
	"fetch",
	"pull",
	"push",
	"log",
	"diff",
	"commit",
	"revert-to-revision",
	"revert-commit",
	"reset-to-head",
	"reset-to-upstream",
	"drop-and-rebuild",
	"list-libraries",
	"add-library",
	"set-library",
	"remove-library",
	"reset-library",
	"push-library",
	"push-all-libraries",
	"reset-all-libraries",
	"future-status",
	"future-wait",
	"future-abort",
];

interface RecordedCall {
	method: string;
	args: unknown[];
}

/**
 * A client whose `projectGit` resource records every call instead of issuing
 * HTTP, so each test asserts the exact method and arguments the CLI forwards.
 */
function recordingClient(
	results: Record<string, unknown> = {},
): { client: DataikuClient; calls: RecordedCall[]; } {
	const calls: RecordedCall[] = [];
	const projectGit = new Proxy({}, {
		get: (_target, method: string,) => (...args: unknown[]): Promise<unknown> => {
			calls.push({ method, args, },);
			return Promise.resolve(method in results ? results[method] : {},);
		},
	},);
	return { client: { projectGit, } as unknown as DataikuClient, calls, };
}

/** Normalizes sync-throwing and async handlers into one awaitable shape. */
async function run(
	action: string,
	client: DataikuClient,
	args: string[] = [],
	flags: Record<string, string | boolean> = {},
): Promise<unknown> {
	return await projectGitCommands[action]!.handler(client, args, flags,);
}

describe("project-git CLI registration", () => {
	it("registers the resource and every contract action", () => {
		expect(RESOURCE_NAMES,).toContain("project-git",);
		expect(commands["project-git"],).toBe(projectGitCommands,);
		expect(Object.keys(projectGitCommands,).sort(),).toEqual([...ACTIONS,].sort(),);
	});

	it("declares only flags the parser accepts", () => {
		const unknownFlags: string[] = [];
		for (const meta of Object.values(projectGitCommands,)) {
			for (const match of meta.usage.matchAll(/--([a-z][a-z0-9-]*)/g,)) {
				if (!KNOWN_LONG_FLAGS.has(match[1]!,)) unknownFlags.push(match[1]!,);
			}
		}
		expect(unknownFlags,).toEqual([],);
	});

	it("exposes no flag that would carry a password on the command line", () => {
		expect(KNOWN_LONG_FLAGS.has("password-env",),).toBe(true,);
		expect(KNOWN_LONG_FLAGS.has("password",),).toBe(false,);
		const usages = Object.values(projectGitCommands,).map((meta,) => meta.usage).join("\n",);
		expect(usages,).not.toContain("--password ",);
	});
});

describe("project-git argument validation", () => {
	it("requires --project-key on every action except the future lifecycle", async () => {
		for (const [action, meta,] of Object.entries(projectGitCommands,)) {
			if (action.startsWith("future-",)) continue;
			const { client, calls, } = recordingClient();
			const positionals = [...meta.usage.matchAll(/<[^>]+>/g,),]
				.map((_match, index,) => `positional-${String(index,)}`);
			await expect(
				run(action, client, positionals,),
			).rejects.toThrow("--project-key is required",);
			expect(calls,).toEqual([],);
		}
	});

	it("does not require --project-key for future lifecycle actions", async () => {
		const { client, calls, } = recordingClient();
		await run("future-status", client, ["JOB1",],);
		await run("future-wait", client, ["JOB1",],);
		await run("future-abort", client, ["JOB1",],);
		expect(calls.map((call,) => call.method),).toEqual([
			"getFutureState",
			"waitForFuture",
			"abortFuture",
		],);
	});

	it("requires the identifier positional", async () => {
		const { client, } = recordingClient();
		await expect(run("create-branch", client, [], { "project-key": "P", },),).rejects.toThrow(
			"Expected 1 argument(s), got 0",
		);
		await expect(run("future-abort", client, [],),).rejects.toThrow("Expected 1 argument(s), got 0",);
	});

	it("rejects unexpected positionals on flag-only actions", async () => {
		const { client, } = recordingClient();
		await expect(run("status", client, ["extra",], { "project-key": "P", },),).rejects.toThrow(
			"Unexpected argument(s): extra",
		);
	});

	it("requires --message where DSS requires a commit message", async () => {
		const { client, } = recordingClient();
		for (const action of ["commit", "push-all-libraries",]) {
			await expect(run(action, client, [], { "project-key": "P", },),).rejects.toThrow(
				"--message is required",
			);
		}
		await expect(
			run("push-library", client, ["shared",], { "project-key": "P", },),
		).rejects.toThrow("--message is required",);
	});

	it("rejects a flag supplied without a value", async () => {
		const { client, } = recordingClient();
		await expect(
			run("log", client, [], { "project-key": "P", "start-commit": true, },),
		).rejects.toThrow("--start-commit requires a value",);
	});

	it("rejects a non-numeric --count", async () => {
		const { client, } = recordingClient();
		await expect(
			run("log", client, [], { "project-key": "P", count: "many", },),
		).rejects.toThrow("Invalid numeric value for --count",);
	});
});

describe("project-git read dispatch", () => {
	it("forwards status, tags, and library reads with only the project key", async () => {
		const { client, calls, } = recordingClient();
		await run("status", client, [], { "project-key": "MYPROJECT", },);
		await run("tags", client, [], { "project-key": "MYPROJECT", },);
		await run("list-libraries", client, [], { "project-key": "MYPROJECT", },);
		expect(calls,).toEqual([
			{ method: "status", args: ["MYPROJECT",], },
			{ method: "listTags", args: ["MYPROJECT",], },
			{ method: "listLibraries", args: ["MYPROJECT",], },
		],);
	});

	it("defaults the remote name to origin", async () => {
		const { client, calls, } = recordingClient();
		await run("get-remote", client, [], { "project-key": "MYPROJECT", },);
		await run("get-remote", client, [], { "project-key": "MYPROJECT", name: "upstream", },);
		expect(calls[0]!.args,).toEqual(["MYPROJECT", "origin",],);
		expect(calls[1]!.args,).toEqual(["MYPROJECT", "upstream",],);
	});
	it("uses --repository for the Git remote without colliding with the global DSS --url", async () => {
		const { client, calls, } = recordingClient({ setRemote: { success: true, }, },);
		await run("set-remote", client, [], {
			"project-key": "MYPROJECT",
			repository: "git@github.com:acme/analytics.git",
		},);
		expect(calls[0]!.args,).toEqual([
			"MYPROJECT",
			{ url: "git@github.com:acme/analytics.git", name: "origin", },
		],);
		await expect(
			run("set-remote", client, [], {
				"project-key": "MYPROJECT",
				url: "git@github.com:acme/analytics.git",
			},),
		).rejects.toThrow("--repository is required",);
	});
	it("keeps the live DSS origin separate from the repository URL", async () => {
		let observedPath = "";
		let observedBody = "";
		await withCliServer((req, res,) => {
			observedPath = req.url ?? "";
			req.setEncoding("utf8",);
			req.on("data", (chunk: string,) => {
				observedBody += chunk;
			},);
			req.on("end", () => sendJson(res, { success: true, },),);
		}, async (url,) => {
			await dss([
				"project-git",
				"set-remote",
				"--repository",
				"git@github.com:acme/analytics.git",
				"--project-key",
				"MYPROJECT",
			], { env: cliEnv(url,), },);
		},);
		expect(observedPath,).toBe("/dip/publicapi/projects/MYPROJECT/git/remotes/origin",);
		expect(JSON.parse(observedBody,),).toEqual({ url: "git@github.com:acme/analytics.git", },);
	});
	it("redacts credentials from legacy remote reads and Git action failures", async () => {
		const secret = "LEGACY@REMOTE@SECRET";
		const remoteUrl = `https://user:${secret}@git.example.com/repo.git`;
		const { client, } = recordingClient({
			status: { remotes: [{ name: "origin", url: remoteUrl, },], },
			getRemote: { url: remoteUrl, },
			listLibraries: [{ repository: remoteUrl, password: secret, },],
			getFutureState: { result: { logs: `failed to read ${remoteUrl}`, }, },
		},);
		const outputs = await Promise.all([
			run("status", client, [], { "project-key": "P", },),
			run("get-remote", client, [], { "project-key": "P", },),
			run("list-libraries", client, [], { "project-key": "P", },),
			run("future-status", client, ["JOB1",],),
		],);
		const serialized = JSON.stringify(outputs,);
		expect(serialized,).not.toContain(secret,);
		expect(serialized,).toContain("[redacted]",);

		const failed = recordingClient({
			fetch: { success: false, logs: `fatal: ${remoteUrl}`, },
		},).client;
		let error: unknown;
		try {
			await run("fetch", failed, [], { "project-key": "P", },);
		} catch (caught) {
			error = caught;
		}
		expect(error,).toBeInstanceOf(CommandResultFailure,);
		expect(JSON.stringify((error as CommandResultFailure).result,),).not.toContain(secret,);
		const failedFuture = {
			projectGit: {
				waitForFuture: async () => {
					throw new Error(`clone failed for ${remoteUrl}`,);
				},
			},
		} as unknown as DataikuClient;
		let futureError: unknown;
		try {
			await run("future-wait", failedFuture, ["JOB1",],);
		} catch (caught) {
			futureError = caught;
		}
		expect(futureError,).toBeInstanceOf(Error,);
		expect((futureError as Error).message,).not.toContain(secret,);
	});

	it("passes --remote through to listBranches", async () => {
		const { client, calls, } = recordingClient();
		await run("branches", client, [], { "project-key": "MYPROJECT", },);
		await run("branches", client, [], { "project-key": "MYPROJECT", remote: true, },);
		expect(calls[0]!.args[1],).toBeUndefined();
		expect(calls[1]!.args[1],).toBe(true,);
	});

	it("maps log and diff flags onto the DSS query names", async () => {
		const { client, calls, } = recordingClient();
		await run("log", client, [], {
			"project-key": "MYPROJECT",
			path: "lib/python",
			"start-commit": "4f1c2ab",
			count: "20",
		},);
		await run("diff", client, [], { "project-key": "MYPROJECT", from: "4f1c2ab", to: "9de77c1", },);
		expect(calls[0]!.args,).toEqual([
			"MYPROJECT",
			{ path: "lib/python", startCommit: "4f1c2ab", count: 20, },
		],);
		expect(calls[1]!.args,).toEqual([
			"MYPROJECT",
			{ commitFrom: "4f1c2ab", commitTo: "9de77c1", },
		],);
	});

	it("normalizes the current branch into a stable envelope", async () => {
		const { client, } = recordingClient({ currentBranch: null, },);
		expect(await run("current-branch", client, [], { "project-key": "MYPROJECT", },),).toEqual({
			branch: null,
		},);
	});
});

describe("project-git write dispatch", () => {
	it("forwards every create-branch option under its DSS body name", async () => {
		const { client, calls, } = recordingClient({ createBranch: { success: true, }, },);
		await run("create-branch", client, ["feature/pricing",], {
			"project-key": "MYPROJECT",
			commit: "4f1c2ab",
			"duplicate-project": true,
			"target-project-key": "PRICING_BRANCH",
			"target-project-folder-id": "folder-7",
		},);
		expect(calls,).toEqual([{
			method: "createBranch",
			args: ["MYPROJECT", "feature/pricing", {
				commit: "4f1c2ab",
				duplicateProject: true,
				targetProjectKey: "PRICING_BRANCH",
				targetProjectFolderId: "folder-7",
			},],
		},],);
	});

	it("forwards every delete-branch option", async () => {
		const { client, calls, } = recordingClient({ deleteBranch: { success: true, }, },);
		await run("delete-branch", client, ["feature/pricing",], {
			"project-key": "MYPROJECT",
			remote: true,
			"delete-remotely": true,
			"force-delete": true,
		},);
		expect(calls[0]!.args,).toEqual([
			"MYPROJECT",
			"feature/pricing",
			{ remote: true, deleteRemotely: true, forceDelete: true, },
		],);
	});

	it("forwards tag, switch, commit, and revert identifiers", async () => {
		const { client, calls, } = recordingClient({
			createTag: { success: true, },
			deleteTag: { success: true, },
			switchBranch: { success: true, },
			commit: { success: true, },
			revertToRevision: { success: true, },
			revertCommit: { success: true, },
		},);
		const key = { "project-key": "MYPROJECT", };
		await run("create-tag", client, ["v1.2.0",], {
			...key,
			reference: "4f1c2ab",
			message: "Release 1.2.0",
		},);
		await run("delete-tag", client, ["v1.2.0",], key,);
		await run("switch", client, ["main",], key,);
		await run("commit", client, [], { ...key, message: "Update pricing", },);
		await run("revert-to-revision", client, ["4f1c2ab",], key,);
		await run("revert-commit", client, ["9de77c1",], key,);
		expect(calls,).toEqual([
			{
				method: "createTag",
				args: ["MYPROJECT", "v1.2.0", { reference: "4f1c2ab", message: "Release 1.2.0", },],
			},
			{ method: "deleteTag", args: ["MYPROJECT", "v1.2.0",], },
			{ method: "switchBranch", args: ["MYPROJECT", "main",], },
			{ method: "commit", args: ["MYPROJECT", { message: "Update pricing", },], },
			{ method: "revertToRevision", args: ["MYPROJECT", "4f1c2ab",], },
			{ method: "revertCommit", args: ["MYPROJECT", "9de77c1",], },
		],);
	});

	it("reports the resolved remote name after a delete", async () => {
		const { client, calls, } = recordingClient();
		const result = await run("remove-remote", client, [], {
			"project-key": "MYPROJECT",
			name: "upstream",
		},);
		expect(calls[0]!.args,).toEqual(["MYPROJECT", "upstream",],);
		expect(result,).toEqual({ removed: "upstream", resource: "project-git-remote", },);
	});
});

describe("project-git library dispatch", () => {
	it("adds a library with the target path positional and python-path opt-out", async () => {
		const { client, calls, } = recordingClient({ addLibrary: { jobId: "JOB1", }, },);
		await run("add-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
			repository: "git@github.com:acme/utils.git",
			checkout: "main",
			"path-in-repository": "python",
			login: "deploy-bot",
			"no-add-to-python-path": true,
		},);
		expect(calls[0]!.method,).toBe("addLibrary",);
		expect(calls[0]!.args[0],).toBe("MYPROJECT",);
		expect(calls[0]!.args[1],).toEqual({
			repository: "git@github.com:acme/utils.git",
			localTargetPath: "shared-utils",
			checkout: "main",
			pathInGitRepository: "python",
			addToPythonPath: false,
			login: "deploy-bot",
			password: undefined,
		},);
	});

	it("leaves addToPythonPath to the SDK default when the opt-out is absent", async () => {
		const { client, calls, } = recordingClient({ addLibrary: { jobId: "JOB1", }, },);
		await run("add-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
			repository: "git@github.com:acme/utils.git",
			checkout: "main",
		},);
		// Recorded argument: the CLI itself built this object, so its shape is known.
		const options = calls[0]!.args[1] as ProjectGitAddLibraryOptions;
		expect(options.addToPythonPath,).toBeUndefined();
	});

	it("requires --repository and --checkout for add-library and set-library", async () => {
		const { client, } = recordingClient();
		await expect(
			run("add-library", client, ["shared-utils",], { "project-key": "MYPROJECT", },),
		).rejects.toThrow("--repository is required",);
		await expect(
			run("set-library", client, ["shared-utils",], {
				"project-key": "MYPROJECT",
				repository: "git@github.com:acme/utils.git",
			},),
		).rejects.toThrow("--checkout is required",);
	});

	it("updates a library through the target-path positional", async () => {
		const { client, calls, } = recordingClient({ setLibrary: "shared-utils", },);
		await run("set-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
			repository: "git@github.com:acme/utils.git",
			checkout: "v2",
		},);
		expect(calls[0]!.args[0],).toBe("MYPROJECT",);
		expect(calls[0]!.args[1],).toBe("shared-utils",);
		expect(calls[0]!.args[2],).toEqual({
			repository: "git@github.com:acme/utils.git",
			pathInGitRepository: undefined,
			checkout: "v2",
			login: undefined,
			password: undefined,
		},);
	});

	it("passes --delete-directory to removeLibrary and echoes it back", async () => {
		const { client, calls, } = recordingClient();
		const kept = await run("remove-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
		},);
		const deleted = await run("remove-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
			"delete-directory": true,
		},);
		expect(calls[0]!.args,).toEqual(["MYPROJECT", "shared-utils", false,],);
		expect(calls[1]!.args,).toEqual(["MYPROJECT", "shared-utils", true,],);
		expect(kept,).toEqual({
			removed: "shared-utils",
			resource: "project-git-library",
			deleteDirectory: false,
		},);
		expect(deleted,).toMatchObject({ deleteDirectory: true, },);
	});

	it("forwards library reset and push calls", async () => {
		const { client, calls, } = recordingClient({
			resetLibrary: { jobId: "J1", },
			pushLibrary: { jobId: "J2", },
			pushAllLibraries: { jobId: "J3", },
			resetAllLibraries: { jobId: "J4", },
		},);
		await run("reset-library", client, ["shared-utils",], { "project-key": "MYPROJECT", },);
		await run("push-library", client, ["shared-utils",], {
			"project-key": "MYPROJECT",
			message: "Update helpers",
		},);
		await run("push-all-libraries", client, [], {
			"project-key": "MYPROJECT",
			message: "Sync libraries",
		},);
		await run("reset-all-libraries", client, [], { "project-key": "MYPROJECT", },);
		expect(calls,).toEqual([
			{ method: "resetLibrary", args: ["MYPROJECT", "shared-utils",], },
			{ method: "pushLibrary", args: ["MYPROJECT", "shared-utils", "Update helpers",], },
			{ method: "pushAllLibraries", args: ["MYPROJECT", "Sync libraries",], },
			{ method: "resetAllLibraries", args: ["MYPROJECT",], },
		],);
	});
});

describe("project-git library password handling", () => {
	const ENV_NAME = "DSS_TEST_GIT_LIBRARY_PASSWORD";
	const SECRET = "s3cr3t-git-token";

	it("reads the password from the named environment variable only", async () => {
		const { client, calls, } = recordingClient({ addLibrary: { jobId: "JOB1", }, },);
		process.env[ENV_NAME] = SECRET;
		try {
			await run("add-library", client, ["shared-utils",], {
				"project-key": "MYPROJECT",
				repository: "git@github.com:acme/utils.git",
				checkout: "main",
				"password-env": ENV_NAME,
			},);
		} finally {
			delete process.env[ENV_NAME];
		}
		// Recorded argument: the CLI itself built this object, so its shape is known.
		const options = calls[0]!.args[1] as ProjectGitAddLibraryOptions;
		expect(options.password,).toBe(SECRET,);
	});

	it("never puts the secret in the command result", async () => {
		const { client, } = recordingClient({
			addLibrary: {
				jobId: "JOB1",
				password: SECRET,
				logs: `https://user:${SECRET}@git.example.com/repo.git`,
			},
		},);
		process.env[ENV_NAME] = SECRET;
		let result: unknown;
		try {
			result = await run("add-library", client, ["shared-utils",], {
				"project-key": "MYPROJECT",
				repository: "git@github.com:acme/utils.git",
				checkout: "main",
				"password-env": ENV_NAME,
			},);
		} finally {
			delete process.env[ENV_NAME];
		}
		expect(JSON.stringify(result,),).not.toContain(SECRET,);
	});

	it("fails clearly when the named variable is unset, naming only the variable", async () => {
		const { client, calls, } = recordingClient();
		delete process.env[ENV_NAME];
		let error: unknown;
		try {
			await run("add-library", client, ["shared-utils",], {
				"project-key": "MYPROJECT",
				repository: "git@github.com:acme/utils.git",
				checkout: "main",
				"password-env": ENV_NAME,
			},);
		} catch (caught) {
			error = caught;
		}
		expect(error,).toBeInstanceOf(Error,);
		if (!(error instanceof Error)) throw new Error("expected an Error to be thrown",);
		const serialized = `${error.message}\n${JSON.stringify(error,)}`;
		expect(serialized,).toContain(ENV_NAME,);
		expect(serialized,).not.toContain(SECRET,);
		expect(calls,).toEqual([],);
	});

	it("rejects --password-env without a variable name", async () => {
		const { client, } = recordingClient();
		await expect(
			run("add-library", client, ["shared-utils",], {
				"project-key": "MYPROJECT",
				repository: "git@github.com:acme/utils.git",
				checkout: "main",
				"password-env": true,
			},),
		).rejects.toThrow("--password-env requires the name of an environment variable",);
	});
});

describe("project-git failure handling", () => {
	it("turns a 200-with-success-false Git action into a nonzero command failure", async () => {
		const { client, } = recordingClient({
			fetch: { success: false, logs: "fatal: could not read from remote repository", },
		},);
		let error: unknown;
		try {
			await run("fetch", client, [], { "project-key": "MYPROJECT", },);
		} catch (caught) {
			error = caught;
		}
		expect(error,).toBeInstanceOf(CommandResultFailure,);
		if (!(error instanceof CommandResultFailure)) {
			throw new Error("expected a CommandResultFailure to be thrown",);
		}
		expect(error.exitCode,).toBe(2,);
		expect(error.code,).toBe("command_result_failure",);
		expect(error.result,).toEqual({
			success: false,
			logs: "fatal: could not read from remote repository",
			resource: "project-git",
			action: "fetch",
		},);
	});

	it("fails every core Git action that reports success false", async () => {
		const cases: Array<[string, string, string[],]> = [
			["set-remote", "setRemote", [],],
			["create-branch", "createBranch", ["feature/x",],],
			["delete-branch", "deleteBranch", ["feature/x",],],
			["create-tag", "createTag", ["v1",],],
			["delete-tag", "deleteTag", ["v1",],],
			["switch", "switchBranch", ["main",],],
			["fetch", "fetch", [],],
			["pull", "pull", [],],
			["push", "push", [],],
			["commit", "commit", [],],
			["revert-to-revision", "revertToRevision", ["4f1c2ab",],],
			["revert-commit", "revertCommit", ["4f1c2ab",],],
			["reset-to-head", "resetToHead", [],],
			["reset-to-upstream", "resetToUpstream", [],],
			["drop-and-rebuild", "dropAndRebuild", [],],
		];
		for (const [action, method, positionals,] of cases) {
			const { client, } = recordingClient({ [method]: { success: false, }, },);
			await expect(
				run(action, client, positionals, {
					"project-key": "MYPROJECT",
					repository: "git@github.com:acme/analytics.git",
					message: "msg",
					"i-know-what-i-am-doing": true,
				},),
			).rejects.toBeInstanceOf(CommandResultFailure,);
		}
	});

	it("passes through a successful action and one that omits success", async () => {
		const { client, } = recordingClient({ push: { success: true, log: "ok", }, },);
		expect(await run("push", client, [], { "project-key": "MYPROJECT", },),).toEqual({
			success: true,
			log: "ok",
		},);
		const bare = recordingClient({ commit: {}, },);
		expect(
			await run("commit", bare.client, [], { "project-key": "MYPROJECT", message: "m", },),
		).toEqual({},);
	});
});

describe("project-git destructive acknowledgement", () => {
	it("refuses drop-and-rebuild without --i-know-what-i-am-doing and issues no request", async () => {
		const { client, calls, } = recordingClient();
		await expect(
			run("drop-and-rebuild", client, [], { "project-key": "MYPROJECT", },),
		).rejects.toThrow("permanently destroys this project's Git history",);
		expect(calls,).toEqual([],);
	});

	it("acknowledges the destruction explicitly to the SDK", async () => {
		const { client, calls, } = recordingClient({ dropAndRebuild: { success: true, }, },);
		await run("drop-and-rebuild", client, [], {
			"project-key": "MYPROJECT",
			"i-know-what-i-am-doing": true,
		},);
		expect(calls,).toEqual([{
			method: "dropAndRebuild",
			args: ["MYPROJECT", { confirmed: true, },],
		},],);
	});
});

describe("project-git future lifecycle", () => {
	it("peeks, waits, and aborts by job id", async () => {
		const { client, calls, } = recordingClient({
			getFutureState: { jobId: "JOB1", hasResult: false, },
			waitForFuture: { done: true, },
		},);
		await run("future-status", client, ["JOB1",], { peek: true, },);
		await run("future-wait", client, ["JOB1",], { timeout: "300000", "poll-interval": "5000", },);
		const aborted = await run("future-abort", client, ["JOB1",],);
		expect(calls,).toEqual([
			{ method: "getFutureState", args: ["JOB1", { peek: true, },], },
			{ method: "waitForFuture", args: ["JOB1", { pollIntervalMs: 5000, timeoutMs: 300000, },], },
			{ method: "abortFuture", args: ["JOB1",], },
		],);
		expect(aborted,).toEqual({ aborted: "JOB1", resource: "project-git-future", },);
	});
	it("fails a completed future whose result reports success false", async () => {
		const { client, } = recordingClient({
			waitForFuture: { success: false, logs: "remote rejected the push", },
		},);

		await expect(run("future-wait", client, ["JOB1",],),).rejects.toBeInstanceOf(
			CommandResultFailure,
		);
	});
});
