import { describe, expect, it, } from "bun:test";
import { pluginCommands, } from "../../src/cli/commands/plugin.js";
import { KNOWN_LONG_FLAGS, } from "../../src/cli/flags.js";
import { RESOURCE_NAMES, } from "../../src/cli/usage.js";
import type { DataikuClient, } from "../../src/client.js";
import { ClientValidationError, } from "../../src/errors.js";
import {
	encodePluginPath,
	validatedPluginGitUrl,
	validatePluginDestinationPath,
	validatePluginId,
	validatePluginName,
	validatePluginPath,
} from "../../src/resources/plugins.js";

const ACTIONS = [
	"list",
	"install-from-zip",
	"install-from-store",
	"install-from-git",
	"download",
	"update-from-zip",
	"update-from-store",
	"update-from-git",
	"settings-get",
	"settings-set",
	"code-env-create",
	"code-env-update",
	"move-to-dev",
	"usages",
	"delete",
	"create-dev",
	"get-git-remote",
	"set-git-remote",
	"delete-git-remote",
	"git-branches",
	"push",
	"pull",
	"fetch",
	"reset-local",
	"reset-remote",
	"contents-list",
	"contents-get",
	"contents-put",
	"contents-delete",
	"details",
	"folder-add",
	"rename",
	"move",
];

interface RecordedCall {
	method: string;
	args: unknown[];
}

/**
 * A client whose `plugins` resource records every call instead of issuing
 * HTTP, so each test asserts the exact method and arguments the CLI forwards.
 */
function recordingClient(
	results: Record<string, unknown> = {},
): { client: DataikuClient; calls: RecordedCall[]; } {
	const calls: RecordedCall[] = [];
	const plugins = new Proxy({}, {
		get: (_target, method: string,) => async (...args: unknown[]): Promise<unknown> => {
			calls.push({ method, args, },);
			if (method in results && results[method] instanceof Error) throw results[method];
			return Promise.resolve(method in results ? results[method] : {},);
		},
	},);
	return {
		client: { plugins, } as unknown as DataikuClient,
		calls,
	};
}

async function run(
	action: string,
	client: DataikuClient,
	args: string[] = [],
	flags: Record<string, string | boolean> = {},
): Promise<unknown> {
	return await pluginCommands[action]!.handler(client, args, flags,);
}

describe("plugin CLI registration", () => {
	it("covers every documented Plugins endpoint with an action", () => {
		expect(RESOURCE_NAMES,).toContain("plugin",);
		expect(Object.keys(pluginCommands,).sort(),).toEqual([...ACTIONS,].sort(),);
	});

	it("declares only flags the parser accepts", () => {
		const unknownFlags: string[] = [];
		for (const meta of Object.values(pluginCommands,)) {
			for (const match of meta.usage.matchAll(/--([a-z][a-z0-9-]*)/g,)) {
				if (!KNOWN_LONG_FLAGS.has(match[1]!,)) unknownFlags.push(match[1]!,);
			}
		}
		expect(unknownFlags,).toEqual([],);
	});
});

describe("plugin path and id validation", () => {
	it("rejects traversal and ambiguous paths", () => {
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
			expect(() => validatePluginPath(badPath,)).toThrow(ClientValidationError,);
		}
		expect(validatePluginPath("/python/lib.py",),).toBe("python/lib.py",);
		expect(encodePluginPath("python/my lib/x.py",),).toBe("python/my%20lib/x.py",);
	});

	it("rejects plugin ids that could escape the route segment", () => {
		for (const badId of ["", "a/b", "a b", "%2e%2e", "a:b",]) {
			expect(() => validatePluginId(badId,)).toThrow(ClientValidationError,);
		}
		expect(validatePluginId("my-plugin.v2",),).toBe("my-plugin.v2",);
	});

	it("rejects rename targets with separators or traversal", () => {
		for (const badName of ["sub/new.py", "..", ".", "x.", " x",]) {
			expect(() => validatePluginName(badName,)).toThrow(ClientValidationError,);
		}
		expect(validatePluginName("new.py",),).toBe("new.py",);
	});

	it("accepts only the documented creation modes", () => {
		expect(validatePluginDestinationPath("/",),).toBe("/",);
		expect(validatePluginDestinationPath("python/mylib",),).toBe("/python/mylib",);
		expect(() => validatePluginDestinationPath("../escape",)).toThrow(ClientValidationError,);
	});
});

describe("plugin git url validation", () => {
	it("rejects HTTP(S) URLs with embedded credentials before any request", () => {
		expect(
			() => validatedPluginGitUrl("https://user:secret@host/repo.git",),
		).toThrow(/must not embed credentials/,);
		expect(
			() => validatedPluginGitUrl("http://token@host/repo",),
		).toThrow(/must not embed credentials/,);
		expect(() => validatedPluginGitUrl("",)).toThrow(/is required/,);
	});

	it("allows SSH and scp-style remotes", () => {
		expect(validatedPluginGitUrl("git@github.com:acme/p.git",),).toBe(
			"git@github.com:acme/p.git",
		);
		expect(validatedPluginGitUrl("ssh://git@git.example.com/acme/p.git",),).toBe(
			"ssh://git@git.example.com/acme/p.git",
		);
	});
});

describe("plugin CLI argument validation before any request", () => {
	it("rejects traversal paths without contacting the resource", async () => {
		const { client, calls, } = recordingClient();
		for (
			const action of ["contents-get", "contents-put", "contents-delete", "folder-add",] as const
		) {
			await expect(run(action, client, ["p", "../escape.py",],),)
				.rejects.toBeInstanceOf(ClientValidationError,);
		}
		await expect(
			run("rename", client, ["p", "python/old.py", "sub/new.py",],),
		).rejects.toThrow(/single segment/,);
		await expect(run("move", client, ["p", "python/old.py", "../escape",],),)
			.rejects.toBeInstanceOf(ClientValidationError,);
		expect(calls,).toEqual([],);
	});

	it("rejects invalid plugin ids without contacting the resource", async () => {
		const { client, calls, } = recordingClient();
		for (const action of ["settings-get", "usages", "delete", "git-branches", "push",] as const) {
			await expect(run(action, client, ["../escape",],),).rejects.toBeInstanceOf(
				ClientValidationError,
			);
		}
		expect(calls,).toEqual([],);
	});

	it("rejects embedded-credential git URLs before any request", async () => {
		const { client, calls, } = recordingClient();
		await expect(
			run("install-from-git", client, [], { repository: "https://u:pw@host/repo", },),
		).rejects.toBeInstanceOf(ClientValidationError,);
		await expect(
			run("set-git-remote", client, ["p",], { repository: "https://u:pw@host/repo", },),
		).rejects.toBeInstanceOf(ClientValidationError,);
		await expect(
			run("create-dev", client, ["p",], {
				"creation-mode": "GIT_CLONE",
				repository: "https://u:pw@host/repo",
			},),
		).rejects.toBeInstanceOf(ClientValidationError,);
		expect(calls,).toEqual([],);
	});

	it("requires --file for zip installs and --output for download", async () => {
		const { client, calls, } = recordingClient();
		await expect(run("install-from-zip", client, [],),).rejects.toThrow(/--file/,);
		await expect(run("update-from-zip", client, ["p",],),).rejects.toThrow(/--file/,);
		await expect(run("download", client, ["p",],),).rejects.toThrow(/--output/,);
		expect(calls,).toEqual([],);
	});

	it("requires --repository for git installs and create-dev GIT modes", async () => {
		const { client, calls, } = recordingClient();
		await expect(run("install-from-git", client, [],),).rejects.toThrow(/--repository/,);
		await expect(
			run("create-dev", client, ["p",], { "creation-mode": "GIT_CLONE", },),
		).rejects.toThrow(/--repository/,);
		expect(calls,).toEqual([],);
	});
});

describe("plugin CLI dry-run plans make zero requests", () => {
	it("plans zip install without reading the file", async () => {
		const { client, calls, } = recordingClient();
		const result = await run("install-from-zip", client, [], {
			file: "/tmp/never-read.zip",
			"dry-run": true,
		},);
		expect(result,).toMatchObject({
			plan: true,
			resource: "plugin",
			action: "install-from-zip",
			method: "POST",
			endpoint: "/public/api/plugins/actions/installFromZip",
		},);
		expect(calls,).toEqual([],);
	});

	it("plans store and git installs with the documented bodies", async () => {
		const { client, calls, } = recordingClient();
		const store = await run("install-from-store", client, ["my-plugin",], { "dry-run": true, },);
		expect(store,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/actions/installFromStore",
			payload: { pluginId: "my-plugin", },
		},);
		const git = await run("install-from-git", client, [], {
			repository: "git@github.com:acme/p.git",
			checkout: "main",
			"dry-run": true,
		},);
		expect(git,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/actions/installFromGit",
			payload: {
				gitRepositoryUrl: "git@github.com:acme/p.git",
				gitCheckout: "main",
				gitSubpath: null,
			},
		},);
		expect(calls,).toEqual([],);
	});

	it("plans delete with force and never dispatches", async () => {
		const { client, calls, } = recordingClient();
		const result = await run("delete", client, ["my-plugin",], { force: true, "dry-run": true, },);
		expect(result,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/plugins/my-plugin/actions/delete",
			payload: { force: true, },
		},);
		expect(calls,).toEqual([],);
	});

	it("plans git mutations under the documented action paths", async () => {
		const { client, calls, } = recordingClient();
		for (
			const [action, endpoint,] of [
				["push", "/public/api/plugins/p/actions/push",],
				["pull", "/public/api/plugins/p/actions/pullRebase",],
				["fetch", "/public/api/plugins/p/actions/fetch",],
				["reset-local", "/public/api/plugins/p/actions/resetToLocalHeadState",],
				["reset-remote", "/public/api/plugins/p/actions/resetToRemoteHeadState",],
			] as const
		) {
			const result = await run(action, client, ["p",], { "dry-run": true, },);
			expect(result,).toMatchObject({ plan: true, method: "POST", endpoint, },);
		}
		expect(calls,).toEqual([],);
	});

	it("plans contents mutations with per-segment encoded endpoints", async () => {
		const { client, calls, } = recordingClient();
		const put = await run("contents-put", client, ["p", "python/my lib/x.py",], {
			content: "secret body",
			"dry-run": true,
		},);
		const plan = put as { endpoint?: string; payload?: unknown; };
		expect(plan.endpoint,).toBe(
			"/public/api/plugins/p/contents/python/my%20lib/x.py",
		);
		expect(plan.payload,).toEqual({ contentSource: "flag", },);
		expect(JSON.stringify(put,),).not.toContain("secret",);
		const del = await run("contents-delete", client, ["p", "python/x.py",], { "dry-run": true, },);
		expect(del,).toMatchObject({
			plan: true,
			method: "DELETE",
			endpoint: "/public/api/plugins/p/contents/python/x.py",
		},);
		const folder = await run("folder-add", client, ["p", "python/mylib",], { "dry-run": true, },);
		expect(folder,).toMatchObject({
			plan: true,
			method: "POST",
			endpoint: "/public/api/plugins/p/folders/python/mylib",
		},);
		expect(calls,).toEqual([],);
	});

	it("plans rename and move with the documented bodies", async () => {
		const { client, calls, } = recordingClient();
		const rename = await run("rename", client, ["p", "python/old.py", "new.py",], {
			"dry-run": true,
		},);
		expect(rename,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/p/contents-actions/rename",
			payload: { oldPath: "/python/old.py", newName: "new.py", },
		},);
		const move = await run("move", client, ["p", "python/old.py", "python/mylib",], {
			"dry-run": true,
		},);
		expect(move,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/p/contents-actions/move",
			payload: { oldPath: "/python/old.py", newPath: "/python/mylib", },
		},);
		expect(calls,).toEqual([],);
	});

	it("plans create-dev with mode-specific git fields and validated urls", async () => {
		const { client, calls, } = recordingClient();
		const empty = await run("create-dev", client, ["p",], { "dry-run": true, },);
		expect(empty,).toMatchObject({
			payload: { pluginId: "p", creationMode: "EMPTY", gitRepository: null, },
		},);
		const git = await run("create-dev", client, ["p",], {
			"creation-mode": "git-export",
			repository: "git@github.com:acme/p.git",
			checkout: "main",
			"path-in-repository": "plugin",
			"dry-run": true,
		},);
		expect(git,).toMatchObject({
			payload: {
				creationMode: "GIT_EXPORT",
				gitRepository: "git@github.com:acme/p.git",
				gitCheckout: "main",
				gitSubpath: "plugin",
			},
		},);
		expect(calls,).toEqual([],);
	});

	it("plans code-env creates and updates as futures", async () => {
		const { client, calls, } = recordingClient();
		const create = await run("code-env-create", client, ["p",], {
			conda: true,
			"python-interpreter": "PYTHON36",
			"dry-run": true,
		},);
		expect(create,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/p/code-env/actions/create",
			payload: { conda: true, pythonInterpreter: "PYTHON36", },
			async: "future",
		},);
		const update = await run("code-env-update", client, ["p",], { "dry-run": true, },);
		expect(update,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/p/code-env/actions/update",
			async: "future",
		},);
		expect(calls,).toEqual([],);
	});

	it("plans settings-set without embedding the settings content", async () => {
		const { client, calls, } = recordingClient();
		const result = await run("settings-set", client, ["p",], {
			content: '{"config":{"secret":"TOPSECRET"},"codeEnvName":"env"}',
			"dry-run": true,
		},);
		expect(result,).toMatchObject({
			plan: true,
			endpoint: "/public/api/plugins/p/settings",
			payload: { configKeys: ["secret",], codeEnvName: "env", },
		},);
		expect(JSON.stringify(result,),).not.toContain("TOPSECRET",);
		expect(calls,).toEqual([],);
	});
});
