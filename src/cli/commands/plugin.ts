import * as fs from "node:fs";
import {
	validatedPluginGitUrl,
	validatePluginDestinationPath,
	validatePluginId,
	validatePluginName,
	validatePluginPath,
} from "../../resources/plugins.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { num, readStdinText, sha256Hex, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, UsageError, } from "../usage.js";

const PLUGIN_EXIT_CODES: Record<string, number> = { usage: 1, error: 2, transient: 3, };

/** Plan-time endpoint helpers. Paths are validated, then encoded per segment. */
function pluginActionEndpoint(pluginId: string, action: string,): string {
	return `/public/api/plugins/${encodeURIComponent(validatePluginId(pluginId,),)}/actions/${action}`;
}

function pluginRootEndpoint(pluginId: string, suffix: string,): string {
	return `/public/api/plugins/${encodeURIComponent(validatePluginId(pluginId,),)}${suffix}`;
}

function pluginContentsEndpoint(pluginId: string, path: string,): string {
	return `${pluginRootEndpoint(pluginId, "/contents/",)}${encodePluginPathForPlan(path,)}`;
}

function pluginFoldersEndpoint(pluginId: string, path: string,): string {
	return `${pluginRootEndpoint(pluginId, "/folders/",)}${encodePluginPathForPlan(path,)}`;
}

function encodePluginPathForPlan(path: string,): string {
	return validatePluginPath(path,)
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

function scopeQueryForPlan(projectKey: string | undefined,): string {
	if (typeof projectKey !== "string" || projectKey.trim() === "") return "";
	return `?projectKey=${encodeURIComponent(projectKey.trim(),)}`;
}

/**
 * Dry-run plan for a mutating plugin command. Never contains file bytes,
 * git credentials, or zip paths' contents — only the request shape.
 */
function pluginPlan(
	action: string,
	options: {
		asyncKind?: string;
		endpoint?: string;
		identifiers?: Record<string, unknown>;
		idempotency?: string;
		method?: string;
		payload?: unknown;
	},
): Record<string, unknown> {
	return planResult("plugin", action, {
		asyncKind: options.asyncKind ?? "none",
		endpoint: options.endpoint,
		identifiers: options.identifiers,
		idempotency: options.idempotency ?? "none",
		method: options.method,
		payload: options.payload,
		exitCodesOnFailure: PLUGIN_EXIT_CODES,
		plannedAndDryRun: true,
	},);
}

function requireFileFlag(
	flags: Record<string, string | boolean>,
	usage: string,
): string {
	const value = flags["file"];
	if (typeof value !== "string" || value.trim() === "") {
		throw new UsageError(`--file PATH is required. Usage: ${usage}`, "missing_required_flag",);
	}
	return value;
}

function optionalStringFlag(
	flags: Record<string, string | boolean>,
	name: string,
): string | undefined {
	const value = flags[name];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.trim() === "") {
		throw new UsageError(`--${name} requires a value.`, "invalid_flag_value", undefined, {
			flag: `--${name}`,
		},);
	}
	return value.trim();
}

const INSTALL_ZIP_USAGE = commandUsage("plugin", "install-from-zip",);
const INSTALL_STORE_USAGE = commandUsage("plugin", "install-from-store",);
const INSTALL_GIT_USAGE = commandUsage("plugin", "install-from-git",);
const DOWNLOAD_USAGE = commandUsage("plugin", "download",);
const UPDATE_ZIP_USAGE = commandUsage("plugin", "update-from-zip",);
const UPDATE_STORE_USAGE = commandUsage("plugin", "update-from-store",);
const UPDATE_GIT_USAGE = commandUsage("plugin", "update-from-git",);
const SETTINGS_GET_USAGE = commandUsage("plugin", "settings-get",);
const SETTINGS_SET_USAGE = commandUsage("plugin", "settings-set",);
const CODE_ENV_CREATE_USAGE = commandUsage("plugin", "code-env-create",);
const CODE_ENV_UPDATE_USAGE = commandUsage("plugin", "code-env-update",);
const MOVE_TO_DEV_USAGE = commandUsage("plugin", "move-to-dev",);
const USAGES_USAGE = commandUsage("plugin", "usages",);
const DELETE_USAGE = commandUsage("plugin", "delete",);
const CREATE_DEV_USAGE = commandUsage("plugin", "create-dev",);
const GET_GIT_REMOTE_USAGE = commandUsage("plugin", "get-git-remote",);
const SET_GIT_REMOTE_USAGE = commandUsage("plugin", "set-git-remote",);
const DELETE_GIT_REMOTE_USAGE = commandUsage("plugin", "delete-git-remote",);
const GIT_BRANCHES_USAGE = commandUsage("plugin", "git-branches",);
const PUSH_USAGE = commandUsage("plugin", "push",);
const PULL_USAGE = commandUsage("plugin", "pull",);
const FETCH_USAGE = commandUsage("plugin", "fetch",);
const RESET_LOCAL_USAGE = commandUsage("plugin", "reset-local",);
const RESET_REMOTE_USAGE = commandUsage("plugin", "reset-remote",);
const CONTENTS_LIST_USAGE = commandUsage("plugin", "contents-list",);
const CONTENTS_GET_USAGE = commandUsage("plugin", "contents-get",);
const CONTENTS_PUT_USAGE = commandUsage("plugin", "contents-put",);
const CONTENTS_DELETE_USAGE = commandUsage("plugin", "contents-delete",);
const DETAILS_USAGE = commandUsage("plugin", "details",);
const FOLDER_ADD_USAGE = commandUsage("plugin", "folder-add",);
const RENAME_USAGE = commandUsage("plugin", "rename",);
const MOVE_USAGE = commandUsage("plugin", "move",);

async function waitFuture(
	client: unknown,
	jobId: string,
	flags: Record<string, string | boolean>,
): Promise<unknown> {
	return (client as { futures: { wait(id: string, opts: unknown,): Promise<unknown>; }; })
		.futures.wait(jobId, {
			pollIntervalMs: num(flags["poll-interval"], "--poll-interval",),
			timeoutMs: num(flags["timeout"], "--timeout",),
		},);
}

export const pluginCommands: Record<string, CommandMeta> = withUsage("plugin", {
	list: {
		handler: (c,) => c.plugins.list(),
		description: "List installed plugins (including development plugins).",
		examples: ["dss plugin list",],
	},
	"install-from-zip": {
		handler: async (c, _a, f,) => {
			requireNoArgs(_a, INSTALL_ZIP_USAGE,);
			const file = requireFileFlag(f, INSTALL_ZIP_USAGE,);
			if (executionMode(f,).dryRun) {
				return pluginPlan("install-from-zip", {
					method: "POST",
					endpoint: "/public/api/plugins/actions/installFromZip",
					identifiers: { file, upload: "multipart zip", },
					idempotency: "none",
				},);
			}
			await c.plugins.installFromZip(file,);
			return { installed: true, from: "zip", };
		},
		description:
			"Install a plugin from a zip file. Fails when the plugin is already installed (use update-from-zip to replace).",
		examples: ["dss plugin install-from-zip --file ./my-plugin.zip",],
	},
	"install-from-store": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, INSTALL_STORE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("install-from-store", {
					method: "POST",
					endpoint: "/public/api/plugins/actions/installFromStore",
					identifiers: { pluginId, },
					payload: { pluginId, },
				},);
			}
			await c.plugins.installFromStore(pluginId,);
			return { installed: pluginId, from: "store", };
		},
		description: "Install a plugin from the Dataiku store. Fails when already installed.",
		examples: ["dss plugin install-from-store my-plugin",],
	},
	"install-from-git": {
		handler: async (_c, _a, f,) => {
			requireNoArgs(_a, INSTALL_GIT_USAGE,);
			const repository = optionalStringFlag(f, "repository",);
			const checkout = optionalStringFlag(f, "checkout",);
			const subpath = optionalStringFlag(f, "path-in-repository",);
			if (repository === undefined) {
				throw new UsageError("--repository URL is required.", "missing_required_flag",);
			}
			const url = validatedPluginGitUrl(repository,);
			if (executionMode(f,).dryRun) {
				return pluginPlan("install-from-git", {
					method: "POST",
					endpoint: "/public/api/plugins/actions/installFromGit",
					identifiers: { repositoryUrl: url, },
					payload: {
						gitRepositoryUrl: url,
						gitCheckout: checkout ?? null,
						gitSubpath: subpath ?? null,
					},
				},);
			}
			await _c.plugins.installFromGit({
				gitRepositoryUrl: url,
				gitCheckout: checkout,
				gitSubpath: subpath,
			},);
			return { installed: true, from: "git", repositoryUrl: url, };
		},
		description:
			"Install a plugin by checking out a Git repository (must contain plugin.json). Fails when already installed. HTTP(S) URLs must not embed credentials.",
		examples: [
			"dss plugin install-from-git --repository git@github.com:acme/my-plugin.git --checkout main",
		],
	},
	download: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, DOWNLOAD_USAGE,);
			const out = f["output"];
			if (typeof out !== "string" || out.trim() === "") {
				throw new UsageError("--output PATH is required.", "missing_required_flag",);
			}
			const pluginId = validatePluginId(a[0],);
			const res = await c.plugins.download(pluginId,);
			const bytes = await writeResponseToFile(out, res,);
			return { path: out, bytes, };
		},
		description: "Download a development plugin as a zip archive to a local file.",
		examples: ["dss plugin download my-plugin --output ./my-plugin.zip",],
	},
	"update-from-zip": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, UPDATE_ZIP_USAGE,);
			const file = requireFileFlag(f, UPDATE_ZIP_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("update-from-zip", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "updateFromZip",),
					identifiers: { pluginId, file, upload: "multipart", },
					idempotency: "none",
				},);
			}
			await c.plugins.updateFromZip(pluginId, file,);
			return { updated: pluginId, from: "zip", };
		},
		description:
			"Re-install a plugin from a zip file. Fails when the plugin is not already installed.",
		examples: ["dss plugin update-from-zip my-plugin --file ./my-plugin.zip",],
	},
	"update-from-store": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, UPDATE_STORE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("update-from-store", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "updateFromStore",),
					identifiers: { pluginId, },
				},);
			}
			await c.plugins.updateFromStore(pluginId,);
			return { updated: pluginId, from: "store", };
		},
		description: "Update a plugin from the Dataiku Store.",
		examples: ["dss plugin update-from-store my-plugin",],
	},
	"update-from-git": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, UPDATE_GIT_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const repository = optionalStringFlag(f, "repository",);
			const checkout = optionalStringFlag(f, "checkout",);
			const subpath = optionalStringFlag(f, "path-in-repository",);
			if (repository === undefined) {
				throw new UsageError("--repository URL is required.", "missing_required_flag",);
			}
			const url = validatedPluginGitUrl(repository,);
			if (executionMode(f,).dryRun) {
				return pluginPlan("update-from-git", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "updateFromGit",),
					identifiers: { pluginId, repositoryUrl: url, },
					payload: {
						gitRepositoryUrl: url,
						gitCheckout: checkout ?? null,
						gitSubpath: subpath ?? null,
					},
				},);
			}
			await c.plugins.updateFromGit(pluginId, {
				gitRepositoryUrl: url,
				gitCheckout: checkout,
				gitSubpath: subpath,
			},);
			return { updated: true, from: "git", repositoryUrl: url, };
		},
		description:
			"Update an installed plugin from a Git repository (must contain plugin.json). Fails when not installed. HTTP(S) URLs must not embed credentials.",
		examples: [
			"dss plugin update-from-git my-plugin --repository git@github.com:acme/my-plugin.git --checkout v2",
		],
	},
	"settings-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, SETTINGS_GET_USAGE,);
			return c.plugins.getSettings(validatePluginId(a[0],), {
				projectKey: optionalStringFlag(f, "project-key",),
			},);
		},
		description:
			"Get plugin settings (config parameters and code env name), optionally scoped to a project.",
		examples: ["dss plugin settings-get my-plugin --project-key MYPROJECT",],
	},
	"settings-set": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, SETTINGS_SET_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const mode = executionMode(f,);
			const settings = readSettingsInput(f,);
			if (mode.dryRun) {
				return pluginPlan("settings-set", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/settings",)
						+ scopeQueryForPlan(f["project-key"] as string | undefined,),
					identifiers: { pluginId, },
					payload: {
						configKeys: Object.keys(settings.config ?? {},),
						codeEnvName: settings.codeEnvName,
					},
				},);
			}
			await c.plugins.setSettings(pluginId, settings, {
				projectKey: optionalStringFlag(f, "project-key",),
			},);
			return { updated: pluginId, settingsKeys: Object.keys(settings,), };
		},
		description:
			"Replace plugin settings. Send only settings previously obtained through settings-get; DSS rejects unknown shapes.",
		examples: [
			'dss plugin settings-set my-plugin --content \'{"config":{"key":"value"},"codeEnvName":"py-env"}\'',
		],
	},
	"code-env-create": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CODE_ENV_CREATE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const conda = f["conda"] === true;
			const pythonInterpreter = optionalStringFlag(f, "python-interpreter",);
			if (executionMode(f,).dryRun) {
				return pluginPlan("code-env-create", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/code-env/actions/create",),
					identifiers: { pluginId, },
					payload: {
						deploymentMode: "PLUGIN_MANAGED",
						conda,
						pythonInterpreter: pythonInterpreter ?? null,
					},
					asyncKind: "future",
				},);
			}
			const future = await c.plugins.createCodeEnv(pluginId, { conda, pythonInterpreter, },);
			if (f["wait"] !== true) return future;
			return await waitFuture(c, future.jobId, f,);
		},
		description:
			"Create the code env of a plugin. Returns the DSS future (jobId); --wait polls it to completion.",
		examples: [
			"dss plugin code-env-create my-plugin --python-interpreter PYTHON36",
			"dss plugin code-env-create my-plugin --conda --wait --timeout 300000",
		],
	},
	"code-env-update": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CODE_ENV_UPDATE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("code-env-update", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/code-env/actions/update",),
					identifiers: { pluginId, },
					asyncKind: "future",
				},);
			}
			const future = await c.plugins.updateCodeEnv(pluginId,);
			if (f["wait"] !== true) return future;
			return await waitFuture(c, future.jobId, f,);
		},
		description:
			"Update (rebuild) the code env of a plugin. Returns the DSS future (jobId); --wait polls it to completion.",
		examples: ["dss plugin code-env-update my-plugin --wait",],
	},
	"move-to-dev": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, MOVE_TO_DEV_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("move-to-dev", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "moveToDev",),
					identifiers: { pluginId, },
				},);
			}
			await c.plugins.moveToDev(pluginId,);
			return { moved: pluginId, to: "dev", };
		},
		description: "Move an installed plugin to the development environment for editing.",
		examples: ["dss plugin move-to-dev my-plugin",],
	},
	usages: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, USAGES_USAGE,);
			return c.plugins.listUsages(validatePluginId(a[0],), {
				projectKey: optionalStringFlag(f, "project-key",),
			},);
		},
		description:
			"List usages of a plugin's elements in projects or globally; missingTypes reports types the usage analysis could not resolve.",
		examples: ["dss plugin usages my-plugin --project-key MYPROJECT",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, DELETE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const force = f["force"] === true;
			if (executionMode(f,).dryRun) {
				return pluginPlan("delete", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "delete",),
					identifiers: { pluginId, },
					payload: { force, },
					idempotency: "none",
				},);
			}
			await c.plugins.delete(pluginId, { force, },);
			return { deleted: pluginId, force, };
		},
		description: "Delete a plugin. DSS refuses when usages are detected unless --force is passed.",
		examples: ["dss plugin delete my-plugin", "dss plugin delete my-plugin --force",],
	},
	"create-dev": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CREATE_DEV_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const creationModeRaw = optionalStringFlag(f, "creation-mode",) ?? "EMPTY";
			const creationMode = normalizeCreationMode(creationModeRaw,);
			const repository = optionalStringFlag(f, "repository",);
			const checkout = optionalStringFlag(f, "checkout",);
			const subpath = optionalStringFlag(f, "path-in-repository",);
			const needsGit = creationMode !== "EMPTY";
			if (needsGit && repository === undefined) {
				throw new UsageError(
					`--repository URL is required when --creation-mode is ${creationMode}.`,
					"missing_required_flag",
				);
			}
			const url = repository === undefined ? undefined : validatedPluginGitUrl(repository,);
			if (executionMode(f,).dryRun) {
				return pluginPlan("create-dev", {
					method: "POST",
					endpoint: "/public/api/plugins/actions/createDev",
					identifiers: { pluginId, creationMode, ...(url ? { repositoryUrl: url, } : {}), },
					payload: {
						pluginId,
						creationMode,
						gitRepository: url ?? null,
						gitCheckout: needsGit ? checkout ?? null : null,
						gitSubpath: creationMode === "GIT_EXPORT" ? subpath ?? null : null,
					},
				},);
			}
			await c.plugins.createDev({
				pluginId,
				creationMode,
				...(needsGit ? { gitRepository: url!, gitCheckout: checkout, gitSubpath: subpath, } : {}),
			},);
			return { created: pluginId, creationMode, };
		},
		description:
			"Create a new development plugin: EMPTY, GIT_CLONE (clone the repository as the plugin), or GIT_EXPORT (use a subpath of the repository). HTTP(S) URLs must not embed credentials.",
		examples: [
			"dss plugin create-dev my-plugin",
			"dss plugin create-dev my-plugin --creation-mode GIT_CLONE --repository git@github.com:acme/my-plugin.git --checkout main",
		],
	},
	"get-git-remote": {
		handler: (c, a,) => {
			requireArgs(a, 1, GET_GIT_REMOTE_USAGE,);
			return c.plugins.getGitRemote(validatePluginId(a[0],),);
		},
		description:
			"Get the Git remote declared for a development plugin (repositoryUrl is null when unset).",
		examples: ["dss plugin get-git-remote my-plugin",],
	},
	"set-git-remote": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, SET_GIT_REMOTE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const repository = optionalStringFlag(f, "repository",);
			if (repository === undefined) {
				throw new UsageError("--repository URL is required.", "missing_required_flag",);
			}
			const url = validatedPluginGitUrl(repository,);
			if (executionMode(f,).dryRun) {
				return pluginPlan("set-git-remote", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/gitRemote",),
					identifiers: { pluginId, },
					payload: { repositoryUrl: url, },
				},);
			}
			const result = await c.plugins.setGitRemote(pluginId, url,);
			return { pluginId, ...result, };
		},
		description:
			"Declare the Git remote for a development plugin. HTTP(S) URLs must not embed credentials; use SSH remotes or DSS-managed Git credentials.",
		examples: ["dss plugin set-git-remote my-plugin --repository git@github.com:acme/my-plugin.git",],
	},
	"delete-git-remote": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, DELETE_GIT_REMOTE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("delete-git-remote", {
					method: "DELETE",
					endpoint: pluginRootEndpoint(pluginId, "/gitRemote",),
					identifiers: { pluginId, },
				},);
			}
			await c.plugins.deleteGitRemote(pluginId,);
			return { deleted: "gitRemote", pluginId, };
		},
		description: "Delete the Git remote declared for a development plugin.",
		examples: ["dss plugin delete-git-remote my-plugin",],
	},
	"git-branches": {
		handler: (c, a,) => {
			requireArgs(a, 1, GIT_BRANCHES_USAGE,);
			return c.plugins.listGitBranches(validatePluginId(a[0],),);
		},
		description: "List the Git branches of a development plugin's declared remote.",
		examples: ["dss plugin git-branches my-plugin",],
	},
	push: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, PUSH_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("push", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "push",),
					identifiers: { pluginId, },
				},);
			}
			const result = await c.plugins.push(pluginId,);
			return { pushed: pluginId, result, };
		},
		description: "Push the development plugin's content to its previously-declared Git remote.",
		examples: ["dss plugin push my-plugin",],
	},
	pull: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, PULL_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("pull", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "pullRebase",),
					identifiers: { pluginId, },
				},);
			}
			const result = await c.plugins.pull(pluginId,);
			return { pulled: pluginId, result, };
		},
		description: "Pull (and rebase) the development plugin's content from its Git remote.",
		examples: ["dss plugin pull my-plugin",],
	},
	fetch: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, FETCH_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("fetch", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "fetch",),
					identifiers: { pluginId, },
				},);
			}
			const result = await c.plugins.fetch(pluginId,);
			return { fetched: pluginId, result, };
		},
		description: "Fetch the development plugin's content from its Git remote without changing files.",
		examples: ["dss plugin fetch my-plugin",],
	},
	"reset-local": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, RESET_LOCAL_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("reset-local", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "resetToLocalHeadState",),
					identifiers: { pluginId, },
				},);
			}
			const result = await c.plugins.resetToLocalHeadState(pluginId,);
			return { reset: "localHeadState", pluginId, result, };
		},
		description:
			"Reset the development plugin to its local HEAD state, discarding uncommitted edits.",
		examples: ["dss plugin reset-local my-plugin",],
	},
	"reset-remote": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, RESET_REMOTE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("reset-remote", {
					method: "POST",
					endpoint: pluginActionEndpoint(pluginId, "resetToRemoteHeadState",),
					identifiers: { pluginId, },
				},);
			}
			const result = await c.plugins.resetToRemoteHeadState(pluginId,);
			return { reset: "remoteHeadState", pluginId, result, };
		},
		description:
			"Reset the development plugin to the remote HEAD state, discarding local work. The remote must be declared first.",
		examples: ["dss plugin reset-remote my-plugin",],
	},
	"contents-list": {
		handler: (c, a,) => {
			requireArgs(a, 1, CONTENTS_LIST_USAGE,);
			return c.plugins.listContents(validatePluginId(a[0],),);
		},
		description: "List the file tree of a development plugin.",
		examples: ["dss plugin contents-list my-plugin",],
	},
	"contents-get": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, CONTENTS_GET_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			const out = f["output"];
			if (typeof out === "string" && out.trim() !== "") {
				const res = await c.plugins.downloadFile(pluginId, path,);
				const bytes = await writeResponseToFile(out, res,);
				return { path: out, bytes, };
			}
			return { data: await c.plugins.getFile(pluginId, path,), };
		},
		description:
			"Read a development plugin file as text. --output PATH writes the raw bytes to a local file instead.",
		examples: [
			"dss plugin contents-get my-plugin python/my-plugin.py",
			"dss plugin contents-get my-plugin static/logo.png --output ./logo.png",
		],
	},
	"contents-put": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, CONTENTS_PUT_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			if (executionMode(f,).dryRun) {
				const source = typeof f["file"] === "string"
					? { contentSource: "file", file: f["file"] as string, }
					: typeof f["content"] === "string"
					? { contentSource: "flag", }
					: { contentSource: "stdin", };
				return pluginPlan("contents-put", {
					method: "POST",
					endpoint: pluginContentsEndpoint(pluginId, path,),
					identifiers: { pluginId, path, },
					payload: source,
				},);
			}
			const content = typeof f["file"] === "string"
				? fs.readFileSync(f["file"] as string,)
				: typeof f["content"] === "string"
				? f["content"] as string
				: await readStdinText();
			await c.plugins.putFile(pluginId, path, content,);
			const bytes = typeof content === "string" ? Buffer.byteLength(content, "utf8",) : content.length;
			return { written: path, pluginId, bytes, sha256: sha256Hex(content,), };
		},
		description:
			"Create or replace a development plugin file with text or binary content; reports the written byte count and sha256.",
		examples: [
			"dss plugin contents-put my-plugin python/lib.py --file ./lib.py",
			"dss plugin contents-put my-plugin python/lib.py --content 'print(1)'",
		],
	},
	"contents-delete": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, CONTENTS_DELETE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("contents-delete", {
					method: "DELETE",
					endpoint: pluginContentsEndpoint(pluginId, path,),
					identifiers: { pluginId, path, },
				},);
			}
			await c.plugins.deleteFile(pluginId, path,);
			return { deleted: path, pluginId, };
		},
		description: "Delete a file (or folder) from a development plugin.",
		examples: ["dss plugin contents-delete my-plugin python/old.py",],
	},
	details: {
		handler: (c, a,) => {
			requireArgs(a, 2, DETAILS_USAGE,);
			return c.plugins.getFileDetails(validatePluginId(a[0],), validatePluginPath(a[1],),);
		},
		description:
			"Get a single development plugin file's detail record (size, mime type, timestamps).",
		examples: ["dss plugin details my-plugin python/my-plugin.py",],
	},
	"folder-add": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, FOLDER_ADD_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("folder-add", {
					method: "POST",
					endpoint: pluginFoldersEndpoint(pluginId, path,),
					identifiers: { pluginId, path, },
				},);
			}
			await c.plugins.addFolder(pluginId, path,);
			return { created: path, pluginId, kind: "folder", };
		},
		description: "Add a folder to a development plugin.",
		examples: ["dss plugin folder-add my-plugin python/mylib",],
	},
	rename: {
		handler: async (c, a, f,) => {
			requireArgs(a, 3, RENAME_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			const newName = validatePluginName(a[2],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("rename", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/contents-actions/rename",),
					identifiers: { pluginId, path, newName, },
					payload: { oldPath: `/${path}`, newName, },
				},);
			}
			await c.plugins.rename(pluginId, path, newName,);
			return { renamed: path, to: newName, pluginId, };
		},
		description: "Rename a file or folder inside a development plugin (same parent folder).",
		examples: ["dss plugin rename my-plugin python/old.py new.py",],
	},
	move: {
		handler: async (c, a, f,) => {
			requireArgs(a, 3, MOVE_USAGE,);
			const pluginId = validatePluginId(a[0],);
			const path = validatePluginPath(a[1],);
			const destination = validatePluginDestinationPath(a[2],);
			if (executionMode(f,).dryRun) {
				return pluginPlan("move", {
					method: "POST",
					endpoint: pluginRootEndpoint(pluginId, "/contents-actions/move",),
					identifiers: { pluginId, path, destinationFolder: destination, },
					payload: { oldPath: `/${path}`, newPath: destination, },
				},);
			}
			await c.plugins.move(pluginId, path, destination,);
			return { moved: path, to: destination, pluginId, };
		},
		description:
			"Move a file or folder inside a development plugin. The destination folder must already exist.",
		examples: ["dss plugin move my-plugin python/old.py python/mylib",],
	},
},);

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function normalizeCreationMode(value: string,): "EMPTY" | "GIT_CLONE" | "GIT_EXPORT" {
	const normalized = value.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "EMPTY" || normalized === "GIT_CLONE" || normalized === "GIT_EXPORT") {
		return normalized;
	}
	throw new UsageError(
		"--creation-mode must be one of EMPTY, GIT_CLONE, GIT_EXPORT.",
		"invalid_enum",
	);
}

interface SettingsPayload {
	config?: Record<string, unknown>;
	codeEnvName?: string;
	[key: string]: unknown;
}

function readSettingsInput(flags: Record<string, string | boolean>,): SettingsPayload {
	let text: string;
	if (typeof flags["file"] === "string") {
		text = fs.readFileSync(flags["file"] as string, "utf-8",);
	} else if (typeof flags["content"] === "string") {
		text = flags["content"];
	} else if (flags["stdin"] === true) {
		text = readStdinText();
	} else {
		throw new UsageError(
			"--content JSON, --file PATH, or --stdin is required.",
			"missing_required_flag",
		);
	}
	const parsed = JSON.parse(text,) as unknown;
	if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed,)) {
		throw new UsageError("Plugin settings must be a JSON object.", "validation_failed",);
	}
	return parsed as SettingsPayload;
}
