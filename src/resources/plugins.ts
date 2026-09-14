import { ClientValidationError, DataikuError, } from "../errors.js";
import { BaseResource, requireNonEmpty, } from "./base.js";
import { validateGitReferencePath, } from "./project-git.js";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

/** Summary entry of `/plugins/` as documented (id/version/isDev/meta). */
export interface PluginSummary {
	id: string;
	version?: string;
	isDev?: boolean;
	meta?: {
		label?: string;
		description?: string;
		/** Opaque server-authored metadata (icon, author, tags, ...). */
		[key: string]: unknown;
	};
	/** Opaque server fields are preserved. */
	[key: string]: unknown;
}

/** Settings of a plugin (config dict + optional code env name). */
export interface PluginSettings {
	config?: Record<string, unknown>;
	codeEnvName?: string;
	[key: string]: unknown;
}

/** One usage entry of a plugin element, as returned by listUsages. */
export interface PluginUsage {
	elementKind?: string;
	elementType?: string;
	objectId?: string;
	objectType?: string;
	projectKey?: string;
	[key: string]: unknown;
}

/** One missing-type entry of a plugin usage analysis. */
export interface PluginMissingType {
	missingType?: string;
	objectId?: string;
	objectType?: string;
	projectKey?: string;
	[key: string]: unknown;
}

export interface PluginUsageReport {
	usages?: PluginUsage[];
	missingTypes?: PluginMissingType[];
	[key: string]: unknown;
}

/** Contents-tree item of a dev plugin (mirrors the library listing shape). */
export interface PluginContentItem {
	name: string;
	/** Full plugin-relative path as observed from DSS contents listings. */
	path?: string;
	mimeType?: string;
	size?: number;
	hasData?: boolean;
	lastModified?: number;
	children?: PluginContentItem[];
	[key: string]: unknown;
}

/** Detail record of a single plugin file (`/plugins/{id}/details/{path}`). */
export interface PluginFileDetails {
	name?: string;
	path?: string;
	mimeType?: string;
	size?: number;
	hasData?: boolean;
	readOnly?: boolean;
	lastModified?: number;
	[key: string]: unknown;
}

/** Git remote info of a dev plugin (`repositoryUrl` is null when unset). */
export interface PluginGitRemote {
	repositoryUrl: string | null;
	[key: string]: unknown;
}

/** Options for creating a development plugin. */
export interface PluginCreateDevOptions {
	pluginId: string;
	creationMode: "EMPTY" | "GIT_CLONE" | "GIT_EXPORT";
	gitRepository?: string;
	gitCheckout?: string;
	gitSubpath?: string;
}

/** Options for installing/updating a plugin from Git. */
export interface PluginGitInstallOptions {
	gitRepositoryUrl: string;
	gitCheckout?: string;
	gitSubpath?: string;
}

/** Options for creating the code env of a plugin. */
export interface PluginCodeEnvCreateOptions {
	conda?: boolean;
	pythonInterpreter?: string;
}

/** Scope selectors for plugin settings and usage analysis. */
export interface PluginProjectScopeOptions {
	projectKey?: string;
}

/* ------------------------------------------------------------------ */
/*  Path helpers                                                       */
/* ------------------------------------------------------------------ */

const PLUGIN_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;

/**
 * Validate a plugin identifier. Plugin ids select the API route segment, so a
 * hostile id must not be able to change which resource the URL addresses.
 */
export function validatePluginId(pluginId: string,): string {
	requireNonEmpty(pluginId, "pluginId",);
	if (!PLUGIN_ID_PATTERN.test(pluginId,)) {
		throw new ClientValidationError(
			`Invalid plugin id: ids use letters, digits, '-', '_', and '.', and start with a letter or digit.`,
			"validation_failed",
			undefined,
			{ pluginId, },
		);
	}
	return pluginId;
}

function pluginPathError(path: string, message: string,): ClientValidationError {
	return new ClientValidationError(
		message,
		"validation_failed",
		"Use a plugin path relative to the plugin root: folders separated by '/', each segment a simple file or folder name.",
		{ path, },
	);
}

const CONTROL_CHARACTER_RE = /[\u0000-\u001f\u007f]/;
const BACKSLASH_RE = /\\/;
const TRAILING_AMBIGUOUS_RE = /[\s.]$/;
const WHITESPACE_ONLY_RE = /^\s+$/;

/**
 * Validate and canonicalize a path inside a plugin. Mirrors the project
 * library validator: rejects empty paths, control characters, backslashes,
 * empty segments, '.'/'..' traversals, whitespace-only or edge-whitespace
 * segments, and trailing dot/space segments.
 *
 * Returns the canonical path with any leading '/' stripped (the contents
 * endpoints take paths relative to the plugin root).
 */
export function validatePluginPath(path: string,): string {
	if (typeof path !== "string" || path.trim() === "") {
		throw pluginPathError(path, "A plugin path is required.",);
	}
	if (CONTROL_CHARACTER_RE.test(path,)) {
		throw pluginPathError(path, "Plugin path must not contain control characters.",);
	}
	const normalized = path.replace(/^\/+/, "",);
	if (normalized === "") {
		throw pluginPathError(path, "Plugin path must name a file or folder below the plugin root.",);
	}
	for (const segment of normalized.split("/",)) {
		if (segment === "") {
			throw pluginPathError(
				path,
				"Plugin path must not contain empty segments (leading, trailing, or doubled '/').",
			);
		}
		if (segment === "." || segment === "..") {
			throw pluginPathError(path, "Plugin path must not contain '.' or '..' segments.",);
		}
		if (BACKSLASH_RE.test(segment,)) {
			throw pluginPathError(path, "Plugin path must not contain backslashes.",);
		}
		if (segment !== segment.trim() || WHITESPACE_ONLY_RE.test(segment,)) {
			throw pluginPathError(
				path,
				"Plugin path segments must not be whitespace-only, or start or end with whitespace.",
			);
		}
		if (TRAILING_AMBIGUOUS_RE.test(segment,)) {
			throw pluginPathError(
				path,
				"Plugin path must not contain segments with a trailing space or dot (ambiguous across filesystem normalizations).",
			);
		}
	}
	return normalized;
}

/**
 * Validate a destination folder for a plugin contents move. '/' (the plugin
 * root) is valid; any other path must pass {@link validatePluginPath}.
 * Returns the canonical destination ('/' or '/<path>').
 */
export function validatePluginDestinationPath(path: string,): string {
	if (path === "/") return "/";
	return `/${validatePluginPath(path,)}`;
}

/** Validate a rename target: a single plugin path segment, no '/'. */
export function validatePluginName(name: string,): string {
	if (typeof name !== "string" || name.trim() === "") {
		throw new ClientValidationError(
			"Plugin rename target must be a non-empty string.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (name.includes("/",)) {
		throw new ClientValidationError(
			"Plugin rename target must be a single segment without '/'.",
			"validation_failed",
			"Rename within the current parent folder; use the move operation to change folders.",
			{ newName: name, },
		);
	}
	if (name === "." || name === "..") {
		throw new ClientValidationError(
			"Plugin rename target must not be '.' or '..'.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (BACKSLASH_RE.test(name,)) {
		throw new ClientValidationError(
			"Plugin rename target must not contain backslashes.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (name !== name.trim() || WHITESPACE_ONLY_RE.test(name,) || TRAILING_AMBIGUOUS_RE.test(name,)) {
		throw new ClientValidationError(
			"Plugin rename target must not start or end with whitespace or a trailing dot.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	return name;
}

/** Validate and URL-encode each segment of a plugin path for endpoint use. */
export function encodePluginPath(path: string,): string {
	const normalized = validatePluginPath(path,);
	return normalized
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

/** URL-encode a plugin id after validation. */
function enc(pluginId: string,): string {
	return encodeURIComponent(validatePluginId(pluginId,),);
}

/* ------------------------------------------------------------------ */
/*  Resource                                                           */
/* ------------------------------------------------------------------ */

/**
 * The documented Plugins REST section (DSS 15): instance-level plugin
 * administration under `/public/api/plugins/...`. All endpoints require admin
 * or plugin-developer privileges server-side.
 */
export class PluginsResource extends BaseResource {
	/* ---- listing & lifecycle ---- */

	/** List installed plugins (including dev plugins). */
	async list(): Promise<PluginSummary[]> {
		return this.client.get<PluginSummary[]>("/public/api/plugins/",);
	}

	/**
	 * Install a plugin from a local zip file. Fails if already installed.
	 * Resolves only after the installation completed: when DSS answers with a
	 * future receipt the future is settled before returning.
	 */
	async installFromZip(filePath: string,): Promise<void> {
		const receipt = await this.client.uploadJson<Record<string, unknown>>(
			"/public/api/plugins/actions/installFromZip",
			filePath,
		);
		await this.settleActionFuture(receipt, "installFromZip",);
	}

	/** Install a plugin from the Dataiku store. Fails if already installed. */
	async installFromStore(pluginId: string,): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			"/public/api/plugins/actions/installFromStore",
			{ pluginId: validatePluginId(pluginId,), },
		);
		await this.settleActionFuture(receipt, "installFromStore",);
	}

	/** Install a plugin by checking out a Git repository. Fails if already installed. */
	async installFromGit(options: PluginGitInstallOptions,): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			"/public/api/plugins/actions/installFromGit",
			{
				gitRepositoryUrl: validatedPluginGitUrl(options.gitRepositoryUrl,),
				gitCheckout: options.gitCheckout ?? null,
				gitSubpath: options.gitSubpath ?? null,
			},
		);
		await this.settleActionFuture(receipt, "installFromGit",);
	}

	/** Download a development plugin as a zip archive. */
	async download(pluginId: string,): Promise<Response> {
		return this.client.stream(
			`/public/api/plugins/${enc(pluginId,)}/download`,
		);
	}

	/**
	 * Update a plugin from a local zip file. Fails if not already installed.
	 * Resolves only after the update completed (future receipt settled).
	 */
	async updateFromZip(pluginId: string, filePath: string,): Promise<void> {
		const receipt = await this.client.uploadJson<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/actions/updateFromZip`,
			filePath,
		);
		await this.settleActionFuture(receipt, "updateFromZip",);
	}

	/** Update a plugin from the Dataiku Store. */
	async updateFromStore(pluginId: string,): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/actions/updateFromStore`,
			{},
		);
		await this.settleActionFuture(receipt, "updateFromStore",);
	}

	/**
	 * Update a plugin from Git. Note the documented route lives at
	 * `/plugins/actions/updateFromGit` (no pluginId in the path); the plugin
	 * is selected server-side by the zip/checkout's plugin.json. Resolves
	 * only after the update completed (future receipt settled).
	 */
	async updateFromGit(options: PluginGitInstallOptions,): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			"/public/api/plugins/actions/updateFromGit",
			{
				gitRepositoryUrl: validatedPluginGitUrl(options.gitRepositoryUrl,),
				gitCheckout: options.gitCheckout ?? null,
				gitSubpath: options.gitSubpath ?? null,
			},
		);
		await this.settleActionFuture(receipt, "updateFromGit",);
	}

	/**
	 * Move an installed plugin to the development environment. Resolves only
	 * after the move completed (future receipt settled).
	 */
	async moveToDev(pluginId: string,): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/actions/moveToDev`,
			{},
		);
		await this.settleActionFuture(receipt, "moveToDev",);
	}

	/**
	 * Delete a plugin; refuses when usages exist unless `force` is set.
	 * Resolves only after the deletion completed: DSS answers this route with
	 * a future, so reporting success before the future settles would leave
	 * the plugin briefly present after the call returned.
	 */
	async delete(pluginId: string, options: { force?: boolean; } = {},): Promise<void> {
		const receipt = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/actions/delete`,
			{ force: options.force === true, },
		);
		await this.settleActionFuture(receipt, "delete",);
	}

	/* ---- settings ---- */

	/** Get plugin settings (optionally scoped to a project). */
	async getSettings(
		pluginId: string,
		options: PluginProjectScopeOptions = {},
	): Promise<PluginSettings> {
		const query = scopeQuery(options.projectKey,);
		return this.client.get<PluginSettings>(
			`/public/api/plugins/${enc(pluginId,)}/settings${query}`,
		);
	}

	/**
	 * Replace plugin settings. DSS expects a settings object previously
	 * obtained through {@link getSettings}; `config` and `codeEnvName` are
	 * the documented fields and any extra keys the caller read are forwarded.
	 */
	async setSettings(
		pluginId: string,
		settings: PluginSettings,
		options: PluginProjectScopeOptions = {},
	): Promise<void> {
		if (settings === null || typeof settings !== "object" || Array.isArray(settings,)) {
			throw new ClientValidationError(
				"Plugin settings must be an object (use the object returned by plugins.getSettings).",
			);
		}
		await this.client.post(
			`/public/api/plugins/${enc(pluginId,)}/settings${scopeQuery(options.projectKey,)}`,
			settings,
		);
	}

	/* ---- usages ---- */

	/** List usages of the plugin's elements, optionally scoped to a project. */
	async listUsages(
		pluginId: string,
		options: PluginProjectScopeOptions = {},
	): Promise<PluginUsageReport> {
		const raw = await this.client.get<PluginUsageReport>(
			`/public/api/plugins/${enc(pluginId,)}/actions/listUsages${scopeQuery(options.projectKey,)}`,
		);
		return raw ?? {};
	}

	/* ---- code env ---- */

	/**
	 * Create the code env of a plugin. Returns the DSS future handle
	 * (`{jobId}`). The request carries the plugin-managed deployment mode and
	 * an explicit interpreter (null = the plugin's default), exactly as the
	 * official Python client sends it.
	 */
	async createCodeEnv(
		pluginId: string,
		options: PluginCodeEnvCreateOptions = {},
	): Promise<{ jobId: string; } & Record<string, unknown>> {
		const raw = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/code-env/actions/create`,
			{
				deploymentMode: "PLUGIN_MANAGED",
				conda: options.conda === true,
				pythonInterpreter: options.pythonInterpreter ?? null,
			},
		);
		return requireJobId(raw, "plugins.createCodeEnv",);
	}

	/** Update the code env of a plugin. Returns the DSS future handle. */
	async updateCodeEnv(pluginId: string,): Promise<{ jobId: string; } & Record<string, unknown>> {
		const raw = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/code-env/actions/update`,
			{},
		);
		return requireJobId(raw, "plugins.updateCodeEnv",);
	}

	/* ---- development plugins ---- */

	/** Create a new development plugin (empty, cloned, or exported from Git). */
	async createDev(options: PluginCreateDevOptions,): Promise<void> {
		if (
			options.creationMode !== "EMPTY" && options.creationMode !== "GIT_CLONE"
			&& options.creationMode !== "GIT_EXPORT"
		) {
			throw new ClientValidationError(
				"creationMode must be one of EMPTY, GIT_CLONE, GIT_EXPORT.",
			);
		}
		const needsGit = options.creationMode !== "EMPTY";
		if (needsGit && !options.gitRepository?.trim()) {
			throw new ClientValidationError(
				`gitRepository is required when creationMode is ${options.creationMode}.`,
			);
		}
		await this.client.post("/public/api/plugins/actions/createDev", {
			pluginId: validatePluginId(options.pluginId,),
			creationMode: options.creationMode,
			gitRepository: needsGit ? validatedPluginGitUrl(options.gitRepository!,) : null,
			gitCheckout: needsGit ? options.gitCheckout ?? null : null,
			gitSubpath: options.creationMode === "GIT_EXPORT" ? options.gitSubpath ?? null : null,
		},);
	}

	/* ---- dev plugin Git remote ---- */

	/** Get the Git remote declared for a dev plugin (null when unset). */
	async getGitRemote(pluginId: string,): Promise<PluginGitRemote> {
		const raw = await this.client.get<PluginGitRemote>(
			`/public/api/plugins/${enc(pluginId,)}/gitRemote`,
		);
		if (raw === undefined || raw === null) return { repositoryUrl: null, };
		if (typeof raw !== "object" || Array.isArray(raw,)) {
			return { repositoryUrl: null, ...wrapUnknown(raw,), };
		}
		const record = raw as Record<string, unknown>;
		return {
			repositoryUrl: typeof record.repositoryUrl === "string" ? record.repositoryUrl : null,
			...record,
		};
	}

	/** Set (create or replace) the Git remote of a dev plugin. */
	async setGitRemote(pluginId: string, repositoryUrl: string,): Promise<PluginGitRemote> {
		const validated = validatedPluginGitUrl(repositoryUrl,);
		const raw = await this.client.post<PluginGitRemote>(
			`/public/api/plugins/${enc(pluginId,)}/gitRemote`,
			{ repositoryUrl: validated, },
		);
		return raw ?? { repositoryUrl: validated, };
	}

	/** Delete the Git remote declared for a dev plugin. */
	async deleteGitRemote(pluginId: string,): Promise<void> {
		await this.client.del(`/public/api/plugins/${enc(pluginId,)}/gitRemote`,);
	}

	/**
	 * List the Git branches of a dev plugin. Observed DSS 15 contract: GET
	 * `/plugins/{pluginId}/gitBranches` answers `200 ["master"]`. The upstream
	 * DSS 15 REST reference documents POST for this path, but the live API
	 * answers that POST with 405 Method Not Allowed (upstream method wrong);
	 * this SDK uses the observed GET, with no POST fallback.
	 */
	async listGitBranches(pluginId: string,): Promise<string[]> {
		const raw = await this.client.get<string[]>(
			`/public/api/plugins/${enc(pluginId,)}/gitBranches`,
		);
		return Array.isArray(raw,) ? raw : [];
	}

	/** Push the dev plugin's content to its previously-declared Git remote. */
	async push(pluginId: string,): Promise<Record<string, unknown>> {
		return this.devGitAction(pluginId, "push",);
	}

	/** Pull (and rebase) the dev plugin's content from its Git remote. */
	async pull(pluginId: string,): Promise<Record<string, unknown>> {
		return this.devGitAction(pluginId, "pullRebase",);
	}

	/** Fetch the dev plugin's content from its Git remote. */
	async fetch(pluginId: string,): Promise<Record<string, unknown>> {
		return this.devGitAction(pluginId, "fetch",);
	}

	/** Reset the dev plugin to its local HEAD state (discards local edits). */
	async resetToLocalHeadState(pluginId: string,): Promise<Record<string, unknown>> {
		return this.devGitAction(pluginId, "resetToLocalHeadState",);
	}

	/** Reset the dev plugin to the remote HEAD state (discards local work). */
	async resetToRemoteHeadState(pluginId: string,): Promise<Record<string, unknown>> {
		return this.devGitAction(pluginId, "resetToRemoteHeadState",);
	}

	private async devGitAction(
		pluginId: string,
		action: string,
	): Promise<Record<string, unknown>> {
		const raw = await this.client.post<Record<string, unknown>>(
			`/public/api/plugins/${enc(pluginId,)}/actions/${action}`,
			{},
		);
		const receipt = raw ?? {};
		await this.settleActionFuture(receipt, action,);
		return receipt;
	}

	/**
	 * Settle a plugin action's future receipt. Plugin action endpoints answer
	 * with `{jobId}` when DSS processes the action asynchronously, and with an
	 * empty or already-settled body for synchronous ones — the official
	 * Python client's `DSSFuture.from_resp` accepts a missing jobId and its
	 * `wait_for_result` treats a jobless state as already complete. A jobless
	 * receipt is returned as-is (no fake wait); a jobId is polled through the
	 * shared futures waiter so callers only observe completion.
	 */
	private async settleActionFuture(
		receipt: Record<string, unknown> | undefined,
		action: string,
	): Promise<void> {
		const jobId = receipt?.["jobId"];
		if (typeof jobId !== "string" || jobId.length === 0) return;
		const waited = await this.client.futures.wait(jobId, {},);
		if (waited.state !== "DONE") {
			throw new DataikuError(
				200,
				"Unexpected Response",
				`plugin ${action} future ${jobId} ended in state ${waited.state}${
					waited.timedOut === true ? " (wait timed out)" : ""
				}`,
			);
		}
	}

	/* ---- dev plugin contents ---- */

	/** List the contents tree of a dev plugin. */
	async listContents(pluginId: string,): Promise<PluginContentItem[]> {
		return this.client.get<PluginContentItem[]>(
			`/public/api/plugins/${enc(pluginId,)}/contents/`,
		);
	}

	/**
	 * Read a dev plugin file as text. DSS answers this route with the raw file
	 * body (no JSON envelope, `dataEncoding` is not honored), so the response
	 * is read as text directly.
	 */
	async getFile(pluginId: string, path: string,): Promise<string> {
		const valid = validatePluginPath(path,);
		return await this.client.getText(this.contentsPath(pluginId, valid,),);
	}

	/** Read a dev plugin file as raw bytes (same raw-body route as getFile). */
	async getFileBytes(pluginId: string, path: string,): Promise<Uint8Array> {
		const valid = validatePluginPath(path,);
		const res = await this.client.stream(this.contentsPath(pluginId, valid,),);
		return new Uint8Array(await res.arrayBuffer(),);
	}

	/** Download a dev plugin file as a raw stream (binary-safe). */
	async downloadFile(pluginId: string, path: string,): Promise<Response> {
		return this.client.stream(this.contentsPath(pluginId, validatePluginPath(path,),),);
	}

	/** Get a single plugin file's detail record (size, mime, timestamps). */
	async getFileDetails(pluginId: string, path: string,): Promise<PluginFileDetails> {
		const valid = validatePluginPath(path,);
		const raw = await this.client.get<PluginFileDetails>(
			`/public/api/plugins/${enc(pluginId,)}/details/${encodePluginPath(valid,)}`,
		);
		return raw ?? {};
	}

	/** Upload (create or replace) a dev plugin file with text or binary content. */
	async putFile(
		pluginId: string,
		path: string,
		content: string | Uint8Array,
	): Promise<void> {
		const valid = validatePluginPath(path,);
		if (typeof content !== "string" && !(content instanceof Uint8Array)) {
			throw new ClientValidationError(
				"Plugin file content must be a string or raw bytes.",
				"validation_failed",
			);
		}
		const payload = typeof content === "string" ? Buffer.from(content, "utf8",) : content;
		await this.client.postRawBody(this.contentsPath(pluginId, valid,), payload,);
	}

	/** Delete a file (or folder) from a dev plugin. */
	async deleteFile(pluginId: string, path: string,): Promise<void> {
		await this.client.del(this.contentsPath(pluginId, validatePluginPath(path,),),);
	}

	/** Add a folder to a dev plugin. */
	async addFolder(pluginId: string, path: string,): Promise<void> {
		const valid = validatePluginPath(path,);
		await this.client.post(
			`/public/api/plugins/${enc(pluginId,)}/folders/${encodePluginPath(valid,)}`,
			{},
		);
	}

	/** Rename a file or folder inside a dev plugin (same parent folder). */
	async rename(pluginId: string, path: string, newName: string,): Promise<void> {
		const validPath = validatePluginPath(path,);
		const validName = validatePluginName(newName,);
		await this.client.post(
			`/public/api/plugins/${enc(pluginId,)}/contents-actions/rename`,
			{ oldPath: `/${validPath}`, newName: validName, },
		);
	}

	/** Move a file or folder inside a dev plugin. The destination folder must exist. */
	async move(pluginId: string, path: string, destinationFolderPath: string,): Promise<void> {
		const validPath = validatePluginPath(path,);
		const destination = validatePluginDestinationPath(destinationFolderPath,);
		await this.client.post(
			`/public/api/plugins/${enc(pluginId,)}/contents-actions/move`,
			{ oldPath: `/${validPath}`, newPath: destination, },
		);
	}

	/* ---- private ---- */

	private contentsPath(pluginId: string, path: string,): string {
		return `/public/api/plugins/${enc(pluginId,)}/contents/${encodePluginPath(path,)}`;
	}
}

/* ------------------------------------------------------------------ */
/*  Shared helpers                                                     */
/* ------------------------------------------------------------------ */

function wrapUnknown(value: unknown,): Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value,)
		? value as Record<string, unknown>
		: { value, };
}

function scopeQuery(projectKey: string | undefined,): string {
	if (projectKey === undefined) return "";
	const trimmed = projectKey.trim();
	if (trimmed === "") return "";
	return `?projectKey=${encodeURIComponent(trimmed,)}`;
}

function requireJobId(
	raw: Record<string, unknown> | undefined,
	method: string,
): { jobId: string; } & Record<string, unknown> {
	if (raw === undefined || raw === null || typeof raw.jobId !== "string" || raw.jobId === "") {
		throw new DataikuError(
			200,
			"Unexpected Response",
			`${method}: DSS did not return a future jobId.`,
		);
	}
	return raw as { jobId: string; } & Record<string, unknown>;
}

/**
 * Reject `https://user:token@host/repo` before it reaches DSS for plugin Git
 * install/update/dev-create and the dev-plugin Git remote. Same policy as the
 * project Git remotes: HTTP(S) URLs must not embed credentials; SSH and
 * scp-style remotes stay allowed.
 */
export function validatedPluginGitUrl(repository: string, field = "Git repository URL",): string {
	const candidate = repository.trim();
	if (candidate === "") throw new ClientValidationError(`${field} is required.`,);
	if (/[\u0000-\u001f\u007f]/u.test(candidate,)) {
		throw new ClientValidationError(`${field} must not contain control characters.`,);
	}
	if (/^https?:/i.test(candidate,)) {
		if (!/^https?:\/\//i.test(candidate,) || candidate.includes("\\",)) {
			throw new ClientValidationError(`${field} must be a valid HTTP(S) URL.`,);
		}
		let parsed: URL;
		try {
			parsed = new URL(candidate,);
		} catch {
			throw new ClientValidationError(`${field} must be a valid HTTP(S) URL.`,);
		}
		if (parsed.username !== "" || parsed.password !== "") {
			throw new ClientValidationError(
				`${field} must not embed credentials (user:password@host). `
					+ "Use an SSH remote or configure the Git credentials in DSS instead.",
			);
		}
	}
	return candidate;
}

/**
 * True when a value is a usable plugin id shape without throwing — used by the
 * CLI to decide which actions need a plugin id positional.
 */
export function isPluginIdCandidate(value: unknown,): boolean {
	return typeof value === "string" && PLUGIN_ID_PATTERN.test(value,);
}

// Re-exported for CLI consumers so a single import site covers path rules.
export { validateGitReferencePath, };
