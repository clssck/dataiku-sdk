import { ClientValidationError, DataikuError, } from "../errors.js";
import type {
	ProjectGitActionResult,
	ProjectGitDiffResult,
	ProjectGitFutureResponse,
	ProjectGitFutureState,
	ProjectGitLibraries,
	ProjectGitLogResult,
	ProjectGitRemote,
	ProjectGitStatus,
	ProjectGitTags,
} from "../schemas.js";
import {
	ProjectGitActionResultSchema,
	ProjectGitDiffResultSchema,
	ProjectGitFutureResponseSchema,
	ProjectGitFutureStateSchema,
	ProjectGitLibrariesSchema,
	ProjectGitLogResultSchema,
	ProjectGitRemoteSchema,
	ProjectGitStatusSchema,
	ProjectGitTagsSchema,
} from "../schemas.js";
import { BaseResource, } from "./base.js";

/**
 * The per-project Git routes exist only under the dataikuapi base, not under
 * the documented `/public/api` REST prefix used by the rest of this SDK, so
 * this family carries its own mount point.
 */
const GIT_API_BASE = "/dip/publicapi";

const DEFAULT_REMOTE = "origin";
const DEFAULT_LOG_COUNT = 1_000;
const DEFAULT_TAG_REFERENCE = "HEAD";
const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_TIMEOUT_MS = 120_000;

/* ------------------------------------------------------------------ */
/*  Options                                                            */
/* ------------------------------------------------------------------ */

export interface ProjectGitSetRemoteOptions {
	url: string;
	name?: string;
}

export interface ProjectGitCreateBranchOptions {
	commit?: string;
	duplicateProject?: boolean;
	targetProjectKey?: string;
	targetProjectFolderId?: string;
}

export interface ProjectGitDeleteBranchOptions {
	remote?: boolean;
	deleteRemotely?: boolean;
	forceDelete?: boolean;
}

export interface ProjectGitCreateTagOptions {
	reference?: string;
	message?: string;
}

export interface ProjectGitPullOptions {
	branchName?: string;
}

export interface ProjectGitPushOptions {
	branchName?: string;
}

export interface ProjectGitLogOptions {
	path?: string;
	startCommit?: string;
	count?: number;
}

export interface ProjectGitDiffOptions {
	commitFrom?: string;
	commitTo?: string;
}

export interface ProjectGitCommitOptions {
	message: string;
}

/**
 * `dropAndRebuild` permanently destroys the project's Git history, so the
 * caller must acknowledge it explicitly before the request is issued.
 */
export interface ProjectGitDropAndRebuildOptions {
	confirmed?: boolean;
}

export interface ProjectGitAddLibraryOptions {
	repository: string;
	localTargetPath: string;
	checkout: string;
	pathInGitRepository?: string;
	addToPythonPath?: boolean;
	login?: string;
	password?: string;
}

export interface ProjectGitSetLibraryOptions {
	repository: string;
	pathInGitRepository?: string;
	checkout: string;
	login?: string;
	password?: string;
}

export interface ProjectGitFutureStateOptions {
	peek?: boolean;
}

export interface ProjectGitFutureWaitOptions {
	pollIntervalMs?: number;
	timeoutMs?: number;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/**
 * The Git routes authenticate with HTTP Basic (API key as the username, empty
 * password) exactly like the official Python client, so they bypass the shared
 * bearer helpers and reach the client's transport directly. Retries, TLS
 * options, timeouts, and DSS error classification still come from the client.
 */
type GitTransportClient = {
	baseUrl: string;
	apiKey: string;
	fetchWithRetry(url: string, init: RequestInit,): Promise<Response>;
};

type QueryValue = string | number | boolean | undefined;

function sleep(ms: number,): Promise<void> {
	const { promise, resolve, } = Promise.withResolvers<void>();
	setTimeout(resolve, ms,);
	return promise;
}

function encodePathSegments(path: string,): string {
	const normalized = path.replace(/^\/+/, "",).replace(/\/+$/, "",);
	if (!normalized) throw new ClientValidationError("A Git reference path is required.",);
	return normalized
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

/**
 * Reject `https://user:token@host/repo` before it reaches DSS: an embedded
 * secret would be stored in the project's Git configuration and echoed back by
 * later reads. SSH and scp-style remotes (`git@host:org/repo.git`) are the
 * documented way to authenticate and stay allowed.
 */
function validatedRepository(repository: string, field: string,): string {
	const candidate = repository.trim();
	if (candidate === "") throw new ClientValidationError(`${field} is required.`,);
	if (/[\u0000-\u001f\u007f]/u.test(candidate,)) {
		throw new ClientValidationError(`${field} must not contain control characters.`,);
	}
	if (/^https?:/i.test(candidate,)) {
		if (candidate.includes("\\",)) {
			throw new ClientValidationError(`${field} must be a valid HTTP(S) URL.`,);
		}
		if (!/^https?:\/\//i.test(candidate,)) {
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

/** Replace every exact occurrence of a secret so echoed errors cannot leak it. */
function redactSecrets(text: string, secrets: string[],): string {
	let result = text;
	for (const secret of secrets) {
		if (secret !== undefined && secret !== "") {
			result = result.split(secret,).join("[redacted]",);
		}
	}
	return result;
}

/**
 * Strip a password that a server misconfiguration may have echoed from an
 * error response. `DataikuError.message` was built at construction, so the
 * raw body, the message, and the stack all need scrubbing.
 */
function scrubErrorSecrets(error: unknown, secrets: string[],): void {
	if (!(error instanceof DataikuError) || secrets.length === 0) return;
	error.body = redactSecrets(error.body, secrets,);
	error.message = redactSecrets(error.message, secrets,);
	if (typeof error.stack === "string") {
		error.stack = redactSecrets(error.stack, secrets,);
	}
}

function describeFutureFailure(error: unknown,): string | undefined {
	if (error === undefined || error === null) return undefined;
	if (typeof error === "string") return error.trim() === "" ? undefined : error;
	if (typeof error === "object") {
		const record = error as Record<string, unknown>;
		for (const key of ["detailedMessage", "message", "errorType",]) {
			const value = record[key];
			if (typeof value === "string" && value.trim() !== "") return value;
		}
		return JSON.stringify(error,);
	}
	return String(error,);
}

/* ------------------------------------------------------------------ */
/*  Resource                                                          */
/* ------------------------------------------------------------------ */

export class ProjectGitResource extends BaseResource {
	/* ---- status & remotes ---- */

	/** Working-copy status: current branch, remotes, tracking counts, changes. */
	async status(projectKey: string,): Promise<ProjectGitStatus> {
		const raw = await this.request("GET", `${this.gitPath(projectKey,)}/status`,);
		return this.client.safeParse(ProjectGitStatusSchema, raw ?? {}, "projectGit.status",);
	}

	/**
	 * Read a Git remote. DSS answers `{}` when the remote is not configured.
	 * The remote name defaults to `origin` like the Python client.
	 */
	async getRemote(projectKey: string, name: string = DEFAULT_REMOTE,): Promise<ProjectGitRemote> {
		const raw = await this.request(
			"GET",
			`${this.gitPath(projectKey,)}/remotes/${encodeURIComponent(name,)}`,
		);
		return this.client.safeParse(ProjectGitRemoteSchema, raw ?? {}, "projectGit.getRemote",);
	}

	/** Create or replace a Git remote. */
	async setRemote(
		projectKey: string,
		options: ProjectGitSetRemoteOptions,
	): Promise<ProjectGitActionResult> {
		const repository = validatedRepository(options.url, "Git remote URL",);
		const raw = await this.request(
			"POST",
			`${this.gitPath(projectKey,)}/remotes/${encodeURIComponent(options.name ?? DEFAULT_REMOTE,)}`,
			{ url: repository, },
		);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.setRemote",);
	}

	/** Delete a Git remote. */
	async removeRemote(projectKey: string, name: string = DEFAULT_REMOTE,): Promise<void> {
		await this.request(
			"DELETE",
			`${this.gitPath(projectKey,)}/remotes/${encodeURIComponent(name,)}`,
		);
	}

	/* ---- branches ---- */

	/** Plain list of branch names, local by default. */
	async listBranches(projectKey: string, remote: boolean = false,): Promise<string[]> {
		const raw = await this.request(
			"GET",
			`${this.gitPath(projectKey,)}/branches${this.query({ remote, },)}`,
		);
		return (raw ?? []) as string[];
	}

	/**
	 * Create a branch and switch to it. With `duplicateProject`, DSS creates a
	 * duplicate project for the branch instead of moving this one.
	 */
	async createBranch(
		projectKey: string,
		name: string,
		options: ProjectGitCreateBranchOptions = {},
	): Promise<ProjectGitActionResult> {
		// DSS consumes the full key set, so absent values travel as nulls.
		const raw = await this.request("POST", `${this.gitPath(projectKey,)}/branches/`, {
			name,
			commit: options.commit ?? null,
			duplicateProject: options.duplicateProject ?? false,
			targetProjectKey: options.targetProjectKey ?? null,
			targetProjectFolderId: options.targetProjectFolderId ?? null,
		},);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.createBranch",
		);
	}

	/** Delete a branch locally and, with `deleteRemotely`, on the remote too. */
	async deleteBranch(
		projectKey: string,
		name: string,
		options: ProjectGitDeleteBranchOptions = {},
	): Promise<ProjectGitActionResult> {
		const raw = await this.request("POST", `${this.actionPath(projectKey,)}/deleteBranch`, {
			name,
			remote: options.remote ?? false,
			deleteRemotely: options.deleteRemotely ?? false,
			forceDelete: options.forceDelete ?? false,
		},);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.deleteBranch",
		);
	}

	/** Current branch name, or null when DSS reports none. */
	async currentBranch(projectKey: string,): Promise<string | null> {
		const raw = await this.request("GET", `${this.gitPath(projectKey,)}/current-branch`,);
		const name = (raw as { name?: unknown; } | undefined)?.name;
		return typeof name === "string" && name !== "" ? name : null;
	}

	/** Switch the project to another branch. */
	async switchBranch(projectKey: string, branchName: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/switchBranch${this.query({ branchName, },)}`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.switchBranch",
		);
	}

	/* ---- tags ---- */

	async listTags(projectKey: string,): Promise<ProjectGitTags> {
		const raw = await this.request("GET", `${this.gitPath(projectKey,)}/tags`,);
		return this.client.safeParse(ProjectGitTagsSchema, raw ?? [], "projectGit.listTags",);
	}

	async createTag(
		projectKey: string,
		name: string,
		options: ProjectGitCreateTagOptions = {},
	): Promise<ProjectGitActionResult> {
		const raw = await this.request("POST", `${this.gitPath(projectKey,)}/tags/`, {
			name,
			reference: options.reference ?? DEFAULT_TAG_REFERENCE,
			message: options.message ?? "",
		},);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.createTag",);
	}

	async deleteTag(projectKey: string, name: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request("POST", `${this.actionPath(projectKey,)}/deleteTag`, {
			name,
		},);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.deleteTag",);
	}

	/* ---- synchronous Git actions ---- */

	/** Fetch from the remote. Failures arrive as `{success: false}` with HTTP 200. */
	async fetch(projectKey: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request("POST", `${this.actionPath(projectKey,)}/fetch`,);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.fetch",);
	}

	/** Pull. DSS only ever exposes a rebase pull; there is no merge variant. */
	async pull(
		projectKey: string,
		options: ProjectGitPullOptions = {},
	): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/pullRebase${this.query({ branchName: options.branchName, },)}`,
		);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.pull",);
	}

	async push(
		projectKey: string,
		options: ProjectGitPushOptions = {},
	): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/push${this.query({ branchName: options.branchName, },)}`,
		);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.push",);
	}

	/** Commit log page. Paginate by passing the previous `nextCommit` as `startCommit`. */
	async log(projectKey: string, options: ProjectGitLogOptions = {},): Promise<ProjectGitLogResult> {
		const raw = await this.request(
			"GET",
			`${this.actionPath(projectKey,)}/log${
				this.query({
					path: options.path,
					startCommit: options.startCommit,
					count: options.count ?? DEFAULT_LOG_COUNT,
				},)
			}`,
		);
		return this.client.safeParse(
			ProjectGitLogResultSchema,
			raw ?? { entries: [], },
			"projectGit.log",
		);
	}

	async diff(
		projectKey: string,
		options: ProjectGitDiffOptions = {},
	): Promise<ProjectGitDiffResult> {
		const raw = await this.request(
			"GET",
			`${this.actionPath(projectKey,)}/diff${
				this.query({ commitFrom: options.commitFrom, commitTo: options.commitTo, },)
			}`,
		);
		return this.client.safeParse(ProjectGitDiffResultSchema, raw ?? {}, "projectGit.diff",);
	}

	/** Commit the project. DSS stages untracked files before committing. */
	async commit(
		projectKey: string,
		options: ProjectGitCommitOptions,
	): Promise<ProjectGitActionResult> {
		const raw = await this.request("POST", `${this.actionPath(projectKey,)}/commit`, {
			message: options.message,
		},);
		return this.client.safeParse(ProjectGitActionResultSchema, raw ?? {}, "projectGit.commit",);
	}

	/** Restore the project to the state of a past commit. */
	async revertToRevision(
		projectKey: string,
		commit: string,
	): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/revertToRevision${this.query({ commit, },)}`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.revertToRevision",
		);
	}

	/** Revert one commit, keeping later history. */
	async revertCommit(projectKey: string, commit: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/revertCommit${this.query({ commit, },)}`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.revertCommit",
		);
	}

	/** Hard reset to local HEAD, dropping uncommitted changes. */
	async resetToHead(projectKey: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/resetToLocalHeadState`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.resetToHead",
		);
	}

	/** Hard reset to the tracked upstream branch, dropping local commits. */
	async resetToUpstream(projectKey: string,): Promise<ProjectGitActionResult> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/resetToRemoteHeadState`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.resetToUpstream",
		);
	}

	/**
	 * Wipe the project's entire Git history and rebuild a fresh repository.
	 * Irreversible, so nothing is sent unless the caller acknowledges it.
	 */
	async dropAndRebuild(
		projectKey: string,
		options: ProjectGitDropAndRebuildOptions = {},
	): Promise<ProjectGitActionResult> {
		if (options.confirmed !== true) {
			throw new ClientValidationError(
				"projectGit.dropAndRebuild permanently destroys the project's Git history. "
					+ "Pass { confirmed: true } to acknowledge before calling it.",
			);
		}
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/dropAndRebuild${this.query({ iKnowWhatIAmDoing: true, },)}`,
		);
		return this.client.safeParse(
			ProjectGitActionResultSchema,
			raw ?? {},
			"projectGit.dropAndRebuild",
		);
	}

	/* ---- external libraries (async futures) ---- */

	async listLibraries(projectKey: string,): Promise<ProjectGitLibraries> {
		const raw = await this.request("GET", `${this.libraryPath(projectKey,)}/`,);
		return this.client.safeParse(
			ProjectGitLibrariesSchema,
			raw ?? [],
			"projectGit.listLibraries",
		);
	}

	/** Attach an external Git library. Runs as a DSS future. */
	async addLibrary(
		projectKey: string,
		options: ProjectGitAddLibraryOptions,
	): Promise<ProjectGitFutureResponse> {
		const repository = validatedRepository(options.repository, "Git library repository URL",);
		const secrets = [options.password,].filter(
			(secret,): secret is string => secret !== undefined && secret !== "",
		);
		const raw = await this.request("POST", `${this.libraryPath(projectKey,)}/`, {
			repository,
			login: options.login ?? null,
			password: options.password ?? null,
			pathInGitRepository: options.pathInGitRepository ?? "",
			localTargetPath: options.localTargetPath,
			checkout: options.checkout,
			addToPythonPath: options.addToPythonPath ?? true,
		}, secrets,);
		return this.client.safeParse(
			ProjectGitFutureResponseSchema,
			raw ?? {},
			"projectGit.addLibrary",
		);
	}

	/** Update an attached library's repository, sub-path, or checkout. */
	async setLibrary(
		projectKey: string,
		gitReferencePath: string,
		options: ProjectGitSetLibraryOptions,
	): Promise<string> {
		const repository = validatedRepository(options.repository, "Git library repository URL",);
		const secrets = [options.password,].filter(
			(secret,): secret is string => secret !== undefined && secret !== "",
		);
		const raw = await this.request(
			"PUT",
			`${this.libraryPath(projectKey,)}/${encodePathSegments(gitReferencePath,)}`,
			{
				repository,
				login: options.login ?? null,
				password: options.password ?? null,
				pathInGitRepository: options.pathInGitRepository ?? "",
				checkout: options.checkout,
			},
			secrets,
		);
		return raw as string;
	}

	/** Detach a library, optionally deleting its local directory. */
	async removeLibrary(
		projectKey: string,
		gitReferencePath: string,
		deleteDirectory: boolean = false,
	): Promise<void> {
		await this.request(
			"DELETE",
			`${this.libraryPath(projectKey,)}/${encodePathSegments(gitReferencePath,)}${
				this.query({ deleteDirectory, },)
			}`,
		);
	}

	/** Reset one library to its remote state. Runs as a DSS future. */
	async resetLibrary(
		projectKey: string,
		gitReferencePath: string,
	): Promise<ProjectGitFutureResponse> {
		const raw = await this.request("POST", `${this.libraryPath(projectKey,)}/action/reset`, {
			gitRef: gitReferencePath,
		},);
		return this.client.safeParse(
			ProjectGitFutureResponseSchema,
			raw ?? {},
			"projectGit.resetLibrary",
		);
	}

	/** Commit and push one library. Runs as a DSS future. */
	async pushLibrary(
		projectKey: string,
		gitReferencePath: string,
		commitMessage: string,
	): Promise<ProjectGitFutureResponse> {
		const raw = await this.request("POST", `${this.libraryPath(projectKey,)}/action/push`, {
			gitRef: gitReferencePath,
			commitMessage,
		},);
		return this.client.safeParse(
			ProjectGitFutureResponseSchema,
			raw ?? {},
			"projectGit.pushLibrary",
		);
	}

	/** Commit and push every attached library. Runs as a DSS future. */
	async pushAllLibraries(
		projectKey: string,
		commitMessage: string,
	): Promise<ProjectGitFutureResponse> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/git-refs/push-all`,
			{ commitMessage, },
		);
		return this.client.safeParse(
			ProjectGitFutureResponseSchema,
			raw ?? {},
			"projectGit.pushAllLibraries",
		);
	}

	/** Reset every attached library to its remote state. Runs as a DSS future. */
	async resetAllLibraries(projectKey: string,): Promise<ProjectGitFutureResponse> {
		const raw = await this.request(
			"POST",
			`${this.actionPath(projectKey,)}/git-refs/reset-all`,
		);
		return this.client.safeParse(
			ProjectGitFutureResponseSchema,
			raw ?? {},
			"projectGit.resetAllLibraries",
		);
	}

	/* ---- futures ---- */

	/**
	 * Read a future's state. `peek` leaves the future's log unconsumed, which is
	 * what polling loops want; the default consumes it like the Python client.
	 */
	async getFutureState(
		jobId: string,
		options: ProjectGitFutureStateOptions = {},
	): Promise<ProjectGitFutureState> {
		const raw = await this.request(
			"GET",
			`/futures/${encodeURIComponent(jobId,)}${this.query({ peek: options.peek === true, },)}`,
		);
		return this.client.safeParse(
			ProjectGitFutureStateSchema,
			raw ?? {},
			"projectGit.getFutureState",
		);
	}

	/** Abort a running future. */
	async abortFuture(jobId: string,): Promise<void> {
		await this.request("DELETE", `/futures/${encodeURIComponent(jobId,)}`,);
	}

	/**
	 * Poll until the future carries a result and return that result. Throws on
	 * an aborted or failed future and on an exhausted budget, so a caller never
	 * mistakes an unfinished future for a successful one.
	 */
	async waitForFuture(jobId: string, options: ProjectGitFutureWaitOptions = {},): Promise<unknown> {
		const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
		const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
		if (!Number.isFinite(pollIntervalMs,) || pollIntervalMs <= 0) {
			throw new ClientValidationError("pollIntervalMs must be a finite positive number.",);
		}
		if (!Number.isFinite(timeoutMs,) || timeoutMs < 0) {
			throw new ClientValidationError("timeoutMs must be a finite non-negative number.",);
		}
		const startedAt = Date.now();

		while (true) {
			// Unlike peek state, the ordinary state includes the completed result.
			const state = await this.getFutureState(jobId, { peek: false, },);

			const failure = describeFutureFailure(state.error,);
			if (failure !== undefined) {
				throw new Error(`Dataiku future ${jobId} failed: ${failure}`,);
			}
			if (state.aborted === true) {
				throw new Error(`Dataiku future ${jobId} was aborted`,);
			}
			if (state.hasResult === true) return state.result;
			if (state.unknown === true) {
				throw new Error(`Dataiku future ${jobId} is unknown to the server`,);
			}
			if (state.alive === false) {
				throw new Error(`Dataiku future ${jobId} ended without producing a result`,);
			}

			// Only the remaining budget is slept, so a caller's deadline is never
			// overshot by a whole poll interval.
			const elapsedMs = Date.now() - startedAt;
			if (elapsedMs >= timeoutMs) {
				throw new Error(
					`Timed out after ${String(elapsedMs,)}ms waiting for Dataiku future ${jobId}`,
				);
			}
			await sleep(Math.min(pollIntervalMs, timeoutMs - elapsedMs,),);
		}
	}

	/* ---- private ---- */

	private gitPath(projectKey: string,): string {
		return `/projects/${this.enc(projectKey,)}/git`;
	}

	private actionPath(projectKey: string,): string {
		return `${this.gitPath(projectKey,)}/actions`;
	}

	private libraryPath(projectKey: string,): string {
		return `${this.gitPath(projectKey,)}/lib-git-refs`;
	}

	private query(params: Record<string, QueryValue>,): string {
		const search = new URLSearchParams();
		for (const [key, value,] of Object.entries(params,)) {
			if (value === undefined) continue;
			search.set(key, String(value,),);
		}
		const queryString = search.toString();
		return queryString === "" ? "" : `?${queryString}`;
	}

	private async request(
		method: string,
		path: string,
		body?: unknown,
		secrets: string[] = [],
	): Promise<unknown> {
		const transport = this.client as unknown as GitTransportClient;
		const init: RequestInit = {
			method,
			headers: {
				// The Git routes are only reachable with dataikuapi-style Basic auth:
				// API key as the username, empty password.
				Authorization: `Basic ${Buffer.from(`${transport.apiKey}:`, "utf8",).toString("base64",)}`,
				Accept: "application/json",
				"Content-Type": "application/json",
			},
		};
		if (body !== undefined) init.body = JSON.stringify(body,);

		try {
			const res = await transport.fetchWithRetry(`${transport.baseUrl}${GIT_API_BASE}${path}`, init,);
			const text = await res.text();
			if (text.trim() === "") return undefined;
			try {
				return JSON.parse(text,) as unknown;
			} catch {
				throw new Error(`Dataiku Git API returned a non-JSON response for ${method} ${path}`,);
			}
		} catch (error) {
			scrubErrorSecrets(error, secrets,);
			throw error;
		}
	}
}
