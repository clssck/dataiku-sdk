import type { ProjectGitActionResult, } from "../../schemas.js";
import { num, parseBooleanOption, requiredStringFlag, } from "../coerce.js";
import { CommandResultFailure, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, UsageError, } from "../usage.js";

/** DSS names the default remote `origin`; the remote flag falls back to it. */
const DEFAULT_REMOTE = "origin";

/**
 * A Git action DSS refused is a command-level failure, not a transport failure,
 * so it exits with the generic error code rather than a transient one.
 */
const GIT_ACTION_FAILURE_EXIT_CODE = 2;

/**
 * Reject a flag passed without a usable value, so an empty `--commit` never
 * silently degrades into "no commit filter" and changes what the call means.
 */
function optionalStringFlag(
	flags: Record<string, string | boolean>,
	name: string,
): string | undefined {
	const value = flags[name];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			`--${name} requires a value.`,
			"invalid_flag_value",
			undefined,
			{ flag: `--${name}`, },
		);
	}
	return value.trim();
}
const SENSITIVE_GIT_OUTPUT_KEYS: Record<string, true> = {
	apikey: true,
	authorization: true,
	password: true,
	token: true,
};

/**
 * DSS may already contain legacy remotes with credentials in the URL, and Git
 * error text can echo them. Sanitize every server-derived Project Git value
 * before it reaches stdout or a structured command failure.
 */
function redactGitUrlUserinfo(text: string,): string {
	return text.replace(/https?:[\\/]{1,2}[^\s"'<>]+/giu, (url,) => {
		const schemeEnd = url.indexOf(":",) + 1;
		let authorityStart = schemeEnd;
		while (url[authorityStart] === "/" || url[authorityStart] === "\\") authorityStart++;
		const slash = url.slice(authorityStart,).search(/[\\/]/u,);
		const authorityEnd = slash === -1 ? url.length : authorityStart + slash;
		const at = url.lastIndexOf("@", authorityEnd - 1,);
		if (at < authorityStart) return url;
		return `${url.slice(0, authorityStart,)}[redacted]@${url.slice(at + 1,)}`;
	},);
}

function sanitizeProjectGitOutput<T,>(value: T, secrets: string[] = [],): T {
	if (typeof value === "string") {
		let sanitized = redactGitUrlUserinfo(value,);
		for (const secret of secrets) {
			if (secret !== "") sanitized = sanitized.split(secret,).join("[redacted]",);
		}
		return sanitized as T;
	}
	if (Array.isArray(value,)) {
		return value.map((item,) => sanitizeProjectGitOutput(item, secrets,)) as T;
	}
	if (value !== null && typeof value === "object") {
		const sanitized: Record<string, unknown> = {};
		for (const [key, item,] of Object.entries(value,)) {
			const normalizedKey = key.replace(/[-_]/g, "",).toLowerCase();
			sanitized[key] = SENSITIVE_GIT_OUTPUT_KEYS[normalizedKey] === true
				? "[redacted]"
				: sanitizeProjectGitOutput(item, secrets,);
		}
		return sanitized as T;
	}
	return value;
}

/**
 * Most Git actions answer HTTP 200 with `{success: false}` and the Git stderr
 * in the payload, so the transport layer sees a success. Surface the server's
 * own detail and exit non-zero instead of reporting a completed action.
 */
function assertGitActionSucceeded(
	result: ProjectGitActionResult,
	action: string,
): ProjectGitActionResult {
	const sanitized = sanitizeProjectGitOutput(result,);
	if (sanitized.success === false) {
		throw new CommandResultFailure(
			{ ...sanitized, resource: "project-git", action, },
			GIT_ACTION_FAILURE_EXIT_CODE,
			"command_result_failure",
		);
	}
	return sanitized;
}
function sanitizeProjectGitError(error: unknown,): unknown {
	if (!(error instanceof Error)) return error;
	error.message = sanitizeProjectGitOutput(error.message,);
	if (error.stack !== undefined) error.stack = sanitizeProjectGitOutput(error.stack,);
	const errorWithBody = error as Error & { body?: unknown; };
	if (typeof errorWithBody.body === "string") {
		errorWithBody.body = sanitizeProjectGitOutput(errorWithBody.body,);
	}
	return error;
}

/**
 * External library credentials are read only from the environment variable
 * named by `--password-env`. The secret never reaches argv, a mutation plan, a
 * returned payload, or an error message: only the variable name travels.
 */
function libraryPassword(flags: Record<string, string | boolean>,): string | undefined {
	const raw = flags["password-env"];
	if (raw === undefined) return undefined;
	if (typeof raw !== "string" || raw.trim().length === 0) {
		throw new UsageError(
			"--password-env requires the name of an environment variable holding the password.",
			"invalid_flag_value",
			"Pass --password-env DSS_GIT_LIBRARY_PASSWORD, never the password itself.",
			{ flag: "--password-env", },
		);
	}
	const name = raw.trim();
	const value = process.env[name];
	if (value === undefined || value === "") {
		throw new UsageError(
			`Environment variable ${name} is not set or is empty.`,
			"missing_required_flag",
			"--password-env names the environment variable that holds the Git repository password; export that variable before running the command.",
			{ flag: "--password-env", env: name, },
		);
	}
	return value;
}

const STATUS_USAGE = "dss project-git status --project-key KEY";
const GET_REMOTE_USAGE = "dss project-git get-remote --project-key KEY [--name NAME]";
const SET_REMOTE_USAGE =
	"dss project-git set-remote --repository URL --project-key KEY [--name NAME]";
const REMOVE_REMOTE_USAGE = "dss project-git remove-remote --project-key KEY [--name NAME]";
const BRANCHES_USAGE = "dss project-git branches --project-key KEY [--remote]";
const CREATE_BRANCH_USAGE =
	"dss project-git create-branch <name> --project-key KEY [--commit COMMIT] [--duplicate-project] [--target-project-key KEY] [--target-project-folder-id ID]";
const DELETE_BRANCH_USAGE =
	"dss project-git delete-branch <name> --project-key KEY [--remote] [--delete-remotely] [--force-delete]";
const CURRENT_BRANCH_USAGE = "dss project-git current-branch --project-key KEY";
const TAGS_USAGE = "dss project-git tags --project-key KEY";
const CREATE_TAG_USAGE =
	"dss project-git create-tag <name> --project-key KEY [--reference REF] [--message MESSAGE]";
const DELETE_TAG_USAGE = "dss project-git delete-tag <name> --project-key KEY";
const SWITCH_USAGE = "dss project-git switch <branch> --project-key KEY";
const FETCH_USAGE = "dss project-git fetch --project-key KEY";
const PULL_USAGE = "dss project-git pull --project-key KEY [--branch NAME]";
const PUSH_USAGE = "dss project-git push --project-key KEY [--branch NAME]";
const LOG_USAGE =
	"dss project-git log --project-key KEY [--path PATH] [--start-commit COMMIT] [--count N]";
const DIFF_USAGE = "dss project-git diff --project-key KEY [--from COMMIT] [--to COMMIT]";
const COMMIT_USAGE = "dss project-git commit --message MESSAGE --project-key KEY";
const REVERT_TO_REVISION_USAGE = "dss project-git revert-to-revision <commit> --project-key KEY";
const REVERT_COMMIT_USAGE = "dss project-git revert-commit <commit> --project-key KEY";
const RESET_TO_HEAD_USAGE = "dss project-git reset-to-head --project-key KEY";
const RESET_TO_UPSTREAM_USAGE = "dss project-git reset-to-upstream --project-key KEY";
const DROP_AND_REBUILD_USAGE =
	"dss project-git drop-and-rebuild --i-know-what-i-am-doing --project-key KEY";
const LIST_LIBRARIES_USAGE = "dss project-git list-libraries --project-key KEY";
const ADD_LIBRARY_USAGE =
	"dss project-git add-library <target-path> --repository URL --checkout REF --project-key KEY [--path-in-repository PATH] [--login LOGIN] [--password-env ENV_NAME] [--no-add-to-python-path]";
const SET_LIBRARY_USAGE =
	"dss project-git set-library <target-path> --repository URL --checkout REF --project-key KEY [--path-in-repository PATH] [--login LOGIN] [--password-env ENV_NAME]";
const REMOVE_LIBRARY_USAGE =
	"dss project-git remove-library <target-path> --project-key KEY [--delete-directory]";
const RESET_LIBRARY_USAGE = "dss project-git reset-library <target-path> --project-key KEY";
const PUSH_LIBRARY_USAGE =
	"dss project-git push-library <target-path> --message MESSAGE --project-key KEY";
const PUSH_ALL_LIBRARIES_USAGE =
	"dss project-git push-all-libraries --message MESSAGE --project-key KEY";
const RESET_ALL_LIBRARIES_USAGE = "dss project-git reset-all-libraries --project-key KEY";
const FUTURE_STATUS_USAGE = "dss project-git future-status <job-id> [--peek]";
const FUTURE_WAIT_USAGE =
	"dss project-git future-wait <job-id> [--timeout MS] [--poll-interval MS]";
const FUTURE_ABORT_USAGE = "dss project-git future-abort <job-id>";

export const projectGitCommands: Record<string, CommandMeta> = {
	status: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, STATUS_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.status(requiredStringFlag(f, "project-key", STATUS_USAGE,),),
			);
		},
		usage: STATUS_USAGE,
		description:
			"Get the project's Git working-copy status: current branch, remotes, tracking counts, and pending changes.",
		examples: ["dss project-git status --project-key MYPROJECT",],
	},
	"get-remote": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, GET_REMOTE_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.getRemote(
					requiredStringFlag(f, "project-key", GET_REMOTE_USAGE,),
					optionalStringFlag(f, "name",) ?? DEFAULT_REMOTE,
				),
			);
		},
		usage: GET_REMOTE_USAGE,
		description:
			"Read one configured Git remote, origin by default. DSS answers an empty object when the remote is not configured.",
		examples: ["dss project-git get-remote --project-key MYPROJECT",],
	},
	"set-remote": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, SET_REMOTE_USAGE,);
			const result = await c.projectGit.setRemote(
				requiredStringFlag(f, "project-key", SET_REMOTE_USAGE,),
				{
					url: requiredStringFlag(f, "repository", SET_REMOTE_USAGE,),
					name: optionalStringFlag(f, "name",) ?? DEFAULT_REMOTE,
				},
			);
			return assertGitActionSucceeded(result, "set-remote",);
		},
		usage: SET_REMOTE_USAGE,
		description:
			"Create or replace a Git remote. HTTP(S) URLs must not embed credentials; use an SSH remote or DSS-managed Git credentials instead.",
		examples: [
			"dss project-git set-remote --repository git@github.com:acme/analytics.git --project-key MYPROJECT",
		],
	},
	"remove-remote": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, REMOVE_REMOTE_USAGE,);
			const name = optionalStringFlag(f, "name",) ?? DEFAULT_REMOTE;
			await c.projectGit.removeRemote(
				requiredStringFlag(f, "project-key", REMOVE_REMOTE_USAGE,),
				name,
			);
			return { removed: name, resource: "project-git-remote", };
		},
		usage: REMOVE_REMOTE_USAGE,
		description: "Delete a Git remote from the project, origin by default.",
		examples: ["dss project-git remove-remote --name upstream --project-key MYPROJECT",],
	},
	branches: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, BRANCHES_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.listBranches(
					requiredStringFlag(f, "project-key", BRANCHES_USAGE,),
					parseBooleanOption(f["remote"], "--remote",),
				),
			);
		},
		usage: BRANCHES_USAGE,
		description: "List local branch names, or remote-tracking branches with --remote.",
		examples: [
			"dss project-git branches --project-key MYPROJECT",
			"dss project-git branches --remote --project-key MYPROJECT",
		],
	},
	"create-branch": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CREATE_BRANCH_USAGE,);
			const result = await c.projectGit.createBranch(
				requiredStringFlag(f, "project-key", CREATE_BRANCH_USAGE,),
				a[0],
				{
					commit: optionalStringFlag(f, "commit",),
					duplicateProject: parseBooleanOption(f["duplicate-project"], "--duplicate-project",),
					targetProjectKey: optionalStringFlag(f, "target-project-key",),
					targetProjectFolderId: optionalStringFlag(f, "target-project-folder-id",),
				},
			);
			return assertGitActionSucceeded(result, "create-branch",);
		},
		usage: CREATE_BRANCH_USAGE,
		description:
			"Create a branch, optionally from a specific commit. With --duplicate-project DSS copies the project onto the new branch instead of switching this one.",
		examples: [
			"dss project-git create-branch feature/pricing --project-key MYPROJECT",
			"dss project-git create-branch release/2024 --commit 4f1c2ab --project-key MYPROJECT",
		],
	},
	"delete-branch": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, DELETE_BRANCH_USAGE,);
			const result = await c.projectGit.deleteBranch(
				requiredStringFlag(f, "project-key", DELETE_BRANCH_USAGE,),
				a[0],
				{
					remote: parseBooleanOption(f["remote"], "--remote",),
					deleteRemotely: parseBooleanOption(f["delete-remotely"], "--delete-remotely",),
					forceDelete: parseBooleanOption(f["force-delete"], "--force-delete",),
				},
			);
			return assertGitActionSucceeded(result, "delete-branch",);
		},
		usage: DELETE_BRANCH_USAGE,
		description:
			"Delete a branch. --remote targets a remote-tracking branch, --delete-remotely also deletes it on the remote, and --force-delete drops unmerged commits.",
		examples: ["dss project-git delete-branch feature/pricing --project-key MYPROJECT",],
	},
	"current-branch": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, CURRENT_BRANCH_USAGE,);
			const branch = await c.projectGit.currentBranch(
				requiredStringFlag(f, "project-key", CURRENT_BRANCH_USAGE,),
			);
			return { branch, };
		},
		usage: CURRENT_BRANCH_USAGE,
		description: "Get the project's current branch name, or null when DSS reports none.",
		examples: ["dss project-git current-branch --project-key MYPROJECT",],
	},
	tags: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, TAGS_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.listTags(requiredStringFlag(f, "project-key", TAGS_USAGE,),),
			);
		},
		usage: TAGS_USAGE,
		description: "List the project's Git tags.",
		examples: ["dss project-git tags --project-key MYPROJECT",],
	},
	"create-tag": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, CREATE_TAG_USAGE,);
			const result = await c.projectGit.createTag(
				requiredStringFlag(f, "project-key", CREATE_TAG_USAGE,),
				a[0],
				{
					reference: optionalStringFlag(f, "reference",),
					message: optionalStringFlag(f, "message",),
				},
			);
			return assertGitActionSucceeded(result, "create-tag",);
		},
		usage: CREATE_TAG_USAGE,
		description:
			"Create a Git tag. --reference defaults to HEAD; passing --message creates an annotated tag.",
		examples: [
			'dss project-git create-tag v1.2.0 --message "Release 1.2.0" --project-key MYPROJECT',
		],
	},
	"delete-tag": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, DELETE_TAG_USAGE,);
			const result = await c.projectGit.deleteTag(
				requiredStringFlag(f, "project-key", DELETE_TAG_USAGE,),
				a[0],
			);
			return assertGitActionSucceeded(result, "delete-tag",);
		},
		usage: DELETE_TAG_USAGE,
		description: "Delete a Git tag from the project.",
		examples: ["dss project-git delete-tag v1.2.0 --project-key MYPROJECT",],
	},
	switch: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, SWITCH_USAGE,);
			const result = await c.projectGit.switchBranch(
				requiredStringFlag(f, "project-key", SWITCH_USAGE,),
				a[0],
			);
			return assertGitActionSucceeded(result, "switch",);
		},
		usage: SWITCH_USAGE,
		description:
			"Switch the project to another branch. DSS rewrites the project's working copy in place.",
		examples: ["dss project-git switch main --project-key MYPROJECT",],
	},
	fetch: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, FETCH_USAGE,);
			const result = await c.projectGit.fetch(requiredStringFlag(f, "project-key", FETCH_USAGE,),);
			return assertGitActionSucceeded(result, "fetch",);
		},
		usage: FETCH_USAGE,
		description: "Fetch from the project's Git remote without changing the working copy.",
		examples: ["dss project-git fetch --project-key MYPROJECT",],
	},
	pull: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, PULL_USAGE,);
			const result = await c.projectGit.pull(
				requiredStringFlag(f, "project-key", PULL_USAGE,),
				{ branchName: optionalStringFlag(f, "branch",), },
			);
			return assertGitActionSucceeded(result, "pull",);
		},
		usage: PULL_USAGE,
		description: "Pull with rebase. DSS exposes no merge variant of pull.",
		examples: ["dss project-git pull --project-key MYPROJECT",],
	},
	push: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, PUSH_USAGE,);
			const result = await c.projectGit.push(
				requiredStringFlag(f, "project-key", PUSH_USAGE,),
				{ branchName: optionalStringFlag(f, "branch",), },
			);
			return assertGitActionSucceeded(result, "push",);
		},
		usage: PUSH_USAGE,
		description: "Push the project's committed changes to its Git remote.",
		examples: ["dss project-git push --project-key MYPROJECT",],
	},
	log: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, LOG_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.log(requiredStringFlag(f, "project-key", LOG_USAGE,), {
					path: optionalStringFlag(f, "path",),
					startCommit: optionalStringFlag(f, "start-commit",),
					count: num(f["count"], "--count",),
				},),
			);
		},
		usage: LOG_USAGE,
		description:
			"Read a page of the project's commit log. Paginate by passing the previous response's nextCommit as --start-commit.",
		examples: ["dss project-git log --count 20 --project-key MYPROJECT",],
	},
	diff: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, DIFF_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.diff(requiredStringFlag(f, "project-key", DIFF_USAGE,), {
					commitFrom: optionalStringFlag(f, "from",),
					commitTo: optionalStringFlag(f, "to",),
				},),
			);
		},
		usage: DIFF_USAGE,
		description:
			"Diff the project between two commits. Omitting both bounds diffs the working copy against HEAD.",
		examples: ["dss project-git diff --from 4f1c2ab --to 9de77c1 --project-key MYPROJECT",],
	},
	commit: {
		handler: async (c, a, f,) => {
			requireNoArgs(a, COMMIT_USAGE,);
			const result = await c.projectGit.commit(
				requiredStringFlag(f, "project-key", COMMIT_USAGE,),
				{ message: requiredStringFlag(f, "message", COMMIT_USAGE,), },
			);
			return assertGitActionSucceeded(result, "commit",);
		},
		usage: COMMIT_USAGE,
		description: "Commit the project. DSS stages untracked files before committing.",
		examples: [
			'dss project-git commit --message "Update pricing recipe" --project-key MYPROJECT',
		],
	},
	"revert-to-revision": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, REVERT_TO_REVISION_USAGE,);
			const result = await c.projectGit.revertToRevision(
				requiredStringFlag(f, "project-key", REVERT_TO_REVISION_USAGE,),
				a[0],
			);
			return assertGitActionSucceeded(result, "revert-to-revision",);
		},
		usage: REVERT_TO_REVISION_USAGE,
		description:
			"Restore the whole project to the state of a past commit, discarding everything after it in the working copy.",
		examples: ["dss project-git revert-to-revision 4f1c2ab --project-key MYPROJECT",],
	},
	"revert-commit": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, REVERT_COMMIT_USAGE,);
			const result = await c.projectGit.revertCommit(
				requiredStringFlag(f, "project-key", REVERT_COMMIT_USAGE,),
				a[0],
			);
			return assertGitActionSucceeded(result, "revert-commit",);
		},
		usage: REVERT_COMMIT_USAGE,
		description: "Revert a single commit while keeping the commits that follow it.",
		examples: ["dss project-git revert-commit 4f1c2ab --project-key MYPROJECT",],
	},
	"reset-to-head": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, RESET_TO_HEAD_USAGE,);
			const result = await c.projectGit.resetToHead(
				requiredStringFlag(f, "project-key", RESET_TO_HEAD_USAGE,),
			);
			return assertGitActionSucceeded(result, "reset-to-head",);
		},
		usage: RESET_TO_HEAD_USAGE,
		description: "Hard reset the project to local HEAD, discarding uncommitted changes.",
		examples: ["dss project-git reset-to-head --project-key MYPROJECT",],
	},
	"reset-to-upstream": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, RESET_TO_UPSTREAM_USAGE,);
			const result = await c.projectGit.resetToUpstream(
				requiredStringFlag(f, "project-key", RESET_TO_UPSTREAM_USAGE,),
			);
			return assertGitActionSucceeded(result, "reset-to-upstream",);
		},
		usage: RESET_TO_UPSTREAM_USAGE,
		description: "Hard reset the project to its tracked upstream branch, discarding local commits.",
		examples: ["dss project-git reset-to-upstream --project-key MYPROJECT",],
	},
	"drop-and-rebuild": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, DROP_AND_REBUILD_USAGE,);
			const projectKey = requiredStringFlag(f, "project-key", DROP_AND_REBUILD_USAGE,);
			// History-destructive and irreversible: refuse before issuing any
			// request unless the caller acknowledged it on the command line.
			if (parseBooleanOption(f["i-know-what-i-am-doing"], "--i-know-what-i-am-doing",) !== true) {
				throw new UsageError(
					"drop-and-rebuild permanently destroys this project's Git history and rebuilds an empty repository.",
					"missing_required_flag",
					"Pass --i-know-what-i-am-doing to acknowledge the irreversible history loss.",
					{ flag: "--i-know-what-i-am-doing", projectKey, },
				);
			}
			const result = await c.projectGit.dropAndRebuild(projectKey, { confirmed: true, },);
			return assertGitActionSucceeded(result, "drop-and-rebuild",);
		},
		usage: DROP_AND_REBUILD_USAGE,
		description:
			"Wipe the project's entire Git history and rebuild a fresh repository. Irreversible, so it requires --i-know-what-i-am-doing.",
		examples: [
			"dss project-git drop-and-rebuild --i-know-what-i-am-doing --project-key MYPROJECT",
		],
	},
	"list-libraries": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, LIST_LIBRARIES_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.listLibraries(
					requiredStringFlag(f, "project-key", LIST_LIBRARIES_USAGE,),
				),
			);
		},
		usage: LIST_LIBRARIES_USAGE,
		description: "List the external Git libraries attached to the project's code library.",
		examples: ["dss project-git list-libraries --project-key MYPROJECT",],
	},
	"add-library": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, ADD_LIBRARY_USAGE,);
			const excludeFromPythonPath = parseBooleanOption(
				f["no-add-to-python-path"],
				"--no-add-to-python-path",
			);
			const password = libraryPassword(f,);
			return sanitizeProjectGitOutput(
				await c.projectGit.addLibrary(
					requiredStringFlag(f, "project-key", ADD_LIBRARY_USAGE,),
					{
						repository: requiredStringFlag(f, "repository", ADD_LIBRARY_USAGE,),
						localTargetPath: a[0],
						checkout: requiredStringFlag(f, "checkout", ADD_LIBRARY_USAGE,),
						pathInGitRepository: optionalStringFlag(f, "path-in-repository",),
						addToPythonPath: excludeFromPythonPath === true ? false : undefined,
						login: optionalStringFlag(f, "login",),
						password,
					},
				),
				password === undefined ? [] : [password,],
			);
		},
		usage: ADD_LIBRARY_USAGE,
		description:
			"Attach an external Git repository to the project's code library and return the DSS future that clones it. The password is read only from the environment variable named by --password-env.",
		examples: [
			"dss project-git add-library shared-utils --repository git@github.com:acme/utils.git --checkout main --project-key MYPROJECT",
		],
	},
	"set-library": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, SET_LIBRARY_USAGE,);
			const password = libraryPassword(f,);
			return sanitizeProjectGitOutput(
				await c.projectGit.setLibrary(
					requiredStringFlag(f, "project-key", SET_LIBRARY_USAGE,),
					a[0],
					{
						repository: requiredStringFlag(f, "repository", SET_LIBRARY_USAGE,),
						pathInGitRepository: optionalStringFlag(f, "path-in-repository",),
						checkout: requiredStringFlag(f, "checkout", SET_LIBRARY_USAGE,),
						login: optionalStringFlag(f, "login",),
						password,
					},
				),
				password === undefined ? [] : [password,],
			);
		},
		usage: SET_LIBRARY_USAGE,
		description:
			"Update an attached library's repository, sub-path, or checkout. The password is read only from the environment variable named by --password-env.",
		examples: [
			"dss project-git set-library shared-utils --repository git@github.com:acme/utils.git --checkout v2 --path-in-repository python --project-key MYPROJECT",
		],
	},
	"remove-library": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, REMOVE_LIBRARY_USAGE,);
			const deleteDirectory = parseBooleanOption(f["delete-directory"], "--delete-directory",)
				?? false;
			await c.projectGit.removeLibrary(
				requiredStringFlag(f, "project-key", REMOVE_LIBRARY_USAGE,),
				a[0],
				deleteDirectory,
			);
			return { removed: a[0], resource: "project-git-library", deleteDirectory, };
		},
		usage: REMOVE_LIBRARY_USAGE,
		description:
			"Detach an external Git library. --delete-directory also removes its local directory from the project library.",
		examples: ["dss project-git remove-library shared-utils --project-key MYPROJECT",],
	},
	"reset-library": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, RESET_LIBRARY_USAGE,);
			return c.projectGit.resetLibrary(
				requiredStringFlag(f, "project-key", RESET_LIBRARY_USAGE,),
				a[0],
			);
		},
		usage: RESET_LIBRARY_USAGE,
		description:
			"Reset one attached library to its remote state, discarding local edits. Returns the DSS future that performs the reset.",
		examples: ["dss project-git reset-library shared-utils --project-key MYPROJECT",],
	},
	"push-library": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, PUSH_LIBRARY_USAGE,);
			return c.projectGit.pushLibrary(
				requiredStringFlag(f, "project-key", PUSH_LIBRARY_USAGE,),
				a[0],
				requiredStringFlag(f, "message", PUSH_LIBRARY_USAGE,),
			);
		},
		usage: PUSH_LIBRARY_USAGE,
		description:
			"Commit and push one attached library. Returns the DSS future that performs the push.",
		examples: [
			'dss project-git push-library shared-utils --message "Update helpers" --project-key MYPROJECT',
		],
	},
	"push-all-libraries": {
		handler: (c, a, f,) => {
			requireNoArgs(a, PUSH_ALL_LIBRARIES_USAGE,);
			return c.projectGit.pushAllLibraries(
				requiredStringFlag(f, "project-key", PUSH_ALL_LIBRARIES_USAGE,),
				requiredStringFlag(f, "message", PUSH_ALL_LIBRARIES_USAGE,),
			);
		},
		usage: PUSH_ALL_LIBRARIES_USAGE,
		description:
			"Commit and push every attached library. Returns the DSS future that performs the pushes.",
		examples: [
			'dss project-git push-all-libraries --message "Sync libraries" --project-key MYPROJECT',
		],
	},
	"reset-all-libraries": {
		handler: (c, a, f,) => {
			requireNoArgs(a, RESET_ALL_LIBRARIES_USAGE,);
			return c.projectGit.resetAllLibraries(
				requiredStringFlag(f, "project-key", RESET_ALL_LIBRARIES_USAGE,),
			);
		},
		usage: RESET_ALL_LIBRARIES_USAGE,
		description:
			"Reset every attached library to its remote state. Returns the DSS future that performs the resets.",
		examples: ["dss project-git reset-all-libraries --project-key MYPROJECT",],
	},
	"future-status": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, FUTURE_STATUS_USAGE,);
			return sanitizeProjectGitOutput(
				await c.projectGit.getFutureState(a[0], {
					peek: parseBooleanOption(f["peek"], "--peek",),
				},),
			);
		},
		usage: FUTURE_STATUS_USAGE,
		description:
			"Get the state of a Git library future by job id. --peek leaves the result queued for a later read.",
		examples: ["dss project-git future-status FUTURE_ID --peek",],
	},
	"future-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, FUTURE_WAIT_USAGE,);
			try {
				const result = sanitizeProjectGitOutput(
					await c.projectGit.waitForFuture(a[0], {
						pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
						timeoutMs: num(f["timeout"], "--timeout",),
					},),
				);
				if (
					result !== null
					&& typeof result === "object"
					&& !Array.isArray(result,)
					&& (result as { success?: unknown; }).success === false
				) {
					return assertGitActionSucceeded(result as ProjectGitActionResult, "future-wait",);
				}
				return result;
			} catch (error) {
				throw sanitizeProjectGitError(error,);
			}
		},
		usage: FUTURE_WAIT_USAGE,
		description: "Wait for a Git library future to finish and return its result.",
		examples: ["dss project-git future-wait FUTURE_ID --timeout 300000",],
	},
	"future-abort": {
		handler: async (c, a,) => {
			requireArgs(a, 1, FUTURE_ABORT_USAGE,);
			await c.projectGit.abortFuture(a[0],);
			return { aborted: a[0], resource: "project-git-future", };
		},
		usage: FUTURE_ABORT_USAGE,
		description: "Abort a running Git library future by job id.",
		examples: ["dss project-git future-abort FUTURE_ID",],
	},
};
