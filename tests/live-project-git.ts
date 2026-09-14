import { expect, } from "bun:test";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";

/**
 * Project Git live-suite module. Exercises the local-only Git surface of the
 * CLI (status/log/diff reads, commit, branches, tags, reverts, resets) on a
 * disposable owned child project. Everything that touches a Git remote or an
 * external library repository is reported as an explicit blocker so the
 * missing isolated remote never hides the local behavior coverage.
 */

interface GitStatus {
	currentBranch?: string | null;
	hasUncommittedChanges?: boolean;
	modified?: string[];
	changed?: string[];
	removed?: string[];
	missing?: string[];
	conflicting?: string[];
	untracked?: string[];
	[key: string]: unknown;
}

interface GitLogResult {
	entries: unknown[];
	nextCommit?: string;
	[key: string]: unknown;
}

interface GitTag {
	name?: string;
	shortName?: string;
	[key: string]: unknown;
}

function blocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

/** DSS log entries are untyped; accept the documented nesting variants. */
function commitIdOf(entry: unknown,): string | undefined {
	if (typeof entry === "string") return entry || undefined;
	if (typeof entry !== "object" || entry === null) return undefined;
	const record = entry as Record<string, unknown>;
	const nested = typeof record.commit === "object" && record.commit !== null
		? record.commit as Record<string, unknown>
		: undefined;
	for (
		const candidate of [
			record.commit,
			record.commitId,
			record.id,
			nested?.commitId,
		]
	) {
		if (typeof candidate === "string" && candidate.trim()) return candidate;
	}
	return undefined;
}

function newestCommitId(log: GitLogResult, what: string,): string {
	const id = commitIdOf(log.entries[0],);
	if (!id) {
		throw new Error(
			`project-git log entry has no recognizable commit id for ${what}: ${
				JSON.stringify(log.entries[0],)
			}`,
		);
	}
	return id;
}

function isDirty(status: GitStatus,): boolean {
	if (status.hasUncommittedChanges === true) return true;
	return ["added", "changed", "removed", "missing", "conflicting", "untracked", "modified",]
		.every(key => !Array.isArray(status[key],) || (status[key] as string[]).length === 0) === false;
}

async function readLog(ctx: LiveContext, projectKey: string,): Promise<GitLogResult> {
	const log = await ctx.run<GitLogResult>(["project-git", "log", "--count", "50",], {
		projectKey,
	},);
	expect(Array.isArray(log.entries,),).toBe(true,);
	return log;
}

async function readStatus(ctx: LiveContext, projectKey: string,): Promise<GitStatus> {
	return ctx.run<GitStatus>(["project-git", "status",], { projectKey, },);
}

/**
 * Write a file into the project library (tracked by the project Git repo) and
 * commit it. DSS may auto-commit the library write itself, so the assertion is
 * "the commit succeeded AND the log grew", never an assumption about which of
 * the two produced the new commit. Returns the resulting head commit id.
 */
async function commitMarker(
	ctx: LiveContext,
	projectKey: string,
	nonce: string,
): Promise<string> {
	const before = await readLog(ctx, projectKey,);
	await ctx.run([
		"project-library",
		"put",
		`live_git_${nonce}.py`,
		"--content",
		`# live git marker ${nonce}\n`,
	], { projectKey, },);
	const dirty = isDirty(await readStatus(ctx, projectKey,),);
	const result = await ctx.run<{ success?: boolean; }>([
		"project-git",
		"commit",
		"--message",
		`Live suite marker ${nonce}`,
	], { projectKey, },);
	expect(result.success,).not.toBe(false,);
	const after = await readLog(ctx, projectKey,);
	expect(after.entries.length,).toBeGreaterThanOrEqual(before.entries.length + 1,);
	if (!dirty) {
		// DSS auto-committed the library write; record it so the report is
		// truthful about what this case demonstrated.
		process.stdout.write(
			`${
				JSON.stringify({
					note: "library write was auto-committed by DSS; commit ran as confirmation",
				},)
			}\n`,
		);
	}
	return newestCommitId(after, nonce,);
}

/**
 * Each case provisions lazily inside its own selected ctx.check callback: a
 * filtered --case selection never creates the git child for wholly unselected
 * or pure-blocker cases, and sharedProject() deletes the project exactly once
 * when the module finishes.
 */
export async function exerciseProjectGit(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	const state: { key?: string; } = {};
	// Guarded commands need a bound owned project; pure-blocker cases never
	// call this, so no project is created for them.
	const sharedProject = async (): Promise<string> => {
		if (!state.key) {
			state.key = await ctx.createProject("git",);
		}
		return state.key;
	};
	try {
		await ctx.check("core.project-git.inspect", [
			"project.create",
			"project-git.status",
			"project-git.current-branch",
			"project-git.branches",
			"project-git.tags",
			"project-git.log",
			"project-git.diff",
			"project-git.get-remote",
			"project-git.list-libraries",
		], async () => {
			const key = await sharedProject();
			const status = await readStatus(ctx, key,);
			expect(typeof status,).toBe("object",);
			const branch = await ctx.run<{ branch: string | null; }>([
				"project-git",
				"current-branch",
			], { projectKey: key, },);
			expect(branch.branch === null || typeof branch.branch === "string",).toBe(true,);
			const branches = await ctx.run<string[]>(["project-git", "branches",], { projectKey: key, },);
			expect(Array.isArray(branches,),).toBe(true,);
			const tags = await ctx.run<GitTag[]>(["project-git", "tags",], { projectKey: key, },);
			expect(Array.isArray(tags,),).toBe(true,);
			await readLog(ctx, key,);
			const diff = await ctx.run<Record<string, unknown>>(["project-git", "diff",], {
				projectKey: key,
			},);
			expect(typeof diff,).toBe("object",);
			// DSS answers an empty object when no remote is configured.
			const remote = await ctx.run<Record<string, unknown>>([
				"project-git",
				"get-remote",
			], { projectKey: key, },);
			expect(typeof remote,).toBe("object",);
			const libraries = await ctx.run<unknown[]>([
				"project-git",
				"list-libraries",
			], { projectKey: key, },);
			// SDK contract: an array of library entries. DSS's raw wire shape
			// is a gitReferences map which the SDK normalizes to entries with
			// localTargetPath preserved, so the CLI always emits an array.
			expect(Array.isArray(libraries,),).toBe(true,);
		},);

		await ctx.check("core.project-git.commit", [
			"project-git.status",
			"variable.set",
			"project-git.commit",
			"project-git.log",
		], async () => {
			const key = await sharedProject();
			const nonce = `commit_i${String(ctx.iteration,)}`;
			await commitMarker(ctx, key, nonce,);
			const log = await readLog(ctx, key,);
			// The new head must be resolvable for the history cases to build on it.
			newestCommitId(log, nonce,);
		},);

		await ctx.check("core.project-git.branches", [
			"project-git.status",
			"project-git.create-branch",
			"project-git.current-branch",
			"project-git.branches",
			"project-git.switch",
			"project-git.delete-branch",
		], async () => {
			const key = await sharedProject();
			const baseline = (await readStatus(ctx, key,)).currentBranch;
			if (typeof baseline !== "string" || !baseline) {
				blocked("DSS reported no current branch; cannot round-trip branch create/switch/delete",);
			}
			const feature = `live_feat_i${String(ctx.iteration,)}`;
			const created = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"create-branch",
				feature,
			], { projectKey: key, },);
			expect(created.success,).not.toBe(false,);
			expect(
				(await ctx.run<{ branch: string | null; }>([
					"project-git",
					"current-branch",
				], { projectKey: key, },)).branch,
			).toBe(feature,);
			expect(
				(await ctx.run<string[]>(["project-git", "branches",], { projectKey: key, },)).includes(
					feature,
				),
			).toBe(true,);
			const switched = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"switch",
				baseline,
			], { projectKey: key, },);
			expect(switched.success,).not.toBe(false,);
			expect(
				(await ctx.run<{ branch: string | null; }>([
					"project-git",
					"current-branch",
				], { projectKey: key, },)).branch,
			).toBe(baseline,);
			const deleted = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"delete-branch",
				feature,
			], { projectKey: key, },);
			expect(deleted.success,).not.toBe(false,);
			expect(
				(await ctx.run<string[]>(["project-git", "branches",], { projectKey: key, },)).includes(
					feature,
				),
			).toBe(false,);
		},);

		await ctx.check("core.project-git.tags", [
			"project-git.create-tag",
			"project-git.tags",
			"project-git.delete-tag",
		], async () => {
			const key = await sharedProject();
			const tag = `live_tag_i${String(ctx.iteration,)}`;
			const created = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"create-tag",
				tag,
				"--message",
				"Live suite tag",
			], { projectKey: key, },);
			expect(created.success,).not.toBe(false,);
			const tagNames = (await ctx.run<GitTag[]>(["project-git", "tags",], { projectKey: key, },))
				.map(t => t.shortName ?? t.name);
			expect(tagNames.includes(tag,),).toBe(true,);
			const deleted = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"delete-tag",
				tag,
			], { projectKey: key, },);
			expect(deleted.success,).not.toBe(false,);
			const afterDelete = (await ctx.run<GitTag[]>(["project-git", "tags",], { projectKey: key, },))
				.map(t => t.shortName ?? t.name);
			expect(afterDelete.includes(tag,),).toBe(false,);
		},);

		await ctx.check("core.project-git.history", [
			"project-git.log",
			"variable.set",
			"project-git.commit",
			"project-git.revert-commit",
			"project-git.revert-to-revision",
			"project-git.reset-to-head",
			"project-git.status",
		], async () => {
			const key = await sharedProject();
			// Commit A gets reverted by commit; commit B stays as history context.
			const commitA = await commitMarker(ctx, key, `revert_a_i${String(ctx.iteration,)}`,);
			const reverted = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"revert-commit",
				commitA,
			], { projectKey: key, },);
			expect(reverted.success,).not.toBe(false,);
			expect(isDirty(await readStatus(ctx, key,),),).toBe(false,);

			const commitB = await commitMarker(ctx, key, `revert_b_i${String(ctx.iteration,)}`,);
			const log = await readLog(ctx, key,);
			// An entry strictly older than B anchors revert-to-revision.
			const older = log.entries.map(commitIdOf,).find(id => typeof id === "string" && id !== commitB);
			if (!older) {
				blocked(
					"Git history has no second commit; cannot exercise revert-to-revision deterministically",
				);
			}
			const restored = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"revert-to-revision",
				older,
			], { projectKey: key, },);
			expect(restored.success,).not.toBe(false,);
			await readStatus(ctx, key,);

			const reset = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"reset-to-head",
			], { projectKey: key, },);
			expect(reset.success,).not.toBe(false,);
			expect(isDirty(await readStatus(ctx, key,),),).toBe(false,);
		},);

		await ctx.check("core.project-git.drop-and-rebuild", [
			"project-git.log",
			"project-git.drop-and-rebuild",
			"project-git.status",
		], async () => {
			const key = await sharedProject();
			const before = (await readLog(ctx, key,)).entries.length;
			const dropped = await ctx.run<{ success?: boolean; }>([
				"project-git",
				"drop-and-rebuild",
				"--i-know-what-i-am-doing",
			], { projectKey: key, },);
			expect(dropped.success,).not.toBe(false,);
			const after = (await readLog(ctx, key,)).entries.length;
			expect(after,).toBeLessThanOrEqual(before,);
			expect(isDirty(await readStatus(ctx, key,),),).toBe(false,);
		},);

		// Pure blocker cases never create a project: the actions cannot run
		// without an isolated remote / external repository, so provisioning a
		// child would only to satisfy a case that will report blocked anyway.
		await ctx.check("core.project-git.remote-sync", [
			"project-git.set-remote",
			"project-git.remove-remote",
			"project-git.fetch",
			"project-git.pull",
			"project-git.push",
			"project-git.reset-to-upstream",
		], async () => {
			blocked(
				"Remote-sync actions (set-remote, remove-remote, fetch, pull, push, reset-to-upstream) require an isolated Git remote; the lab never points owned projects at shared or external repositories.",
			);
		}, { capability: "project-git.isolated-remote", required: false, },);

		await ctx.check("core.project-git.external-libraries", [
			"project-git.add-library",
			"project-git.set-library",
			"project-git.remove-library",
			"project-git.reset-library",
			"project-git.reset-all-libraries",
			"project-git.push-library",
			"project-git.push-all-libraries",
			"project-git.future-abort",
		], async () => {
			blocked(
				"Git library actions attach, update, push, or reset external repository checkouts and need an external Git repository plus a running future to abort; none is reserved for this lab.",
			);
		}, { capability: "project-git.external-repository", required: false, },);

		await ctx.check("core.project-git.library-futures", [
			"project-git.reset-all-libraries",
			"project-git.future-status",
			"project-git.future-wait",
			"project-git.future-abort",
		], async () => {
			// With zero attached libraries, reset-all contacts no external
			// repository and yields a DSS future that finishes immediately.
			const key = await sharedProject();
			const attached = await ctx.run<unknown[]>([
				"project-git",
				"list-libraries",
			], { projectKey: key, },);
			expect(Array.isArray(attached,),).toBe(true,);
			if (attached.length > 0) {
				blocked(
					"reset-all-libraries only runs with zero attached libraries so no external repository is contacted",
				);
			}
			// One fresh future per consumptive operation, mirroring core.future
			// tests: --peek status leaves the result queued, future-wait
			// CONSUMES it, and abort is a distinct future receipt that DSS
			// reports as terminated. Sharing one future across a peeked
			// abort and a later wait makes the wait fail with "future was
			// aborted" — the abort genuinely terminated that job.
			const startFuture = async (): Promise<string> => {
				const future = await ctx.run<{ jobId?: string; }>([
					"project-git",
					"reset-all-libraries",
				], { projectKey: key, },);
				const jobId = typeof future.jobId === "string" ? future.jobId : undefined;
				if (!jobId) {
					throw new Error(
						`reset-all-libraries returned no jobId: ${JSON.stringify(future,)}`,
					);
				}
				await ctx.recordFuture(jobId,);
				return jobId;
			};

			// peek does not consume: status readable, then the SAME future
			// can still be waited to completion. Official wait_for_result
			// contract: the completed future's payload is returned verbatim
			// (a zero-library reset-all may carry a {success:false} diagnostic
			// bag = "no messages", not a failure), so future-wait exits 0.
			const waitId = await startFuture();
			const status = await ctx.run<Record<string, unknown>>([
				"project-git",
				"future-status",
				waitId,
				"--peek",
			],);
			expect(typeof status,).toBe("object",);
			const waitResult = await ctx.run<Record<string, unknown>>([
				"project-git",
				"future-wait",
				waitId,
				"--timeout",
				"120000",
			],);
			expect(typeof waitResult,).toBe("object",);

			// A separate fresh future for abort: aborting terminates THIS job,
			// so afterwards future-wait on it must surface the abort instead
			// of pretending success.
			const abortId = await startFuture();
			await ctx.run(["project-git", "future-abort", abortId,],);
			const afterAbort = await ctx.run<Record<string, unknown>>([
				"project-git",
				"future-status",
				abortId,
				"--peek",
			],);
			expect(typeof afterAbort,).toBe("object",);
		}, { capability: "project-git.local-library-futures", required: false, },);
	} finally {
		if (state.key) {
			await ctx.deleteProject(state.key,);
		}
	}
}
