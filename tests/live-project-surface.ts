import { expect, } from "bun:test";
import type { DoctorResult, FixtureDiscoveryResult, } from "../src/cli/doctor.js";
import type { Discussion, } from "../src/resources/discussions.js";
import type {
	FlowMapResult,
	ProjectPermissionRule,
	ProjectPermissions,
} from "../src/resources/projects.js";
import type {
	DataQualityComputeResult,
	FutureState,
	FutureWaitResult,
	ProjectDetails,
	ProjectMetadata,
	ProjectSummary,
	ProjectTags,
} from "../src/schemas.js";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";

/**
 * Project-surface live-suite module: project reads, metadata / tags /
 * permissions round-trips, discussion lifecycle, doctor and fixtures
 * discovery, and DSS future reads over a future this lab started.
 *
 * Reads run against the bound root project. Every mutation targets a
 * disposable child created for the case and deleted in finally; the root
 * project is only mutated by `data-quality compute`, which the collaboration
 * module already exercises against the same baseline rule.
 */

const TAG_NAME = "live_project_surface";
const TAG_COLOR = "#2ab1ac";
const DISCUSSION_TOPIC = "Live project surface";
const FUTURE_WAIT_TIMEOUT_MS = 180_000;
const DQ_RULE_DISPLAY_NAME = "Live project surface row count";

function requireString(value: unknown, label: string,): string {
	if (typeof value !== "string" || !value.trim()) {
		throw new LiveCapabilityError(`${label} is unavailable in this lab`,);
	}
	return value;
}

function marker(ctx: LiveContext, base: string,): string {
	return `${base} i${String(ctx.iteration,)}`;
}

/** Runs body against a fresh child project and removes it, surfacing every failure. */
async function withChildProject(
	ctx: LiveContext,
	label: string,
	body: (key: string,) => Promise<void>,
): Promise<void> {
	const errors: unknown[] = [];
	let key: string | undefined;
	try {
		key = await ctx.createProject(label,);
		await body(key,);
	} catch (error) {
		errors.push(error,);
	} finally {
		if (key !== undefined) {
			try {
				await ctx.deleteProject(key,);
			} catch (error) {
				errors.push(error,);
			}
		}
	}
	if (errors.length === 1) throw errors[0];
	if (errors.length) {
		throw new AggregateError(
			errors,
			errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
		);
	}
}

function baselineDataQualityRule(ctx: LiveContext,): { dataset: string; ruleId?: string; } {
	const dataset = Object.keys(ctx.fixtures.datasets,)[0];
	if (!dataset) {
		throw new LiveCapabilityError(
			"future reads need at least one baseline dataset in ctx.fixtures.datasets; core data module did not provision any",
		);
	}
	return { dataset, ruleId: ctx.fixtures.dataQualityRuleIds?.[0], };
}

/** Reuses the collaboration baseline rule, or provisions an equivalent owned one when absent. */
async function ensureDataQualityRule(
	ctx: LiveContext,
): Promise<{ dataset: string; ruleId: string; }> {
	const { dataset, ruleId, } = baselineDataQualityRule(ctx,);
	if (ruleId) return { dataset, ruleId, };
	const rules = await ctx.run<Array<{ id?: string; displayName?: string; }>>([
		"data-quality",
		"rules",
		dataset,
	],);
	const existing = rules.find(rule => rule.displayName === DQ_RULE_DISPLAY_NAME);
	if (existing?.id) return { dataset, ruleId: existing.id, };
	const created = await ctx.run<{ created: string; id?: string; }>([
		"data-quality",
		"create-rule",
		dataset,
		"--data",
		JSON.stringify({
			type: "RecordCountInRangeRule",
			displayName: DQ_RULE_DISPLAY_NAME,
			softMinimum: 0,
			softMinimumEnabled: true,
			hardMaximum: 1_000_000_000,
			hardMaximumEnabled: true,
		},),
	],);
	return {
		dataset,
		ruleId: requireString(created.id ?? created.created, "created data quality rule id",),
	};
}

/** Starts a rule computation without waiting, records the future, and returns its id. */
async function startOwnedFuture(ctx: LiveContext,): Promise<string> {
	const { dataset, ruleId, } = await ensureDataQualityRule(ctx,);
	const started = await ctx.run<DataQualityComputeResult>([
		"data-quality",
		"compute",
		dataset,
		"--rule-id",
		ruleId,
	],);
	const id = requireString(started.jobId, "data-quality compute future jobId",);
	await ctx.recordFuture(id,);
	return id;
}

async function exerciseProjectReads(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.project.reads", [
		"project.list",
		"project.get",
		"project.flow",
		"project.map",
	], async () => {
		const list = await ctx.run<ProjectSummary[]>(["project", "list",],);
		const listed = list.find(project => project.projectKey === ctx.projectKey);
		expect(listed,).toBeTruthy();
		const details = await ctx.run<ProjectDetails>(["project", "get",],);
		expect(details.projectKey,).toBe(ctx.projectKey,);
		expect(details.name,).toBe(listed!.name,);
		const flow = await ctx.run<{ nodes?: unknown; }>(["project", "flow",],);
		expect(typeof flow,).toBe("object",);
		expect(flow.nodes,).toBeDefined();
		const datasets = Object.keys(ctx.fixtures.datasets,);
		const mapped = await ctx.run<FlowMapResult>(["project", "map",],);
		expect(mapped.truncation.truncated,).toBe(false,);
		expect(mapped.map.nodes.length,).toBeGreaterThanOrEqual(datasets.length,);
		for (const dataset of datasets) {
			expect(mapped.map.nodes.some(node => node.id === dataset || node.id.endsWith(`.${dataset}`,)),)
				.toBe(true,);
		}
		const rendered = await ctx.run<FlowMapResult>([
			"project",
			"map",
			"--render",
			"mermaid",
			"--max-nodes",
			"1",
		],);
		expect(rendered.rendering?.format,).toBe("mermaid",);
		expect(rendered.rendering?.content,).toContain("flowchart",);
		expect(rendered.truncation.truncated,).toBe(mapped.map.nodes.length > 1,);
		expect(rendered.truncation.nodeCountBefore,).toBe(mapped.map.nodes.length,);
		expect(rendered.truncation.nodeCountAfter,).toBe(Math.min(1, mapped.map.nodes.length,),);
	},);
}

async function exerciseProjectMetadata(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.project.metadata", [
		"project.metadata",
		"project.metadata-set",
		"project.get",
	], async () => {
		await withChildProject(ctx, "metadata", async key => {
			const before = await ctx.run<ProjectMetadata>(["project", "metadata",], { projectKey: key, },);
			const shortDesc = marker(ctx, "live project surface metadata",);
			const next: ProjectMetadata = {
				...before,
				shortDesc,
				tags: [...(before.tags ?? []).filter(tag => tag !== TAG_NAME), TAG_NAME,],
			};
			const planned = await ctx.run<{ dryRun: boolean; next: ProjectMetadata; }>([
				"project",
				"metadata-set",
				"--data",
				JSON.stringify(next,),
				"--dry-run",
			], { projectKey: key, },);
			expect(planned.dryRun,).toBe(true,);
			expect(planned.next.shortDesc,).toBe(shortDesc,);
			const unchanged = await ctx.run<ProjectMetadata>(["project", "metadata",], {
				projectKey: key,
			},);
			expect(unchanged.shortDesc ?? "",).toBe(before.shortDesc ?? "",);
			const updated = await ctx.run<{ updated: boolean; }>([
				"project",
				"metadata-set",
				"--data",
				JSON.stringify(next,),
			], { projectKey: key, },);
			expect(updated.updated,).toBe(true,);
			const after = await ctx.run<ProjectMetadata>(["project", "metadata",], { projectKey: key, },);
			expect(after.shortDesc,).toBe(shortDesc,);
			expect(after.tags,).toContain(TAG_NAME,);
			const details = await ctx.run<ProjectDetails>(["project", "get",], { projectKey: key, },);
			expect(details.projectKey,).toBe(key,);
			expect(details.shortDesc,).toBe(shortDesc,);
			expect(details.tags,).toContain(TAG_NAME,);
			expect((await ctx.client.projects.metadata(key,)).shortDesc,).toBe(shortDesc,);
		},);
	},);
}

async function exerciseProjectTags(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.project.tags", ["project.tags-get", "project.tags-set",], async () => {
		await withChildProject(ctx, "tags", async key => {
			const before = await ctx.run<ProjectTags>(["project", "tags-get",], { projectKey: key, },);
			expect(typeof before.tags,).toBe("object",);
			expect(before.tags[TAG_NAME],).toBeUndefined();
			const next: ProjectTags = { tags: { ...before.tags, [TAG_NAME]: { color: TAG_COLOR, }, }, };
			const updated = await ctx.run<{ updated: boolean; }>([
				"project",
				"tags-set",
				"--data",
				JSON.stringify(next,),
			], { projectKey: key, },);
			expect(updated.updated,).toBe(true,);
			const after = await ctx.run<ProjectTags>(["project", "tags-get",], { projectKey: key, },);
			expect(after.tags[TAG_NAME]?.color?.toLowerCase(),).toBe(TAG_COLOR,);
			for (const name of Object.keys(before.tags,)) expect(after.tags[name],).toBeDefined();
			// Full-replace semantics: a payload without the tag removes it again.
			await ctx.run(["project", "tags-set", "--data", JSON.stringify(before,),], {
				projectKey: key,
			},);
			const restored = await ctx.run<ProjectTags>(["project", "tags-get",], { projectKey: key, },);
			expect(restored.tags[TAG_NAME],).toBeUndefined();
			expect(Object.keys(restored.tags,).sort(),).toEqual(Object.keys(before.tags,).sort(),);
		},);
	},);
}

/** A group rule already granted on the root project proves the group exists on this instance. */
async function rootGroupRule(ctx: LiveContext,): Promise<ProjectPermissionRule | undefined> {
	const root = await ctx.client.projects.getPermissions(ctx.projectKey,);
	return root.permissions?.find(rule => typeof rule.group === "string" && rule.group.trim());
}

async function exerciseProjectPermissions(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.project.permissions", [
		"project.permissions-get",
		"project.permissions-set",
	], async () => {
		await withChildProject(ctx, "permissions", async key => {
			const before = await ctx.run<ProjectPermissions>(["project", "permissions-get",], {
				projectKey: key,
			},);
			expect(before.owner,).toBe(ctx.owner,);
			expect(Array.isArray(before.permissions,),).toBe(true,);
			const baseline = before.permissions ?? [];
			const borrowed = await rootGroupRule(ctx,);
			const added: ProjectPermissionRule | undefined = borrowed
					&& !baseline.some(rule => rule.group === borrowed.group)
				? { ...borrowed, admin: false, readProjectContent: true, writeProjectContent: false, }
				: undefined;
			const next: ProjectPermissions = {
				...before,
				permissions: added ? [...baseline, added,] : baseline,
			};
			const updated = await ctx.run<{ updated: boolean; }>([
				"project",
				"permissions-set",
				"--data",
				JSON.stringify(next,),
			], { projectKey: key, },);
			expect(updated.updated,).toBe(true,);
			const after = await ctx.run<ProjectPermissions>(["project", "permissions-get",], {
				projectKey: key,
			},);
			expect(after.owner,).toBe(before.owner,);
			expect(after.permissions?.length,).toBe(next.permissions!.length,);
			if (added) {
				const landed = after.permissions?.find(rule => rule.group === added.group);
				expect(landed,).toBeTruthy();
				expect(landed?.readProjectContent,).toBe(true,);
				expect(landed?.writeProjectContent,).toBe(false,);
				expect(landed?.admin,).toBe(false,);
				// Full-replace semantics: restoring the baseline drops the borrowed rule.
				await ctx.run(["project", "permissions-set", "--data", JSON.stringify(before,),], {
					projectKey: key,
				},);
				const restored = await ctx.client.projects.getPermissions(key,);
				expect(restored.permissions?.some(rule => rule.group === added.group),).toBe(false,);
				expect(restored.permissions?.length,).toBe(baseline.length,);
			}
		},);
	},);
}

async function exerciseDiscussionLifecycle(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.discussion.lifecycle", [
		"discussion.create",
		"discussion.list",
		"discussion.get",
		"discussion.reply",
	], async () => {
		await withChildProject(ctx, "discussion", async key => {
			const object = ["PROJECT", key,] as const;
			const empty = await ctx.run<Discussion[]>(["discussion", "list", ...object,], {
				projectKey: key,
			},);
			expect(empty,).toEqual([],);
			const topic = marker(ctx, DISCUSSION_TOPIC,);
			const firstReply = "Opened by the live project surface case.";
			const created = await ctx.run<Discussion>([
				"discussion",
				"create",
				...object,
				"--topic",
				topic,
				"--reply",
				firstReply,
			], { projectKey: key, },);
			const id = requireString(created.id, "created discussion id",);
			expect(created.topic,).toBe(topic,);
			const listed = await ctx.run<Discussion[]>(["discussion", "list", ...object,], {
				projectKey: key,
			},);
			expect(listed.map(discussion => discussion.id),).toEqual([id,],);
			const fetched = await ctx.run<Discussion>(["discussion", "get", ...object, id,], {
				projectKey: key,
			},);
			expect(fetched.id,).toBe(id,);
			expect(fetched.replies?.map(reply => reply.text),).toEqual([firstReply,],);
			const secondReply = "Follow-up reply from the live project surface case.";
			const replied = await ctx.run<Discussion>([
				"discussion",
				"reply",
				...object,
				id,
				"--text",
				secondReply,
			], { projectKey: key, },);
			expect(replied.id,).toBe(id,);
			const final = await ctx.client.discussions.get(...object, id, key,);
			// DSS returns replies newest-first; the reply count and contents are the contract.
			expect(final.replies?.map(reply => reply.text),).toHaveLength(2,);
			expect([...final.replies ?? [],].map(reply => reply.text).sort(),).toEqual(
				[firstReply, secondReply,].sort(),
			);
		},);
	},);
}

async function exerciseDoctor(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.doctor.run", ["doctor.run",], async () => {
		const report = await ctx.run<DoctorResult>(["doctor", "--project-key", ctx.projectKey,],);
		expect(report.ok,).toBe(true,);
		expect(report.context.projectKey,).toBe(ctx.projectKey,);
		expect(report.context.hasUrl && report.context.hasApiKey,).toBe(true,);
		for (const name of ["credentials_present", "connectivity", "default_project",]) {
			const check = report.checks.find(entry => entry.name === name);
			expect(check?.ok,).toBe(true,);
		}
		expect(report.permissions,).toBeUndefined();
		const capabilities = await ctx.run<DoctorResult>([
			"doctor",
			"--project-key",
			ctx.projectKey,
			"--capabilities",
			"--fast",
		],);
		expect(capabilities.ok,).toBe(true,);
		expect(capabilities.permissions?.canListProjects,).toBe("yes",);
		expect(capabilities.permissions?.canReadProject,).toBe("yes",);
		expect(capabilities.environment?.projectKey,).toBe(ctx.projectKey,);
		const fixtureDataset = capabilities.fixtures?.defaultDataset;
		if (fixtureDataset !== null && fixtureDataset !== undefined) {
			expect(Object.keys(ctx.fixtures.datasets,),).toContain(fixtureDataset,);
		}
	},);
}

async function exerciseFixtures(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.fixtures.run", ["fixtures.run",], async () => {
		const datasets = Object.keys(ctx.fixtures.datasets,);
		const discovered = await ctx.run<FixtureDiscoveryResult>(["fixtures",],);
		expect(discovered.projectKey,).toBe(ctx.projectKey,);
		expect(discovered.allowTypes,).toEqual(["Filesystem", "Inline",],);
		// fixtures.default* are the first listed candidates regardless of type;
		// safeDataset is the first candidate whose type passes the allow filter.
		const listedFirst = discovered.fixtures.defaultDataset;
		if (listedFirst !== null) expect(datasets,).toContain(listedFirst,);
		const safe = discovered.safeDataset as { name?: string; } | null;
		expect(Array.isArray(discovered.unsafe.datasets,),).toBe(true,);
		if (safe === null && datasets.length) {
			expect(discovered.unsafe.datasets.length,).toBeGreaterThan(0,);
		}
		if (safe?.name !== undefined) expect(datasets,).toContain(safe.name,);
		// A type filter matching nothing leaves no safe candidate and rejects every dataset.
		const restricted = await ctx.run<FixtureDiscoveryResult>([
			"fixtures",
			"--allow-types",
			"NoSuchDatasetType",
		],);
		expect(restricted.allowTypes,).toEqual(["NoSuchDatasetType",],);
		expect(restricted.safeDataset,).toBeNull();
		if (datasets.length) {
			for (const name of datasets) {
				if (ctx.fixtures.datasets[name] === name) {
					expect(
						restricted.unsafe.datasets.some(entry => entry.name === name),
					).toBe(true,);
				}
			}
		}
	},);
}

async function exerciseFutureReads(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.future.reads", [
		"data-quality.compute",
		"future.peek",
		"future.get",
		"future.wait",
	], async () => {
		// Peek never consumes, so the same future is later awaited to completion.
		const peekable = await startOwnedFuture(ctx,);
		const peeked = await ctx.run<FutureState>(["future", "peek", peekable,],);
		expect(peeked.unknown,).not.toBe(true,);
		expect(peeked.aborted,).not.toBe(true,);
		const waited = await ctx.run<FutureWaitResult>([
			"future",
			"wait",
			peekable,
			"--timeout",
			String(FUTURE_WAIT_TIMEOUT_MS,),
		],);
		expect(waited.futureId,).toBe(peekable,);
		expect(waited.timedOut,).not.toBe(true,);
		expect(waited.success,).toBe(true,);
		expect(waited.hasResult,).toBe(true,);
		// DSS removes a future record once its result is retrieved (peek=false),
		// so future.get runs on its own separately started future and is the
		// only consumptive call on that id.
		const gettable = await startOwnedFuture(ctx,);
		expect(gettable,).not.toBe(peekable,);
		const got = await ctx.run<FutureState>(["future", "get", gettable,],);
		expect(got.unknown,).not.toBe(true,);
		expect(got.aborted,).not.toBe(true,);
		expect(got.alive === true || got.hasResult === true,).toBe(true,);
	},);
}

async function exerciseFutureAbort(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.future.abort", ["future.abort",], async () => {
		const id = await startOwnedFuture(ctx,);
		await ctx.run(["future", "abort", id,],);
		const state = await ctx.run<FutureState>(["future", "peek", id,],);
		expect(state.unknown,).not.toBe(true,);
		expect(state.aborted === true || state.alive === false,).toBe(true,);
	}, { capability: "future-abort-scope", required: false, },);
}

export async function exerciseProjectSurface(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	await exerciseProjectReads(ctx,);
	await exerciseProjectMetadata(ctx,);
	await exerciseProjectTags(ctx,);
	await exerciseProjectPermissions(ctx,);
	await exerciseDiscussionLifecycle(ctx,);
	await exerciseDoctor(ctx,);
	await exerciseFixtures(ctx,);
	await exerciseFutureReads(ctx,);
	await exerciseFutureAbort(ctx,);
}
