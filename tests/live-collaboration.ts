import { expect, } from "bun:test";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";

/**
 * Collaboration live-suite module: owns the wiki / scenario / notebook /
 * dashboard / insight / flow-zone / variables / project-library / jobs /
 * metrics / data-quality coverage for the "core" profile.
 *
 * provisionCollaboration(ctx) — setup phase only. Creates the baseline
 * artifacts once and records their ids in ctx.fixtures.
 *
 * exerciseCollaboration(ctx) — run phase, repeatable. Reuses baseline ids for
 * read / update-and-verify checks, creates transient case-owned resources for
 * full CRUD lifecycles and removes them in finally.
 *
 * All mutations go through the CLI (ctx.run). The SDK client is used only for
 * independent read-back verification.
 */

const WIKI_BASELINE_NAME = "Live collaboration wiki";
const WIKI_BASELINE_CONTENT =
	"# Live collaboration wiki\n\nBaseline article provisioned by the live suite.\n";
const WIKI_BASELINE_CONTENT_V2 = "# Live collaboration wiki (verified)\n";
const SCENARIO_BASELINE_ID = "live_collab_scenario";
const SCENARIO_BASELINE_NAME = "Live collaboration baseline scenario";
const INSIGHT_BASELINE_NAME = "Live collaboration insight";
const DASHBOARD_BASELINE_NAME = "Live collaboration dashboard";
const NOTEBOOK_BASELINE_NAME = "live_collab_notebook";
const ZONE_BASELINE_NAME = "Live collaboration zone";
const ZONE_BASELINE_COLOR = "#2ab1ac";
const ZONE_BASELINE_COLOR_V2 = "#4286f4";
const LIBRARY_DIR = "python/live_collab";
const LIBRARY_FILE = `${LIBRARY_DIR}/check.py`;
const LIBRARY_BASELINE_CONTENT = "# live collaboration baseline\nCOLLAB_MARKER = 'baseline'\n";
const LIBRARY_BASELINE_CONTENT_V2 = "# live collaboration baseline (verified)\n";
const LIBRARY_TRANSIENT_CONTENT = "# transient\n";

const WIKI_TRANSIENT_CONTENT = "# transient\n";
const WIKI_TRANSIENT_CONTENT_V2 = "# transient v2\n";
const VARIABLE_KEY = "liveCollaboration";
const DQ_RULE_DISPLAY_NAME = "Live collaboration row count";
const JOB_BUILD_TIMEOUT_MS = 240_000;
const SCENARIO_WAIT_TIMEOUT_MS = 120_000;
const DQ_COMPUTE_TIMEOUT_MS = 180_000;

function uniq(ctx: LiveContext, base: string,): string {
	return `${base}_i${String(ctx.iteration,)}`;
}

function firstDatasetName(ctx: LiveContext,): string {
	const name = Object.keys(ctx.fixtures.datasets,)[0];
	if (!name) {
		throw new LiveCapabilityError(
			"collaboration requires at least one baseline dataset in ctx.fixtures.datasets; core data module did not provision any",
		);
	}
	return name;
}

function datasetList(ctx: LiveContext, cap = 3,): string[] {
	return Object.keys(ctx.fixtures.datasets,).slice(0, cap,);
}

function expectedRowsFor(ctx: LiveContext, dataset: string,): number | undefined {
	const rows = ctx.fixtures.expectedRows[dataset];
	return typeof rows === "number" ? rows : undefined;
}

function requireString(value: unknown, label: string,): string {
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`Expected non-empty string for ${label}, got ${JSON.stringify(value,)}`,);
	}
	return value;
}

function itemsRef(value: unknown,): string | undefined {
	if (value === null || typeof value !== "object" || Array.isArray(value,)) return undefined;
	for (const role of Object.values(value as Record<string, unknown>,)) {
		const items = (role as { items?: unknown; } | undefined)?.items;
		if (!Array.isArray(items,)) continue;
		for (const item of items) {
			const ref = (item as { ref?: unknown; } | null)?.ref;
			if (typeof ref === "string" && ref.length > 0) return ref;
		}
	}
	return undefined;
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

export async function provisionCollaboration(ctx: LiveContext,): Promise<void> {
	const dataset = firstDatasetName(ctx,);

	await ctx.check("collab.setup.variables", ["variable.set", "variable.get",], async () => {
		const marker = ctx.projectKey;
		await ctx.run([
			"variable",
			"set",
			"--standard",
			JSON.stringify({ [VARIABLE_KEY]: { marker, }, },),
		],);
		const vars = await ctx.run<{ standard: Record<string, unknown>; }>(["variable", "get",],);
		expect((vars.standard[VARIABLE_KEY] as Record<string, unknown> | undefined)?.marker,).toBe(
			marker,
		);
	},);

	await ctx.check("collab.setup.project-library", [
		"project-library.create-folder",
		"project-library.put",
	], async () => {
		await ctx.run(["project-library", "create-folder", LIBRARY_DIR, "--if-not-exists",],);
		const put = await ctx.run<{ sha256: string; }>([
			"project-library",
			"put",
			LIBRARY_FILE,
			"--content",
			LIBRARY_BASELINE_CONTENT,
		],);
		expect(put.sha256,).toMatch(/^[0-9a-f]{64}$/u,);
		const read = await ctx.client.projectLibrary.getFile(LIBRARY_FILE, ctx.projectKey,);
		expect(read,).toBe(LIBRARY_BASELINE_CONTENT,);
	},);

	await ctx.check(
		"collab.setup.flow-zone",
		["flow-zone.create", "flow-zone.move", "flow-zone.get",],
		async () => {
			const zones = await ctx.run<Array<{ id: string; name: string; }>>(["flow-zone", "list",],);
			const existing = zones.find((zone,) => zone.name === ZONE_BASELINE_NAME);
			let createdId: string | undefined = existing?.id;
			if (createdId === undefined) {
				const created = await ctx.run<{ created: string; }>([
					"flow-zone",
					"create",
					"--name",
					ZONE_BASELINE_NAME,
					"--color",
					ZONE_BASELINE_COLOR,
				],);
				createdId = requireString(created.created, "flow-zone create id",);
			}
			const zoneId = createdId;
			await ctx.run(["flow-zone", "move", zoneId, "--dataset", dataset,],);
			const zone = await ctx.run<
				{ id: string; items?: Array<{ objectId: string; objectType: string; }>; }
			>([
				"flow-zone",
				"get",
				zoneId,
			],);
			expect(zone.id,).toBe(zoneId,);
			expect(
				(zone.items ?? []).some((item,) => item.objectType === "DATASET" && item.objectId === dataset),
			).toBe(true,);
		},
	);

	await ctx.check(
		"collab.setup.scenario",
		["scenario.create", "scenario.run-and-wait",],
		async () => {
			const scenarios = await ctx.run<Array<{ id: string; }>>(["scenario", "list",],);
			if (!scenarios.some((scenario,) => scenario.id === SCENARIO_BASELINE_ID)) {
				await ctx.run([
					"scenario",
					"create",
					SCENARIO_BASELINE_ID,
					SCENARIO_BASELINE_NAME,
					"--type",
					"step_based",
				],);
			}
			const wait = await ctx.run<{ scenarioId: string; outcome: string; success: boolean; }>([
				"scenario",
				"run-and-wait",
				SCENARIO_BASELINE_ID,
				"--timeout",
				String(SCENARIO_WAIT_TIMEOUT_MS,),
			],);
			expect(wait.scenarioId,).toBe(SCENARIO_BASELINE_ID,);
			expect(wait.outcome,).toBe("SUCCESS",);
			expect(wait.success,).toBe(true,);
			ctx.fixtures.scenarioId = SCENARIO_BASELINE_ID;
		},
	);

	await ctx.check("collab.setup.insight", ["insight.create", "insight.get",], async () => {
		const insights = await ctx.run<Array<{ id: string; name: string; }>>(["insight", "list",],);
		const existing = insights.find((insight,) => insight.name === INSIGHT_BASELINE_NAME);
		let insightId: string | undefined = existing?.id;
		if (insightId === undefined) {
			const created = await ctx.run<{ created: string; }>([
				"insight",
				"create",
				"--name",
				INSIGHT_BASELINE_NAME,
				"--type",
				"dataset_table",
				"--params",
				JSON.stringify({ datasetSmartName: dataset, },),
				"--listed",
				"true",
			],);
			insightId = requireString(created.created, "insight create id",);
		}
		const details = await ctx.run<{ id: string; type?: string; params?: Record<string, unknown>; }>([
			"insight",
			"get",
			insightId,
		],);
		expect(details.type,).toBe("dataset_table",);
		expect((details.params as Record<string, unknown> | undefined)?.datasetSmartName,).toBe(dataset,);
		ctx.fixtures.insightId = insightId;
	},);

	await ctx.check("collab.setup.dashboard", ["dashboard.create", "dashboard.get",], async () => {
		const dashboards = await ctx.run<Array<{ id: string; name: string; }>>(["dashboard", "list",],);
		const existing = dashboards.find((dashboard,) => dashboard.name === DASHBOARD_BASELINE_NAME);
		let dashboardId: string | undefined = existing?.id;
		if (dashboardId === undefined) {
			const created = await ctx.run<{ created: string; }>([
				"dashboard",
				"create",
				"--name",
				DASHBOARD_BASELINE_NAME,
				"--listed",
				"true",
				"--data",
				JSON.stringify({
					pages: [{
						id: "page1",
						grid: {
							tiles: [{
								tileType: "INSIGHT",
								insightId: ctx.fixtures.insightId,
								x: 0,
								y: 0,
								w: 6,
								h: 6,
							},],
						},
					},],
				},),
			],);
			dashboardId = requireString(created.created, "dashboard create id",);
		}
		const details = await ctx.run<{ name: string; pages?: unknown; }>([
			"dashboard",
			"get",
			dashboardId,
		],);
		expect(details.name,).toBe(DASHBOARD_BASELINE_NAME,);
		expect(JSON.stringify(details,),).toContain(requireString(ctx.fixtures.insightId, "insightId",),);
		ctx.fixtures.dashboardId = dashboardId;
	},);

	await ctx.check(
		"collab.setup.notebook",
		["notebook.save-jupyter", "notebook.get-jupyter",],
		async () => {
			if (!ctx.fixtures.notebookName) {
				const notebooks = await ctx.run<Array<{ name: string; }>>(["notebook", "list-jupyter",],);
				if (!notebooks.some((notebook,) => notebook.name === NOTEBOOK_BASELINE_NAME)) {
					const saved = await ctx.run<{ saved: string; created: boolean; hash: string; }>([
						"notebook",
						"save-jupyter",
						NOTEBOOK_BASELINE_NAME,
						"--data",
						JSON.stringify({
							metadata: {
								kernelspec: { name: "python3", display_name: "Python 3", language: "python", },
							},
							nbformat: 4,
							nbformat_minor: 5,
							cells: [{
								cell_type: "code",
								source: ["print('live collaboration baseline')\n",],
								metadata: {},
								outputs: [],
								execution_count: null,
							},],
						},),
					],);
					expect(saved.created,).toBe(true,);
					expect(saved.hash,).toMatch(/^[0-9a-f]{64}$/u,);
				}
			}
			const name = ctx.fixtures.notebookName ?? NOTEBOOK_BASELINE_NAME;
			const content = await ctx.run<{ cells: Array<{ source: string | string[]; }>; }>([
				"notebook",
				"get-jupyter",
				name,
			],);
			expect(JSON.stringify(content.cells,),).toContain("live collaboration baseline",);
			ctx.fixtures.notebookName = name;
		},
	);

	await ctx.check("collab.setup.wiki", ["wiki.create", "wiki.get",], async () => {
		const articles = await ctx.run<Array<{ article: { id: string; name: string; }; }>>([
			"wiki",
			"list",
		],);
		const existing = articles.find((entry,) => entry.article.name === WIKI_BASELINE_NAME);
		let articleId: string | undefined = existing?.article.id;
		if (articleId === undefined) {
			const created = await ctx.run<{ created: string; }>([
				"wiki",
				"create",
				"--name",
				WIKI_BASELINE_NAME,
				"--content",
				WIKI_BASELINE_CONTENT,
			],);
			articleId = requireString(created.created, "wiki create id",);
		}
		const article = await ctx.run<{ article: { id: string; name: string; }; payload?: string; }>([
			"wiki",
			"get",
			articleId,
		],);
		expect(article.article.name,).toBe(WIKI_BASELINE_NAME,);
		expect(article.payload,).toContain("Live collaboration wiki",);
		ctx.fixtures.wikiArticleId = articleId;
	},);

	await ctx.check(
		"collab.setup.data-quality",
		["data-quality.create-rule", "data-quality.compute",],
		async () => {
			const ruleIds: string[] = [];
			for (const ds of datasetList(ctx,)) {
				const rules = await ctx.run<Array<{ id?: string; displayName?: string; }>>([
					"data-quality",
					"rules",
					ds,
				],);
				const existing = rules.find((rule,) => rule.displayName === DQ_RULE_DISPLAY_NAME);
				if (existing?.id) {
					ruleIds.push(existing.id,);
					continue;
				}
				const created = await ctx.run<{ created: string; id?: string; }>([
					"data-quality",
					"create-rule",
					ds,
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
				ruleIds.push(existing?.id ?? created.id ?? created.created,);
			}
			ctx.fixtures.dataQualityRuleIds = ruleIds;
			const compute = await ctx.run<Record<string, unknown>>([
				"data-quality",
				"compute",
				datasetList(ctx,)[0]!,
				"--wait",
				"--timeout",
				String(DQ_COMPUTE_TIMEOUT_MS,),
			],);
			expect(compute,).toBeTruthy();
		},
	);

	await ctx.check(
		"collab.setup.metrics",
		["metrics.dataset-compute", "metrics.dataset-get",],
		async () => {
			for (const ds of datasetList(ctx, 2,)) {
				await ctx.run(["metrics", "dataset-compute", ds,],);
				const values = await ctx.run<
					{ metrics: Array<{ metric: { id: string; }; lastValues: unknown[]; }>; }
				>([
					"metrics",
					"dataset-get",
					ds,
				],);
				expect(Array.isArray(values.metrics,),).toBe(true,);
				expect(values.metrics.length,).toBeGreaterThan(0,);
			}
		},
	);
}

// ---------------------------------------------------------------------------
// Run (repeatable)
// ---------------------------------------------------------------------------

export async function exerciseCollaboration(ctx: LiveContext,): Promise<void> {
	const dataset = firstDatasetName(ctx,);
	const insightId = requireString(ctx.fixtures.insightId, "fixtures.insightId",);
	const dashboardId = requireString(ctx.fixtures.dashboardId, "fixtures.dashboardId",);
	const scenarioId = requireString(ctx.fixtures.scenarioId, "fixtures.scenarioId",);
	const articleId = requireString(ctx.fixtures.wikiArticleId, "fixtures.wikiArticleId",);

	await ctx.check("collab.variables", ["variable.get", "variable.set",], async () => {
		const vars = await ctx.run<{ standard: Record<string, Record<string, unknown>>; }>([
			"variable",
			"get",
		],);
		expect(vars.standard[VARIABLE_KEY],).toBeTruthy();
		const iteration = ctx.iteration;
		await ctx.run([
			"variable",
			"set",
			"--standard",
			JSON.stringify({
				...vars.standard,
				[VARIABLE_KEY]: { ...vars.standard[VARIABLE_KEY], iteration, },
			},),
		],);
		const after = await ctx.run<{ standard: Record<string, Record<string, unknown>>; }>([
			"variable",
			"get",
		],);
		expect(after.standard[VARIABLE_KEY]?.iteration,).toBe(iteration,);
		expect(after.standard[VARIABLE_KEY]?.marker,).toBe(vars.standard[VARIABLE_KEY]?.marker,);
	},);

	await ctx.check("collab.project-metadata", [
		"project.metadata",
		"project.settings-get",
		"project.settings-set",
	], async () => {
		const metadata = await ctx.run<Record<string, unknown>>(["project", "metadata",],);
		expect(typeof metadata,).toBe("object",);
		const settings = await ctx.run<Record<string, unknown>>(["project", "settings-get",],);
		const keys = Object.keys(settings,);
		if (keys.length === 0) {
			throw new LiveCapabilityError("project settings are empty; cannot round-trip a settings key",);
		}
		const key = keys[0]!;
		await ctx.run([
			"project",
			"settings-set",
			"--data",
			JSON.stringify({ [key]: settings[key], },),
		],);
		const after = await ctx.run<Record<string, unknown>>(["project", "settings-get",],);
		expect(JSON.stringify(after[key],),).toBe(JSON.stringify(settings[key],),);
	},);

	await ctx.check(
		"collab.flow-zone",
		["flow-zone.list", "flow-zone.get", "flow-zone.update",],
		async () => {
			const zones = await ctx.run<Array<{ id: string; name: string; color?: string; }>>([
				"flow-zone",
				"list",
			],);
			const zone = zones.find((entry,) => entry.name === ZONE_BASELINE_NAME);
			expect(zone,).toBeTruthy();
			const zoneId = requireString(zone?.id, "baseline flow zone id",);
			const found = await ctx.run<Array<{ id: string; }>>([
				"flow-zone",
				"find",
				"--dataset",
				dataset,
			],);
			expect(found.some((entry,) => entry.id === zoneId),).toBe(true,);
			const updated = await ctx.run<{ name?: string; color?: string; }>([
				"flow-zone",
				"update",
				zoneId,
				"--color",
				ZONE_BASELINE_COLOR_V2,
			],);
			expect(updated.color,).toBe(ZONE_BASELINE_COLOR_V2,);
			const restored = await ctx.run<{ color?: string; }>([
				"flow-zone",
				"update",
				zoneId,
				"--color",
				ZONE_BASELINE_COLOR,
			],);
			expect(restored.color,).toBe(ZONE_BASELINE_COLOR,);
		},
	);

	await ctx.check("collab.scenario", [
		"scenario.get",
		"scenario.status",
		"scenario.update",
		"scenario.run",
		"scenario.run-and-wait",
	], async () => {
		const details = await ctx.run<{ id?: string; name?: string; }>(["scenario", "get", scenarioId,],);
		expect(details.name,).toBe(SCENARIO_BASELINE_NAME,);
		const status = await ctx.run<{ id?: string; lastRun?: { outcome?: string; }; }>([
			"scenario",
			"status",
			scenarioId,
		],);
		expect(typeof status,).toBe("object",);
		const iteratedName = `${SCENARIO_BASELINE_NAME} (iter ${String(ctx.iteration,)})`;
		const update = await ctx.run<{ verified: boolean; changed: boolean; }>([
			"scenario",
			"update",
			scenarioId,
			"--data",
			JSON.stringify({ name: iteratedName, },),
		],);
		expect(update.verified,).toBe(true,);
		expect(update.changed,).toBe(true,);
		const after = await ctx.run<{ name?: string; }>(["scenario", "get", scenarioId,],);
		expect(after.name,).toBe(iteratedName,);
		await ctx.run([
			"scenario",
			"update",
			scenarioId,
			"--data",
			JSON.stringify({ name: SCENARIO_BASELINE_NAME, },),
		],);
		const run = await ctx.run<{ runId: string; }>(["scenario", "run", scenarioId,],);
		expect(run.runId.length,).toBeGreaterThan(0,);
		const wait = await ctx.run<
			{ scenarioId: string; outcome: string; success: boolean; steps?: unknown[]; }
		>([
			"scenario",
			"run-and-wait",
			scenarioId,
			"--timeout",
			String(SCENARIO_WAIT_TIMEOUT_MS,),
		],);
		expect(wait.scenarioId,).toBe(scenarioId,);
		expect(wait.outcome,).toBe("SUCCESS",);
		expect(wait.success,).toBe(true,);
	},);

	await ctx.check(
		"collab.wiki",
		["wiki.settings", "wiki.list", "wiki.get", "wiki.update",],
		async () => {
			const settings = await ctx.run<Record<string, unknown>>(["wiki", "settings",],);
			expect(typeof settings,).toBe("object",);
			const list = await ctx.run<Array<{ article: { id: string; }; }>>(["wiki", "list",],);
			expect(list.some((entry,) => entry.article.id === articleId),).toBe(true,);
			const updated = await ctx.run<{ article: { id: string; }; payload?: string; }>([
				"wiki",
				"update",
				articleId,
				"--content",
				WIKI_BASELINE_CONTENT_V2,
			],);
			expect(updated.article.id,).toBe(articleId,);
			const after = await ctx.run<{ payload?: string; }>(["wiki", "get", articleId,],);
			expect(after.payload,).toBe(WIKI_BASELINE_CONTENT_V2,);
			await ctx.run(["wiki", "update", articleId, "--content", WIKI_BASELINE_CONTENT,],);
			const restored = await ctx.run<{ payload?: string; }>(["wiki", "get", articleId,],);
			expect(restored.payload,).toBe(WIKI_BASELINE_CONTENT,);
		},
	);

	await ctx.check("collab.wiki-lifecycle", [
		"wiki.create",
		"wiki.get",
		"wiki.update",
		"wiki.delete",
	], async () => {
		const name = uniq(ctx, "live_collab_wiki",);
		const created = await ctx.run<{ created: string; }>([
			"wiki",
			"create",
			"--name",
			name,
			"--content",
			WIKI_TRANSIENT_CONTENT,
		],);
		const id = requireString(created.created, "transient wiki id",);
		try {
			const got = await ctx.run<{ article: { id: string; name: string; }; }>(["wiki", "get", id,],);
			expect(got.article.name,).toBe(name,);
			const updated = await ctx.run<{ article: { id: string; }; payload?: string; }>([
				"wiki",
				"update",
				id,
				"--content",
				WIKI_TRANSIENT_CONTENT_V2,
			],);
			expect(updated.article.id,).toBe(id,);
			const after = await ctx.run<{ payload?: string; }>(["wiki", "get", id,],);
			expect(after.payload,).toBe(WIKI_TRANSIENT_CONTENT_V2,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>(["wiki", "delete", id,],);
			expect(deleted.deleted,).toBe(id,);
		}
	},);

	await ctx.check("collab.notebook", [
		"notebook.list-jupyter",
		"notebook.get-jupyter",
		"notebook.save-jupyter",
	], async () => {
		const name = requireString(ctx.fixtures.notebookName, "fixtures.notebookName",);
		const list = await ctx.run<Array<{ name: string; }>>(["notebook", "list-jupyter",],);
		expect(list.some((notebook,) => notebook.name === name),).toBe(true,);
		const before = await ctx.run<
			{ metadata: Record<string, unknown>; nbformat: number; cells: Array<Record<string, unknown>>; }
		>([
			"notebook",
			"get-jupyter",
			name,
		],);
		const cellCountBefore = before.cells.length;
		const appended = {
			...before,
			cells: [...before.cells, {
				cell_type: "markdown",
				source: ["live collaboration probe\n",],
				metadata: {},
			},],
		};
		const saved = await ctx.run<{ saved: string; created: boolean; hash: string; }>([
			"notebook",
			"save-jupyter",
			name,
			"--data",
			JSON.stringify(appended,),
		],);
		expect(saved.saved,).toBe(name,);
		expect(saved.hash,).toMatch(/^[0-9a-f]{64}$/u,);
		const after = await ctx.run<{ cells: Array<Record<string, unknown>>; }>([
			"notebook",
			"get-jupyter",
			name,
		],);
		expect(after.cells.length,).toBe(cellCountBefore + 1,);
		await ctx.run(["notebook", "save-jupyter", name, "--data", JSON.stringify(before,),],);
		const restored = await ctx.run<{ cells: Array<Record<string, unknown>>; }>([
			"notebook",
			"get-jupyter",
			name,
		],);
		expect(restored.cells.length,).toBe(cellCountBefore,);
	},);

	await ctx.check("collab.insight", ["insight.list", "insight.get", "insight.update",], async () => {
		const list = await ctx.run<Array<{ id: string; }>>(["insight", "list",],);
		expect(list.some((insight,) => insight.id === insightId),).toBe(true,);
		const details = await ctx.run<{ id: string; type?: string; params?: Record<string, unknown>; }>([
			"insight",
			"get",
			insightId,
		],);
		expect(details.type,).toBe("dataset_table",);
		const originalDataset = requireString(
			(details.params as Record<string, unknown> | undefined)?.datasetSmartName,
			"insight params.datasetSmartName",
		);
		const altDataset = Object.keys(ctx.fixtures.datasets,).find((name,) => name !== originalDataset)
			?? originalDataset;
		if (altDataset !== originalDataset) {
			const updated = await ctx.run<{ params?: Record<string, unknown>; }>([
				"insight",
				"update",
				insightId,
				"--params",
				JSON.stringify({ datasetSmartName: altDataset, },),
			],);
			expect((updated.params as Record<string, unknown> | undefined)?.datasetSmartName,).toBe(
				altDataset,
			);
			await ctx.run([
				"insight",
				"update",
				insightId,
				"--params",
				JSON.stringify({ datasetSmartName: originalDataset, },),
			],);
			const restored = await ctx.run<{ params?: Record<string, unknown>; }>([
				"insight",
				"get",
				insightId,
			],);
			expect((restored.params as Record<string, unknown> | undefined)?.datasetSmartName,).toBe(
				originalDataset,
			);
		}
	},);

	await ctx.check("collab.insight-lifecycle", [
		"insight.create",
		"insight.get",
		"insight.update",
		"insight.delete",
	], async () => {
		const name = uniq(ctx, "Live collab insight",);
		const created = await ctx.run<{ created: string; }>([
			"insight",
			"create",
			"--name",
			name,
			"--type",
			"dataset_table",
			"--params",
			JSON.stringify({ datasetSmartName: dataset, },),
			"--listed",
			"false",
		],);
		const id = requireString(created.created, "transient insight id",);
		try {
			const got = await ctx.run<{ id: string; type?: string; }>(["insight", "get", id,],);
			expect(got.type,).toBe("dataset_table",);
			const updated = await ctx.run<{ params?: Record<string, unknown>; }>([
				"insight",
				"update",
				id,
				"--params",
				JSON.stringify({ datasetSmartName: dataset, },),
			],);
			expect((updated.params as Record<string, unknown> | undefined)?.datasetSmartName,).toBe(
				dataset,
			);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>(["insight", "delete", id,],);
			expect(deleted.deleted,).toBe(id,);
		}
	},);

	await ctx.check(
		"collab.dashboard",
		["dashboard.list", "dashboard.get", "dashboard.update",],
		async () => {
			const list = await ctx.run<Array<{ id: string; }>>(["dashboard", "list",],);
			expect(list.some((dashboard,) => dashboard.id === dashboardId),).toBe(true,);
			const got = await ctx.run<{ name: string; }>(["dashboard", "get", dashboardId,],);
			expect(got.name,).toBe(DASHBOARD_BASELINE_NAME,);
			const iteratedName = `${DASHBOARD_BASELINE_NAME} (iter ${String(ctx.iteration,)})`;
			const updated = await ctx.run<{ name: string; }>([
				"dashboard",
				"update",
				dashboardId,
				"--name",
				iteratedName,
			],);
			expect(updated.name,).toBe(iteratedName,);
			const restored = await ctx.run<{ name: string; }>([
				"dashboard",
				"update",
				dashboardId,
				"--name",
				DASHBOARD_BASELINE_NAME,
			],);
			expect(restored.name,).toBe(DASHBOARD_BASELINE_NAME,);
		},
	);

	await ctx.check("collab.dashboard-lifecycle", [
		"dashboard.create",
		"dashboard.get",
		"dashboard.update",
		"dashboard.delete",
	], async () => {
		const name = uniq(ctx, "Live collab dashboard",);
		const created = await ctx.run<{ created: string; }>([
			"dashboard",
			"create",
			"--name",
			name,
			"--listed",
			"false",
			"--data",
			JSON.stringify({
				pages: [{
					id: "page1",
					grid: { tiles: [{ tileType: "INSIGHT", insightId, x: 0, y: 0, w: 6, h: 6, },], },
				},],
			},),
		],);
		const id = requireString(created.created, "transient dashboard id",);
		try {
			const got = await ctx.run<{ name: string; pages?: unknown; }>(["dashboard", "get", id,],);
			expect(got.name,).toBe(name,);
			expect(JSON.stringify(got.pages,),).toContain(insightId,);
			const updated = await ctx.run<{ name: string; }>([
				"dashboard",
				"update",
				id,
				"--listed",
				"true",
			],);
			expect(updated.name,).toBe(name,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>(["dashboard", "delete", id,],);
			expect(deleted.deleted,).toBe(id,);
		}
	},);

	await ctx.check("collab.project-library", [
		"project-library.list",
		"project-library.get",
		"project-library.get-bytes",
		"project-library.diff",
		"project-library.put",
		"project-library.create-file",
		"project-library.rename",
		"project-library.move",
		"project-library.delete",
	], async () => {
		const contents = await ctx.run<Array<{ name: string; children?: Array<{ name: string; }>; }>>([
			"project-library",
			"list",
		],);
		const dir = contents.find((item,) => item.name === LIBRARY_DIR.split("/",)[0]);
		expect(dir,).toBeTruthy();
		const body = await ctx.run<string>(["project-library", "get", LIBRARY_FILE,],);
		expect(body,).toBe(LIBRARY_BASELINE_CONTENT,);
		const bytes = await ctx.run<{ path: string; sha256: string; }>([
			"project-library",
			"get-bytes",
			LIBRARY_FILE,
			"--output",
			`${ctx.dir}/${uniq(ctx, "live_collab_bytes.bin",)}`,
		],);
		expect(bytes.sha256,).toMatch(/^[0-9a-f]{64}$/u,);
		const diff = await ctx.run<{ unchanged: boolean; }>([
			"project-library",
			"diff",
			LIBRARY_FILE,
			"--content",
			LIBRARY_BASELINE_CONTENT,
		],);
		expect(diff.unchanged,).toBe(true,);
		const put = await ctx.run<{ sha256: string; }>([
			"project-library",
			"put",
			LIBRARY_FILE,
			"--content",
			LIBRARY_BASELINE_CONTENT_V2,
			"--expect-sha256",
			bytes.sha256,
		],);
		expect(put.sha256,).toMatch(/^[0-9a-f]{64}$/u,);
		expect(put.sha256,).not.toBe(bytes.sha256,);
		await ctx.run(["project-library", "put", LIBRARY_FILE, "--content", LIBRARY_BASELINE_CONTENT,],);
		const restored = await ctx.run<string>(["project-library", "get", LIBRARY_FILE,],);
		expect(restored,).toBe(LIBRARY_BASELINE_CONTENT,);

		const transientDir = uniq(ctx, "python/live_collab/transient",);
		const transientFile = `${transientDir}/probe.py`;
		const transientRenamed = `${transientDir}/renamed.py`;
		try {
			await ctx.run(["project-library", "create-folder", transientDir, "--if-not-exists",],);
			await ctx.run(["project-library", "create-file", transientFile, "--if-not-exists",],);
			await ctx.run([
				"project-library",
				"put",
				transientFile,
				"--content",
				LIBRARY_TRANSIENT_CONTENT,
			],);
			const renamed = await ctx.run<{ renamed: string; to: string; }>([
				"project-library",
				"rename",
				transientFile,
				"renamed.py",
			],);
			expect(renamed.to,).toBe("renamed.py",);
			const destination = `${transientDir}/destination`;
			await ctx.run(["project-library", "create-folder", destination,],);
			await ctx.run([
				"project-library",
				"move",
				transientRenamed,
				destination,
			],);
			const afterMove = await ctx.run<string>([
				"project-library",
				"get",
				`${destination}/renamed.py`,
			],);
			expect(afterMove,).toBe(LIBRARY_TRANSIENT_CONTENT,);
		} finally {
			await ctx.run(["project-library", "delete", transientDir,],);
		}
	},);

	await ctx.check("collab.jobs", [
		"job.build-and-wait",
		"job.list",
		"job.get",
		"job.summary",
		"job.log",
	], async () => {
		const recipes = await ctx.run<Array<{ name: string; }>>(["recipe", "list",],);
		const recipeName = Object.keys(ctx.fixtures.recipes,)[0] ?? recipes[0]?.name;
		expect(recipeName,).toBeTruthy();
		const recipe = await ctx.run<{ recipe: { outputs?: unknown; }; }>([
			"recipe",
			"get",
			recipeName!,
		],);
		const output = itemsRef(recipe.recipe.outputs,);
		expect(output,).toBeTruthy();
		const build = await ctx.run<{ success: boolean; jobId: string; state: string; }>([
			"job",
			"build-and-wait",
			output!,
			"--build-mode",
			"NON_RECURSIVE_BUILD",
			"--timeout",
			String(JOB_BUILD_TIMEOUT_MS,),
			"--summary",
		],);
		expect(build.success,).toBe(true,);
		expect(build.state,).toBe("DONE",);
		const jobId = requireString(build.jobId, "build job id",);
		const list = await ctx.run<unknown>(["job", "list", "--latest", "--limit", "5",],);
		expect(JSON.stringify(list,),).toContain(jobId,);
		const job = await ctx.run<Record<string, unknown>>(["job", "get", jobId,],);
		expect(JSON.stringify(job,),).toContain(jobId,);
		const summary = await ctx.run<Record<string, unknown>>(["job", "summary", jobId,],);
		expect(typeof summary,).toBe("object",);
		const log = await ctx.run<string>(["job", "log", jobId, "--max-lines", "20",],);
		expect(typeof log,).toBe("string",);
		const expected = expectedRowsFor(ctx, output!,);
		if (expected !== undefined) {
			const assertCount = await ctx.run<{ satisfied: boolean; count: number; }>([
				"dataset",
				"assert-count",
				output!,
				"--expected",
				String(expected,),
			],);
			expect(assertCount.satisfied,).toBe(true,);
		}
	},);

	await ctx.check("collab.metrics", [
		"metrics.dataset-get",
		"metrics.dataset-compute",
		"metrics.dataset-history",
		"metrics.folder-get",
	], async () => {
		const computed = await ctx.run<Record<string, unknown>>([
			"metrics",
			"dataset-compute",
			dataset,
		],);
		expect(computed,).toBeTruthy();
		const values = await ctx.run<
			{ metrics: Array<{ metric: { id: string; }; lastValues: unknown[]; }>; }
		>([
			"metrics",
			"dataset-get",
			dataset,
		],);
		const records = values.metrics.find((entry,) => entry.metric.id === "basic:COUNT_COLUMNS");
		expect(records,).toBeTruthy();
		const history = await ctx.run<{ values: unknown[]; }>([
			"metrics",
			"dataset-history",
			dataset,
			"basic:COUNT_COLUMNS",
		],);
		expect(Array.isArray(history.values,),).toBe(true,);
		expect(history.values.length,).toBeGreaterThan(0,);
		if (ctx.fixtures.folderId) {
			const folderMetrics = await ctx.run<{ metrics: unknown[]; }>([
				"metrics",
				"folder-get",
				ctx.fixtures.folderId as string,
			],);
			expect(Array.isArray(folderMetrics.metrics,),).toBe(true,);
		}
	},);

	await ctx.check("collab.data-quality", [
		"data-quality.rules",
		"data-quality.get-rule",
		"data-quality.status",
		"data-quality.last-results",
		"data-quality.assert-results",
		"data-quality.history",
		"data-quality.project-status",
		"data-quality.compute",
		"data-quality.create-rule",
		"data-quality.update-rule",
		"data-quality.delete-rule",
	], async () => {
		const rules = await ctx.run<Array<{ id?: string; displayName?: string; }>>([
			"data-quality",
			"rules",
			dataset,
		],);
		const baselineRule = rules.find((rule,) => rule.displayName === DQ_RULE_DISPLAY_NAME);
		expect(baselineRule,).toBeTruthy();
		const ruleId = requireString(baselineRule?.id, "baseline data quality rule id",);
		const got = await ctx.run<{ id?: string; type?: string; }>([
			"data-quality",
			"get-rule",
			dataset,
			ruleId,
		],);
		expect(got.type,).toBe("RecordCountInRangeRule",);
		await ctx.run([
			"data-quality",
			"compute",
			dataset,
			"--rule-id",
			ruleId,
			"--wait",
			"--timeout",
			String(DQ_COMPUTE_TIMEOUT_MS,),
		],);
		const last = await ctx.run<Array<{ id?: string; outcome?: string; status?: string; }>>([
			"data-quality",
			"last-results",
			dataset,
			"--rule-id",
			ruleId,
		],);
		expect(last.length,).toBeGreaterThan(0,);
		const assertResults = await ctx.run<{ satisfied: boolean; checked: number; failed: unknown[]; }>([
			"data-quality",
			"assert-results",
			dataset,
			"--rule-id",
			ruleId,
		],);
		expect(assertResults.satisfied,).toBe(true,);
		expect(assertResults.checked,).toBeGreaterThan(0,);
		expect(assertResults.failed,).toEqual([],);
		const status = await ctx.run<unknown>(["data-quality", "status", dataset,],);
		expect(status,).toBeTruthy();
		const history = await ctx.run<unknown>([
			"data-quality",
			"history",
			dataset,
			"--rule-id",
			ruleId,
		],);
		expect(history,).toBeTruthy();
		const projectStatus = await ctx.run<unknown>(["data-quality", "project-status",],);
		expect(JSON.stringify(projectStatus,),).toContain(dataset,);

		const transientName = uniq(ctx, "Live collab dq rule",);
		const created = await ctx.run<{ created: string; }>([
			"data-quality",
			"create-rule",
			dataset,
			"--data",
			JSON.stringify({
				type: "RecordCountInRangeRule",
				displayName: transientName,
				softMinimum: 0,
				softMinimumEnabled: true,
				hardMaximum: 1_000_000_000,
				hardMaximumEnabled: true,
			},),
		],);
		const transientId = requireString(created.created, "transient data quality rule id",);
		try {
			const updated = await ctx.run<Record<string, unknown>>([
				"data-quality",
				"update-rule",
				dataset,
				transientId,
				"--data",
				JSON.stringify({ enabled: false, },),
			],);
			expect(updated,).toBeTruthy();
			const gotTransient = await ctx.run<{ id?: string; enabled?: boolean; }>([
				"data-quality",
				"get-rule",
				dataset,
				transientId,
			],);
			expect(gotTransient.enabled,).toBe(false,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>([
				"data-quality",
				"delete-rule",
				dataset,
				transientId,
			],);
			expect(deleted.deleted,).toBe(transientId,);
		}
	},);
}
