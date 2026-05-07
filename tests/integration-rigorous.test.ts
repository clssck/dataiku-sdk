import { afterAll, describe, expect, it, } from "bun:test";
import { mkdtemp, readFile, rm, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import type { DataikuClient, } from "../src/client.js";
import type { DssRawResult, } from "./integration-harness.js";
import {
	createCleanupStack,
	createClient,
	describeIntegration,
	describeMutatingProjectIntegration,
	describeProjectIntegration,
	dssRaw,
	parseJsonOutput,
	uniqueTestName,
} from "./integration-harness.js";
import {
	localValidationCases,
	type ReadOnlyCommandCase,
	readOnlyCommandCases,
	type SdkParityKind,
} from "./integration-matrix.js";

type FindingCategory =
	| "automation-risk"
	| "bug"
	| "docs-gap"
	| "dss-behavior"
	| "feature-opportunity"
	| "resource-gap"
	| "ux-friction";

type IntegrationFinding = {
	id: string;
	category: FindingCategory;
	status: "observed" | "candidate" | "skipped";
	severity: "low" | "medium" | "high";
	persona?: string;
	command?: string;
	observed: string;
	expected?: string;
	suggestedAction?: string;
	cleanupVerified?: boolean;
};

const findings: IntegrationFinding[] = [];
const findingIds = new Set<string>();

function addFinding(finding: IntegrationFinding,): void {
	if (findingIds.has(finding.id,)) return;
	findingIds.add(finding.id,);
	findings.push(finding,);
}

afterAll(() => {
	if (process.env.RUN_DATAIKU_INTEGRATION_REPORT === "1") {
		process.stdout.write(`${JSON.stringify({ integrationFindings: findings, }, null, 2,)}\n`,);
	}
},);

function commandWithProject(entry: ReadOnlyCommandCase,): string[] {
	if (!entry.requiresProject) return entry.args;
	if (entry.args.includes("--project-key",)) return entry.args;
	return [...entry.args, "--project-key", process.env.DATAIKU_PROJECT_KEY!,];
}

function parseCliJson(result: DssRawResult, label: string,): unknown {
	expect(result.code, `${label} exit code\nstderr=${result.stderr}`,).toBe(0,);
	expect(result.stdout.trim().length, `${label} stdout`,).toBeGreaterThan(0,);
	return parseJsonOutput(result.stdout,);
}

function expectResultShape(
	value: unknown,
	shape: ReadOnlyCommandCase["resultShape"],
	label: string,
): void {
	if (shape === "array") {
		expect(Array.isArray(value,), label,).toBe(true,);
		return;
	}
	if (shape === "object") {
		expect(value !== null && typeof value === "object" && !Array.isArray(value,), label,).toBe(true,);
		return;
	}
	expect(typeof value, label,).toBe("string",);
}

function arrayComparable(
	value: unknown,
	fields: string[] | undefined,
): Array<Record<string, unknown>> {
	if (!Array.isArray(value,)) return [];
	return value.map((item,) => {
		if (item === null || typeof item !== "object" || Array.isArray(item,)) return {};
		const record = item as Record<string, unknown>;
		const out: Record<string, unknown> = {};
		for (const field of fields ?? []) {
			if (record[field] !== undefined) out[field] = record[field];
		}
		return out;
	},);
}

function stableObjectFields(
	value: unknown,
	fields: string[] | undefined,
): Record<string, unknown> {
	if (value === null || typeof value !== "object" || Array.isArray(value,)) return {};
	const record = value as Record<string, unknown>;
	const out: Record<string, unknown> = {};
	for (const field of fields ?? []) {
		if (record[field] !== undefined) out[field] = record[field];
	}
	return out;
}

async function sdkValue(kind: SdkParityKind, client: DataikuClient,): Promise<unknown> {
	switch (kind) {
		case "project-list":
			return client.projects.list();
		case "project-get":
			return client.projects.get();
		case "project-metadata":
			return client.projects.metadata();
		case "project-map":
			return client.projects.map({ maxNodes: 25, maxEdges: 50, },);
		case "flow-zone-list":
			return client.flowZones.list();
		case "dataset-list":
			return client.datasets.list();
		case "recipe-list":
			return client.recipes.list();
		case "job-list":
			return client.jobs.list();
		case "scenario-list":
			return client.scenarios.list();
		case "folder-list":
			return client.folders.list();
		case "variable-get":
			return client.variables.get();
		case "connection-infer":
			return client.connections.infer();
		case "code-env-list":
			return client.codeEnvs.list();
		case "dashboard-list":
			return client.dashboards.list();
		case "insight-list":
			return client.insights.list();
		case "notebook-list-jupyter":
			return client.notebooks.listJupyter();
		case "notebook-list-sql":
			return client.notebooks.listSql();
		case "wiki-settings":
			return client.wiki.settings();
		case "wiki-list":
			return client.wiki.list();
	}
}

describeIntegration("Rigorous integration: CLI discoverability and local validation", () => {
	it("prints help for every registered command without resolving DSS resources", async () => {
		const registryResult = await dssRaw(["commands",],);
		const registry = parseCliJson(registryResult, "commands",) as Record<
			string,
			Record<string, unknown>
		>;

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const action of Object.keys(actions,)) {
				const help = await dssRaw([resource, action, "--help",],);
				expect(help.code, `${resource} ${action} --help`,).toBe(0,);
				expect(help.stderr, `${resource} ${action} --help`,).toContain("Usage:",);
			}
		}
	}, 60_000,);

	for (const scenario of localValidationCases) {
		it(`locally rejects ${scenario.id}`, async () => {
			const result = await dssRaw(scenario.args,);
			expect(result.code, `${scenario.id} exit code`,).toBe(scenario.expectedCode,);
			expect(`${result.stdout}${result.stderr}`, scenario.id,).toContain(scenario.expectedMessage,);
		});
	}
},);

describeProjectIntegration("Rigorous integration: read-only SDK/CLI parity", () => {
	for (const entry of readOnlyCommandCases) {
		it(`checks ${entry.id} JSON output contract`, async () => {
			if (entry.featureProbe) {
				addFinding({
					id: `${entry.id}-feature-probe`,
					category: "feature-opportunity",
					status: "candidate",
					severity: "low",
					persona: entry.persona,
					command: entry.args.join(" ",),
					observed: entry.featureProbe,
				},);
			}

			const args = commandWithProject(entry,);
			const jsonResult = await dssRaw(args,);
			const cliJson = parseCliJson(jsonResult, entry.id,);
			expectResultShape(cliJson, entry.resultShape, `${entry.id} result shape`,);
		});

		if (entry.sdkParity) {
			it(`compares ${entry.id} SDK and CLI stable fields`, async () => {
				const client = createClient();
				const args = commandWithProject(entry,);
				const cliJson = parseCliJson(await dssRaw(args,), `${entry.id} cli`,);
				const sdkJson = await sdkValue(entry.sdkParity, client,);

				if (entry.resultShape === "array") {
					expect(Array.isArray(sdkJson,), `${entry.id} sdk array`,).toBe(true,);
					expect((cliJson as unknown[]).length, `${entry.id} list count`,).toBe(
						(sdkJson as unknown[]).length,
					);
					if ((cliJson as unknown[]).length > 0 && entry.stableFields?.length) {
						expect(arrayComparable(cliJson, entry.stableFields,)[0],).toEqual(
							arrayComparable(sdkJson, entry.stableFields,)[0],
						);
					}
					return;
				}

				expect(stableObjectFields(cliJson, entry.stableFields,),).toEqual(
					stableObjectFields(sdkJson, entry.stableFields,),
				);
			});
		}
	}

	it("returns retry metadata for a forced timeout", async () => {
		const result = await dssRaw([
			"--request-timeout",
			"1",
			"flow-zone",
			"list",
			"--project-key",
			process.env.DATAIKU_PROJECT_KEY!,
		],);
		expect(result.code, "timeout should be transient failure",).toBe(3,);
		const payload = parseJsonOutput<Record<string, unknown>>(result.stderr || result.stdout,);
		expect(payload.category,).toBe("transient",);
		expect(payload.retryable,).toBe(true,);
		expect(String(payload.error ?? "",),).toContain("Retry attempts:",);
	}, 30_000,);
},);

describeMutatingProjectIntegration("Rigorous integration: safe mutating workflows", () => {
	it("round-trips a temporary flow zone through SDK and CLI", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let zoneId: string | undefined;

		try {
			const zoneName = uniqueTestName("sdk_cli_it_zone",);
			const created = await client.flowZones.create({ name: zoneName, color: "#2ab1ac", },);
			zoneId = created.id;
			cleanup.defer(async () => {
				if (zoneId) await client.flowZones.delete(zoneId,);
			},);
			expect(created.name,).toBe(zoneName,);

			const dryRun = parseCliJson(
				await dssRaw(["flow-zone", "delete", zoneId, "--dryrun",],),
				"flow-zone dryrun",
			) as Record<string, unknown>;
			expect(dryRun.dryRun,).toBe(true,);

			const datasets = await client.datasets.list();
			const firstDataset = datasets.find((dataset,) => typeof dataset.name === "string");
			if (!firstDataset?.name) {
				addFinding({
					id: "flow-zone-move-no-datasets",
					category: "resource-gap",
					status: "skipped",
					severity: "low",
					observed: "No dataset was available to test flow-zone move.",
					cleanupVerified: true,
				},);
			} else {
				const moved = parseCliJson(
					await dssRaw(["flow-zone", "move", zoneId, "--dataset", firstDataset.name,],),
					"flow-zone move",
				) as { items?: Array<{ objectId?: string; objectType?: string; }>; };
				expect(
					moved.items?.some((item,) =>
						item.objectType === "DATASET" && item.objectId === firstDataset.name
					),
				)
					.toBe(true,);
			}

			await client.flowZones.delete(zoneId,);
			zoneId = undefined;
			expect(await client.flowZones.list(),).not.toContainEqual(
				expect.objectContaining({ id: created.id, },),
			);
		} finally {
			await cleanup.run();
		}
	});

	it("round-trips disposable wiki articles and dashboards through SDK and CLI", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let articleId: string | undefined;
		let dashboardId: string | undefined;

		try {
			const articleName = uniqueTestName("sdk_cli_it_wiki",);
			const createdArticle = parseCliJson(
				await dssRaw(["wiki", "create", "--name", articleName, "--content", "initial body",],),
				"wiki create",
			) as { article?: { id?: string; name?: string; }; payload?: string; };
			articleId = createdArticle.article?.id;
			expect(articleId, "created wiki article id",).toBeTruthy();
			cleanup.defer(async () => {
				if (articleId) await client.wiki.delete(articleId,).catch(() => undefined);
			},);
			expect(createdArticle.article?.name,).toBe(articleName,);
			expect(createdArticle.payload,).toBe("initial body",);

			const sdkArticle = await client.wiki.get(articleId!,);
			expect(sdkArticle.payload,).toBe("initial body",);
			const renamedArticle = await client.wiki.update(articleId!, {
				name: `${articleName}_renamed`,
				content: "updated body",
			},);
			expect(renamedArticle.article.name,).toBe(`${articleName}_renamed`,);
			expect(renamedArticle.payload,).toBe("updated body",);
			const cliArticle = parseCliJson(await dssRaw(["wiki", "get", articleId!,],), "wiki get",) as {
				article?: { name?: string; };
				payload?: string;
			};
			expect(cliArticle.article?.name,).toBe(`${articleName}_renamed`,);
			expect(cliArticle.payload,).toBe("updated body",);

			const wikiDeleteDryRun = parseCliJson(
				await dssRaw(["wiki", "delete", articleId!, "--dry-run",],),
				"wiki delete dry-run",
			) as Record<string, unknown>;
			expect(wikiDeleteDryRun.dryRun,).toBe(true,);
			expect((await client.wiki.get(articleId!,)).article.id,).toBe(articleId,);
			parseCliJson(await dssRaw(["wiki", "delete", articleId!,],), "wiki delete",);
			const deletedArticleId = articleId;
			articleId = undefined;
			const remainingArticles = await client.wiki.list();
			expect(remainingArticles.map((article,) => article.article.id),).not.toContain(
				deletedArticleId,
			);

			const dashboardName = uniqueTestName("sdk_cli_it_dashboard",);
			const createdDashboard = parseCliJson(
				await dssRaw(["dashboard", "create", "--name", dashboardName,],),
				"dashboard create",
			) as { id?: string; name?: string; pages?: unknown[]; };
			dashboardId = createdDashboard.id;
			expect(dashboardId, "created dashboard id",).toBeTruthy();
			cleanup.defer(async () => {
				if (dashboardId) await client.dashboards.delete(dashboardId,).catch(() => undefined);
			},);
			expect(createdDashboard.name,).toBe(dashboardName,);
			expect(Array.isArray(createdDashboard.pages,),).toBe(true,);

			const dashboardDeleteDryRun = parseCliJson(
				await dssRaw(["dashboard", "delete", dashboardId!, "--dry-run",],),
				"dashboard delete dry-run",
			) as Record<string, unknown>;
			expect(dashboardDeleteDryRun.dryRun,).toBe(true,);
			expect((await client.dashboards.get(dashboardId!,)).id,).toBe(dashboardId,);
			const renamedDashboard = await client.dashboards.update(dashboardId!, {
				name: `${dashboardName}_renamed`,
			},);
			expect(renamedDashboard.name,).toBe(`${dashboardName}_renamed`,);
			const cliDashboard = parseCliJson(
				await dssRaw(["dashboard", "get", dashboardId!,],),
				"dashboard get",
			) as {
				name?: string;
			};
			expect(cliDashboard.name,).toBe(`${dashboardName}_renamed`,);
			parseCliJson(await dssRaw(["dashboard", "delete", dashboardId!,],), "dashboard delete",);
			const deletedDashboardId = dashboardId;
			dashboardId = undefined;
			const remainingDashboards = await client.dashboards.list();
			expect(remainingDashboards.map((dashboard,) => dashboard.id),).not.toContain(
				deletedDashboardId,
			);
		} finally {
			await cleanup.run();
		}
	});

	it("round-trips disposable insights and data quality rules through SDK and CLI", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let insightId: string | undefined;
		let dataQualityRule: { datasetName: string; ruleId: string; } | undefined;

		try {
			const insights = await client.insights.list();
			const sourceInsight = insights[0]?.id ? await client.insights.get(insights[0].id,) : undefined;
			if (!sourceInsight) {
				addFinding({
					id: "insight-mutation-needs-source-insight",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed: "No existing insight was available to clone for disposable insight CRUD validation.",
					cleanupVerified: true,
				},);
			} else {
				const insightName = uniqueTestName("sdk_cli_it_insight",);
				const prototype = JSON.parse(JSON.stringify(sourceInsight,),) as Record<string, unknown>;
				delete prototype.id;
				delete prototype.versionTag;
				delete prototype.creationTag;
				prototype.name = insightName;
				prototype.listed = false;

				const createdInsight = await client.insights.create({ data: prototype, },);
				insightId = createdInsight.id;
				cleanup.defer(async () => {
					if (insightId) await client.insights.delete(insightId,).catch(() => undefined);
				},);
				expect(createdInsight.name,).toBe(insightName,);

				const updateDryRun = parseCliJson(
					await dssRaw(["insight", "update", insightId, "--name", `${insightName}_dry`, "--dry-run",],),
					"insight update dry-run",
				) as { current?: { id?: string; }; next?: { name?: string; }; };
				expect(updateDryRun.current?.id,).toBe(insightId,);
				expect(updateDryRun.next?.name,).toBe(`${insightName}_dry`,);

				const updatedInsight = await client.insights.update(insightId, {
					name: `${insightName}_updated`,
				},);
				expect(updatedInsight.name,).toBe(`${insightName}_updated`,);
				const cliInsight = parseCliJson(
					await dssRaw(["insight", "get", insightId,],),
					"insight get",
				) as {
					name?: string;
				};
				expect(cliInsight.name,).toBe(`${insightName}_updated`,);

				const deleteDryRun = parseCliJson(
					await dssRaw(["insight", "delete", insightId, "--dry-run",],),
					"insight delete dry-run",
				) as Record<string, unknown>;
				expect(deleteDryRun.dryRun,).toBe(true,);
				await client.insights.delete(insightId,);
				const deletedInsightId = insightId;
				insightId = undefined;
				expect((await client.insights.list()).map((insight,) => insight.id),).not.toContain(
					deletedInsightId,
				);
			}

			const datasetName = (await client.datasets.list()).find((dataset,) =>
				typeof dataset.name === "string"
			)?.name;
			if (!datasetName) {
				addFinding({
					id: "data-quality-mutation-needs-dataset",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed: "No dataset was available for disposable data quality rule CRUD validation.",
					cleanupVerified: true,
				},);
				return;
			}

			const ruleName = uniqueTestName("sdk_cli_it_dq_rule",);
			const createdRule = parseCliJson(
				await dssRaw([
					"data-quality",
					"create-rule",
					datasetName,
					"--data",
					JSON.stringify({
						type: "RecordCountInRangeRule",
						softMinimum: 0,
						softMinimumEnabled: true,
						displayName: ruleName,
					},),
				],),
				"data-quality create-rule",
			) as { id?: string; displayName?: string; };
			expect(createdRule.id, "created data quality rule id",).toBeTruthy();
			dataQualityRule = { datasetName, ruleId: createdRule.id!, };
			cleanup.defer(async () => {
				if (dataQualityRule) {
					await client.dataQuality.deleteRule(dataQualityRule.datasetName, dataQualityRule.ruleId,)
						.catch(() => undefined);
				}
			},);
			expect(createdRule.displayName,).toBe(ruleName,);

			expect(await client.dataQuality.statusByPartition(datasetName,),).toBeDefined();
			expect(Array.isArray(await client.dataQuality.lastResults(datasetName,),),).toBe(true,);
			expect(Array.isArray(await client.dataQuality.history(datasetName,),),).toBe(true,);
			const computeDryRun = parseCliJson(
				await dssRaw([
					"data-quality",
					"compute",
					datasetName,
					"--rule-id",
					dataQualityRule.ruleId,
					"--dry-run",
				],),
				"data-quality compute dry-run",
			) as Record<string, unknown>;
			expect(computeDryRun.dryRun,).toBe(true,);

			const computeWait = parseCliJson(
				await dssRaw([
					"data-quality",
					"compute",
					datasetName,
					"--rule-id",
					dataQualityRule.ruleId,
					"--wait",
					"--timeout",
					"120000",
					"--poll-interval",
					"1000",
				],),
				"data-quality compute wait",
			) as { success?: boolean; result?: unknown; };
			expect(computeWait.success,).toBe(true,);
			expect(computeWait.result,).toBeDefined();

			const datasetStatus = parseCliJson(
				await dssRaw(["data-quality", "status", datasetName,],),
				"data-quality status",
			);
			expect(datasetStatus,).toBeDefined();
			const dryRun = parseCliJson(
				await dssRaw([
					"data-quality",
					"update-rule",
					datasetName,
					dataQualityRule.ruleId,
					"--data",
					JSON.stringify({ displayName: `${ruleName}_dry`, },),
					"--dry-run",
				],),
				"data-quality update-rule dry-run",
			) as { current?: { displayName?: string; }; next?: { displayName?: string; }; };
			expect(dryRun.current?.displayName,).toBe(ruleName,);
			expect(dryRun.next?.displayName,).toBe(`${ruleName}_dry`,);

			const updatedRule = await client.dataQuality.updateRule(datasetName, dataQualityRule.ruleId, {
				data: { displayName: `${ruleName}_updated`, },
			},);
			expect(updatedRule.displayName,).toBe(`${ruleName}_updated`,);
			const cliRule = parseCliJson(
				await dssRaw(["data-quality", "get-rule", datasetName, dataQualityRule.ruleId,],),
				"data-quality get-rule",
			) as { displayName?: string; };
			expect(cliRule.displayName,).toBe(`${ruleName}_updated`,);

			const deleteDryRun = parseCliJson(
				await dssRaw([
					"data-quality",
					"delete-rule",
					datasetName,
					dataQualityRule.ruleId,
					"--dry-run",
				],),
				"data-quality delete-rule dry-run",
			) as Record<string, unknown>;
			expect(deleteDryRun.dryRun,).toBe(true,);
			await client.dataQuality.deleteRule(datasetName, dataQualityRule.ruleId,);
			const deletedRuleId = dataQualityRule.ruleId;
			dataQualityRule = undefined;
			const remainingRules = await client.dataQuality.listRules(datasetName,);
			expect(remainingRules.map((rule,) => rule.id),).not.toContain(deletedRuleId,);
		} finally {
			await cleanup.run();
		}
	});

	it("guards variable round-trip behind explicit variable mutation gate", async () => {
		if (process.env.RUN_DATAIKU_INTEGRATION_VARIABLES !== "1") {
			addFinding({
				id: "variables-roundtrip-extra-gated",
				category: "automation-risk",
				status: "skipped",
				severity: "medium",
				observed:
					"Variable deletion requires whole-variable replacement; test is gated by RUN_DATAIKU_INTEGRATION_VARIABLES=1 to avoid clobbering concurrent edits.",
				expected: "A variable unset/patch endpoint would allow safer cleanup.",
				suggestedAction: "Add `variable unset KEY` or SDK patch/delete support if DSS supports it.",
			},);
			return;
		}

		const client = createClient();
		const key = uniqueTestName("sdk_cli_it_var",);
		const before = await client.variables.get();
		try {
			await client.variables.set({ local: { [key]: "ok", }, },);
			const afterSet = await client.variables.get();
			expect(afterSet.local[key],).toBe("ok",);
		} finally {
			await client.variables.set({ standard: before.standard, local: before.local, replace: true, },);
		}
		const restored = await client.variables.get();
		expect(restored,).toEqual(before,);
	});

	it("guards managed-folder file workflow behind explicit folder id", async () => {
		const folderId = process.env.DATAIKU_TEST_FOLDER_ID;
		if (!folderId) {
			addFinding({
				id: "folder-file-workflow-needs-test-folder",
				category: "feature-opportunity",
				status: "skipped",
				severity: "medium",
				observed:
					"No DATAIKU_TEST_FOLDER_ID was configured for safe upload/download/delete-file testing.",
				expected:
					"A `folder create` + `folder delete` lifecycle would let tests create disposable managed folders without preconfigured fixtures.",
				suggestedAction:
					"Add managed-folder delete support or configure DATAIKU_TEST_FOLDER_ID for file lifecycle tests.",
			},);
			return;
		}

		const client = createClient();
		const tempDir = await mkdtemp(join(tmpdir(), "dss-rigorous-",),);
		const remotePath = `/${uniqueTestName("sdk_cli_it_file",)}.txt`;
		const localPath = join(tempDir, "upload.txt",);
		const downloadPath = join(tempDir, "download.txt",);
		await writeFile(localPath, "hello rigorous integration\n", "utf-8",);

		try {
			await client.folders.upload(folderId, remotePath, localPath,);
			const contents = await client.folders.contents(folderId,);
			expect(contents.some((item,) => item.path === remotePath || item.path === remotePath.slice(1,)),)
				.toBe(true,);
			await client.folders.download(folderId, remotePath, { localPath: downloadPath, },);
			expect(await readFile(downloadPath, "utf-8",),).toBe("hello rigorous integration\n",);
		} finally {
			await client.folders.deleteFile(folderId, remotePath,).catch(() => undefined);
			await rm(tempDir, { recursive: true, force: true, },);
		}
	});
},);
