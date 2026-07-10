import { afterAll, expect, it, } from "bun:test";
import { mkdtemp, readFile, rm, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import type { DataikuClient, } from "../src/client.js";
import type { JupyterNotebookContent, SqlNotebookContent, } from "../src/schemas.js";
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
	registryExpectationsForReadOnlyCase,
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

type RigorousFixtures = {
	projectKey?: string;
	fixtures?: Record<string, string | null>;
	safeDataset?: Record<string, unknown> | null;
	safeManagedFolder?: Record<string, unknown> | null;
	safeJupyterNotebook?: Record<string, unknown> | null;
	unsafe?: Record<string, unknown>;
};

let rigorousFixturesPromise: Promise<RigorousFixtures> | undefined;

function fixtureString(value: unknown, fields: string[],): string | undefined {
	if (value === null || typeof value !== "object" || Array.isArray(value,)) return undefined;
	const record = value as Record<string, unknown>;
	for (const field of fields) {
		const candidate = record[field];
		if (typeof candidate === "string" && candidate.length > 0) return candidate;
	}
	return undefined;
}

async function rigorousFixtures(): Promise<RigorousFixtures> {
	rigorousFixturesPromise ??= (async () => {
		const result = await dssRaw([
			"fixtures",
			"--json",
			"--project-key",
			process.env.DATAIKU_PROJECT_KEY!,
		],);
		return parseCliJson(result, "fixtures",) as RigorousFixtures;
	})();
	return rigorousFixturesPromise;
}

type DisposableSqlConnectionConfig = {
	type: "PostgreSQL";
	params: {
		host: string;
		port: string;
		db: string;
		user: string;
		password: string;
	};
};

function disposableSqlConnectionConfig(): DisposableSqlConnectionConfig | undefined {
	const host = process.env.DATAIKU_SQL_LIVE_HOST?.trim();
	const db = process.env.DATAIKU_SQL_LIVE_DATABASE?.trim();
	const user = process.env.DATAIKU_SQL_LIVE_USER?.trim();
	const password = process.env.DATAIKU_SQL_LIVE_PASSWORD;
	if (!host || !db || !user || password === undefined) return undefined;
	return {
		type: "PostgreSQL",
		params: {
			host,
			port: process.env.DATAIKU_SQL_LIVE_PORT?.trim() || "5432",
			db,
			user,
			password,
		},
	};
}

function disposableJupyterNotebookContent(label: string,): JupyterNotebookContent {
	return {
		metadata: {
			kernelspec: {
				name: "python3",
				display_name: "Python 3",
				language: "python",
			},
			language_info: { name: "python", },
		},
		nbformat: 4,
		nbformat_minor: 5,
		cells: [{
			cell_type: "code",
			source: [`print(${JSON.stringify(label,)})`,],
			metadata: {},
			outputs: [{
				output_type: "stream",
				name: "stdout",
				text: [`${label}\n`,],
			},],
			execution_count: 1,
		},],
	};
}

async function createDisposableJupyterNotebook(
	client: DataikuClient,
	name: string,
): Promise<void> {
	await client.post(
		`/public/api/projects/${
			encodeURIComponent(process.env.DATAIKU_PROJECT_KEY!,)
		}/jupyter-notebooks/${encodeURIComponent(name,)}`,
		disposableJupyterNotebookContent(name,),
	);
}

async function createDisposableSqlNotebook(
	client: DataikuClient,
	connection: string,
): Promise<string> {
	const created = await client.post<{ id?: string; }>(
		`/public/api/projects/${encodeURIComponent(process.env.DATAIKU_PROJECT_KEY!,)}/sql-notebooks/`,
		{
			projectKey: process.env.DATAIKU_PROJECT_KEY!,
			connection,
			cells: [{
				id: uniqueTestName("sdk_cli_it_sql_cell",),
				type: "QUERY",
				name: "Disposable probe",
				code: "SELECT 1",
			},],
		},
	);
	if (!created?.id) throw new Error("DSS did not return an id for disposable SQL notebook.",);
	return created.id;
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
		case "analysis-list":
			return client.analyses.list();
		case "saved-model-list":
			return client.savedModels.list();
		case "model-evaluation-store-list":
			return client.modelEvaluationStores.list();
	}
}

describeIntegration("Rigorous integration: CLI discoverability and local validation", () => {
	it("prints a registry for every command without resolving DSS resources", async () => {
		const registryResult = await dssRaw(["commands", "run",],);
		const registry = parseCliJson(registryResult, "commands run",) as Record<
			string,
			Record<string, { flags?: Array<{ name: string; }>; }>
		>;

		for (const [resource, actions,] of Object.entries(registry,)) {
			for (const [action, meta,] of Object.entries(actions,)) {
				const flagNames = meta.flags?.map((flag,) => flag.name) ?? [];
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("help",);
				expect(flagNames, `${resource} ${action} flags`,).not.toContain("report-json",);
			}
		}
	}, 60_000,);

	it("aligns read-only matrix expectations with registry metadata", async () => {
		const registryResult = await dssRaw(["commands", "run",],);
		const registry = parseCliJson(registryResult, "commands",) as Record<
			string,
			Record<string, Record<string, unknown>>
		>;

		for (const entry of readOnlyCommandCases) {
			const meta = registry[entry.resource]?.[entry.action];
			const expected = registryExpectationsForReadOnlyCase(entry,);
			expect(meta, `${entry.id} registry entry`,).toBeDefined();
			expect(meta?.outputShape, `${entry.id} outputShape`,).toBe(expected.outputShape,);
			expect(meta?.sideEffect, `${entry.id} sideEffect`,).toBe(expected.sideEffect,);
			expect(meta?.destructive, `${entry.id} destructive`,).toBe(expected.destructive,);
			expect(meta?.mutatesDss, `${entry.id} mutatesDss`,).toBe(expected.mutatesDss,);
			expect(meta?.async, `${entry.id} async`,).toBe(expected.async,);
			expect(meta?.dryRun, `${entry.id} dryRun`,).toBe(expected.dryRun,);
			expect(meta?.exitCodes, `${entry.id} exitCodes`,).toEqual(expected.exitCodes,);
			expect(Array.isArray(meta?.requiredFlags,), `${entry.id} requiredFlags`,).toBe(true,);
			expect(Array.isArray(meta?.optionalFlags,), `${entry.id} optionalFlags`,).toBe(true,);
		}
	});

	for (const scenario of localValidationCases) {
		it(`locally rejects ${scenario.id}`, async () => {
			const result = await dssRaw(scenario.args,);
			expect(result.code, `${scenario.id} exit code`,).toBe(scenario.expectedCode,);
			expect(`${result.stdout}${result.stderr}`, scenario.id,).toContain(scenario.expectedMessage,);
		});
	}
},);

describeProjectIntegration("Rigorous integration: read-only SDK/CLI parity", () => {
	it("discovers fixture candidates for live smoke tests", async () => {
		const fixtures = await rigorousFixtures();
		expect(fixtures.projectKey, "fixtures projectKey",).toBe(process.env.DATAIKU_PROJECT_KEY,);
		expect(fixtures.fixtures, "fixtures block",).toBeDefined();
		if (!fixtures.safeDataset) {
			addFinding({
				id: "fixtures-no-safe-dataset",
				category: "automation-risk",
				status: "skipped",
				severity: "medium",
				command: "fixtures --json",
				observed: "dss fixtures did not discover a safeDataset candidate.",
				expected:
					"At least one Filesystem or Inline dataset is available for safe read-only live smoke tests.",
				suggestedAction:
					"Create a small Filesystem or Inline dataset fixture, or pass --allow-types for the playground.",
			},);
		} else {
			expect(fixtureString(fixtures.safeDataset, ["name",],), "safeDataset name",).toBeDefined();
		}
		if (!fixtures.safeJupyterNotebook) {
			addFinding({
				id: "fixtures-no-safe-jupyter",
				category: "automation-risk",
				status: "skipped",
				severity: "low",
				command: "fixtures --json",
				observed: "dss fixtures did not discover a non-underscore Jupyter notebook candidate.",
				expected: "A non-system Jupyter notebook is available when notebook smoke tests need one.",
			},);
		}
	});

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
		expect(payload.code,).toBe("transient",);
		expect(payload.retryable,).toBe(true,);
		expect(String(payload.error ?? "",),).toContain("Retry attempts:",);
	}, 30_000,);
},);

describeIntegration("Rigorous integration: gated unproven SDK coverage", () => {
	it("runs SQL query methods when a SQL live fixture is configured", async () => {
		let sqlConnection = process.env.DATAIKU_SQL_CONNECTION;
		const sqlDatasetFullName = process.env.DATAIKU_SQL_DATASET_FULL_NAME;
		const cleanup = createCleanupStack();

		if (process.env.RUN_DATAIKU_SQL_LIVE !== "1") {
			addFinding({
				id: "sql-live-fixture-not-configured",
				category: "resource-gap",
				status: "skipped",
				severity: "medium",
				observed: "SQL live coverage requires RUN_DATAIKU_SQL_LIVE=1.",
				expected: "A SQL-compatible read-only fixture is configured for SELECT 1 live coverage.",
				cleanupVerified: true,
			},);
			return;
		}

		const client = createClient();
		if (!sqlConnection && !sqlDatasetFullName) {
			const disposableConnection = disposableSqlConnectionConfig();
			if (
				process.env.RUN_DATAIKU_ADMIN_MUTATING !== "1"
				|| process.env.DATAIKU_SQL_LIVE_CREATE_CONNECTION !== "1"
				|| !disposableConnection
			) {
				addFinding({
					id: "sql-live-fixture-not-configured",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed:
						"SQL live coverage needs DATAIKU_SQL_CONNECTION/DATAIKU_SQL_DATASET_FULL_NAME or RUN_DATAIKU_ADMIN_MUTATING=1 plus DATAIKU_SQL_LIVE_CREATE_CONNECTION=1 and disposable PostgreSQL connection env.",
					expected: "A SQL-compatible read-only fixture is configured for SELECT 1 live coverage.",
					cleanupVerified: true,
				},);
				return;
			}

			const connectionName = uniqueTestName("OMP_SQL_CONN",);
			await client.post("/public/api/admin/connections", {
				name: connectionName,
				description: "OMP disposable SQL live integration probe",
				type: disposableConnection.type,
				params: disposableConnection.params,
				usableBy: "ALL",
				allowedGroups: [],
			},);
			sqlConnection = connectionName;
			cleanup.defer(async () => {
				await client.del(`/public/api/admin/connections/${encodeURIComponent(connectionName,)}`,);
			},);
		}

		try {
			const result = await client.sql.query({
				query: "SELECT 1",
				...(sqlConnection ? { connection: sqlConnection, } : { datasetFullName: sqlDatasetFullName, }),
				projectKey: process.env.DATAIKU_PROJECT_KEY,
			},);

			expect(result.queryId, "SQL query id",).toBeTruthy();
			expect(Array.isArray(result.rows,),).toBe(true,);
			expect(result.rows.length, "SQL query rows",).toBeGreaterThan(0,);
			expect(result.schema,).toBeDefined();

			const cliResult = parseCliJson(
				await dssRaw([
					"sql",
					"query",
					"SELECT 1",
					...(sqlConnection
						? ["--connection", sqlConnection,]
						: ["--dataset", sqlDatasetFullName!,]),
				],),
				"sql query",
			) as { queryId?: string; rows?: unknown[][]; schema?: unknown; };
			expect(cliResult.queryId, "CLI SQL query id",).toBeTruthy();
			expect(Array.isArray(cliResult.rows,),).toBe(true,);
			expect(cliResult.rows?.length, "CLI SQL query rows",).toBeGreaterThan(0,);
			expect(cliResult.schema,).toBeDefined();
		} finally {
			await cleanup.run();
		}
	}, 120_000,);

	it("uses disposable notebook fixtures for no-loss mutation coverage", async () => {
		if (process.env.RUN_DATAIKU_INTEGRATION_MUTATING !== "1") {
			addFinding({
				id: "notebook-mutation-needs-mutating-gate",
				category: "automation-risk",
				status: "skipped",
				severity: "medium",
				observed:
					"Jupyter and SQL notebook mutation coverage is gated by RUN_DATAIKU_INTEGRATION_MUTATING=1.",
				expected: "Notebook mutation tests only run when mutating integration is explicitly enabled.",
				cleanupVerified: true,
			},);
			return;
		}

		const client = createClient();
		const cleanup = createCleanupStack();
		const jupyterToDelete = new Set<string>();
		const sqlToDelete = new Set<string>();
		cleanup.defer(async () => {
			for (const name of jupyterToDelete) {
				await client.notebooks.deleteJupyter(name,);
			}
			for (const id of sqlToDelete) {
				await client.notebooks.deleteSql(id,);
			}
		},);

		try {
			const sdkJupyterName = `${uniqueTestName("sdk_cli_it_jupyter_sdk",)}.ipynb`;
			await createDisposableJupyterNotebook(client, sdkJupyterName,);
			jupyterToDelete.add(sdkJupyterName,);

			const sdkOriginal = await client.notebooks.getJupyter(sdkJupyterName,);
			const sdkSaved: JupyterNotebookContent = {
				...sdkOriginal,
				cells: [
					...sdkOriginal.cells,
					{
						cell_type: "markdown",
						source: ["SDK mutation proof",],
						metadata: {},
					},
				],
			};
			await client.notebooks.saveJupyter(sdkJupyterName, sdkSaved,);
			expect((await client.notebooks.getJupyter(sdkJupyterName,)).cells.length,).toBe(
				sdkSaved.cells.length,
			);

			await client.notebooks.clearJupyterOutputs(sdkJupyterName,);
			const sdkCleared = await client.notebooks.getJupyter(sdkJupyterName,);
			expect(
				sdkCleared.cells.every((cell,) => {
					const outputs = (cell as { outputs?: unknown[]; }).outputs;
					return outputs === undefined || outputs.length === 0;
				},),
			).toBe(true,);
			expect(Array.isArray(await client.notebooks.listJupyterSessions(sdkJupyterName,),),).toBe(true,);
			await client.notebooks.deleteJupyter(sdkJupyterName,);
			jupyterToDelete.delete(sdkJupyterName,);

			const cliJupyterName = `${uniqueTestName("sdk_cli_it_jupyter_cli",)}.ipynb`;
			await createDisposableJupyterNotebook(client, cliJupyterName,);
			jupyterToDelete.add(cliJupyterName,);
			const cliJupyterContent = disposableJupyterNotebookContent(`${cliJupyterName}_updated`,);
			const saveJupyter = parseCliJson(
				await dssRaw([
					"notebook",
					"save-jupyter",
					cliJupyterName,
					"--data",
					JSON.stringify(cliJupyterContent,),
				],),
				"notebook save-jupyter",
			) as { saved?: string; };
			expect(saveJupyter.saved,).toBe(cliJupyterName,);
			const clearJupyter = parseCliJson(
				await dssRaw(["notebook", "clear-jupyter-outputs", cliJupyterName,],),
				"notebook clear-jupyter-outputs",
			) as { cleared?: string; };
			expect(clearJupyter.cleared,).toBe(cliJupyterName,);
			const sessionsJupyter = parseCliJson(
				await dssRaw(["notebook", "sessions-jupyter", cliJupyterName,],),
				"notebook sessions-jupyter",
			);
			expect(Array.isArray(sessionsJupyter,),).toBe(true,);
			const unloadDryRun = parseCliJson(
				await dssRaw(["notebook", "unload-jupyter", cliJupyterName, "missing-session", "--dry-run",],),
				"notebook unload-jupyter dry-run",
			) as Record<string, unknown>;
			expect(unloadDryRun.dryRun,).toBe(true,);
			const deleteJupyterDryRun = parseCliJson(
				await dssRaw(["notebook", "delete-jupyter", cliJupyterName, "--dry-run",],),
				"notebook delete-jupyter dry-run",
			) as Record<string, unknown>;
			expect(deleteJupyterDryRun.dryRun,).toBe(true,);
			const deleteJupyter = parseCliJson(
				await dssRaw(["notebook", "delete-jupyter", cliJupyterName,],),
				"notebook delete-jupyter",
			) as { deleted?: string; };
			expect(deleteJupyter.deleted,).toBe(cliJupyterName,);
			jupyterToDelete.delete(cliJupyterName,);

			addFinding({
				id: "jupyter-unload-needs-running-session",
				category: "resource-gap",
				status: "skipped",
				severity: "medium",
				observed:
					"Disposable Jupyter notebooks can be created, saved, cleared, session-listed, and deleted, but no running session is created by the public notebook API.",
				expected: "unloadJupyter is live-tested only against a disposable running notebook session.",
				cleanupVerified: true,
			},);

			const sqlNotebookConnection = process.env.DATAIKU_TEST_SQL_NOTEBOOK_CONNECTION
				?? process.env.DATAIKU_SQL_CONNECTION
				?? "filesystem_managed";
			const sdkSqlId = await createDisposableSqlNotebook(client, sqlNotebookConnection,);
			sqlToDelete.add(sdkSqlId,);
			const sdkSqlOriginal = await client.notebooks.getSql(sdkSqlId,);
			const sdkSqlSaved: SqlNotebookContent = {
				...sdkSqlOriginal,
				cells: [
					...sdkSqlOriginal.cells,
					{
						id: uniqueTestName("sdk_cli_it_sql_cell",),
						type: "QUERY",
						name: "SDK mutation proof",
						code: "SELECT 1",
					},
				],
			};
			await client.notebooks.saveSql(sdkSqlId, sdkSqlSaved,);
			expect((await client.notebooks.getSql(sdkSqlId,)).cells.length,).toBe(
				sdkSqlSaved.cells.length,
			);
			expect(await client.notebooks.getSqlHistory(sdkSqlId,),).toBeDefined();
			await client.notebooks.clearSqlHistory(sdkSqlId, { numRunsToRetain: 9_999, },);
			await client.notebooks.deleteSql(sdkSqlId,);
			sqlToDelete.delete(sdkSqlId,);

			const cliSqlId = await createDisposableSqlNotebook(client, sqlNotebookConnection,);
			sqlToDelete.add(cliSqlId,);
			const cliSqlContent = await client.notebooks.getSql(cliSqlId,);
			const saveSql = parseCliJson(
				await dssRaw([
					"notebook",
					"save-sql",
					cliSqlId,
					"--data",
					JSON.stringify(cliSqlContent,),
				],),
				"notebook save-sql",
			) as { saved?: string; };
			expect(saveSql.saved,).toBe(cliSqlId,);
			expect(
				typeof parseCliJson(
					await dssRaw(["notebook", "history-sql", cliSqlId,],),
					"notebook history-sql",
				),
			).toBe("object",);
			const clearSql = parseCliJson(
				await dssRaw(["notebook", "clear-sql-history", cliSqlId, "--retain", "9999",],),
				"notebook clear-sql-history",
			) as { cleared?: string; };
			expect(clearSql.cleared,).toBe(cliSqlId,);
			const deleteSqlDryRun = parseCliJson(
				await dssRaw(["notebook", "delete-sql", cliSqlId, "--dry-run",],),
				"notebook delete-sql dry-run",
			) as Record<string, unknown>;
			expect(deleteSqlDryRun.dryRun,).toBe(true,);
			const deleteSql = parseCliJson(
				await dssRaw(["notebook", "delete-sql", cliSqlId,],),
				"notebook delete-sql",
			) as { deleted?: string; };
			expect(deleteSql.deleted,).toBe(cliSqlId,);
			sqlToDelete.delete(cliSqlId,);
		} finally {
			await cleanup.run();
		}
	}, 180_000,);

	it("guards admin code-env lifecycle behind an explicit admin gate", async () => {
		if (process.env.RUN_DATAIKU_ADMIN_MUTATING !== "1") {
			addFinding({
				id: "code-env-mutation-admin-gated",
				category: "automation-risk",
				status: "skipped",
				severity: "medium",
				observed:
					"Code-env mutators are global/admin operations and require RUN_DATAIKU_ADMIN_MUTATING=1.",
				expected:
					"A disposable code environment is created only under an explicit admin mutation gate.",
				cleanupVerified: true,
			},);
			return;
		}

		const client = createClient();
		const envLang = "PYTHON";
		const envName = uniqueTestName(process.env.OMP_CODE_ENV_PREFIX ?? "OMP_CODE_ENV",);
		let created = false;

		try {
			const createDryRun = parseCliJson(
				await dssRaw([
					"code-env",
					"create",
					envLang,
					envName,
					"--deployment-mode",
					"DESIGN_MANAGED",
					"--python-interpreter",
					"PYTHON311",
					"--dry-run",
				],),
				"code-env create dry-run",
			) as Record<string, unknown>;
			expect(createDryRun.dryRun,).toBe(true,);
			expect(createDryRun.envName,).toBe(envName,);

			await client.codeEnvs.create({
				envLang,
				envName,
				deploymentMode: "DESIGN_MANAGED",
				params: { pythonInterpreter: "PYTHON311", },
				wait: true,
			},);
			created = true;

			const details = await client.codeEnvs.get(envLang, envName,);
			expect(details.envName,).toBe(envName,);

			const definition = await client.codeEnvs.getDefinition(envLang, envName,);
			expect(definition,).toBeDefined();
			await client.codeEnvs.setDefinition(envLang, envName, definition,);
			await client.codeEnvs.setPackages(envLang, envName, [],);
			await client.codeEnvs.setJupyterSupport(envLang, envName, false, { wait: true, },);
			expect(Array.isArray(await client.codeEnvs.listUsages(envLang, envName,),),).toBe(true,);

			const updatePackages = await client.codeEnvs.updatePackages(envLang, envName, {
				forceRebuildEnv: false,
				wait: false,
			},);
			expect(updatePackages,).toBeDefined();

			await client.codeEnvs.delete(envLang, envName, { wait: true, },);
			created = false;
		} finally {
			if (created) {
				await client.codeEnvs.delete(envLang, envName, { wait: true, },).catch(() => undefined);
			}
		}
	}, 300_000,);
},);

describeMutatingProjectIntegration("Rigorous integration: safe mutating workflows", () => {
	it("round-trips temporary flow zones and moves only disposable flow objects", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let zoneId: string | undefined;
		let secondZoneId: string | undefined;
		let datasetName: string | undefined;

		try {
			const zoneName = uniqueTestName("sdk_cli_it_zone",);
			const created = await client.flowZones.create({ name: zoneName, color: "#2ab1ac", },);
			zoneId = created.id;
			cleanup.defer(async () => {
				if (zoneId) await client.flowZones.delete(zoneId,).catch(() => undefined);
			},);
			expect(created.name,).toBe(zoneName,);

			const dryRun = parseCliJson(
				await dssRaw(["flow-zone", "delete", zoneId!, "--dryrun",],),
				"flow-zone dryrun",
			) as Record<string, unknown>;
			expect(dryRun.dryRun,).toBe(true,);

			datasetName = uniqueTestName("sdk_cli_it_zone_dataset",);
			try {
				await client.datasets.create({
					datasetName,
					connection: "filesystem_managed",
					dsType: "Filesystem",
				},);
				cleanup.defer(async () => {
					if (datasetName) await client.datasets.delete(datasetName,);
				},);
			} catch (error) {
				addFinding({
					id: "flow-zone-move-needs-disposable-dataset-create",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed: `Could not create disposable dataset for flow-zone movement: ${
						error instanceof Error ? error.message : String(error,)
					}`,
					expected:
						"Disposable filesystem dataset creation is available so flow-zone move tests never touch existing project objects.",
					cleanupVerified: true,
				},);
				datasetName = undefined;
			}

			if (datasetName) {
				const movedBySdk = await client.flowZones.moveItem(zoneId!, {
					objectType: "DATASET",
					objectId: datasetName,
				},);
				expect(
					movedBySdk.items?.some((item,) =>
						item.objectType === "DATASET" && item.objectId === datasetName
					),
				)
					.toBe(true,);

				const secondZoneName = uniqueTestName("sdk_cli_it_zone_second",);
				const secondZone = await client.flowZones.create({
					name: secondZoneName,
					color: "#cc0000",
				},);
				secondZoneId = secondZone.id;
				cleanup.defer(async () => {
					if (secondZoneId) await client.flowZones.delete(secondZoneId,).catch(() => undefined);
				},);

				const moveDryRun = parseCliJson(
					await dssRaw(["flow-zone", "move", secondZoneId!, "--dataset", datasetName, "--dry-run",],),
					"flow-zone move dry-run",
				) as { dryRun?: boolean; items?: Array<{ objectId?: string; objectType?: string; }>; };
				expect(moveDryRun.dryRun,).toBe(true,);
				expect(moveDryRun.items,).toContainEqual({
					objectType: "DATASET",
					objectId: datasetName,
				},);

				const movedByCli = parseCliJson(
					await dssRaw(["flow-zone", "move", secondZoneId!, "--dataset", datasetName,],),
					"flow-zone move",
				) as { items?: Array<{ objectId?: string; objectType?: string; }>; };
				expect(
					movedByCli.items?.some((item,) =>
						item.objectType === "DATASET" && item.objectId === datasetName
					),
				)
					.toBe(true,);
			}

			if (secondZoneId) {
				await client.flowZones.delete(secondZoneId,);
				secondZoneId = undefined;
			}
			await client.flowZones.delete(zoneId!,);
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
	}, 120_000,);

	it("round-trips disposable insights and data quality rules through SDK and CLI", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let insightId: string | undefined;
		let dataQualityRule: { datasetName: string; ruleId: string; } | undefined;
		let dataQualityDatasetName: string | undefined;

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

			const datasetName = uniqueTestName("sdk_cli_it_dq_dataset",);
			try {
				await client.datasets.create({
					datasetName,
					connection: "filesystem_managed",
					dsType: "Filesystem",
				},);
				dataQualityDatasetName = datasetName;
				cleanup.defer(async () => {
					if (dataQualityDatasetName) await client.datasets.delete(dataQualityDatasetName,);
				},);
			} catch (error) {
				addFinding({
					id: "data-quality-mutation-needs-disposable-dataset-create",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed: `Could not create disposable dataset for data-quality rule CRUD validation: ${
						error instanceof Error ? error.message : String(error,)
					}`,
					expected:
						"Disposable filesystem dataset creation is available so data-quality rule coverage never mutates existing datasets.",
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
	}, 180_000,);

	it("builds disposable dataset jobs through SDK and CLI planning", async () => {
		const client = createClient();
		const cleanup = createCleanupStack();
		let datasetName: string | undefined;

		try {
			datasetName = uniqueTestName("sdk_cli_it_job_dataset",);
			try {
				await client.datasets.create({
					datasetName,
					connection: "filesystem_managed",
					dsType: "Filesystem",
				},);
				cleanup.defer(async () => {
					if (datasetName) await client.datasets.delete(datasetName,);
				},);
			} catch (error) {
				addFinding({
					id: "job-build-needs-disposable-dataset-create",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed: `Could not create disposable dataset for job build coverage: ${
						error instanceof Error ? error.message : String(error,)
					}`,
					expected:
						"Disposable filesystem dataset creation is available so job build coverage never targets existing outputs.",
					cleanupVerified: true,
				},);
				return;
			}

			const buildDryRun = parseCliJson(
				await dssRaw(["job", "build", datasetName!, "--dry-run",],),
				"job build dry-run",
			) as Record<string, unknown>;
			expect(buildDryRun.dryRun,).toBe(true,);
			expect(buildDryRun.target,).toBe(datasetName,);
			expect(buildDryRun.method,).toBe("POST",);

			const build = await client.jobs.build(datasetName!, {
				buildMode: "NON_RECURSIVE_FORCED_BUILD",
			},);
			expect(build.jobId, "build job id",).toBeTruthy();

			const wait = await client.jobs.wait(build.jobId, {
				includeLogs: true,
				maxLogLines: 20,
				pollIntervalMs: 1_000,
				timeoutMs: 120_000,
			},);
			expect(wait.success, `job ${build.jobId} state ${wait.state}`,).toBe(true,);
			expect(wait.jobId,).toBe(build.jobId,);
			expect(typeof wait.log,).toBe("string",);

			const details = await client.jobs.get(build.jobId,);
			expect(details,).toBeDefined();
			expect(typeof await client.jobs.log(build.jobId, { maxLogLines: 20, },),).toBe("string",);

			const cliWait = parseCliJson(
				await dssRaw([
					"job",
					"wait",
					build.jobId,
					"--timeout",
					"120000",
					"--poll-interval",
					"1000",
				],),
				"job wait",
			) as { success?: boolean; jobId?: string; };
			expect(cliWait.success,).toBe(true,);
			expect(cliWait.jobId,).toBe(build.jobId,);

			const buildAndWaitDryRun = parseCliJson(
				await dssRaw(["job", "build-and-wait", datasetName!, "--dry-run",],),
				"job build-and-wait dry-run",
			) as Record<string, unknown>;
			expect(buildAndWaitDryRun.dryRun,).toBe(true,);
			expect(buildAndWaitDryRun.target,).toBe(datasetName,);

			const buildAndWait = await client.jobs.buildAndWait(datasetName!, {
				buildMode: "NON_RECURSIVE_FORCED_BUILD",
				includeLogs: true,
				maxLogLines: 20,
				pollIntervalMs: 1_000,
				timeoutMs: 120_000,
			},);
			expect(buildAndWait.success, `job ${buildAndWait.jobId} state ${buildAndWait.state}`,).toBe(
				true,
			);

			const abortDryRun = parseCliJson(
				await dssRaw(["job", "abort", buildAndWait.jobId, "--dry-run",],),
				"job abort dry-run",
			) as Record<string, unknown>;
			expect(abortDryRun.dryRun,).toBe(true,);
			expect(abortDryRun.id,).toBe(buildAndWait.jobId,);

			const abortFixtures = await rigorousFixtures();
			const abortInputDataset = fixtureString(abortFixtures.safeDataset, ["name", "id",],)
				?? datasetName!;
			const abortRecipeName = uniqueTestName("sdk_cli_it_abort_recipe",);
			const abortOutputDataset = uniqueTestName("sdk_cli_it_abort_output",);
			const abortPayload = [
				"import time",
				"time.sleep(120)",
				"import dataiku",
				"import pandas as pd",
				`out = dataiku.Dataset(${JSON.stringify(abortOutputDataset,)})`,
				'out.write_with_schema(pd.DataFrame({"x": [1]}))',
				"",
			].join("\n",);
			const abortRecipe = await client.recipes.create({
				type: "python",
				name: abortRecipeName,
				inputDatasets: [abortInputDataset,],
				outputDataset: abortOutputDataset,
				outputConnection: "filesystem_managed",
				payload: abortPayload,
			},);
			expect(abortRecipe.recipeName,).toBe(abortRecipeName,);
			cleanup.defer(async () => {
				await client.datasets.delete(abortOutputDataset,);
			},);
			cleanup.defer(async () => {
				await client.recipes.delete(abortRecipeName,);
			},);

			const terminalAbortStates = ["ABORTED", "KILLED", "CANCELED", "CANCELLED",];
			const terminalStates = new Set([
				"DONE",
				"FAILED",
				"ABORTED",
				"KILLED",
				"CANCELED",
				"CANCELLED",
				"ERROR",
			],);
			const addJobAbortFixtureFinding = (observed: string,): void => {
				addFinding({
					id: "job-abort-needs-long-running-fixture",
					category: "resource-gap",
					status: "skipped",
					severity: "medium",
					observed,
					expected:
						"A disposable recipe build remains running long enough for SDK and CLI job abort calls to be issued.",
					cleanupVerified: true,
				},);
			};
			const startAbortableJob = async (label: string,): Promise<string | undefined> => {
				let started: { jobId: string; };
				try {
					started = await client.jobs.build(abortOutputDataset, {
						buildMode: "NON_RECURSIVE_FORCED_BUILD",
					},);
				} catch (error) {
					addJobAbortFixtureFinding(
						`${label} abort fixture build could not start: ${
							error instanceof Error ? error.message : String(error,)
						}`,
					);
					return undefined;
				}
				await new Promise((resolve,) => setTimeout(resolve, 5_000,));
				const jobDetails = await client.jobs.get(started.jobId,);
				const state = ((jobDetails.baseStatus as { state?: string; } | undefined)?.state ?? "")
					.toUpperCase();
				if (terminalStates.has(state,)) {
					addJobAbortFixtureFinding(
						`${label} abort fixture job ${started.jobId} reached terminal state ${state} before abort could be issued.`,
					);
					return undefined;
				}
				return started.jobId;
			};

			const sdkAbortJobId = await startAbortableJob("SDK",);
			if (!sdkAbortJobId) return;
			await client.jobs.abort(sdkAbortJobId,);
			const sdkAbortWait = await client.jobs.wait(sdkAbortJobId, {
				includeLogs: true,
				maxLogLines: 20,
				pollIntervalMs: 1_000,
				timeoutMs: 120_000,
			},);
			expect(sdkAbortWait.success, `SDK abort job ${sdkAbortJobId} state ${sdkAbortWait.state}`,).toBe(
				false,
			);
			expect(terminalAbortStates,).toContain(sdkAbortWait.state.toUpperCase(),);

			const cliAbortJobId = await startAbortableJob("CLI",);
			if (!cliAbortJobId) return;
			const cliAbort = parseCliJson(
				await dssRaw(["job", "abort", cliAbortJobId,],),
				"job abort",
			) as { aborted?: string; resource?: string; };
			expect(cliAbort.aborted,).toBe(cliAbortJobId,);
			expect(cliAbort.resource,).toBe("job",);
			const cliAbortWait = await client.jobs.wait(cliAbortJobId, {
				pollIntervalMs: 1_000,
				timeoutMs: 120_000,
			},);
			expect(cliAbortWait.success, `CLI abort job ${cliAbortJobId} state ${cliAbortWait.state}`,).toBe(
				false,
			);
			expect(terminalAbortStates,).toContain(cliAbortWait.state.toUpperCase(),);
		} finally {
			await cleanup.run();
		}
	}, 180_000,);

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
		const dryRun = parseCliJson(
			await dssRaw(["variable", "set", "--local", JSON.stringify({ [key]: "ok", },), "--dry-run",],),
			"variable set dry-run",
		) as { dryRun?: boolean; next?: { local?: Record<string, unknown>; }; };
		expect(dryRun.dryRun,).toBe(true,);
		expect(dryRun.next?.local?.[key],).toBe("ok",);
		try {
			await client.variables.set({ local: { [key]: "ok", }, },);
			const afterSet = await client.variables.get();
			expect(afterSet.local[key],).toBe("ok",);
			const cliAfterSet = parseCliJson(
				await dssRaw(["variable", "get",],),
				"variable get after set",
			) as { local?: Record<string, unknown>; };
			expect(cliAfterSet.local?.[key],).toBe("ok",);
		} finally {
			await client.variables.set({ standard: before.standard, local: before.local, replace: true, },);
		}
		const restored = await client.variables.get();
		expect(restored,).toEqual(before,);
	});

	it("round-trips files through a disposable managed folder", async () => {
		const client = createClient();
		const folderName = uniqueTestName("sdk_cli_it_folder",);
		let folderId: string | undefined;
		try {
			const folder = await client.folders.create({
				name: folderName,
				type: "Filesystem",
				connection: "filesystem_managed",
			},);
			folderId = folder.id;
			expect(folderId, "created folder id",).toBeTruthy();
		} catch (error) {
			addFinding({
				id: "folder-file-workflow-needs-disposable-folder-create",
				category: "resource-gap",
				status: "skipped",
				severity: "medium",
				observed: `Could not create disposable managed folder for file workflow validation: ${
					error instanceof Error ? error.message : String(error,)
				}`,
				expected:
					"Disposable filesystem managed-folder creation is available so folder file workflow coverage never touches existing folders.",
				cleanupVerified: true,
			},);
			return;
		}

		const tempDir = await mkdtemp(join(tmpdir(), "dss-rigorous-",),);
		const remotePath = `/${uniqueTestName("sdk_cli_it_file",)}.txt`;
		const localPath = join(tempDir, "upload.txt",);
		const downloadPath = join(tempDir, "download.txt",);
		await writeFile(localPath, "hello rigorous integration\n", "utf-8",);

		try {
			await client.folders.upload(folderId!, remotePath, localPath,);
			const contents = await client.folders.contents(folderId!,);
			expect(contents.some((item,) => item.path === remotePath || item.path === remotePath.slice(1,)),)
				.toBe(true,);
			await client.folders.download(folderId!, remotePath, { localPath: downloadPath, },);
			expect(await readFile(downloadPath, "utf-8",),).toBe("hello rigorous integration\n",);
		} finally {
			if (folderId) {
				await client.folders.deleteFile(folderId, remotePath,).catch(() => undefined);
				await client.folders.delete(folderId,).catch(() => undefined);
			}
			await rm(tempDir, { recursive: true, force: true, },);
		}
	});
},);
