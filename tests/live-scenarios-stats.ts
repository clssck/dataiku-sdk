import { expect, } from "bun:test";
import { LiveCapabilityError, LiveCommandError, type LiveContext, } from "./live-context.js";

/**
 * Scenario / statistics / data-quality-timeline / dashboard-export live-suite
 * module for the "core" profile.
 *
 * exerciseScenariosStats(ctx) — run phase, repeatable. Every case creates its
 * own transient resources (custom Python scenarios, statistics worksheets, an
 * export-only dashboard) inside the root project, exercises the CLI against
 * them and removes them in finally. Root fixtures (datasets, baseline
 * scenario / dashboard / rules) are only read.
 *
 * All mutations go through the CLI (ctx.run). The SDK client is used only for
 * independent read-back verification.
 */

const SCENARIO_WAIT_TIMEOUT_MS = 120_000;
const SCENARIO_ABORT_SETTLE_TIMEOUT_MS = 120_000;
const SCENARIO_START_TIMEOUT_MS = 60_000;
const STATISTICS_FUTURE_TIMEOUT_MS = 180_000;
const DQ_COMPUTE_TIMEOUT_MS = 180_000;
const POLL_INTERVAL_MS = 2_000;

const STATISTICS_DATASET = "orderlines";
const STATISTICS_COLUMN = "unit_price";
const STATISTICS_SECOND_COLUMN = "qty";

const SCENARIO_MARKER = "live scenarios-stats marker";
const SCENARIO_SCRIPT = `print(${JSON.stringify(SCENARIO_MARKER,)})\n`;
const SCENARIO_SCRIPT_V2 = `print(${JSON.stringify(`${SCENARIO_MARKER} v2`,)})\n`;
/** Long enough for the abort to observe a running scenario; DSS kills the interpreter on abort. */
const SCENARIO_SLEEP_SCRIPT = "import time\nfor _ in range(240):\n    time.sleep(1)\n";

function uniq(ctx: LiveContext, base: string,): string {
	return `${base}_i${String(ctx.iteration,)}`;
}

function requireString(value: unknown, label: string,): string {
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`Expected non-empty string for ${label}, got ${JSON.stringify(value,)}`,);
	}
	return value;
}

function sleep(ms: number,): Promise<void> {
	const { promise, resolve, } = Promise.withResolvers<void>();
	setTimeout(resolve, ms,);
	return promise;
}

function requireFixtureDataset(ctx: LiveContext, name: string,): string {
	const dataset = ctx.fixtures.datasets[name];
	if (!dataset) {
		throw new LiveCapabilityError(
			`scenarios-stats requires baseline dataset "${name}" in ctx.fixtures.datasets; core data module did not provision it`,
		);
	}
	return dataset;
}

function requireNumber(value: unknown, label: string,): number {
	if (typeof value !== "number" || !Number.isFinite(value,)) {
		throw new Error(`Expected finite number for ${label}, got ${JSON.stringify(value,)}`,);
	}
	return value;
}

interface ScenarioStatusView {
	id?: string;
	active?: boolean;
	running?: boolean;
	lastRun?: { runId?: string; outcome?: string; };
}

interface ScenarioRunSummaryView {
	runId: string;
	start?: number;
	end?: number;
	result?: Record<string, unknown>;
}

interface StatisticsFutureView {
	jobId?: string | null;
	hasResult?: boolean;
	result?: unknown;
}

async function scenarioStatus(ctx: LiveContext, id: string,): Promise<ScenarioStatusView> {
	return ctx.run<ScenarioStatusView>(["scenario", "status", id,],);
}

/** Poll the light status until `running` matches `expected`, or fail with the last observation. */
async function waitForScenarioRunning(
	ctx: LiveContext,
	id: string,
	expected: boolean,
	timeoutMs: number,
): Promise<ScenarioStatusView> {
	const deadline = Date.now() + timeoutMs;
	let last = await scenarioStatus(ctx, id,);
	while (last.running !== expected) {
		if (Date.now() >= deadline) {
			throw new Error(
				`scenario ${id} did not reach running=${String(expected,)} within ${
					String(timeoutMs,)
				}ms; last status ${JSON.stringify(last,)}`,
			);
		}
		await sleep(POLL_INTERVAL_MS,);
		last = await scenarioStatus(ctx, id,);
	}
	return last;
}

async function deleteScenarioIfExists(ctx: LiveContext, id: string,): Promise<void> {
	const deleted = await ctx.run<{ deleted?: string; skipped?: string; }>([
		"scenario",
		"delete",
		id,
		"--if-exists",
	],);
	expect(deleted.deleted ?? deleted.skipped,).toBe(id,);
}

/** Resolve a statistics future response: synchronous results are returned as-is, otherwise wait on the future. */
async function awaitStatisticsFuture(
	ctx: LiveContext,
	response: StatisticsFutureView,
	label: string,
): Promise<unknown> {
	if (response.hasResult === true) return response.result;
	const jobId = requireString(response.jobId, `${label} future jobId`,);
	const waited = await ctx.run<
		{ state: string; success: boolean; timedOut?: boolean; result?: unknown; }
	>([
		"future",
		"wait",
		jobId,
		"--timeout",
		String(STATISTICS_FUTURE_TIMEOUT_MS,),
	],);
	expect(waited.timedOut,).not.toBe(true,);
	expect(waited.state,).toBe("DONE",);
	expect(waited.success,).toBe(true,);
	return waited.result;
}

function univariateCard(column: string,): Record<string, unknown> {
	return { type: "univariate_header", xColumns: [{ name: column, type: "CONTINUOUS", },], };
}

function isPlainObject(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

function worksheetDatasetSelection(dataset: string,): Record<string, unknown> {
	return {
		inputDatasetSmartName: dataset,
		datasetSelection: {
			partitionSelectionMethod: "ALL",
			maxRecords: 30000,
			samplingMethod: "FULL",
		},
	};
}

interface DashboardExportErrorEnvelope {
	code?: unknown;
	status?: unknown;
	hint?: unknown;
	details?: { dssCategory?: unknown; statusText?: unknown; body?: unknown; };
}

/**
 * Classify a dashboard-export CLI failure.
 *
 * The CLI error report redacts the gateway's HTML body (details.body becomes
 * "{}"), so the raw "Dataiku instance not found" phrase is not observable from
 * the case. The observed envelope for an unserved route is exactly:
 * code "not_found", status 404, hint "gateway returned HTML", details.body
 * "{}" and details.dssCategory "not_found".
 *
 * Before declaring the endpoint unavailable, the same documented URL
 * (POST /dashboards/{id}/action/export) is re-verified READ-ONLY through the
 * SDK against a dashboard that is known to exist (we just GET-verified it): a
 * not_found with the same gateway signature on a verified-existing dashboard
 * proves the route is not served on this instance. Any other failure shape
 * (wrong id → different envelope, payload bug, unexpected status) propagates
 * as a hard failure.
 */
async function classifyDashboardExportFailure(
	ctx: LiveContext,
	dashboardId: string,
	error: unknown,
): Promise<unknown> {
	if (!(error instanceof LiveCommandError)) return error;
	const envelope: DashboardExportErrorEnvelope = isPlainObject(error.result,)
		? error.result
		: {};
	const status = typeof envelope.status === "number" ? envelope.status : undefined;
	const hint = typeof envelope.hint === "string" ? envelope.hint : "";
	const isGatewayRouteMiss = envelope.code === "not_found"
		&& status === 404
		&& hint.includes("gateway returned HTML",)
		&& envelope.details?.dssCategory === "not_found"
		&& envelope.details?.body === "{}";
	if (!isGatewayRouteMiss) return error;

	// Read-only corroboration: the dashboard definitely exists (GET passed just
	// before the export attempt), so re-read it through the SDK. A 404 here with
	// the same gateway signature would contradict the successful GET and stays a
	// failure; a successful read pins the miss on the export route itself.
	let dashboardReadable = false;
	try {
		await ctx.client.dashboards.get(dashboardId, ctx.projectKey,);
		dashboardReadable = true;
	} catch {
		dashboardReadable = false;
	}
	if (!dashboardReadable) return error;
	return new LiveCapabilityError(
		`documented dashboard export route returned 404; graphics export prerequisite could not be verified: ${
			error.message.replace(/\s+/gu, " ",)
		}`,
	);
}

// ---------------------------------------------------------------------------
// Run (repeatable)
// ---------------------------------------------------------------------------

export async function exerciseScenariosStats(ctx: LiveContext,): Promise<void> {
	await ctx.check("core.scenario.custom-lifecycle", [
		"scenario.create",
		"scenario.list",
		"scenario.get",
		"scenario.update",
		"scenario.payload-set",
		"scenario.payload-get",
		"scenario.run",
		"scenario.last-runs",
		"scenario.get-run",
		"scenario.log",
		"scenario.delete",
	], async () => {
		const id = uniq(ctx, "live_scn_custom",);
		const name = `Live custom scenario ${String(ctx.iteration,)}`;
		const created = await ctx.run<{ created: string; name: string; }>([
			"scenario",
			"create",
			id,
			name,
			"--type",
			"custom_python",
		],);
		expect(created.created,).toBe(id,);
		try {
			const list = await ctx.run<Array<{ id: string; }>>(["scenario", "list",],);
			expect(list.some((scenario,) => scenario.id === id),).toBe(true,);
			const details = await ctx.run<{ id: string; name?: string; type?: string; }>([
				"scenario",
				"get",
				id,
			],);
			expect(details.name,).toBe(name,);
			expect(details.type,).toBe("custom_python",);

			// Custom Python scenarios run in the project's default code env (INHERIT), like runScript.
			const envUpdate = await ctx.run<{ verified: boolean; }>([
				"scenario",
				"update",
				id,
				"--data",
				JSON.stringify({ params: { envSelection: { envMode: "INHERIT", }, }, },),
			],);
			expect(envUpdate.verified,).toBe(true,);

			const setPayload = await ctx.run<{ updated: string; part: string; }>([
				"scenario",
				"payload-set",
				id,
				"--data",
				JSON.stringify({ script: SCENARIO_SCRIPT, extension: "py", },),
			],);
			expect(setPayload.updated,).toBe(id,);
			expect(setPayload.part,).toBe("payload",);
			const payload = await ctx.run<{ script?: string; }>(["scenario", "payload-get", id,],);
			expect(payload.script,).toBe(SCENARIO_SCRIPT,);
			const sdkPayload = await ctx.client.scenarios.getPayload(id, { projectKey: ctx.projectKey, },);
			expect(sdkPayload.script,).toBe(SCENARIO_SCRIPT,);

			// Replace the script and confirm the second write, not the first, is what DSS keeps.
			await ctx.run([
				"scenario",
				"payload-set",
				id,
				"--data",
				JSON.stringify({ script: SCENARIO_SCRIPT_V2, extension: "py", },),
			],);
			const payloadV2 = await ctx.run<{ script?: string; }>(["scenario", "payload-get", id,],);
			expect(payloadV2.script,).toBe(SCENARIO_SCRIPT_V2,);

			const run = await ctx.run<
				{ scenarioId: string; runId: string; outcome: string; success: boolean; timedOut?: boolean; }
			>([
				"scenario",
				"run",
				id,
				"--wait",
				"--timeout",
				String(SCENARIO_WAIT_TIMEOUT_MS,),
			],);
			expect(run.scenarioId,).toBe(id,);
			expect(run.timedOut,).not.toBe(true,);
			expect(run.outcome,).toBe("SUCCESS",);
			expect(run.success,).toBe(true,);
			const runId = requireString(run.runId, "custom scenario run id",);

			const lastRuns = await ctx.run<ScenarioRunSummaryView[]>([
				"scenario",
				"last-runs",
				id,
				"--limit",
				"5",
			],);
			expect(lastRuns.length,).toBeGreaterThan(0,);
			const summary = lastRuns.find((entry,) => entry.runId === runId);
			expect(summary,).toBeTruthy();
			expect(summary?.result?.outcome,).toBe("SUCCESS",);

			const runDetails = await ctx.run<
				{ scenarioRun?: { runId?: string; result?: { outcome?: string; }; }; stepRuns?: unknown[]; }
			>([
				"scenario",
				"get-run",
				id,
				runId,
			],);
			expect(runDetails.scenarioRun?.runId,).toBe(runId,);
			expect(runDetails.scenarioRun?.result?.outcome,).toBe("SUCCESS",);

			const log = await ctx.run<string>(["scenario", "log", id, runId,],);
			expect(typeof log,).toBe("string",);
			expect(log,).toContain(`${SCENARIO_MARKER} v2`,);
			const logFile = await ctx.run<{ path: string; truncated: boolean; }>([
				"scenario",
				"log",
				id,
				runId,
				"--max-log-bytes",
				"4096",
				"--output",
				`${ctx.dir}/${uniq(ctx, "live_scn_custom_log",)}.txt`,
			],);
			expect(typeof logFile.truncated,).toBe("boolean",);
			const written = await Bun.file(logFile.path,).text();
			expect(written.length,).toBeGreaterThan(0,);
			expect(written.length,).toBeLessThanOrEqual(4096,);
		} finally {
			await deleteScenarioIfExists(ctx, id,);
			const remaining = await ctx.run<Array<{ id: string; }>>(["scenario", "list",],);
			expect(remaining.some((scenario,) => scenario.id === id),).toBe(false,);
		}
	},);

	await ctx.check("core.scenario.activation", [
		"scenario.create",
		"scenario.active-set",
		"scenario.status",
		"scenario.get",
		"scenario.delete",
	], async () => {
		const id = uniq(ctx, "live_scn_active",);
		await ctx.run(["scenario", "create", id, `Live activation scenario ${String(ctx.iteration,)}`,],);
		try {
			const initial = await scenarioStatus(ctx, id,);
			expect(initial.id ?? id,).toBe(id,);
			expect(initial.running,).not.toBe(true,);

			const activated = await ctx.run<
				{ scenarioId: string; active: boolean; before?: boolean; status: { active?: boolean; }; }
			>([
				"scenario",
				"active-set",
				id,
				"true",
			],);
			expect(activated.scenarioId,).toBe(id,);
			expect(activated.active,).toBe(true,);
			expect(activated.status.active,).toBe(true,);
			expect((await scenarioStatus(ctx, id,)).active,).toBe(true,);
			const activeDetails = await ctx.run<{ active?: boolean; }>(["scenario", "get", id,],);
			expect(activeDetails.active,).toBe(true,);

			const deactivated = await ctx.run<
				{ scenarioId: string; active: boolean; before?: boolean; status: { active?: boolean; }; }
			>([
				"scenario",
				"active-set",
				id,
				"false",
			],);
			expect(deactivated.active,).toBe(false,);
			expect(deactivated.before,).toBe(true,);
			expect(deactivated.status.active,).toBe(false,);
			expect((await scenarioStatus(ctx, id,)).active,).toBe(false,);
			const sdkStatus = await ctx.client.scenarios.status(id, ctx.projectKey,);
			expect(sdkStatus.active,).toBe(false,);
		} finally {
			await deleteScenarioIfExists(ctx, id,);
		}
	},);

	await ctx.check("core.scenario.abort", [
		"scenario.create",
		"scenario.payload-set",
		"scenario.run",
		"scenario.status",
		"scenario.abort",
		"scenario.last-runs",
		"scenario.get-run",
		"scenario.delete",
	], async () => {
		const id = uniq(ctx, "live_scn_abort",);
		await ctx.run([
			"scenario",
			"create",
			id,
			`Live abort scenario ${String(ctx.iteration,)}`,
			"--type",
			"custom_python",
		],);
		try {
			await ctx.run([
				"scenario",
				"update",
				id,
				"--data",
				JSON.stringify({ params: { envSelection: { envMode: "INHERIT", }, }, },),
			],);
			await ctx.run([
				"scenario",
				"payload-set",
				id,
				"--data",
				JSON.stringify({ script: SCENARIO_SLEEP_SCRIPT, extension: "py", },),
			],);
			const triggered = await ctx.run<{ runId: string; }>(["scenario", "run", id,],);
			expect(triggered.runId.length,).toBeGreaterThan(0,);

			const running = await waitForScenarioRunning(ctx, id, true, SCENARIO_START_TIMEOUT_MS,);
			expect(running.running,).toBe(true,);

			const aborted = await ctx.run<{ aborted: string; }>(["scenario", "abort", id,],);
			expect(aborted.aborted,).toBe(id,);

			const settled = await waitForScenarioRunning(
				ctx,
				id,
				false,
				SCENARIO_ABORT_SETTLE_TIMEOUT_MS,
			);
			expect(settled.running,).toBe(false,);

			// The abort is accepted asynchronously; poll the finished run entry until DSS
			// records its outcome (the light status can settle before the run result lands).
			const deadline = Date.now() + SCENARIO_ABORT_SETTLE_TIMEOUT_MS;
			let run: ScenarioRunSummaryView | undefined;
			let outcome: string | undefined;
			while (Date.now() < deadline) {
				const lastRuns = await ctx.run<ScenarioRunSummaryView[]>([
					"scenario",
					"last-runs",
					id,
					"--limit",
					"1",
				],);
				run = lastRuns[0];
				outcome = typeof run?.result?.outcome === "string" ? run.result.outcome : undefined;
				if (outcome !== undefined) break;
				await sleep(POLL_INTERVAL_MS,);
			}
			const runId = requireString(run?.runId, "aborted scenario run id",);
			expect(outcome, `aborted run ${runId} outcome in last-runs`,).toBe("ABORTED",);

			const details = await ctx.run<
				{ scenarioRun?: { runId?: string; result?: { outcome?: string; }; }; }
			>([
				"scenario",
				"get-run",
				id,
				runId,
			],);
			expect(
				details.scenarioRun?.result?.outcome,
				`aborted run ${runId} scenarioRun.result.outcome in get-run`,
			).toBe("ABORTED",);
			// The documented DSS 15 /light response carries only id/running/active — verified
			// live (read-only): no lastRun/outcome field exists on ScenarioWithStatus in 15.0.1,
			// so the finished-run outcome is asserted from last-runs and get-run above. The SDK
			// status read confirms the scenario is no longer running.
			const sdkStatus = await ctx.client.scenarios.status(id, ctx.projectKey,);
			expect(sdkStatus.running, "SDK status.running after abort",).toBe(false,);
		} finally {
			// A scenario still running cannot be deleted; make sure it is stopped before removal.
			const status = await scenarioStatus(ctx, id,);
			if (status.running === true) {
				await ctx.run(["scenario", "abort", id,],);
				await waitForScenarioRunning(ctx, id, false, SCENARIO_ABORT_SETTLE_TIMEOUT_MS,);
			}
			await deleteScenarioIfExists(ctx, id,);
		}
	},);

	await ctx.check("core.statistics.worksheet-lifecycle", [
		"statistics.create-worksheet",
		"statistics.list-worksheets",
		"statistics.get-worksheet",
		"statistics.update-worksheet",
		"statistics.delete-worksheet",
	], async () => {
		const dataset = requireFixtureDataset(ctx, STATISTICS_DATASET,);
		const name = uniq(ctx, "Live statistics worksheet",);
		const created = await ctx.run<{ id: string; name?: string; }>([
			"statistics",
			"create-worksheet",
			dataset,
			"--data",
			JSON.stringify({ name, dataSpec: worksheetDatasetSelection(dataset,), },),
		],);
		const worksheetId = requireString(created.id, "statistics worksheet id",);
		try {
			expect(created.name,).toBe(name,);
			const list = await ctx.run<Array<{ id: string; name?: string; }>>([
				"statistics",
				"list-worksheets",
				dataset,
			],);
			expect(list.some((entry,) => entry.id === worksheetId && entry.name === name),).toBe(true,);

			const got = await ctx.run<
				{
					id: string;
					name?: string;
					dataSpec?: {
						inputDatasetSmartName?: string;
						datasetSelection?: { samplingMethod?: string; };
					};
					rootCard?: { type?: string; cards?: unknown[]; };
				}
			>([
				"statistics",
				"get-worksheet",
				dataset,
				worksheetId,
			],);
			expect(got.id,).toBe(worksheetId,);
			expect(got.dataSpec?.inputDatasetSmartName,).toBe(dataset,);
			expect(got.dataSpec?.datasetSelection?.samplingMethod,).toBe("FULL",);
			expect(isPlainObject(got.rootCard,),).toBe(true,);

			const renamed = `${name} (updated)`;
			const cardsBefore = got.rootCard?.cards ?? [];
			const updated = await ctx.run<{ id: string; name?: string; }>([
				"statistics",
				"update-worksheet",
				dataset,
				worksheetId,
				"--data",
				JSON.stringify({
					...got,
					name: renamed,
					rootCard: {
						...got.rootCard,
						cards: [...cardsBefore, univariateCard(STATISTICS_COLUMN,),],
					},
				},),
			],);
			expect(updated.id,).toBe(worksheetId,);
			expect(updated.name,).toBe(renamed,);
			const after = await ctx.client.statistics.getWorksheet(dataset, worksheetId, ctx.projectKey,);
			expect(after.name,).toBe(renamed,);
			const afterRootCard = after.rootCard;
			const cardsAfter = afterRootCard !== undefined && isPlainObject(afterRootCard,)
					&& Array.isArray(afterRootCard.cards,)
				? afterRootCard.cards
				: [];
			expect(cardsAfter.length,).toBe(cardsBefore.length + 1,);
			expect(JSON.stringify(cardsAfter,),).toContain(STATISTICS_COLUMN,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>([
				"statistics",
				"delete-worksheet",
				dataset,
				worksheetId,
			],);
			expect(deleted.deleted,).toBe(worksheetId,);
			const remaining = await ctx.run<Array<{ id: string; }>>([
				"statistics",
				"list-worksheets",
				dataset,
			],);
			expect(remaining.some((entry,) => entry.id === worksheetId),).toBe(false,);
		}
	},);

	await ctx.check("core.statistics.computation", [
		"statistics.create-worksheet",
		"statistics.get-worksheet",
		"statistics.update-worksheet",
		"statistics.run-worksheet",
		"statistics.run-card",
		"statistics.run-computation",
		"future.wait",
		"statistics.delete-worksheet",
	], async () => {
		const dataset = requireFixtureDataset(ctx, STATISTICS_DATASET,);
		const created = await ctx.run<{ id: string; }>([
			"statistics",
			"create-worksheet",
			dataset,
			"--data",
			JSON.stringify({
				name: uniq(ctx, "Live statistics computation",),
				dataSpec: worksheetDatasetSelection(dataset,),
			},),
		],);
		const worksheetId = requireString(created.id, "statistics worksheet id",);
		try {
			// Give the root card one univariate card so run-worksheet computes something real.
			const got = await ctx.run<{ rootCard?: { cards?: unknown[]; }; }>([
				"statistics",
				"get-worksheet",
				dataset,
				worksheetId,
			],);
			const cardsBefore = got.rootCard?.cards ?? [];
			await ctx.run([
				"statistics",
				"update-worksheet",
				dataset,
				worksheetId,
				"--data",
				JSON.stringify({
					...got,
					rootCard: {
						...got.rootCard,
						cards: [...cardsBefore, univariateCard(STATISTICS_COLUMN,),],
					},
				},),
			],);

			const worksheetRun = await ctx.run<StatisticsFutureView>([
				"statistics",
				"run-worksheet",
				dataset,
				worksheetId,
			],);
			const worksheetResult = await awaitStatisticsFuture(ctx, worksheetRun, "run-worksheet",);
			expect(isPlainObject(worksheetResult,),).toBe(true,);
			// The worksheet-run result is the root card's envelope. With a univariate card on
			// unit_price the envelope carries per-column results naming the column.
			expect(JSON.stringify(worksheetResult,),).toContain(STATISTICS_COLUMN,);

			const cardRun = await ctx.run<StatisticsFutureView>([
				"statistics",
				"run-card",
				dataset,
				worksheetId,
				"--data",
				JSON.stringify(univariateCard(STATISTICS_SECOND_COLUMN,),),
			],);
			const cardResult = await awaitStatisticsFuture(ctx, cardRun, "run-card",);
			// DSS 15.0.1 answers a standalone run-card with the card envelope; per-column
			// result population for a column outside the saved worksheet is not guaranteed
			// (iteration-5 observed an empty results[] envelope), so assert the computed
			// envelope shape rather than column presence.
			expect(isPlainObject(cardResult,),).toBe(true,);
			expect(JSON.stringify(cardResult,),).toContain("univariate_header",);

			const computationRun = await ctx.run<StatisticsFutureView>([
				"statistics",
				"run-computation",
				dataset,
				worksheetId,
				"--data",
				JSON.stringify({ type: "count", },),
			],);
			const computationResult = await awaitStatisticsFuture(ctx, computationRun, "run-computation",);
			const resultRecord: Record<string, unknown> = isPlainObject(computationResult,)
				? computationResult
				: {};
			// Real computed value, not an envelope: the count computation must produce the
			// dataset's actual row count. A payload without a numeric count is a hard failure
			// carrying the raw body so the DSS result shape can be diagnosed, never masked.
			const expectedRows = requireNumber(
				ctx.fixtures.expectedRows[dataset],
				`fixtures.expectedRows["${dataset}"]`,
			);
			const rawCount = "count" in resultRecord ? resultRecord.count : undefined;
			const countValue = typeof rawCount === "number" ? rawCount : undefined;
			expect(
				[typeof countValue, JSON.stringify(computationResult,).slice(0, 400,),],
				`run-computation {type:count} numeric count for ${dataset}`,
			).toEqual(["number", expect.any(String,),],);
			expect(countValue, "computed row count",).toBe(expectedRows,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>([
				"statistics",
				"delete-worksheet",
				dataset,
				worksheetId,
			],);
			expect(deleted.deleted,).toBe(worksheetId,);
		}
	},);

	await ctx.check("core.data-quality.timeline", [
		"data-quality.create-rule",
		"data-quality.compute",
		"data-quality.project-timeline",
		"data-quality.project-status",
		"data-quality.delete-rule",
	], async () => {
		const dataset = Object.keys(ctx.fixtures.datasets,)[0];
		if (!dataset) {
			throw new LiveCapabilityError("data-quality timeline requires a baseline dataset",);
		}
		// Self-sufficient prereq: a transient rule so this case works even when the
		// collaboration setup case is not selected.
		const ruleDisplayName = uniq(ctx, "Live scenarios-stats dq rule",);
		const created = await ctx.run<{ created: string; }>([
			"data-quality",
			"create-rule",
			dataset,
			"--data",
			JSON.stringify({
				type: "RecordCountInRangeRule",
				displayName: ruleDisplayName,
				softMinimum: 0,
				softMinimumEnabled: true,
				hardMaximum: 1_000_000_000,
				hardMaximumEnabled: true,
			},),
		],);
		const ruleId = requireString(created.created, "transient data quality rule id",);
		const before = Date.now();
		try {
			const compute = await ctx.run<Record<string, unknown>>([
				"data-quality",
				"compute",
				dataset,
				"--wait",
				"--timeout",
				String(DQ_COMPUTE_TIMEOUT_MS,),
			],);
			expect(compute,).toBeTruthy();

			const timeline = await ctx.run<Array<Record<string, unknown>>>([
				"data-quality",
				"project-timeline",
			],);
			expect(Array.isArray(timeline,),).toBe(true,);
			expect(timeline.length,).toBeGreaterThan(0,);
			for (const entry of timeline) expect(isPlainObject(entry,),).toBe(true,);

			// DSS rejects a minTimestamp in the future with 400 (verified against DSS 15.0.1),
			// so exercise the window parameters with verified-legal shapes: a max-only window
			// before all data yields [], and a bounded window that spans the computation day
			// is non-empty.
			const emptyWindow = await ctx.run<Array<Record<string, unknown>>>([
				"data-quality",
				"project-timeline",
				"--max-timestamp",
				String(before - 86_400_000,),
			],);
			expect(Array.isArray(emptyWindow,),).toBe(true,);
			expect(emptyWindow.length,).toBe(0,);

			const bounded = await ctx.run<Array<Record<string, unknown>>>([
				"data-quality",
				"project-timeline",
				"--min-timestamp",
				String(before - 86_400_000,),
				"--max-timestamp",
				String(Date.now() + 60_000,),
			],);
			expect(bounded.length,).toBeGreaterThan(0,);
			const sdkTimeline = await ctx.client.dataQuality.projectTimeline({
				projectKey: ctx.projectKey,
			},);
			expect(sdkTimeline.length,).toBe(timeline.length,);

			const projectStatus = await ctx.run<unknown>(["data-quality", "project-status",],);
			expect(JSON.stringify(projectStatus,),).toContain(dataset,);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>([
				"data-quality",
				"delete-rule",
				dataset,
				ruleId,
			],);
			expect(deleted.deleted,).toBe(ruleId,);
		}
	},);

	await ctx.check("core.data-quality.partition-status", [
		"data-quality.status",
		"data-quality.status-by-partition",
	], async () => {
		const dataset = Object.keys(ctx.fixtures.datasets,)[0];
		if (!dataset) {
			throw new LiveCapabilityError("data-quality partition status requires a baseline dataset",);
		}
		const status = await ctx.run<unknown>(["data-quality", "status", dataset,],);
		expect(status,).toBeTruthy();

		const byPartition = await ctx.run<Record<string, unknown>>([
			"data-quality",
			"status-by-partition",
			dataset,
		],);
		expect(isPlainObject(byPartition,),).toBe(true,);
		const all = await ctx.run<Record<string, unknown>>([
			"data-quality",
			"status-by-partition",
			dataset,
			"--include-all-partitions",
		],);
		expect(isPlainObject(all,),).toBe(true,);
		// The fixture datasets are not partitioned: DSS reports exactly one virtual partition ("NP")
		// or a keyed map; --include-all-partitions can only widen that view, never shrink it.
		const partitionKeys = (value: Record<string, unknown>,): string[] => {
			const partitions = value.partitions ?? value.byPartition ?? value;
			return isPlainObject(partitions,) ? Object.keys(partitions,) : [];
		};
		expect(partitionKeys(all,).length,).toBeGreaterThanOrEqual(partitionKeys(byPartition,).length,);
		const sdk = await ctx.client.dataQuality.statusByPartition(dataset, {
			includeAllPartitions: true,
			projectKey: ctx.projectKey,
		},);
		expect(JSON.stringify(sdk,),).toBe(JSON.stringify(all,),);
	},);

	await ctx.check("core.dashboard.export", [
		"dashboard.create",
		"dashboard.get",
		"dashboard.export",
		"dashboard.delete",
	], async () => {
		const name = uniq(ctx, "Live dashboard export",);
		const insightId = ctx.fixtures.insightId;
		const pages = insightId
			? [{
				id: "page1",
				grid: { tiles: [{ tileType: "INSIGHT", insightId, x: 0, y: 0, w: 6, h: 6, },], },
			},]
			: [{ id: "page1", grid: { tiles: [], }, },];
		const created = await ctx.run<{ created: string; }>([
			"dashboard",
			"create",
			"--name",
			name,
			"--listed",
			"false",
			"--data",
			JSON.stringify({ pages, },),
		],);
		const id = requireString(created.created, "export dashboard id",);
		try {
			const got = await ctx.run<{ id?: string; name: string; }>(["dashboard", "get", id,],);
			expect(got.name,).toBe(name,);

			const outputPath = `${ctx.dir}/${uniq(ctx, "live_dashboard_export",)}.pdf`;
			let exported: {
				path: string;
				bytes: number;
				dashboardId: string;
				paperSize: string;
				orientation: string;
				slideIndex: number;
			};
			try {
				exported = await ctx.run([
					"dashboard",
					"export",
					id,
					"--output",
					outputPath,
					"--paper-size",
					"A4",
					"--orientation",
					"PORTRAIT",
					"--file-type",
					"PDF",
					"--slide-index",
					"0",
				],);
			} catch (error) {
				throw await classifyDashboardExportFailure(ctx, id, error,);
			}
			expect(exported.dashboardId,).toBe(id,);
			expect(exported.paperSize,).toBe("A4",);
			expect(exported.orientation,).toBe("PORTRAIT",);
			expect(exported.slideIndex,).toBe(0,);
			expect(exported.bytes,).toBeGreaterThan(0,);
			const bytes = new Uint8Array(await Bun.file(exported.path,).arrayBuffer(),);
			expect(bytes.length,).toBe(exported.bytes,);
			expect(new TextDecoder().decode(bytes.subarray(0, 5,),),).toBe("%PDF-",);
		} finally {
			const deleted = await ctx.run<{ deleted: string; }>(["dashboard", "delete", id,],);
			expect(deleted.deleted,).toBe(id,);
			const remaining = await ctx.run<Array<{ id: string; }>>(["dashboard", "list",],);
			expect(remaining.some((dashboard,) => dashboard.id === id),).toBe(false,);
		}
	},);
}
