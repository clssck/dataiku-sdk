/**
 * Flow and job cases that mutate topology (rename, partition, clone, restore,
 * zone organize, abort). They run in one disposable child project so the
 * shared root fixtures are never renamed, rebuilt, or reorganized.
 */
import { expect, } from "bun:test";
import { join, } from "node:path";
import { LIVE_CSV_FORMAT, type LiveContext, } from "./live-context.js";

const ORDERS_CSV = [
	"order_id,customer_id,amount,day",
	"O1,C1,10.5,2024-01-01",
	"O2,C2,7,2024-01-01",
	"O3,C1,3.25,2024-01-02",
	"O4,C3,99,2024-01-02",
	"",
].join("\n",);
const ORDERS_ROWS = 4;
const ORDERS_COLUMNS = [
	{ name: "order_id", type: "string", },
	{ name: "customer_id", type: "string", },
	{ name: "amount", type: "double", },
	{ name: "day", type: "string", },
];

const ORDERS = "fj_orders";
const SYNCED = "fj_orders_sync";
const PARTITIONED = "fj_orders_part";
const SLOW_OUTPUT = "fj_orders_slow";
const SYNC_RECIPE = "fj_sync_orders";
const PARTITION_RECIPE = "fj_sync_partitioned";
const SLOW_RECIPE = "fj_python_slow";
const PARTITION = "2024-01-01";
const JOB_TIMEOUT_MS = 300_000;
const POLL_MS = 3_000;
const ZONE_NAME = "Flow jobs raw";
const ZONE_COLOR = "#64748b";

/** Long enough for an abort to land while the recipe process is running. */
const SLOW_PYTHON_CODE = [
	"import time",
	"import dataiku",
	"",
	"time.sleep(120)",
	`dataiku.Dataset("${SLOW_OUTPUT}").write_with_schema(dataiku.Dataset("${ORDERS}").get_dataframe())`,
	"",
].join("\n",);

interface RecipeSettings {
	recipe: {
		name: string;
		type: string;
		tags?: string[];
		inputs?: Record<string, { items?: Array<{ ref: string; }>; }>;
		outputs?: Record<string, { items?: Array<{ ref: string; }>; }>;
	};
	payload?: string;
}

interface JobWait {
	success: boolean;
	jobId: string;
	state: string;
	elapsedMs: number;
	pollCount: number;
	logSummary?: { lineCount: number; };
}

function outputRefs(settings: RecipeSettings,): string[] {
	return Object.values(settings.recipe.outputs ?? {},).flatMap((role,) =>
		(role.items ?? []).map((item,) => item.ref)
	);
}

export async function exerciseFlowJobs(ctx: LiveContext,): Promise<void> {
	if (ctx.phase !== "run") return;
	const iter = ctx.iteration;
	let child: string | undefined;
	const run = <T = unknown,>(args: string[], expectedExit?: number,): Promise<T> => {
		if (!child) throw new Error("Flow/jobs child project was not provisioned",);
		return ctx.run<T>(args, { projectKey: child, expectedExit, },);
	};
	const createManaged = (name: string,) =>
		run(["dataset", "create-managed", "--name", name, "--connection", ctx.connection,],);

	// ---------------------------------------------------------------------------
	// Lazy provisioning: each helper is idempotent and called inside selected
	// ctx.check bodies only, so any single selected case provisions exactly its
	// own prerequisites. Unselected cases never mutate anything.
	// ---------------------------------------------------------------------------

	/** Lazily create the disposable child project (selected-case scope). */
	async function ensureProject(): Promise<string> {
		if (child) return child;
		child = await ctx.createProject("flow-jobs",);
		return child;
	}

	/** Lazily upload the input dataset; schema refresh is idempotent per run. */
	async function ensureOrdersDataset(): Promise<void> {
		await ensureProject();
		const csvPath = await ctx.writeFile(`flow-jobs/${ORDERS}-${iter}.csv`, ORDERS_CSV,);
		await run([
			"dataset",
			"create",
			"--name",
			ORDERS,
			"--type",
			"UploadedFiles",
			"--if-not-exists",
		],);
		const files = await run<unknown[]>(["dataset", "files", ORDERS,],);
		if (files.length === 0) {
			await run(["dataset", "upload-file", ORDERS, csvPath, "--file-name", `${ORDERS}.csv`,],);
			const schemaPath = await ctx.writeFile(
				`flow-jobs/${ORDERS}-${iter}.schema.json`,
				JSON.stringify({ columns: ORDERS_COLUMNS, },),
			);
			await run(["dataset", "refresh-schema", ORDERS, "--data-file", schemaPath,],);
			await run(["dataset", "update", ORDERS, "--data", JSON.stringify(LIVE_CSV_FORMAT,),],);
		}
	}

	/** Lazily create a managed output and the sync recipe reading the input. */
	async function ensureSyncRecipe(): Promise<void> {
		await ensureOrdersDataset();
		const datasets = await run<Array<{ name: string; }>>(["dataset", "list",],);
		if (!datasets.some((entry,) => entry.name === SYNCED)) {
			await createManaged(SYNCED,);
			await run([
				"recipe",
				"create",
				"--type",
				"sync",
				"--name",
				SYNC_RECIPE,
				"--input",
				ORDERS,
				"--output",
				SYNCED,
			],);
		}
	}

	/** Lazily create the slow python recipe used by restore and abort cases. */
	async function ensureSlowRecipe(): Promise<void> {
		await ensureOrdersDataset();
		const recipes = await run<Array<{ name: string; }>>(["recipe", "list",],);
		if (!recipes.some((entry,) => entry.name === SLOW_RECIPE)) {
			await createManaged(SLOW_OUTPUT,);
			await run([
				"recipe",
				"create",
				"--type",
				"python",
				"--name",
				SLOW_RECIPE,
				"--input",
				ORDERS,
				"--output",
				SLOW_OUTPUT,
			],);
			const slowPath = await ctx.writeFile(`flow-jobs/${SLOW_RECIPE}-${iter}.py`, SLOW_PYTHON_CODE,);
			await run(["recipe", "set-payload", SLOW_RECIPE, "--file", slowPath, "--no-backup",],);
		}
	}

	try {
		// --- Disposable child project with an uploaded input and a sync output.
		await ctx.check("core.flow-jobs.project", [
			"project.create",
			"dataset.create",
			"dataset.upload-file",
			"dataset.refresh-schema",
			"dataset.update",
			"dataset.create-managed",
			"recipe.create",
			"recipe.set-payload",
			"dataset.assert-count",
		], async () => {
			await ensureProject();
			await ensureOrdersDataset();
			await ensureSyncRecipe();
			await ensureSlowRecipe();
		},);

		// --- Read-only inspection of the uploaded input and its sync output. --
		await ctx.check("core.dataset.inspect", [
			"dataset.info",
			"dataset.schema",
			"dataset.validate-build",
			"dataset.column-lineage",
		], async () => {
			await ensureOrdersDataset();
			await ensureSyncRecipe();
			// info() carries type/usage metadata but no schema on DSS 15.
			const info = await run<{ name?: string; type?: string; }>([
				"dataset",
				"info",
				ORDERS,
			],);
			expect(info,).toMatchObject({ name: ORDERS, type: "UploadedFiles", },);
			const schema = await run<{ columns: Array<{ name: string; }>; }>([
				"dataset",
				"schema",
				ORDERS,
			],);
			expect(schema.columns.map((column,) => column.name),).toEqual(
				ORDERS_COLUMNS.map((column,) => column.name),
			);

			const validation = await run<{
				valid: boolean;
				datasetName: string;
				projectKey: string;
				type: string | null;
				formatType: string | null;
				warnings: string[];
				materializationChecked: boolean;
			}>(["dataset", "validate-build", ORDERS,],);
			expect(validation,).toMatchObject({
				valid: true,
				datasetName: ORDERS,
				projectKey: child,
				type: "UploadedFiles",
				formatType: LIVE_CSV_FORMAT.formatType,
				warnings: [],
				materializationChecked: false,
			},);

			const lineage = await run<Record<string, unknown>>([
				"dataset",
				"column-lineage",
				SYNCED,
				"order_id",
				"--max-dataset-count",
				"50",
			],);
			expect(lineage,).toBeTruthy();
			expect(typeof lineage,).toBe("object",);

			// Validation rejects a non-positive dataset cap before any request.
			const rejected = await run<{ code?: string; }>(
				["dataset", "column-lineage", SYNCED, "order_id", "--max-dataset-count", "0",],
				1,
			);
			expect(rejected.code,).toBe("validation_failed",);
		},);

		await ctx.check("core.dataset.metadata", [
			"dataset.metadata",
			"dataset.metadata-set",
		], async () => {
			await ensureOrdersDataset();
			const tags = ["live-flow-jobs",];
			// DSS 15 dataset metadata GET/PUT round-trips tags/checklists/custom; the
			// documented `label` field is not returned by this endpoint.
			const current = await run<Record<string, unknown>>(["dataset", "metadata", ORDERS,],);
			expect(typeof current,).toBe("object",);
			const updated = await run<{ updated: string; }>([
				"dataset",
				"metadata-set",
				ORDERS,
				"--data",
				JSON.stringify({ ...current, tags, },),
			],);
			expect(updated.updated,).toBe(ORDERS,);
			const after = await run<{ tags?: string[]; }>(["dataset", "metadata", ORDERS,],);
			expect(after.tags,).toEqual(tags,);
			// Replace semantics: dropping tags from the object removes them.
			await run([
				"dataset",
				"metadata-set",
				ORDERS,
				"--data",
				JSON.stringify({ ...after, tags: [], },),
			],);
			const cleared = await run<{ tags?: string[]; }>(["dataset", "metadata", ORDERS,],);
			expect(cleared.tags ?? [],).toEqual([],);
		},);

		// --- Rename updates downstream recipe references, then rename back. --
		await ctx.check("core.dataset.rename", [
			"dataset.rename",
			"dataset.get",
			"recipe.get",
		], async () => {
			await ensureSyncRecipe();
			const renamed = `${SYNCED}_renamed`;
			const result = await run<{ renamed: string; to: string; }>([
				"dataset",
				"rename",
				SYNCED,
				renamed,
			],);
			expect(result,).toEqual({ renamed: SYNCED, to: renamed, },);
			try {
				const details = await run<{ name: string; }>(["dataset", "get", renamed,],);
				expect(details.name,).toBe(renamed,);
				// The old name is gone; not_found maps to the generic error exit 2.
				await run(["dataset", "get", SYNCED,], 2,);
				const recipe = await run<RecipeSettings>(["recipe", "get", SYNC_RECIPE, "--no-payload",],);
				expect(outputRefs(recipe,),).toEqual([renamed,],);
			} finally {
				await run(["dataset", "rename", renamed, SYNCED,],);
			}
			const restored = await run<RecipeSettings>(["recipe", "get", SYNC_RECIPE, "--no-payload",],);
			expect(outputRefs(restored,),).toEqual([SYNCED,],);
		},);

		// --- Partitioned managed output: empty listing, then one built partition.
		await ctx.check("core.dataset.partitions", [
			"dataset.create-managed",
			"dataset.update",
			"dataset.list-partitions",
			"recipe.create",
			"job.build-and-wait",
		], async () => {
			await ensureOrdersDataset();
			const datasets = await run<Array<{ name: string; }>>(["dataset", "list",],);
			if (!datasets.some((entry,) => entry.name === PARTITIONED)) {
				await createManaged(PARTITIONED,);
				await run([
					"dataset",
					"update",
					PARTITIONED,
					"--data",
					JSON.stringify({
						partitioning: {
							dimensions: [{ name: "day", type: "value", params: {}, },],
							filePathPattern: "%{day}/.*",
							ignoreNonMatchingFile: true,
						},
					},),
				],);
			}
			const settings = await run<{ partitioning?: { dimensions?: Array<{ name: string; }>; }; }>([
				"dataset",
				"get",
				PARTITIONED,
			],);
			expect(settings.partitioning?.dimensions?.map((dimension,) => dimension.name),).toEqual([
				"day",
			],);
			// list-partitions on a never-built partitioned dataset surfaces DSS's
			// "files missing" validation error; the observable listing contract
			// starts once the first partition exists below.

			const recipes = await run<Array<{ name: string; }>>(["recipe", "list",],);
			if (!recipes.some((entry,) => entry.name === PARTITION_RECIPE)) {
				await run([
					"recipe",
					"create",
					"--type",
					"sync",
					"--name",
					PARTITION_RECIPE,
					"--input",
					ORDERS,
					"--output",
					PARTITIONED,
				],);
			}
			const build = await run<JobWait>([
				"job",
				"build-and-wait",
				PARTITIONED,
				"--partition",
				PARTITION,
				"--timeout",
				String(JOB_TIMEOUT_MS,),
				"--poll-interval",
				String(POLL_MS,),
			],);
			expect(build.success,).toBe(true,);
			expect(build.state,).toBe("DONE",);
			expect(await run<string[]>(["dataset", "list-partitions", PARTITIONED,],),).toEqual([
				PARTITION,
			],);
		},);

		// --- Clone a recipe together with its output settings. ---------------
		await ctx.check("core.recipe.clone", [
			"recipe.clone",
			"recipe.get",
			"recipe.validate-graph",
			"dataset.get",
		], async () => {
			await ensureSyncRecipe();
			const cloneName = `${SYNC_RECIPE}_clone`;
			const cloneOutput = `${SYNCED}_clone`;
			const cloned = await run<{
				recipeName: string;
				sourceRecipeName: string;
				outputRewrites: Record<string, string>;
				copiedOutputDatasets: string[];
			}>([
				"recipe",
				"clone",
				SYNC_RECIPE,
				"--name",
				cloneName,
				"--output",
				cloneOutput,
				"--copy-output-settings",
				"--path",
				`/dataiku/${child}/${cloneOutput}`,
			],);
			expect(cloned,).toMatchObject({
				recipeName: cloneName,
				sourceRecipeName: SYNC_RECIPE,
				outputRewrites: { [SYNCED]: cloneOutput, },
				copiedOutputDatasets: [cloneOutput,],
			},);
			const settings = await run<RecipeSettings>(["recipe", "get", cloneName, "--no-payload",],);
			expect(settings.recipe,).toMatchObject({ name: cloneName, type: "sync", },);
			expect(outputRefs(settings,),).toEqual([cloneOutput,],);
			const output = await run<{ name: string; managed?: boolean; }>([
				"dataset",
				"get",
				cloneOutput,
			],);
			expect(output,).toMatchObject({ name: cloneOutput, managed: true, },);
			const graph = await run<
				{ valid: boolean; missingInputs: unknown[]; missingOutputs: unknown[]; }
			>(
				["recipe", "validate-graph", cloneName,],
			);
			expect(graph,).toMatchObject({ valid: true, missingInputs: [], missingOutputs: [], },);
			// The source recipe is untouched by the clone.
			const source = await run<RecipeSettings>(["recipe", "get", SYNC_RECIPE, "--no-payload",],);
			expect(outputRefs(source,),).toEqual([SYNCED,],);
		},);

		// --- Settings merge and metadata replace on a recipe. -----------------
		await ctx.check("core.recipe.update", [
			"recipe.update",
			"recipe.metadata",
			"recipe.metadata-set",
			"recipe.get",
		], async () => {
			await ensureSyncRecipe();
			const updated = await run<{ updated: string; }>([
				"recipe",
				"update",
				SYNC_RECIPE,
				"--data",
				JSON.stringify({ recipe: { tags: ["live-flow-jobs",], }, },),
			],);
			expect(updated.updated,).toBe(SYNC_RECIPE,);
			const settings = await run<RecipeSettings>(["recipe", "get", SYNC_RECIPE, "--no-payload",],);
			expect(settings.recipe.tags,).toEqual(["live-flow-jobs",],);
			// Merge semantics: the graph survives a tag-only patch.
			expect(outputRefs(settings,),).toEqual([SYNCED,],);

			// Definition fields outside the recipe key are rejected before any write.
			await run(
				["recipe", "update", SYNC_RECIPE, "--data", JSON.stringify({ outputs: {}, },),],
				2,
			);

			const metadata = await run<Record<string, unknown>>(["recipe", "metadata", SYNC_RECIPE,],);
			expect(typeof metadata,).toBe("object",);
			// DSS 15 recipe metadata GET/PUT round-trips tags/checklists/custom;
			// the documented `label` field is not returned by this endpoint.
			const tags = ["live-flow-jobs",];
			const set = await run<{ updated: string; }>([
				"recipe",
				"metadata-set",
				SYNC_RECIPE,
				"--data",
				JSON.stringify({ ...metadata, tags, },),
			],);
			expect(set.updated,).toBe(SYNC_RECIPE,);
			const after = await run<{ tags?: string[]; }>(["recipe", "metadata", SYNC_RECIPE,],);
			expect(after.tags,).toEqual(tags,);
		},);

		// --- Backup, detect drift, restore, and prove the payload round trip.
		await ctx.check("core.recipe.restore", [
			"recipe.set-payload",
			"recipe.assert-unchanged",
			"recipe.restore",
			"recipe.cat",
		], async () => {
			await ensureSlowRecipe();
			const original = await run<string>(["recipe", "cat", SLOW_RECIPE,],);
			expect(original,).toBe(SLOW_PYTHON_CODE,);
			const drifted = `${SLOW_PYTHON_CODE}# drift ${iter}\n`;
			const driftPath = await ctx.writeFile(`flow-jobs/${SLOW_RECIPE}-${iter}.drift.py`, drifted,);
			const backupDir = join(ctx.dir, "flow-jobs", `recipe-backups-${iter}`,);
			const set = await run<{ updated: string; backupCreated: boolean; backupPath?: string; }>([
				"recipe",
				"set-payload",
				SLOW_RECIPE,
				"--file",
				driftPath,
				"--backup-dir",
				backupDir,
			],);
			expect(set.backupCreated,).toBe(true,);
			const backupPath = set.backupPath;
			if (!backupPath || !backupPath.startsWith(backupDir,)) {
				throw new Error(`Backup was not written under ${backupDir}: ${String(backupPath,)}`,);
			}
			expect(await run<string>(["recipe", "cat", SLOW_RECIPE,],),).toBe(drifted,);

			// A drift finding is a synchronous assertion result: documented exit 4
			// with assertion_failed. On a non-zero exit the CLI prints the error
			// envelope, and the structured command result rides in details.result.
			const changed = await run<{
				code: string;
				details?: { result?: { unchanged: boolean; failures: Array<{ name: string; }>; }; };
			}>([
				"recipe",
				"assert-unchanged",
				SLOW_RECIPE,
				"--since",
				backupPath,
			], 4,);
			expect(changed.code,).toBe("assertion_failed",);
			expect(changed.details?.result?.unchanged,).toBe(false,);
			expect(changed.details?.result?.failures.map((failure,) => failure.name),).toEqual([
				"payload",
			],);

			const restored = await run<{ restored: string; payloadOnly: boolean; }>([
				"recipe",
				"restore",
				SLOW_RECIPE,
				"--backup",
				backupPath,
			],);
			expect(restored,).toMatchObject({ restored: SLOW_RECIPE, payloadOnly: false, },);
			expect(await run<string>(["recipe", "cat", SLOW_RECIPE,],),).toBe(SLOW_PYTHON_CODE,);
			const unchanged = await run<{ unchanged: boolean; failures: unknown[]; }>([
				"recipe",
				"assert-unchanged",
				SLOW_RECIPE,
				"--since",
				backupPath,
			],);
			expect(unchanged.unchanged,).toBe(true,);
			expect(unchanged.failures,).toEqual([],);
		},);

		// --- Declarative zone organization over the child flow. ---------------
		await ctx.check("core.flow-zone.organize", [
			"flow-zone.plan",
			"flow-zone.organize",
			"flow-zone.find",
			"flow-zone.get",
			"flow-zone.graph",
			"flow-zone.delete",
		], async () => {
			await ensureOrdersDataset();
			await ensureSyncRecipe();
			const before = await run<{ topologyFingerprint: string; zones: Array<{ name?: string; }>; }>(
				["flow-zone", "plan",],
			);
			expect(typeof before.topologyFingerprint,).toBe("string",);
			expect(before.zones.some((zone,) => zone.name === ZONE_NAME),).toBe(false,);

			const plan = {
				topologyFingerprint: before.topologyFingerprint,
				zones: [{
					name: ZONE_NAME,
					color: ZONE_COLOR,
					datasets: [ORDERS,],
					recipes: [SYNC_RECIPE,],
				},],
			};
			const dryRun = await run<{ dryRun: boolean; zoneCount: number; itemCount: number; }>([
				"flow-zone",
				"organize",
				"--data",
				JSON.stringify(plan,),
				"--validate-objects",
				"--dry-run",
			],);
			expect(dryRun,).toMatchObject({ dryRun: true, zoneCount: 1, itemCount: 2, },);

			const organized = await run<{
				organized: boolean;
				topologyUnchanged: boolean;
				created: Array<{ id: string; name: string; }>;
				moved: Array<{ zoneId: string; items: unknown[]; }>;
			}>(["flow-zone", "organize", "--data", JSON.stringify(plan,), "--validate-objects",],);
			expect(organized.organized,).toBe(true,);
			expect(organized.topologyUnchanged,).toBe(true,);
			expect(organized.created.map((zone,) => zone.name),).toEqual([ZONE_NAME,],);
			const zoneId = organized.created[0]!.id;
			expect(organized.moved,).toEqual([
				expect.objectContaining({
					zoneId,
					items: expect.arrayContaining([
						{ objectId: ORDERS, objectType: "DATASET", },
						{ objectId: SYNC_RECIPE, objectType: "RECIPE", },
					],),
				},),
			],);

			try {
				const byName = await run<
					Array<{ id: string; name: string; items: Array<{ objectId: string; }>; }>
				>([
					"flow-zone",
					"find",
					ZONE_NAME,
				],);
				expect(byName.map((zone,) => zone.id),).toEqual([zoneId,],);
				// recipe.clone inherits the source recipe's zone, so the zone may
				// hold more than the two planned objects; assert containment.
				const itemIds = byName[0]!.items.map((item,) => item.objectId);
				expect(itemIds,).toEqual(expect.arrayContaining([ORDERS, SYNC_RECIPE,],),);
				const byDataset = await run<Array<{ id: string; containsMatchingObject: boolean; }>>([
					"flow-zone",
					"find",
					"--dataset",
					ORDERS,
				],);
				expect(byDataset,).toEqual([
					expect.objectContaining({ id: zoneId, containsMatchingObject: true, },),
				],);
				const notInZone = await run<unknown[]>(["flow-zone", "find", "--dataset", SLOW_OUTPUT,],);
				expect(notInZone.some((zone,) => (zone as { id: string; }).id === zoneId),).toBe(false,);

				const zone = await run<{ id: string; name: string; color?: string; }>([
					"flow-zone",
					"get",
					zoneId,
				],);
				expect(zone,).toMatchObject({ id: zoneId, name: ZONE_NAME, color: ZONE_COLOR, },);

				const idempotent = await run<{ created: unknown[]; updated: unknown[]; moved: unknown[]; }>(
					["flow-zone", "organize", "--data", JSON.stringify(plan,),],
				);
				expect(idempotent,).toMatchObject({ created: [], updated: [], moved: [], },);

				const exported = await run<
					{ zones: Array<{ id?: string; name?: string; items: Array<{ objectId: string; }>; }>; }
				>(
					["flow-zone", "plan",],
				);
				const exportedZone = exported.zones.find((entry,) => entry.id === zoneId);
				expect(exportedZone?.name,).toBe(ZONE_NAME,);
				expect(exportedZone?.items.map((item,) => item.objectId) ?? [],).toEqual(
					expect.arrayContaining([ORDERS, SYNC_RECIPE,],),
				);

				const fullGraph = await run<Record<string, unknown>>(["flow-zone", "graph",],);
				expect(JSON.stringify(fullGraph,),).toContain(SYNC_RECIPE,);
				const zoneGraph = await run<Record<string, unknown>>(["flow-zone", "graph", zoneId,],);
				const zoneGraphText = JSON.stringify(zoneGraph,);
				expect(zoneGraphText,).toContain(`"${SYNC_RECIPE}"`,);
				// The clone inherits the source zone, so the clone recipe may appear;
				// the slow python recipe must not.
				expect(zoneGraphText,).not.toContain(`"${SLOW_RECIPE}"`,);
			} finally {
				const deleted = await run<{ deleted: string; }>(["flow-zone", "delete", zoneId,],);
				expect(deleted.deleted,).toBe(zoneId,);
			}
			expect(await run<unknown[]>(["flow-zone", "find", ZONE_NAME,],),).toEqual([],);
			// --if-exists on a missing zone is a skip result, not an error.
			const skipped = await run<{ skipped: string; reason: string; resource: string; }>([
				"flow-zone",
				"delete",
				zoneId,
				"--if-exists",
			],);
			expect(skipped,).toMatchObject({ skipped: zoneId, reason: "missing", resource: "flow-zone", },);
			// Zone deletion returns items to the default zone without touching topology.
			const after = await run<{ topologyFingerprint: string; }>(["flow-zone", "plan",],);
			expect(after.topologyFingerprint,).toBe(before.topologyFingerprint,);
		},);

		// --- Detached build followed by wait/watch/monitor over the same job.
		await ctx.check("core.job.lifecycle", [
			"job.build",
			"job.wait",
			"job.watch",
			"job.monitor",
			"job.get",
			"dataset.assert-count",
		], async () => {
			await ensureSyncRecipe();
			const started = await run<{ jobId: string; }>([
				"job",
				"build",
				SYNCED,
				"--build-mode",
				"RECURSIVE_FORCED_BUILD",
			],);
			expect(typeof started.jobId,).toBe("string",);
			const jobId = started.jobId;
			const waited = await run<JobWait>([
				"job",
				"wait",
				jobId,
				"--summary",
				"--timeout",
				String(JOB_TIMEOUT_MS,),
				"--poll-interval",
				String(POLL_MS,),
			],);
			expect(waited,).toMatchObject({ success: true, jobId, state: "DONE", },);
			// A finished job resolves immediately under watch and monitor.
			const watched = await run<JobWait & { timedOut?: boolean; }>([
				"job",
				"watch",
				jobId,
				"--timeout",
				String(JOB_TIMEOUT_MS,),
			],);
			expect(watched,).toMatchObject({ success: true, jobId, state: "DONE", },);
			const monitored = await run<JobWait>([
				"job",
				"monitor",
				jobId,
				"--summary",
				"--timeout",
				String(JOB_TIMEOUT_MS,),
			],);
			expect(monitored,).toMatchObject({ success: true, jobId, state: "DONE", },);
			expect(monitored.logSummary?.lineCount,).toBeGreaterThan(0,);

			const details = await run<{ baseStatus?: { state?: string; def?: { id?: string; }; }; }>([
				"job",
				"get",
				jobId,
			],);
			expect(details.baseStatus?.def?.id,).toBe(jobId,);
			expect(details.baseStatus?.state,).toBe("DONE",);
			const count = await run<{ satisfied: boolean; }>([
				"dataset",
				"assert-count",
				SYNCED,
				"--expected",
				String(ORDERS_ROWS,),
			],);
			expect(count.satisfied,).toBe(true,);
		},);

		// --- UI cat-activity-log URL resolves through the public log endpoint.
		await ctx.check(
			"core.job.log-url",
			["job.build-and-wait", "job.get", "job.log-url",],
			async () => {
				// Any finished job carries the activity ids this case needs; the fast
				// sync output avoids the 120s python recipe entirely (verified live:
				// job GET exposes activities at baseStatus.activities and the
				// activityIdsByRecipeName map, not at the top level).
				await ensureSyncRecipe();
				const build = await run<JobWait>([
					"job",
					"build-and-wait",
					SYNCED,
					"--build-mode",
					"NON_RECURSIVE_FORCED_BUILD",
					"--timeout",
					String(JOB_TIMEOUT_MS,),
					"--poll-interval",
					String(POLL_MS,),
				],);
				expect(build,).toMatchObject({ success: true, state: "DONE", },);
				const jobId = build.jobId;
				const details = await run<{
					activityIdsByRecipeName?: Record<string, string[]>;
					baseStatus?: { activities?: Record<string, { activityId?: string; }>; };
				}>(["job", "get", jobId,],);
				const activityId = Object.values(details.activityIdsByRecipeName ?? {},).flat()[0]
					?? Object.values(details.baseStatus?.activities ?? {},)[0]?.activityId;
				if (!activityId) {
					throw new Error(`Job ${jobId} exposes no activity ids to build a log URL from`,);
				}
				const url = `https://dss.local/dip/api/flow/jobs/cat-activity-log?projectKey=${
					encodeURIComponent(child!,)
				}&jobId=${encodeURIComponent(jobId,)}&activityId=${encodeURIComponent(activityId,)}&logId=main`;
				const log = await run<string>(["job", "log-url", url, "--max-lines", "50",],);
				expect(typeof log,).toBe("string",);
				expect(log.length,).toBeGreaterThan(0,);
				// A URL missing the required query parameters is a usage error:
				// documented exit 1 with the stable usage_error code.
				const rejected = await run<{ code?: string; }>(
					["job", "log-url", "https://dss.local/dip/api/flow/jobs/cat-activity-log?jobId=x",],
					1,
				);
				expect(rejected.code,).toBe("usage_error",);
			},
		);

		// --- Abort a running python job and observe the terminal state. -------
		await ctx.check("core.job.abort", ["job.build", "job.abort", "job.wait",], async () => {
			// The abort target only needs the slow recipe to exist; a prior build
			// is unnecessary work before the job we are about to abort.
			await ensureSlowRecipe();
			const started = await run<{ jobId: string; }>(["job", "build", SLOW_OUTPUT,],);
			const jobId = started.jobId;
			const aborted = await run<{ aborted: string; resource: string; }>(["job", "abort", jobId,],);
			expect(aborted,).toEqual({ aborted: jobId, resource: "job", },);
			// A non-DONE terminal state is a failed long-running result: documented
			// exit 4 with the wait result in the envelope's details.result.
			const waited = await run<{
				code: string;
				details?: { result?: JobWait; };
			}>([
				"job",
				"wait",
				jobId,
				"--timeout",
				String(JOB_TIMEOUT_MS,),
				"--poll-interval",
				String(POLL_MS,),
			], 4,);
			expect(waited.code,).toBe("long_running_failure",);
			expect(waited.details?.result?.jobId,).toBe(jobId,);
			expect(waited.details?.result?.success,).toBe(false,);
			expect(waited.details?.result?.state,).toBe("ABORTED",);
			// Aborting an already terminal job is still accepted by DSS.
			const again = await run<{ aborted: string; }>(["job", "abort", jobId,],);
			expect(again.aborted,).toBe(jobId,);
		},);
	} finally {
		if (child) await ctx.deleteProject(child,);
	}
}
