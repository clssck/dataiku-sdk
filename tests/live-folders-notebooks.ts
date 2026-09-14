import { expect, } from "bun:test";
import type {
	FolderDetails,
	FolderItem,
	FolderSummary,
	JupyterNotebookSummary,
	MacroDefinition,
	MacroState,
	MacroSummary,
	MacroWaitResult,
	NotebookSession,
	SqlNotebookContent,
	SqlNotebookHistory,
	SqlNotebookSummary,
} from "../src/schemas.js";
import { matchesLiveCase, } from "./live-cases.js";
import { LiveCapabilityError, type LiveContext, } from "./live-context.js";
import { withOwnedSqlConnection, } from "./live-infrastructure-disposable.js";
import { buildStoredZip, pluginJson, } from "./live-plugins.js";

/**
 * Folders / notebooks / code / macros live-suite module.
 *
 * Every case runs in the run phase against one module-owned disposable
 * project (created inside selected ctx.check callbacks, deleted in finally)
 * so wholly unselected modules create nothing and repeated iterations never
 * collide with the shared fixture project. Cases are split so that a missing
 * external dependency only blocks the step that genuinely needs it:
 *
 * - folder lifecycle uses the writable managed-folder storage DSS selects
 *   (the same path the core fixture folder was created through);
 * - Jupyter lifecycle and kernel-session commands need no external runtime;
 * - SQL notebook listing is runtime-independent, while the SQL notebook
 *   lifecycle needs a real SQL connection (DATAIKU_SQL_CONNECTION) because DSS
 *   rejects SQL notebooks bound to non-SQL connections; that is reported as an
 *   explicit per-case blocker;
 * - `code run` executes through the project's builtin Python environment;
 * - macro cases never execute a merely-discovered macro. They run either an
 *   explicitly configured macro (DATAIKU_LIVE_MACRO_ID, operator-audited) or a
 *   macro this suite audited by authoring it: a disposable plugin archive
 *   (plugin.json + python-runnables/<macro>/{runnable.json,runnable.py}, the
 *   documented macro component layout) whose single parameterless runnable
 *   only reads the owned project's own metadata and renders it as HTML. The
 *   runnable ships in the INITIAL archive so installation registers it — no
 *   post-install dev-file writes and no undocumented reload. The plugin is
 *   created through the guarded global path (reserve → install-from-zip → bind
 *   → delete) and is cleaned up in finally.
 */

const SQL_CONNECTION_ENV = "DATAIKU_SQL_CONNECTION";
/**
 * Operator override naming an explicitly audited runnableType to run instead
 * of the self-provisioned plugin macro. Never a discovered-macro shortcut:
 * the operator is responsible for its audit trail.
 */
const MACRO_ID_ENV = "DATAIKU_LIVE_MACRO_ID";
/** Directory name of the macro component inside the disposable plugin. */
const OWNED_MACRO_ID = "live_project_metadata";
/** runnable.json for the python-runnables component (documented fields only). */
const MACRO_RUNNABLE_JSON = JSON.stringify({
	meta: {
		label: "Live suite project metadata",
		description: "Reads only the owned project's own metadata.",
	},
	params: [],
	permissions: [],
	resultType: "HTML",
},);
/**
 * runnable.py implementing dataiku.runnables.Runnable exactly as the official
 * macro component docs specify. Reads only the project it is invoked in and
 * renders its metadata as HTML; no other project, no global object, no writes.
 */
const MACRO_RUNNABLE_PY = `import json

import dataiku
from dataiku.runnables import Runnable


class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key

    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        project = dataiku.api_client().get_project(self.project_key)
        payload = {
            "projectKey": self.project_key,
            "numDatasets": len(project.list_datasets()),
            "numRecipes": len(project.list_recipes()),
        }
        return "<html><body><pre>%s</pre></body></html>" % json.dumps(payload, sort_keys=True)
`;
const MACRO_WAIT_TIMEOUT_MS = 120_000;
const MACRO_POLL_INTERVAL_MS = 1_000;
const CODE_RUN_TIMEOUT_MS = 180_000;

function uniq(ctx: LiveContext, base: string,): string {
	return `${base}_i${String(ctx.iteration,)}`;
}

function blocked(message: string,): never {
	throw new LiveCapabilityError(message, "blocked",);
}

function requireString(value: unknown, label: string,): string {
	if (typeof value !== "string" || value.length === 0) {
		throw new Error(`Expected non-empty string for ${label}, got ${JSON.stringify(value,)}`,);
	}
	return value;
}

function jupyterContent(source: string, outputs: unknown[] = [],): Record<string, unknown> {
	return {
		nbformat: 4,
		nbformat_minor: 5,
		metadata: {
			kernelspec: { name: "python3", display_name: "Python 3", language: "python", },
			language_info: { name: "python", },
		},
		cells: [{
			cell_type: "code",
			execution_count: outputs.length ? 1 : null,
			metadata: {},
			source: [source,],
			outputs,
		},],
	};
}

// ---------------------------------------------------------------------------
// Folders
// ---------------------------------------------------------------------------

async function exerciseFolderLifecycle(ctx: LiveContext, projectKey: string,): Promise<void> {
	const name = uniq(ctx, "live_folder_lifecycle",);
	const tag = `live-i${String(ctx.iteration,)}`;
	const remotePath = `/lifecycle/${String(ctx.iteration,)}/marker.txt`;
	const marker = `folder lifecycle iteration ${String(ctx.iteration,)}\n`;
	const localPath = await ctx.writeFile(`folder-lifecycle-${String(ctx.iteration,)}.txt`, marker,);

	const before = await ctx.run<FolderSummary[]>(["folder", "list",], { projectKey, },);
	expect(before.some((folder,) => folder.name === name),).toBe(false,);

	const created = await ctx.run<FolderDetails & { created: string; }>([
		"folder",
		"create",
		"--name",
		name,
	], { projectKey, },);
	const folderId = requireString(created.created, "folder.create.created",);
	let deleted = false;
	try {
		const details = await ctx.run<FolderDetails>(["folder", "get", folderId,], { projectKey, },);
		expect(details.id,).toBe(folderId,);
		expect(details.name,).toBe(name,);
		expect(requireString(details.params?.connection, "folder.params.connection",),).toBeTruthy();

		// Name-based resolution must reach the same folder as the id.
		const byName = await ctx.run<FolderDetails>(["folder", "get", name,], { projectKey, },);
		expect(byName.id,).toBe(folderId,);

		const updated = await ctx.run<{ updated: string; }>([
			"folder",
			"update",
			folderId,
			"--data",
			JSON.stringify({ tags: [tag,], },),
		], { projectKey, },);
		expect(updated.updated,).toBe(folderId,);
		const afterUpdate = await ctx.run<FolderDetails>(["folder", "get", folderId,], {
			projectKey,
		},);
		expect(afterUpdate.tags,).toContain(tag,);
		expect(afterUpdate.params?.connection,).toBe(details.params?.connection,);

		await ctx.run(["folder", "upload", folderId, remotePath, localPath,], { projectKey, },);
		const contents = await ctx.run<FolderItem[]>(["folder", "contents", folderId,], {
			projectKey,
		},);
		const item = contents.find((entry,) => entry.path === remotePath);
		expect(item?.size,).toBe(Buffer.byteLength(marker,),);

		await ctx.run(["folder", "delete-file", folderId, remotePath,], { projectKey, },);
		const emptied = await ctx.run<FolderItem[]>(["folder", "contents", folderId,], {
			projectKey,
		},);
		expect(emptied.some((entry,) => entry.path === remotePath),).toBe(false,);

		const removal = await ctx.run<{ deleted: string; }>(["folder", "delete", folderId,], {
			projectKey,
		},);
		deleted = true;
		expect(removal.deleted,).toBe(folderId,);
		const after = await ctx.run<FolderSummary[]>(["folder", "list",], { projectKey, },);
		expect(after.some((folder,) => folder.id === folderId),).toBe(false,);
	} finally {
		if (!deleted) {
			await ctx.run(["folder", "delete", folderId, "--if-exists",], { projectKey, },);
		}
	}
}

// ---------------------------------------------------------------------------
// Jupyter notebooks
// ---------------------------------------------------------------------------

async function exerciseJupyterLifecycle(ctx: LiveContext, projectKey: string,): Promise<void> {
	const name = uniq(ctx, "live_jupyter_lifecycle",);
	const outputs = [{
		output_type: "stream",
		name: "stdout",
		text: ["stale output\n",],
	},];
	const created = await ctx.run<{ saved: string; created: boolean; hash: string; }>([
		"notebook",
		"save-jupyter",
		name,
		"--data",
		JSON.stringify(jupyterContent("print('live jupyter lifecycle')\n", outputs,),),
	], { projectKey, },);
	expect(created.created,).toBe(true,);
	expect(created.hash,).toMatch(/^[0-9a-f]{64}$/,);
	try {
		const list = await ctx.run<JupyterNotebookSummary[]>(["notebook", "list-jupyter",], {
			projectKey,
		},);
		expect(list.some((notebook,) => notebook.name === name),).toBe(true,);

		const withOutputs = await ctx.run<
			{ cells: Array<{ outputs?: unknown[]; source: string | string[]; }>; }
		>(["notebook", "get-jupyter", name,], { projectKey, },);
		expect(withOutputs.cells,).toHaveLength(1,);
		expect(withOutputs.cells[0]?.outputs,).toHaveLength(1,);

		// A stale --expect-hash must be rejected before any write.
		await ctx.run([
			"notebook",
			"save-jupyter",
			name,
			"--data",
			JSON.stringify(jupyterContent("print('never written')\n",),),
			"--expect-hash",
			"0".repeat(64,),
		], { projectKey, expectedExit: 1, },);
		const untouched = await ctx.run<{ cells: Array<{ source: string | string[]; }>; }>([
			"notebook",
			"get-jupyter",
			name,
		], { projectKey, },);
		expect(String(untouched.cells[0]?.source,),).toContain("live jupyter lifecycle",);

		const updated = await ctx.run<{ created: boolean; hash: string; }>([
			"notebook",
			"save-jupyter",
			name,
			"--data",
			JSON.stringify(jupyterContent("print('live jupyter lifecycle v2')\n", outputs,),),
			"--expect-hash",
			created.hash,
		], { projectKey, },);
		expect(updated.created,).toBe(false,);
		expect(updated.hash,).not.toBe(created.hash,);

		const cleared = await ctx.run<{ cleared: string; }>([
			"notebook",
			"clear-jupyter-outputs",
			name,
		], { projectKey, },);
		expect(cleared.cleared,).toBe(name,);
		const withoutOutputs = await ctx.run<
			{ cells: Array<{ outputs?: unknown[]; source: string | string[]; }>; }
		>(["notebook", "get-jupyter", name,], { projectKey, },);
		expect(withoutOutputs.cells[0]?.outputs ?? [],).toHaveLength(0,);
		expect(String(withoutOutputs.cells[0]?.source,),).toContain("v2",);
	} finally {
		const removal = await ctx.run<{ deleted?: string; skipped?: string; }>([
			"notebook",
			"delete-jupyter",
			name,
			"--if-exists",
		], { projectKey, },);
		expect(removal.deleted,).toBe(name,);
	}
	const afterDelete = await ctx.run<JupyterNotebookSummary[]>(["notebook", "list-jupyter",], {
		projectKey,
	},);
	expect(afterDelete.some((notebook,) => notebook.name === name),).toBe(false,);
	const missing = await ctx.run<
		{ deleted?: string; skipped?: string; reason?: string; }
	>([
		"notebook",
		"delete-jupyter",
		name,
		"--if-exists",
	], { projectKey, },);
	expect(missing.deleted,).toBeUndefined();
}

/**
 * Kernel sessions: the public API cannot start a kernel, so the session
 * surface is demonstrated against a notebook with no live kernel — an empty
 * session list, `--active true` excluding it, and `unload-jupyter --all`
 * reporting nothing to unload. A stray session left by an interactive user
 * would be surfaced (and unloaded) rather than hidden.
 */
async function exerciseJupyterSessions(ctx: LiveContext, projectKey: string,): Promise<void> {
	const name = uniq(ctx, "live_jupyter_sessions",);
	await ctx.run([
		"notebook",
		"save-jupyter",
		name,
		"--data",
		JSON.stringify(jupyterContent("print('live jupyter sessions')\n",),),
	], { projectKey, },);
	try {
		const sessions = await ctx.run<NotebookSession[]>(["notebook", "sessions-jupyter", name,], {
			projectKey,
		},);
		expect(Array.isArray(sessions,),).toBe(true,);
		for (const session of sessions) expect(session.sessionId,).toBeTruthy();

		const active = await ctx.run<JupyterNotebookSummary[]>([
			"notebook",
			"list-jupyter",
			"--active",
			"true",
		], { projectKey, },);
		expect(active.some((notebook,) => notebook.name === name),).toBe(sessions.length > 0,);
		const inactive = await ctx.run<JupyterNotebookSummary[]>([
			"notebook",
			"list-jupyter",
			"--active",
			"false",
		], { projectKey, },);
		expect(inactive.some((notebook,) => notebook.name === name),).toBe(sessions.length === 0,);

		for (const session of sessions) {
			const unloaded = await ctx.run<{ unloaded: string; sessionId: string; }>([
				"notebook",
				"unload-jupyter",
				name,
				session.sessionId,
			], { projectKey, },);
			expect(unloaded.sessionId,).toBe(session.sessionId,);
		}
		const all = await ctx.run<{ unloaded: unknown[]; all: boolean; }>([
			"notebook",
			"unload-jupyter",
			"--all",
		], { projectKey, },);
		expect(all.all,).toBe(true,);
		expect(Array.isArray(all.unloaded,),).toBe(true,);
		const remaining = await ctx.run<NotebookSession[]>([
			"notebook",
			"sessions-jupyter",
			name,
		], { projectKey, },);
		expect(remaining,).toHaveLength(0,);
	} finally {
		await ctx.run(["notebook", "delete-jupyter", name, "--if-exists",], { projectKey, },);
	}
}

// ---------------------------------------------------------------------------
// SQL notebooks
// ---------------------------------------------------------------------------

async function exerciseSqlNotebookList(ctx: LiveContext, projectKey: string,): Promise<void> {
	const list = await ctx.run<SqlNotebookSummary[]>(["notebook", "list-sql",], { projectKey, },);
	expect(Array.isArray(list,),).toBe(true,);
	for (const notebook of list) {
		expect(notebook.id,).toBeTruthy();
		expect(notebook.connection,).toBeTruthy();
	}
	// A fresh module-owned project has no SQL notebooks yet.
	expect(list,).toHaveLength(0,);
}

/**
 * SQL notebooks need a SQL-type connection. Precedence: the explicitly
 * configured DATAIKU_SQL_CONNECTION, else the shared disposable in-memory
 * SQLite JDBC connection (tests/live-infrastructure-disposable.ts owns the
 * canonical create/bind/delete). A 403 on connection create surfaces as a
 * blocked case (required: false), never as a fake success.
 */
async function exerciseSqlNotebookLifecycle(ctx: LiveContext, projectKey: string,): Promise<void> {
	const configured = process.env[SQL_CONNECTION_ENV]?.trim();
	if (configured) {
		await runSqlNotebookLifecycle(ctx, projectKey, configured,);
		return;
	}
	// The helper deletes its connection when the body resolves, so the entire
	// notebook lifecycle must run INSIDE the body — returning the name alone
	// would delete the connection before save-sql references it.
	await withOwnedSqlConnection(
		ctx,
		"sqlnb",
		(connection,) => runSqlNotebookLifecycle(ctx, projectKey, connection,),
	);
}

async function runSqlNotebookLifecycle(
	ctx: LiveContext,
	projectKey: string,
	connection: string,
): Promise<void> {
	const requested = uniq(ctx, "live_sql_lifecycle",);
	const cellId = "live_cell_1";
	// DSS enforces a JSON string for SQL cell code: an array fails to parse
	// server-side (Expected a string but was BEGIN_ARRAY at $.cells[0].code).
	const firstCellCode = "SELECT 1 AS one";
	const secondCellCode = "SELECT 2 AS two";
	const content: SqlNotebookContent = {
		connection,
		cells: [{ id: cellId, type: "QUERY", name: "probe", code: firstCellCode, },],
	};
	const created = await ctx.run<
		{ saved: string; requested: string; created: boolean; hash: string; }
	>([
		"notebook",
		"save-sql",
		requested,
		"--data",
		JSON.stringify(content,),
	], { projectKey, },);
	expect(created.created,).toBe(true,);
	expect(created.requested,).toBe(requested,);
	// DSS allocates the notebook id server-side; only the receipt id is usable
	// for every later read/update/history/delete call.
	const sqlId = requireString(created.saved, "save-sql.saved",);
	expect(sqlId,).not.toBe(requested,);
	try {
		const list = await ctx.run<SqlNotebookSummary[]>(["notebook", "list-sql",], { projectKey, },);
		const summary = list.find((notebook,) => notebook.id === sqlId);
		expect(summary?.connection,).toBe(connection,);

		const fetched = await ctx.run<SqlNotebookContent>(["notebook", "get-sql", sqlId,], {
			projectKey,
		},);
		expect(fetched.connection,).toBe(connection,);
		expect(fetched.cells.map(cell => cell.code),).toEqual([firstCellCode,],);

		// Update payload stays content-only (connection + cells): server receipt
		// fields are never echoed back into a write.
		const updated = await ctx.run<{ created: boolean; hash: string; }>([
			"notebook",
			"save-sql",
			sqlId,
			"--data",
			JSON.stringify({
				connection: fetched.connection,
				cells: [...fetched.cells, {
					id: "live_cell_2",
					type: "QUERY",
					name: "probe 2",
					code: secondCellCode,
				},],
			},),
			"--expect-hash",
			created.hash,
		], { projectKey, },);
		expect(updated.created,).toBe(false,);
		const refetched = await ctx.run<SqlNotebookContent>(["notebook", "get-sql", sqlId,], {
			projectKey,
		},);
		expect(refetched.cells,).toHaveLength(2,);
		expect(refetched.cells.map(cell => cell.code),).toEqual([firstCellCode, secondCellCode,],);

		// No public endpoint executes SQL notebook cells, so the history surface
		// is exercised on an unexecuted notebook: readable, then clearable.
		const history = await ctx.run<SqlNotebookHistory>(["notebook", "history-sql", sqlId,], {
			projectKey,
		},);
		for (const runs of Object.values(history,)) expect(Array.isArray(runs,),).toBe(true,);
		const cleared = await ctx.run<{ cleared: string; }>([
			"notebook",
			"clear-sql-history",
			sqlId,
			"--cell-id",
			cellId,
			"--retain",
			"0",
		], { projectKey, },);
		expect(cleared.cleared,).toBe(sqlId,);
		const afterClear = await ctx.run<SqlNotebookHistory>(["notebook", "history-sql", sqlId,], {
			projectKey,
		},);
		expect(afterClear[cellId] ?? [],).toHaveLength(0,);
	} finally {
		const removal = await ctx.run<{ deleted?: string; }>([
			"notebook",
			"delete-sql",
			sqlId,
			"--if-exists",
		], { projectKey, },);
		expect(removal.deleted,).toBe(sqlId,);
	}
	const afterDelete = await ctx.run<SqlNotebookSummary[]>(["notebook", "list-sql",], {
		projectKey,
	},);
	expect(afterDelete.some((notebook,) => notebook.id === sqlId),).toBe(false,);
}

// ---------------------------------------------------------------------------
// code run
// ---------------------------------------------------------------------------

async function exerciseCodeRun(ctx: LiveContext, projectKey: string,): Promise<void> {
	const marker = `LIVE_CODE_RUN_${String(ctx.iteration,)}_${projectKey}`;
	const script = [
		"import dataiku",
		"project = dataiku.api_client().get_default_project()",
		`print("${marker}", project.project_key)`,
		"",
	].join("\n",);
	const file = await ctx.writeFile(`code-run-${String(ctx.iteration,)}.py`, script,);
	const run = await ctx.run<{
		outcome: string;
		success: boolean;
		runId: string;
		output: string;
		cleanup: { status: string; };
	}>([
		"code",
		"run",
		"--file",
		file,
		"--timeout",
		String(CODE_RUN_TIMEOUT_MS,),
	], { projectKey, },);
	expect(run.outcome,).toBe("SUCCESS",);
	expect(run.success,).toBe(true,);
	expect(run.runId,).toBeTruthy();
	expect(run.output,).toContain(`${marker} ${projectKey}`,);
	// The throwaway scenario must be gone: code run leaves no scenario behind.
	expect(run.cleanup.status,).toBe("deleted",);
	const scenarios = await ctx.run<Array<{ id: string; }>>(["scenario", "list",], { projectKey, },);
	expect(scenarios,).toHaveLength(0,);
}

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

function macroLabel(macro: MacroSummary | MacroDefinition,): string {
	return macro.meta?.label ?? macro.runnableType ?? "";
}

function hasRequiredParams(definition: MacroDefinition,): boolean {
	return (definition.params ?? []).some((param,) => {
		const record = param as Record<string, unknown>;
		return record["mandatory"] === true && record["defaultValue"] === undefined;
	},);
}
/**
 * Discovery is read-only: it never selects anything for execution. The
 * inventory (and per-macro definitions) are the case's observable evidence.
 */
async function exerciseMacroDiscovery(ctx: LiveContext, projectKey: string,): Promise<void> {
	const macros = await ctx.run<MacroSummary[]>(["macro", "list",], { projectKey, },);
	expect(Array.isArray(macros,),).toBe(true,);
	process.stdout.write(
		`${
			JSON.stringify({
				macroInventory: macros.map(macro => macro.runnableType ?? macroLabel(macro,)),
			},)
		}\n`,
	);
	for (const summary of macros.slice(0, 5,)) {
		const runnableType = requireString(summary.runnableType, "macro.runnableType",);
		const definition = await ctx.run<MacroDefinition>(["macro", "get", runnableType,], {
			projectKey,
		},);
		expect(definition.runnableType,).toBe(runnableType,);
	}
}

/** Resolution outcome: a runnable to execute, and/or the bound plugin receipt. */
interface ResolvedMacro {
	runnableType?: string;
	provisionedPlugin?: string;
	problem?: string;
}

/**
 * The only macro the execution cases may run: the explicitly audited override,
 * or the disposable plugin macro this suite authored. Once the plugin is bound
 * the receipt is always returned — even when the registration lookup fails —
 * so teardown still deletes it. Pre-bind failures throw (unconfirmed ledger
 * entries belong to the context-owned recovery, never to this module).
 */
async function resolveOwnedMacro(ctx: LiveContext, projectKey: string,): Promise<ResolvedMacro> {
	const override = process.env[MACRO_ID_ENV]?.trim();
	if (override) {
		const definition = await ctx.run<MacroDefinition>(["macro", "get", override,], {
			projectKey,
		},);
		if (hasRequiredParams(definition,)) {
			blocked(
				`${MACRO_ID_ENV}=${override} declares mandatory parameters without defaults; the suite cannot fill them safely.`,
			);
		}
		return { runnableType: override, };
	}
	// Install the complete component in one archive: installPluginFromZip
	// reserve → archive validation → install-from-zip → marker bind. The
	// runnable definition ships inside the archive, so registration happens at
	// install time and no post-install dev-file write (or reload) is needed.
	const entry = await ctx.installPluginFromZip(
		"macroplugin",
		(name, marker,) => writeMacroPluginArchive(ctx, name, marker,),
	);
	const pluginId = requireString(entry.id, "owned plugin id",);
	ctx.fixtures.pluginId = pluginId;
	// Resolve the server's own runnableType through the unique owner plugin id
	// (live identifier is pyrunnable_<pluginId>_<macroDir>, never predicted).
	const macros = await ctx.run<MacroSummary[]>(["macro", "list",], { projectKey, },);
	const owned = macros.find(macro => macro.ownerPluginId === pluginId);
	if (!owned?.runnableType) {
		return {
			provisionedPlugin: pluginId,
			problem: `No runnable of the installed plugin ${pluginId} is registered; macro list shows: ${
				macros.map(macro => `${macro.runnableType ?? "?"} [${macroLabel(macro,)}]`).join(", ",)
				|| "none"
			}`,
		};
	}
	return { runnableType: owned.runnableType, provisionedPlugin: pluginId, };
}

/**
 * The disposable plugin archive: plugin.json plus the documented python
 * runnable component (python-runnables/<id>/{runnable.json,runnable.py}),
 * built with the canonical stored-zip writer so the archive is valid for
 * installPluginFromZip's inspector.
 */
async function writeMacroPluginArchive(
	ctx: LiveContext,
	name: string,
	marker: string,
): Promise<string> {
	const encoder = new TextEncoder();
	const bytes = buildStoredZip([
		{ name: `${name}/plugin.json`, data: encoder.encode(pluginJson(name, marker,),), },
		{
			name: `${name}/python-runnables/${OWNED_MACRO_ID}/runnable.json`,
			data: encoder.encode(MACRO_RUNNABLE_JSON,),
		},
		{
			name: `${name}/python-runnables/${OWNED_MACRO_ID}/runnable.py`,
			data: encoder.encode(MACRO_RUNNABLE_PY,),
		},
	],);
	return ctx.writeFile(`plugin-${name}-macro.zip`, bytes,);
}

async function pollMacroUntilDone(
	ctx: LiveContext,
	projectKey: string,
	runnableType: string,
	runId: string,
): Promise<MacroState> {
	const deadline = Date.now() + MACRO_WAIT_TIMEOUT_MS;
	while (true) {
		const state = await ctx.run<MacroState>(["macro", "state", runnableType, runId,], {
			projectKey,
		},);
		if (state.running !== true) return state;
		if (Date.now() >= deadline) {
			throw new Error(
				`Macro ${runnableType} run ${runId} still running after ${String(MACRO_WAIT_TIMEOUT_MS,)}ms`,
			);
		}
		await Bun.sleep(MACRO_POLL_INTERVAL_MS,);
	}
}

async function exerciseMacroRun(
	ctx: LiveContext,
	projectKey: string,
	runnableType: string,
): Promise<void> {
	const started = await ctx.run<{ runId: string; }>(["macro", "run", runnableType,], {
		projectKey,
	},);
	const runId = requireString(started.runId, "macro.run.runId",);
	const state = await pollMacroUntilDone(ctx, projectKey, runnableType, runId,);
	expect(state.exists,).not.toBe(false,);
	if (state.resultError !== undefined || state.storedError !== undefined) {
		throw new Error(
			`Macro ${runnableType} run ${runId} failed: ${
				JSON.stringify(state.resultError ?? state.storedError,).slice(0, 1500,)
			}`,
		);
	}
	const result = await ctx.run<unknown>(["macro", "result", runnableType, runId,], {
		projectKey,
	},);
	expect(result,).toBeDefined();

	const waited = await ctx.run<MacroWaitResult>([
		"macro",
		"run-and-wait",
		runnableType,
		"--timeout",
		String(MACRO_WAIT_TIMEOUT_MS,),
		"--poll-interval",
		String(MACRO_POLL_INTERVAL_MS,),
	], { projectKey, },);
	expect(waited.runnableType,).toBe(runnableType,);
	expect(waited.runId,).not.toBe(runId,);
	expect(waited.running,).toBe(false,);
	expect(waited.success,).toBe(true,);
	expect(waited.timedOut,).not.toBe(true,);
}

/**
 * Abort a run this case started itself. Built-in macros finish in
 * milliseconds, so the abort usually lands on an already-finished run: DSS
 * accepts it, and the terminal state afterwards must not report the run as
 * still running. Whether the abort pre-empted execution is recorded in the
 * result, never asserted.
 */
async function exerciseMacroAbort(
	ctx: LiveContext,
	projectKey: string,
	runnableType: string,
): Promise<void> {
	const started = await ctx.run<{ runId: string; }>(["macro", "run", runnableType,], {
		projectKey,
	},);
	const runId = requireString(started.runId, "macro.run.runId",);
	const aborted = await ctx.run<{ aborted: string; runId: string; }>([
		"macro",
		"abort",
		runnableType,
		runId,
	], { projectKey, },);
	expect(aborted.aborted,).toBe(runId,);
	const state = await pollMacroUntilDone(ctx, projectKey, runnableType, runId,);
	expect(state.running,).not.toBe(true,);
	process.stdout.write(
		`${
			JSON.stringify({
				macroAbort: runnableType,
				runId,
				preempted: state.resultError !== undefined || state.storedError !== undefined,
			},)
		}\n`,
	);
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

export async function exerciseFoldersNotebooks(ctx: LiveContext,): Promise<void> {
	// Selection invariant: a wholly unselected module must not create anything.
	// The module project and plugin are only materialized for selected ids.
	const SELECTED_IDS = [
		"core.folder.lifecycle",
		"core.notebook.jupyter-lifecycle",
		"core.notebook.jupyter-sessions",
		"core.notebook.sql-list",
		"core.notebook.sql-lifecycle",
		"core.code.run",
		"core.macro.discovery",
		"core.macro.run",
		"core.macro.abort",
	] as const;
	// Empty selection means every case runs (full-suite mode); a non-empty
	// selection restricts the module to ids matching a selector.
	const selected = ctx.selection.length === 0
		? [...SELECTED_IDS,]
		: SELECTED_IDS.filter(id => ctx.selection.some(selection => matchesLiveCase(id, selection,)));
	if (selected.length === 0) return;

	const projectKey = await ctx.createProject("fldnb",);
	try {
		if (selected.includes("core.folder.lifecycle",)) {
			await ctx.check("core.folder.lifecycle", [
				"folder.list",
				"folder.create",
				"folder.get",
				"folder.update",
				"folder.upload",
				"folder.contents",
				"folder.delete-file",
				"folder.delete",
			], () => exerciseFolderLifecycle(ctx, projectKey,),);
		}

		if (selected.includes("core.notebook.jupyter-lifecycle",)) {
			await ctx.check("core.notebook.jupyter-lifecycle", [
				"notebook.save-jupyter",
				"notebook.list-jupyter",
				"notebook.get-jupyter",
				"notebook.clear-jupyter-outputs",
				"notebook.delete-jupyter",
			], () => exerciseJupyterLifecycle(ctx, projectKey,),);
		}

		if (selected.includes("core.notebook.jupyter-sessions",)) {
			await ctx.check("core.notebook.jupyter-sessions", [
				"notebook.save-jupyter",
				"notebook.sessions-jupyter",
				"notebook.list-jupyter",
				"notebook.unload-jupyter",
				"notebook.delete-jupyter",
			], () => exerciseJupyterSessions(ctx, projectKey,),);
		}

		if (selected.includes("core.notebook.sql-list",)) {
			await ctx.check(
				"core.notebook.sql-list",
				["notebook.list-sql",],
				() => exerciseSqlNotebookList(ctx, projectKey,),
			);
		}

		if (selected.includes("core.notebook.sql-lifecycle",)) {
			await ctx.check(
				"core.notebook.sql-lifecycle",
				[
					"notebook.save-sql",
					"notebook.list-sql",
					"notebook.get-sql",
					"notebook.history-sql",
					"notebook.clear-sql-history",
					"notebook.delete-sql",
				],
				() => exerciseSqlNotebookLifecycle(ctx, projectKey,),
				{
					capability: "notebook.sql-connection",
					required: false,
				},
			);
		}

		if (selected.includes("core.code.run",)) {
			await ctx.check(
				"core.code.run",
				["code.run", "scenario.list",],
				() => exerciseCodeRun(ctx, projectKey,),
			);
		}

		if (selected.includes("core.macro.discovery",)) {
			await ctx.check(
				"core.macro.discovery",
				["macro.list", "macro.get",],
				() => exerciseMacroDiscovery(ctx, projectKey,),
			);
		}
		const wantsMacroRun = selected.includes("core.macro.run",);
		const wantsMacroAbort = selected.includes("core.macro.abort",);
		if (wantsMacroRun || wantsMacroAbort) {
			const runActions = ["macro.run", "macro.state", "macro.result", "macro.run-and-wait",];
			const abortActions = ["macro.run", "macro.abort", "macro.state",];
			// Declarations mirror the case that actually performs the step: the
			// first selected case provisions the owned plugin (install + the
			// registration list), the last one deletes it. The env-override path
			// performs neither and declares neither.
			if (!process.env[MACRO_ID_ENV]?.trim()) {
				(wantsMacroRun ? runActions : abortActions).push(
					"plugin.install-from-zip",
					"macro.list",
				);
				(wantsMacroAbort ? abortActions : runActions).push("plugin.delete",);
			}
			let macro: ResolvedMacro | undefined;
			let macroFailure: unknown;
			// Provision and dispose inside the selected check callbacks only: a
			// provisioning or teardown failure is recorded on the enclosing case
			// (failed) while the remaining matrix continues. The unconfirmed
			// ledger entry, if any, is left for the context-owned cleanup —
			// never deleted from here.
			const ensureMacro = async (): Promise<ResolvedMacro> => {
				if (macro) return macro;
				if (macroFailure !== undefined) throw macroFailure;
				try {
					macro = await resolveOwnedMacro(ctx, projectKey,);
					return macro;
				} catch (error) {
					macroFailure = error;
					throw error;
				}
			};
			// The registration lookup can fail after the plugin is bound; the
			// receipt is still in `macro`, so disposal must run regardless.
			const requireRunnable = (resolved: ResolvedMacro,): string => {
				if (resolved.runnableType === undefined) {
					blocked(resolved.problem ?? "The audited macro was not registered.",);
				}
				return resolved.runnableType;
			};
			// Deletion targets only the bound plugin this module created; a
			// failure here is a case failure, never a silent pass.
			const disposeMacro = async (): Promise<void> => {
				if (!macro?.provisionedPlugin) return;
				await ctx.deleteGlobal("plugin", macro.provisionedPlugin,);
				if (ctx.fixtures.pluginId === macro.provisionedPlugin) delete ctx.fixtures.pluginId;
			};
			// Body and teardown are both surfaced: teardown still runs when the
			// body threw, and a teardown failure fails the case either way.
			const withMacroTeardown = async (body: () => Promise<void>,): Promise<void> => {
				const errors: unknown[] = [];
				try {
					await body();
				} catch (error) {
					errors.push(error,);
				}
				try {
					await disposeMacro();
				} catch (error) {
					errors.push(error,);
				}
				if (errors.length === 1) throw errors[0];
				if (errors.length) {
					throw new AggregateError(
						errors,
						errors.map(e => e instanceof Error ? e.message : String(e,)).join("; ",),
					);
				}
			};
			if (wantsMacroRun) {
				// When both cases run, disposal belongs to the last one (abort).
				await ctx.check("core.macro.run", runActions, async () => {
					const body = async () => {
						const resolved = await ensureMacro();
						await exerciseMacroRun(ctx, projectKey, requireRunnable(resolved,),);
					};
					if (wantsMacroAbort) await body();
					else await withMacroTeardown(body,);
				}, { capability: "macro.audited-source", required: false, },);
			}
			if (wantsMacroAbort) {
				await ctx.check(
					"core.macro.abort",
					abortActions,
					() =>
						withMacroTeardown(async () => {
							const resolved = await ensureMacro();
							await exerciseMacroAbort(ctx, projectKey, requireRunnable(resolved,),);
						},),
					{ capability: "macro.audited-source", required: false, },
				);
			}
		}
	} finally {
		await ctx.deleteProject(projectKey,);
	}
}
