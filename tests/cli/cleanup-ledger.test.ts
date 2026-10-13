import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { commands, } from "../../src/cli/commands/index.js";
import {
	buildCommandRegistry,
	isAllowedCleanupAction,
	supportsCleanupLedger,
} from "../../src/cli/contract.js";
import { cleanupLedgerEntry, } from "../../src/cli/helpers/cleanup.js";

interface LedgerCase {
	name: string;
	resource: string;
	action: string;
	args: string[];
	flags: Record<string, string | boolean>;
	result: Record<string, unknown>;
	projectKey: string | undefined;
	/** Exact cleanup argv the advertised create must yield. */
	argv: string[];
	entry: Record<string, unknown>;
}

describe("cleanup ledger entries for the advertised create commands", () => {
	const cases: LedgerCase[] = [
		{
			name: "streaming-endpoint.create targets its exact delete argv",
			resource: "streaming-endpoint",
			action: "create",
			args: ["my-stream", "kafka",],
			flags: { "project-key": "PROJ", },
			result: { id: "my-stream", },
			projectKey: undefined,
			argv: ["streaming-endpoint", "delete", "my-stream", "--project-key", "PROJ",],
			entry: {
				resource: "streaming-endpoint",
				action: "create",
				id: "my-stream",
				projectKey: "PROJ",
			},
		},
		{
			name: "meaning.create targets its exact delete argv with --if-exists",
			resource: "meaning",
			action: "create",
			args: ["vip", "VIP", "VALUES_LIST",],
			flags: {},
			result: { id: "vip", },
			projectKey: undefined,
			argv: ["meaning", "delete", "vip", "--if-exists",],
			entry: { resource: "meaning", action: "create", id: "vip", name: "VIP", },
		},
		{
			name: "workspace.create resolves workspaceKey from --data",
			resource: "workspace",
			action: "create",
			args: [],
			flags: { data: `{"workspaceKey":"MY_WS","displayName":"My Workspace"}`, },
			result: { workspaceKey: "MY_WS", },
			projectKey: undefined,
			argv: ["workspace", "delete", "MY_WS",],
			entry: { resource: "workspace", action: "create", id: "MY_WS", name: "My Workspace", },
		},
		{
			name: "project-library.create-file targets its exact delete argv",
			resource: "project-library",
			action: "create-file",
			args: ["lib/utils.py",],
			flags: { "project-key": "PROJ", },
			result: { created: "lib/utils.py", },
			projectKey: undefined,
			argv: ["project-library", "delete", "lib/utils.py", "--project-key", "PROJ",],
			entry: {
				resource: "project-library",
				action: "create-file",
				path: "lib/utils.py",
				projectKey: "PROJ",
			},
		},
		{
			name: "project-library.create-folder targets its exact delete argv",
			resource: "project-library",
			action: "create-folder",
			args: ["lib/helpers",],
			flags: {},
			result: { created: "lib/helpers", },
			projectKey: "FALLBACK",
			argv: ["project-library", "delete", "lib/helpers", "--project-key", "FALLBACK",],
			entry: {
				resource: "project-library",
				action: "create-folder",
				path: "lib/helpers",
				projectKey: "FALLBACK",
			},
		},
		{
			name: "notebook.save-jupyter records a delete only when the save created the notebook",
			resource: "notebook",
			action: "save-jupyter",
			args: ["scratch_notebook",],
			flags: { "project-key": "PROJ", },
			result: { saved: "scratch_notebook", resource: "jupyter-notebook", created: true, },
			projectKey: undefined,
			argv: [
				"notebook",
				"delete-jupyter",
				"scratch_notebook",
				"--if-exists",
				"--project-key",
				"PROJ",
			],
			entry: {
				resource: "notebook",
				action: "save-jupyter",
				name: "scratch_notebook",
				projectKey: "PROJ",
			},
		},
		{
			name: "notebook.save-sql deletes the server-allocated id from the create receipt",
			resource: "notebook",
			action: "save-sql",
			args: ["scratch_sql",],
			flags: {},
			result: { saved: "0DTZujd", requested: "scratch_sql", resource: "sql-notebook", created: true, },
			projectKey: "FALLBACK",
			argv: ["notebook", "delete-sql", "0DTZujd", "--if-exists", "--project-key", "FALLBACK",],
			entry: {
				resource: "notebook",
				action: "save-sql",
				id: "0DTZujd",
				projectKey: "FALLBACK",
			},
		},
		{
			name: "dataset.create-managed deletes the created dataset in the project DSS landed it in",
			resource: "dataset",
			action: "create-managed",
			args: [],
			flags: { name: "orders_copy", "project-key": "REQUESTED", },
			result: {
				created: "orders_copy",
				resource: "dataset",
				managed: true,
				projectKey: "PROJ",
				creationSettings: { connectionId: "filesystem_managed", },
			},
			projectKey: undefined,
			argv: ["dataset", "delete", "orders_copy", "--if-exists", "--project-key", "PROJ",],
			entry: {
				resource: "dataset",
				action: "create-managed",
				name: "orders_copy",
				projectKey: "PROJ",
			},
		},
		{
			name: "dataset.create-managed falls back to the ambient project key",
			resource: "dataset",
			action: "create-managed",
			args: [],
			flags: { name: "orders_copy", },
			result: { created: "orders_copy", resource: "dataset", managed: true, },
			projectKey: "AMBIENT",
			argv: ["dataset", "delete", "orders_copy", "--if-exists", "--project-key", "AMBIENT",],
			entry: {
				resource: "dataset",
				action: "create-managed",
				name: "orders_copy",
				projectKey: "AMBIENT",
			},
		},
		{
			name: "streaming-endpoint.create-managed targets its exact delete argv",
			resource: "streaming-endpoint",
			action: "create-managed",
			args: ["orders_stream",],
			flags: { connection: "kafka_main", "project-key": "PROJ", },
			result: { created: "orders_stream", resource: "streaming-endpoint", },
			projectKey: undefined,
			argv: ["streaming-endpoint", "delete", "orders_stream", "--project-key", "PROJ",],
			entry: {
				resource: "streaming-endpoint",
				action: "create-managed",
				id: "orders_stream",
				projectKey: "PROJ",
			},
		},
	];

	it("never records a notebook save that updated an existing notebook", () => {
		expect(
			cleanupLedgerEntry(
				"notebook",
				"save-jupyter",
				["notebook-1",],
				{},
				{ saved: "notebook-1", resource: "jupyter-notebook", created: false, },
				undefined,
			),
		).toBeUndefined();
		expect(
			cleanupLedgerEntry(
				"notebook",
				"save-sql",
				["sql-1",],
				{},
				{ saved: "sql-1", resource: "sql-notebook", created: false, },
				undefined,
			),
		).toBeUndefined();
	});

	it("never records a notebook save whose result does not prove creation", () => {
		expect(
			cleanupLedgerEntry(
				"notebook",
				"save-jupyter",
				["notebook-1",],
				{},
				{ saved: "notebook-1", resource: "jupyter-notebook", },
				undefined,
			),
		).toBeUndefined();
	});

	it("never records a notebook save dry run", () => {
		expect(
			cleanupLedgerEntry(
				"notebook",
				"save-jupyter",
				["notebook-1",],
				{ "dry-run": true, },
				{ dryRun: true, action: "save-jupyter", resource: "jupyter-notebook", },
				undefined,
			),
		).toBeUndefined();
	});

	for (const testCase of cases) {
		it(testCase.name, () => {
			const entry = cleanupLedgerEntry(
				testCase.resource,
				testCase.action,
				testCase.args,
				testCase.flags,
				testCase.result,
				testCase.projectKey,
			);
			expect(entry,).toMatchObject({
				...testCase.entry,
				cleanup: { argv: testCase.argv, },
			},);
			expect(typeof entry?.ts,).toBe("string",);
		},);
	}

	it("resolves workspaceKey from --data-file content", () => {
		const dir = mkdtempSync(join(tmpdir(), "dss-cleanup-ws-",),);
		const dataFile = join(dir, "ws.json",);
		writeFileSync(dataFile, `{"workspaceKey":"FILE_WS"}`, "utf-8",);
		try {
			const entry = cleanupLedgerEntry(
				"workspace",
				"create",
				[],
				{ "data-file": dataFile, },
				{ workspaceKey: "FILE_WS", },
				undefined,
			);
			expect(entry,).toMatchObject({
				id: "FILE_WS",
				cleanup: { argv: ["workspace", "delete", "FILE_WS",], },
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("never emits a workspace entry when only --stdin carried the input", () => {
		const entry = cleanupLedgerEntry(
			"workspace",
			"create",
			[],
			{ stdin: true, },
			{ workspaceKey: "STDIN_WS", },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("never emits a workspace entry when --data is unparsable JSON", () => {
		const entry = cleanupLedgerEntry(
			"workspace",
			"create",
			[],
			{ data: "{not json", },
			{ workspaceKey: "BAD_WS", },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("never emits a workspace entry when the input lacks workspaceKey", () => {
		const entry = cleanupLedgerEntry(
			"workspace",
			"create",
			[],
			{ data: `{"displayName":"No Key"}`, },
			{},
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("never emits a workspace entry when --data-file is unreadable", () => {
		const entry = cleanupLedgerEntry(
			"workspace",
			"create",
			[],
			{ "data-file": join(tmpdir(), `dss-missing-${Date.now()}.json`,), },
			{ workspaceKey: "GHOST_WS", },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("records project-library entries from the fallback project key only", () => {
		const entry = cleanupLedgerEntry(
			"project-library",
			"create-file",
			["lib/other.py",],
			{},
			{ created: "lib/other.py", },
			undefined,
		);
		expect(entry,).toMatchObject({
			path: "lib/other.py",
			cleanup: { argv: ["project-library", "delete", "lib/other.py",], },
		},);
		expect(entry,).not.toHaveProperty("projectKey",);
	});
});

/**
 * One representative successful create per action that advertises
 * `--record-cleanup`. The coherence test below fails when an action is
 * advertised without a row here, so a new create cannot ship a flag that the
 * ledger derivation would only reject after the mutation.
 */
interface Representative {
	args: string[];
	flags: Record<string, string | boolean>;
	result: Record<string, unknown>;
}

const INCARNATION = "a".repeat(64,);

const REPRESENTATIVES: Record<string, Representative> = {
	"analysis.create": { args: [], flags: {}, result: { created: "AN1", }, },
	"app.create-instance": {
		args: ["MYAPP",],
		flags: {},
		result: {
			projectKey: "INST",
			projectIncarnationHash: INCARNATION,
			futureTargetVerified: true,
		},
	},
	"app.create-successor-instance": {
		args: ["MYAPP",],
		flags: {},
		result: {
			projectKey: "INST",
			projectIncarnationHash: INCARNATION,
			futureTargetVerified: true,
		},
	},
	"code-env.create": { args: ["PYTHON", "env1",], flags: {}, result: {}, },
	"connection.create": {
		args: [],
		flags: { data: `{"name":"conn1","type":"PostgreSQL"}`, },
		result: {},
	},
	"dashboard.create": { args: [], flags: {}, result: { created: "DASH1", }, },
	"data-collection.create": { args: [], flags: {}, result: { id: "DC1", msg: "ok", }, },
	"data-quality.create-rule": { args: ["orders",], flags: {}, result: { id: "RULE1", }, },
	"dataset.clone": { args: ["orders", "orders_2",], flags: {}, result: { target: "orders_2", }, },
	"dataset.create": { args: [], flags: { name: "orders", }, result: { created: "orders", }, },
	"dataset.create-managed": {
		args: [],
		flags: { name: "orders_copy", },
		result: { created: "orders_copy", resource: "dataset", managed: true, projectKey: "PROJ", },
	},
	"flow-zone.create": { args: [], flags: { name: "Z", }, result: { created: "zone1", }, },
	"folder.create": { args: [], flags: { name: "raw", }, result: { created: "folder1", }, },
	"folder.upload": { args: ["folder1", "a.csv",], flags: {}, result: {}, },
	"group.create": { args: [], flags: { data: `{"name":"analysts"}`, }, result: { msg: "ok", }, },
	"insight.create": { args: [], flags: {}, result: { created: "INS1", }, },
	"meaning.create": { args: ["vip", "VIP", "VALUES_LIST",], flags: {}, result: { id: "vip", }, },
	"messaging-channel.create": {
		args: [],
		flags: { type: "shell", },
		result: { id: "ops-shell", type: "shell", },
	},
	"ml-task.create": { args: ["AN1",], flags: {}, result: { created: "TASK1", }, },
	"ml-task.create-for-dataset": {
		args: ["orders",],
		flags: {},
		result: { created: "TASK1", analysisId: "AN1", mlTaskId: "TASK1", },
	},
	"model-evaluation-store.create": { args: [], flags: {}, result: { created: "MES1", }, },
	"notebook.save-jupyter": { args: ["nb",], flags: {}, result: { saved: "nb", created: true, }, },
	"notebook.save-sql": {
		args: ["q",],
		flags: {},
		result: { saved: "sqlid", requested: "q", created: true, },
	},
	"plugin.create-dev": { args: ["my-plugin",], flags: {}, result: { created: "my-plugin", }, },
	"project-folder.create-child": {
		args: ["PARENT",],
		flags: { name: "child", },
		result: { created: "CHILD1", id: "CHILD1", },
	},
	"project-library.create-file": { args: ["lib/a.py",], flags: {}, result: {}, },
	"project-library.create-folder": { args: ["lib/dir",], flags: {}, result: {}, },
	"project.import": {
		args: [],
		flags: {},
		result: { usedProjectKey: "IMPORTED", projectIncarnationHash: INCARNATION, },
	},
	"recipe.clone": { args: ["r1", "r2",], flags: {}, result: { recipeName: "r2", }, },
	"recipe.create": { args: [], flags: { name: "r1", }, result: { created: "r1", }, },
	"saved-model.create-external": {
		args: ["Model",],
		flags: {},
		result: { id: "SM1", projectKey: "PROJ", },
	},
	"scenario.create": { args: ["scn", "Scenario",], flags: {}, result: {}, },
	"streaming-endpoint.create": {
		args: ["stream", "kafka",],
		flags: {},
		result: { id: "stream", },
	},
	"streaming-endpoint.create-managed": {
		args: ["stream",],
		flags: {},
		result: { created: "stream", resource: "streaming-endpoint", },
	},
	"user.create": { args: [], flags: { data: `{"login":"alice"}`, }, result: { msg: "ok", }, },
	"wiki.create": { args: [], flags: { name: "W", }, result: { created: "ART1", }, },
	"workspace.create": { args: [], flags: { data: `{"workspaceKey":"WS1"}`, }, result: {}, },
};

describe("--record-cleanup registry coherence", () => {
	const registry = buildCommandRegistry();
	const advertised = Object.entries(registry,).flatMap(([resource, actions,],) =>
		Object.entries(actions,)
			.filter(([, entry,],) => entry.flags.some((flag,) => flag.name === "record-cleanup"))
			.map(([action,],) => `${resource}.${action}`)
	);

	it("advertises --record-cleanup exactly where the contract supports the ledger", () => {
		const supported = Object.entries(registry,).flatMap(([resource, actions,],) =>
			Object.keys(actions,).filter((action,) => supportsCleanupLedger(resource, action,))
				.map((action,) => `${resource}.${action}`)
		);
		expect(advertised.toSorted(),).toEqual(supported.toSorted(),);
	});

	it("has a representative result for every advertised action and no stale rows", () => {
		expect(Object.keys(REPRESENTATIVES,).toSorted(),).toEqual(advertised.toSorted(),);
	});

	for (const key of advertised) {
		it(`${key} derives a replayable delete argv from a successful result`, () => {
			const [resource, action,] = key.split(".",) as [string, string,];
			const sample = REPRESENTATIVES[key];
			expect(sample, `representative for ${key}`,).toBeDefined();
			const entry = cleanupLedgerEntry(
				resource,
				action,
				sample!.args,
				sample!.flags,
				sample!.result,
				"AMBIENT",
			);
			expect(entry, `cleanup entry for ${key}`,).toBeDefined();
			const [deleteResource, deleteAction,] = entry!.cleanup.argv;
			expect(deleteAction, `${key} cleanup argv`,).toMatch(/^delete(-|$)/,);
			expect(isAllowedCleanupAction(deleteResource, deleteAction,), `${key} replay allowlist`,)
				.toBe(true,);
			expect(commands[deleteResource]?.[deleteAction], `${key} replay handler`,).toBeDefined();
		});
	}

	it("never derives an entry for a create that DSS reported as skipped", () => {
		expect(
			cleanupLedgerEntry(
				"dataset",
				"create-managed",
				[],
				{ name: "orders_copy", },
				{ skipped: true, },
				"PROJ",
			),
		).toBeUndefined();
	});
});
