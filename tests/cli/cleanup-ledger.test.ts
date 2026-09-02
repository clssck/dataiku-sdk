import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
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
			name: "notebook.save-sql records a delete only when the save created the notebook",
			resource: "notebook",
			action: "save-sql",
			args: ["scratch_sql",],
			flags: {},
			result: { saved: "scratch_sql", resource: "sql-notebook", created: true, },
			projectKey: "FALLBACK",
			argv: ["notebook", "delete-sql", "scratch_sql", "--if-exists", "--project-key", "FALLBACK",],
			entry: {
				resource: "notebook",
				action: "save-sql",
				id: "scratch_sql",
				projectKey: "FALLBACK",
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
