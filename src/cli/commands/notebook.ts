import { jsonInput, num, } from "../coerce.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const notebookCommands: Record<string, CommandMeta> = {
	"list-jupyter": {
		handler: (c, _a, f,) => c.notebooks.listJupyter(f["project-key"] as string | undefined,),
		usage: "dss notebook list-jupyter [--project-key KEY]",
		description: "List Jupyter notebooks.",
		examples: ["dss notebook list-jupyter",],
	},
	"get-jupyter": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss notebook get-jupyter <name>",);
			return c.notebooks.getJupyter(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss notebook get-jupyter <name> [--project-key KEY]",
		description: "Get a Jupyter notebook.",
		examples: ["dss notebook get-jupyter my_notebook",],
	},
	"delete-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss notebook delete-jupyter <name>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.notebooks.getJupyter(a[0], pk,));
				if (!current) return skipResult("jupyter-notebook", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "jupyter-notebook", name: a[0], current, };
				}
			}
			await c.notebooks.deleteJupyter(a[0], pk,);
			return { deleted: a[0], resource: "jupyter-notebook", };
		},
		usage: "dss notebook delete-jupyter <name> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a Jupyter notebook.",
		examples: ["dss notebook delete-jupyter my_notebook --dry-run",],
	},
	"clear-jupyter-outputs": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss notebook clear-jupyter-outputs <name>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await c.notebooks.getJupyter(a[0], pk,);
				const next = {
					...current,
					cells: current.cells.map((cell,) => ({ ...cell, outputs: [], execution_count: null, })),
				};
				return {
					dryRun: true,
					action: "clear-jupyter-outputs",
					resource: "jupyter-notebook",
					name: a[0],
					current,
					next,
				};
			}
			await c.notebooks.clearJupyterOutputs(a[0], pk,);
			return { cleared: a[0], resource: "jupyter-notebook", };
		},
		usage: "dss notebook clear-jupyter-outputs <name> [--dry-run] [--project-key KEY]",
		description: "Clear all cell outputs from a Jupyter notebook.",
		examples: ["dss notebook clear-jupyter-outputs my_notebook --dry-run",],
	},
	"sessions-jupyter": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss notebook sessions-jupyter <name>",);
			return c.notebooks.listJupyterSessions(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss notebook sessions-jupyter <name> [--project-key KEY]",
		description: "List active kernel sessions for a Jupyter notebook.",
		examples: ["dss notebook sessions-jupyter my_notebook",],
	},
	"unload-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss notebook unload-jupyter <name> <sessionId>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const sessions = await c.notebooks.listJupyterSessions(a[0], pk,);
				const current = sessions.find((session,) => session.sessionId === a[1]);
				return {
					dryRun: true,
					action: "unload-jupyter",
					resource: "jupyter-notebook",
					name: a[0],
					sessionId: a[1],
					current,
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/jupyter-notebooks/${encodeURIComponent(a[0],)}/sessions/${encodeURIComponent(a[1],)}`,
					),
					method: "DELETE",
				};
			}
			await c.notebooks.unloadJupyter(a[0], a[1], pk,);
			return { unloaded: a[0], sessionId: a[1], resource: "jupyter-notebook", };
		},
		usage: "dss notebook unload-jupyter <name> <sessionId> [--dry-run] [--project-key KEY]",
		description: "Unload a Jupyter notebook kernel session.",
		examples: ["dss notebook unload-jupyter my_notebook SESSION_ID --dry-run",],
	},
	"list-sql": {
		handler: (c, _a, f,) => c.notebooks.listSql(f["project-key"] as string | undefined,),
		usage: "dss notebook list-sql [--project-key KEY]",
		description: "List SQL notebooks.",
		examples: ["dss notebook list-sql",],
	},
	"get-sql": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss notebook get-sql <id>",);
			return c.notebooks.getSql(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss notebook get-sql <id> [--project-key KEY]",
		description: "Get a SQL notebook.",
		examples: ["dss notebook get-sql my_sql_notebook",],
	},
	"delete-sql": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss notebook delete-sql <id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.notebooks.getSql(a[0], pk,));
				if (!current) return skipResult("sql-notebook", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "sql-notebook", id: a[0], current, };
				}
			}
			await c.notebooks.deleteSql(a[0], pk,);
			return { deleted: a[0], resource: "sql-notebook", };
		},
		usage: "dss notebook delete-sql <id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a SQL notebook.",
		examples: ["dss notebook delete-sql my_sql_notebook --dry-run",],
	},
	"history-sql": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss notebook history-sql <id>",);
			return c.notebooks.getSqlHistory(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss notebook history-sql <id> [--project-key KEY]",
		description: "Get query history for a SQL notebook.",
		examples: ["dss notebook history-sql my_sql_notebook",],
	},
	"save-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin]",
			);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required (notebook JSON content).",
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await readIfExists(() => c.notebooks.getJupyter(a[0], pk,));
				return {
					dryRun: true,
					action: "save-jupyter",
					resource: "jupyter-notebook",
					name: a[0],
					current,
					next: data,
				};
			}
			await c.notebooks.saveJupyter(a[0], data as never, pk,);
			return { saved: a[0], resource: "jupyter-notebook", };
		},
		usage:
			"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
		description: "Save content to a Jupyter notebook.",
		examples: [
			"dss notebook save-jupyter my_notebook --data-file notebook.json --dry-run",
			"cat notebook.json | dss notebook save-jupyter my_notebook --stdin",
		],
	},
	"save-sql": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin]",);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required (SQL notebook content JSON).",
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await readIfExists(() => c.notebooks.getSql(a[0], pk,));
				return {
					dryRun: true,
					action: "save-sql",
					resource: "sql-notebook",
					id: a[0],
					current,
					next: data,
				};
			}
			await c.notebooks.saveSql(a[0], data as never, pk,);
			return { saved: a[0], resource: "sql-notebook", };
		},
		usage:
			"dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
		description: "Save content to a SQL notebook.",
		examples: ["dss notebook save-sql my_sql_notebook --data-file content.json --dry-run",],
	},
	"clear-sql-history": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss notebook clear-sql-history <id>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				cellId: f["cell-id"] as string | undefined,
				numRunsToRetain: num(f["retain"],),
				projectKey: pk,
			};
			if (f["dry-run"] === true) {
				const current = await c.notebooks.getSqlHistory(a[0], pk,);
				return {
					dryRun: true,
					action: "clear-sql-history",
					resource: "sql-notebook",
					id: a[0],
					current,
					next: options,
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/sql-notebooks/${encodeURIComponent(a[0],)}/history/clear`,
					),
					method: "POST",
				};
			}
			await c.notebooks.clearSqlHistory(a[0], options,);
			return { cleared: a[0], resource: "sql-notebook", };
		},
		usage:
			"dss notebook clear-sql-history <id> [--cell-id CID] [--retain N] [--dry-run] [--project-key KEY]",
		description: "Clear query history for a SQL notebook.",
		examples: [
			"dss notebook clear-sql-history my_sql_notebook --dry-run",
			"dss notebook clear-sql-history my_sql_notebook --cell-id CELL1 --retain 5",
		],
	},
};
