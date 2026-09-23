import { jsonInput, num, parseBooleanOption, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, UsageError, } from "../usage.js";

export const notebookCommands: Record<string, CommandMeta> = withUsage("notebook", {
	"list-jupyter": {
		handler: (c, _a, f,) =>
			c.notebooks.listJupyter(f["project-key"] as string | undefined, {
				active: parseBooleanOption(f["active"], "--active",),
			},),
		description:
			"List Jupyter notebooks. Pass --active to use the official ?active= filter: true lists currently running notebooks, false lists non-running ones.",
		examples: [
			"dss notebook list-jupyter",
			"dss notebook list-jupyter --active true",
		],
	},
	"get-jupyter": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "get-jupyter",),);
			return c.notebooks.getJupyter(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a Jupyter notebook.",
		examples: ["dss notebook get-jupyter my_notebook",],
	},
	"delete-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "delete-jupyter",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.notebooks.getJupyter(a[0], pk,));
				if (!current) return skipResult("jupyter-notebook", a[0], "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "jupyter-notebook", name: a[0], current, };
				}
			}
			await c.notebooks.deleteJupyter(a[0], pk,);
			return { deleted: a[0], resource: "jupyter-notebook", };
		},
		description: "Delete a Jupyter notebook.",
		examples: ["dss notebook delete-jupyter my_notebook --dry-run",],
	},
	"clear-jupyter-outputs": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "clear-jupyter-outputs",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				// The real mutation is a single server-side DELETE of the
				// outputs endpoint, so the dry run can only report the current
				// content (read-only) and the exact request that would run; the
				// stripped `next` shape would misdescribe the operation.
				const current = await c.notebooks.getJupyter(a[0], pk,);
				return {
					dryRun: true,
					action: "clear-jupyter-outputs",
					resource: "jupyter-notebook",
					name: a[0],
					current,
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/jupyter-notebooks/${encodeURIComponent(a[0],)}/outputs`,
					),
					method: "DELETE",
				};
			}
			await c.notebooks.clearJupyterOutputs(a[0], pk,);
			return { cleared: a[0], resource: "jupyter-notebook", };
		},
		description: "Clear all cell outputs from a Jupyter notebook.",
		examples: ["dss notebook clear-jupyter-outputs my_notebook --dry-run",],
	},
	"sessions-jupyter": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "sessions-jupyter",),);
			return c.notebooks.listJupyterSessions(a[0], f["project-key"] as string | undefined,);
		},
		description: "List active kernel sessions for a Jupyter notebook.",
		examples: ["dss notebook sessions-jupyter my_notebook",],
	},
	"unload-jupyter": {
		handler: async (c, a, f,) => {
			const pk = f["project-key"] as string | undefined;
			if (f["all"] === true) {
				requireNoArgs(a, commandUsage("notebook", "unload-jupyter",),);
				if (executionMode(f,).dryRun) {
					const planned = [];
					for (const notebook of await c.notebooks.listJupyter(pk, { active: true, },)) {
						for (const session of await c.notebooks.listJupyterSessions(notebook.name, pk,)) {
							planned.push({
								name: notebook.name,
								sessionId: session.sessionId,
								endpoint: encodedProjectEndpoint(
									c,
									pk,
									`/jupyter-notebooks/${encodeURIComponent(notebook.name,)}/sessions/${
										encodeURIComponent(session.sessionId,)
									}`,
								),
								method: "DELETE",
							},);
						}
					}
					return {
						dryRun: true,
						action: "unload-jupyter",
						resource: "jupyter-notebook",
						all: true,
						planned,
					};
				}
				const unloaded = await c.notebooks.unloadJupyterAll(pk,);
				return { unloaded, resource: "jupyter-notebook", all: true, };
			}
			requireArgs(a, 2, commandUsage("notebook", "unload-jupyter",),);
			if (executionMode(f,).dryRun) {
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
		description:
			"Unload a Jupyter notebook kernel session, or every session of every running notebook with --all.",
		examples: [
			"dss notebook unload-jupyter my_notebook SESSION_ID --dry-run",
			"dss notebook unload-jupyter --all --dry-run",
		],
	},
	"list-sql": {
		handler: (c, _a, f,) => c.notebooks.listSql(f["project-key"] as string | undefined,),
		description: "List SQL notebooks.",
		examples: ["dss notebook list-sql",],
	},
	"get-sql": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "get-sql",),);
			return c.notebooks.getSql(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a SQL notebook.",
		examples: ["dss notebook get-sql my_sql_notebook",],
	},
	"delete-sql": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "delete-sql",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.notebooks.getSql(a[0], pk,));
				if (!current) return skipResult("sql-notebook", a[0], "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "sql-notebook", id: a[0], current, };
				}
			}
			await c.notebooks.deleteSql(a[0], pk,);
			return { deleted: a[0], resource: "sql-notebook", };
		},
		description: "Delete a SQL notebook.",
		examples: ["dss notebook delete-sql my_sql_notebook --dry-run",],
	},
	"history-sql": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "history-sql",),);
			return c.notebooks.getSqlHistory(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get query history for a SQL notebook.",
		examples: ["dss notebook history-sql my_sql_notebook",],
	},
	"save-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("notebook", "save-jupyter",),
			);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required (notebook JSON content).",
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
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
			const result = await c.notebooks.saveOrCreateJupyter(a[0], data as never, pk, {
				expectHash: f["expect-hash"] as string | undefined,
			},);
			return {
				saved: a[0],
				resource: "jupyter-notebook",
				created: result.created,
				hash: result.hash,
			};
		},
		description:
			"Save content to a Jupyter notebook, creating it if missing. Reports created versus updated and the persisted content hash; --expect-hash aborts with a stale-read error when the stored content hash changed.",
		examples: [
			"dss notebook save-jupyter my_notebook --data-file notebook.json --dry-run",
			"cat notebook.json | dss notebook save-jupyter my_notebook --stdin",
		],
	},
	"save-sql": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "save-sql",),);
			const data = jsonInput(f,);
			if (!data) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required (SQL notebook content JSON).",
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
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
			const result = await c.notebooks.saveOrCreateSql(a[0], data as never, pk, {
				expectHash: f["expect-hash"] as string | undefined,
			},);
			return {
				// `saved` is the persisted notebook id: on create DSS allocates it
				// server-side, so every later command must use this id. `requested`
				// echoes the handle, which becomes the notebook's display name.
				saved: result.id,
				requested: a[0],
				resource: "sql-notebook",
				created: result.created,
				hash: result.hash,
			};
		},
		description:
			"Save content to a SQL notebook, creating it if missing. On create DSS allocates the notebook id: `saved` is the persisted id to use for every later read/update/delete, and `requested` echoes the handle, which becomes the display name. Reports created versus updated and the persisted content hash; --expect-hash rejects a stale read before writing and refuses a missing notebook.",
		examples: ["dss notebook save-sql my_sql_notebook --data-file content.json --dry-run",],
	},
	"clear-sql-history": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("notebook", "clear-sql-history",),);
			const pk = f["project-key"] as string | undefined;
			const options = {
				cellId: f["cell-id"] as string | undefined,
				numRunsToRetain: num(f["retain"], "--retain",),
				projectKey: pk,
			};
			if (executionMode(f,).dryRun) {
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
		description: "Clear query history for a SQL notebook.",
		examples: [
			"dss notebook clear-sql-history my_sql_notebook --dry-run",
			"dss notebook clear-sql-history my_sql_notebook --cell-id CELL1 --retain 5",
		],
	},
},);
