import type {
	JupyterNotebookContent,
	JupyterNotebookSummary,
	NotebookSession,
	SqlNotebookContent,
	SqlNotebookSummary,
} from "../schemas.js";
import {
	JupyterNotebookContentSchema,
	JupyterNotebookSummaryArraySchema,
	NotebookSessionArraySchema,
	SqlNotebookContentSchema,
	SqlNotebookSummaryArraySchema,
} from "../schemas.js";
import { BaseResource, } from "./base.js";

export class NotebooksResource extends BaseResource {
	// ── Jupyter Notebooks ──────────────────────────────────────────────

	/** List all Jupyter notebooks in a project. */
	async listJupyter(projectKey?: string,): Promise<JupyterNotebookSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/`,
		);
		return this.client.safeParse(JupyterNotebookSummaryArraySchema, raw, "notebooks.listJupyter",);
	}

	/** Get the full content of a Jupyter notebook. */
	async getJupyter(name: string, projectKey?: string,): Promise<JupyterNotebookContent> {
		const nameEnc = encodeURIComponent(name,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}`,
		);
		return this.client.safeParse(JupyterNotebookContentSchema, raw, "notebooks.getJupyter",);
	}

	/** Save (overwrite) a Jupyter notebook's content. */
	async saveJupyter(
		name: string,
		content: JupyterNotebookContent,
		projectKey?: string,
	): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}`,
			content,
		);
	}

	/** Delete a Jupyter notebook. */
	async deleteJupyter(name: string, projectKey?: string,): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}`,
		);
	}

	/** Clear all cell outputs from a Jupyter notebook. */
	async clearJupyterOutputs(name: string, projectKey?: string,): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}/outputs`,
		);
	}

	/** List running kernel sessions for a Jupyter notebook. */
	async listJupyterSessions(name: string, projectKey?: string,): Promise<NotebookSession[]> {
		const nameEnc = encodeURIComponent(name,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}/sessions`,
		);
		return this.client.safeParse(NotebookSessionArraySchema, raw, "notebooks.sessionsJupyter",);
	}

	/** Unload (stop) a running Jupyter notebook session. */
	async unloadJupyter(name: string, sessionId: string, projectKey?: string,): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		const sidEnc = encodeURIComponent(sessionId,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}/sessions/${sidEnc}`,
		);
	}

	// ── SQL Notebooks ──────────────────────────────────────────────────

	/** List all SQL notebooks in a project. */
	async listSql(projectKey?: string,): Promise<SqlNotebookSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/`,
		);
		return this.client.safeParse(SqlNotebookSummaryArraySchema, raw, "notebooks.listSql",);
	}

	/** Get the full content of a SQL notebook. */
	async getSql(id: string, projectKey?: string,): Promise<SqlNotebookContent> {
		const idEnc = encodeURIComponent(id,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}`,
		);
		return this.client.safeParse(SqlNotebookContentSchema, raw, "notebooks.getSql",);
	}

	/** Save (overwrite) a SQL notebook's content. */
	async saveSql(id: string, content: SqlNotebookContent, projectKey?: string,): Promise<void> {
		const idEnc = encodeURIComponent(id,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}`,
			content,
		);
	}

	/** Delete a SQL notebook. */
	async deleteSql(id: string, projectKey?: string,): Promise<void> {
		const idEnc = encodeURIComponent(id,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}`,
		);
	}

	/** Get execution history for a SQL notebook (keyed by cell ID). */
	async getSqlHistory(id: string, projectKey?: string,): Promise<Record<string, unknown[]>> {
		const idEnc = encodeURIComponent(id,);
		return this.client.get<Record<string, unknown[]>>(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}/history`,
		);
	}

	/** Clear execution history for a SQL notebook. */
	async clearSqlHistory(
		id: string,
		opts?: { cellId?: string; numRunsToRetain?: number; projectKey?: string; },
	): Promise<void> {
		const idEnc = encodeURIComponent(id,);
		await this.client.post<void>(
			`/public/api/projects/${this.enc(opts?.projectKey,)}/sql-notebooks/${idEnc}/history/clear`,
			{ cellId: opts?.cellId, numRunsToRetain: opts?.numRunsToRetain, },
		);
	}
}
