import { ClientValidationError, DataikuError, } from "../errors.js";
import type {
	JupyterNotebookContent,
	JupyterNotebookSummary,
	NotebookSession,
	SqlNotebookContent,
	SqlNotebookHistory,
	SqlNotebookSummary,
} from "../schemas.js";
import {
	JupyterNotebookContentSchema,
	JupyterNotebookSummaryArraySchema,
	NotebookSessionArraySchema,
	SqlNotebookContentSchema,
	SqlNotebookHistorySchema,
	SqlNotebookSummaryArraySchema,
} from "../schemas.js";
import { stableHash, } from "../utils/stable-hash.js";
import { BaseResource, } from "./base.js";

/** Result of a save that lands through POST (create) or PUT (update). */
export interface NotebookSaveResult {
	/** True when this call created the notebook, false when it overwrote one. */
	created: boolean;
	/** SHA-256 (hex) of the persisted content after a confirming re-read. */
	hash: string;
}

export interface SaveNotebookOptions {
	/** Reject the save when the stored content hash no longer matches. */
	expectHash?: string;
}

export interface JupyterUnloadAllResult {
	name: string;
	unloadedSessionIds: string[];
}

const EXPECT_HASH_PATTERN = /^[0-9a-fA-F]{64}$/;

export class NotebooksResource extends BaseResource {
	// ── Jupyter Notebooks ──────────────────────────────────────────────

	/**
	 * List Jupyter notebooks in a project.
	 *
	 * Pass `opts.active` to use the official `?active=` query filter: `true`
	 * returns only currently running notebooks, `false` only non-running ones
	 * (verified against `list_jupyter_notebooks` in the official Python API
	 * client). Omit it to list without the filter.
	 */
	async listJupyter(
		projectKey?: string,
		opts?: { active?: boolean; },
	): Promise<JupyterNotebookSummary[]> {
		const suffix = opts?.active === undefined
			? "/"
			: `/?active=${String(opts.active,)}`;
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks${suffix}`,
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

	/**
	 * Create a Jupyter notebook.
	 *
	 * The API takes the notebook name in the path; the body is the content.
	 */
	async createJupyter(
		name: string,
		content: JupyterNotebookContent,
		projectKey?: string,
	): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		await this.client.post<void>(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}`,
			content,
		);
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

	/**
	 * Save a Jupyter notebook, creating it when missing.
	 *
	 * `created` reflects what actually happened: a 404 on the fresh read makes
	 * the follow-up POST a creation; an existing notebook is overwritten by
	 * PUT. The returned `hash` is always the hash of the persisted content
	 * from a confirming re-read, so it can be passed straight to
	 * `--expect-hash` on a later save. The PUT/POST themselves stay
	 * unconditional: DSS has no conditional write, so a writer committing
	 * between the read and the write is not detected.
	 */
	async saveOrCreateJupyter(
		name: string,
		content: JupyterNotebookContent,
		projectKey?: string,
		opts?: SaveNotebookOptions,
	): Promise<NotebookSaveResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const before = await this.prepareSave(
			() => this.getJupyter(name, pk,),
			opts?.expectHash,
			pk,
			"Jupyter notebook",
			name,
		);
		if (before === undefined) {
			await this.createJupyter(name, content, pk,);
		} else {
			await this.saveJupyter(name, content, pk,);
		}
		const persisted = await this.getJupyter(name, pk,);
		return { created: before === undefined, hash: stableHash(persisted,), };
	}

	/** Delete a Jupyter notebook. */
	async deleteJupyter(name: string, projectKey?: string,): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}`,
		);
	}

	/**
	 * Clear all cell outputs from a Jupyter notebook.
	 *
	 * Uses the official `DELETE .../outputs` endpoint (verified against
	 * `DSSJupyterNotebook.clear_outputs` in the official Python API client),
	 * so the clear is a single atomic server-side operation instead of a
	 * non-atomic GET-strip-PUT round trip.
	 */
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

	/**
	 * Unload (stop) a running Jupyter notebook session.
	 *
	 * DSS public APIs expose session listing and unloading, but this SDK has no
	 * path to start a disposable kernel session; live unload coverage therefore
	 * requires an externally-started disposable session fixture.
	 */
	async unloadJupyter(name: string, sessionId: string, projectKey?: string,): Promise<void> {
		const nameEnc = encodeURIComponent(name,);
		const sidEnc = encodeURIComponent(sessionId,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/jupyter-notebooks/${nameEnc}/sessions/${sidEnc}`,
		);
	}

	/**
	 * Unload every session of every currently running Jupyter notebook.
	 *
	 * DSS exposes no single unload-all endpoint (verified against the official
	 * Python API client, which only ever issues per-session DELETEs), so this
	 * composes the verified endpoints: list active notebooks, list each
	 * notebook's sessions, and DELETE each session. Sessions that vanish
	 * between listing and unloading count as already unloaded.
	 */
	async unloadJupyterAll(projectKey?: string,): Promise<JupyterUnloadAllResult[]> {
		const active = await this.listJupyter(projectKey, { active: true, },);
		const results: JupyterUnloadAllResult[] = [];
		for (const summary of active) {
			const sessions = await this.listJupyterSessions(summary.name, projectKey,);
			const unloadedSessionIds: string[] = [];
			for (const session of sessions) {
				try {
					await this.unloadJupyter(summary.name, session.sessionId, projectKey,);
					unloadedSessionIds.push(session.sessionId,);
				} catch (error) {
					if (!(error instanceof DataikuError && error.category === "not_found")) throw error;
				}
			}
			results.push({ name: summary.name, unloadedSessionIds, },);
		}
		return results;
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

	/** Create a SQL notebook. */
	async createSql(id: string, content: SqlNotebookContent, projectKey?: string,): Promise<void> {
		await this.client.post<void>(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/`,
			{ ...content, id, projectKey: this.resolveProjectKey(projectKey,), },
		);
	}

	/** Save (overwrite) a SQL notebook's content. */
	async saveSql(id: string, content: SqlNotebookContent, projectKey?: string,): Promise<void> {
		const idEnc = encodeURIComponent(id,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}`,
			content,
		);
	}

	/**
	 * Save a SQL notebook, creating it when missing. Same contract as
	 * `saveOrCreateJupyter`: `created` reflects the fresh read, and `hash` is
	 * the hash of the persisted content from a confirming re-read.
	 */
	async saveOrCreateSql(
		id: string,
		content: SqlNotebookContent,
		projectKey?: string,
		opts?: SaveNotebookOptions,
	): Promise<NotebookSaveResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const before = await this.prepareSave(
			() => this.getSql(id, pk,),
			opts?.expectHash,
			pk,
			"SQL notebook",
			id,
		);
		if (before === undefined) {
			await this.createSql(id, content, pk,);
		} else {
			await this.saveSql(id, content, pk,);
		}
		const persisted = await this.getSql(id, pk,);
		return { created: before === undefined, hash: stableHash(persisted,), };
	}

	/** Delete a SQL notebook. */
	async deleteSql(id: string, projectKey?: string,): Promise<void> {
		const idEnc = encodeURIComponent(id,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}`,
		);
	}

	/** Get execution history for a SQL notebook (keyed by cell ID). */
	async getSqlHistory(id: string, projectKey?: string,): Promise<SqlNotebookHistory> {
		const idEnc = encodeURIComponent(id,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/sql-notebooks/${idEnc}/history`,
		);
		return this.client.safeParse(SqlNotebookHistorySchema, raw, "notebooks.getSqlHistory",);
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

	// ── Shared save helpers ────────────────────────────────────────────

	/**
	 * Probe the stored notebook before a save. A missing notebook resolves to
	 * `undefined` (the caller will create it); anything else propagates. When
	 * `expectHash` is armed, the fresh read doubles as the stale-read guard:
	 * the hash of the stored content must match, or the save is rejected
	 * before any write. Like every DSS write, the PUT/POST that follows stays
	 * unconditional — the guard cannot detect a writer that commits between
	 * this read and the write.
	 */
	private async prepareSave<T,>(
		read: () => Promise<T>,
		expectHash: string | undefined,
		pk: string,
		resourceLabel: string,
		id: string,
	): Promise<T | undefined> {
		// Shape validation runs before any request so an armed but malformed
		// guard rejects without touching DSS.
		const expected = expectHash === undefined
			? undefined
			: this.validatedExpectHash(expectHash, pk, resourceLabel, id,);
		let current: T | undefined;
		try {
			current = await read();
		} catch (error) {
			if (!(error instanceof DataikuError && error.category === "not_found")) throw error;
		}
		if (expected === undefined) return current;
		const actual = current === undefined ? undefined : stableHash(current,);
		if (actual !== expected) {
			throw new ClientValidationError(
				`The ${resourceLabel} ${JSON.stringify(id,)} changed since it was read.`,
				"validation_failed",
				"Re-read the notebook and retry with the current hash value.",
				{
					projectKey: pk,
					id,
					expectedHash: expected,
					...(actual !== undefined ? { actualHash: actual, } : { absent: true, }),
				},
			);
		}
		return current;
	}

	/** Validate the armed `--expect-hash` value into a comparable hex digest. */
	private validatedExpectHash(
		expectHash: string,
		pk: string,
		resourceLabel: string,
		id: string,
	): string {
		if (!EXPECT_HASH_PATTERN.test(expectHash,)) {
			throw new ClientValidationError(
				`Expected ${resourceLabel} hash must be a 64-character SHA-256 hex digest.`,
				"validation_failed",
				"Use the hash value returned by a previous save or read.",
				{ projectKey: pk, id, },
			);
		}
		return expectHash.toLowerCase();
	}
}
