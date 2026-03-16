import type { SqlQueryResponse, SqlQueryResult, } from "../schemas.js";
import { BaseResource, } from "./base.js";

export class SqlResource extends BaseResource {
	/**
	 * Start a SQL query and return the queryId + schema.
	 * Specify either `connection` (run against a DB connection)
	 * or `datasetFullName` (run against a dataset's connection).
	 */
	async startQuery(opts: {
		query: string;
		connection?: string;
		datasetFullName?: string;
		database?: string;
		preQueries?: string[];
		postQueries?: string[];
		type?: string;
	},): Promise<SqlQueryResult> {
		return this.client.post<SqlQueryResult>(`/public/api/sql/queries`, opts,);
	}

	/**
	 * Stream results of a started query as parsed JSON (array of arrays).
	 */
	async streamResults(queryId: string,): Promise<unknown[][]> {
		const id = encodeURIComponent(queryId,);
		const text = await this.client.getText(`/public/api/sql/queries/${id}/stream?format=json`,);
		return JSON.parse(text,) as unknown[][];
	}

	/**
	 * Verify that a query finished successfully server-side.
	 * Throws on failure.
	 */
	async finishStreaming(queryId: string,): Promise<void> {
		const id = encodeURIComponent(queryId,);
		const text = await this.client.getText(`/public/api/sql/queries/${id}/finish-streaming`,);
		if (text.length > 0) {
			throw new Error(`SQL query ${queryId} failed: ${text}`,);
		}
	}

	/**
	 * Execute a SQL query end-to-end: start, stream all rows, verify, return combined result.
	 * This is the primary method most callers want.
	 */
	async query(opts: {
		query: string;
		connection?: string;
		datasetFullName?: string;
		database?: string;
		preQueries?: string[];
		postQueries?: string[];
		type?: string;
	},): Promise<SqlQueryResponse> {
		const { queryId, schema, } = await this.startQuery(opts,);
		const rows = await this.streamResults(queryId,);
		await this.finishStreaming(queryId,);
		return { queryId, schema, rows, };
	}
}
