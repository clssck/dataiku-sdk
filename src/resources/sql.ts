import { DataikuError, } from "../errors.js";
import type { SqlQueryResponse, SqlQueryResult, } from "../schemas.js";
import { BaseResource, } from "./base.js";

const UNSUPPORTED_SQL_DATASET_CONNECTION_DETAIL = "neither of sql nor hdfs type";

function isUnsupportedSqlDatasetConnectionError(error: unknown,): error is DataikuError {
	if (!(error instanceof DataikuError)) return false;
	const detail = `${error.statusText}\n${error.body}\n${error.message}`.toLowerCase();
	return detail.includes(UNSUPPORTED_SQL_DATASET_CONNECTION_DETAIL,);
}

function buildUnsupportedSqlDatasetConnectionMessage(datasetFullName?: string,): string {
	const subject = datasetFullName
		? `Dataset "${datasetFullName}" uses a connection that DSS does not support for direct SQL queries.`
		: "This query uses a connection that DSS does not support for direct SQL queries.";
	return `${subject} Use --connection with a SQL-compatible connection instead.`;
}

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
		return this.client.post<SqlQueryResult>("/public/api/sql/queries/", {
			...opts,
			type: opts.type ?? "sql",
		},);
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
		const queryOpts = { ...opts, type: opts.type ?? "sql", };
		try {
			const { queryId, schema, } = await this.startQuery(queryOpts,);
			const rows = await this.streamResults(queryId,);
			await this.finishStreaming(queryId,);
			return { queryId, schema, rows, };
		} catch (error) {
			if (!isUnsupportedSqlDatasetConnectionError(error,)) throw error;
			throw new Error(buildUnsupportedSqlDatasetConnectionMessage(queryOpts.datasetFullName,), {
				cause: error,
			},);
		}
	}
}
