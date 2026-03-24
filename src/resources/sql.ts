import { DataikuError, } from "../errors.js";
import type { SqlQueryResponse, SqlQueryResult, } from "../schemas.js";
import { BaseResource, } from "./base.js";

const UNSUPPORTED_SQL_DATASET_CONNECTION_DETAIL = "neither of sql nor hdfs type";

type SqlQueryOptions = {
	query: string;
	connection?: string;
	datasetFullName?: string;
	database?: string;
	preQueries?: string[];
	postQueries?: string[];
	type?: string;
	projectKey?: string;
};

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

function asRecord(value: unknown,): Record<string, unknown> | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value,)) return undefined;
	return value as Record<string, unknown>;
}

function asString(value: unknown,): string | undefined {
	if (typeof value !== "string") return undefined;
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : undefined;
}

function splitDatasetIdentifier(
	datasetFullName: string,
	fallbackProjectKey?: string,
): { datasetName: string; projectKey?: string; } {
	const trimmed = datasetFullName.trim();
	const dotIndex = trimmed.indexOf(".",);
	if (dotIndex <= 0) {
		return { datasetName: trimmed, projectKey: fallbackProjectKey, };
	}
	return {
		projectKey: trimmed.slice(0, dotIndex,),
		datasetName: trimmed.slice(dotIndex + 1,),
	};
}

export class SqlResource extends BaseResource {
	private resolveOptionalProjectKey(projectKey?: string,): string | undefined {
		try {
			return this.resolveProjectKey(projectKey,);
		} catch {
			return undefined;
		}
	}

	/**
	 * Start a SQL query and return the queryId + schema.
	 * Specify either `connection` (run against a DB connection)
	 * or `datasetFullName` (run against a dataset's connection).
	 */
	async startQuery(opts: SqlQueryOptions,): Promise<SqlQueryResult> {
		return this.client.post<SqlQueryResult>("/public/api/sql/queries/", {
			...opts,
			projectKey: opts.projectKey ?? this.resolveOptionalProjectKey(opts.projectKey,),
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

	private async executeQuery(opts: SqlQueryOptions,): Promise<SqlQueryResponse> {
		const { queryId, schema, } = await this.startQuery(opts,);
		const rows = await this.streamResults(queryId,);
		await this.finishStreaming(queryId,);
		return { queryId, schema, rows, };
	}

	private async resolveDatasetQueryFallback(
		opts: SqlQueryOptions,
	): Promise<SqlQueryOptions | null> {
		const datasetFullName = opts.datasetFullName;
		if (!datasetFullName) return null;

		try {
			const identifier = splitDatasetIdentifier(datasetFullName, opts.projectKey,);
			const projectKey = identifier.projectKey
				? identifier.projectKey
				: this.resolveProjectKey(opts.projectKey,);
			const dsEnc = encodeURIComponent(identifier.datasetName,);
			const raw = await this.client.get<Record<string, unknown>>(
				`/public/api/projects/${encodeURIComponent(projectKey,)}/datasets/${dsEnc}`,
			);
			const params = asRecord(raw.params,);
			const connection = asString(params?.connection,);
			if (!connection) return null;
			return {
				...opts,
				connection,
				datasetFullName: undefined,
				database: opts.database ?? asString(params?.schema,) ?? asString(params?.catalog,),
				projectKey,
			};
		} catch {
			return null;
		}
	}

	/**
	 * Execute a SQL query end-to-end: start, stream all rows, verify, return combined result.
	 * This is the primary method most callers want.
	 */
	async query(opts: SqlQueryOptions,): Promise<SqlQueryResponse> {
		const queryOpts = { ...opts, type: opts.type ?? "sql", };
		try {
			return await this.executeQuery(queryOpts,);
		} catch (error) {
			if (!isUnsupportedSqlDatasetConnectionError(error,)) throw error;
			const retryOpts = await this.resolveDatasetQueryFallback(queryOpts,);
			if (!retryOpts) {
				throw new Error(buildUnsupportedSqlDatasetConnectionMessage(queryOpts.datasetFullName,), {
					cause: error,
				},);
			}
			return this.executeQuery(retryOpts,);
		}
	}
}
