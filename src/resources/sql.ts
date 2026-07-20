import { ClientValidationError, DataikuError, } from "../errors.js";
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

function firstStringField(record: Record<string, unknown>, fields: string[],): string | undefined {
	for (const field of fields) {
		const value = record[field];
		if (typeof value === "string" && value.trim().length > 0) return value.trim();
	}
	return undefined;
}

function isLikelySqlErrorDetail(detail: string,): boolean {
	const lower = detail.toLowerCase();
	return /\b[A-Z_]+(?:_ERROR|_NOT_FOUND|_MISMATCH|_DENIED|_EXCEEDED)\b/.test(detail,)
		|| lower.includes("athena",)
		|| lower.includes("sql",)
		|| lower.includes("query",)
		|| lower.includes("column",)
		|| lower.includes("table",)
		|| lower.includes("line ",);
}

function sqlErrorDetailFromBody(body: string,): string | undefined {
	const trimmed = body.trim();
	if (!trimmed) return undefined;
	try {
		const parsed = JSON.parse(trimmed,) as unknown;
		if (parsed && typeof parsed === "object" && !Array.isArray(parsed,)) {
			const record = parsed as Record<string, unknown>;
			const nested = asRecord(record.details,) ?? asRecord(record.error,);
			const nestedDetail = nested
				? firstStringField(nested, ["message", "errorMessage", "error", "reason", "cause",],)
				: undefined;
			if (nestedDetail && isLikelySqlErrorDetail(nestedDetail,)) return nestedDetail;
			const direct = firstStringField(record, [
				"message",
				"detailedMessage",
				"errorMessage",
				"error",
				"reason",
				"cause",
			],);
			if (direct && isLikelySqlErrorDetail(direct,)) return direct;
		}
	} catch {
		// Fall back to regex extraction from raw DSS/Athena text below.
	}

	const match = trimmed.match(
		/\b[A-Z_]+(?:_ERROR|_NOT_FOUND|_MISMATCH|_DENIED|_EXCEEDED)\b[:\s-]+[^\n\r]+/,
	);
	return match?.[0] ?? (isLikelySqlErrorDetail(trimmed,) ? trimmed.slice(0, 500,) : undefined);
}

function withSqlErrorContext(error: unknown,): never {
	if (error instanceof DataikuError) {
		const detail = sqlErrorDetailFromBody(error.body,);
		if (detail) {
			let body = error.body;
			try {
				const parsed = JSON.parse(error.body,) as unknown;
				if (parsed && typeof parsed === "object" && !Array.isArray(parsed,)) {
					body = JSON.stringify({
						...(parsed as Record<string, unknown>),
						message: `SQL query failed: ${detail}`,
						sqlError: detail,
					},);
				} else {
					body = `SQL query failed: ${detail}\n${error.body}`;
				}
			} catch {
				body = `SQL query failed: ${detail}\n${error.body}`;
			}
			throw new DataikuError(error.status, error.statusText, body, error.retry,);
		}
	}
	throw error;
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
		return { queryId, schema, columns: schema, rows, };
	}

	private async resolveDatasetQueryFallback(
		opts: SqlQueryOptions,
	): Promise<SqlQueryOptions | null> {
		const datasetFullName = opts.datasetFullName;
		if (!datasetFullName) return null;

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
			if (!isUnsupportedSqlDatasetConnectionError(error,)) withSqlErrorContext(error,);
			const retryOpts = await this.resolveDatasetQueryFallback(queryOpts,);
			if (!retryOpts) {
				throw new ClientValidationError(
					buildUnsupportedSqlDatasetConnectionMessage(queryOpts.datasetFullName,),
				);
			}
			try {
				return await this.executeQuery(retryOpts,);
			} catch (retryError) {
				withSqlErrorContext(retryError,);
			}
		}
	}
}
