import { readFileSync, } from "node:fs";
import { mkdir, writeFile, } from "node:fs/promises";
import { dirname, resolve, } from "node:path";
import { readStdinText, stripUtf8Bom, } from "../coerce.js";
import { enqueueCliWarning, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

const SQL_QUERY_USAGE =
	"dss sql query (SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin) (--connection CONN | --dataset FULL_NAME) [--database DB] [--output PATH|--output-file PATH] [--preview N] [--start-retries N] [--request-timeout MS] [--project-key KEY]";

const DEFAULT_SQL_PREVIEW_ROWS = 5;

/**
 * Parse `--preview N` into a non-negative row count. Rejects non-integers,
 * negatives, and empty values loudly so a bad flag never silently degrades to a
 * default. `--preview 0` is valid and yields an empty preview (explicit opt-out).
 */
function parseSqlPreviewCount(value: string | boolean | undefined,): number {
	if (typeof value !== "string") {
		throw new UsageError(
			`--preview requires an integer value. Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	const trimmed = value.trim();
	const parsed = Number(trimmed,);
	if (trimmed.length === 0 || !Number.isInteger(parsed,) || parsed < 0) {
		throw new UsageError(
			`--preview must be a non-negative integer (got "${value}"). Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	return parsed;
}

function parseSqlStartRetries(value: string | boolean | undefined,): number | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string") {
		throw new UsageError(
			`--start-retries requires a positive integer. Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	const trimmed = value.trim();
	const parsed = Number(trimmed,);
	if (trimmed.length === 0 || !Number.isInteger(parsed,) || parsed < 1) {
		throw new UsageError(
			`--start-retries must be a positive integer (got "${value}"). Usage: ${SQL_QUERY_USAGE}`,
			"validation_failed",
		);
	}
	return parsed;
}

export function resolveSqlInput(args: string[], flags: Record<string, string | boolean>,): string {
	const sources: Array<{ label: string; read: () => string; }> = [];

	if (typeof flags["sql"] === "string") {
		sources.push({
			label: flags["sql"] === "-" ? "--sql -" : "--sql",
			read: () => flags["sql"] === "-" ? readStdinText() : String(flags["sql"],),
		},);
	}
	if (typeof flags["sql-file"] === "string") {
		sources.push({
			label: "--sql-file",
			read: () => readFileSync(flags["sql-file"] as string, "utf-8",),
		},);
	}
	if (flags["stdin"] === true) {
		sources.push({ label: "--stdin", read: readStdinText, },);
	}
	if (args.length > 1) {
		throw new UsageError(
			`Expected at most one positional SQL argument. Quote the SQL or use --sql-file/--stdin.\nUsage: ${SQL_QUERY_USAGE}`,
		);
	}
	if (args[0] !== undefined) {
		sources.push({ label: "positional SQL", read: () => args[0], },);
	}

	if (sources.length === 0) {
		throw new UsageError(`SQL input is required. Usage: ${SQL_QUERY_USAGE}`,);
	}
	if (sources.length > 1) {
		throw new UsageError(
			`Choose exactly one SQL input source: --sql, --sql-file, --stdin, or one positional SQL argument. Usage: ${SQL_QUERY_USAGE}`,
		);
	}

	const query = stripUtf8Bom(sources[0]!.read(),);
	if (query.trim().length === 0) {
		throw new UsageError(
			`SQL input from ${sources[0]!.label} must not be empty. Usage: ${SQL_QUERY_USAGE}`,
		);
	}
	return query;
}

export interface SqlQueryInvocation {
	query: string;
	connection?: string;
	datasetFullName?: string;
	database?: string;
	projectKey?: string;
	type: "sql";
}

export function resolveSqlQueryInvocation(
	args: string[],
	flags: Record<string, string | boolean>,
	projectKeyOverride?: string,
): SqlQueryInvocation {
	const connection = flags["connection"] as string | undefined;
	const datasetFullName = flags["dataset"] as string | undefined;
	if ((connection ? 1 : 0) + (datasetFullName ? 1 : 0) !== 1) {
		throw new UsageError(`Pass exactly one of --connection or --dataset. Usage: ${SQL_QUERY_USAGE}`,);
	}
	return {
		query: resolveSqlInput(args, flags,),
		connection,
		datasetFullName,
		database: flags["database"] as string | undefined,
		projectKey: projectKeyOverride ?? flags["project-key"] as string | undefined,
		type: "sql",
	};
}

export const sqlCommands: Record<string, CommandMeta> = {
	query: {
		handler: async (c, a, f,) => {
			const outputFile = (f["output"] as string | undefined)
				?? (f["output-file"] as string | undefined);
			const previewProvided = f["preview"] !== undefined;
			const previewCount = previewProvided
				? parseSqlPreviewCount(f["preview"],)
				: DEFAULT_SQL_PREVIEW_ROWS;
			const result = await c.sql.query({
				...resolveSqlQueryInvocation(a, f,),
				retryMaxAttempts: parseSqlStartRetries(f["start-retries"],),
			},);
			if (!outputFile && !previewProvided) return result;
			if (!outputFile) {
				const truncated = result.rows.length > previewCount;
				if (truncated) {
					enqueueCliWarning({
						code: "sql_preview_truncated",
						rowCount: result.rows.length,
						previewRows: previewCount,
						hint: "Use --output PATH to write the full result without placing it on stdout.",
					},);
				}
				return {
					queryId: result.queryId,
					schema: result.schema,
					columns: result.columns ?? result.schema,
					rowCount: result.rows.length,
					preview: result.rows.slice(0, previewCount,),
					truncated,
				};
			}

			const outputPath = resolve(outputFile,);
			await mkdir(dirname(outputPath,), { recursive: true, },);
			await writeFile(outputPath, `${JSON.stringify(result, null, 2,)}\n`, "utf-8",);
			return {
				queryId: result.queryId,
				schema: result.schema,
				columns: result.columns ?? result.schema,
				rowCount: result.rows.length,
				preview: result.rows.slice(0, previewCount,),
				outputPath,
				written: outputPath,
			};
		},
		usage: SQL_QUERY_USAGE,
		description:
			"Run potentially mutating SQL against a DSS connection or dataset. Use --preview N for bounded stdout or --output PATH for full rows. --start-retries may execute SQL more than once; use it only when repetition is safe.",
		examples: [
			"dss sql query 'SELECT * FROM orders LIMIT 10' --connection my_pg",
			"dss sql query --sql-file query.sql --connection my_pg",
			"echo 'SELECT 1' | dss sql query --stdin --dataset MYPROJ.orders",
			"dss sql query --sql-file query.sql --connection my_pg --output results.json --request-timeout 120000",
			"dss sql query --sql-file query.sql --connection my_pg --output results.json --preview 10",
			"dss sql query 'SELECT 1' --connection my_pg --start-retries 4",
		],
	},
};
