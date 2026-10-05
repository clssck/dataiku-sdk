import { ClientValidationError, } from "../errors.js";

/**
 * Reject dataset types the public API can create but never populate. An
 * editable (`Inline`) dataset keeps its rows in a DSS config file that no
 * public endpoint writes; without it every downstream build fails.
 */
export function assertDatasetTypeCreatable(dsType: string,): void {
	if (dsType.toLowerCase() !== "inline") return;
	throw new ClientValidationError(
		"Inline (editable) datasets cannot be created here: the DSS public API cannot write their rows, and an Inline dataset without rows fails every downstream build (SourceDatasetNotReadyException). For a small static table, create an UploadedFiles dataset and upload a CSV with dss dataset upload-file, or write the rows from a python recipe into a managed dataset.",
		"validation_failed",
	);
}

/**
 * Dataset types that read from a SQL connection. These are the types DSS
 * detects schemas for through `testAndDetectSettings/externalSQL` (the list
 * dataikuapi's `DSSDataset.autodetect_settings` uses).
 */
export const SQL_DATASET_TYPES = [
	"JDBC",
	"PostgreSQL",
	"MySQL",
	"Vertica",
	"Snowflake",
	"Redshift",
	"Greenplum",
	"Teradata",
	"Oracle",
	"SQLServer",
	"SAPHANA",
	"Netezza",
	"BigQuery",
	"Athena",
	"hiveserver2",
	"Synapse",
	"FabricWarehouse",
	"Databricks",
	"DatabricksLakebase",
] as const;

export function isSqlDatasetType(dsType: string,): boolean {
	const lower = dsType.toLowerCase();
	return SQL_DATASET_TYPES.some((type,) => type.toLowerCase() === lower);
}

/**
 * Validate the SQL source options (`table`/`dbSchema`/`catalog`/`query`) of a
 * dataset creation against its type. Shared by `dataset create`, its plan, and
 * the SDK so every entry point rejects the same combinations.
 */
export function assertSqlDatasetSource(
	dsType: string,
	source: { table?: string; dbSchema?: string; catalog?: string; query?: string; },
): void {
	const hasTable = source.table !== undefined;
	const hasQuery = source.query !== undefined;
	if (hasTable && hasQuery) {
		throw new ClientValidationError(
			"Pass either a table or a query for a SQL dataset, not both.",
			"conflicting_input_sources",
		);
	}
	if (!hasTable && (source.dbSchema !== undefined || source.catalog !== undefined)) {
		throw new ClientValidationError(
			"A schema or catalog only applies together with a table.",
			"validation_failed",
			"Add --table, or drop --schema/--catalog (a query names its own tables).",
		);
	}
	if (hasTable && source.table!.trim() === "") {
		throw new ClientValidationError("table must be a non-empty table name.", "invalid_flag_value",);
	}
	if (hasQuery && source.query!.trim() === "") {
		throw new ClientValidationError("query must be a non-empty SQL query.", "invalid_flag_value",);
	}
	if ((hasTable || hasQuery) && !isSqlDatasetType(dsType,)) {
		throw new ClientValidationError(
			`A table or query only applies to SQL dataset types, not "${dsType}".`,
			"validation_failed",
			`SQL types: ${SQL_DATASET_TYPES.join(", ",)}.`,
		);
	}
}

/** Wire body for POST /datasets/; shared with `dataset create --plan` so the plan matches the request. */
export function buildDatasetCreateBody(opts: {
	projectKey: string;
	datasetName: string;
	connection?: string;
	dsType: string;
	table?: string;
	dbSchema?: string;
	catalog?: string;
	query?: string;
	formatType?: string;
	formatParams?: Record<string, unknown>;
	managed?: boolean;
},): Record<string, unknown> {
	assertDatasetTypeCreatable(opts.dsType,);
	if (opts.dsType.toLowerCase() === "uploadedfiles") {
		return {
			projectKey: opts.projectKey,
			name: opts.datasetName,
			type: opts.dsType,
			params: opts.connection ? { uploadConnection: opts.connection, } : {},
		};
	}
	if (!opts.connection) {
		throw new ClientValidationError("connection is required unless dsType is UploadedFiles.",);
	}
	if (opts.query !== undefined) {
		if (opts.table !== undefined) {
			throw new ClientValidationError(
				"Pass either a table or a query for a SQL dataset, not both.",
				"conflicting_input_sources",
			);
		}
		if (opts.query.trim() === "") {
			throw new ClientValidationError("query must be a non-empty SQL query.", "invalid_flag_value",);
		}
		return {
			projectKey: opts.projectKey,
			name: opts.datasetName,
			type: opts.dsType,
			params: { connection: opts.connection, mode: "query", query: opts.query, },
			managed: opts.managed ?? false,
		};
	}
	if (opts.table) {
		const params: Record<string, unknown> = {
			connection: opts.connection,
			mode: "table",
			table: opts.table,
		};
		if (opts.dbSchema) params.schema = opts.dbSchema;
		if (opts.catalog) params.catalog = opts.catalog;

		return {
			projectKey: opts.projectKey,
			name: opts.datasetName,
			type: opts.dsType,
			params,
			managed: opts.managed ?? false,
		};
	}

	return {
		projectKey: opts.projectKey,
		name: opts.datasetName,
		type: opts.dsType,
		params: {
			connection: opts.connection,
			path: `/dataiku/${opts.projectKey}/${opts.datasetName}`,
		},
		formatType: opts.formatType ?? "csv",
		formatParams: opts.formatParams ?? {
			style: "excel",
			charset: "utf8",
			separator: "\t",
			quoteChar: '"',
			escapeChar: "\\",
			dateSerializationFormat: "ISO",
			arrayMapFormat: "json",
			parseHeaderRow: true,
			compress: "gz",
		},
		managed: opts.managed ?? true,
	};
}
