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

/** Wire body for POST /datasets/; shared with `dataset create --plan` so the plan matches the request. */
export function buildDatasetCreateBody(opts: {
	projectKey: string;
	datasetName: string;
	connection?: string;
	dsType: string;
	table?: string;
	dbSchema?: string;
	catalog?: string;
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
