import { ClientValidationError, } from "../errors.js";

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
