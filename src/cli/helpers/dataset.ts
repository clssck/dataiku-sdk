import { assertSqlDatasetSource, } from "../../resources/dataset-create.js";
import type { DatasetDetails, } from "../../schemas.js";
import { readInputText, stripUtf8Bom, } from "../coerce.js";
import { UsageError, } from "../usage.js";

export interface DatasetSqlSourceFlags {
	table?: string;
	dbSchema?: string;
	catalog?: string;
	query?: string;
}

/**
 * The SQL source of `dataset create` (`--table [--schema S] [--catalog C]` or
 * `--query SQL`/`--query-file PATH`), validated against the dataset type.
 * Empty when no SQL flag is given.
 */
export function datasetSqlSourceFromFlags(
	flags: Record<string, string | boolean>,
	dsType: string,
): DatasetSqlSourceFlags {
	const text = (flag: string,): string | undefined => {
		const value = flags[flag];
		if (value === undefined) return undefined;
		if (typeof value !== "string") {
			throw new UsageError(`--${flag} requires a value.`, "missing_required_flag",);
		}
		return value;
	};
	if (flags["query"] !== undefined && flags["query-file"] !== undefined) {
		throw new UsageError(
			"Conflicting input sources: --query, --query-file. Provide exactly one.",
			"conflicting_input_sources",
			undefined,
			{ sources: ["query", "query-file",], },
		);
	}
	const queryFile = text("query-file",);
	const inline = text("query",);
	const source: DatasetSqlSourceFlags = {
		...(text("table",) !== undefined ? { table: text("table",)!, } : {}),
		...(text("schema",) !== undefined ? { dbSchema: text("schema",)!, } : {}),
		...(text("catalog",) !== undefined ? { catalog: text("catalog",)!, } : {}),
		...(queryFile !== undefined
			? { query: stripUtf8Bom(readInputText(queryFile, "--query-file",),), }
			: inline !== undefined
			? { query: inline, }
			: {}),
	};
	assertSqlDatasetSource(dsType, source,);
	return source;
}

export function datasetSourceSummary(details: DatasetDetails,): Record<string, unknown> {
	const params = details.params ?? {};
	return {
		resource: "dataset",
		name: details.name,
		projectKey: details.projectKey,
		type: details.type,
		managed: details.managed,
		connection: params.connection ?? params.uploadConnection,
		catalog: params.catalog,
		schema: params.schema,
		table: params.table,
		path: params.path,
		folderSmartId: params.folderSmartId,
		formatType: details.formatType,
	};
}
