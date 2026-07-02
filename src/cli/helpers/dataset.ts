import type { DatasetDetails, } from "../../schemas.js";

export function datasetSourceSummary(details: DatasetDetails,): Record<string, unknown> {
	const params = details.params ?? {};
	return {
		resource: "dataset",
		name: details.name,
		projectKey: details.projectKey,
		type: details.type,
		managed: details.managed,
		connection: params.connection,
		catalog: params.catalog,
		schema: params.schema,
		table: params.table,
		path: params.path,
		folderSmartId: params.folderSmartId,
		formatType: details.formatType,
	};
}
