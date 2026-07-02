import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const metricsCommands: Record<string, CommandMeta> = {
	"dataset-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss metrics dataset-get <dataset> [--project-key KEY]",);
			return c.metrics.getDatasetMetrics(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss metrics dataset-get <dataset> [--project-key KEY]",
		description: "Get the last computed metric values for a dataset.",
		examples: ["dss metrics dataset-get customers",],
	},
	"dataset-compute": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss metrics dataset-compute <dataset> [--project-key KEY]",);
			return c.metrics.computeDatasetMetrics(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss metrics dataset-compute <dataset> [--project-key KEY]",
		description: "Compute the DSS-configured metrics for a dataset.",
		examples: ["dss metrics dataset-compute customers",],
	},
	"dataset-history": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, "dss metrics dataset-history <dataset> <metricId> [--project-key KEY]",);
			return c.metrics.getDatasetMetricHistory(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss metrics dataset-history <dataset> <metricId> [--project-key KEY]",
		description: "Get the history of one dataset metric.",
		examples: ["dss metrics dataset-history customers records:COUNT_RECORDS",],
	},
	"folder-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss metrics folder-get <folderId> [--project-key KEY]",);
			return c.metrics.getFolderMetrics(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss metrics folder-get <folderId> [--project-key KEY]",
		description: "Get the last computed metric values for a managed folder.",
		examples: ["dss metrics folder-get aBcDeFgH",],
	},
};
