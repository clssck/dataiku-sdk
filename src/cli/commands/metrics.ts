import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const metricsCommands: Record<string, CommandMeta> = withUsage("metrics", {
	"dataset-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("metrics", "dataset-get",),);
			return c.metrics.getDatasetMetrics(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get the last computed metric values for a dataset.",
		examples: ["dss metrics dataset-get customers",],
	},
	"dataset-compute": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("metrics", "dataset-compute",),);
			return c.metrics.computeDatasetMetrics(a[0], f["project-key"] as string | undefined,);
		},
		description: "Compute the DSS-configured metrics for a dataset.",
		examples: ["dss metrics dataset-compute customers",],
	},
	"dataset-history": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("metrics", "dataset-history",),);
			return c.metrics.getDatasetMetricHistory(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "Get the history of one dataset metric.",
		examples: ["dss metrics dataset-history customers records:COUNT_RECORDS",],
	},
	"folder-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("metrics", "folder-get",),);
			return c.metrics.getFolderMetrics(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get the last computed metric values for a managed folder.",
		examples: ["dss metrics folder-get aBcDeFgH",],
	},
},);
