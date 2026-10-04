import { unknownJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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
	"dataset-run-checks": {
		handler: (c, a, f,) => {
			const data = unknownJsonInput(f,);
			const checks = data && typeof data === "object" && "checks" in data ? data.checks : undefined;
			if (data !== undefined && !Array.isArray(checks,)) {
				throw new UsageError('--data must be {"checks": [...]}.', "validation_failed",);
			}
			return c.metrics.runDatasetChecks(a[0]!, {
				projectKey: f["project-key"] as string | undefined,
				partitions: f["partitions"] as string | undefined,
				...(Array.isArray(checks,) ? { checks, } : {}),
			},);
		},
		description:
			'Run the checks configured on a dataset, or the {"checks": [...]} definitions passed as JSON, and return their outcomes.',
		examples: [
			"dss metrics dataset-run-checks customers",
			"dss metrics dataset-run-checks customers --data-file checks.json",
		],
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
