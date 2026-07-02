import { deepMerge, } from "../../utils/deep-merge.js";
import { num, parseBooleanOption, requiredJsonInput, } from "../coerce.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const dataQualityCommands: Record<string, CommandMeta> = {
	rules: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality rules <dataset>",);
			return c.dataQuality.listRules(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss data-quality rules <dataset> [--project-key KEY]",
		description: "List data quality rules for a dataset.",
		examples: ["dss data-quality rules orders",],
	},
	"get-rule": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, "dss data-quality get-rule <dataset> <rule-id>",);
			return c.dataQuality.getRule(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss data-quality get-rule <dataset> <rule-id> [--project-key KEY]",
		description: "Get one data quality rule by id.",
		examples: ["dss data-quality get-rule orders RULE_ID",],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality status <dataset>",);
			return c.dataQuality.status(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss data-quality status <dataset> [--project-key KEY]",
		description: "Get the aggregate data quality status for a dataset.",
		examples: ["dss data-quality status orders",],
	},
	"create-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality create-rule <dataset> --data JSON",);
			const config = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
			const pk = f["project-key"] as string | undefined;
			const identity = typeof config.id === "string"
				? config.id
				: typeof config.displayName === "string"
				? config.displayName
				: undefined;
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = identity
					? (await c.dataQuality.listRules(a[0], pk,)).find((rule,) =>
						rule.id === identity || rule.displayName === identity
					)
					: undefined;
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("data-quality", identity ?? existing.id, "exists", {
						dataset: a[0],
						current: existing,
					},);
				}
				if (f["if-not-exists"] === true && !identity && f["dry-run"] !== true) {
					throw new UsageError("--if-not-exists requires rule id or displayName in the rule JSON.",);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create-rule",
						resource: "data-quality",
						dataset: a[0],
						payload: config,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.dataQuality.createRule(a[0], {
				config,
				projectKey: pk,
			},);
			return {
				created: created.id ?? identity ?? "rule",
				dataset: a[0],
				resource: "data-quality",
				...created,
			};
		},
		usage:
			"dss data-quality create-rule <dataset> (--data JSON|--data-file PATH|--stdin) [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a data quality rule from raw rule config.",
		examples: [
			'dss data-quality create-rule orders --data \'{"type":"RecordCountInRangeRule","softMinimum":1,"softMinimumEnabled":true,"displayName":"Has rows"}\' --dry-run',
		],
	},
	"update-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss data-quality update-rule <dataset> <rule-id> --data JSON",);
			const data = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
			if (f["dry-run"] === true) {
				const current = await c.dataQuality.getRule(
					a[0],
					a[1],
					f["project-key"] as string | undefined,
				);
				const next = deepMerge(current as unknown as Record<string, unknown>, data,);
				return {
					dryRun: true,
					action: "update-rule",
					resource: "data-quality",
					dataset: a[0],
					ruleId: a[1],
					current,
					next,
				};
			}
			return c.dataQuality.updateRule(a[0], a[1], {
				data,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss data-quality update-rule <dataset> <rule-id> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Update a data quality rule via GET-before-PUT merge.",
		examples: [
			"dss data-quality update-rule orders RULE_ID --data '{\"enabled\":false}' --dry-run",
		],
	},
	"delete-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss data-quality delete-rule <dataset> <rule-id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.dataQuality.getRule(a[0], a[1], pk,));
				if (!current) return skipResult("data-quality", a[1], "missing", { dataset: a[0], },);
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "delete-rule",
						resource: "data-quality",
						dataset: a[0],
						ruleId: a[1],
						current,
					};
				}
			}
			await c.dataQuality.deleteRule(a[0], a[1], pk,);
			return { deleted: a[1], dataset: a[0], resource: "data-quality", };
		},
		usage:
			"dss data-quality delete-rule <dataset> <rule-id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a data quality rule.",
		examples: ["dss data-quality delete-rule orders RULE_ID --dry-run",],
	},
	"status-by-partition": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality status-by-partition <dataset>",);
			return c.dataQuality.statusByPartition(a[0], {
				includeAllPartitions: f["include-all-partitions"] === true,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss data-quality status-by-partition <dataset> [--include-all-partitions] [--project-key KEY]",
		description: "Get data quality status by dataset partition.",
		examples: ["dss data-quality status-by-partition orders --include-all-partitions",],
	},
	"last-results": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality last-results <dataset>",);
			return c.dataQuality.lastResults(a[0], {
				partition: f["partition"] as string | undefined,
				ruleId: f["rule-id"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss data-quality last-results <dataset> [--partition P] [--rule-id ID] [--project-key KEY]",
		description: "Get latest data quality rule results for a dataset.",
		examples: ["dss data-quality last-results orders",],
	},
	history: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality history <dataset>",);
			return c.dataQuality.history(a[0], {
				minTimestamp: num(f["min-timestamp"],),
				maxTimestamp: num(f["max-timestamp"],),
				resultsPerPage: num(f["results-per-page"],),
				page: num(f["page"],),
				ruleId: f["rule-id"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss data-quality history <dataset> [--rule-id ID] [--min-timestamp MS] [--max-timestamp MS] [--results-per-page N] [--page N] [--project-key KEY]",
		description: "Get data quality rule execution history.",
		examples: ["dss data-quality history orders --results-per-page 100",],
	},
	"project-status": {
		handler: (c, _a, f,) =>
			c.dataQuality.projectStatus({
				onlyMonitored: parseBooleanOption(f["only-monitored"], "--only-monitored",),
				projectKey: f["project-key"] as string | undefined,
			},),
		usage: "dss data-quality project-status [--only-monitored true|false] [--project-key KEY]",
		description: "Get project-level data quality status by dataset.",
		examples: ["dss data-quality project-status --only-monitored false",],
	},
	"project-timeline": {
		handler: (c, _a, f,) =>
			c.dataQuality.projectTimeline({
				minTimestamp: num(f["min-timestamp"],),
				maxTimestamp: num(f["max-timestamp"],),
				projectKey: f["project-key"] as string | undefined,
			},),
		usage:
			"dss data-quality project-timeline [--min-timestamp MS] [--max-timestamp MS] [--project-key KEY]",
		description: "Get project-level data quality timeline aggregates.",
		examples: ["dss data-quality project-timeline --min-timestamp 1714521600000",],
	},
	compute: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss data-quality compute <dataset>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				partition: f["partition"] as string | undefined,
				pollIntervalMs: num(f["poll-interval"],),
				ruleId: f["rule-id"] as string | undefined,
				projectKey: pk,
				timeoutMs: num(f["timeout"],),
			};
			if (f["dry-run"] === true) {
				const params = new URLSearchParams();
				params.set("partition", options.partition?.trim() ? options.partition : "NP",);
				if (options.ruleId !== undefined) params.set("ruleId", options.ruleId,);
				return {
					dryRun: true,
					action: "compute",
					resource: "data-quality",
					dataset: a[0],
					...options,
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/datasets/${
							encodeURIComponent(a[0],)
						}/data-quality/actions/compute-rules?${params.toString()}`,
					),
					method: "POST",
				};
			}
			if (f["wait"] === true) return c.dataQuality.computeRulesAndWait(a[0], options,);
			return c.dataQuality.computeRules(a[0], options,);
		},
		usage:
			"dss data-quality compute <dataset> [--partition P] [--rule-id ID] [--wait] [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
		description:
			"Start data quality rule computation, optionally waiting on the returned DSS future.",
		examples: [
			"dss data-quality compute orders --dry-run",
			"dss data-quality compute orders --wait",
		],
	},
};
