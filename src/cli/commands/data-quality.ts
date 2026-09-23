import { deepMerge, } from "../../utils/deep-merge.js";
import { num, parseBooleanOption, requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";
/**
 * A data quality result counts as passing only when it actually carries a
 * verdict and every verdict it carries reports OK. A result with no outcome
 * and no status has proven nothing, and a result whose fields disagree
 * (outcome ERROR alongside status OK) is a failure — never a pass.
 */
function resultReportsOk(result: { outcome?: string; status?: string; },): boolean {
	const verdicts = [result.outcome, result.status,]
		.filter((value,): value is string => typeof value === "string" && value.trim().length > 0)
		.map((value,) => value.trim().toUpperCase());
	return verdicts.length > 0 && verdicts.every((value,) => value === "OK");
}

export const dataQualityCommands: Record<string, CommandMeta> = withUsage("data-quality", {
	rules: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "rules",),);
			return c.dataQuality.listRules(a[0], f["project-key"] as string | undefined,);
		},
		description: "List data quality rules for a dataset.",
		examples: ["dss data-quality rules orders",],
	},
	"get-rule": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("data-quality", "get-rule",),);
			return c.dataQuality.getRule(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "Get one data quality rule by id.",
		examples: ["dss data-quality get-rule orders RULE_ID",],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "status",),);
			return c.dataQuality.status(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get the aggregate data quality status for a dataset.",
		examples: ["dss data-quality status orders",],
	},
	"create-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "create-rule",),);
			const config = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
			const pk = f["project-key"] as string | undefined;
			const identity = typeof config.id === "string"
				? config.id
				: typeof config.displayName === "string"
				? config.displayName
				: undefined;
			if (f["if-not-exists"] === true || executionMode(f,).dryRun) {
				const existing = identity
					? (await c.dataQuality.listRules(a[0], pk,)).find((rule,) =>
						rule.id === identity || rule.displayName === identity
					)
					: undefined;
				if (existing && f["if-not-exists"] === true && !executionMode(f,).dryRun) {
					return skipResult("data-quality", identity ?? existing.id, "exists", {
						dataset: a[0],
						current: existing,
					},);
				}
				if (f["if-not-exists"] === true && !identity && !executionMode(f,).dryRun) {
					throw new UsageError("--if-not-exists requires rule id or displayName in the rule JSON.",);
				}
				if (executionMode(f,).dryRun) {
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
		description: "Create a data quality rule from raw rule config.",
		examples: [
			'dss data-quality create-rule orders --data \'{"type":"RecordCountInRangeRule","softMinimum":1,"softMinimumEnabled":true,"displayName":"Has rows"}\' --dry-run',
		],
	},
	"update-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("data-quality", "update-rule",),);
			const data = requiredJsonInput(f, "--data, --data-file, or --stdin is required.",);
			if (executionMode(f,).dryRun) {
				const current = await c.dataQuality.getRule(
					a[0],
					a[1],
					f["project-key"] as string | undefined,
				);
				const next = deepMerge(current, data,);
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
		description: "Update a data quality rule via GET-before-PUT merge.",
		examples: [
			"dss data-quality update-rule orders RULE_ID --data '{\"enabled\":false}' --dry-run",
		],
	},
	"delete-rule": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("data-quality", "delete-rule",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.dataQuality.getRule(a[0], a[1], pk,));
				if (!current) return skipResult("data-quality", a[1], "missing", { dataset: a[0], },);
				if (executionMode(f,).dryRun) {
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
		description: "Delete a data quality rule.",
		examples: ["dss data-quality delete-rule orders RULE_ID --dry-run",],
	},
	"status-by-partition": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "status-by-partition",),);
			return c.dataQuality.statusByPartition(a[0], {
				includeAllPartitions: f["include-all-partitions"] === true,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get data quality status by dataset partition.",
		examples: ["dss data-quality status-by-partition orders --include-all-partitions",],
	},
	"last-results": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "last-results",),);
			return c.dataQuality.lastResults(a[0], {
				partition: f["partition"] as string | undefined,
				ruleId: f["rule-id"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get latest data quality rule results for a dataset.",
		examples: ["dss data-quality last-results orders",],
	},
	"assert-results": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "assert-results",),);
			const ruleId = f["rule-id"] as string | undefined;
			const pk = f["project-key"] as string | undefined;
			const results = await c.dataQuality.lastResults(a[0], { ruleId, projectKey: pk, },);
			const selected = results.filter((result,) =>
				ruleId === undefined || result.id === ruleId || result.ruleId === ruleId
			);
			const failed = selected
				.filter((result,) => !resultReportsOk(result,))
				.map((result,) => ({
					ruleId: result.id ?? result.ruleId ?? result.name ?? null,
					outcome: result.outcome ?? null,
					status: result.status ?? null,
				}));
			return {
				satisfied: selected.length > 0 && failed.length === 0,
				dataset: a[0],
				...(ruleId !== undefined ? { ruleId, } : {}),
				checked: selected.length,
				failed,
				...(selected.length === 0 ? { reason: "no_results", } : {}),
			};
		},
		description:
			"Assert the latest data quality results: at least one selected rule must exist and every selected result must report OK. Returns { satisfied, checked, failed } with failed rule ids/outcomes only; a failed assertion exits 4 with assertion_failed.",
		examples: [
			"dss data-quality assert-results orders",
			"dss data-quality assert-results orders --rule-id RULE_ID",
		],
	},
	history: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "history",),);
			return c.dataQuality.history(a[0], {
				minTimestamp: num(f["min-timestamp"], "--min-timestamp",),
				maxTimestamp: num(f["max-timestamp"], "--max-timestamp",),
				resultsPerPage: num(f["results-per-page"], "--results-per-page",),
				page: num(f["page"], "--page",),
				ruleId: f["rule-id"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get data quality rule execution history.",
		examples: ["dss data-quality history orders --results-per-page 100",],
	},
	"project-status": {
		handler: (c, _a, f,) =>
			c.dataQuality.projectStatus({
				onlyMonitored: parseBooleanOption(f["only-monitored"], "--only-monitored",),
				projectKey: f["project-key"] as string | undefined,
			},),
		description: "Get project-level data quality status by dataset.",
		examples: ["dss data-quality project-status --only-monitored false",],
	},
	"project-timeline": {
		handler: (c, _a, f,) =>
			c.dataQuality.projectTimeline({
				minTimestamp: num(f["min-timestamp"], "--min-timestamp",),
				maxTimestamp: num(f["max-timestamp"], "--max-timestamp",),
				projectKey: f["project-key"] as string | undefined,
			},),
		description: "Get project-level data quality timeline aggregates.",
		examples: ["dss data-quality project-timeline --min-timestamp 1714521600000",],
	},
	compute: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("data-quality", "compute",),);
			const pk = f["project-key"] as string | undefined;
			const options = {
				partition: f["partition"] as string | undefined,
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				ruleId: f["rule-id"] as string | undefined,
				projectKey: pk,
				timeoutMs: num(f["timeout"], "--timeout",),
			};
			if (executionMode(f,).dryRun) {
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
		description:
			"Start data quality rule computation, optionally waiting on the returned DSS future.",
		examples: [
			"dss data-quality compute orders --dry-run",
			"dss data-quality compute orders --wait",
		],
	},
},);
