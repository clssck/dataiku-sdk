import { buildCommandRegistry, commandActionSummary, } from "../src/cli/contract.js";
import type { CaseResult, } from "./live-context.js";

/** Explicit reviewed inventory. New actions must be classified, not silently absorbed by a resource wildcard. */
export const LIVE_COVERAGE_CATALOGUE: Record<
	string,
	{ profile: string; reason: string; actions: readonly string[]; }
> = {
	"project": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"duplicate",
			"export",
			"flow",
			"get",
			"import",
			"inspect-archive",
			"list",
			"map",
			"metadata",
			"permissions-get",
			"permissions-set",
			"settings-get",
			"settings-set",
		],
	},
	"analysis": {
		"profile": "ml",
		"reason":
			"Requires a DSS Visual ML license and training runtime; uncovered actions need an asserted lifecycle scenario.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
		],
	},
	"ml-task": {
		"profile": "ml",
		"reason":
			"Requires a DSS Visual ML license and training runtime; uncovered actions need an asserted lifecycle scenario.",
		"actions": [
			"create",
			"delete",
			"deploy",
			"get-settings",
			"list-models",
			"model-details",
			"set-settings",
			"status",
			"train",
		],
	},
	"saved-model": {
		"profile": "ml",
		"reason":
			"Requires a DSS Visual ML license and training runtime; uncovered actions need an asserted lifecycle scenario.",
		"actions": [
			"delete",
			"get",
			"list",
			"list-versions",
			"set-active",
			"version-details",
		],
	},
	"model-evaluation-store": {
		"profile": "ml",
		"reason":
			"Requires a DSS Visual ML license and training runtime; uncovered actions need an asserted lifecycle scenario.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"list-evaluations",
		],
	},
	"app": {
		"profile": "applications",
		"reason":
			"Requires the relevant application/plugin capability; external templates must be explicitly configured and read-only.",
		"actions": [
			"business-app-instance-permissions",
			"compare-manifest",
			"create-instance",
			"create-successor-instance",
			"delete-instance",
			"instance-manifest",
			"instances",
			"list",
			"manifest",
			"manifest-version",
			"permissions-diff",
			"permissions-restore",
			"permissions-snapshot",
			"save-instance-manifest",
			"set-manifest-version",
			"successor-preflight",
			"validate-manifest",
			"verify-instance",
		],
	},
	"business-app": {
		"profile": "applications",
		"reason":
			"Requires the relevant application/plugin capability; external templates must be explicitly configured and read-only.",
		"actions": [
			"create-instance",
			"get",
			"install-from-archive",
			"instances",
			"list",
			"save-settings",
			"settings",
			"upgrade-instance",
		],
	},
	"webapp": {
		"profile": "applications",
		"reason":
			"Requires the relevant application/plugin capability; external templates must be explicitly configured and read-only.",
		"actions": [
			"backend-state",
			"create",
			"get-settings",
			"list",
			"restart-backend",
			"stop-backend",
			"update-settings",
		],
	},
	"api-service": {
		"profile": "applications",
		"reason":
			"Requires the relevant application/plugin capability; external templates must be explicitly configured and read-only.",
		"actions": [
			"add-prediction-endpoint",
			"create",
			"create-package",
			"delete-package",
			"download-package",
			"get-settings",
			"list",
			"list-packages",
			"package-summary",
			"publish-package",
			"save-settings",
		],
	},
	"api-deployer": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"create-deployment",
			"create-infra",
			"create-service",
			"delete-deployment",
			"delete-infra",
			"delete-service",
			"delete-version",
			"deploy",
			"deployment-settings",
			"deployment-status",
			"get-deployment",
			"get-infra",
			"get-service",
			"list-deployments",
			"list-infras",
			"list-services",
			"list-stages",
			"publish-version",
			"save-deployment-settings",
		],
	},
	"bundle": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"activate",
			"delete-exported",
			"delete-imported",
			"download-exported",
			"export",
			"import-from-archive",
			"import-from-stream",
			"list-exported",
			"list-imported",
			"preload",
			"publish",
		],
	},
	"project-deployer": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"create-deployment",
			"create-infra",
			"create-project",
			"delete-deployment",
			"deploy",
			"deployment-status",
			"get-deployment",
			"list-deployments",
			"list-infras",
			"list-projects",
			"project-status",
			"save-deployment-settings",
			"upload-bundle",
		],
	},
	"project-git": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"add-library",
			"branches",
			"commit",
			"create-branch",
			"create-tag",
			"current-branch",
			"delete-branch",
			"delete-tag",
			"diff",
			"drop-and-rebuild",
			"fetch",
			"future-abort",
			"future-status",
			"future-wait",
			"get-remote",
			"list-libraries",
			"log",
			"pull",
			"push",
			"push-all-libraries",
			"push-library",
			"remove-library",
			"remove-remote",
			"reset-all-libraries",
			"reset-library",
			"reset-to-head",
			"reset-to-upstream",
			"revert-commit",
			"revert-to-revision",
			"set-library",
			"set-remote",
			"status",
			"switch",
			"tags",
		],
	},
	"project-library": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create-file",
			"create-folder",
			"delete",
			"diff",
			"get",
			"get-bytes",
			"list",
			"move",
			"put",
			"rename",
		],
	},
	"streaming-endpoint": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"update-settings",
		],
	},
	"continuous-activity": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"list",
			"start",
			"status",
			"stop",
		],
	},
	"statistics": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create-worksheet",
			"delete-worksheet",
			"get-worksheet",
			"list-worksheets",
			"run-card",
			"run-computation",
			"run-worksheet",
			"update-worksheet",
		],
	},
	"discussion": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"get",
			"list",
			"reply",
		],
	},
	"meaning": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"update",
		],
	},
	"workspace": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"add-object",
			"create",
			"delete",
			"get",
			"list",
			"list-objects",
			"update-settings",
		],
	},
	"metrics": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"dataset-compute",
			"dataset-get",
			"dataset-history",
			"folder-get",
		],
	},
	"doctor": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"run",
		],
	},
	"wiki": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"settings",
			"update",
		],
	},
	"dashboard": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"export",
			"get",
			"list",
			"update",
		],
	},
	"insight": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"update",
		],
	},
	"data-quality": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"assert-results",
			"compute",
			"create-rule",
			"delete-rule",
			"get-rule",
			"history",
			"last-results",
			"project-status",
			"project-timeline",
			"rules",
			"status",
			"status-by-partition",
			"update-rule",
		],
	},
	"future": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"abort",
			"get",
			"peek",
			"wait",
		],
	},
	"flow-zone": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"find",
			"get",
			"graph",
			"list",
			"move",
			"organize",
			"plan",
			"update",
		],
	},
	"dataset": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"assert-count",
			"assert-schema",
			"clear",
			"clone",
			"create",
			"delete",
			"download",
			"files",
			"get",
			"list",
			"list-partitions",
			"metadata",
			"preview",
			"refresh-schema",
			"rename",
			"schema",
			"source",
			"update",
			"upload-file",
			"validate-build",
		],
	},
	"recipe": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"add-input",
			"assert-unchanged",
			"cat",
			"clone",
			"create",
			"delete",
			"diff",
			"download",
			"download-code",
			"get",
			"get-payload",
			"list",
			"remove-input",
			"restore",
			"run",
			"set-payload",
			"update",
			"validate-graph",
		],
	},
	"job": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"abort",
			"build",
			"build-and-wait",
			"get",
			"list",
			"log",
			"log-url",
			"monitor",
			"summary",
			"wait",
			"watch",
		],
	},
	"scenario": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"create",
			"delete",
			"get",
			"list",
			"run",
			"run-and-wait",
			"status",
			"update",
		],
	},
	"folder": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"contents",
			"create",
			"delete",
			"delete-file",
			"download",
			"get",
			"list",
			"update",
			"upload",
		],
	},
	"variable": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"get",
			"set",
		],
	},
	"connection": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"infer",
			"list",
			"schemas",
			"tables",
		],
	},
	"code-env": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"create",
			"delete",
			"get",
			"get-definition",
			"get-log",
			"list",
			"list-logs",
			"set-definition",
			"set-jupyter",
			"set-packages",
			"update-images",
			"update-packages",
			"usages",
			"version",
		],
	},
	"sql": {
		"profile": "infrastructure",
		"reason":
			"Requires explicit instance privileges or configured external infrastructure; the core project does not authorize global mutations.",
		"actions": [
			"query",
		],
	},
	"code": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"run",
		],
	},
	"notebook": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"clear-jupyter-outputs",
			"clear-sql-history",
			"delete-jupyter",
			"delete-sql",
			"get-jupyter",
			"get-sql",
			"history-sql",
			"list-jupyter",
			"list-sql",
			"save-jupyter",
			"save-sql",
			"sessions-jupyter",
			"unload-jupyter",
		],
	},
	"commands": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"run",
		],
	},
	"agent": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"contract",
		],
	},
	"version": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"run",
		],
	},
	"install-skill": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"run",
		],
	},
	"cleanup": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"run",
		],
	},
	"fixtures": {
		"profile": "core",
		"reason":
			"Project-scoped behavioral coverage requires a passing scenario; discovery and plans alone do not qualify.",
		"actions": [
			"run",
		],
	},
	"batch": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"run",
		],
	},
	"auth": {
		"profile": "offline",
		"reason":
			"Local configuration/protocol behavior belongs to the hermetic suite; no live execution is claimed.",
		"actions": [
			"login",
		],
	},
};
export interface CoverageRow {
	id: string;
	resource: string;
	action: string;
	profile: string;
	status: "passed" | "failed" | "blocked" | "unsupported" | "uncovered";
	reason: string;
	caseIds: string[];
	executed: boolean;
}
export function checkLiveCoverageCatalogue(
	summary: Record<string, string[]> = commandActionSummary(buildCommandRegistry(),),
): void {
	const actual = new Set(
		Object.entries(summary,).flatMap(([resource, actions,],) =>
			actions.map(action => `${resource}.${action}`)
		),
	);
	const classified = new Set(
		Object.entries(LIVE_COVERAGE_CATALOGUE,).flatMap(([resource, entry,],) =>
			entry.actions.map(action => `${resource}.${action}`)
		),
	);
	const added = [...actual,].filter(id => !classified.has(id,));
	const removed = [...classified,].filter(id => !actual.has(id,));
	if (added.length || removed.length) {
		throw new Error(
			`Live coverage catalogue drift: unclassified=${added.sort().join(",",)}; obsolete=${
				removed.sort().join(",",)
			}`,
		);
	}
}
export function liveCoverage(cases: CaseResult[], enabledProfiles: string[],): CoverageRow[] {
	const rows: CoverageRow[] = [];
	for (const [resource, entry,] of Object.entries(LIVE_COVERAGE_CATALOGUE,)) {
		for (const action of entry.actions) {
			const id = `${resource}.${action}`;
			const relevant = cases.filter(item => item.actions.includes(id,));
			const executed = relevant.some(item => item.executedActions.includes(id,));
			const failed = relevant.find(item => item.status === "failed");
			const passing = relevant.find(item =>
				item.status === "passed" && item.executedActions.includes(id,)
			);
			const limited = relevant.find(item =>
				item.status === "blocked" || item.status === "unsupported"
			);
			const status = failed ? "failed" : passing ? "passed" : limited ? limited.status : "uncovered";
			const reason = failed?.error
				?? (passing
					? "Successful non-preview CLI invocation inside a passing behavioral case."
					: limited?.error ?? (!enabledProfiles.includes(entry.profile,)
						? `Profile not selected: ${entry.profile}. ${entry.reason}`
						: entry.reason));
			rows.push({
				id,
				resource,
				action,
				profile: entry.profile,
				status,
				reason,
				caseIds: relevant.map(item => item.id),
				executed,
			},);
		}
	}
	return rows.sort((a, b,) => a.id.localeCompare(b.id,));
}
