import type { StableErrorCode, } from "../errors.js";
import { commandSyntaxTree, syntaxHasFlag, } from "./syntax.js";

export const RESOURCE_NAMES = [
	"agent",
	"app",
	"batch",
	"business-app",
	"webapp",
	"api-service",
	"api-deployer",
	"bundle",
	"project-deployer",
	"project-git",
	"project-folder",
	"data-collection",
	"llm",
	"knowledge-bank",
	"macro",
	"plugin",
	"user",
	"group",
	"project",
	"project-library",
	"streaming-endpoint",
	"continuous-activity",
	"statistics",
	"discussion",
	"meaning",
	"workspace",
	"metrics",
	"doctor",
	"wiki",
	"dashboard",
	"insight",
	"data-quality",
	"analysis",
	"ml-task",
	"saved-model",
	"model-evaluation-store",
	"future",
	"flow-zone",
	"dataset",
	"recipe",
	"job",
	"scenario",
	"folder",
	"variable",
	"connection",
	"code-env",
	"sql",
	"code",
	"notebook",
	"auth",
	"cleanup",
	"commands",
	"fixtures",
	"install-skill",
	"version",
]
	.sort();

const PROJECT_SCOPED_RESOURCES = new Set([
	"analysis",
	"data-quality",
	"dashboard",
	"knowledge-bank",
	"dataset",
	"llm",
	"flow-zone",
	"insight",
	"folder",
	"fixtures",
	"job",
	"notebook",
	"macro",
	"ml-task",
	"model-evaluation-store",
	"recipe",
	"scenario",
	"sql",
	"variable",
	"saved-model",
	"wiki",
],);

export function inferRequiresProject(resource: string, action: string,): boolean {
	if (
		resource === "agent" || resource === "auth" || resource === "doctor" || resource === "commands"
		|| resource === "install-skill" || resource === "version"
	) return false;
	if (resource === "sql" && action === "query") return false;
	if (PROJECT_SCOPED_RESOURCES.has(resource,)) return true;
	// Plugin project scope is optional, not a requirement.
	if (resource === "plugin") return false;
	if (resource === "project-git") return !action.startsWith("future-",);
	const syntax = commandSyntaxTree(resource, action,);
	return syntax !== undefined && syntaxHasFlag(syntax, "project-key",);
}

export class UsageError extends Error {
	readonly code: StableErrorCode;
	readonly hint?: string;
	readonly details?: Record<string, unknown>;

	constructor(
		message: string,
		code: StableErrorCode = "usage_error",
		hint?: string,
		details?: Record<string, unknown>,
	) {
		super(message,);
		this.name = "UsageError";
		this.code = code;
		this.hint = hint;
		this.details = details;
	}
}

export const COMMANDS_RUN_HINT =
	"Use `dss commands run --fields RESOURCE.ACTION` for scoped command discovery; use `dss commands run` for the action summary or `--output PATH` to export the full registry.";

export function unsupportedHelpFlag(): UsageError {
	return new UsageError(
		"Help screens are not supported.",
		"usage_error",
		COMMANDS_RUN_HINT,
		{ command: "dss commands run", },
	);
}

export function noCommandError(): UsageError {
	return new UsageError(
		"No command provided.",
		"usage_error",
		COMMANDS_RUN_HINT,
		{ command: "dss commands run", resources: RESOURCE_NAMES, },
	);
}

export function missingActionError(
	resource: string,
	validActions: string[],
	usage?: string,
): UsageError {
	return new UsageError(
		`Missing action for ${resource}.`,
		"usage_error",
		usage ?? COMMANDS_RUN_HINT,
		{ resource, validActions, },
	);
}

export function unknownResourceError(resource: string,): UsageError {
	return new UsageError(
		`Unknown resource: ${resource}.`,
		"usage_error",
		COMMANDS_RUN_HINT,
		{ resource, validResources: RESOURCE_NAMES, },
	);
}

export function unknownActionError(
	resource: string,
	action: string | undefined,
	validActions: string[],
	hint?: string,
): UsageError {
	return new UsageError(
		`Unknown action: ${resource} ${action ?? ""}`.trim(),
		"usage_error",
		hint ?? COMMANDS_RUN_HINT,
		{ resource, action, validActions, },
	);
}

export function requireArgs(args: string[], count: number, usage: string,): void {
	if (args.length < count) {
		throw new UsageError(
			`Expected ${count} argument(s), got ${args.length}.\nUsage: ${usage}`,
			"missing_required_arg",
		);
	}
}

export function requireNoArgs(args: string[], usage: string,): void {
	if (args.length > 0) {
		throw new UsageError(
			`Unexpected argument(s): ${args.join(" ",)}.\nUsage: ${usage}`,
			"usage_error",
		);
	}
}
