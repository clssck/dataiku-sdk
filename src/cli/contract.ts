import { readFileSync, } from "node:fs";
import { join, } from "node:path";
import { validateCredentials, } from "../auth.js";
import { getCredentialsPath, saveCredentials, } from "../config.js";
import { DataikuError, } from "../errors.js";
import {
	jobBuildTargetTypeFromFlags,
	json,
	jsonInput,
	num,
	parseBooleanOption,
	parseJsonObject,
	requiredJsonInput,
	schemaColumnsInput,
	textInput,
} from "./coerce.js";
import { commands, } from "./commands/index.js";
import { dataikuEnvironmentEnabled, } from "./env.js";
import { BOOLEAN_FLAGS, FLAG_ALIASES, KNOWN_LONG_FLAGS, } from "./flags.js";
import { flowZoneColor, flowZoneMoveItems, flowZoneName, } from "./helpers/flow-zone.js";
import { recipeBackupPath, recipeRunShouldWait, } from "./helpers/recipe.js";
import { encodedProjectEndpointForPlan, planResult, } from "./output.js";
import { resolveCredentials, resolveTlsSettings, } from "./runtime.js";
import type {
	CommandFlagChoice,
	CommandMeta,
	CommandPayloadSchema,
	CommandRegistryOverride,
} from "./types.js";
import { requireArgs, UsageError, } from "./usage.js";
import {
	AGENT_CONTRACT_SCHEMA_ID,
	AGENT_CONTRACT_VERSION,
	cliVersionResult,
	JSON_SCHEMA_DRAFT,
} from "./version.js";

function codeEnvWait(flags: Record<string, string | boolean>,): boolean {
	return flags["no-wait"] !== true;
}

function codeEnvParams(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const params = json(flags["params"],) ?? jsonInput(flags,) ?? {};
	if (typeof flags["python-interpreter"] === "string") {
		params.pythonInterpreter = flags["python-interpreter"];
	}
	return params;
}

function splitPackageSpec(raw: string,): string[] {
	return raw.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0);
}

function codeEnvPackageList(flags: Record<string, string | boolean>,): string[] {
	const packages: string[] = [];
	if (typeof flags["file"] === "string") {
		packages.push(...splitPackageSpec(readFileSync(flags["file"], "utf-8",),),);
	}
	if (typeof flags["packages"] === "string") {
		packages.push(...splitPackageSpec(flags["packages"],),);
	}
	if (typeof flags["package"] === "string") {
		packages.push(...splitPackageSpec(flags["package"],),);
	}
	if (packages.length === 0) {
		throw new UsageError(
			"--packages, --package, or --file is required. Use newline-separated package specs for version constraints.",
		);
	}
	return packages;
}

export const AUTH_ACTIONS: Record<string, {
	handler: (flags: Record<string, string | boolean>,) => Promise<unknown>;
	usage: string;
	description?: string;
	examples?: string[];
	requiredFlags?: string[];
}> = {
	login: {
		handler: async (flags,) => {
			const tlsSettings = resolveTlsSettings(flags,);
			const useEnv = dataikuEnvironmentEnabled();
			const url = typeof flags["url"] === "string"
				? flags["url"]
				: useEnv
					? process.env.DATAIKU_URL ?? ""
					: "";
			const apiKey = typeof flags["api-key"] === "string"
				? flags["api-key"]
				: useEnv
					? process.env.DATAIKU_API_KEY ?? ""
					: "";
			const projectKey = typeof flags["project-key"] === "string"
				? flags["project-key"]
				: useEnv
					? process.env.DATAIKU_PROJECT_KEY
					: undefined;

			if (!url || !apiKey) {
				throw new UsageError(
					"Missing --url and/or --api-key for auth login.",
					"missing_required_flag",
					"Pass --url and --api-key, or set DATAIKU_URL and DATAIKU_API_KEY.",
					{ requiredFlags: ["url", "api-key",], env: ["DATAIKU_URL", "DATAIKU_API_KEY",], },
				);
			}

			const result = await validateCredentials(url, apiKey, tlsSettings,);
			if (!result.valid) {
				if (result.dataikuError) throw result.dataikuError;
				throw new DataikuError(
					0,
					"Authentication Failed",
					result.error ?? "Credential validation failed",
				);
			}

			const path = getCredentialsPath();
			saveCredentials({ url, apiKey, projectKey, ...tlsSettings, },);
			return { saved: true, path, };
		},
		usage: "dss auth login --url URL --api-key KEY [--project-key KEY] [--insecure] [--ca-cert PATH]",
		description: "Validate and save DSS credentials from flags or environment variables.",
		examples: [
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY",
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ",
		],
		requiredFlags: ["url", "api-key",],
	},
};

export type CommandSideEffect = "read" | "write" | "auth";
export type CommandOutputShape = "object" | "array" | "string" | "void";
export type CommandDestructiveLevel = "none" | "reversible" | "destructive";
export type CommandAsyncKind = "none" | "job" | "future";
export type CommandIdempotency = "safe" | "convergent" | "if-not-exists" | "if-exists" | "none";

export interface CommandInputContract {
	stdin?: boolean;
	dataFlag?: boolean;
	dataFileFlag?: boolean;
}

export interface CommandExitCodes {
	ok: 0;
	usage: 1;
	error: 2;
	transient: 3;
	longRunningFailure?: 4;
}

export type CommandFlagMetadata = {
	name: string;
	kind: "boolean" | "value";
	valueType?: string;
	enumValues?: string[];
	aliases?: string[];
};

export interface CommandStructuredExample {
	shell: string;
	argv?: string[];
	payload?: unknown;
}

export interface CommandUnsafeOutput {
	condition: string;
	kind: "raw-stdout" | "local-file";
	detail: string;
	safeAlternative?: string;
}

export interface CommandAgentSchemas {
	argv: Record<string, unknown>;
	input?: Record<string, unknown>;
	output: Record<string, unknown>;
}

export interface CommandRegistryEntry {
	resource: string;
	action: string;
	usage: string;
	description?: string;
	examples?: string[];
	structuredExamples: CommandStructuredExample[];
	flags: CommandFlagMetadata[];
	positionals: string[];
	sideEffect: CommandSideEffect;
	requiresAuth: boolean;
	requiresProject: boolean;
	outputShape: CommandOutputShape;
	inputContract: CommandInputContract;
	destructive: CommandDestructiveLevel;
	producesLocalFile: boolean;
	mutatesDss: boolean;
	async: CommandAsyncKind;
	idempotency: CommandIdempotency;
	dryRun: boolean;
	requiredFlags: string[];
	requiredOneOf?: CommandFlagChoice[];
	optionalFlags: string[];
	payloadSchema?: CommandPayloadSchema;
	schemas: CommandAgentSchemas;
	unsafeOutputs?: CommandUnsafeOutput[];
	examplePayload?: unknown;
	cleanupCommand?: string;
	exitCodes: CommandExitCodes;
	cleanupHint?: string;
	agentContractVersion: number;
}

const READ_ACTIONS = new Set([
	"cat",
	"contents",
	"deployment-settings",
	"deployment-status",
	"diff",
	"download",
	"download-code",
	"flow",
	"get",
	"get-rule",
	"get-definition",
	"get-jupyter",
	"get-payload",
	"get-sql",
	"graph",
	"history-sql",
	"history",
	"infer",
	"list",
	"last-results",
	"list-jupyter",
	"list-sql",
	"log",
	"log-url",
	"map",
	"metadata",
	"peek",
	"source",
	"summary",
	"wait",
	"watch",
	"preview",
	"query",
	"schema",
	"schemas",
	"sessions-jupyter",
	"status",
	"rules",
	"settings",
	"settings-get",
	"status-by-partition",
	"usages",
],);

const PROJECT_SCOPED_RESOURCES = new Set([
	"data-quality",
	"dashboard",
	"dataset",
	"flow-zone",
	"insight",
	"folder",
	"fixtures",
	"job",
	"notebook",
	"recipe",
	"scenario",
	"sql",
	"variable",
	"wiki",
],);

const GLOBAL_AGENT_FLAGS = ["json", "verbose", "fields",];
const AUTHENTICATED_AGENT_FLAGS = [
	"url",
	"api-key",
	"request-timeout",
	"retries",
	"insecure",
	"ca-cert",
];
export const COMMANDS_USAGE = "dss commands run [--json]";
const COMMANDS_DESCRIPTION = "Print the machine-readable command registry for agent planning.";
const COMMANDS_EXAMPLES = ["dss commands run", "dss commands run --json",];
export const AGENT_CONTRACT_USAGE = "dss agent contract";
const AGENT_CONTRACT_DESCRIPTION =
	"Print the versioned JSON contract agents should use to drive dss.";
const AGENT_CONTRACT_EXAMPLES = ["dss agent contract",];
const VERSION_USAGE = "dss version";
const VERSION_DESCRIPTION = "Print the CLI version and git revision as JSON.";
const VERSION_EXAMPLES = ["dss version", "dss --version",];
const INSTALL_SKILL_USAGE =
	"dss install-skill [--global] [--agent NAME] [--target PATH] [--list-agents] [--dry-run] [--plan]";
const INSTALL_SKILL_DESCRIPTION = "Install the dataiku-dss agent skill for detected coding agents.";
const INSTALL_SKILL_EXAMPLES = [
	"dss install-skill --list-agents",
	"dss install-skill --agent omp --dry-run",
];
export const CLEANUP_USAGE = "dss cleanup --file PATH [--dry-run|--apply] [--continue-on-error]";
const CLEANUP_DESCRIPTION = "Replay cleanup ledger entries in reverse order.";
const CLEANUP_EXAMPLES = [
	"dss cleanup --file cleanup.jsonl",
	"dss cleanup --file cleanup.jsonl --apply",
];
const FIXTURES_USAGE = "dss fixtures [--json] [--project-key KEY] [--allow-types CSV]";
const FIXTURES_DESCRIPTION = "Discover safe live-test fixtures for agent workflows.";
const FIXTURES_EXAMPLES = [
	"dss fixtures --json",
	"dss fixtures --json --allow-types Filesystem,Inline",
];

const ALLOWED_CLEANUP_ACTIONS: ReadonlySet<string> = new Set([
	// Must mirror every cleanup.argv shape emitted by cleanupLedgerEntry().
	"dataset delete",
	"recipe delete",
	"scenario delete",
	"flow-zone delete",
	"wiki delete",
	"dashboard delete",
	"insight delete",
	"data-quality delete-rule",
	"code-env delete",
	"folder delete-file",
],);

export function isAllowedCleanupAction(resource: string, action: string,): boolean {
	return ALLOWED_CLEANUP_ACTIONS.has(`${resource} ${action}`,);
}

function uniqueStrings(values: string[],): string[] {
	return [...new Set(values,),];
}

function flagKind(name: string,): "boolean" | "value" {
	return BOOLEAN_FLAGS.has(name,) ? "boolean" : "value";
}

function registryKey(resource: string, action: string,): string {
	return `${resource}.${action}`;
}

function splitShellLike(input: string,): string[] | undefined {
	const tokens: string[] = [];
	let current = "";
	let quote: "'" | '"' | undefined;
	for (let index = 0; index < input.length; index++) {
		const char = input[index]!;
		if (quote) {
			if (char === quote) {
				quote = undefined;
			} else if (char === "\\" && quote === '"' && index + 1 < input.length) {
				current += input[++index]!;
			} else {
				current += char;
			}
			continue;
		}
		if (char === "'" || char === '"') {
			quote = char;
			continue;
		}
		if (/\s/.test(char,)) {
			if (current.length > 0) {
				tokens.push(current,);
				current = "";
			}
			continue;
		}
		if (char === "|" || char === "<" || char === ">" || char === ";" || char === "&") {
			return undefined;
		}
		current += char;
	}
	if (quote) return undefined;
	if (current.length > 0) tokens.push(current,);
	return tokens;
}

function exampleArgv(example: string,): string[] | undefined {
	const tokens = splitShellLike(example,);
	if (!tokens || tokens[0] !== "dss") return undefined;
	return tokens.slice(1,);
}

function structuredExamples(
	examples: string[] | undefined,
	examplePayload: unknown,
): CommandStructuredExample[] {
	return (examples ?? []).map((shell,) => {
		const argv = exampleArgv(shell,);
		return {
			shell,
			...(argv ? { argv, } : {}),
			...(examplePayload !== undefined && shell.includes("--data",)
				? { payload: examplePayload, }
				: {}),
		};
	},);
}

function outputJsonSchema(shape: CommandOutputShape,): Record<string, unknown> {
	if (shape === "array") return { type: "array", items: true, };
	if (shape === "string") return { type: "string", };
	return { type: "object", additionalProperties: true, };
}

function payloadJsonSchema(
	payloadSchema: CommandPayloadSchema | undefined,
): Record<string, unknown> | undefined {
	if (!payloadSchema) return undefined;
	return payloadSchema.jsonShape === "array"
		? { type: "array", items: true, }
		: { type: "object", additionalProperties: true, };
}

function argvJsonSchema(
	resource: string,
	action: string,
	_flags: CommandFlagMetadata[],
	_positionals: string[],
	_requiredFlags: string[],
	_requiredOneOf: CommandFlagChoice[],
): Record<string, unknown> {
	return {
		type: "object",
		additionalProperties: true,
		required: ["argv",],
		properties: {
			argv: { type: "array", items: { type: "string", }, minItems: 1, },
			resource: { const: resource, },
			action: { const: action, },
		},
	};
}

function unsafeOutputs(
	resource: string,
	action: string,
	flags: CommandFlagMetadata[],
	producesLocalFile: boolean,
): CommandUnsafeOutput[] | undefined {
	const outputs: CommandUnsafeOutput[] = [];
	if (flags.some((flag,) => flag.name === "raw")) {
		outputs.push({
			condition: "--raw without --output",
			kind: "raw-stdout",
			detail: `${resource} ${action} can intentionally write raw payload bytes/text instead of JSON.`,
			safeAlternative: flags.some((flag,) => flag.name === "output")
				? "Pass --output PATH so stdout remains a JSON string containing the path."
				: "Omit --raw when a JSON stdout value is required.",
		},);
	}
	if (producesLocalFile) {
		outputs.push({
			condition: "--output or --output-file",
			kind: "local-file",
			detail: "Command writes bytes to a local path and returns JSON metadata on stdout.",
		},);
	}
	return outputs.length > 0 ? outputs : undefined;
}

export function buildCommandSchemas(
	resource: string,
	action: string,
	flags: CommandFlagMetadata[],
	positionals: string[],
	requiredFlags: string[],
	requiredOneOf: CommandFlagChoice[],
	payloadSchema: CommandPayloadSchema | undefined,
	outputShape: CommandOutputShape,
): CommandAgentSchemas {
	return {
		argv: argvJsonSchema(resource, action, flags, positionals, requiredFlags, requiredOneOf,),
		...(payloadSchema ? { input: payloadJsonSchema(payloadSchema,), } : {}),
		output: outputJsonSchema(outputShape,),
	};
}

const EXPLICIT_REGISTRY_OVERRIDES: Record<string, CommandRegistryOverride> = {
	"dashboard.create": {
		examplePayload: { pages: [], },
	},
	"dashboard.update": {
		examplePayload: { name: "Updated dashboard", },
	},
	"data-quality.create-rule": {
		examplePayload: {
			type: "RecordCountInRangeRule",
			softMinimum: 1,
			softMinimumEnabled: true,
			displayName: "Has rows",
		},
	},
	"data-quality.update-rule": {
		examplePayload: { enabled: false, },
	},
	"dataset.update": {
		examplePayload: { tags: ["production",], },
	},
	"insight.create": {
		examplePayload: {
			name: "Agent insight",
			type: "chart",
			listed: false,
			params: {},
		},
	},
	"insight.update": {
		examplePayload: { listed: false, },
	},
	"recipe.update": {
		examplePayload: { recipe: { params: {}, }, },
	},
	"scenario.update": {
		examplePayload: { active: false, },
	},
	"wiki.update": {
		examplePayload: { article: { name: "Updated article", }, },
	},
};

function extractUsageFlags(usage: string,): string[] {
	const flags: string[] = [];
	for (const match of usage.matchAll(/--([a-z0-9-]+)/g,)) {
		flags.push(FLAG_ALIASES[match[1]!] ?? match[1]!,);
	}
	return uniqueStrings(flags,).filter((flag,) => KNOWN_LONG_FLAGS.has(flag,));
}

function extractPositionals(usage: string,): string[] {
	return uniqueStrings([...usage.matchAll(/<([^>]+)>/g,),].map((match,) => match[1]),);
}

function inferSideEffect(resource: string, action: string,): CommandSideEffect {
	if (resource === "auth") return "auth";
	if (
		resource === "agent" || resource === "doctor" || resource === "commands"
		|| resource === "fixtures"
		|| resource === "version"
	) {
		return "read";
	}
	if (resource === "install-skill") return "write";
	if (resource === "data-quality" && action === "compute") return "write";
	if (READ_ACTIONS.has(action,)) return "read";
	if (
		/^(create|clone|restore|update|delete|set|save|upload|run|build|abort|move|refresh|clear|unload|install|login|logout|add|remove|publish|activate|deploy|import|export|preload|upgrade|start|stop|restart|duplicate|put|rename|reply|compute|organize)/
			.test(action,)
		// Compound actions whose mutating verb is a suffix (e.g. permissions-set, dataset-compute).
		|| /-(set|compute)$/.test(action,)
	) {
		return "write";
	}
	return "read";
}

function inferRequiresAuth(resource: string,): boolean {
	return resource !== "agent"
		&& resource !== "auth"
		&& resource !== "commands"
		&& resource !== "install-skill"
		&& resource !== "version";
}

export function inferRequiresProject(resource: string, _action: string, usage: string,): boolean {
	if (
		resource === "agent" || resource === "auth" || resource === "doctor" || resource === "commands"
		|| resource === "install-skill" || resource === "version"
	) {
		return false;
	}
	if (PROJECT_SCOPED_RESOURCES.has(resource,)) return true;
	return usage.includes("--project-key",);
}

const ARRAY_OUTPUT_ACTIONS = new Set([
	"history",
	"find",
	"infer",
	"last-results",
	"list",
	"list-jupyter",
	"list-sql",
	"project-timeline",
	"rules",
	"schemas",
	"sessions-jupyter",
	"usages",
],);

const STRING_OUTPUT_ACTIONS = new Set([
	"diff",
	"download",
	"download-code",
	"get-payload",
	"cat",
	"log",
	"log-url",
],);

function inferOutputShape(resource: string, action: string,): CommandOutputShape {
	if (
		resource === "agent" || resource === "auth" || resource === "commands"
		|| resource === "install-skill"
		|| resource === "version"
	) {
		return "object";
	}
	if (/^list(-|$)/.test(action,)) return "array";
	if (ARRAY_OUTPUT_ACTIONS.has(action,)) return "array";
	if (resource === "dataset" && action === "download") return "object";
	if (resource === "project-library" && action === "get") return "string";
	if (STRING_OUTPUT_ACTIONS.has(action,)) return "string";
	return "object";
}

export function inferInputContract(usage: string,): CommandInputContract {
	return {
		...(usage.includes("--stdin",) ? { stdin: true, } : {}),
		...(usage.includes("--data ",) || usage.includes("--data JSON",) ? { dataFlag: true, } : {}),
		...(usage.includes("--data-file",) ? { dataFileFlag: true, } : {}),
	};
}

function stripOptionalUsageGroups(usage: string,): string {
	return usage.replace(/\[[^\]]*\]/g, " ",);
}

function stripAllUsageGroups(usage: string,): string {
	return usage.replace(/\[[^\]]*\]/g, " ",).replace(/\([^)]*\)/g, " ",);
}

function topLevelParenGroups(usage: string,): string[] {
	const groups: string[] = [];
	let depth = 0;
	let current = "";
	for (const char of usage) {
		if (char === "(") {
			if (depth > 0) current += char;
			else current = "";
			depth++;
		} else if (char === ")") {
			depth--;
			if (depth === 0) groups.push(current,);
			else current += char;
		} else if (depth > 0) {
			current += char;
		}
	}
	return groups;
}

function splitTopLevelChoices(group: string,): string[] {
	const parts: string[] = [];
	let depth = 0;
	let current = "";
	for (const char of group) {
		if (char === "[" || char === "(") depth++;
		else if (char === "]" || char === ")") depth--;
		if (char === "|" && depth === 0) {
			parts.push(current,);
			current = "";
		} else {
			current += char;
		}
	}
	parts.push(current,);
	return parts;
}

function flagsInUsageFragment(fragment: string,): string[] {
	return extractUsageFlags(fragment.replace(/\[[^\]]*\]/g, " ",),);
}

/**
 * Split required usage flags into unconditional flags and mutually-exclusive
 * choice groups. A required `(--a X | --b Y)` group becomes a requiredOneOf entry
 * (pick exactly one alternative; an alternative listing several flags must be
 * supplied together) instead of marking every flag as unconditionally required.
 */
function deriveRequiredUsage(
	usage: string,
): { requiredFlags: string[]; requiredOneOf: CommandFlagChoice[]; } {
	const requiredFlags = extractUsageFlags(stripAllUsageGroups(usage,),);
	const requiredOneOf: CommandFlagChoice[] = [];
	for (const group of topLevelParenGroups(usage,)) {
		const alternatives = splitTopLevelChoices(group,);
		if (alternatives.length <= 1) {
			requiredFlags.push(...flagsInUsageFragment(group,),);
			continue;
		}
		const oneOf = alternatives
			.map((alternative,) => flagsInUsageFragment(alternative,))
			.filter((alternativeFlags,) => alternativeFlags.length > 0);
		if (oneOf.length > 1) requiredOneOf.push({ oneOf, },);
		else if (oneOf.length === 1) requiredFlags.push(...oneOf[0]!,);
	}
	return { requiredFlags: uniqueStrings(requiredFlags,), requiredOneOf, };
}

const GLOBAL_FLAG_VALUE_HINTS: Record<string, { valueType: string; enumValues?: string[]; }> = {
	url: { valueType: "URL", },
	fields: { valueType: "CSV", },
	"api-key": { valueType: "KEY", },
	"request-timeout": { valueType: "MS", },
	retries: { valueType: "N", },
	"ca-cert": { valueType: "PATH", },
	"project-key": { valueType: "KEY", },
	"record-cleanup": { valueType: "PATH", },
};

/** Derive a value placeholder (and enum members) for each value flag from its usage token. */
function extractFlagValueHints(
	usage: string,
): Map<string, { valueType: string; enumValues?: string[]; }> {
	const hints = new Map<string, { valueType: string; enumValues?: string[]; }>();
	for (const match of usage.matchAll(/--([a-z0-9-]+)\s+([a-z]+(?:\|[a-z]+)+)/g,)) {
		const flag = FLAG_ALIASES[match[1]!] ?? match[1]!;
		if (!hints.has(flag,)) {
			hints.set(flag, { valueType: "enum", enumValues: match[2]!.split("|",), },);
		}
	}
	for (const match of usage.matchAll(/--([a-z0-9-]+)\s+(<[^>]+>|[A-Z][A-Za-z0-9_]*)/g,)) {
		const flag = FLAG_ALIASES[match[1]!] ?? match[1]!;
		if (!hints.has(flag,)) hints.set(flag, { valueType: match[2]!, },);
	}
	return hints;
}

function inferPayloadSchema(
	inputContract: CommandInputContract,
): CommandPayloadSchema | undefined {
	if (!inputContract.stdin && !inputContract.dataFlag && !inputContract.dataFileFlag) {
		return undefined;
	}
	return { ...inputContract, jsonShape: "object", };
}

function inferExitCodes(asyncKind: CommandAsyncKind,): CommandExitCodes {
	return {
		ok: 0,
		usage: 1,
		error: 2,
		transient: 3,
		...(asyncKind !== "none" ? { longRunningFailure: 4 as const, } : {}),
	};
}

function cleanupCommandFromDeleteUsage(resource: string, action: string,): string | undefined {
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	const deleteAction = action === "create-rule" ? "delete-rule" : "delete";
	const deleteUsage = commands[resource]?.[deleteAction]?.usage;
	if (!deleteUsage) return undefined;
	const base = stripOptionalUsageGroups(deleteUsage,).replace(/\s+/g, " ",).trim();
	if (deleteUsage.includes("--if-exists",)) return `${base} --if-exists`;
	return base;
}

export function supportsCleanupLedger(resource: string, action: string,): boolean {
	return cleanupCommandFromDeleteUsage(resource, action,) !== undefined
		|| `${resource}.${action}` === "folder.upload";
}

function inferDestructiveLevel(
	sideEffect: CommandSideEffect,
	action: string,
): CommandDestructiveLevel {
	if (sideEffect !== "write") return "none";
	if (/^(delete|abort|clear|unload|logout)/.test(action,)) return "destructive";
	return "reversible";
}

function inferAsyncKind(resource: string, action: string,): CommandAsyncKind {
	if (
		resource === "job" && ["build", "build-and-wait", "wait", "monitor", "watch",].includes(action,)
	) {
		return "job";
	}
	if (resource === "recipe" && action === "run") return "job";
	if (resource === "future" && ["get", "peek", "wait", "abort",].includes(action,)) return "future";
	if (resource === "scenario" && ["run", "run-and-wait", "status",].includes(action,)) {
		return "future";
	}
	if (resource === "data-quality" && action === "compute") return "future";
	if (resource === "code" && action === "run") return "future";
	return "none";
}

function inferIdempotency(
	sideEffect: CommandSideEffect,
	action: string,
	usage: string,
): CommandIdempotency {
	if (sideEffect === "read") return "safe";
	if (action.startsWith("create",) && usage.includes("--if-not-exists",)) return "if-not-exists";
	if (action.startsWith("delete",) && usage.includes("--if-exists",)) return "if-exists";
	if (/^(clear|refresh|set|save)/.test(action,)) return "convergent";
	return "none";
}

export function inferCleanupHint(resource: string, action: string,): string | undefined {
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	const deleteAction = action === "create-rule" ? "delete-rule" : "delete";
	const deleteUsage = commands[resource]?.[deleteAction]?.usage;
	const ifExists = deleteUsage?.includes("--if-exists",) ? " --if-exists" : "";
	if (resource === "code-env") {
		return `Delete with \`dss code-env delete <lang> <name>${ifExists}\`.`;
	}
	if (resource === "data-quality") {
		return `Delete with \`dss data-quality delete-rule <dataset> <rule-id>${ifExists}\`.`;
	}
	return `Delete with \`dss ${resource} delete <id>${ifExists}\` when the created object is disposable.`;
}

function buildRegistryEntry(
	resource: string,
	action: string,
	meta: CommandMeta,
): CommandRegistryEntry {
	const requiresAuth = inferRequiresAuth(resource,);
	const requiresProject = inferRequiresProject(resource, action, meta.usage,);
	const sideEffect = inferSideEffect(resource, action,);
	const destructive = inferDestructiveLevel(sideEffect, action,);
	const asyncKind = inferAsyncKind(resource, action,);
	const mutatesDss = sideEffect === "write" && resource !== "auth" && resource !== "install-skill";
	const supportsPlan = mutatesDss || sideEffect === "write";
	const supportsCleanup = supportsCleanupLedger(resource, action,);
	const usageFlags = extractUsageFlags(meta.usage,);
	const flags = uniqueStrings([
		...usageFlags,
		...(supportsPlan ? ["plan",] : []),
		...(supportsCleanup ? ["record-cleanup",] : []),
		...GLOBAL_AGENT_FLAGS,
		...(requiresAuth ? AUTHENTICATED_AGENT_FLAGS : []),
		...(requiresProject ? ["project-key",] : []),
	],);
	const derivedRequired = deriveRequiredUsage(meta.usage,);
	const requiredFlags = meta.requiredFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.requiredFlags
		?? derivedRequired.requiredFlags;
	const requiredOneOf = meta.requiredOneOf
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.requiredOneOf
		?? derivedRequired.requiredOneOf;
	const oneOfFlags = new Set(requiredOneOf.flatMap((choice,) => choice.oneOf.flat()),);
	const optionalFlags = meta.optionalFlags
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.optionalFlags
		?? flags.filter((flag,) => !requiredFlags.includes(flag,) && !oneOfFlags.has(flag,));
	const valueHints = extractFlagValueHints(meta.usage,);
	const inputContract = inferInputContract(meta.usage,);
	const cleanupHint = inferCleanupHint(resource, action,);
	const payloadSchema = meta.payloadSchema
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.payloadSchema
		?? inferPayloadSchema(inputContract,);
	const examplePayload = meta.examplePayload
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.examplePayload;
	const cleanupCommand = meta.cleanupCommand
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.cleanupCommand
		?? cleanupCommandFromDeleteUsage(resource, action,);
	const flagMetadata: CommandFlagMetadata[] = flags.map((name,) => {
		const aliases = Object.entries(FLAG_ALIASES,)
			.filter(([raw, canonical,],) =>
				canonical === name && new RegExp(`--${raw}(?![a-z0-9-])`,).test(meta.usage,)
			)
			.map(([raw,],) => raw);
		const aliasPart = aliases.length > 0 ? { aliases, } : {};
		const kind = flagKind(name,);
		if (kind === "boolean") return { name, kind, ...aliasPart, };
		const hint = valueHints.get(name,) ?? GLOBAL_FLAG_VALUE_HINTS[name];
		if (!hint) return { name, kind, ...aliasPart, };
		return {
			name,
			kind,
			valueType: hint.valueType,
			...(hint.enumValues ? { enumValues: hint.enumValues, } : {}),
			...aliasPart,
		};
	},);
	const positionals = extractPositionals(meta.usage,);
	const outputShape = inferOutputShape(resource, action,);
	const producesLocalFile = meta.usage.includes("--output PATH",)
		|| meta.usage.includes("--output-file PATH",);
	const uniqueRequiredFlags = uniqueStrings(requiredFlags,);
	const uniqueOptionalFlags = uniqueStrings(optionalFlags,);
	const unsafe = unsafeOutputs(resource, action, flagMetadata, producesLocalFile,);
	return {
		resource,
		action,
		usage: meta.usage,
		description: meta.description,
		examples: meta.examples,
		structuredExamples: structuredExamples(meta.examples, examplePayload,),
		flags: flagMetadata,
		positionals,
		sideEffect,
		requiresAuth,
		requiresProject,
		outputShape,
		inputContract,
		destructive,
		producesLocalFile,
		mutatesDss,
		async: asyncKind,
		idempotency: inferIdempotency(sideEffect, action, meta.usage,),
		dryRun: meta.usage.includes("--dry-run",),
		requiredFlags: uniqueRequiredFlags,
		optionalFlags: uniqueOptionalFlags,
		...(requiredOneOf.length > 0 ? { requiredOneOf, } : {}),
		...(payloadSchema ? { payloadSchema, } : {}),
		schemas: buildCommandSchemas(
			resource,
			action,
			flagMetadata,
			positionals,
			uniqueRequiredFlags,
			requiredOneOf,
			payloadSchema,
			outputShape,
		),
		...(unsafe ? { unsafeOutputs: unsafe, } : {}),
		...(examplePayload !== undefined ? { examplePayload, } : {}),
		...(cleanupCommand ? { cleanupCommand, } : {}),
		exitCodes: inferExitCodes(asyncKind,),
		...(cleanupHint ? { cleanupHint, } : {}),
		agentContractVersion: AGENT_CONTRACT_VERSION,
	};
}

export function buildCommandRegistry(): Record<string, Record<string, CommandRegistryEntry>> {
	const registry: Record<string, Record<string, CommandRegistryEntry>> = {};
	for (const [resource, actions,] of Object.entries(commands,)) {
		registry[resource] = {};
		for (const [action, meta,] of Object.entries(actions,)) {
			registry[resource][action] = buildRegistryEntry(resource, action, meta,);
		}
	}
	registry.commands = {
		run: buildRegistryEntry("commands", "run", {
			handler: async () => undefined,
			usage: COMMANDS_USAGE,
			description: COMMANDS_DESCRIPTION,
			examples: COMMANDS_EXAMPLES,
		},),
	};
	registry.agent = {
		contract: buildRegistryEntry("agent", "contract", {
			handler: async () => undefined,
			usage: AGENT_CONTRACT_USAGE,
			description: AGENT_CONTRACT_DESCRIPTION,
			examples: AGENT_CONTRACT_EXAMPLES,
		},),
	};
	registry.version = {
		run: buildRegistryEntry("version", "run", {
			handler: async () => undefined,
			usage: VERSION_USAGE,
			description: VERSION_DESCRIPTION,
			examples: VERSION_EXAMPLES,
		},),
	};
	registry["install-skill"] = {
		run: buildRegistryEntry("install-skill", "run", {
			handler: async () => undefined,
			usage: INSTALL_SKILL_USAGE,
			description: INSTALL_SKILL_DESCRIPTION,
			examples: INSTALL_SKILL_EXAMPLES,
		},),
	};
	registry.cleanup = {
		run: buildRegistryEntry("cleanup", "run", {
			handler: async () => undefined,
			usage: CLEANUP_USAGE,
			description: CLEANUP_DESCRIPTION,
			examples: CLEANUP_EXAMPLES,
		},),
	};
	registry.fixtures = {
		run: buildRegistryEntry("fixtures", "run", {
			handler: async () => undefined,
			usage: FIXTURES_USAGE,
			description: FIXTURES_DESCRIPTION,
			examples: FIXTURES_EXAMPLES,
		},),
	};
	registry.batch = {
		run: buildRegistryEntry("batch", "run", {
			handler: async () => undefined,
			usage: BATCH_USAGE,
			description: BATCH_DESCRIPTION,
			examples: BATCH_EXAMPLES,
			examplePayload: BATCH_EXAMPLE_PAYLOAD,
			payloadSchema: { stdin: true, dataFlag: true, dataFileFlag: true, jsonShape: "array", },
		},),
	};
	registry.auth = {};
	for (const [action, meta,] of Object.entries(AUTH_ACTIONS,)) {
		registry.auth[action] = buildRegistryEntry("auth", action, {
			handler: async () => undefined,
			usage: meta.usage,
			description: meta.description,
			examples: meta.examples,
			requiredFlags: meta.requiredFlags,
		},);
	}
	return registry;
}

function commandFlagJsonSchema(): Record<string, unknown> {
	return {
		type: "object",
		required: ["name", "kind",],
		additionalProperties: false,
		properties: {
			name: { type: "string", },
			kind: { enum: ["boolean", "value",], },
			valueType: { type: "string", },
			enumValues: { type: "array", items: { type: "string", }, },
			aliases: { type: "array", items: { type: "string", }, },
		},
	};
}

function commandRegistryEntryJsonSchema(): Record<string, unknown> {
	return {
		type: "object",
		required: [
			"resource",
			"action",
			"usage",
			"flags",
			"positionals",
			"sideEffect",
			"requiresAuth",
			"requiresProject",
			"outputShape",
			"inputContract",
			"schemas",
			"exitCodes",
			"agentContractVersion",
		],
		additionalProperties: true,
		properties: {
			resource: { type: "string", },
			action: { type: "string", },
			usage: { type: "string", },
			description: { type: "string", },
			examples: { type: "array", items: { type: "string", }, },
			structuredExamples: { type: "array", items: { type: "object", additionalProperties: true, }, },
			flags: { type: "array", items: commandFlagJsonSchema(), },
			positionals: { type: "array", items: { type: "string", }, },
			sideEffect: { enum: ["read", "write", "auth",], },
			requiresAuth: { type: "boolean", },
			requiresProject: { type: "boolean", },
			outputShape: { enum: ["object", "array", "string", "void",], },
			inputContract: { type: "object", additionalProperties: { type: "boolean", }, },
			destructive: { enum: ["none", "reversible", "destructive",], },
			producesLocalFile: { type: "boolean", },
			mutatesDss: { type: "boolean", },
			async: { enum: ["none", "job", "future",], },
			idempotency: { enum: ["safe", "convergent", "if-not-exists", "if-exists", "none",], },
			dryRun: { type: "boolean", },
			requiredFlags: { type: "array", items: { type: "string", }, },
			optionalFlags: { type: "array", items: { type: "string", }, },
			schemas: { type: "object", additionalProperties: true, },
			unsafeOutputs: { type: "array", items: { type: "object", additionalProperties: true, }, },
			exitCodes: { type: "object", additionalProperties: { type: "number", }, },
			agentContractVersion: { const: AGENT_CONTRACT_VERSION, },
		},
	};
}

function commandRegistryJsonSchema(): Record<string, unknown> {
	return {
		$schema: JSON_SCHEMA_DRAFT,
		type: "object",
		additionalProperties: {
			type: "object",
			additionalProperties: commandRegistryEntryJsonSchema(),
		},
	};
}

function errorEnvelopeJsonSchema(): Record<string, unknown> {
	return {
		type: "object",
		required: ["type", "ok", "error", "code", "category", "exitCode",],
		additionalProperties: true,
		properties: {
			type: { const: "error", },
			ok: { const: false, },
			error: { type: "string", },
			code: { type: "string", },
			category: { enum: ["usage", "dss", "internal",], },
			exitCode: { type: "number", },
			resource: { type: "string", },
			action: { type: "string", },
			projectKey: { type: "string", },
		},
	};
}

function warningEventJsonSchema(): Record<string, unknown> {
	return {
		type: "object",
		required: ["type", "warnings",],
		additionalProperties: false,
		properties: {
			type: { const: "warning", },
			warnings: { type: "array", items: { type: "object", additionalProperties: true, }, },
		},
	};
}

function traceEventJsonSchema(): Record<string, unknown> {
	return {
		type: "object",
		required: ["type", "phase", "method", "url", "attempt", "maxAttempts",],
		additionalProperties: false,
		properties: {
			type: { const: "trace", },
			phase: { enum: ["request", "response", "error",], },
			method: { type: "string", },
			url: { type: "string", },
			attempt: { type: "number", },
			maxAttempts: { type: "number", },
			status: { type: "number", },
			elapsedMs: { type: "number", },
			detail: { type: "string", },
		},
	};
}

export function agentContractJsonSchema(): Record<string, unknown> {
	return {
		$schema: JSON_SCHEMA_DRAFT,
		$id: AGENT_CONTRACT_SCHEMA_ID,
		type: "object",
		required: ["protocol", "agentContractVersion", "cli", "commands", "schemas", "stdio",],
		additionalProperties: true,
		properties: {
			protocol: { const: "dataiku-sdk-agent", },
			agentContractVersion: { const: AGENT_CONTRACT_VERSION, },
			cli: { type: "object", additionalProperties: true, },
			commands: { type: "object", additionalProperties: true, },
			schemas: { type: "object", additionalProperties: true, },
			stdio: { type: "object", additionalProperties: true, },
		},
	};
}

function commandActionSummary(
	registry: Record<string, Record<string, CommandRegistryEntry>>,
): Record<string, string[]> {
	const summary: Record<string, string[]> = {};
	for (const [resource, actions,] of Object.entries(registry,)) {
		summary[resource] = Object.keys(actions,).sort();
	}
	return summary;
}

export function buildAgentContract(): Record<string, unknown> {
	const registry = buildCommandRegistry();
	return {
		protocol: "dataiku-sdk-agent",
		agentContractVersion: AGENT_CONTRACT_VERSION,
		cli: cliVersionResult(),
		commands: {
			discoveryCommand: "dss commands run",
			actions: commandActionSummary(registry,),
		},
		schemas: {
			agentContract: agentContractJsonSchema(),
			commandRegistry: commandRegistryJsonSchema(),
			commandRegistryEntry: commandRegistryEntryJsonSchema(),
			errorEnvelope: errorEnvelopeJsonSchema(),
			warningEvent: warningEventJsonSchema(),
			traceEvent: traceEventJsonSchema(),
		},
		stdio: {
			stdout: {
				success: "single-json-value",
				rawEscapeHatches: [
					"recipe get-payload --raw without --output",
					"recipe cat --raw without --output",
				],
			},
			stderr: {
				format: "jsonl",
				events: ["warning", "trace", "error",],
				error: "single-final-error-event-on-nonzero-exit",
			},
		},
		planning: {
			discoveryCommand: "dss commands run",
			contractCommand: AGENT_CONTRACT_USAGE,
			mutatingCommandsAdvertisePlan: true,
		},
		compatibility: {
			fieldsAreAdditiveWithinMajor: true,
			failFastWhenUnsupported: "Check agentContractVersion before planning commands.",
		},
	};
}

function exitCodesOnFailure(entry: CommandRegistryEntry,): Record<string, number> {
	return {
		usage: entry.exitCodes.usage,
		error: entry.exitCodes.error,
		transient: entry.exitCodes.transient,
		...(entry.exitCodes.longRunningFailure !== undefined
			? { longRunningFailure: entry.exitCodes.longRunningFailure, }
			: {}),
	};
}

function projectKeyForPlan(
	entry: CommandRegistryEntry,
	flags: Record<string, string | boolean>,
): string | undefined {
	if (!entry.requiresProject) return undefined;
	const projectKey = resolveCredentials(flags,).projectKey;
	if (projectKey) return projectKey;
	throw new UsageError(
		`Missing project key. Pass --project-key or set DATAIKU_PROJECT_KEY before planning ${entry.resource} ${entry.action}.`,
	);
}

function requiredPlanFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string {
	const value = flags[name];
	if (typeof value === "string" && value.trim().length > 0) return value;
	throw new UsageError(`--${name} is required. Usage: ${usage}`,);
}

function optionalJsonFlag(
	flags: Record<string, string | boolean>,
	name: string,
): Record<string, unknown> | undefined {
	const value = flags[name];
	return typeof value === "string" ? parseJsonObject(value, `--${name}`,) : undefined;
}

function requiredPlanJsonInput(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	return requiredJsonInput(flags, `--data, --data-file, or --stdin is required. Usage: ${usage}`,);
}

function requiredPlanPositionals(usage: string,): string[] {
	return [...stripOptionalUsageGroups(usage,).matchAll(/<([^>]+)>/g,),].map((match,) => match[1]!);
}

function dataQualityEndpoint(projectKey: string, datasetName: string, suffix: string,): string {
	return encodedProjectEndpointForPlan(
		projectKey,
		`/datasets/${encodeURIComponent(datasetName,)}/data-quality${suffix}`,
	);
}

function querySuffix(params: Record<string, string | number | boolean | undefined>,): string {
	const search = new URLSearchParams();
	for (const [key, value,] of Object.entries(params,)) {
		if (value !== undefined) search.set(key, String(value,),);
	}
	const raw = search.toString();
	return raw ? `?${raw}` : "";
}

function jobBuildPayload(
	target: string,
	projectKey: string,
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const targetType = jobBuildTargetTypeFromFlags(flags,);
	const partition = flags["partition"] as string | undefined;
	const output: Record<string, unknown> = { projectKey, id: target, type: targetType, };
	if (targetType === "DATASET") {
		if (partition !== undefined) output.partition = partition;
	} else {
		output.targetManagedFolderProjectKey = projectKey;
		output.targetManagedFolder = target;
		output.targetPartition = partition ?? "NP";
	}
	const payload: Record<string, unknown> = {
		outputs: [output,],
		type: (flags["build-mode"] as string | undefined) ?? "NON_RECURSIVE_FORCED_BUILD",
	};
	if (flags["force-rebuild"] === true && targetType === "DATASET") {
		payload.autoUpdateSchemaBeforeEachRecipeRun = true;
	}
	return payload;
}

export function commandPlanShape(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
	projectKey: string | undefined,
): {
	endpoint?: string;
	identifiers?: Record<string, unknown>;
	method?: string;
	payload?: unknown;
	localWrites?: unknown;
	wait?: unknown;
} {
	const projectEndpoint = (suffix: string,) => {
		if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
		return encodedProjectEndpointForPlan(projectKey, suffix,);
	};
	const id = args[0];
	switch (`${resource}.${action}`) {
		case "wiki.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/wiki/",),
				identifiers: { name, },
				payload: {
					projectKey,
					name,
					parent: flags["parent"] as string | undefined ?? null,
					content: textInput(flags,),
				},
			};
		}
		case "wiki.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
				payload: {
					...jsonInput(flags,),
					name: flags["name"] as string | undefined,
					content: textInput(flags,),
				},
			};
		case "wiki.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/wiki/${encodeURIComponent(id,)}`,),
				identifiers: { article: id, },
			};
		case "dashboard.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/dashboards/",),
				identifiers: { name, },
				payload: { ...(jsonInput(flags,) ?? { pages: [], }), name, },
			};
		}
		case "dashboard.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: { ...jsonInput(flags,), name: flags["name"] as string | undefined, },
			};
		case "dashboard.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "insight.create": {
			const data = jsonInput(flags,);
			const name = flags["name"] as string | undefined;
			const type = flags["type"] as string | undefined;
			if (!data && (!name || !type)) {
				throw new UsageError(
					"--data or both --name and --type are required. Usage: dss insight create --name NAME --type TYPE",
				);
			}
			const prototype: Record<string, unknown> = { ...data, };
			if (name !== undefined) prototype.name = name;
			if (type !== undefined) prototype.type = type;
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			if (listed !== undefined) prototype.listed = listed;
			const params = optionalJsonFlag(flags, "params",);
			if (params !== undefined) prototype.params = params;
			return {
				method: "POST",
				endpoint: projectEndpoint("/insights/",),
				identifiers: { name, type, },
				payload: {
					insightPrototype: prototype,
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		}
		case "insight.update":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: {
					insight: {
						...jsonInput(flags,),
						name: flags["name"] as string | undefined,
						listed: parseBooleanOption(flags["listed"], "--listed",),
						params: optionalJsonFlag(flags, "params",),
					},
					contentType: flags["content-type"] as string | undefined,
					payload: textInput(flags,),
				},
			};
		case "insight.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/insights/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "data-quality.create-rule":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(projectKey!, args[0], "/rules",),
				identifiers: { dataset: args[0], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.update-rule":
			return {
				method: "PUT",
				endpoint: dataQualityEndpoint(projectKey!, args[0], `/rules/${encodeURIComponent(args[1],)}`,),
				identifiers: { dataset: args[0], ruleId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-quality.delete-rule":
			return {
				method: "DELETE",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/rules/${encodeURIComponent(args[1],)}${querySuffix({ ruleId: args[1], },)}`,
				),
				identifiers: { dataset: args[0], ruleId: args[1], },
			};
		case "data-quality.compute":
			return {
				method: "POST",
				endpoint: dataQualityEndpoint(
					projectKey!,
					args[0],
					`/actions/compute-rules${querySuffix({
						partition: (flags["partition"] as string | undefined) ?? "NP",
						ruleId: flags["rule-id"] as string | undefined,
					},)
					}`,
				),
				identifiers: { dataset: args[0], ruleId: flags["rule-id"] as string | undefined, },
				wait: flags["wait"] === true,
			};
		case "future.abort":
			return {
				method: "POST",
				endpoint: `/public/api/futures/${encodeURIComponent(id,)}/abort`,
				identifiers: { id, },
			};
		case "flow-zone.create": {
			const name = flowZoneName(flags["name"],);
			const payload = { name, color: flowZoneColor(flags["color"],), projectKey, };
			return {
				method: "POST",
				endpoint: projectEndpoint("/flow/zones",),
				identifiers: { name, },
				payload,
			};
		}
		case "flow-zone.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
				payload: {
					name: typeof flags["name"] === "string" ? flowZoneName(flags["name"],) : undefined,
					color: flowZoneColor(flags["color"],),
					projectKey,
				},
			};
		case "flow-zone.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "flow-zone.move":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/flow/zones/${encodeURIComponent(id,)}/add-items`,),
				identifiers: { id, },
				payload: flowZoneMoveItems(flags,),
			};
		case "dataset.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			const dsType = requiredPlanFlag(flags, "type", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/",),
				identifiers: { name, },
				payload: { datasetName: name, connection, dsType, projectKey, },
			};
		}
		case "dataset.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "dataset.refresh-schema": {
			const columns = schemaColumnsInput(flags, entry.usage,);
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}/schema`,),
				identifiers: { name: id, },
				payload: { columns, },
			};
		}
		case "dataset.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "recipe.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "recipe.create": {
			const type = requiredPlanFlag(flags, "type", entry.usage,);
			const outputDataset = flags["output"] as string | undefined;
			const outputFolder = flags["output-folder"] as string | undefined;
			if (outputDataset && outputFolder) {
				throw new UsageError("--output and --output-folder are mutually exclusive.",);
			}
			if (!outputDataset && !outputFolder) {
				throw new UsageError("--output or --output-folder is required.",);
			}
			if (outputFolder && !flags["output-connection"]) {
				throw new UsageError("--output-connection is required when using --output-folder.",);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/recipes/",),
				identifiers: { name: flags["name"] as string | undefined, },
				payload: {
					type,
					name: flags["name"] as string | undefined,
					inputDatasets: flags["input"] ? [flags["input"] as string,] : undefined,
					outputDataset,
					outputFolder,
					outputConnection: flags["output-connection"] as string | undefined,
					projectKey,
				},
			};
		}
		case "recipe.run":
			return {
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { recipe: id, },
				payload: {
					recipe: id,
					outputResolution: "dynamic",
					projectKey,
					partition: flags["partition"] as string | undefined,
				},
				wait: recipeRunShouldWait(flags,),
			};
		case "recipe.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "recipe.set-payload": {
			const file = requiredPlanFlag(flags, "file", entry.usage,);
			const backupDir = flags["no-backup"] === true
				? undefined
				: (flags["backup-dir"] as string | undefined)
				?? join(process.cwd(), ".dss-backups", "recipes",);
			const backupPath = backupDir ? recipeBackupPath(id, backupDir,) : undefined;
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: {
					file,
					content: textInput(flags,),
					...(backupPath ? { backupPath, } : {}),
				},
				...(backupPath
					? { localWrites: [{ path: backupPath, source: "remote recipe backup", before: "PUT", },], }
					: {}),
			};
		}
		case "job.build":
		case "job.build-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { target: id, },
				payload: jobBuildPayload(id, projectKey!, flags,),
				wait: action === "build-and-wait" || flags["wait"] === true,
			};
		case "job.abort":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/jobs/${encodeURIComponent(id,)}/abort/`,),
				identifiers: { id, },
			};
		case "scenario.run":
		case "scenario.run-and-wait":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/run/`,),
				identifiers: { id, },
				payload: {},
				wait: action === "run-and-wait" || flags["wait"] === true,
			};
		case "scenario.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
			};
		case "scenario.create":
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: { id: args[0], name: args[1], },
				payload: {
					id: args[0],
					name: args[1],
					projectKey,
					type: (flags["type"] as string | undefined) ?? "step_based",
				},
			};
		case "scenario.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const type = requiredPlanFlag(flags, "type", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			return {
				method: "POST",
				endpoint: projectEndpoint("/managedfolders/",),
				identifiers: { name, },
				payload: { name, type, connection, path: flags["path"] as string | undefined, projectKey, },
			};
		}
		case "folder.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "folder.delete":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/managedfolders/${encodeURIComponent(id,)}`,),
				identifiers: { folder: id, },
			};
		case "folder.upload":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], localPath: args[2], },
			};
		case "folder.delete-file":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(
					`/managedfolders/${encodeURIComponent(args[0],)}/contents/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { folder: args[0], path: args[1], },
			};
		case "variable.set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/variables/",),
				payload: {
					standard: optionalJsonFlag(flags, "standard",),
					local: optionalJsonFlag(flags, "local",),
					replace: flags["replace"] === true,
				},
			};
		case "code-env.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/code-envs/",
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					envLang: args[0],
					envName: args[1],
					deploymentMode: requiredPlanFlag(flags, "deployment-mode", entry.usage,),
					params: codeEnvParams(flags,),
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.set-definition":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)
					}`,
				identifiers: { lang: args[0], name: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "code-env.set-packages":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)
					}/packages`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					packages: codeEnvPackageList(flags,),
					installCorePackages: parseBooleanOption(
						flags["install-core-packages"],
						"--install-core-packages",
					),
				},
			};
		case "code-env.update-packages":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)
					}/packages/actions/update`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					forceRebuildEnv: flags["force-rebuild"] === true,
					versionToUpdate: flags["env-version"] as string | undefined,
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.set-jupyter":
			return {
				method: "POST",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)
					}/jupyter`,
				identifiers: { lang: args[0], name: args[1], },
				payload: {
					active: parseBooleanOption(flags["active"], "--active",),
					wait: codeEnvWait(flags,),
				},
				wait: codeEnvWait(flags,),
			};
		case "code-env.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)
					}`,
				identifiers: { lang: args[0], name: args[1], },
				wait: codeEnvWait(flags,),
			};
		case "notebook.save-jupyter":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "notebook.delete-jupyter":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "notebook.clear-jupyter-outputs":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
				payload: { clearOutputs: true, },
			};
		case "notebook.unload-jupyter":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/jupyter-notebooks/${encodeURIComponent(args[0],)}/sessions/${encodeURIComponent(args[1],)
					}/unload`,
				),
				identifiers: { name: args[0], sessionId: args[1], },
			};
		case "notebook.save-sql":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "notebook.delete-sql":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "notebook.clear-sql-history":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}/history`,),
				identifiers: { id, cellId: flags["cell-id"] as string | undefined, },
				payload: { retain: num(flags["retain"],), },
			};
		case "project.create": {
			const settings = jsonInput(flags,) ?? null;
			return {
				method: "POST",
				endpoint: "/public/api/projects/",
				identifiers: { projectKey: args[0], name: args[1], },
				payload: {
					projectKey: args[0],
					name: args[1],
					owner: (flags["owner"] as string | undefined) ?? null,
					settings,
					description: null,
					permissions: [],
					tags: [],
				},
			};
		}
		case "project.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/projects/${encodeURIComponent(id,)}${querySuffix({
					clearManagedDatasets: flags["drop-data"] === true,
					clearOutputManagedFolders: false,
					clearJobAndScenarioLogs: true,
					wait: true,
				},)
					}`,
				identifiers: { projectKey: id, },
			};
		case "project.duplicate": {
			const options = jsonInput(flags,);
			return {
				method: "POST",
				endpoint: `/public/api/projects/${encodeURIComponent(args[0],)}/duplicate/`,
				identifiers: { sourceKey: args[0], targetKey: args[1], targetName: args[2], },
				payload: {
					targetProjectName: args[2],
					targetProjectKey: args[1],
					duplicationMode: (options?.duplicationMode as string | undefined) ?? "MINIMAL",
					exportAnalysisModels: (options?.exportAnalysisModels as boolean | undefined) ?? true,
					exportSavedModels: (options?.exportSavedModels as boolean | undefined) ?? true,
					exportGitRepository: options?.exportGitRepository ?? null,
					exportInsightsData: (options?.exportInsightsData as boolean | undefined) ?? true,
					remapping: options?.remapping ?? {},
					...(options?.targetProjectFolderId !== undefined
						? { targetProjectFolderId: options.targetProjectFolderId, }
						: {}),
				},
			};
		}
		case "project.export": {
			const output = requiredPlanFlag(flags, "output", entry.usage,);
			return {
				method: "POST",
				endpoint: `/public/api/projects/${encodeURIComponent(id,)}/export`,
				identifiers: { projectKey: id, output, },
				payload: jsonInput(flags,) ?? {},
				localWrites: [{ path: output, source: "remote project archive", after: "POST", },],
			};
		}
		case "project.import":
			return {
				method: "POST",
				endpoint: "/public/api/projects/import/upload",
				identifiers: { filePath: id, },
				payload: {
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: id,
					fileName: "tmp-import.zip",
				},
			};
		case "project.settings-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/settings",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project.permissions-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint("/permissions",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "continuous-activity.start":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/continuous-activities/${encodeURIComponent(id,)}/start`,),
				identifiers: { recipeId: id, },
				payload: jsonInput(flags,) ?? {},
			};
		case "metrics.dataset-compute":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/datasets/${encodeURIComponent(id,)}/actions/computeMetrics?partition=`,
				),
				identifiers: { dataset: id, },
			};
		case "flow-zone.organize":
			return {
				method: "POST",
				endpoint: projectEndpoint("/flow/zones",),
				payload: jsonInput(flags,),
			};
		default:
			return {
				method: action.startsWith("delete",) || action === "abort" ? "DELETE" : "POST",
				endpoint: projectKey
					? projectEndpoint(`/${resource}s/${id ? encodeURIComponent(id,) : ""}`,)
					: undefined,
				identifiers: id ? { id, } : undefined,
				payload: jsonInput(flags,),
			};
	}
}

export function buildMutationPlan(
	resource: string,
	action: string,
	meta: CommandMeta,
	args: string[],
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const entry = buildRegistryEntry(resource, action, meta,);
	if (!entry.mutatesDss && entry.sideEffect !== "write") {
		throw new UsageError(`--plan is only supported for mutating commands. Usage: ${meta.usage}`,);
	}
	const requiredPositionals = requiredPlanPositionals(meta.usage,);
	requireArgs(args, requiredPositionals.length, meta.usage,);
	const projectKey = projectKeyForPlan(entry, flags,);
	const shape = commandPlanShape(resource, action, args, flags, entry, projectKey,);
	return planResult(resource, action, {
		...shape,
		asyncKind: entry.async,
		exitCodesOnFailure: exitCodesOnFailure(entry,),
		idempotency: entry.idempotency,
		plannedAndDryRun: flags["dry-run"] === true,
	},);
}

export const BATCH_USAGE =
	"dss batch (--data JSON|--data-file PATH|--stdin) [--continue-on-error] [--dry-run]";
const BATCH_DESCRIPTION =
	"Run a sequence of dss commands from a JSON array of argv arrays. Fail-fast by default; returns one envelope with a per-step ok/result/error and exits non-zero if any step failed.";
export const BATCH_HINT =
	'Pass a JSON array of argv arrays, e.g. [["dataset","list"],["recipe","update","r","--data-file","p.json"]].';
export const BATCH_EXAMPLE_PAYLOAD: string[][] = [
	["recipe", "set-payload", "compute_orders", "--file", "code.py", "--no-backup",],
	["recipe", "update", "compute_orders", "--data-file", "env.json",],
	["dataset", "update", "orders", "--data-file", "ds.json",],
];
const BATCH_EXAMPLES = [
	"dss batch --data-file steps.json",
	"dss batch --stdin --continue-on-error",
];
