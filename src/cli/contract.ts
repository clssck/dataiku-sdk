import * as fs from "node:fs";
import * as path from "node:path";
import { validateCredentials, } from "../auth.js";
import { getCredentialsPath, saveCredentials, } from "../config.js";
import { DataikuError, } from "../errors.js";
import { APP_MANIFEST_CONCURRENCY_CONTROL, } from "../resources/applications.js";
import {
	jobBuildTargetTypeFromFlags,
	json,
	jsonInput,
	num,
	parseBooleanOption,
	parseJsonObject,
	recipeInputDatasetsFromFlags,
	requiredJsonInput,
	rewritePairsFromFlags,
	schemaColumnsInput,
	stringField,
	textInput,
} from "./coerce.js";
import { commands, } from "./commands/index.js";
import { dataikuEnvironmentEnabled, } from "./env.js";
import { BOOLEAN_FLAGS, FLAG_ALIASES, KNOWN_LONG_FLAGS, SHORT_FLAGS, } from "./flags.js";
import { flowZoneColor, flowZoneMoveItems, flowZoneName, } from "./helpers/flow-zone.js";
import { recipeBackupPath, recipeRunShouldWait, } from "./helpers/recipe.js";
import { encodedProjectEndpointForPlan, planResult, } from "./output.js";
import { resolveTlsSettings, } from "./runtime.js";
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
		packages.push(...splitPackageSpec(fs.readFileSync(flags["file"], "utf-8",),),);
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

			const credentialsPath = getCredentialsPath();
			saveCredentials({ url, apiKey, projectKey, ...tlsSettings, },);
			return { saved: true, path: credentialsPath, };
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
	assertionFailure?: 4;
}

export type CommandFlagMetadata = {
	name: string;
	kind: "boolean" | "value";
	valueType?: string;
	enumValues?: string[];
	aliases?: string[];
	allowEmptyValue?: boolean;
};

export interface CommandStructuredExample {
	shell: string;
	argv?: string[];
	payload?: unknown;
}

export interface CommandUnsafeOutput {
	condition: string;
	kind: "local-file";
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
	"manifest-version",
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
	"verify-instance",
],);

const PROJECT_SCOPED_RESOURCES = new Set([
	"analysis",
	"data-quality",
	"dashboard",
	"dataset",
	"flow-zone",
	"insight",
	"folder",
	"fixtures",
	"job",
	"notebook",
	"ml-task",
	"model-evaluation-store",
	"recipe",
	"scenario",
	"sql",
	"variable",
	"saved-model",
	"wiki",
],);
/**
 * Project Git actions that only observe repository state. Everything else in the
 * `project-git` resource is a mutation: the verb-shaped names (`fetch`, `pull`,
 * `switch`, `commit`, `reset-*`, `revert-*`, `drop-and-rebuild`, `future-abort`)
 * match none of the mutating-verb patterns and would otherwise fall through to
 * the read default, advertising network and history writes as safe reads.
 */
const PROJECT_GIT_READ_ACTIONS: Record<string, true> = {
	branches: true,
	"current-branch": true,
	diff: true,
	"future-status": true,
	"future-wait": true,
	"get-remote": true,
	"list-libraries": true,
	log: true,
	status: true,
	tags: true,
};

/**
 * Git mutations that destroy work rather than add to it: `drop-and-rebuild`
 * discards the repository and rebuilds it from DSS, the resets and reverts throw
 * away local commits or working state, `remove-library` can delete the checked
 * out directory, and the branch/tag deletions can remove remote refs.
 */
const PROJECT_GIT_DESTRUCTIVE_ACTIONS: Record<string, true> = {
	"delete-branch": true,
	"delete-tag": true,
	"drop-and-rebuild": true,
	"future-abort": true,
	"remove-library": true,
	"reset-all-libraries": true,
	"reset-library": true,
	"reset-to-head": true,
	"reset-to-upstream": true,
	"revert-commit": true,
	"revert-to-revision": true,
};

/** Project Git actions that hand back a DSS future (`{jobId}`) instead of a result. */
const PROJECT_GIT_FUTURE_ACTIONS: Record<string, true> = {
	"add-library": true,
	"future-abort": true,
	"future-status": true,
	"future-wait": true,
	"push-all-libraries": true,
	"push-library": true,
	"reset-all-libraries": true,
	"reset-library": true,
};

/** Git mutations that converge: replaying them lands on the same repository state. */
const PROJECT_GIT_CONVERGENT_ACTIONS: Record<string, true> = {
	fetch: true,
	"reset-all-libraries": true,
	"reset-library": true,
	"reset-to-head": true,
	"reset-to-upstream": true,
};

const GLOBAL_AGENT_FLAGS = ["verbose", "fields",];
const AUTHENTICATED_AGENT_FLAGS = [
	"url",
	"api-key",
	"request-timeout",
	"retries",
	"insecure",
	"ca-cert",
];
export const COMMANDS_USAGE = "dss commands run [--fields PATHS] [--output PATH]";
const COMMANDS_DESCRIPTION =
	"Print a compact resource/action summary by default; use --fields for scoped command metadata or --output PATH to export the full registry without sending it through stdout.";
const COMMANDS_EXAMPLES = [
	"dss commands run",
	"dss commands run --fields dataset",
	"dss commands run --fields dataset.create",
	"dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples",
	"dss commands run --output commands.json",
];
export const AGENT_CONTRACT_COMMAND = "dss agent contract";
export const AGENT_CONTRACT_USAGE = "dss agent contract [--fields PATHS]";
const AGENT_CONTRACT_DESCRIPTION =
	"Print the versioned JSON agent contract; scope bootstrap fields, use commands.actions to enumerate the surface, and read schemas only when needed.";
const AGENT_CONTRACT_EXAMPLES = [
	"dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility",
	"dss agent contract --fields commands.actions",
];
const VERSION_USAGE = "dss version";
const VERSION_DESCRIPTION =
	"Print the CLI version, checkout/build revisions, load source, runtime, and stale-build status as JSON.";
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
const FIXTURES_USAGE = "dss fixtures [--project-key KEY] [--allow-types CSV]";
const FIXTURES_DESCRIPTION = "Discover safe live-test fixtures for agent workflows.";
const FIXTURES_EXAMPLES = [
	"dss fixtures",
	"dss fixtures --allow-types Filesystem,Inline",
];

const ALLOWED_CLEANUP_ACTIONS: ReadonlySet<string> = new Set([
	// Must mirror every cleanup.argv shape emitted by cleanupLedgerEntry().
	"analysis delete",
	"model-evaluation-store delete",
	"ml-task delete",
	"dataset delete",
	"recipe delete",
	"scenario delete",
	"flow-zone delete",
	"wiki delete",
	"dashboard delete",
	"insight delete",
	"data-quality delete-rule",
	"code-env delete",
	"folder delete",
	"folder delete-file",
	"project delete",
	"app delete-instance",
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
	const argv = canonicalizeArgv(tokens.slice(1,),);
	// Global flag aliases like `dss --version` have no command path to emit.
	return argv[0]?.startsWith("--",) ? undefined : argv;
}

/**
 * Fold a parsed shell argv into the machine-canonical flag form the argv
 * schemas advertise: value flags become one `--flag=VALUE` token and boolean
 * flags stay standalone, so structured examples always validate against their
 * own generated argv schema. Alias flags are rewritten to their canonical
 * name. Positional tokens and tokens after the `--` separator pass through
 * unchanged; the human-facing `shell` examples are never rewritten.
 */
function canonicalizeArgv(argv: string[],): string[] {
	const canonical: string[] = [];
	let afterSeparator = false;
	for (let index = 0; index < argv.length; index++) {
		const token = argv[index]!;
		if (afterSeparator || token === "--") {
			canonical.push(token,);
			afterSeparator = true;
			continue;
		}
		if (token.length === 2 && token[0] === "-" && token[1] !== "-") {
			const flagName = SHORT_FLAGS[token[1]!];
			if (!flagName) {
				canonical.push(token,);
				continue;
			}
			if (BOOLEAN_FLAGS.has(flagName,)) {
				canonical.push(`--${flagName}`,);
				continue;
			}
			const next = argv[index + 1];
			if (next === undefined || (next !== "-" && next.startsWith("-",))) {
				canonical.push(token,);
				continue;
			}
			canonical.push(`--${flagName}=${next}`,);
			index++;
			continue;
		}
		if (!token.startsWith("--",)) {
			canonical.push(token,);
			continue;
		}
		const eqIdx = token.indexOf("=",);
		const rawFlagName = eqIdx === -1 ? token.slice(2,) : token.slice(2, eqIdx,);
		const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
		if (eqIdx !== -1) {
			canonical.push(
				BOOLEAN_FLAGS.has(flagName,) ? `--${flagName}` : `--${flagName}=${token.slice(eqIdx + 1,)}`,
			);
			continue;
		}
		if (BOOLEAN_FLAGS.has(flagName,)) {
			canonical.push(`--${flagName}`,);
			continue;
		}
		const next = argv[index + 1];
		if (next === undefined || next.startsWith("--",)) {
			canonical.push(`--${flagName}`,);
			continue;
		}
		canonical.push(`--${flagName}=${next}`,);
		index++;
	}
	return canonical;
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

/**
 * Value flags the runtime meaningfully accepts with an empty value. Only flags
 * with proven clear semantics belong here: `--version-notes ""` clears saved
 * notes, while `--content ""` clears text payloads such as wiki articles,
 * insights, and project-library files. Canonical argv patterns emit `*` for
 * these and `+` for every other value flag, so `--flag=` alone can never
 * validate for a required non-empty flag.
 */
const ALLOW_EMPTY_VALUE_FLAGS: Record<string, true> = {
	content: true,
	"version-notes": true,
};

function flagValueTokenPattern(flag: CommandFlagMetadata,): string {
	const names = [flag.name, ...(flag.aliases ?? []),];
	const quantifier = flag.allowEmptyValue === true ? "*" : "+";
	return `^--(${names.join("|",)})=[\\s\\S]${quantifier}$`;
}

function booleanFlagPattern(names: string[],): string {
	return `^--(${names.join("|",)})$`;
}

function shortFlagsFor(names: string[],): string[] {
	return Object.entries(SHORT_FLAGS,)
		.filter(([, canonical,],) => names.includes(canonical,))
		.map(([short,],) => short);
}

function shortFlagPattern(flags: CommandFlagMetadata[],): string | undefined {
	const shortChars = shortFlagsFor(
		flags.filter((flag,) => flag.kind === "boolean").map((flag,) => flag.name),
	);
	return shortChars.length > 0 ? `^-(${shortChars.join("|",)})$` : undefined;
}

/**
 * Machine-canonical argv prefix for a registry entry: the public usage tokens
 * between `dss` and the first positional. Synthetic single-action commands
 * whose usage omits their action token (e.g. `dss batch ...`, `dss version`)
 * use an actionless prefix, while real command paths that spell `run` in their
 * usage (e.g. `dss commands run`) keep the action token.
 */
function argvPrefix(resource: string, action: string, usage: string,): string[] {
	const usageTokens = usage.split(/\s+/,).filter((token,) => token.length > 0);
	const head = usageTokens.slice(1,);
	return head[0] === resource && head[1] === action ? [resource, action,] : [resource,];
}

/**
 * Describe one invocation as a canonical argv token array. Agents emit the
 * usage-derived prefix, then the required `<...>` positionals first; flags and
 * values follow, so `prefixItems` pins the head of the array and the tail is
 * validated against the canonical flag token alphabet. Constraints mirror the
 * runtime checks in `validateRegistryCommandInputs`: required positional arity,
 * required flags, required choice groups, and rejection of unknown long and
 * short flags. Value flags only validate in `--flag=VALUE` form, so a bare
 * value flag can never pass without its value; boolean flags stay standalone.
 * Tradeoff: a positional/value token that itself begins with `-` (e.g. a
 * negative number) is rejected unless it is the literal `-` stdin marker or a
 * supported flag; the runtime additionally accepts such tokens via its
 * negative-number and `--` separator rules.
 */
function argvJsonSchema(
	resource: string,
	action: string,
	flags: CommandFlagMetadata[],
	usage: string,
	requiredFlags: string[],
	requiredOneOf: CommandFlagChoice[],
): Record<string, unknown> {
	const prefix = argvPrefix(resource, action, usage,);
	const aliases: Record<string, string[]> = {};
	for (const flag of flags) {
		aliases[flag.name] = flag.aliases ?? [];
	}
	const booleanNames = flags.filter((flag,) => flag.kind === "boolean")
		.flatMap((flag,) => [flag.name, ...(flag.aliases ?? []),]);
	const valueFlags = flags.filter((flag,) => flag.kind === "value");
	const requiredPositionals = requiredPlanPositionals(usage,);
	const itemSchemas: Record<string, unknown>[] = [
		{ not: { pattern: "^-", }, },
		{ const: "-", },
		{ pattern: "^--$", },
	];
	if (booleanNames.length > 0) itemSchemas.push({ pattern: booleanFlagPattern(booleanNames,), },);
	if (valueFlags.length > 0) {
		itemSchemas.push({ pattern: valueFlags.map(flagValueTokenPattern,).join("|",), },);
	}
	const shortPattern = shortFlagPattern(flags,);
	if (shortPattern) itemSchemas.push({ pattern: shortPattern, },);
	const contains = (names: string[],): Record<string, unknown> => {
		const requiredValuePatterns = flags
			.filter((flag,) => flag.kind === "value" && names.includes(flag.name,))
			.map(flagValueTokenPattern,);
		const pattern = requiredValuePatterns.length > 0
			? requiredValuePatterns.join("|",)
			: booleanFlagPattern(names,);
		return { contains: { pattern, }, minContains: 1, };
	};
	const constraints: Record<string, unknown>[] = requiredFlags.map((name,) =>
		contains([name, ...(aliases[name] ?? []),],)
	);
	for (const choice of requiredOneOf) {
		const alternativeSchema = (alternative: string[],): Record<string, unknown> => {
			const required = alternative.map((name,) => contains([name, ...(aliases[name] ?? []),],));
			return required.length === 1 ? required[0]! : { allOf: required, };
		};
		constraints.push({ anyOf: choice.oneOf.map(alternativeSchema,), },);
	}
	return {
		$schema: JSON_SCHEMA_DRAFT,
		type: "object",
		additionalProperties: true,
		required: ["argv",],
		properties: {
			argv: {
				type: "array",
				prefixItems: [
					...prefix.map((token,) => ({ const: token, })),
					...requiredPositionals.map((name,) => ({
						type: "string",
						title: name.replace(/\.\.\.$/, "",),
						not: { pattern: "^-", },
					})),
				],
				items: { type: "string", anyOf: itemSchemas, },
				minItems: prefix.length + requiredPositionals.length,
				...(constraints.length > 0 ? { allOf: constraints, } : {}),
			},
			resource: { const: resource, },
			action: { const: action, },
		},
	};
}

function unsafeOutputs(
	resource: string,
	action: string,
	producesLocalFile: boolean,
): CommandUnsafeOutput[] | undefined {
	const outputs: CommandUnsafeOutput[] = [];
	if (producesLocalFile) {
		const sensitivePermissions = resource === "app" && action === "permissions-snapshot";
		outputs.push({
			condition: "--output or --output-file",
			kind: "local-file",
			detail: sensitivePermissions
				? "Command writes access-control identities and permissions to an owner-only local file."
				: "Command writes bytes to a local path and returns JSON metadata on stdout.",
			...(sensitivePermissions
				? {
					safeAlternative:
						"Keep the file owner-only and outside version control unless repository policy explicitly permits committing access-control data.",
				}
				: {}),
		},);
	}
	return outputs.length > 0 ? outputs : undefined;
}

export function buildCommandSchemas(
	resource: string,
	action: string,
	flags: CommandFlagMetadata[],
	requiredFlags: string[],
	requiredOneOf: CommandFlagChoice[],
	payloadSchema: CommandPayloadSchema | undefined,
	outputShape: CommandOutputShape,
	usage: string,
): CommandAgentSchemas {
	return {
		argv: argvJsonSchema(resource, action, flags, usage, requiredFlags, requiredOneOf,),
		...(payloadSchema ? { input: payloadJsonSchema(payloadSchema,), } : {}),
		output: outputJsonSchema(outputShape,),
	};
}

const EXPLICIT_REGISTRY_OVERRIDES: Record<string, CommandRegistryOverride> = {
	"dashboard.create": {
		examplePayload: { name: "Agent dashboard", pages: [], },
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
	"variable.set": {
		requiredFlags: [],
		requiredOneOf: [{ oneOf: [["standard",], ["local",],], },],
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
	// Project and dashboard exports stream a rendered/archive artifact and write it
	// locally; the DSS-side resource is untouched.
	if ((resource === "project" || resource === "dashboard") && action === "export") return "read";
	if (resource === "data-quality" && action === "compute") return "write";
	// Project Git is classified from the explicit read table, not by verb shape:
	// `switch`, `fetch`, `pull`, `push`, `commit`, the `reset-*`/`revert-*`
	// families, `drop-and-rebuild`, and `future-abort` all mutate repository or
	// future state yet match none of the mutating-verb patterns.
	if (resource === "project-git") {
		return PROJECT_GIT_READ_ACTIONS[action] === true ? "read" : "write";
	}
	if (READ_ACTIONS.has(action,)) return "read";
	if (
		/^(create|clone|restore|update|delete|set|save|upload|run|build|abort|move|refresh|clear|unload|install|login|logout|add|remove|publish|activate|deploy|import|export|preload|upgrade|start|stop|restart|duplicate|put|rename|reply|compute|organize)/
			.test(action,)
		// Compound actions whose mutating verb is a suffix (e.g. permissions-set, dataset-compute).
		|| /-(set|compute|restore)$/.test(action,)
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

export function inferRequiresProject(resource: string, action: string, usage: string,): boolean {
	if (
		resource === "agent" || resource === "auth" || resource === "doctor" || resource === "commands"
		|| resource === "install-skill" || resource === "version"
	) {
		return false;
	}
	if (PROJECT_SCOPED_RESOURCES.has(resource,)) return true;
	if (resource === "project-git") return !action.startsWith("future-",);
	return usage.includes("--project-key",);
}

const ARRAY_OUTPUT_ACTIONS = new Set([
	"history",
	"find",
	"files",
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
	if (resource === "project-git") {
		if (action === "branches" || action === "tags" || action === "list-libraries") return "array";
		if (action === "set-library") return "string";
		return "object";
	}
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
	for (let index = 0; index < group.length; index++) {
		const char = group[index]!;
		if (char === "[" || char === "(") depth++;
		else if (char === "]" || char === ")") depth--;
		const nextAlternative = group.slice(index + 1,).trimStart();
		if (char === "|" && depth === 0 && nextAlternative.startsWith("--",)) {
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
 * Split required usage flags into unconditional flags and required choice
 * groups. A required `(--a X | --b Y)` group becomes a requiredOneOf entry
 * (at least one alternative; an alternative listing several flags must be
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
		// A group mixing a positional alternative with flags (e.g. sql query's
		// `(SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin)`) cannot be
		// expressed as a flag-only choice: filtering out the flagless alternative
		// would falsely require one of the flags. Omit the group from the flag
		// contract entirely; the handler stays the runtime authority.
		if (alternatives.some((alternative,) => flagsInUsageFragment(alternative,).length === 0)) {
			continue;
		}
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
	for (
		const match of usage.matchAll(
			/--([a-z0-9-]+)\s+([A-Za-z][A-Za-z0-9_-]*(?:\|[A-Za-z][A-Za-z0-9_-]*)+)(?![A-Za-z0-9_=-])/g,
		)
	) {
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

/**
 * Long-running commands surface exit 4 when the remote work itself fails.
 * `batch run` is synchronous, yet any step it dispatches may be a long-running
 * lifecycle command (app instance create/delete, job build, scenario run), so a
 * batch can end on 4 as well and must advertise it.
 * `recipe assert-unchanged` is synchronous: a drift check exits 4 with the
 * stable code `assertion_failed`, advertised as `assertionFailure` and never
 * as a long-running outcome.
 */
function inferExitCodes(
	resource: string,
	action: string,
	asyncKind: CommandAsyncKind,
): CommandExitCodes {
	const longRunning = asyncKind !== "none" || `${resource}.${action}` === "batch.run";
	const assertion = `${resource}.${action}` === "recipe.assert-unchanged"
		|| `${resource}.${action}` === "dataset.assert-count"
		|| `${resource}.${action}` === "dataset.assert-schema"
		|| `${resource}.${action}` === "data-quality.assert-results"
		|| `${resource}.${action}` === "batch.run";
	return {
		ok: 0,
		usage: 1,
		error: 2,
		transient: 3,
		...(longRunning ? { longRunningFailure: 4 as const, } : {}),
		...(assertion ? { assertionFailure: 4 as const, } : {}),
	};
}

function cleanupCommandFromDeleteUsage(resource: string, action: string,): string | undefined {
	if (`${resource}.${action}` === "project.import") {
		return "dss project delete <used-project-key> --if-exists --expect-project-incarnation <hash>";
	}
	if (resource === "project" && (action === "create" || action === "duplicate")) {
		return undefined;
	}
	if (!(action.startsWith("create",) || action === "clone" || action === "duplicate")) {
		return undefined;
	}
	const deleteAction = action === "create-rule"
		? "delete-rule"
		: action === "create-instance" || action === "create-successor-instance"
		? "delete-instance"
		: "delete";
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

/**
 * Write actions that no public DSS route can undo. `dataset upload-file` adds a
 * file to the dataset's uploaded-files set, and DSS exposes no endpoint to
 * replace or delete an individual uploaded file, so the write is permanent and
 * no cleanup-ledger entry can recover the previous file set.
 */
const EXPLICIT_DESTRUCTIVE_KEYS: Record<string, true> = {
	"dataset.upload-file": true,
};

function inferDestructiveLevel(
	resource: string,
	sideEffect: CommandSideEffect,
	action: string,
): CommandDestructiveLevel {
	if (sideEffect !== "write") return "none";
	if (EXPLICIT_DESTRUCTIVE_KEYS[`${resource}.${action}`] === true) return "destructive";
	if (resource === "project-git") {
		return PROJECT_GIT_DESTRUCTIVE_ACTIONS[action] === true ? "destructive" : "reversible";
	}
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
	// Project Git mutation results carry a future job id only for the library
	// calls and the future lifecycle itself; plain Git actions settle inline.
	if (resource === "project-git" && PROJECT_GIT_FUTURE_ACTIONS[action] === true) return "future";
	if (resource === "code" && action === "run") return "future";
	if (
		resource === "app"
		&& ["create-instance", "create-successor-instance", "delete-instance",].includes(action,)
	) {
		return "future";
	}
	return "none";
}

function inferIdempotency(
	resource: string,
	sideEffect: CommandSideEffect,
	action: string,
	usage: string,
): CommandIdempotency {
	if (sideEffect === "read") return "safe";
	if (action.startsWith("create",) && usage.includes("--if-not-exists",)) return "if-not-exists";
	if (action.startsWith("delete",) && usage.includes("--if-exists",)) return "if-exists";
	if (`${resource}.${action}` === "app.set-manifest-version") return "none";
	// `app delete-instance` converges without an `--if-exists` flag: an absent
	// target project is reported as an already-absent success instead of an
	// error, so replaying the delete cannot fail on absence alone.
	if (`${resource}.${action}` === "app.delete-instance") return "convergent";
	// Git fetch and the repository resets converge: replaying them lands on the
	// same repository state instead of failing on an already-applied change.
	if (resource === "project-git" && PROJECT_GIT_CONVERGENT_ACTIONS[action] === true) {
		return "convergent";
	}
	if (/^(clear|refresh|set|save)/.test(action,)) return "convergent";
	return "none";
}

/**
 * `app create-instance` and `app create-successor-instance` hand back a DSS
 * creation future, and the created project only becomes safe to delete once
 * that future is terminal. A directly runnable delete command cannot express
 * that gate, so these commands advertise no `cleanupCommand` at all: recovery
 * goes through the recorded cleanup ledger, whose replay carries the creation
 * future ID and refuses to delete while creation is unconfirmed.
 */
const LEDGER_ONLY_CLEANUP_KEYS: Record<string, true> = {
	"app.create-instance": true,
	"app.create-successor-instance": true,
};
const LEDGER_ONLY_CLEANUP_HINT =
	"Pass `--record-cleanup PATH` when creating, then recover with `dss cleanup --file PATH --apply`. Do not issue direct instance deletion as cleanup: it bypasses the creation-future gate and can delete the project while DSS is still creating it.";

export function inferCleanupHint(resource: string, action: string,): string | undefined {
	// Git mutations are not cleanup-ledger operations: no project-git branch or
	// tag create has a generic `delete` command to close it out.
	if (resource === "project-git") return undefined;
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	if (LEDGER_ONLY_CLEANUP_KEYS[`${resource}.${action}`] === true) {
		return LEDGER_ONLY_CLEANUP_HINT;
	}
	const deleteAction = action === "create-rule"
		? "delete-rule"
		: action === "create-instance" || action === "create-successor-instance"
		? "delete-instance"
		: "delete";
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
	const requiresAuth = meta.localHandler === undefined && inferRequiresAuth(resource,);
	const requiresProject = inferRequiresProject(resource, action, meta.usage,);
	const sideEffect = inferSideEffect(resource, action,);
	const destructive = inferDestructiveLevel(resource, sideEffect, action,);
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
	const inferredCleanupCommand = meta.cleanupCommand
		?? EXPLICIT_REGISTRY_OVERRIDES[registryKey(resource, action,)]?.cleanupCommand
		?? cleanupCommandFromDeleteUsage(resource, action,);
	const cleanupCommand = LEDGER_ONLY_CLEANUP_KEYS[`${resource}.${action}`] === true
		? undefined
		: inferredCleanupCommand;
	const flagMetadata: CommandFlagMetadata[] = flags.map((name,) => {
		const aliases = Object.entries(FLAG_ALIASES,)
			.filter(([raw, canonical,],) =>
				canonical === name && new RegExp(`--${raw}(?![a-z0-9-])`,).test(meta.usage,)
			)
			.map(([raw,],) => raw);
		const aliasPart = aliases.length > 0 ? { aliases, } : {};
		const kind = flagKind(name,);
		if (kind === "boolean") return { name, kind, ...aliasPart, };
		const allowEmptyPart = ALLOW_EMPTY_VALUE_FLAGS[name] === true ? { allowEmptyValue: true, } : {};
		const hint = valueHints.get(name,) ?? GLOBAL_FLAG_VALUE_HINTS[name];
		if (!hint) return { name, kind, ...aliasPart, ...allowEmptyPart, };
		return {
			name,
			kind,
			valueType: hint.valueType,
			...(hint.enumValues ? { enumValues: hint.enumValues, } : {}),
			...aliasPart,
			...allowEmptyPart,
		};
	},);
	const positionals = extractPositionals(meta.usage,);
	const outputShape = inferOutputShape(resource, action,);
	const producesLocalFile = meta.usage.includes("--output PATH",)
		|| meta.usage.includes("--output-file PATH",);
	const uniqueRequiredFlags = uniqueStrings(requiredFlags,);
	const uniqueOptionalFlags = uniqueStrings(optionalFlags,);
	const unsafe = unsafeOutputs(resource, action, producesLocalFile,);
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
		idempotency: inferIdempotency(resource, sideEffect, action, meta.usage,),
		dryRun: meta.usage.includes("--dry-run",),
		requiredFlags: uniqueRequiredFlags,
		optionalFlags: uniqueOptionalFlags,
		...(requiredOneOf.length > 0 ? { requiredOneOf, } : {}),
		...(payloadSchema ? { payloadSchema, } : {}),
		schemas: buildCommandSchemas(
			resource,
			action,
			flagMetadata,
			uniqueRequiredFlags,
			requiredOneOf,
			payloadSchema,
			outputShape,
			meta.usage,
		),
		...(unsafe ? { unsafeOutputs: unsafe, } : {}),
		...(examplePayload !== undefined ? { examplePayload, } : {}),
		...(cleanupCommand ? { cleanupCommand, } : {}),
		exitCodes: inferExitCodes(resource, action, asyncKind,),
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
			allowEmptyValue: { type: "boolean", },
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

export function commandActionSummary(
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
			fullRegistryExportCommand: "dss commands run --output PATH",
			scopedDiscoveryCommand: "dss commands run --fields RESOURCE[.ACTION[.FIELD...]]",
			actionIndexCommand: "dss agent contract --fields commands.actions",
			scopedDiscoveryExamples: [
				"dss commands run --fields dataset",
				"dss commands run --fields dataset.create",
			],
			scopedDiscoveryHint:
				"`dss commands run` returns the resource/action summary. --fields RESOURCE returns every action of one resource; --fields RESOURCE.ACTION returns a single registry entry keyed by the dotted path; append .FIELD paths to project nested metadata. Comma-separate paths to select several. Use --output PATH to export the full registry.",
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
				format: "compact-json",
				success: "single-json-value",
				failure: "structured-error-object",
				richFailureResults:
					"doctor/batch/cleanup nonzero outcomes are their own compact result on stdout ({ok:false,...}) with the command's exit code; not re-wrapped.",
			},
			stderr: {
				format: "jsonl",
				events: ["warning", "trace",],
			},
		},
		planning: {
			discoveryCommand: "dss commands run",
			contractCommand: AGENT_CONTRACT_COMMAND,
			bootstrapCommand:
				"dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility",
			preferredDiscoveryCommand: "dss commands run --fields RESOURCE.ACTION",
			actionIndexCommand: "dss agent contract --fields commands.actions",
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
		...(entry.exitCodes.assertionFailure !== undefined
			? { assertionFailure: entry.exitCodes.assertionFailure, }
			: {}),
	};
}

/**
 * Plan-local project key resolution. `--plan` is a purely local preview, so it
 * resolves the target project from the explicit `--project-key` flag and the
 * documented `DATAIKU_PROJECT_KEY` environment variable only. The saved
 * credentials file is never opened and no API key is ever resolved, so planning
 * can neither depend on nor leak stored secrets.
 */
function planProjectKeyFromArgs(
	flags: Record<string, string | boolean>,
): string | undefined {
	const fromFlag = flags["project-key"];
	if (typeof fromFlag === "string" && fromFlag.trim().length > 0) return fromFlag.trim();
	if (!dataikuEnvironmentEnabled()) return undefined;
	const fromEnv = process.env.DATAIKU_PROJECT_KEY;
	return fromEnv !== undefined && fromEnv.trim().length > 0 ? fromEnv.trim() : undefined;
}

/**
 * Commands whose plan must pin the target with an explicit `--project-key`
 * instead of falling back to ambient `DATAIKU_PROJECT_KEY`, so a cleanup plan
 * can never silently retarget a project the caller did not name.
 */
const EXPLICIT_PLAN_PROJECT_KEY: Record<string, true> = { "app.delete-instance": true, };

/** Plan-local project key, or the canonical `--project-key` usage error. */
function requiredPlanProjectKey(
	flags: Record<string, string | boolean>,
	usage: string,
): string {
	return planProjectKeyFromArgs(flags,) ?? requiredPlanFlag(flags, "project-key", usage,);
}

function projectKeyForPlan(
	entry: CommandRegistryEntry,
	flags: Record<string, string | boolean>,
): string | undefined {
	if (!entry.requiresProject) return undefined;
	if (EXPLICIT_PLAN_PROJECT_KEY[`${entry.resource}.${entry.action}`] === true) {
		return requiredPlanFlag(flags, "project-key", entry.usage,);
	}
	const projectKey = planProjectKeyFromArgs(flags,);
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
	// Return trimmed like the runtime's requiredStringFlag so plans advertise
	// exactly the identifier the command will act on.
	if (typeof value === "string" && value.trim().length > 0) return value.trim();
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
/**
 * Git and future API mount. The official Python client mounts these routes on
 * `/dip/publicapi`, which is also what the project-git resource uses; do not
 * normalize them to the repo-wide `/public/api`.
 */
const PROJECT_GIT_API_ROOT = "/dip/publicapi";

function projectGitEndpoint(projectKey: string | undefined, suffix: string,): string {
	if (!projectKey) throw new UsageError("--project-key is required for project-git mutations.",);
	return `${PROJECT_GIT_API_ROOT}/projects/${encodeURIComponent(projectKey,)}/git${suffix}`;
}

function projectGitFutureEndpoint(jobId: string,): string {
	return `${PROJECT_GIT_API_ROOT}/futures/${encodeURIComponent(jobId,)}`;
}

/** Encode a library reference path per segment, mirroring the resource helper. */
function encodeGitReferencePath(pathValue: string,): string {
	const normalized = pathValue.replace(/^\/+/, "",).replace(/\/+$/, "",);
	return normalized.split("/",).map((segment,) => encodeURIComponent(segment,)).join("/",);
}

/**
 * `--plan` bypasses the SDK boundary guard, so HTTP(S) URLs with embedded
 * userinfo are rejected here before any plan is printed. SSH/scp-style URLs
 * (`git@host:org/repo.git`, `ssh://...`) remain valid.
 */
function validatedPlanRepositoryUrl(url: string, flag: string, usage: string,): string {
	const candidate = url.trim();
	if (/[\u0000-\u001f\u007f]/u.test(candidate,)) {
		throw new UsageError(`--${flag} must not contain control characters. Usage: ${usage}`,);
	}
	if (/^https?:/i.test(candidate,)) {
		if (candidate.includes("\\",)) {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		if (!/^https?:\/\//i.test(candidate,)) {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		let parsed: URL;
		try {
			parsed = new URL(candidate,);
		} catch {
			throw new UsageError(`--${flag} must be a valid HTTP(S) URL. Usage: ${usage}`,);
		}
		if (parsed.username !== "" || parsed.password !== "") {
			throw new UsageError(
				`--${flag} must not contain embedded credentials (userinfo). Usage: ${usage}`,
			);
		}
	}
	return candidate;
}

function requiredPlanRepositoryUrl(
	flags: Record<string, string | boolean>,
	flag: string,
	usage: string,
): string {
	const url = requiredPlanFlag(flags, flag, usage,);
	return validatedPlanRepositoryUrl(url, flag, usage,);
}

/** Wait procedure advertised for the library calls that return a job id. */
function projectGitFutureWait(): Record<string, unknown> {
	return {
		when: "after-dispatch",
		endpoint: `${PROJECT_GIT_API_ROOT}/futures/{jobId}?peek=false`,
		description: "Poll the returned job id until the future reports a result; never abort it.",
	};
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

function uploadPayload(filePath: string,): Record<string, unknown> {
	return {
		contentType: "multipart/form-data",
		fileField: "file",
		filePath,
		fileName: path.basename(filePath,),
	};
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
	requests?: unknown;
} {
	const projectEndpoint = (suffix: string,) => {
		if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
		return encodedProjectEndpointForPlan(projectKey, suffix,);
	};
	const id = args[0];
	const codeEnvEndpoint = (suffix = "",) =>
		`/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
			encodeURIComponent(args[1],)
		}${suffix}`;
	const statisticsWorksheetsEndpoint = (datasetName: string,) =>
		projectEndpoint(`/datasets/${encodeURIComponent(datasetName,)}/statistics/worksheets/`,);
	const statisticsWorksheetEndpoint = (datasetName: string, worksheetId: string,) =>
		`${statisticsWorksheetsEndpoint(datasetName,)}${encodeURIComponent(worksheetId,)}`;
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
			const data = jsonInput(flags,);
			const flagName = flags["name"] as string | undefined;
			const dataName = data?.["name"];
			const name = flagName ?? (typeof dataName === "string" ? dataName : undefined);
			if (!name) {
				throw new UsageError(
					"--name or dashboard settings containing a string name are required. Usage: dss dashboard create --name NAME",
				);
			}
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			const payload: Record<string, unknown> = { ...(data ?? { pages: [], }), name, };
			if (listed !== undefined) payload.listed = listed;
			return {
				method: "POST",
				endpoint: projectEndpoint("/dashboards/",),
				identifiers: { name, },
				payload,
			};
		}
		case "dashboard.update": {
			const payload: Record<string, unknown> = { ...jsonInput(flags,), };
			if (typeof flags["name"] === "string") payload.name = flags["name"];
			const listed = parseBooleanOption(flags["listed"], "--listed",);
			if (listed !== undefined) payload.listed = listed;
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/dashboards/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload,
			};
		}
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
					`/actions/compute-rules${
						querySuffix({
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
			const connection = flags["connection"] as string | undefined;
			const dsType = requiredPlanFlag(flags, "type", entry.usage,);
			if (!connection && dsType.toLowerCase() !== "uploadedfiles") {
				throw new UsageError("--connection is required unless --type is UploadedFiles.",);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/",),
				identifiers: { name, },
				payload: { datasetName: name, connection, dsType, projectKey, },
			};
		}
		case "dataset.clone": {
			const source = args[0];
			const target = args[1];
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/",),
				identifiers: { source, target, },
				payload: {
					sourceDataset: source,
					targetDataset: target,
					path: flags["path"] as string | undefined,
					table: flags["table"] as string | undefined,
					metastoreTableName: flags["metastore-table"] as string | undefined,
					allowSamePath: flags["allow-same-path"] === true,
					projectKey,
				},
			};
		}
		case "dataset.rename":
			return {
				method: "POST",
				endpoint: projectEndpoint("/actions/renameDataset",),
				identifiers: { oldName: args[0], newName: args[1], },
				payload: { oldName: args[0], newName: args[1], },
			};
		case "dataset.upload-file": {
			const fileName = requiredPlanFlag(flags, "file-name", entry.usage,);
			const datasetEndpoint = projectEndpoint(`/datasets/${encodeURIComponent(args[0],)}`,);
			const filesEndpoint = `${datasetEndpoint}/uploaded/files`;
			const upload = {
				method: "POST",
				endpoint: filesEndpoint,
				payload: { ...uploadPayload(args[1],), fileName, },
			};
			return {
				...upload,
				identifiers: {
					datasetName: args[0],
					localPath: args[1],
					fileName,
				},
				requests: [
					{ sequence: 1, method: "GET", endpoint: datasetEndpoint, },
					{ sequence: 2, method: "GET", endpoint: filesEndpoint, },
					{ sequence: 3, ...upload, },
					{
						sequence: 4,
						method: "GET",
						endpoint: filesEndpoint,
						assert: {
							filename: fileName,
							length: "local file byte length",
						},
					},
				],
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
		case "recipe.add-input":
		case "recipe.remove-input": {
			const role = (flags["role"] as string | undefined) ?? "main";
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(args[0],)}`,),
				identifiers: { recipe: args[0], dataset: args[1], role, },
				payload: {
					operation: action === "add-input" ? "append" : "remove",
					dataset: args[1],
					role,
					projectKey,
				},
			};
		}
		case "recipe.clone": {
			const positionalSource = args[0];
			const fromFlag = typeof flags["from"] === "string" ? flags["from"].trim() : "";
			const source = positionalSource ?? fromFlag;
			if (!source) {
				throw new UsageError(
					`Source recipe is required. Usage: ${entry.usage}`,
					"missing_required_flag",
				);
			}
			if (positionalSource && fromFlag && positionalSource !== fromFlag) {
				throw new UsageError(
					"Positional source and --from must match when both are provided.",
					"invalid_enum",
				);
			}
			const toFlag = typeof flags["to"] === "string" ? flags["to"].trim() : "";
			const nameFlag = typeof flags["name"] === "string" ? flags["name"].trim() : "";
			const target = toFlag || nameFlag;
			if (!target) {
				throw new UsageError(
					`--name or --to is required. Usage: ${entry.usage}`,
					"missing_required_flag",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/recipes/",),
				identifiers: { source, target, },
				payload: {
					sourceRecipe: source,
					targetRecipe: target,
					inputRewrites: rewritePairsFromFlags(flags, "replace-input",),
					outputRewrites: rewritePairsFromFlags(flags, "replace-output",),
					payloadTextRewrites: rewritePairsFromFlags(flags, "replace-payload-text",),
					outputDataset: flags["output"] as string | undefined,
					copyOutputSettings: flags["copy-output-settings"] === true,
					outputPath: flags["path"] as string | undefined,
					metastoreTableName: flags["metastore-table"] as string | undefined,
					projectKey,
				},
			};
		}
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
					inputDatasets: recipeInputDatasetsFromFlags(flags,),
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
					?? path.join(process.cwd(), ".dss-backups", "recipes",);
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
			const type = flags["type"] as string | undefined;
			const connection = flags["connection"] as string | undefined;
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
		case "project-deployer.create-project": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/projects",
				identifiers: { projectKey: payload.projectKey, id: payload.id, },
				payload,
			};
		}
		case "project-deployer.upload-bundle":
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/projects/bundles",
				identifiers: { filePath: id, },
				payload: uploadPayload(id,),
			};
		case "project-deployer.create-deployment": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/deployments",
				identifiers: { deploymentId: payload.deploymentId ?? payload.id, },
				payload,
			};
		}
		case "project-deployer.save-deployment-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}/settings`,
				identifiers: { deploymentId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project-deployer.deploy":
			return {
				method: "POST",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}/actions/update`,
				identifiers: { deploymentId: id, },
				payload: {},
			};
		case "project-deployer.delete-deployment":
			return {
				method: "DELETE",
				endpoint: `/public/api/project-deployer/deployments/${encodeURIComponent(id,)}`,
				identifiers: { deploymentId: id, },
			};
		case "project-deployer.create-infra": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/project-deployer/infras",
				identifiers: { infraId: payload.id, },
				payload,
			};
		}
		case "api-deployer.create-infra": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/infras",
				identifiers: { infraId: payload.id, },
				payload,
			};
		}
		case "api-deployer.delete-infra":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/infras/${encodeURIComponent(id,)}`,
				identifiers: { infraId: id, },
			};
		case "api-deployer.create-service": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/services",
				identifiers: { serviceId: payload.id ?? payload.publishedServiceId, },
				payload,
			};
		}
		case "api-deployer.delete-service":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(id,)}`,
				identifiers: { serviceId: id, },
			};
		case "api-deployer.publish-version":
			return {
				method: "POST",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(args[0],)}/versions`,
				identifiers: { serviceId: args[0], filePath: args[1], },
				payload: uploadPayload(args[1],),
			};
		case "api-deployer.delete-version":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/services/${encodeURIComponent(args[0],)}/versions/${
					encodeURIComponent(args[1],)
				}`,
				identifiers: { serviceId: args[0], version: args[1], },
			};
		case "api-deployer.create-deployment": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/api-deployer/deployments",
				identifiers: { deploymentId: payload.deploymentId ?? payload.id, },
				payload,
			};
		}
		case "api-deployer.save-deployment-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}/settings`,
				identifiers: { deploymentId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "api-deployer.deploy":
			return {
				method: "POST",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}/actions/update`,
				identifiers: { deploymentId: id, },
				payload: {},
			};
		case "api-deployer.delete-deployment":
			return {
				method: "DELETE",
				endpoint: `/public/api/api-deployer/deployments/${encodeURIComponent(id,)}`,
				identifiers: { deploymentId: id, },
			};
		case "workspace.create": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/workspaces/",
				identifiers: { workspaceKey: payload.workspaceKey, },
				payload,
			};
		}
		case "workspace.update-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}`,
				identifiers: { workspaceKey: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "workspace.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}`,
				identifiers: { workspaceKey: id, },
			};
		case "workspace.add-object":
			return {
				method: "POST",
				endpoint: `/public/api/workspaces/${encodeURIComponent(id,)}/objects`,
				identifiers: { workspaceKey: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "meaning.create": {
			const body = jsonInput(flags,) ?? {};
			const payload = {
				...body,
				id: args[0],
				label: args[1],
				type: args[2],
				description: body.description ?? null,
				entries: body.entries ?? null,
				mappings: body.mappings ?? null,
				pattern: body.pattern ?? null,
				normalizationMode: body.normalizationMode ?? null,
				detectable: body.detectable ?? false,
			};
			return {
				method: "POST",
				endpoint: "/public/api/meanings/",
				identifiers: { id: args[0], },
				payload,
			};
		}
		case "meaning.update":
			return {
				method: "PUT",
				endpoint: `/public/api/meanings/${encodeURIComponent(id,)}`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "meaning.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/meanings/${encodeURIComponent(id,)}`,
				identifiers: { id, },
			};
		case "business-app.save-settings":
			return {
				method: "PUT",
				endpoint: `/public/api/business-apps/${encodeURIComponent(id,)}/settings`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "business-app.create-instance":
			return {
				method: "POST",
				endpoint: `/public/api/business-apps/${encodeURIComponent(id,)}/instances`,
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "business-app.upgrade-instance":
			return {
				method: "POST",
				endpoint: `/public/api/business-apps/${encodeURIComponent(args[0],)}/instances/${
					encodeURIComponent(args[1],)
				}/upgrade`,
				identifiers: { id: args[0], projectKey: args[1], },
				payload: {},
			};
		case "business-app.install-from-archive":
			return {
				method: "POST",
				endpoint: "/public/api/business-apps/install-from-archive",
				identifiers: { filePath: id, },
				payload: uploadPayload(id,),
			};
		case "app.save-instance-manifest": {
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			return {
				method: "PUT",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/app-manifest`,
				identifiers: { projectKey: targetProjectKey, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		}
		case "app.create-instance": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const rawTargetProjectKey = stringField(payload, ["targetProjectKey",],);
			if (!rawTargetProjectKey || rawTargetProjectKey.trim() === "") {
				throw new UsageError(
					"Instance creation payload must include a non-empty targetProjectKey.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			// The runtime trims and rewrites body.targetProjectKey; the plan
			// advertises the normalized identifier and payload.
			const targetProjectKey = rawTargetProjectKey.trim();
			const normalizedPayload = { ...payload, targetProjectKey, };
			return {
				method: "POST",
				endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
				identifiers: {
					appId: id,
					targetProjectKey,
					preflightExecuted: false,
					preflightWillRunDuringApply: true,
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					incarnationObservationRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: flags["wait"] === true
								? "after-terminal-future-target"
								: "conditional-inline-hasResult-target",
							intent:
								"After DSS reports inline or terminal creation success for the requested key, observe creationTag for later cleanup binding.",
						},
					],
					preflightRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: "before-create",
							intent: "Require the target project to be absent before creating the instance.",
						},
						{
							method: "GET",
							endpoint: "/public/api/projects/",
							when: "conditional",
							intent:
								"Fallback list probe issued only when the direct project GET is forbidden (403). Presence proves a collision; absence cannot prove availability.",
						},
						{
							method: "GET",
							endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances/`,
							when: "conditional",
							intent:
								"Fallback app-instance list probe issued when the direct project GET is forbidden (403). Presence proves a collision; absence still rejects creation as unverifiable.",
						},
					],
					note:
						"Strict preflight: the instance POST runs only after the payload targetProjectKey is confirmed absent. Creation never writes into an existing or unprovable project.",
				},
				payload: normalizedPayload,
				wait: flags["wait"] === true,
			};
		}
		case "app.set-manifest-version": {
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			const payloadPatch: Record<string, unknown> = {};
			const version = flags["manifest-version"] as string | undefined;
			if (version !== undefined) {
				if (version.trim() === "") {
					throw new UsageError(
						"App manifest version must be a non-empty string.",
						"validation_failed",
						`Usage: ${entry.usage}`,
					);
				}
				payloadPatch.version = version;
			}
			const versionNotes = flags["version-notes"] as string | undefined;
			if (versionNotes !== undefined) payloadPatch.versionNotes = versionNotes;
			if (version === undefined && versionNotes === undefined) {
				throw new UsageError(
					"At least one of --manifest-version or --version-notes is required.",
					"usage_error",
					`Usage: ${entry.usage}`,
				);
			}
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError(
					"Expected manifest hash must be a 64-character SHA-256 hex digest.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			return {
				method: "PUT",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/app-manifest`,
				identifiers: {
					projectKey: targetProjectKey,
					payloadPatch,
					...(expectHash !== undefined ? { expectHash: expectHash.toLowerCase(), } : {}),
					concurrencyControl: APP_MANIFEST_CONCURRENCY_CONTROL,
					staleReadCheck: expectHash === undefined
						? "none"
						: "client-side-expect-hash-compare-before-put",
					note: expectHash === undefined
						? "Unconditional PUT: no stale-read check is armed because --expect-hash was not supplied."
						: "The hash is compared client-side against a fresh read; the PUT itself stays unconditional, so this command can overwrite a writer that commits between that read and this PUT without detecting the lost update.",
				},
			};
		}
		case "app.create-successor-instance": {
			const sourceProjectKey = requiredPlanFlag(flags, "from", entry.usage,);
			const targetProjectKey = requiredPlanFlag(flags, "to", entry.usage,);
			if (sourceProjectKey === targetProjectKey) {
				throw new UsageError(
					"--from and --to must be different project keys.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			const targetProjectName = flags["name"] as string | undefined;
			const copyPermissions = parseBooleanOption(flags["copy-permissions"], "--copy-permissions",)
				?? false;
			return {
				method: "POST",
				endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
				identifiers: {
					appId: id,
					sourceProjectKey,
					targetProjectKey,
					preflightExecuted: false,
					preflightWillRunDuringApply: true,
					...(targetProjectName !== undefined ? { targetProjectName, } : {}),
					copyPermissions,
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					postFutureRequests: [
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							intent:
								"After the terminal future names the successor key, observe creationTag and bind later target checks and cleanup to that hash.",
						},
					],
					...(copyPermissions
						? {
							permissionConcurrencyControl: "client-side-non-atomic-stale-identity-and-hash-checks",
						}
						: {}),
					preflightRequests: [
						{
							method: "GET",
							endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances/`,
							when: "before-create",
							intent: "Verify the --from project is a registered instance of the app.",
						},
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/app-manifest`,
							when: "before-create",
							intent: "Verify the --from project is an APP_INSTANCE project.",
						},
						{
							method: "GET",
							endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
							when: "before-create",
							intent: "Require the --to target project to be absent before creating the successor.",
						},
						{
							method: "GET",
							endpoint: "/public/api/projects/",
							when: "conditional",
							intent:
								"Fallback list probe issued only when the direct project GET is forbidden (403). Presence proves a collision; absence cannot prove availability and rejects creation.",
						},
					],
					...(copyPermissions
						? {
							permissionRequests: [
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
									intent: "Snapshot the predecessor instance ACL before creation.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Read the successor ACL to decide whether the copy is a no-op.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Recheck successor creationTag after reading its ACL; stop if the project key was reused.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
									intent:
										"Recheck the predecessor ACL immediately before the write and stop if its hash drifted.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Recheck successor creationTag immediately before the unconditional permission PUT.",
								},
								{
									method: "PUT",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Apply the predecessor ACL snapshot to the successor instance.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/permissions`,
									intent: "Verify the successor ACL hash equals the predecessor snapshot hash.",
								},
								{
									method: "GET",
									endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`,
									intent:
										"Detect successor project-key reuse across the permission write and verification read.",
								},
							],
						}
						: {}),
					note:
						"Additive, non-transactional: strict preflight verifies the predecessor instance and the --to target absence before the single instance POST, then waits on the DSS future. The predecessor is never modified or deleted; cleanup targets only the successor key. DSS exposes no immutable future target ID, conditional DELETE, or conditional permission PUT: later creationTag and ACL checks narrow and detect races but cannot atomically join creation provenance or serialize writes.",
				},
				payload: {
					targetProjectKey,
					targetProjectName: targetProjectName ?? targetProjectKey,
				},
				wait: true,
			};
		}
		case "app.delete-instance": {
			const targetProjectKey = requiredPlanFlag(flags, "project-key", entry.usage,);
			const futureId = flags["future-id"] === undefined
				? undefined
				: requiredPlanFlag(flags, "future-id", entry.usage,);
			const expectedProjectIncarnation = flags["expect-project-incarnation"] === undefined
				? undefined
				: requiredPlanFlag(flags, "expect-project-incarnation", entry.usage,);
			if (
				expectedProjectIncarnation !== undefined
				&& !/^[0-9a-f]{64}$/.test(expectedProjectIncarnation,)
			) {
				throw new UsageError(
					"--expect-project-incarnation must be a 64-character lowercase SHA-256 hash.",
					"validation_failed",
				);
			}
			const unconfirmedCreation = parseBooleanOption(
				flags["unconfirmed-creation"],
				"--unconfirmed-creation",
			) ?? false;
			const manifestProbe = `/public/api/projects/${
				encodeURIComponent(targetProjectKey,)
			}/app-manifest`;
			const projectDetailsProbe = `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`;
			const typeValidationRequests = [
				{
					method: "GET",
					endpoint: manifestProbe,
					when: "before-delete",
					intent: "Verify the target is an APP_INSTANCE project before deleting.",
				},
				{
					method: "GET",
					endpoint: projectDetailsProbe,
					when: expectedProjectIncarnation === undefined
						? "conditional-type-check-before-delete"
						: "incarnation-and-conditional-type-check-before-delete",
					intent: expectedProjectIncarnation === undefined
						? "Fallback probe issued only when the app-manifest response omits projectAppType (live DSS does)."
						: "Recompute the current project-incarnation hash from creationTag after the manifest probe; the same response supplies projectAppType when the manifest omits it.",
				},
			];
			const preflightRequests = typeValidationRequests;
			if (unconfirmedCreation) {
				return {
					identifiers: {
						projectKey: targetProjectKey,
						...(futureId !== undefined ? { futureId, } : {}),
						unconfirmedCreation: true,
						note:
							"Indeterminate creation without a DSS future ID: no DSS request is issued. The command reports an unresolved cleanup failure (exit 4, cleanupResolved false) without deleting, because creation may still be running.",
					},
				};
			}
			if (futureId === undefined) {
				return {
					method: "DELETE",
					endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}`,
					identifiers: {
						projectKey: targetProjectKey,
						...(expectedProjectIncarnation === undefined
							? {}
							: {
								projectIncarnationGate: {
									required: false,
									provided: true,
									expectedHash: expectedProjectIncarnation,
								},
								incarnationControl: "client-side-non-atomic-stale-identity-check",
							}),
						preflightRequests,
						note: expectedProjectIncarnation === undefined
							? "Convergent direct delete: the manifest preflight rejects non-instance targets, an absent target (404) is an already-absent success issued without any DELETE, and only a verified instance target receives the project DELETE. No project-incarnation binding was requested."
							: "Incarnation-bound direct delete: after verifying APP_INSTANCE type, a project GET must match the expected creationTag hash before an unconditional DELETE. DSS exposes no immutable project ID or conditional DELETE, so this client-side check narrows but cannot serialize against project-key reuse after the GET.",
					},
				};
			}
			return {
				method: "DELETE",
				endpoint: `/public/api/projects/${encodeURIComponent(targetProjectKey,)}`,
				identifiers: {
					projectKey: targetProjectKey,
					futureId,
					projectIncarnationGate: {
						required: true,
						provided: expectedProjectIncarnation !== undefined,
						...(expectedProjectIncarnation === undefined
							? {}
							: { expectedHash: expectedProjectIncarnation, }),
					},
					incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
					preflightRequests: [
						{
							method: "GET",
							endpoint: manifestProbe,
							when: "before-future-wait",
							intent:
								"Verify the target is an APP_INSTANCE project before the supplied future is touched, so an invalid target cannot affect it.",
						},
						{
							method: "GET",
							endpoint: projectDetailsProbe,
							when: "conditional-before-wait",
							intent:
								"Fallback probe issued only when the pre-wait app-manifest response omits projectAppType (live DSS does).",
						},
					],
					futureGate: [
						{
							method: "GET",
							endpoint: `/public/api/futures/${encodeURIComponent(futureId,)}?peek=false`,
							intent:
								"Wait for the supplied creation future to settle or time out; never abort it, and repeat this read-only GET while it is live.",
						},
					],
					postFutureValidationRequests: expectedProjectIncarnation === undefined
						? []
						: typeValidationRequests,
					note: expectedProjectIncarnation === undefined
						? "The target type is verified before the future is touched. Waiting never aborts the future, but a terminal target match still cannot authorize deletion without --expect-project-incarnation; the command then exits with validation_failed and issues no DELETE."
						: "The target type is verified before the future is touched. The DELETE runs only after the terminal future reports the requested target, then a later project GET still matches the recorded creationTag hash and the target is re-verified as APP_INSTANCE. DSS exposes no immutable future target ID or conditional DELETE: the future target and creationTag are independent, non-atomic observations that narrow but cannot eliminate a project-key-reuse race.",
				},
			};
		}
		case "app.permissions-restore": {
			const file = requiredPlanFlag(flags, "file", entry.usage,);
			const targetProjectKey = requiredPlanProjectKey(flags, entry.usage,);
			const permissionsEndpoint = `/public/api/projects/${
				encodeURIComponent(targetProjectKey,)
			}/permissions`;
			const projectDetailsEndpoint = `/public/api/projects/${encodeURIComponent(targetProjectKey,)}/`;
			return {
				method: "PUT",
				endpoint: permissionsEndpoint,
				identifiers: {
					file,
					projectKey: targetProjectKey,
					localPreflight: [
						"Read and hash-verify the owner-only snapshot file.",
						"Require its project key and canonical DSS URL to match this invocation.",
					],
					preflightRequests: [
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent: "Require the snapshot project-incarnation hash to match creationTag.",
						},
						{
							method: "GET",
							endpoint: permissionsEndpoint,
							intent: "Read current permissions; an equal hash makes the PUT unnecessary.",
						},
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent:
								"Recheck the project incarnation after the permission read and immediately before any PUT.",
						},
					],
					conditionalWrite: {
						method: "PUT",
						endpoint: permissionsEndpoint,
						when: "permissions-differ-and-dry-run-is-false",
					},
					verificationRequests: [
						{
							method: "GET",
							endpoint: permissionsEndpoint,
							intent: "Verify DSS persisted the desired permission hash.",
						},
						{
							method: "GET",
							endpoint: projectDetailsEndpoint,
							intent: "Detect project-key reuse across the permission write.",
						},
					],
					incarnationControl: "client-side-non-atomic-stale-identity-check",
					note:
						"DSS exposes no conditional permission PUT or immutable project ID. The repeated creationTag checks narrow and detect key-reuse races but cannot serialize the check with the PUT.",
				},
			};
		}
		case "statistics.create-worksheet":
			return {
				method: "POST",
				endpoint: statisticsWorksheetsEndpoint(args[0],),
				identifiers: { dataset: args[0], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.update-worksheet":
			return {
				method: "PUT",
				endpoint: statisticsWorksheetEndpoint(args[0], args[1],),
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.delete-worksheet":
			return {
				method: "DELETE",
				endpoint: statisticsWorksheetEndpoint(args[0], args[1],),
				identifiers: { dataset: args[0], worksheetId: args[1], },
			};
		case "statistics.run-worksheet":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-card`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
			};
		case "statistics.run-card":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-card`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "statistics.run-computation":
			return {
				method: "POST",
				endpoint: `${statisticsWorksheetEndpoint(args[0], args[1],)}/actions/run-computation`,
				identifiers: { dataset: args[0], worksheetId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "bundle.export":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/bundles/exported/${encodeURIComponent(id,)}`,),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "bundle.publish":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/bundles/${encodeURIComponent(id,)}/publish`,),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "bundle.activate":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/bundles/imported/${encodeURIComponent(id,)}/actions/activate`,),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "bundle.preload":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/bundles/imported/${encodeURIComponent(id,)}/actions/preload`,),
				identifiers: { bundleId: id, },
				payload: {},
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
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
				}`,
				identifiers: { lang: args[0], name: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "code-env.set-packages": {
			const installCorePackages = parseBooleanOption(
				flags["install-core-packages"],
				"--install-core-packages",
			);
			const payload: Record<string, unknown> = {
				specPackageList: codeEnvPackageList(flags,).join("\n",),
			};
			if (installCorePackages !== undefined) {
				payload.desc = { installCorePackages, };
			}
			return {
				method: "PUT",
				endpoint: codeEnvEndpoint(),
				identifiers: { lang: args[0], name: args[1], },
				payload,
			};
		}
		case "code-env.update-packages": {
			const versionToUpdate = typeof flags["env-version"] === "string"
				? flags["env-version"]
				: typeof flags["version"] === "string"
				? flags["version"]
				: undefined;
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/packages",)}${
					querySuffix({
						forceRebuildEnv: flags["force-rebuild"] === true,
						versionToUpdate,
						wait: codeEnvWait(flags,),
					},)
				}`,
				identifiers: { lang: args[0], name: args[1], },
				wait: codeEnvWait(flags,),
			};
		}
		case "code-env.set-jupyter": {
			const active = parseBooleanOption(flags["active"], "--active",);
			if (active === undefined) {
				throw new UsageError(
					"--active is required. Usage: dss code-env set-jupyter <lang> <name> --active true|false",
				);
			}
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/jupyter",)}${
					querySuffix({ active, wait: codeEnvWait(flags,), },)
				}`,
				identifiers: { lang: args[0], name: args[1], },
				wait: codeEnvWait(flags,),
			};
		}
		case "code-env.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/admin/code-envs/${encodeURIComponent(args[0],)}/${
					encodeURIComponent(args[1],)
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
					`/jupyter-notebooks/${encodeURIComponent(args[0],)}/sessions/${
						encodeURIComponent(args[1],)
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
				payload: { retain: num(flags["retain"], "--retain",), },
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
		case "project.delete": {
			const expectedProjectIncarnation = flags["expect-project-incarnation"] === undefined
				? undefined
				: requiredPlanFlag(flags, "expect-project-incarnation", entry.usage,);
			if (
				expectedProjectIncarnation !== undefined
				&& !/^[0-9a-f]{64}$/.test(expectedProjectIncarnation,)
			) {
				throw new UsageError(
					"--expect-project-incarnation must be a 64-character lowercase SHA-256 hash.",
					"validation_failed",
				);
			}
			const endpoint = `/public/api/projects/${encodeURIComponent(id,)}${
				querySuffix({
					clearManagedDatasets: flags["drop-data"] === true,
					clearOutputManagedFolders: false,
					clearJobAndScenarioLogs: true,
					wait: true,
				},)
			}`;
			const guarded = flags["if-exists"] === true
				|| flags["dry-run"] === true
				|| expectedProjectIncarnation !== undefined;
			if (!guarded) {
				return {
					method: "DELETE",
					endpoint,
					identifiers: { projectKey: id, },
				};
			}
			const projectProbe = `/public/api/projects/${encodeURIComponent(id,)}/`;
			const incarnationGate = expectedProjectIncarnation === undefined
				? { required: false, provided: false, }
				: {
					required: true,
					provided: true,
					expectedHash: expectedProjectIncarnation,
				};
			if (flags["dry-run"] === true) {
				return {
					method: "GET",
					endpoint: projectProbe,
					identifiers: {
						projectKey: id,
						dryRun: true,
						ifExists: flags["if-exists"] === true,
						projectIncarnationGate: incarnationGate,
						note:
							"Read-only guarded-delete preflight. No DELETE is issued; a supplied creationTag hash must match.",
					},
				};
			}
			return {
				method: "DELETE",
				endpoint,
				identifiers: {
					projectKey: id,
					ifExists: flags["if-exists"] === true,
					projectIncarnationGate: incarnationGate,
					incarnationControl: expectedProjectIncarnation === undefined
						? "none"
						: "client-side-non-atomic-stale-identity-check",
					preflightRequests: [{
						method: "GET",
						endpoint: projectProbe,
						intent: expectedProjectIncarnation === undefined
							? "Confirm existence before a convergent delete."
							: "Recompute the current project-incarnation hash from creationTag before DELETE.",
					},],
					note: expectedProjectIncarnation === undefined
						? "Convergent delete: a 404 preflight is an already-absent success only with --if-exists."
						: "Incarnation-bound delete: the GET must match the expected creationTag hash before an unconditional DELETE. DSS exposes no conditional project DELETE, so the client-side gate cannot serialize against key reuse after the GET.",
				},
			};
		}
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
		case "project.import": {
			const settings = jsonInput(flags,) ?? {};
			const rawTarget = flags["target-project-key"] as string | undefined;
			const targetProjectKey = rawTarget?.trim();
			if (rawTarget !== undefined && targetProjectKey === "") {
				throw new UsageError(
					`--target-project-key must not be empty. Usage: ${entry.usage}`,
				);
			}
			const settingsTarget = settings.targetProjectKey;
			if (
				settingsTarget !== undefined
				&& (typeof settingsTarget !== "string" || settingsTarget.trim() === "")
			) {
				throw new UsageError(
					`targetProjectKey in import settings must be a non-empty string. Usage: ${entry.usage}`,
				);
			}
			if (
				targetProjectKey !== undefined
				&& settingsTarget !== undefined
				&& targetProjectKey !== settingsTarget.trim()
			) {
				throw new UsageError(
					`--target-project-key conflicts with targetProjectKey in import settings. Usage: ${entry.usage}`,
				);
			}
			const processPayload = targetProjectKey === undefined
				? settings
				: { ...settings, targetProjectKey, };
			const finalizedPayload = Object.keys(processPayload,).length === 0
				? { _: "_", }
				: processPayload;
			const upload = {
				method: "POST",
				endpoint: "/public/api/projects/import/upload",
				payload: {
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: id,
					fileName: "tmp-import.zip",
				},
			};
			return {
				...upload,
				identifiers: {
					filePath: id,
					...(targetProjectKey ? { targetProjectKey, } : {}),
					archivePreflight: {
						local: true,
						required: true,
						checks: [
							"zip-integrity",
							"safe-unique-members",
							"manifest",
							"flow-references",
							"orphan-members",
						],
					},
					successVerification: {
						usedProjectKeyRequired: true,
						projectReadRequired: true,
						projectIncarnationRequired: true,
						remappingReported: true,
					},
					...(flags["record-cleanup"] === undefined
						? {}
						: {
							cleanupBinding: "actual used project key plus verified creationTag incarnation hash",
						}),
				},
				requests: [
					{ sequence: 1, ...upload, },
					{
						sequence: 2,
						method: "POST",
						endpoint: "/public/api/projects/import/{importId}/process",
						pathBindings: { importId: "requests[0].response.id", },
						payload: finalizedPayload,
					},
				],
			};
		}
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
		case "continuous-activity.stop":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/continuous-activities/${encodeURIComponent(id,)}/stop`,),
				identifiers: { recipeId: id, },
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
		/* ---- project-git: mutations only; reads are never planned ---- */
		case "project-git.set-remote": {
			const remote = (flags["name"] as string | undefined) ?? "origin";
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/remotes/${encodeURIComponent(remote,)}`,
				),
				identifiers: { remote, },
				payload: { url: requiredPlanRepositoryUrl(flags, "repository", entry.usage,), },
			};
		}
		case "project-git.remove-remote": {
			const remote = (flags["name"] as string | undefined) ?? "origin";
			return {
				method: "DELETE",
				endpoint: projectGitEndpoint(
					projectKey,
					`/remotes/${encodeURIComponent(remote,)}`,
				),
				identifiers: { remote, },
			};
		}
		case "project-git.create-branch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/branches/",),
				identifiers: { name: id, },
				payload: {
					name: id,
					commit: (flags["commit"] as string | undefined) ?? null,
					duplicateProject: parseBooleanOption(
						flags["duplicate-project"],
						"--duplicate-project",
					) ?? false,
					targetProjectKey: (flags["target-project-key"] as string | undefined) ?? null,
					targetProjectFolderId: (flags["target-project-folder-id"] as string | undefined) ?? null,
				},
			};
		case "project-git.delete-branch": {
			const remote = parseBooleanOption(flags["remote"], "--remote",) ?? false;
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/deleteBranch",),
				identifiers: { name: id, remote, },
				payload: {
					name: id,
					remote,
					deleteRemotely: parseBooleanOption(
						flags["delete-remotely"],
						"--delete-remotely",
					) ?? false,
					forceDelete: parseBooleanOption(flags["force-delete"], "--force-delete",) ?? false,
				},
			};
		}
		case "project-git.switch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/switchBranch${querySuffix({ branchName: id, },)}`,
				),
				identifiers: { branch: id, },
			};
		case "project-git.create-tag":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/tags/",),
				identifiers: { name: id, },
				payload: {
					name: id,
					reference: (flags["reference"] as string | undefined) ?? "HEAD",
					message: (flags["message"] as string | undefined) ?? "",
				},
			};
		case "project-git.delete-tag":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/deleteTag",),
				identifiers: { name: id, },
				payload: { name: id, },
			};
		case "project-git.fetch":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/fetch",),
			};
		case "project-git.pull":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/pullRebase${
						querySuffix({
							branchName: flags["branch"] as string | undefined,
						},)
					}`,
				),
				...((flags["branch"] as string | undefined) !== undefined
					? { identifiers: { branch: flags["branch"], }, }
					: {}),
			};
		case "project-git.push":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/push${
						querySuffix({
							branchName: flags["branch"] as string | undefined,
						},)
					}`,
				),
				...((flags["branch"] as string | undefined) !== undefined
					? { identifiers: { branch: flags["branch"], }, }
					: {}),
			};
		case "project-git.commit": {
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/commit",),
				identifiers: { message, },
				payload: { message, },
			};
		}
		case "project-git.revert-to-revision":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/revertToRevision${querySuffix({ commit: id, },)}`,
				),
				identifiers: { commit: id, },
			};
		case "project-git.revert-commit":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/revertCommit${querySuffix({ commit: id, },)}`,
				),
				identifiers: { commit: id, },
			};
		case "project-git.reset-to-head":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/resetToLocalHeadState",),
			};
		case "project-git.reset-to-upstream":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/resetToRemoteHeadState",),
			};
		case "project-git.drop-and-rebuild": {
			if (
				parseBooleanOption(
					flags["i-know-what-i-am-doing"],
					"--i-know-what-i-am-doing",
				) !== true
			) {
				throw new UsageError(
					`--i-know-what-i-am-doing is required to acknowledge the irreversible Git history loss. Usage: ${entry.usage}`,
				);
			}
			return {
				method: "POST",
				endpoint: projectGitEndpoint(
					projectKey,
					`/actions/dropAndRebuild${querySuffix({ iKnowWhatIAmDoing: true, },)}`,
				),
			};
		}
		case "project-git.add-library": {
			const targetPath = id;
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/",),
				identifiers: { localTargetPath: targetPath, },
				payload: {
					repository: requiredPlanRepositoryUrl(flags, "repository", entry.usage,),
					login: (flags["login"] as string | undefined) ?? null,
					password: flags["password-env"] !== undefined ? "***" : null,
					pathInGitRepository: (flags["path-in-repository"] as string | undefined) ?? "",
					localTargetPath: targetPath,
					checkout: requiredPlanFlag(flags, "checkout", entry.usage,),
					addToPythonPath: parseBooleanOption(
						flags["no-add-to-python-path"],
						"--no-add-to-python-path",
					) !== true,
				},
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.set-library": {
			const targetPath = id;
			return {
				method: "PUT",
				endpoint: projectGitEndpoint(
					projectKey,
					`/lib-git-refs/${encodeGitReferencePath(targetPath,)}`,
				),
				identifiers: { library: targetPath, },
				payload: {
					repository: requiredPlanRepositoryUrl(flags, "repository", entry.usage,),
					login: (flags["login"] as string | undefined) ?? null,
					password: flags["password-env"] !== undefined ? "***" : null,
					pathInGitRepository: (flags["path-in-repository"] as string | undefined) ?? "",
					checkout: requiredPlanFlag(flags, "checkout", entry.usage,),
				},
			};
		}
		case "project-git.remove-library": {
			const targetPath = id;
			return {
				method: "DELETE",
				endpoint: projectGitEndpoint(
					projectKey,
					`/lib-git-refs/${encodeGitReferencePath(targetPath,)}${
						querySuffix({
							deleteDirectory: parseBooleanOption(
								flags["delete-directory"],
								"--delete-directory",
							) ?? false,
						},)
					}`,
				),
				identifiers: { library: targetPath, },
			};
		}
		case "project-git.reset-library": {
			const targetPath = id;
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/action/reset",),
				identifiers: { library: targetPath, },
				payload: { gitRef: targetPath, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.push-library": {
			const targetPath = id;
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/action/push",),
				identifiers: { library: targetPath, message, },
				payload: { gitRef: targetPath, commitMessage: message, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.push-all-libraries": {
			const message = requiredPlanFlag(flags, "message", entry.usage,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/git-refs/push-all",),
				identifiers: { message, },
				payload: { commitMessage: message, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.reset-all-libraries":
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/actions/git-refs/reset-all",),
				wait: projectGitFutureWait(),
			};
		case "project-git.future-abort":
			return {
				method: "DELETE",
				endpoint: projectGitFutureEndpoint(id,),
				identifiers: { jobId: id, },
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
/**
 * Batch plans may surface exit 4: any dispatched step can be a long-running
 * lifecycle command whose remote work fails, or a synchronous assertion
 * mismatch (`recipe assert-unchanged`). Batch itself stays async "none".
 */
export const BATCH_PLAN_EXIT_CODES: Record<string, number> = {
	usage: 1,
	error: 2,
	transient: 3,
	longRunningFailure: 4,
	assertionFailure: 4,
};

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
