import { validateCredentials, } from "../auth.js";
import { getCredentialsPath, saveCredentials, } from "../config.js";
import { DataikuError, } from "../errors.js";
import actionOutputSchemasMetadata from "../generated/action-output-schemas.json" with {
	type: "json",
};
import { commands, } from "./commands/index.js";
import { BOOLEAN_FLAGS, FLAG_ALIASES, SHORT_FLAGS, } from "./flags.js";
import { resolveLoginCredentials, } from "./runtime.js";
import {
	type CommandInputContract,
	type CommandPositionalMetadata,
	commandSyntax,
	commandSyntaxKeys,
	commandSyntaxTree,
	commandUsage,
	renderNodes,
	syntaxCommandWords,
	syntaxHasFlag,
	type SyntaxNode,
} from "./syntax.js";
import type {
	CommandFlagChoice,
	CommandMeta,
	CommandPayloadSchema,
	CommandRegistryOverride,
} from "./types.js";
import { inferRequiresProject, UsageError, } from "./usage.js";
import {
	AGENT_CONTRACT_SCHEMA_ID,
	AGENT_CONTRACT_VERSION,
	cliVersionResult,
	JSON_SCHEMA_DRAFT,
} from "./version.js";

export function splitPackageSpec(raw: string,): string[] {
	return raw.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0);
}

export const AUTH_ACTIONS: Record<string, {
	handler: (flags: Record<string, string | boolean>,) => Promise<unknown>;
	description?: string;
	examples?: string[];
	requiredFlags?: string[];
}> = {
	login: {
		handler: async (flags,) => {
			const { url, apiKey, projectKey, tlsSettings, } = resolveLoginCredentials(flags,);

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

export interface CommandRequiredInputAlternative {
	flags?: string[];
	positionals?: string[];
}

export interface CommandRequiredInputGroup {
	oneOf: CommandRequiredInputAlternative[];
}

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
	positionalArguments: CommandPositionalMetadata[];
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
	requiredInputGroups?: CommandRequiredInputGroup[];
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
/**
 * Security admin actions that hand back a DSS future instead of settling
 * inline: resyncs mint sync jobs, external-user/group fetches mint a
 * supplier-enumeration job, and provision runs a supplier import. Without
 * this table they fall through to the read default and `--plan` rejects
 * them as non-mutating.
 */
const ADMIN_SECURITY_FUTURE_ACTIONS: Record<string, true> = {
	"external-groups": true,
	"external-users": true,
	provision: true,
	resync: true,
	"resync-multi": true,
};

/** Connection tables-import actions return a DSS future reference. */
const CONNECTION_FUTURE_ACTIONS: Record<string, true> = {
	"execute-import": true,
	"prepare-import": true,
};

/** Plugin actions that only observe state despite POST shape (documented). */
const PLUGIN_GIT_OBSERVER_ACTIONS: Record<string, true> = {
	"git-branches": true,
	list: true,
	usages: true,
};

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

/** Plugin actions that discard work or publish it beyond the instance. */
const PLUGIN_DESTRUCTIVE_ACTIONS: Record<string, true> = {
	"contents-delete": true,
	delete: true,
	"delete-git-remote": true,
	fetch: true,
	pull: true,
	push: true,
	"reset-local": true,
	"reset-remote": true,
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
export const COMMANDS_USAGE = commandUsage("commands", "run",);
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
export const AGENT_CONTRACT_USAGE = commandUsage("agent", "contract",);
const AGENT_CONTRACT_DESCRIPTION =
	"Print the versioned JSON agent contract; scope bootstrap fields, use commands.actions to enumerate the surface, and read schemas only when needed.";
const AGENT_CONTRACT_EXAMPLES = [
	"dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility",
	"dss agent contract --fields commands.actions",
];
const VERSION_USAGE = commandUsage("version", "run",);
const VERSION_DESCRIPTION =
	"Print the CLI version, checkout/build revisions, load source, runtime, and stale-build status as JSON.";
const VERSION_EXAMPLES = ["dss version", "dss --version",];
const INSTALL_SKILL_USAGE = commandUsage("install-skill", "run",);
const INSTALL_SKILL_DESCRIPTION =
	"Report missing/stale/current skill status and atomically install changed dataiku-dss agent skills.";
const INSTALL_SKILL_EXAMPLES = [
	"dss install-skill --list-agents",
	"dss install-skill --agent omp --dry-run",
];
export const CLEANUP_USAGE = commandUsage("cleanup", "run",);
const CLEANUP_DESCRIPTION =
	"Replay cleanup entries in reverse order; failures include structured code/category/exitCode/retryability summaries.";
const CLEANUP_EXAMPLES = [
	"dss cleanup --file cleanup.jsonl",
	"dss cleanup --file cleanup.jsonl --apply",
];
const FIXTURES_USAGE = commandUsage("fixtures", "run",);
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
	"meaning delete",
	"notebook delete-jupyter",
	"notebook delete-sql",
	"project-library delete",
	"streaming-endpoint delete",
	"workspace delete",
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

/**
 * Discovery output schemas: the generated declarative metadata committed at
 * `src/generated/action-output-schemas.json`. Plain data only, so discovery
 * (`dss commands run`, `dss agent contract`) never loads the TypeBox graph.
 * The content matches `typeBoxCommandOutputSchemas()` exactly; parity and
 * freshness are enforced by tests/cli/discovery-output-schemas.test.ts and
 * `bun run check`.
 */
const actionOutputSchemas: Record<string, Record<string, unknown>> = actionOutputSchemasMetadata
	.schemas;

function outputJsonSchema(
	resource: string,
	action: string,
	shape: CommandOutputShape,
): Record<string, unknown> {
	const precise = actionOutputSchemas[registryKey(resource, action,)];
	if (precise) return precise;
	if (shape === "array") return { type: "array", items: true, };
	if (shape === "string") return { type: "string", };
	return { type: "object", additionalProperties: true, };
}

function payloadJsonSchema(
	payloadSchema: CommandPayloadSchema | undefined,
): Record<string, unknown> | undefined {
	if (!payloadSchema) return undefined;
	if (payloadSchema.contentType === "text/plain") {
		return { type: "string", contentMediaType: "text/plain", };
	}
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
function argvPrefix(resource: string, action: string,): string[] {
	const words = syntaxCommandWords(commandSyntaxTree(resource, action,) ?? [],);
	return words[0] === resource && words[1] === action ? [resource, action,] : [resource,];
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
	const prefix = argvPrefix(resource, action,);
	const aliases: Record<string, string[]> = {};
	for (const flag of flags) {
		aliases[flag.name] = flag.aliases ?? [];
	}
	const booleanNames = flags.filter((flag,) => flag.kind === "boolean")
		.flatMap((flag,) => [flag.name, ...(flag.aliases ?? []),]);
	const valueFlags = flags.filter((flag,) => flag.kind === "value");
	const requiredPositionals = commandSyntax(resource, action,).positionalArguments
		.filter((positional,) => positional.required).map((positional,) => positional.name);
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
				? "Writes access-control identities and permissions to an owner-only local file."
				: "Writes a local file; stdout contains JSON metadata.",
			...(sensitivePermissions
				? {
					safeAlternative:
						"Keep owner-only and outside version control unless repository policy explicitly permits committing access-control data.",
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
		output: outputJsonSchema(resource, action, outputShape,),
	};
}

const EXPLICIT_REGISTRY_OVERRIDES: Record<string, CommandRegistryOverride> = {
	"code.run": {
		payloadSchema: { stdin: true, contentType: "text/plain", },
	},
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

const COMMAND_REQUIRED_INPUT_GROUPS: Record<string, CommandRequiredInputGroup[]> = {
	"notebook.unload-jupyter": [
		{
			oneOf: [
				{ positionals: ["name", "sessionId",], },
				{ flags: ["all",], },
			],
		},
	],
	"sql.query": [
		{
			oneOf: [
				{ positionals: ["SQL",], },
				{ flags: ["sql",], },
				{ flags: ["sql-file",], },
				{ flags: ["stdin",], },
			],
		},
		{ oneOf: [{ flags: ["connection",], }, { flags: ["dataset",], },], },
	],
	"flow-zone.move": [
		{
			oneOf: [
				{ positionals: ["id",], },
				{ flags: ["zone",], },
				{ flags: ["zone-id",], },
			],
		},
		{
			oneOf: [
				{ flags: ["dataset",], },
				{ flags: ["recipe",], },
				{ flags: ["folder",], },
				{ flags: ["object",], },
			],
		},
	],
};

function requiredInputGroups(resource: string, action: string,): CommandRequiredInputGroup[] {
	return COMMAND_REQUIRED_INPUT_GROUPS[registryKey(resource, action,)] ?? [];
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
	if (resource === "sql" && action === "query") return "write";
	if (resource === "ml-task" && action === "train") return "write";
	// Project and dashboard exports stream a rendered/archive artifact and write it
	// locally; the DSS-side resource is untouched.
	if ((resource === "project" || resource === "dashboard") && action === "export") return "read";
	if (resource === "data-quality" && action === "compute") return "write";
	if (resource === "user" && ADMIN_SECURITY_FUTURE_ACTIONS[action] === true) return "write";
	if (resource === "connection" && CONNECTION_FUTURE_ACTIONS[action] === true) return "write";
	// Saved-model evaluation triggers scoring work on DSS side (cost-bearing).
	if (resource === "saved-model" && action === "evaluate-version") return "write";
	// LLM Mesh completions/embeddings invoke the LLM provider (cost-bearing,
	// arbitrary-prompt execution); the verb shapes match no mutating pattern
	// and would fall through to read, wrongly rejecting --plan. Knowledge-bank
	// search stays read: it only queries an existing index.
	if (resource === "llm" && (action === "completions" || action === "embeddings")) {
		return "write";
	}
	// Project Git is classified from the explicit read table, not by verb shape:
	// `switch`, `fetch`, `pull`, `push`, `commit`, the `reset-*`/`revert-*`
	// families, `drop-and-rebuild`, and `future-abort` all mutate repository or
	// future state yet match none of the mutating-verb patterns.
	// Plugin dev-Git actions mutate repository/plugin state like project-git:
	// `fetch`/`pull`/`push`/`reset-*` never match the generic mutating-verb
	// regex, so they are classified explicitly as write (destructive level
	// from the explicit table below). `git-branches` is a state observer
	// (observed GET on DSS 15; the upstream docs' POST is wrong) and stays
	// read.
	if (resource === "plugin") {
		return PLUGIN_GIT_OBSERVER_ACTIONS[action] === true ? "read" : "write";
	}
	if (resource === "project-git") {
		return PROJECT_GIT_READ_ACTIONS[action] === true ? "read" : "write";
	}
	if (READ_ACTIONS.has(action,)) return "read";
	if (
		/^(create|clone|restore|update|delete|set|save|upload|run|build|abort|move|refresh|clear|unload|install|login|logout|add|remove|publish|activate|deploy|import|export|preload|upgrade|start|stop|restart|duplicate|put|rename|reply|compute|organize)/
			.test(action,)
		// Compound actions whose mutating verb is a suffix (e.g. permissions-set,
		// dataset-compute, saved-model external-metadata-put).
		|| /-(set|compute|restore|put)$/.test(action,)
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

const GLOBAL_FLAG_VALUE_HINTS: Record<string, { valueType: string; enumValues?: string[]; }> = {
	url: { valueType: "URL", },
	fields: { valueType: "CSV", },
	"api-key": { valueType: "KEY", },
	"request-timeout": { valueType: "MS", },
	retries: { valueType: "N", },
	"ca-cert": { valueType: "PATH", },
	"project-key": { valueType: "KEY", },
	"record-cleanup": { valueType: "PATH", },
	color: { valueType: "HEX", },
	data: { valueType: "JSON", },
	local: { valueType: "JSON", },
	standard: { valueType: "JSON", },
	until: { valueType: "STATE", },
};

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
		|| `${resource}.${action}` === "flow-zone.organize"
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
	if (`${resource}.${action}` === "notebook.save-jupyter") {
		return "dss notebook delete-jupyter <name> --if-exists";
	}
	if (`${resource}.${action}` === "notebook.save-sql") {
		return "dss notebook delete-sql <the returned `saved` id> --if-exists";
	}
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
	const deleteSyntax = commandSyntaxTree(resource, deleteAction,);
	if (!deleteSyntax) return undefined;
	const base = renderNodes(deleteSyntax, true,);
	if (syntaxHasFlag(deleteSyntax, "if-exists",)) return `${base} --if-exists`;
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
	"sql.query": true,
	// Plugin dev-Git and content deletions discard work with no undo route.
	"plugin.delete": true,
	"plugin.delete-git-remote": true,
	"plugin.push": true,
	"plugin.pull": true,
	"plugin.fetch": true,
	"plugin.reset-local": true,
	"plugin.reset-remote": true,
	"plugin.contents-delete": true,
	// Macro runs execute arbitrary plugin code.
	"macro.run": true,
	"macro.run-and-wait": true,
	// Saved-model version deletion removes model versions permanently.
	"saved-model.delete-versions": true,
};

function inferDestructiveLevel(
	resource: string,
	sideEffect: CommandSideEffect,
	action: string,
): CommandDestructiveLevel {
	if (sideEffect !== "write") return "none";
	if (EXPLICIT_DESTRUCTIVE_KEYS[`${resource}.${action}`] === true) return "destructive";
	if (resource === "plugin") {
		return PLUGIN_DESTRUCTIVE_ACTIONS[action] === true ? "destructive" : "reversible";
	}
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
	if (resource === "ml-task" && action === "train") return "job";
	if (resource === "future" && ["get", "peek", "wait", "abort",].includes(action,)) return "future";
	if (resource === "scenario" && ["run", "run-and-wait", "status",].includes(action,)) {
		return "future";
	}
	if (resource === "user" && ADMIN_SECURITY_FUTURE_ACTIONS[action] === true) return "future";
	if (resource === "connection" && CONNECTION_FUTURE_ACTIONS[action] === true) return "future";
	if (resource === "data-quality" && action === "compute") return "future";
	// Project Git mutation results carry a future job id only for the library
	// calls and the future lifecycle itself; plain Git actions settle inline.
	if (resource === "project-git" && PROJECT_GIT_FUTURE_ACTIONS[action] === true) return "future";
	if (resource === "code" && action === "run") return "future";
	if (resource === "webapp" && action === "restart-backend") return "future";
	if (
		resource === "code-env"
		&& ["create", "update-packages", "update-images", "set-jupyter", "delete",].includes(action,)
	) return "future";
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
	syntax: SyntaxNode[],
): CommandIdempotency {
	if (sideEffect === "read") return "safe";
	if (`${resource}.${action}` === "install-skill.run") return "convergent";
	if (action.startsWith("create",) && syntaxHasFlag(syntax, "if-not-exists",)) {
		return "if-not-exists";
	}
	if (action.startsWith("delete",) && syntaxHasFlag(syntax, "if-exists",)) return "if-exists";
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
	const key = `${resource}.${action}`;
	if (key === "notebook.save-jupyter") {
		return "If created:true, delete with `dss notebook delete-jupyter <name> --if-exists`.";
	}
	if (key === "notebook.save-sql") {
		return "If created:true, the persisted id is in `saved`; delete with `dss notebook delete-sql <saved> --if-exists`.";
	}
	if (!(action.startsWith("create",) || action === "clone")) return undefined;
	if (LEDGER_ONLY_CLEANUP_KEYS[key] === true) return LEDGER_ONLY_CLEANUP_HINT;
	const deleteAction = action === "create-rule"
		? "delete-rule"
		: action === "create-instance" || action === "create-successor-instance"
		? "delete-instance"
		: "delete";
	const deleteSyntax = commandSyntaxTree(resource, deleteAction,);
	if (!deleteSyntax) return undefined;
	const ifExists = syntaxHasFlag(deleteSyntax, "if-exists",) ? " --if-exists" : "";
	if (resource === "code-env") {
		return `Delete with \`dss code-env delete <lang> <name>${ifExists}\`.`;
	}
	if (resource === "data-quality") {
		return `Delete with \`dss data-quality delete-rule <dataset> <rule-id>${ifExists}\`.`;
	}
	return `For disposable creates: \`dss ${resource} delete <id>${ifExists}\`.`;
}

export function buildRegistryEntry(
	resource: string,
	action: string,
	meta: CommandMeta,
): CommandRegistryEntry {
	const requiresAuth = meta.localHandler === undefined && inferRequiresAuth(resource,);
	const requiresProject = inferRequiresProject(resource, action,);
	const sideEffect = inferSideEffect(resource, action,);
	const destructive = inferDestructiveLevel(resource, sideEffect, action,);
	const asyncKind = inferAsyncKind(resource, action,);
	const mutatesDss = sideEffect === "write" && resource !== "auth" && resource !== "install-skill";
	// auth login writes local credentials only; its --plan previews that write.
	const supportsPlan = sideEffect === "write" || resource === "auth";
	const supportsCleanup = supportsCleanupLedger(resource, action,);
	const syntax = commandSyntax(resource, action,);
	const usageFlags = syntax.flags;
	const flags = uniqueStrings([
		...usageFlags,
		...(supportsPlan ? ["plan",] : []),
		...(supportsCleanup ? ["record-cleanup",] : []),
		...GLOBAL_AGENT_FLAGS,
		...(requiresAuth ? AUTHENTICATED_AGENT_FLAGS : []),
		...(requiresProject ? ["project-key",] : []),
	],);
	const derivedRequired = syntax;
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
	const valueHints = syntax.valueHints;
	const inputContract = syntax.inputContract;
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
		const aliases = syntax.aliases[name] ?? [];
		const aliasPart = aliases.length > 0 ? { aliases, } : {};
		const kind = flagKind(name,);
		if (kind === "boolean") return { name, kind, ...aliasPart, };
		const allowEmptyPart = ALLOW_EMPTY_VALUE_FLAGS[name] === true ? { allowEmptyValue: true, } : {};
		const hint = valueHints[name] ?? GLOBAL_FLAG_VALUE_HINTS[name];
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
	const positionalArguments = syntax.positionalArguments;
	const positionals = positionalArguments.map((positional,) => positional.name);
	const inputGroups = requiredInputGroups(resource, action,);
	const outputShape = inferOutputShape(resource, action,);
	const producesLocalFile = syntax.producesLocalFile;
	const uniqueRequiredFlags = uniqueStrings(requiredFlags,);
	const uniqueOptionalFlags = uniqueStrings(optionalFlags,);
	const unsafe = unsafeOutputs(resource, action, producesLocalFile,);
	let schemas: CommandAgentSchemas | undefined;
	return {
		resource,
		action,
		usage: meta.usage,
		description: meta.description,
		examples: meta.examples,
		structuredExamples: structuredExamples(meta.examples, examplePayload,),
		flags: flagMetadata,
		positionals,
		positionalArguments,
		sideEffect,
		requiresAuth,
		requiresProject,
		outputShape,
		inputContract,
		destructive,
		producesLocalFile,
		mutatesDss,
		async: asyncKind,
		idempotency: inferIdempotency(
			resource,
			sideEffect,
			action,
			commandSyntaxTree(resource, action,) ?? [],
		),
		dryRun: syntax.dryRun,
		requiredFlags: uniqueRequiredFlags,
		optionalFlags: uniqueOptionalFlags,
		...(requiredOneOf.length > 0 ? { requiredOneOf, } : {}),
		...(inputGroups.length > 0 ? { requiredInputGroups: inputGroups, } : {}),
		...(payloadSchema ? { payloadSchema, } : {}),
		get schemas() {
			return schemas ??= buildCommandSchemas(
				resource,
				action,
				flagMetadata,
				uniqueRequiredFlags,
				requiredOneOf,
				payloadSchema,
				outputShape,
				meta.usage,
			);
		},
		...(unsafe ? { unsafeOutputs: unsafe, } : {}),
		...(examplePayload !== undefined ? { examplePayload, } : {}),
		...(cleanupCommand ? { cleanupCommand, } : {}),
		exitCodes: inferExitCodes(resource, action, asyncKind,),
		...(cleanupHint ? { cleanupHint, } : {}),
		agentContractVersion: AGENT_CONTRACT_VERSION,
	};
}

function commandDefinitions(
	resourceFilter?: string,
): Record<string, Record<string, CommandMeta>> {
	const registry: Record<string, Record<string, CommandMeta>> = {};
	for (const resource of Object.keys(commands,)) {
		if (resourceFilter !== undefined && resource !== resourceFilter) continue;
		// Forward the lazy lookup: enumerating resources must not load command modules.
		Object.defineProperty(registry, resource, { enumerable: true, get: () => commands[resource]!, },);
	}
	if (resourceFilter === undefined || resourceFilter === "commands") {
		registry.commands = {
			run: {
				handler: async () => undefined,
				usage: COMMANDS_USAGE,
				description: COMMANDS_DESCRIPTION,
				examples: COMMANDS_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "agent") {
		registry.agent = {
			contract: {
				handler: async () => undefined,
				usage: AGENT_CONTRACT_USAGE,
				description: AGENT_CONTRACT_DESCRIPTION,
				examples: AGENT_CONTRACT_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "version") {
		registry.version = {
			run: {
				handler: async () => undefined,
				usage: VERSION_USAGE,
				description: VERSION_DESCRIPTION,
				examples: VERSION_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "install-skill") {
		registry["install-skill"] = {
			run: {
				handler: async () => undefined,
				usage: INSTALL_SKILL_USAGE,
				description: INSTALL_SKILL_DESCRIPTION,
				examples: INSTALL_SKILL_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "cleanup") {
		registry.cleanup = {
			run: {
				handler: async () => undefined,
				usage: CLEANUP_USAGE,
				description: CLEANUP_DESCRIPTION,
				examples: CLEANUP_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "fixtures") {
		registry.fixtures = {
			run: {
				handler: async () => undefined,
				usage: FIXTURES_USAGE,
				description: FIXTURES_DESCRIPTION,
				examples: FIXTURES_EXAMPLES,
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "batch") {
		registry.batch = {
			run: {
				handler: async () => undefined,
				usage: BATCH_USAGE,
				description: BATCH_DESCRIPTION,
				examples: BATCH_EXAMPLES,
				examplePayload: BATCH_EXAMPLE_PAYLOAD,
				payloadSchema: { stdin: true, dataFlag: true, dataFileFlag: true, jsonShape: "array", },
			},
		};
	}
	if (resourceFilter === undefined || resourceFilter === "auth") {
		registry.auth = {};
		for (const [action, meta,] of Object.entries(AUTH_ACTIONS,)) {
			registry.auth[action] = {
				handler: async () => undefined,
				usage: commandUsage("auth", action,),
				description: meta.description,
				examples: meta.examples,
				requiredFlags: meta.requiredFlags,
			};
		}
	}
	return registry;
}

export function buildCommandRegistry(
	resourceFilter?: string,
): Record<string, Record<string, CommandRegistryEntry>> {
	const registry: Record<string, Record<string, CommandRegistryEntry>> = {};
	for (const [resource, actions,] of Object.entries(commandDefinitions(resourceFilter,),)) {
		const entries: Record<string, CommandRegistryEntry> = {};
		registry[resource] = entries;
		for (const [action, meta,] of Object.entries(actions,)) {
			let entry: CommandRegistryEntry | undefined;
			Object.defineProperty(entries, action, {
				enumerable: true,
				get: () => entry ??= buildRegistryEntry(resource, action, meta,),
			},);
		}
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
			"positionalArguments",
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
			positionalArguments: {
				type: "array",
				items: {
					type: "object",
					required: ["name", "required",],
					additionalProperties: false,
					properties: { name: { type: "string", }, required: { type: "boolean", }, },
				},
			},
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
			requiredInputGroups: { type: "array", items: { type: "object", additionalProperties: true, }, },
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
			category: { enum: ["usage", "permission_or_environment", "dss", "internal",], },
			exitCode: { type: "number", },
			hint: { type: "string", },
			status: { type: "number", },
			retryable: { type: "boolean", },
			requestId: { type: "string", },
			details: { type: "object", additionalProperties: true, },
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

export function commandActionSummary(): Record<string, string[]> {
	// command-syntax.json lists exactly the registered commands (pinned by
	// tests/cli/command-syntax.test.ts), so action names come from it and the
	// summary loads no command module.
	const actionsByResource: Record<string, string[]> = {};
	for (const key of commandSyntaxKeys()) {
		const dot = key.indexOf(".",);
		(actionsByResource[key.slice(0, dot,)] ??= []).push(key.slice(dot + 1,),);
	}
	const summary: Record<string, string[]> = {};
	for (const resource of Object.keys(commandDefinitions(),)) {
		summary[resource] = (actionsByResource[resource] ?? []).sort();
	}
	return summary;
}

export function buildAgentContract(): Record<string, unknown> {
	let commandSection: Record<string, unknown> | undefined;
	let schemaSection: Record<string, unknown> | undefined;
	return {
		protocol: "dataiku-sdk-agent",
		agentContractVersion: AGENT_CONTRACT_VERSION,
		cli: cliVersionResult(),
		get commands() {
			return commandSection ??= {
				discoveryCommand: "dss commands run",
				fullRegistryExportCommand: "dss commands run --output PATH",
				scopedDiscoveryCommand: "dss commands run --fields RESOURCE[.ACTION[.FIELD...]]",
				actionIndexCommand: "dss agent contract --fields commands.actions",
				scopedDiscoveryExamples: [
					"dss commands run --fields dataset",
					"dss commands run --fields dataset.create",
				],
				scopedDiscoveryHint:
					"Default: resource/action summary. --fields RESOURCE: all resource entries; RESOURCE.ACTION: one entry keyed by that path; append .FIELD for nested metadata. Comma-separate paths. --output PATH exports the full registry.",
				actions: commandActionSummary(),
			};
		},
		get schemas() {
			return schemaSection ??= {
				agentContract: agentContractJsonSchema(),
				commandRegistry: commandRegistryJsonSchema(),
				commandRegistryEntry: commandRegistryEntryJsonSchema(),
				errorEnvelope: errorEnvelopeJsonSchema(),
				warningEvent: warningEventJsonSchema(),
				traceEvent: traceEventJsonSchema(),
			};
		},
		stdio: {
			stdout: {
				format: "compact-json",
				success: "single-json-value",
				failure: "structured-error-object",
				failureResultDetailLimitBytes: 65_536,
				fieldProjection: "Missing --fields paths: null on stdout; field_projection_missing on stderr.",
				richFailureResults:
					"doctor/batch/cleanup failures: own compact {ok:false,...} result on stdout and command exit code; no wrapper.",
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
			failFastWhenUnsupported: "Check agentContractVersion before planning.",
		},
	};
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

export const BATCH_USAGE = commandUsage("batch", "run",);
const BATCH_DESCRIPTION =
	"Run dss argv arrays fail-fast by default. The result aggregates failed/retryable/failureCodes; process exit remains the first failed step's exit code.";
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
