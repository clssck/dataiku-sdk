import * as fs from "node:fs";
import * as path from "node:path";
import { validateCredentials, } from "../auth.js";
import { getCredentialsPath, saveCredentials, } from "../config.js";
import { DataikuError, } from "../errors.js";
import actionOutputSchemasMetadata from "../generated/action-output-schemas.json" with {
	type: "json",
};
import { APP_MANIFEST_CONCURRENCY_CONTROL, } from "../resources/applications.js";
import { buildDatasetCreateBody, } from "../resources/datasets.js";
import { validatePluginDestinationPath, validatePluginPath, } from "../resources/plugins.js";
import {
	encodeLibraryPath,
	PROJECT_LIBRARY_CONCURRENCY_CONTROL,
	validateLibraryDestinationPath,
	validateLibraryName,
	validateLibraryPath,
} from "../resources/project-library.js";
import { buildRecipeCreateRequest, } from "../resources/recipes.js";
import { encodeGitReferencePath, validateGitReferencePath, } from "../utils/git-reference.js";
import {
	jobBuildTargetTypeFromFlags,
	json,
	jsonInput,
	num,
	parseBooleanOption,
	parseJsonObject,
	requiredJsonInput,
	rewritePairsFromFlags,
	schemaColumnsInput,
	sha256Hex,
	stableHash,
	stringField,
	textInput,
} from "./coerce.js";
import { parseCodeRunIntegerFlag, resolveCodeInputWithSource, } from "./commands/code.js";
import { commands, } from "./commands/index.js";
import { projectLibraryPutPayload, } from "./commands/project-library.js";
import { recipeCreateOptionsFromFlags, } from "./commands/recipe.js";
import { resolveSqlQueryInvocation, } from "./commands/sql.js";
import { ambientProjectKey, } from "./env.js";
import { BOOLEAN_FLAGS, executionMode, FLAG_ALIASES, SHORT_FLAGS, } from "./flags.js";
import { flowZoneColor, flowZoneMoveItems, flowZoneName, } from "./helpers/flow-zone.js";
import { recipeBackupPath, recipeRunShouldWait, } from "./helpers/recipe.js";
import { encodedProjectEndpointForPlan, planResult, } from "./output.js";
import { resolveLoginCredentials, } from "./runtime.js";
import {
	type CommandInputContract,
	type CommandPositionalMetadata,
	commandSyntax,
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
import { inferRequiresProject, requireArgs, UsageError, } from "./usage.js";
import {
	AGENT_CONTRACT_SCHEMA_ID,
	AGENT_CONTRACT_VERSION,
	cliVersionResult,
	JSON_SCHEMA_DRAFT,
} from "./version.js";

function codeEnvWait(flags: Record<string, string | boolean>,): boolean {
	return flags["no-wait"] !== true;
}

function codeEnvLang(value: string | undefined, usage: string,): "PYTHON" | "R" {
	if (value !== "PYTHON" && value !== "R") {
		throw new UsageError(
			`Invalid code environment language ${JSON.stringify(value,)}. Usage: ${usage}`,
		);
	}
	return value;
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

/**
 * Resolve requested package specs from --file/--packages/--package. An
 * explicit source that resolves to zero specs is a legitimate clear
 * (set-packages replaces specPackageList wholesale); only a call with no
 * package source flag at all is a usage error.
 */
function codeEnvPackageList(flags: Record<string, string | boolean>,): string[] {
	const file = flags["file"];
	const packages = typeof file === "string"
		? splitPackageSpec(fs.readFileSync(file, "utf-8",),)
		: [];
	if (typeof flags["packages"] === "string") {
		packages.push(...splitPackageSpec(flags["packages"],),);
	}
	if (typeof flags["package"] === "string") {
		packages.push(...splitPackageSpec(flags["package"],),);
	}
	if (
		typeof file !== "string"
		&& typeof flags["packages"] !== "string"
		&& typeof flags["package"] !== "string"
	) {
		throw new UsageError(
			"--packages, --package, or --file is required. Use newline-separated package specs for version constraints; an explicitly empty value (or empty file) clears the requested package list.",
		);
	}
	return packages;
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

function buildRegistryEntry(
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
		registry[resource] = commands[resource]!;
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
	const summary: Record<string, string[]> = {};
	for (const [resource, actions,] of Object.entries(commandDefinitions(),)) {
		summary[resource] = Object.keys(actions,).sort();
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
	const fromEnv = ambientProjectKey();
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

/**
 * Optional caller-chosen identifier. Absent selects the command's generated
 * mode; a present-but-empty value is an input error, never a silent omission.
 */
function optionalPlanFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string | undefined {
	const value = flags[name];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			`--${name} must not be empty when supplied; omit --${name} entirely to generate the successor key during apply. Usage: ${usage}`,
			"validation_failed",
		);
	}
	return value.trim();
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

function projectFolderEndpoint(folderId: string,): string {
	return `/public/api/project-folders/${encodeURIComponent(folderId,)}`;
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

function pluginRootEndpoint(pluginId: string, suffix: string,): string {
	return `/public/api/plugins/${encodeURIComponent(pluginId,)}${suffix}`;
}

function pluginActionEndpoint(pluginId: string, action: string,): string {
	return pluginRootEndpoint(pluginId, `/actions/${action}`,);
}

function pluginContentsEndpoint(pluginId: string, contentPath: string,): string {
	return `${pluginRootEndpoint(pluginId, "/contents/",)}${
		encodePluginSegmentsForPlan(contentPath,)
	}`;
}

function pluginFoldersEndpoint(pluginId: string, contentPath: string,): string {
	return `${pluginRootEndpoint(pluginId, "/folders/",)}${encodePluginSegmentsForPlan(contentPath,)}`;
}

/** Encodes a plugin content path per segment for plan-time endpoints. */
function encodePluginSegmentsForPlan(contentPath: string,): string {
	return contentPath.split("/",).map((segment,) => encodeURIComponent(segment,)).join("/",);
}

/**
 * Plan payload for plugin git install/update bodies. The repository URL is
 * validated (embedded userinfo rejected) and secrets are never echoed: the
 * plan carries the URL only, never embedded credentials.
 */
function pluginGitPlanPayload(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const repository = requiredPlanFlag(
		flags,
		"repository",
		"dss plugin install-from-git --repository URL",
	);
	// Mirrors PluginsResource install/update bodies: absent options are sent as null.
	return {
		gitRepositoryUrl: validatedPlanRepositoryUrl(repository, "repository", "--repository URL",),
		gitCheckout: typeof flags["checkout"] === "string" ? flags["checkout"] : null,
		gitSubpath: typeof flags["path-in-repository"] === "string" ? flags["path-in-repository"] : null,
	};
}

function optionalPlanProjectScope(
	flags: Record<string, string | boolean>,
): string {
	const projectKey = flags["project-key"];
	if (typeof projectKey !== "string" || projectKey.trim() === "") return "";
	return `?projectKey=${encodeURIComponent(projectKey.trim(),)}`;
}

function settingsConfigKeys(flags: Record<string, string | boolean>,): string[] {
	const content = flags["content"];
	if (typeof content !== "string") return [];
	try {
		const parsed: unknown = JSON.parse(content,);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed,)) return [];
		return Object.keys(parsed as Record<string, unknown>,);
	} catch {
		return [];
	}
}

function contentSourceKind(flags: Record<string, string | boolean>,): string {
	if (flags["stdin"] === true) return "stdin";
	if (typeof flags["file"] === "string") return "file";
	if (typeof flags["content"] === "string") return "content";
	return "unknown";
}

/**
 * Plan-safe view of a user create/update body: password values are replaced by
 * a fixed marker so secrets never appear in --plan output.
 */
function redactedUserPayload(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	if (payload["password"] === undefined) return payload;
	return { ...payload, password: "<omitted>", };
}

/**
 * Plan-safe view of a connection create/update body: the connection-type
 * specific `params` object may carry credentials (passwords, keys, tokens), so
 * the plan carries only the marker, never the raw params.
 */
function redactedConnectionPayload(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	if (payload["params"] === undefined) return payload;
	return { ...payload, params: "<omitted; may include credentials>", };
}

/** Required CSV flag for plans (comma-separated logins list). */
function requiredPlanCsv(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string[] {
	const raw = requiredPlanFlag(flags, name, usage,);
	return raw.split(",",).map((entry,) => entry.trim()).filter((entry,) => entry.length > 0);
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
	exact?: boolean;
	identifiers?: Record<string, unknown>;
	method?: string;
	payload?: unknown;
	localWrites?: unknown;
	reason?: string;
	wait?: unknown;
	requests?: unknown;
} {
	const projectEndpoint = (suffix: string,) => {
		if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
		return encodedProjectEndpointForPlan(projectKey, suffix,);
	};
	const id = args[0];
	const codeEnvEndpoint = (suffix = "",) =>
		`/public/api/admin/code-envs/${encodeURIComponent(codeEnvLang(args[0], entry.usage,),)}/${
			encodeURIComponent(args[1],)
		}${suffix}`;
	const statisticsWorksheetsEndpoint = (datasetName: string,) =>
		projectEndpoint(`/datasets/${encodeURIComponent(datasetName,)}/statistics/worksheets/`,);
	const statisticsWorksheetEndpoint = (datasetName: string, worksheetId: string,) =>
		`${statisticsWorksheetsEndpoint(datasetName,)}${encodeURIComponent(worksheetId,)}`;
	switch (`${resource}.${action}`) {
		case "sql.query": {
			const payload = resolveSqlQueryInvocation(args, flags, planProjectKeyFromArgs(flags,),);
			return {
				method: "POST",
				endpoint: "/public/api/sql/queries/",
				identifiers: {
					connection: payload.connection,
					dataset: payload.datasetFullName,
				},
				payload,
			};
		}
		case "ml-task.train":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/models/lab/${encodeURIComponent(args[0],)}/${encodeURIComponent(args[1],)}/train`,
				),
				identifiers: { analysisId: args[0], mlTaskId: args[1], },
				payload: {
					sessionName: flags["session-name"] as string | undefined,
					runQueue: false,
				},
				wait: flags["wait"] === true,
			};
		case "wiki.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const content = textInput(flags,);
			const create = {
				method: "POST",
				endpoint: projectEndpoint("/wiki/",),
				payload: { projectKey, name, parent: flags["parent"] as string | undefined ?? null, },
			};
			return {
				...create,
				identifiers: { name, },
				// Content is not accepted by the create endpoint; it lands in a follow-up update.
				...(content === undefined ? {} : {
					requests: [
						{ sequence: 1, ...create, },
						{ sequence: 2, method: "GET", endpoint: projectEndpoint("/wiki/{createdArticleId}",), },
						{
							sequence: 3,
							method: "PUT",
							endpoint: projectEndpoint("/wiki/{createdArticleId}",),
							payload: { payload: content, },
						},
					],
				}),
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
					`--name or dashboard settings containing a string name are required. Usage: ${
						commandUsage("dashboard", "create",)
					}`,
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
					`--data or both --name and --type are required. Usage: ${commandUsage("insight", "create",)}`,
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
				method: "DELETE",
				endpoint: `/public/api/futures/${encodeURIComponent(id,)}`,
				identifiers: { id, },
			};
		case "flow-zone.create": {
			const name = flowZoneName(flags["name"],);
			const payload = { name, color: flowZoneColor(flags["color"],) ?? "#2ab1ac", };
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
			const endpoint = projectEndpoint("/datasets/",); // throws without a project key
			return {
				method: "POST",
				endpoint,
				identifiers: { name, },
				payload: buildDatasetCreateBody({
					projectKey: projectKey!,
					datasetName: name,
					connection,
					dsType,
				},),
			};
		}
		case "dataset.clone": {
			const source = args[0];
			const target = args[1];
			return {
				exact: false,
				reason:
					"Apply GETs the source dataset and copies its current connection, format, and schema into the new dataset body.",
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
			};
		case "dataset.metadata-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/datasets/${encodeURIComponent(id,)}/metadata`,),
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "dataset.create-managed": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const connection = requiredPlanFlag(flags, "connection", entry.usage,);
			const creationSettings: Record<string, unknown> = { connectionId: connection, };
			const specificSettings: Record<string, unknown> = {};
			if (typeof flags["type-option-id"] === "string") {
				creationSettings.typeOptionId = flags["type-option-id"];
			}
			if (typeof flags["format-option-id"] === "string") {
				specificSettings.formatOptionId = flags["format-option-id"];
			}
			if (typeof flags["copy-partitioning-from"] === "string") {
				specificSettings.partitioningOptionId = `copy:${
					flags["partitioning-folder"] === true ? "folder" : "dataset"
				}:${flags["copy-partitioning-from"]}`;
			}
			if (Object.keys(specificSettings,).length > 0) {
				creationSettings.specificSettings = specificSettings;
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/datasets/managed",),
				identifiers: { name, },
				payload: { name, creationSettings, },
			};
		}
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
			requiredPlanFlag(flags, "type", entry.usage,);
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
			const endpoint = projectEndpoint("/recipes/",); // throws without a project key
			// Same flag mapping and body construction as the handler and RecipesResource.create.
			const { recipePrototype, creationSettings, } = buildRecipeCreateRequest(
				recipeCreateOptionsFromFlags(flags,),
				projectKey!,
			);
			return {
				method: "POST",
				endpoint,
				identifiers: { name: recipePrototype.name as string, },
				payload: { recipePrototype, creationSettings, },
			};
		}
		case "recipe.run":
			return {
				exact: false,
				reason:
					"The job endpoint is exact, but DSS reads the recipe and its outputs before constructing the job payload; use recipe run --dry-run for a resolved payload.",
				method: "POST",
				endpoint: projectEndpoint("/jobs/",),
				identifiers: { recipe: id, },
				wait: recipeRunShouldWait(flags,),
			};
		case "code.run": {
			const { script, source, } = resolveCodeInputWithSource(args, flags,);
			const sourceSha256 = sha256Hex(script,);
			const scenarioBase = projectEndpoint("/scenarios/{generatedScenarioId}",);
			const envName = flags["env"] as string | undefined;
			const keepScenario = flags["keep"] === true;
			const timeoutMs = parseCodeRunIntegerFlag(flags["timeout"], "--timeout",) ?? 120_000;
			const maxLogBytes = flags["full-log"] === true
				? 0
				: parseCodeRunIntegerFlag(flags["max-log-bytes"], "--max-log-bytes",) ?? 1_048_576;
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: {
					source,
					sourceSha256,
					sourceBytes: Buffer.byteLength(script,),
				},
				requests: [
					{
						method: "POST",
						endpoint: projectEndpoint("/scenarios/",),
						payload: {
							id: "{generatedScenarioId}",
							name: "dss code run ({generatedScenarioId})",
							projectKey,
							type: "custom_python",
							params: {
								envSelection: envName
									? { envMode: "EXPLICIT_ENV", envName, }
									: { envMode: "INHERIT", },
							},
						},
					},
					{
						method: "PUT",
						endpoint: `${scenarioBase}/payload`,
						payload: { extension: "py", script: "<omitted>", },
						redactedFields: ["payload.script",],
					},
					{ method: "POST", endpoint: `${scenarioBase}/run/`, payload: {}, },
					{
						method: "GET",
						endpoint:
							`${scenarioBase}/get-run-for-trigger?triggerId={triggerId}&triggerRunId={triggerRunId}`,
						repeat: "until scenarioRun.result.outcome or timeout",
					},
					{
						method: "GET",
						endpoint: `${scenarioBase}/{runId}/log`,
						boundedBytes: maxLogBytes,
					},
					...keepScenario ? [] : [{ method: "DELETE", endpoint: scenarioBase, cleanup: true, },],
				],
				wait: {
					timeoutMs,
					pollEndpoint: `${scenarioBase}/get-run-for-trigger`,
				},
			};
		}
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
		case "scenario.abort":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/abort`,),
				identifiers: { id, },
			};
		case "scenario.payload-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/payload`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "scenario.active-set": {
			const activeRaw = args[1];
			if (activeRaw !== "true" && activeRaw !== "false") {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			const lightEndpoint = projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/light`,);
			const active = activeRaw === "true";
			// DSS parses the light PUT as a full Scenario, so the command echoes the
			// current light status with `active` overridden (see ScenariosResource.setActive).
			return {
				method: "PUT",
				endpoint: lightEndpoint,
				identifiers: { id, },
				payload: { active, },
				requests: [
					{ sequence: 1, method: "GET", endpoint: lightEndpoint, },
					{
						sequence: 2,
						method: "PUT",
						endpoint: lightEndpoint,
						payload: { "...current": true, active, },
					},
					{ sequence: 3, method: "GET", endpoint: lightEndpoint, },
				],
			};
		}
		case "folder.create": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			const type = flags["type"] as string | undefined;
			const connection = flags["connection"] as string | undefined;
			const pathFlag = flags["path"] as string | undefined;
			const body = {
				name,
				projectKey,
				type: type ?? null,
				params: { connection, path: pathFlag?.trim() || "/${projectKey}/${odbId}", },
			};
			// Mirrors FoldersResource.create.
			if (connection === undefined) {
				return {
					exact: false,
					reason:
						"Without --connection, apply reads DSS admin settings to pick the managed-folder connection (falling back to filesystem_folders); params.connection is set then.",
					method: "POST",
					endpoint: projectEndpoint("/managedfolders/",),
					identifiers: { name, },
					payload: body,
				};
			}
			return {
				method: "POST",
				endpoint: projectEndpoint("/managedfolders/",),
				identifiers: { name, },
				payload: body,
			};
		}
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
		case "scenario.create": {
			const type = (flags["type"] as string | undefined) ?? "step_based";
			return {
				method: "POST",
				endpoint: projectEndpoint("/scenarios/",),
				identifiers: { id: args[0], name: args[1], },
				// Mirrors ScenariosResource.create's default params for step-based scenarios.
				payload: {
					id: args[0],
					name: args[1],
					projectKey,
					type,
					params: type === "step_based" ? { steps: [], triggers: [], reporters: [], } : {},
				},
			};
		}
		case "scenario.update":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/scenarios/${encodeURIComponent(id,)}/`,),
				identifiers: { id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
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
				exact: false,
				reason:
					"The payload lists the requested changes; apply PUTs the full {standard, local} object (merged with the current variables unless --replace).",
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
		case "llm.list": {
			const params = new URLSearchParams();
			const purpose = flags["purpose"];
			if (typeof purpose === "string" && purpose.trim() !== "") {
				params.set("purpose", purpose.trim(),);
			}
			const query = params.size > 0 ? `?${params.toString()}` : "";
			return {
				method: "GET",
				endpoint: projectEndpoint(`/llms/${query}`,),
			};
		}
		case "llm.completions":
		case "llm.embeddings": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const llmId = stringField(payload, ["llmId",],);
			if (llmId === undefined || llmId.trim() === "") {
				throw new UsageError(
					`llmId is required and must be a non-empty string (e.g. "openai:openai1:gpt-4").\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			const queries = payload["queries"];
			if (!Array.isArray(queries,) || queries.length === 0) {
				throw new UsageError(
					`queries is required and must be a non-empty array of query objects.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					action === "completions" ? "/llms/completions" : "/llms/embeddings",
				),
				identifiers: { llmId, },
				payload,
			};
		}
		case "knowledge-bank.search": {
			if (typeof id !== "string" || id.trim() === "") {
				throw new UsageError(
					`knowledgeBankId must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const query = stringField(payload, ["query",],);
			if (query === undefined || query.trim() === "") {
				throw new UsageError(
					`query is required and must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/knowledge-banks/${encodeURIComponent(id,)}/search`,
				),
				identifiers: { knowledgeBankId: id, },
				payload,
			};
		}
		case "knowledge-bank.clear":
			if (typeof id !== "string" || id.trim() === "") {
				throw new UsageError(
					`knowledgeBankId must be a non-empty string.\nUsage: ${entry.usage}`,
					"invalid_flag_value",
				);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/knowledge-banks/${encodeURIComponent(id,)}/clear`,
				),
				identifiers: { knowledgeBankId: id, },
			};
		case "data-collection.create":
			return {
				method: "POST",
				endpoint: "/public/api/data-collections/",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-collection.settings-set":
			return {
				method: "PUT",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}`,
				identifiers: { dataCollectionId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "data-collection.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}`,
				identifiers: { dataCollectionId: id, },
			};
		case "data-collection.add-object": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			const cid = id
				?? (typeof payload["dataCollectionId"] === "string"
					? payload["dataCollectionId"] as string
					: undefined);
			if (!cid) throw new UsageError(`Usage: ${entry.usage}`,);
			const reference = { ...payload, };
			delete (reference as Record<string, unknown>)["dataCollectionId"];
			return {
				method: "POST",
				endpoint: `/public/api/data-collections/${encodeURIComponent(cid,)}/objects`,
				identifiers: { dataCollectionId: cid, },
				payload: reference,
			};
		}
		case "data-collection.remove-dataset": {
			const projectKeyArg = args[1];
			const datasetName = args[2];
			if (!projectKeyArg || !datasetName) {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "DELETE",
				endpoint: `/public/api/data-collections/${encodeURIComponent(id,)}/objects/dataset/${
					encodeURIComponent(projectKeyArg,)
				}/${encodeURIComponent(datasetName,)}`,
				identifiers: { dataCollectionId: id, projectKey: projectKeyArg, datasetName, },
			};
		}
		case "project.tags-set":
			return {
				method: "PUT",
				endpoint: encodedProjectEndpointForPlan(
					projectKey ?? requiredPlanProjectKey(flags, entry.usage,),
					"/tags",
				),
				identifiers: { projectKey: projectKey ?? requiredPlanProjectKey(flags, entry.usage,), },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "project.metadata-set":
			return {
				method: "PUT",
				endpoint: encodedProjectEndpointForPlan(
					projectKey ?? requiredPlanProjectKey(flags, entry.usage,),
					"/metadata",
				),
				identifiers: { projectKey: projectKey ?? requiredPlanProjectKey(flags, entry.usage,), },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "macro.run":
		case "macro.run-and-wait": {
			// The plan payload mirrors the live POST body exactly: params and
			// adminParams when provided via --data, {} otherwise. Validation is
			// identical to the live path (JSON object shape), zero network.
			const data = optionalJsonFlag(flags, "data",);
			const runPayload: Record<string, unknown> = {};
			if (data && typeof data === "object" && !Array.isArray(data,)) {
				if (data["params"] !== undefined) runPayload["params"] = data["params"];
				if (data["adminParams"] !== undefined) runPayload["adminParams"] = data["adminParams"];
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(`/runnables/${encodeURIComponent(id,)}?wait=false`,),
				identifiers: { id, },
				payload: runPayload,
				wait: action === "macro.run-and-wait" || flags["wait"] === true,
			};
		}
		case "macro.abort": {
			const runId = args[1];
			if (!runId) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/runnables/${encodeURIComponent(id,)}/abort/${encodeURIComponent(runId,)}`,
				),
				identifiers: { id, runId, },
			};
		}
		case "recipe.metadata-set":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/recipes/${encodeURIComponent(id,)}/metadata`,),
				identifiers: { recipeName: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "user.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/users",
				payload: redactedUserPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "group.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/groups",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "saved-model.create-external": {
			const config = optionalJsonFlag(flags, "configuration",) ?? optionalJsonFlag(flags, "data",);
			return {
				method: "POST",
				endpoint: projectEndpoint("/savedmodels/",),
				identifiers: { name: id, },
				payload: {
					savedModelType: requiredPlanFlag(flags, "type", entry.usage,),
					name: id,
					...(typeof flags["prediction-type"] === "string"
						? { predictionType: flags["prediction-type"], }
						: {}),
					...(config ? { proxyModelConfiguration: config, } : {}),
				},
			};
		}
		case "saved-model.update-settings":
			return {
				method: "PUT",
				endpoint: projectEndpoint(`/savedmodels/${encodeURIComponent(id,)}`,),
				identifiers: { savedModelId: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "saved-model.delete-versions": {
			const versionsRaw = args[1];
			if (typeof versionsRaw !== "string" || versionsRaw.trim() === "") {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(`/savedmodels/${encodeURIComponent(id,)}/actions/delete-versions`,),
				identifiers: { savedModelId: id, },
				payload: {
					versions: versionsRaw.split(",",).map((v,) => v.trim()).filter((v,) => v.length > 0),
					removeIntermediate: parseBooleanOption(
						flags["remove-intermediate"],
						"--remove-intermediate",
					) ?? true,
				},
			};
		}
		case "saved-model.import-mlflow-version":
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${encodeURIComponent(args[1] ?? "",)}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: {
					source: { kind: "local-archive", archive: requiredPlanFlag(flags, "archive", entry.usage,), },
					...(typeof flags["code-env"] === "string"
						? { codeEnvName: flags["code-env"], }
						: {}),
					// Execute always sends the query parameter with the
					// external-caller default NONE; the plan mirrors it.
					containerExecConfigName: typeof flags["container-exec-config"] === "string"
						? flags["container-exec-config"]
						: "NONE",
					setActive: parseBooleanOption(flags["set-active"], "--set-active",) ?? true,
					...(flags["binary-classification-threshold"] !== undefined
						? {
							binaryClassificationThreshold: num(
								flags["binary-classification-threshold"],
								"--binary-classification-threshold",
							),
						}
						: {}),
				},
			};
		case "saved-model.import-mlflow-version-from-folder":
			return {
				exact: false,
				reason:
					"Apply sends these values as query parameters (with codeEnvName and binaryClassificationThreshold defaults) on an empty multipart body; the payload shows them structured.",
				method: "POST",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${encodeURIComponent(args[1] ?? "",)}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: {
					source: {
						kind: "managed-folder",
						folderRef: requiredPlanFlag(flags, "folder", entry.usage,),
						path: flags["path"] as string | undefined,
					},
					...(typeof flags["code-env"] === "string" ? { codeEnvName: flags["code-env"], } : {}),
					// Execute always sends the query parameter with the
					// external-caller default NONE; the plan mirrors it.
					containerExecConfigName: typeof flags["container-exec-config"] === "string"
						? flags["container-exec-config"]
						: "NONE",
					setActive: parseBooleanOption(flags["set-active"], "--set-active",) ?? true,
				},
			};
		case "saved-model.external-metadata-put": {
			// DSS requires containerExecConfigName on this endpoint; for an
			// external API caller it resolves as LOCAL-CONFIG -> NONE, so the
			// plan mirrors the NONE default unless --container-exec-config
			// overrides it. Same contract as execute.
			const containerExecConfigName = typeof flags["container-exec-config"] === "string"
				? flags["container-exec-config"]
				: "NONE";
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${
						encodeURIComponent(args[1] ?? "",)
					}/external-ml/metadata?containerExecConfigName=${
						encodeURIComponent(containerExecConfigName,)
					}`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		}
		case "saved-model.evaluate-version": {
			const payload: Record<string, unknown> = {
				datasetRef: requiredPlanFlag(flags, "dataset", entry.usage,),
				// Execute always sends an explicit containerExecConfigName
				// (official external-caller semantics: LOCAL-CONFIG -> NONE);
				// the plan mirrors that default and any explicit override.
				containerExecConfigName: typeof flags["container-exec-config"] === "string"
					? flags["container-exec-config"]
					: "NONE",
			};
			// --sampling is a JSON flag value; the plan carries the parsed
			// object under the same key as the execute body (samplingParam).
			if (flags["sampling"] !== undefined) {
				payload.samplingParam = json(flags["sampling"], "--sampling",);
			}
			return {
				method: "POST",
				endpoint: `${
					projectEndpoint(
						`/savedmodels/${encodeURIComponent(id,)}/versions/${
							encodeURIComponent(args[1] ?? "",)
						}/external-ml/actions/evaluate`,
					)
				}?useOptimalThreshold=${
					parseBooleanOption(flags["use-optimal-threshold"], "--use-optimal-threshold",) ?? true
				}&skipExpensiveReports=${
					parseBooleanOption(flags["skip-expensive-reports"], "--skip-expensive-reports",) ?? true
				}`,
				identifiers: { savedModelId: id, versionId: args[1], },
				payload,
			};
		}
		case "saved-model.set-user-meta":
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/savedmodels/${encodeURIComponent(id,)}/versions/${
						encodeURIComponent(args[1] ?? "",)
					}/user-meta`,
				),
				identifiers: { savedModelId: id, versionId: args[1], },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "user.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/users/${encodeURIComponent(id,)}`,
				identifiers: { login: id, },
				payload: redactedUserPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "user.delete":
		case "group.delete":
			return {
				method: "DELETE",
				endpoint: resource === "user"
					? `/public/api/admin/users/${encodeURIComponent(id,)}`
					: `/public/api/admin/groups/${encodeURIComponent(id,)}`,
				identifiers: resource === "user" ? { login: id, } : { name: id, },
			};
		case "user.resync":
			return {
				method: "POST",
				endpoint: `/public/api/admin/users/${encodeURIComponent(id,)}/actions/resync`,
				identifiers: { login: id, },
			};
		case "user.resync-multi": {
			const logins = requiredPlanCsv(flags, "logins", entry.usage,);
			return {
				method: "POST",
				endpoint: "/public/api/admin/users/actions/resync-multi",
				identifiers: { count: logins.length, },
				payload: logins,
			};
		}
		case "user.external-users":
			return {
				method: "GET",
				endpoint: "/public/api/admin/users/actions/external-users",
			};
		case "user.external-groups":
			return {
				method: "GET",
				endpoint: "/public/api/admin/users/actions/external-groups",
			};
		case "connection.test":
			return {
				method: "GET",
				endpoint: `/public/api/connections/${encodeURIComponent(id,)}/test`,
				identifiers: { connectionName: id, },
			};
		case "user.provision":
			return {
				method: "POST",
				endpoint: "/public/api/admin/users/actions/provision",
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "group.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/groups/${encodeURIComponent(id,)}`,
				identifiers: { name: id, },
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "connection.create":
			return {
				method: "POST",
				endpoint: "/public/api/admin/connections",
				payload: redactedConnectionPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "connection.update":
			return {
				method: "PUT",
				endpoint: `/public/api/admin/connections/${encodeURIComponent(id,)}`,
				identifiers: { connectionName: id, },
				payload: redactedConnectionPayload(requiredPlanJsonInput(flags, entry.usage,),),
			};
		case "connection.delete":
			return {
				method: "DELETE",
				endpoint: `/public/api/admin/connections/${encodeURIComponent(id,)}`,
				identifiers: { connectionName: id, },
			};
		case "connection.prepare-import": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
			return {
				method: "POST",
				endpoint: encodedProjectEndpointForPlan(
					projectKey,
					"/datasets/tables-import/actions/prepare-from-keys",
				),
				identifiers: { projectKey, },
				payload,
			};
		}
		case "connection.execute-import": {
			const payload = requiredPlanJsonInput(flags, entry.usage,);
			if (!projectKey) throw new UsageError(`Missing project key for ${resource} ${action}.`,);
			return {
				method: "POST",
				endpoint: encodedProjectEndpointForPlan(
					projectKey,
					"/datasets/tables-import/actions/execute-from-candidates",
				),
				identifiers: { projectKey, },
				payload,
			};
		}
		case "plugin.install-from-zip":
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromZip",
				payload: { file: requiredPlanFlag(flags, "file", entry.usage,), upload: "multipart", },
			};
		case "plugin.install-from-store":
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromStore",
				identifiers: { pluginId: id, },
				payload: { pluginId: id, },
			};
		case "plugin.install-from-git": {
			const payload = pluginGitPlanPayload(flags,);
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/installFromGit",
				payload,
			};
		}
		case "plugin.update-from-zip":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromZip",),
				identifiers: { pluginId: id, },
				payload: { file: requiredPlanFlag(flags, "file", entry.usage,), upload: "multipart", },
			};
		case "plugin.update-from-store":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromStore",),
				identifiers: { pluginId: id, },
			};
		case "plugin.update-from-git":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "updateFromGit",),
				identifiers: { pluginId: id, },
				payload: pluginGitPlanPayload(flags,),
			};
		case "plugin.settings-set": {
			const scope = optionalPlanProjectScope(flags,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/settings",)}${scope}`,
				identifiers: { pluginId: id, },
				payload: { configKeys: settingsConfigKeys(flags,), },
			};
		}
		case "plugin.code-env-create": {
			const conda = parseBooleanOption(flags["conda"], "--conda",) ?? false;
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/code-env/actions/create",)}`,
				identifiers: { pluginId: id, },
				payload: {
					deploymentMode: "PLUGIN_MANAGED",
					conda,
					pythonInterpreter: flags["python-interpreter"] ?? null,
				},
				wait: flags["wait"] === true,
			};
		}
		case "plugin.code-env-update":
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/code-env/actions/update",)}`,
				identifiers: { pluginId: id, },
				wait: flags["wait"] === true,
			};
		case "plugin.move-to-dev":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "moveToDev",),
				identifiers: { pluginId: id, },
			};
		case "plugin.delete":
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, "delete",),
				identifiers: { pluginId: id, },
				payload: { force: parseBooleanOption(flags["force"], "--force",) ?? false, },
			};
		case "plugin.create-dev": {
			const creationMode = requiredPlanFlag(flags, "creation-mode", entry.usage,);
			const needsGit = creationMode !== "EMPTY";
			// Mirrors PluginsResource.createDev: git fields are always present, null when unused.
			const payload: Record<string, unknown> = {
				pluginId: id,
				creationMode,
				gitRepository: needsGit && typeof flags["repository"] === "string"
					? validatedPlanRepositoryUrl(flags["repository"], "repository", "--repository URL",)
					: null,
				gitCheckout: needsGit && typeof flags["checkout"] === "string" ? flags["checkout"] : null,
				gitSubpath: creationMode === "GIT_EXPORT" && typeof flags["path-in-repository"] === "string"
					? flags["path-in-repository"]
					: null,
			};
			return {
				method: "POST",
				endpoint: "/public/api/plugins/actions/createDev",
				identifiers: { pluginId: id, creationMode, },
				payload,
			};
		}
		case "plugin.set-git-remote": {
			const repository = requiredPlanFlag(flags, "repository", entry.usage,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/gitRemote",)}`,
				identifiers: { pluginId: id, },
				payload: { repositoryUrl: validatedPlanRepositoryUrl(repository, "repository", entry.usage,), },
			};
		}
		case "plugin.delete-git-remote":
			return {
				method: "DELETE",
				endpoint: pluginRootEndpoint(id, "/gitRemote",),
				identifiers: { pluginId: id, },
			};
		case "plugin.push":
		case "plugin.pull":
		case "plugin.fetch":
		case "plugin.reset-local":
		case "plugin.reset-remote": {
			const actionMap: Record<string, string> = {
				"plugin.push": "push",
				"plugin.pull": "pullRebase",
				"plugin.fetch": "fetch",
				"plugin.reset-local": "resetToLocalHeadState",
				"plugin.reset-remote": "resetToRemoteHeadState",
			};
			return {
				method: "POST",
				endpoint: pluginActionEndpoint(id, actionMap[`${resource}.${action}`]!,),
				identifiers: { pluginId: id, },
			};
		}
		case "plugin.contents-put":
			return {
				method: "POST",
				endpoint: pluginContentsEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
				payload: { contentSource: contentSourceKind(flags,), },
			};
		case "plugin.contents-delete":
			return {
				method: "DELETE",
				endpoint: pluginContentsEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
			};
		case "plugin.folder-add":
			return {
				method: "POST",
				endpoint: pluginFoldersEndpoint(id, args[1] ?? "",),
				identifiers: { pluginId: id, path: args[1], },
			};
		case "plugin.rename": {
			if (!args[2]) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/contents-actions/rename",)}`,
				identifiers: { pluginId: id, path: args[1], },
				payload: { oldPath: `/${validatePluginPath(args[1] ?? "",)}`, newName: args[2], },
			};
		}
		case "plugin.move": {
			const destination = args[2];
			if (!destination) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${pluginRootEndpoint(id, "/contents-actions/move",)}`,
				identifiers: { pluginId: id, path: args[1], },
				payload: {
					oldPath: `/${validatePluginPath(args[1] ?? "",)}`,
					newPath: validatePluginDestinationPath(destination,),
				},
			};
		}
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
			if (payload["targetProjectKey"] === undefined) {
				// Generated-at-apply mode: --plan never allocates or reserves a
				// random key. The payload stays exactly as supplied, the runtime
				// generates the key (and defaults the display name to it) during
				// apply, and no absence probe runs for a generated key, so the
				// plan carries no preflight requests and no fake concrete GET
				// path: `{targetProjectKey}` resolves at apply time.
				return {
					exact: false,
					reason:
						"targetProjectKey (and targetProjectName when omitted) are generated during apply; the sent body adds them.",
					method: "POST",
					endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
					identifiers: {
						appId: id,
						targetProjectKeyGeneratedDuringApply: true,
						...(payload["targetProjectName"] === undefined
							? { targetProjectNameGeneratedDuringApply: true, }
							: {}),
						preflightExecuted: false,
						preflightWillRunDuringApply: false,
						preflightRequests: [],
						incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
						incarnationObservationRequests: [
							{
								method: "GET",
								endpointTemplate: "/public/api/projects/{targetProjectKey}/",
								when: flags["wait"] === true
									? "after-terminal-future-target"
									: "conditional-inline-hasResult-target",
								intent:
									"After DSS reports inline or terminal creation success for the generated key, observe creationTag for later cleanup binding.",
							},
						],
						note:
							"Generated-at-apply: a random APP_ targetProjectKey is generated client-side during apply (never at plan time); targetProjectName defaults to it when omitted. No absent-project preflight runs for a generated key.",
					},
					payload,
					wait: flags["wait"] === true,
				};
			}
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
			const targetProjectKey = optionalPlanFlag(flags, "to", entry.usage,);
			if (targetProjectKey !== undefined && sourceProjectKey === targetProjectKey) {
				throw new UsageError(
					"--from and --to must be different project keys.",
					"validation_failed",
					`Usage: ${entry.usage}`,
				);
			}
			const targetProjectName = flags["name"] as string | undefined;
			const copyPermissions = parseBooleanOption(flags["copy-permissions"], "--copy-permissions",)
				?? false;
			if (targetProjectKey === undefined) {
				// Generated-at-apply mode: --plan never allocates a key and no
				// target exists yet, so the plan makes no target-absence claim
				// and advertises no target probe. The key is generated once
				// during apply, immediately before the single instance POST;
				// `{targetProjectKey}` resolves only after the terminal future
				// names it. Source and template gates still run before the POST.
				return {
					method: "POST",
					endpoint: `/public/api/apps/${encodeURIComponent(id,)}/instances`,
					identifiers: {
						appId: id,
						sourceProjectKey,
						targetProjectKeyGeneratedDuringApply: true,
						targetPreflight: "not-applicable-generated-key",
						preflightExecuted: false,
						preflightWillRunDuringApply: true,
						...(targetProjectName !== undefined
							? { targetProjectName, }
							: { targetProjectNameGeneratedDuringApply: true, }),
						copyPermissions,
						incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
						incarnationObservationRequests: [
							{
								method: "GET",
								endpointTemplate: "/public/api/projects/{targetProjectKey}/",
								when: "after-terminal-future-target",
								intent:
									"After the terminal future names the generated successor key, observe creationTag and bind later target checks and cleanup to that hash.",
							},
						],
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
						],
						...(copyPermissions
							? {
								permissionConcurrencyControl: "client-side-non-atomic-stale-identity-and-hash-checks",
								permissionRequests: [
									{
										method: "GET",
										endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
										intent: "Snapshot the predecessor instance ACL before creation.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Read the generated successor ACL to decide whether the copy is a no-op.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Recheck generated successor creationTag after reading its ACL; stop if the project key was reused.",
									},
									{
										method: "GET",
										endpoint: `/public/api/projects/${encodeURIComponent(sourceProjectKey,)}/permissions`,
										intent:
											"Recheck the predecessor ACL immediately before the write and stop if its hash drifted.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Recheck generated successor creationTag immediately before the unconditional permission PUT.",
									},
									{
										method: "PUT",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Apply the predecessor ACL snapshot to the generated successor instance.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/permissions",
										intent: "Verify the generated successor ACL hash equals the predecessor snapshot hash.",
									},
									{
										method: "GET",
										endpointTemplate: "/public/api/projects/{targetProjectKey}/",
										intent:
											"Detect generated successor project-key reuse across the permission write and verification read.",
									},
								],
							}
							: {}),
						note:
							"Generated-at-apply: the successor key is generated once during apply (never at plan time) and no absent-project preflight runs for a generated key, so the plan claims no target absence. Source and template gates run before the single instance POST, then the terminal future's own result names the successor key. The predecessor is never modified or deleted; cleanup targets the generated successor key only.",
					},
					payload: targetProjectName !== undefined ? { targetProjectName, } : {},
					wait: true,
				};
			}
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
		case "webapp.create":
			return {
				method: "POST",
				endpoint: projectEndpoint("/webapps/",),
				payload: requiredPlanJsonInput(flags, entry.usage,),
			};
		case "webapp.update-settings": {
			const patch = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected webapp hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/webapps/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason: "Apply GETs the current settings, deep-merges the patch, then PUTs the full object.",
				method: "PUT",
				endpoint,
				identifiers: {
					webappId: id,
					patchHash: stableHash(patch,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				payload: { patch: "<omitted>", patchHash: stableHash(patch,), },
				requests: [
					{ method: "GET", endpoint, purpose: "read full settings and verify expectHash", },
					{
						method: "PUT",
						endpoint,
						condition: "settings read and expected hash matched",
						payload: "<merged full settings>",
					},
				],
			};
		}
		case "webapp.stop-backend":
			return {
				method: "PUT",
				endpoint: projectEndpoint(
					`/webapps/${encodeURIComponent(id,)}/backend/actions/stop`,
				),
				identifiers: { webappId: id, },
				payload: {},
			};
		case "webapp.restart-backend": {
			const endpoint = projectEndpoint(
				`/webapps/${encodeURIComponent(id,)}/backend/actions/restart`,
			);
			return {
				method: "PUT",
				endpoint,
				identifiers: { webappId: id, },
				payload: {},
				wait: {
					requested: flags["wait"] === true,
					timeoutMs: num(flags["timeout"], "--timeout",),
					pollIntervalMs: num(flags["poll-interval"], "--poll-interval",),
				},
			};
		}
		case "api-service.create":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/apiservices/${encodeURIComponent(id,)}`,),
				identifiers: { serviceId: id, },
				payload: {},
			};
		case "api-service.save-settings": {
			const settings = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError(
					"Expected API service settings hash must be a 64-character SHA-256 hex digest.",
				);
			}
			const endpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(id,)}/settings`,
			);
			const payload = {
				settings: "<omitted>",
				settingsHash: stableHash(settings,),
				...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
			};
			return {
				method: "PUT",
				endpoint,
				identifiers: { serviceId: id, },
				payload,
				requests: [
					...(expectHash
						? [{ method: "GET", endpoint, purpose: "verify expectHash", },]
						: []),
					{
						method: "PUT",
						endpoint,
						condition: expectHash ? "hash matched" : "unconditional",
						payload,
					},
				],
			};
		}
		case "api-service.add-prediction-endpoint": {
			const endpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(id,)}/settings`,
			);
			return {
				exact: false,
				reason: "Apply GETs the full settings, appends one endpoint, then PUTs the full object.",
				method: "PUT",
				endpoint,
				identifiers: { serviceId: id, endpointId: args[1], savedModelId: args[2], },
				payload: { id: args[1], type: "STD_PREDICTION", modelRef: args[2], },
				requests: [
					{ method: "GET", endpoint, purpose: "read full settings", },
					{ method: "PUT", endpoint, payload: "<merged full settings>", },
				],
			};
		}
		case "api-service.create-package":
		case "api-service.delete-package":
		case "api-service.publish-package": {
			const serviceId = args[0];
			const packageId = args[1];
			const packageEndpoint = projectEndpoint(
				`/apiservices/${encodeURIComponent(serviceId,)}/packages/${encodeURIComponent(packageId,)}`,
			);
			if (action === "delete-package") {
				return {
					method: "DELETE",
					endpoint: packageEndpoint,
					identifiers: { serviceId, packageId, },
				};
			}
			if (action === "publish-package") {
				return {
					method: "POST",
					endpoint: `${packageEndpoint}/publish${
						querySuffix({
							publishedServiceId: flags["published-service-id"] as string | undefined,
						},)
					}`,
					identifiers: { serviceId, packageId, },
				};
			}
			return {
				method: "POST",
				endpoint: packageEndpoint + querySuffix({
					releaseNotes: flags["release-notes"] as string | undefined,
				},),
				identifiers: { serviceId, packageId, },
			};
		}
		case "bundle.export": {
			const evaluateProjectStandardsChecks = parseBooleanOption(
				flags["evaluate-standards-checks"],
				"--evaluate-standards-checks",
			) ?? true;
			return {
				method: "PUT",
				endpoint: projectEndpoint("/bundles/exported/" + encodeURIComponent(id,),) + querySuffix({
					releaseNotes: flags["release-notes"] as string | undefined,
					evaluateProjectStandardsChecks,
				},),
				identifiers: { bundleId: id, },
			};
		}
		case "bundle.publish":
			return {
				method: "POST",
				endpoint: projectEndpoint("/bundles/" + encodeURIComponent(id,) + "/publish",) + querySuffix({
					publishedProjectKey: flags["published-project-key"] as string | undefined,
				},),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "bundle.activate": {
			const rawScenarios = flags["scenarios"];
			let scenariosToEnable: Record<string, boolean> | undefined;
			if (rawScenarios !== undefined && rawScenarios !== false) {
				const parsed = json(rawScenarios,);
				if (
					typeof parsed !== "object"
					|| parsed === null
					|| Array.isArray(parsed,)
					|| !Object.values(parsed,).every((value,) => typeof value === "boolean")
				) {
					throw new UsageError("--scenarios must be a JSON object mapping scenario IDs to true|false.",);
				}
				scenariosToEnable = parsed as Record<string, boolean>;
			}
			return {
				method: "POST",
				endpoint: projectEndpoint(
					`/bundles/imported/${encodeURIComponent(id,)}/actions/activate`,
				),
				identifiers: { bundleId: id, },
				payload: scenariosToEnable === undefined
					? {}
					: { scenariosActiveOnActivation: scenariosToEnable, },
			};
		}
		case "bundle.preload":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/bundles/imported/${encodeURIComponent(id,)}/actions/preload`,),
				identifiers: { bundleId: id, },
				payload: {},
			};
		case "project-library.create-file":
		case "project-library.create-folder": {
			const libraryPath = validateLibraryPath(id,);
			const kind = action === "create-file" ? "contents" : "folders";
			return {
				exact: false,
				reason:
					"Apply performs a live absence check before POST; --if-not-exists may stop after that read, and create never overwrites an existing item.",
				method: "POST",
				endpoint: projectEndpoint(`/libraries/${kind}/${encodeLibraryPath(libraryPath,)}`,),
				identifiers: { path: libraryPath, },
			};
		}
		case "project-library.put": {
			const libraryPath = validateLibraryPath(id,);
			const endpoint = projectEndpoint(`/libraries/contents/${encodeLibraryPath(libraryPath,)}`,);
			const payload = projectLibraryPutPayload(flags,);
			return {
				method: "POST",
				endpoint,
				identifiers: {
					path: libraryPath,
					concurrencyControl: payload.expectSha256
						? PROJECT_LIBRARY_CONCURRENCY_CONTROL
						: "none",
				},
				payload,
				...(payload.expectSha256
					? {
						requests: [
							{
								method: "GET",
								endpoint: `${endpoint}?dataEncoding=base64`,
								purpose: "verify expectSha256",
							},
							{ method: "POST", endpoint, condition: "hash matched", payload, },
						],
					}
					: {}),
			};
		}
		case "project-library.delete": {
			const libraryPath = validateLibraryPath(id,);
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/libraries/contents/${encodeLibraryPath(libraryPath,)}`,),
				identifiers: { path: libraryPath, },
			};
		}
		case "project-library.rename": {
			const libraryPath = validateLibraryPath(id,);
			const newName = validateLibraryName(args[1],);
			return {
				method: "POST",
				endpoint: projectEndpoint("/libraries/contents-actions/rename/",),
				identifiers: { path: libraryPath, newName, },
				payload: { oldPath: `/${libraryPath}`, newName, },
			};
		}
		case "project-library.move": {
			const libraryPath = validateLibraryPath(id,);
			const destinationFolder = validateLibraryDestinationPath(args[1],);
			return {
				method: "POST",
				endpoint: projectEndpoint("/libraries/contents-actions/move",),
				identifiers: { path: libraryPath, destinationFolder, },
				payload: { oldPath: `/${libraryPath}`, newPath: destinationFolder, },
			};
		}
		case "code-env.create": {
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint()}?wait=${wait}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				payload: {
					...codeEnvParams(flags,),
					deploymentMode: requiredPlanFlag(flags, "deployment-mode", entry.usage,),
				},
				wait,
			};
		}
		case "code-env.set-definition": {
			const definition = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("--expect-hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = codeEnvEndpoint();
			const definitionHash = stableHash(definition,);
			const normalizedExpectHash = expectHash?.toLowerCase();
			return {
				exact: false,
				reason:
					"Apply PUTs the supplied definition object unchanged; this plan omits that body and reports only its hash.",
				method: "PUT",
				endpoint,
				identifiers: {
					lang: codeEnvLang(args[0], entry.usage,),
					name: args[1],
					definitionHash,
					...(normalizedExpectHash ? { expectHash: normalizedExpectHash, } : {}),
				},
				payload: "<omitted>",
				...(normalizedExpectHash
					? {
						requests: [
							{ method: "GET", endpoint, purpose: "verify expectHash", },
							{
								method: "PUT",
								endpoint,
								condition: "hash matched",
								payload: "<omitted>",
								redactedFields: ["payload",],
							},
						],
					}
					: {}),
			};
		}
		case "code-env.set-packages": {
			const installCorePackages = parseBooleanOption(
				flags["install-core-packages"],
				"--install-core-packages",
			);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("--expect-hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = codeEnvEndpoint();
			return {
				exact: false,
				reason:
					"Apply GETs the full definition, merges only package fields, then PUTs the full result.",
				method: "PUT",
				endpoint,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				payload: {
					packages: codeEnvPackageList(flags,),
					...(installCorePackages !== undefined ? { installCorePackages, } : {}),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "read full definition and verify expectHash", },
					{
						method: "PUT",
						endpoint,
						condition: "definition read and hash matched",
						payload: "<merged full definition>",
					},
				],
			};
		}
		case "code-env.update-packages": {
			const versionToUpdate = typeof flags["env-version"] === "string"
				? flags["env-version"]
				: typeof flags["version"] === "string"
				? flags["version"]
				: undefined;
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/packages",)}${
					querySuffix({
						forceRebuildEnv: flags["force-rebuild"] === true,
						versionToUpdate,
						wait,
					},)
				}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.update-images": {
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/images",)}${
					querySuffix({ envVersion: flags["env-version"] as string | undefined, wait, },)
				}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.set-jupyter": {
			const active = parseBooleanOption(flags["active"], "--active",);
			if (active === undefined) {
				throw new UsageError(
					`--active is required. Usage: ${commandUsage("code-env", "set-jupyter",)}`,
				);
			}
			const wait = codeEnvWait(flags,);
			return {
				method: "POST",
				endpoint: `${codeEnvEndpoint("/jupyter",)}${querySuffix({ active, wait, },)}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "code-env.delete": {
			const wait = codeEnvWait(flags,);
			return {
				method: "DELETE",
				endpoint: `${codeEnvEndpoint()}?wait=${wait}`,
				identifiers: { lang: codeEnvLang(args[0], entry.usage,), name: args[1], },
				wait,
			};
		}
		case "notebook.save-jupyter": {
			const content = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected notebook hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason:
					"A fresh GET selects POST when absent or PUT when present; a confirming GET supplies the persisted hash.",
				endpoint,
				identifiers: {
					name: id,
					contentHash: stableHash(content,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "resolve create/update and check --expect-hash", },
					{
						method: "POST|PUT",
						endpoint,
						condition: "POST after not_found; otherwise PUT",
						payload: "<omitted>",
						redactedFields: ["payload",],
					},
					{ method: "GET", endpoint, purpose: "confirm persisted hash", },
				],
			};
		}
		case "notebook.delete-jupyter":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { name: id, },
			};
		case "notebook.clear-jupyter-outputs":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/jupyter-notebooks/${encodeURIComponent(id,)}/outputs`,),
				identifiers: { name: id, },
			};
		case "notebook.unload-jupyter":
			if (flags["all"] === true) {
				if (args.length > 0) throw new UsageError(entry.usage,);
				return {
					exact: false,
					reason:
						"DSS has no unload-all endpoint; apply lists active notebooks and sessions, then issues one verified session DELETE per result.",
					identifiers: { all: true, },
					requests: [
						{ method: "GET", endpoint: projectEndpoint("/jupyter-notebooks/?active=true",), },
						{
							method: "GET",
							endpoint: projectEndpoint("/jupyter-notebooks/{name}/sessions",),
							forEach: "active notebook",
						},
						{
							method: "DELETE",
							endpoint: projectEndpoint("/jupyter-notebooks/{name}/sessions/{sessionId}",),
							forEach: "listed session",
						},
					],
				};
			}
			requireArgs(args, 2, entry.usage,);
			return {
				method: "DELETE",
				endpoint: projectEndpoint(
					`/jupyter-notebooks/${encodeURIComponent(args[0],)}/sessions/${encodeURIComponent(args[1],)}`,
				),
				identifiers: { name: args[0], sessionId: args[1], },
			};
		case "notebook.save-sql": {
			const content = requiredPlanJsonInput(flags, entry.usage,);
			const expectHash = flags["expect-hash"] as string | undefined;
			if (expectHash !== undefined && !/^[a-fA-F0-9]{64}$/.test(expectHash,)) {
				throw new UsageError("Expected notebook hash must be a 64-character SHA-256 hex digest.",);
			}
			const endpoint = projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,);
			return {
				exact: false,
				reason:
					"A fresh GET selects collection POST when absent or entity PUT when present; a confirming GET supplies the persisted hash.",
				endpoint,
				identifiers: {
					id,
					contentHash: stableHash(content,),
					...(expectHash ? { expectHash: expectHash.toLowerCase(), } : {}),
				},
				requests: [
					{ method: "GET", endpoint, purpose: "resolve create/update and check --expect-hash", },
					{
						method: "POST",
						endpoint: projectEndpoint("/sql-notebooks/",),
						condition: "after not_found",
						payload: "<omitted; includes id and projectKey>",
						redactedFields: ["payload",],
					},
					{
						method: "PUT",
						endpoint,
						condition: "when present",
						payload: "<omitted>",
						redactedFields: ["payload",],
					},
					{ method: "GET", endpoint, purpose: "confirm persisted hash", },
				],
			};
		}
		case "notebook.delete-sql":
			return {
				method: "DELETE",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}`,),
				identifiers: { id, },
			};
		case "notebook.clear-sql-history":
			return {
				method: "POST",
				endpoint: projectEndpoint(`/sql-notebooks/${encodeURIComponent(id,)}/history/clear`,),
				identifiers: { id, },
				payload: {
					cellId: flags["cell-id"] as string | undefined,
					numRunsToRetain: num(flags["retain"], "--retain",),
				},
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
				|| executionMode(flags,).dryRun
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
			if (executionMode(flags,).dryRun) {
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
			const targetPath = validateGitReferencePath(id,);
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
			const targetPath = validateGitReferencePath(id,);
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
			const targetPath = validateGitReferencePath(id,);
			return {
				method: "POST",
				endpoint: projectGitEndpoint(projectKey, "/lib-git-refs/action/reset",),
				identifiers: { library: targetPath, },
				payload: { gitRef: targetPath, },
				wait: projectGitFutureWait(),
			};
		}
		case "project-git.push-library": {
			const targetPath = validateGitReferencePath(id,);
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
		case "project-folder.settings-set": {
			const payload = requiredPlanJsonInput(
				flags,
				`--data, --data-file, or --stdin is required. Usage: ${entry.usage}`,
			);
			return {
				method: "PUT",
				endpoint: `${projectFolderEndpoint(id,)}/settings`,
				identifiers: { folderId: id, },
				payload,
			};
		}
		case "project-folder.move": {
			const destination = args[1];
			if (!destination) throw new UsageError(`Usage: ${entry.usage}`,);
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/move${querySuffix({ destination, },)}`,
				identifiers: { folderId: id, destination, },
			};
		}
		case "project-folder.delete":
			return {
				method: "DELETE",
				endpoint: projectFolderEndpoint(id,),
				identifiers: { folderId: id, },
			};
		case "project-folder.create-child": {
			const name = requiredPlanFlag(flags, "name", entry.usage,);
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/children${querySuffix({ name, },)}`,
				identifiers: { parentFolderId: id, name, },
			};
		}
		case "project-folder.move-project": {
			const projectKeyArg = args[1];
			const destination = args[2];
			if (!projectKeyArg || !destination) {
				throw new UsageError(`Usage: ${entry.usage}`,);
			}
			return {
				method: "POST",
				endpoint: `${projectFolderEndpoint(id,)}/projects/${encodeURIComponent(projectKeyArg,)}/move${
					querySuffix({ destination, },)
				}`,
				identifiers: { folderId: id, projectKey: projectKeyArg, destination, },
			};
		}
		default:
			return {
				exact: false,
				reason:
					`No exact offline request shape is defined for ${resource}.${action}; no endpoint was guessed.`,
				identifiers: id ? { id, } : undefined,
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
	const requiredPositionals = entry.positionalArguments.filter((positional,) => positional.required);
	requireArgs(args, requiredPositionals.length, meta.usage,);
	const projectKey = projectKeyForPlan(entry, flags,);
	const shape = commandPlanShape(resource, action, args, flags, entry, projectKey,);
	return planResult(resource, action, {
		...shape,
		asyncKind: entry.async,
		exitCodesOnFailure: exitCodesOnFailure(entry,),
		idempotency: entry.idempotency,
		plannedAndDryRun: executionMode(flags,).dryRun,
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
