#!/usr/bin/env node

import * as fs from "node:fs/promises";
import { num, unknownJsonInput, } from "./cli/coerce.js";
import { commands, } from "./cli/commands/index.js";
import {
	AGENT_CONTRACT_USAGE,
	AUTH_ACTIONS,
	BATCH_EXAMPLE_PAYLOAD,
	BATCH_HINT,
	BATCH_PLAN_EXIT_CODES,
	BATCH_USAGE,
	buildAgentContract,
	buildCommandRegistry,
	buildMutationPlan,
	CLEANUP_USAGE,
	commandActionSummary,
	type CommandRegistryEntry,
	COMMANDS_USAGE,
	inferRequiresProject,
	isAllowedCleanupAction,
	supportsCleanupLedger,
} from "./cli/contract.js";
import { runDoctor, runFixtures, } from "./cli/doctor.js";
import { dataikuEnvironmentEnabled, loadEnvFile, } from "./cli/env.js";
import {
	FLAG_ALIASES,
	isNegativeNumberToken,
	parseArgs,
	SHORT_FLAGS,
	VALUE_FLAGS,
} from "./cli/flags.js";
import { cleanupLedgerEntry, } from "./cli/helpers/cleanup.js";
import {
	commandFailureExitCode,
	commandFailureMessage,
	CommandResultFailure,
	flushCliWarnings,
	isFailedWaitResult,
	planResult,
	projectResultFields,
	setOutputFieldProjection,
	writeCommandResult,
} from "./cli/output.js";
import { currentCommandContext, resolveCredentials, resolveTlsSettings, } from "./cli/runtime.js";
import {
	COMMANDS_RUN_HINT,
	missingActionError,
	noCommandError,
	requireArgs,
	unknownActionError,
	unknownResourceError,
	UsageError,
} from "./cli/usage.js";
import { cliVersionResult, } from "./cli/version.js";
import { DataikuClient, } from "./client.js";
import { getCredentialsPath, } from "./config.js";
import {
	canonicalStatusText,
	ClientValidationError,
	DataikuError,
	dataikuErrorCode,
	type StableErrorCode,
} from "./errors.js";
import {
	AGENTS,
	detectAgents,
	findWorkspaceRoot,
	installSkill,
	planSkillInstalls,
} from "./skill.js";
import {
	appendCleanupLedgerEntry,
	type CleanupLedgerEntry,
	findCleanupLedgerBindingViolation,
	preflightCleanupLedgerPath,
	readCleanupLedger,
	reserveCleanupLedgerDssUrl,
} from "./utils/cleanup-ledger.js";
import { canonicalDssUrl, } from "./utils/dss-url.js";

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Command registry
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Agent-facing command inventory
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// .env auto-loading
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Auth commands (run before client creation)
// ---------------------------------------------------------------------------

function findUnboundLifecycleCleanup(
	entries: CleanupLedgerEntry[],
): {
	index: number;
	resource: string;
	action: string;
	reason: "missing" | "invalid";
} | undefined {
	for (const [index, entry,] of entries.entries()) {
		const parsed = parseArgs(entry.cleanup.argv,);
		const [resource, action,] = parsed.positional;
		if (resource === "app" && action === "delete-instance") {
			if (parsed.flags["unconfirmed-creation"] === true) continue;
			const expected = parsed.flags["expect-project-incarnation"];
			if (typeof expected !== "string") return { index, resource, action, reason: "missing", };
			if (!/^[0-9a-f]{64}$/.test(expected,)) return { index, resource, action, reason: "invalid", };
			continue;
		}
		if (resource === "project" && action === "delete") {
			const expected = parsed.flags["expect-project-incarnation"];
			if (typeof expected !== "string") return { index, resource, action, reason: "missing", };
			if (!/^[0-9a-f]{64}$/.test(expected,)) return { index, resource, action, reason: "invalid", };
			continue;
		}
	}
	return undefined;
}

async function runCleanup(flags: Record<string, string | boolean>,): Promise<{
	result: Record<string, unknown>;
	exitCode: number;
}> {
	const filePath = flags["file"];
	if (typeof filePath !== "string" || filePath.trim().length === 0) {
		throw new UsageError(`--file is required. Usage: ${CLEANUP_USAGE}`,);
	}
	let entries: CleanupLedgerEntry[];
	try {
		entries = await readCleanupLedger(filePath,);
	} catch (error) {
		throw new UsageError(
			`Could not read cleanup ledger: ${error instanceof Error ? error.message : String(error,)}`,
		);
	}
	const ordered: CleanupLedgerEntry[] = [];
	for (let index = entries.length - 1; index >= 0; index--) ordered.push(entries[index]!,);
	const steps = ordered.map((entry, index,) => ({
		index,
		resource: entry.resource,
		action: entry.action,
		id: entry.id,
		name: entry.name,
		path: entry.path,
		projectKey: entry.projectKey,
		dssUrl: typeof entry.dssUrl === "string" ? canonicalDssUrl(entry.dssUrl,) : null,
		cleanup: entry.cleanup,
	}));
	if (flags["apply"] !== true) {
		return { result: { dryRun: true, steps, }, exitCode: 0, };
	}

	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	if (!url) {
		throw new UsageError(
			"Missing Dataiku URL. Set DATAIKU_URL or pass --url.",
			"missing_required_flag",
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY or pass --api-key.",
			"missing_required_flag",
		);
	}
	const requestTimeoutMs = num(flags["request-timeout"], "--request-timeout",);
	const retryMaxAttempts = num(flags["retries"], "--retries",);
	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);

	// Refuse before any DSS call when the ledger is not entirely bound to this
	// server: a mixed ledger must never partially apply.
	const bindingViolation = findCleanupLedgerBindingViolation(ordered, client.getBaseUrl(),);
	if (bindingViolation) {
		return {
			result: {
				applied: false,
				steps,
				bindingError: {
					entryIndex: bindingViolation.index,
					resource: bindingViolation.resource,
					action: bindingViolation.action,
					reason: bindingViolation.reason,
					expectedDssUrl: client.getBaseUrl(),
					...(bindingViolation.found === undefined
						? {}
						: { foundDssUrl: bindingViolation.found, }),
				},
			},
			exitCode: 2,
		};
	}

	// Lifecycle binding is as load-bearing as server binding. Validate the whole
	// reversed plan before step one so a later legacy app or project entry
	// cannot leave an earlier cleanup applied and then fail on project-key
	// reuse protection.
	const lifecycleViolation = findUnboundLifecycleCleanup(ordered,);
	if (lifecycleViolation) {
		return {
			result: {
				applied: false,
				steps,
				lifecycleError: {
					entryIndex: lifecycleViolation.index,
					resource: lifecycleViolation.resource,
					action: lifecycleViolation.action,
					reason: lifecycleViolation.reason,
				},
			},
			exitCode: 2,
		};
	}

	const applied: Array<Record<string, unknown>> = [];
	const failures: Array<Record<string, unknown>> = [];
	for (const [index, entry,] of ordered.entries()) {
		try {
			const parsed = parseArgs(entry.cleanup.argv,);
			const [resource, action, ...args] = parsed.positional;
			if (
				!resource || !action || !isAllowedCleanupAction(resource, action,)
				|| !commands[resource]?.[action]
			) {
				throw new UsageError(`Invalid cleanup argv: ${entry.cleanup.argv.join(" ",)}`,);
			}
			if (
				resource === "project"
				&& action === "delete"
				&& typeof parsed.flags["expect-project-incarnation"] !== "string"
			) {
				throw new UsageError(
					"Project cleanup entry is not bound to a project incarnation. Refusing deletion after possible project-key reuse.",
					"validation_failed",
					"Capture a new cleanup entry from the current CLI. Legacy entries cannot safely delete projects.",
				);
			}
			if (
				resource === "app"
				&& action === "delete-instance"
				&& parsed.flags["unconfirmed-creation"] !== true
				&& typeof parsed.flags["expect-project-incarnation"] !== "string"
			) {
				throw new UsageError(
					"App cleanup entry is not bound to a project incarnation. Refusing deletion after possible project-key reuse.",
					"validation_failed",
					"Capture a new cleanup entry from the current CLI. Legacy entries cannot safely delete app projects.",
				);
			}
			const result = await commands[resource][action].handler(client, args, parsed.flags,);
			if (isFailedWaitResult(result,)) {
				const failure = {
					index,
					cleanup: entry.cleanup,
					error: commandFailureMessage(result,),
					result,
				};
				failures.push(failure,);
				if (flags["continue-on-error"] !== true) {
					return {
						result: { applied: true, steps, results: applied, failures, },
						exitCode: 2,
					};
				}
				continue;
			}
			applied.push({ index, cleanup: entry.cleanup, result, },);
		} catch (error) {
			const failure = {
				index,
				cleanup: entry.cleanup,
				error: error instanceof Error ? error.message : String(error,),
			};
			failures.push(failure,);
			if (flags["continue-on-error"] !== true) {
				return {
					result: { applied: true, steps, results: applied, failures, },
					exitCode: 2,
				};
			}
		}
	}
	return {
		result: { applied: true, steps, results: applied, failures, },
		exitCode: failures.length > 0 ? 2 : 0,
	};
}

interface BatchStepResult {
	index: number;
	args: string[];
	resource?: string;
	action?: string;
	ok: boolean | null;
	result?: unknown;
	error?: ErrorReportEnvelope;
	skipped?: boolean;
}

function parseBatchSteps(payload: unknown,): string[][] {
	if (!Array.isArray(payload,)) {
		throw new UsageError(
			"Batch payload must be a JSON array of command-argument arrays.",
			"validation_failed",
			BATCH_HINT,
			{ example: BATCH_EXAMPLE_PAYLOAD, },
		);
	}
	return payload.map((step, index,) => {
		if (!Array.isArray(step,) || !step.every((token,) => typeof token === "string")) {
			throw new UsageError(
				`Batch step ${index} must be an array of string arguments.`,
				"validation_failed",
				BATCH_HINT,
			);
		}
		return step as string[];
	},);
}

type CommandRegistry = Record<string, Record<string, CommandRegistryEntry>>;

const ALWAYS_ALLOWED_COMMAND_FLAGS: Record<string, true> = {
	json: true,
	verbose: true,
	fields: true,
	url: true,
	"api-key": true,
	"request-timeout": true,
	timeout: true,
	retries: true,
	insecure: true,
	"ca-cert": true,
	"project-key": true,
	plan: true,
	"dry-run": true,
};

const META_COMMAND_RESOURCES: Record<string, true> = {
	doctor: true,
	auth: true,
	"install-skill": true,
	agent: true,
	commands: true,
	version: true,
	cleanup: true,
	fixtures: true,
	batch: true,
};

let commandRegistryCache: CommandRegistry | undefined;

function cachedCommandRegistry(): CommandRegistry {
	if (commandRegistryCache === undefined) {
		commandRegistryCache = buildCommandRegistry();
	}
	return commandRegistryCache;
}

function supportedCommandFlags(entry: CommandRegistryEntry,): Record<string, true> {
	const supported: Record<string, true> = { ...ALWAYS_ALLOWED_COMMAND_FLAGS, };
	for (const flag of entry.flags) supported[flag.name] = true;
	for (const flag of entry.requiredFlags) supported[flag] = true;
	for (const flag of entry.optionalFlags) supported[flag] = true;
	for (const choice of entry.requiredOneOf ?? []) {
		for (const alternative of choice.oneOf) {
			for (const flag of alternative) supported[flag] = true;
		}
	}
	return supported;
}

function scopedCommandDiscoveryHint(resource: string, action: string,): string {
	return `Use \`dss commands run --fields ${resource}.${action}\` to list the flags this command supports.`;
}

/**
 * Resolve the registry field selectors for `commands run --fields`. Omitting the flag
 * selects the action summary; `--output` exports the full registry, but an explicitly supplied value carrying no selector is a
 * usage error: an agent that asked for a scoped subset must never be handed the full
 * registry instead.
 */
function commandRegistrySelectors(flags: Record<string, string | boolean>,): string[] {
	const fieldsFlag = flags["fields"];
	if (typeof fieldsFlag !== "string") return [];
	const selectors = fieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) =>
		field.length > 0
	);
	if (selectors.length === 0) {
		throw new UsageError(
			"--fields requires at least one selector. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].",
			"usage_error",
			COMMANDS_RUN_HINT,
			{ fields: fieldsFlag, },
		);
	}
	return selectors;
}

function validateSupportedCommandFlags(
	resource: string,
	action: string,
	flags: Record<string, string | boolean>,
): void {
	const entry = cachedCommandRegistry()[resource]?.[action];
	if (!entry) return;
	const supported = supportedCommandFlags(entry,);
	for (const flagName of Object.keys(flags,)) {
		if (supported[flagName] !== true) {
			throw new UsageError(
				`Unknown flag --${flagName} for ${resource} ${action}`,
				"unknown_flag",
				scopedCommandDiscoveryHint(resource, action,),
			);
		}
	}
	if (flags["dry-run"] === true && !entry.dryRun) {
		throw new UsageError(
			`--dry-run is not supported for ${resource} ${action}.`,
			"unknown_flag",
			entry.flags.some((flag,) => flag.name === "plan")
				? "Remove --dry-run; use --plan to preview the mutation instead."
				: `Remove --dry-run; ${resource} ${action} has no dry-run mode.`,
		);
	}
}

function stripOptionalUsageGroupsForValidation(usage: string,): string {
	let stripped = "";
	let optionalDepth = 0;
	for (const char of usage) {
		if (char === "[") {
			optionalDepth++;
			if (optionalDepth === 1) stripped += " ";
			continue;
		}
		if (char === "]" && optionalDepth > 0) {
			optionalDepth--;
			if (optionalDepth === 0) stripped += " ";
			continue;
		}
		if (optionalDepth === 0) stripped += char;
	}
	return stripped;
}

function requiredPositionalCount(usage: string,): number {
	const requiredUsage = stripOptionalUsageGroupsForValidation(usage,);
	const positionalTokens = [...requiredUsage.matchAll(/<[^>]+>/g,),];
	return positionalTokens.length;
}

function flagIsProvided(
	flags: Record<string, string | boolean>,
	name: string,
	allowEmpty = false,
): boolean {
	const value = flags[name];
	if (value === undefined || value === false) return false;
	if (typeof value !== "string") return true;
	return allowEmpty || value.length > 0;
}

function validateRequiredCommandInputs(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
): void {
	const argCount = requiredPositionalCount(entry.usage,);
	const allowEmptyFlags: Record<string, true> = {};
	for (const flag of entry.flags) {
		if (flag.allowEmptyValue === true) allowEmptyFlags[flag.name] = true;
	}
	if (args.length < argCount) requireArgs(args, argCount, entry.usage,);
	for (const flagName of entry.requiredFlags) {
		if (!flagIsProvided(flags, flagName, allowEmptyFlags[flagName] === true,)) {
			throw new UsageError(
				`--${flagName} is required. Usage: ${entry.usage}`,
				"missing_required_flag",
			);
		}
	}
	for (const choice of entry.requiredOneOf ?? []) {
		const satisfied = choice.oneOf.some((alternative,) =>
			alternative.every((flagName,) =>
				flagIsProvided(flags, flagName, allowEmptyFlags[flagName] === true,)
			)
		);
		if (!satisfied) {
			const alternatives = choice.oneOf.map((alternative,) =>
				alternative.map((flagName,) => `--${flagName}`).join(" and ",)
			).join(" or ",);
			throw new UsageError(
				`One of ${alternatives} is required for ${resource} ${action}. Usage: ${entry.usage}`,
				"missing_required_flag",
			);
		}
	}
}

function validateRegistryCommandInputs(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
): void {
	const entry = cachedCommandRegistry()[resource]?.[action];
	if (!entry) return;
	validateSupportedCommandFlags(resource, action, flags,);
	validateRequiredCommandInputs(resource, action, args, flags, entry,);
}

class BatchDryRunValidationComplete extends Error {
	constructor() {
		super("Batch dry-run validation reached client execution.",);
		this.name = "BatchDryRunValidationComplete";
	}
}

const batchDryRunClientTarget = function batchDryRunClientTarget(): void {};
const batchDryRunClientHandler: ProxyHandler<typeof batchDryRunClientTarget> = {
	get: (_target, property, receiver,) => {
		if (property === "then") return undefined;
		return receiver;
	},
	apply: () => {
		throw new BatchDryRunValidationComplete();
	},
};
const batchDryRunClient = new Proxy(
	batchDryRunClientTarget,
	batchDryRunClientHandler,
) as unknown as DataikuClient;

async function validateDryRunHandlerPreconditions(
	handler: (
		client: DataikuClient,
		args: string[],
		flags: Record<string, string | boolean>,
	) => Promise<unknown>,
	args: string[],
	flags: Record<string, string | boolean>,
): Promise<void> {
	const readsFromStdin = flags["stdin"] === true || flags["sql"] === "-";
	if (readsFromStdin) return;
	try {
		await handler(batchDryRunClient, args, flags,);
	} catch (error) {
		if (error instanceof BatchDryRunValidationComplete) return;
		throw error;
	}
}

function validateMetaCommandInputs(
	resource: string,
	action: string | undefined,
	flags: Record<string, string | boolean>,
): string | undefined {
	if (resource === "doctor") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("doctor", action, ["run",],);
		}
		return "run";
	}
	if (resource === "auth") {
		const validActions = Object.keys(AUTH_ACTIONS,);
		if (!action) {
			throw missingActionError("auth", validActions, "dss auth login --url URL --api-key KEY",);
		}
		if (!AUTH_ACTIONS[action]) {
			throw unknownActionError(
				"auth",
				action,
				validActions,
				"auth only supports 'login'. To check credentials/connectivity, run 'dss doctor'.",
			);
		}
		return action;
	}
	if (resource === "install-skill") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("install-skill", action, ["run",],);
		}
		const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
		if (agentFilter && !AGENTS[agentFilter]) {
			throw new UsageError(
				`Unknown agent: ${agentFilter}.`,
				"usage_error",
				COMMANDS_RUN_HINT,
				{ agent: agentFilter, validAgents: Object.keys(AGENTS,), },
			);
		}
		return "run";
	}
	if (resource === "agent") {
		if (!action) throw missingActionError("agent", ["contract",], AGENT_CONTRACT_USAGE,);
		if (action !== "contract") throw unknownActionError("agent", action, ["contract",],);
		return action;
	}
	if (resource === "commands") {
		if (!action) throw missingActionError("commands", ["run",], COMMANDS_USAGE,);
		if (action !== "run") throw unknownActionError("commands", action, ["run",],);
		validateSupportedCommandFlags("commands", "run", flags,);
		commandRegistrySelectors(flags,);
		return action;
	}
	if (resource === "version") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("version", action, ["run",],);
		}
		return "run";
	}
	if (resource === "cleanup") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("cleanup", action, ["run",],);
		}
		const entry = cachedCommandRegistry().cleanup?.run;
		if (entry) validateRequiredCommandInputs("cleanup", "run", [], flags, entry,);
		return "run";
	}
	if (resource === "fixtures") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("fixtures", action, ["run",],);
		}
		return "run";
	}
	if (resource === "batch") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("batch", action, ["run",],);
		}
		const entry = cachedCommandRegistry().batch?.run;
		if (entry) validateRequiredCommandInputs("batch", "run", [], flags, entry,);
		return "run";
	}
	return undefined;
}

async function validateBatchStep(
	argv: string[],
): Promise<
	{
		positional: string[];
		flags: Record<string, string | boolean>;
		resource?: string;
		action?: string;
	}
> {
	const { positional, flags, } = parseArgs(argv,);
	const resource = positional[0];
	const action = positional[1];
	if (!resource) throw noCommandError();
	const metaAction = validateMetaCommandInputs(resource, action, flags,);
	if (metaAction) return { positional, flags, resource, action: metaAction, };
	const resourceActions = commands[resource];
	if (!resourceActions) throw unknownResourceError(resource,);
	if (!action) {
		throw missingActionError(resource, Object.keys(resourceActions,), `dss ${resource} <action>`,);
	}
	const meta = resourceActions[action];
	if (!meta) throw unknownActionError(resource, action, Object.keys(resourceActions,),);
	validateRegistryCommandInputs(resource, action, positional.slice(2,), flags,);
	await validateDryRunHandlerPreconditions(meta.handler, positional.slice(2,), flags,);
	return { positional, flags, resource, action, };
}

function batchStepNeedsClient(argv: string[],): boolean {
	try {
		const { positional, flags, } = parseArgs(argv,);
		if (flags["plan"] === true) return false;
		const resource = positional[0];
		if (!resource) return false;
		const isMetaCommand = META_COMMAND_RESOURCES[resource] === true;
		return !isMetaCommand;
	} catch {
		return true;
	}
}

function batchStepCommandContext(argv: string[],): { resource?: string; action?: string; } {
	const positionals: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--") {
			positionals.push(...argv.slice(index + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const name = arg.slice(2,).split("=",)[0] ?? "";
			const canonical = FLAG_ALIASES[name] ?? name;
			if (!arg.includes("=",) && VALUE_FLAGS.has(canonical,)) index++;
			continue;
		}
		if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long && VALUE_FLAGS.has(long,)) index++;
			continue;
		}
		positionals.push(arg,);
	}
	return { resource: positionals[0], action: positionals[1], };
}

const SENSITIVE_ARGV_VALUE_FLAGS: Record<string, true> = {
	"api-key": true,
	repository: true,
};

function redactArgv(argv: string[],): string[] {
	const redacted: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index]!;
		if (arg === "--") {
			redacted.push(...argv.slice(index,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const eqIdx = arg.indexOf("=",);
			const rawFlagName = eqIdx === -1 ? arg.slice(2,) : arg.slice(2, eqIdx,);
			const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
			if (SENSITIVE_ARGV_VALUE_FLAGS[flagName] === true) {
				if (eqIdx !== -1) {
					redacted.push(`${arg.slice(0, eqIdx + 1,)}***`,);
				} else {
					redacted.push(arg,);
					const next = argv[index + 1];
					if (
						next !== undefined
						&& (next === "-" || !next.startsWith("-",) || isNegativeNumberToken(next,))
					) {
						redacted.push("***",);
						index++;
					}
				}
				continue;
			}
		}
		redacted.push(arg,);
	}
	return redacted;
}

function validateBatchStepMode(argv: string[], index: number,): void {
	let positional: string[];
	let flags: Record<string, string | boolean>;
	try {
		({ positional, flags, } = parseArgs(argv,));
	} catch {
		return;
	}
	if (flags["plan"] !== true && flags["dry-run"] !== true) return;
	const resource = positional[0];
	const action = positional[1];
	if (!resource) return;
	if (META_COMMAND_RESOURCES[resource] === true) return;
	const resourceActions = commands[resource];
	if (!resourceActions) return;
	const meta = resourceActions[action ?? ""];
	if (!meta) return;
	const entry = cachedCommandRegistry()[resource]?.[action ?? ""];
	if (!entry) return;
	if (flags["plan"] === true && !entry.flags.some((flag,) => flag.name === "plan")) {
		throw new UsageError(
			`Batch step ${index} requests --plan, which is not supported for ${resource} ${action}.`,
			"unknown_flag",
			"Remove --plan from the step; only mutating commands support --plan.",
		);
	}
	if (flags["dry-run"] === true && !entry.dryRun) {
		throw new UsageError(
			`Batch step ${index} requests --dry-run, which is not supported for ${resource} ${action}.`,
			"unknown_flag",
			"Remove --dry-run from the step.",
		);
	}
}

function validateBatchStepModes(steps: string[][],): void {
	for (let index = 0; index < steps.length; index++) {
		validateBatchStepMode(steps[index]!, index,);
	}
}

function runInstallSkill(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
	const isGlobal = flags["global"] === true;
	const targetDir = typeof flags["target"] === "string" ? flags["target"] : undefined;

	const targets = (() => {
		if (!agentFilter) return detectAgents();
		const def = AGENTS[agentFilter];
		if (!def) {
			throw new UsageError(
				`Unknown agent: ${agentFilter}.`,
				"usage_error",
				COMMANDS_RUN_HINT,
				{ agent: agentFilter, validAgents: Object.keys(AGENTS,), },
			);
		}
		return [{ id: agentFilter, def, via: "flag" as const, },];
	})();

	if (flags["list-agents"] === true) {
		return {
			agents: targets.map((target,) => ({
				id: target.id,
				name: target.def.name,
				via: target.via,
			})),
		};
	}

	if (targets.length === 0) {
		throw new UsageError(
			"No coding agents detected.",
			"usage_error",
			"Use --agent NAME to choose one of the supported agents.",
			{ validAgents: Object.keys(AGENTS,), },
		);
	}

	const scope = isGlobal ? "global" : "project";
	const cwd = targetDir ?? (isGlobal ? process.cwd() : findWorkspaceRoot(process.cwd(),));
	const installed = planSkillInstalls(targets, { global: isGlobal, cwd, },);

	if (flags["plan"] === true) {
		return planResult("install-skill", "run", {
			identifiers: { scope, target: cwd, },
			payload: { installed, },
			idempotency: "none",
			asyncKind: "none",
			exitCodesOnFailure: { usage: 1, error: 2, transient: 3, },
			plannedAndDryRun: flags["dry-run"] === true,
		},);
	}

	return {
		scope,
		target: cwd,
		installed: flags["dry-run"] === true
			? installed
			: installSkill(targets, { global: isGlobal, cwd, },),
		...(flags["dry-run"] === true ? { dryRun: true, } : {}),
	};
}

const META_PLAN_EXIT_CODES: Record<string, number> = { usage: 1, error: 2, transient: 3, };

function authLoginPlan(flags: Record<string, string | boolean>,): Record<string, unknown> {
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

	const payload: Record<string, unknown> = { apiKeyProvided: true, };
	if (projectKey) payload.projectKey = projectKey;
	if (tlsSettings.tlsRejectUnauthorized !== undefined) {
		payload.tlsRejectUnauthorized = tlsSettings.tlsRejectUnauthorized;
	}
	if (tlsSettings.caCertPath) payload.caCertPath = tlsSettings.caCertPath;
	const configTarget = getCredentialsPath();

	return planResult("auth", "login", {
		identifiers: {
			url: url.trim().replace(/\/+$/, "",),
			configTarget,
		},
		payload,
		localWrites: [{ path: configTarget, target: "credentials", redacted: ["apiKey",], },],
		idempotency: "convergent",
		asyncKind: "none",
		exitCodesOnFailure: META_PLAN_EXIT_CODES,
	},);
}

function cleanupPlan(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const filePath = flags["file"];
	if (typeof filePath !== "string" || filePath.trim().length === 0) {
		throw new UsageError(`--file is required. Usage: ${CLEANUP_USAGE}`,);
	}
	return planResult("cleanup", "run", {
		identifiers: { file: filePath, },
		payload: {
			apply: flags["apply"] === true,
			continueOnError: flags["continue-on-error"] === true,
		},
		idempotency: "none",
		asyncKind: "none",
		exitCodesOnFailure: META_PLAN_EXIT_CODES,
		plannedAndDryRun: flags["dry-run"] === true,
	},);
}

function batchPlan(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const payload = unknownJsonInput(flags,);
	if (payload === undefined) {
		throw new UsageError(
			`Provide steps via --data, --data-file, or --stdin. Usage: ${BATCH_USAGE}`,
			"missing_required_flag",
			BATCH_HINT,
		);
	}
	const steps = parseBatchSteps(payload,);
	return planResult("batch", "run", {
		identifiers: {
			total: steps.length,
			needsClient: steps.some((argv,) => batchStepNeedsClient(argv,)),
		},
		payload: {
			steps: steps.map((argv,) => redactArgv(argv,)),
			continueOnError: flags["continue-on-error"] === true,
			dryRun: flags["dry-run"] === true,
		},
		idempotency: "none",
		asyncKind: "none",
		exitCodesOnFailure: BATCH_PLAN_EXIT_CODES,
		plannedAndDryRun: flags["dry-run"] === true,
	},);
}

async function runMetaCommand(
	resource: string,
	action: string | undefined,
	flags: Record<string, string | boolean>,
): Promise<{ action: string; result: unknown; exitCode: number; } | undefined> {
	if (resource === "doctor") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("doctor", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		const { result, exitCode, } = await runDoctor(flags,);
		return { action: "run", result, exitCode, };
	}
	if (resource === "auth") {
		const validActions = Object.keys(AUTH_ACTIONS,);
		if (!action) {
			throw missingActionError("auth", validActions, "dss auth login --url URL --api-key KEY",);
		}
		currentCommandContext.action = action;
		const authMeta = AUTH_ACTIONS[action];
		if (!authMeta) {
			throw unknownActionError(
				"auth",
				action,
				validActions,
				"auth only supports 'login'. To check credentials/connectivity, run 'dss doctor'.",
			);
		}
		if (flags["plan"] === true) {
			return { action, result: authLoginPlan(flags,), exitCode: 0, };
		}
		return { action, result: await authMeta.handler(flags,), exitCode: 0, };
	}
	if (resource === "install-skill") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("install-skill", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		return { action: "run", result: runInstallSkill(flags,), exitCode: 0, };
	}
	if (resource === "agent") {
		if (!action) throw missingActionError("agent", ["contract",], AGENT_CONTRACT_USAGE,);
		currentCommandContext.action = action;
		if (action !== "contract") throw unknownActionError("agent", action, ["contract",],);
		return { action, result: buildAgentContract(), exitCode: 0, };
	}
	if (resource === "commands") {
		if (!action) throw missingActionError("commands", ["run",], COMMANDS_USAGE,);
		currentCommandContext.action = action;
		if (action !== "run") throw unknownActionError("commands", action, ["run",],);
		validateSupportedCommandFlags("commands", "run", flags,);
		const selectors = commandRegistrySelectors(flags,);
		const registry = buildCommandRegistry();
		for (const selector of selectors) {
			const selectorParts = selector.split(".",);
			if (selectorParts.some((part,) => part.length === 0)) {
				throw new UsageError(
					`Invalid --fields selector: ${selector}. Expected RESOURCE or RESOURCE.ACTION[.FIELD...].`,
					"usage_error",
					COMMANDS_USAGE,
					{ selector, },
				);
			}
			const [selectedResource, selectedAction,] = selectorParts;
			const resourceActions = registry[selectedResource];
			if (!resourceActions) throw unknownResourceError(selectedResource,);
			if (selectedAction && !resourceActions[selectedAction]) {
				throw unknownActionError(
					selectedResource,
					selectedAction,
					Object.keys(resourceActions,),
				);
			}
		}
		const outputPath = flags["output"];
		const selectedRegistry = selectors.length === 0
			? registry
			: projectResultFields(registry, selectors,);
		if (typeof outputPath === "string") {
			const exported = selectedRegistry;
			await fs.writeFile(outputPath, `${JSON.stringify(exported,)}\n`, "utf-8",);
			return { action, result: { path: outputPath, }, exitCode: 0, };
		}
		return {
			action,
			result: selectors.length === 0 ? commandActionSummary(registry,) : selectedRegistry,
			exitCode: 0,
		};
	}
	if (resource === "version") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("version", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		return { action: "run", result: cliVersionResult(), exitCode: 0, };
	}
	if (resource === "cleanup") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("cleanup", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		if (flags["plan"] === true) {
			return { action: "run", result: cleanupPlan(flags,), exitCode: 0, };
		}
		const { result, exitCode, } = await runCleanup(flags,);
		return { action: "run", result, exitCode, };
	}
	if (resource === "fixtures") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("fixtures", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		return { action: "run", result: await runFixtures(flags,), exitCode: 0, };
	}
	if (resource === "batch") {
		if (action !== undefined && action !== "run") {
			throw unknownActionError("batch", action, ["run",],);
		}
		currentCommandContext.action = action ?? "run";
		if (flags["plan"] === true) {
			return { action: "run", result: batchPlan(flags,), exitCode: 0, };
		}
		const { result, exitCode, } = await runBatch(flags,);
		return { action: "run", result, exitCode, };
	}
	return undefined;
}

/**
 * Cleanup-ledger support is checked before a handler runs so an unsupported
 * `--record-cleanup` fails without mutating DSS. Direct dispatch and batch steps
 * share this guard.
 */
function assertCleanupLedgerSupported(
	resource: string,
	action: string,
	flags: Record<string, string | boolean>,
): void {
	if (typeof flags["record-cleanup"] !== "string" || flags["dry-run"] === true) return;
	if (!supportsCleanupLedger(resource, action,)) {
		throw new UsageError(`--record-cleanup is not supported for ${resource} ${action}.`,);
	}
}

/**
 * Preflight a requested cleanup ledger path before any remote mutation so an
 * unwritable (or unwritable-to-be) ledger fails fast with no DSS-side change.
 * Shares assertCleanupLedgerSupported's placement: direct dispatch and batch
 * steps both call it, and dry-run/preview runs never touch the ledger.
 */
async function preflightCleanupLedgerForFlags(
	client: DataikuClient,
	flags: Record<string, string | boolean>,
): Promise<void> {
	if (flags["dry-run"] === true) return;
	const ledgerPath = flags["record-cleanup"];
	if (typeof ledgerPath !== "string") return;
	if (ledgerPath.trim().length === 0) {
		throw new UsageError("--record-cleanup requires a non-empty file path.", "validation_failed",);
	}
	const assertExistingEntriesBound = async (): Promise<void> => {
		let entries: CleanupLedgerEntry[];
		try {
			entries = await readCleanupLedger(ledgerPath,);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			throw error;
		}
		const violation = findCleanupLedgerBindingViolation(entries, client.getBaseUrl(),);
		if (violation) {
			throw new UsageError(
				`Existing cleanup ledger entry ${
					violation.index + 1
				} is not bound to the configured DSS server.`,
				"validation_failed",
				"Use a new cleanup ledger path, or run the mutation against the DSS server already bound to this ledger.",
				{
					entryIndex: violation.index,
					resource: violation.resource,
					action: violation.action,
					reason: violation.reason,
				},
			);
		}
	};
	try {
		await preflightCleanupLedgerPath(ledgerPath,);
		await assertExistingEntriesBound();
		await reserveCleanupLedgerDssUrl(ledgerPath, client.getBaseUrl(),);
		// Re-read after winning the atomic reservation. This catches an entry
		// appended between the initial compatibility check and reservation.
		await assertExistingEntriesBound();
	} catch (error) {
		if (error instanceof UsageError) throw error;
		throw new UsageError(
			`Could not preflight cleanup ledger: ${error instanceof Error ? error.message : String(error,)}`,
			"validation_failed",
		);
	}
}

/**
 * Append the cleanup entry for a completed handler result. Callers must pass the
 * raw, unprojected result and must call this before any failure exit code is
 * raised: an indeterminate post-POST outcome stays addressable only when the
 * ledger records it first. Eligibility (dry runs, skipped work, an explicit
 * `cleanupEligible:false`) is decided by `cleanupLedgerEntry`, never here.
 */
async function recordCleanupLedgerEntry(
	client: DataikuClient,
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	result: unknown,
	projectKey: string | undefined,
): Promise<void> {
	const ledgerPath = flags["record-cleanup"];
	if (typeof ledgerPath !== "string" || flags["dry-run"] === true) return;
	const entry = cleanupLedgerEntry(resource, action, args, flags, result, projectKey,);
	if (entry) await appendCleanupLedgerEntry(ledgerPath, entry, client.getBaseUrl(),);
}

async function runBatch(flags: Record<string, string | boolean>,): Promise<{
	result: Record<string, unknown>;
	exitCode: number;
}> {
	const payload = unknownJsonInput(flags,);
	if (payload === undefined) {
		throw new UsageError(
			`Provide steps via --data, --data-file, or --stdin. Usage: ${BATCH_USAGE}`,
			"missing_required_flag",
			BATCH_HINT,
		);
	}
	const steps = parseBatchSteps(payload,);

	if (flags["dry-run"] === true) {
		const batchProjectKey = typeof flags["project-key"] === "string"
			? flags["project-key"]
			: dataikuEnvironmentEnabled()
			? process.env.DATAIKU_PROJECT_KEY
			: undefined;
		const planned = await Promise.all(steps.map(async (argv, index,) => {
			const context = batchStepCommandContext(argv,);
			Object.assign(currentCommandContext, { ...context, projectKey: batchProjectKey, },);
			try {
				validateBatchStepMode(argv, index,);
				const step = await validateBatchStep(argv,);
				Object.assign(currentCommandContext, {
					resource: step.resource,
					action: step.action,
					projectKey: typeof step.flags["project-key"] === "string"
						? step.flags["project-key"]
						: batchProjectKey,
				},);
				return {
					index,
					args: redactArgv(argv,),
					resource: step.resource,
					action: step.action,
					runnable: true,
				};
			} catch (error) {
				const envelope = buildErrorReport(error,);
				return {
					index,
					args: redactArgv(argv,),
					resource: context.resource,
					action: context.action,
					runnable: false,
					error: envelope,
				};
			}
		},),);
		return {
			result: { dryRun: true, total: steps.length, steps: planned, },
			exitCode: planned.every((step,) => step.runnable) ? 0 : 1,
		};
	}

	validateBatchStepModes(steps,);

	let projectKey = typeof flags["project-key"] === "string"
		? flags["project-key"]
		: dataikuEnvironmentEnabled()
		? process.env.DATAIKU_PROJECT_KEY
		: undefined;
	let client: DataikuClient | undefined;
	const needsClient = steps.some((argv,) => batchStepNeedsClient(argv,));
	if (needsClient) {
		const {
			url,
			apiKey,
			projectKey: resolvedProjectKey,
			tlsRejectUnauthorized,
			caCertPath,
		} = resolveCredentials(flags,);
		projectKey = resolvedProjectKey;
		if (!url) {
			throw new UsageError(
				"Missing Dataiku URL.",
				"missing_required_flag",
				"Set DATAIKU_URL or pass --url.",
				{
					requiredFlags: ["url",],
					env: ["DATAIKU_URL",],
				},
			);
		}
		if (!apiKey) {
			throw new UsageError(
				"Missing API key.",
				"missing_required_flag",
				"Set DATAIKU_API_KEY or pass --api-key.",
				{
					requiredFlags: ["api-key",],
					env: ["DATAIKU_API_KEY",],
				},
			);
		}
		client = new DataikuClient({
			url,
			apiKey,
			projectKey,
			verbose: flags["verbose"] === true,
			requestTimeoutMs: num(flags["request-timeout"], "--request-timeout",),
			retryMaxAttempts: num(flags["retries"], "--retries",),
			tlsRejectUnauthorized,
			caCertPath,
		},);
	}

	const continueOnError = flags["continue-on-error"] === true;
	const results: BatchStepResult[] = [];
	let firstFailureExit: number | undefined;

	for (let index = 0; index < steps.length; index++) {
		const argv = steps[index]!;
		const context = batchStepCommandContext(argv,);
		let resource = context.resource;
		let action = context.action;
		if (firstFailureExit !== undefined && !continueOnError) {
			results.push({ index, args: redactArgv(argv,), resource, action, ok: null, skipped: true, },);
			continue;
		}
		Object.assign(currentCommandContext, {
			resource,
			action,
			projectKey,
		},);
		try {
			const { positional, flags: stepFlags, } = parseArgs(argv,);
			resource = positional[0];
			action = positional[1];
			Object.assign(currentCommandContext, {
				resource,
				action,
				projectKey: typeof stepFlags["project-key"] === "string"
					? stepFlags["project-key"]
					: projectKey,
			},);
			if (!resource) throw noCommandError();
			const metaCommand = await runMetaCommand(resource, action, stepFlags,);
			let result: unknown;
			if (metaCommand) {
				action = metaCommand.action;
				currentCommandContext.action = action;
				if (metaCommand.exitCode !== 0) {
					throw new CommandResultFailure(metaCommand.result, metaCommand.exitCode,);
				}
				result = metaCommand.result;
			} else {
				const resourceActions = commands[resource];
				if (!resourceActions) throw unknownResourceError(resource,);
				if (!action) {
					throw missingActionError(resource, Object.keys(resourceActions,), `dss ${resource} <action>`,);
				}
				const meta = resourceActions[action];
				if (!meta) throw unknownActionError(resource, action, Object.keys(resourceActions,),);
				validateSupportedCommandFlags(resource, action, stepFlags,);
				if (stepFlags["plan"] === true) {
					result = buildMutationPlan(resource, action, meta, positional.slice(2,), stepFlags,);
				} else {
					if (!client) {
						throw new UsageError(
							"Missing Dataiku URL.",
							"missing_required_flag",
							"Set DATAIKU_URL or pass --url.",
							{ requiredFlags: ["url",], env: ["DATAIKU_URL",], },
						);
					}
					assertCleanupLedgerSupported(resource, action, stepFlags,);
					await preflightCleanupLedgerForFlags(client, stepFlags,);
					const stepArgs = positional.slice(2,);
					result = await meta.handler(client, stepArgs, stepFlags,);
					await recordCleanupLedgerEntry(
						client,
						resource,
						action,
						stepArgs,
						stepFlags,
						result,
						projectKey,
					);
				}
				const failureExitCode = commandFailureExitCode(result,);
				if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
			}
			const stepFieldsFlag = stepFlags["fields"];
			const stepFields = typeof stepFieldsFlag === "string"
				? stepFieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) => field.length > 0)
				: [];
			const stepResult = stepFields.length > 0 && !(resource === "commands" && action === "run")
				? projectResultFields(result, stepFields,)
				: result;
			results.push({
				index,
				args: redactArgv(argv,),
				resource,
				action,
				ok: true,
				result: stepResult,
			},);
		} catch (error) {
			const envelope = buildErrorReport(error,);
			results.push({ index, args: redactArgv(argv,), resource, action, ok: false, error: envelope, },);
			if (firstFailureExit === undefined) firstFailureExit = envelope.exitCode;
		}
	}

	const ok = firstFailureExit === undefined;
	return {
		result: {
			ok,
			total: steps.length,
			completed: results.filter((step,) => step.ok !== null).length,
			steps: results,
		},
		exitCode: ok ? 0 : firstFailureExit ?? 2,
	};
}

interface ErrorReportEnvelope {
	type: "error";
	ok: false;
	error: string;
	code: StableErrorCode;
	category: "usage" | "permission_or_environment" | "dss" | "internal";
	exitCode: number;
	hint?: string;
	resource?: string;
	action?: string;
	projectKey?: string;
	requestId?: string;
	status?: number;
	retryable?: boolean;
	details?: Record<string, unknown>;
}

function rawFlagValue(argv: string[], flagName: string,): string | undefined {
	const longFlag = `--${flagName}`;
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === longFlag) {
			const next = argv[index + 1];
			return next && !next.startsWith("-",) ? next : undefined;
		}
		if (arg.startsWith(`${longFlag}=`,)) return arg.slice(longFlag.length + 1,);
	}
	return undefined;
}

function commandIsProjectScoped(
	resource: string | undefined,
	action: string | undefined,
): boolean {
	if (!resource) return false;
	const usage = commands[resource]?.[action ?? ""]?.usage ?? "";
	return inferRequiresProject(resource, action ?? "", usage,);
}

function rawCommandContext(): { resource?: string; action?: string; projectKey?: string; } {
	const argv = process.argv.slice(2,);
	const positionals: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--") {
			positionals.push(...argv.slice(index + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const name = arg.slice(2,).split("=",)[0] ?? "";
			const canonical = FLAG_ALIASES[name] ?? name;
			if (!arg.includes("=",) && VALUE_FLAGS.has(canonical,)) index++;
			continue;
		}
		if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long && VALUE_FLAGS.has(long,)) index++;
			continue;
		}
		positionals.push(arg,);
	}
	const resource = currentCommandContext.resource ?? positionals[0];
	const action = currentCommandContext.action ?? positionals[1];
	const explicitProjectKey = rawFlagValue(argv, "project-key",) ?? rawFlagValue(argv, "project",);
	const ambientProjectKey = dataikuEnvironmentEnabled()
		? process.env.DATAIKU_PROJECT_KEY
		: undefined;
	return {
		resource,
		action,
		projectKey: explicitProjectKey
			?? (commandIsProjectScoped(resource, action,)
				? currentCommandContext.projectKey ?? ambientProjectKey
				: undefined),
	};
}

const SAFE_RESPONSE_METADATA_KEYS = [
	"requestId",
	"request_id",
	"errorId",
	"elapsedMs",
] as const;

function safeResponseMetadata(body: string,): Record<string, string | number | boolean> {
	try {
		const parsed: unknown = JSON.parse(body,);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed,)) return {};
		const metadata: Record<string, string | number | boolean> = {};
		for (const key of SAFE_RESPONSE_METADATA_KEYS) {
			const value = (parsed as Record<string, unknown>)[key];
			if (
				(typeof value === "string" && value.length > 0)
				|| (typeof value === "number" && Number.isFinite(value,))
				|| typeof value === "boolean"
			) {
				metadata[key] = value;
			}
		}
		return metadata;
	} catch {
		return {};
	}
}

function requestIdFromBody(body: string,): string | undefined {
	const metadata = safeResponseMetadata(body,);
	for (const key of ["requestId", "request_id", "errorId",] as const) {
		const value = metadata[key];
		if (typeof value === "string") return value;
	}
	return undefined;
}

const MISSING_PROJECT_KEY_ERROR_PREFIX = "projectKey is required";

function errorExitCode(err: unknown,): number {
	if (err instanceof CommandResultFailure) return err.exitCode;
	if (err instanceof ClientValidationError && err.code === "ambiguous_outcome") return 2;
	if (err instanceof UsageError || err instanceof ClientValidationError) return 1;
	if (err instanceof DataikuError) return err.category === "transient" ? 3 : 2;
	if (err instanceof Error && err.message.startsWith(MISSING_PROJECT_KEY_ERROR_PREFIX,)) return 1;
	return 2;
}

function buildErrorReport(err: unknown,): ErrorReportEnvelope {
	const context = rawCommandContext();
	const exitCode = errorExitCode(err,);
	if (err instanceof ClientValidationError && err.code === "ambiguous_outcome") {
		return {
			type: "error",
			ok: false,
			error: err.message,
			code: err.code,
			category: "dss",
			exitCode,
			...(err.hint ? { hint: err.hint, } : {}),
			...(err.details ? { details: err.details, } : {}),
			...context,
		};
	}
	if (err instanceof UsageError || err instanceof ClientValidationError) {
		return {
			type: "error",
			ok: false,
			error: err.message,
			code: err.code,
			category: err.code === "target_absence_unverifiable"
				? "permission_or_environment"
				: "usage",
			exitCode,
			...(err.code === "target_absence_unverifiable" ? { retryable: false, } : {}),
			...(err.hint ? { hint: err.hint, } : {}),
			...(err.details ? { details: err.details, } : {}),
			...context,
		};
	}
	if (err instanceof CommandResultFailure) {
		return {
			type: "error",
			ok: false,
			error: err.message,
			code: err.code,
			category: "dss",
			exitCode: err.exitCode,
			details: { result: err.result, },
			...context,
		};
	}
	if (err instanceof DataikuError) {
		const errorMessage = err.category === "not_found" && context.resource
			? `Not found: ${context.resource}${context.action ? ` ${context.action}` : ""}`
				+ `${context.projectKey ? ` in project ${context.projectKey}` : ""}`
				+ " — verify the object identifier and project key."
				+ `\nHint: ${err.retryHint}`
			: err.safeMessage;
		const safeMetadata = safeResponseMetadata(err.body,);
		if (err.trustedTarget) safeMetadata.target = err.trustedTarget;
		if (err.trustedElapsedMs !== undefined) safeMetadata.elapsedMs = err.trustedElapsedMs;
		const safeBody = JSON.stringify(safeMetadata,);
		return {
			type: "error",
			ok: false,
			error: errorMessage,
			code: dataikuErrorCode(err.category,),
			category: "dss",
			exitCode,
			hint: err.retryHint,
			status: err.status,
			retryable: err.retryable,
			requestId: err.requestId ?? requestIdFromBody(err.body,),
			details: {
				dssCategory: err.category,
				statusText: canonicalStatusText(err.status,),
				body: safeBody,
				...(err.retry ? { retry: err.retry, } : {}),
			},
			...context,
		};
	}
	if (err instanceof Error && err.message.startsWith(MISSING_PROJECT_KEY_ERROR_PREFIX,)) {
		return {
			type: "error",
			ok: false,
			error: err.message,
			code: "missing_required_flag",
			category: "usage",
			exitCode,
			hint: "Pass --project-key or set DATAIKU_PROJECT_KEY.",
			...context,
		};
	}
	const message = err instanceof Error ? err.message : String(err,);
	return {
		type: "error",
		ok: false,
		error: message,
		code: "internal_error",
		category: "internal",
		exitCode,
		...context,
	};
}
function writeErrorReport(err: unknown,): void {
	flushCliWarnings();
	process.stdout.write(`${JSON.stringify(buildErrorReport(err,),)}\n`,);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	loadEnvFile();
	const { positional, flags, } = parseArgs(process.argv.slice(2,),);
	const fieldsFlag = flags["fields"];
	if (typeof fieldsFlag === "string") {
		const selected = fieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) =>
			field.length > 0
		);
		if (selected.length > 0) {
			const commandsRunProjection = positional[0] === "commands" && positional[1] === "run";
			if (!commandsRunProjection) setOutputFieldProjection(selected,);
		}
	}

	if (flags["version"] === true) {
		writeCommandResult(cliVersionResult(),);
		return;
	}

	if (positional.length === 0) throw noCommandError();

	const resource = positional[0]!;
	Object.assign(currentCommandContext, {
		resource,
		action: positional[1],
		projectKey: typeof flags["project-key"] === "string"
			? flags["project-key"]
			: dataikuEnvironmentEnabled()
			? process.env.DATAIKU_PROJECT_KEY
			: undefined,
	},);

	const metaCommand = await runMetaCommand(resource, positional[1], flags,);
	if (metaCommand) {
		writeCommandResult(metaCommand.result,);
		if (metaCommand.exitCode !== 0) process.exitCode = metaCommand.exitCode;
		return;
	}

	if (!commands[resource]) throw unknownResourceError(resource,);

	const resourceActions = commands[resource]!;
	if (positional.length === 1) {
		throw missingActionError(
			resource,
			Object.keys(resourceActions,),
			`dss ${resource} <action> [args...]`,
		);
	}

	const action = positional[1]!;
	currentCommandContext.action = action;
	const actionMeta = resourceActions[action];

	if (!actionMeta) throw unknownActionError(resource, action, Object.keys(resourceActions,),);
	validateSupportedCommandFlags(resource, action, flags,);

	const args = positional.slice(2,);
	if (flags["plan"] === true) {
		const plan = buildMutationPlan(resource, action, actionMeta, args, flags,);
		writeCommandResult(plan,);
		return;
	}
	if (actionMeta.localHandler) {
		const result = await actionMeta.localHandler(args, flags,);
		const failureExitCode = commandFailureExitCode(result,);
		if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
		writeCommandResult(result,);
		return;
	}

	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);
	currentCommandContext.projectKey = projectKey;

	if (!url) {
		throw new UsageError(
			"Missing Dataiku URL.",
			"missing_required_flag",
			"Set DATAIKU_URL or pass --url.",
			{ requiredFlags: ["url",], env: ["DATAIKU_URL",], },
		);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key.",
			"missing_required_flag",
			"Set DATAIKU_API_KEY or pass --api-key.",
			{ requiredFlags: ["api-key",], env: ["DATAIKU_API_KEY",], },
		);
	}

	const requestTimeoutMs = num(flags["request-timeout"], "--request-timeout",);
	const retryMaxAttempts = num(flags["retries"], "--retries",);

	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		retryMaxAttempts,
		tlsRejectUnauthorized,
		caCertPath,
	},);

	assertCleanupLedgerSupported(resource, action, flags,);
	await preflightCleanupLedgerForFlags(client, flags,);
	const result = await actionMeta.handler(client, args, flags,);
	await recordCleanupLedgerEntry(client, resource, action, args, flags, result, projectKey,);
	const failureExitCode = commandFailureExitCode(result,);
	if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
	writeCommandResult(result,);
}

main().catch((err: unknown,) => {
	writeErrorReport(err,);
	process.exitCode = errorExitCode(err,);
},);
