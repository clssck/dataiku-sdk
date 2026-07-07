#!/usr/bin/env node

import { num, unknownJsonInput, } from "./cli/coerce.js";
import { commands, } from "./cli/commands/index.js";
import {
	AGENT_CONTRACT_USAGE,
	AUTH_ACTIONS,
	BATCH_EXAMPLE_PAYLOAD,
	BATCH_HINT,
	BATCH_USAGE,
	buildAgentContract,
	buildCommandRegistry,
	buildMutationPlan,
	CLEANUP_USAGE,
	type CommandRegistryEntry,
	COMMANDS_USAGE,
	inferRequiresProject,
	isAllowedCleanupAction,
	supportsCleanupLedger,
} from "./cli/contract.js";
import { runDoctor, runFixtures, } from "./cli/doctor.js";
import { dataikuEnvironmentEnabled, loadEnvFile, } from "./cli/env.js";
import { FLAG_ALIASES, parseArgs, SHORT_FLAGS, VALUE_FLAGS, } from "./cli/flags.js";
import { cleanupLedgerEntry, } from "./cli/helpers/cleanup.js";
import {
	commandFailureExitCode,
	CommandResultFailure,
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
	readCleanupLedger,
} from "./utils/cleanup-ledger.js";

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
	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],);
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
			const result = await commands[resource][action].handler(client, args, parsed.flags,);
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

const REDACTED_ARG_VALUE = "[REDACTED]";
const SENSITIVE_BATCH_ARG_FLAGS = new Set(["--api-key",],);

function redactBatchStepArgs(argv: string[],): string[] {
	let redactNext = false;
	return argv.map((token,) => {
		if (redactNext) {
			redactNext = false;
			return REDACTED_ARG_VALUE;
		}

		const equalsIndex = token.indexOf("=",);
		const flag = equalsIndex === -1 ? token : token.slice(0, equalsIndex,);
		if (!SENSITIVE_BATCH_ARG_FLAGS.has(flag,)) return token;
		if (equalsIndex === -1) {
			redactNext = true;
			return token;
		}
		return `${flag}=${REDACTED_ARG_VALUE}`;
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
			throw new UsageError(`Unknown flag --${flagName} for ${resource} ${action}`, "unknown_flag",);
		}
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

function flagIsProvided(flags: Record<string, string | boolean>, name: string,): boolean {
	const value = flags[name];
	if (value === undefined || value === false) return false;
	return typeof value !== "string" || value.length > 0;
}

function validateRequiredCommandInputs(
	resource: string,
	action: string,
	args: string[],
	flags: Record<string, string | boolean>,
	entry: CommandRegistryEntry,
): void {
	const argCount = requiredPositionalCount(entry.usage,);
	if (args.length < argCount) requireArgs(args, argCount, entry.usage,);
	for (const flagName of entry.requiredFlags) {
		if (!flagIsProvided(flags, flagName,)) {
			throw new UsageError(
				`--${flagName} is required. Usage: ${entry.usage}`,
				"missing_required_flag",
			);
		}
	}
	for (const choice of entry.requiredOneOf ?? []) {
		const satisfied = choice.oneOf.some((alternative,) =>
			alternative.every((flagName,) => flagIsProvided(flags, flagName,))
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
		const { positional, } = parseArgs(argv,);
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
			steps: steps.map((argv,) => redactBatchStepArgs(argv,),),
			continueOnError: flags["continue-on-error"] === true,
			dryRun: flags["dry-run"] === true,
		},
		idempotency: "none",
		asyncKind: "none",
		exitCodesOnFailure: META_PLAN_EXIT_CODES,
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
		return { action, result: buildCommandRegistry(), exitCode: 0, };
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
				const step = await validateBatchStep(argv,);
				Object.assign(currentCommandContext, {
					resource: step.resource,
					action: step.action,
					projectKey: typeof step.flags["project-key"] === "string"
						? step.flags["project-key"]
						: batchProjectKey,
				},);
				return { index, args: argv, resource: step.resource, action: step.action, runnable: true, };
			} catch (error) {
				const envelope = buildErrorReport(error,);
				return {
					index,
					args: argv,
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
			requestTimeoutMs: num(flags["request-timeout"],),
			retryMaxAttempts: num(flags["retries"],),
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
			results.push({ index, args: argv, resource, action, ok: null, skipped: true, },);
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
				if (!client) {
					throw new UsageError(
						"Missing Dataiku URL.",
						"missing_required_flag",
						"Set DATAIKU_URL or pass --url.",
						{ requiredFlags: ["url",], env: ["DATAIKU_URL",], },
					);
				}
				result = await meta.handler(client, positional.slice(2,), stepFlags,);
				const failureExitCode = commandFailureExitCode(result,);
				if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
			}
			const stepFieldsFlag = stepFlags["fields"];
			const stepFields = typeof stepFieldsFlag === "string"
				? stepFieldsFlag.split(",",).map((field,) => field.trim()).filter((field,) => field.length > 0)
				: [];
			const stepResult = stepFields.length > 0 ? projectResultFields(result, stepFields,) : result;
			results.push({ index, args: argv, resource, action, ok: true, result: stepResult, },);
		} catch (error) {
			const envelope = buildErrorReport(error,);
			results.push({ index, args: argv, resource, action, ok: false, error: envelope, },);
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
	category: "usage" | "dss" | "internal";
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

function requestIdFromBody(body: string,): string | undefined {
	try {
		const parsed = JSON.parse(body,) as Record<string, unknown>;
		const value = parsed.requestId ?? parsed.request_id ?? parsed.errorId;
		return typeof value === "string" && value.length > 0 ? value : undefined;
	} catch {
		return undefined;
	}
}

const MISSING_PROJECT_KEY_ERROR_PREFIX = "projectKey is required";

function errorExitCode(err: unknown,): number {
	if (err instanceof CommandResultFailure) return err.exitCode;
	if (err instanceof UsageError || err instanceof ClientValidationError) return 1;
	if (err instanceof DataikuError) return err.category === "transient" ? 3 : 2;
	if (err instanceof Error && err.message.startsWith(MISSING_PROJECT_KEY_ERROR_PREFIX,)) return 1;
	return 2;
}

function buildErrorReport(err: unknown,): ErrorReportEnvelope {
	const context = rawCommandContext();
	const exitCode = errorExitCode(err,);
	if (err instanceof UsageError || err instanceof ClientValidationError) {
		return {
			type: "error",
			ok: false,
			error: err.message,
			code: err.code,
			category: "usage",
			exitCode,
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
			code: "long_running_failure",
			category: "dss",
			exitCode: err.exitCode,
			details: { result: err.result, },
			...context,
		};
	}
	if (err instanceof DataikuError) {
		const errorMessage = err.category === "not_found"
				&& err.message.includes("Dataiku instance not found",)
				&& context.resource
			? `Not found: ${context.resource}${context.action ? ` ${context.action}` : ""}`
				+ `${context.projectKey ? ` in project ${context.projectKey}` : ""}`
				+ ` — verify the object identifier and project key (DSS: ${err.message.split("\n",)[0]}).`
			: err.message;
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
				statusText: err.statusText,
				body: err.body,
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
	process.stderr.write(`${JSON.stringify(buildErrorReport(err,),)}\n`,);
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
		if (selected.length > 0) setOutputFieldProjection(selected,);
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
		if (metaCommand.exitCode !== 0) process.exit(metaCommand.exitCode,);
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

	const requestTimeoutMs = num(flags["request-timeout"],);
	const retryMaxAttempts = num(flags["retries"],);

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

	if (typeof flags["record-cleanup"] === "string" && flags["dry-run"] !== true) {
		if (!supportsCleanupLedger(resource, action,)) {
			throw new UsageError(`--record-cleanup is not supported for ${resource} ${action}.`,);
		}
	}
	const result = await actionMeta.handler(client, args, flags,);
	if (typeof flags["record-cleanup"] === "string" && flags["dry-run"] !== true) {
		const entry = cleanupLedgerEntry(resource, action, args, flags, result, projectKey,);
		if (entry) await appendCleanupLedgerEntry(flags["record-cleanup"], entry,);
	}
	const failureExitCode = commandFailureExitCode(result,);
	if (failureExitCode !== undefined) throw new CommandResultFailure(result, failureExitCode,);
	if (flags["raw"] === true && typeof result === "string" && typeof flags["output"] !== "string") {
		process.stdout.write(result,);
	} else {
		writeCommandResult(result,);
	}
}

main().catch((err: unknown,) => {
	writeErrorReport(err,);
	process.exit(errorExitCode(err,),);
},);
