import { parseBooleanOption, } from "./coerce.js";
import { unknownFlagError, unsupportedHelpFlag, UsageError, } from "./usage.js";

/**
 * Positional arguments scanned with the same flag grammar as `parseArgs`, so
 * error reporting can recover the invoked resource/action without depending
 * on the command registry being loaded.
 */
export function rawPositionals(argv: string[],): string[] {
	const positionals: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index]!;
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
	return positionals;
}

/** Unknown flags name the invoked command, so the error carries its usage line. */
function commandUnknownFlagError(flagLabel: string, argv: string[],): UsageError {
	const [resource, action,] = rawPositionals(argv,);
	return unknownFlagError(flagLabel, resource, action,);
}

/** Planning wins dispatch; dryRun remains set for combined plan metadata. */
export function executionMode(
	flags: Record<string, string | boolean>,
): { readonly plan: boolean; readonly dryRun: boolean; } {
	return {
		plan: parseBooleanOption(flags["plan"], "--plan",) ?? false,
		dryRun: parseBooleanOption(flags["dry-run"], "--dry-run",) ?? false,
	};
}

export const BOOLEAN_FLAGS = new Set([
	"verbose",
	"version",
	"stdin",
	"insecure",
	"global",
	"list-agents",
	"include-raw",
	"include-payload",
	"no-payload",
	"include-logs",
	"summary",
	"replace",
	"dry-run",
	"plan",
	"apply",
	"capabilities",
	"fast",
	"include-all-partitions",
	"wait",
	"if-not-exists",
	"if-exists",
	"no-wait",
	"force-rebuild",
	"auto-update-schema",
	"latest",
	"full",
	"copy-output-settings",
	"copy-permissions",
	"continue-on-error",
	"no-backup",
	"no-auto-rebuild",
	"all-users",
	"with-scenarios",
	"for-everybody",
	"payload-only",
	"allow-same-path",
	"sync",
	"validate-objects",
	"recreate-on-move",
	"errors-only",
	"keep",
	"full-log",
	"drop-data",
	"normalize",
	"unconfirmed-creation",
	"remote",
	"duplicate-project",
	"delete-remotely",
	"force-delete",
	"i-know-what-i-am-doing",
	"no-add-to-python-path",
	"delete-directory",
	"peek",
	"raw-data",
	"all",
	"connected",
	"conda",
	"force",
	"partitioning-folder",
],);

export const SHORT_FLAGS: Record<string, string> = {
	v: "verbose",
	V: "version",
	o: "output",
};

/** Long-flag aliases: these are normalized to the canonical name in parseArgs. */
export const FLAG_ALIASES: Record<string, string> = {
	project: "project-key",
	dryrun: "dry-run",
	"skip-tls-verify": "insecure",
	"extra-ca-certs": "ca-cert",
	explain: "plan",
	"zone-name": "zone",
	rows: "max-rows",
};

export const VALUE_FLAGS = new Set([
	"fields",
	"owner",
	"topic",
	"reply",
	"configuration",
	"partitions",
	"since",
	"keys",
	"candidates",
	"text",
	"activity",
	"agent",
	"api-key",
	"build-mode",
	"backup-dir",
	"backend-type",
	"backup",
	"branch",
	"ca-cert",
	"catalog",
	"checkout",
	"cell-id",
	"allow-types",
	"color",
	"commit",
	"connection",
	"contains",
	"content",
	"content-type",
	"count",
	"data",
	"active",
	"deployment-mode",
	"env-version",
	"expect-hash",
	"expect-project-incarnation",
	"expect-sha256",
	"expected",
	"expect-version",
	"data-file",
	"database",
	"dataset",
	"file",
	"file-name",
	"file-type",
	"env",
	"evaluate-standards-checks",
	"install-core-packages",
	"fuzzy-on",
	"fuzzy-distance",
	"guess-policy",
	"fuzzy-threshold",
	"folder",
	"input",
	"input-dataset",
	"from",
	"knowledge-bank",
	"labeling-task",
	"lang",
	"join-on",
	"join-type",
	"package",
	"packages",
	"published-service-id",
	"published-project-key",
	"release-notes",
	"scenarios",
	"local",
	"login",
	"manifest-version",
	"max-edges",
	"max-lines",
	"max-log-lines",
	"max-log-bytes",
	"listed",
	"max-nodes",
	"max-rows",
	"limit",
	"max-timestamp",
	"message",
	"only-monitored",
	"min-timestamp",
	"mode",
	"log-filter",
	"log-id",
	"model-evaluation-store",
	"model-name",
	"name",
	"object",
	"metastore-table",
	"output",
	"output-file",
	"output-connection",
	"output-folder",
	"orientation",
	"page",
	"paper-size",
	"partition",
	"parent",
	"path",
	"path-in-repository",
	"preview",
	"render",
	"prediction-type",
	"project-key",
	"query",
	"query-file",
	"recipe",
	"reference",
	"repository",
	"request-timeout",
	"params",
	"password-env",
	"results-per-page",
	"record-cleanup",
	"rule-id",
	"role",
	"retries",
	"future-id",
	"poll-interval",
	"python-interpreter",
	"replace-input",
	"replace-output",
	"replace-payload-text",
	"retain",
	"saved-model",
	"session-name",
	"slide-index",
	"sql",
	"schema",
	"sql-file",
	"start-commit",
	"start-retries",
	"standard",
	"state",
	"streaming-endpoint",
	"target",
	"target-project-folder-id",
	"target-project-key",
	"task-type",
	"test-dataset",
	"train-dataset",
	"target-type",
	"timeout",
	"table",
	"type",
	"url",
	"until",
	"version-notes",
	"to",
	"zone",
	"zone-id",
	"purpose",
	"step-id",
	"max-bytes",
	"type-option-id",
	"format-option-id",
	"copy-partitioning-from",
	"creation-mode",
	"max-dataset-count",
	"logins",
	"max-log-bytes",
	"llm-id",
	"remove-intermediate",
	"code-env",
	"container-exec-config",
	"set-active",
	"binary-classification-threshold",
	"sampling",
	"use-optimal-threshold",
	"skip-expensive-reports",
	"full-class-name",
	"include-libs",
	"archive",
	"stop-at",
	"mark-ok",
	"template-file",
	"group-by",
	"aggregate",
	"order-by",
	"rename",
	"fill-empty",
	"formula",
	"filter",
	"drop-columns",
	"keep-columns",
	"columns",
	"format-option",
	"bundle-id",
	"archive-path",
	"project-folder",
	"classes",
	"version-id",
	"evaluation-dataset",
	"experiments",
	"view-type",
	"format",
	"kind",
	"features",
	"sample-size",
	"random-state",
	"n-jobs",
	"time-variable",
	"timeseries-ids",
	"full-reguess",
],);

export const REPEATABLE_VALUE_FLAGS = new Set([
	"dataset",
	"folder",
	"fuzzy-on",
	"input",
	"join-on",
	"object",
	"package",
	"recipe",
	"replace-input",
	"replace-output",
	"replace-payload-text",
],);

export const KNOWN_LONG_FLAGS = new Set([
	...BOOLEAN_FLAGS,
	...VALUE_FLAGS,
	...Object.keys(FLAG_ALIASES,),
	...Object.values(FLAG_ALIASES,),
],);

export function normalizeLongFlag(rawFlagName: string, argv: string[],): string {
	if (rawFlagName === "help") throw unsupportedHelpFlag();
	const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
	if (!KNOWN_LONG_FLAGS.has(rawFlagName,) && !KNOWN_LONG_FLAGS.has(flagName,)) {
		throw commandUnknownFlagError(`--${rawFlagName}`, argv,);
	}
	return flagName;
}

export function isNegativeNumberToken(value: string,): boolean {
	return value.startsWith("-",) && Number.isFinite(Number(value,),);
}

/** `--name` or `--name=value`: a token that parses as a long flag. */
const LONG_FLAG_SHAPE = /^--[A-Za-z][A-Za-z0-9-]*(?:=|$)/;

/**
 * Whether the token after a value flag is that flag's value rather than the
 * next flag. `-` (stdin), negative numbers, and anything not flag-shaped
 * qualify, so a SQL value beginning with a `-- comment` is a value; `--` alone
 * and `--flag`/`--flag=x` are not.
 */
export function isFlagValueToken(token: string,): boolean {
	if (token === "-" || isNegativeNumberToken(token,)) return true;
	if (!token.startsWith("-",)) return true;
	return token.startsWith("--",) && token !== "--" && !LONG_FLAG_SHAPE.test(token,);
}

export function requireFlagValue(
	flagLabel: string,
	next: string | undefined,
): string {
	if (next === undefined || !isFlagValueToken(next,)) {
		const inlineForm = flagLabel.startsWith("--",) ? `${flagLabel}=VALUE` : undefined;
		const hint = next === undefined || !next.startsWith("-",)
			? undefined
			: flagLabel === "--sql"
			? 'A SQL value that starts with `--` (a comment) can be passed as --sql="-- ...", from a file with --sql-file PATH, or on stdin with --stdin.'
			: inlineForm
			? `If the value itself starts with \`-\`, pass it as ${inlineForm}.`
			: undefined;
		throw new UsageError(
			`Flag ${flagLabel} requires a value.`,
			"missing_required_flag",
			hint,
		);
	}
	return next;
}

export function setParsedFlagValue(
	flags: Record<string, string | boolean>,
	flagName: string,
	value: string,
): void {
	const current = flags[flagName];
	if (REPEATABLE_VALUE_FLAGS.has(flagName,) && typeof current === "string" && current.length > 0) {
		flags[flagName] = `${current},${value}`;
		return;
	}
	flags[flagName] = value;
}

export interface ParsedArgs {
	positional: string[];
	flags: Record<string, string | boolean>;
}

export function parseArgs(argv: string[],): ParsedArgs {
	const positional: string[] = [];
	const flags: Record<string, string | boolean> = {};
	let i = 0;
	while (i < argv.length) {
		const arg = argv[i];
		if (arg === "--") {
			positional.push(...argv.slice(i + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const eqIdx = arg.indexOf("=",);
			if (eqIdx !== -1) {
				const raw = arg.slice(2, eqIdx,);
				const flagName = normalizeLongFlag(raw, argv,);
				const value = arg.slice(eqIdx + 1,);
				if (BOOLEAN_FLAGS.has(flagName,)) {
					flags[flagName] = parseBooleanOption(value, `--${flagName}`,)!;
				} else {
					setParsedFlagValue(flags, flagName, value,);
				}
			} else {
				const rawFlagName = arg.slice(2,);
				const flagName = normalizeLongFlag(rawFlagName, argv,);
				if (BOOLEAN_FLAGS.has(flagName,)) {
					flags[flagName] = true;
				} else {
					const next = requireFlagValue(`--${rawFlagName}`, argv[i + 1],);
					setParsedFlagValue(flags, flagName, next,);
					i++;
				}
			}
		} else if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long) {
				if (BOOLEAN_FLAGS.has(long,)) {
					flags[long] = true;
				} else {
					const next = requireFlagValue(`-${arg[1]}`, argv[i + 1],);
					setParsedFlagValue(flags, long, next,);
					i++;
				}
			} else {
				if (arg[1] === "h") throw unsupportedHelpFlag();
				throw commandUnknownFlagError(`-${arg[1]}`, argv,);
			}
		} else {
			positional.push(arg,);
		}
		i++;
	}
	return { positional, flags, };
}
