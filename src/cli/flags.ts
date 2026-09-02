import { unsupportedHelpFlag, UsageError, } from "./usage.js";

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
	"latest",
	"copy-output-settings",
	"copy-permissions",
	"continue-on-error",
	"no-backup",
	"payload-only",
	"allow-same-path",
	"sync",
	"validate-objects",
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
	"all",
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
	"text",
	"partitions",
	"since",
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

export function normalizeLongFlag(rawFlagName: string,): string {
	if (rawFlagName === "help") throw unsupportedHelpFlag();
	const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
	if (!KNOWN_LONG_FLAGS.has(rawFlagName,) && !KNOWN_LONG_FLAGS.has(flagName,)) {
		throw new UsageError(`Unknown flag: --${rawFlagName}`, "unknown_flag",);
	}
	return flagName;
}

export function isNegativeNumberToken(value: string,): boolean {
	return value.startsWith("-",) && Number.isFinite(Number(value,),);
}

export function requireFlagValue(
	flagLabel: string,
	next: string | undefined,
): string {
	if (
		next === undefined || (next !== "-" && next.startsWith("-",) && !isNegativeNumberToken(next,))
	) {
		throw new UsageError(`Flag ${flagLabel} requires a value.`, "missing_required_flag",);
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
				const flagName = normalizeLongFlag(raw,);
				setParsedFlagValue(flags, flagName, arg.slice(eqIdx + 1,),);
			} else {
				const rawFlagName = arg.slice(2,);
				const flagName = normalizeLongFlag(rawFlagName,);
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
				throw new UsageError(`Unknown flag: -${arg[1]}`, "unknown_flag",);
			}
		} else {
			positional.push(arg,);
		}
		i++;
	}
	return { positional, flags, };
}
