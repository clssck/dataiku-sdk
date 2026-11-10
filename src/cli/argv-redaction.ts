import type * as MessagingChannelSecrets from "../utils/messaging-channel-secrets.js";
import { redactCredentialPairs, redactUrlUserinfo, } from "../utils/secret-sanitize.js";
import type * as ConnectionCommands from "./commands/connection.js";
import type * as UserCommands from "./commands/user.js";
import { FLAG_ALIASES, isFlagValueToken, rawPositionals, } from "./flags.js";
import { commandSyntaxKeys, commandSyntaxTree, type SyntaxNode, } from "./syntax.js";

/** Flags whose whole value is a secret. */
const SENSITIVE_ARGV_VALUE_FLAGS: Record<string, true> = {
	"api-key": true,
	repository: true,
};

type ValueSanitizer = (value: unknown,) => unknown;

/**
 * Domain sanitizers for resources whose JSON payloads carry credentials a
 * key-pair scan cannot see (user `secrets` arrays, connection credential
 * objects, SMTP session properties). Loaded on first use, like the command
 * modules that own them.
 */
const RESOURCE_SECRET_SANITIZERS: Record<string, () => ValueSanitizer> = {
	user: () => (require("./commands/user.js",) as typeof UserCommands).sanitizeUserSecrets,
	connection: () =>
		(require("./commands/connection.js",) as typeof ConnectionCommands).sanitizeConnectionSecrets,
	"messaging-channel": () =>
		(require("../utils/messaging-channel-secrets.js",) as typeof MessagingChannelSecrets)
			.sanitizeMessagingChannelSecrets,
	// Scenario reporters embed messaging-channel configurations.
	scenario: () =>
		(require("../utils/messaging-channel-secrets.js",) as typeof MessagingChannelSecrets)
			.sanitizeMessagingChannelSecrets,
};

/** Usage placeholders that mark an inline JSON value: `JSON`, `'{...}'`, `'{"k":"v"}'`. */
const JSON_VALUE_PLACEHOLDER = /^(?:JSON$|'[[{])/;

let jsonValueFlags: Set<string> | undefined;

function collectJsonValueFlags(nodes: SyntaxNode[], found: Set<string>,): void {
	for (const node of nodes) {
		if (typeof node === "string" || "arg" in node) continue;
		if ("flag" in node) {
			if (node.value !== undefined && JSON_VALUE_PLACEHOLDER.test(node.value,)) {
				found.add(FLAG_ALIASES[node.flag] ?? node.flag,);
			}
			continue;
		}
		for (const alternative of "optional" in node ? node.optional : node.oneOf) {
			collectJsonValueFlags(alternative, found,);
		}
	}
}

/**
 * Flags any command's usage line declares as inline JSON. The union across
 * commands applies to every step, so a step naming an unknown command still
 * cannot echo a credential.
 */
function inlineJsonFlags(): Set<string> {
	if (!jsonValueFlags) {
		jsonValueFlags = new Set();
		for (const key of commandSyntaxKeys()) {
			const [resource = "", action = "",] = key.split(".",);
			collectJsonValueFlags(commandSyntaxTree(resource, action,) ?? [], jsonValueFlags,);
		}
	}
	return jsonValueFlags;
}

/**
 * Redact credentials inside an inline JSON value while keeping the rest
 * readable. A value that is not JSON passes through unchanged.
 */
function redactInlineJson(value: string, resource: string | undefined,): string {
	let parsed: unknown;
	try {
		parsed = JSON.parse(value,);
	} catch {
		return value;
	}
	const sanitizer = resource !== undefined && Object.hasOwn(RESOURCE_SECRET_SANITIZERS, resource,)
		? RESOURCE_SECRET_SANITIZERS[resource]()
		: undefined;
	// Re-serialize only when the domain sanitizer changed something, so clean
	// values keep their original spelling.
	const sanitized = sanitizer ? JSON.stringify(sanitizer(parsed,),) : undefined;
	const text = sanitized !== undefined && sanitized !== JSON.stringify(parsed,) ? sanitized : value;
	return redactCredentialPairs(redactUrlUserinfo(text,),);
}

/** A batch step's argv as echoed in output: secret flag values masked, inline JSON scrubbed. */
export function redactArgv(argv: string[],): string[] {
	const resource = rawPositionals(argv,)[0];
	const redactValue = (flagName: string, value: string,): string =>
		SENSITIVE_ARGV_VALUE_FLAGS[flagName] === true
			? "***"
			: inlineJsonFlags().has(flagName,)
			? redactInlineJson(value, resource,)
			: value;
	const redacted: string[] = [];
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--") {
			redacted.push(...argv.slice(index,),);
			break;
		}
		if (!arg.startsWith("--",)) {
			redacted.push(arg,);
			continue;
		}
		const eqIdx = arg.indexOf("=",);
		const rawFlagName = eqIdx === -1 ? arg.slice(2,) : arg.slice(2, eqIdx,);
		const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
		if (eqIdx !== -1) {
			redacted.push(`${arg.slice(0, eqIdx + 1,)}${redactValue(flagName, arg.slice(eqIdx + 1,),)}`,);
			continue;
		}
		redacted.push(arg,);
		if (SENSITIVE_ARGV_VALUE_FLAGS[flagName] !== true && !inlineJsonFlags().has(flagName,)) continue;
		const next = argv[index + 1];
		if (next !== undefined && isFlagValueToken(next,)) {
			redacted.push(redactValue(flagName, next,),);
			index++;
		}
	}
	return redacted;
}
