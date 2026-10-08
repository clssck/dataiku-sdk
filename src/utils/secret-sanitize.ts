/**
 * Shared recursive secret sanitizer for CLI output and error surfaces.
 *
 * Walks any JSON-like value and:
 * - replaces values under sensitive keys (per-domain key table) with `[redacted]`;
 * - replaces exact occurrences of known secret values anywhere in strings;
 * - optionally redacts userinfo embedded in HTTP(S) URL authorities
 *   (`https://user:pass@host/...` -> `https://[redacted]@host/...`).
 *
 * The SDK resource layer forwards values verbatim to DSS over TLS; sanitizing
 * structured output is a CLI presentation concern and must never leak secret
 * material instead. DSS-produced read text that consumers never need secrets
 * from (job logs) is scrubbed at the SDK download via `redactCredentialPairs`.
 */

/** Marker replacing every redacted value or secret occurrence. */
export const SECRET_REDACTED = "[redacted]";

/** Sensitive keys are matched after stripping `-`/`_` and lowercasing. */
export function normalizeSecretKey(key: string,): string {
	return key.replace(/[-_]/g, "",).toLowerCase();
}

export interface SecretSanitizeOptions {
	/**
	 * Per-domain table of sensitive keys (normalized form -> true). Policies
	 * deliberately differ between domains; never merge them.
	 */
	sensitiveKeys: Record<string, true>;
	/**
	 * Optional extra predicate over normalized keys for key families a flat
	 * table cannot express (e.g. the user `secrets` array).
	 */
	isSensitiveKey?: (normalizedKey: string,) => boolean;
	/** Exact secret values to replace everywhere in strings. */
	secrets?: string[];
	/** Also redact userinfo embedded in HTTP(S) URL authorities. */
	redactUrlUserinfo?: boolean;
}

/**
 * Replace every exact occurrence of each secret so echoed errors cannot leak
 * it. Empty secrets are skipped (an empty needle would corrupt every string).
 */
export function replaceSecrets(text: string, secrets: readonly string[],): string {
	let result = text;
	for (const secret of secrets) {
		if (secret !== "" && secret !== undefined && secret !== null) {
			result = result.split(secret,).join(SECRET_REDACTED,);
		}
	}
	return result;
}

/**
 * Redact the userinfo of every HTTP(S) URL in a string. DSS may contain legacy
 * remotes with credentials in the URL, and Git error text can echo them.
 */
export function redactUrlUserinfo(text: string,): string {
	return text.replace(/https?:[\\/]{1,2}[^\s"'<>]+/giu, (url,) => {
		const schemeEnd = url.indexOf(":",) + 1;
		let authorityStart = schemeEnd;
		while (url[authorityStart] === "/" || url[authorityStart] === "\\") authorityStart++;
		const slash = url.slice(authorityStart,).search(/[\\/]/u,);
		const authorityEnd = slash === -1 ? url.length : authorityStart + slash;
		const at = url.lastIndexOf("@", authorityEnd - 1,);
		if (at < authorityStart) return url;
		return `${url.slice(0, authorityStart,)}${SECRET_REDACTED}@${url.slice(at + 1,)}`;
	},);
}

/**
 * Recursively sanitize `value`: strings lose exact secret occurrences (and
 * URL userinfo when enabled), objects redact values under sensitive keys and
 * recurse, arrays recurse. Non-string scalars pass through unchanged.
 */
export function sanitizeSecrets<T,>(value: T, options: SecretSanitizeOptions,): T {
	const { secrets = [], } = options;
	const stringPass = (text: string,): string => {
		const sanitized = options.redactUrlUserinfo ? redactUrlUserinfo(text,) : text;
		return replaceSecrets(sanitized, secrets,);
	};
	if (typeof value === "string") {
		return stringPass(value,) as T;
	}
	if (Array.isArray(value,)) {
		return value.map((item,) => sanitizeSecrets(item, options,)) as T;
	}
	if (value !== null && typeof value === "object") {
		const sanitized: Record<string, unknown> = {};
		for (const [key, item,] of Object.entries(value,)) {
			const normalizedKey = normalizeSecretKey(key,);
			sanitized[key] = options.sensitiveKeys[normalizedKey] === true
					|| options.isSensitiveKey?.(normalizedKey,) === true
				? SECRET_REDACTED
				: sanitizeSecrets(item, options,);
		}
		return sanitized as T;
	}
	return value;
}

/** Credential-like key fragments (normalized); values under them are secrets. */
const CREDENTIAL_KEY_FRAGMENTS = [
	"password",
	"passwd",
	"passphrase",
	"secret",
	"token",
	"apikey",
	"accesskey",
	"privatekey",
	"credential",
	"authorization",
] as const;

/** Keys naming how credentials work (`credentialsMode`, `passwordType`), not secrets. */
const CREDENTIAL_SETTING_SUFFIXES = ["mode", "type", "policy",] as const;

/** Credential-like key (normalized fragment match, minus credential settings). */
function isCredentialKey(key: string,): boolean {
	const normalized = normalizeSecretKey(key,);
	if (CREDENTIAL_SETTING_SUFFIXES.some((suffix,) => normalized.endsWith(suffix,))) return false;
	return CREDENTIAL_KEY_FRAGMENTS.some((fragment,) => normalized.includes(fragment,));
}

/** JSON-style `"key": "value"` pair with escape-aware string bodies. */
const JSON_STRING_PAIR = /"((?:[^"\\\n]|\\.)*)"(\s*:\s*)"((?:[^"\\\n]|\\.)*)"/g;

/**
 * Redact the string value of every JSON-style `"key":"value"` pair in free
 * text whose key is credential-like (e.g. the `jobTicketSecret` DSS writes
 * into job logs). Bare `key` fields are not redacted here: in log text they
 * name ordinary identifiers far more often than secrets.
 */
export function redactCredentialPairs(text: string,): string {
	return text.replace(
		JSON_STRING_PAIR,
		(pair, key: string, separator: string, value: string,) =>
			value.length > 0 && isCredentialKey(key,) ? `"${key}"${separator}"${SECRET_REDACTED}"` : pair,
	);
}

/**
 * Every string value under a credential-like key (any depth), plus bare
 * `key` fields such as `authRealm.key`. Collected from a command's own JSON
 * input so that DSS error text echoing the payload can be scrubbed; error
 * redaction only, so the broad key match errs toward hiding.
 */
export function credentialValues(value: unknown,): string[] {
	const found: string[] = [];
	const visit = (node: unknown, sensitive: boolean,): void => {
		if (typeof node === "string") {
			if (sensitive && node.length > 0) found.push(node,);
			return;
		}
		if (Array.isArray(node,)) {
			for (const item of node) visit(item, sensitive,);
			return;
		}
		if (node === null || typeof node !== "object") return;
		for (const [key, item,] of Object.entries(node,)) {
			visit(item, sensitive || isCredentialKey(key,) || normalizeSecretKey(key,) === "key",);
		}
	};
	visit(value, false,);
	return found;
}

/**
 * Scrub secret material from an error's message, stack, and string body.
 * `DataikuError.message` is built at construction, so all three carriers need
 * the same string pass as output values.
 */
export function sanitizeErrorSecrets(
	error: unknown,
	options: SecretSanitizeOptions,
): unknown {
	if (!(error instanceof Error)) return error;
	const stringPass = (text: string,): string => {
		const sanitized = options.redactUrlUserinfo ? redactUrlUserinfo(text,) : text;
		return replaceSecrets(sanitized, options.secrets ?? [],);
	};
	error.message = stringPass(error.message,);
	if (typeof error.stack === "string") {
		error.stack = stringPass(error.stack,);
	}
	const errorWithBody = error as Error & { body?: unknown; };
	if (typeof errorWithBody.body === "string") {
		errorWithBody.body = stringPass(errorWithBody.body,);
	}
	return error;
}
