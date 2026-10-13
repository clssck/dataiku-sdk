import { isRecord, } from "./records.js";
import {
	credentialValues,
	normalizeSecretKey,
	sanitizeErrorSecrets,
	sanitizeSecrets,
	SECRET_REDACTED,
} from "./secret-sanitize.js";
import { stableHash, } from "./stable-hash.js";

/**
 * Keys of a messaging-channel configuration that carry credentials or bearer
 * material: SMTP/Graph/SES secrets, Slack/Teams/Google Chat webhooks (the URL
 * itself is the credential), Twilio tokens. Matched after stripping `-`/`_` and
 * lowercasing. The SDK resource layer forwards values verbatim to DSS (over
 * TLS) and returns server responses unmodified; CLI output, plans, previews and
 * errors replace these with `[redacted]`. Login names and sender addresses are
 * identifiers, not secrets, and stay visible.
 */
const SENSITIVE_MESSAGING_CHANNEL_KEYS: Record<string, true> = {
	accesskey: true,
	apikey: true,
	apitoken: true,
	authorization: true,
	authorizationtoken: true,
	authtoken: true,
	clientsecret: true,
	credential: true,
	credentials: true,
	password: true,
	privatekey: true,
	secretkey: true,
	token: true,
	webhookkey: true,
	webhooktoken: true,
	webhookurl: true,
};

/**
 * SMTP session properties are `{key, value, secret}` entries (`mail.smtp.password`, ...).
 * The value is a secret when DSS flags the entry `secret` or the property name says so.
 */
const SECRET_PROPERTY_NAME = /pass|secret|token|auth|credential|key/i;

function isSecretSessionProperty(property: unknown,): property is Record<string, unknown> {
	return isRecord(property,) && typeof property["key"] === "string"
		&& (property["secret"] === true || SECRET_PROPERTY_NAME.test(property["key"],));
}

/** Blank the value of secret session properties, at any depth. */
function redactSessionProperties(value: unknown,): unknown {
	if (Array.isArray(value,)) return value.map(redactSessionProperties,);
	if (!isRecord(value,)) return value;
	return Object.fromEntries(
		Object.entries(value,).map(([name, item,],) => {
			if (normalizeSecretKey(name,) === "sessionproperties" && Array.isArray(item,)) {
				return [
					name,
					item.map((property,) =>
						isSecretSessionProperty(property,) ? { ...property, value: SECRET_REDACTED, } : property
					),
				];
			}
			return [name, redactSessionProperties(item,),];
		},),
	);
}

/**
 * Replace every credential in a channel list item, definition, settings object,
 * creation body, or permission payload with `[redacted]`. `secrets` adds exact
 * values to scrub from free text (error messages echoing the request).
 */
export function sanitizeMessagingChannelSecrets<T,>(value: T, secrets: string[] = [],): T {
	return sanitizeSecrets(redactSessionProperties(value,) as T, {
		sensitiveKeys: SENSITIVE_MESSAGING_CHANNEL_KEYS,
		secrets,
		redactUrlUserinfo: true,
	},);
}

/** Exact secret values of a request body, for scrubbing a DSS error that echoes it. */
export function messagingChannelSecretsFromBody(body: unknown,): string[] {
	const found: string[] = [];
	const visit = (value: unknown,): void => {
		if (Array.isArray(value,)) {
			value.forEach(visit,);
			return;
		}
		if (!isRecord(value,)) return;
		for (const [key, item,] of Object.entries(value,)) {
			if (SENSITIVE_MESSAGING_CHANNEL_KEYS[normalizeSecretKey(key,)] === true) {
				if (typeof item === "string") found.push(item,);
				continue;
			}
			if (normalizeSecretKey(key,) === "sessionproperties" && Array.isArray(item,)) {
				for (const property of item) {
					if (isSecretSessionProperty(property,) && typeof property["value"] === "string") {
						found.push(property["value"],);
					}
				}
				continue;
			}
			visit(item,);
		}
	};
	visit(body,);
	return [...found, ...credentialValues(body,),];
}

/** Scrub an error that may echo a submitted channel configuration before it reaches output. */
export function sanitizeMessagingChannelError(error: unknown, secrets: string[],): unknown {
	return sanitizeErrorSecrets(error, {
		sensitiveKeys: SENSITIVE_MESSAGING_CHANNEL_KEYS,
		secrets,
		redactUrlUserinfo: true,
	},);
}

/**
 * Optimistic-concurrency token for channel settings. It hashes the redacted
 * view, so it can be published next to redacted output without handing out a
 * checksum of the credentials (a checksum would let low-entropy passwords be
 * guessed offline). Credential-only edits therefore do not change it.
 */
export function messagingChannelSettingsHash(settings: unknown,): string {
	return stableHash(sanitizeMessagingChannelSecrets(settings,),);
}
