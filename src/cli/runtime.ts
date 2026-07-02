import { type DssCredentials, loadCredentials, } from "../config.js";
import { dataikuEnvironmentEnabled, } from "./env.js";

export type TlsSettings = Pick<DssCredentials, "tlsRejectUnauthorized" | "caCertPath">;

function parseTlsRejectUnauthorizedEnv(value: string | undefined,): boolean | undefined {
	if (value === undefined) return undefined;
	const normalized = value.trim().toLowerCase();
	if (normalized === "0" || normalized === "false" || normalized === "no") return false;
	if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
	return undefined;
}

export function resolveTlsSettings(
	flags: Record<string, string | boolean>,
	saved?: TlsSettings,
): TlsSettings {
	let tlsRejectUnauthorized = flags["insecure"] === true ? false : undefined;
	let caCertPath = flags["ca-cert"] as string | undefined;

	tlsRejectUnauthorized ??= parseTlsRejectUnauthorizedEnv(process.env.NODE_TLS_REJECT_UNAUTHORIZED,);
	caCertPath ??= process.env.NODE_EXTRA_CA_CERTS;

	if (tlsRejectUnauthorized === undefined) {
		tlsRejectUnauthorized = saved?.tlsRejectUnauthorized;
	}
	caCertPath ??= saved?.caCertPath;

	return { tlsRejectUnauthorized, caCertPath, };
}

export const currentCommandContext: { resource?: string; action?: string; projectKey?: string; } =
	{};

// ---------------------------------------------------------------------------
// Credential resolution
// ---------------------------------------------------------------------------

export function resolveCredentials(flags: Record<string, string | boolean>,): {
	url: string;
	apiKey: string;
	projectKey?: string;
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
} {
	const hasUrlFlag = Object.hasOwn(flags, "url",);
	const hasApiKeyFlag = Object.hasOwn(flags, "api-key",);
	const hasProjectKeyFlag = Object.hasOwn(flags, "project-key",);
	let url = hasUrlFlag ? flags["url"] as string | undefined : undefined;
	let apiKey = hasApiKeyFlag ? flags["api-key"] as string | undefined : undefined;
	let projectKey = hasProjectKeyFlag ? flags["project-key"] as string | undefined : undefined;
	const saved = loadCredentials();
	const useEnv = dataikuEnvironmentEnabled();

	if (useEnv) {
		if (!hasUrlFlag) url ??= process.env.DATAIKU_URL;
		if (!hasApiKeyFlag) apiKey ??= process.env.DATAIKU_API_KEY;
		if (!hasProjectKeyFlag) projectKey ??= process.env.DATAIKU_PROJECT_KEY;
	}

	if (saved) {
		if (!hasUrlFlag) url ??= saved.url;
		if (!hasApiKeyFlag) apiKey ??= saved.apiKey;
		if (!hasProjectKeyFlag) projectKey ??= saved.projectKey;
	}

	return {
		url: url ?? "",
		apiKey: apiKey ?? "",
		projectKey,
		...resolveTlsSettings(flags, saved ?? undefined,),
	};
}
