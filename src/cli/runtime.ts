import { type DssCredentials, loadCredentials, } from "../config.js";
import { dataikuEnvironmentEnabled, envFileProvenance, } from "./env.js";
import { enqueueCliWarning, } from "./output.js";
import { UsageError, } from "./usage.js";

export type TlsSettings = Pick<DssCredentials, "tlsRejectUnauthorized" | "caCertPath">;

/**
 * Where a resolved credential or TLS setting originated. `project` marks values
 * supplied by the invocation directory's `.env` working-tree file; they are the
 * only untrusted source and must never select the endpoint for a key from any
 * other source.
 */
export type CredentialSource = "flag" | "env" | "project" | "install" | "saved";

export type CredentialSources = {
	url?: CredentialSource;
	apiKey?: CredentialSource;
	tlsRejectUnauthorized?: CredentialSource;
	caCertPath?: CredentialSource;
};

function envVarSource(name: string,): CredentialSource {
	return envFileProvenance(name,) ?? "env";
}

/**
 * Reject an endpoint (URL/TLS) taken from the project `.env` when the API key
 * would come from anywhere else: project-supplied endpoints must not harvest a
 * user key and transmit it. A project `.env` that supplies the URL and the key
 * together stays supported, as does `DATAIKU_DISABLE_ENV=1`, which stops the
 * loader from recording any project provenance.
 */
export function assertCredentialBinding(sources: CredentialSources,): void {
	if (sources.apiKey === undefined || sources.apiKey === "project") return;
	const projectEndpoint = sources.url === "project"
		|| sources.tlsRejectUnauthorized === "project"
		|| sources.caCertPath === "project";
	if (!projectEndpoint) return;
	throw new UsageError(
		"Refusing to pair credentials: the DSS URL/TLS configuration comes from the project .env, but the API key comes from another source.",
		"conflicting_input_sources",
		"Supply both DATAIKU_URL and DATAIKU_API_KEY in the project .env, pass --url/--api-key explicitly, or set DATAIKU_DISABLE_ENV=1 to ignore the project .env.",
		{
			url: sources.url,
			apiKey: sources.apiKey,
			...(sources.tlsRejectUnauthorized === "project"
				? { tlsRejectUnauthorized: sources.tlsRejectUnauthorized, }
				: {}),
			...(sources.caCertPath === "project"
				? { caCertPath: sources.caCertPath, }
				: {}),
		},
	);
}

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
	sources: Partial<Pick<CredentialSources, "tlsRejectUnauthorized" | "caCertPath">> = {},
): TlsSettings {
	let tlsRejectUnauthorized = flags["insecure"] === true ? false : undefined;
	let caCertPath = flags["ca-cert"] as string | undefined;
	if (flags["insecure"] === true) sources.tlsRejectUnauthorized = "flag";
	if (caCertPath !== undefined) sources.caCertPath = "flag";

	if (tlsRejectUnauthorized === undefined) {
		const envValue = process.env.NODE_TLS_REJECT_UNAUTHORIZED;
		const parsed = parseTlsRejectUnauthorizedEnv(envValue,);
		if (envValue !== undefined && parsed !== undefined) {
			tlsRejectUnauthorized = parsed;
			sources.tlsRejectUnauthorized = envVarSource("NODE_TLS_REJECT_UNAUTHORIZED",);
		}
	}
	if (caCertPath === undefined) {
		const envCaCert = process.env.NODE_EXTRA_CA_CERTS;
		if (envCaCert !== undefined) {
			caCertPath = envCaCert;
			sources.caCertPath = envVarSource("NODE_EXTRA_CA_CERTS",);
		}
	}
	if (tlsRejectUnauthorized === undefined) {
		tlsRejectUnauthorized = saved?.tlsRejectUnauthorized;
		if (tlsRejectUnauthorized !== undefined) sources.tlsRejectUnauthorized = "saved";
	}
	if (caCertPath === undefined) {
		caCertPath = saved?.caCertPath;
		if (caCertPath !== undefined) sources.caCertPath = "saved";
	}

	return { tlsRejectUnauthorized, caCertPath, };
}

const LOOPBACK_IPV4 = /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/;

/**
 * Scheme and authority of a DSS URL, or undefined when it does not parse (the
 * client reports that). `loopback` hosts (`localhost`, `127.0.0.0/8`, `::1`)
 * keep their traffic on the machine, so plain HTTP there exposes nothing.
 * The authority never includes userinfo, path or query.
 */
export function inspectTransport(
	url: string,
): { protocol: string; host: string; loopback: boolean; } | undefined {
	try {
		const { protocol, host, hostname, } = new URL(url,);
		return {
			protocol,
			host,
			loopback: hostname === "localhost" || hostname === "[::1]" || LOOPBACK_IPV4.test(hostname,),
		};
	} catch {
		return undefined;
	}
}

const emittedTransportWarnings = new Set<string>();

function warnTransportOnce(code: string, warning: Record<string, unknown>,): void {
	if (emittedTransportWarnings.has(code,)) return;
	emittedTransportWarnings.add(code,);
	enqueueCliWarning({ code, ...warning, },);
}

/**
 * Tell the caller when the configured transport is unsafe: a key sent in
 * cleartext, or certificate verification switched off. Warns, never refuses —
 * plain-HTTP DSS hosts are legitimate. Names the host only, never userinfo, path
 * or the key.
 */
function warnAboutTransport(
	url: string,
	tlsRejectUnauthorized: boolean | undefined,
	tlsSource: CredentialSource | undefined,
): void {
	const transport = inspectTransport(url,);
	const cleartext = transport?.protocol === "http:";
	if (transport && cleartext && !transport.loopback) {
		warnTransportOnce("insecure_transport", {
			host: transport.host,
			hint: "API key is sent in cleartext; use https.",
		},);
	}
	// Verification is irrelevant without TLS; the cleartext warning covers it.
	if (tlsRejectUnauthorized === false && !cleartext) {
		const source = tlsSource === "flag" || tlsSource === "saved"
			? tlsSource
			: "NODE_TLS_REJECT_UNAUTHORIZED";
		warnTransportOnce("tls_verification_disabled", {
			source,
			hint: source === "saved"
				? "Server identity is not checked; re-enable with `dss auth login` without --insecure."
				: "Server identity is not checked; unset it or use --ca-cert.",
		},);
	}
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
	const sources: CredentialSources = {};
	if (hasUrlFlag) sources.url = "flag";
	if (hasApiKeyFlag) sources.apiKey = "flag";
	const saved = loadCredentials();
	const useEnv = dataikuEnvironmentEnabled();

	if (useEnv) {
		if (!hasUrlFlag && process.env.DATAIKU_URL !== undefined) {
			url = process.env.DATAIKU_URL;
			sources.url = envVarSource("DATAIKU_URL",);
		}
		if (!hasApiKeyFlag && process.env.DATAIKU_API_KEY !== undefined) {
			apiKey = process.env.DATAIKU_API_KEY;
			sources.apiKey = envVarSource("DATAIKU_API_KEY",);
		}
		if (!hasProjectKeyFlag && process.env.DATAIKU_PROJECT_KEY !== undefined) {
			projectKey = process.env.DATAIKU_PROJECT_KEY;
		}
	}

	if (saved) {
		if (!hasUrlFlag && url === undefined) {
			url = saved.url;
			sources.url = "saved";
		}
		if (!hasApiKeyFlag && apiKey === undefined) {
			apiKey = saved.apiKey;
			sources.apiKey = "saved";
		}
		if (!hasProjectKeyFlag && projectKey === undefined) {
			projectKey = saved.projectKey;
		}
	}

	const tlsSettings = resolveTlsSettings(flags, saved ?? undefined, sources,);
	assertCredentialBinding(sources,);
	warnAboutTransport(url ?? "", tlsSettings.tlsRejectUnauthorized, sources.tlsRejectUnauthorized,);

	return {
		url: url ?? "",
		apiKey: apiKey ?? "",
		projectKey,
		...tlsSettings,
	};
}

/**
 * Resolve credentials for `auth login` from explicit flags and the process
 * environment only — never from saved credentials — with provenance tracking
 * and binding enforcement. The call throws before transmitting anything when
 * the project `.env` supplies the URL/TLS configuration while the API key
 * comes from another source.
 *
 * `effectiveTlsSettings` (environment included) is what the validation request
 * uses. `tlsSettings` is what login persists: only values given explicitly as
 * `--insecure` / `--ca-cert`, so one login in a shell with a transient
 * `NODE_TLS_REJECT_UNAUTHORIZED=0` cannot switch verification off for every
 * later command.
 */
export function resolveLoginCredentials(flags: Record<string, string | boolean>,): {
	url: string;
	apiKey: string;
	projectKey?: string;
	tlsSettings: TlsSettings;
	effectiveTlsSettings: TlsSettings;
} {
	const hasUrlFlag = typeof flags["url"] === "string";
	const hasApiKeyFlag = typeof flags["api-key"] === "string";
	const hasProjectKeyFlag = typeof flags["project-key"] === "string";
	let url = hasUrlFlag ? flags["url"] as string | undefined : undefined;
	let apiKey = hasApiKeyFlag ? flags["api-key"] as string | undefined : undefined;
	let projectKey = hasProjectKeyFlag ? flags["project-key"] as string | undefined : undefined;
	const sources: CredentialSources = {};
	if (hasUrlFlag) sources.url = "flag";
	if (hasApiKeyFlag) sources.apiKey = "flag";
	const useEnv = dataikuEnvironmentEnabled();

	if (useEnv) {
		if (!hasUrlFlag && process.env.DATAIKU_URL !== undefined) {
			url = process.env.DATAIKU_URL;
			sources.url = envVarSource("DATAIKU_URL",);
		}
		if (!hasApiKeyFlag && process.env.DATAIKU_API_KEY !== undefined) {
			apiKey = process.env.DATAIKU_API_KEY;
			sources.apiKey = envVarSource("DATAIKU_API_KEY",);
		}
		if (!hasProjectKeyFlag && process.env.DATAIKU_PROJECT_KEY !== undefined) {
			projectKey = process.env.DATAIKU_PROJECT_KEY;
		}
	}

	const effectiveTlsSettings = resolveTlsSettings(flags, undefined, sources,);
	assertCredentialBinding(sources,);
	const tlsSettings: TlsSettings = {
		tlsRejectUnauthorized: sources.tlsRejectUnauthorized === "flag"
			? effectiveTlsSettings.tlsRejectUnauthorized
			: undefined,
		caCertPath: sources.caCertPath === "flag" ? effectiveTlsSettings.caCertPath : undefined,
	};
	return {
		url: url ?? "",
		apiKey: apiKey ?? "",
		projectKey,
		tlsSettings,
		effectiveTlsSettings,
	};
}
