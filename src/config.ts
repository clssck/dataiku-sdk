import { chmodSync, mkdirSync, readFileSync, unlinkSync, writeFileSync, } from "node:fs";
import { homedir, } from "node:os";
import { dirname, join, resolve, } from "node:path";

export interface DssCredentials {
	url: string;
	apiKey: string;
	projectKey?: string;
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
}

export function getConfigDir(): string {
	if (process.env.DSS_CONFIG_DIR) return process.env.DSS_CONFIG_DIR;
	if (process.env.XDG_CONFIG_HOME) return resolve(process.env.XDG_CONFIG_HOME, "dataiku",);
	if (process.platform === "win32" && process.env.APPDATA) {
		return resolve(process.env.APPDATA, "dataiku",);
	}
	return resolve(homedir(), ".config", "dataiku",);
}

export function getCredentialsPath(): string {
	return join(getConfigDir(), "credentials.json",);
}

export function loadCredentials(): DssCredentials | null {
	try {
		const raw = readFileSync(getCredentialsPath(), "utf-8",);
		const parsed: unknown = JSON.parse(raw,);
		if (
			!parsed
			|| typeof parsed !== "object"
			|| Array.isArray(parsed,)
			|| typeof (parsed as Record<string, unknown>).url !== "string"
			|| typeof (parsed as Record<string, unknown>).apiKey !== "string"
		) {
			return null;
		}
		const obj = parsed as Record<string, unknown>;
		return {
			url: obj.url as string,
			apiKey: obj.apiKey as string,
			projectKey: typeof obj.projectKey === "string" ? obj.projectKey : undefined,
			tlsRejectUnauthorized: typeof obj.tlsRejectUnauthorized === "boolean"
				? obj.tlsRejectUnauthorized
				: undefined,
			caCertPath: typeof obj.caCertPath === "string" ? obj.caCertPath : undefined,
		};
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
		throw err;
	}
}

export function saveCredentials(creds: DssCredentials,): void {
	const path = getCredentialsPath();
	mkdirSync(dirname(path,), { recursive: true, },);
	const data: Record<string, string | boolean> = { url: creds.url, apiKey: creds.apiKey, };
	if (creds.projectKey) data.projectKey = creds.projectKey;
	if (creds.tlsRejectUnauthorized !== undefined) {
		data.tlsRejectUnauthorized = creds.tlsRejectUnauthorized;
	}
	if (creds.caCertPath) data.caCertPath = creds.caCertPath;
	writeFileSync(path, `${JSON.stringify(data, null, 2,)}\n`, "utf-8",);
	chmodSync(path, 0o600,);
}

export function deleteCredentials(): void {
	try {
		unlinkSync(getCredentialsPath(),);
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
		throw err;
	}
}

export function maskApiKey(apiKey: string,): string {
	if (apiKey.length <= 12) return "***";
	return `${apiKey.slice(0, 6,)}...${apiKey.slice(-6,)}`;
}
