import {
	closeSync,
	existsSync,
	mkdirSync,
	openSync,
	readFileSync,
	renameSync,
	rmSync,
	writeSync,
} from "node:fs";
import { homedir, } from "node:os";
import { basename, dirname, join, resolve, } from "node:path";

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
	const dir = dirname(path,);
	// Only a directory we create is narrowed; an existing one keeps the owner's choice.
	if (!existsSync(dir,)) mkdirSync(dir, { recursive: true, mode: 0o700, },);
	const data: Record<string, string | boolean> = { url: creds.url, apiKey: creds.apiKey, };
	if (creds.projectKey) data.projectKey = creds.projectKey;
	if (creds.tlsRejectUnauthorized !== undefined) {
		data.tlsRejectUnauthorized = creds.tlsRejectUnauthorized;
	}
	if (creds.caCertPath) data.caCertPath = creds.caCertPath;
	// The key is private from the first byte: an exclusive, owner-only temp file,
	// then a rename that replaces (never writes through) whatever sits at `path`.
	const temporaryPath = join(dir, `.${basename(path,)}.tmp-${crypto.randomUUID()}`,);
	try {
		const fd = openSync(temporaryPath, "wx", 0o600,);
		try {
			writeSync(fd, `${JSON.stringify(data, null, 2,)}\n`,);
		} finally {
			closeSync(fd,);
		}
		renameSync(temporaryPath, path,);
	} finally {
		rmSync(temporaryPath, { force: true, },);
	}
}
