import { DataikuClient, } from "./client.js";
import { DataikuError, } from "./errors.js";

export interface CredentialValidationResult {
	valid: boolean;
	error?: string;
	dataikuError?: DataikuError;
}

export interface CredentialValidationOptions {
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
}

export async function validateCredentials(
	url: string,
	apiKey: string,
	options: CredentialValidationOptions = {},
): Promise<CredentialValidationResult> {
	try {
		const client = new DataikuClient({
			url,
			apiKey,
			requestTimeoutMs: 10_000,
			retryMaxAttempts: 1,
			tlsRejectUnauthorized: options.tlsRejectUnauthorized,
			caCertPath: options.caCertPath,
		},);
		await client.projects.list();
		return { valid: true, };
	} catch (err: unknown) {
		if (err instanceof DataikuError) {
			return { valid: false, error: err.message, dataikuError: err, };
		}
		return { valid: false, error: err instanceof Error ? err.message : String(err,), };
	}
}
