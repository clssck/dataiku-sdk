import { DataikuClient, } from "./client.js";
import { DataikuError, } from "./errors.js";

export interface CredentialValidationResult {
	valid: boolean;
	error?: string;
	dataikuError?: DataikuError;
}

export async function validateCredentials(
	url: string,
	apiKey: string,
): Promise<CredentialValidationResult> {
	try {
		const client = new DataikuClient({
			url,
			apiKey,
			requestTimeoutMs: 10_000,
			retryMaxAttempts: 1,
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
