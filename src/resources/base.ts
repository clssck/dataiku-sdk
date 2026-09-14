import type { DataikuClient, } from "../client.js";
import { ClientValidationError, DataikuError, } from "../errors.js";

/** Never disguise an unexpected successful HTTP payload as an empty listing. */
export function requireArrayResponse<T,>(value: unknown, operation: string,): T[] {
	if (!Array.isArray(value,)) {
		throw new DataikuError(200, "Unexpected Response", `${operation}: DSS did not return an array.`,);
	}
	return value as T[];
}

/** Validate identifiers without changing significant whitespace or encoding. */
export function requireNonEmpty(value: unknown, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new ClientValidationError(`${name} must be a non-empty string.`, "validation_failed",);
	}
	return value;
}

export abstract class BaseResource {
	constructor(protected readonly client: DataikuClient,) {}

	protected resolveProjectKey(pk?: string,): string {
		return this.client.resolveProjectKey(pk,);
	}

	protected enc(pk?: string,): string {
		return encodeURIComponent(this.resolveProjectKey(pk,),);
	}
}
