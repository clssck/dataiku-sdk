import type { DataikuClient, } from "../client.js";
import { ClientValidationError, unexpectedResponseError, } from "../errors.js";

/** Never disguise an unexpected successful HTTP payload as an empty listing. */
export function requireArrayResponse<T,>(value: unknown, operation: string,): T[] {
	if (!Array.isArray(value,)) {
		throw unexpectedResponseError(`${operation}: DSS did not return an array.`,);
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

export function requireObject(value: unknown, name: string,): Record<string, unknown> {
	if (!value || typeof value !== "object" || Array.isArray(value,)) {
		throw new ClientValidationError(`${name} must be a JSON object.`, "validation_failed",);
	}
	return value as Record<string, unknown>;
}

export function requireNonEmptyArray(value: string[], name: string,): string[] {
	if (!Array.isArray(value,) || value.length === 0) {
		throw new ClientValidationError(`${name} must be a non-empty array.`, "validation_failed",);
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
