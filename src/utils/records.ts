/** A non-null, non-array object. */
export function isRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

/** The value as a record, or undefined when it is not one. */
export function asRecord(value: unknown,): Record<string, unknown> | undefined {
	return isRecord(value,) ? value : undefined;
}
