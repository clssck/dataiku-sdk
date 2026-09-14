function isPlainObject(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

export function deepMerge<T extends object,>(
	base: T,
	patch: Record<string, unknown>,
): T & Record<string, unknown> {
	const out: Record<string, unknown> = { ...base, } as Record<string, unknown>;

	for (const [key, patchValue,] of Object.entries(patch,)) {
		const baseValue = out[key];
		if (isPlainObject(baseValue,) && isPlainObject(patchValue,)) {
			out[key] = deepMerge(baseValue, patchValue,);
			continue;
		}
		out[key] = patchValue;
	}

	return out as T & Record<string, unknown>;
}
