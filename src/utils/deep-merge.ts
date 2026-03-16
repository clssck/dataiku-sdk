function isPlainObject(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

export function deepMerge<T extends Record<string, unknown>,>(
	base: T,
	patch: Record<string, unknown>,
): T {
	const out: Record<string, unknown> = { ...base, };

	for (const [key, patchValue,] of Object.entries(patch,)) {
		const baseValue = out[key];
		if (isPlainObject(baseValue,) && isPlainObject(patchValue,)) {
			out[key] = deepMerge(baseValue, patchValue,);
			continue;
		}
		out[key] = patchValue;
	}

	return out as T;
}
