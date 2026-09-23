export function sha256Hex(value: string | Uint8Array,): string {
	return new Bun.CryptoHasher("sha256",).update(value,).digest("hex",);
}

/** Lowercase or uppercase SHA-256 hex digest accepted by every `--expect-hash` guard. */
export const SHA256_HEX_PATTERN = /^[0-9a-fA-F]{64}$/;

/** Locale-independent UTF-16 code-unit order for deterministic machine output. */
export function compareStrings(a: string, b: string,): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Deterministic JSON rendering: object keys are emitted in sorted order so two
 * structurally identical values always produce the same string (and therefore
 * the same hash) regardless of key insertion order. Array order is preserved
 * because it is semantic.
 */
export function stableJson(value: unknown,): string {
	if (value === undefined) return "undefined";
	if (value === null || typeof value !== "object") return JSON.stringify(value,);
	if (Array.isArray(value,)) return `[${value.map((item,) => stableJson(item,)).join(",",)}]`;
	const entries = Object.entries(value as Record<string, unknown>,).sort(([a,], [b,],) =>
		compareStrings(a, b,)
	);
	return `{${
		entries.map(([key, item,],) => `${JSON.stringify(key,)}:${stableJson(item,)}`).join(",",)
	}}`;
}

export function stableHash(value: unknown,): string {
	return sha256Hex(stableJson(value,),);
}
