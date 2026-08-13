/**
 * Canonical identity for a DSS base URL stored in local safety artifacts.
 * Formatting differences do not break binding, and embedded URL credentials
 * are never serialized or echoed.
 */
export function canonicalDssUrl(raw: string,): string {
	const trimmed = raw.trim().replace(/\/+$/, "",);
	try {
		const parsed = new URL(trimmed,);
		parsed.username = "";
		parsed.password = "";
		return parsed.toString().replace(/\/+$/, "",);
	} catch {
		return trimmed.replace(/^([a-zA-Z][a-zA-Z0-9+.-]*:\/\/)[^/@]*@/, "$1",);
	}
}
