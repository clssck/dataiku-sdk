const WINDOWS_RESERVED_FILE_NAMES = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])$/i;

/** Sanitize a string for use as a local filename. */
export function sanitizeFileName(name: string, fallback: string,): string {
	const sanitized = name
		.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_",)
		.replace(/[. ]+$/g, "",)
		.trim();
	if (!sanitized) return fallback;
	const dotIndex = sanitized.indexOf(".",);
	const baseName = dotIndex === -1 ? sanitized : sanitized.slice(0, dotIndex,);
	const extension = dotIndex === -1 ? "" : sanitized.slice(dotIndex,);
	if (WINDOWS_RESERVED_FILE_NAMES.test(baseName,)) return `${baseName}_${extension}`;
	return sanitized;
}
