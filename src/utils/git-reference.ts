import { ClientValidationError, } from "../errors.js";

function gitReferencePathError(path: string, message: string,): ClientValidationError {
	return new ClientValidationError(
		message,
		"validation_failed",
		"Use a Git library reference path relative to the project library root: '/' -separated segment names, with no '.' or '..' segments.",
		{ path, },
	);
}

/**
 * Validate and canonicalize a Git library reference path. This is the single
 * validator used by both SDK dispatch and CLI mutation planning.
 *
 * Strip only the permitted outer '/' separators, then require nonempty
 * ordinary segments: no empty segments and no '.' or '..' segments. A hostile
 * path therefore cannot change which API resource a dispatcher's URL
 * normalization would otherwise select.
 */
export function validateGitReferencePath(path: string,): string {
	if (typeof path !== "string" || path.trim() === "") {
		throw gitReferencePathError(path, "A Git library reference path is required.",);
	}
	const normalized = path.replace(/^\/+/, "",).replace(/\/+$/, "",);
	if (normalized === "") {
		throw gitReferencePathError(path, "A Git library reference path is required.",);
	}
	for (const segment of normalized.split("/",)) {
		if (segment === "") {
			throw gitReferencePathError(
				path,
				"A Git library reference path must not contain empty segments (consecutive or repeated '/').",
			);
		}
		if (segment === "." || segment === "..") {
			throw gitReferencePathError(
				path,
				"A Git library reference path must not contain '.' or '..' segments.",
			);
		}
	}
	return normalized;
}

/** Validate a Git library reference path and URL-encode it per segment. */
export function encodeGitReferencePath(path: string,): string {
	return validateGitReferencePath(path,)
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}
