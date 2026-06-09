export type DataikuErrorCategory =
	| "not_found"
	| "forbidden"
	| "validation"
	| "transient"
	| "unknown";

export type StableErrorCode =
	| "usage_error"
	| "unknown_flag"
	| "missing_required_arg"
	| "missing_required_flag"
	| "invalid_enum"
	| "not_found"
	| "permission_denied"
	| "validation_failed"
	| "transient"
	| "long_running_failure"
	| "internal_error";

export function dataikuErrorCode(category: DataikuErrorCategory,): StableErrorCode {
	switch (category) {
		case "not_found":
			return "not_found";
		case "forbidden":
			return "permission_denied";
		case "validation":
			return "validation_failed";
		case "transient":
			return "transient";
		case "unknown":
			return "internal_error";
	}
}

export interface DataikuErrorTaxonomy {
	category: DataikuErrorCategory;
	retryable: boolean;
	retryHint: string;
}

export interface DataikuRetryMetadata {
	method: string;
	enabled: boolean;
	maxAttempts: number;
	attempts: number;
	retries: number;
	delaysMs: number[];
	timedOut: boolean;
}

const TLS_CERTIFICATE_HINT =
	"TLS certificate verification failed. Trust the DSS/corporate CA with --ca-cert PATH or NODE_EXTRA_CA_CERTS; use --insecure only for temporary troubleshooting.";
const BUSINESS_APPS_API_UNAVAILABLE_HINT =
	"Business Apps API is not available on this DSS instance. Use classic app commands or check DSS version/feature availability.";

function isCertificateTrustFailure(lowerBody: string,): boolean {
	return lowerBody.includes("unable to verify the first certificate",)
		|| lowerBody.includes("unable to get local issuer certificate",)
		|| lowerBody.includes("unable to verify leaf signature",)
		|| lowerBody.includes("self-signed certificate",)
		|| lowerBody.includes("certificate has expired",)
		|| lowerBody.includes("cert_has_expired",)
		|| lowerBody.includes("self_signed_cert_in_chain",)
		|| lowerBody.includes("unable_to_verify_leaf_signature",)
		|| lowerBody.includes("err_tls_cert_altname_invalid",)
		|| (lowerBody.includes("certificate",) && lowerBody.includes("verify",));
}

function hasDelimitedPath(body: string, path: string,): boolean {
	let index = body.indexOf(path,);
	while (index !== -1) {
		const after = body[index + path.length];
		if (
			after === undefined || after === "\n" || after === "\r" || after === " " || after === "\t"
			|| after === '"' || after === "'" || after === "`" || after === "}" || after === ")"
			|| after === "]"
		) return true;
		index = body.indexOf(path, index + path.length,);
	}
	return false;
}

function isBusinessAppsApiRootNotFound(lowerBody: string,): boolean {
	return hasDelimitedPath(lowerBody, "/public/api/business-apps/",)
		|| hasDelimitedPath(lowerBody, "/dip/publicapi/business-apps/",)
		|| hasDelimitedPath(lowerBody, "/publicapi/business-apps/",);
}

export function classifyDataikuError(status: number, body: string,): DataikuErrorTaxonomy {
	const lowerBody = body.toLowerCase();

	if (status === 0) {
		if (isCertificateTrustFailure(lowerBody,)) {
			return {
				category: "validation",
				retryable: false,
				retryHint: TLS_CERTIFICATE_HINT,
			};
		}

		return {
			category: "transient",
			retryable: true,
			retryHint: "Network/transport failure. Retry with backoff and verify DSS URL reachability.",
		};
	}

	const isSqlInputConnectionMismatch = (status === 400 || status >= 500)
		&& lowerBody.includes("s3 dataset",)
		&& lowerBody.includes("athena connection",);
	if (isSqlInputConnectionMismatch) {
		return {
			category: "validation",
			retryable: false,
			retryHint:
				"SQL recipes require SQL/Athena-backed input datasets. Use a SQL-compatible input, associate the S3 dataset with Athena, or create a Python recipe for file/S3 inputs.",
		};
	}

	const isMissingDatasetRootPath = status === 500
		&& lowerBody.includes("root path of the dataset",)
		&& lowerBody.includes("does not exist",);
	if (isMissingDatasetRootPath) {
		return {
			category: "validation",
			retryable: false,
			retryHint:
				"Dataset files are missing on storage. Build/materialize the dataset or upstream recipes before preview/download.",
		};
	}

	const isServerNotFoundLike = status >= 500
		&& (lowerBody.includes("not found",) || lowerBody.includes("does not exist",))
		&& ["dataset", "recipe", "scenario", "project", "folder",].some((token,) =>
			lowerBody.includes(token,)
		);
	if (isServerNotFoundLike) {
		return {
			category: "not_found",
			retryable: false,
			retryHint:
				"Requested object was not found. Verify projectKey and object identifiers before retrying.",
		};
	}

	const isSqlEngineValidation = status >= 400
		&& (lowerBody.includes("column_not_found",)
			|| lowerBody.includes("table_not_found",)
			|| lowerBody.includes("no_such_table",)
			|| lowerBody.includes("column does not exist",));
	if (isSqlEngineValidation) {
		return {
			category: "validation",
			retryable: false,
			retryHint:
				"Athena/SQL engine rejected the query: check column names, table names, and schema with dss dataset schema or dss connection tables. Do not retry unchanged SQL.",
		};
	}

	const isServerValidationLike = status >= 500
		&& (lowerBody.includes("invalid",)
			|| lowerBody.includes("validation",)
			|| lowerBody.includes("bad request",)
			|| lowerBody.includes("illegal argument",));
	if (isServerValidationLike) {
		return {
			category: "validation",
			retryable: false,
			retryHint: "Request appears invalid for this endpoint. Fix parameters/payload before retrying.",
		};
	}

	const isServerPermissionLike = status >= 500
		&& (lowerBody.includes("not allowed to access",)
			|| lowerBody.includes("access denied",)
			|| (lowerBody.includes("permission",)
				&& (lowerBody.includes("cannot use",) || lowerBody.includes("not allowed",))));
	if (isServerPermissionLike) {
		return {
			category: "forbidden",
			retryable: false,
			retryHint: "Check API key validity and project permissions for the requested action.",
		};
	}

	if (status === 404) {
		if (isBusinessAppsApiRootNotFound(lowerBody,)) {
			return {
				category: "not_found",
				retryable: false,
				retryHint: BUSINESS_APPS_API_UNAVAILABLE_HINT,
			};
		}

		const isHtmlGatewayResponse = lowerBody.includes("<!doctype html>",);
		return {
			category: "not_found",
			retryable: false,
			retryHint: isHtmlGatewayResponse
				? "Resource was not found (gateway returned HTML). Verify DSS URL, projectKey, and object identifiers."
				: "Verify projectKey and object identifiers (dataset/recipe/scenario/folder IDs).",
		};
	}

	if (status === 401 || status === 403) {
		return {
			category: "forbidden",
			retryable: false,
			retryHint: "Check API key validity and project permissions for the requested action.",
		};
	}

	if (status === 400 || status === 409 || status === 422) {
		return {
			category: "validation",
			retryable: false,
			retryHint: "Fix request parameters/payload and try again (same request will likely fail).",
		};
	}

	if (status === 408 || status === 425 || status === 429 || (status >= 500 && status <= 599)) {
		return {
			category: "transient",
			retryable: true,
			retryHint:
				"Retry with exponential backoff. If it persists, check DSS availability and upstream proxies.",
		};
	}

	return {
		category: "unknown",
		retryable: false,
		retryHint: "Inspect the response details and DSS logs to determine whether retry is appropriate.",
	};
}

export class DataikuError extends Error {
	public category: DataikuErrorCategory;
	public retryable: boolean;
	public retryHint: string;
	public retry?: DataikuRetryMetadata;
	public requestId?: string;

	constructor(
		public status: number,
		public statusText: string,
		public body: string,
		retry?: DataikuRetryMetadata,
		requestId?: string,
	) {
		const details = DataikuError.buildDetails(status, statusText, body, retry,);
		super(details.message,);
		this.name = "DataikuError";
		this.category = details.category;
		this.retryable = details.retryable;
		this.retryHint = details.retryHint;
		this.retry = retry;
		this.requestId = requestId;
	}

	private static extractSummary(_status: number, _statusText: string, body: string,): string {
		try {
			const parsed = JSON.parse(body,);
			if (parsed.message) return String(parsed.message,);
		} catch {
			// not JSON — use raw body
		}
		if (!body) return "(empty response body)";
		return body.length > 200 ? `${body.slice(0, 200,)}…` : body;
	}

	private static formatRetryMetadata(retry?: DataikuRetryMetadata,): string | undefined {
		if (!retry) return undefined;
		const shownDelays = retry.delaysMs.slice(0, 10,);
		const delaysSuffix = retry.delaysMs.length > shownDelays.length ? ", …" : "";
		const delaysPart = shownDelays.length > 0 ? `[${shownDelays.join(", ",)}${delaysSuffix}]` : "[]";
		return [
			`Retry attempts: ${retry.attempts}/${retry.maxAttempts}`,
			`Retry policy: ${retry.enabled ? "enabled" : "disabled"} for ${retry.method}`,
			`Retries performed: ${retry.retries}`,
			`Backoff delays (ms): ${delaysPart}`,
			`Timed out: ${retry.timedOut ? "yes" : "no"}`,
		].join(" | ",);
	}

	private static buildDetails(
		status: number,
		statusText: string,
		body: string,
		retry?: DataikuRetryMetadata,
	): { message: string; } & DataikuErrorTaxonomy {
		const summary = DataikuError.extractSummary(status, statusText, body,);
		const taxonomy = classifyDataikuError(status, body,);
		const retrySummary = DataikuError.formatRetryMetadata(retry,);
		return {
			...taxonomy,
			message: [
				`${status} ${statusText}: ${summary}`,
				`Error type: ${taxonomy.category}`,
				`Retryable: ${taxonomy.retryable ? "yes" : "no"}`,
				`Hint: ${taxonomy.retryHint}`,
				...(retrySummary ? [retrySummary,] : []),
			].join("\n",),
		};
	}
}
