export type DataikuErrorCategory =
	| "not_found"
	| "forbidden"
	| "validation"
	| "transient"
	| "unknown";

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

export function classifyDataikuError(status: number, body: string,): DataikuErrorTaxonomy {
	if (status === 0) {
		return {
			category: "transient",
			retryable: true,
			retryHint: "Network/transport failure. Retry with backoff and verify DSS URL reachability.",
		};
	}

	const lowerBody = body.toLowerCase();
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

	if (status === 404) {
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

	constructor(
		public status: number,
		public statusText: string,
		public body: string,
		retry?: DataikuRetryMetadata,
	) {
		const details = DataikuError.buildDetails(status, statusText, body, retry,);
		super(details.message,);
		this.name = "DataikuError";
		this.category = details.category;
		this.retryable = details.retryable;
		this.retryHint = details.retryHint;
		this.retry = retry;
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
