import { ClientValidationError, } from "../errors.js";
import { BaseResource, requireNonEmpty, } from "./base.js";

/**
 * Usage purpose of an LLM as documented in the DSS 15 REST reference
 * (`GET /projects/{projectKey}/llms/{?purpose}`). The list is open: DSS
 * versions may add purposes, so unknown server-provided values stay usable.
 */
export type LlmPurpose =
	| "GENERIC_COMPLETION"
	| "TEXT_EMBEDDING_EXTRACTION"
	| "IMAGE_GENERATION"
	| (string & {});

/** One entry of the LLM list endpoint. The rest of the object is server-defined. */
export interface LlmDescriptor extends Record<string, unknown> {
	/** Fully-qualified LLM id, e.g. `openai:openai1:gpt-4`. Contains colons. */
	id?: string;
	/** Backend type, e.g. `OPENAI`, `BEDROCK`, `HUGGINGFACE_TRANSFORMER_LOCAL`. */
	type?: string;
	/** DSS connection backing the LLM, e.g. `openai1`. */
	connection?: string;
	/** Model name as configured on the connection. */
	model?: string;
	/** True when the LLM supports prompt-based interactions. */
	promptDriven?: boolean;
}

/** Per-query settings block documented for the completions endpoint. */
export interface LlmCompletionSettings {
	temperature?: number;
	maxOutputTokens?: number;
	topP?: number;
	[key: string]: unknown;
}

/**
 * One completions query. The documented shape is a chat `messages` array; the
 * full documented request field set (roles, contents) is server-defined, so
 * the object is left open rather than fake-strictly modeled.
 */
export interface LlmCompletionQuery extends Record<string, unknown> {
	messages?: Array<Record<string, unknown>>;
}

/** One embeddings query. `text` is the documented input field. */
export interface LlmEmbeddingQuery extends Record<string, unknown> {
	text?: string;
}

/** One completion result. Fields are the documented stable set plus extras. */
export interface LlmCompletionResponse extends Record<string, unknown> {
	ok?: boolean;
	text?: string;
	promptTokens?: number;
	completionTokens?: number;
	totalTokens?: number;
	/** Cost estimate in cost units, per DSS LLM Mesh accounting. */
	estimatedCost?: number;
}

/** One embedding result. Fields are the documented stable set plus extras. */
export interface LlmEmbeddingResponse extends Record<string, unknown> {
	ok?: boolean;
	/** Embedding vector as returned by the backend. */
	embedding?: number[];
	promptTokens?: number;
	estimatedCost?: number;
}

/** Response envelope of the completions endpoint. */
export interface LlmCompletionsResult extends Record<string, unknown> {
	responses?: LlmCompletionResponse[];
}

/** Response envelope of the embeddings endpoint. */
export interface LlmEmbeddingsResult extends Record<string, unknown> {
	responses?: LlmEmbeddingResponse[];
}

export class LlmsResource extends BaseResource {
	/**
	 * List the LLMs available in a project, including Retrieval-Augmented
	 * Generation ones. Requires READ_CONF.
	 */
	async list(
		options: { purpose?: LlmPurpose; projectKey?: string; } = {},
	): Promise<LlmDescriptor[]> {
		const params = new URLSearchParams();
		if (options.purpose !== undefined) {
			params.set("purpose", requireNonEmpty(options.purpose, "purpose",),);
		}
		const query = params.size > 0 ? `?${params.toString()}` : "";
		return this.client.get<LlmDescriptor[]>(
			`/public/api/projects/${this.enc(options.projectKey,)}/llms/${query}`,
		);
	}

	/**
	 * Perform completions on an LLM. This call is cost-bearing: it invokes an
	 * external LLM provider and may incur charges.
	 */
	async completions(
		body: { llmId: string; queries: LlmCompletionQuery[]; settings?: LlmCompletionSettings; },
		projectKey?: string,
	): Promise<LlmCompletionsResult> {
		const payload = validateLlmInferenceBody(body, "completions",);
		return this.client.post<LlmCompletionsResult>(
			`/public/api/projects/${this.enc(projectKey,)}/llms/completions`,
			payload,
		);
	}

	/**
	 * Perform embeddings on an LLM. This call is cost-bearing: it invokes an
	 * external LLM provider and may incur charges.
	 */
	async embeddings(
		body: { llmId: string; queries: LlmEmbeddingQuery[]; },
		projectKey?: string,
	): Promise<LlmEmbeddingsResult> {
		const payload = validateLlmInferenceBody(body, "embeddings",);
		return this.client.post<LlmEmbeddingsResult>(
			`/public/api/projects/${this.enc(projectKey,)}/llms/embeddings`,
			payload,
		);
	}
}

/**
 * Validate a completions/embeddings request body on the client boundary:
 * `llmId` must be a non-empty string and `queries` a non-empty array of
 * objects. Unknown documented fields pass through untouched.
 */
function validateLlmInferenceBody(
	body: { llmId: string; queries: ReadonlyArray<unknown>; settings?: unknown; },
	action: "completions" | "embeddings",
): Record<string, unknown> {
	if (body === null || typeof body !== "object" || Array.isArray(body,)) {
		throw new ClientValidationError(
			`llms.${action} body must be an object with llmId and queries.`,
		);
	}
	const llmId = requireNonEmpty(body.llmId, "llmId",);
	if (!Array.isArray(body.queries,) || body.queries.length === 0) {
		throw new ClientValidationError(
			`llms.${action} queries must be a non-empty array of query objects.`,
			"validation_failed",
			'Pass at least one query, e.g. queries: [{ messages: [...] }] for completions or [{ text: "..." }] for embeddings.',
		);
	}
	for (const [index, query,] of body.queries.entries()) {
		if (query === null || typeof query !== "object" || Array.isArray(query,)) {
			throw new ClientValidationError(
				`llms.${action} queries[${index}] must be an object.`,
			);
		}
	}
	const payload: Record<string, unknown> = {
		llmId,
		queries: body.queries,
	};
	if (body.settings !== undefined) {
		if (
			body.settings === null || typeof body.settings !== "object" || Array.isArray(body.settings,)
		) {
			throw new ClientValidationError("llms.completions settings must be an object.",);
		}
		payload["settings"] = body.settings;
	}
	return payload;
}
