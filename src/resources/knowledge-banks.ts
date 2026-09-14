import { ClientValidationError, } from "../errors.js";
import { BaseResource, requireNonEmpty, } from "./base.js";

/** Search algorithm for knowledge-bank search, per the DSS 15 REST schema. */
export type KnowledgeBankSearchType =
	| "SIMILARITY"
	| "SIMILARITY_THRESHOLD"
	| "MMR"
	| "HYBRID"
	| (string & {});

/**
 * Optional search parameters, documented in the DSS 15 REST reference for
 * `POST /projects/{projectKey}/knowledge-banks/{knowledgeBankId}/search`.
 * Which knobs apply depends on the backing vector store.
 */
export interface KnowledgeBankSearchParams {
	/** Maximum number of documents to return (default 10). */
	maxDocuments?: number;
	/** Search algorithm. Defaults to SIMILARITY. */
	searchType?: KnowledgeBankSearchType;
	/** Similarity threshold; only applied for SIMILARITY_THRESHOLD. */
	similarityThreshold?: number;
	/** Documents considered before selection; only applied for MMR. */
	mmrK?: number;
	/** 0..1 balance of diversity (0) vs relevancy (1); only applied for MMR. */
	mmrDiversity?: number;
	/** Proprietary rerankers; only valid for Azure AI and ElasticSearch stores. */
	useAdvancedReranking?: boolean;
	/** Weight of lower-ranked documents; only valid for ElasticSearch stores. */
	rrfRankConstant?: number;
	/** Documents considered per search type; only valid for ElasticSearch stores. */
	rrfRankWindowSize?: number;
	[key: string]: unknown;
}

/** Request body for knowledge-bank search. */
export interface KnowledgeBankSearchBody {
	query: string;
	params?: KnowledgeBankSearchParams;
}

/** One retrieved knowledge-bank document. Metadata is server-defined. */
export interface KnowledgeBankDocument extends Record<string, unknown> {
	/** Document text content. */
	text?: string;
	/** Arbitrary server-defined metadata map. */
	metadata?: Record<string, unknown>;
	/** Relevance score in [0, 1] as returned by the backend. */
	score?: number;
}

/** Response envelope of the knowledge-bank search endpoint. */
export interface KnowledgeBankSearchResult extends Record<string, unknown> {
	documents?: KnowledgeBankDocument[];
}

export class KnowledgeBanksResource extends BaseResource {
	/**
	 * Search a knowledge bank for documents matching a query string.
	 * Requires READ_CONF.
	 */
	async search(
		knowledgeBankId: string,
		body: KnowledgeBankSearchBody,
		projectKey?: string,
	): Promise<KnowledgeBankSearchResult> {
		const id = encodeURIComponent(requireNonEmpty(knowledgeBankId, "knowledgeBankId",),);
		const payload = validateKnowledgeBankSearchBody(body,);
		return this.client.post<KnowledgeBankSearchResult>(
			`/public/api/projects/${this.enc(projectKey,)}/knowledge-banks/${id}/search`,
			payload,
		);
	}

	/**
	 * Clear all data from a knowledge bank. DESTRUCTIVE: the stored knowledge
	 * bank content is removed and cannot be recovered from this call. Requires
	 * WRITE_CONF.
	 */
	async clear(knowledgeBankId: string, projectKey?: string,): Promise<void> {
		const id = encodeURIComponent(requireNonEmpty(knowledgeBankId, "knowledgeBankId",),);
		await this.client.post(
			`/public/api/projects/${this.enc(projectKey,)}/knowledge-banks/${id}/clear`,
			{},
		);
	}
}

/**
 * Validate a knowledge-bank search body on the client boundary: `query` must
 * be a non-empty string and `params`, when present, an object. Documented
 * params are checked for type; unknown fields pass through untouched.
 */
function validateKnowledgeBankSearchBody(body: KnowledgeBankSearchBody,): Record<string, unknown> {
	if (body === null || typeof body !== "object" || Array.isArray(body,)) {
		throw new ClientValidationError(
			"knowledgeBanks.search body must be an object with a query string.",
		);
	}
	const query = requireNonEmpty(body.query, "query",);
	const payload: Record<string, unknown> = { query, };
	const params = body.params;
	if (params !== undefined) {
		if (params === null || typeof params !== "object" || Array.isArray(params,)) {
			throw new ClientValidationError("knowledgeBanks.search params must be an object.",);
		}
		const searchParams: Record<string, unknown> = { ...params, };
		validateSearchParamNumber(searchParams, "maxDocuments",);
		validateSearchParamNumber(searchParams, "similarityThreshold",);
		validateSearchParamNumber(searchParams, "mmrK",);
		validateSearchParamNumber(searchParams, "mmrDiversity",);
		validateSearchParamNumber(searchParams, "rrfRankConstant",);
		validateSearchParamNumber(searchParams, "rrfRankWindowSize",);
		const reranking = searchParams["useAdvancedReranking"];
		if (reranking !== undefined && typeof reranking !== "boolean") {
			throw new ClientValidationError(
				"knowledgeBanks.search params.useAdvancedReranking must be a boolean.",
			);
		}
		payload["params"] = searchParams;
	}
	return payload;
}

function validateSearchParamNumber(
	params: Record<string, unknown>,
	name: string,
): void {
	const value = params[name];
	if (value === undefined) return;
	if (typeof value !== "number" || !Number.isFinite(value,)) {
		throw new ClientValidationError(
			`knowledgeBanks.search params.${name} must be a finite number.`,
		);
	}
}
