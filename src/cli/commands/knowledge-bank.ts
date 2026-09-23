import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const KB_PLAN_EXIT_CODES = { usage: 1, error: 2, transient: 3, };

const KB_SEARCH_USAGE = commandUsage("knowledge-bank", "search",);
const KB_CLEAR_USAGE = commandUsage("knowledge-bank", "clear",);

/**
 * Validate a search body before any execution mode: the same check runs for
 * live, --plan, and --dry-run so planning can never succeed for a body the
 * live call would reject.
 */
function requireKnowledgeBankSearchBody(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const body = requiredJsonInput(
		flags,
		"--data, --data-file, or --stdin is required (search body with a query string).",
	);
	const query = body["query"];
	if (typeof query !== "string" || query.trim().length === 0) {
		throw new UsageError(
			`query is required and must be a non-empty string.\nUsage: ${KB_SEARCH_USAGE}`,
			"invalid_flag_value",
		);
	}
	return body;
}
function requireNonEmptyPositional(value: string, usage: string,): string {
	if (value.trim().length === 0) {
		throw new UsageError(
			`knowledgeBankId must be a non-empty string.\nUsage: ${usage}`,
			"invalid_flag_value",
		);
	}
	return value;
}

export const knowledgeBankCommands: Record<string, CommandMeta> = withUsage("knowledge-bank", {
	search: {
		handler: async (c, a, f,) => {
			const knowledgeBankId = requireNonEmptyPositional(a[0]!, KB_SEARCH_USAGE,);
			const body = requireKnowledgeBankSearchBody(f,);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return planResult("knowledge-bank", "search", {
					asyncKind: "none",
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(projectKey ?? c.resolveProjectKey(undefined,),)
					}/knowledge-banks/${encodeURIComponent(knowledgeBankId,)}/search`,
					identifiers: { knowledgeBankId, },
					payload: body,
					idempotency: "none",
					exitCodesOnFailure: KB_PLAN_EXIT_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.knowledgeBanks.search(knowledgeBankId, body as never, projectKey,);
		},
		description:
			"Search a knowledge bank for documents matching a query. Runs retrieval against the backing vector store. Body: { query, params?: { maxDocuments, searchType, similarityThreshold, mmrK, mmrDiversity, useAdvancedReranking, rrfRankConstant, rrfRankWindowSize } }.",
		examples: [
			'dss knowledge-bank search my-kb --data \'{"query":"Hello, world!"}\'',
			'dss knowledge-bank search my-kb --data \'{"query":"refund policy","params":{"maxDocuments":5,"searchType":"SIMILARITY"}}\' --dry-run',
		],
	},
	clear: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, KB_CLEAR_USAGE,);
			const knowledgeBankId = requireNonEmptyPositional(a[0]!, KB_CLEAR_USAGE,);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return planResult("knowledge-bank", "clear", {
					asyncKind: "none",
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(projectKey ?? c.resolveProjectKey(undefined,),)
					}/knowledge-banks/${encodeURIComponent(knowledgeBankId,)}/clear`,
					identifiers: { knowledgeBankId, },
					idempotency: "convergent",
					exitCodesOnFailure: KB_PLAN_EXIT_CODES,
					plannedAndDryRun: true,
				},);
			}
			await c.knowledgeBanks.clear(knowledgeBankId, projectKey,);
			return { cleared: knowledgeBankId, resource: "knowledge-bank", };
		},
		description:
			"DESTRUCTIVE: clear all data from a knowledge bank. The stored content is removed and cannot be recovered from this command.",
		examples: [
			"dss knowledge-bank clear my-kb --dry-run",
			"dss knowledge-bank clear my-kb",
		],
	},
},);
