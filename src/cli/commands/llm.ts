import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { planResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

const LLM_PLAN_EXIT_CODES = { usage: 1, error: 2, transient: 3, };

const LLM_LIST_USAGE =
	"dss llm list [--purpose GENERIC_COMPLETION|TEXT_EMBEDDING_EXTRACTION|IMAGE_GENERATION|NAME] [--project-key KEY]";
const LLM_COMPLETIONS_USAGE =
	"dss llm completions (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]";
const LLM_EMBEDDINGS_USAGE =
	"dss llm embeddings (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]";

/**
 * Validate a completions/embeddings payload before any execution mode: the
 * same check runs for live, --plan, and --dry-run so planning can never
 * succeed for a body the live call would reject.
 */
function requireLlmInferenceBody(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	const body = requiredJsonInput(
		flags,
		"--data, --data-file, or --stdin is required (LLM request body with llmId and queries).",
	);
	const llmId = body["llmId"];
	if (typeof llmId !== "string" || llmId.trim().length === 0) {
		throw new UsageError(
			`llmId is required and must be a non-empty string (e.g. "openai:openai1:gpt-4").\nUsage: ${usage}`,
			"invalid_flag_value",
		);
	}
	const queries = body["queries"];
	if (!Array.isArray(queries,) || queries.length === 0) {
		throw new UsageError(
			`queries is required and must be a non-empty array of query objects.\nUsage: ${usage}`,
			"invalid_flag_value",
		);
	}
	return body;
}

export const llmCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => {
			const purpose = f["purpose"] as string | undefined;
			return c.llms.list({
				purpose,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: LLM_LIST_USAGE,
		description:
			"List LLMs available in a project (includes Retrieval-Augmented Generation ones). Does not invoke any LLM.",
		examples: ["dss llm list", "dss llm list --purpose TEXT_EMBEDDING_EXTRACTION",],
	},
	completions: {
		handler: async (c, _a, f,) => {
			const body = requireLlmInferenceBody(f, LLM_COMPLETIONS_USAGE,);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return planResult("llm", "completions", {
					asyncKind: "none",
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(projectKey ?? c.resolveProjectKey(undefined,),)
					}/llms/completions`,
					identifiers: {
						llmId: typeof body["llmId"] === "string" ? body["llmId"] : undefined,
					},
					payload: body,
					idempotency: "none",
					exitCodesOnFailure: LLM_PLAN_EXIT_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.llms.completions(body as never, projectKey,);
		},
		usage: LLM_COMPLETIONS_USAGE,
		description:
			"Perform completions on an LLM. COST-BEARING: invokes the LLM provider and may incur charges. Body: { llmId, queries: [{ messages: [...] }], settings?: {...} }.",
		examples: [
			'dss llm completions --data \'{"llmId":"openai:openai1:gpt-4","queries":[{"messages":[{"role":"user","content":"Hello"}]}]}\'',
			'dss llm completions --data \'{"llmId":"openai:openai1:gpt-4","queries":[...],"settings":{"temperature":0.9}}\' --dry-run',
		],
	},
	embeddings: {
		handler: async (c, _a, f,) => {
			const body = requireLlmInferenceBody(f, LLM_EMBEDDINGS_USAGE,);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return planResult("llm", "embeddings", {
					asyncKind: "none",
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(projectKey ?? c.resolveProjectKey(undefined,),)
					}/llms/embeddings`,
					identifiers: {
						llmId: typeof body["llmId"] === "string" ? body["llmId"] : undefined,
					},
					payload: body,
					idempotency: "none",
					exitCodesOnFailure: LLM_PLAN_EXIT_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.llms.embeddings(body as never, projectKey,);
		},
		usage: LLM_EMBEDDINGS_USAGE,
		description:
			'Perform embeddings on an LLM. COST-BEARING: invokes the LLM provider and may incur charges. Body: { llmId, queries: [{ text: "..." }] }.',
		examples: [
			'dss llm embeddings --data \'{"llmId":"openai:openai1:ada002-text-embedding","queries":[{"text":"Who wrote Beethoven 9th?"}]}\'',
			'dss llm embeddings --data \'{"llmId":"x","queries":[{"text":"y"}]}\' --dry-run',
		],
	},
};
