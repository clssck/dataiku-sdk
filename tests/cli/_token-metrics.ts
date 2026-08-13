import { Tiktoken, } from "js-tiktoken/lite";
import o200kBase from "js-tiktoken/ranks/o200k_base";

export const AGENT_TOKEN_ENCODING = "o200k_base";

const encoder = new Tiktoken(o200kBase,);

export interface AgentTextMetrics {
	encoding: typeof AGENT_TOKEN_ENCODING;
	tokens: number;
	utf8Bytes: number;
}

/**
 * Measures agent-facing text with a pinned, offline tokenizer. The result is an
 * exact o200k_base count, not a model-independent claim: other providers may
 * tokenize the same text differently.
 */
export function measureAgentText(text: string,): AgentTextMetrics {
	return {
		encoding: AGENT_TOKEN_ENCODING,
		tokens: encoder.encode(text,).length,
		utf8Bytes: Buffer.byteLength(text, "utf-8",),
	};
}
