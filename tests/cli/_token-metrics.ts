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
 * Golden fingerprint of the exact o200k_base model shipped in js-tiktoken
 * 1.0.21 (npm), recorded 2026-08-13. Every value below was read from the
 * loaded ranks module and is re-derived at runtime: a different ranks table,
 * pat_str, special-token set, or encoding build fails the pin.
 */
const GOLDEN_O200K_BASE = {
	patStrLength: 364,
	patStrFnv1a: 1_214_966_031,
	bpeRanksLength: 2_325_049,
	bpeRanksFnv1a: 4_089_887_228,
	specialTokens: { "<|endoftext|>": 199_999, "<|endofprompt|>": 200_018, },
	helloWorldIds: [24_912, 2_375,],
	probeText: 'dss agent contract --fields protocol,cli {"ok":true,"tokens":[1,2,3]}\ndone.',
	probeIds: [
		67,
		1_087,
		11_793,
		6_698,
		2_230,
		19_358,
		16_689,
		11,
		48_644,
		10_494,
		525,
		1_243,
		3_309,
		3_532,
		64_329,
		16_853,
		16,
		11,
		17,
		11,
		18,
		55_354,
		21_715,
		13,
	],
	endoftextId: 199_999,
} as const;

const nativeModulePath = process.env.DSS_TOKENIZER_MODULE;
const nativeTokenizer = nativeModulePath
	? await import(nativeModulePath) as { countTokens?: (text: string, encoding: string,) => number; }
	: undefined;
if (nativeTokenizer && typeof nativeTokenizer.countTokens !== "function") {
	throw new Error("DSS_TOKENIZER_MODULE must export countTokens(text, encoding)",);
}
const nativeCounter = nativeTokenizer?.countTokens;
const specialTokens = Object.keys(o200kBase.special_tokens,);

function countNativeTokens(text: string,): number {
	const count = nativeCounter!(text, "O200kBase",);
	if (!Number.isSafeInteger(count,) || count < 0) {
		throw new Error("Native token counter returned an invalid count",);
	}
	return count;
}

if (nativeCounter) {
	// Keep the pinned JS model authoritative: an explicitly configured broken
	// or incompatible native module must fail, never silently relax budgets.
	for (
		const text of ["", "hello world", GOLDEN_O200K_BASE.probeText, 'é 中文 😀\n\t{"value":1.25}',]
	) {
		if (countNativeTokens(text,) !== encoder.encode(text,).length) {
			throw new Error("Native token counter disagrees with pinned o200k_base",);
		}
	}
}

/**
 * Measures agent-facing text with a pinned, offline tokenizer. The result is an
 * exact o200k_base count, not a model-independent claim: other providers may
 * tokenize the same text differently.
 */
export function measureAgentText(
	text: string,
	backend: "auto" | "reference" = "auto",
): AgentTextMetrics {
	// The native ordinary-text encoder does not reject reserved special-token
	// spellings. Delegate those inputs to JS to preserve its validation contract.
	const native = backend === "auto" && nativeCounter
		&& !specialTokens.some(token => text.includes(token,));
	return {
		encoding: AGENT_TOKEN_ENCODING,
		tokens: native ? countNativeTokens(text,) : encoder.encode(text,).length,
		utf8Bytes: Buffer.byteLength(text, "utf-8",),
	};
}

function fnv1a32(text: string,): number {
	let hash = 0x811c_9dc5;
	for (let i = 0; i < text.length; i++) {
		hash ^= text.charCodeAt(i,);
		hash = Math.imul(hash, 0x0100_0193,) >>> 0;
	}
	return hash >>> 0;
}

export interface AgentEncodingFingerprint {
	encoding: typeof AGENT_TOKEN_ENCODING;
	patStrLength: number;
	patStrFnv1a: number;
	bpeRanksLength: number;
	bpeRanksFnv1a: number;
	specialTokens: Record<string, number>;
	helloWorldIds: number[];
	probeIds: number[];
	endoftextId: number;
}

/**
 * Diagnostics computed from the ranks module actually loaded by this file.
 */
export function agentEncodingFingerprint(): AgentEncodingFingerprint {
	return {
		encoding: AGENT_TOKEN_ENCODING,
		patStrLength: o200kBase.pat_str.length,
		patStrFnv1a: fnv1a32(o200kBase.pat_str,),
		bpeRanksLength: o200kBase.bpe_ranks.length,
		bpeRanksFnv1a: fnv1a32(o200kBase.bpe_ranks,),
		specialTokens: { ...o200kBase.special_tokens, },
		helloWorldIds: encoder.encode("hello world",),
		probeIds: encoder.encode(GOLDEN_O200K_BASE.probeText,),
		endoftextId: encoder.encode("<|endoftext|>", "all",)[0] ?? -1,
	};
}

/**
 * Validates that the loaded ranks are the exact golden o200k_base model.
 * Throws with a diagnostic listing every mismatch; returns the fingerprint on
 * success.
 */
export function assertAgentEncodingPinned(): AgentEncodingFingerprint {
	const actual = agentEncodingFingerprint();
	const mismatches: string[] = [];
	const report = (name: string, actualValue: unknown, goldenValue: unknown,) => {
		mismatches.push(
			`${name}: got ${JSON.stringify(actualValue,)}; golden ${JSON.stringify(goldenValue,)}`,
		);
	};
	if (actual.patStrLength !== GOLDEN_O200K_BASE.patStrLength) {
		report("pat_str length", actual.patStrLength, GOLDEN_O200K_BASE.patStrLength,);
	}
	if (actual.patStrFnv1a !== GOLDEN_O200K_BASE.patStrFnv1a) {
		report("pat_str FNV-1a", actual.patStrFnv1a, GOLDEN_O200K_BASE.patStrFnv1a,);
	}
	if (actual.bpeRanksLength !== GOLDEN_O200K_BASE.bpeRanksLength) {
		report("bpe_ranks length", actual.bpeRanksLength, GOLDEN_O200K_BASE.bpeRanksLength,);
	}
	if (actual.bpeRanksFnv1a !== GOLDEN_O200K_BASE.bpeRanksFnv1a) {
		report("bpe_ranks FNV-1a", actual.bpeRanksFnv1a, GOLDEN_O200K_BASE.bpeRanksFnv1a,);
	}
	if (JSON.stringify(actual.specialTokens,) !== JSON.stringify(GOLDEN_O200K_BASE.specialTokens,)) {
		report("special tokens", actual.specialTokens, GOLDEN_O200K_BASE.specialTokens,);
	}
	if (JSON.stringify(actual.helloWorldIds,) !== JSON.stringify(GOLDEN_O200K_BASE.helloWorldIds,)) {
		report("hello world ids", actual.helloWorldIds, GOLDEN_O200K_BASE.helloWorldIds,);
	}
	if (JSON.stringify(actual.probeIds,) !== JSON.stringify(GOLDEN_O200K_BASE.probeIds,)) {
		report("probe ids", actual.probeIds, GOLDEN_O200K_BASE.probeIds,);
	}
	if (actual.endoftextId !== GOLDEN_O200K_BASE.endoftextId) {
		report("<|endoftext|> id", actual.endoftextId, GOLDEN_O200K_BASE.endoftextId,);
	}
	if (mismatches.length > 0) {
		throw new Error(
			`tokenizer pin failed for ${AGENT_TOKEN_ENCODING}:\n- ${mismatches.join("\n- ",)}`,
		);
	}
	return actual;
}
