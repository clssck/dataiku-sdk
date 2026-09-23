import { ClientValidationError, } from "../errors.js";
import type { RecipeCreateOptions, } from "../schemas.js";
import { asRecord, } from "../utils/records.js";

/*
 * Pure, schema-free recipe-create request construction. `recipe create --plan`
 * imports this without loading the SDK schema graph.
 */

export function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

export function asStringArray(value: unknown,): string[] | undefined {
	if (!Array.isArray(value,)) return undefined;
	const out = value.filter((v,): v is string => typeof v === "string" && v.length > 0);
	return out.length > 0 ? out : undefined;
}

export type JoinType = "LEFT" | "INNER" | "RIGHT" | "FULL";

export type JoinKeyPair = {
	left: string;
	right: string;
};

export function parseJoinKeyPairs(values: string[], optionName = "joinOn",): JoinKeyPair[] {
	const pairs: JoinKeyPair[] = [];
	for (const raw of values) {
		const token = raw.trim();
		if (!token) continue;
		const eq = token.indexOf("=",);
		if (eq === -1) {
			pairs.push({ left: token, right: token, },);
			continue;
		}
		const left = token.slice(0, eq,).trim();
		const right = token.slice(eq + 1,).trim();
		if (!left || !right) {
			throw new ClientValidationError(
				`${optionName} values must use COL or LEFT=RIGHT.`,
				"validation_failed",
			);
		}
		pairs.push({ left, right, },);
	}
	return pairs;
}

export function normalizeFuzzyDistance(value: unknown,): string {
	const normalized = typeof value === "string" && value.trim().length > 0
		? value.trim().toUpperCase()
		: "DAMERAU_LEVENSHTEIN";
	if (normalized === "DAMERAU_LEVENSHTEIN") return "LEVENSHTEIN";
	if (
		normalized === "HAMMING" || normalized === "JACCARD" || normalized === "COSINE"
		|| normalized === "EUCLIDEAN"
	) {
		return normalized;
	}
	throw new ClientValidationError(
		"fuzzyDistance must be one of DAMERAU_LEVENSHTEIN, HAMMING, JACCARD, COSINE, or EUCLIDEAN.",
		"validation_failed",
	);
}

export function normalizeJoinType(value: unknown,): JoinType {
	const normalized = typeof value === "string" && value.trim().length > 0
		? value.trim().toUpperCase()
		: "LEFT";
	if (
		normalized === "LEFT" || normalized === "INNER" || normalized === "RIGHT"
		|| normalized === "FULL"
	) {
		return normalized;
	}
	throw new ClientValidationError(
		"joinType must be one of LEFT, INNER, RIGHT, or FULL.",
		"validation_failed",
	);
}

export function recipeInputItems(
	recipe: Record<string, unknown>,
): Array<{ ref: string; role: string; }> {
	const inputs = asRecord(recipe.inputs,);
	if (!inputs) return [];
	const result: Array<{ ref: string; role: string; }> = [];
	const seen = new Set<string>();
	for (const [role, roleValue,] of Object.entries(inputs,)) {
		const items = asRecord(roleValue,)?.items;
		if (!Array.isArray(items,)) continue;
		for (const itemValue of items) {
			const item = asRecord(itemValue,);
			const ref = asString(item?.ref,);
			if (!ref || seen.has(ref,)) continue;
			seen.add(ref,);
			result.push({ ref, role, },);
		}
	}
	return result;
}

/**
 * Pure request construction for POST /recipes/ (no DSS calls). Shared with
 * `recipe create --plan` so the plan equals the request the command sends.
 */

export function buildRecipeCreateRequest(opts: RecipeCreateOptions, pk: string,) {
	const { type, payload, outputConnection: rawConnection, joinType: rawJoinType, } = opts;
	const outputFolder = asString(opts.outputFolder,);

	// Build inputs/outputs from simple form (inputDatasets + outputDataset) or
	// advanced form (inputs + outputs); both may coexist — simple form wins when
	// the advanced form is absent.
	const inputDatasets = asStringArray(opts.inputDatasets,);
	const requestedOutputDataset = asString(opts.outputDataset,);

	let inputs: Record<string, unknown> | undefined = asRecord(opts.inputs,);
	let outputs: Record<string, unknown> | undefined = asRecord(opts.outputs,);

	if (!inputs) {
		inputs = {
			main: {
				items: inputDatasets?.map((ref,) => ({ ref, deps: [], })) ?? [],
			},
		};
	}

	// Auto-generate name if not provided
	const outputNameForDefaultRecipe = requestedOutputDataset ?? outputFolder;
	const name = opts.name ?? (type && outputNameForDefaultRecipe
		? `${type}_${outputNameForDefaultRecipe}`
		: undefined);
	const temporaryOutputDataset = outputFolder && !requestedOutputDataset && name
		? `${name}_folder_output_marker`
		: undefined;
	const outputDataset = requestedOutputDataset ?? temporaryOutputDataset;

	if (!outputs && outputDataset) {
		outputs = {
			main: {
				items: [{ ref: outputDataset, appendMode: false, },],
			},
		};
	}

	if (!type || !name || !outputs) {
		throw new ClientValidationError(
			"type and outputDataset/outputFolder or (name + outputs) are required for create.",
			"validation_failed",
		);
	}

	const joinCols = typeof opts.joinOn === "string" ? [opts.joinOn,] : asStringArray(opts.joinOn,);
	const joinKeys = type === "join" && joinCols?.length
		? parseJoinKeyPairs(joinCols,)
		: undefined;
	const fuzzyOn = opts.fuzzyOn ?? (type === "fuzzyjoin" ? opts.joinOn : undefined);
	const fuzzyCols = typeof fuzzyOn === "string" ? [fuzzyOn,] : asStringArray(fuzzyOn,);
	const fuzzyKeys = type === "fuzzyjoin" && fuzzyCols?.length
		? parseJoinKeyPairs(fuzzyCols, "fuzzyOn",)
		: undefined;
	const normalizedJoinType = joinKeys?.length || fuzzyKeys?.length
		? normalizeJoinType(rawJoinType,)
		: undefined;
	const fuzzyDistance = fuzzyKeys?.length ? normalizeFuzzyDistance(opts.fuzzyDistance,) : undefined;
	const fuzzyThreshold = opts.fuzzyThreshold ?? 1;
	if (!Number.isFinite(fuzzyThreshold,)) {
		throw new ClientValidationError("fuzzyThreshold must be a finite number.", "validation_failed",);
	}

	const recipePrototype: Record<string, unknown> = {
		type,
		name,
		projectKey: pk,
		...(type === "fuzzyjoin" ? {} : { inputs, }),
		outputs,
	};
	const creationSettings: Record<string, unknown> = {};
	if (payload !== undefined) {
		creationSettings.script = payload;
	}
	if (type === "fuzzyjoin") {
		creationSettings.virtualInputs = recipeInputItems({ inputs, },).map((item,) => item.ref);
	}
	return {
		creationSettings,
		fuzzyDistance,
		fuzzyKeys,
		fuzzyThreshold,
		inputDatasets,
		inputs,
		joinKeys,
		name,
		normalizedJoinType,
		outputFolder,
		outputs,
		payload,
		rawConnection,
		recipePrototype,
		temporaryOutputDataset,
		type,
	};
}
