import { ClientValidationError, } from "../errors.js";
import type { RecipeCreateOptions, RecipeSchemaUpdateComputable, } from "../schemas.js";
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
 * Recipe types whose output schema DSS refuses to compute ("Output schema can't
 * be automatically computed on 'python' recipes"): the code sets the schema
 * when it runs. Plugin recipes (`CustomCode_*`) are code too.
 */
const CODE_RECIPE_TYPES: Record<string, true> = {
	python: true,
	r: true,
	julia: true,
	pyspark: true,
	sparkr: true,
	spark_scala: true,
	shell: true,
	sql_script: true,
};

/** Whether DSS can compute this recipe type's output schema (visual and SQL query recipes). */
export function recipeOutputSchemaIsComputable(recipeType: string,): boolean {
	const normalized = recipeType.trim().toLowerCase();
	return CODE_RECIPE_TYPES[normalized] !== true && !normalized.startsWith("customcode_",);
}

/** The id DSS expects in actions/updateOutputSchema (dataikuapi RequiredSchemaUpdates.apply). */
export function schemaUpdateComputableId(computable: RecipeSchemaUpdateComputable,): string {
	return (computable.type === "DATASET" ? computable.datasetName : computable.id) ?? "";
}

/** A computed schema is worth writing when it has columns and differs from (or fills) the current one. */
export function schemaUpdateIsPending(computable: RecipeSchemaUpdateComputable,): boolean {
	const columns = computable.newSchema?.columns ?? [];
	if (columns.length === 0) return false;
	return computable.previousSchemaWasEmpty === true
		|| (computable.incompatibilities?.length ?? 0) > 0;
}

/** DSS grouping aggregate flags on each `values[]` entry, by lowercase name. */
const GROUPING_AGGREGATES: Record<string, string> = {
	count: "count",
	countdistinct: "countDistinct",
	sum: "sum",
	avg: "avg",
	min: "min",
	max: "max",
	median: "median",
	stddev: "stddev",
	first: "first",
	last: "last",
	concat: "concat",
	concatdistinct: "concatDistinct",
	sum2: "sum2",
};

function splitPair(raw: string, flag: string,): [string, string,] {
	const eq = raw.indexOf("=",);
	const left = eq === -1 ? "" : raw.slice(0, eq,).trim();
	const right = eq === -1 ? "" : raw.slice(eq + 1,).trim();
	if (left.length === 0 || right.length === 0) {
		throw new ClientValidationError(
			`${flag} expects NAME=VALUE, got "${raw}".`,
			"validation_failed",
		);
	}
	return [left, right,];
}

/** Grouping keys, aggregates per column (DSS flag names), and the first/last ordering column. */
export interface GroupingPayloadConfig {
	keys: string[];
	aggregates: Record<string, string[]>;
	orderBy?: string;
}

/** Grouping configuration from create options, or undefined when none was asked for. */
export function groupingPayloadConfig(
	opts: RecipeCreateOptions,
): GroupingPayloadConfig | undefined {
	const keys = opts.groupBy ?? [];
	const specs = opts.aggregate ?? [];
	if (keys.length === 0 && specs.length === 0) return undefined;
	const aggregates: Record<string, string[]> = {};
	for (const spec of specs) {
		const colon = spec.lastIndexOf(":",);
		const column = colon === -1 ? "" : spec.slice(0, colon,).trim();
		const fns = colon === -1
			? []
			: spec.slice(colon + 1,).split("+",).map((fn,) => fn.trim().toLowerCase());
		if (
			column.length === 0 || fns.length === 0
			|| fns.some((fn,) => GROUPING_AGGREGATES[fn] === undefined)
		) {
			throw new ClientValidationError(
				`--aggregate expects COL:FN[+FN] with FN one of ${
					Object.values(GROUPING_AGGREGATES,).join(", ",)
				}; got "${spec}".`,
				"validation_failed",
			);
		}
		aggregates[column] = [
			...(aggregates[column] ?? []),
			...fns.map((fn,) => GROUPING_AGGREGATES[fn]!),
		];
	}
	const ordered = Object.values(aggregates,).some((fns,) =>
		fns.includes("first",) || fns.includes("last",)
	);
	if (ordered && !opts.orderBy) {
		throw new ClientValidationError(
			"first/last aggregates need an ordering column: pass --order-by COL.",
			"validation_failed",
		);
	}
	return { keys, aggregates, ...(opts.orderBy ? { orderBy: opts.orderBy, } : {}), };
}

/**
 * Apply grouping keys and aggregates to the payload DSS created: `keys` lists
 * the group columns; each aggregated column's `values[]` entry gets its
 * aggregate flags (and `orderColumn` for first/last).
 */
export function applyGroupingPayload(
	payload: Record<string, unknown>,
	config: GroupingPayloadConfig,
): Record<string, unknown> {
	const values = Array.isArray(payload.values,)
		? payload.values.map((value,) => asRecord(value,) ?? {})
		: [];
	for (const [column, fns,] of Object.entries(config.aggregates,)) {
		let entry = values.find((value,) => value.column === column);
		if (!entry) {
			entry = { column, };
			values.push(entry,);
		}
		for (const fn of fns) entry[fn] = true;
		if (config.orderBy && (fns.includes("first",) || fns.includes("last",))) {
			entry.orderColumn = config.orderBy;
		}
	}
	return { ...payload, keys: config.keys.map((column,) => ({ column, })), values, };
}

function processorStep(type: string, params: Record<string, unknown>,): Record<string, unknown> {
	return {
		metaType: "PROCESSOR",
		type,
		params,
		disabled: false,
		preview: false,
		alwaysShowComment: false,
	};
}

/**
 * Prepare (shaker) steps from create options, in a fixed order: rename,
 * fill empty, formula column, row filter, drop columns, keep columns. Later
 * steps see renamed columns. Empty when no prepare option was given.
 */
export function prepareSteps(opts: RecipeCreateOptions,): Record<string, unknown>[] {
	const steps: Record<string, unknown>[] = [];
	const renamings = (opts.rename ?? []).map((raw,) => {
		const [from, to,] = splitPair(raw, "--rename",);
		return { from, to, };
	},);
	if (renamings.length > 0) steps.push(processorStep("ColumnRenamer", { renamings, },),);
	for (const raw of opts.fillEmpty ?? []) {
		const [column, value,] = splitPair(raw, "--fill-empty",);
		steps.push(
			processorStep("FillEmptyWithValue", { appliesTo: "SINGLE_COLUMN", columns: [column,], value, },),
		);
	}
	if (opts.formula) {
		const [column, expression,] = splitPair(opts.formula, "--formula",);
		steps.push(processorStep("CreateColumnWithGREL", { column, expression, },),);
	}
	if (opts.filter) {
		steps.push(
			processorStep("FilterOnCustomFormula", { expression: opts.filter, action: "KEEP_ROW", },),
		);
	}
	if (opts.dropColumns?.length) {
		steps.push(
			processorStep("ColumnsSelector", {
				appliesTo: "COLUMNS",
				columns: opts.dropColumns,
				keep: false,
			},),
		);
	}
	if (opts.keepColumns?.length) {
		steps.push(
			processorStep("ColumnsSelector", {
				appliesTo: "COLUMNS",
				columns: opts.keepColumns,
				keep: true,
			},),
		);
	}
	return steps;
}

/**
 * Pure request construction for POST /recipes/ (no DSS calls). Shared with
 * `recipe create --plan` so the plan equals the request the command sends.
 */

export function buildRecipeCreateRequest(opts: RecipeCreateOptions, pk: string,) {
	const { payload, outputConnection: rawConnection, joinType: rawJoinType, } = opts;
	// dataikuapi and the UI call the shaker recipe "prepare".
	const type = opts.type === "prepare" ? "shaker" : opts.type;
	const grouping = groupingPayloadConfig(opts,);
	const steps = prepareSteps(opts,);
	if (grouping && type !== "grouping") {
		throw new ClientValidationError(
			"--group-by/--aggregate/--order-by need --type grouping.",
			"validation_failed",
		);
	}
	if (steps.length > 0 && type !== "shaker") {
		throw new ClientValidationError(
			"--rename/--fill-empty/--formula/--filter/--drop-columns/--keep-columns need --type prepare.",
			"validation_failed",
		);
	}
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
		grouping,
		inputDatasets,
		inputs,
		joinKeys,
		name,
		normalizedJoinType,
		outputFolder,
		outputs,
		payload,
		prepareSteps: steps,
		rawConnection,
		recipePrototype,
		temporaryOutputDataset,
		type,
	};
}
