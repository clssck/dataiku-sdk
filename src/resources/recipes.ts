import { writeFile, } from "node:fs/promises";
import { resolve, } from "node:path";
import {
	ClientValidationError,
	DataikuError,
	nonJsonResponseBody,
	unexpectedResponseError,
} from "../errors.js";
import {
	ProjectMetadataSchema,
	RecipeSchemaUpdatesSchema,
	RecipeSummaryArraySchema,
} from "../schemas.js";
import type {
	BuildMode,
	JobWaitResult,
	ProjectMetadata,
	RecipeCreateOptions,
	RecipeCreateResult,
	RecipeSchemaUpdateComputable,
	RecipeSchemaUpdates,
	RecipeSummary,
	RecipeUpdateSchemaResult,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { asRecord, } from "../utils/records.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";
import type { JobBuildTarget, JobBuildTargetType, JobLogFilter, JobLogSummary, } from "./jobs.js";
import {
	applyGroupingPayload,
	asString,
	buildRecipeCreateRequest,
	recipeInputItems,
	recipeOutputSchemaIsComputable,
	schemaUpdateComputableId,
	schemaUpdateIsPending,
} from "./recipe-create.js";

// ---------------------------------------------------------------------------
// Helpers: type narrowing
// ---------------------------------------------------------------------------

function parseRecipePayload(payload: string | undefined,): Record<string, unknown> {
	if (!payload) return {};
	let parsed: unknown;
	try {
		parsed = JSON.parse(payload,);
	} catch {
		throw new DataikuError(200, "Invalid JSON response", nonJsonResponseBody(payload,),);
	}
	return asRecord(parsed,) ?? {};
}

function recipeVirtualInputs(
	payload: Record<string, unknown>,
	inputCount: number,
): Record<string, unknown>[] {
	const current = Array.isArray(payload.virtualInputs,) ? payload.virtualInputs : [];
	const virtualInputs: Record<string, unknown>[] = [];
	for (let i = 0; i < inputCount; i++) {
		virtualInputs.push({
			preFilter: {},
			outputColumnsSelectionMode: "AUTO_NON_CONFLICTING",
			computedColumns: [],
			...asRecord(current[i],),
			index: i,
		},);
	}
	return virtualInputs;
}
const RECIPE_DEFINITION_FIELDS = new Set(["params", "inputs", "outputs", "scriptSettings",],);

export interface RecipeRunOutput extends JobBuildTarget {
	ref: string;
	role: string;
}

export interface RecipeGraphReference {
	ref: string;
	role: string;
	type?: JobBuildTargetType;
	exists: boolean;
	id?: string;
}

export interface RecipeGraphValidationResult {
	valid: boolean;
	recipeName: string;
	projectKey: string;
	inputs: RecipeGraphReference[];
	outputs: RecipeGraphReference[];
	missingInputs: RecipeGraphReference[];
	missingOutputs: RecipeGraphReference[];
	ambiguousOutputs: string[];
	warnings: string[];
}

export interface RecipeRunOptions {
	buildMode?: BuildMode;
	includeLogs?: boolean;
	maxLogLines?: number;
	partition?: string;
	pollIntervalMs?: number;
	projectKey?: string;
	wait?: boolean;
	timeoutMs?: number;
	logFilter?: JobLogFilter;
	summary?: boolean;
}

export type RecipeRunResult =
	& { logSummary?: JobLogSummary; recipeName: string; outputs: RecipeRunOutput[]; }
	& ({ jobId: string; } | JobWaitResult);

export interface RecipeCloneOptions {
	projectKey?: string;
	name: string;
	outputDataset?: string;
	outputRewrites?: Record<string, string>;
	inputRewrites?: Record<string, string>;
	payloadRewrites?: Record<string, string>;
	payloadTextRewrites?: Record<string, string>;
	copyOutputSettings?: boolean;
	outputPath?: string;
	metastoreTableName?: string;
	recipeType?: string;
}

export interface RecipeCloneResult {
	sourceRecipeName: string;
	recipeName: string;
	projectKey: string;
	outputRewrites: Record<string, string>;
	inputRewrites: Record<string, string>;
	payloadRewrites: Record<string, string>;
	payloadTextRewrites: Record<string, string>;
	copiedOutputDatasets: string[];
}

function rootRecipeDefinitionFields(data: Record<string, unknown>,): string[] {
	return Object.keys(data,).filter((key,) => RECIPE_DEFINITION_FIELDS.has(key,));
}

function normalizeRecipeOutputType(value: unknown,): JobBuildTargetType | undefined {
	if (typeof value !== "string") return undefined;
	const normalized = value.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "DATASET") return "DATASET";
	if (normalized === "MANAGED_FOLDER" || normalized === "FOLDER") return "MANAGED_FOLDER";
	if (normalized === "MODEL_EVALUATION_STORE") return "MODEL_EVALUATION_STORE";
	return undefined;
}

function recipeOutputItems(
	recipe: Record<string, unknown>,
): Array<{ ref: string; role: string; type?: JobBuildTargetType; }> {
	const outputs = asRecord(recipe.outputs,);
	if (!outputs) return [];
	const result: Array<{ ref: string; role: string; type?: JobBuildTargetType; }> = [];
	const seen = new Set<string>();
	for (const [role, roleValue,] of Object.entries(outputs,)) {
		const items = asRecord(roleValue,)?.items;
		if (!Array.isArray(items,)) continue;
		for (const itemValue of items) {
			const item = asRecord(itemValue,);
			const ref = asString(item?.ref,);
			if (!ref) continue;
			const seenKey = ref;
			if (seen.has(seenKey,)) continue;
			seen.add(seenKey,);
			// The evaluation recipe's "evaluationStore" role targets a model
			// evaluation store even when the item carries no explicit type
			// (recipe.py EvaluationRecipeCreator.with_output_evaluation_store);
			// the store is a valid job target (run object_type_map).
			const type = normalizeRecipeOutputType(item?.type ?? item?.targetType ?? item?.objectType,)
				?? (role === "evaluationStore" ? "MODEL_EVALUATION_STORE" as const : undefined);
			result.push({
				ref,
				role,
				...(type ? { type, } : {}),
			},);
		}
	}
	return result;
}

function rewriteRefs(value: unknown, rewrites: Record<string, string>,): unknown {
	if (Object.keys(rewrites,).length === 0) return value;
	if (Array.isArray(value,)) return value.map((item,) => rewriteRefs(item, rewrites,));
	const record = asRecord(value,);
	if (!record) return value;
	const next: Record<string, unknown> = {};
	for (const [key, item,] of Object.entries(record,)) {
		if (key === "ref" && typeof item === "string" && rewrites[item]) {
			next[key] = rewrites[item];
		} else {
			next[key] = rewriteRefs(item, rewrites,);
		}
	}
	return next;
}

function escapedRegExp(value: string,): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&",);
}

function isSqlRecipeType(recipeType: unknown,): boolean {
	return typeof recipeType === "string" && recipeType.toLowerCase().includes("sql",);
}

function escapeQuotedIdentifier(identifier: string, quote: string,): string {
	if (quote === '"') return identifier.replace(/"/g, '""',);
	if (quote === "`") return identifier.replace(/`/g, "``",);
	return identifier;
}

function escapeBracketIdentifier(identifier: string,): string {
	return identifier.replace(/\]/g, "]]",);
}

function rewriteSqlTableReferences(
	payload: string,
	rewrites: Record<string, string>,
): string {
	const bareIdentifierPattern = /^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)*$/;
	let next = payload;
	for (const [from, to,] of Object.entries(rewrites,)) {
		if (!from) continue;
		const escaped = escapedRegExp(from,);
		const pattern = new RegExp(
			String
				.raw`\b(FROM|JOIN)(\s+)(?:(["\`])${escaped}\3|(\[)${escaped}\]|${escaped})(?![A-Za-z0-9_.])`,
			"gi",
		);
		next = next.replace(
			pattern,
			(
				_match: string,
				keyword: string,
				space: string,
				quote: string | undefined,
				bracket: string | undefined,
			) => {
				if (quote) {
					const escapedTo = escapeQuotedIdentifier(to, quote,);
					return `${keyword}${space}${quote}${escapedTo}${quote}`;
				}
				if (bracket) {
					const escapedTo = escapeBracketIdentifier(to,);
					return `${keyword}${space}[${escapedTo}]`;
				}
				if (!bareIdentifierPattern.test(to,)) {
					throw new ClientValidationError(
						`Unsafe SQL rewrite target for ${from}: ${to}`,
						"validation_failed",
					);
				}
				return `${keyword}${space}${to}`;
			},
		);
	}
	return next;
}

function rewritePayloadText(
	payload: string,
	rewrites: Record<string, string>,
): string {
	const entries = Object
		.entries(rewrites,)
		.filter(([from,],) => from.length > 0)
		.sort(([left,], [right,],) => right.length - left.length);
	if (entries.length === 0) return payload;
	const replacements = new Map(entries,);
	const alternatives = entries
		.map(([from,],) => escapedRegExp(from,))
		.join("|",);
	const pattern = new RegExp(
		String.raw`(?<![A-Za-z0-9_$])(${alternatives})(?![A-Za-z0-9_$])`,
		"g",
	);
	return payload.replace(pattern, (match: string,) => replacements.get(match,) ?? match,);
}

function rewritePayload(
	payload: string | undefined,
	rewrites: Record<string, string>,
	payloadTextRewrites: Record<string, string> = {},
	recipeType?: unknown,
): string | undefined {
	if (
		payload === undefined
		|| (Object.keys(rewrites,).length === 0 && Object.keys(payloadTextRewrites,).length === 0)
	) {
		return payload;
	}
	let next = payload;
	for (const [from, to,] of Object.entries(rewrites,)) {
		if (!from) continue;
		const escaped = escapedRegExp(from,);
		next = next.replace(
			new RegExp(`\\bdataiku\\.(Dataset|Folder)\\(\\s*(['"])${escaped}\\2\\s*\\)`, "g",),
			(_match, kind: string, quote: string,) => `dataiku.${kind}(${quote}${to}${quote})`,
		);
	}
	if (isSqlRecipeType(recipeType,)) {
		next = rewriteSqlTableReferences(next, rewrites,);
	}
	next = rewritePayloadText(next, payloadTextRewrites,);
	return next;
}

function cloneRecipeDefinition(
	recipe: Record<string, unknown>,
	targetName: string,
	projectKey: string,
	rewrites: Record<string, string>,
): Record<string, unknown> {
	const cloned = rewriteRefs(structuredClone(recipe,), rewrites,) as Record<string, unknown>;
	delete cloned.versionTag;
	delete cloned.neverBuilt;
	cloned.name = targetName;
	cloned.projectKey = projectKey;
	return cloned;
}

function inferRecipeCodeExtension(recipeType: unknown,): string {
	const normalized = typeof recipeType === "string" ? recipeType.trim().toLowerCase() : "";
	if (!normalized) return ".txt";
	if (normalized.includes("python",) || normalized.includes("pyspark",)) return ".py";
	if (normalized.includes("sql",)) return ".sql";
	if (normalized === "r" || normalized.startsWith("r_",)) return ".R";
	if (normalized.includes("scala",)) return ".scala";
	if (normalized.includes("shell",)) return ".sh";
	return ".txt";
}

// ---------------------------------------------------------------------------
// Helpers: retry predicate
// ---------------------------------------------------------------------------

function shouldRetryRecipeCreateWithOutputProvisioning(error: unknown,): error is DataikuError {
	if (!(error instanceof DataikuError)) return false;
	if (
		error.category !== "validation"
		&& error.category !== "not_found"
		&& error.category !== "unknown"
	) {
		return false;
	}
	const detail = `${error.statusText}\n${error.body}`.toLowerCase();
	const mentionsMissingDataset = detail.includes("dataset",)
		&& (detail.includes("not found",)
			|| detail.includes("does not exist",)
			|| detail.includes("unknown",));
	return mentionsMissingDataset;
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

export class RecipesResource extends BaseResource {
	/** List all recipes in a project. */
	async list(projectKey?: string,): Promise<RecipeSummary[]> {
		const enc = this.enc(projectKey,);
		const raw = await this.client.get<unknown>(`/public/api/projects/${enc}/recipes/`,);
		return this.client.safeParse(RecipeSummaryArraySchema, raw, "recipes.list",);
	}

	/** Get recipe metadata (label, tags, description, custom fields). */
	async metadata(
		recipeName: string,
		opts?: { projectKey?: string; },
	): Promise<ProjectMetadata> {
		const enc = this.enc(opts?.projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${enc}/recipes/${rnEnc}/metadata`,
		);
		return this.client.safeParse(ProjectMetadataSchema, raw, "recipes.metadata",);
	}

	/**
	 * Replace recipe metadata with the supplied full object.
	 *
	 * DSS PUT semantics replace the whole metadata object: pass a metadata
	 * object obtained from {@link metadata}, edit it, and send it back — fields
	 * absent from the payload are removed from the recipe.
	 */
	async setMetadata(
		recipeName: string,
		metadata: ProjectMetadata,
		opts?: { projectKey?: string; },
	): Promise<void> {
		const enc = this.enc(opts?.projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		await this.client.putVoid(
			`/public/api/projects/${enc}/recipes/${rnEnc}/metadata`,
			metadata,
		);
	}

	/**
	 * Get a recipe definition (and optionally its payload).
	 * Returns the raw API response shape: `{ recipe, payload }`.
	 */
	async get(
		recipeName: string,
		opts?: {
			includePayload?: boolean;
			payloadMaxLines?: number;
			projectKey?: string;
		},
	): Promise<{ recipe: Record<string, unknown>; payload?: string; }> {
		const enc = this.enc(opts?.projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		const params = new URLSearchParams();
		if (opts?.includePayload) params.set("includePayload", "true",);
		// oxlint-disable-next-line eqeqeq -- intentional null check
		if (opts?.payloadMaxLines != null) params.set("payloadMaxLines", String(opts.payloadMaxLines,),);
		const qs = params.toString();
		const url = `/public/api/projects/${enc}/recipes/${rnEnc}${qs ? `?${qs}` : ""}`;
		const result = await this.client.get<{ recipe: Record<string, unknown>; payload?: string; }>(
			url,
		);
		const recipe = asRecord(result?.recipe,);
		if (!result || !recipe) {
			throw new DataikuError(
				404,
				"Not Found",
				`Recipe "${recipeName}" not found in project "${
					this.resolveProjectKey(opts?.projectKey,)
				}" (DSS returned empty response).`,
			);
		}
		return opts?.includePayload ? { ...result, recipe, } : { recipe, };
	}

	/** Validate declared recipe graph references before running/building. */
	async validateGraph(
		recipeName: string,
		opts?: { projectKey?: string; },
	): Promise<RecipeGraphValidationResult> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const { recipe, } = await this.get(recipeName, { projectKey: pk, },);
		const inputItems = recipeInputItems(recipe,);
		const outputItems = recipeOutputItems(recipe,);
		const [datasets, folders, stores,] = await Promise.all([
			this.client.datasets.list(pk,),
			this.client.folders.list(pk,),
			this.client.modelEvaluationStores.list(pk,),
		],);
		const datasetNames = new Set(datasets.map((dataset,) => dataset.name),);
		const storeIds = new Set(stores.map((store,) => store.id),);
		const folderIdByRef = new Map<string, string>();
		for (const folder of folders) {
			folderIdByRef.set(folder.id, folder.id,);
			if (folder.name) folderIdByRef.set(folder.name, folder.id,);
		}

		const resolveReference = (
			item: { ref: string; role: string; type?: JobBuildTargetType; },
			requireExplicitOutputType: boolean,
		): RecipeGraphReference => {
			const folderId = folderIdByRef.get(item.ref,);
			const isDataset = datasetNames.has(item.ref,);
			if (item.type === "DATASET") {
				return { ref: item.ref, role: item.role, type: "DATASET", exists: isDataset, id: item.ref, };
			}
			if (item.type === "MODEL_EVALUATION_STORE") {
				return {
					ref: item.ref,
					role: item.role,
					type: "MODEL_EVALUATION_STORE",
					exists: storeIds.has(item.ref,),
					id: item.ref,
				};
			}
			if (item.type === "MANAGED_FOLDER") {
				return {
					ref: item.ref,
					role: item.role,
					type: "MANAGED_FOLDER",
					exists: folderId !== undefined,
					id: folderId ?? item.ref,
				};
			}
			if (isDataset && (!folderId || !requireExplicitOutputType)) {
				return { ref: item.ref, role: item.role, type: "DATASET", exists: true, id: item.ref, };
			}
			if (folderId && !isDataset) {
				return { ref: item.ref, role: item.role, type: "MANAGED_FOLDER", exists: true, id: folderId, };
			}
			return { ref: item.ref, role: item.role, exists: false, };
		};

		const inputs = inputItems.map((item,) => resolveReference(item, false,));
		const outputs = outputItems.map((item,) => resolveReference(item, true,));
		const ambiguousOutputs = outputItems
			.filter((item,) => !item.type && datasetNames.has(item.ref,) && folderIdByRef.has(item.ref,))
			.map((item,) => item.ref);
		const missingInputs = inputs.filter((item,) => !item.exists);
		const missingOutputs = outputs.filter((item,) => !item.exists);
		const warnings: string[] = [];
		if (outputItems.length === 0) warnings.push("Recipe has no declared outputs to build.",);
		for (const ref of ambiguousOutputs) {
			warnings.push(
				`Output "${ref}" matches both a dataset and a managed folder; declare an explicit output type.`,
			);
		}
		for (const output of outputs) {
			if (!output.exists) {
				warnings.push(`Declared output "${output.ref}" was not found in project "${pk}".`,);
			}
		}
		for (const input of missingInputs) {
			warnings.push(`Declared input "${input.ref}" was not found in project "${pk}".`,);
		}

		return {
			valid: missingInputs.length === 0
				&& missingOutputs.length === 0
				&& ambiguousOutputs.length === 0
				&& outputItems.length > 0,
			recipeName,
			projectKey: pk,
			inputs,
			outputs,
			missingInputs,
			missingOutputs,
			ambiguousOutputs,
			warnings,
		};
	}

	/** Resolve recipe outputs to job-build targets. */
	async resolveRunOutputs(
		recipeName: string,
		opts?: { partition?: string; projectKey?: string; },
	): Promise<RecipeRunOutput[]> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const { recipe, } = await this.get(recipeName, { projectKey: pk, },);
		const outputItems = recipeOutputItems(recipe,);
		if (outputItems.length === 0) {
			throw new ClientValidationError(
				`Recipe "${recipeName}" has no output items to build.`,
				"validation_failed",
			);
		}

		const [datasets, folders,] = await Promise.all([
			this.client.datasets.list(pk,),
			this.client.folders.list(pk,),
		],);
		const datasetNames = new Set(datasets.map((dataset,) => dataset.name),);
		const folderIdByRef = new Map<string, string>();
		for (const folder of folders) {
			folderIdByRef.set(folder.id, folder.id,);
			if (folder.name) folderIdByRef.set(folder.name, folder.id,);
		}

		return outputItems.map((item,) => {
			if (item.type === "DATASET") {
				return {
					ref: item.ref,
					role: item.role,
					id: item.ref,
					type: "DATASET",
					projectKey: pk,
					partition: opts?.partition,
				};
			}

			if (item.type === "MODEL_EVALUATION_STORE") {
				// The store id is the target id; evaluation builds into the store
				// exactly like the official client (recipe.run maps
				// COMPUTABLE_MODEL_EVALUATION_STORE -> MODEL_EVALUATION_STORE).
				return {
					ref: item.ref,
					role: item.role,
					id: item.ref,
					type: "MODEL_EVALUATION_STORE",
					projectKey: pk,
					partition: opts?.partition,
				};
			}

			const folderId = folderIdByRef.get(item.ref,);
			if (item.type === "MANAGED_FOLDER") {
				return {
					ref: item.ref,
					role: item.role,
					id: folderId ?? item.ref,
					type: "MANAGED_FOLDER",
					projectKey: pk,
					partition: opts?.partition,
				};
			}

			const isDataset = datasetNames.has(item.ref,);
			if (isDataset && folderId) {
				throw new ClientValidationError(
					`Recipe "${recipeName}" output "${item.ref}" matches both a dataset and a managed folder. Add an explicit output type to the recipe definition or build the target directly with --target-type.`,
					"validation_failed",
				);
			}
			if (folderId) {
				return {
					ref: item.ref,
					role: item.role,
					id: folderId,
					type: "MANAGED_FOLDER",
					projectKey: pk,
					partition: opts?.partition,
				};
			}
			if (isDataset) {
				return {
					ref: item.ref,
					role: item.role,
					id: item.ref,
					type: "DATASET",
					projectKey: pk,
					partition: opts?.partition,
				};
			}
			throw new ClientValidationError(
				`Recipe "${recipeName}" output "${item.ref}" was not found as a dataset or managed folder in project "${pk}".`,
				"validation_failed",
			);
		},);
	}

	/** Run a recipe by building its resolved outputs. */
	async run(recipeName: string, opts?: RecipeRunOptions,): Promise<RecipeRunResult> {
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const outputs = await this.resolveRunOutputs(recipeName, {
			partition: opts?.partition,
			projectKey: pk,
		},);
		const shouldWait = opts?.wait === true
			|| opts?.includeLogs === true
			|| opts?.summary === true
			|| opts?.timeoutMs !== undefined
			|| opts?.pollIntervalMs !== undefined;

		if (shouldWait) {
			const waitResult = await this.client.jobs.buildAndWaitOutputs(outputs, {
				buildMode: opts?.buildMode,
				includeLogs: opts?.includeLogs,
				maxLogLines: opts?.maxLogLines,
				logFilter: opts?.logFilter,
				pollIntervalMs: opts?.pollIntervalMs,
				projectKey: pk,
				timeoutMs: opts?.timeoutMs,
				summary: opts?.summary,
			},);
			return { recipeName, outputs, ...waitResult, };
		}

		const started = await this.client.jobs.buildOutputs(outputs, {
			buildMode: opts?.buildMode,
			projectKey: pk,
		},);
		return { recipeName, outputs, ...started, };
	}

	/** Create a recipe, with optional output dataset provisioning and join configuration. */
	async create(opts: RecipeCreateOptions,): Promise<RecipeCreateResult> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const enc = encodeURIComponent(pk,);

		const {
			creationSettings,
			fuzzyDistance,
			fuzzyKeys,
			fuzzyThreshold,
			grouping,
			inputDatasets,
			inputs,
			joinKeys,
			normalizedJoinType,
			outputFolder,
			outputs,
			prepareSteps: prepareStepList,
			rawConnection,
			recipePrototype,
			temporaryOutputDataset,
			type,
		} = buildRecipeCreateRequest(opts, pk,);

		const createRecipe = () =>
			this.client.post<Record<string, unknown>>(`/public/api/projects/${enc}/recipes/`, {
				recipePrototype,
				creationSettings,
			},);

		const createdDatasets: string[] = [];
		let usedOutputProvisioningFallback = false;

		// Missing outputs are created through POST /datasets/managed, like the DSS UI
		// and dataikuapi's new_managed_dataset: DSS picks the dataset type, storage
		// path, and format from the connection (an S3 connection yields an S3
		// dataset under the connection's naming rule), so the client never guesses
		// type-specific params.
		const provisionOutputDatasets = async (): Promise<void> => {
			const existingDs = await this.client.get<
				Array<{ name: string; params?: { connection?: string; }; managed?: boolean; }>
			>(`/public/api/projects/${enc}/datasets/`,);

			let outputConnection = asString(rawConnection,);
			if (!outputConnection) {
				const managedDs = existingDs.find((d,) => d.managed && d.params?.connection);
				if (managedDs?.params?.connection) {
					outputConnection = managedDs.params.connection;
				}
			}

			if (!outputConnection) return;

			const existingNames = new Set([...existingDs.map((d,) => d.name), ...createdDatasets,],);
			const outputRoles = outputs as Record<string, { items?: Array<{ ref?: string; }>; }>;
			for (const role of Object.values(outputRoles,)) {
				for (const item of role.items ?? []) {
					if (!item.ref || existingNames.has(item.ref,)) continue;
					await this.client.datasets.createManaged(
						{ name: item.ref, connection: outputConnection, },
						pk,
					);
					existingNames.add(item.ref,);
					createdDatasets.push(item.ref,);
				}
			}
		};
		let finalRecipeName!: string;

		// DSS renames some recipe types server-side (e.g. prediction_scoring
		// becomes "score_<inputDataset>") and documents the creation response as
		// the final unique name: Body {"name": "recipe1"} ("Returns the final
		// unique name of the recipe"). The response name is the contract —
		// falling back to the requested name would mask a missing/invalid
		// receipt, so a body without it is an error.
		try {
			if (rawConnection) await provisionOutputDatasets();
			const created = await createRecipe();
			const receivedName = asString(created?.["name"],);
			if (!receivedName) {
				throw unexpectedResponseError(
					'DSS create-recipe response did not include the final recipe name (documented body: {"name": ...}).',
				);
			}
			finalRecipeName = receivedName;
		} catch (error) {
			if (!shouldRetryRecipeCreateWithOutputProvisioning(error,)) {
				throw error;
			}
			usedOutputProvisioningFallback = true;
			await provisionOutputDatasets();
			const retryCreated = await createRecipe();
			const retryName = asString(retryCreated?.["name"],);
			if (!retryName) {
				throw unexpectedResponseError(
					'DSS create-recipe response did not include the final recipe name (documented body: {"name": ...}).',
				);
			}
			finalRecipeName = retryName;
		}

		// For join recipes: configure join conditions after creation
		let joinConfigured = false;
		if (type === "join" && joinKeys?.length) {
			const rnEnc = encodeURIComponent(finalRecipeName,);
			const full = await this.client.get<{
				recipe: Record<string, unknown>;
				payload?: string;
			}>(`/public/api/projects/${enc}/recipes/${rnEnc}`,);
			const joinPayload = parseRecipePayload(full.payload,);
			const inputCount = recipeInputItems({ inputs, },).length || inputDatasets?.length || 2;
			const joinType = normalizedJoinType ?? "LEFT";

			joinPayload.virtualInputs = recipeVirtualInputs(joinPayload, inputCount,);
			joinPayload.joins = Array.from({ length: Math.max(0, inputCount - 1,), }, (_, index,) => {
				const table2 = index + 1;
				return {
					table1: 0,
					table2,
					conditionsMode: "AND",
					type: joinType,
					outerJoinOnTheLeft: joinType === "LEFT",
					on: joinKeys.map((key,) => ({
						column1: { name: key.left, table: 0, },
						column2: { name: key.right, table: table2, },
						type: "EQ",
					})),
				};
			},);

			await this.client.put(`/public/api/projects/${enc}/recipes/${rnEnc}`, {
				...full,
				recipe: {
					...full.recipe,
					inputs,
					outputs,
				},
				payload: JSON.stringify(joinPayload,),
			},);
			joinConfigured = true;
		}

		if (type === "fuzzyjoin" && fuzzyKeys?.length) {
			const rnEnc = encodeURIComponent(finalRecipeName,);
			const full = await this.client.get<{
				recipe: Record<string, unknown>;
				payload?: string;
			}>(`/public/api/projects/${enc}/recipes/${rnEnc}`,);
			const fuzzyPayload = parseRecipePayload(full.payload,);
			const inputCount = recipeInputItems({ inputs, },).length || inputDatasets?.length || 2;
			const joinType = normalizedJoinType ?? "LEFT";

			fuzzyPayload.virtualInputs = recipeVirtualInputs(fuzzyPayload, inputCount,);
			fuzzyPayload.joins = Array.from({ length: Math.max(0, inputCount - 1,), }, (_, index,) => {
				const table2 = index + 1;
				return {
					table1: 0,
					table2,
					conditionsMode: "AND",
					type: joinType,
					on: fuzzyKeys.map((key,) => ({
						column1: { name: key.left, table: 0, },
						column2: { name: key.right, table: table2, },
						type: "EQ",
						fuzzyMatchDesc: {
							distanceType: fuzzyDistance ?? "LEVENSHTEIN",
							threshold: fuzzyThreshold,
						},
						...(opts.fuzzyNormalize === true
							? {
								normaliseDesc: {
									caseInsensitive: true,
									normaliseText: true,
									unicodeCasting: true,
								},
							}
							: {}),
					})),
				};
			},);

			await this.client.put(`/public/api/projects/${enc}/recipes/${rnEnc}`, {
				...full,
				recipe: {
					...full.recipe,
					inputs,
					outputs,
				},
				payload: JSON.stringify(fuzzyPayload,),
			},);
			joinConfigured = true;
		}

		// Grouping keys/aggregates and prepare steps edit the payload DSS created.
		let payloadConfigured = false;
		if (grouping || prepareStepList.length > 0) {
			const rnEnc = encodeURIComponent(finalRecipeName,);
			const full = await this.client.get<{ recipe: Record<string, unknown>; payload?: string; }>(
				`/public/api/projects/${enc}/recipes/${rnEnc}`,
			);
			const created = parseRecipePayload(full.payload,);
			const configured = grouping
				? applyGroupingPayload(created, grouping,)
				: {
					...created,
					steps: [...(Array.isArray(created.steps,) ? created.steps : []), ...prepareStepList,],
				};
			await this.client.put(`/public/api/projects/${enc}/recipes/${rnEnc}`, {
				...full,
				payload: JSON.stringify(configured,),
			},);
			payloadConfigured = true;
		}

		let temporaryOutputDatasetDeleted: boolean | undefined;
		if (outputFolder) {
			await this.update(finalRecipeName, {
				recipe: {
					outputs: {
						main: {
							items: [{ ref: outputFolder, appendMode: false, },],
						},
					},
				},
			}, pk,);

			if (temporaryOutputDataset) {
				try {
					await this.client.del(
						`/public/api/projects/${enc}/datasets/${encodeURIComponent(temporaryOutputDataset,)}`,
					);
					temporaryOutputDatasetDeleted = true;
					const createdIndex = createdDatasets.indexOf(temporaryOutputDataset,);
					if (createdIndex !== -1) createdDatasets.splice(createdIndex, 1,);
				} catch {
					temporaryOutputDatasetDeleted = false;
				}
			}
		}

		// DSS computes visual-recipe output schemas only when asked (the UI asks on
		// save; dataikuapi exposes compute_schema_updates). Without this, a join or
		// grouping output keeps an empty schema and its build "succeeds" with no
		// columns. Only outputs this call created, or whose schema is still empty,
		// are written: an existing schema is never replaced implicitly.
		let outputSchemaUpdated: string[] | undefined;
		let outputSchemaUpdateError: string | undefined;
		if (!outputFolder && recipeOutputSchemaIsComputable(type,)) {
			try {
				const created = new Set(createdDatasets,);
				const { updated, } = await this.applySchemaUpdates(
					finalRecipeName,
					await this.computeSchemaUpdates(finalRecipeName, pk,),
					pk,
					(computable,) =>
						computable.previousSchemaWasEmpty === true
						|| created.has(schemaUpdateComputableId(computable,),),
				);
				if (updated.length > 0) outputSchemaUpdated = updated;
			} catch (error) {
				if (!(error instanceof DataikuError)) throw error;
				outputSchemaUpdateError = error.message.split("\n",)[0];
			}
		}

		return {
			recipeName: finalRecipeName,
			type,
			createdDatasets,
			joinConfigured,
			...(payloadConfigured ? { payloadConfigured, } : {}),
			outputProvisioningFallbackUsed: usedOutputProvisioningFallback,
			...(outputSchemaUpdated ? { outputSchemaUpdated, } : {}),
			...(outputSchemaUpdateError ? { outputSchemaUpdateError, } : {}),
			...(outputFolder ? { outputFolder, } : {}),
			...(temporaryOutputDataset ? { temporaryOutputDataset, } : {}),
			...(temporaryOutputDatasetDeleted !== undefined ? { temporaryOutputDatasetDeleted, } : {}),
		};
	}

	/**
	 * Compute the output schemas DSS derives from the recipe's current settings
	 * (GET /recipes/{name}/schema-update, dataikuapi `compute_schema_updates`).
	 * DSS rejects code recipes, whose code sets the schema at run time.
	 */
	async computeSchemaUpdates(
		recipeName: string,
		projectKey?: string,
	): Promise<RecipeSchemaUpdates> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/recipes/${
				encodeURIComponent(recipeName,)
			}/schema-update`,
		);
		return this.client.safeParse(RecipeSchemaUpdatesSchema, raw, "recipes.computeSchemaUpdates",);
	}

	/**
	 * Write each pending computed schema accepted by `select` through
	 * actions/updateOutputSchema. Pending outputs `select` rejects are reported,
	 * not written.
	 */
	private async applySchemaUpdates(
		recipeName: string,
		updates: RecipeSchemaUpdates,
		projectKey: string | undefined,
		select: (computable: RecipeSchemaUpdateComputable,) => boolean,
	): Promise<
		{ updated: string[]; pending: string[]; unchanged: string[]; datasetsNeedingAction: unknown[]; }
	> {
		const endpoint = `/public/api/projects/${this.enc(projectKey,)}/recipes/${
			encodeURIComponent(recipeName,)
		}/actions/updateOutputSchema`;
		const updated: string[] = [];
		const pending: string[] = [];
		const unchanged: string[] = [];
		const datasetsNeedingAction: unknown[] = [];
		for (const computable of updates.computables) {
			const computableId = schemaUpdateComputableId(computable,);
			if (!schemaUpdateIsPending(computable,)) {
				unchanged.push(computableId,);
				continue;
			}
			if (!select(computable,)) {
				pending.push(computableId,);
				continue;
			}
			const applied = await this.client.post<{ datasetsNeedingAction?: unknown[]; } | undefined>(
				endpoint,
				{ computableType: computable.type, computableId, newSchema: computable.newSchema, },
			);
			updated.push(computableId,);
			if (applied?.datasetsNeedingAction?.length) {
				datasetsNeedingAction.push(...applied.datasetsNeedingAction,);
			}
		}
		return { updated, pending, unchanged, datasetsNeedingAction, };
	}

	/**
	 * Replace each output schema that differs from the DSS-computed one
	 * (dataikuapi `compute_schema_updates().apply()`, the UI's "Update schema").
	 * With `derivedOutputs`, only outputs whose schema is empty or listed there
	 * are written: callers pass the outputs whose schema matched the computed
	 * one before an edit, so DSS-derived schemas follow the edit while
	 * hand-edited ones are left alone. `dryRun` writes nothing. Skipped
	 * differing outputs are listed under `pending`. Code recipes are rejected
	 * before the schema computation: DSS cannot compute their output schema.
	 */
	async updateSchema(
		recipeName: string,
		opts?: { projectKey?: string; derivedOutputs?: string[]; dryRun?: boolean; },
	): Promise<RecipeUpdateSchemaResult> {
		const { recipe, } = await this.get(recipeName, { projectKey: opts?.projectKey, },);
		const recipeType = typeof recipe.type === "string" ? recipe.type : "";
		if (!recipeOutputSchemaIsComputable(recipeType,)) {
			throw new ClientValidationError(
				`DSS cannot compute output schemas for ${recipeType} recipes: the code sets the schema when it runs. Build the recipe, or set columns with dss dataset refresh-schema.`,
				"validation_failed",
			);
		}
		const updates = await this.computeSchemaUpdates(recipeName, opts?.projectKey,);
		const derived = opts?.derivedOutputs ? new Set(opts.derivedOutputs,) : undefined;
		const applied = await this.applySchemaUpdates(
			recipeName,
			updates,
			opts?.projectKey,
			(computable,) =>
				opts?.dryRun !== true
				&& (derived === undefined || computable.previousSchemaWasEmpty === true
					|| derived.has(schemaUpdateComputableId(computable,),)),
		);
		return {
			recipeName,
			updated: applied.updated,
			pending: applied.pending,
			unchanged: applied.unchanged,
			totalIncompatibilities: updates.totalIncompatibilities ?? 0,
			computables: updates.computables,
			...(applied.datasetsNeedingAction.length > 0
				? { datasetsNeedingAction: applied.datasetsNeedingAction, }
				: {}),
		};
	}

	/**
	 * Update a recipe by merging the patch into the current definition.
	 * The `recipe` sub-object is deep-merged to preserve nested fields.
	 */
	async update(
		recipeName: string,
		data: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const enc = this.enc(projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${enc}/recipes/${rnEnc}`,
		);
		const currentRecipe = asRecord(current.recipe,);
		if (!currentRecipe) {
			throw new ClientValidationError(
				`Recipe "${recipeName}" was not found or returned an empty definition.`,
				"validation_failed",
			);
		}
		const misplacedRecipeFields = rootRecipeDefinitionFields(data,);
		if (misplacedRecipeFields.length > 0) {
			throw new ClientValidationError(
				`Recipe fields ${
					misplacedRecipeFields.join(", ",)
				} must be nested under "recipe". Example: {"recipe":{"outputs":{...},"params":{...}}}`,
				"validation_failed",
			);
		}
		const mergedRecipe = deepMerge(currentRecipe, asRecord(data.recipe,) ?? {},);
		const merged = { ...current, ...data, recipe: mergedRecipe, };
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${enc}/recipes/${rnEnc}`,
			merged,
		);
	}

	/** Replace a full recipe API document. */
	async replace(
		recipeName: string,
		document: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const enc = this.enc(projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${enc}/recipes/${rnEnc}`,
			document,
		);
	}

	/** Clone recipe graph/settings and optionally clone a dataset output. */
	async clone(sourceName: string, opts: RecipeCloneOptions,): Promise<RecipeCloneResult> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const source = await this.get(sourceName, { includePayload: true, projectKey: pk, },);
		const outputRewrites: Record<string, string> = {};
		if (opts.outputRewrites) Object.assign(outputRewrites, opts.outputRewrites,);
		if (opts.outputDataset !== undefined) {
			const outputs = recipeOutputItems(source.recipe,).filter((item,) =>
				item.type !== "MANAGED_FOLDER"
			);
			if (outputs.length !== 1 && Object.keys(outputRewrites,).length === 0) {
				throw new ClientValidationError(
					`Recipe "${sourceName}" has ${outputs.length} dataset outputs; pass explicit outputRewrites instead of outputDataset.`,
					"validation_failed",
				);
			}
			if (outputs[0]) outputRewrites[outputs[0].ref] = opts.outputDataset;
		}
		const inputRewrites: Record<string, string> = {};
		if (opts.inputRewrites) Object.assign(inputRewrites, opts.inputRewrites,);
		const graphRewrites: Record<string, string> = { ...inputRewrites, ...outputRewrites, };
		const payloadRewrites: Record<string, string> = { ...graphRewrites, };
		if (opts.payloadRewrites) Object.assign(payloadRewrites, opts.payloadRewrites,);
		if (
			opts.copyOutputSettings === true
			&& Object.keys(outputRewrites,).length > 1
			&& (opts.outputPath !== undefined || opts.metastoreTableName !== undefined)
		) {
			throw new ClientValidationError(
				"Cannot reuse --path or --metastore-table for multiple cloned output datasets; pass per-output settings in a separate step.",
				"validation_failed",
			);
		}
		const payloadTextRewrites: Record<string, string> = {};
		if (opts.payloadTextRewrites) Object.assign(payloadTextRewrites, opts.payloadTextRewrites,);
		const recipe = cloneRecipeDefinition(source.recipe, opts.name, pk, graphRewrites,);
		const payload = rewritePayload(
			source.payload,
			payloadRewrites,
			payloadTextRewrites,
			opts.recipeType ?? source.recipe.type,
		);
		const copiedOutputDatasets: string[] = [];
		if (opts.copyOutputSettings) {
			for (const [from, to,] of Object.entries(outputRewrites,)) {
				await this.client.datasets.clone(from, to, {
					projectKey: pk,
					path: opts.outputPath,
					metastoreTableName: opts.metastoreTableName,
				},);
				copiedOutputDatasets.push(to,);
			}
		}
		const rnEnc = encodeURIComponent(opts.name,);
		await this.client.post<Record<string, unknown>>(
			`/public/api/projects/${encodeURIComponent(pk,)}/recipes/`,
			{
				recipePrototype: recipe,
				creationSettings: payload !== undefined ? { script: payload, } : {},
			},
		);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${encodeURIComponent(pk,)}/recipes/${rnEnc}`,
			{ recipe, ...(payload !== undefined ? { payload, } : {}), },
		);
		return {
			sourceRecipeName: sourceName,
			recipeName: opts.name,
			projectKey: pk,
			outputRewrites,
			inputRewrites,
			payloadRewrites,
			payloadTextRewrites,
			copiedOutputDatasets,
		};
	}

	/**
		* Download a recipe code payload to a local file.

		* Returns the path to the written file.
		*/
	async downloadCode(
		recipeName: string,
		opts?: { outputPath?: string; projectKey?: string; },
	): Promise<string> {
		const result = await this.get(recipeName, {
			includePayload: true,
			projectKey: opts?.projectKey,
		},);
		if (!result.payload) {
			throw new ClientValidationError(
				`Recipe "${recipeName}" has no code payload.`,
				"validation_failed",
			);
		}
		const safeRecipeName = sanitizeFileName(recipeName, "recipe",);
		const filePath = opts?.outputPath ?? resolve(
			process.cwd(),
			`${safeRecipeName}${inferRecipeCodeExtension(result.recipe.type,)}`,
		);
		await writeFile(filePath, result.payload, "utf-8",);
		return filePath;
	}

	/** Get only the code payload of a recipe as a raw string. */
	async getPayload(
		recipeName: string,
		opts?: { projectKey?: string; },
	): Promise<string> {
		const result = await this.get(recipeName, {
			includePayload: true,
			projectKey: opts?.projectKey,
		},);
		if (!result.payload) {
			throw new ClientValidationError(
				`Recipe "${recipeName}" has no code payload.`,
				"validation_failed",
			);
		}
		return result.payload;
	}

	/** Replace only the code payload of a recipe. */
	async setPayload(
		recipeName: string,
		payload: string,
		opts?: { projectKey?: string; },
	): Promise<void> {
		const current = await this.get(recipeName, {
			includePayload: true,
			projectKey: opts?.projectKey,
		},);
		const enc = this.enc(opts?.projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		await this.client.put(`/public/api/projects/${enc}/recipes/${rnEnc}`, {
			...current,
			payload,
		},);
	}

	/** Delete a recipe. */
	async delete(recipeName: string, projectKey?: string,): Promise<void> {
		const enc = this.enc(projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		await this.client.del(`/public/api/projects/${enc}/recipes/${rnEnc}`,);
	}

	/**
	 * Download a recipe definition as a JSON file.
	 * Returns the path to the written file.
	 */
	async download(
		recipeName: string,
		opts?: { outputPath?: string; projectKey?: string; },
	): Promise<string> {
		const enc = this.enc(opts?.projectKey,);
		const rnEnc = encodeURIComponent(recipeName,);
		const recipe = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${enc}/recipes/${rnEnc}`,
		);
		const safeRecipeName = sanitizeFileName(recipeName, "recipe",);
		const filePath = opts?.outputPath ?? resolve(process.cwd(), `${safeRecipeName}.json`,);
		await writeFile(filePath, JSON.stringify(recipe, null, 2,), "utf-8",);
		return filePath;
	}
}
