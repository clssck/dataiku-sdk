import { writeFile, } from "node:fs/promises";
import { resolve, } from "node:path";
import { DataikuError, } from "../errors.js";
import { RecipeSummaryArraySchema, } from "../schemas.js";
import type { RecipeCreateOptions, RecipeCreateResult, RecipeSummary, } from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";

// ---------------------------------------------------------------------------
// Helpers: type narrowing
// ---------------------------------------------------------------------------

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function asStringArray(value: unknown,): string[] | undefined {
	if (!Array.isArray(value,)) return undefined;
	const out = value.filter((v,): v is string => typeof v === "string" && v.length > 0);
	return out.length > 0 ? out : undefined;
}

function asRecord(value: unknown,): Record<string, unknown> | undefined {
	if (!value || typeof value !== "object" || Array.isArray(value,)) return undefined;
	return value as Record<string, unknown>;
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
		return { ...result, recipe, };
	}

	/** Create a recipe, with optional output dataset provisioning and join configuration. */
	async create(opts: RecipeCreateOptions,): Promise<RecipeCreateResult> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const enc = encodeURIComponent(pk,);

		const { type, payload, outputConnection: rawConnection, joinType: rawJoinType, } = opts;

		// Build inputs/outputs from simple form (inputDatasets + outputDataset) or
		// advanced form (inputs + outputs); both may coexist — simple form wins when
		// the advanced form is absent.
		const inputDatasets = asStringArray(opts.inputDatasets,);
		const outputDataset = asString(opts.outputDataset,);

		let inputs: Record<string, unknown> | undefined = asRecord(opts.inputs,);
		let outputs: Record<string, unknown> | undefined = asRecord(opts.outputs,);

		if (!inputs && inputDatasets) {
			inputs = {
				main: {
					items: inputDatasets.map((ref,) => ({ ref, deps: [], })),
				},
			};
		}
		if (!outputs && outputDataset) {
			outputs = {
				main: {
					items: [{ ref: outputDataset, appendMode: false, },],
				},
			};
		}

		// Auto-generate name if not provided
		const name = opts.name ?? (type && outputDataset ? `${type}_${outputDataset}` : undefined);

		if (!type || !name || !inputs || !outputs) {
			throw new Error(
				"type and (inputDatasets + outputDataset) or (name + inputs + outputs) are required for create.",
			);
		}

		const recipePrototype: Record<string, unknown> = {
			type,
			name,
			projectKey: pk,
			inputs,
			outputs,
		};
		const creationSettings: Record<string, unknown> = {};
		if (payload !== undefined) {
			creationSettings.script = payload;
		}

		const createRecipe = () =>
			this.client.post<Record<string, unknown>>(`/public/api/projects/${enc}/recipes/`, {
				recipePrototype,
				creationSettings,
			},);

		const createdDatasets: string[] = [];
		let usedOutputProvisioningFallback = false;

		try {
			await createRecipe();
		} catch (error) {
			if (!shouldRetryRecipeCreateWithOutputProvisioning(error,)) {
				throw error;
			}
			usedOutputProvisioningFallback = true;

			// Fetch existing datasets to infer output connection and type
			const existingDs = await this.client.get<
				Array<{
					name: string;
					type?: string;
					params?: {
						connection?: string;
						schema?: string;
						catalog?: string;
					};
					managed?: boolean;
				}>
			>(`/public/api/projects/${enc}/datasets/`,);

			let outputConnection = asString(rawConnection,);
			if (!outputConnection) {
				const managedDs = existingDs.find((d,) => d.managed && d.params?.connection);
				if (managedDs?.params?.connection) {
					outputConnection = managedDs.params.connection;
				}
			}

			if (outputConnection) {
				const existingNames = new Set(existingDs.map((d,) => d.name),);
				const connectionSample = existingDs.find(
					(d,) => d.params?.connection === outputConnection && d.type,
				);
				const inferredOutputType = connectionSample?.type ?? "Filesystem";

				const outputRoles = outputs as Record<string, { items?: Array<{ ref?: string; }>; }>;
				for (const role of Object.values(outputRoles,)) {
					for (const item of role.items ?? []) {
						if (item.ref && !existingNames.has(item.ref,)) {
							const datasetBody: Record<string, unknown> = inferredOutputType === "Filesystem"
								? {
									projectKey: pk,
									name: item.ref,
									type: inferredOutputType,
									params: {
										connection: outputConnection,
										path: `\${projectKey}/${item.ref}`,
									},
									formatType: "csv",
									formatParams: {
										style: "excel",
										charset: "utf8",
										separator: "\t",
										quoteChar: '"',
										escapeChar: "\\",
										dateSerializationFormat: "ISO",
										arrayMapFormat: "json",
										parseHeaderRow: true,
										compress: "gz",
									},
									managed: true,
								}
								: {
									projectKey: pk,
									name: item.ref,
									type: inferredOutputType,
									params: {
										connection: outputConnection,
										mode: "table",
										table: item.ref,
										...(connectionSample?.params?.schema
											? { schema: connectionSample.params.schema, }
											: {}),
										...(connectionSample?.params?.catalog
											? { catalog: connectionSample.params.catalog, }
											: {}),
									},
									managed: connectionSample?.managed ?? false,
								};

							await this.client.post(`/public/api/projects/${enc}/datasets/`, datasetBody,);
							existingNames.add(item.ref,);
							createdDatasets.push(item.ref,);
						}
					}
				}
			}

			await createRecipe();
		}

		// For join recipes: configure join conditions after creation
		let joinConfigured = false;
		const joinCols = typeof opts.joinOn === "string" ? [opts.joinOn,] : asStringArray(opts.joinOn,);
		const joinType = asString(rawJoinType,) ?? "LEFT";

		if (type === "join" && joinCols?.length) {
			const rnEnc = encodeURIComponent(name,);
			const full = await this.client.get<{
				recipe: Record<string, unknown>;
				payload: string;
			}>(`/public/api/projects/${enc}/recipes/${rnEnc}`,);

			// DSS returns empty payload for fresh join recipes — construct from scratch
			const inputCount = inputDatasets?.length
				?? (inputs as Record<string, { items?: unknown[]; }>)?.main?.items?.length
				?? 2;

			const virtualInputs: Record<string, unknown>[] = [{ index: 0, preFilter: {}, },];
			for (let i = 1; i < inputCount; i++) {
				virtualInputs.push({
					index: i,
					on: joinCols.map((col,) => ({
						column: col,
						type: "string",
						related: col,
						relatedType: "string",
						maxMatches: 1,
					})),
					joinType,
					preFilter: {},
				},);
			}

			const joinPayload = {
				virtualInputs,
				computedColumns: [],
				postFilter: {},
			};

			// Ensure inputs/outputs are set — DSS may not persist them from the POST for join recipes
			const updatedFull = {
				...full,
				recipe: {
					...(full.recipe as Record<string, unknown>),
					inputs,
					outputs,
				},
				payload: JSON.stringify(joinPayload,),
			};

			await this.client.put(`/public/api/projects/${enc}/recipes/${rnEnc}`, updatedFull,);
			joinConfigured = true;
		}

		return {
			recipeName: name,
			type,
			createdDatasets,
			joinConfigured,
			outputProvisioningFallbackUsed: usedOutputProvisioningFallback,
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
			throw new Error(`Recipe "${recipeName}" was not found or returned an empty definition.`,);
		}
		const mergedRecipe = deepMerge(currentRecipe, asRecord(data.recipe,) ?? {},);
		const merged = { ...current, ...data, recipe: mergedRecipe, };
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${enc}/recipes/${rnEnc}`,
			merged,
		);
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
			throw new Error(`Recipe "${recipeName}" has no code payload.`,);
		}
		const safeRecipeName = sanitizeFileName(recipeName, "recipe",);
		const filePath = opts?.outputPath ?? resolve(
			process.cwd(),
			`${safeRecipeName}${inferRecipeCodeExtension(result.recipe.type,)}`,
		);
		await writeFile(filePath, result.payload, "utf-8",);
		return filePath;
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
