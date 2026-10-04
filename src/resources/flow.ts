import { ClientValidationError, } from "../errors.js";
import type { FutureWaitResult, } from "../schemas.js";
import { BaseResource, } from "./base.js";
import { type DocumentationTemplate, startDocumentation, } from "./documentation.js";
import { type DssTask, dssTask, type FutureWaitOptions, } from "./futures.js";

/** Schema propagation options (dataikuapi `DSSSchemaPropagationRunBuilder` settings). */
export interface SchemaPropagationOptions {
	projectKey?: string;
	/** Rebuild datasets when propagation needs their data (DSS default: true). */
	autoRebuild?: boolean;
	/** Recipes where propagation stops (dataikuapi `stop_at`). */
	excludedRecipes?: string[];
	/** Recipes always considered OK (dataikuapi `mark_recipe_as_ok`). */
	markAsOkRecipes?: string[];
	/** Partitioned flows: default partition value per dimension when rebuilding. */
	defaultPartitionValuesByDimension?: Record<string, string>;
	/** Partitioned flows: partition per computable, keyed `PROJECTKEY.id`. */
	partitionsByComputable?: Record<string, string>;
	/** Per-recipe-type and per-recipe update options, as dataikuapi sends them. */
	recipeUpdateOptions?: { byType?: Record<string, unknown>; byName?: Record<string, unknown>; };
}

/**
 * Flow-wide tools: schema propagation from a dataset through every downstream
 * recipe, and flow documentation export. Zones and the flow graph live on
 * `flowZones`; per-recipe schema updates on `recipes.updateSchema`.
 */
export class FlowResource extends BaseResource {
	private flowPath(projectKey?: string,): string {
		return `/public/api/projects/${this.enc(projectKey,)}/flow`;
	}

	/**
	 * Start propagating `datasetName`'s schema through the flow
	 * (POST /flow/tools/propagate-schema/, dataikuapi
	 * `new_schema_propagation(...).start()`). Returns the DSS future.
	 */
	async propagateSchema(
		datasetName: string,
		opts: SchemaPropagationOptions = {},
	): Promise<DssTask> {
		const name = datasetName.trim();
		if (name.length === 0) {
			throw new ClientValidationError(
				"propagateSchema requires a dataset name.",
				"validation_failed",
			);
		}
		const pk = this.resolveProjectKey(opts.projectKey,);
		const raw = await this.client.post<unknown>(
			`${this.flowPath(pk,)}/tools/propagate-schema/`,
			schemaPropagationBody(pk, name, opts,),
		);
		return dssTask(raw, "Schema propagation",);
	}

	async propagateSchemaAndWait(
		datasetName: string,
		opts: SchemaPropagationOptions & FutureWaitOptions = {},
	): Promise<FutureWaitResult | DssTask> {
		return this.client.futures.waitTask(await this.propagateSchema(datasetName, opts,), opts,);
	}

	/**
	 * Start generating the flow documentation (docx) from the DSS default
	 * template, an uploaded docx template, or a docx in a managed folder.
	 * The finished future's result carries the `exportId` to download.
	 */
	async generateDocumentation(
		template: DocumentationTemplate = { kind: "default", },
		projectKey?: string,
	): Promise<DssTask> {
		const base = `${this.flowPath(projectKey,)}/documentation`;
		return startDocumentation(
			this.client,
			{
				default: `${base}/generate`,
				file: `${base}/generate-with-template`,
				folder: `${base}/generate-with-template-in-folder`,
			},
			template,
			"Flow documentation generation",
		);
	}

	/** GET a generated flow documentation (docx) as a binary stream. */
	async downloadDocumentation(exportId: string, projectKey?: string,): Promise<Response> {
		return this.client.stream(
			`${this.flowPath(projectKey,)}/documentation/generated/${encodeURIComponent(exportId,)}`,
		);
	}
}

/** Request body of POST /flow/tools/propagate-schema/ (pure; shared with `--plan`). */
export function schemaPropagationBody(
	projectKey: string,
	datasetName: string,
	opts: SchemaPropagationOptions,
): Record<string, unknown> {
	return {
		options: {
			recipeUpdateOptions: {
				byType: opts.recipeUpdateOptions?.byType ?? {},
				byName: opts.recipeUpdateOptions?.byName ?? {},
			},
			defaultPartitionValuesByDimension: opts.defaultPartitionValuesByDimension ?? {},
			partitionsByComputable: opts.partitionsByComputable ?? {},
			excludedRecipes: opts.excludedRecipes ?? [],
			markAsOkRecipes: opts.markAsOkRecipes ?? [],
			autoRebuild: opts.autoRebuild ?? true,
		},
		sources: [{ projectKey, id: datasetName, },],
	};
}
