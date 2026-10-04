import type { DataikuClient, } from "../client.js";
import { ClientValidationError, } from "../errors.js";
import { requireObject, } from "./base.js";
import { type DocumentationTemplate, startDocumentation, } from "./documentation.js";
import { type DssTask, dssTask, } from "./futures.js";

/** Sampling and execution settings of a diagnostics computation (dataikuapi `computationParams`). */
export interface ModelDiagnosticsComputation {
	/** Records sampled from the test set (DSS default 10000). */
	sampleSize?: number;
	randomState?: number;
	nJobs?: number;
	debugMode?: boolean;
}

/** Diagnostics a trained model computes on demand, then serves as results. */
export type ComputedModelDiagnostic =
	| "subpopulation-analyses"
	| "partial-dependencies"
	| "timeseries-residuals";

/** Every diagnostics result a trained model serves. */
export type ModelDiagnostic =
	| ComputedModelDiagnostic
	| "per-timeseries-metrics"
	| "per-timeseries-evaluation-forecasts";

/** Diagnostics that are started with POST and return a DSS future. */
export const COMPUTED_MODEL_DIAGNOSTICS: Record<string, true> = {
	"subpopulation-analyses": true,
	"partial-dependencies": true,
	"timeseries-residuals": true,
};

/** Diagnostics results served by GET (computed ones plus per-series results of forecasting models). */
export const MODEL_DIAGNOSTICS: Record<string, true> = {
	...COMPUTED_MODEL_DIAGNOSTICS,
	"per-timeseries-metrics": true,
	"per-timeseries-evaluation-forecasts": true,
};

/**
 * Request body that starts a diagnostics computation. Subpopulation analyses
 * and partial dependencies take the features to analyse; timeseries
 * residuals take no body.
 */
export function modelDiagnosticsBody(
	kind: ComputedModelDiagnostic,
	features: string[],
	computation: ModelDiagnosticsComputation = {},
): Record<string, unknown> | undefined {
	if (kind === "timeseries-residuals") return undefined;
	if (features.length === 0) {
		throw new ClientValidationError(`${kind} needs at least one feature.`, "missing_required_arg",);
	}
	return {
		features,
		computationParams: {
			...(computation.sampleSize !== undefined ? { sample_size: computation.sampleSize, } : {}),
			...(computation.randomState !== undefined ? { random_state: computation.randomState, } : {}),
			...(computation.nJobs !== undefined ? { n_jobs: computation.nJobs, } : {}),
			...(computation.debugMode !== undefined ? { debug_mode: computation.debugMode, } : {}),
		},
	};
}

/** Query options for the scoring JAR export (GET .../scoring-jar). */
export interface ScoringJarOptions {
	fullClassName?: string;
	includeLibs?: boolean;
}

/**
 * One trained model: a visual-ML lab model (`mlTasks.model(...)`) or a
 * saved-model version (`savedModels.version(...)`). Both expose the same
 * routes (user meta, scoring exports, diagnostics, documentation) under
 * different bases. Generated documents are downloaded from the owning
 * resource (`mlTasks.downloadModelDocumentation`, `savedModels.downloadDocumentation`).
 */
export class TrainedModel {
	constructor(
		private readonly client: DataikuClient,
		/** Route of the trained model, e.g. `.../models/lab/{a}/{t}/models/{id}`. */
		private readonly modelPath: string,
	) {}

	/** Replace the user metadata: only the `userMeta` field of the model details, edited. */
	async setUserMeta(userMeta: Record<string, unknown>,): Promise<void> {
		await this.client.putVoid(`${this.modelPath}/user-meta`, requireObject(userMeta, "userMeta",),);
	}

	/** GET the Java scoring JAR (license-gated, optimized-scoring models only) as a binary stream. */
	async downloadScoringJar(options: ScoringJarOptions = {},): Promise<Response> {
		const query = new URLSearchParams();
		if (options.fullClassName !== undefined) query.set("fullClassName", options.fullClassName,);
		if (options.includeLibs !== undefined) query.set("includeLibs", String(options.includeLibs,),);
		return this.client.stream(`${this.modelPath}/scoring-jar${query.size > 0 ? `?${query}` : ""}`,);
	}

	/** GET the PMML export (license-gated, PMML-compatible models only) as a stream. */
	async downloadScoringPmml(): Promise<Response> {
		return this.client.stream(`${this.modelPath}/scoring-pmml`,);
	}

	/**
	 * Start computing subpopulation analyses or partial dependencies over
	 * `features`, or timeseries residuals. Returns the DSS future.
	 */
	async compute(
		kind: ComputedModelDiagnostic,
		features: string[] = [],
		computation: ModelDiagnosticsComputation = {},
	): Promise<DssTask> {
		if (COMPUTED_MODEL_DIAGNOSTICS[kind] !== true) {
			throw new ClientValidationError(
				`Computed diagnostic must be one of ${Object.keys(COMPUTED_MODEL_DIAGNOSTICS,).join(", ",)}.`,
				"invalid_enum",
			);
		}
		const raw = await this.client.post<unknown>(
			`${this.modelPath}/${kind}`,
			modelDiagnosticsBody(kind, features, computation,),
		);
		return dssTask(raw, `Computing ${kind}`,);
	}

	/** Results computed so far (subpopulation analyses, partial dependencies, residuals, per-series metrics or forecasts). */
	async get(kind: ModelDiagnostic,): Promise<unknown> {
		if (MODEL_DIAGNOSTICS[kind] !== true) {
			throw new ClientValidationError(
				`Diagnostic must be one of ${Object.keys(MODEL_DIAGNOSTICS,).join(", ",)}.`,
				"invalid_enum",
			);
		}
		return this.client.get<unknown>(`${this.modelPath}/${kind}`,);
	}

	/** Start generating the model documentation (docx); the finished result carries `exportId`. */
	async generateDocumentation(
		template: DocumentationTemplate = { kind: "default", },
	): Promise<DssTask> {
		const base = `${this.modelPath}/generate-documentation-from`;
		return startDocumentation(
			this.client,
			{
				default: `${base}-default-template`,
				file: `${base}-custom-template`,
				folder: `${base}-template-in-folder`,
			},
			template,
			"Model documentation generation",
		);
	}
}
