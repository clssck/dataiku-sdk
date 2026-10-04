import type { DataikuClient, } from "../../client.js";
import { unexpectedResponseError, } from "../../errors.js";
import {
	COMPUTED_MODEL_DIAGNOSTICS,
	type ComputedModelDiagnostic,
	MODEL_DIAGNOSTICS,
	type ModelDiagnostic,
	type ModelDiagnosticsComputation,
	type TrainedModel,
} from "../../resources/trained-model.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { num, parseBooleanOption, requiredJsonInput, splitCsvFlag, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { commandUsage, } from "../syntax.js";
import type { CommandDefinition, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";
import {
	documentationTemplateFromFlags,
	runDocumentationCommand,
	taskWaitOptions,
} from "./documentation.js";

type Flags = Record<string, string | boolean>;

export function diagnosticKindFromFlags(flags: Flags,): ModelDiagnostic {
	const kind = flags["kind"];
	if (typeof kind !== "string" || MODEL_DIAGNOSTICS[kind] !== true) {
		throw new UsageError(
			`--kind must be one of ${Object.keys(MODEL_DIAGNOSTICS,).join(", ",)}.`,
			"invalid_enum",
		);
	}
	return kind as ModelDiagnostic;
}

/** `compute-diagnostics` flags → kind, features, and computation settings (shared with --plan). */
export function diagnosticsComputationFromFlags(flags: Flags,): {
	kind: ComputedModelDiagnostic;
	features: string[];
	computation: ModelDiagnosticsComputation;
} {
	const kind = flags["kind"];
	if (typeof kind !== "string" || COMPUTED_MODEL_DIAGNOSTICS[kind] !== true) {
		throw new UsageError(
			`--kind must be one of ${Object.keys(COMPUTED_MODEL_DIAGNOSTICS,).join(", ",)}.`,
			"invalid_enum",
		);
	}
	const sampleSize = num(flags["sample-size"], "--sample-size",);
	const randomState = num(flags["random-state"], "--random-state",);
	const nJobs = num(flags["n-jobs"], "--n-jobs",);
	return {
		kind: kind as ComputedModelDiagnostic,
		features: splitCsvFlag(flags["features"],),
		computation: {
			...(sampleSize !== undefined ? { sampleSize, } : {}),
			...(randomState !== undefined ? { randomState, } : {}),
			...(nJobs !== undefined ? { nJobs, } : {}),
		},
	};
}

export interface TrainedModelCommandSpec {
	resource: "ml-task" | "saved-model";
	/** Positional ids in examples, e.g. `ANALYSIS_ID TASK_ID MODEL_ID`. */
	ids: string;
	/** What one model is called in descriptions: "lab model" or "saved-model version". */
	noun: string;
	model: (client: DataikuClient, args: string[], projectKey: string | undefined,) => TrainedModel;
	downloadDocumentation: (
		client: DataikuClient,
		exportId: string,
		projectKey: string | undefined,
	) => Promise<Response>;
}

/** Write a binary export to `--output`; DSS answering without a body is an unexpected response. */
async function writeExport(res: Response, out: string, operation: string,) {
	if (!res.body) throw unexpectedResponseError(`${operation} response did not include a body`,);
	return { path: out, bytes: await writeResponseToFile(out, res,), };
}

function requiredOutput(flags: Flags,): string {
	const out = flags["output"];
	if (typeof out !== "string") {
		throw new UsageError("--output PATH is required.", "missing_required_flag",);
	}
	return out;
}

/**
 * The actions lab models and saved-model versions share, over one
 * {@link TrainedModel}: user meta, scoring exports, diagnostics, documentation.
 */
export function trainedModelCommands(
	spec: TrainedModelCommandSpec,
): Record<string, CommandDefinition> {
	const { resource, ids, noun, } = spec;
	const argCount = ids.split(" ",).length;
	const model = (c: DataikuClient, a: string[], f: Flags, action: string,) => {
		requireArgs(a, argCount, commandUsage(resource, action,),);
		return spec.model(c, a, f["project-key"] as string | undefined,);
	};
	return {
		"set-user-meta": {
			handler: async (c, a, f,) => {
				const target = model(c, a, f, "set-user-meta",);
				const userMeta = requiredJsonInput(
					f,
					"User-meta JSON is required via --data, --data-file, or --stdin.",
				);
				if (executionMode(f,).dryRun) {
					return {
						dryRun: true,
						action: "set-user-meta",
						resource,
						ids: a.slice(0, argCount,),
						payload: userMeta,
					};
				}
				await target.setUserMeta(userMeta,);
				return { updated: a[argCount - 1], resource, };
			},
			description:
				`Update the user metadata (name, description, labels, ...) of a ${noun}. Send only the "userMeta" field of its details, edited.`,
			examples: [
				`dss ${resource} set-user-meta ${ids} --data '{"name":"Churn model","description":"v2"}'`,
			],
		},
		"download-scoring-jar": {
			handler: async (c, a, f,) => {
				const target = model(c, a, f, "download-scoring-jar",);
				const out = requiredOutput(f,);
				const fullClassName = f["full-class-name"];
				const res = await target.downloadScoringJar({
					...(typeof fullClassName === "string" ? { fullClassName, } : {}),
					...(f["include-libs"] !== undefined
						? { includeLibs: parseBooleanOption(f["include-libs"], "--include-libs",), }
						: {}),
				},);
				return writeExport(res, out, "Scoring JAR download",);
			},
			description:
				`Download the optimized scoring JAR of a ${noun} (license-gated server side). --full-class-name sets the Java class; --include-libs toggles bundling the scoring libraries.`,
			examples: [`dss ${resource} download-scoring-jar ${ids} --output ./scoring.jar`,],
		},
		"download-scoring-pmml": {
			handler: async (c, a, f,) => {
				const target = model(c, a, f, "download-scoring-pmml",);
				const out = requiredOutput(f,);
				return writeExport(await target.downloadScoringPmml(), out, "Scoring PMML download",);
			},
			description: `Download the PMML scoring file of a ${noun} (license-gated server side).`,
			examples: [`dss ${resource} download-scoring-pmml ${ids} --output ./model.pmml`,],
		},
		diagnostics: {
			handler: (c, a, f,) => model(c, a, f, "diagnostics",).get(diagnosticKindFromFlags(f,),),
			description:
				`Read computed diagnostics of a ${noun}: subpopulation-analyses, partial-dependencies, timeseries-residuals, or (forecasting) per-timeseries-metrics / per-timeseries-evaluation-forecasts.`,
			examples: [`dss ${resource} diagnostics ${ids} --kind partial-dependencies`,],
		},
		"compute-diagnostics": {
			handler: async (c, a, f,) => {
				const target = model(c, a, f, "compute-diagnostics",);
				const { kind, features, computation, } = diagnosticsComputationFromFlags(f,);
				const task = await target.compute(kind, features, computation,);
				return f["wait"] === true ? c.futures.waitTask(task, taskWaitOptions(f,),) : task;
			},
			description:
				`Compute subpopulation-analyses or partial-dependencies over --features COLS (--sample-size N, default 10000), or timeseries-residuals, for a ${noun}. Returns the DSS future; --wait waits and returns the result.`,
			examples: [
				`dss ${resource} compute-diagnostics ${ids} --kind partial-dependencies --features age,income --wait`,
			],
		},
		"generate-documentation": {
			handler: async (c, a, f,) => {
				const target = model(c, a, f, "generate-documentation",);
				const pk = f["project-key"] as string | undefined;
				const task = await target.generateDocumentation(documentationTemplateFromFlags(f,),);
				return runDocumentationCommand(
					c,
					f,
					task,
					(exportId,) => spec.downloadDocumentation(c, exportId, pk,),
				);
			},
			description:
				`Generate the model documentation (Word docx) of a ${noun} from the DSS default template, an uploaded --template-file, or a template in a managed folder (--folder FOLDER_ID --path PATH). Returns the DSS future; --wait waits; --output PATH waits and downloads.`,
			examples: [`dss ${resource} generate-documentation ${ids} --output model.docx`,],
		},
		"download-documentation": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, commandUsage(resource, "download-documentation",),);
				const out = requiredOutput(f,);
				const res = await spec.downloadDocumentation(c, a[0]!, f["project-key"] as string | undefined,);
				return writeExport(res, out, "Model documentation download",);
			},
			description:
				"Download a generated model documentation by the exportId from a finished generate-documentation future.",
			examples: [`dss ${resource} download-documentation EXPORT_ID --output model.docx`,],
		},
	};
}
