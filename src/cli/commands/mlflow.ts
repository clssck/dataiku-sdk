import type {
	MlflowDeployRunOptions,
	MlflowExperimentsDatasetOptions,
	MlflowInferenceInfo,
	MlflowPredictionType,
} from "../../resources/mlflow-extension.js";
import { num, parseBooleanOption, splitCsvFlag, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { UsageError, } from "../usage.js";

type Flags = Record<string, string | boolean>;

/** `dss mlflow set-inference-info` flags → SDK inference info (shared with --plan). */
export function inferenceInfoFromFlags(runId: string, f: Flags,): MlflowInferenceInfo {
	const predictionType = f["prediction-type"];
	if (typeof predictionType !== "string") {
		throw new UsageError("--prediction-type is required.", "missing_required_flag",);
	}
	const classes = splitCsvFlag(f["classes"],);
	return {
		runId,
		predictionType: predictionType.toUpperCase() as MlflowPredictionType,
		...(classes.length > 0 ? { classes, } : {}),
		...(typeof f["code-env"] === "string" ? { codeEnvName: f["code-env"], } : {}),
		...(typeof f["target"] === "string" ? { target: f["target"], } : {}),
	};
}

/** `dss mlflow deploy-run` flags → SDK deploy options (shared with --plan). */
export function deployRunOptionsFromFlags(
	runId: string,
	savedModelId: string,
	f: Flags,
): MlflowDeployRunOptions {
	const classLabels = splitCsvFlag(f["classes"],);
	const setActive = f["set-active"];
	const threshold = f["binary-classification-threshold"];
	const optimal = f["use-optimal-threshold"];
	const skipReports = f["skip-expensive-reports"];
	return {
		runId,
		savedModelId,
		...(typeof f["version-id"] === "string" ? { versionId: f["version-id"], } : {}),
		...(typeof f["model-name"] === "string" ? { modelName: f["model-name"], } : {}),
		...(typeof f["evaluation-dataset"] === "string"
			? { evaluationDataset: f["evaluation-dataset"], }
			: {}),
		...(typeof f["target"] === "string" ? { targetColumnName: f["target"], } : {}),
		...(classLabels.length > 0 ? { classLabels, } : {}),
		...(typeof f["code-env"] === "string" ? { codeEnvName: f["code-env"], } : {}),
		...(setActive !== undefined ? { activate: parseBooleanOption(setActive, "--set-active",), } : {}),
		...(threshold !== undefined
			? { binaryClassificationThreshold: num(threshold, "--binary-classification-threshold",), }
			: {}),
		...(optimal !== undefined
			? { useOptimalThreshold: parseBooleanOption(optimal, "--use-optimal-threshold",), }
			: {}),
		...(skipReports !== undefined
			? { skipExpensiveReports: parseBooleanOption(skipReports, "--skip-expensive-reports",), }
			: {}),
	};
}

/** `dss mlflow create-experiments-dataset` flags → SDK options (shared with --plan). */
export function experimentsDatasetOptionsFromFlags(
	datasetName: string,
	f: Flags,
): MlflowExperimentsDatasetOptions {
	const experimentIds = splitCsvFlag(f["experiments"],);
	const viewType = f["view-type"];
	const format = f["format"];
	return {
		datasetName,
		...(experimentIds.length > 0 ? { experimentIds, } : {}),
		...(typeof viewType === "string"
			? {
				viewType: viewType.toUpperCase() as NonNullable<MlflowExperimentsDatasetOptions["viewType"]>,
			}
			: {}),
		...(typeof f["filter"] === "string" ? { filter: f["filter"], } : {}),
		...(typeof format === "string"
			? { format: format.toUpperCase() as NonNullable<MlflowExperimentsDatasetOptions["format"]>, }
			: {}),
	};
}

export const mlflowCommands: Record<string, CommandMeta> = withUsage("mlflow", {
	"list-models": {
		handler: (c, a, f,) =>
			c.mlflowExtension.listModels(a[0]!, f["project-key"] as string | undefined,),
		description: "List the models an MLflow experiment-tracking run logged ({runId, artifactPath}).",
		examples: ["dss mlflow list-models RUN_ID",],
	},
	"set-inference-info": {
		handler: async (c, a, f,) => {
			await c.mlflowExtension.setRunInferenceInfo(
				inferenceInfoFromFlags(a[0]!, f,),
				f["project-key"] as string | undefined,
			);
			return { updated: a[0], resource: "mlflow", };
		},
		description:
			"Record an MLflow run's prediction type, classes, code env, and target so deploy-run can use them.",
		examples: [
			"dss mlflow set-inference-info RUN_ID --prediction-type MULTICLASS --classes a,b,c --target species",
		],
	},
	"deploy-run": {
		handler: (c, a, f,) =>
			c.mlflowExtension.deployRun(
				deployRunOptionsFromFlags(a[0]!, a[1]!, f,),
				f["project-key"] as string | undefined,
			),
		description:
			"Deploy an MLflow run's model as a version of an existing saved model (default version id: timestamp); --evaluation-dataset evaluates it. Synchronous: DSS loads the model in its code env, often past 30 s, so raise --request-timeout.",
		examples: [
			"dss mlflow deploy-run RUN_ID SAVED_MODEL_ID --version-id v2 --code-env py311_mlflow --request-timeout 300000",
		],
	},
	"create-experiments-dataset": {
		handler: async (c, a, f,) => {
			await c.mlflowExtension.createExperimentsDataset(
				experimentsDatasetOptionsFromFlags(a[0]!, f,),
				f["project-key"] as string | undefined,
			);
			return { created: a[0], resource: "mlflow", };
		},
		description: "Create a virtual dataset exposing the project's MLflow experiment-tracking runs.",
		examples: ["dss mlflow create-experiments-dataset mlflow_runs --experiments FGXlefMU",],
	},
	"garbage-collect": {
		handler: async (c, _a, f,) => {
			if (executionMode(f,).dryRun) {
				return { dryRun: true, action: "garbage-collect", resource: "mlflow", };
			}
			await c.mlflowExtension.garbageCollect(f["project-key"] as string | undefined,);
			return { garbageCollected: true, resource: "mlflow", };
		},
		description: "Permanently delete the project's MLflow experiments and runs marked as deleted.",
		examples: ["dss mlflow garbage-collect --dry-run",],
	},
	"clean-db": {
		handler: async (c, _a, f,) => {
			if (executionMode(f,).dryRun) return { dryRun: true, action: "clean-db", resource: "mlflow", };
			await c.mlflowExtension.cleanDb(f["project-key"] as string | undefined,);
			return { cleaned: true, resource: "mlflow", };
		},
		description:
			"Admin: delete ALL MLflow experiment-tracking data of the project (experiments, runs, params, metrics, tags). Irreversible.",
		examples: ["dss mlflow clean-db --plan",],
	},
},);
