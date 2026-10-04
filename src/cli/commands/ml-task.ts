import type { MlTaskCreateFields, MlTaskReguessOptions, } from "../../resources/ml-tasks.js";
import {
	num,
	parseBooleanOption,
	requiredJsonInput,
	requiredStringFlag,
	splitCsvFlag,
} from "../coerce.js";
import { executionMode, } from "../flags.js";
import { trainedModelCommands, } from "../helpers/trained-model.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

function optionalStringFlag(
	flags: Record<string, string | boolean>,
	name: string,
): string | undefined {
	const value = flags[name];
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`--${name} requires a non-empty value.`, "usage_error",);
	}
	return value.trim();
}

function taskType(
	flags: Record<string, string | boolean>,
	usage: string,
): "PREDICTION" | "CLUSTERING" {
	const normalized = requiredStringFlag(flags, "task-type", usage,).toUpperCase();
	if (normalized === "PREDICTION" || normalized === "CLUSTERING") return normalized;
	throw new UsageError(
		"--task-type must be PREDICTION or CLUSTERING.",
		"invalid_enum",
	);
}

/** Task definition flags shared by `create`, `create-for-dataset`, and their --plan. */
export function mlTaskFieldsFromFlags(
	flags: Record<string, string | boolean>,
	usage: string,
): MlTaskCreateFields {
	const normalizedTaskType = taskType(flags, usage,);
	const targetVariable = optionalStringFlag(flags, "target",);
	if (normalizedTaskType === "PREDICTION" && targetVariable === undefined) {
		throw new UsageError(
			"--target is required for PREDICTION ML tasks.",
			"missing_required_flag",
		);
	}
	const timeseriesIdentifiers = splitCsvFlag(flags["timeseries-ids"],);
	return {
		taskType: normalizedTaskType,
		targetVariable,
		predictionType: optionalStringFlag(flags, "prediction-type",),
		timeVariable: optionalStringFlag(flags, "time-variable",),
		...(timeseriesIdentifiers.length > 0 ? { timeseriesIdentifiers, } : {}),
		backendType: optionalStringFlag(flags, "backend-type",),
		guessPolicy: optionalStringFlag(flags, "guess-policy",),
	};
}

/** `reguess` flags → SDK options (shared with --plan). */
export function mlTaskReguessOptionsFromFlags(
	flags: Record<string, string | boolean>,
): MlTaskReguessOptions {
	const timeseriesIdentifiers = splitCsvFlag(flags["timeseries-ids"],);
	return {
		predictionType: optionalStringFlag(flags, "prediction-type",),
		targetVariable: optionalStringFlag(flags, "target",),
		timeVariable: optionalStringFlag(flags, "time-variable",),
		...(timeseriesIdentifiers.length > 0 ? { timeseriesIdentifiers, } : {}),
		...(flags["full-reguess"] !== undefined
			? { fullReguess: parseBooleanOption(flags["full-reguess"], "--full-reguess",), }
			: {}),
	};
}

export const mlTaskCommands: Record<string, CommandMeta> = withUsage("ml-task", {
	create: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("ml-task", "create",);
			requireArgs(a, 1, usage,);
			const created = await c.mlTasks.create({
				analysisId: a[0]!,
				...mlTaskFieldsFromFlags(f, usage,),
				projectKey: f["project-key"] as string | undefined,
			},);
			return { created: created.mlTaskId, resource: "ml-task", analysisId: a[0], ...created, };
		},
		description: "Create a prediction or clustering task in an analysis.",
		examples: [
			"dss ml-task create ANALYSIS_ID --task-type prediction --target churn --project-key PROJECT",
		],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("ml-task", "status",),);
			return c.mlTasks.status(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "Get a Visual ML task's current status.",
		examples: ["dss ml-task status ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"get-settings": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("ml-task", "get-settings",),
			);
			return c.mlTasks.getSettings(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "Get a Visual ML task's settings.",
		examples: ["dss ml-task get-settings ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"set-settings": {
		handler: (c, a, f,) => {
			const usage = commandUsage("ml-task", "set-settings",);
			requireArgs(a, 2, usage,);
			const settings = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (ML task settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			return c.mlTasks.saveSettings(a[0], a[1], settings, projectKey,);
		},
		description: "Replace a Visual ML task's settings from JSON input.",
		examples: [
			"dss ml-task set-settings ANALYSIS_ID TASK_ID --data-file settings.json --project-key PROJECT",
		],
	},
	train: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("ml-task", "train",);
			requireArgs(a, 2, usage,);
			const options = {
				analysisId: a[0],
				mlTaskId: a[1],
				sessionName: optionalStringFlag(f, "session-name",),
				wait: f["wait"] === true,
				timeoutMs: num(f["timeout"], "--timeout",),
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				projectKey: f["project-key"] as string | undefined,
			};
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "train",
					resource: "ml-task",
					...options,
				};
			}
			return c.mlTasks.train(options,);
		},
		description:
			"Start ML task training, optionally waiting for trained model IDs. --timeout bounds only the wait phase after DSS accepts training; an invalid timeout fails before any request.",
		examples: [
			"dss ml-task train ANALYSIS_ID TASK_ID --session-name baseline --wait --timeout 60000 --project-key PROJECT",
			"dss ml-task train ANALYSIS_ID TASK_ID --wait --timeout 0 --poll-interval 1000 --project-key PROJECT",
		],
	},
	"list-models": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("ml-task", "list-models",),
			);
			return c.mlTasks.listTrainedModels(a[0], a[1], f["project-key"] as string | undefined,);
		},
		description: "List trained models for a Visual ML task.",
		examples: ["dss ml-task list-models ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"model-details": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				commandUsage("ml-task", "model-details",),
			);
			return c.mlTasks.trainedModelDetails(
				a[0],
				a[1],
				a[2],
				f["project-key"] as string | undefined,
			);
		},
		description: "Get details for one trained model.",
		examples: [
			"dss ml-task model-details ANALYSIS_ID TASK_ID MODEL_ID --project-key PROJECT",
		],
	},
	deploy: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("ml-task", "deploy",);
			requireArgs(a, 3, usage,);
			const options = {
				analysisId: a[0],
				mlTaskId: a[1],
				modelId: a[2],
				modelName: requiredStringFlag(f, "model-name", usage,),
				trainDatasetRef: requiredStringFlag(f, "train-dataset", usage,),
				testDatasetRef: optionalStringFlag(f, "test-dataset",),
				projectKey: f["project-key"] as string | undefined,
			};
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "deploy",
					resource: "ml-task",
					...options,
				};
			}
			return c.mlTasks.deployToFlow(options,);
		},
		description: "Deploy a trained model to the Flow as a saved model.",
		examples: [
			"dss ml-task deploy ANALYSIS_ID TASK_ID MODEL_ID --model-name churn-model --train-dataset train --test-dataset test --project-key PROJECT",
			"dss ml-task deploy ANALYSIS_ID TASK_ID MODEL_ID --model-name churn-model --train-dataset train --dry-run --project-key PROJECT",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("ml-task", "delete",),
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "delete",
					resource: "ml-task",
					analysisId: a[0],
					mlTaskId: a[1],
					projectKey,
				};
			}
			await c.mlTasks.delete(a[0], a[1], projectKey,);
			return { deleted: a[1], resource: "ml-task", analysisId: a[0], };
		},
		description: "Delete a Visual ML task.",
		examples: [
			"dss ml-task delete ANALYSIS_ID TASK_ID --project-key PROJECT",
			"dss ml-task delete ANALYSIS_ID TASK_ID --dry-run --project-key PROJECT",
		],
	},
	list: {
		handler: (c, _a, f,) => c.mlTasks.list(f["project-key"] as string | undefined,),
		description:
			"List every ML task of the project with its visual analysis (one analysis: dss analysis list-ml-tasks).",
		examples: ["dss ml-task list",],
	},
	"create-for-dataset": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("ml-task", "create-for-dataset",);
			requireArgs(a, 1, usage,);
			const created = await c.mlTasks.createForDataset({
				inputDataset: a[0]!,
				...mlTaskFieldsFromFlags(f, usage,),
				projectKey: f["project-key"] as string | undefined,
			},);
			return { created: created.mlTaskId, resource: "ml-task", ...created, };
		},
		description:
			"Create a new visual analysis on a dataset with one prediction, clustering, or forecasting task (--prediction-type TIMESERIES_FORECAST --time-variable COL [--timeseries-ids COLS]). DSS guesses settings asynchronously: poll status until guessing is false.",
		examples: [
			"dss ml-task create-for-dataset customers --task-type prediction --target churn",
			"dss ml-task create-for-dataset sales --task-type prediction --target amount --prediction-type TIMESERIES_FORECAST --time-variable day",
		],
	},
	reguess: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("ml-task", "reguess",),);
			await c.mlTasks.reguess(
				a[0]!,
				a[1]!,
				mlTaskReguessOptionsFromFlags(f,),
				f["project-key"] as string | undefined,
			);
			return { reguessed: a[1], resource: "ml-task", analysisId: a[0], };
		},
		description:
			"Re-guess an ML task's settings: all of them, or after changing one core parameter (--prediction-type, --target, --time-variable, --timeseries-ids), only the impacted ones unless --full-reguess true.",
		examples: ["dss ml-task reguess ANALYSIS_ID TASK_ID --target churn_flag",],
	},
	"reguess-forecasting": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("ml-task", "reguess-forecasting",),);
			await c.mlTasks.reguessForecasting(
				a[0]!,
				a[1]!,
				requiredJsonInput(
					f,
					"Forecasting parameters are required via --data, --data-file, or --stdin (forecastHorizon, validationHorizons, timestepParams, updateAlgorithmSettings).",
				),
				f["project-key"] as string | undefined,
			);
			return { reguessed: a[1], resource: "ml-task", analysisId: a[0], };
		},
		description:
			"Time series forecasting tasks: change forecasting parameters (forecastHorizon, validationHorizons, timestepParams {timeunit, numberOfTimeunits, ...}, updateAlgorithmSettings) and re-guess impacted algorithm settings.",
		examples: [
			'dss ml-task reguess-forecasting ANALYSIS_ID TASK_ID --data \'{"forecastHorizon":7,"updateAlgorithmSettings":true}\'',
		],
	},
	...trainedModelCommands({
		resource: "ml-task",
		ids: "ANALYSIS_ID TASK_ID MODEL_ID",
		noun: "lab model",
		model: (c, a, pk,) => c.mlTasks.model(a[0]!, a[1]!, a[2]!, pk,),
		downloadDocumentation: (c, exportId, pk,) => c.mlTasks.downloadModelDocumentation(exportId, pk,),
	},),
},);
