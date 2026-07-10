import { requiredJsonInput, requiredStringFlag, } from "../coerce.js";
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

export const mlTaskCommands: Record<string, CommandMeta> = {
	create: {
		handler: async (c, a, f,) => {
			const usage =
				"dss ml-task create <analysisId> --task-type PREDICTION|CLUSTERING [--target COLUMN] [--prediction-type TYPE] [--backend-type TYPE] [--guess-policy POLICY] [--project-key KEY]";
			requireArgs(a, 1, usage,);
			const projectKey = f["project-key"] as string | undefined;
			const normalizedTaskType = taskType(f, usage,);
			const targetVariable = optionalStringFlag(f, "target",);
			if (normalizedTaskType === "PREDICTION" && targetVariable === undefined) {
				throw new UsageError(
					"--target is required for PREDICTION ML tasks.",
					"missing_required_flag",
				);
			}
			const options = {
				analysisId: a[0],
				taskType: normalizedTaskType,
				targetVariable,
				predictionType: optionalStringFlag(f, "prediction-type",),
				backendType: optionalStringFlag(f, "backend-type",),
				guessPolicy: optionalStringFlag(f, "guess-policy",),
				projectKey,
			};
			const created = await c.mlTasks.create(options,);
			return { created: created.mlTaskId, resource: "ml-task", analysisId: a[0], ...created, };
		},
		usage:
			"dss ml-task create <analysisId> --task-type PREDICTION|CLUSTERING [--target COLUMN] [--prediction-type TYPE] [--backend-type TYPE] [--guess-policy POLICY] [--project-key KEY]",
		description: "Create a prediction or clustering task in an analysis.",
		examples: [
			"dss ml-task create ANALYSIS_ID --task-type prediction --target churn --project-key PROJECT",
		],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 2, "dss ml-task status <analysisId> <mlTaskId> [--project-key KEY]",);
			return c.mlTasks.status(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss ml-task status <analysisId> <mlTaskId> [--project-key KEY]",
		description: "Get a Visual ML task's current status.",
		examples: ["dss ml-task status ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"get-settings": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss ml-task get-settings <analysisId> <mlTaskId> [--project-key KEY]",
			);
			return c.mlTasks.getSettings(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss ml-task get-settings <analysisId> <mlTaskId> [--project-key KEY]",
		description: "Get a Visual ML task's settings.",
		examples: ["dss ml-task get-settings ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"set-settings": {
		handler: (c, a, f,) => {
			const usage =
				"dss ml-task set-settings <analysisId> <mlTaskId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]";
			requireArgs(a, 2, usage,);
			const settings = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (ML task settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			return c.mlTasks.saveSettings(a[0], a[1], settings, projectKey,);
		},
		usage:
			"dss ml-task set-settings <analysisId> <mlTaskId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a Visual ML task's settings from JSON input.",
		examples: [
			"dss ml-task set-settings ANALYSIS_ID TASK_ID --data-file settings.json --project-key PROJECT",
		],
	},
	train: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss ml-task train <analysisId> <mlTaskId> [--session-name NAME] [--wait] [--project-key KEY]",
			);
			const options = {
				analysisId: a[0],
				mlTaskId: a[1],
				sessionName: optionalStringFlag(f, "session-name",),
				wait: f["wait"] === true,
				projectKey: f["project-key"] as string | undefined,
			};
			return c.mlTasks.train(options,);
		},
		usage:
			"dss ml-task train <analysisId> <mlTaskId> [--session-name NAME] [--wait] [--project-key KEY]",
		description: "Start ML task training, optionally waiting for trained model IDs.",
		examples: [
			"dss ml-task train ANALYSIS_ID TASK_ID --session-name baseline --wait --project-key PROJECT",
		],
	},
	"list-models": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss ml-task list-models <analysisId> <mlTaskId> [--project-key KEY]",
			);
			return c.mlTasks.listTrainedModels(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss ml-task list-models <analysisId> <mlTaskId> [--project-key KEY]",
		description: "List trained models for a Visual ML task.",
		examples: ["dss ml-task list-models ANALYSIS_ID TASK_ID --project-key PROJECT",],
	},
	"model-details": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				3,
				"dss ml-task model-details <analysisId> <mlTaskId> <modelId> [--project-key KEY]",
			);
			return c.mlTasks.trainedModelDetails(
				a[0],
				a[1],
				a[2],
				f["project-key"] as string | undefined,
			);
		},
		usage: "dss ml-task model-details <analysisId> <mlTaskId> <modelId> [--project-key KEY]",
		description: "Get details for one trained model.",
		examples: [
			"dss ml-task model-details ANALYSIS_ID TASK_ID MODEL_ID --project-key PROJECT",
		],
	},
	deploy: {
		handler: async (c, a, f,) => {
			const usage =
				"dss ml-task deploy <analysisId> <mlTaskId> <modelId> --model-name NAME --train-dataset DATASET [--test-dataset DATASET] [--dry-run] [--project-key KEY]";
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
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "deploy",
					resource: "ml-task",
					...options,
				};
			}
			return c.mlTasks.deployToFlow(options,);
		},
		usage:
			"dss ml-task deploy <analysisId> <mlTaskId> <modelId> --model-name NAME --train-dataset DATASET [--test-dataset DATASET] [--dry-run] [--project-key KEY]",
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
				"dss ml-task delete <analysisId> <mlTaskId> [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
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
		usage: "dss ml-task delete <analysisId> <mlTaskId> [--dry-run] [--project-key KEY]",
		description: "Delete a Visual ML task.",
		examples: [
			"dss ml-task delete ANALYSIS_ID TASK_ID --project-key PROJECT",
			"dss ml-task delete ANALYSIS_ID TASK_ID --dry-run --project-key PROJECT",
		],
	},
};
