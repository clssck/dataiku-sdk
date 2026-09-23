import { unexpectedResponseError, } from "../../errors.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { json, jsonInput, parseBooleanOption, requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/**
 * Mutating actions accept --plan (buildMutationPlan in contract.ts owns the
 * --plan path centrally); the handlers below keep --dry-run support with a
 * zero-request planned payload, mirroring set-active/delete.
 */
export const savedModelCommands: Record<string, CommandMeta> = withUsage("saved-model", {
	list: {
		handler: (c, _a, f,) => c.savedModels.list(f["project-key"] as string | undefined,),
		description: "List saved models in a project.",
		examples: ["dss saved-model list --project-key PROJECT",],
	},
	"create-external": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "create-external",);
			requireArgs(a, 1, usage,);
			const savedModelType = (f["type"] as string | undefined)?.trim();
			if (savedModelType !== "MLFLOW_PYFUNC" && savedModelType !== "PROXY_MODEL") {
				throw new UsageError(
					"--type must be MLFLOW_PYFUNC or PROXY_MODEL.",
					"invalid_enum",
				);
			}
			const predictionTypeRaw = f["prediction-type"] as string | undefined;
			const predictionType = predictionTypeRaw === undefined || predictionTypeRaw === ""
				? undefined
				: predictionTypeRaw.trim();
			if (
				predictionType !== undefined
				&& predictionType !== "BINARY_CLASSIFICATION"
				&& predictionType !== "MULTICLASS"
				&& predictionType !== "REGRESSION"
			) {
				throw new UsageError(
					"--prediction-type must be BINARY_CLASSIFICATION, MULTICLASS, or REGRESSION.",
					"invalid_enum",
				);
			}
			const configuration = f["configuration"] !== undefined
				? requiredJsonInput(
					f,
					"--configuration must be a JSON object (--data/--data-file/--stdin).",
				)
				: jsonInput(f,);
			if (savedModelType === "PROXY_MODEL" && configuration === undefined) {
				throw new UsageError(
					'--configuration JSON is required for PROXY_MODEL (e.g. {"protocol":"sagemaker","region":"eu-west-1"}).',
					"missing_required_flag",
				);
			}
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "create-external",
					resource: "saved-model",
					id: a[0],
					projectKey,
					payload: {
						savedModelType,
						name: a[0],
						...(predictionType !== undefined ? { predictionType, } : {}),
						...(configuration !== undefined
							? { proxyModelConfiguration: configuration as Record<string, unknown>, }
							: {}),
					},
				};
			}
			return c.savedModels.createExternal({
				savedModelType,
				name: a[0]!,
				...(predictionType !== undefined ? { predictionType, } : {}),
				...(configuration !== undefined
					? { proxyModelConfiguration: configuration as Record<string, unknown>, }
					: {}),
			}, projectKey,);
		},
		description:
			"Create a saved model for MLflow or external (proxy) models. --type selects savedModelType; --prediction-type is BINARY_CLASSIFICATION, MULTICLASS, or REGRESSION; --configuration (JSON via --data/--data-file/--stdin) is the PROXY_MODEL protocol configuration forwarded as proxyModelConfiguration.",
		examples: [
			"dss saved-model create-external 'MLflow Demo' --type MLFLOW_PYFUNC --prediction-type BINARY_CLASSIFICATION",
			'dss saved-model create-external \'SageMaker Model\' --type PROXY_MODEL --prediction-type BINARY_CLASSIFICATION --data \'{"protocol":"sagemaker","region":"eu-west-1"}\'',
		],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("saved-model", "get",),);
			return c.savedModels.get(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get a saved model.",
		examples: ["dss saved-model get MODEL_ID --project-key PROJECT",],
	},
	"update-settings": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "update-settings",);
			requireArgs(a, 1, usage,);
			const settings = requiredJsonInput(
				f,
				"Saved-model settings JSON is required via --data, --data-file, or --stdin (GET-then-PUT the full settings document).",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "update-settings",
					resource: "saved-model",
					id: a[0],
					projectKey,
					payload: settings,
				};
			}
			return c.savedModels.updateSettings(a[0]!, settings, projectKey,).then(() => ({
				updated: a[0],
				resource: "saved-model",
			}));
		},
		description:
			"Save the settings of a saved model. The payload must be the complete settings document as returned by `saved-model get` (GET-then-PUT workflow, per the official client).",
		examples: [
			"dss saved-model get MODEL_ID --project-key P | jq . > settings.json && dss saved-model update-settings MODEL_ID --data-file settings.json --project-key P",
		],
	},
	"list-versions": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("saved-model", "list-versions",),);
			return c.savedModels.listVersions(a[0], f["project-key"] as string | undefined,);
		},
		description: "List versions of a saved model.",
		examples: ["dss saved-model list-versions MODEL_ID --project-key PROJECT",],
	},
	"version-details": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("saved-model", "version-details",),
			);
			return c.savedModels.versionDetails(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
			);
		},
		description: "Get details for one saved-model version.",
		examples: [
			"dss saved-model version-details MODEL_ID VERSION_ID --project-key PROJECT",
		],
	},
	"version-snippet": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("saved-model", "version-snippet",),
			);
			return c.savedModels.versionSnippet(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
			);
		},
		description: "Get the snippet (short summary) for one saved-model version.",
		examples: [
			"dss saved-model version-snippet MODEL_ID VERSION_ID --project-key PROJECT",
		],
	},
	"set-active": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("saved-model", "set-active",),
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "set-active",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: { versionId: a[1], },
				};
			}
			return c.savedModels.setActiveVersion(a[0], a[1], projectKey,);
		},
		description: "Set the active version of a saved model.",
		examples: [
			"dss saved-model set-active MODEL_ID VERSION_ID --project-key PROJECT",
			"dss saved-model set-active MODEL_ID VERSION_ID --dry-run --project-key PROJECT",
		],
	},
	"delete-versions": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "delete-versions",);
			requireArgs(a, 2, usage,);
			const versions = a[1]!.split(",",).map((v,) => v.trim()).filter((v,) => v.length > 0);
			if (versions.length === 0) {
				throw new UsageError(
					"<versionsCSV> must contain at least one version id.",
					"validation_failed",
				);
			}
			const removeIntermediate = parseBooleanOption(f["remove-intermediate"], "--remove-intermediate",)
				?? true;
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "delete-versions",
					resource: "saved-model",
					id: a[0],
					projectKey,
					payload: { versions, removeIntermediate, },
				};
			}
			await c.savedModels.deleteVersions(versions, { removeIntermediate, }, a[0]!, projectKey,);
			return { deletedVersions: versions, resource: "saved-model", id: a[0], };
		},
		description:
			"Delete one or more versions of a saved model (comma-separated version ids). Destructive. Implemented as POST .../actions/delete-versions per the official Python client (the REST reference's GET entry is a doc bug); the transport never retries this request.",
		examples: [
			"dss saved-model delete-versions MODEL_ID VERSION_A,VERSION_B --project-key PROJECT",
			"dss saved-model delete-versions MODEL_ID VERSION_A --remove-intermediate false --dry-run --project-key PROJECT",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("saved-model", "delete",),
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.savedModels.get(a[0], projectKey,));
				if (!current) return skipResult("saved-model", a[0], "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "saved-model", id: a[0], current, };
				}
			}
			await c.savedModels.delete(a[0], projectKey,);
			return { deleted: a[0], resource: "saved-model", };
		},
		description: "Delete a saved model.",
		examples: [
			"dss saved-model delete MODEL_ID --if-exists --project-key PROJECT",
			"dss saved-model delete MODEL_ID --dry-run --project-key PROJECT",
		],
	},
	"import-mlflow-version": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "import-mlflow-version",);
			requireArgs(a, 2, usage,);
			const archive = f["archive"] as string | undefined;
			if (!archive) throw new UsageError("--archive PATH is required.", "missing_required_flag",);
			const projectKey = f["project-key"] as string | undefined;
			const options = mlflowImportOptions(f,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "import-mlflow-version",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: { ...options, source: { kind: "local-archive", archive, }, },
				};
			}
			return c.savedModels.importMlflowVersion(archive, a[1]!, options, a[0]!, projectKey,);
		},
		description:
			"Import a new MLflow version from a local zip archive of the MLflow model folder (multipart upload). The saved model must be of type MLFLOW_PYFUNC.",
		examples: [
			"dss saved-model import-mlflow-version MODEL_ID v1 --archive ./mlflow-model.zip --project-key PROJECT",
			"dss saved-model import-mlflow-version MODEL_ID v1 --archive ./model.zip --code-env INHERIT --set-active false --project-key PROJECT",
		],
	},
	"import-mlflow-version-from-folder": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "import-mlflow-version-from-folder",);
			requireArgs(a, 2, usage,);
			const folder = f["folder"] as string | undefined;
			if (!folder) {
				throw new UsageError("--folder PROJECT.FOLDER_ID is required.", "missing_required_flag",);
			}
			const path = f["path"] as string | undefined;
			if (!path) throw new UsageError("--path PATH is required.", "missing_required_flag",);
			const projectKey = f["project-key"] as string | undefined;
			const options = mlflowImportOptions(f,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "import-mlflow-version-from-folder",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: { ...options, source: { kind: "managed-folder", folderRef: folder, path, }, },
				};
			}
			return c.savedModels.importMlflowVersionFromFolder(
				folder,
				path,
				a[1]!,
				options,
				a[0]!,
				projectKey,
			);
		},
		description:
			"Import a new MLflow version from an MLflow model folder stored in a managed folder (folderRef PROJECT_KEY.FOLDER_ID). Sends the backend-mandated multipart request with a zero-byte file part, matching the official Python client.",
		examples: [
			"dss saved-model import-mlflow-version-from-folder MODEL_ID v1 --folder PROJ.z7pSyYzE --path /artifacts/model --project-key PROJ",
		],
	},
	"external-metadata-get": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("saved-model", "external-metadata-get",),
			);
			return c.savedModels.externalMetadataGet(
				a[0],
				a[1],
				f["project-key"] as string | undefined,
			);
		},
		description: "Get the external-ml metadata of a saved-model version (MLflow or proxy).",
		examples: [
			"dss saved-model external-metadata-get MODEL_ID VERSION_ID --project-key PROJECT",
		],
	},
	"external-metadata-put": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "external-metadata-put",);
			requireArgs(a, 2, usage,);
			const metadata = requiredJsonInput(
				f,
				"Metadata JSON is required via --data, --data-file, or --stdin (GET-then-PUT the full external-ml metadata document).",
			);
			const projectKey = f["project-key"] as string | undefined;
			const containerExecConfigName = f["container-exec-config"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "external-metadata-put",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: {
						metadata,
						...(containerExecConfigName !== undefined ? { containerExecConfigName, } : {}),
					},
				};
			}
			return c.savedModels.externalMetadataPut(
				a[0]!,
				a[1]!,
				metadata,
				containerExecConfigName === undefined ? undefined : { containerExecConfigName, },
				projectKey,
			).then(() => ({ updated: a[0], versionId: a[1], resource: "saved-model", }));
		},
		description:
			"Save the external-ml metadata of a saved-model version. The payload must be the full metadata document as returned by external-metadata-get (GET-then-PUT). DSS requires the containerExecConfigName query parameter; it defaults to NONE for an external API caller (LOCAL-CONFIG resolution) when --container-exec-config is omitted.",
		examples: [
			"dss saved-model external-metadata-put MODEL_ID VERSION_ID --data-file metadata.json --project-key PROJECT",
		],
	},
	"evaluate-version": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "evaluate-version",);
			requireArgs(a, 2, usage,);
			const dataset = f["dataset"] as string | undefined;
			if (!dataset) throw new UsageError("--dataset REF is required.", "missing_required_flag",);
			const projectKey = f["project-key"] as string | undefined;
			// --sampling carries its own JSON value: parse the flag itself (the
			// old requiredJsonInput read --data/--stdin, so a valid JSON flag
			// was rejected as missing).
			const sampling = json(f["sampling"], "--sampling",);
			// Official external-caller semantics (DSS 15): the evaluation
			// container exec resolves as LOCAL-CONFIG -> NONE, so the body
			// always carries an explicit containerExecConfigName defaulting to
			// NONE; --container-exec-config still passes an explicit value
			// (including INHERIT) through unchanged.
			const containerExecConfigName = (f["container-exec-config"] as string | undefined) ?? "NONE";
			const body = {
				datasetRef: dataset,
				containerExecConfigName,
				...(sampling !== undefined ? { samplingParam: sampling, } : {}),
			};
			const options = {
				...(f["use-optimal-threshold"] !== undefined
					? {
						useOptimalThreshold: parseBooleanOption(
							f["use-optimal-threshold"],
							"--use-optimal-threshold",
						),
					}
					: {}),
				...(f["skip-expensive-reports"] !== undefined
					? {
						skipExpensiveReports: parseBooleanOption(
							f["skip-expensive-reports"],
							"--skip-expensive-reports",
						),
					}
					: {}),
			};
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "evaluate-version",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: { ...body, ...options, },
				};
			}
			await c.savedModels.evaluateVersion(body, options, a[0]!, a[1]!, projectKey,);
			return { evaluated: a[0], versionId: a[1], resource: "saved-model", };
		},
		description:
			'Evaluate an external/MLflow model version on a dataset. external-metadata-put must have been called first. --sampling is an optional JSON sampling parameter (e.g. {"samplingMethod":"HEAD_SEQUENTIAL","maxRecords":100}).',
		examples: [
			"dss saved-model evaluate-version MODEL_ID VERSION_ID --dataset PROJECT.my-dataset --project-key PROJECT",
			"dss saved-model evaluate-version MODEL_ID VERSION_ID --dataset ds --skip-expensive-reports true --project-key PROJECT",
		],
	},
	"download-scoring-jar": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "download-scoring-jar",);
			requireArgs(a, 2, usage,);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const fullClassName = f["full-class-name"] as string | undefined;
			const jarOptions = {
				...(fullClassName !== undefined ? { fullClassName, } : {}),
				...(f["include-libs"] !== undefined
					? { includeLibs: parseBooleanOption(f["include-libs"], "--include-libs",), }
					: {}),
			};
			const res = await c.savedModels.downloadScoringJar(
				jarOptions,
				a[0]!,
				a[1]!,
				f["project-key"] as string | undefined,
			);
			if (!res.body) {
				throw unexpectedResponseError(
					"savedModels.downloadScoringJar response did not include a body",
				);
			}
			const bytes = await writeResponseToFile(out, res,);
			return { path: out, bytes, };
		},
		description:
			"Download the optimized scoring JAR of a version (license-gated server side). --full-class-name forwards the documented fullClassName query parameter; --include-libs toggles including scoring libraries.",
		examples: [
			"dss saved-model download-scoring-jar MODEL_ID VERSION_ID --output ./scoring.jar --project-key PROJECT",
			"dss saved-model download-scoring-jar MODEL_ID VERSION_ID --output ./scoring.jar --full-class-name model.Model --include-libs false --project-key PROJECT",
		],
	},
	"download-scoring-pmml": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				commandUsage("saved-model", "download-scoring-pmml",),
			);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const res = await c.savedModels.downloadScoringPmml(
				a[0]!,
				a[1]!,
				f["project-key"] as string | undefined,
			);
			if (!res.body) {
				throw unexpectedResponseError(
					"savedModels.downloadScoringPmml response did not include a body",
				);
			}
			const bytes = await writeResponseToFile(out, res,);
			return { path: out, bytes, };
		},
		description: "Download the PMML scoring file of a version (license-gated server side).",
		examples: [
			"dss saved-model download-scoring-pmml MODEL_ID VERSION_ID --output ./model.pmml --project-key PROJECT",
		],
	},
	"set-user-meta": {
		handler: async (c, a, f,) => {
			const usage = commandUsage("saved-model", "set-user-meta",);
			requireArgs(a, 2, usage,);
			const userMeta = requiredJsonInput(
				f,
				"User-meta JSON is required via --data, --data-file, or --stdin.",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "set-user-meta",
					resource: "saved-model",
					id: a[0],
					versionId: a[1],
					projectKey,
					payload: userMeta,
				};
			}
			return c.savedModels.setUserMeta(a[0]!, a[1]!, userMeta, projectKey,).then(() => ({
				updated: a[0],
				versionId: a[1],
				resource: "saved-model",
			}));
		},
		description:
			'Update the user metadata of a model version. Send only the "userMeta" field of a previously-retrieved version-details object.',
		examples: [
			'dss saved-model set-user-meta MODEL_ID VERSION_ID --data \'{"name":"Churn model","description":"v2"}\' --project-key PROJECT',
		],
	},
},);

function mlflowImportOptions(f: Record<string, string | boolean>,): {
	codeEnvName?: string;
	containerExecConfigName?: string;
	setActive?: boolean;
	binaryClassificationThreshold?: number;
} {
	const codeEnvName = f["code-env"] as string | undefined;
	const containerExecConfigName = f["container-exec-config"] as string | undefined;
	const setActive = f["set-active"] !== undefined
		? parseBooleanOption(f["set-active"], "--set-active",)
		: undefined;
	const thresholdRaw = f["binary-classification-threshold"];
	const binaryClassificationThreshold = thresholdRaw === undefined
		? undefined
		: Number(thresholdRaw,);
	if (
		binaryClassificationThreshold !== undefined && !Number.isFinite(binaryClassificationThreshold,)
	) {
		throw new UsageError(
			"--binary-classification-threshold must be a number.",
			"invalid_flag_value",
		);
	}
	return {
		...(codeEnvName !== undefined ? { codeEnvName, } : {}),
		...(containerExecConfigName !== undefined ? { containerExecConfigName, } : {}),
		...(setActive !== undefined ? { setActive, } : {}),
		...(binaryClassificationThreshold !== undefined ? { binaryClassificationThreshold, } : {}),
	};
}
