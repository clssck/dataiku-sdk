// Client
export { DataikuClient, type DataikuClientConfig, } from "./client.js";

// Auth & Config
export {
	type CredentialValidationOptions,
	type CredentialValidationResult,
	validateCredentials,
} from "./auth.js";
export {
	deleteCredentials,
	type DssCredentials,
	getConfigDir,
	getCredentialsPath,
	loadCredentials,
	maskApiKey,
	saveCredentials,
} from "./config.js";

// Errors
export {
	DataikuError,
	type DataikuErrorCategory,
	type DataikuErrorTaxonomy,
	type DataikuRetryMetadata,
} from "./errors.js";

// Resources (for advanced use / extension)
export { CodeEnvsResource, } from "./resources/code-envs.js";
export {
	type ConnectionSchemaListOptions,
	ConnectionsResource,
	type ConnectionTableListOptions,
} from "./resources/connections.js";
export { DashboardsResource, } from "./resources/dashboards.js";
export { DataQualityResource, } from "./resources/data-quality.js";
export {
	type DatasetBuildValidationResult,
	type DatasetCloneOptions,
	type DatasetCloneResult,
	type DatasetSchemaColumnInput,
	DatasetsResource,
} from "./resources/datasets.js";
export { type FlowZoneItemInput, FlowZonesResource, } from "./resources/flow-zones.js";
export { FoldersResource, } from "./resources/folders.js";
export { FuturesResource, } from "./resources/futures.js";
export { InsightsResource, } from "./resources/insights.js";
export {
	computeNextPollDelayMs,
	type JobBuildAndWaitOptions,
	type JobBuildOptions,
	type JobBuildTarget,
	type JobBuildTargetType,
	type JobLogFilter,
	type JobLogProgress,
	type JobLogSummary,
	JobsResource,
	parseJobLogProgress,
} from "./resources/jobs.js";
export { NotebooksResource, } from "./resources/notebooks.js";
export { type FlowMapResult, ProjectsResource, } from "./resources/projects.js";
export {
	type RecipeCloneOptions,
	type RecipeCloneResult,
	type RecipeGraphReference,
	type RecipeGraphValidationResult,
	type RecipeRunOptions,
	type RecipeRunOutput,
	type RecipeRunResult,
	RecipesResource,
} from "./resources/recipes.js";
export {
	normalizeScenarioUpdateData,
	SCENARIO_CANONICAL_EDITABLE_FIELDS,
	type ScenarioFieldChange,
	type ScenarioFieldMismatch,
	ScenariosResource,
	type ScenarioUpdateNormalization,
	type ScenarioUpdatePreview,
	scenarioUpdatePreview,
	type ScenarioUpdateResult,
} from "./resources/scenarios.js";
export { SqlResource, } from "./resources/sql.js";
export { VariablesResource, } from "./resources/variables.js";
export { WikiResource, } from "./resources/wiki.js";

// Schemas (TypeBox schema objects for runtime validation)
export {
	BuildModeSchema,
	CodeEnvActionResultSchema,
	CodeEnvCreateOptionsSchema,
	CodeEnvDetailsSchema,
	CodeEnvPackageListSchema,
	CodeEnvSetPackagesOptionsSchema,
	CodeEnvSummaryArraySchema,
	CodeEnvSummarySchema,
	CodeEnvUpdatePackagesOptionsSchema,
	CodeEnvUsageArraySchema,
	CodeEnvWaitOptionsSchema,
	ConnectionSummarySchema,
	DashboardDetailsSchema,
	DashboardSummaryArraySchema,
	DashboardSummarySchema,
	DataQualityComputeResultSchema,
	DataQualityProjectStatusSchema,
	DataQualityRuleArraySchema,
	DataQualityRuleResultArraySchema,
	DataQualityRuleResultSchema,
	DataQualityRuleSchema,
	DataQualityRulesSchema,
	DataQualityStatusByPartitionSchema,
	DataQualityStatusSchema,
	DataQualityTimelineEntrySchema,
	DataQualityTimelineSchema,
	DatasetCreateOptionsSchema,
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
	DatasetSummarySchema,
	FlowMapOptionsSchema,
	FlowZoneArraySchema,
	FlowZoneCreateOptionsSchema,
	FlowZoneItemSchema,
	FlowZoneObjectTypeSchema,
	FlowZonePositionSchema,
	FlowZoneSchema,
	FlowZoneUpdateOptionsSchema,
	FolderCreateOptionsSchema,
	FolderDetailsSchema,
	FolderItemArraySchema,
	FolderItemSchema,
	FolderSummaryArraySchema,
	FolderSummarySchema,
	FutureStateSchema,
	FutureWaitResultSchema,
	InsightDetailsSchema,
	InsightSummaryArraySchema,
	InsightSummarySchema,
	JobSummaryArraySchema,
	JobSummarySchema,
	JobWaitResultSchema,
	JupyterCellSchema,
	JupyterNotebookContentSchema,
	JupyterNotebookSummaryArraySchema,
	JupyterNotebookSummarySchema,
	NotebookSessionArraySchema,
	NotebookSessionSchema,
	parseSchema,
	ProjectDetailsSchema,
	ProjectMetadataSchema,
	ProjectSummaryArraySchema,
	ProjectSummarySchema,
	ProjectVariablesSchema,
	RecipeCreateOptionsSchema,
	RecipeCreateResultSchema,
	RecipeDetailsSchema,
	RecipeSummaryArraySchema,
	RecipeSummarySchema,
	safeParseSchema,
	ScenarioDetailsSchema,
	ScenarioStatusSchema,
	ScenarioSummaryArraySchema,
	ScenarioSummarySchema,
	SqlNotebookCellSchema,
	SqlNotebookContentSchema,
	SqlNotebookSummaryArraySchema,
	SqlNotebookSummarySchema,
	SqlQueryResponseSchema,
	SqlQueryResultSchema,
	SqlQuerySchemaSchema,
	WikiArticleDataArraySchema,
	WikiArticleDataSchema,
	WikiArticleMetadataSchema,
	WikiSettingsSchema,
	WikiTaxonomyNodeSchema,
} from "./schemas.js";

export type { SafeParseResult, } from "./schemas.js";

// Types (inferred from schemas)
export type {
	BuildMode,
	CodeEnvActionResult,
	CodeEnvCreateOptions,
	CodeEnvDetails,
	CodeEnvPackageList,
	CodeEnvSetPackagesOptions,
	CodeEnvSummary,
	CodeEnvUpdatePackagesOptions,
	CodeEnvUsage,
	CodeEnvWaitOptions,
	ConnectionSummary,
	DashboardDetails,
	DashboardSummary,
	DataQualityComputeResult,
	DataQualityProjectStatus,
	DataQualityRule,
	DataQualityRuleResult,
	DataQualityRules,
	DataQualityStatus,
	DataQualityStatusByPartition,
	DataQualityTimeline,
	DataQualityTimelineEntry,
	DatasetCreateOptions,
	DatasetDetails,
	DatasetSchema,
	DatasetSummary,
	FlowMapOptions,
	FlowZone,
	FlowZoneCreateOptions,
	FlowZoneItem,
	FlowZoneObjectType,
	FlowZonePosition,
	FlowZoneUpdateOptions,
	FolderCreateOptions,
	FolderDetails,
	FolderItem,
	FolderSummary,
	FutureState,
	FutureWaitResult,
	InsightDetails,
	InsightSummary,
	JobSummary,
	JobWaitResult,
	JupyterCell,
	JupyterNotebookContent,
	JupyterNotebookSummary,
	NotebookSession,
	ProjectDetails,
	ProjectMetadata,
	ProjectSummary,
	ProjectVariables,
	RecipeCreateOptions,
	RecipeCreateResult,
	RecipeDetails,
	RecipeSummary,
	ScenarioDetails,
	ScenarioStatus,
	ScenarioSummary,
	SqlNotebookCell,
	SqlNotebookContent,
	SqlNotebookSummary,
	SqlQueryResponse,
	SqlQueryResult,
	SqlQuerySchema,
	WikiArticleData,
	WikiArticleMetadata,
	WikiSettings,
	WikiTaxonomyNode,
} from "./schemas.js";

// Utilities
export { deepMerge, } from "./utils/deep-merge.js";
export {
	type NormalizedFlowEdge,
	type NormalizedFlowMap,
	type NormalizedFlowNode,
	normalizeFlowGraph,
} from "./utils/flow-map.js";
export { sanitizeFileName, } from "./utils/sanitize.js";

// Stream validation
export { validateStreamColumns, } from "./resources/datasets.js";
