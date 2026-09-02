// Client
export {
	DataikuClient,
	type DataikuClientConfig,
	type DataikuClientTraceEvent,
} from "./client.js";

// Auth & Config
export {
	type CredentialValidationOptions,
	type CredentialValidationResult,
	validateCredentials,
} from "./auth.js";
export {
	type DssCredentials,
	getConfigDir,
	getCredentialsPath,
	loadCredentials,
	saveCredentials,
} from "./config.js";

// Errors
export {
	ClientValidationError,
	DataikuError,
	type DataikuErrorCategory,
	type DataikuErrorTaxonomy,
	type DataikuRetryMetadata,
} from "./errors.js";

// Resources (for advanced use / extension)
export * from "./resources/analyses.js";
export { ApiDeployerResource, } from "./resources/api-deployer.js";
export { ApiServicesResource, } from "./resources/api-services.js";
export { ApplicationsResource, } from "./resources/applications.js";
export { BundlesResource, ProjectDeployerResource, } from "./resources/bundles.js";
export { CodeEnvsResource, } from "./resources/code-envs.js";
export {
	type ConnectionSchemaListOptions,
	ConnectionsResource,
	type ConnectionTableListOptions,
} from "./resources/connections.js";
export { ContinuousActivitiesResource, } from "./resources/continuous-activities.js";
export { DashboardsResource, } from "./resources/dashboards.js";
export { DataQualityResource, } from "./resources/data-quality.js";
export {
	type DatasetBuildValidationResult,
	type DatasetCloneOptions,
	type DatasetCloneResult,
	type DatasetSchemaColumnInput,
	DatasetsResource,
	type UploadDatasetFileOptions,
	type UploadDatasetFileResult,
	type UploadedFileMetadata,
} from "./resources/datasets.js";
export { DiscussionsResource, } from "./resources/discussions.js";
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
	type JobLogUnavailableReason,
	JobsResource,
	type JobWaitOutcome,
	parseJobLogProgress,
} from "./resources/jobs.js";
export { MeaningsResource, } from "./resources/meanings.js";
export { MetricsResource, } from "./resources/metrics.js";
export * from "./resources/ml-tasks.js";
export * from "./resources/model-evaluation-stores.js";
export { NotebooksResource, } from "./resources/notebooks.js";
export * from "./resources/project-git.js";
export { ProjectLibraryResource, } from "./resources/project-library.js";
export {
	type FlowMapResult,
	type ProjectImportProcessResult,
	type ProjectImportResult,
	type ProjectImportSettings,
	type ProjectImportUploadResult,
	ProjectsResource,
} from "./resources/projects.js";
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
export * from "./resources/saved-models.js";
export {
	normalizeScenarioUpdateData,
	SCENARIO_CANONICAL_EDITABLE_FIELDS,
	type ScenarioFieldChange,
	type ScenarioFieldMismatch,
	type ScenarioScriptRunResult,
	ScenariosResource,
	type ScenarioUpdateNormalization,
	type ScenarioUpdatePreview,
	scenarioUpdatePreview,
	type ScenarioUpdateResult,
} from "./resources/scenarios.js";
export { SqlResource, } from "./resources/sql.js";
export { StatisticsResource, } from "./resources/statistics.js";
export { StreamingEndpointsResource, } from "./resources/streaming-endpoints.js";
export { VariablesResource, } from "./resources/variables.js";
export { WebappsResource, } from "./resources/webapps.js";
export { WikiResource, } from "./resources/wiki.js";
export { WorkspacesResource, } from "./resources/workspaces.js";

// Schemas (TypeBox schema objects for runtime validation)
export {
	BuildModeSchema,
	CodeEnvActionResultSchema,
	CodeEnvCreateOptionsSchema,
	CodeEnvDetailsSchema,
	CodeEnvGetLogOptionsSchema,
	CodeEnvLogResultSchema,
	CodeEnvLogSummaryArraySchema,
	CodeEnvLogSummarySchema,
	CodeEnvPackageListSchema,
	CodeEnvSetPackagesOptionsSchema,
	CodeEnvSummaryArraySchema,
	CodeEnvSummarySchema,
	CodeEnvUpdateImagesOptionsSchema,
	CodeEnvUpdatePackagesOptionsSchema,
	CodeEnvUsageArraySchema,
	CodeEnvVersionForProjectSchema,
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
	ProjectGitActionResultSchema,
	ProjectGitDiffResultSchema,
	ProjectGitFutureResponseSchema,
	ProjectGitFutureStateSchema,
	ProjectGitLibrariesSchema,
	ProjectGitLibrarySchema,
	ProjectGitLogResultSchema,
	ProjectGitRemoteSchema,
	ProjectGitStatusSchema,
	ProjectGitTagSchema,
	ProjectGitTagsSchema,
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
	ScenarioStepRunSchema,
	ScenarioSummaryArraySchema,
	ScenarioSummarySchema,
	ScenarioWaitResultSchema,
	SqlNotebookCellSchema,
	SqlNotebookContentSchema,
	SqlNotebookHistorySchema,
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
	CodeEnvGetLogOptions,
	CodeEnvLogResult,
	CodeEnvLogSummary,
	CodeEnvPackageList,
	CodeEnvSetPackagesOptions,
	CodeEnvSummary,
	CodeEnvUpdateImagesOptions,
	CodeEnvUpdatePackagesOptions,
	CodeEnvUsage,
	CodeEnvVersionForProject,
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
	ProjectGitActionResult,
	ProjectGitDiffResult,
	ProjectGitFutureResponse,
	ProjectGitFutureState,
	ProjectGitLibraries,
	ProjectGitLibrary,
	ProjectGitLogResult,
	ProjectGitRemote,
	ProjectGitStatus,
	ProjectGitTag,
	ProjectGitTags,
	ProjectMetadata,
	ProjectSummary,
	ProjectVariables,
	RecipeCreateOptions,
	RecipeCreateResult,
	RecipeDetails,
	RecipeSummary,
	ScenarioDetails,
	ScenarioStatus,
	ScenarioStepRun,
	ScenarioSummary,
	ScenarioWaitResult,
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
	type AnalyzedFlowMap,
	type AnalyzedFlowNode,
	analyzeFlowMap,
	type FlowMapComponent,
	type FlowMapDiagnostic,
	type FlowMapRendering,
	type FlowMapZone,
	type FlowRenderFormat,
	flowTopologyFingerprint,
	renderFlowMap,
} from "./utils/flow-analysis.js";
export {
	type NormalizedFlowEdge,
	type NormalizedFlowMap,
	type NormalizedFlowNode,
	normalizeFlowGraph,
} from "./utils/flow-map.js";
export * from "./utils/project-archive.js";
export { sanitizeFileName, } from "./utils/sanitize.js";

// Stream validation
export { validateStreamColumns, } from "./resources/datasets.js";
