// Client
export {
	DataikuClient,
	type DataikuClientConfig,
	type DataikuClientTraceEvent,
	type DataikuGetOptions,
	type UploadFormPart,
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
	type ConnectionAdminInput,
	type ConnectionSchemaListOptions,
	ConnectionsResource,
	type ConnectionTableListOptions,
} from "./resources/connections.js";
export { ContinuousActivitiesResource, } from "./resources/continuous-activities.js";
export { DashboardsResource, } from "./resources/dashboards.js";
export {
	type DataCollectionCreateRequest,
	type DataCollectionObject,
	type DataCollectionObjectReferenceType,
	type DataCollectionPermission,
	type DataCollectionSettings,
	DataCollectionsResource,
	type DataCollectionSummary,
} from "./resources/data-collections.js";
export { DataQualityResource, } from "./resources/data-quality.js";
export {
	type DatasetBuildValidationResult,
	type DatasetCloneOptions,
	type DatasetCloneResult,
	type DatasetManagedCreateOptions,
	type DatasetManagedCreateResult,
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
export { type DssGroup, type GroupCreateRequest, GroupsResource, } from "./resources/groups.js";
export { InsightsResource, } from "./resources/insights.js";
export {
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
export * from "./resources/knowledge-banks.js";
export * from "./resources/llms.js";
export {
	type MacroResultOptions,
	type MacroRunFailure,
	type MacroRunHandle,
	type MacroRunOptions,
	type MacroRunWaitedResult,
	MacrosResource,
	type MacroWaitOptions,
} from "./resources/macros.js";
export { MeaningsResource, } from "./resources/meanings.js";
export { MetricsResource, } from "./resources/metrics.js";
export * from "./resources/ml-tasks.js";
export * from "./resources/model-evaluation-stores.js";
export { NotebooksResource, } from "./resources/notebooks.js";
export {
	type PluginCodeEnvCreateOptions,
	type PluginContentItem,
	type PluginCreateDevOptions,
	type PluginFileDetails,
	type PluginGitInstallOptions,
	type PluginGitRemote,
	type PluginMissingType,
	type PluginProjectScopeOptions,
	type PluginSettings,
	PluginsResource,
	type PluginSummary,
	type PluginUsage,
	type PluginUsageReport,
} from "./resources/plugins.js";
export {
	type ProjectFolder,
	type ProjectFolderPermission,
	type ProjectFolderSettings,
	ProjectFoldersResource,
} from "./resources/project-folders.js";
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
	type ScenarioActiveUpdateResult,
	type ScenarioFieldChange,
	type ScenarioFieldMismatch,
	type ScenarioLightStatus,
	type ScenarioRunDetails,
	type ScenarioRunLog,
	type ScenarioRunStepReport,
	type ScenarioRunSummary,
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
export {
	type DssUser,
	type ExternalUserEntry,
	type ProvisionRequest,
	type UserActivity,
	type UserCreateRequest,
	UsersResource,
} from "./resources/users.js";
export { VariablesResource, } from "./resources/variables.js";
export { WebappsResource, } from "./resources/webapps.js";
export { WikiResource, } from "./resources/wiki.js";
export { WorkspacesResource, } from "./resources/workspaces.js";
export { computeNextPollDelayMs, } from "./utils/polling.js";

// Schemas, derived types, and parse helpers
export * from "./schemas.js";

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
