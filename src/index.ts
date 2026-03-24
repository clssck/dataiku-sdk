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
export { ConnectionsResource, } from "./resources/connections.js";
export { DatasetsResource, } from "./resources/datasets.js";
export { FoldersResource, } from "./resources/folders.js";
export { computeNextPollDelayMs, JobsResource, } from "./resources/jobs.js";
export { NotebooksResource, } from "./resources/notebooks.js";
export { type FlowMapResult, ProjectsResource, } from "./resources/projects.js";
export { RecipesResource, } from "./resources/recipes.js";
export { ScenariosResource, } from "./resources/scenarios.js";
export { SqlResource, } from "./resources/sql.js";
export { VariablesResource, } from "./resources/variables.js";

// Schemas (TypeBox schema objects for runtime validation)
export {
	BuildModeSchema,
	CodeEnvDetailsSchema,
	CodeEnvSummaryArraySchema,
	CodeEnvSummarySchema,
	ConnectionSummarySchema,
	DatasetCreateOptionsSchema,
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
	DatasetSummarySchema,
	FlowMapOptionsSchema,
	FolderDetailsSchema,
	FolderItemArraySchema,
	FolderItemSchema,
	FolderSummaryArraySchema,
	FolderSummarySchema,
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
} from "./schemas.js";

export type { SafeParseResult, } from "./schemas.js";

// Types (inferred from schemas)
export type {
	BuildMode,
	CodeEnvDetails,
	CodeEnvSummary,
	ConnectionSummary,
	DatasetCreateOptions,
	DatasetDetails,
	DatasetSchema,
	DatasetSummary,
	FlowMapOptions,
	FolderDetails,
	FolderItem,
	FolderSummary,
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
