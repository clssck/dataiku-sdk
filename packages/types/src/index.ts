import { type Static, type TSchema, Type, } from "@sinclair/typebox";
import { Value, } from "@sinclair/typebox/value";

// ---------------------------------------------------------------------------
// Runtime validation helper
// ---------------------------------------------------------------------------

/**
 * Validate `data` against a TypeBox schema, throwing on structural mismatch.
 * Uses Value.Assert so extra DSS fields (additionalProperties) are preserved.
 */
export function parseSchema<S extends TSchema,>(schema: S, data: unknown,): Static<S> {
	Value.Assert(schema, data,);
	return data as Static<S>;
}

/** Result of a non-throwing schema validation. Data is always returned. */
export type SafeParseResult<T,> = {
	success: true;
	data: T;
} | {
	success: false;
	data: T;
	errors: string[];
};

/**
 * Validate `data` against a TypeBox schema without throwing.
 * Always returns the data (cast as T) — on mismatch, includes human-readable
 * error strings so callers can warn instead of crash.
 */
export function safeParseSchema<S extends TSchema,>(
	schema: S,
	data: unknown,
): SafeParseResult<Static<S>> {
	if (Value.Check(schema, data,)) {
		return { success: true, data: data as Static<S>, };
	}
	const errors = [...Value.Errors(schema, data,),].map(
		(e,) => `${e.path}: ${e.message} (got ${JSON.stringify(e.value,)})`,
	);
	return { success: false, data: data as Static<S>, errors, };
}

// ---------------------------------------------------------------------------
// Projects
// ---------------------------------------------------------------------------

export const ProjectSummarySchema = Type.Object({
	projectKey: Type.String(),
	name: Type.String(),
	shortDesc: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type ProjectSummary = Static<typeof ProjectSummarySchema>;

export const ProjectDetailsSchema = Type.Object({
	projectKey: Type.String(),
	name: Type.String(),
	shortDesc: Type.Optional(Type.String(),),
	projectStatus: Type.Optional(Type.String(),),
	ownerLogin: Type.Optional(Type.String(),),
	tags: Type.Optional(Type.Array(Type.String(),),),
	versionTag: Type.Optional(Type.Object({
		versionNumber: Type.Number(),
		lastModifiedOn: Type.Number(),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type ProjectDetails = Static<typeof ProjectDetailsSchema>;

export const ProjectMetadataSchema = Type.Object({
	label: Type.Optional(Type.String(),),
	shortDesc: Type.Optional(Type.String(),),
	description: Type.Optional(Type.String(),),
	tags: Type.Optional(Type.Array(Type.String(),),),
	customFields: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	checklists: Type.Optional(Type.Object({
		checklists: Type.Optional(Type.Array(Type.Object({
			title: Type.String(),
			items: Type.Optional(Type.Array(Type.Object({
				done: Type.Boolean(),
			}, { additionalProperties: true, },),),),
		}, { additionalProperties: true, },),),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type ProjectMetadata = Static<typeof ProjectMetadataSchema>;

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

export const DatasetSummarySchema = Type.Object({
	name: Type.String(),
	type: Type.Optional(Type.String(),),
	shortDesc: Type.Optional(Type.String(),),
	managed: Type.Optional(Type.Boolean(),),
	params: Type.Optional(Type.Object({
		connection: Type.Optional(Type.String(),),
		schema: Type.Optional(Type.String(),),
		catalog: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type DatasetSummary = Static<typeof DatasetSummarySchema>;

export const DatasetSchemaSchema = Type.Object({
	columns: Type.Array(Type.Object({
		name: Type.String(),
		type: Type.String(),
		comment: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type DatasetSchema = Static<typeof DatasetSchemaSchema>;

export const DatasetDetailsSchema = Type.Object({
	name: Type.String(),
	type: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
	managed: Type.Optional(Type.Boolean(),),
	params: Type.Optional(Type.Object({
		connection: Type.Optional(Type.String(),),
		path: Type.Optional(Type.String(),),
		table: Type.Optional(Type.String(),),
		schema: Type.Optional(Type.String(),),
		catalog: Type.Optional(Type.String(),),
		folderSmartId: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
	formatType: Type.Optional(Type.String(),),
	formatParams: Type.Optional(Type.Object({
		separator: Type.Optional(Type.String(),),
		charset: Type.Optional(Type.String(),),
		compress: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
	schema: Type.Optional(DatasetSchemaSchema,),
	tags: Type.Optional(Type.Array(Type.String(),),),
	shortDesc: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type DatasetDetails = Static<typeof DatasetDetailsSchema>;

export const DatasetCreateOptionsSchema = Type.Object({
	datasetName: Type.String(),
	connection: Type.String(),
	dsType: Type.Optional(Type.String(),),
	table: Type.Optional(Type.String(),),
	dbSchema: Type.Optional(Type.String(),),
	catalog: Type.Optional(Type.String(),),
	formatType: Type.Optional(Type.String(),),
	formatParams: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	managed: Type.Optional(Type.Boolean(),),
	projectKey: Type.Optional(Type.String(),),
},);
export type DatasetCreateOptions = Static<typeof DatasetCreateOptionsSchema>;

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

export const RecipeSummarySchema = Type.Object({
	name: Type.String(),
	type: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type RecipeSummary = Static<typeof RecipeSummarySchema>;

export const RecipeDetailsSchema = Type.Object({
	name: Type.String(),
	type: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
	inputs: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	outputs: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	params: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	payload: Type.Optional(Type.String(),),
	versionTag: Type.Optional(Type.Object({
		versionNumber: Type.Number(),
	}, { additionalProperties: true, },),),
	neverBuilt: Type.Optional(Type.Boolean(),),
}, { additionalProperties: true, },);
export type RecipeDetails = Static<typeof RecipeDetailsSchema>;

export const RecipeCreateOptionsSchema = Type.Object({
	type: Type.String(),
	name: Type.Optional(Type.String(),),
	inputDatasets: Type.Optional(Type.Array(Type.String(),),),
	outputDataset: Type.Optional(Type.String(),),
	inputs: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	outputs: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	payload: Type.Optional(Type.String(),),
	outputConnection: Type.Optional(Type.String(),),
	joinOn: Type.Optional(Type.Union([Type.String(), Type.Array(Type.String(),),],),),
	joinType: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
},);
export type RecipeCreateOptions = Static<typeof RecipeCreateOptionsSchema>;

export const RecipeCreateResultSchema = Type.Object({
	recipeName: Type.String(),
	type: Type.String(),
	createdDatasets: Type.Array(Type.String(),),
	joinConfigured: Type.Boolean(),
	outputProvisioningFallbackUsed: Type.Boolean(),
},);
export type RecipeCreateResult = Static<typeof RecipeCreateResultSchema>;

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

export const JobSummarySchema = Type.Object({
	def: Type.Optional(Type.Object({
		id: Type.Optional(Type.String(),),
		type: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
	baseStatus: Type.Optional(Type.Object({
		def: Type.Optional(Type.Object({
			id: Type.Optional(Type.String(),),
			type: Type.Optional(Type.String(),),
		}, { additionalProperties: true, },),),
		state: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type JobSummary = Static<typeof JobSummarySchema>;

export const JobWaitResultSchema = Type.Object({
	jobId: Type.String(),
	state: Type.String(),
	type: Type.String(),
	elapsedMs: Type.Number(),
	pollCount: Type.Number(),
	success: Type.Boolean(),
	timedOut: Type.Optional(Type.Boolean(),),
	progress: Type.Object({
		done: Type.Number(),
		failed: Type.Number(),
		running: Type.Number(),
		total: Type.Union([Type.Number(), Type.Null(),],),
	},),
	log: Type.Optional(Type.String(),),
},);
export type JobWaitResult = Static<typeof JobWaitResultSchema>;

export const BuildModeSchema = Type.Union([
	Type.Literal("RECURSIVE_BUILD",),
	Type.Literal("NON_RECURSIVE_FORCED_BUILD",),
	Type.Literal("RECURSIVE_FORCED_BUILD",),
	Type.Literal("RECURSIVE_MISSING_ONLY_BUILD",),
],);
export type BuildMode = Static<typeof BuildModeSchema>;

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

export const ScenarioSummarySchema = Type.Object({
	id: Type.String(),
	name: Type.Optional(Type.String(),),
	active: Type.Optional(Type.Boolean(),),
}, { additionalProperties: true, },);
export type ScenarioSummary = Static<typeof ScenarioSummarySchema>;

export const ScenarioDetailsSchema = Type.Object({
	id: Type.String(),
	name: Type.Optional(Type.String(),),
	active: Type.Optional(Type.Boolean(),),
	type: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
	params: Type.Optional(Type.Object({
		steps: Type.Optional(Type.Array(Type.Unknown(),),),
		triggers: Type.Optional(Type.Array(Type.Unknown(),),),
		reporters: Type.Optional(Type.Array(Type.Unknown(),),),
		customScript: Type.Optional(Type.Object({
			script: Type.Optional(Type.String(),),
		}, { additionalProperties: true, },),),
	}, { additionalProperties: true, },),),
	versionTag: Type.Optional(Type.Object({
		versionNumber: Type.Number(),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type ScenarioDetails = Static<typeof ScenarioDetailsSchema>;

export const ScenarioStatusSchema = Type.Object({
	id: Type.Optional(Type.String(),),
	name: Type.Optional(Type.String(),),
	active: Type.Optional(Type.Boolean(),),
	running: Type.Optional(Type.Boolean(),),
	nextRun: Type.Optional(Type.Number(),),
	lastRun: Type.Optional(Type.Object({
		runId: Type.Optional(Type.String(),),
		outcome: Type.Optional(Type.String(),),
		start: Type.Optional(Type.Number(),),
		end: Type.Optional(Type.Number(),),
		trigger: Type.Optional(Type.Object({
			type: Type.Optional(Type.String(),),
		}, { additionalProperties: true, },),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type ScenarioStatus = Static<typeof ScenarioStatusSchema>;

// ---------------------------------------------------------------------------
// Folders
// ---------------------------------------------------------------------------

export const FolderSummarySchema = Type.Object({
	id: Type.String(),
	name: Type.Optional(Type.String(),),
	type: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type FolderSummary = Static<typeof FolderSummarySchema>;

export const FolderDetailsSchema = Type.Object({
	id: Type.String(),
	name: Type.Optional(Type.String(),),
	type: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
	params: Type.Optional(Type.Object({
		connection: Type.Optional(Type.String(),),
		path: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
	tags: Type.Optional(Type.Array(Type.String(),),),
}, { additionalProperties: true, },);
export type FolderDetails = Static<typeof FolderDetailsSchema>;

export const FolderItemSchema = Type.Object({
	path: Type.String(),
	size: Type.Optional(Type.Number(),),
	lastModified: Type.Optional(Type.Number(),),
}, { additionalProperties: true, },);
export type FolderItem = Static<typeof FolderItemSchema>;

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------

export const ProjectVariablesSchema = Type.Object({
	standard: Type.Record(Type.String(), Type.Unknown(),),
	local: Type.Record(Type.String(), Type.Unknown(),),
}, { additionalProperties: true, },);
export type ProjectVariables = Static<typeof ProjectVariablesSchema>;

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

export const ConnectionSummarySchema = Type.Object({
	name: Type.String(),
	types: Type.Optional(Type.Array(Type.String(),),),
	managed: Type.Optional(Type.Boolean(),),
	dbSchemas: Type.Optional(Type.Array(Type.String(),),),
}, { additionalProperties: true, },);
export type ConnectionSummary = Static<typeof ConnectionSummarySchema>;

// ---------------------------------------------------------------------------
// Code Envs
// ---------------------------------------------------------------------------

export const CodeEnvSummarySchema = Type.Object({
	envName: Type.String(),
	envLang: Type.String(),
	pythonInterpreter: Type.Optional(Type.String(),),
	deploymentMode: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type CodeEnvSummary = Static<typeof CodeEnvSummarySchema>;

export const CodeEnvDetailsSchema = Type.Object({
	envName: Type.String(),
	envLang: Type.String(),
	pythonInterpreter: Type.Optional(Type.String(),),
	requestedPackages: Type.Array(Type.String(),),
	installedPackages: Type.Array(Type.String(),),
},);
export type CodeEnvDetails = Static<typeof CodeEnvDetailsSchema>;

// ---------------------------------------------------------------------------
// Jupyter Notebooks
// ---------------------------------------------------------------------------

export const JupyterCellSchema = Type.Object({
	cell_type: Type.String(),
	source: Type.Array(Type.String(),),
	metadata: Type.Optional(Type.Record(Type.String(), Type.Unknown(),),),
	outputs: Type.Optional(Type.Array(Type.Unknown(),),),
	execution_count: Type.Optional(Type.Union([Type.Number(), Type.Null(),],),),
}, { additionalProperties: true, },);
export type JupyterCell = Static<typeof JupyterCellSchema>;

export const JupyterNotebookSummarySchema = Type.Object({
	name: Type.String(),
	projectKey: Type.String(),
	language: Type.String(),
	kernelSpec: Type.Optional(Type.Object({
		name: Type.String(),
		display_name: Type.Optional(Type.String(),),
		language: Type.Optional(Type.String(),),
	}, { additionalProperties: true, },),),
}, { additionalProperties: true, },);
export type JupyterNotebookSummary = Static<typeof JupyterNotebookSummarySchema>;

export const JupyterNotebookContentSchema = Type.Object({
	metadata: Type.Record(Type.String(), Type.Unknown(),),
	nbformat: Type.Number(),
	nbformat_minor: Type.Number(),
	cells: Type.Array(JupyterCellSchema,),
}, { additionalProperties: true, },);
export type JupyterNotebookContent = Static<typeof JupyterNotebookContentSchema>;

export const NotebookSessionSchema = Type.Object({
	sessionId: Type.String(),
	kernelId: Type.Optional(Type.String(),),
	projectKey: Type.Optional(Type.String(),),
	notebookName: Type.Optional(Type.String(),),
	sessionCreator: Type.Optional(Type.String(),),
	kernelExecutionState: Type.Optional(Type.String(),),
}, { additionalProperties: true, },);
export type NotebookSession = Static<typeof NotebookSessionSchema>;

// ---------------------------------------------------------------------------
// SQL Notebooks
// ---------------------------------------------------------------------------

export const SqlNotebookCellSchema = Type.Object({
	id: Type.String(),
	type: Type.String(),
	name: Type.Optional(Type.String(),),
	code: Type.String(),
}, { additionalProperties: true, },);
export type SqlNotebookCell = Static<typeof SqlNotebookCellSchema>;

export const SqlNotebookSummarySchema = Type.Object({
	id: Type.String(),
	projectKey: Type.Optional(Type.String(),),
	language: Type.String(),
	connection: Type.String(),
}, { additionalProperties: true, },);
export type SqlNotebookSummary = Static<typeof SqlNotebookSummarySchema>;

export const SqlNotebookContentSchema = Type.Object({
	connection: Type.String(),
	cells: Type.Array(SqlNotebookCellSchema,),
}, { additionalProperties: true, },);
export type SqlNotebookContent = Static<typeof SqlNotebookContentSchema>;

// ---------------------------------------------------------------------------
// SQL Queries
// ---------------------------------------------------------------------------

export const SqlQuerySchemaSchema = Type.Object({
	name: Type.String(),
	type: Type.String(),
}, { additionalProperties: true, },);
export type SqlQuerySchema = Static<typeof SqlQuerySchemaSchema>;

export const SqlQueryResultSchema = Type.Object({
	queryId: Type.String(),
	hasResults: Type.Boolean(),
	schema: Type.Array(SqlQuerySchemaSchema,),
}, { additionalProperties: true, },);
export type SqlQueryResult = Static<typeof SqlQueryResultSchema>;

export const SqlQueryResponseSchema = Type.Object({
	queryId: Type.String(),
	schema: Type.Array(SqlQuerySchemaSchema,),
	rows: Type.Array(Type.Array(Type.Unknown(),),),
},);
export type SqlQueryResponse = Static<typeof SqlQueryResponseSchema>;

// ---------------------------------------------------------------------------
// Flow Map (moved from projects.ts)
// ---------------------------------------------------------------------------

export const FlowMapOptionsSchema = Type.Object({
	maxNodes: Type.Optional(Type.Number(),),
	maxEdges: Type.Optional(Type.Number(),),
	includeRaw: Type.Optional(Type.Boolean(),),
},);
export type FlowMapOptions = Static<typeof FlowMapOptionsSchema>;

// ---------------------------------------------------------------------------
// Array wrappers (for runtime validation of list() responses)
// ---------------------------------------------------------------------------

export const ProjectSummaryArraySchema = Type.Array(ProjectSummarySchema,);
export const DatasetSummaryArraySchema = Type.Array(DatasetSummarySchema,);
export const RecipeSummaryArraySchema = Type.Array(RecipeSummarySchema,);
export const JobSummaryArraySchema = Type.Array(JobSummarySchema,);
export const ScenarioSummaryArraySchema = Type.Array(ScenarioSummarySchema,);
export const FolderSummaryArraySchema = Type.Array(FolderSummarySchema,);
export const FolderItemArraySchema = Type.Array(FolderItemSchema,);
export const CodeEnvSummaryArraySchema = Type.Array(CodeEnvSummarySchema,);
export const JupyterNotebookSummaryArraySchema = Type.Array(JupyterNotebookSummarySchema,);
export const SqlNotebookSummaryArraySchema = Type.Array(SqlNotebookSummarySchema,);
export const NotebookSessionArraySchema = Type.Array(NotebookSessionSchema,);
