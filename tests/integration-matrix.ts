export type Persona = "newbie" | "expert" | "error-prone";
export type RiskLevel = "read-only" | "local-validation" | "mutating" | "expensive";
export type ResultShape = "array" | "object" | "string";
export type RegistryOutputShape = ResultShape | "void";
export type RegistrySideEffect = "read" | "write" | "auth";
export type RegistryDestructiveLevel = "none" | "reversible" | "destructive";
export type RegistryAsyncKind = "none" | "job" | "future";

export type SdkParityKind =
	| "project-list"
	| "project-get"
	| "project-metadata"
	| "project-map"
	| "flow-zone-list"
	| "dataset-list"
	| "recipe-list"
	| "job-list"
	| "scenario-list"
	| "folder-list"
	| "variable-get"
	| "connection-infer"
	| "code-env-list"
	| "dashboard-list"
	| "insight-list"
	| "notebook-list-jupyter"
	| "notebook-list-sql"
	| "wiki-settings"
	| "wiki-list";

export type ReadOnlyCommandCase = {
	id: string;
	persona: Persona;
	resource: string;
	action: string;
	args: string[];
	risk: "read-only";
	requiresProject: boolean;
	resultShape: ResultShape;
	sdkParity?: SdkParityKind;
	stableFields?: string[];
	featureProbe?: string;
};

export type RegistryExpectations = {
	outputShape: RegistryOutputShape;
	sideEffect: RegistrySideEffect;
	destructive: RegistryDestructiveLevel;
	mutatesDss: boolean;
	async: RegistryAsyncKind;
	dryRun: boolean;
	exitCodes: { ok: 0; usage: 1; error: 2; transient: 3; longRunningFailure?: 4; };
};

export function registryExpectationsForReadOnlyCase(
	entry: ReadOnlyCommandCase,
): RegistryExpectations {
	return {
		outputShape: entry.resultShape,
		sideEffect: "read",
		destructive: "none",
		mutatesDss: false,
		async: entry.resource === "future" ? "future" : "none",
		dryRun: false,
		exitCodes: {
			ok: 0,
			usage: 1,
			error: 2,
			transient: 3,
			...(entry.resource === "future" ? { longRunningFailure: 4 as const, } : {}),
		},
	};
}

export type LocalValidationCase = {
	id: string;
	persona: Persona;
	args: string[];
	risk: "local-validation";
	expectedCode: number;
	expectedMessage: string;
	featureProbe?: string;
};

export const readOnlyCommandCases: ReadOnlyCommandCase[] = [
	{
		id: "project-list",
		persona: "expert",
		resource: "project",
		action: "list",
		args: ["project", "list",],
		risk: "read-only",
		requiresProject: false,
		resultShape: "array",
		sdkParity: "project-list",
		stableFields: ["projectKey",],
		featureProbe: "auth doctor could verify project reachability after credentials load",
	},
	{
		id: "project-get",
		persona: "expert",
		resource: "project",
		action: "get",
		args: ["project", "get",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "object",
		sdkParity: "project-get",
		stableFields: ["projectKey",],
	},
	{
		id: "project-metadata",
		persona: "expert",
		resource: "project",
		action: "metadata",
		args: ["project", "metadata",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "object",
		sdkParity: "project-metadata",
	},
	{
		id: "project-map",
		persona: "expert",
		resource: "project",
		action: "map",
		args: ["project", "map", "--max-nodes", "25", "--max-edges", "50",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "object",
		sdkParity: "project-map",
		stableFields: ["map",],
		featureProbe: "project map filtering/search would help large flows",
	},
	{
		id: "flow-zone-list",
		persona: "expert",
		resource: "flow-zone",
		action: "list",
		args: ["flow-zone", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "flow-zone-list",
		stableFields: ["id", "name",],
		featureProbe:
			"flow-zone list --include-default may be useful if DSS exposes default zone separately",
	},
	{
		id: "dataset-list",
		persona: "expert",
		resource: "dataset",
		action: "list",
		args: ["dataset", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "dataset-list",
		stableFields: ["name",],
		featureProbe: "dataset exists/delete --if-exists would simplify automation",
	},
	{
		id: "recipe-list",
		persona: "expert",
		resource: "recipe",
		action: "list",
		args: ["recipe", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "recipe-list",
		stableFields: ["name",],
		featureProbe: "recipe validate-update dry-run could prevent malformed recipe patches",
	},
	{
		id: "job-list",
		persona: "expert",
		resource: "job",
		action: "list",
		args: ["job", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "job-list",
		stableFields: ["id",],
		featureProbe: "job last --target TARGET would help inspect recent automation",
	},
	{
		id: "scenario-list",
		persona: "expert",
		resource: "scenario",
		action: "list",
		args: ["scenario", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "scenario-list",
		stableFields: ["id",],
		featureProbe: "scenario enable/disable commands would support safe maintenance workflows",
	},
	{
		id: "folder-list",
		persona: "expert",
		resource: "folder",
		action: "list",
		args: ["folder", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "folder-list",
		stableFields: ["id", "name",],
		featureProbe: "folder delete and folder sync would complete the managed-folder workflow",
	},
	{
		id: "variable-get",
		persona: "expert",
		resource: "variable",
		action: "get",
		args: ["variable", "get",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "object",
		sdkParity: "variable-get",
		stableFields: ["standard", "local",],
		featureProbe: "variable unset/diff would avoid full replacement for cleanup",
	},
	{
		id: "connection-infer",
		persona: "expert",
		resource: "connection",
		action: "infer",
		args: ["connection", "infer",],
		risk: "read-only",
		requiresProject: false,
		resultShape: "array",
		sdkParity: "connection-infer",
		stableFields: ["name",],
		featureProbe: "connection test NAME would verify write/read capabilities before mutating tests",
	},
	{
		id: "code-env-list",
		persona: "expert",
		resource: "code-env",
		action: "list",
		args: ["code-env", "list",],
		risk: "read-only",
		requiresProject: false,
		resultShape: "array",
		sdkParity: "code-env-list",
		stableFields: ["envName", "name",],
		featureProbe: "code-env packages NAME would expose package inventory",
	},
	{
		id: "notebook-list-jupyter",
		persona: "expert",
		resource: "notebook",
		action: "list-jupyter",
		args: ["notebook", "list-jupyter",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "notebook-list-jupyter",
		stableFields: ["name",],
		featureProbe: "notebook export/import would help agent-driven notebook workflows",
	},
	{
		id: "notebook-list-sql",
		persona: "expert",
		resource: "notebook",
		action: "list-sql",
		args: ["notebook", "list-sql",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "notebook-list-sql",
		stableFields: ["id", "name",],
		featureProbe: "notebook sessions --unload-all --dry-run could simplify cleanup",
	},
	{
		id: "dashboard-list",
		persona: "expert",
		resource: "dashboard",
		action: "list",
		args: ["dashboard", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "dashboard-list",
		stableFields: ["id", "name",],
	},
	{
		id: "insight-list",
		persona: "expert",
		resource: "insight",
		action: "list",
		args: ["insight", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "insight-list",
		stableFields: ["id", "name",],
	},
	{
		id: "wiki-settings",
		persona: "expert",
		resource: "wiki",
		action: "settings",
		args: ["wiki", "settings",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "object",
		sdkParity: "wiki-settings",
		stableFields: ["projectKey",],
	},
	{
		id: "wiki-list",
		persona: "expert",
		resource: "wiki",
		action: "list",
		args: ["wiki", "list",],
		risk: "read-only",
		requiresProject: true,
		resultShape: "array",
		sdkParity: "wiki-list",
		stableFields: ["article",],
	},
];

export const localValidationCases: LocalValidationCase[] = [
	{
		id: "unknown-long-flag",
		persona: "expert",
		args: ["flow-zone", "list", "--wat", "yes",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Unknown flag: --wat",
	},
	{
		id: "unknown-short-flag",
		persona: "error-prone",
		args: ["flow-zone", "list", "-z",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Unknown flag: -z",
	},
	{
		id: "removed-format-flag",
		persona: "newbie",
		args: ["flow-zone", "list", "--format", "csv",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Unknown flag: --format",
	},
	{
		id: "missing-flow-zone-create-name",
		persona: "newbie",
		args: ["flow-zone", "create",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "--name is required",
	},
	{
		id: "missing-flow-zone-move-object",
		persona: "newbie",
		args: ["flow-zone", "move", "zone-1",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "At least one object is required",
	},
	{
		id: "invalid-flow-zone-color",
		persona: "error-prone",
		args: ["flow-zone", "create", "--name", "Exports", "--color", "red",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "--color must be a hex color",
	},
	{
		id: "malformed-flow-zone-object",
		persona: "error-prone",
		args: ["flow-zone", "move", "zone-1", "--object", "DATASET",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Invalid --object value: DATASET",
	},
	{
		id: "invalid-flow-zone-object-type",
		persona: "error-prone",
		args: ["flow-zone", "move", "zone-1", "--object", "BANANA:id",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Invalid flow zone object type: BANANA",
	},
	{
		id: "empty-flow-zone-id",
		persona: "error-prone",
		args: ["flow-zone", "get", "",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Flow zone id must not be empty",
	},
];
