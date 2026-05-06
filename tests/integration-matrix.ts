export type Persona = "newbie" | "expert" | "error-prone";
export type RiskLevel = "read-only" | "local-validation" | "mutating" | "expensive";
export type ResultShape = "array" | "object" | "string";

export type FormatChecks = {
	json?: boolean;
	quiet?: boolean;
	table?: boolean;
	tsv?: boolean;
};

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
	| "notebook-list-jupyter"
	| "notebook-list-sql";

export type ReadOnlyCommandCase = {
	id: string;
	persona: Persona;
	resource: string;
	action: string;
	args: string[];
	risk: "read-only";
	requiresProject: boolean;
	resultShape: ResultShape;
	formatChecks: FormatChecks;
	sdkParity?: SdkParityKind;
	stableFields?: string[];
	featureProbe?: string;
};

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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, },
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
		formatChecks: { json: true, quiet: true, },
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
		formatChecks: { json: true, quiet: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
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
		formatChecks: { json: true, quiet: true, table: true, tsv: true, },
		sdkParity: "notebook-list-sql",
		stableFields: ["id", "name",],
		featureProbe: "notebook sessions --unload-all --dry-run could simplify cleanup",
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
		id: "invalid-format",
		persona: "newbie",
		args: ["flow-zone", "list", "--format", "csv",],
		risk: "local-validation",
		expectedCode: 1,
		expectedMessage: "Invalid --format value: csv",
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
