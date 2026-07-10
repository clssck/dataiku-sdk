import { describe, expect, it, } from "bun:test";
import {
	AnalysesResource,
	ApiServicesResource,
	CodeEnvsResource,
	ConnectionsResource,
	DashboardsResource,
	DataikuClient,
	DataQualityResource,
	DatasetsResource,
	FlowZonesResource,
	FoldersResource,
	FuturesResource,
	InsightsResource,
	JobsResource,
	MlTasksResource,
	ModelEvaluationStoresResource,
	NotebooksResource,
	ProjectsResource,
	RecipesResource,
	SavedModelsResource,
	ScenariosResource,
	SqlResource,
	VariablesResource,
	WikiResource,
} from "../src/index.js";

const SDK_SURFACE: Array<
	{ key: keyof DataikuClient; ctor: new(...args: never[]) => unknown; methods: string[]; }
> = [
	{ key: "projects", ctor: ProjectsResource, methods: ["list", "get", "metadata", "flow", "map",], },
	{ key: "futures", ctor: FuturesResource, methods: ["get", "peek", "state", "abort", "wait",], },
	{
		key: "flowZones",
		ctor: FlowZonesResource,
		methods: ["list", "get", "create", "update", "delete", "moveItems", "moveItem", "graph",],
	},
	{
		key: "dashboards",
		ctor: DashboardsResource,
		methods: ["list", "get", "create", "update", "delete",],
	},
	{
		key: "datasets",
		ctor: DatasetsResource,
		methods: [
			"list",
			"get",
			"schema",
			"preview",
			"metadata",
			"download",
			"create",
			"update",
			"delete",
		],
	},
	{
		key: "recipes",
		ctor: RecipesResource,
		methods: [
			"list",
			"get",
			"create",
			"update",
			"download",
			"downloadCode",
			"getPayload",
			"setPayload",
			"delete",
		],
	},
	{
		key: "jobs",
		ctor: JobsResource,
		methods: ["list", "get", "log", "build", "buildAndWait", "wait", "abort",],
	},
	{
		key: "scenarios",
		ctor: ScenariosResource,
		methods: ["list", "get", "create", "run", "status", "update", "delete", "runAndWait",],
	},
	{
		key: "folders",
		ctor: FoldersResource,
		methods: [
			"create",
			"list",
			"resolveId",
			"get",
			"update",
			"delete",
			"contents",
			"download",
			"upload",
			"deleteFile",
		],
	},
	{ key: "variables", ctor: VariablesResource, methods: ["get", "set",], },
	{ key: "connections", ctor: ConnectionsResource, methods: ["list", "infer",], },
	{
		key: "dataQuality",
		ctor: DataQualityResource,
		methods: [
			"rules",
			"listRules",
			"createRule",
			"updateRule",
			"deleteRule",
			"getRule",
			"status",
			"statusByPartition",
			"lastResults",
			"history",
			"computeRules",
			"computeRulesAndWait",
			"projectStatus",
			"projectTimeline",
		],
	},
	{
		key: "insights",
		ctor: InsightsResource,
		methods: ["list", "get", "create", "update", "delete",],
	},
	{
		key: "codeEnvs",
		ctor: CodeEnvsResource,
		methods: [
			"list",
			"get",
			"getDefinition",
			"create",
			"setDefinition",
			"setPackages",
			"updatePackages",
			"setJupyterSupport",
			"delete",
			"listUsages",
		],
	},
	{
		key: "sql",
		ctor: SqlResource,
		methods: ["startQuery", "streamResults", "finishStreaming", "query",],
	},
	{
		key: "notebooks",
		ctor: NotebooksResource,
		methods: [
			"listJupyter",
			"getJupyter",
			"saveJupyter",
			"deleteJupyter",
			"clearJupyterOutputs",
			"listJupyterSessions",
			"unloadJupyter",
			"listSql",
			"getSql",
			"saveSql",
			"deleteSql",
			"getSqlHistory",
			"clearSqlHistory",
		],
	},
	{
		key: "wiki",
		ctor: WikiResource,
		methods: ["settings", "list", "get", "create", "update", "delete",],
	},
	{
		key: "analyses",
		ctor: AnalysesResource,
		methods: ["list", "get", "create", "delete",],
	},
	{
		key: "mlTasks",
		ctor: MlTasksResource,
		methods: [
			"create",
			"status",
			"getSettings",
			"saveSettings",
			"train",
			"listTrainedModels",
			"trainedModelDetails",
			"deployToFlow",
			"delete",
		],
	},
	{
		key: "savedModels",
		ctor: SavedModelsResource,
		methods: ["list", "get", "listVersions", "versionDetails", "setActiveVersion", "delete",],
	},
	{
		key: "modelEvaluationStores",
		ctor: ModelEvaluationStoresResource,
		methods: ["list", "get", "create", "listEvaluations", "delete",],
	},
	{
		key: "apiServices",
		ctor: ApiServicesResource,
		methods: [
			"list",
			"create",
			"getSettings",
			"saveSettings",
			"addPredictionEndpoint",
			"listPackages",
			"getPackageSummary",
			"createPackage",
			"deletePackage",
			"downloadPackageArchive",
			"publishPackage",
		],
	},
];

describe("SDK public surface", () => {
	it("exposes every resource namespace and expected method", () => {
		const client = new DataikuClient({
			url: "http://127.0.0.1:1",
			apiKey: "test-key",
			projectKey: "TEST",
		},);

		for (const { key, ctor, methods, } of SDK_SURFACE) {
			const resource = client[key] as object;
			expect(resource, key,).toBeInstanceOf(ctor,);
			for (const method of methods) {
				expect(typeof (resource as Record<string, unknown>)[method], `${String(key,)}.${method}`,).toBe(
					"function",
				);
			}
		}
	});
});
