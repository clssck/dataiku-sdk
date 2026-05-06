import { describe, expect, it, } from "bun:test";
import {
	CodeEnvsResource,
	ConnectionsResource,
	DataikuClient,
	DatasetsResource,
	FlowZonesResource,
	FoldersResource,
	JobsResource,
	NotebooksResource,
	ProjectsResource,
	RecipesResource,
	ScenariosResource,
	SqlResource,
	VariablesResource,
} from "../src/index.js";

const SDK_SURFACE: Array<
	{ key: keyof DataikuClient; ctor: new(...args: never[]) => unknown; methods: string[]; }
> = [
	{ key: "projects", ctor: ProjectsResource, methods: ["list", "get", "metadata", "flow", "map",], },
	{
		key: "flowZones",
		ctor: FlowZonesResource,
		methods: ["list", "get", "create", "update", "delete", "moveItems", "moveItem", "graph",],
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
		methods: ["create", "list", "resolveId", "get", "contents", "download", "upload", "deleteFile",],
	},
	{ key: "variables", ctor: VariablesResource, methods: ["get", "set",], },
	{ key: "connections", ctor: ConnectionsResource, methods: ["list", "infer",], },
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
