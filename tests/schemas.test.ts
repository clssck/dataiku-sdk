import { describe, expect, it, } from "bun:test";
import {
	BuildModeSchema,
	CodeEnvSummarySchema,
	ConnectionSummarySchema,
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
	DatasetSummarySchema,
	FolderSummarySchema,
	JobSummarySchema,
	JupyterNotebookSummarySchema,
	parseSchema,
	ProjectSummaryArraySchema,
	ProjectSummarySchema,
	ProjectVariablesSchema,
	RecipeSummarySchema,
	ScenarioSummarySchema,
	SqlNotebookSummarySchema,
} from "../src/schemas.js";

describe("ProjectSummary", () => {
	it("accepts valid data and preserves extra fields", () => {
		const data = { projectKey: "FOO", name: "My Project", extraField: 123, };
		const result = parseSchema(ProjectSummarySchema, data,);
		expect(result,).toEqual(data,);
	});

	it("throws when projectKey is missing", () => {
		expect(
			() => parseSchema(ProjectSummarySchema, { name: "My Project", },),
		).toThrow();
	});

	it("throws when projectKey is wrong type", () => {
		expect(
			() => parseSchema(ProjectSummarySchema, { projectKey: 42, name: "My Project", },),
		).toThrow();
	});

	it("accepts missing optional shortDesc", () => {
		const data = { projectKey: "FOO", name: "My Project", };
		expect(parseSchema(ProjectSummarySchema, data,),).toEqual(data,);
	});
});

describe("DatasetSummary", () => {
	it("accepts valid data with only required name", () => {
		const data = { name: "my_ds", };
		expect(parseSchema(DatasetSummarySchema, data,),).toEqual(data,);
	});

	it("throws when name is missing", () => {
		expect(
			() => parseSchema(DatasetSummarySchema, {},),
		).toThrow();
	});

	it("preserves extra DSS fields", () => {
		const data = { name: "ds", customMeta: { tier: "gold", }, zoneId: "z1", };
		const result = parseSchema(DatasetSummarySchema, data,);
		expect(result.customMeta.tier,).toBe("gold",);
		expect(result.zoneId,).toBe("z1",);
	});
});

describe("DatasetDetails", () => {
	it("accepts valid data with nested params", () => {
		const data = { name: "ds", type: "Filesystem", managed: true, params: { connection: "fs", }, };
		expect(parseSchema(DatasetDetailsSchema, data,),).toEqual(data,);
	});

	it("throws when name is missing", () => {
		expect(
			() => parseSchema(DatasetDetailsSchema, { type: "Filesystem", },),
		).toThrow();
	});
});

describe("DatasetSchema", () => {
	it("accepts valid columns", () => {
		const data = { columns: [{ name: "id", type: "int", },], };
		expect(parseSchema(DatasetSchemaSchema, data,),).toEqual(data,);
	});

	it("accepts column with optional comment", () => {
		const data = { columns: [{ name: "id", type: "int", comment: "Primary key", },], };
		expect(parseSchema(DatasetSchemaSchema, data,),).toEqual(data,);
	});

	it("throws when columns is missing", () => {
		expect(
			() => parseSchema(DatasetSchemaSchema, {},),
		).toThrow();
	});

	it("throws when a column is missing name", () => {
		expect(
			() => parseSchema(DatasetSchemaSchema, { columns: [{ type: "int", },], },),
		).toThrow();
	});

	it("throws when a column is missing type", () => {
		expect(
			() => parseSchema(DatasetSchemaSchema, { columns: [{ name: "id", },], },),
		).toThrow();
	});
});

describe("RecipeSummary", () => {
	it("accepts valid data", () => {
		const data = { name: "compute_foo", };
		expect(parseSchema(RecipeSummarySchema, data,),).toEqual(data,);
	});

	it("throws when name is missing", () => {
		expect(
			() => parseSchema(RecipeSummarySchema, {},),
		).toThrow();
	});
});

describe("JobSummary", () => {
	it("accepts valid data with nested def and baseStatus", () => {
		const data = { def: { id: "j1", }, baseStatus: { state: "DONE", }, };
		expect(parseSchema(JobSummarySchema, data,),).toEqual(data,);
	});

	it("accepts empty object since all fields are optional", () => {
		const data = {};
		expect(parseSchema(JobSummarySchema, data,),).toEqual(data,);
	});
});

describe("ScenarioSummary", () => {
	it("accepts valid data", () => {
		const data = { id: "s1", name: "Daily", };
		expect(parseSchema(ScenarioSummarySchema, data,),).toEqual(data,);
	});

	it("throws when id is missing", () => {
		expect(
			() => parseSchema(ScenarioSummarySchema, { name: "Daily", },),
		).toThrow();
	});
});

describe("FolderSummary", () => {
	it("accepts valid data", () => {
		const data = { id: "f1", };
		expect(parseSchema(FolderSummarySchema, data,),).toEqual(data,);
	});

	it("throws when id is missing", () => {
		expect(
			() => parseSchema(FolderSummarySchema, { name: "orphan", },),
		).toThrow();
	});
});

describe("ProjectVariables", () => {
	it("accepts valid standard and local records", () => {
		const data = { standard: { a: 1, }, local: { b: 2, }, };
		expect(parseSchema(ProjectVariablesSchema, data,),).toEqual(data,);
	});

	it("throws when standard is missing", () => {
		expect(
			() => parseSchema(ProjectVariablesSchema, { local: { b: 2, }, },),
		).toThrow();
	});

	it("throws when local is missing", () => {
		expect(
			() => parseSchema(ProjectVariablesSchema, { standard: { a: 1, }, },),
		).toThrow();
	});
});

describe("ConnectionSummary", () => {
	it("accepts valid data", () => {
		const data = { name: "pg_conn", };
		expect(parseSchema(ConnectionSummarySchema, data,),).toEqual(data,);
	});

	it("throws when name is missing", () => {
		expect(
			() => parseSchema(ConnectionSummarySchema, {},),
		).toThrow();
	});
});

describe("CodeEnvSummary", () => {
	it("accepts valid data", () => {
		const data = { envName: "py39", envLang: "PYTHON", };
		expect(parseSchema(CodeEnvSummarySchema, data,),).toEqual(data,);
	});

	it("throws when envLang is missing", () => {
		expect(
			() => parseSchema(CodeEnvSummarySchema, { envName: "py39", },),
		).toThrow();
	});

	it("throws when envName is missing", () => {
		expect(
			() => parseSchema(CodeEnvSummarySchema, { envLang: "PYTHON", },),
		).toThrow();
	});
});

describe("BuildMode", () => {
	it("accepts valid literal RECURSIVE_BUILD", () => {
		expect(parseSchema(BuildModeSchema, "RECURSIVE_BUILD",),).toBe("RECURSIVE_BUILD",);
	});

	it("accepts NON_RECURSIVE_FORCED_BUILD", () => {
		expect(
			parseSchema(BuildModeSchema, "NON_RECURSIVE_FORCED_BUILD",),
		).toBe("NON_RECURSIVE_FORCED_BUILD",);
	});

	it("throws for invalid mode string", () => {
		expect(
			() => parseSchema(BuildModeSchema, "INVALID_MODE",),
		).toThrow();
	});

	it("throws for non-string value", () => {
		expect(
			() => parseSchema(BuildModeSchema, 42,),
		).toThrow();
	});
});

describe("Array schemas", () => {
	it("DatasetSummaryArraySchema accepts valid array", () => {
		const data = [{ name: "a", }, { name: "b", },];
		expect(parseSchema(DatasetSummaryArraySchema, data,),).toEqual(data,);
	});

	it("DatasetSummaryArraySchema throws when second element is invalid", () => {
		expect(
			() => parseSchema(DatasetSummaryArraySchema, [{ name: "a", }, { notName: true, },],),
		).toThrow();
	});

	it("ProjectSummaryArraySchema accepts empty array", () => {
		expect(parseSchema(ProjectSummaryArraySchema, [],),).toEqual([],);
	});

	it("ProjectSummaryArraySchema accepts valid entries", () => {
		const data = [{ projectKey: "A", name: "Alpha", }, { projectKey: "B", name: "Beta", },];
		expect(parseSchema(ProjectSummaryArraySchema, data,),).toEqual(data,);
	});

	it("ProjectSummaryArraySchema throws for invalid entry", () => {
		expect(
			() => parseSchema(ProjectSummaryArraySchema, [{ name: "no key", },],),
		).toThrow();
	});
});

describe("JupyterNotebookSummary", () => {
	it("accepts valid data", () => {
		const data = { name: "nb1", projectKey: "P", language: "python", };
		expect(parseSchema(JupyterNotebookSummarySchema, data,),).toEqual(data,);
	});

	it("accepts kernelSpec with only required name", () => {
		const data = {
			name: "nb1",
			projectKey: "P",
			language: "python",
			kernelSpec: { name: "python3", },
		};
		expect(parseSchema(JupyterNotebookSummarySchema, data,),).toEqual(data,);
	});

	it("throws when language is missing", () => {
		expect(
			() => parseSchema(JupyterNotebookSummarySchema, { name: "nb1", projectKey: "P", },),
		).toThrow();
	});

	it("throws when projectKey is missing", () => {
		expect(
			() => parseSchema(JupyterNotebookSummarySchema, { name: "nb1", language: "python", },),
		).toThrow();
	});
});

describe("SqlNotebookSummary", () => {
	it("accepts valid data", () => {
		const data = { id: "sql1", language: "sql", connection: "pg", };
		expect(parseSchema(SqlNotebookSummarySchema, data,),).toEqual(data,);
	});

	it("throws when connection is missing", () => {
		expect(
			() => parseSchema(SqlNotebookSummarySchema, { id: "sql1", language: "sql", },),
		).toThrow();
	});

	it("throws when id is missing", () => {
		expect(
			() => parseSchema(SqlNotebookSummarySchema, { language: "sql", connection: "pg", },),
		).toThrow();
	});
});
