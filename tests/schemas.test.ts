import { describe, expect, it, } from "bun:test";
import {
	BuildModeSchema,
	CodeEnvSummarySchema,
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
	DataQualityTimelineSchema,
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
	DatasetSummarySchema,
	FlowZoneObjectTypeSchema,
	FlowZoneSchema,
	FolderSummarySchema,
	FutureStateSchema,
	FutureWaitResultSchema,
	InsightDetailsSchema,
	InsightSummaryArraySchema,
	InsightSummarySchema,
	JobSummarySchema,
	JupyterNotebookSummarySchema,
	parseSchema,
	ProjectSummaryArraySchema,
	ProjectSummarySchema,
	ProjectVariablesSchema,
	RecipeSummarySchema,
	ScenarioSummarySchema,
	SqlNotebookSummarySchema,
	WikiArticleDataArraySchema,
	WikiArticleDataSchema,
	WikiSettingsSchema,
	WikiTaxonomyNodeSchema,
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

describe("FlowZone", () => {
	it("accepts valid zone data and preserves DSS-managed fields", () => {
		const data = {
			id: "zone-1",
			name: "Exports",
			color: "#2ab1ac",
			items: [{ objectType: "DATASET", objectId: "orders", },],
			position: { x: 10, y: 20, },
		};
		expect(parseSchema(FlowZoneSchema, data,),).toEqual(data,);
	});

	it("throws when required identifiers are missing", () => {
		expect(
			() => parseSchema(FlowZoneSchema, { name: "Exports", },),
		).toThrow();
	});

	it("accepts supported flow object types", () => {
		expect(parseSchema(FlowZoneObjectTypeSchema, "MANAGED_FOLDER",),).toBe("MANAGED_FOLDER",);
		expect(parseSchema(FlowZoneObjectTypeSchema, "DATASET",),).toBe("DATASET",);
	});

	it("throws for unsupported flow object types", () => {
		expect(
			() => parseSchema(FlowZoneObjectTypeSchema, "UNKNOWN",),
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

describe("Wiki schemas", () => {
	it("accepts settings taxonomy and preserves DSS fields", () => {
		const data = {
			projectKey: "TEST",
			homeArticleId: "article-1",
			taxonomy: [{ id: "article-1", children: [{ id: "child-1", children: [], extra: true, },], },],
			custom: "preserved",
		};
		expect(parseSchema(WikiSettingsSchema, data,),).toEqual(data,);
		expect(parseSchema(WikiTaxonomyNodeSchema, data.taxonomy[0],),).toEqual(data.taxonomy[0],);
	});

	it("accepts article data and rejects missing metadata id", () => {
		const data = {
			article: { id: "article-1", name: "Article", projectKey: "TEST", layout: "DOCUMENT", },
			payload: "# Body",
			attachments: [],
		};
		expect(parseSchema(WikiArticleDataSchema, data,),).toEqual(data,);
		expect(() => parseSchema(WikiArticleDataSchema, { article: { name: "Article", }, },)).toThrow();
	});
});

describe("Dashboard schemas", () => {
	it("accepts summary/details data and preserves layout fields", () => {
		const summary = { id: "dash-1", name: "Dashboard", projectKey: "TEST", numPages: 1, };
		const details = {
			id: "dash-1",
			name: "Dashboard",
			projectKey: "TEST",
			pages: [{ id: "page-1", grid: { tiles: [], }, },],
		};
		expect(parseSchema(DashboardSummarySchema, summary,),).toEqual(summary,);
		expect(parseSchema(DashboardDetailsSchema, details,),).toEqual(details,);
	});

	it("rejects dashboard entries without required identifiers", () => {
		expect(() => parseSchema(DashboardSummarySchema, { name: "Dashboard", },)).toThrow();
		expect(() => parseSchema(DashboardDetailsSchema, { id: "dash-1", },)).toThrow();
	});
});

describe("Insight schemas", () => {
	it("accepts summary/details data and preserves params", () => {
		const summary = { id: "insight-1", name: "Insight", type: "chart", projectKey: "TEST", };
		const details = {
			id: "insight-1",
			name: "Insight",
			type: "chart",
			projectKey: "TEST",
			params: { view: "main", },
		};
		expect(parseSchema(InsightSummarySchema, summary,),).toEqual(summary,);
		expect(parseSchema(InsightDetailsSchema, details,),).toEqual(details,);
	});

	it("rejects insight entries without id or name", () => {
		expect(() => parseSchema(InsightSummarySchema, { name: "Insight", },)).toThrow();
		expect(() => parseSchema(InsightDetailsSchema, { id: "insight-1", },)).toThrow();
	});
});

describe("DataQuality schemas", () => {
	it("accepts rules response and preserves rule config fields", () => {
		const rule = {
			id: "rule-1",
			type: "RecordCountInRangeRule",
			displayName: "Has rows",
			softMinimum: 1,
			softMinimumEnabled: true,
		};
		const data = { monitor: { enabled: true, }, checks: [rule,], displayedState: { status: "OK", }, };
		expect(parseSchema(DataQualityRulesSchema, data,),).toEqual(data,);
		expect(parseSchema(DataQualityRuleSchema, rule,),).toEqual(rule,);
	});

	it("validates rule result/status/compute wrapper shapes", () => {
		expect(parseSchema(DataQualityRuleResultSchema, { id: "rule-1", outcome: "OK", extra: true, },),)
			.toEqual({ id: "rule-1", outcome: "OK", extra: true, },);
		expect(parseSchema(DataQualityStatusByPartitionSchema, { NP: { status: "OK", }, },),)
			.toEqual({ NP: { status: "OK", }, },);
		expect(parseSchema(DataQualityComputeResultSchema, { jobId: "job-1", },),).toEqual({
			jobId: "job-1",
		},);
	});

	it("rejects data quality rules without id", () => {
		expect(() => parseSchema(DataQualityRuleSchema, { displayName: "No id", },)).toThrow();
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

	it("new wiki/dashboard/insight/data-quality array schemas validate entries", () => {
		expect(
			parseSchema(WikiArticleDataArraySchema, [{ article: { id: "article-1", }, payload: "body", },],),
		).toHaveLength(1,);
		expect(parseSchema(DashboardSummaryArraySchema, [{ id: "dash-1", name: "Dashboard", },],),)
			.toHaveLength(1,);
		expect(parseSchema(InsightSummaryArraySchema, [{ id: "insight-1", name: "Insight", },],),)
			.toHaveLength(1,);
		expect(parseSchema(DataQualityRuleArraySchema, [{ id: "rule-1", displayName: "Rule", },],),)
			.toHaveLength(1,);
		expect(parseSchema(DataQualityRuleResultArraySchema, [{ id: "rule-1", outcome: "OK", },],),)
			.toHaveLength(1,);
		expect(parseSchema(DataQualityStatusSchema, "OK",),).toBe("OK",);
		expect(parseSchema(DataQualityStatusSchema, { outcome: "SUCCESS", },),).toHaveProperty(
			"outcome",
			"SUCCESS",
		);
		expect(parseSchema(DataQualityProjectStatusSchema, { orders: { outcome: "SUCCESS", }, },),)
			.toHaveProperty("orders.outcome", "SUCCESS",);
		expect(parseSchema(DataQualityTimelineSchema, [{ day: "2026-05-07", outcome: "SUCCESS", },],),)
			.toHaveLength(1,);
		expect(parseSchema(FutureStateSchema, { jobId: "future-1", hasResult: false, },),)
			.toHaveProperty("jobId", "future-1",);
		expect(parseSchema(FutureWaitResultSchema, {
			futureId: "future-1",
			state: "DONE",
			elapsedMs: 1,
			pollCount: 1,
			success: true,
			hasResult: true,
		},),).toHaveProperty("success", true,);
		expect(() => parseSchema(DashboardSummaryArraySchema, [{ id: "dash-1", },],)).toThrow();
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
