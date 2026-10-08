import { describe, expect, it, } from "bun:test";
import { typeBoxCommandOutputSchemas, } from "../../src/cli/output-schemas.js";
import { shapeCommandResult, } from "../../src/cli/result-shapes.js";
import { cliEnv, dss, sendJson, withCliServer, } from "./_harness.js";

const NO_FLAGS: Record<string, string | boolean> = {};

function failedJob(): Record<string, unknown> {
	return {
		removed: false,
		initiator: { login: "engineer@example.com", displayName: "engineer", },
		baseStatus: {
			def: {
				type: "NON_RECURSIVE_FORCED_BUILD",
				projectKey: "PROJ",
				id: "Build_py_out__NP__2026",
				name: "Build py_out (NP)",
				initiator: "engineer@example.com",
				triggeredFrom: "API",
				outputs: [
					{
						type: "DATASET",
						targetDatasetProjectKey: "PROJ",
						targetDataset: "py_out",
						targetPartition: "NP",
					},
					{
						type: "DATASET",
						targetDatasetProjectKey: "OTHER",
						targetDataset: "events",
						targetPartition: "2024-01",
					},
					{
						type: "MANAGED_FOLDER",
						targetManagedFolderProjectKey: "PROJ",
						targetManagedFolder: "abcd1234",
						targetPartition: "NP",
					},
				],
			},
			state: "FAILED",
			jobStartTime: 1000,
			jobEndTime: 2000,
			activities: {
				python_py_out_NP: {
					activityId: "python_py_out_NP",
					recipeName: "python_py_out",
					recipeType: "python",
					state: "FAILED",
					totalTime: 132866,
					warnings: { warnings: {}, totalCount: 2, },
					firstFailure: {
						errorType: "<class 'ValueError'>",
						message: "short",
						detailedMessage: "At line 3: boom",
						stackTrace: [],
					},
					sources: [{ type: "DATASET", id: "PROJ.cust_prep", },],
				},
				sync_NP: {
					activityId: "sync_NP",
					recipeName: "sync",
					recipeType: "sync",
					state: "RUNNING",
					totalTime: 5,
					warnings: { warnings: {}, totalCount: 0, },
					message: "Execution container is initializing",
				},
			},
		},
		globalState: { notStarted: 0, failed: 1, done: 0, running: 1, total: 2, },
		error: {
			errorType: "<class 'ValueError'>",
			message: "short",
			detailedMessage: "At line 3: boom",
		},
		logTail: { totalLines: 0, lines: ["x".repeat(500,),], },
		runtimeSummary: { activities: [], },
	};
}

function infoFixture(): Record<string, unknown> {
	return {
		type: "UploadedFiles",
		name: "customers",
		partitioned: false,
		analyses: [],
		dataset: {
			type: "UploadedFiles",
			managed: false,
			name: "customers",
			formatType: "csv",
			tags: [],
			params: { uploadConnection: "filesystem_managed", },
			schema: {
				columns: [
					{ name: "id", type: "bigint", maxLength: -1, meaning: "", comment: "", },
					{ name: "email", type: "string", meaning: "Email", comment: "contact", },
				],
			},
			metrics: { probes: [], },
		},
		dataQualityStatus: { scope: "RULE", numberItems: 0, },
		recipes: [{ name: "prep", type: "shaker", }, { name: "group", type: "grouping", },],
		buildable: false,
		upstreamBuildable: false,
		downstreamBuildable: true,
		versioning: { lastModifiedOn: 99, lastModifiedBy: { login: "me", displayName: "Me", }, },
		timeline: { items: [{ action: "DATASET_EDIT", },], },
	};
}

function datasetFixture(): Record<string, unknown> {
	return {
		type: "Filesystem",
		managed: true,
		featureGroup: false,
		name: "by_country",
		projectKey: "PROJ",
		checklists: { checklists: [], },
		checks: [],
		customMeta: { kv: {}, },
		formatParams: {
			separator: "\t",
			compress: "",
			hiveSeparators: ["\u0002",],
			skipRowsBeforeHeader: 0,
		},
		versionTag: { versionNumber: 1, },
		creationTag: { versionNumber: 0, },
		tags: [],
		params: {
			connection: "fs",
			path: "${projectKey}/by_country",
			filesSelectionRules: { includeRules: [], },
		},
		schema: { columns: [{ name: "c", type: "string", comment: "", },], userModified: true, },
		metrics: { probes: [{ type: "basic", },], },
		metricsChecks: { checks: [], },
	};
}

async function jobGet(args: string[],): Promise<string> {
	let stdout = "";
	await withCliServer((req, res,) => {
		expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
			"/public/api/projects/TEST/jobs/Build_py_out__NP__2026/",
		);
		sendJson(res, failedJob(),);
	}, async (url,) => {
		const result = await dss(["job", "get", "Build_py_out__NP__2026", ...args,], {
			env: cliEnv(url,),
		},);
		expect(result.stderr,).toBe("",);
		stdout = result.stdout;
	},);
	return stdout;
}

describe("job get compact shape", () => {
	it("maps state, outputs, progress, error and activities and never includes the log tail", () => {
		const shaped = shapeCommandResult("job", "get", failedJob(), NO_FLAGS,) as Record<
			string,
			unknown
		>;
		expect(shaped,).toEqual({
			id: "Build_py_out__NP__2026",
			name: "Build py_out (NP)",
			type: "NON_RECURSIVE_FORCED_BUILD",
			state: "FAILED",
			initiator: "engineer@example.com",
			triggeredFrom: "API",
			startTime: 1000,
			endTime: 2000,
			outputs: [
				{ type: "DATASET", id: "py_out", },
				{ type: "DATASET", id: "events", projectKey: "OTHER", partition: "2024-01", },
				{ type: "MANAGED_FOLDER", id: "abcd1234", },
			],
			progress: { notStarted: 0, failed: 1, done: 0, running: 1, total: 2, },
			error: { type: "<class 'ValueError'>", message: "At line 3: boom", },
			activities: [
				{
					id: "python_py_out_NP",
					recipe: "python_py_out",
					recipeType: "python",
					state: "FAILED",
					totalTime: 132866,
					warnings: 2,
					error: { type: "<class 'ValueError'>", message: "At line 3: boom", },
				},
				{
					id: "sync_NP",
					recipe: "sync",
					recipeType: "sync",
					state: "RUNNING",
					totalTime: 5,
					warnings: 0,
					message: "Execution container is initializing",
				},
			],
		},);
		expect(JSON.stringify(shaped,),).not.toContain("logTail",);
		expect(JSON.stringify(shaped,),).not.toContain("xxxx",);
	});

	it("omits absent optional fields instead of emitting null", () => {
		const shaped = shapeCommandResult("job", "get", {
			baseStatus: { def: { id: "J", outputs: [], }, state: "DONE", activities: {}, },
		}, NO_FLAGS,) as Record<string, unknown>;
		expect(shaped,).toEqual({ id: "J", state: "DONE", outputs: [], activities: [], },);
	});
});

describe("dataset info compact shape", () => {
	it("keeps identity, schema, flow neighbours and freshness only", () => {
		expect(shapeCommandResult("dataset", "info", infoFixture(), NO_FLAGS,),).toEqual({
			name: "customers",
			type: "UploadedFiles",
			managed: false,
			connection: "filesystem_managed",
			formatType: "csv",
			partitioned: false,
			schema: [
				{ name: "id", type: "bigint", },
				{ name: "email", type: "string", meaning: "Email", comment: "contact", },
			],
			recipes: ["prep", "group",],
			buildable: false,
			upstreamBuildable: false,
			downstreamBuildable: true,
			lastModifiedOn: 99,
			lastModifiedBy: "me",
		},);
	});

	it("reports tags, data quality and the last build when DSS has them", () => {
		const raw = infoFixture();
		(raw["dataset"] as Record<string, unknown>)["tags"] = ["prod",];
		(raw["dataset"] as Record<string, unknown>)["params"] = { connection: "pg", };
		raw["dataQualityStatus"] = { scope: "RULE", numberItems: 3, numberWorstOutcome: 1, };
		raw["lastBuild"] = { jobId: "J1", buildSuccess: true, buildEndTime: 5, id: "customers", };
		const shaped = shapeCommandResult("dataset", "info", raw, NO_FLAGS,) as Record<string, unknown>;
		expect(shaped["connection"],).toBe("pg",);
		expect(shaped["tags"],).toEqual(["prod",],);
		expect(shaped["dataQuality"],).toEqual({
			scope: "RULE",
			numberItems: 3,
			numberWorstOutcome: 1,
		},);
		expect(shaped["lastBuild"],).toEqual({ jobId: "J1", success: true, endTime: 5, },);
	});
});

describe("dataset get / project get pruning", () => {
	it("drops empty values and metadata blocks, keeps false and 0, and keeps the settings structure", () => {
		const raw = datasetFixture();
		const before = JSON.stringify(raw,);
		expect(shapeCommandResult("dataset", "get", raw, NO_FLAGS,),).toEqual({
			type: "Filesystem",
			managed: true,
			featureGroup: false,
			name: "by_country",
			projectKey: "PROJ",
			formatParams: { separator: "\t", skipRowsBeforeHeader: 0, },
			params: { connection: "fs", path: "${projectKey}/by_country", },
			schema: { columns: [{ name: "c", type: "string", },], userModified: true, },
		},);
		expect(JSON.stringify(raw,),).toBe(before,);
	});

	it("collapses project permission flags into the denied ones without losing any", () => {
		const raw = {
			projectKey: "PROJ",
			name: "Proj",
			isProjectAdmin: false,
			canReadProjectContent: true,
			canWriteProjectContent: false,
			canExportGitRepository: false,
			canSomethingElse: "kept",
			objectImgHash: 1,
			isProjectImg: false,
			defaultImgColor: "#fff",
			imgPattern: 6,
			metrics: { probes: [], },
			versionTag: { versionNumber: 0, },
			creationTag: { versionNumber: 0, },
			checklists: { checklists: [], },
			contributors: [],
			tutorialProject: false,
		};
		const shaped = shapeCommandResult("project", "get", raw, NO_FLAGS,) as Record<string, unknown>;
		expect(shaped,).toEqual({
			projectKey: "PROJ",
			name: "Proj",
			canSomethingElse: "kept",
			tutorialProject: false,
			deniedPermissions: ["isProjectAdmin", "canWriteProjectContent", "canExportGitRepository",],
		},);
	});

	it("always reports deniedPermissions, empty when everything is granted", () => {
		const shaped = shapeCommandResult("project", "get", {
			projectKey: "P",
			name: "P",
			canReadProjectContent: true,
		}, NO_FLAGS,);
		expect(shaped,).toEqual({ projectKey: "P", name: "P", deniedPermissions: [], },);
	});
});

describe("bypassing the compact default", () => {
	it("returns the DSS object for --full and for --fields", () => {
		const raw = failedJob();
		expect(shapeCommandResult("job", "get", raw, { full: true, },),).toBe(raw,);
		expect(shapeCommandResult("job", "get", raw, { fields: "state", },),).toBe(raw,);
	});

	it("leaves other actions and non-object results unchanged and still shapes lists", () => {
		const raw = { baseStatus: {}, };
		expect(shapeCommandResult("job", "summary", raw, NO_FLAGS,),).toBe(raw,);
		expect(shapeCommandResult("job", "get", "text", NO_FLAGS,),).toBe("text",);
		expect(
			shapeCommandResult("dataset", "list", [{
				name: "a",
				type: "S3",
				managed: true,
				params: { connection: "c", },
			},], NO_FLAGS,),
		).toEqual([{ name: "a", type: "S3", managed: true, connection: "c", },],);
	});
});

describe("CLI job get output", () => {
	it("is compact by default, raw with --full, and a raw projection with --fields", async () => {
		const compact = JSON.parse(await jobGet([],),) as Record<string, unknown>;
		expect(compact["state"],).toBe("FAILED",);
		expect("logTail" in compact,).toBe(false,);
		expect(JSON.parse(await jobGet(["--full",],),),).toEqual(failedJob(),);
		expect(JSON.parse(await jobGet(["--fields", "baseStatus.state",],),),).toEqual({
			"baseStatus.state": "FAILED",
		},);
	});
});

describe("discovery output schemas of the compact reads", () => {
	const schemas = typeBoxCommandOutputSchemas();

	it("describes the compact job.get and dataset.info defaults", () => {
		const job = schemas["job.get"] as { properties: Record<string, unknown>; };
		expect(Object.keys(job.properties,),).toEqual([
			"id",
			"name",
			"type",
			"state",
			"initiator",
			"triggeredFrom",
			"startTime",
			"endTime",
			"outputs",
			"progress",
			"error",
			"activities",
		],);
		const info = schemas["dataset.info"] as { properties: Record<string, unknown>; };
		expect("schema" in info.properties,).toBe(true,);
		expect("lastBuild" in info.properties,).toBe(true,);
	});

	it("describes dataset.get and project.get without the dropped metadata", () => {
		const project = schemas["project.get"] as {
			properties: Record<string, unknown>;
			required: string[];
		};
		expect("versionTag" in project.properties,).toBe(false,);
		expect("creationTag" in project.properties,).toBe(false,);
		expect("deniedPermissions" in project.properties,).toBe(true,);
		expect(project.required,).toEqual(["projectKey", "name", "deniedPermissions",],);
		const dataset = schemas["dataset.get"] as { properties: Record<string, unknown>; };
		expect("name" in dataset.properties,).toBe(true,);
		expect("schema" in dataset.properties,).toBe(true,);
	});
});
