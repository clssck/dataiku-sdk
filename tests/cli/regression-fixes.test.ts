import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { cleanupLedgerEntry, } from "../../src/cli/helpers/cleanup.js";
import {
	cliEnv,
	dss,
	dssFailure,
	dssWithInput,
	readBody,
	sendJson,
	withCliServer,
} from "./_harness.js";

const hermeticEnv = {
	PATH: process.env.PATH ?? "",
	HOME: process.env.HOME ?? "",
	DATAIKU_DISABLE_ENV: "1",
};

function cliEnvWithoutProject(url: string,) {
	return {
		...cliEnv(url,),
		DATAIKU_PROJECT_KEY: "",
	};
}

describe("CLI regression fixes", () => {
	it("advertises project map as project-scoped with a project-key flag", () => {
		const registry = buildCommandRegistry();
		const map = registry.project?.map;

		expect(map?.usage,).toBe(
			"dss project map [--max-nodes N] [--max-edges N] [--include-raw] [--project-key KEY]",
		);
		expect(map?.requiresProject,).toBe(true,);
		expect(map?.flags,).toContainEqual(
			expect.objectContaining({ name: "project-key", kind: "value", },),
		);
		expect(map?.optionalFlags,).toContain("project-key",);
	});

	it("advertises and records cleanup for project duplicates", () => {
		const registry = buildCommandRegistry();
		const duplicate = registry.project?.duplicate;

		expect(duplicate?.flags,).toContainEqual(
			expect.objectContaining({ name: "record-cleanup", kind: "value", },),
		);
		expect(duplicate?.optionalFlags,).toContain("record-cleanup",);
		expect(duplicate?.cleanupCommand,).toBe("dss project delete <projectKey>",);

		const entry = cleanupLedgerEntry(
			"project",
			"duplicate",
			["SRC", "DST", "Destination",],
			{},
			{ targetProjectKey: "DST", },
			undefined,
		);

		expect(entry,).toMatchObject({
			action: "duplicate",
			resource: "project",
			name: "DST",
			cleanup: { argv: ["project", "delete", "DST", "--drop-data",], },
		},);
		expect(entry,).not.toHaveProperty("projectKey",);
	});

	it("honors dry-run for project library, statistics, and meaning creates without mutating DSS", async () => {
		const mutatingRequests: string[] = [];
		await withCliServer((req, res,) => {
			if (["POST", "PUT", "DELETE",].includes(req.method ?? "",)) {
				mutatingRequests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			}
			sendJson(res, { unexpected: true, },);
		}, async (url,) => {
			const projectLibraryPlan = JSON.parse(
				(await dss(["project-library", "create-file", "python/new.py", "--dry-run",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(projectLibraryPlan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "project-library",
				action: "create-file",
				path: "python/new.py",
				method: "POST",
				endpoint: "/public/api/projects/TEST/libraries/contents/python/new.py",
			},);

			const worksheetDefinition = {
				name: "Order stats",
				dataSpec: {
					inputDatasetSmartName: "orders",
					datasetSelection: {
						partitionSelectionMethod: "ALL",
						maxRecords: 30000,
						samplingMethod: "FULL",
					},
				},
			};
			const worksheetPlan = JSON.parse(
				(await dss([
					"statistics",
					"create-worksheet",
					"orders",
					"--data",
					JSON.stringify(worksheetDefinition,),
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(worksheetPlan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "statistics",
				action: "create-worksheet",
				dataset: "orders",
				method: "POST",
				endpoint: "/public/api/projects/TEST/datasets/orders/statistics/worksheets/",
				payload: worksheetDefinition,
			},);

			const meaningPlan = JSON.parse(
				(await dss([
					"meaning",
					"create",
					"customer_type",
					"CustomerType",
					"VALUES_LIST",
					"--data",
					'{"entries":["vip"]}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(meaningPlan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "meaning",
				action: "create",
				id: "customer_type",
				method: "POST",
				endpoint: "/public/api/meanings/",
				payload: {
					id: "customer_type",
					label: "CustomerType",
					type: "VALUES_LIST",
					description: null,
					entries: ["vip",],
					mappings: null,
					pattern: null,
					normalizationMode: null,
					detectable: false,
				},
			},);
		},);
		expect(mutatingRequests,).toEqual([],);
	});

	it("plans fixed mutation endpoints with exact methods and payloads without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const env = cliEnv(url,);
			const cases = [
				{
					name: "statistics create-worksheet",
					argv: [
						"statistics",
						"create-worksheet",
						"orders",
						"--data",
						'{"name":"Order stats"}',
						"--plan",
						"--project-key",
						"TEST",
					],
					expected: {
						resource: "statistics",
						action: "create-worksheet",
						method: "POST",
						endpoint: "/public/api/projects/TEST/datasets/orders/statistics/worksheets/",
						payload: { name: "Order stats", },
					},
				},
				{
					name: "continuous-activity stop",
					argv: [
						"continuous-activity",
						"stop",
						"build_flow",
						"--plan",
						"--project-key",
						"TEST",
					],
					expected: {
						resource: "continuous-activity",
						action: "stop",
						method: "POST",
						endpoint: "/public/api/projects/TEST/continuous-activities/build_flow/stop",
					},
					payloadAbsent: true,
				},
				{
					name: "api-deployer create-infra",
					argv: [
						"api-deployer",
						"create-infra",
						"--data",
						'{"id":"infra-1","name":"Infra 1"}',
						"--plan",
					],
					expected: {
						resource: "api-deployer",
						action: "create-infra",
						method: "POST",
						endpoint: "/public/api/api-deployer/infras",
						payload: { id: "infra-1", name: "Infra 1", },
					},
				},
				{
					name: "workspace create",
					argv: [
						"workspace",
						"create",
						"--data",
						'{"workspaceKey":"WS1","displayName":"Workspace 1"}',
						"--plan",
					],
					expected: {
						resource: "workspace",
						action: "create",
						method: "POST",
						endpoint: "/public/api/workspaces/",
						payload: { workspaceKey: "WS1", displayName: "Workspace 1", },
					},
				},
				{
					name: "meaning update",
					argv: [
						"meaning",
						"update",
						"customer_type",
						"--data",
						'{"label":"Customer type","type":"VALUES_LIST"}',
						"--plan",
					],
					expected: {
						resource: "meaning",
						action: "update",
						method: "PUT",
						endpoint: "/public/api/meanings/customer_type",
						payload: { label: "Customer type", type: "VALUES_LIST", },
					},
				},
				{
					name: "dashboard update keeps JSON name",
					argv: [
						"dashboard",
						"update",
						"dash-1",
						"--data",
						'{"name":"Kept from data"}',
						"--plan",
						"--project-key",
						"TEST",
					],
					expected: {
						resource: "dashboard",
						action: "update",
						method: "PUT",
						endpoint: "/public/api/projects/TEST/dashboards/dash-1/",
						payload: { name: "Kept from data", },
					},
				},
				{
					name: "bundle export",
					argv: ["bundle", "export", "v1", "--plan", "--project-key", "TEST",],
					expected: {
						resource: "bundle",
						action: "export",
						method: "PUT",
						endpoint: "/public/api/projects/TEST/bundles/exported/v1",
						payload: {},
					},
				},
			];

			for (const { name, argv, expected, payloadAbsent, } of cases) {
				const plan = JSON.parse((await dss(argv, { env, },)).stdout,) as Record<string, unknown>;
				expect(plan.plan, name,).toBe(true,);
				expect(plan, name,).toMatchObject(expected,);
				expect(plan.endpoint, name,).not.toBeUndefined();
				if (payloadAbsent) expect(Object.hasOwn(plan, "payload",), name,).toBe(false,);
				else expect(plan.payload, name,).toEqual(expected.payload,);
			}
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans project lifecycle and settings commands with concrete DSS endpoints", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const env = cliEnvWithoutProject(url,);
			const createPlan = JSON.parse(
				(await dss(["project", "create", "NEW", "New Project", "--owner", "alice", "--plan",], {
					env,
				},)).stdout,
			) as Record<string, unknown>;
			expect(createPlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "create",
				projectKey: "NEW",
				name: "New Project",
				method: "POST",
				endpoint: "/public/api/projects/",
				payload: {
					projectKey: "NEW",
					name: "New Project",
					owner: "alice",
				},
			},);

			const deletePlan = JSON.parse(
				(await dss(["project", "delete", "OLD", "--plan",], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(deletePlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "delete",
				projectKey: "OLD",
				method: "DELETE",
				endpoint:
					"/public/api/projects/OLD?clearManagedDatasets=false&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
			},);

			const dropDataPlan = JSON.parse(
				(await dss(["project", "delete", "OLD", "--drop-data", "--plan",], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(dropDataPlan.endpoint,).toBe(
				"/public/api/projects/OLD?clearManagedDatasets=true&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
			);

			const duplicatePlan = JSON.parse(
				(await dss(["project", "duplicate", "SRC", "DST", "Destination", "--plan",], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(duplicatePlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "duplicate",
				sourceKey: "SRC",
				targetKey: "DST",
				targetName: "Destination",
				method: "POST",
				endpoint: "/public/api/projects/SRC/duplicate/",
				payload: {
					targetProjectName: "Destination",
					targetProjectKey: "DST",
					duplicationMode: "MINIMAL",
					exportAnalysisModels: true,
					exportSavedModels: true,
					exportGitRepository: null,
					exportInsightsData: true,
					remapping: {},
				},
			},);

			const exportPlan = JSON.parse(
				(await dss(["project", "export", "SRC", "--output", "out.zip", "--plan",], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(exportPlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "export",
				projectKey: "SRC",
				output: "out.zip",
				method: "POST",
				endpoint: "/public/api/projects/SRC/export",
				payload: {},
				localWrites: [{ path: "out.zip", source: "remote project archive", after: "POST", },],
			},);

			const importPlan = JSON.parse(
				(await dss(["project", "import", "archive.zip", "--plan",], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(importPlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "import",
				filePath: "archive.zip",
				method: "POST",
				endpoint: "/public/api/projects/import/upload",
				payload: {
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: "archive.zip",
				},
			},);

			const settingsPayload = { settings: { showInitialTour: false, }, };
			const settingsPlan = JSON.parse(
				(await dss([
					"project",
					"settings-set",
					"--project-key",
					"SETTINGS",
					"--data",
					JSON.stringify(settingsPayload,),
					"--plan",
				], { env, },)).stdout,
			) as Record<string, unknown>;
			expect(settingsPlan,).toMatchObject({
				plan: true,
				resource: "project",
				action: "settings-set",
				method: "PUT",
				endpoint: "/public/api/projects/SETTINGS/settings",
				payload: settingsPayload,
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("rejects unsupported known flags on project list while accepting global flags", async () => {
		const failure = await dssFailure(["project", "list", "--name", "X",], { env: hermeticEnv, },);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "unknown_flag",
			category: "usage",
			exitCode: 1,
			resource: "project",
			action: "list",
		},);
		expect(report.error,).toBe("Unknown flag --name for project list",);

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/",);
			sendJson(res, [{ projectKey: "P1", name: "One", ignored: true, },],);
		}, async (url,) => {
			const success = await dss([
				"project",
				"list",
				"--timeout",
				"5000",
				"--json",
				"--fields",
				"projectKey,name",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(success.stdout,),).toEqual([{ projectKey: "P1", name: "One", },],);
		},);
	});
	it("does not expose arbitrary remote error response text", async () => {
		const sentinels = [
			"REMOTE_MESSAGE_SENTINEL",
			"REMOTE_API_KEY_SENTINEL",
			"REMOTE_PASSWORD_SENTINEL",
			"REMOTE_AUTHORIZATION_SENTINEL",
			"REMOTE_BEARER_SENTINEL",
			"REMOTE_TARGET_SENTINEL",
			"REMOTE_STATUS_TEXT_SENTINEL",
		];
		await withCliServer((req, res,) => {
			expect(req.url,).toContain("/public/api/projects/",);
			res.statusMessage = sentinels[6]!;
			sendJson(res, {
				message: sentinels[0],
				apiKey: sentinels[1],
				nested: {
					password: sentinels[2],
					authorization: sentinels[3],
					token: `Bearer ${sentinels[4]}`,
				},
				requestId: "req-safe-123",
				target: sentinels[5],
				elapsedMs: 37,
			}, 400,);
		}, async (url,) => {
			const failure = await dssFailure(["project", "list",], { env: cliEnv(url,), },);
			const combined = `${failure.stdout}\n${failure.stderr}`;
			for (const sentinel of sentinels) expect(combined,).not.toContain(sentinel,);
			const report = JSON.parse(failure.stderr,) as {
				code?: string;
				category?: string;
				exitCode?: number;
				requestId?: string;
				status?: number;
				retryable?: boolean;
				hint?: string;
				details?: { dssCategory?: string; statusText?: string; body?: string; };
			};
			expect(report,).toMatchObject({
				code: "validation_failed",
				category: "dss",
				exitCode: 2,
				requestId: "req-safe-123",
				status: 400,
				retryable: false,
				hint: expect.any(String,),
				details: {
					dssCategory: "validation",
					statusText: "Bad Request",
					body: JSON.stringify({
						requestId: "req-safe-123",
						elapsedMs: 37,
					},),
				},
			},);
			expect(report.details.body,).not.toContain("message",);
			expect(report.details.body,).not.toContain("apiKey",);
		},);
	});

	it("batch runs meta commands and dry-run validates steps deeply", async () => {
		const meta = await dss(["batch", "--data", '[["version"]]',], { env: hermeticEnv, },);
		const metaReport = JSON.parse(meta.stdout,) as {
			ok: boolean;
			completed: number;
			steps: Array<{ ok: boolean; resource: string; action: string; result: { version?: string; }; }>;
		};
		expect(metaReport.ok,).toBe(true,);
		expect(metaReport.completed,).toBe(1,);
		expect(metaReport.steps[0],).toMatchObject({
			ok: true,
			resource: "version",
			action: "run",
		},);
		expect(metaReport.steps[0]!.result.version,).toEqual(expect.any(String,),);

		const dryRunFailure = await dssFailure([
			"batch",
			"--dry-run",
			"--data",
			'[["dataset","get"],["sql","query","select 1"]]',
		], { env: hermeticEnv, },);
		expect(dryRunFailure.code,).toBe(1,);
		expect(dryRunFailure.stderr,).toBe("",);
		const dryRunReport = JSON.parse(dryRunFailure.stdout,) as {
			dryRun: boolean;
			steps: Array<{ runnable: boolean; error?: { code: string; error: string; exitCode: number; }; }>;
		};
		expect(dryRunReport.dryRun,).toBe(true,);
		expect(dryRunReport.steps,).toHaveLength(2,);
		expect(dryRunReport.steps[0],).toMatchObject({
			runnable: false,
			error: { code: "missing_required_arg", exitCode: 1, },
		},);
		expect(dryRunReport.steps[0]!.error?.error,).toContain("dss dataset get <name>",);
		expect(dryRunReport.steps[1],).toMatchObject({
			runnable: false,
			error: { code: "missing_required_flag", exitCode: 1, },
		},);
		expect(dryRunReport.steps[1]!.error?.error,).toContain(
			"One of --connection or --dataset is required",
		);
	});

	it("job log-url missing query parameters is a usage error before DSS access", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const failure = await dssFailure([
				"job",
				"log-url",
				"https://dss.example/dip/api/flow/jobs/cat-activity-log?projectKey=TEST&jobId=JOB",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "usage_error",
				category: "usage",
				exitCode: 1,
				resource: "job",
				action: "log-url",
			},);
			expect(report.error,).toContain("projectKey, jobId, and activityId",);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("app instance-manifest rejects unexpected positional arguments before DSS access", async () => {
		const failure = await dssFailure([
			"app",
			"instance-manifest",
			"EXTRA1",
			"EXTRA2",
		], { env: cliEnv("http://127.0.0.1:9/",), },);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "usage_error",
			category: "usage",
			exitCode: 1,
			resource: "app",
			action: "instance-manifest",
		},);
		expect(report.error,).toContain("Unexpected argument(s)",);
	});

	it("app save-instance-manifest rejects app-instance projects before PUT", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			const request = `${req.method ?? ""} ${url.pathname}`;
			requests.push(request,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/INST/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", homepageSections: [], },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/INST/app-manifest") {
				res.statusCode = 500;
				res.end("unexpected PUT",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${request}`,);
		}, async (url,) => {
			const failure = await dssFailure([
				"app",
				"save-instance-manifest",
				"--data",
				'{"homepageSections":[]}',
				"--project-key",
				"INST",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			const events = failure.stderr
				.split(/\r?\n/u,)
				.filter((line,) => line.length > 0)
				.map((line,) => JSON.parse(line,) as Record<string, unknown>);
			const report = events.find((event,) => event.type === "error");
			expect(report,).toMatchObject({
				code: "validation_failed",
				category: "usage",
				exitCode: 1,
				resource: "app",
				action: "save-instance-manifest",
				details: { projectAppType: "APP_INSTANCE", projectKey: "INST", },
			},);
		},);
		expect(requests,).toEqual(["GET /public/api/projects/INST/app-manifest",],);
		expect(requests.some((request,) => request.startsWith("PUT ",)),).toBe(false,);
	});

	it("flow-zone graph without an id fetches the full project graph", async () => {
		let requestPath = "";
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requestPath = `${req.method ?? ""} ${url.pathname}`;
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/graph/") {
				sendJson(res, { nodes: [{ id: "orders", type: "DATASET", },], edges: [], },);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const success = await dss(["flow-zone", "graph",], { env: cliEnv(url,), },);
			expect(JSON.parse(success.stdout,),).toEqual({
				nodes: [{ id: "orders", type: "DATASET", },],
				edges: [],
			},);
		},);
		expect(requestPath,).toBe("GET /public/api/projects/TEST/flow/graph/",);
	});

	it("future abort dry-run plans without peeking or aborting", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, { unexpected: true, },);
		}, async (url,) => {
			const plan = JSON.parse(
				(await dss(["future", "abort", "missing-future", "--dry-run",], { env: cliEnv(url,), },))
					.stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				resource: "future",
				action: "abort",
				id: "missing-future",
				method: "POST",
				endpoint: "/public/api/futures/missing-future/abort",
			},);
		},);
		expect(requests,).toEqual([],);
	});

	it("sql query accepts --sql - as stdin", async () => {
		let capturedBody: Record<string, unknown> | undefined;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/sql/queries/") {
				capturedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { queryId: "q-stdin-dash", hasResults: true, schema: [], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/sql/queries/q-stdin-dash/stream") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end("[]",);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/sql/queries/q-stdin-dash/finish-streaming"
			) {
				res.statusCode = 200;
				res.end("",);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const success = await dssWithInput(
				["sql", "query", "--sql", "-", "--connection", "CONN",],
				"SELECT 42\n",
				{ env: cliEnv(url,), },
			);
			expect(JSON.parse(success.stdout,),).toEqual({
				queryId: "q-stdin-dash",
				schema: [],
				columns: [],
				rows: [],
			},);
		},);
		expect(capturedBody,).toMatchObject({
			query: "SELECT 42\n",
			connection: "CONN",
			projectKey: "TEST",
		},);
	});

	it("api-service list-packages checks service existence before returning packages", async () => {
		let settingsRequested = false;
		let packagesRequested = false;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/apiservices/missing-service/settings"
			) {
				settingsRequested = true;
				sendJson(res, { message: "API service missing-service not found", }, 404,);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/apiservices/missing-service/packages"
			) {
				packagesRequested = true;
				sendJson(res, [],);
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["api-service", "list-packages", "missing-service", "--project-key", "TEST",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "not_found",
				category: "dss",
				exitCode: 2,
				resource: "api-service",
				action: "list-packages",
				projectKey: "TEST",
				status: 404,
			},);
		},);
		expect(settingsRequested,).toBe(true,);
		expect(packagesRequested,).toBe(false,);
	});

	it("commands run exposes alias, usage, and cleanup hint regressions", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",], { env: hermeticEnv, },);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<
			string,
			Record<string, {
				cleanupHint?: string;
				flags: Array<{ name: string; kind: string; aliases?: string[]; }>;
				usage: string;
			}>
		>;

		expect(registry.dataset?.preview?.usage,).toContain("[--rows N]",);
		expect(registry.dataset?.preview?.flags,).toContainEqual(
			expect.objectContaining({
				name: "max-rows",
				kind: "value",
				aliases: expect.arrayContaining(["rows",],),
			},),
		);
		const datasetGetProjectKey = registry.dataset?.get?.flags.find((flag,) =>
			flag.name === "project-key"
		);
		expect(datasetGetProjectKey,).toEqual(
			expect.objectContaining({ name: "project-key", kind: "value", },),
		);
		expect(datasetGetProjectKey,).not.toHaveProperty("aliases",);
		expect(registry.job?.wait?.usage,).toContain("[--project-key KEY]",);
		expect(registry["project-library"]?.["create-file"]?.cleanupHint ?? "",).not.toContain(
			"--if-exists",
		);
		expect(registry.dataset?.create?.cleanupHint ?? "",).toContain("--if-exists",);
	});

	it("meaning delete --if-exists skips Unknown meaning 400 without deleting", async () => {
		const requests: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? ""} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname.includes("/meanings/",)) {
				sendJson(res, { message: "Unknown meaning: nope", }, 400,);
				return;
			}
			if (req.method === "DELETE") {
				res.statusCode = 500;
				res.end(`unexpected DELETE ${url.pathname}`,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["meaning", "delete", "nope", "--if-exists",], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({ skipped: "nope", reason: "missing", },);
		},);
		expect(requests,).toEqual(["GET /public/api/meanings/nope",],);
		expect(requests.some((request,) => request.startsWith("DELETE ",)),).toBe(false,);
	});
	it("recipe clone maps missing output dataset creation to copy-output-settings usage guidance", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/recipes/source_recipe"
			) {
				expect(url.searchParams.get("includePayload",),).toBe("true",);
				sendJson(res, {
					recipe: {
						name: "source_recipe",
						type: "python",
						outputs: { main: { items: [{ ref: "old_output", },], }, },
					},
					payload: "",
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
				sendJson(res, {
					message: "Need to create output dataset or folder, but creationInfo params are suppressing it",
				}, 400,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure([
				"recipe",
				"clone",
				"source_recipe",
				"--name",
				"target_recipe",
				"--output",
				"newout",
				"--project-key",
				"TEST",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "missing_required_flag",
				category: "usage",
				exitCode: 1,
				resource: "recipe",
				action: "clone",
			},);
			expect(report.error,).toContain("--copy-output-settings",);
		},);
	});
	it("reports missing project key for project-scoped app commands as usage", async () => {
		const failure = await dssFailure([
			"app",
			"instance-manifest",
			"--url",
			"http://127.0.0.1:9",
			"--api-key",
			"dummy-key",
		], {
			env: {
				...cliEnv("http://127.0.0.1:9",),
				DATAIKU_URL: "http://127.0.0.1:9",
				DATAIKU_API_KEY: "dummy-key",
				DATAIKU_PROJECT_KEY: "",
				DATAIKU_DISABLE_ENV: "1",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "missing_required_flag",
			category: "usage",
			exitCode: 1,
		},);
		const hint = String(report.hint ?? "",);
		expect(hint,).toContain("--project-key",);
		expect(hint,).toContain("DATAIKU_PROJECT_KEY",);
	});
	it("rewrites generic DSS 404 dataset failures with command context without exposing raw body", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname.includes("/datasets/NOPE",)) {
				sendJson(res, { message: "Dataiku instance not found", }, 404,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure([
				"dataset",
				"get",
				"NOPE",
				"--project-key",
				"TEST",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stderr,) as {
				code?: string;
				error?: string;
				details?: { body?: string; };
			};
			expect(report.code,).toBe("not_found",);
			expect(report.error,).toContain("Not found: dataset get in project TEST",);
			expect(report.details?.body,).toBe("{}",);
		},);
	});
});
