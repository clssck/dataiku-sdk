import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	join,
	readBody,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI command behavioral smoke coverage", () => {
	it("smokes project, flow-zone, and dataset command gaps", async () => {
		const datasetOut = join(tmpdir(), `dss-cli-dataset-download-${Date.now()}.csv`,);
		let flowZoneUpdateBody: Record<string, unknown> | undefined;
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/graph/") {
					sendJson(res, { nodes: [], links: [], },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1/graph"
				) {
					sendJson(res, { zoneId: "zone-1", nodes: [], },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					sendJson(res, {
						id: "zone-1",
						name: (flowZoneUpdateBody?.name as string | undefined) ?? "Zone 1",
						color: (flowZoneUpdateBody?.color as string | undefined) ?? "#2ab1ac",
						items: [],
					},);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					flowZoneUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/flow/zones/zone-1") {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders") {
					sendJson(res, { name: "orders", type: "Filesystem", projectKey: "TEST", },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/schema"
				) {
					sendJson(res, { columns: [{ name: "order_id", type: "string", },], },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/data/"
				) {
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/plain",);
					res.end("order_id\nA1\n",);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/metadata"
				) {
					sendJson(res, { tags: ["agent-tested",], },);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(JSON.parse((await dss(["project", "flow",], { env: cliEnv(url,), },)).stdout,),).toEqual(
					{
						nodes: [],
						links: [],
					},
				);
				expect(
					JSON.parse((await dss(["flow-zone", "graph", "zone-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ zoneId: "zone-1", nodes: [], },);
				expect(
					JSON.parse(
						(await dss(["flow-zone", "update", "zone-1", "--name", "Zone 2",], {
							env: cliEnv(url,),
						},)).stdout,
					),
				)
					.toHaveProperty("name", "Zone 2",);
				expect(flowZoneUpdateBody,).toMatchObject({ id: "zone-1", name: "Zone 2", },);
				expect(
					JSON.parse((await dss(["flow-zone", "delete", "zone-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ deleted: "zone-1", resource: "flow-zone", },);
				expect(
					JSON.parse((await dss(["dataset", "get", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("name", "orders",);
				expect(
					JSON.parse((await dss(["dataset", "schema", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ columns: [{ name: "order_id", type: "string", },], },);
				expect(JSON.parse(
					(await dss(["dataset", "preview", "orders", "--max-rows", "1",], {
						env: cliEnv(url,),
					},)).stdout,
				),).toEqual({ columns: [{ name: "order_id", },], rows: [["A1",],], rowCount: 1, },);
				expect(
					JSON.parse((await dss(["dataset", "metadata", "orders",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ tags: ["agent-tested",], },);
				expect(JSON.parse(
					(await dss(["dataset", "download", "orders", "--output", datasetOut,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toMatchObject({ path: datasetOut, truncated: false, },);
				expect(readFileSync(datasetOut, "utf-8",),).toBe("order_id\nA1\n",);
			},);
		} finally {
			rmSync(datasetOut, { force: true, },);
		}
	}, 30_000,);

	it("smokes dataset, recipe, job, scenario, and connection command gaps", async () => {
		const recipeOut = join(tmpdir(), `dss-cli-recipe-download-${Date.now()}.json`,);
		let datasetCreateBody: Record<string, unknown> | undefined;
		let recipeCreateBody: Record<string, unknown> | undefined;
		let recipeUpdateBody: Record<string, unknown> | undefined;
		let scenarioCreateBody: Record<string, unknown> | undefined;
		let scenarioUpdateBody: Record<string, unknown> | undefined;
		let scenarioDefinition: Record<string, unknown> = {
			id: "scenario-1",
			name: "Scenario 1",
			params: { steps: [], },
		};
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
					datasetCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { name: "new_orders", type: "Filesystem", },);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
					recipeCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, },);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					sendJson(res, {
						recipe: { name: "compute_orders", type: "python", params: { old: true, }, },
					},);
					return;
				}
				if (
					req.method === "PUT" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					recipeUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, },);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-1/") {
					sendJson(res, {
						baseStatus: { def: { id: "job-1", type: "NON_RECURSIVE_FORCED_BUILD", }, state: "DONE", },
					},);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/job-1/abort/") {
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/connections/get-names/") {
					sendJson(res, ["filesystem_managed",],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					sendJson(res, scenarioDefinition,);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/") {
					scenarioCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "PUT" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					scenarioUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					scenarioDefinition = scenarioUpdateBody;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/run/"
				) {
					sendJson(res, { id: "run-1", },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/light/"
				) {
					sendJson(res, {
						id: "scenario-1",
						running: false,
						lastRun: { runId: "run-1", outcome: "SUCCESS", },
					},);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/scenarios/scenario-1/"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(JSON.parse(
					(await dss([
						"dataset",
						"create",
						"--name",
						"new_orders",
						"--connection",
						"filesystem_managed",
						"--type",
						"Filesystem",
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ created: "new_orders", resource: "dataset", },);
				expect(datasetCreateBody,).toMatchObject({
					name: "new_orders",
					type: "Filesystem",
					projectKey: "TEST",
				},);

				expect(JSON.parse(
					(await dss([
						"recipe",
						"create",
						"--type",
						"python",
						"--name",
						"compute_orders",
						"--input",
						"orders",
						"--output",
						"orders_out",
					], { env: cliEnv(url,), },)).stdout,
				),).toMatchObject({ recipeName: "compute_orders", type: "python", },);
				expect(recipeCreateBody?.recipePrototype,).toMatchObject({
					name: "compute_orders",
					type: "python",
					projectKey: "TEST",
				},);

				expect(JSON.parse(
					(await dss([
						"recipe",
						"update",
						"compute_orders",
						"--data",
						'{"recipe":{"params":{"new":true}}}',
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ updated: "compute_orders", resource: "recipe", },);
				expect((recipeUpdateBody?.recipe as Record<string, unknown>)?.params,).toEqual({
					old: true,
					new: true,
				},);

				expect(JSON.parse(
					(await dss(["recipe", "download", "compute_orders", "--output", recipeOut,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe(recipeOut,);
				expect(JSON.parse(readFileSync(recipeOut, "utf-8",),),).toHaveProperty(
					"recipe.name",
					"compute_orders",
				);

				expect(
					JSON.parse(
						(await dss(["recipe", "delete", "compute_orders",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "compute_orders", resource: "recipe", },);
				expect(JSON.parse((await dss(["job", "get", "job-1",], { env: cliEnv(url,), },)).stdout,),)
					.toHaveProperty("baseStatus.state", "DONE",);
				expect(JSON.parse((await dss(["job", "abort", "job-1",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual({ aborted: "job-1", resource: "job", },);
				expect(JSON.parse((await dss(["connection", "list",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual(["filesystem_managed",],);

				expect(
					JSON.parse(
						(await dss(["connection", "list", "--type", "Filesystem",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual(["filesystem_managed",],);
				expect(
					JSON.parse((await dss(["scenario", "get", "scenario-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("id", "scenario-1",);
				expect(
					JSON.parse(
						(await dss(["scenario", "create", "scenario-1", "Scenario 1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ created: "scenario-1", name: "Scenario 1", resource: "scenario", },);
				expect(scenarioCreateBody,).toMatchObject({
					id: "scenario-1",
					name: "Scenario 1",
					projectKey: "TEST",
				},);
				const scenarioUpdate = JSON.parse(
					(await dss([
						"scenario",
						"update",
						"scenario-1",
						"--data",
						'{"active":false}',
					], { env: cliEnv(url,), },)).stdout,
				) as Record<string, unknown>;
				expect(scenarioUpdate,).toMatchObject({
					updated: "scenario-1",
					resource: "scenario",
					verified: true,
					changed: true,
				},);
				expect(scenarioUpdate.changes,).toEqual([{
					path: "active",
					before: undefined,
					after: false,
				},],);
				expect(scenarioUpdateBody,).toMatchObject({ id: "scenario-1", active: false, },);
				expect(
					JSON.parse((await dss(["scenario", "run", "scenario-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ runId: "run-1", },);
				expect(
					JSON.parse(
						(await dss(["scenario", "status", "scenario-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("lastRun.outcome", "SUCCESS",);
				expect(JSON.parse(
					(await dss([
						"scenario",
						"run-and-wait",
						"scenario-1",
						"--timeout",
						"1000",
					], { env: cliEnv(url,), },)).stdout,
				),).toMatchObject({
					scenarioId: "scenario-1",
					runId: "run-1",
					success: true,
				},);
				expect(
					JSON.parse(
						(await dss(["scenario", "delete", "scenario-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "scenario-1", resource: "scenario", },);
			},);
		} finally {
			rmSync(recipeOut, { force: true, },);
		}
	}, 45_000,);

	it("smokes folder, code-env, and notebook command gaps", async () => {
		const uploadPath = join(tmpdir(), `dss-cli-folder-upload-${Date.now()}.txt`,);
		const downloadPath = join(tmpdir(), `dss-cli-folder-download-${Date.now()}.txt`,);
		const jupyterContent = {
			metadata: {},
			nbformat: 4,
			nbformat_minor: 5,
			cells: [{
				cell_type: "code",
				source: ["print(1)",],
				outputs: [{ text: "old", },],
				execution_count: 1,
			},],
		};
		const sqlContent = {
			connection: "filesystem_managed",
			cells: [{ id: "cell-1", type: "QUERY", code: "SELECT 1", },],
		};
		let codeEnvDefinitionBody: Record<string, unknown> | undefined;
		let savedJupyterBody: Record<string, unknown> | undefined;
		let savedSqlBody: Record<string, unknown> | undefined;
		let clearSqlHistoryBody: Record<string, unknown> | undefined;
		try {
			writeFileSync(uploadPath, "upload body\n",);
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
					sendJson(res, [{ id: "folder-1", name: "Folder 1", type: "Filesystem", },],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-1"
				) {
					sendJson(res, { id: "folder-1", name: "Folder 1", type: "Filesystem", },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 200;
					res.setHeader("Content-Type", "text/plain",);
					res.end("download body\n",);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/managedfolders/folder-1/contents/%2Fremote.txt"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1/") {
					sendJson(res, {
						envName: "default_v1",
						envLang: "PYTHON",
						desc: { pythonInterpreter: "PYTHON311", },
						specPackageList: "openpyxl==3.1.5\npolars",
						actualPackageList: "openpyxl==3.1.5\npolars==1.40.1",
					},);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1") {
					sendJson(res, { envName: "default_v1", envLang: "PYTHON", specPackageList: "polars", },);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1") {
					codeEnvDefinitionBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { updated: true, },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/admin/code-envs/usages") {
					sendJson(res, [{ envName: "default_v1", envLang: "PYTHON", usages: [], },],);
					return;
				}
				if (
					req.method === "GET" && url.pathname === "/public/api/admin/code-envs/PYTHON/default_v1/usages"
				) {
					sendJson(res, [{ projectKey: "TEST", },],);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					sendJson(res, jupyterContent,);
					return;
				}
				if (
					req.method === "PUT"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					savedJupyterBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1/sessions"
				) {
					sendJson(res, [{ sessionId: "session-1", notebookName: "notebook-1", },],);
					return;
				}
				if (
					req.method === "DELETE"
					&& url.pathname === "/public/api/projects/TEST/jupyter-notebooks/notebook-1/sessions/session-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1") {
					sendJson(res, sqlContent,);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1") {
					savedSqlBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				if (
					req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1"
				) {
					res.statusCode = 204;
					res.end();
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1/history"
				) {
					sendJson(res, { "cell-1": [{ startedOn: 1, },], },);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname === "/public/api/projects/TEST/sql-notebooks/sql-1/history/clear"
				) {
					clearSqlHistoryBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					sendJson(res, { ok: true, }, 204,);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				expect(
					JSON.parse((await dss(["folder", "get", "folder-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("id", "folder-1",);
				expect(JSON.parse(
					(await dss(["folder", "download", "folder-1", "/remote.txt", downloadPath,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toBe(downloadPath,);
				expect(readFileSync(downloadPath, "utf-8",),).toBe("download body\n",);
				expect(JSON.parse(
					(await dss(["folder", "upload", "folder-1", "/remote.txt", uploadPath,], {
						env: cliEnv(url,),
					},)).stdout,
				),).toEqual({
					uploaded: "/remote.txt",
					folder: "folder-1",
					localPath: uploadPath,
					resource: "folder",
				},);
				expect(
					JSON.parse(
						(await dss(["folder", "delete-file", "folder-1", "/remote.txt",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ deleted: "/remote.txt", folder: "folder-1", resource: "folder", },);

				expect(
					JSON.parse(
						(await dss(["code-env", "get", "PYTHON", "default_v1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toMatchObject({
						envName: "default_v1",
						envLang: "PYTHON",
						requestedPackages: ["openpyxl==3.1.5", "polars",],
					},);
				expect(
					JSON.parse(
						(await dss(["code-env", "get-definition", "PYTHON", "default_v1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toHaveProperty("specPackageList", "polars",);
				expect(JSON.parse(
					(await dss([
						"code-env",
						"set-definition",
						"PYTHON",
						"default_v1",
						"--data",
						'{"envName":"default_v1","envLang":"PYTHON","specPackageList":"polars"}',
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ updated: true, },);
				expect(codeEnvDefinitionBody,).toHaveProperty("specPackageList", "polars",);
				expect(JSON.parse((await dss(["code-env", "usages",], { env: cliEnv(url,), },)).stdout,),)
					.toEqual([{ envName: "default_v1", envLang: "PYTHON", usages: [], },],);
				expect(
					JSON.parse(
						(await dss(["code-env", "usages", "PYTHON", "default_v1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual([{ projectKey: "TEST", },],);

				expect(
					JSON.parse(
						(await dss(["notebook", "get-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("nbformat", 4,);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"save-jupyter",
						"notebook-1",
						"--data",
						JSON.stringify(jupyterContent,),
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ saved: "notebook-1", resource: "jupyter-notebook", },);
				expect(savedJupyterBody,).toHaveProperty("nbformat", 4,);
				expect(
					JSON.parse(
						(await dss(["notebook", "clear-jupyter-outputs", "notebook-1",], { env: cliEnv(url,), },))
							.stdout,
					),
				)
					.toEqual({ cleared: "notebook-1", resource: "jupyter-notebook", },);
				expect(savedJupyterBody,).toBeDefined();
				const savedCells = (savedJupyterBody as Record<string, unknown>).cells as Array<
					Record<string, unknown>
				>;
				expect(savedCells[0]?.outputs as unknown[],).toEqual([],);
				expect(
					JSON.parse(
						(await dss(["notebook", "sessions-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual([{ sessionId: "session-1", notebookName: "notebook-1", },],);
				expect(
					JSON.parse(
						(await dss(["notebook", "unload-jupyter", "notebook-1", "session-1",], {
							env: cliEnv(url,),
						},)).stdout,
					),
				)
					.toEqual({ unloaded: "notebook-1", sessionId: "session-1", resource: "jupyter-notebook", },);
				expect(
					JSON.parse(
						(await dss(["notebook", "delete-jupyter", "notebook-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toEqual({ deleted: "notebook-1", resource: "jupyter-notebook", },);

				expect(
					JSON.parse((await dss(["notebook", "get-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toHaveProperty("connection", "filesystem_managed",);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"save-sql",
						"sql-1",
						"--data",
						JSON.stringify(sqlContent,),
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ saved: "sql-1", resource: "sql-notebook", },);
				expect(savedSqlBody,).toHaveProperty("connection", "filesystem_managed",);
				expect(
					JSON.parse(
						(await dss(["notebook", "history-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,
					),
				)
					.toHaveProperty("cell-1", [{ startedOn: 1, },],);
				expect(JSON.parse(
					(await dss([
						"notebook",
						"clear-sql-history",
						"sql-1",
						"--cell-id",
						"cell-1",
						"--retain",
						"2",
					], { env: cliEnv(url,), },)).stdout,
				),).toEqual({ cleared: "sql-1", resource: "sql-notebook", },);
				expect(clearSqlHistoryBody,).toEqual({ cellId: "cell-1", numRunsToRetain: 2, },);
				expect(
					JSON.parse((await dss(["notebook", "delete-sql", "sql-1",], { env: cliEnv(url,), },)).stdout,),
				)
					.toEqual({ deleted: "sql-1", resource: "sql-notebook", },);
			},);
		} finally {
			rmSync(uploadPath, { force: true, },);
			rmSync(downloadPath, { force: true, },);
		}
	}, 45_000,);

	it("smokes wiki, dashboard, and insight commands", async () => {
		let wikiUpdateBody: Record<string, unknown> | undefined;
		let dashboardCreateBody: Record<string, unknown> | undefined;
		let dashboardUpdateBody: Record<string, unknown> | undefined;
		let insightName = "Insight 1";
		let insightCreateBody: Record<string, unknown> | undefined;
		let insightUpdateBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/") {
				sendJson(res, { projectKey: "TEST", taxonomy: [{ id: "article-1", children: [], },], },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				sendJson(res, {
					article: { id: "article-1", name: "Article 1", projectKey: "TEST", },
					payload: "old",
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/wiki/") {
				const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { article: { id: "article-2", name: body.name, projectKey: "TEST", }, },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/wiki/article-2") {
				sendJson(res, {
					article: { id: "article-2", name: "Created", projectKey: "TEST", },
					payload: "created",
				},);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				wikiUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, wikiUpdateBody,);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/wiki/article-2") {
				const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, body,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/wiki/article-1") {
				res.statusCode = 204;
				res.end();
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/") {
				sendJson(res, [{ id: "dash-1", name: "Dashboard 1", projectKey: "TEST", listed: true, },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				sendJson(res, { id: "dash-1", name: "Dashboard 1", projectKey: "TEST", pages: [], },);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/dashboards/") {
				dashboardCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "dash-2", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/dashboards/dash-2/") {
				sendJson(res, { id: "dash-2", name: "Created dashboard", projectKey: "TEST", pages: [], },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				dashboardUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, dashboardUpdateBody,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/dashboards/dash-1/") {
				res.statusCode = 204;
				res.end();
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/") {
				sendJson(res, [{ id: "insight-1", name: insightName, type: "chart", projectKey: "TEST", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/insight-1/") {
				sendJson(res, {
					id: "insight-1",
					name: insightName,
					type: "chart",
					projectKey: "TEST",
					params: {},
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/insights/") {
				insightCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "insight-2", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/insights/insight-2/") {
				sendJson(res, {
					id: "insight-2",
					name: "Created insight",
					type: "chart",
					projectKey: "TEST",
					params: {},
				},);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/insights/insight-1/") {
				insightUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				const insight = insightUpdateBody.insight as { name?: string; };
				insightName = insight.name ?? insightName;
				sendJson(res, { id: "insight-1", },);
				return;
			}
			if (
				req.method === "DELETE" && url.pathname === "/public/api/projects/TEST/insights/insight-1/"
			) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			expect(JSON.parse((await dss(["wiki", "settings",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveProperty("projectKey", "TEST",);
			expect(JSON.parse((await dss(["wiki", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(JSON.parse((await dss(["wiki", "get", "article-1",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveProperty("payload", "old",);

			const wikiCreateDryRun = JSON.parse(
				(
					await dss(["wiki", "create", "--name", "Dry run", "--content", "preview", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(wikiCreateDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "wiki",
				name: "Dry run",
				payload: { name: "Dry run", content: "preview", },
			},);
			expect(JSON.parse(
				(await dss(["wiki", "create", "--name", "Created", "--content", "created",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("article.id", "article-2",);

			const wikiUpdateDryRun = JSON.parse(
				(
					await dss([
						"wiki",
						"update",
						"article-1",
						"--name",
						"Dry Updated",
						"--content",
						"dry",
						"--dry-run",
					], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as {
				current?: { payload?: string; };
				next?: { article?: { name?: string; }; payload?: string; };
			};
			expect(wikiUpdateDryRun.current?.payload,).toBe("old",);
			expect(wikiUpdateDryRun.next?.article?.name,).toBe("Dry Updated",);
			expect(wikiUpdateDryRun.next?.payload,).toBe("dry",);
			expect(
				JSON.parse(
					(await dss(["wiki", "update", "article-1", "--name", "Updated", "--content", "new",], {
						env: cliEnv(url,),
					},)).stdout,
				),
			).toHaveProperty("payload", "new",);
			expect(wikiUpdateBody?.article,).toMatchObject({ id: "article-1", name: "Updated", },);

			const wikiDeleteDryRun = JSON.parse(
				(
					await dss(["wiki", "delete", "article-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(wikiDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "wiki",
				article: "article-1",
			},);
			expect(
				JSON.parse((await dss(["wiki", "delete", "article-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "article-1", resource: "wiki", },);

			expect(JSON.parse((await dss(["dashboard", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(
				JSON.parse((await dss(["dashboard", "get", "dash-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("name", "Dashboard 1",);

			const dashboardCreateDryRun = JSON.parse(
				(
					await dss(["dashboard", "create", "--name", "Dry dashboard", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(dashboardCreateDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "dashboard",
				name: "Dry dashboard",
				payload: { pages: [], },
			},);
			expect(JSON.parse(
				(await dss(["dashboard", "create", "--name", "Created dashboard",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("id", "dash-2",);
			expect(dashboardCreateBody,).toEqual({ pages: [], name: "Created dashboard", },);

			const dashboardUpdateDryRun = JSON.parse(
				(
					await dss(["dashboard", "update", "dash-1", "--name", "Dry dashboard update", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { current?: { name?: string; }; next?: { name?: string; }; };
			expect(dashboardUpdateDryRun.current?.name,).toBe("Dashboard 1",);
			expect(dashboardUpdateDryRun.next?.name,).toBe("Dry dashboard update",);
			expect(JSON.parse(
				(await dss(["dashboard", "update", "dash-1", "--name", "Updated dashboard",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("name", "Updated dashboard",);
			expect(dashboardUpdateBody,).toMatchObject({ id: "dash-1", name: "Updated dashboard", },);

			const dashboardDeleteDryRun = JSON.parse(
				(
					await dss(["dashboard", "delete", "dash-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(dashboardDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "dashboard",
				id: "dash-1",
			},);
			expect(
				JSON.parse((await dss(["dashboard", "delete", "dash-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "dash-1", resource: "dashboard", },);

			expect(JSON.parse((await dss(["insight", "list",], { env: cliEnv(url,), },)).stdout,),)
				.toHaveLength(1,);
			expect(
				JSON.parse((await dss(["insight", "get", "insight-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("type", "chart",);
			const insightCreateDryRun = JSON.parse(
				(
					await dss(["insight", "create", "--name", "Dry insight", "--type", "chart", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { payload?: { name?: string; type?: string; }; };
			expect(insightCreateDryRun.payload,).toMatchObject({
				name: "Dry insight",
				type: "chart",
			},);
			expect(JSON.parse(
				(await dss([
					"insight",
					"create",
					"--name",
					"Created insight",
					"--type",
					"chart",
					"--params",
					"{}",
				], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("id", "insight-2",);
			expect(insightCreateBody?.insightPrototype,).toMatchObject({
				name: "Created insight",
				type: "chart",
			},);

			const insightUpdateDryRun = JSON.parse(
				(
					await dss(["insight", "update", "insight-1", "--name", "Dry insight update", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as { current?: { name?: string; }; next?: { name?: string; }; };
			expect(insightUpdateDryRun.current?.name,).toBe("Insight 1",);
			expect(insightUpdateDryRun.next?.name,).toBe("Dry insight update",);
			expect(JSON.parse(
				(await dss(["insight", "update", "insight-1", "--name", "Updated insight",], {
					env: cliEnv(url,),
				},)).stdout,
			),).toHaveProperty("name", "Updated insight",);
			expect(insightUpdateBody?.insight,).toMatchObject({
				id: "insight-1",
				name: "Updated insight",
			},);

			const insightDeleteDryRun = JSON.parse(
				(
					await dss(["insight", "delete", "insight-1", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(insightDeleteDryRun,).toMatchObject({
				dryRun: true,
				action: "delete",
				resource: "insight",
				id: "insight-1",
			},);
			expect(
				JSON.parse((await dss(["insight", "delete", "insight-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ deleted: "insight-1", resource: "insight", },);
		},);
	}, 45_000,);

	it("smokes data quality commands", async () => {
		let ruleName = "Has rows";
		let createRuleBody: Record<string, unknown> | undefined;
		let updateRuleBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules"
			) {
				sendJson(res, {
					monitor: {},
					displayedState: {},
					checks: [{ id: "rule-1", displayName: ruleName, type: "RecordCountInRangeRule", },],
				},);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/status"
			) {
				sendJson(res, { outcome: "SUCCESS", enabled: true, },);
				return;
			}
			if (
				req.method === "POST"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules"
			) {
				createRuleBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ...createRuleBody, id: "rule-2", },);
				return;
			}
			if (
				req.method === "PUT"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules/rule-1"
			) {
				updateRuleBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				ruleName = String(updateRuleBody.displayName ?? ruleName,);
				res.statusCode = 204;
				res.end();
				return;
			}
			if (
				req.method === "DELETE"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules/rule-1"
			) {
				expect(url.searchParams.get("ruleId",),).toBe("rule-1",);
				res.statusCode = 204;
				res.end();
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/status-by-partition"
			) {
				sendJson(res, { NP: { status: "OK", }, },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/last-rules-result"
			) {
				sendJson(res, [{ id: "rule-1", outcome: "OK", },],);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/datasets/orders/data-quality/rules-history"
			) {
				sendJson(res, [{ id: "rule-1", outcome: "OK", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/data-quality/status") {
				expect(url.searchParams.get("onlyMonitored",),).toBe("false",);
				sendJson(res, { orders: { outcome: "SUCCESS", }, },);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/TEST/data-quality/timeline"
			) {
				expect(url.searchParams.get("minTimestamp",),).toBe("1714521600000",);
				sendJson(res, [{ day: "2026-05-01", currentOutcome: "SUCCESS", },],);
				return;
			}
			if (
				req.method === "POST"
				&& url.pathname
					=== "/public/api/projects/TEST/datasets/orders/data-quality/actions/compute-rules"
			) {
				sendJson(res, { jobId: "job-1", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				sendJson(res, {
					jobId: "job-1",
					hasResult: true,
					alive: false,
					result: { outcome: "SUCCESS", },
				},);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/futures/job-1") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			expect(
				JSON.parse((await dss(["data-quality", "rules", "orders",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(await dss(["data-quality", "get-rule", "orders", "rule-1",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("displayName", "Has rows",);

			expect(
				JSON.parse((await dss(["data-quality", "status", "orders",], { env: cliEnv(url,), },)).stdout,),
			)
				.toHaveProperty("outcome", "SUCCESS",);

			const createDryRun = JSON.parse(
				(
					await dss([
						"data-quality",
						"create-rule",
						"orders",
						"--data",
						'{"type":"RecordCountInRangeRule","displayName":"New rule"}',
						"--dry-run",
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(createDryRun,).toMatchObject({
				dryRun: true,
				action: "create-rule",
				dataset: "orders",
			},);
			expect(JSON.parse(
				(
					await dss([
						"data-quality",
						"create-rule",
						"orders",
						"--data",
						'{"type":"RecordCountInRangeRule","displayName":"New rule"}',
					], { env: cliEnv(url,), },)
				).stdout,
			),).toHaveProperty("id", "rule-2",);
			expect(createRuleBody,).toMatchObject({
				type: "RecordCountInRangeRule",
				displayName: "New rule",
			},);

			const updateDryRun = JSON.parse(
				(
					await dss([
						"data-quality",
						"update-rule",
						"orders",
						"rule-1",
						"--data",
						'{"displayName":"Dry rule"}',
						"--dry-run",
					], { env: cliEnv(url,), },)
				).stdout,
			) as { current?: { displayName?: string; }; next?: { displayName?: string; }; };
			expect(updateDryRun.current?.displayName,).toBe("Has rows",);
			expect(updateDryRun.next?.displayName,).toBe("Dry rule",);
			expect(JSON.parse(
				(
					await dss([
						"data-quality",
						"update-rule",
						"orders",
						"rule-1",
						"--data",
						'{"displayName":"Updated rule"}',
					], { env: cliEnv(url,), },)
				).stdout,
			),).toHaveProperty("displayName", "Updated rule",);
			expect(updateRuleBody,).toMatchObject({ id: "rule-1", displayName: "Updated rule", },);

			const deleteDryRun = JSON.parse(
				(
					await dss(["data-quality", "delete-rule", "orders", "rule-1", "--dry-run",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(deleteDryRun,).toMatchObject({ dryRun: true, action: "delete-rule", ruleId: "rule-1", },);
			expect(JSON.parse(
				(
					await dss(["data-quality", "delete-rule", "orders", "rule-1",], { env: cliEnv(url,), },)
				).stdout,
			),).toEqual({ deleted: "rule-1", dataset: "orders", resource: "data-quality", },);

			expect(JSON.parse(
				(
					await dss(["data-quality", "status-by-partition", "orders", "--include-all-partitions",], {
						env: cliEnv(url,),
					},)
				).stdout,
			),).toHaveProperty("NP.status", "OK",);
			expect(
				JSON.parse(
					(await dss(["data-quality", "last-results", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(await dss(["data-quality", "history", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveLength(1,);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"project-status",
							"--only-monitored",
							"false",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toHaveProperty("orders.outcome", "SUCCESS",);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"project-timeline",
							"--min-timestamp",
							"1714521600000",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toHaveLength(1,);
			expect(JSON.parse(
				(
					await dss(["data-quality", "compute", "orders", "--dry-run",], { env: cliEnv(url,), },)
				).stdout,
			),).toMatchObject({ dryRun: true, action: "compute", dataset: "orders", },);
			expect(
				JSON.parse(
					(await dss(["data-quality", "compute", "orders",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("jobId", "job-1",);
			expect(
				JSON.parse(
					(
						await dss([
							"data-quality",
							"compute",
							"orders",
							"--wait",
							"--poll-interval",
							"1",
						], { env: cliEnv(url,), },)
					).stdout,
				),
			)
				.toMatchObject({ futureId: "job-1", success: true, result: { outcome: "SUCCESS", }, },);
			expect(
				JSON.parse(
					(await dss(["future", "peek", "job-1",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toHaveProperty("result.outcome", "SUCCESS",);
			expect(
				JSON.parse(
					(await dss(["future", "wait", "job-1", "--poll-interval", "1",], { env: cliEnv(url,), },))
						.stdout,
				),
			)
				.toHaveProperty("success", true,);
			expect(
				JSON.parse(
					(await dss(["future", "abort", "job-1", "--dry-run",], { env: cliEnv(url,), },)).stdout,
				),
			)
				.toMatchObject({ dryRun: true, action: "abort", resource: "future", id: "job-1", },);
			expect(
				JSON.parse((await dss(["future", "abort", "job-1",], { env: cliEnv(url,), },)).stdout,),
			)
				.toEqual({ aborted: "job-1", resource: "future", },);
		},);
	}, 30_000,);
});
