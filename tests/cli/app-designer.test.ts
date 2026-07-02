import { describe, expect, it, } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, } from "node:fs";
import { join, } from "node:path";
import { tmpdir, } from "node:os";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI App Designer helpers", () => {
	it("runs nested app manifest commands and preserves legacy manifest lookup", async () => {
		const tempDir = mkdtempSync(join(tmpdir(), "dataiku-app-manifest-",),);
		const patchPath = join(tempDir, "manifest.patch.json",);
		writeFileSync(patchPath, JSON.stringify({ settings: { enabled: true, }, },),);
		const requests: string[] = [];
		const putBodies: unknown[] = [];

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				requests.push(`${req.method} ${url.pathname}`,);

				if (req.method === "GET" && url.pathname === "/public/api/apps/APP_ID/") {
					sendJson(res, { appId: "APP_ID", label: "Template", },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/app-manifest") {
					sendJson(res, {
						projectAppType: "APP_TEMPLATE",
						homepageSections: [],
						projectExportManifest: {},
					},);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/app-manifest") {
					putBodies.push(JSON.parse(await readBody(req,),),);
					res.statusCode = 204;
					res.end();
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
					sendJson(res, [{ id: "folder-id", name: "output", },],);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/folder-id") {
					sendJson(res, { id: "folder-id", name: "output", },);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const env = cliEnv(url,);
				expect(JSON.parse((await dss(["app", "manifest", "APP_ID",], { env, },)).stdout,),)
					.toMatchObject({ appId: "APP_ID", });
				expect(JSON.parse((await dss(["app", "manifest", "get", "--project-key", "TEST",], { env, },)).stdout,),)
					.toMatchObject({ projectAppType: "APP_TEMPLATE", });
				expect(JSON.parse((await dss([
					"app",
					"manifest",
					"update",
					"--data-file",
					patchPath,
					"--project-key",
					"TEST",
				], { env, },)).stdout,),).toMatchObject({ changed: true, });
				expect(JSON.parse((await dss([
					"app",
					"manifest",
					"export-resource",
					"--managed-folder",
					"output",
					"--project-key",
					"TEST",
				], { env, },)).stdout,),).toMatchObject({
					changed: true,
					folderId: "folder-id",
				});
			},);
		} finally {
			rmSync(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toEqual([
			"GET /public/api/apps/APP_ID/",
			"GET /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/managedfolders/",
			"GET /public/api/projects/TEST/managedfolders/folder-id",
			"PUT /public/api/projects/TEST/app-manifest",
		],);
		expect(putBodies[0],).toMatchObject({ settings: { enabled: true, }, });
		expect(putBodies[1],).toMatchObject({
			projectExportManifest: {
				exportManagedFolders: true,
				includedManagedFolders: [{ id: "folder-id", },],
			},
		},);
	});

	it("adds source-backed scenario tiles and rejects unsupported homepage tile schemas clearly", async () => {
		const requests: string[] = [];
		let scenarioTileBody: unknown;

		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/app-manifest") {
				sendJson(res, {
					projectAppType: "APP_TEMPLATE",
					homepageSections: [{ tiles: [], },],
				},);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/app-manifest") {
				scenarioTileBody = JSON.parse(await readBody(req,),);
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const env = cliEnv(url,);
			expect(JSON.parse((await dss([
				"app",
				"homepage",
				"add-scenario-tile",
				"--scenario",
				"GENERATE_GOSILICO_INPUT",
				"--button-text",
				"Generate",
				"--project-key",
				"TEST",
			], { env, },)).stdout,),).toMatchObject({ changed: true, });

			const variableFailure = await dssFailure([
				"app",
				"homepage",
				"add-project-variable-tile",
				"--variable",
				"gosilico_workbook_id",
				"--label",
				"Workbook ID",
				"--button-text",
				"Workbook ID",
				"--project-key",
				"TEST",
			], { env, },);
			expect(variableFailure.code,).toBe(1,);
			expect(JSON.parse(variableFailure.stderr,),).toMatchObject({
				code: "homepage_tile_schema_unavailable",
			});

			const folderFailure = await dssFailure([
				"app",
				"homepage",
				"add-managed-folder-tile",
				"--folder",
				"output",
				"--prompt",
				"Download generated workbook",
				"--project-key",
				"TEST",
			], { env, },);
			expect(folderFailure.code,).toBe(1,);
			expect(JSON.parse(folderFailure.stderr,),).toMatchObject({
				code: "homepage_tile_schema_unavailable",
				details: {
					closestSupportedAlternative: "dss app manifest export-resource --managed-folder <folder>",
				},
			});
		},);

		expect(scenarioTileBody,).toMatchObject({
			homepageSections: [{
				tiles: [{ type: "SCENARIO_RUN", scenarioId: "GENERATE_GOSILICO_INPUT", prompt: "Generate", },],
			},],
		},);
		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/app-manifest",
			"PUT /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
			"GET /public/api/projects/TEST/app-manifest",
		],);
	});

	it("gets and sets scenario scripts through the payload endpoint", async () => {
		const tempDir = mkdtempSync(join(tmpdir(), "dataiku-scenario-script-",),);
		const inputPath = join(tempDir, "scenario.py",);
		const outputPath = join(tempDir, "downloaded.py",);
		writeFileSync(inputPath, "print('set')\r\n",);
		const requests: string[] = [];
		const putBodies: unknown[] = [];

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				requests.push(`${req.method} ${url.pathname}`,);
				if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/scenarios/GENERATE/payload") {
					sendJson(res, { script: "print('get')\n", },);
					return;
				}
				if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/scenarios/GENERATE/payload") {
					putBodies.push(JSON.parse(await readBody(req,),),);
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const env = cliEnv(url,);
				expect(JSON.parse((await dss([
					"scenario",
					"get-script",
					"GENERATE",
					"--project-key",
					"TEST",
				], { env, },)).stdout,),).toBe("print('get')\n",);
				expect((await dss([
					"scenario",
					"get-script",
					"GENERATE",
					"--raw",
					"--project-key",
					"TEST",
				], { env, },)).stdout,).toBe("print('get')\n",);
				expect(JSON.parse((await dss([
					"scenario",
					"get-script",
					"GENERATE",
					"--output",
					outputPath,
					"--project-key",
					"TEST",
				], { env, },)).stdout,),).toBe(outputPath,);
				expect(readFileSync(outputPath, "utf-8",),).toBe("print('get')\n",);
				expect(JSON.parse((await dss([
					"scenario",
					"set-script",
					"GENERATE",
					"--file",
					inputPath,
					"--project-key",
					"TEST",
				], { env, },)).stdout,),).toMatchObject({ updated: "GENERATE", });
			},);
		} finally {
			rmSync(tempDir, { recursive: true, force: true, },);
		}

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/scenarios/GENERATE/payload",
			"GET /public/api/projects/TEST/scenarios/GENERATE/payload",
			"GET /public/api/projects/TEST/scenarios/GENERATE/payload",
			"PUT /public/api/projects/TEST/scenarios/GENERATE/payload",
		],);
		expect(putBodies,).toEqual([{ script: "print('set')\r\n", },],);
	});

	it("plans registry-safe app manifest aliases and homepage helpers accurately", async () => {
		const env = {
			...cliEnv("http://127.0.0.1:9",),
			DATAIKU_DISABLE_ENV: "1",
		};
		const manifestUpdatePlan = JSON.parse((await dss([
			"app",
			"manifest-update",
			"--data",
			'{"homepageSections":[]}',
			"--project-key",
			"TEST",
			"--plan",
		], { env, },)).stdout,) as Record<string, unknown>;
		expect(manifestUpdatePlan,).toMatchObject({
			plan: true,
			resource: "app",
			action: "manifest-update",
			method: "PUT",
			endpoint: "/public/api/projects/TEST/app-manifest",
			payload: { homepageSections: [], },
		});

		const exportPlan = JSON.parse((await dss([
			"app",
			"manifest-export-resource",
			"--managed-folder",
			"output",
			"--project-key",
			"TEST",
			"--plan",
		], { env, },)).stdout,) as Record<string, unknown>;
		expect(exportPlan,).toMatchObject({
			method: "PUT",
			endpoint: "/public/api/projects/TEST/app-manifest",
			payload: { operation: "export-managed-folder-resource", managedFolder: "output", },
		});

		const scenarioPlan = JSON.parse((await dss([
			"app",
			"homepage",
			"add-scenario-tile",
			"--scenario",
			"GENERATE",
			"--button-text",
			"Generate",
			"--project-key",
			"TEST",
			"--plan",
		], { env, },)).stdout,) as Record<string, unknown>;
		expect(scenarioPlan,).toMatchObject({
			method: "PUT",
			endpoint: "/public/api/projects/TEST/app-manifest",
			payload: { tile: { type: "SCENARIO_RUN", scenarioId: "GENERATE", prompt: "Generate", }, },
		});

		const unsupportedPlan = JSON.parse((await dss([
			"app",
			"homepage",
			"add-managed-folder-tile",
			"--folder",
			"output",
			"--prompt",
			"Download",
			"--project-key",
			"TEST",
			"--plan",
		], { env, },)).stdout,) as Record<string, unknown>;
		expect(unsupportedPlan.method,).toBeUndefined();
		expect(unsupportedPlan.endpoint,).toBeUndefined();
		expect(unsupportedPlan,).toMatchObject({
			payload: {
				validationOnly: true,
				error: "homepage_tile_schema_unavailable",
				closestSupportedAlternative: "dss app manifest export-resource --managed-folder <folder>",
			},
		});
	});
});
