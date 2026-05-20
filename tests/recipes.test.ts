import { describe, expect, it, } from "bun:test";
import { mkdtemp, readFile, rm, } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, resolve, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) {
		body += chunk.toString();
	}
	return body;
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
	},);
}

async function withRecipeServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);

	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

describe("RecipesResource", () => {
	it("guards empty successful responses and keeps payload query parameters", async () => {
		let requestedPath = "";

		await withRecipeServer((req, res,) => {
			requestedPath = req.url ?? "";
			res.statusCode = 200;
			res.end("",);
		}, async (url,) => {
			const client = createClient(url,);
			let error: unknown;
			try {
				await client.recipes.get("missing recipe", {
					includePayload: true,
					payloadMaxLines: 25,
				},);
			} catch (caught) {
				error = caught;
			}

			expect(error,).toBeInstanceOf(DataikuError,);
			const dataikuError = error as DataikuError;
			expect(dataikuError.status,).toBe(404,);
			expect(dataikuError.category,).toBe("not_found",);
			expect(dataikuError.message,).toContain('Recipe "missing recipe" not found in project "TEST"',);
		},);

		expect(requestedPath,).toBe(
			"/public/api/projects/TEST/recipes/missing%20recipe?includePayload=true&payloadMaxLines=25",
		);
	});

	it("omits payload unless includePayload is requested", async () => {
		await withRecipeServer((req, res,) => {
			expect(req.url,).toBe("/public/api/projects/TEST/recipes/with-payload",);
			sendJson(res, {
				recipe: { name: "with-payload", type: "python", },
				payload: "print('large payload')\n",
			},);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.get("with-payload",);
			expect(result,).toEqual({
				recipe: { name: "with-payload", type: "python", },
			},);
		},);
	});

	it("deep-merges nested recipe fields during update", async () => {
		const currentRecipe = {
			recipe: {
				name: "nested-recipe",
				type: "python",
				params: {
					nested: {
						keep: true,
						replace: "old",
					},
					preserved: {
						value: 1,
					},
				},
				scriptSettings: {
					engine: "python",
				},
			},
			metadata: {
				version: 1,
			},
		};
		let updatedBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			if (req.method === "GET") {
				sendJson(res, currentRecipe,);
				return;
			}
			if (req.method === "PUT") {
				updatedBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}
			res.statusCode = 405;
			res.end("Unexpected method",);
		}, async (url,) => {
			const client = createClient(url,);
			await client.recipes.update("nested-recipe", {
				metadata: {
					version: 2,
				},
				recipe: {
					params: {
						nested: {
							replace: "new",
							added: "value",
						},
					},
				},
			},);
		},);

		expect(updatedBody,).toBeDefined();
		expect(updatedBody,).toMatchObject({
			metadata: {
				version: 2,
			},
			recipe: {
				name: "nested-recipe",
				type: "python",
				params: {
					nested: {
						keep: true,
						replace: "new",
						added: "value",
					},
					preserved: {
						value: 1,
					},
				},
				scriptSettings: {
					engine: "python",
				},
			},
		},);
	});

	it("rejects recipe definition fields at the root of update payloads", async () => {
		await withRecipeServer((_req, res,) => {
			sendJson(res, {
				recipe: {
					name: "nested-recipe",
					type: "python",
					outputs: {},
				},
			},);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.recipes.update("nested-recipe", {
				outputs: {
					main: { items: [{ ref: "folder-id", appendMode: false, },], },
				},
			},),).rejects.toThrow('must be nested under "recipe"',);
		},);
	});

	it("clones recipe graph, settings, and payload with output rewrites", async () => {
		const requests: string[] = [];
		let postBody: Record<string, unknown> | undefined;
		let putBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);
			if (req.method === "GET") {
				sendJson(res, {
					recipe: {
						name: "source_recipe",
						type: "python",
						inputs: { main: { items: [{ ref: "input_ds", },], }, },
						outputs: { main: { items: [{ ref: "old_output", appendMode: false, },], }, },
						params: { envSelection: { envMode: "EXPLICIT_ENV", envName: "py39", }, },
						versionTag: { versionNumber: 12, },
						neverBuilt: false,
					},
					payload:
						"# old_output should remain in comments\nvalue = 'old_output'\ndataiku.Dataset('old_output').write_with_schema(df)\ndataiku.Dataset('old_output_extra')\n",
				},);
				return;
			}
			if (req.method === "POST") {
				postBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { recipeName: "target_recipe", },);
				return;
			}
			if (req.method === "PUT") {
				putBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.clone("source_recipe", {
				name: "target_recipe",
				outputDataset: "new_output",
			},);
			expect(result.outputRewrites,).toEqual({ old_output: "new_output", },);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/recipes/source_recipe?includePayload=true",
			"POST /public/api/projects/TEST/recipes/",
			"PUT /public/api/projects/TEST/recipes/target_recipe",
		],);
		expect(postBody?.recipePrototype,).toMatchObject({
			name: "target_recipe",
			projectKey: "TEST",
			outputs: { main: { items: [{ ref: "new_output", appendMode: false, },], }, },
			params: { envSelection: { envMode: "EXPLICIT_ENV", envName: "py39", }, },
		},);
		expect((postBody?.recipePrototype as Record<string, unknown>).versionTag,).toBeUndefined();
		expect((postBody?.recipePrototype as Record<string, unknown>).neverBuilt,).toBeUndefined();
		expect((postBody?.creationSettings as { script?: string; }).script,).toContain("new_output",);
		expect(putBody?.payload as string,).toContain("new_output",);
		expect(putBody?.payload as string,).toContain("# old_output should remain in comments",);
		expect(putBody?.payload as string,).toContain("value = 'old_output'",);
		expect(putBody?.payload as string,).toContain("dataiku.Dataset('old_output_extra')",);
	});

	it("applies explicit payload text rewrites globally", async () => {
		let putBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			if (req.method === "GET") {
				sendJson(res, {
					recipe: {
						name: "source_recipe",
						type: "python",
						outputs: { main: { items: [{ ref: "old_output", },], }, },
					},
					payload: "# old_column is a literal note\ndataiku.Dataset('old_output')\n",
				},);
				return;
			}
			if (req.method === "POST") {
				sendJson(res, { recipeName: "target_recipe", },);
				return;
			}
			if (req.method === "PUT") {
				putBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await client.recipes.clone("source_recipe", {
				name: "target_recipe",
				outputDataset: "new_output",
				payloadTextRewrites: { old_column: "new_column", },
			},);
		},);

		expect(putBody?.payload as string,).toContain("# new_column is a literal note",);
		expect(putBody?.payload as string,).toContain("dataiku.Dataset('new_output')",);
	});

	it("rewrites SQL FROM and JOIN table references without global text replacement", async () => {
		let putBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			if (req.method === "GET") {
				sendJson(res, {
					recipe: {
						name: "source_recipe",
						type: "sql_query",
						inputs: {
							main: { items: [{ ref: "old_input", },], },
							lookup: { items: [{ ref: "old_lookup", },], },
						},
						outputs: { main: { items: [{ ref: "old_output", },], }, },
					},
					payload:
						'SELECT *\nFROM old_input oi\nJOIN "old_lookup" l ON oi.id = l.id\nJOIN old_input_extra untouched ON true\n',
				},);
				return;
			}
			if (req.method === "POST") {
				sendJson(res, { recipeName: "target_recipe", },);
				return;
			}
			if (req.method === "PUT") {
				putBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			await client.recipes.clone("source_recipe", {
				name: "target_recipe",
				inputRewrites: {
					old_input: "new_input",
					old_lookup: "new_lookup",
				},
			},);
		},);

		const payload = putBody?.payload as string;
		expect(payload,).toContain("FROM new_input oi",);
		expect(payload,).toContain('JOIN "new_lookup" l',);
		expect(payload,).toContain("JOIN old_input_extra untouched",);
		expect(payload,).not.toContain("FROM old_input oi",);
	});

	it("rejects one storage override for multiple copied output datasets", async () => {
		await withRecipeServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/source_recipe",);
			sendJson(res, {
				recipe: {
					name: "source_recipe",
					type: "python",
					outputs: {
						main: {
							items: [
								{ ref: "old_output_a", },
								{ ref: "old_output_b", },
							],
						},
					},
				},
				payload: "",
			},);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.recipes.clone("source_recipe", {
				name: "target_recipe",
				outputRewrites: {
					old_output_a: "new_output_a",
					old_output_b: "new_output_b",
				},
				copyOutputSettings: true,
				outputPath: "/dataiku/TEST/reused",
			},),).rejects.toThrow("Cannot reuse --path or --metastore-table",);
		},);
	});

	it("downloads recipe code with an inferred file extension", async () => {
		const payloadByRecipeName: Record<string, { type: string; payload: string; ext: string; }> = {
			"python-recipe": { type: "python", payload: "print('python')\n", ext: ".py", },
			"sql-recipe": { type: "sql_query", payload: "select 1;\n", ext: ".sql", },
			"shell-recipe": { type: "shell", payload: "echo shell\n", ext: ".sh", },
			"r-recipe": { type: "r", payload: "print('r')\n", ext: ".R", },
			"scala-recipe": { type: "spark_scala", payload: "println(1)\n", ext: ".scala", },
			"unknown-recipe": { type: "visual_prepare", payload: "payload\n", ext: ".txt", },
		};
		const requestedPaths: string[] = [];
		const tempDir = await mkdtemp(join(tmpdir(), "recipes-download-",),);
		const originalCwd = process.cwd();

		try {
			process.chdir(tempDir,);
			await withRecipeServer((req, res,) => {
				requestedPaths.push(req.url ?? "",);
				const recipeName = decodeURIComponent((req.url ?? "").split("/",).pop()!.split("?",)[0]!,);
				const recipe = payloadByRecipeName[recipeName];
				if (!recipe) {
					res.statusCode = 404;
					res.end("Not found",);
					return;
				}
				sendJson(res, {
					recipe: { type: recipe.type, },
					payload: recipe.payload,
				},);
			}, async (url,) => {
				const client = createClient(url,);
				for (const [recipeName, expected,] of Object.entries(payloadByRecipeName,)) {
					const filePath = await client.recipes.downloadCode(recipeName,);
					expect(filePath,).toBe(resolve(tempDir, `${recipeName}${expected.ext}`,),);
					expect(await readFile(filePath, "utf-8",),).toBe(expected.payload,);
				}
			},);
		} finally {
			process.chdir(originalCwd,);
			await rm(tempDir, { recursive: true, force: true, },);
		}

		expect(requestedPaths,).toEqual([
			"/public/api/projects/TEST/recipes/python-recipe?includePayload=true",
			"/public/api/projects/TEST/recipes/sql-recipe?includePayload=true",
			"/public/api/projects/TEST/recipes/shell-recipe?includePayload=true",
			"/public/api/projects/TEST/recipes/r-recipe?includePayload=true",
			"/public/api/projects/TEST/recipes/scala-recipe?includePayload=true",
			"/public/api/projects/TEST/recipes/unknown-recipe?includePayload=true",
		],);
	});

	it("throws when downloadCode has no payload to write", async () => {
		await withRecipeServer((_, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
			},);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.recipes.downloadCode("empty",),).rejects.toThrow(
				'Recipe "empty" has no code payload.',
			);
		},);
	});

	it("pre-provisions filesystem outputs under a project path when output connection is explicit", async () => {
		const requests: string[] = [];
		let datasetCreateBody: Record<string, unknown> | undefined;
		let recipeCreateBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [
					{
						name: "input_ds",
						type: "Filesystem",
						params: { connection: "s3_conn", },
						managed: true,
					},
				],);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				datasetCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { name: "output_ds", },);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
				recipeCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { name: "python_output_ds", },);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.create({
				type: "python",
				inputDatasets: ["input_ds",],
				outputDataset: "output_ds",
				outputConnection: "s3_conn",
			},);

			expect(result,).toEqual({
				recipeName: "python_output_ds",
				type: "python",
				createdDatasets: ["output_ds",],
				joinConfigured: false,
				outputProvisioningFallbackUsed: false,
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/",
			"POST /public/api/projects/TEST/datasets/",
			"POST /public/api/projects/TEST/recipes/",
		],);
		expect(datasetCreateBody,).toMatchObject({
			projectKey: "TEST",
			name: "output_ds",
			type: "Filesystem",
			params: {
				connection: "s3_conn",
				path: "/dataiku/TEST/output_ds",
				metastoreTableName: "output_ds",
			},
			managed: true,
		},);
		expect(recipeCreateBody,).toMatchObject({
			recipePrototype: {
				type: "python",
				name: "python_output_ds",
			},
		},);
	});

	it("creates managed-folder output recipes through a temporary dataset and patches the output", async () => {
		const requests: string[] = [];
		let tempDatasetBody: Record<string, unknown> | undefined;
		let recipeCreateBody: Record<string, unknown> | undefined;
		let recipeUpdateBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}`,);

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [
					{
						name: "input_ds",
						type: "Filesystem",
						params: { connection: "s3_conn", },
						managed: true,
					},
				],);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				tempDatasetBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { name: "python_FOLDERID_folder_output_marker", },);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
				recipeCreateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { name: "python_FOLDERID", },);
				return;
			}

			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/python_FOLDERID"
			) {
				sendJson(res, {
					recipe: {
						name: "python_FOLDERID",
						type: "python",
						outputs: {
							main: {
								items: [{ ref: "python_FOLDERID_folder_output_marker", appendMode: false, },],
							},
						},
					},
				},);
				return;
			}

			if (
				req.method === "PUT" && url.pathname === "/public/api/projects/TEST/recipes/python_FOLDERID"
			) {
				recipeUpdateBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}

			if (
				req.method === "DELETE"
				&& url.pathname === "/public/api/projects/TEST/datasets/python_FOLDERID_folder_output_marker"
			) {
				sendJson(res, { ok: true, },);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.create({
				type: "python",
				inputDatasets: ["input_ds",],
				outputFolder: "FOLDERID",
				outputConnection: "s3_conn",
			},);

			expect(result,).toEqual({
				recipeName: "python_FOLDERID",
				type: "python",
				createdDatasets: [],
				joinConfigured: false,
				outputProvisioningFallbackUsed: false,
				outputFolder: "FOLDERID",
				temporaryOutputDataset: "python_FOLDERID_folder_output_marker",
				temporaryOutputDatasetDeleted: true,
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/",
			"POST /public/api/projects/TEST/datasets/",
			"POST /public/api/projects/TEST/recipes/",
			"GET /public/api/projects/TEST/recipes/python_FOLDERID",
			"PUT /public/api/projects/TEST/recipes/python_FOLDERID",
			"DELETE /public/api/projects/TEST/datasets/python_FOLDERID_folder_output_marker",
		],);
		expect(tempDatasetBody,).toMatchObject({
			name: "python_FOLDERID_folder_output_marker",
			params: {
				connection: "s3_conn",
				path: "/dataiku/TEST/python_FOLDERID_folder_output_marker",
			},
		},);
		expect(recipeCreateBody,).toMatchObject({
			recipePrototype: {
				outputs: {
					main: {
						items: [{ ref: "python_FOLDERID_folder_output_marker", appendMode: false, },],
					},
				},
			},
		},);
		expect(recipeUpdateBody,).toMatchObject({
			recipe: {
				outputs: {
					main: {
						items: [{ ref: "FOLDERID", appendMode: false, },],
					},
				},
			},
		},);
	});

	it("runs recipes by resolving managed-folder outputs", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_FOLDERID"
			) {
				sendJson(res, {
					recipe: {
						name: "compute_FOLDERID",
						type: "python",
						outputs: {
							main: {
								items: [{ ref: "Exports", appendMode: false, },],
							},
						},
					},
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [],);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "FOLDERID", name: "Exports", type: "Filesystem", },],);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				buildRequestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-folder", },);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-folder", type: "MANAGED_FOLDER_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-folder/log/") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end(">>> DONE: 19 experiments\n",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.run("compute_FOLDERID", {
				includeLogs: true,
				pollIntervalMs: 1,
				timeoutMs: 5_000,
			},);

			expect(result,).toMatchObject({
				recipeName: "compute_FOLDERID",
				success: true,
				jobId: "job-folder",
				state: "DONE",
				type: "MANAGED_FOLDER_BUILD",
				outputs: [{
					ref: "Exports",
					role: "main",
					id: "FOLDERID",
					type: "MANAGED_FOLDER",
					projectKey: "TEST",
				},],
				log: ">>> DONE: 19 experiments\n",
			},);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "FOLDERID",
				type: "MANAGED_FOLDER",
				targetManagedFolderProjectKey: "TEST",
				targetManagedFolder: "FOLDERID",
				targetPartition: "NP",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
		expect(requests,).toContain("GET /public/api/projects/TEST/recipes/compute_FOLDERID",);
		expect(requests,).toContain("GET /public/api/projects/TEST/datasets/",);
		expect(requests,).toContain("GET /public/api/projects/TEST/managedfolders/",);
		const jobRequests = requests.filter((request,) =>
			request.startsWith("POST /public/api/projects/TEST/jobs/",)
			|| request.startsWith("GET /public/api/projects/TEST/jobs/job-folder",)
		);
		expect(jobRequests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-folder/",
			"GET /public/api/projects/TEST/jobs/job-folder/log/",
		],);
	});

	it("waits for summaries and propagates dataset partitions", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method} ${url.pathname}${url.search}`,);

			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_output"
			) {
				sendJson(res, {
					recipe: {
						name: "compute_output",
						type: "python",
						outputs: {
							main: {
								items: [{ ref: "output_dataset", appendMode: false, },],
							},
						},
					},
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "output_dataset", type: "Filesystem", },],);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [],);
				return;
			}

			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/jobs/") {
				buildRequestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { id: "job-summary", },);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-summary/") {
				sendJson(res, {
					baseStatus: {
						def: { id: "job-summary", type: "DATASET_BUILD", },
						state: "DONE",
					},
					globalState: { done: 1, failed: 0, running: 0, total: 1, },
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/jobs/job-summary/log/") {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/plain",);
				res.end("first line\nsecond line\n",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.run("compute_output", {
				summary: true,
				maxLogLines: 1,
				partition: "P1",
			},);

			expect(result,).toMatchObject({
				recipeName: "compute_output",
				success: true,
				jobId: "job-summary",
				state: "DONE",
				logSummary: {
					state: "DONE",
					lineCount: 2,
					lines: ["second line",],
				},
			},);
			expect(result,).not.toHaveProperty("log",);
		},);

		expect(buildRequestBody,).toEqual({
			outputs: [{
				projectKey: "TEST",
				id: "output_dataset",
				type: "DATASET",
				partition: "P1",
			},],
			type: "NON_RECURSIVE_FORCED_BUILD",
		},);
		expect(
			requests.filter((request,) => request.startsWith("POST /public/api/projects/TEST/jobs/",)),
		)
			.toEqual(["POST /public/api/projects/TEST/jobs/",],);
		expect(requests,).toContain("GET /public/api/projects/TEST/jobs/job-summary/log/",);
	});
});

describe("RecipesResource.validateGraph", () => {
	it("reports missing and ambiguous recipe graph references", async () => {
		await withRecipeServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);

			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/compute_orders"
			) {
				sendJson(res, {
					recipe: {
						name: "compute_orders",
						type: "python",
						inputs: {
							main: { items: [{ ref: "missing_input", }, { ref: "orders", },], },
						},
						outputs: {
							main: { items: [{ ref: "ambiguous", }, { ref: "missing_output", },], },
						},
					},
				},);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "orders", }, { name: "ambiguous", },],);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/managedfolders/") {
				sendJson(res, [{ id: "folder-id", name: "ambiguous", },],);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.recipes.validateGraph("compute_orders",);

			expect(result,).toMatchObject({
				valid: false,
				recipeName: "compute_orders",
				projectKey: "TEST",
				ambiguousOutputs: ["ambiguous",],
				missingInputs: [{ ref: "missing_input", role: "main", exists: false, },],
				missingOutputs: [
					{ ref: "ambiguous", role: "main", exists: false, },
					{ ref: "missing_output", role: "main", exists: false, },
				],
			},);
			expect(result.warnings,).toContain(
				'Output "ambiguous" matches both a dataset and a managed folder; declare an explicit output type.',
			);
			expect(result.warnings,).toContain(
				'Declared input "missing_input" was not found in project "TEST".',
			);
		},);
	});
});
