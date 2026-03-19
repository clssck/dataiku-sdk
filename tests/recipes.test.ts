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
});
