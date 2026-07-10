import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	putItemRefs,
	readBody,
	readFileSync,
	realpathSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("recipe create join flags", () => {
	it("forwards repeatable exact join flags in a dry run", async () => {
		const { stdout, stderr, } = await dss([
			"recipe",
			"create",
			"--type",
			"join",
			"--input",
			"left_ds",
			"--input",
			"right_ds",
			"--output",
			"joined_ds",
			"--join-on",
			"left_id=right_id",
			"--join-on",
			"shared_id",
			"--join-type",
			"INNER",
			"--dry-run",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			dryRun: true,
			action: "create",
			resource: "recipe",
			payload: {
				type: "join",
				inputDatasets: ["left_ds", "right_ds",],
				outputDataset: "joined_ds",
				joinOn: ["left_id=right_id", "shared_id",],
				joinType: "INNER",
			},
		},);
	});

	it("forwards fuzzy matching flags in a dry run", async () => {
		const { stdout, stderr, } = await dss([
			"recipe",
			"create",
			"--type",
			"fuzzyjoin",
			"--input",
			"left_ds,right_ds",
			"--output",
			"joined_ds",
			"--fuzzy-on",
			"left_name=right_name",
			"--fuzzy-on",
			"email",
			"--fuzzy-distance",
			"damerau_levenshtein",
			"--fuzzy-threshold",
			"0.72",
			"--normalize",
			"--dry-run",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			dryRun: true,
			action: "create",
			resource: "recipe",
			payload: {
				type: "fuzzyjoin",
				inputDatasets: ["left_ds", "right_ds",],
				outputDataset: "joined_ds",
				fuzzyOn: ["left_name=right_name", "email",],
				fuzzyDistance: "DAMERAU_LEVENSHTEIN",
				fuzzyThreshold: 0.72,
				fuzzyNormalize: true,
			},
		},);
	});

	it("rejects an invalid fuzzy distance before making a request", async () => {
		const failure = await dssFailure([
			"recipe",
			"create",
			"--type",
			"fuzzyjoin",
			"--input",
			"left_ds,right_ds",
			"--output",
			"joined_ds",
			"--fuzzy-on",
			"name",
			"--fuzzy-distance",
			"edit-distance",
			"--dry-run",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "invalid_enum",
			resource: "recipe",
			action: "create",
		},);
		expect(report.error,).toContain("DAMERAU_LEVENSHTEIN",);
	});
});

describe("recipe input commands", () => {
	const baseRecipe = {
		name: "r1",
		type: "python",
		inputs: { main: { items: [{ ref: "input_a", deps: [], },], }, },
		outputs: { main: { items: [{ ref: "out_ds", },], }, },
	};

	type PutCapture = { puts: number; body?: Record<string, unknown>; };

	function recipeInputServer(recipe: Record<string, unknown>, capture: PutCapture,) {
		return async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/recipes/r1") {
				sendJson(res, { recipe, },);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/TEST/recipes/r1") {
				capture.puts += 1;
				capture.body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, { ok: true, },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		};
	}

	it("add-input appends one item and PUTs the full list", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const { stdout, } = await dss(["recipe", "add-input", "r1", "input_b",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toMatchObject({
				action: "add-input",
				inputs: ["input_a", "input_b",],
			},);
		},);
		expect(capture.puts,).toBe(1,);
		expect(putItemRefs(capture.body,),).toEqual(["input_a", "input_b",],);
	});

	it("add-input rejects a duplicate input without a PUT", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const failure = await dssFailure(["recipe", "add-input", "r1", "input_a",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toContain("is already a",);
			expect(failure.stderr,).toContain("input of recipe",);
		},);
		expect(capture.puts,).toBe(0,);
	});

	it("add-input --if-not-exists skips an existing input without a PUT", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const { stdout, } = await dss(
				["recipe", "add-input", "r1", "input_a", "--if-not-exists",],
				{ env: cliEnv(url,), },
			);
			expect(JSON.parse(stdout,),).toMatchObject({
				skipped: "r1",
				reason: "exists",
				dataset: "input_a",
			},);
		},);
		expect(capture.puts,).toBe(0,);
	});

	it("add-input --dry-run previews the planned inputs without a PUT", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const { stdout, } = await dss(
				["recipe", "add-input", "r1", "input_b", "--dry-run",],
				{ env: cliEnv(url,), },
			);
			expect(JSON.parse(stdout,),).toMatchObject({
				dryRun: true,
				action: "add-input",
				inputs: ["input_a", "input_b",],
			},);
		},);
		expect(capture.puts,).toBe(0,);
	});

	it("remove-input drops one item and PUTs the remainder", async () => {
		const recipe = {
			name: "r1",
			type: "python",
			inputs: { main: { items: [{ ref: "input_a", }, { ref: "input_b", },], }, },
		};
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(recipe, capture,), async (url,) => {
			const { stdout, } = await dss(["recipe", "remove-input", "r1", "input_b",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toMatchObject({ action: "remove-input", inputs: ["input_a",], },);
		},);
		expect(capture.puts,).toBe(1,);
		expect(putItemRefs(capture.body,),).toEqual(["input_a",],);
	});

	it("remove-input rejects a missing input without a PUT", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const failure = await dssFailure(["recipe", "remove-input", "r1", "input_x",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toContain("is not a",);
			expect(failure.stderr,).toContain("input of recipe",);
		},);
		expect(capture.puts,).toBe(0,);
	});

	it("remove-input --if-exists skips a missing input without a PUT", async () => {
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(baseRecipe, capture,), async (url,) => {
			const { stdout, } = await dss(
				["recipe", "remove-input", "r1", "input_x", "--if-exists",],
				{ env: cliEnv(url,), },
			);
			expect(JSON.parse(stdout,),).toMatchObject({
				skipped: "r1",
				reason: "missing",
				dataset: "input_x",
			},);
		},);
		expect(capture.puts,).toBe(0,);
	});

	it("add-input preserves other roles and existing item fields", async () => {
		const recipe = {
			name: "r1",
			type: "python",
			inputs: {
				main: { items: [{ ref: "input_a", deps: ["dep1",], extra: "keep", },], },
				secondary: { items: [{ ref: "side", },], },
			},
		};
		const capture: PutCapture = { puts: 0, };
		await withCliServer(recipeInputServer(recipe, capture,), async (url,) => {
			await dss(["recipe", "add-input", "r1", "input_b",], { env: cliEnv(url,), },);
		},);
		const putRecipe = capture.body?.recipe as {
			inputs: Record<string, { items: Array<Record<string, unknown>>; }>;
		};
		expect(putRecipe.inputs.main.items,).toEqual([
			{ ref: "input_a", deps: ["dep1",], extra: "keep", },
			{ ref: "input_b", deps: [], },
		],);
		expect(putRecipe.inputs.secondary.items,).toEqual([{ ref: "side", },],);
	});
});

describe("CLI recipe get", () => {
	it("prints compact recipe settings when DSS returns a payload and --no-payload is set", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
			expect(url.searchParams.get("includePayload",),).toBeNull();
			sendJson(res, {
				recipe: { name: "my_recipe", type: "python", },
				payload: "print('large payload')\n",
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["recipe", "get", "my_recipe", "--no-payload",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({
				recipe: { name: "my_recipe", type: "python", },
			},);
		},);
	});
});

describe("CLI recipe get-payload and set-payload", () => {
	it("get-payload prints recipe code to stdout", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
				payload: "print('hello')\n",
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["recipe", "get-payload", "my_recipe",], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toBe("print('hello')\n",);
		},);
	});

	it("get-payload --raw preserves payload bytes on stdout", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
				payload: "print('hello')\r\nprint('bye')\n",
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["recipe", "get-payload", "my_recipe", "--raw",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			expect(stdout,).toBe("print('hello')\r\nprint('bye')\n",);
		},);
	});

	it("get-payload writes to --output file", async () => {
		const outPath = join(tmpdir(), `dss-cli-getpayload-${Date.now()}.py`,);
		try {
			await withCliServer((_req, res,) => {
				sendJson(res, {
					recipe: { type: "python", },
					payload: "import os\n",
				},);
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"get-payload",
					"my_recipe",
					"--output",
					outPath,
				], { env: cliEnv(url,), },);
				expect(JSON.parse(stdout,),).toBe(outPath,);
				expect(readFileSync(outPath, "utf-8",),).toBe("import os\n",);
			},);
		} finally {
			rmSync(outPath, { force: true, },);
		}
	});

	it("set-payload reads from --file and PUTs", async () => {
		const filePath = join(tmpdir(), `dss-cli-setpayload-${Date.now()}.py`,);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		let putBody: string | undefined;

		try {
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: "print('old')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--no-backup",
				], { env: cliEnv(url,), },);
				expect(stdout,).toContain('"updated": "my_recipe"',);
				expect(putBody,).toBeDefined();
				const parsed = JSON.parse(putBody!,);
				expect(parsed.payload,).toBe("print('updated')\n",);
			},);
		} finally {
			rmSync(filePath, { force: true, },);
		}
	});

	it("set-payload writes a remote payload backup before PUT", async () => {
		const filePath = join(tmpdir(), `dss-cli-setpayload-${Date.now()}.py`,);
		const backupDir = join(tmpdir(), `dss-cli-backup-${Date.now()}`,);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		const requestEvents: string[] = [];
		let putBody: string | undefined;

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				requestEvents.push(`${req.method} ${url.pathname}${url.search}`,);
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: putBody === undefined ? "print('remote')\n" : "print('updated')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const { stdout, } = await dss([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--backup-dir",
					backupDir,
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as { backupPath: string; backupCreated: boolean; };
				expect(result.backupCreated,).toBe(true,);
				expect(result.backupPath.startsWith(backupDir,),).toBe(true,);
				const backup = JSON.parse(readFileSync(result.backupPath, "utf-8",),) as {
					payload: string;
					payloadHash: string;
					recipe: { name: string; };
				};
				expect(backup.payload,).toBe("print('remote')\n",);
				expect(backup.payloadHash,).toHaveLength(64,);
				expect(backup.recipe.name,).toBe("my_recipe",);
				expect(putBody,).toBeDefined();
				expect(JSON.parse(putBody!,).payload,).toBe("print('updated')\n",);
			},);
		} finally {
			expect(requestEvents,).toEqual([
				"GET /public/api/projects/TEST/recipes/my_recipe?includePayload=true",
				"PUT /public/api/projects/TEST/recipes/my_recipe",
			],);
			rmSync(filePath, { force: true, },);
			rmSync(backupDir, { recursive: true, force: true, },);
		}
	});

	it("set-payload uses the default backup directory", async () => {
		const tempDir = join(tmpdir(), `dss-cli-default-backup-${Date.now()}`,);
		const filePath = join(tempDir, "updated.py",);
		let putBody: string | undefined;

		try {
			mkdirSync(tempDir, { recursive: true, },);
			writeFileSync(filePath, "print('updated')\n", "utf-8",);
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "my_recipe", },
						payload: "print('remote')\n",
					},);
					return;
				}
				if (req.method === "PUT") {
					putBody = await readBody(req,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const resolvedTempDir = realpathSync(tempDir,);
				const result = JSON.parse(
					(await dss(["recipe", "set-payload", "my_recipe", "--file", filePath,], {
						cwd: tempDir,
						env: cliEnv(url,),
					},)).stdout,
				) as { backupCreated: boolean; backupPath: string; };

				expect(result.backupCreated,).toBe(true,);
				expect(result.backupPath.startsWith(join(resolvedTempDir, ".dss-backups", "recipes",),),).toBe(
					true,
				);
				const backup = JSON.parse(readFileSync(result.backupPath, "utf-8",),) as {
					normalizedPayloadHash: string;
					payload: string;
				};
				expect(backup.payload,).toBe("print('remote')\n",);
				expect(backup.normalizedPayloadHash,).toHaveLength(64,);
				expect(JSON.parse(putBody!,).payload,).toBe("print('updated')\n",);
			},);
		} finally {
			rmSync(tempDir, { recursive: true, force: true, },);
		}
	});

	it("set-payload fails without --file", async () => {
		const failure = await dssFailure(["recipe", "set-payload", "my_recipe",], {
			env: cliEnv("http://localhost:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toContain("--file is required",);
	});

	it("get-payload writes --raw payloads to --output and returns the path as JSON", async () => {
		let requests = 0;
		const outputPath = join(tmpdir(), `dss-raw-output-${Date.now()}.py`,);
		try {
			await withCliServer((req, res,) => {
				requests++;
				const url = new URL(req.url ?? "/", "http://localhost",);
				expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
				expect(url.searchParams.get("includePayload",),).toBe("true",);
				sendJson(res, {
					recipe: { name: "my_recipe", type: "python", },
					payload: "print('remote')\n",
				},);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"recipe",
					"get-payload",
					"my_recipe",
					"--raw",
					"--output",
					outputPath,
				], { env: cliEnv(url,), },);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toBe(outputPath,);
				expect(readFileSync(outputPath, "utf-8",),).toBe("print('remote')\n",);
			},);
			expect(requests,).toBe(1,);
		} finally {
			rmSync(outputPath, { force: true, },);
		}
	});

	it("recipe run resolves managed-folder outputs and waits for logs", async () => {
		const requests: string[] = [];
		let buildRequestBody: Record<string, unknown> | undefined;

		await withCliServer(async (req, res,) => {
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
								items: [{ ref: "FOLDERID", appendMode: false, },],
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
				res.end(
					"2026-01-01 backend-log ignore\nstderr: noisy\nstdout: preparing\n>>> DONE: 19 experiments\n",
				);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"recipe",
				"run",
				"compute_FOLDERID",
				"--include-logs",
				"--max-log-lines",
				"20",
				"--log-filter",
				"stdout",
				"--summary",
				"--timeout",
				"5000",
			], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result,).toMatchObject({
				recipeName: "compute_FOLDERID",
				success: true,
				jobId: "job-folder",
				state: "DONE",
				type: "MANAGED_FOLDER_BUILD",
				log: "stdout: preparing\n>>> DONE: 19 experiments",
				logSummary: {
					state: "DONE",
					lineCount: 2,
					lines: ["stdout: preparing", ">>> DONE: 19 experiments",],
				},
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
});
