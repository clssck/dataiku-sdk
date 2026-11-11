import { describe, expect, it, } from "bun:test";
import { lstatSync, mkdtempSync, readdirSync, symlinkSync, } from "node:fs";
import { ensureRecipeBackupDir, writeRecipeBackup, } from "../../src/cli/helpers/recipe.js";
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
	statSync,
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
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			code: "invalid_enum",
		},);
		expect(report.error,).toContain("DAMERAU_LEVENSHTEIN",);
	});
	it("rejects an invalid fuzzy threshold with its canonical flag name", async () => {
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
			"--fuzzy-threshold",
			"not-a-number",
			"--dry-run",
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);

		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(JSON.parse(failure.stdout,),).toMatchObject({
			code: "invalid_flag_value",
			details: {
				flag: "--fuzzy-threshold",
				value: "not-a-number",
			},
		},);
	});
});

describe("recipe create prepare flags and naming", () => {
	const prepareArgs = [
		"recipe",
		"create",
		"--type",
		"prepare",
		"--input",
		"in_ds",
		"--output",
		"out_ds",
		"--name",
		"add_status",
	];

	async function planSteps(extra: string[],) {
		const { stdout, stderr, } = await dss([
			...prepareArgs,
			...extra,
			"--plan",
			"--project-key",
			"TEST",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		const plan = JSON.parse(stdout,) as {
			requests: Array<{ payloadEdits?: { steps?: Array<{ type: string; }>; }; }>;
		};
		const steps = plan.requests.find((request,) => request.payloadEdits,)?.payloadEdits?.steps ?? [];
		return { steps: steps.map((step,) => step.type,), stderr, };
	}

	it("warns on --plan when a --formula after --rename names the renamed-away column", async () => {
		const { steps, stderr, } = await planSteps(["--rename", "kind=k2", "--formula", "x=kind + 1",],);
		expect(steps,).toEqual(["ColumnRenamer", "CreateColumnWithGREL",],);
		const event = JSON.parse(stderr,) as { type: string; warnings: Array<Record<string, unknown>>; };
		expect(event.type,).toBe("warning",);
		expect(event.warnings.length,).toBe(1,);
		expect(event.warnings[0].code,).toBe("recipe_formula_uses_renamed_column",);
		expect(event.warnings[0].flag,).toBe("--formula",);
		expect(event.warnings[0].columns,).toEqual([{ renamed: "kind", to: "k2", },],);
	});

	it("also warns on --dry-run and on a --filter that runs after --rename", async () => {
		await withCliServer((_req, res,) => sendJson(res, [],), async (url,) => {
			const { stderr, } = await dss([
				...prepareArgs,
				"--rename",
				"kind=k2",
				"--filter",
				"kind == 'a'",
				"--dry-run",
				"--project-key",
				"TEST",
			], { env: cliEnv(url,), },);
			const event = JSON.parse(stderr,) as { warnings: Array<Record<string, unknown>>; };
			expect(event.warnings.map((warning,) => [warning.code, warning.flag,],),).toEqual([
				["recipe_formula_uses_renamed_column", "--filter",],
			],);
		},);
	});

	it("runs prepare steps in flag order, so a formula typed before --rename keeps the old name valid", async () => {
		const { steps, stderr, } = await planSteps(["--formula", "x=kind + 1", "--rename", "kind=k2",],);
		expect(steps,).toEqual(["CreateColumnWithGREL", "ColumnRenamer",],);
		expect(stderr,).toBe("",);
	});

	it("does not warn for new names, string literals, longer identifiers, or method calls", async () => {
		const { stderr, } = await planSteps([
			"--rename",
			"kind=k2",
			"--formula",
			'x=k2 + "kind" + kinds + upper(name)',
		],);
		expect(stderr,).toBe("",);
	});

	it("flags the recipe name DSS returned when it differs from --name", async () => {
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
				await readBody(req,);
				sendJson(res, { name: "compute_out_ds", },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"recipe",
				"create",
				"--type",
				"python",
				"--input",
				"in_ds",
				"--output",
				"out_ds",
				"--name",
				"add_status",
				"--project-key",
				"TEST",
			], { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result.created,).toBe("compute_out_ds",);
			expect(result.requestedName,).toBe("add_status",);
			const event = JSON.parse(stderr,) as { warnings: Array<Record<string, unknown>>; };
			expect(event.warnings.length,).toBe(1,);
			expect(event.warnings[0].code,).toBe("recipe_renamed",);
			expect(event.warnings[0].requestedName,).toBe("add_status",);
			expect(event.warnings[0].recipe,).toBe("compute_out_ds",);
			expect(event.warnings[0].reason,).toBe("dss_chose_name",);
		},);
	});

	it("reports a numeric suffix as name_taken and stays silent when the name is honored", async () => {
		const returned = ["add_status_1", "add_status",];
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/recipes/") {
				await readBody(req,);
				sendJson(res, { name: returned.shift(), },);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const args = [
				"recipe",
				"create",
				"--type",
				"python",
				"--input",
				"in_ds",
				"--output",
				"out_ds",
				"--name",
				"add_status",
				"--project-key",
				"TEST",
			];
			const taken = await dss(args, { env: cliEnv(url,), },);
			const event = JSON.parse(taken.stderr,) as { warnings: Array<Record<string, unknown>>; };
			expect(event.warnings[0].reason,).toBe("name_taken",);
			const honored = await dss(args, { env: cliEnv(url,), },);
			expect(honored.stderr,).toBe("",);
			expect(JSON.parse(honored.stdout,),).not.toHaveProperty("requestedName",);
		},);
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
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(String(report.error,),).toContain("is already a",);
			expect(String(report.error,),).toContain("input of recipe",);
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
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(String(report.error,),).toContain("is not a",);
			expect(String(report.error,),).toContain("input of recipe",);
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

describe("recipe diff and local file validation", () => {
	it("recipe diff reports no differences when only line endings differ", async () => {
		const filePath = join(tmpdir(), `dss-cli-crlf-diff-${Date.now()}.py`,);
		writeFileSync(filePath, "print('a')\r\nprint('b')\r\n", "utf-8",);
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				expect(req.method,).toBe("GET",);
				expect(url.pathname,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
				sendJson(res, {
					recipe: { type: "python", name: "my_recipe", },
					payload: "print('a')\nprint('b')\n",
				},);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					["recipe", "diff", "my_recipe", "--file", filePath,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toBe("No differences.",);
			},);
		} finally {
			rmSync(filePath, { force: true, },);
		}
	});

	it("recipe diff reads the local file before any DSS request and maps missing files to usage", async () => {
		let requests = 0;
		await withCliServer((
			_req,
			_res,
		) => {
			requests += 1;
		}, async (url,) => {
			const missingPath = join(tmpdir(), `dss-cli-missing-${Date.now()}.py`,);
			const failure = await dssFailure(["recipe", "diff", "my_recipe", "--file", missingPath,], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "not_found",
				category: "usage",
				exitCode: 1,
			},);
			expect(String(report.error,),).toContain(missingPath,);
			expect(requests,).toBe(0,);
		},);
	});

	it("set-payload maps a missing local file to usage validation before any DSS request", async () => {
		let requests = 0;
		await withCliServer((
			_req,
			_res,
		) => {
			requests += 1;
		}, async (url,) => {
			const missingPath = join(tmpdir(), `dss-cli-missing-setpayload-${Date.now()}.py`,);
			const failure = await dssFailure(
				["recipe", "set-payload", "my_recipe", "--file", missingPath, "--no-backup",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "not_found",
				category: "usage",
				exitCode: 1,
			},);
			expect(String(report.error,),).toContain(missingPath,);
			expect(requests,).toBe(0,);
		},);
	});

	it("assert-unchanged maps a missing backup file to usage validation before any DSS request", async () => {
		let requests = 0;
		await withCliServer((
			_req,
			_res,
		) => {
			requests += 1;
		}, async (url,) => {
			const missingBackup = join(tmpdir(), `dss-cli-missing-backup-${Date.now()}.json`,);
			const failure = await dssFailure(
				["recipe", "assert-unchanged", "my_recipe", "--since", missingBackup,],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "not_found",
				category: "usage",
				exitCode: 1,
			},);
			expect(String(report.error,),).toContain(missingBackup,);
			expect(requests,).toBe(0,);
		},);
	});

	it("restore maps a missing backup file to usage validation before any DSS request", async () => {
		let requests = 0;
		await withCliServer((
			_req,
			_res,
		) => {
			requests += 1;
		}, async (url,) => {
			const missingBackup = join(tmpdir(), `dss-cli-missing-restore-${Date.now()}.json`,);
			const failure = await dssFailure(
				["recipe", "restore", "my_recipe", "--backup", missingBackup,],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				code: "not_found",
				category: "usage",
				exitCode: 1,
			},);
			expect(String(report.error,),).toContain(missingBackup,);
			expect(requests,).toBe(0,);
		},);
	});
});

const columns = (names: string[],) => ({
	columns: names.map((name,) => ({ name, type: "string", }),),
});

const payloadStdout = async (
	command: "get-payload" | "cat",
	type: string,
	payload: string,
): Promise<string> => {
	let stdout = "";
	await withCliServer((_req, res,) => {
		sendJson(res, { recipe: { type, }, payload, },);
	}, async (url,) => {
		const result = await dss(["recipe", command, "my_recipe",], { env: cliEnv(url,), },);
		expect(result.stderr,).toBe("",);
		stdout = result.stdout;
	},);
	return stdout;
};

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

	it("get-payload returns payload bytes as a JSON-encoded string on stdout", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, {
				recipe: { type: "python", },
				payload: "print('hello')\r\nprint('bye')\n",
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["recipe", "get-payload", "my_recipe",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toBe("print('hello')\r\nprint('bye')\n",);
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

	describe("visual-recipe JSON payloads", () => {
		const prepare = {
			columnsSelection: { mode: "ALL", },
			steps: [
				{ type: "ColumnRenamer", params: { renamings: [{ from: "a", to: "b", },], }, },
				{ type: "CreateColumnWithGREL", params: { column: "c", expression: "b * 2", }, },
			],
		};
		const prepareText = JSON.stringify(prepare, null, 2,);

		for (const command of ["get-payload", "cat",] as const) {
			it(`${command} prints a JSON object payload as an object, not an escaped string`, async () => {
				const stdout = await payloadStdout(command, "shaker", prepareText,);
				expect(JSON.parse(stdout,),).toEqual(prepare,);
				expect(stdout,).not.toContain('\\"',);
				expect(stdout,).not.toContain("\\n",);
			});

			it(`${command} prints a JSON array payload as an array`, async () => {
				const stdout = await payloadStdout(command, "shaker", '[\n  {"a": 1},\n  2\n]\n',);
				expect(JSON.parse(stdout,),).toEqual([{ a: 1, }, 2,],);
			});

			it(`${command} prints a Python payload as the exact JSON string`, async () => {
				const code = 'import dataiku\nprint(\'{"not": "json"}\')\n';
				expect(JSON.parse(await payloadStdout(command, "python", code,),),).toBe(code,);
			});

			it(`${command} still rejects an empty payload as validation_failed`, async () => {
				await withCliServer((_req, res,) => {
					sendJson(res, { recipe: { type: "python", }, payload: "", },);
				}, async (url,) => {
					const failure = await dssFailure(["recipe", command, "my_recipe",], { env: cliEnv(url,), },);
					expect(failure.code,).toBe(1,);
					expect(JSON.parse(failure.stdout,),).toMatchObject({
						code: "validation_failed",
						error: 'Recipe "my_recipe" has no code payload.',
					},);
				},);
			});

			it(`${command} keeps payload text that parses to a JSON scalar as a string`, async () => {
				expect(JSON.parse(await payloadStdout(command, "python", "42",),),).toBe("42",);
				expect(JSON.parse(await payloadStdout(command, "python", '"x"',),),).toBe('"x"',);
				expect(JSON.parse(await payloadStdout(command, "python", "null",),),).toBe("null",);
			});

			it(`${command} keeps invalid JSON that starts with a brace as a string`, async () => {
				for (const text of ['{"steps": [', "{ not json }\n", "[1, 2",]) {
					expect(JSON.parse(await payloadStdout(command, "python", text,),),).toBe(text,);
				}
			});
		}

		it("get-payload --output writes the exact server bytes for a visual recipe", async () => {
			const outPath = join(tmpdir(), `dss-cli-getpayload-visual-${Date.now()}.json`,);
			const text = `${prepareText}\r\n`;
			try {
				await withCliServer((_req, res,) => {
					sendJson(res, { recipe: { type: "shaker", }, payload: text, },);
				}, async (url,) => {
					const { stdout, } = await dss(
						["recipe", "get-payload", "my_recipe", "--output", outPath,],
						{ env: cliEnv(url,), },
					);
					expect(JSON.parse(stdout,),).toBe(outPath,);
					expect(readFileSync(outPath, "utf-8",),).toBe(text,);
				},);
			} finally {
				rmSync(outPath, { force: true, },);
			}
		});
	});
	it("--raw is rejected as an unknown flag on get-payload", async () => {
		const failure = await dssFailure(["recipe", "get-payload", "my_recipe", "--raw",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			error: "Unknown flag: --raw",
			code: "unknown_flag",
			category: "usage",
			exitCode: 1,
		},);
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
				const result = JSON.parse(stdout,) as Record<string, unknown>;
				expect(result.updated,).toBe("my_recipe",);
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
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(String(report.error,),).toContain("--file is required",);
	});

	it("get-payload writes payloads to --output and returns the path as JSON", async () => {
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
					"",
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
				logSummary: { state: "DONE", lineCount: 2, },
			},);
			expect(result["logSummary"],).not.toHaveProperty("lines",);
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
			|| request.startsWith("GET /public/api/projects/TEST/jobs/job-folder",),
		);
		expect(jobRequests,).toEqual([
			"POST /public/api/projects/TEST/jobs/",
			"GET /public/api/projects/TEST/jobs/job-folder/",
			"GET /public/api/projects/TEST/jobs/job-folder/log/",
		],);
	});

	it("set-payload moves DSS-derived output schemas with the payload and keeps edited ones", async () => {
		const payloadPath = join(tmpdir(), `dss-grouping-${Date.now()}.json`,);
		writeFileSync(payloadPath, '{"keys":[{"column":"category"}]}',);
		let saved = false;
		const applied: unknown[] = [];
		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				const base = "/public/api/projects/TEST/recipes/group_orders";
				if (req.method === "GET" && url.pathname === base) {
					sendJson(res, { recipe: { name: "group_orders", type: "grouping", }, payload: "{}", },);
					return;
				}
				if (req.method === "PUT" && url.pathname === base) {
					await readBody(req,);
					saved = true;
					sendJson(res, {},);
					return;
				}
				if (req.method === "GET" && url.pathname === `${base}/schema-update`) {
					// derived_out matches the computed schema before the upload (DSS-derived) and
					// differs after it; edited_out differs throughout (hand-edited).
					sendJson(res, {
						computables: [
							{
								type: "DATASET",
								datasetName: "derived_out",
								previousSchemaWasEmpty: false,
								incompatibilities: saved ? ["Different number of columns",] : [],
								newSchema: columns(saved ? ["category", "count",] : ["count",],),
							},
							{
								type: "DATASET",
								datasetName: "edited_out",
								previousSchemaWasEmpty: false,
								incompatibilities: ["Different number of columns",],
								newSchema: columns(["category", "count",],),
							},
						],
					},);
					return;
				}
				if (req.method === "POST" && url.pathname === `${base}/actions/updateOutputSchema`) {
					const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
					applied.push(body.computableId,);
					sendJson(res, { hasAnyProblem: false, datasetsNeedingAction: [], },);
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					["recipe", "set-payload", "group_orders", "--file", payloadPath, "--no-backup",],
					{ env: cliEnv(url,), },
				);
				expect(JSON.parse(stdout,),).toMatchObject({ outputSchemaUpdated: ["derived_out",], },);
				expect(stderr,).toContain("recipe_output_schema_outdated",);
				expect(stderr,).toContain("edited_out",);
			},);
			expect(applied,).toEqual(["derived_out",],);
		} finally {
			rmSync(payloadPath, { force: true, },);
		}
	});
});

describe("recipe set-payload backup hardening", () => {
	it("creates owner-only 0700 backup directories and a 0600 backup file", async () => {
		const baseDir = mkdtempSync(join(tmpdir(), "dss-cli-backup-safe-",),);
		const backupDir = join(baseDir, "nested", ".dss-backups", "recipes",);
		const filePath = join(baseDir, "updated.py",);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		let putBody: string | undefined;
		try {
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
				const { stdout, } = await dss([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--backup-dir",
					backupDir,
				], { env: cliEnv(url,), },);
				const result = JSON.parse(stdout,) as {
					backupCreated: boolean;
					backupPath: string;
				};
				expect(result.backupCreated,).toBe(true,);
				expect(lstatSync(backupDir,).isSymbolicLink(),).toBe(false,);
				if (process.platform !== "win32") {
					// POSIX mode bits are meaningless on Windows (stat reports 0666).
					expect(statSync(backupDir,).mode & 0o777,).toBe(0o700,);
					expect(statSync(join(baseDir, "nested", ".dss-backups",),).mode & 0o777,).toBe(
						0o700,
					);
					expect(statSync(join(baseDir, "nested",),).mode & 0o777,).toBe(0o700,);
					expect(statSync(result.backupPath,).mode & 0o777,).toBe(0o600,);
				}
			},);
			expect(JSON.parse(putBody!,).payload,).toBe("print('updated')\n",);
		} finally {
			rmSync(baseDir, { recursive: true, force: true, },);
		}
	});

	it("refuses a symlinked backup directory before writing or PUTting", async () => {
		const baseDir = mkdtempSync(join(tmpdir(), "dss-cli-backup-linkdir-",),);
		const realDir = join(baseDir, "real",);
		const backupDir = join(baseDir, "backup-link",);
		mkdirSync(realDir,);
		symlinkSync(realDir, backupDir,);
		const filePath = join(baseDir, "updated.py",);
		writeFileSync(filePath, "print('updated')\n", "utf-8",);
		let putBody: string | undefined;
		try {
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
				const failure = await dssFailure([
					"recipe",
					"set-payload",
					"my_recipe",
					"--file",
					filePath,
					"--backup-dir",
					backupDir,
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
				expect(report.error,).toContain("symlink",);
				expect(report.code,).toBe("usage_error",);
			},);
			expect(putBody,).toBeUndefined();
			expect(readdirSync(realDir,).length,).toBe(0,);
		} finally {
			rmSync(baseDir, { recursive: true, force: true, },);
		}
	});

	it("writeRecipeBackup refuses an existing destination without touching it", () => {
		const baseDir = mkdtempSync(join(tmpdir(), "dss-cli-backup-exists-",),);
		try {
			const backupPath = join(baseDir, "backup.json",);
			writeRecipeBackup(backupPath, "first",);
			expect(() => writeRecipeBackup(backupPath, "second",),).toThrow(/already exists/,);
			expect(readFileSync(backupPath, "utf-8",),).toBe("first",);
			if (process.platform !== "win32") expect(statSync(backupPath,).mode & 0o777,).toBe(0o600,);
		} finally {
			rmSync(baseDir, { recursive: true, force: true, },);
		}
	});

	it("writeRecipeBackup refuses a symlink destination without touching its target", () => {
		const baseDir = mkdtempSync(join(tmpdir(), "dss-cli-backup-linkfile-",),);
		try {
			const target = join(baseDir, "target.json",);
			writeFileSync(target, "original", "utf-8",);
			const backupPath = join(baseDir, "backup.json",);
			symlinkSync(target, backupPath,);
			expect(() => writeRecipeBackup(backupPath, "second",),).toThrow(/already exists/,);
			expect(readFileSync(target, "utf-8",),).toBe("original",);
		} finally {
			rmSync(baseDir, { recursive: true, force: true, },);
		}
	});

	it("ensureRecipeBackupDir refuses a symlink component in the path", () => {
		const baseDir = mkdtempSync(join(tmpdir(), "dss-cli-backup-linkpath-",),);
		try {
			const realDir = join(baseDir, "real",);
			mkdirSync(realDir,);
			symlinkSync(realDir, join(baseDir, "link",),);
			expect(() => ensureRecipeBackupDir(join(baseDir, "link", "nested",),),).toThrow(
				/symlink/,
			);
		} finally {
			rmSync(baseDir, { recursive: true, force: true, },);
		}
	});

	it("ensureRecipeBackupDir tolerates an OS-level symlink above the temp directory but not inside the named path", () => {
		// macOS: tmpdir() is /var/folders/... and /var -> /private/var. Model that
		// by pointing TMPDIR at a symlink to a real directory.
		if (process.platform === "win32") return;
		const realTmp = mkdtempSync(join(tmpdir(), "dss-cli-backup-realtmp-",),);
		const linkedTmp = `${realTmp}-link`;
		symlinkSync(realTmp, linkedTmp,);
		const previousTmpdir = process.env.TMPDIR;
		process.env.TMPDIR = linkedTmp;
		try {
			const backupDir = join(linkedTmp, "nested", ".dss-backups", "recipes",);
			expect(() => ensureRecipeBackupDir(backupDir,),).not.toThrow();
			expect(lstatSync(join(realTmp, "nested", ".dss-backups", "recipes",),).isDirectory(),).toBe(
				true,
			);

			symlinkSync(join(realTmp, "nested",), join(realTmp, "planted",),);
			expect(() => ensureRecipeBackupDir(join(linkedTmp, "planted", "recipes",),),).toThrow(
				/symlink/,
			);
		} finally {
			if (previousTmpdir === undefined) delete process.env.TMPDIR;
			else process.env.TMPDIR = previousTmpdir;
			rmSync(linkedTmp, { force: true, },);
			rmSync(realTmp, { recursive: true, force: true, },);
		}
	});

	it("ensureRecipeBackupDir refuses the filesystem root as backup directory", () => {
		expect(() => ensureRecipeBackupDir("/",),).toThrow(/filesystem root/,);
	});
});

/** Run `set-payload my_recipe ARGS` against a recipe holding `payload`; `put` is what DSS was asked to store. */
async function edit(
	payload: string,
	args: string[],
): Promise<{ code: number | null; result: Record<string, unknown>; put?: string; }> {
	let put: string | undefined;
	let outcome: { code: number | null; stdout: string; } = { code: 0, stdout: "", };
	await withCliServer(async (req, res,) => {
		if (req.method === "PUT") {
			const body = JSON.parse(await readBody(req,),) as { payload: string; };
			put = body.payload;
			sendJson(res, {},);
			return;
		}
		sendJson(res, { recipe: { type: "python", name: "my_recipe", }, payload, },);
	}, async (url,) => {
		const argv = ["recipe", "set-payload", "my_recipe", ...args, "--no-backup",];
		try {
			outcome = { code: 0, stdout: (await dss(argv, { env: cliEnv(url,), },)).stdout, };
		} catch (error) {
			outcome = error as { code: number | null; stdout: string; };
		}
	},);
	return {
		code: outcome.code,
		result: JSON.parse(outcome.stdout,) as Record<string, unknown>,
		...(put === undefined ? {} : { put, }),
	};
}

describe("recipe set-payload --old/--new", () => {
	const CODE = 'out = df.groupby("country")["amount_euro"].sum()\nout.columns = ["revenue"]\n';

	it("replaces the single occurrence in the payload DSS holds, with no local file", async () => {
		const { code, result, put, } = await edit(CODE, [
			"--old",
			"amount_euro",
			"--new",
			"amount_eur",
		],);
		expect(code,).toBe(0,);
		expect(put,).toBe(CODE.replace("amount_euro", "amount_eur",),);
		expect(result,).toMatchObject({ updated: "my_recipe", replacements: 1, backupCreated: false, },);
		expect(result,).not.toHaveProperty("file",);
	});

	it("writes nothing when --old is absent or ambiguous, and replaces every occurrence with --all", async () => {
		const twice = `${CODE}print("amount_euro")\n`;
		const absent = await edit(CODE, ["--old", "amount_usd", "--new", "amount_eur",],);
		expect(absent,).toMatchObject({ code: 1, result: { code: "not_found", }, },);
		expect(absent,).not.toHaveProperty("put",);
		const ambiguous = await edit(twice, ["--old", "amount_euro", "--new", "amount_eur",],);
		expect(ambiguous,).toMatchObject({ code: 1, result: { code: "validation_failed", }, },);
		expect(String(ambiguous.result["error"],),).toContain("2 times",);
		expect(ambiguous,).not.toHaveProperty("put",);
		const all = await edit(twice, ["--old", "amount_euro", "--new", "amount_eur", "--all",],);
		expect(all.result,).toMatchObject({ replacements: 2, },);
		expect(all.put,).toBe(twice.replaceAll("amount_euro", "amount_eur",),);
	});

	it("takes the new text literally and deletes with an empty --new", async () => {
		const literal = await edit("total = x\n", ["--old", "x", "--new", "$&[$1]$$",],);
		expect(literal.put,).toBe("total = $&[$1]$$\n",);
		const deleted = await edit("x = 1  # tmp\n", ["--old", "  # tmp", "--new", "",],);
		expect(deleted.put,).toBe("x = 1\n",);
	});

	it("matches multi-line LF text against a CRLF payload and keeps its line endings", async () => {
		const { put, } = await edit("a = 1\r\nb = 2\r\nc = 3\r\n", [
			"--old",
			"a = 1\nb = 2",
			"--new",
			"a = 1\nb = 20",
		],);
		expect(put,).toBe("a = 1\r\nb = 20\r\nc = 3\r\n",);
	});

	it("previews the edit with --dry-run as one payload change", async () => {
		const { result, put, } = await edit(CODE, [
			"--old",
			"amount_euro",
			"--new",
			"amount_eur",
			"--dry-run",
		],);
		expect(put,).toBeUndefined();
		expect(result,).toMatchObject({
			dryRun: true,
			replacements: 1,
			changeCount: 1,
			changes: [{
				path: "payload",
				before: CODE,
				after: CODE.replace("amount_euro", "amount_eur",),
			},],
		},);
	});

	it("rejects --file together with --old, and an edit with nothing to change, before any request", async () => {
		const env = cliEnv("http://127.0.0.1:1",);
		const both = await dssFailure(
			["recipe", "set-payload", "r", "--file", "code.py", "--old", "a", "--new", "b",],
			{ env, },
		);
		expect(JSON.parse(both.stdout,),).toMatchObject({ code: "conflicting_input_sources", },);
		const same = await dssFailure(["recipe", "set-payload", "r", "--old", "a", "--new", "a",], {
			env,
		},);
		expect(JSON.parse(same.stdout,),).toMatchObject({ code: "invalid_flag_value", },);
		const half = await dssFailure(["recipe", "set-payload", "r", "--old", "a",], { env, },);
		expect(JSON.parse(half.stdout,),).toMatchObject({ code: "missing_required_flag", },);
	});

	it("plans an edit as a non-exact PUT that names the replacement", async () => {
		const { stdout, } = await dss([
			"recipe",
			"set-payload",
			"r",
			"--old",
			"a",
			"--new",
			"b",
			"--no-backup",
			"--plan",
			"--project-key",
			"TEST",
		], { env: cliEnv("http://127.0.0.1:1",), },);
		expect(JSON.parse(stdout,),).toMatchObject({
			exact: false,
			method: "PUT",
			payload: { replace: { old: "a", new: "b", all: false, }, },
		},);
	});
});

function serveCreateAndBuild(state: "DONE" | "FAILED", requests: string[],) {
	return async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		requests.push(`${req.method} ${url.pathname}`,);
		if (req.method === "POST" && url.pathname.endsWith("/recipes/",)) {
			await readBody(req,);
			sendJson(res, { name: "py_out", },);
		} else if (req.method === "POST" && url.pathname.endsWith("/jobs/",)) {
			const body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			requests.push(`build ${String(body["type"],)}`,);
			sendJson(res, { id: "JOB1", },);
		} else if (url.pathname.endsWith("/jobs/JOB1/log/",)) {
			res.setHeader("content-type", "text/plain",);
			res.end("line\n",);
		} else if (url.pathname.endsWith("/jobs/JOB1/",)) {
			sendJson(res, {
				baseStatus: { def: { id: "JOB1", type: "RECURSIVE_BUILD", }, state, },
				globalState: { done: state === "DONE" ? 1 : 0, failed: state === "DONE" ? 0 : 1, total: 1, },
			},);
		} else {
			sendJson(res, { columns: [{ name: "a", type: "string", },], userModified: false, },);
		}
	};
}

describe("recipe create --build", () => {
	const createArgs = [
		"recipe",
		"create",
		"--type",
		"python",
		"--input",
		"in_ds",
		"--output",
		"out",
		"--name",
		"py_out",
		"--build",
		"--project-key",
		"TEST",
	];

	it("creates the recipe, then builds its output recursively and reports the job", async () => {
		const requests: string[] = [];
		await withCliServer(serveCreateAndBuild("DONE", requests,), async (url,) => {
			const { stdout, } = await dss(createArgs, { env: cliEnv(url,), },);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result,).toMatchObject({
				created: "py_out",
				build: { jobId: "JOB1", state: "DONE", success: true, },
			},);
			expect(result,).not.toHaveProperty("next",);
		},);
		expect(requests.indexOf("build RECURSIVE_BUILD",),).toBeGreaterThan(
			requests.indexOf("POST /public/api/projects/TEST/recipes/",),
		);
	});

	it("exits 4 with the created recipe and the failed job when the build fails", async () => {
		await withCliServer(serveCreateAndBuild("FAILED", [],), async (url,) => {
			const failure = await dssFailure(createArgs, { env: cliEnv(url,), },);
			expect(failure.code,).toBe(4,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				code: "long_running_failure",
				details: {
					result: { created: "py_out", build: { jobId: "JOB1", state: "FAILED", success: false, }, },
				},
			},);
		},);
	});

	it("reports a build DSS refuses to start with the command that retries it", async () => {
		await withCliServer(async (req, res,) => {
			await readBody(req,);
			if (req.method === "POST" && (req.url ?? "").includes("/recipes/",)) {
				sendJson(res, { name: "py_out", },);
				return;
			}
			sendJson(res, { message: "Dataset out cannot be built", }, 400,);
		}, async (url,) => {
			const failure = await dssFailure(createArgs, { env: cliEnv(url,), },);
			expect(failure.code,).toBe(4,);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				details: {
					result: {
						created: "py_out",
						build: { success: false, error: expect.stringContaining("cannot be built",), },
						next: [[
							"dss",
							"job",
							"build-and-wait",
							"out",
							"--build-mode",
							"RECURSIVE_BUILD",
							"--summary",
							"--project-key",
							"TEST",
						],],
					},
				},
			},);
		},);
	});

	it("rejects --build for a folder output and --timeout without --build before any request", async () => {
		const env = cliEnv("http://127.0.0.1:1",);
		const folder = await dssFailure([
			"recipe",
			"create",
			"--type",
			"python",
			"--output-folder",
			"F1",
			"--output-connection",
			"fs",
			"--build",
		], { env, },);
		expect(folder.code,).toBe(1,);
		expect(JSON.parse(folder.stdout,),).toMatchObject({
			error: expect.stringContaining("--build",),
		},);
		const timeout = await dssFailure(
			["recipe", "create", "--type", "sync", "--output", "out", "--timeout", "5",],
			{ env, },
		);
		expect(timeout.code,).toBe(1,);
	});
});
