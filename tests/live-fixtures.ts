/**
 * Core live fixtures: deterministic uploaded datasets (nulls, duplicates,
 * Unicode, CSV quoting), managed datasets, a managed folder, and the visual +
 * code recipe graph (sync / prepare / join / fuzzyjoin / group / python).
 *
 * `provisionCoreFixtures` runs once during the setup phase and registers every
 * baseline id in `ctx.fixtures`; `exerciseCoreFixtures` runs during run phase,
 * never re-provisions the baseline, and keeps every case-owned mutation inside
 * a try/finally so repeated runs are idempotent.
 */
import { expect, } from "bun:test";
import { LIVE_CSV_FORMAT, type LiveContext, } from "./live-context.js";

// ---------------------------------------------------------------------------
// Deterministic fixture data
// ---------------------------------------------------------------------------

/** CSV rows with explicit quoting; empty trailing fields are nulls. */
const CORE_CSV: Record<string, string> = {
	customers: [
		"id,name,city,signup_note",
		"C1,Alice,Paris,early",
		"C2,Bob,Lyon,",
		'C3,Chloé,"Paris, France",unicode-é→北京',
		'C4,北京 Man,Beijing,"quotes ""nested"" ok"',
		'C5,"Eve ""Q""",,formula-sigil-below',
		"C1,Alice,Paris,early",
		"",
	].join("\n",),
	products: [
		"product_id,product_name,price",
		"P1,Widget,9.99",
		"P2,Gadget,19.5",
		"P3,滑板,45.0",
		"P3b,,",
		"P1,Widget,9.99",
		"",
	].join("\n",),
	orders: [
		"order_id,customer_id,city,order_ts",
		"O1,C1,Paris,2026-01-15T10:00:00.000Z",
		"O2,C2,Lyon,2026-01-16T11:30:00.000Z",
		"O3,C4,Beijing,2026-02-01T08:15:00.000Z",
		"O4,C5,,2026-02-10T09:45:00.000Z",
		"",
	].join("\n",),
	orderlines: [
		"order_id,product_id,qty,unit_price",
		"O1,P1,2,3.0",
		"O1,P2,1,19.5",
		"O2,P1,3,3.0",
		"O3,P3,2,45.0",
		"O3,P2,1,",
		"O4,P1,5,3.0",
		"O2,P2,2,19.5",
		"",
	].join("\n",),
	events: [
		"event_id,event_ts,payload",
		'E1,2026-03-01T00:00:00.000Z,"=1+1"',
		'E2,2026-03-02T00:00:00.000Z,"has, comma"',
		'E3,2026-03-03T00:00:00.000Z,"quote "" inside"',
		'E4,2026-03-04T00:00:00.000Z,"line1\nline2"',
		"E5,2026-03-05T00:00:00.000Z,Chloé→北京",
		"",
	].join("\n",),
	// ML peer contract: numeric features + binary bigint target churn_flag, no nulls.
	synthetic_ml_train: [
		"feature_a,feature_b,churn_flag",
		"1.5,10.0,0",
		"2.5,20.0,1",
		"3.5,30.0,0",
		"4.5,40.0,1",
		"5.5,50.0,0",
		"6.5,60.0,1",
		"6.6,70.0,0",
		"0.5,80.0,1",
		"",
	].join("\n",),
};

const CORE_COLUMNS: Record<string, Array<{ name: string; type: string; }>> = {
	customers: [
		{ name: "id", type: "string", },
		{ name: "name", type: "string", },
		{ name: "city", type: "string", },
		{ name: "signup_note", type: "string", },
	],
	products: [
		{ name: "product_id", type: "string", },
		{ name: "product_name", type: "string", },
		{ name: "price", type: "double", },
	],
	orders: [
		{ name: "order_id", type: "string", },
		{ name: "customer_id", type: "string", },
		{ name: "city", type: "string", },
		{ name: "order_ts", type: "string", },
	],
	orderlines: [
		{ name: "order_id", type: "string", },
		{ name: "product_id", type: "string", },
		{ name: "qty", type: "bigint", },
		{ name: "unit_price", type: "double", },
	],
	events: [
		{ name: "event_id", type: "string", },
		{ name: "event_ts", type: "string", },
		{ name: "payload", type: "string", },
	],
	synthetic_ml_train: [
		{ name: "feature_a", type: "double", },
		{ name: "feature_b", type: "double", },
		{ name: "churn_flag", type: "bigint", },
	],
};

/** Row counts of the uploaded inputs, asserted on every run. */
const CORE_EXPECTED_ROWS: Record<string, number> = {
	customers: 6,
	products: 5,
	orders: 4,
	orderlines: 7,
	events: 5,
	synthetic_ml_train: 8,
};

/** Recipe graph created at setup; outputs are rebuilt (forced) on every run. */
const CORE_RECIPE_GRAPH: ReadonlyArray<{
	name: string;
	type: string;
	input: string;
	output: keyof typeof OUTPUT_EXPECTED_ROWS;
}> = [
	{ name: "sync_orders", type: "sync", input: "orders", output: "orders_managed", },
	{
		name: "prepare_orderlines",
		type: "shaker",
		input: "orderlines",
		output: "orderlines_prepared",
	},
	{ name: "join_orders_customers", type: "join", input: "orders", output: "orders_joined", },
	{
		name: "fuzzyjoin_orders_customers",
		type: "fuzzyjoin",
		input: "orders",
		output: "orders_fuzzy",
	},
	{ name: "group_orders", type: "grouping", input: "orders", output: "orders_grouped", },
	{ name: "python_enrich_orders", type: "python", input: "orders", output: "orders_enriched", },
];

const UPLOAD_DATASETS = [
	"customers",
	"products",
	"orders",
	"orderlines",
	"events",
	"synthetic_ml_train",
] as const;

const MANAGED_RECIPE_OUTPUTS = [
	"orders_managed",
	"orders_joined",
	"orders_fuzzy",
	"orderlines_prepared",
	"orders_grouped",
	"orders_enriched",
] as const;

/** Expected persisted rows of the recipe outputs (deterministic data above). */
const OUTPUT_EXPECTED_ROWS = {
	orders_managed: 4, // sync copy of orders
	orders_joined: 5, // INNER join customer_id: O1×2 (duplicate C1 rows) + O2 + O3 + O4
	orders_fuzzy: 5, // fuzzy city match, threshold 0.2: O1×3 (Paris rows) + O2 + O3
	orderlines_prepared: 7, // prepare adds line_total, keeps every row
	orders_grouped: 4, // one group per distinct orders.customer_id
	orders_enriched: 4, // python copy of orders + marker column
};

const PYTHON_ENRICH_CODE = [
	"import dataiku",
	"",
	'orders = dataiku.Dataset("orders")',
	'orders_enriched = dataiku.Dataset("orders_enriched")',
	"df = orders.get_dataframe()",
	'df["enriched_marker"] = "live"',
	"orders_enriched.write_with_schema(df)",
	"",
].join("\n",);

const PREPARE_SCRIPT = JSON.stringify({
	steps: [{
		metaType: "PROCESSOR",
		type: "CreateColumnWithGREL",
		params: { expression: "qty * unit_price", column: "line_total", },
		disabled: false,
		preview: false,
	},],
},);

const GROUP_SCRIPT = JSON.stringify({
	"keys": [{ "column": "customer_id", },],
	"values": [{
		"count": true,
		"avg": false,
		"max": false,
		"min": false,
		"countDistinct": false,
		"sum": false,
		"concat": false,
		"concatDistinct": false,
		"stddev": false,
		"median": false,
		"sum2": false,
		"first": false,
		"last": false,
		"firstLastNotNull": false,
		"column": "order_id",
	},],
	"computedColumns": [],
	"globalCount": false,
	"selectAllColumns": false,
	"enlargeYourBits": true,
	"preFilter": { "distinct": false, "enabled": false, },
	"postFilter": { "distinct": false, "enabled": false, },
	"engineParams": {},
},);

const FOLDER_MARKER = "dss_sdk_live_core_baseline\n";

// ---------------------------------------------------------------------------
// Provisioning (setup phase)
// ---------------------------------------------------------------------------

export async function provisionCoreFixtures(ctx: LiveContext,): Promise<void> {
	// Run phase must reuse the baseline; never rebuild it here.
	if (ctx.fixtures.datasets["customers"]) return;

	// UploadedFiles inputs with deterministic CSV bytes and explicit schemas.
	for (const name of UPLOAD_DATASETS) {
		const fileName = `${name}.csv`;
		const localPath = await ctx.writeFile(`core/${fileName}`, CORE_CSV[name]!,);
		await ctx.run(["dataset", "create", "--name", name, "--type", "UploadedFiles",],);
		await ctx.run(["dataset", "upload-file", name, localPath, "--file-name", fileName,],);
		const schemaPath = await ctx.writeFile(
			`core/${name}.schema.json`,
			JSON.stringify({ columns: CORE_COLUMNS[name], }, null, 2,),
		);
		await ctx.run(["dataset", "refresh-schema", name, "--data-file", schemaPath,],);
		await ctx.run(["dataset", "update", name, "--data", JSON.stringify(LIVE_CSV_FORMAT,),],);
		ctx.fixtures.datasets[name] = name;
	}
	Object.assign(ctx.fixtures.expectedRows, CORE_EXPECTED_ROWS,);

	// DSS derives the dataset type and storage location from the managed connection.
	for (const output of MANAGED_RECIPE_OUTPUTS) {
		await ctx.createManagedDataset(output,);
		ctx.fixtures.datasets[output] = output;
	}
	for (const [output, rows,] of Object.entries(OUTPUT_EXPECTED_ROWS,)) {
		ctx.fixtures.expectedRows[output] = rows;
	}

	// Managed folder baseline + a marker file.
	await ctx.run(["folder", "create", "--name", "dss_sdk_live_core_folder",],);
	const folderList = await ctx.run<Array<{ id: string; name?: string; }>>(["folder", "list",],);
	const folder = folderList.find((entry,) => entry.name === "dss_sdk_live_core_folder");
	if (!folder) throw new Error("Core folder was not created in the run project",);
	ctx.fixtures.folderId = folder.id;
	const markerPath = await ctx.writeFile("core/baseline-readme.txt", FOLDER_MARKER,);
	await ctx.run(["folder", "upload", folder.id, "/baseline/readme.txt", markerPath,],);

	// Recipe graph. Output datasets already exist, so no output provisioning.
	for (const recipe of CORE_RECIPE_GRAPH) {
		const joinFlags = recipe.type === "join"
			? ["--join-on", "customer_id=id", "--join-type", "INNER",]
			: [];
		const fuzzyFlags = recipe.type === "fuzzyjoin"
			? [
				"--fuzzy-on",
				"city",
				"--fuzzy-distance",
				"DAMERAU_LEVENSHTEIN",
				"--fuzzy-threshold",
				"0.2",
				"--normalize",
			]
			: [];
		await ctx.run([
			"recipe",
			"create",
			"--type",
			recipe.type,
			"--name",
			recipe.name,
			"--input",
			recipe.input,
			...(recipe.type === "join" || recipe.type === "fuzzyjoin" ? ["--input", "customers",] : []),
			"--output",
			recipe.output,
			...joinFlags,
			...fuzzyFlags,
		],);
		ctx.fixtures.recipes[recipe.name] = recipe.type;
	}

	const prepareScriptPath = await ctx.writeFile("core/prepare-script.json", PREPARE_SCRIPT,);
	await ctx.run([
		"recipe",
		"set-payload",
		"prepare_orderlines",
		"--file",
		prepareScriptPath,
		"--no-backup",
	],);
	const groupScriptPath = await ctx.writeFile("core/group-script.json", GROUP_SCRIPT,);
	await ctx.run([
		"recipe",
		"set-payload",
		"group_orders",
		"--file",
		groupScriptPath,
		"--no-backup",
	],);
	const pythonPath = await ctx.writeFile("core/enrich_orders.py", PYTHON_ENRICH_CODE,);
	await ctx.run([
		"recipe",
		"set-payload",
		"python_enrich_orders",
		"--file",
		pythonPath,
		"--no-backup",
	],);

	for (const recipe of CORE_RECIPE_GRAPH) {
		if (recipe.type !== "python") await ctx.propagateRecipeSchema(recipe.name,);
	}
	await ctx.save();
}

// ---------------------------------------------------------------------------
// Exercise (run phase, repeatable)
// ---------------------------------------------------------------------------

function schemaFileColumns(
	columns: Array<{ name: string; type: string; }>,
): { columns: Array<{ name: string; type: string; }>; userModified: boolean; } {
	return { columns, userModified: true, };
}

export async function exerciseCoreFixtures(ctx: LiveContext,): Promise<void> {
	if (ctx.phase === "setup") return;
	const folderId = ctx.fixtures.folderId;
	if (!folderId) throw new Error("Core fixtures were not provisioned (missing folderId)",);

	// --- Baseline read-only assertions over the provisioned datasets. --------
	await ctx.check("core.dataset.baseline", [
		"dataset.list",
		"dataset.get",
		"dataset.schema",
		"dataset.preview",
		"dataset.assert-count",
		"dataset.assert-schema",
		"dataset.source",
		"dataset.files",
		"dataset.metadata",
		"dataset.download",
	], async () => {
		const list = await ctx.run<Array<{ name: string; type?: string; managed?: boolean; }>>([
			"dataset",
			"list",
		],);
		const listed = new Set(list.map((entry,) => entry.name),);
		for (const name of [...UPLOAD_DATASETS, ...MANAGED_RECIPE_OUTPUTS,]) {
			expect(listed.has(name,),).toBe(true,);
		}

		const customers = await ctx.run<Record<string, unknown>>(["dataset", "get", "customers",],);
		expect(customers,).toMatchObject({ name: "customers", type: "UploadedFiles", },);

		const schema = await ctx.run<{ columns: Array<{ name: string; type: string; }>; }>([
			"dataset",
			"schema",
			"customers",
		],);
		expect(schema.columns.map((column,) => column.name),).toEqual([
			"id",
			"name",
			"city",
			"signup_note",
		],);

		const preview = await ctx.run<{
			columns: Array<{ name: string; }>;
			rows: string[][];
			rowCount: number;
			truncated: boolean;
		}>(["dataset", "preview", "customers", "--max-rows", "100",],);
		expect(preview.rowCount,).toBe(CORE_EXPECTED_ROWS.customers,);
		expect(preview.truncated,).toBe(false,);
		const names = preview.rows.map((row,) => row[1]);
		// Duplicates and Unicode survive a full round trip through DSS.
		expect(names.filter((name,) => name === "Alice"),).toHaveLength(2,);
		expect(names,).toContain("Chloé",);
		expect(names,).toContain("北京 Man",);

		const count = await ctx.run<{ satisfied: boolean; count: number; exact: boolean; }>([
			"dataset",
			"assert-count",
			"customers",
			"--expected",
			String(CORE_EXPECTED_ROWS.customers,),
		],);
		expect(count.satisfied,).toBe(true,);
		expect(count.exact,).toBe(true,);

		const schemaExpectationPath = await ctx.writeFile(
			"core/customers.expected-schema.json",
			JSON.stringify(schemaFileColumns(CORE_COLUMNS.customers,),),
		);
		const schemaAssert = await ctx.run<{ satisfied: boolean; totalDifferences: number; }>([
			"dataset",
			"assert-schema",
			"customers",
			"--data-file",
			schemaExpectationPath,
		],);
		expect(schemaAssert.satisfied,).toBe(true,);
		expect(schemaAssert.totalDifferences,).toBe(0,);

		const source = await ctx.run<Record<string, unknown>>([
			"dataset",
			"source",
			"synthetic_ml_train",
		],);
		expect(source,).toMatchObject({ name: "synthetic_ml_train", },);

		const files = await ctx.run<Array<{ filename: string; length?: number; }>>([
			"dataset",
			"files",
			"customers",
		],);
		expect(files,).toHaveLength(1,);
		expect(files[0]?.filename,).toBe("customers.csv",);
		expect(files[0]?.length,).toBeGreaterThan(0,);

		const metadata = await ctx.run<Record<string, unknown>>(["dataset", "metadata", "customers",],);
		expect(metadata,).toBeDefined();

		// Download: spreadsheet-safe by default, exact bytes with --raw-data.
		const safePath = await ctx.writeFile("downloads/events-safe.csv", "",);
		const safe = await ctx.run<{ path: string; rows: number; truncated: boolean; }>([
			"dataset",
			"download",
			"events",
			"--output",
			safePath,
		],);
		expect(safe.truncated,).toBe(false,);
		expect(safe.rows,).toBe(CORE_EXPECTED_ROWS.events,);
		const safeText = await Bun.file(safe.path,).text();
		expect(safeText,).toContain("'=1+1",); // formula sigil neutralized
		expect(safeText,).toContain('"has, comma"',); // CSV quoting preserved
		expect(safeText,).toContain("Chloé→北京",); // Unicode preserved

		const rawPath = await ctx.writeFile("downloads/events-raw.csv", "",);
		const raw = await ctx.run<{ path: string; rows: number; }>([
			"dataset",
			"download",
			"events",
			"--raw-data",
			"--output",
			rawPath,
		],);
		expect(raw.path,).toBe(rawPath,);
		const rawText = await Bun.file(rawPath,).text();
		expect(rawText,).toContain(",=1+1\n",); // formula value is not escaped in raw mode

		// An unsatisfied assertion exits 4 with the assertion_failed envelope.
		const mismatch = await ctx.run<Record<string, unknown>>(
			["dataset", "assert-count", "customers", "--expected", "999",],
			{ expectedExit: 4, },
		);
		expect(mismatch,).toMatchObject({
			code: "assertion_failed",
			category: "dss",
			exitCode: 4,
		},);
	},);

	// --- Case-owned dataset lifecycle: clone, update, clear, delete. ---------
	await ctx.check("core.dataset.lifecycle", [
		"dataset.create",
		"dataset.upload-file",
		"dataset.clone",
		"dataset.update",
		"dataset.get",
		"dataset.clear",
		"dataset.assert-count",
		"dataset.delete",
	], async () => {
		const iter = ctx.iteration;
		const cloneName = `orders_clone_${iter}`;
		const uploadName = `uploads_case_${iter}`;
		try {
			await ctx.run([
				"dataset",
				"clone",
				"orders_managed",
				cloneName,
				"--path",
				`/dataiku/${ctx.projectKey}/${cloneName}`,
			],);
			const clone = await ctx.run<{ managed?: boolean; name?: string; }>([
				"dataset",
				"get",
				cloneName,
			],);
			expect(clone,).toMatchObject({ name: cloneName, managed: true, },);

			await ctx.run([
				"dataset",
				"update",
				cloneName,
				"--data",
				JSON.stringify({ tags: ["live-exercise",], },),
			],);
			const tagged = await ctx.run<{ tags?: string[]; }>(["dataset", "get", cloneName,],);
			expect(tagged.tags,).toContain("live-exercise",);

			await ctx.run(["dataset", "create", "--name", uploadName, "--type", "UploadedFiles",],);
			const uploadPath = await ctx.writeFile(`core/case-${iter}.csv`, "a,b\n1,2\n",);
			await ctx.run([
				"dataset",
				"upload-file",
				uploadName,
				uploadPath,
				"--file-name",
				`case-${iter}.csv`,
			],);
			const caseFiles = await ctx.run<Array<{ filename: string; }>>([
				"dataset",
				"files",
				uploadName,
			],);
			expect(caseFiles.map((file,) => file.filename),).toEqual([`case-${iter}.csv`,],);
			await ctx.run(["dataset", "clear", uploadName,],);
			expect(await ctx.run<unknown[]>(["dataset", "files", uploadName,],),).toEqual([],);
		} finally {
			for (const name of [cloneName, uploadName,]) {
				await ctx.run(["dataset", "delete", name, "--if-exists",],);
			}
		}
	},);

	// --- Managed folder byte roundtrip. --------------------------------------
	await ctx.check("core.folder.roundtrip", [
		"folder.upload",
		"folder.contents",
		"folder.download",
		"folder.delete-file",
	], async () => {
		const iter = ctx.iteration;
		const remotePath = `/roundtrip/${iter}/payload.bin`;
		const payload = new Uint8Array([
			...new TextEncoder().encode("Chloé→北京 ✓ live-suite\n",),
			0x00,
			0x01,
			0xfe,
			0xff,
		],);
		const localPath = await ctx.writeFile(`roundtrip-${iter}.bin`, payload,);
		const downloadedPath = await ctx.writeFile(`roundtrip-${iter}.download.bin`, "",);
		try {
			await ctx.run(["folder", "upload", folderId, remotePath, localPath,],);

			const contents = await ctx.run<Array<{ path: string; size?: number; }>>([
				"folder",
				"contents",
				folderId,
			],);
			const entry = contents.find((item,) => item.path === remotePath);
			expect(entry,).toBeDefined();
			expect(entry?.size,).toBe(payload.byteLength,);

			const downloaded = await ctx.run<string>([
				"folder",
				"download",
				folderId,
				remotePath,
				downloadedPath,
			],);
			expect(downloaded,).toBe(downloadedPath,);
			const roundTrip = new Uint8Array(await Bun.file(downloadedPath,).arrayBuffer(),);
			expect(Buffer.compare(Buffer.from(roundTrip,), Buffer.from(payload,),),).toBe(0,);
		} finally {
			await ctx.run(["folder", "delete-file", folderId, remotePath,],);
		}
	},);

	// --- Recipe graph metadata and validation. -------------------------------
	await ctx.check(
		"core.recipe.graph",
		["recipe.list", "recipe.get", "recipe.validate-graph",],
		async () => {
			const recipes = await ctx.run<Array<{ name: string; type?: string; }>>(["recipe", "list",],);
			const listed = new Set(recipes.map((entry,) => entry.name),);
			for (const [name,] of Object.entries(ctx.fixtures.recipes,)) {
				expect(listed.has(name,),).toBe(true,);
			}
			for (const [name, type,] of Object.entries(ctx.fixtures.recipes,)) {
				const settings = await ctx.run<{ recipe: Record<string, unknown>; payload?: string; }>([
					"recipe",
					"get",
					name,
					"--no-payload",
				],);
				expect(settings.recipe,).toMatchObject({ name, type, },);
				const graph = await ctx.run<
					{ valid: boolean; missingInputs: unknown[]; missingOutputs: unknown[]; }
				>([
					"recipe",
					"validate-graph",
					name,
				],);
				expect(graph.valid,).toBe(true,);
				expect(graph.missingInputs,).toEqual([],);
				expect(graph.missingOutputs,).toEqual([],);
			}
		},
	);

	// --- Recipe runs with expected persisted outputs. ------------------------
	await ctx.check("core.recipe.runs", [
		"recipe.run",
		"dataset.assert-count",
		"dataset.preview",
		"dataset.schema",
	], async () => {
		for (const recipe of CORE_RECIPE_GRAPH) {
			const run = await ctx.run<
				{ recipeName: string; state?: string; jobId?: string; outputs: unknown[]; }
			>([
				"recipe",
				"run",
				recipe.name,
				"--wait",
				"--timeout",
				"300000",
				"--poll-interval",
				"3000",
			],);
			expect(run.recipeName,).toBe(recipe.name,);
			expect(run.state ?? "DONE",).toBe("DONE",);
			expect(Array.isArray(run.outputs,),).toBe(true,);

			const expected = OUTPUT_EXPECTED_ROWS[recipe.output];
			const count = await ctx.run<{ satisfied: boolean; count: number; exact: boolean; }>([
				"dataset",
				"assert-count",
				recipe.output,
				"--expected",
				String(expected,),
			],);
			expect(count.satisfied,).toBe(true,);
		}

		// Persisted output content, not just counts.
		const joined = await ctx.run<{ columns: Array<{ name: string; }>; rows: string[][]; }>([
			"dataset",
			"preview",
			"orders_joined",
			"--max-rows",
			"100",
		],);
		const orderIdIndex = joined.columns.findIndex((column,) => column.name === "order_id");
		expect(orderIdIndex,).toBeGreaterThanOrEqual(0,);
		const o1Rows = joined.rows.filter((row,) => row[orderIdIndex] === "O1");
		expect(o1Rows,).toHaveLength(2,);

		const prepared = await ctx.run<{ columns: Array<{ name: string; }>; rows: string[][]; }>([
			"dataset",
			"preview",
			"orderlines_prepared",
			"--max-rows",
			"100",
		],);
		expect(prepared.columns.map((column,) => column.name),).toContain("line_total",);

		const grouped = await ctx.run<
			{ columns: Array<{ name: string; }>; rows: string[][]; rowCount: number; }
		>([
			"dataset",
			"preview",
			"orders_grouped",
			"--max-rows",
			"100",
		],);
		expect(grouped.rowCount,).toBe(OUTPUT_EXPECTED_ROWS.orders_grouped,);

		const enriched = await ctx.run<{ columns: Array<{ name: string; }>; }>([
			"dataset",
			"schema",
			"orders_enriched",
		],);
		expect(enriched.columns.map((column,) => column.name),).toContain("enriched_marker",);
	},);

	// --- Case-owned recipe lifecycle (payload, graph edits, delete). ---------
	await ctx.check("core.recipe.lifecycle", [
		"recipe.create",
		"recipe.set-payload",
		"recipe.get-payload",
		"recipe.cat",
		"recipe.diff",
		"recipe.download-code",
		"recipe.download",
		"recipe.add-input",
		"recipe.remove-input",
		"recipe.delete",
	], async () => {
		const iter = ctx.iteration;
		const recipeName = `python_case_${iter}`;
		const outputName = `orders_case_${iter}`;
		const payload =
			`import dataiku\ndataiku.Dataset("${outputName}").write_with_schema(dataiku.Dataset("orders").get_dataframe())\n`;
		const codePath = await ctx.writeFile(`case-${iter}.py`, payload,);
		const codeOutPath = await ctx.writeFile(`case-${iter}.downloaded.py`, "",);
		const definitionPath = await ctx.writeFile(`case-${iter}.recipe.json`, "",);
		try {
			await ctx.createManagedDataset(outputName,);
			await ctx.run([
				"recipe",
				"create",
				"--type",
				"python",
				"--name",
				recipeName,
				"--input",
				"orders",
				"--output",
				outputName,
			],);
			await ctx.run(["recipe", "set-payload", recipeName, "--file", codePath, "--no-backup",],);

			const remotePayload = await ctx.run<string>(["recipe", "get-payload", recipeName,],);
			expect(remotePayload,).toBe(payload,);
			const catPayload = await ctx.run<string>(["recipe", "cat", recipeName,],);
			expect(catPayload,).toBe(payload,);

			const diff = await ctx.run<string>(["recipe", "diff", recipeName, "--file", codePath,],);
			expect(diff,).toBe("No differences.",);

			const downloadedCode = await ctx.run<string>([
				"recipe",
				"download-code",
				recipeName,
				"--output",
				codeOutPath,
			],);
			expect(downloadedCode,).toBe(codeOutPath,);
			expect(await Bun.file(codeOutPath,).text(),).toBe(payload,);

			const definitionOut = await ctx.run<string>([
				"recipe",
				"download",
				recipeName,
				"--output",
				definitionPath,
			],);
			expect(definitionOut,).toBe(definitionPath,);
			const definition = JSON.parse(await Bun.file(definitionPath,).text(),) as {
				recipe?: { name?: string; type?: string; };
			};
			expect(definition.recipe,).toMatchObject({ name: recipeName, type: "python", },);

			const added = await ctx.run<{ inputs: string[]; }>([
				"recipe",
				"add-input",
				recipeName,
				"customers",
			],);
			expect(added.inputs,).toContain("customers",);
			const removed = await ctx.run<{ inputs: string[]; }>([
				"recipe",
				"remove-input",
				recipeName,
				"customers",
			],);
			expect(removed.inputs,).not.toContain("customers",);
		} finally {
			try {
				await ctx.run(["recipe", "delete", recipeName, "--if-exists",],);
			} finally {
				await ctx.run(["dataset", "delete", outputName, "--if-exists",],);
			}
		}
	},);
}
