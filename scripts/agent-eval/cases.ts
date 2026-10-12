/**
 * Agent-eval cases: each is a self-contained DSS fixture (setup), the task
 * prompt given to the headless agent, and a verification of DSS state plus the
 * agent's final answer. Setup and verification go through `bin/dss` only.
 */
import * as fsp from "node:fs/promises";
import * as path from "node:path";

import type { SessionMetrics, } from "./session-log.js";

export interface DssResult {
	exitCode: number;
	stdout: string;
	stderr: string;
	/** Parsed stdout when it is JSON, else undefined. */
	json: unknown;
}

export interface CaseContext {
	projectKey: string;
	/** DSS login that owns created projects. */
	owner: string;
	/** Managed storage connection for created datasets. */
	connection: string;
	/** Scratch directory for fixture files (removed by the runner). */
	workDir: string;
	/** Run `bin/dss` with the trial credentials; throws on a non-zero exit unless `allowFailure`. */
	dss(
		args: string[],
		options?: { allowFailure?: boolean; timeoutMs?: number; },
	): Promise<DssResult>;
}

export interface RunOutcome {
	/** Final answer text (agent stdout). */
	answer: string;
	/** Parsed session log; undefined when no log was found. */
	metrics: SessionMetrics | undefined;
	/** dss invocations the agent made that mutate DSS (registry `mutatesDss`, dry-run/plan excluded). */
	mutatingCalls: string[][];
}

export interface Verdict {
	ok: boolean;
	reason: string;
}

export interface EvalCase {
	id: string;
	title: string;
	/** Read-only cases fail when the agent makes mutating dss calls. */
	readOnly: boolean;
	setup(ctx: CaseContext,): Promise<void>;
	prompt(ctx: CaseContext,): string;
	verify(ctx: CaseContext, outcome: RunOutcome,): Promise<Verdict>;
}

const CSV_FORMAT = JSON.stringify({
	formatType: "csv",
	formatParams: {
		style: "excel",
		charset: "utf8",
		separator: ",",
		quoteChar: '"',
		escapeChar: "\\",
		parseHeaderRow: true,
	},
},);

interface Column {
	name: string;
	type: "bigint" | "double" | "string";
}

const CUSTOMERS_CSV =
	"customer_id,name,country\n1,Alice,FR\n2,Bob,DE\n3,Chloe,FR\n4,Dan,US\n5,Eve,DE\n";
const CUSTOMERS_COLUMNS: Column[] = [
	{ name: "customer_id", type: "bigint", },
	{ name: "name", type: "string", },
	{ name: "country", type: "string", },
];
const ORDERS_CSV = "order_id,customer_id,amount,day\n"
	+ "100,1,12.5,2026-10-01\n101,2,40.0,2026-10-01\n102,1,7.25,2026-10-02\n103,3,99.9,2026-10-02\n"
	+ "104,4,-5.0,2026-10-03\n105,5,18.0,2026-10-03\n106,2,3.0,2026-10-04\n107,4,22.0,2026-10-04\n";
const ORDERS_COLUMNS: Column[] = [
	{ name: "order_id", type: "bigint", },
	{ name: "customer_id", type: "bigint", },
	{ name: "amount", type: "double", },
	{ name: "day", type: "string", },
];

async function createProject(ctx: CaseContext, name: string,): Promise<void> {
	await ctx.dss(["project", "create", ctx.projectKey, name, "--owner", ctx.owner,],);
}

/** Uploaded CSV dataset with an explicit schema and the CSV format (header row). */
async function createCsvDataset(
	ctx: CaseContext,
	name: string,
	csv: string,
	columns: Column[],
): Promise<void> {
	const project = ["--project-key", ctx.projectKey,];
	const csvPath = path.join(ctx.workDir, `${name}.csv`,);
	const schemaPath = path.join(ctx.workDir, `${name}.schema.json`,);
	await fsp.writeFile(csvPath, csv,);
	await fsp.writeFile(schemaPath, JSON.stringify({ columns, },),);
	await ctx.dss(["dataset", "create", "--name", name, "--type", "UploadedFiles", ...project,],);
	await ctx.dss([
		"dataset",
		"upload-file",
		name,
		csvPath,
		"--file-name",
		`${name}.csv`,
		...project,
	],);
	await ctx.dss(["dataset", "refresh-schema", name, "--data-file", schemaPath, ...project,],);
	await ctx.dss(["dataset", "update", name, "--data", CSV_FORMAT, ...project,],);
}

async function previewRows(
	ctx: CaseContext,
	dataset: string,
): Promise<{ columns: string[]; rows: string[][]; }> {
	const result = await ctx.dss(["dataset", "preview", dataset, "--project-key", ctx.projectKey,],);
	const json = result.json as
		| { columns?: Array<{ name?: string; }>; rows?: unknown[][]; }
		| undefined;
	return {
		columns: (json?.columns ?? []).map((column,) => String(column.name ?? "",)),
		rows: (json?.rows ?? []).map((row,) => row.map((cell,) => String(cell ?? "",))),
	};
}

/**
 * True when the text states `number` as a number of its own, compared by value
 * so "54.90", "54.9" and "1,000" vs "1000" all match (not a fragment of a longer number).
 */
export function mentionsNumber(text: string, number: string,): boolean {
	const target = Number(number,);
	// A number must not be glued to a word, dot, or dash (dates, run ids, versions).
	const found = text.replace(/,(?=\d{3}\b)/g, "",).match(/(?<![\w.-])-?\d+(?:\.\d+)?(?![\w.-]*\d)/g,)
		?? [];
	return found.some((candidate,) => Number(candidate,) === target);
}

// ---------------------------------------------------------------------------
// scenario-fix: failing nightly scenario caused by a wrong column name
// ---------------------------------------------------------------------------

const BROKEN_RECIPE = "import dataiku\n\n"
	+ 'df = dataiku.Dataset("orders_joined").get_dataframe()\n'
	+ "# revenue per country in EUR\n"
	+ 'out = df.groupby("country", as_index=False)["amount_euro"].sum()\n'
	+ 'out = out.rename(columns={"amount_euro": "revenue_eur"})\n'
	+ 'dataiku.Dataset("revenue_by_country").write_with_schema(out)\n';

const SCENARIO_STEPS = {
	params: {
		steps: [{
			id: "build",
			type: "build_flowitem",
			name: "Build revenue",
			runConditionType: "RUN_IF_STATUS_MATCH",
			runConditionStatuses: ["SUCCESS", "WARNING",],
			params: {
				builds: [{ type: "DATASET", itemId: "revenue_by_country", partitionsSpec: "", },],
				jobType: "RECURSIVE_FORCED_BUILD",
				refreshHiveMetastore: true,
				proceedOnFailure: false,
			},
		},],
	},
};

const scenarioFix: EvalCase = {
	id: "scenario-fix",
	title: "Diagnose and fix a failing scenario (wrong column name in a Python recipe)",
	readOnly: false,
	async setup(ctx,) {
		const project = ["--project-key", ctx.projectKey,];
		const managed = ["--output-connection", ctx.connection,];
		await createProject(ctx, "Agent eval: scenario-fix",);
		await createCsvDataset(ctx, "customers", CUSTOMERS_CSV, CUSTOMERS_COLUMNS,);
		await createCsvDataset(ctx, "orders", ORDERS_CSV, ORDERS_COLUMNS,);
		await ctx.dss([
			"recipe",
			"create",
			"--type",
			"prepare",
			"--input",
			"orders",
			"--output",
			"orders_prep",
			"--filter",
			"amount > 0",
			"--formula",
			"amount_eur=amount * 0.9",
			...managed,
			...project,
		],);
		await ctx.dss([
			"recipe",
			"create",
			"--type",
			"join",
			"--input",
			"orders_prep,customers",
			"--output",
			"orders_joined",
			"--join-on",
			"customer_id",
			"--join-type",
			"LEFT",
			...managed,
			...project,
		],);
		await ctx.dss([
			"recipe",
			"create",
			"--type",
			"python",
			"--input",
			"orders_joined",
			"--output",
			"revenue_by_country",
			...managed,
			...project,
		],);
		const recipePath = path.join(ctx.workDir, "revenue.py",);
		await fsp.writeFile(recipePath, BROKEN_RECIPE,);
		await ctx.dss([
			"recipe",
			"set-payload",
			"python_revenue_by_country",
			"--file",
			recipePath,
			"--no-backup",
			...project,
		],);
		await ctx.dss(["scenario", "create", "nightly", "Nightly revenue build", ...project,],);
		await ctx.dss([
			"scenario",
			"update",
			"nightly",
			"--data",
			JSON.stringify(SCENARIO_STEPS,),
			...project,
		],);
		await ctx.dss(["scenario", "run-and-wait", "nightly", "--timeout", "600000", ...project,], {
			allowFailure: true,
			timeoutMs: 660_000,
		},);
		const runs =
			(await ctx.dss(["scenario", "last-runs", "nightly", "--limit", "1", ...project,],)).json;
		const outcome = Array.isArray(runs,)
			? (runs[0] as { outcome?: string; } | undefined)?.outcome
			: undefined;
		if (outcome !== "FAILED") {
			throw new Error(`scenario-fix setup: expected a FAILED nightly run, got ${outcome}`,);
		}
	},
	prompt(ctx,) {
		return `Our Dataiku DSS project ${ctx.projectKey} has a scenario called 'nightly' that started failing. `
			+ "Find the root cause, fix it with the smallest sensible change, and re-run the scenario until it succeeds. "
			+ "Then tell me the revenue per country (EUR) from the dataset revenue_by_country. "
			+ "Use the `dss` command-line tool (already installed and authenticated). Do not delete anything.";
	},
	async verify(ctx, outcome,) {
		const project = ["--project-key", ctx.projectKey,];
		const runs =
			(await ctx.dss(["scenario", "last-runs", "nightly", "--limit", "1", ...project,],)).json;
		const last = Array.isArray(runs,)
			? (runs[0] as { outcome?: string; } | undefined)?.outcome
			: undefined;
		if (last !== "SUCCESS") {
			return { ok: false, reason: `last nightly run outcome is ${last ?? "missing"}`, };
		}
		const code = (await ctx.dss(["recipe", "cat", "python_revenue_by_country", ...project,], {
			allowFailure: true,
		},))
			.json;
		const source = typeof code === "string" ? code : "";
		if (!/amount_eur(?!o)/.test(source,)) {
			return { ok: false, reason: "recipe code does not reference amount_eur", };
		}
		if (/amount_euro/.test(source,)) {
			return { ok: false, reason: "recipe code still references amount_euro", };
		}
		const missing = ["54.9", "107.685", "19.8",].filter((value,) =>
			!mentionsNumber(outcome.answer, value,)
		);
		if (missing.length > 0) return { ok: false, reason: `answer is missing ${missing.join(", ",)}`, };
		return {
			ok: true,
			reason: "scenario SUCCESS, recipe uses amount_eur, answer has DE 54.9 / FR 107.685 / US 19.8",
		};
	},
};

// ---------------------------------------------------------------------------
// data-question: read-only analysis of two datasets
// ---------------------------------------------------------------------------

const dataQuestion: EvalCase = {
	id: "data-question",
	title: "Answer a data question from datasets without changing anything",
	readOnly: true,
	async setup(ctx,) {
		await createProject(ctx, "Agent eval: data-question",);
		await createCsvDataset(ctx, "customers", CUSTOMERS_CSV, CUSTOMERS_COLUMNS,);
		await createCsvDataset(ctx, "orders", ORDERS_CSV, ORDERS_COLUMNS,);
	},
	prompt(ctx,) {
		return `In the Dataiku DSS project ${ctx.projectKey}, the datasets customers (customer_id, name, country) `
			+ "and orders (order_id, customer_id, amount, day) hold our sales. "
			+ "Which country has the highest total order amount, what is that total, "
			+ "and how many orders have an amount over 20? "
			+ "This is a read-only question: do not create, modify, build, run or delete anything in DSS. "
			+ "Use the `dss` command-line tool (already installed and authenticated).";
	},
	async verify(_ctx, outcome,) {
		const problems: string[] = [];
		if (!/\bFR\b|France/i.test(outcome.answer,)) problems.push("answer does not name FR",);
		if (!mentionsNumber(outcome.answer, "119.65",)) {
			problems.push("answer is missing the FR total 119.65",);
		}
		if (!mentionsNumber(outcome.answer, "3",)) {
			problems.push("answer is missing the count of orders over 20 (3)",);
		}
		if (outcome.mutatingCalls.length > 0) {
			problems.push(
				`agent made mutating dss calls: ${
					outcome.mutatingCalls.map((argv,) => argv.join(" ",)).join(" | ",)
				}`,
			);
		}
		return problems.length === 0
			? { ok: true, reason: "answer has FR / 119.65 / 3 and no mutating dss calls", }
			: { ok: false, reason: problems.join("; ",), };
	},
};

// ---------------------------------------------------------------------------
// build-flow: create and build a join + group flow
// ---------------------------------------------------------------------------

const EXPECTED_REVENUE: Record<string, number> = { DE: 61, FR: 119.65, US: 17, };

const buildFlow: EvalCase = {
	id: "build-flow",
	title: "Create and build a join + aggregate flow producing revenue_by_country",
	readOnly: false,
	async setup(ctx,) {
		await createProject(ctx, "Agent eval: build-flow",);
		await createCsvDataset(ctx, "customers", CUSTOMERS_CSV, CUSTOMERS_COLUMNS,);
		await createCsvDataset(ctx, "orders", ORDERS_CSV, ORDERS_COLUMNS,);
	},
	prompt(ctx,) {
		return `In the Dataiku DSS project ${ctx.projectKey} the datasets customers (customer_id, name, country) `
			+ "and orders (order_id, customer_id, amount, day) already exist. "
			+ "Create a flow that produces a new dataset named revenue_by_country with one row per customer country "
			+ "and the sum of the order amount for that country (join orders to customers on customer_id, "
			+ `then group by country), store it on the managed connection ${ctx.connection}, and build it. `
			+ "Do not modify or delete the existing datasets. Then tell me the resulting rows. "
			+ "Use the `dss` command-line tool (already installed and authenticated).";
	},
	async verify(ctx,) {
		let preview: { columns: string[]; rows: string[][]; };
		try {
			preview = await previewRows(ctx, "revenue_by_country",);
		} catch (error) {
			return {
				ok: false,
				reason: `revenue_by_country cannot be read: ${(error as Error).message.slice(0, 200,)}`,
			};
		}
		const countryIndex = preview.columns.indexOf("country",);
		if (countryIndex === -1) {
			return { ok: false, reason: `no country column (columns: ${preview.columns.join(",",)})`, };
		}
		const expectedCountries = Object.keys(EXPECTED_REVENUE,);
		const countries = preview.rows.map((row,) => row[countryIndex] ?? "");
		if (
			preview.rows.length !== expectedCountries.length
			|| expectedCountries.some((c,) => !countries.includes(c,))
		) {
			return {
				ok: false,
				reason: `expected one row for each of ${expectedCountries.join(",",)}, got ${
					countries.join(",",)
				}`,
			};
		}
		const matching = preview.columns.filter((_, index,) =>
			index !== countryIndex
			&& preview.rows.every((row,) => {
				const expected = EXPECTED_REVENUE[row[countryIndex] ?? ""];
				return expected !== undefined && Math.abs(Number(row[index],) - expected,) < 0.01;
			},)
		);
		return matching.length > 0
			? {
				ok: true,
				reason: `revenue_by_country rows match (${matching[0]}: DE 61, FR 119.65, US 17)`,
			}
			: { ok: false, reason: "no column holds the expected sums DE 61 / FR 119.65 / US 17", };
	},
};

export const EVAL_CASES: EvalCase[] = [scenarioFix, dataQuestion, buildFlow,];
