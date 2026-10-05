import { describe, expect, it, } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

const NEW_SCHEMA = {
	columns: [
		{ name: "id", type: "bigint", originalType: "NUMBER", },
		{ name: "customer", type: "string", originalType: "VARCHAR", },
	],
	userModified: false,
};

interface Recorded {
	call: string;
	body: unknown;
}

/** Mock DSS: records writes, answers the create POST, detection POST, and schema PUT. */
function sqlDatasetServer(detection: { status: number; body: unknown; },) {
	const recorded: Recorded[] = [];
	const handler = async (
		req: Parameters<Parameters<typeof withCliServer>[0]>[0],
		res: Parameters<Parameters<typeof withCliServer>[0]>[1],
	): Promise<void> => {
		const pathname = new URL(req.url ?? "/", "http://localhost",).pathname;
		const text = await readBody(req,);
		recorded.push({
			call: `${req.method} ${pathname}`,
			body: text ? JSON.parse(text,) : undefined,
		},);
		if (pathname.endsWith("/actions/testAndDetectSettings/externalSQL",)) {
			sendJson(res, detection.body, detection.status,);
			return;
		}
		sendJson(res, {},);
	};
	return { recorded, handler, };
}

const DETECTED = {
	connectionOK: true,
	tablesListOK: true,
	queryOK: true,
	schemaDetection: { newSchema: NEW_SCHEMA, },
};

describe("dataset create over SQL tables and queries", () => {
	it("creates a table-mode dataset, detects the schema through DSS, and stores it", async () => {
		const { recorded, handler, } = sqlDatasetServer({ status: 200, body: DETECTED, },);
		await withCliServer(handler, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"create",
				"--name",
				"sales",
				"--type",
				"Snowflake",
				"--connection",
				"sf",
				"--table",
				"SALES",
				"--schema",
				"PUBLIC",
				"--catalog",
				"DB",
			], { env: cliEnv(url,), },);

			expect(recorded.map((entry,) => entry.call),).toEqual([
				"POST /public/api/projects/TEST/datasets/",
				"POST /public/api/projects/TEST/datasets/sales/actions/testAndDetectSettings/externalSQL",
				"PUT /public/api/projects/TEST/datasets/sales/schema",
			],);
			expect(recorded[0]!.body,).toEqual({
				projectKey: "TEST",
				name: "sales",
				type: "Snowflake",
				params: { connection: "sf", mode: "table", table: "SALES", schema: "PUBLIC", catalog: "DB", },
				managed: false,
			},);
			expect(recorded[2]!.body,).toEqual(NEW_SCHEMA,);
			expect(JSON.parse(stdout,),).toEqual({
				created: "sales",
				resource: "dataset",
				schemaDetected: true,
				columns: [{ name: "id", type: "bigint", }, { name: "customer", type: "string", },],
			},);
			expect(stderr.trim(),).toBe("",);
		},);
	});

	it("creates a query-mode dataset from --query-file on any SQL type", async () => {
		const { recorded, handler, } = sqlDatasetServer({ status: 200, body: DETECTED, },);
		const dir = mkdtempSync(join(tmpdir(), "dss-sql-query-",),);
		try {
			const file = join(dir, "orders.sql",);
			writeFileSync(file, "\uFEFFSELECT id, customer\nFROM orders\nWHERE open\n",);
			await withCliServer(handler, async (url,) => {
				const { stdout, } = await dss([
					"dataset",
					"create",
					"--name",
					"open_orders",
					"--type",
					"PostgreSQL",
					"--connection",
					"pg",
					"--query-file",
					file,
				], { env: cliEnv(url,), },);

				expect(recorded[0]!.call,).toBe("POST /public/api/projects/TEST/datasets/",);
				expect(recorded[0]!.body,).toEqual({
					projectKey: "TEST",
					name: "open_orders",
					type: "PostgreSQL",
					params: {
						connection: "pg",
						mode: "query",
						query: "SELECT id, customer\nFROM orders\nWHERE open\n",
					},
					managed: false,
				},);
				expect(recorded.map((entry,) => entry.call).slice(1,),).toEqual([
					"POST /public/api/projects/TEST/datasets/open_orders/actions/testAndDetectSettings/externalSQL",
					"PUT /public/api/projects/TEST/datasets/open_orders/schema",
				],);
				expect(JSON.parse(stdout,),).toMatchObject({ schemaDetected: true, },);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("keeps the dataset and warns when DSS cannot detect the schema", async () => {
		const { recorded, handler, } = sqlDatasetServer({
			status: 200,
			body: {
				connectionOK: false,
				connectionError: { message: "Invalid settings", detailedMessage: "warehouse is suspended", },
			},
		},);
		await withCliServer(handler, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"create",
				"--name",
				"sales",
				"--type",
				"Snowflake",
				"--connection",
				"sf",
				"--query",
				"SELECT 1 AS a",
			], { env: cliEnv(url,), },);

			expect(recorded.map((entry,) => entry.call),).toEqual([
				"POST /public/api/projects/TEST/datasets/",
				"POST /public/api/projects/TEST/datasets/sales/actions/testAndDetectSettings/externalSQL",
			],);
			expect(JSON.parse(stdout,),).toEqual({
				created: "sales",
				resource: "dataset",
				schemaDetected: false,
				columns: [],
				schemaError: "warehouse is suspended",
			},);
			const event = JSON.parse(stderr.trim(),) as { warnings: Array<Record<string, unknown>>; };
			const warning = event.warnings[0]!;
			expect(warning["code"],).toBe("dataset_schema_not_detected",);
			expect(warning["dataset"],).toBe("sales",);
			expect(warning["error"],).toBe("warehouse is suspended",);
		},);
	});

	it("treats a rejected detection request like a failed detection", async () => {
		const { recorded, handler, } = sqlDatasetServer({
			status: 400,
			body: { message: "Connection 'sf' does not exist", },
		},);
		await withCliServer(handler, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"create",
				"--name",
				"sales",
				"--type",
				"Snowflake",
				"--connection",
				"sf",
				"--table",
				"SALES",
			], { env: cliEnv(url,), },);
			expect(
				recorded.map((entry,) => entry.call).includes(
					"DELETE /public/api/projects/TEST/datasets/sales",
				),
			)
				.toBe(false,);
			const result = JSON.parse(stdout,) as Record<string, unknown>;
			expect(result["created"],).toBe("sales",);
			expect(result["schemaDetected"],).toBe(false,);
			expect(String(result["schemaError"],),).toContain("Connection 'sf' does not exist",);
		},);
	});

	it("--dry-run shows the exact request body and sends nothing", async () => {
		const { recorded, handler, } = sqlDatasetServer({ status: 200, body: DETECTED, },);
		await withCliServer(async (req, res,) => {
			if (req.method === "GET") {
				sendJson(res, [],);
				return;
			}
			await handler(req, res,);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"create",
				"--name",
				"sales",
				"--type",
				"Snowflake",
				"--connection",
				"sf",
				"--table",
				"SALES",
				"--schema",
				"PUBLIC",
				"--dry-run",
			], { env: cliEnv(url,), },);
			const output = JSON.parse(stdout,) as Record<string, unknown>;
			expect(output["dryRun"],).toBe(true,);
			expect(output["request"],).toEqual({
				method: "POST",
				endpoint: "/public/api/projects/TEST/datasets/",
				body: {
					projectKey: "TEST",
					name: "sales",
					type: "Snowflake",
					params: { connection: "sf", mode: "table", table: "SALES", schema: "PUBLIC", },
					managed: false,
				},
			},);
			expect(recorded,).toEqual([],);
		},);
	});

	describe("rejects invalid flag combinations before contacting DSS", () => {
		const base = ["dataset", "create", "--name", "x", "--connection", "c",];
		const cases: Array<{ title: string; args: string[]; code: string; message: string; }> = [
			{
				title: "--table with --query",
				args: ["--type", "Snowflake", "--table", "T", "--query", "SELECT 1",],
				code: "conflicting_input_sources",
				message: "either a table or a query",
			},
			{
				title: "--query with --query-file",
				args: ["--type", "Snowflake", "--query", "SELECT 1", "--query-file", "q.sql",],
				code: "conflicting_input_sources",
				message: "--query, --query-file",
			},
			{
				title: "--table on a non-SQL type",
				args: ["--type", "Filesystem", "--table", "T",],
				code: "validation_failed",
				message: "only applies to SQL dataset types",
			},
			{
				title: "--query on a non-SQL type",
				args: ["--type", "S3", "--query", "SELECT 1",],
				code: "validation_failed",
				message: "only applies to SQL dataset types",
			},
			{
				title: "--schema without --table",
				args: ["--type", "Snowflake", "--schema", "PUBLIC",],
				code: "validation_failed",
				message: "only applies together with a table",
			},
			{
				title: "--catalog with --query",
				args: ["--type", "Snowflake", "--query", "SELECT 1", "--catalog", "DB",],
				code: "validation_failed",
				message: "only applies together with a table",
			},
			{
				title: "a blank --query",
				args: ["--type", "Snowflake", "--query", "   ",],
				code: "invalid_flag_value",
				message: "non-empty SQL query",
			},
			{
				title: "an unreadable --query-file",
				args: ["--type", "Snowflake", "--query-file", "/nonexistent/dir/q.sql",],
				code: "validation_failed",
				message: "Could not read --query-file file",
			},
		];
		for (const { title, args, code, message, } of cases) {
			it(title, async () => {
				const failure = await dssFailure([...base, ...args,], {
					env: cliEnv("http://127.0.0.1:1",),
				},);
				const report = JSON.parse(failure.stdout,) as {
					code: string;
					error: string;
					exitCode: number;
				};
				expect(report.code,).toBe(code,);
				expect(report.exitCode,).toBe(1,);
				expect(report.error,).toContain(message,);
			},);
		}
	});
});
