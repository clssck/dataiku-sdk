import { describe, expect, it, } from "bun:test";
import type { IncomingMessage, ServerResponse, } from "node:http";
import {
	cliEnv,
	dss,
	dssFailure,
	dssWithInput,
	join,
	mkdirSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

const TSV_CONTENT_TYPE = "text/tab-separated-values; charset=utf-8";

function tsvResponse(res: ServerResponse, lines: string[],): void {
	res.statusCode = 200;
	res.setHeader("Content-Type", TSV_CONTENT_TYPE,);
	res.end(lines.join("\n",),);
}

describe("dataset assertions", () => {
	describe("dataset preview truncation", () => {
		it("probes one row past the cap and reports truncated with a warning", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				expect(req.method,).toBe("GET",);
				const url = new URL(req.url ?? "/", "http://localhost",);
				expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/data/",);
				requestedLimit = url.searchParams.get("limit",) ?? "";
				tsvResponse(res, [
					"name\tcity",
					"Alice\tParis",
					"Bob\tBerlin",
					"Carole\tLyon",
				],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"dataset",
					"preview",
					"orders",
					"--max-rows",
					"2",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("3",);
				expect(JSON.parse(stdout,),).toEqual({
					columns: [{ name: "name", }, { name: "city", },],
					rows: [["Alice", "Paris",], ["Bob", "Berlin",],],
					rowCount: 2,
					truncated: true,
					limit: 2,
				},);
				const warningEvent = JSON.parse(stderr.trim(),) as {
					warnings: Array<{ code: string; dataset: string; rows: number; limit: number; }>;
				};
				expect(warningEvent.warnings,).toHaveLength(1,);
				expect(warningEvent.warnings[0],).toMatchObject({
					code: "dataset_preview_truncated",
					dataset: "orders",
					rows: 2,
					limit: 2,
				},);
			},);
		});

		it("reports truncated false without a warning at exactly the cap", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
				tsvResponse(res, ["name\tcity", "Alice\tParis", "Bob\tBerlin",],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"dataset",
					"preview",
					"orders",
					"--max-rows",
					"2",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("3",);
				expect(JSON.parse(stdout,),).toMatchObject({
					rowCount: 2,
					truncated: false,
					limit: 2,
				},);
				expect(stderr,).toBe("",);
			},);
		});

		it("keeps the public maximum at 500 while probing one row past it", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
				const rows = Array.from({ length: 500, }, (_, i,) => `row${i}\tx${i}`,);
				tsvResponse(res, ["name\tcity", ...rows, "extra\trow",],);
			}, async (url,) => {
				const { stdout, } = await dss([
					"dataset",
					"preview",
					"orders",
					"--max-rows",
					"500",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("501",);
				const preview = JSON.parse(stdout,) as {
					rowCount: number;
					truncated: boolean;
					limit: number;
				};
				expect(preview,).toMatchObject({ rowCount: 500, truncated: true, limit: 500, },);
			},);
		});

		it("rejects nonsensical row limits before contacting DSS", async () => {
			for (const value of ["0", "-2", "1.5", "9007199254740992",]) {
				const failure = await dssFailure([
					"dataset",
					"preview",
					"orders",
					"--max-rows",
					value,
				], { env: cliEnv("http://127.0.0.1:1",), },);
				expect(failure.code,).toBe(1,);
				const report = JSON.parse(failure.stdout,) as { code: string; error: string; };
				expect(report.code,).toBe("validation_failed",);
				expect(report.error,).toContain("--max-rows must be a positive integer",);
			}
		});
	});

	it("rejects --max-rows above the public maximum before contacting DSS", async () => {
		for (const value of ["501", "1000",]) {
			const failure = await dssFailure([
				"dataset",
				"preview",
				"orders",
				"--max-rows",
				value,
			], { env: cliEnv("http://127.0.0.1:1",), },);
			expect(failure.code,).toBe(1,);
			const report = JSON.parse(failure.stdout,) as { code: string; error: string; };
			expect(report.code,).toBe("validation_failed",);
			expect(report.error,).toContain("--max-rows must be a positive integer no greater than 500",);
			expect(report.error,).toContain(value,);
		}
	});

	it("gives actionable guidance when a preview is truncated below the cap", async () => {
		let requestedLimit = "";
		await withCliServer((req, res,) => {
			requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
			tsvResponse(res, ["name\tcity", "Alice\tParis", "Bob\tBerlin", "Carole\tLyon",],);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"preview",
				"orders",
				"--max-rows",
				"2",
			], { env: cliEnv(url,), },);
			expect(requestedLimit,).toBe("3",);
			expect(JSON.parse(stdout,),).toMatchObject({ rowCount: 2, truncated: true, limit: 2, },);
			const warning = JSON.parse(stderr.trim(),) as { warnings: Array<{ message: string; }>; };
			expect(warning.warnings,).toHaveLength(1,);
			expect(warning.warnings[0].message,).toContain("Re-run with --max-rows N",);
		},);
	});

	it("points at dataset download when the public maximum is reached", async () => {
		let requestedLimit = "";
		await withCliServer((req, res,) => {
			requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
			const rows = Array.from({ length: 500, }, (_, i,) => `row${i}\tx${i}`,);
			tsvResponse(res, ["name\tcity", ...rows, "extra\trow",],);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"preview",
				"orders",
				"--max-rows",
				"500",
			], { env: cliEnv(url,), },);
			expect(requestedLimit,).toBe("501",);
			expect(JSON.parse(stdout,),).toMatchObject({ rowCount: 500, truncated: true, limit: 500, },);
			const warning = JSON.parse(stderr.trim(),) as { warnings: Array<{ message: string; }>; };
			expect(warning.warnings,).toHaveLength(1,);
			expect(warning.warnings[0].message,).toContain("dss dataset download",);
		},);
	});

	describe("dataset assert-count", () => {
		it("satisfies on an exact count", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
				tsvResponse(res, ["name", "r1", "r2", "r3",],);
			}, async (url,) => {
				const { stdout, stderr, } = await dss([
					"dataset",
					"assert-count",
					"orders",
					"--expected",
					"3",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("4",);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toEqual({
					expected: 3,
					count: 3,
					exact: true,
					satisfied: true,
					dataset: "orders",
				},);
			},);
		});

		it("fails on an under count with the exact observed count", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
				tsvResponse(res, ["name", "r1", "r2", "r3",],);
			}, async (url,) => {
				const failure = await dssFailure([
					"dataset",
					"assert-count",
					"orders",
					"--expected",
					"5",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("6",);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					code: string;
					exitCode: number;
					error: string;
					details: { result: Record<string, unknown>; };
				};
				expect(report,).toMatchObject({
					code: "assertion_failed",
					category: "dss",
					exitCode: 4,
					error: "Command completed with unsatisfied assertion result.",
				},);
				expect(report.details.result,).toEqual({
					expected: 5,
					count: 3,
					exact: true,
					satisfied: false,
					dataset: "orders",
				},);
			},);
		});

		it("fails on an over count with a lower bound", async () => {
			let requestedLimit = "";
			await withCliServer((req, res,) => {
				requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
				tsvResponse(res, ["name", "r1", "r2", "r3",],);
			}, async (url,) => {
				const failure = await dssFailure([
					"dataset",
					"assert-count",
					"orders",
					"--expected",
					"2",
				], { env: cliEnv(url,), },);
				expect(requestedLimit,).toBe("3",);
				expect(failure.code,).toBe(4,);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(report.details.result,).toEqual({
					expected: 2,
					count: 3,
					exact: false,
					satisfied: false,
					dataset: "orders",
				},);
			},);
		});

		it("rejects a missing or nonsensical --expected", async () => {
			for (
				const args of [
					["dataset", "assert-count", "orders",],
					["dataset", "assert-count", "orders", "--expected", "-1",],
					["dataset", "assert-count", "orders", "--expected", "1.5",],
					["dataset", "assert-count", "orders", "--expected", "9007199254740992",],
				]
			) {
				const failure = await dssFailure(args, { env: cliEnv("http://127.0.0.1:1",), },);
				expect(failure.code,).toBe(1,);
				const report = JSON.parse(failure.stdout,) as { code: string; error: string; };
				expect(report.code,).toBe("validation_failed",);
				expect(report.error,).toContain("--expected must be a non-negative integer",);
			}
		});
	});

	it("counts interior blank single-column rows exactly", async () => {
		let requestedLimit = "";
		await withCliServer((req, res,) => {
			requestedLimit = new URL(req.url ?? "/", "http://localhost",).searchParams.get("limit",) ?? "";
			tsvResponse(res, ["name", "r1", "", "r2",],);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"assert-count",
				"orders",
				"--expected",
				"3",
			], { env: cliEnv(url,), },);
			expect(requestedLimit,).toBe("4",);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toEqual({
				expected: 3,
				count: 3,
				exact: true,
				satisfied: true,
				dataset: "orders",
			},);
		},);
	});

	it("counts consecutive interior and trailing blank single-column rows exactly", async () => {
		await withCliServer((req, res,) => {
			tsvResponse(res, ["name", "", "", "r1", "", "",],);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"assert-count",
				"orders",
				"--expected",
				"4",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				count: 4,
				exact: true,
				satisfied: true,
			},);
		},);
	});

	it("keeps the bounded probe behavior with blank single-column rows", async () => {
		await withCliServer((req, res,) => {
			tsvResponse(res, ["name", "r1", "", "r2",],);
		}, async (url,) => {
			const failure = await dssFailure([
				"dataset",
				"assert-count",
				"orders",
				"--expected",
				"1",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(4,);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				expected: 1,
				count: 2,
				exact: false,
				satisfied: false,
			},);
		},);
	});

	describe("dataset assert-schema", () => {
		const schemaServer =
			(schema: unknown,) => async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
				expect(req.method,).toBe("GET",);
				expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
					"/public/api/projects/TEST/datasets/orders/schema",
				);
				sendJson(res, schema,);
			};

		it("satisfies on structurally identical schemas regardless of key order", async () => {
			const actual = {
				columns: [
					{ name: "id", type: "bigint", comment: "Identifier", },
					{ name: "city", type: "string", },
				],
			};
			const expected = {
				columns: [
					{ type: "bigint", name: "id", comment: "Identifier", },
					{ type: "string", name: "city", },
				],
			};
			await withCliServer(schemaServer(actual,), async (url,) => {
				const { stdout, stderr, } = await dss([
					"dataset",
					"assert-schema",
					"orders",
					"--data",
					JSON.stringify(expected,),
				], { env: cliEnv(url,), },);
				expect(stderr,).toBe("",);
				const report = JSON.parse(stdout,) as {
					satisfied: boolean;
					differences: unknown[];
					expectedHash: string;
					actualHash: string;
				};
				expect(report.satisfied,).toBe(true,);
				expect(report.differences,).toEqual([],);
				expect(report.expectedHash,).toBe(report.actualHash,);
			},);
		});

		it("reports concise differences and hashes without leaking schema values", async () => {
			const actual = {
				columns: [
					{ name: "id", type: "bigint", },
					{ name: "city", type: "string", comment: "SUPERSECRET_COMMENT", },
				],
			};
			const expected = {
				columns: [
					{ name: "id", type: "bigint", },
					{ name: "city", type: "varchar", },
				],
			};
			await withCliServer(schemaServer(actual,), async (url,) => {
				const failure = await dssFailure([
					"dataset",
					"assert-schema",
					"orders",
					"--data",
					JSON.stringify(expected,),
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				const report = JSON.parse(failure.stdout,) as {
					code: string;
					error: string;
					details: {
						result: {
							satisfied: boolean;
							expectedHash: string;
							actualHash: string;
							totalDifferences: number;
							differences: Array<{ path: string; kind: string; }>;
						};
					};
				};
				expect(report.code,).toBe("assertion_failed",);
				expect(report.details.result.satisfied,).toBe(false,);
				expect(report.details.result.totalDifferences,).toBeGreaterThan(0,);
				expect(report.details.result.differences,).toContainEqual(
					expect.objectContaining({ path: "columns[1].type", kind: "changed", },),
				);
				expect(report.details.result.expectedHash,).not.toBe(report.details.result.actualHash,);
				// Unrelated schema values must never appear in the report.
				expect(failure.stdout,).not.toContain("SUPERSECRET_COMMENT",);
			},);
		});

		it("accepts the expected schema from stdin", async () => {
			await withCliServer(
				schemaServer({ columns: [{ name: "id", type: "bigint", },], },),
				async (url,) => {
					const { stdout, } = await dssWithInput(
						[
							"dataset",
							"assert-schema",
							"orders",
							"--stdin",
						],
						JSON.stringify({ columns: [{ name: "id", type: "bigint", },], },),
						{
							env: cliEnv(url,),
						},
					);
					expect(JSON.parse(stdout,),).toMatchObject({ satisfied: true, differences: [], },);
				},
			);
		});

		it("requires exactly one input source", async () => {
			const missing = await dssFailure(["dataset", "assert-schema", "orders",], {
				env: cliEnv("http://127.0.0.1:1",),
			},);
			expect(missing.code,).toBe(1,);
			expect(JSON.parse(missing.stdout,) as { error: string; },).toMatchObject({
				error: expect.stringContaining("--data, --data-file, or --stdin is required",),
			},);
			const conflicting = await dssFailure([
				"dataset",
				"assert-schema",
				"orders",
				"--data",
				"{}",
				"--data-file",
				"x.json",
			], { env: cliEnv("http://127.0.0.1:1",), },);
			expect(conflicting.code,).toBe(1,);
			expect(JSON.parse(conflicting.stdout,) as { code: string; },).toMatchObject({
				code: "conflicting_input_sources",
			},);
		});
	});

	describe("data-quality assert-results", () => {
		const resultsServer =
			(results: unknown[],) => async (req: IncomingMessage, res: ServerResponse,): Promise<void> => {
				expect(req.method,).toBe("GET",);
				expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
					"/public/api/projects/TEST/datasets/orders/data-quality/last-rules-result",
				);
				sendJson(res, results,);
			};

		it("passes when every selected result reports OK", async () => {
			await withCliServer(
				resultsServer([
					{ id: "r1", name: "Has rows", outcome: "OK", },
					{ id: "r2", name: "No nulls", status: "OK", },
				],),
				async (url,) => {
					const { stdout, stderr, } = await dss([
						"data-quality",
						"assert-results",
						"orders",
					], { env: cliEnv(url,), },);
					expect(stderr,).toBe("",);
					expect(JSON.parse(stdout,),).toEqual({
						satisfied: true,
						dataset: "orders",
						checked: 2,
						failed: [],
					},);
				},
			);
		});

		it("fails with rule ids and outcomes only, never rule payloads", async () => {
			await withCliServer(
				resultsServer([
					{
						id: "r1",
						name: "Has rows",
						outcome: "OK",
						rule: { description: "SUPERSECRET_RULE_PAYLOAD", },
					},
					{
						id: "r2",
						name: "No nulls",
						outcome: "ERROR",
						status: "ERROR",
						rule: { description: "SUPERSECRET_RULE_PAYLOAD", },
					},
				],),
				async (url,) => {
					const failure = await dssFailure([
						"data-quality",
						"assert-results",
						"orders",
					], { env: cliEnv(url,), },);
					expect(failure.code,).toBe(4,);
					const report = JSON.parse(failure.stdout,) as {
						code: string;
						details: { result: { satisfied: boolean; checked: number; failed: unknown[]; }; };
					};
					expect(report.code,).toBe("assertion_failed",);
					expect(report.details.result,).toMatchObject({
						satisfied: false,
						checked: 2,
						failed: [{ ruleId: "r2", outcome: "ERROR", status: "ERROR", },],
					},);
					expect(failure.stdout,).not.toContain("SUPERSECRET_RULE_PAYLOAD",);
				},
			);
		});

		it("fails when a result reports an unknown outcome", async () => {
			await withCliServer(resultsServer([{ id: "r1", outcome: "UNKNOWN", },],), async (url,) => {
				const failure = await dssFailure([
					"data-quality",
					"assert-results",
					"orders",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: { failed: Array<{ ruleId: string; outcome: string | null; }>; }; };
				};
				expect(report.details.result.failed,).toEqual([
					{ ruleId: "r1", outcome: "UNKNOWN", status: null, },
				],);
			},);
		});

		it("fails when no results are available", async () => {
			await withCliServer(resultsServer([],), async (url,) => {
				const failure = await dssFailure([
					"data-quality",
					"assert-results",
					"orders",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				const report = JSON.parse(failure.stdout,) as {
					details: {
						result: { satisfied: boolean; checked: number; reason: string; failed: unknown[]; };
					};
				};
				expect(report.details.result,).toEqual({
					satisfied: false,
					dataset: "orders",
					checked: 0,
					failed: [],
					reason: "no_results",
				},);
			},);
		});

		it("honors --rule-id to select only the matching results", async () => {
			let capturedUrl = "";
			await withCliServer(async (req, res,) => {
				capturedUrl = req.url ?? "";
				expect(new URL(capturedUrl, "http://localhost",).pathname,).toBe(
					"/public/api/projects/TEST/datasets/orders/data-quality/last-rules-result",
				);
				sendJson(res, [
					{ id: "r1", outcome: "OK", },
					{ id: "r2", outcome: "ERROR", },
				],);
			}, async (url,) => {
				const { stdout, } = await dss([
					"data-quality",
					"assert-results",
					"orders",
					"--rule-id",
					"r1",
				], { env: cliEnv(url,), },);
				expect(capturedUrl,).toContain("ruleId=r1",);
				expect(JSON.parse(stdout,),).toMatchObject({
					satisfied: true,
					ruleId: "r1",
					checked: 1,
					failed: [],
				},);
			},);
		});

		it("fails when a result supplies no outcome or status", async () => {
			await withCliServer(resultsServer([{ id: "r1", name: "No verdict", },],), async (url,) => {
				const failure = await dssFailure([
					"data-quality",
					"assert-results",
					"orders",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				const report = JSON.parse(failure.stdout,) as {
					details: {
						result: {
							failed: Array<{ ruleId: string; outcome: string | null; status: string | null; }>;
						};
					};
				};
				expect(report.details.result.failed,).toEqual([
					{ ruleId: "r1", outcome: null, status: null, },
				],);
			},);
		});

		it("fails when a result carries conflicting outcome and status", async () => {
			await withCliServer(
				resultsServer([{ id: "r1", outcome: "OK", status: "ERROR", },],),
				async (url,) => {
					const failure = await dssFailure([
						"data-quality",
						"assert-results",
						"orders",
					], { env: cliEnv(url,), },);
					expect(failure.code,).toBe(4,);
					const report = JSON.parse(failure.stdout,) as {
						details: {
							result: {
								failed: Array<{ ruleId: string; outcome: string | null; status: string | null; }>;
							};
						};
					};
					expect(report.details.result.failed,).toEqual([
						{ ruleId: "r1", outcome: "OK", status: "ERROR", },
					],);
				},
			);
		});
	});

	describe("legacy recipe assertion behavior", () => {
		it("still exits 4 with assertion_failed on unchanged:false drift", async () => {
			const dir = join(tmpdir(), `dss-assertions-recipe-${Date.now()}`,);
			mkdirSync(dir, { recursive: true, },);
			const backupPath = join(dir, "backup.json",);
			writeFileSync(
				backupPath,
				JSON.stringify({ resource: "recipe", payloadHash: "not-the-current-payload", },),
				"utf-8",
			);
			try {
				await withCliServer(async (req, res,) => {
					if (req.method === "GET") {
						sendJson(res, { recipe: { type: "python", name: "r", }, payload: "print('current')\n", },);
						return;
					}
					res.statusCode = 404;
					res.end();
				}, async (url,) => {
					const failure = await dssFailure([
						"recipe",
						"assert-unchanged",
						"r",
						"--since",
						backupPath,
					], { env: cliEnv(url,), },);
					expect(failure.code,).toBe(4,);
					const report = JSON.parse(failure.stdout,) as {
						code: string;
						error: string;
						details: { result: { unchanged: boolean; }; };
					};
					expect(report,).toMatchObject({
						code: "assertion_failed",
						error: "Command completed with failed assertion result.",
					},);
					expect(report.details.result.unchanged,).toBe(false,);
				},);
			} finally {
				rmSync(dir, { recursive: true, force: true, },);
			}
		});

		it("still reports unchanged:true with exit 0 when nothing drifted", async () => {
			const dir = join(tmpdir(), `dss-assertions-recipe-${Date.now()}`,);
			mkdirSync(dir, { recursive: true, },);
			const backupPath = join(dir, "backup.py",);
			writeFileSync(backupPath, "print('current')\n", "utf-8",);
			try {
				await withCliServer(async (req, res,) => {
					if (req.method === "GET") {
						sendJson(res, { recipe: { type: "python", name: "r", }, payload: "print('current')\n", },);
						return;
					}
					res.statusCode = 404;
					res.end();
				}, async (url,) => {
					const { stdout, stderr, } = await dss([
						"recipe",
						"assert-unchanged",
						"r",
						"--since",
						backupPath,
					], { env: cliEnv(url,), },);
					expect(stderr,).toBe("",);
					expect(JSON.parse(stdout,) as { unchanged: boolean; },).toMatchObject({
						unchanged: true,
					},);
				},);
			} finally {
				rmSync(dir, { recursive: true, force: true, },);
			}
		});
	});
});
