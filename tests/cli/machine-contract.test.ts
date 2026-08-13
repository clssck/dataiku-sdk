import { describe, expect, it, } from "bun:test";
import { buildAgentContract, buildCommandRegistry, } from "../../src/cli/contract.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("machine contract: stdio streams and failure codes", () => {
	it("pins the agent-contract stdio metadata wording and shape", () => {
		const contract = buildAgentContract();
		const stdio = contract.stdio as Record<string, Record<string, unknown>>;
		expect(stdio.stdout,).toEqual({
			success: "single-json-value",
			rawEscapeHatches: [
				"recipe get-payload --raw without --output",
				"recipe cat --raw without --output",
			],
		},);
		expect(stdio.stderr,).toEqual({
			format: "jsonl",
			events: ["warning", "trace", "error",],
			error: "single-final-error-event-on-nonzero-exit",
			failureRouting: {
				commandFailure: "doctor/batch/cleanup failures: single JSON stdout; empty stderr",
				dispatchFailure:
					"usage/unknown/transport/internal failures: empty stdout; single JSON error stderr",
			},
		},);
	});

	it("advertises recipe assert-unchanged exit 4 as assertionFailure, never long-running", () => {
		const registry = buildCommandRegistry();
		const entry = registry.recipe?.["assert-unchanged"];
		expect(entry,).toBeDefined();
		expect(entry?.exitCodes,).toEqual({
			ok: 0,
			usage: 1,
			error: 2,
			transient: 3,
			assertionFailure: 4,
		},);
		expect(entry?.async,).toBe("none",);
		expect(registry.batch?.run?.exitCodes,).toEqual({
			ok: 0,
			usage: 1,
			error: 2,
			transient: 3,
			longRunningFailure: 4,
			assertionFailure: 4,
		},);
	});

	it("advertises assertionFailure in batch plan exit metadata", async () => {
		const { stdout, stderr, } = await dss([
			"batch",
			"--plan",
			"--data",
			JSON.stringify([["recipe", "assert-unchanged", "r", "--since", "backup.json",],],),
		], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(stderr,).toBe("",);
		const plan = JSON.parse(stdout,) as { exitCodesOnFailure: Record<string, number>; };
		expect(plan.exitCodesOnFailure,).toMatchObject({
			longRunningFailure: 4,
			assertionFailure: 4,
		},);
	});

	it("exits 4 with assertion_failed on stderr when assert-unchanged detects drift", async () => {
		const backupPath = join(tmpdir(), `dss-assert-backup-${Date.now()}.json`,);
		writeFileSync(
			backupPath,
			JSON.stringify({ resource: "recipe", payloadHash: "not-the-current-payload", },),
			"utf-8",
		);
		try {
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "r", },
						payload: "print('current')\n",
					},);
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
				], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(4,);
				expect(failure.stdout,).toBe("",);
				const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
				expect(report,).toMatchObject({
					type: "error",
					ok: false,
					code: "assertion_failed",
					category: "dss",
					exitCode: 4,
					error: "Command completed with failed assertion result.",
				},);
				const details = report.details as Record<string, unknown>;
				expect((details.result as Record<string, unknown>).unchanged,).toBe(false,);
			},);
		} finally {
			rmSync(backupPath, { force: true, },);
		}
	});

	it("reports unchanged on stdout with empty stderr when nothing drifted", async () => {
		const backupPath = join(tmpdir(), `dss-assert-backup-${Date.now()}.py`,);
		writeFileSync(backupPath, "print('current')\n", "utf-8",);
		try {
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "r", },
						payload: "print('current')\n",
					},);
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
				], {
					env: cliEnv(url,),
				},);
				expect(stderr,).toBe("",);
				const report = JSON.parse(stdout,) as Record<string, unknown>;
				expect(report.unchanged,).toBe(true,);
			},);
		} finally {
			rmSync(backupPath, { force: true, },);
		}
	});

	it("keeps batch failures on stdout and preserves assertion codes through nesting", async () => {
		const backupPath = join(tmpdir(), `dss-assert-backup-${Date.now()}.json`,);
		writeFileSync(
			backupPath,
			JSON.stringify({ resource: "recipe", payloadHash: "not-the-current-payload", },),
			"utf-8",
		);
		try {
			await withCliServer(async (req, res,) => {
				if (req.method === "GET") {
					sendJson(res, {
						recipe: { type: "python", name: "r", },
						payload: "print('current')\n",
					},);
					return;
				}
				res.statusCode = 404;
				res.end();
			}, async (url,) => {
				const failure = await dssFailure([
					"batch",
					"--data",
					JSON.stringify([["recipe", "assert-unchanged", "r", "--since", backupPath,],],),
				], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const envelope = JSON.parse(failure.stdout,) as {
					ok: boolean;
					steps: Array<{ ok: boolean; error: { code: string; exitCode: number; }; }>;
				};
				expect(envelope.ok,).toBe(false,);
				expect(envelope.steps[0]?.ok,).toBe(false,);
				expect(envelope.steps[0]?.error.code,).toBe("assertion_failed",);
				expect(envelope.steps[0]?.error.exitCode,).toBe(4,);

				const nestedFailure = await dssFailure([
					"batch",
					"--data",
					JSON.stringify([
						[
							"batch",
							"run",
							"--data",
							JSON.stringify([
								["recipe", "assert-unchanged", "r", "--since", backupPath,],
							],),
						],
					],),
				], { env: cliEnv(url,), },);
				expect(nestedFailure.code,).toBe(4,);
				expect(nestedFailure.stderr,).toBe("",);
				const nestedEnvelope = JSON.parse(nestedFailure.stdout,) as {
					steps: Array<{ error: { code: string; exitCode: number; }; }>;
				};
				expect(nestedEnvelope.steps[0]?.error,).toMatchObject({
					code: "assertion_failed",
					exitCode: 4,
				},);
			},);
		} finally {
			rmSync(backupPath, { force: true, },);
		}
	});

	it("keeps dispatch failures as one JSON error on stderr with empty stdout", async () => {
		const failure = await dssFailure(["recipe", "assert-unchanged",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stdout,).toBe("",);
		const report = JSON.parse(failure.stderr,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			type: "error",
			ok: false,
			code: "missing_required_arg",
			category: "usage",
			exitCode: 1,
		},);
	});
});
