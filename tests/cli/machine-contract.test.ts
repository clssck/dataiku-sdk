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
			format: "compact-json",
			success: "single-json-value",
			failure: "structured-error-object",
			richFailureResults:
				"doctor/batch/cleanup nonzero outcomes are their own compact result on stdout ({ok:false,...}) with the command's exit code; not re-wrapped.",
		},);
		expect(stdio.stderr,).toEqual({
			format: "jsonl",
			events: ["warning", "trace",],
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

	it("exits 4 with assertion_failed on stdout when assert-unchanged detects drift", async () => {
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
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
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

	it("keeps dispatch failures as one JSON error on stdout with empty stderr", async () => {
		const failure = await dssFailure(["recipe", "assert-unchanged",], {
			env: cliEnv("http://127.0.0.1:1",),
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
		expect(report,).toMatchObject({
			type: "error",
			ok: false,
			code: "missing_required_arg",
			category: "usage",
			exitCode: 1,
		},);
	});
	it("routes removed --json and --raw flags to stdout as unknown_flag dispatch errors", async () => {
		for (const flag of ["--json", "--raw",]) {
			const failure = await dssFailure(["commands", "run", flag,], {
				env: cliEnv("http://127.0.0.1:1",),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as Record<string, unknown>;
			expect(report,).toMatchObject({
				type: "error",
				ok: false,
				error: `Unknown flag: ${flag}`,
				code: "unknown_flag",
				category: "usage",
				exitCode: 1,
			},);
		}
	});
});
describe("machine contract: project-git classification", () => {
	const registry = buildCommandRegistry();
	const git = registry["project-git"];

	const READS = [
		"status",
		"get-remote",
		"branches",
		"current-branch",
		"tags",
		"log",
		"diff",
		"list-libraries",
		"future-status",
		"future-wait",
	];
	const MUTATIONS = [
		"set-remote",
		"remove-remote",
		"create-branch",
		"delete-branch",
		"create-tag",
		"delete-tag",
		"switch",
		"fetch",
		"pull",
		"push",
		"commit",
		"revert-to-revision",
		"revert-commit",
		"reset-to-head",
		"reset-to-upstream",
		"drop-and-rebuild",
		"add-library",
		"set-library",
		"remove-library",
		"reset-library",
		"push-library",
		"push-all-libraries",
		"reset-all-libraries",
		"future-abort",
	];
	const DESTRUCTIVE = [
		"delete-branch",
		"delete-tag",
		"drop-and-rebuild",
		"future-abort",
		"remove-library",
		"reset-all-libraries",
		"reset-library",
		"reset-to-head",
		"reset-to-upstream",
		"revert-commit",
		"revert-to-revision",
	];
	const FUTURES = [
		"add-library",
		"reset-library",
		"push-library",
		"push-all-libraries",
		"reset-all-libraries",
		"future-status",
		"future-wait",
		"future-abort",
	];

	it("discovers every project-git action in the registry", () => {
		expect(git,).toBeDefined();
		expect(Object.keys(git ?? {},).sort(),).toEqual(
			[...READS, ...MUTATIONS,].sort(),
		);
	});

	it("classifies every Git network or state mutation as write, never in a read", () => {
		for (const action of MUTATIONS) {
			expect(git?.[action]?.sideEffect, `${action} sideEffect`,).toBe("write",);
			expect(git?.[action]?.mutatesDss, `${action} mutatesDss`,).toBe(true,);
		}
	});

	it("classifies the observing actions as reads even when their verb reads like a write", () => {
		for (const action of READS) {
			expect(git?.[action]?.sideEffect, `${action} sideEffect`,).toBe("read",);
			expect(git?.[action]?.mutatesDss, `${action} mutatesDss`,).toBe(false,);
		}
	});

	it("marks history-, remote-, and directory-destructive Git mutations", () => {
		for (const action of DESTRUCTIVE) {
			expect(git?.[action]?.destructive, `${action} destructive`,).toBe("destructive",);
		}
		for (const action of MUTATIONS.filter((item,) => !DESTRUCTIVE.includes(item,))) {
			expect(git?.[action]?.destructive, `${action} destructive`,).toBe("reversible",);
		}
	});

	it("marks the library calls and future lifecycle as async futures with exit 4", () => {
		for (const action of FUTURES) {
			expect(git?.[action]?.async, `${action} async`,).toBe("future",);
			expect(git?.[action]?.exitCodes?.longRunningFailure, `${action} exit`,).toBe(4,);
		}
		for (const action of MUTATIONS.filter((item,) => !FUTURES.includes(item,))) {
			expect(git?.[action]?.async, `${action} async`,).toBe("none",);
		}
	});

	it("requires --project-key on every action except the three future lifecycle calls", () => {
		for (const action of Object.keys(git ?? {},)) {
			expect(
				git?.[action]?.requiresProject,
				`${action} requiresProject`,
			).toBe(!action.startsWith("future-",),);
		}
	});

	it("advertises branch and tag lists as arrays and log/diff as objects", () => {
		expect(git?.branches?.outputShape,).toBe("array",);
		expect(git?.tags?.outputShape,).toBe("array",);
		expect(git?.log?.outputShape,).toBe("object",);
		expect(git?.diff?.outputShape,).toBe("object",);
		expect(git?.["get-remote"]?.outputShape,).toBe("object",);
	});

	it("keeps Git mutations out of the cleanup ledger", () => {
		for (const action of ["create-branch", "create-tag", "add-library", "set-library",]) {
			expect(git?.[action]?.cleanupCommand, `${action} cleanupCommand`,).toBeUndefined();
			expect(git?.[action]?.cleanupHint, `${action} cleanupHint`,).toBeUndefined();
			expect(git?.[action]?.optionalFlags, `${action} record-cleanup`,).not.toContain(
				"record-cleanup",
			);
		}
	});

	it("labels reusable fetch/reset mutations convergent and one-shot commits idempotency none", () => {
		for (
			const action of [
				"fetch",
				"reset-to-head",
				"reset-to-upstream",
				"reset-library",
				"reset-all-libraries",
			]
		) {
			expect(git?.[action]?.idempotency, `${action} idempotency`,).toBe("convergent",);
		}
		expect(git?.commit?.idempotency,).toBe("none",);
		expect(git?.["create-branch"]?.idempotency,).toBe("none",);
	});
});
