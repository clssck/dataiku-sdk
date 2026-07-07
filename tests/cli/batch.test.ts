import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, sendJson, withCliServer, } from "./_harness.js";

describe("CLI batch command", () => {
	const hermetic = {
		PATH: process.env.PATH ?? "",
		HOME: process.env.HOME ?? "",
		DATAIKU_DISABLE_ENV: "1",
	};

	it("dry-run resolves steps without DSS calls and flags unknown commands", async () => {
		const ok = await dss(["batch", "--data", '[["dataset","list"]]', "--dry-run",], {
			env: hermetic,
		},);
		const okReport = JSON.parse(ok.stdout,) as {
			dryRun: boolean;
			steps: Array<{ runnable: boolean; }>;
		};
		expect(okReport.dryRun,).toBe(true,);
		expect(okReport.steps[0]!.runnable,).toBe(true,);

		const bad = await dssFailure(["batch", "--data", '[["dataset","frobnicate"]]', "--dry-run",], {
			env: hermetic,
		},);
		expect(bad.code,).toBe(1,);
		const badReport = JSON.parse(bad.stdout,) as { steps: Array<{ runnable: boolean; }>; };
		expect(badReport.steps[0]!.runnable,).toBe(false,);
	});

	it("redacts nested API keys from batch plans", async () => {
		const secret = "DSS_SUPER_SECRET_12345";
		const plan = JSON.parse(
			(await dss([
				"batch",
				"run",
				"--data",
				JSON.stringify([
					["auth", "login", "--url", "https://dss.example", "--api-key", secret,],
					["auth", "login", "--url", "https://dss.example", `--api-key=${secret}`,],
				],),
				"--plan",
			], { env: hermetic, },)).stdout,
		) as { payload: { steps: string[][]; }; };

		expect(JSON.stringify(plan,),).not.toContain(secret,);
		expect(plan.payload.steps,).toEqual([
			["auth", "login", "--url", "https://dss.example", "--api-key", "[REDACTED]",],
			["auth", "login", "--url", "https://dss.example", "--api-key=[REDACTED]",],
		],);
	});

	it("plans cleanup and batch meta commands without reading ledgers or contacting DSS", async () => {
		const missingLedger = `/tmp/dss-cli-cleanup-plan-${String(Date.now(),)}.jsonl`;
		const cleanupPlan = JSON.parse(
			(await dss(["cleanup", "run", "--file", missingLedger, "--plan",], { env: hermetic, },))
				.stdout,
		) as Record<string, unknown>;
		expect(cleanupPlan,).toMatchObject({
			plan: true,
			resource: "cleanup",
			action: "run",
			file: missingLedger,
		},);

		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
		}, async (url,) => {
			const batchPlan = JSON.parse(
				(await dss(["batch", "run", "--data", '[["project","list"]]', "--plan",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(batchPlan,).toMatchObject({
				plan: true,
				resource: "batch",
				action: "run",
				total: 1,
				needsClient: true,
				payload: {
					steps: [["project", "list",],],
				},
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("rejects a non-array payload with a usage error", async () => {
		const failure = await dssFailure(["batch", "--data", '{"not":"an array"}',], { env: hermetic, },);
		expect(failure.code,).toBe(1,);
		const report = JSON.parse(failure.stderr,) as { code: string; };
		expect(report.code,).toBe("validation_failed",);
	});

	it("fails fast: a failed step skips the rest and exits non-zero", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.includes("/datasets/MISSING",)) {
				res.statusCode = 404;
				res.end(JSON.stringify({ message: "not found", },),);
				return;
			}
			sendJson(res, [{ projectKey: "TEST", name: "T", },],);
		}, async (url,) => {
			const failure = await dssFailure([
				"batch",
				"--data",
				'[["dataset","get","MISSING","--project-key","TEST"],["project","list"]]',
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stdout,) as {
				ok: boolean;
				completed: number;
				steps: Array<{ ok: boolean | null; skipped?: boolean; error?: { code: string; }; }>;
			};
			expect(report.ok,).toBe(false,);
			expect(report.completed,).toBe(1,);
			expect(report.steps[0]!.ok,).toBe(false,);
			expect(report.steps[0]!.error?.code,).toBe("not_found",);
			expect(report.steps[1]!.ok,).toBe(null,);
			expect(report.steps[1]!.skipped,).toBe(true,);
		},);
	});

	it("continue-on-error runs every step and projects per-step --fields", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (url.pathname.includes("/datasets/MISSING",)) {
				res.statusCode = 404;
				res.end(JSON.stringify({ message: "not found", },),);
				return;
			}
			sendJson(res, [{ projectKey: "TEST", name: "T", extra: "drop", },],);
		}, async (url,) => {
			const failure = await dssFailure([
				"batch",
				"--continue-on-error",
				"--data",
				'[["dataset","get","MISSING","--project-key","TEST"],["project","list","--fields","projectKey"]]',
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(2,);
			const report = JSON.parse(failure.stdout,) as {
				steps: Array<{ ok: boolean | null; result?: unknown; }>;
			};
			expect(report.steps[0]!.ok,).toBe(false,);
			expect(report.steps[1]!.ok,).toBe(true,);
			expect(report.steps[1]!.result,).toEqual([{ projectKey: "TEST", },],);
		},);
	});
});
