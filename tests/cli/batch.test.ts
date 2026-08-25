import { describe, expect, it, } from "bun:test";
import { projectIncarnationHash, } from "../../src/utils/project-incarnation.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileExists,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
} from "./_harness.js";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";

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
		expect(bad.stderr,).toBe("",);
		const badReport = JSON.parse(bad.stdout,) as { steps: Array<{ runnable: boolean; }>; };
		expect(badReport.steps[0]!.runnable,).toBe(false,);
	});
	it("preserves dotted command-registry projections in batch results", async () => {
		const { stdout, stderr, } = await dss([
			"batch",
			"--data",
			JSON.stringify([["commands", "run", "--fields", "dataset.create",],],),
		], { env: hermetic, },);
		expect(stderr,).toBe("",);
		const report = JSON.parse(stdout,) as {
			ok: boolean;
			steps: Array<{
				result: Record<string, { usage?: string; }>;
			}>;
		};
		expect(report.ok,).toBe(true,);
		expect(report.steps[0]?.result["dataset.create"]?.usage,).toContain("dss dataset create",);
	});

	it("dry-run honors allow-empty value flags and still rejects empty non-allow-empty ones", async () => {
		const accepted = JSON.parse(
			(await dss([
				"batch",
				"--data",
				JSON.stringify([
					["app", "set-manifest-version", "--project-key", "TEMPLATE", "--version-notes", "",],
					["wiki", "update", "ARTICLE", "--content", "", "--project-key", "TEST",],
					["insight", "update", "INSIGHT", "--content", "", "--project-key", "TEST",],
				],),
				"--dry-run",
			], { env: hermetic, },)).stdout,
		) as { steps: Array<{ runnable: boolean; }>; };
		expect(accepted.steps,).toHaveLength(3,);
		expect(accepted.steps.every((step,) => step.runnable),).toBe(true,);

		const rejected = await dssFailure([
			"batch",
			"--data",
			JSON.stringify([
				["app", "set-manifest-version", "--project-key", "TEMPLATE", "--manifest-version", "",],
			],),
			"--dry-run",
		], { env: hermetic, },);
		expect(rejected.code,).toBe(1,);
		expect(rejected.stderr,).toBe("",);
		const rejectedReport = JSON.parse(rejected.stdout,) as {
			steps: Array<{ runnable: boolean; }>;
		};
		expect(rejectedReport.steps[0]!.runnable,).toBe(false,);
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
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as { code: string; };
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
			expect(failure.stderr,).toBe("",);
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
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				steps: Array<{ ok: boolean | null; result?: unknown; }>;
			};
			expect(report.steps[0]!.ok,).toBe(false,);
			expect(report.steps[1]!.ok,).toBe(true,);
			expect(report.steps[1]!.result,).toEqual([{ projectKey: "TEST", },],);
		},);
	});

	it("rejects --dry-run for commands without a dry-run mode before any network call", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			sendJson(res, { ok: true, },);
		}, async (url,) => {
			const failure = await dssFailure(["project", "delete", "OLD", "--dry-run",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("--dry-run is not supported for project delete",);
			expect(JSON.parse(failure.stdout,),).toMatchObject({
				code: "unknown_flag",
				category: "usage",
				exitCode: 1,
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("executes batch --plan steps offline without issuing requests", async () => {
		const { stdout, } = await dss([
			"batch",
			"--data",
			'[["project","delete","OLD","--plan"]]',
		], { env: hermetic, },);
		const report = JSON.parse(stdout,) as {
			ok: boolean;
			steps: Array<{ ok: boolean; args: string[]; result: Record<string, unknown>; }>;
		};
		expect(report.ok,).toBe(true,);
		expect(report.steps[0]!.ok,).toBe(true,);
		expect(report.steps[0]!.result,).toMatchObject({
			plan: true,
			action: "delete",
			resource: "project",
		},);
	});

	it("rejects batch steps requesting unsupported --plan or --dry-run before executing any step", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			sendJson(res, { ok: true, },);
		}, async (url,) => {
			const planFailure = await dssFailure([
				"batch",
				"--data",
				'[["dataset","list","--plan"],["project","delete","OLD","--plan"]]',
			], { env: cliEnv(url,), },);
			expect(planFailure.code,).toBe(1,);
			expect(planFailure.stderr,).toBe("",);
			expect(planFailure.stdout,).toContain(
				"Batch step 0 requests --plan, which is not supported for dataset list",
			);

			const dryRunFailure = await dssFailure([
				"batch",
				"--data",
				'[["project","delete","OLD","--dry-run"]]',
			], { env: cliEnv(url,), },);
			expect(dryRunFailure.code,).toBe(1,);
			expect(dryRunFailure.stderr,).toBe("",);
			expect(dryRunFailure.stdout,).toContain(
				"Batch step 0 requests --dry-run, which is not supported for project delete",
			);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("reports unsupported step modes as unrunnable in outer batch dry-run", async () => {
		const failure = await dssFailure([
			"batch",
			"--dry-run",
			"--data",
			'[["dataset","list","--plan"],["project","delete","OLD","--dry-run"]]',
		], { env: hermetic, },);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		const report = JSON.parse(failure.stdout,) as {
			steps: Array<{ index: number; runnable: boolean; error?: { error?: string; }; }>;
		};
		expect(report.steps,).toHaveLength(2,);
		expect(report.steps[0],).toMatchObject({ index: 0, runnable: false, },);
		expect(report.steps[0]?.error?.error,).toContain(
			"Batch step 0 requests --plan, which is not supported for dataset list",
		);
		expect(report.steps[1],).toMatchObject({ index: 1, runnable: false, },);
		expect(report.steps[1]?.error?.error,).toContain(
			"Batch step 1 requests --dry-run, which is not supported for project delete",
		);
	});

	it("redacts --api-key values from batch dry-run output, both separated and equals forms", async () => {
		const secret = "SUPERSECRET_KEY_12345";
		const payload = JSON.stringify([
			["dataset", "update", "orders", "--data", '{"description":"x"}', "--api-key", secret,],
			["dataset", "get", "orders", `--api-key=${secret}`,],
			["dataset", "get", "orders", "--api-key", "-",],
			["dataset", "get", "orders", "--api-key", "-123",],
		],);
		const { stdout, } = await dss(["batch", "--data", payload, "--dry-run",], {
			env: hermetic,
		},);
		expect(stdout,).not.toContain(secret,);
		const report = JSON.parse(stdout,) as { steps: Array<{ args: string[]; runnable: boolean; }>; };
		expect(report.steps[0]!.args,).toEqual([
			"dataset",
			"update",
			"orders",
			"--data",
			'{"description":"x"}',
			"--api-key",
			"***",
		],);
		expect(report.steps[1]!.args,).toEqual(["dataset", "get", "orders", "--api-key=***",],);
		expect(report.steps[2]!.args,).toEqual(["dataset", "get", "orders", "--api-key", "***",],);
		expect(report.steps[3]!.args,).toEqual(["dataset", "get", "orders", "--api-key", "***",],);
	});
	it("redacts Project Git repository values from batch output", async () => {
		const secret = "https://user:SUPERSECRET_REMOTE@git.example.com/repo.git";
		const payload = JSON.stringify([
			[
				"project-git",
				"set-remote",
				"--project-key",
				"P",
				"--repository",
				secret,
			],
			[
				"project-git",
				"add-library",
				"lib",
				"--project-key",
				"P",
				`--repository=${secret}`,
				"--checkout",
				"main",
			],
		],);
		const { stdout, } = await dss(["batch", "--data", payload, "--dry-run",], {
			env: hermetic,
		},);
		expect(stdout,).not.toContain("SUPERSECRET_REMOTE",);
		const report = JSON.parse(stdout,) as { steps: Array<{ args: string[]; }>; };
		expect(report.steps[0]!.args,).toContain("***",);
		expect(report.steps[1]!.args,).toContain("--repository=***",);
	});

	it("redacts --api-key values from batch --plan output and executed step results", async () => {
		const secret = "SUPERSECRET_KEY_67890";
		const planStdout = (
			await dss([
				"batch",
				"--plan",
				"--data",
				JSON.stringify([["dataset", "update", "orders", "--api-key", secret,],],),
			], { env: hermetic, },)
		).stdout;
		expect(planStdout,).not.toContain(secret,);

		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			sendJson(res, { name: "orders", },);
		}, async (url,) => {
			const { stdout, } = await dss([
				"batch",
				"--data",
				JSON.stringify([["dataset", "get", "orders", "--api-key", secret,],],),
			], { env: cliEnv(url,), },);
			expect(stdout,).not.toContain(secret,);
			expect(JSON.parse(stdout,),).toMatchObject({
				steps: [{ args: ["dataset", "get", "orders", "--api-key", "***",], ok: true, },],
			},);
		},);
		expect(requestCount,).toBe(1,);
	});
});

describe("CLI batch cleanup ledger parity", () => {
	const TEMPLATE_MANIFEST = {
		projectKey: "MYAPP_TEMPLATE",
		projectAppType: "APP_TEMPLATE",
		version: "2.0.0",
		versionNotes: null,
		homepageSections: [],
	};
	const INSTANCE_MANIFEST = {
		projectKey: "RELEASE_INSTANCE",
		projectAppType: "APP_INSTANCE",
		version: "2.0.0",
		versionNotes: null,
		homepageSections: [],
	};
	const NEW_INSTANCE_DETAILS = {
		projectKey: "NEW_INSTANCE",
		projectAppType: "APP_INSTANCE",
		creationTag: { versionNumber: 1, lastModifiedOn: 1_700_000_000_000, },
	};
	const NEW_INSTANCE_INCARNATION_HASH = projectIncarnationHash(
		"NEW_INSTANCE",
		NEW_INSTANCE_DETAILS,
	)!;
	const DONE_FUTURE = {
		jobId: "job-9",
		hasResult: true,
		alive: false,
		result: { projectKey: "NEW_INSTANCE", },
	};
	function successorRoutes(
		overrides: {
			post?: (req: IncomingMessage, res: ServerResponse,) => void;
			future?: (req: IncomingMessage, res: ServerResponse,) => void;
		} = {},
	) {
		let created = false;
		return (req: IncomingMessage, res: ServerResponse,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(
					res,
					created
						? [{ projectKey: "OLD_INSTANCE", }, { projectKey: "NEW_INSTANCE", },]
						: [{ projectKey: "OLD_INSTANCE", },],
				);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/") {
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(
					res,
					created ? NEW_INSTANCE_DETAILS : { message: "Project not found", },
					created ? 200 : 404,
				);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				if (overrides.post) {
					overrides.post(req, res,);
					return;
				}
				created = true;
				sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-9") {
				if (overrides.future) {
					overrides.future(req, res,);
					return;
				}
				sendJson(res, DONE_FUTURE,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		};
	}

	function successorStep(ledger: string, extra: string[] = [],): string {
		return JSON.stringify([
			[
				"app",
				"create-successor-instance",
				"MYAPP",
				"--from",
				"OLD_INSTANCE",
				"--to",
				"NEW_INSTANCE",
				...extra,
				"--record-cleanup",
				ledger,
			],
		],);
	}

	function tempLedger(): { dir: string; ledger: string; cleanup: () => void; } {
		const dir = join(tmpdir(), `dss-batch-cleanup-${Date.now()}-${Math.random()}`,);
		mkdirSync(dir, { recursive: true, },);
		return {
			dir,
			ledger: join(dir, "cleanup.jsonl",),
			cleanup: () => rmSync(dir, { recursive: true, force: true, },),
		};
	}

	it("records the successor target for a successful batched successor step", async () => {
		const { ledger, cleanup, } = tempLedger();
		try {
			await withCliServer(successorRoutes(), async (url,) => {
				const { stdout, } = await dss(
					["batch", "--data", successorStep(ledger,),],
					{ env: cliEnv(url,), },
				);
				const report = JSON.parse(stdout,) as { ok: boolean; steps: Array<{ ok: boolean; }>; };
				expect(report.ok,).toBe(true,);
				expect(report.steps[0]!.ok,).toBe(true,);
			},);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as Record<string, unknown>;
			expect(entry,).toMatchObject({
				resource: "app",
				action: "create-successor-instance",
				name: "NEW_INSTANCE",
				cleanup: {
					argv: [
						"app",
						"delete-instance",
						"--project-key",
						"NEW_INSTANCE",
						"--expect-project-incarnation",
						NEW_INSTANCE_INCARNATION_HASH,
					],
				},
			},);
		} finally {
			cleanup();
		}
	});

	it("records a failed-wait eligible successor before the batch reports exit 4", async () => {
		const { ledger, cleanup, } = tempLedger();
		try {
			await withCliServer(
				successorRoutes({
					future: (_req, res,) => sendJson(res, { jobId: "job-9", hasResult: false, alive: false, },),
				},),
				async (url,) => {
					const failure = await dssFailure(
						["batch", "--data", successorStep(ledger,),],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					const report = JSON.parse(failure.stdout,) as {
						ok: boolean;
						steps: Array<{ ok: boolean | null; error?: { exitCode: number; }; }>;
					};
					expect(report.ok,).toBe(false,);
					expect(report.steps[0]!.ok,).toBe(false,);
					expect(report.steps[0]!.error?.exitCode,).toBe(4,);
				},
			);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as Record<string, unknown>;
			expect(entry,).toMatchObject({
				resource: "app",
				action: "create-successor-instance",
				name: "NEW_INSTANCE",
				cleanup: {
					argv: [
						"app",
						"delete-instance",
						"--project-key",
						"NEW_INSTANCE",
						"--unconfirmed-creation",
					],
				},
			},);
		} finally {
			cleanup();
		}
	});

	it("skips the ledger when the create is definitively rejected", async () => {
		const { ledger, cleanup, } = tempLedger();
		try {
			await withCliServer(
				successorRoutes({
					post: (_req, res,) => sendJson(res, { message: "Project key already exists", }, 409,),
				},),
				async (url,) => {
					const failure = await dssFailure(
						["batch", "--data", successorStep(ledger,),],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failure.stdout,).toContain("CREATE_FAILED",);
				},
			);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			cleanup();
		}
	});

	it("skips the ledger for a step-level --dry-run", async () => {
		const { ledger, cleanup, } = tempLedger();
		try {
			await withCliServer(successorRoutes(), async (url,) => {
				const { stdout, } = await dss(
					["batch", "--data", successorStep(ledger, ["--dry-run",],),],
					{ env: cliEnv(url,), },
				);
				const report = JSON.parse(stdout,) as { ok: boolean; steps: Array<{ ok: boolean; }>; };
				expect(report.ok,).toBe(true,);
				expect(report.steps[0]!.ok,).toBe(true,);
			},);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			cleanup();
		}
	});

	it("targets only the successor and never leaks the predecessor or the API key", async () => {
		const { ledger, cleanup, } = tempLedger();
		try {
			await withCliServer(successorRoutes(), async (url,) => {
				const { stdout, } = await dss(
					["batch", "--data", successorStep(ledger,),],
					{ env: cliEnv(url,), },
				);
				expect(stdout,).not.toContain("test-key",);
			},);
			const raw = readFileSync(ledger, "utf-8",);
			const entry = JSON.parse(raw,) as {
				name?: string;
				cleanup: { argv: string[]; };
			};
			expect(entry.name,).toBe("NEW_INSTANCE",);
			expect(entry.cleanup.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--expect-project-incarnation",
				NEW_INSTANCE_INCARNATION_HASH,
			],);
			expect(raw,).not.toContain("OLD_INSTANCE",);
			expect(raw,).not.toContain("test-key",);
		} finally {
			cleanup();
		}
	});
});
