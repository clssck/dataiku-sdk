import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileSync,
	rmSync,
	sendJson,
	statSync,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

describe("CLI agent-readiness mutation contracts", () => {
	it("dry-runs dataset writes and reports idempotent create skips", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/") {
				sendJson(res, [{ name: "orders", type: "Filesystem", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders") {
				sendJson(res, { name: "orders", tags: ["old",], },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const skipped = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--if-not-exists",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(skipped,).toMatchObject({ skipped: "orders", reason: "exists", resource: "dataset", },);

			const createDryRun = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"new_orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(createDryRun,).toMatchObject({
				dryRun: true,
				action: "create",
				resource: "dataset",
				name: "new_orders",
				payload: { datasetName: "new_orders", connection: "filesystem", dsType: "Filesystem", },
			},);

			const updateDryRun = JSON.parse(
				(await dss([
					"dataset",
					"update",
					"orders",
					"--data",
					'{"tags":["new"]}',
					"--dry-run",
				], { env: cliEnv(url,), },)).stdout,
			) as { current?: { tags?: string[]; }; next?: { tags?: string[]; }; };
			expect(updateDryRun.current?.tags,).toEqual(["old",],);
			expect(updateDryRun.next?.tags,).toEqual(["new",],);
		},);
	});

	it("dry-runs long-running job and scenario commands without POSTing", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const jobDryRun = JSON.parse(
				(await dss([
					"job",
					"build",
					"orders",
					"--wait",
					"--timeout",
					"10",
					"--poll-interval",
					"1",
					"--dry-run",
				], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(jobDryRun,).toMatchObject({
				dryRun: true,
				action: "build",
				resource: "job",
				target: "orders",
				method: "POST",
			},);
			expect(jobDryRun.endpoint,).toBe("/public/api/projects/TEST/jobs/",);

			const scenarioDryRun = JSON.parse(
				(await dss([
					"scenario",
					"run",
					"nightly",
					"--wait",
					"--timeout",
					"10",
					"--poll-interval",
					"1",
					"--dry-run",
				], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(scenarioDryRun,).toMatchObject({
				dryRun: true,
				action: "run",
				resource: "scenario",
				id: "nightly",
				method: "POST",
			},);
			expect(scenarioDryRun.endpoint,).toBe("/public/api/projects/TEST/scenarios/nightly/run/",);
		},);
	});

	it("plans mutations without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url ?? ""}`,);
		}, async (url,) => {
			const datasetPlan = JSON.parse(
				(await dss([
					"dataset",
					"create",
					"--name",
					"planned_orders",
					"--connection",
					"filesystem",
					"--type",
					"Filesystem",
					"--plan",
					"--project-key",
					"TEST",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(datasetPlan,).toMatchObject({
				plan: true,
				action: "create",
				resource: "dataset",
				name: "planned_orders",
				method: "POST",
				endpoint: "/public/api/projects/TEST/datasets/",
				payload: {
					datasetName: "planned_orders",
					connection: "filesystem",
					dsType: "Filesystem",
					projectKey: "TEST",
				},
			},);

			const scenarioPlan = JSON.parse(
				(await dss(["scenario", "run", "nightly", "--plan", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(scenarioPlan,).toMatchObject({
				plan: true,
				action: "run",
				resource: "scenario",
				id: "nightly",
				method: "POST",
				endpoint: "/public/api/projects/TEST/scenarios/nightly/run/",
				payload: {},
			},);

			const jobPlan = JSON.parse(
				(await dss(["job", "build", "orders", "--plan", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(jobPlan,).toMatchObject({
				plan: true,
				action: "build",
				resource: "job",
				target: "orders",
				method: "POST",
				endpoint: "/public/api/projects/TEST/jobs/",
			},);

			const flowZonePlan = JSON.parse(
				(await dss([
					"flow-zone",
					"move",
					"zone-1",
					"--dataset",
					"orders",
					"--plan",
					"--project-key",
					"TEST",
				], { env: cliEnv(url,), },)).stdout,
			) as Record<string, unknown>;
			expect(flowZonePlan,).toMatchObject({
				plan: true,
				action: "move",
				resource: "flow-zone",
				id: "zone-1",
				method: "POST",
				endpoint: "/public/api/projects/TEST/flow/zones/zone-1/add-items",
				payload: [{ objectType: "DATASET", objectId: "orders", },],
			},);
			const plannedAndDryRun = JSON.parse(
				(await dss(["scenario", "run", "nightly", "--plan", "--dry-run", "--project-key", "TEST",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(plannedAndDryRun,).toMatchObject({
				plan: true,
				plannedAndDryRun: true,
				action: "run",
				resource: "scenario",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans recipe payload backups as local writes", async () => {
		const filePath = join(tmpdir(), `dss-cli-plan-payload-${Date.now()}.py`,);
		const backupDir = join(tmpdir(), `dss-cli-plan-backup-${Date.now()}`,);
		writeFileSync(filePath, "print('planned')\n", "utf-8",);
		let requestCount = 0;

		try {
			await withCliServer((req, res,) => {
				requestCount++;
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${req.url ?? ""}`,);
			}, async (url,) => {
				const plan = JSON.parse(
					(await dss([
						"recipe",
						"set-payload",
						"my_recipe",
						"--file",
						filePath,
						"--backup-dir",
						backupDir,
						"--plan",
						"--project-key",
						"TEST",
					], { env: cliEnv(url,), },)).stdout,
				) as {
					endpoint: string;
					localWrites: Array<{ before: string; path: string; source: string; }>;
					payload: { backupPath: string; content: string; file: string; };
				};

				expect(plan.endpoint,).toBe("/public/api/projects/TEST/recipes/my_recipe",);
				expect(plan.payload,).toMatchObject({
					file: filePath,
					content: "print('planned')\n",
				},);
				expect(plan.payload.backupPath.startsWith(backupDir,),).toBe(true,);
				expect(plan.localWrites,).toEqual([{
					path: plan.payload.backupPath,
					source: "remote recipe backup",
					before: "PUT",
				},],);
			},);
			expect(requestCount,).toBe(0,);
		} finally {
			rmSync(filePath, { force: true, },);
			rmSync(backupDir, { recursive: true, force: true, },);
		}
	});

	it("validates mutation plan inputs locally", async () => {
		const failure = await dssFailure([
			"dataset",
			"create",
			"--name",
			"planned_orders",
			"--type",
			"Filesystem",
			"--plan",
			"--project-key",
			"TEST",
		], {
			env: {
				PATH: process.env.PATH,
				HOME: process.env.HOME,
				DATAIKU_PROJECT_KEY: "TEST",
			},
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(failure.stdout,).toContain("--connection is required",);
	});
	it("plans UploadedFiles creation without an explicit connection", async () => {
		const { stdout, stderr, } = await dss([
			"dataset",
			"create",
			"--name",
			"uploads",
			"--type",
			"UploadedFiles",
			"--plan",
			"--project-key",
			"TEST",
		], {
			env: {
				PATH: process.env.PATH,
				HOME: process.env.HOME,
				DATAIKU_PROJECT_KEY: "TEST",
			},
		},);

		expect(stderr,).toBe("",);
		expect(JSON.parse(stdout,),).toMatchObject({
			plan: true,
			resource: "dataset",
			action: "create",
			payload: {
				datasetName: "uploads",
				dsType: "UploadedFiles",
			},
		},);
	});

	it("dry-runs variable and notebook mutations with current and next state without mutating notebooks", async () => {
		let mutationRequests = 0;
		const notebook = {
			cells: [
				{
					cell_type: "code",
					source: ["1",],
					outputs: [{ text: "old", },],
					execution_count: 7,
				},
				{
					cell_type: "markdown",
					source: ["# Overview\n",],
					metadata: { tags: ["intro",], },
				},
				{
					cell_type: "raw",
					source: ["unexecuted payload\n",],
					metadata: { format: "text/plain", },
					custom: { retained: true, },
				},
			],
			metadata: {},
			nbformat: 4,
			nbformat_minor: 5,
		};
		await withCliServer((req, res,) => {
			if (req.method !== "GET") mutationRequests++;
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEST/variables/") {
				sendJson(res, { standard: { a: 1, }, local: {}, },);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/jupyter-notebooks/book"
			) {
				sendJson(res, notebook,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const varsDryRun = JSON.parse(
				(await dss(["variable", "set", "--standard", '{"b":2}', "--dry-run",], { env: cliEnv(url,), },))
					.stdout,
			) as { next?: { standard?: Record<string, unknown>; }; };
			expect(varsDryRun.next?.standard,).toEqual({ a: 1, b: 2, },);

			const clearDryRun = JSON.parse(
				(await dss(["notebook", "clear-jupyter-outputs", "book", "--dry-run",], {
					env: cliEnv(url,),
				},)).stdout,
			) as {
				current?: { cells?: Array<Record<string, unknown>>; };
				next?: { cells?: Array<Record<string, unknown>>; };
			};
			expect(clearDryRun.next?.cells?.[0]?.outputs,).toEqual([],);
			expect(clearDryRun.next?.cells?.[0]?.execution_count,).toBe(0,);
			expect(clearDryRun.next?.cells?.[0],).toEqual({
				...notebook.cells[0],
				outputs: [],
				execution_count: 0,
			},);
			expect(clearDryRun.current,).toStrictEqual(notebook,);
			expect(clearDryRun.next?.cells?.slice(1,),).toStrictEqual(notebook.cells.slice(1,),);
			expect(JSON.stringify(clearDryRun.next?.cells?.slice(1,),),).toBe(
				JSON.stringify(notebook.cells.slice(1,),),
			);
		},);
		expect(mutationRequests,).toBe(0,);
	});

	it("dry-runs install-skill without credentials or file writes", async () => {
		const tmpDir = join(tmpdir(), `dss-install-skill-dry-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const result = JSON.parse(
				(await dss(["install-skill", "--agent", "omp", "--target", tmpDir, "--dry-run",], {
					cwd: tmpDir,
					env: { PATH: process.env.PATH, HOME: process.env.HOME, },
				},)).stdout,
			) as Record<string, unknown>;
			const installed = result.installed as Array<Record<string, unknown>>;
			expect(result,).toMatchObject({
				dryRun: true,
				scope: "project",
				target: tmpDir,
			},);
			expect(installed,).toEqual([{
				agent: "omp",
				path: join(tmpDir, ".omp", "skills", "dataiku-dss", "SKILL.md",),
				via: "flag",
			},],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("records cleanup ledger entries bound to the canonical server URL", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-ledger-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		let serverUrl = "";
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/scenarios/") {
					sendJson(res, {},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				serverUrl = url;
				await dss([
					"scenario",
					"create",
					"cleanup_test",
					"Cleanup Test",
					"--record-cleanup",
					ledgerPath,
				], { env: { ...cliEnv(url,), DATAIKU_URL: `${url}/`, }, },);
			},);
			const lines = readFileSync(ledgerPath, "utf-8",).trim().split("\n",);
			expect(lines,).toHaveLength(1,);
			const entry = JSON.parse(lines[0],) as {
				action?: string;
				resource?: string;
				id?: string;
				dssUrl?: string;
				cleanup?: { argv?: string[]; };
			};
			expect(entry,).toMatchObject({
				action: "create",
				resource: "scenario",
				id: "cleanup_test",
				cleanup: {
					argv: ["scenario", "delete", "cleanup_test", "--if-exists", "--project-key", "TEST",],
				},
			},);
			// The ledger must be bound to the exact canonical request base URL,
			// even when the operator supplied trailing slashes.
			expect(entry.dssUrl,).toBe(serverUrl,);
			expect(entry.dssUrl,).not.toMatch(/\/$/,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("atomically reserves a new cleanup ledger for one DSS server", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-reservation-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			await withCliServer((_req, res,) => {
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (firstUrl,) => {
				const firstFailure = await dssFailure([
					"scenario",
					"create",
					"first",
					"First",
					"--record-cleanup",
					ledgerPath,
				], { env: cliEnv(firstUrl,), },);
				expect(firstFailure.code,).toBe(3,);
				expect(firstFailure.stderr,).toBe("",);
				const bindingPath = `${ledgerPath}.dss-url`;
				expect(readFileSync(bindingPath, "utf-8",).trim(),).toBe(firstUrl,);
				if (process.platform !== "win32") {
					expect(statSync(bindingPath,).mode & 0o777,).toBe(0o600,);
				}
			},);
			const requests: string[] = [];
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("mutation must not run",);
			}, async (secondUrl,) => {
				const failure = await dssFailure([
					"scenario",
					"create",
					"second",
					"Second",
					"--record-cleanup",
					ledgerPath,
				], { env: cliEnv(secondUrl,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain(
					"Cleanup ledger is reserved for a different DSS server",
				);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("redacts embedded URL credentials from cleanup previews", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-preview-redaction-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			writeFileSync(
				ledgerPath,
				`${
					JSON.stringify({
						ts: "2026-05-07T00:00:00.000Z",
						action: "create",
						resource: "scenario",
						dssUrl: "https://user:secret@example.com/",
						cleanup: { argv: ["scenario", "delete", "old", "--if-exists",], },
					},)
				}\n`,
			);
			const { stdout, stderr, } = await dss(["cleanup", "--file", ledgerPath,],);
			expect(stderr,).toBe("",);
			expect(stdout,).not.toContain("user",);
			expect(stdout,).not.toContain("secret",);
			const report = JSON.parse(stdout,) as { steps: Array<{ dssUrl?: string; }>; };
			expect(report.steps[0]?.dssUrl,).toBe("https://example.com",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("blocks direct dispatch on an unwritable cleanup ledger path before any DSS call", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-preflight-direct-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerAsDirectory = join(tmpDir, "ledger-dir",);
		mkdirSync(ledgerAsDirectory, { recursive: true, },);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				const failure = await dssFailure([
					"scenario",
					"create",
					"cleanup_preflight",
					"Preflight test",
					"--record-cleanup",
					ledgerAsDirectory,
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("Cleanup ledger path is a directory",);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("blocks direct mutation when an existing cleanup ledger belongs to another DSS server", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-binding-direct-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const original = `${
			JSON.stringify({
				ts: "2026-05-07T00:00:00.000Z",
				action: "create",
				resource: "scenario",
				dssUrl: "http://other-dss.example",
				cleanup: { argv: ["scenario", "delete", "old", "--if-exists",], },
			},)
		}\n`;
		writeFileSync(ledgerPath, original,);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("mutation must not run",);
			}, async (url,) => {
				const failure = await dssFailure([
					"scenario",
					"create",
					"new_scenario",
					"New scenario",
					"--record-cleanup",
					ledgerPath,
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("is not bound to the configured DSS server",);
				expect(failure.stdout,).toContain('"reason":"mismatch"',);
			},);
			expect(requests,).toEqual([],);
			expect(readFileSync(ledgerPath, "utf-8",),).toBe(original,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("blocks a batch mutation when an existing cleanup ledger is legacy-unbound", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-binding-batch-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const original = `${
			JSON.stringify({
				ts: "2026-05-07T00:00:00.000Z",
				action: "create",
				resource: "scenario",
				cleanup: { argv: ["scenario", "delete", "old", "--if-exists",], },
			},)
		}\n`;
		writeFileSync(ledgerPath, original,);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("mutation must not run",);
			}, async (url,) => {
				const failure = await dssFailure([
					"batch",
					"--data",
					JSON.stringify([[
						"scenario",
						"create",
						"new_scenario",
						"New scenario",
						"--record-cleanup",
						ledgerPath,
					],],),
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					steps: Array<{
						ok: boolean;
						error?: { error?: string; details?: { reason?: string; }; };
					}>;
				};
				expect(report.steps[0]?.ok,).toBe(false,);
				expect(report.steps[0]?.error?.error,).toContain(
					"is not bound to the configured DSS server",
				);
				expect(report.steps[0]?.error?.details?.reason,).toBe("missing",);
			},);
			expect(requests,).toEqual([],);
			expect(readFileSync(ledgerPath, "utf-8",),).toBe(original,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("blocks batch steps on an unwritable cleanup ledger path before any DSS call", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-preflight-batch-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerAsDirectory = join(tmpDir, "ledger-dir",);
		mkdirSync(ledgerAsDirectory, { recursive: true, },);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				const failure = await dssFailure([
					"batch",
					"--data",
					JSON.stringify([
						[
							"scenario",
							"create",
							"cleanup_preflight_batch",
							"Preflight test",
							"--record-cleanup",
							ledgerAsDirectory,
						],
					],),
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					steps: Array<{ ok: boolean; error?: { error?: string; }; }>;
				};
				expect(report.steps[0]?.ok,).toBe(false,);
				expect(report.steps[0]?.error?.error,).toContain("Could not preflight cleanup ledger",);
				expect(report.steps[0]?.error?.error,).toContain("Cleanup ledger path is a directory",);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("applies cleanup ledgers in reverse order", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-apply-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const entries = [
			{
				ts: "2026-05-07T00:00:00.000Z",
				action: "create",
				resource: "scenario",
				id: "first",
				projectKey: "TEST",
				cleanup: { argv: ["scenario", "delete", "first", "--if-exists", "--project-key", "TEST",], },
			},
			{
				ts: "2026-05-07T00:00:01.000Z",
				action: "create",
				resource: "scenario",
				id: "second",
				projectKey: "TEST",
				cleanup: { argv: ["scenario", "delete", "second", "--if-exists", "--project-key", "TEST",], },
			},
		];
		const deleted: string[] = [];
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				const match = url.pathname.match(/^\/public\/api\/projects\/TEST\/scenarios\/([^/]+)\/$/,);
				if (req.method === "GET" && match) {
					sendJson(res, { id: match[1], name: match[1], },);
					return;
				}
				if (req.method === "DELETE" && match) {
					deleted.push(match[1]!,);
					sendJson(res, {},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${entries.map((entry,) => JSON.stringify({ ...entry, dssUrl: url, },)).join("\n",)}\n`,
				);
				const applied = JSON.parse(
					(await dss(["cleanup", "--file", ledgerPath, "--apply",], { env: cliEnv(url,), },)).stdout,
				) as { applied?: boolean; failures?: unknown[]; };
				expect(applied.applied,).toBe(true,);
				expect(applied.failures,).toEqual([],);
			},);
			expect(deleted,).toEqual(["second", "first",],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("reports missing cleanup ledger files as usage errors", async () => {
		const missingPath = join(tmpdir(), `dss-missing-ledger-${Date.now()}.jsonl`,);
		const failure = await dssFailure(["cleanup", "--file", missingPath,], {
			env: { PATH: process.env.PATH, HOME: process.env.HOME, },
		},);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(failure.stdout,).toContain("Could not read cleanup ledger",);
	});

	it("rejects malformed cleanup ledger entries", async () => {
		const tmpDir = join(tmpdir(), `dss-bad-cleanup-ledger-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		writeFileSync(ledgerPath, `${JSON.stringify({ cleanup: { argv: [1, 2, 3,], }, },)}\n`,);
		try {
			const failure = await dssFailure(["cleanup", "--file", ledgerPath,], {
				env: { PATH: process.env.PATH, HOME: process.env.HOME, },
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("Invalid cleanup ledger entry at line 1",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("rejects cleanup apply for non-allowlisted commands", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-disallowed-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			await withCliServer((_req, res,) => {
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create",
							resource: "scenario",
							id: "x",
							dssUrl: url,
							cleanup: { argv: ["folder", "upload", "f", "/r", "/tmp/local",], },
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain('"failures"',);
				expect(failure.stdout,).toContain("Invalid cleanup argv: folder upload f /r /tmp/local",);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("classifies a failed wait result during cleanup apply as a cleanup failure", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-failed-wait-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/app-manifest"
				) {
					sendJson(res, { message: "Project not found", }, 404,);
					return;
				}
				if (url.pathname === "/public/api/futures/f-1") {
					sendJson(res, {
						jobId: "f-1",
						alive: true,
						hasResult: false,
						aborted: false,
						unknown: false,
					},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create-instance",
							resource: "app",
							id: "f-1",
							dssUrl: url,
							cleanup: {
								argv: [
									"app",
									"delete-instance",
									"--project-key",
									"TEST",
									"--future-id",
									"f-1",
									"--expect-project-incarnation",
									"0".repeat(64,),
									"--timeout",
									"0",
									"--poll-interval",
									"1",
								],
							},
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied?: boolean;
					results?: unknown[];
					failures?: Array<{
						error?: string;
						result?: {
							state?: string;
							projectKey?: string;
							deletePerformed?: boolean | null;
							remediation?: string;
						};
					}>;
				};
				expect(report.applied,).toBe(true,);
				expect(report.results ?? [],).toEqual([],);
				expect(report.failures,).toHaveLength(1,);
				expect(report.failures?.[0]?.error,).toContain(
					"failed long-running result: FUTURE_STILL_RUNNING",
				);
				expect(report.failures?.[0]?.result,).toMatchObject({
					state: "FUTURE_STILL_RUNNING",
					projectKey: "TEST",
					deletePerformed: false,
				},);
				expect(report.failures?.[0]?.result?.remediation,).toContain(
					"retry only with a future whose terminal result reports this project",
				);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("applies allowlisted data-quality delete-rule cleanups", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-dq-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		try {
			await withCliServer((_req, res,) => {
				res.statusCode = 404;
				res.end(`{"message":"not found"}`,);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create-rule",
							resource: "data-quality",
							id: "rule-1",
							dssUrl: url,
							cleanup: {
								argv: [
									"data-quality",
									"delete-rule",
									"ds1",
									"rule-1",
									"--if-exists",
									"--project-key",
									"TEST",
								],
							},
						},)
					}\n`,
				);
				const { stdout, } = await dss(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(stdout,).not.toContain("Invalid cleanup argv",);
				const result = JSON.parse(stdout,) as { failures?: unknown[]; };
				expect(result.failures ?? [],).toEqual([],);
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
	it("refuses cleanup apply for a ledger bound to a different DSS server before any request", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-bound-other-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create",
							resource: "scenario",
							id: "x",
							dssUrl: "http://127.0.0.1:1",
							cleanup: { argv: ["scenario", "delete", "x", "--if-exists", "--project-key", "TEST",], },
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied?: boolean;
					bindingError?: { entryIndex?: number; reason?: string; foundDssUrl?: string; };
				};
				expect(report.applied,).toBe(false,);
				expect(report.bindingError,).toMatchObject({
					entryIndex: 0,
					reason: "mismatch",
					foundDssUrl: "http://127.0.0.1:1",
				},);
				expect(failure.stdout,).not.toContain('"failures"',);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses cleanup apply for a legacy unbound ledger entry before any request", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-legacy-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create",
							resource: "scenario",
							id: "x",
							cleanup: { argv: ["scenario", "delete", "x", "--if-exists", "--project-key", "TEST",], },
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied?: boolean;
					bindingError?: { entryIndex?: number; reason?: string; };
				};
				expect(report.applied,).toBe(false,);
				expect(report.bindingError,).toMatchObject({ entryIndex: 0, reason: "missing", },);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("refuses mixed ledgers without applying any entry", async () => {
		const tmpDir = join(tmpdir(), `dss-cleanup-mixed-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		const ledgerPath = join(tmpDir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("unexpected",);
			}, async (url,) => {
				// The first-applied entry (last ledger line) is bound to this
				// server; the first line is bound elsewhere. A partial apply
				// would already have hit the server before the violation.
				writeFileSync(
					ledgerPath,
					`${
						JSON.stringify({
							ts: "2026-05-07T00:00:00.000Z",
							action: "create",
							resource: "scenario",
							id: "first",
							dssUrl: "http://127.0.0.1:1",
							cleanup: { argv: ["scenario", "delete", "first", "--if-exists", "--project-key", "TEST",], },
						},)
					}\n${
						JSON.stringify({
							ts: "2026-05-07T00:00:01.000Z",
							action: "create",
							resource: "scenario",
							id: "second",
							dssUrl: url,
							cleanup: {
								argv: ["scenario", "delete", "second", "--if-exists", "--project-key", "TEST",],
							},
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledgerPath, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied?: boolean;
					bindingError?: { entryIndex?: number; reason?: string; };
				};
				expect(report.applied,).toBe(false,);
				// Apply order is reversed: ordered[0] is "second" (valid) and
				// ordered[1] is "first" (mismatch). Nothing may have run.
				expect(report.bindingError,).toMatchObject({ entryIndex: 1, reason: "mismatch", },);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});
});
