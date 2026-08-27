import { describe, expect, it, } from "bun:test";
import { cleanupLedgerEntry, } from "../../src/cli/helpers/cleanup.js";
import { projectIncarnationHash, } from "../../src/utils/project-incarnation.js";
import { writeProjectArchive, } from "./_archive-fixtures.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readBody,
	readFileSync,
	rmSync,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

const LANDED_DETAILS = {
	projectKey: "LANDED",
	name: "Landed Project",
	creationTag: { lastModifiedOn: 7, },
};
const LANDED_HASH = projectIncarnationHash("LANDED", LANDED_DETAILS,);

const hermeticEnv = {
	PATH: process.env.PATH ?? "",
	HOME: process.env.HOME ?? "",
	DATAIKU_DISABLE_ENV: "1",
};

const BOUND_CLEANUP_ARGV = [
	"project",
	"delete",
	"LANDED",
	"--if-exists",
	"--expect-project-incarnation",
	LANDED_HASH,
];

function makeImportEntry(dssUrl: string | undefined,): Record<string, unknown> {
	return {
		ts: "2026-08-26T00:00:00.000Z",
		action: "import",
		resource: "project",
		projectKey: "LANDED",
		...(dssUrl !== undefined ? { dssUrl, } : {}),
		cleanup: { argv: BOUND_CLEANUP_ARGV, },
	};
}

describe("Project import cleanup ledger entries", () => {
	it("records an identity-bound cleanup entry from a verified import", () => {
		const entry = cleanupLedgerEntry(
			"project",
			"import",
			["/tmp/archive.zip",],
			{ "target-project-key": "REQUESTED", },
			{
				success: true,
				usedProjectKey: "LANDED",
				projectIncarnationHash: LANDED_HASH,
				importId: "tmp-import-1",
				requestedProjectKey: "REQUESTED",
				remapped: true,
			},
			undefined,
		);
		expect(entry,).toEqual({
			ts: expect.any(String,),
			action: "import",
			resource: "project",
			projectKey: "LANDED",
			name: "LANDED",
			cleanup: { argv: BOUND_CLEANUP_ARGV, },
		},);
	});

	it("never records a project cleanup entry without an incarnation hash", () => {
		expect(
			cleanupLedgerEntry(
				"project",
				"import",
				["/tmp/archive.zip",],
				{},
				{ success: true, usedProjectKey: "LANDED", importId: "tmp-import-1", },
				undefined,
			),
		).toBeUndefined();
	});

	it("never records a project cleanup entry without the used project key", () => {
		expect(
			cleanupLedgerEntry(
				"project",
				"import",
				["/tmp/archive.zip",],
				{},
				{ success: true, projectIncarnationHash: LANDED_HASH, importId: "tmp-import-1", },
				undefined,
			),
		).toBeUndefined();
	});
});

describe("Project cleanup ledger replay", () => {
	it("previews a recorded project cleanup entry", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-preview-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			writeFileSync(ledger, `${JSON.stringify(makeImportEntry(undefined,),)}\n`,);
			const preview = JSON.parse(
				(await dss(["cleanup", "--file", ledger,], { env: hermeticEnv, },)).stdout,
			) as { dryRun: boolean; steps: Array<Record<string, unknown>>; };
			expect(preview.dryRun,).toBe(true,);
			expect(preview.steps,).toHaveLength(1,);
			expect(preview.steps[0],).toMatchObject({
				resource: "project",
				action: "import",
				projectKey: "LANDED",
				dssUrl: null,
				cleanup: { argv: BOUND_CLEANUP_ARGV, },
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses cleanup apply for an unbound project entry before any request", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-unbound-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("cleanup must not start",);
			}, async (url,) => {
				writeFileSync(
					ledger,
					`${
						JSON.stringify({
							...makeImportEntry(url,),
							cleanup: { argv: ["project", "delete", "LANDED", "--if-exists",], },
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledger, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied: boolean;
					lifecycleError: Record<string, unknown>;
				};
				expect(report.applied,).toBe(false,);
				expect(report.lifecycleError,).toMatchObject({
					entryIndex: 0,
					resource: "project",
					action: "delete",
					reason: "missing",
				},);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects an invalid project incarnation hash in the ledger before any request", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-invalid-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("cleanup must not start",);
			}, async (url,) => {
				writeFileSync(
					ledger,
					`${
						JSON.stringify({
							...makeImportEntry(url,),
							cleanup: {
								argv: [
									"project",
									"delete",
									"LANDED",
									"--if-exists",
									"--expect-project-incarnation",
									"short",
								],
							},
						},)
					}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledger, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				const report = JSON.parse(failure.stdout,) as {
					lifecycleError: Record<string, unknown>;
				};
				expect(report.lifecycleError,).toMatchObject({
					entryIndex: 0,
					resource: "project",
					action: "delete",
					reason: "invalid",
				},);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("preflights every project delete binding before applying the first step", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-mixed-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("cleanup must not start",);
			}, async (url,) => {
				// First-applied (reversed) entry is bound; the later line is a
				// legacy unbound project delete. Nothing may have run.
				writeFileSync(
					ledger,
					`${
						JSON.stringify({
							...makeImportEntry(url,),
							cleanup: { argv: ["project", "delete", "LANDED", "--if-exists",], },
						},)
					}\n${JSON.stringify(makeImportEntry(url,),)}\n`,
				);
				const failure = await dssFailure(["cleanup", "--file", ledger, "--apply",], {
					env: cliEnv(url,),
				},);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied: boolean;
					lifecycleError: Record<string, unknown>;
				};
				expect(report.applied,).toBe(false,);
				expect(report.lifecycleError,).toMatchObject({
					entryIndex: 1,
					resource: "project",
					action: "delete",
					reason: "missing",
				},);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("applies a bound project cleanup entry after incarnation match", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-apply-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.method === "GET" && req.url === "/public/api/projects/LANDED/") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify(LANDED_DETAILS,),);
					return;
				}
				if (req.method === "DELETE" && (req.url ?? "").startsWith("/public/api/projects/LANDED",)) {
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				writeFileSync(ledger, `${JSON.stringify(makeImportEntry(url,),)}\n`,);
				const applied = JSON.parse(
					(
						await dss(["cleanup", "--file", ledger, "--apply",], {
							env: cliEnv(url,),
						},)
					).stdout,
				) as {
					applied: boolean;
					results: Array<Record<string, unknown>>;
					failures: unknown[];
				};
				expect(applied.applied,).toBe(true,);
				expect(applied.failures,).toEqual([],);
				expect(applied.results,).toHaveLength(1,);
				expect(applied.results[0],).toMatchObject({
					cleanup: { argv: BOUND_CLEANUP_ARGV, },
					result: { deleted: true, projectKey: "LANDED", },
				},);
			},);
			expect(
				requests,
			).toEqual([
				"GET /public/api/projects/LANDED/",
				"DELETE /public/api/projects/LANDED?clearManagedDatasets=false&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("converges on an absent project during replay without a DELETE", async () => {
		const dir = join(tmpdir(), `dss-project-cleanup-absent-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				if (req.method === "GET" && req.url === "/public/api/projects/LANDED/") {
					res.statusCode = 404;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ message: "project not found", },),);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				writeFileSync(ledger, `${JSON.stringify(makeImportEntry(url,),)}\n`,);
				const applied = JSON.parse(
					(
						await dss(["cleanup", "--file", ledger, "--apply",], {
							env: cliEnv(url,),
						},)
					).stdout,
				) as {
					applied: boolean;
					results: Array<Record<string, unknown>>;
					failures: unknown[];
				};
				expect(applied.applied,).toBe(true,);
				expect(applied.failures,).toEqual([],);
				expect(applied.results,).toHaveLength(1,);
				expect(applied.results[0],).toMatchObject({
					result: { deleted: false, alreadyAbsent: true, projectKey: "LANDED", },
				},);
			},);
			// The DELETE request itself must never be issued for an absent target.
			expect(requests.filter((request,) => request.startsWith("DELETE",)),).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});

describe("Project import CLI wiring", () => {
	it("records an identity-bound cleanup entry from a successful CLI import", async () => {
		const dir = join(tmpdir(), `dss-project-import-cleanup-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const archivePath = join(dir, "archive.zip",);
		writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(async (req, res,) => {
				if (req.url === "/public/api/projects/import/upload") {
					await readBody(req,);
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ id: "tmp-import-1", },),);
					return;
				}
				if (req.url === "/public/api/projects/import/tmp-import-1/process") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ success: true, usedProjectKey: "LANDED", },),);
					return;
				}
				if (req.method === "GET" && req.url === "/public/api/projects/LANDED/") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify(LANDED_DETAILS,),);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					[
						"project",
						"import",
						archivePath,
						"--target-project-key",
						"REQUESTED",
						"--record-cleanup",
						ledger,
					],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const result = JSON.parse(stdout,) as Record<string, unknown>;
				expect(result,).toMatchObject({
					success: true,
					usedProjectKey: "LANDED",
					importId: "tmp-import-1",
					requestedProjectKey: "REQUESTED",
					remapped: true,
					projectIncarnationHash: LANDED_HASH,
				},);
				const entry = JSON.parse(
					readFileSync(ledger, "utf8",).trim().split("\n",)[0]!,
				) as Record<string, unknown>;
				expect(entry,).toMatchObject({
					action: "import",
					resource: "project",
					projectKey: "LANDED",
					name: "LANDED",
					cleanup: { argv: BOUND_CLEANUP_ARGV, },
				},);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("inspect-archive returns the local report without contacting DSS", async () => {
		const dir = join(tmpdir(), `dss-project-inspect-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const archivePath = join(dir, "archive.zip",);
		writeProjectArchive(archivePath, "ARCHIVE_KEY",);
		const requests: string[] = [];
		try {
			await withCliServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				res.statusCode = 500;
				res.end("inspect-archive must not contact DSS",);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					["project", "inspect-archive", archivePath,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				const report = JSON.parse(stdout,) as Record<string, unknown>;
				expect(report,).toMatchObject({
					valid: true,
					memberCount: 1,
					sourceProjectKey: "ARCHIVE_KEY",
					issues: [],
				},);
			},);
			expect(requests,).toEqual([],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});
