import { describe, expect, it, } from "bun:test";
import { cleanupLedgerEntry, } from "../../src/cli/helpers/cleanup.js";
import { projectIncarnationHash, } from "../../src/utils/project-incarnation.js";
import {
	ARCHIVE_FIXTURE_SECRETS,
	credentialBearingArchive,
	writeProjectArchive,
} from "./_archive-fixtures.js";
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

	it("keeps the DSS status and message of an indeterminate import process failure", async () => {
		const dir = join(tmpdir(), `dss-project-import-conflict-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const archivePath = join(dir, "archive.zip",);
		writeProjectArchive(archivePath, "ARCHIVE_KEY",);
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
					// DSS 15.0.1 answers a taken target key with a 500.
					res.statusCode = 500;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({
						errorType: "com.dataiku.dip.exceptions.CodedException",
						message:
							"Project key TAKEN is already used. Please check that you have not already imported this project.",
					},),);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					["project", "import", archivePath, "--target-project-key", "TAKEN",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(2,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "ambiguous_outcome",
					status: 500,
					details: {
						dssMessage:
							"Project key TAKEN is already used. Please check that you have not already imported this project.",
						dssErrorType: "com.dataiku.dip.exceptions.CodedException",
						importId: "tmp-import-1",
						targetProjectKey: "TAKEN",
					},
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

async function runExport(
	archive: Uint8Array,
): Promise<{ stdout: string; stderr: string; dir: string; out: string; }> {
	const dir = join(tmpdir(), `dss-project-export-creds-${Date.now()}-${Math.random()}`,);
	mkdirSync(dir, { recursive: true, },);
	const out = join(dir, "export.zip",);
	let captured = { stdout: "", stderr: "", };
	await withCliServer((req, res,) => {
		if (req.method === "POST" && req.url === "/public/api/projects/SRC/export") {
			res.statusCode = 200;
			res.setHeader("Content-Type", "application/zip",);
			res.end(Buffer.from(archive,),);
			return;
		}
		res.statusCode = 404;
		res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
	}, async (url,) => {
		captured = await dss(["project", "export", "SRC", "--output", out,], {
			env: cliEnv(url,),
		},);
	},);
	return { ...captured, dir, out, };
}

describe("Project export credential warning", () => {
	it("warns with a path/field inventory and never prints secret values", async () => {
		const { stdout, stderr, dir, out, } = await runExport(credentialBearingArchive("SRC",),);
		try {
			expect(JSON.parse(stdout,),).toEqual({ path: out, credentialFindings: 3, },);
			const warningEvent = JSON.parse(stderr.trim().split("\n",).at(-1,)!,) as {
				type: string;
				warnings: Array<Record<string, unknown>>;
			};
			expect(warningEvent.type,).toBe("warning",);
			const warning = warningEvent.warnings.find((entry,) =>
				entry.code === "export_contains_credentials"
			)!;
			expect(warning,).toBeDefined();
			expect(warning.total,).toBe(3,);
			expect(String(warning.hint,),).toMatch(/sanitize/i,);
			expect(String(warning.hint,),).toMatch(/retained versions/i,);
			expect(warning.findings,).toEqual([
				{ member: "project_config/apikeys.json", rule: "member_name", },
				{
					member: "project_config/variables.json",
					rule: "json_key",
					keyPath: "$.standard.DHL_API_KEY",
					field: "DHL_API_KEY",
				},
				{
					member: "project_config/web_apps/app1.json",
					rule: "json_key",
					keyPath: "$.config.service.apiKey",
					field: "apiKey",
				},
			],);
			for (const secret of ARCHIVE_FIXTURE_SECRETS) {
				expect(stdout,).not.toContain(secret,);
				expect(stderr,).not.toContain(secret,);
			}
			// The archive is written verbatim: export performs no redaction.
			expect(readFileSync(out,).includes(ARCHIVE_FIXTURE_SECRETS[0],),).toBe(true,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("stays silent and reports zero findings for an archive without credentials", async () => {
		const dir = join(tmpdir(), `dss-project-export-clean-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const clean = join(dir, "clean-source.zip",);
		writeProjectArchive(clean, "SRC",);
		const out = join(dir, "export.zip",);
		try {
			await withCliServer((req, res,) => {
				if (req.method === "POST" && req.url === "/public/api/projects/SRC/export") {
					res.statusCode = 200;
					res.setHeader("Content-Type", "application/zip",);
					res.end(readFileSync(clean,),);
					return;
				}
				res.statusCode = 404;
				res.end(`unexpected ${req.method ?? ""} ${req.url ?? ""}`,);
			}, async (url,) => {
				const { stdout, stderr, } = await dss(
					["project", "export", "SRC", "--output", out,],
					{ env: cliEnv(url,), },
				);
				expect(stderr,).toBe("",);
				expect(JSON.parse(stdout,),).toEqual({ path: out, credentialFindings: 0, },);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("inspect-archive reports the same inventory locally without secret values", async () => {
		const dir = join(tmpdir(), `dss-project-inspect-creds-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const archivePath = join(dir, "archive.zip",);
		writeFileSync(archivePath, credentialBearingArchive("SRC",),);
		try {
			const { stdout, stderr, } = await dss(["project", "inspect-archive", archivePath,], {
				env: { ...hermeticEnv, },
			},);
			expect(stderr,).toBe("",);
			const report = JSON.parse(stdout,) as {
				valid: boolean;
				credentialFindings: Array<Record<string, unknown>>;
				issues: Array<{ code: string; }>;
			};
			expect(report.valid,).toBe(true,);
			expect(report.credentialFindings,).toHaveLength(3,);
			expect(report.issues.map((issue,) => issue.code),).toContain("credential_structure",);
			for (const secret of ARCHIVE_FIXTURE_SECRETS) expect(stdout,).not.toContain(secret,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
});
