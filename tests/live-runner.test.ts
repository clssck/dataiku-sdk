import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import {
	evaluateCleanupIntegrity,
	layoutFor,
	parseLiveSuiteArgs,
	readPointer,
	verifyLiveRoot,
} from "../scripts/live-suite.js";
import type { LabPointer, } from "../scripts/live-suite.js";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import type { LiveManifest, } from "../tests/live-context.js";
import { sendJson, withCliServer, } from "./cli/_harness.js";

// ---------------------------------------------------------------------------
// Fixtures: offline manifests; no env, no network, no live mutations.
// ---------------------------------------------------------------------------

const credentials = { url: "https://dss.example.com", apiKey: "test-key", } as const;

function manifestWithProjects(
	keys: Array<{ key: string; state: "pending" | "bound" | "deleted"; }>,
): LiveManifest {
	return {
		version: 1,
		fixtureVersion: 1,
		runId: "0123456789abcdef",
		dssUrl: credentials.url,
		projectKey: keys[0]?.key ?? "",
		owner: "ci",
		connection: "filesystem",
		profiles: ["core",],
		fixtures: { datasets: {}, expectedRows: {}, recipes: {}, },
		projects: keys.map(({ key, state, },) => ({
			key,
			state,
			...(state === "bound" ? { incarnation: "a".repeat(64,), } : {}),
			createdAt: "2026-01-01T00:00:00.000Z",
		})),
		cases: [],
		commands: [],
		iteration: 1,
		startedAt: "2026-01-01T00:00:00.000Z",
		beforeProjects: {},
		capabilities: {},
		cleanup: { status: "pending", errors: [], },
	};
}

// ---------------------------------------------------------------------------
// Cleanup integrity evidence (the clean "external projects unchanged: NO" bug)
// ---------------------------------------------------------------------------

describe("evaluateCleanupIntegrity", () => {
	const owned = manifestWithProjects([
		{ key: "SDK_LIVE_ABC123_MAIN", state: "deleted", },
		{ key: "SDK_LIVE_ABC123_FIXTURES_1", state: "deleted", },
	],);
	const externalBefore = {
		"USER_WORK": "hash-user-work",
		"OTHER_TEAM": "hash-other-team",
		"SDK_LIVE_ABC123_MAIN": "hash-owned-main",
		"SDK_LIVE_ABC123_FIXTURES_1": "hash-owned-fixtures",
	} as const;

	it("treats deleted owned projects as expected teardown, not external drift", () => {
		// Owned lab projects are gone after cleanup; external projects are intact.
		const after = { "USER_WORK": "hash-user-work", "OTHER_TEAM": "hash-other-team", };
		const report = evaluateCleanupIntegrity(externalBefore, after, owned,);
		expect(report.verified,).toBe(true,);
		expect(report.missing,).toEqual([],);
		expect(report.changed,).toEqual([],);
		expect(report.unexplained,).toEqual([],);
		expect(report.deletedOwned.sort(),).toEqual([
			"SDK_LIVE_ABC123_FIXTURES_1",
			"SDK_LIVE_ABC123_MAIN",
		],);
	});

	it("fails verified when a genuinely external project vanishes", () => {
		// External drift: USER_WORK disappears alongside the owned teardown.
		const after = { "OTHER_TEAM": "hash-other-team", };
		const report = evaluateCleanupIntegrity(externalBefore, after, owned,);
		expect(report.verified,).toBe(false,);
		expect(report.missing,).toEqual(["USER_WORK",],);
		// Owned deletions are still recorded as expected.
		expect(report.deletedOwned.length,).toBe(2,);
	});

	it("fails verified when an external project changes identity", () => {
		const after = {
			"USER_WORK": "hash-user-work-MUTATED",
			"OTHER_TEAM": "hash-other-team",
		};
		const report = evaluateCleanupIntegrity(externalBefore, after, owned,);
		expect(report.verified,).toBe(false,);
		expect(report.changed,).toEqual(["USER_WORK",],);
		expect(report.missing,).toEqual([],);
	});

	it("fails verified when an unexpected project appears", () => {
		const after = {
			"USER_WORK": "hash-user-work",
			"OTHER_TEAM": "hash-other-team",
			"MYSTERY": "hash-mystery",
		};
		const report = evaluateCleanupIntegrity(externalBefore, after, owned,);
		expect(report.verified,).toBe(false,);
		expect(report.unexplained,).toEqual(["MYSTERY",],);
	});

	it("does not exempt a still-bound owned project from the missing set", () => {
		// A bound owned project that disappears WITHOUT being deleted is drift:
		// manifest claims bound, after lacks it.
		const bound = manifestWithProjects([{ key: "SDK_LIVE_DEF456_MAIN", state: "bound", },],);
		const before = { "SDK_LIVE_DEF456_MAIN": "hash-main", };
		const report = evaluateCleanupIntegrity(before, {}, bound,);
		expect(report.verified,).toBe(false,);
		expect(report.missing,).toEqual(["SDK_LIVE_DEF456_MAIN",],);
		expect(report.deletedOwned,).toEqual([],);
	});
});

// ---------------------------------------------------------------------------
// CLI selection and state-root parsing
// ---------------------------------------------------------------------------

describe("parseLiveSuiteArgs", () => {
	it("parses verbs, case selections, and profiles", () => {
		const options = parseLiveSuiteArgs([
			"run",
			"--case",
			"core.dataset*,collab.wiki",
			"--profile",
			"ml",
		],);
		expect(options.verb,).toBe("run",);
		expect(options.cases,).toEqual(["core.dataset*", "collab.wiki",],);
		expect(options.profile,).toBe("ml",);
	});

	it("defaults the profile to core and rejects unknown profiles", () => {
		expect(parseLiveSuiteArgs(["status",],).profile,).toBe("core",);
		expect(() => parseLiveSuiteArgs(["run", "--profile", "nope",],)).toThrow(/Unknown profile/,);
	});

	it("requires exactly one verb", () => {
		expect(() => parseLiveSuiteArgs([],)).toThrow(/verb is required/,);
		expect(() => parseLiveSuiteArgs(["run", "clean",],)).toThrow(/Exactly one verb/,);
	});

	it("maps --help to a non-mutating status request", () => {
		const options = parseLiveSuiteArgs(["--help",],);
		expect(options.help,).toBe(true,);
		expect(options.verb,).toBe("status",);
	});

	it("collects repeated --case flags", () => {
		const options = parseLiveSuiteArgs([
			"run",
			"--case",
			"core.dataset.baseline",
			"--case",
			"collab.wiki , project.lifecycle",
		],);
		expect(options.cases,).toEqual(["core.dataset.baseline", "collab.wiki", "project.lifecycle",],);
	});
});

// ---------------------------------------------------------------------------
// State-root isolation: per-server lab layout under a custom verification root
// ---------------------------------------------------------------------------

describe("layoutFor", () => {
	it("anchors a relative state dir to the repo root and isolates per server", () => {
		const layout = layoutFor(credentials, ".live-tests/verification",);
		expect(path.basename(layout.stateRoot,),).toBe("verification",);
		expect(layout.stateRoot.startsWith(process.cwd(),),).toBe(true,);
		// Same custom root: pointer-free lab dirs stay inside the state root and
		// are keyed by the canonical DSS URL so another server cannot collide.
		expect(layout.labDir.startsWith(layout.stateRoot + path.sep,),).toBe(true,);
		expect(layout.manifestPath.startsWith(layout.labDir + path.sep,),).toBe(true,);
		expect(layout.pointerPath,).toBe(path.join(layout.stateRoot, "current.json",),);
		expect(layout.lockPath,).toBe(path.join(layout.stateRoot, "lock",),);
	});

	it("derives different lab ids for different DSS servers", () => {
		const a = layoutFor({ url: "https://a.example.com", apiKey: "k", }, ".live-tests/verification",);
		const b = layoutFor({ url: "https://b.example.com", apiKey: "k", }, ".live-tests/verification",);
		expect(a.labDir,).not.toBe(b.labDir,);
	});

	it("honors a pointer-backed lab dir and manifest inside the state root", () => {
		const base = layoutFor(credentials, ".live-tests/verification",);
		const pointer: LabPointer = {
			dssUrl: credentials.url,
			manifest: path.join(base.labDir, "manifest.json",),
			labDir: path.join(base.stateRoot, "lab-0001",),
			updatedAt: "2026-01-01T00:00:00.000Z",
		};
		const layout = layoutFor(credentials, ".live-tests/verification", pointer,);
		expect(layout.labDir,).toBe(pointer.labDir,);
		expect(layout.manifestPath,).toBe(pointer.manifest,);
	});

	it("absolute state dirs are honored verbatim", () => {
		const absolute = path.join(os.tmpdir(), "live-runner-test-state",);
		const layout = layoutFor(credentials, absolute,);
		expect(layout.stateRoot,).toBe(absolute,);
	});
});

// ---------------------------------------------------------------------------
// Pointer validation: containment and corruption handling
// ---------------------------------------------------------------------------

describe("readPointer", () => {
	it("returns undefined when no pointer file exists", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "live-pointer-",),);
		try {
			expect(await readPointer(root,),).toBeUndefined();
		} finally {
			await fs.rm(root, { recursive: true, force: true, },);
		}
	});

	it("reads a pointer whose lab dir stays inside the state root", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "live-pointer-",),);
		try {
			const labDir = path.join(root, "lab-0001",);
			await fs.mkdir(labDir, { recursive: true, },);
			const pointer: LabPointer = {
				dssUrl: credentials.url,
				manifest: path.join(labDir, "manifest.json",),
				labDir,
				updatedAt: "2026-01-01T00:00:00.000Z",
			};
			await fs.writeFile(path.join(root, "current.json",), JSON.stringify(pointer,),);
			const read = await readPointer(root,);
			expect(read?.labDir,).toBe(labDir,);
			expect(read?.manifest,).toBe(path.join(labDir, "manifest.json",),);
		} finally {
			await fs.rm(root, { recursive: true, force: true, },);
		}
	});

	it("rejects a pointer whose lab dir escapes the state root", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "live-pointer-",),);
		try {
			const outside = await fs.mkdtemp(path.join(os.tmpdir(), "live-outside-",),);
			const pointer: LabPointer = {
				dssUrl: credentials.url,
				manifest: path.join(outside, "manifest.json",),
				labDir: outside,
				updatedAt: "2026-01-01T00:00:00.000Z",
			};
			await fs.writeFile(path.join(root, "current.json",), JSON.stringify(pointer,),);
			await expect(readPointer(root,),).rejects.toThrow(/escapes the state root/,);
			await fs.rm(outside, { recursive: true, force: true, },);
		} finally {
			await fs.rm(root, { recursive: true, force: true, },);
		}
	});

	it("rejects a manifest outside an otherwise contained lab", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "live-pointer-",),);
		try {
			await fs.writeFile(
				path.join(root, "current.json",),
				JSON.stringify({
					dssUrl: credentials.url,
					labDir: path.join(root, "lab",),
					manifest: path.join(root, "foreign.json",),
				},),
			);
			await expect(readPointer(root,),).rejects.toThrow();
		} finally {
			await fs.rm(root, { recursive: true, force: true, },);
		}
	});
	it("rejects a malformed pointer with an actionable message", async () => {
		const root = await fs.mkdtemp(path.join(os.tmpdir(), "live-pointer-",),);
		try {
			await fs.writeFile(path.join(root, "current.json",), "{not json",);
			await expect(readPointer(root,),).rejects.toThrow(/not valid JSON/,);
		} finally {
			await fs.rm(root, { recursive: true, force: true, },);
		}
	});
});

describe("live selection preflight", () => {
	it("does not expand an empty explicit selection into a full run", () => {
		expect(() => parseLiveSuiteArgs(["all", "--case", " , ",],)).toThrow();
		expect(() => parseLiveSuiteArgs(["run", "--case", "core.*", "--case", "",],)).toThrow();
		expect(parseLiveSuiteArgs(["all", "--case", "", "--help",],).help,).toBe(true,);
	});
	it("rejects unknown, setup-only and inactive-profile selections", () => {
		for (const id of ["missing.*", "collab.setup.*", "ml.*",]) {
			expect(() => parseLiveSuiteArgs(["all", "--case", id,],)).toThrow();
		}
		expect(parseLiveSuiteArgs(["run", "--profile", "ml", "--case", "ml.*",],).cases,).toEqual([
			"ml.*",
		],);
		for (const verb of ["setup", "clean", "status",]) {
			expect(() => parseLiveSuiteArgs([verb, "--case", "core.*",],)).toThrow();
		}
	});
	it("rejects invalid all selections before requests or state creation", async () => {
		const dir = await fs.mkdtemp(path.join(os.tmpdir(), "live-invalid-selection-",),);
		let requests = 0;
		try {
			await withCliServer((_req, res,) => {
				requests++;
				sendJson(res, [],);
			}, async url => {
				const child = Bun.spawn([
					process.execPath,
					"--no-env-file",
					"scripts/live-suite.ts",
					"all",
					"--case",
					"missing.*",
					"--state-dir",
					path.join(dir, "state",),
				], {
					env: { ...process.env, DATAIKU_URL: url, DATAIKU_API_KEY: "offline-key", },
					stdout: "pipe",
					stderr: "pipe",
				},);
				await Promise.all([new Response(child.stdout,).text(), new Response(child.stderr,).text(),],);
				expect(await child.exited,).toBe(2,);
				expect(requests,).toBe(0,);
				expect(await fs.readdir(dir,),).toEqual([],);
			},);
		} finally {
			await fs.rm(dir, { recursive: true, force: true, },);
		}
	});
});

describe("live root preflight", () => {
	it("preserves permission errors instead of reporting a missing lab", async () => {
		await withCliServer((_req, res,) => {
			res.writeHead(403, { "Content-Type": "application/json", },);
			res.end(JSON.stringify({ message: "Forbidden", },),);
		}, async url => {
			const result = await verifyLiveRoot(
				new DataikuClient({ url, apiKey: "offline-key", retryMaxAttempts: 1, },),
				manifestWithProjects([{ key: "LAB", state: "bound", },],),
			).catch(error => error);
			expect(result,).toBeInstanceOf(DataikuError,);
			expect(result.status,).toBe(403,);
		},);
	});
	it("preserves transport failures and distinguishes absence from changed identity", async () => {
		for (const mode of ["transport", "missing", "changed",]) {
			await withCliServer((req, res,) => {
				if (mode === "transport") {
					req.socket.destroy();
					return;
				}
				if (mode === "missing") {
					res.writeHead(404, { "Content-Type": "application/json", },);
					res.end("{}",);
					return;
				}
				sendJson(res, {
					projectKey: "LAB",
					name: "replacement",
					owner: "ci",
					creationTag: { lastModifiedOn: 2, },
				},);
			}, async url => {
				const result = await verifyLiveRoot(
					new DataikuClient({ url, apiKey: "offline-key", retryMaxAttempts: 1, },),
					manifestWithProjects([{ key: "LAB", state: "bound", },],),
				).catch(error => error);
				if (mode === "transport") expect(result,).toBeInstanceOf(DataikuError,);
				else if (mode === "missing") expect(result.cause.status,).toBe(404,);
				else {
					expect(result,).toBeInstanceOf(Error,);
					expect(result.message,).toContain("identity",);
					expect(result.cause,).toBeUndefined();
				}
			},);
		}
	});
});

describe("failed cleanup recovery", () => {
	it("retains failure evidence and invalidates readiness when deletion is refused", async () => {
		const dir = await fs.mkdtemp(path.join(os.tmpdir(), "live-clean-failure-",),);
		const key = "SDK_LIVE_0123456789ABCDEF_ROOT_0";
		const methods: string[] = [];
		try {
			await withCliServer((req, res,) => {
				methods.push(req.method!,);
				sendJson(
					res,
					req.url?.endsWith("/projects/",) || req.url?.endsWith("/projects",)
						? [{ projectKey: key, },]
						: {
							projectKey: key,
							name: `replacement-${methods.length}`,
							owner: "ci",
							creationTag: { lastModifiedOn: 2, },
						},
				);
			}, async url => {
				const labDir = path.join(dir, "lab",);
				await fs.mkdir(labDir,);
				const manifestPath = path.join(labDir, "manifest.json",);
				const manifest = manifestWithProjects([{ key, state: "bound", },],);
				manifest.dssUrl = url;
				manifest.setupComplete = true;
				await fs.writeFile(manifestPath, JSON.stringify(manifest,),);
				const child = Bun.spawn([
					process.execPath,
					"--no-env-file",
					"scripts/live-suite.ts",
					"clean",
					"--manifest",
					manifestPath,
					"--state-dir",
					dir,
				], {
					env: { ...process.env, DATAIKU_URL: url, DATAIKU_API_KEY: "offline-key", },
					stdout: "pipe",
					stderr: "pipe",
				},);
				const [, stderr,] = await Promise.all([
					new Response(child.stdout,).text(),
					new Response(child.stderr,).text(),
				],);
				expect(await child.exited,).toBe(1,);
				expect(methods.every(method => method === "GET"),).toBe(true,);
				const report = JSON.parse(await fs.readFile(path.join(dir, "cleanup-report.json",), "utf8",),);
				expect(report.cleanup.status,).toBe("failed",);
				expect(report.integrity.verified,).toBe(false,);
				expect(report.integrity.changed,).toEqual([key,],);
				expect(stderr,).toContain(report.cleanup.errors[0],);
				const saved = JSON.parse(await fs.readFile(manifestPath, "utf8",),);
				expect(saved.setupComplete,).toBe(false,);
				expect(saved.projects[0].state,).toBe("bound",);
			},);
		} finally {
			await fs.rm(dir, { recursive: true, force: true, },);
		}
	});
});
