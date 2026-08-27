import { afterEach, describe, expect, it, } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { doctorEnvironment, type DoctorVersionInfo, } from "../../src/cli/doctor.js";
import {
	buildMetadataRevision,
	buildVersionPayload,
	cliVersionResult,
	detectRuntime,
	gitFullRevision,
	gitRevision,
	PACKAGE_ROOT,
	sourceTreeNewerThanBuild,
} from "../../src/cli/version.js";
import { DataikuClient, } from "../../src/client.js";
import {
	BUN,
	cliEnv,
	dss,
	dssFailure,
	exec,
	readFileSync,
	SDK_ROOT,
	withCliServer,
} from "./_harness.js";

const FORTY_A = "a".repeat(40,);
const FORTY_B = "b".repeat(40,);
const DATE_HEADER = "Tue, 25 Aug 2026 09:15:00 GMT";

/* ------------------------------------------------------------------ */
/*  Client: getWithMetadata                                            */
/* ------------------------------------------------------------------ */

describe("DataikuClient.getWithMetadata", () => {
	const originalFetch = globalThis.fetch;
	afterEach(() => {
		globalThis.fetch = originalFetch;
	},);

	it("returns parsed JSON plus selected headers case-insensitively", async () => {
		globalThis.fetch = (async () =>
			new Response(JSON.stringify({ projectKey: "TEST", },), {
				headers: {
					"dSS-vErsIon": "12.4.3",
					"dss-api-VERSION": "14.7.3",
					Date: DATE_HEADER,
					"X-Request-Id": "req-abc-123",
					Authorization: "Bearer secret-should-not-leak",
				},
			},)) as typeof fetch;
		const client = new DataikuClient({ url: "http://localhost", apiKey: "test-key", },);
		const result = await client.getWithMetadata<{ projectKey: string; }>(
			"/public/api/projects/",
		);
		expect(result.data,).toEqual({ projectKey: "TEST", },);
		expect(result.meta,).toEqual({
			dssVersion: "12.4.3",
			dssApiVersion: "14.7.3",
			date: DATE_HEADER,
			requestId: "req-abc-123",
		},);
		// Meta is a fixed non-sensitive whitelist: no authorization data escapes.
		expect(Object.keys(result.meta,),).toEqual([
			"dssVersion",
			"dssApiVersion",
			"date",
			"requestId",
		],);
		expect(JSON.stringify(result,),).not.toContain("secret-should-not-leak",);
		expect(JSON.stringify(result,),).not.toContain("Bearer",);
	});

	it("leaves absent headers null and never invents values", async () => {
		globalThis.fetch = (async () => new Response("[]",)) as typeof fetch;
		const client = new DataikuClient({ url: "http://localhost", apiKey: "test-key", },);
		const result = await client.getWithMetadata("/public/api/projects/",);
		expect(result.data,).toEqual([],);
		expect(result.meta,).toEqual({
			dssVersion: null,
			dssApiVersion: null,
			date: null,
			requestId: null,
		},);
	});
});

/* ------------------------------------------------------------------ */
/*  Doctor environment: documented header provenance                   */
/* ------------------------------------------------------------------ */

async function runDoctorCapabilities(headers: Record<string, string> | undefined,): Promise<{
	requestedUrls: string[];
	environment: Record<string, unknown> | undefined;
}> {
	const requestedUrls: string[] = [];
	let parsed: Record<string, unknown> = {};
	await withCliServer((req, res,) => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		requestedUrls.push(url.pathname,);
		// Node synthesizes a Date header by default; disable it so "absent"
		// really means absent and "present" is under test control.
		res.sendDate = false;
		res.setHeader("Content-Type", "application/json",);
		if (headers) {
			for (const [name, value,] of Object.entries(headers,)) res.setHeader(name, value,);
		}
		if (url.pathname === "/public/api/projects/") {
			res.end("[]",);
			return;
		}
		if (url.pathname === "/public/api/connections/get-names/") {
			res.end("[]",);
			return;
		}
		res.statusCode = 404;
		res.end(`unexpected request ${url.pathname}`,);
	}, async (url,) => {
		// Without a project key the doctor request surface stays minimal:
		// projects (connectivity) and connections (canMutateConnection probe).
		const env = cliEnv(url,);
		delete env.DATAIKU_PROJECT_KEY;
		const { stdout, stderr, } = await dss(["doctor", "--capabilities", "--fast",], { env, },);
		expect(stderr,).toBe("",);
		parsed = JSON.parse(stdout,) as Record<string, unknown>;
	},);
	return {
		requestedUrls,
		environment: parsed.environment as Record<string, unknown> | undefined,
	};
}

describe("doctor environment version provenance", () => {
	it("populates environment from documented headers on the project-list call", async () => {
		const { requestedUrls, environment, } = await runDoctorCapabilities({
			"DSS-Version": "12.4.3",
			"DSS-API-Version": "14.7.3",
			Date: DATE_HEADER,
		},);
		expect(environment?.dssVersion,).toBe("12.4.3",);
		expect(environment?.dssApiVersion,).toBe("14.7.3",);
		expect(environment?.instanceTime,).toBe(DATE_HEADER,);
		expect(requestedUrls,).toEqual([
			"/public/api/projects/",
			"/public/api/connections/get-names/",
		],);
	});

	it("reads the documented headers case-insensitively", async () => {
		const { environment, } = await runDoctorCapabilities({
			"dSS-vErsIon": "12.5.1",
			"dss-api-version": "14.8.2",
		},);
		expect(environment?.dssVersion,).toBe("12.5.1",);
		expect(environment?.dssApiVersion,).toBe("14.8.2",);
		// No Date header was served, so instanceTime stays absent.
		expect(environment?.instanceTime,).toBeUndefined();
	});

	it("keeps absent headers absent and never fabricates version info", async () => {
		const { requestedUrls, environment, } = await runDoctorCapabilities(undefined,);
		expect(environment,).toBeDefined();
		expect(environment?.dssVersion,).toBeUndefined();
		expect(environment?.dssApiVersion,).toBeUndefined();
		expect(environment?.instanceTime,).toBeUndefined();
		// Non-existent feature/version-4 endpoints are never probed.
		expect(requestedUrls.some((u,) => u.includes("features",)),).toBe(false,);
		expect(requestedUrls.some((u,) => u.includes("v4",)),).toBe(false,);
	});

	it("doctorEnvironment spreads only present header values", () => {
		const info: DoctorVersionInfo = {
			dssVersion: "12.4.3",
			dssApiVersion: null,
			instanceTime: DATE_HEADER,
		};
		const env = doctorEnvironment("TEST", info,);
		expect(env.dssVersion,).toBe("12.4.3",);
		expect(env.dssApiVersion,).toBeUndefined();
		expect(env.instanceTime,).toBe(DATE_HEADER,);

		const none = doctorEnvironment(undefined, {
			dssVersion: null,
			dssApiVersion: null,
			instanceTime: null,
		},);
		expect(none.dssVersion,).toBeUndefined();
		expect(none.dssApiVersion,).toBeUndefined();
		expect(none.instanceTime,).toBeUndefined();
	});
});
/* ------------------------------------------------------------------ */
/*  Doctor: malformed project-list payloads                            */
/* ------------------------------------------------------------------ */

async function runMalformedDoctor(body: string,): Promise<{
	code: number | null;
	stdout: string;
	stderr: string;
}> {
	let failure: { code: number | null; stdout: string; stderr: string; } | undefined;
	await withCliServer((req, res,) => {
		const url = new URL(req.url ?? "/", "http://localhost",);
		res.sendDate = false;
		res.setHeader("Content-Type", "application/json",);
		res.setHeader("DSS-Version", "12.4.3",);
		if (url.pathname === "/public/api/projects/") {
			res.setHeader("DSS-API-Version", "14.7.3",);
			res.end(body,);
			return;
		}
		if (url.pathname === "/public/api/connections/get-names/") {
			res.end("[]",);
			return;
		}
		res.statusCode = 404;
		res.end(`unexpected request ${url.pathname}`,);
	}, async (url,) => {
		const env = cliEnv(url,);
		delete env.DATAIKU_PROJECT_KEY;
		failure = await dssFailure(["doctor", "--capabilities", "--fast",], { env, },);
	},);
	return failure as { code: number | null; stdout: string; stderr: string; };
}

describe("doctor malformed project-list payloads", () => {
	it("never reports connectivity success and never crashes on a non-array body", async () => {
		for (const body of ['{"not":"an array"}', "null", '"projects"', "17",]) {
			const failure = await runMalformedDoctor(body,);
			expect(failure.stderr,).toBe("",);
			expect(failure.code,).toBe(2,);
			const parsed = JSON.parse(failure.stdout,) as {
				ok: boolean;
				checks: Array<{ name: string; ok: boolean; details?: Record<string, unknown>; }>;
				environment?: Record<string, unknown>;
			};
			expect(parsed.ok,).toBe(false,);
			const connectivity = parsed.checks.find((check,) => check.name === "connectivity");
			expect(connectivity?.ok,).toBe(false,);
			expect(connectivity?.details?.["reason"],).toBe("invalid_project_list",);
			expect(connectivity?.details?.["errorCount"],).toBeGreaterThan(0,);
			// The documented headers on the same response still populate the
			// environment, independent of body shape.
			expect(parsed.environment?.dssVersion,).toBe("12.4.3",);
			expect(parsed.environment?.dssApiVersion,).toBe("14.7.3",);
		}
	});
});

/* ------------------------------------------------------------------ */
/*  Version provenance payload                                         */
/* ------------------------------------------------------------------ */

describe("buildVersionPayload provenance", () => {
	it("source execution never claims a build revision", () => {
		const payload = buildVersionPayload({
			packageVersion: "1.0.0",
			checkoutRevision: "abc1234",
			checkoutFullRevision: FORTY_A,
			loadSource: "source",
			buildRevision: FORTY_A,
			runtime: "bun",
		},);
		expect(payload,).toMatchObject({
			version: "1.0.0",
			gitRevision: "abc1234",
			source: "source",
			runtime: "bun",
			buildRevision: null,
			staleBuild: false,
		},);
	});

	it("dist retains its build revision and flags staleness against the checkout", () => {
		const stale = buildVersionPayload({
			packageVersion: "1.0.0",
			checkoutRevision: "aaa1111",
			checkoutFullRevision: FORTY_A,
			loadSource: "dist",
			buildRevision: FORTY_B,
			runtime: "node",
		},);
		expect(stale.source,).toBe("dist",);
		expect(stale.buildRevision,).toBe(FORTY_B,);
		expect(stale.staleBuild,).toBe(true,);
		expect(stale.runtime,).toBe("node",);

		const current = buildVersionPayload({
			packageVersion: "1.0.0",
			checkoutRevision: "aaa1111",
			checkoutFullRevision: FORTY_B,
			loadSource: "dist",
			buildRevision: FORTY_B,
			runtime: "bun",
		},);
		expect(current.staleBuild,).toBe(false,);
	});

	it("flags a dist stale when source changed after a same-revision build", () => {
		const payload = buildVersionPayload({
			packageVersion: "1.0.0",
			checkoutRevision: "aaa1111",
			checkoutFullRevision: FORTY_A,
			loadSource: "dist",
			buildRevision: FORTY_A,
			sourceNewerThanBuild: true,
			runtime: "node",
		},);
		expect(payload.staleBuild,).toBe(true,);
	});

	it("compares source mtimes with the build-completion marker", () => {
		const root = mkdtempSync(join(tmpdir(), "dss-version-",),);
		try {
			const sourceDirectory = join(root, "src", "cli",);
			const sourceFile = join(sourceDirectory, "version.ts",);
			const distDirectory = join(root, "dist",);
			const buildMarker = join(distDirectory, "build-metadata.json",);
			mkdirSync(sourceDirectory, { recursive: true, },);
			mkdirSync(distDirectory, { recursive: true, },);
			writeFileSync(sourceFile, "export const version = 1;\n",);
			writeFileSync(buildMarker, '{"format":1}\n',);

			const buildTime = new Date(Date.now() + 10_000,);
			utimesSync(buildMarker, buildTime, buildTime,);
			expect(sourceTreeNewerThanBuild(root,),).toBe(false,);

			const changedTime = new Date(buildTime.getTime() + 10_000,);
			utimesSync(sourceFile, changedTime, changedTime,);
			expect(sourceTreeNewerThanBuild(root,),).toBe(true,);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("does not flag an installed package that omits the source tree", () => {
		const root = mkdtempSync(join(tmpdir(), "dss-version-",),);
		try {
			const distDirectory = join(root, "dist",);
			mkdirSync(distDirectory, { recursive: true, },);
			writeFileSync(join(distDirectory, "build-metadata.json",), '{"format":1}\n',);
			expect(sourceTreeNewerThanBuild(root,),).toBe(false,);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("never claims staleness without a comparable checkout revision", () => {
		const payload = buildVersionPayload({
			packageVersion: "1.0.0",
			checkoutRevision: undefined,
			checkoutFullRevision: undefined,
			loadSource: "dist",
			buildRevision: FORTY_A,
			runtime: "bun",
		},);
		expect(payload.gitRevision,).toBeNull();
		expect(payload.buildRevision,).toBe(FORTY_A,);
		expect(payload.staleBuild,).toBe(false,);
	});

	it("live payload reports source execution with real checkout revision", () => {
		const payload = cliVersionResult();
		expect(payload.source,).toBe("source",);
		expect(payload.buildRevision,).toBeNull();
		expect(payload.staleBuild,).toBe(false,);
		expect(payload.runtime,).toBe(detectRuntime(),);
		expect(payload.gitRevision,).toEqual(gitFullRevision(PACKAGE_ROOT,)?.slice(0, 7,) ?? null,);
	});
});
/* ------------------------------------------------------------------ */
/*  Git revision resolution: packed refs, worktrees, validation        */
/* ------------------------------------------------------------------ */

describe("git revision resolution", () => {
	const FORTY_41 = "a".repeat(41,);

	function gitRoot(): string {
		return mkdtempSync(join(tmpdir(), "dss-git-",),);
	}

	it("resolves a detached HEAD revision as-is", () => {
		const root = gitRoot();
		try {
			mkdirSync(join(root, ".git",), { recursive: true, },);
			writeFileSync(join(root, ".git", "HEAD",), `${FORTY_A}\n`,);
			expect(gitFullRevision(root,),).toBe(FORTY_A,);
			expect(gitRevision(root,),).toBe(FORTY_A.slice(0, 7,),);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("resolves revisions from packed-refs, skipping comments and peeled lines", () => {
		const root = gitRoot();
		try {
			const gitDir = join(root, ".git",);
			mkdirSync(join(gitDir, "refs", "heads",), { recursive: true, },);
			writeFileSync(join(gitDir, "HEAD",), "ref: refs/heads/main\n",);
			writeFileSync(join(gitDir, "refs", "heads", "other",), `${FORTY_B}\n`,);
			writeFileSync(
				join(gitDir, "packed-refs",),
				[
					"# pack-refs with: peeled fully-peeled sorted",
					`${FORTY_B} refs/heads/other`,
					`${FORTY_B} refs/heads/tagged`,
					`^${FORTY_A}`,
					`${FORTY_A} refs/heads/main`,
				].join("\n",) + "\n",
			);
			expect(gitFullRevision(root,),).toBe(FORTY_A,);
			expect(gitRevision(root,),).toBe(FORTY_A.slice(0, 7,),);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("resolves revisions through a linked worktree commondir", () => {
		const root = gitRoot();
		try {
			const mainGit = join(root, ".git",);
			const worktreeGit = join(mainGit, "worktrees", "wt",);
			const checkout = join(root, "wt",);
			mkdirSync(join(mainGit, "refs", "heads",), { recursive: true, },);
			mkdirSync(worktreeGit, { recursive: true, },);
			mkdirSync(checkout, { recursive: true, },);
			writeFileSync(join(checkout, ".git",), `gitdir: ${worktreeGit}\n`,);
			writeFileSync(join(worktreeGit, "HEAD",), "ref: refs/heads/feature\n",);
			writeFileSync(join(worktreeGit, "commondir",), "../..\n",);
			writeFileSync(join(mainGit, "refs", "heads", "feature",), `${FORTY_B}\n`,);
			expect(gitFullRevision(checkout,),).toBe(FORTY_B,);

			// The shared store works through packed-refs as well.
			rmSync(join(mainGit, "refs", "heads", "feature",),);
			writeFileSync(join(mainGit, "packed-refs",), `${FORTY_A} refs/heads/feature\n`,);
			expect(gitFullRevision(checkout,),).toBe(FORTY_A,);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("rejects revisions that are not full lowercase hexadecimal", () => {
		const root = gitRoot();
		try {
			const gitDir = join(root, ".git",);
			mkdirSync(join(gitDir, "refs", "heads",), { recursive: true, },);
			writeFileSync(join(gitDir, "refs", "heads", "evil",), `${FORTY_B}\n`,);
			const headPath = join(gitDir, "HEAD",);
			const cases: Array<[string, string | undefined,]> = [
				[`${FORTY_A.toUpperCase()}\n`, undefined,],
				[`${FORTY_A.slice(0, 39,)}\n`, undefined,],
				[`${FORTY_41}\n`, undefined,],
				["garbage\n", undefined,],
				["\n", undefined,],
				["ref: refs/heads/missing\n", undefined,],
				["ref: ../outside\n", undefined,],
				["ref: refs/heads/evil\n", FORTY_B,],
			];
			for (const [head, expected,] of cases) {
				writeFileSync(headPath, head,);
				expect(gitFullRevision(root,),).toBe(expected,);
				expect(gitRevision(root,),).toBe(expected?.slice(0, 7,),);
			}
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("buildMetadataRevision accepts only a full lowercase hex revision", () => {
		const root = gitRoot();
		try {
			const metadataPath = join(root, "dist", "build-metadata.json",);
			mkdirSync(join(root, "dist",), { recursive: true, },);
			writeFileSync(metadataPath, JSON.stringify({ buildRevision: FORTY_A, },),);
			expect(buildMetadataRevision(root,),).toBe(FORTY_A,);
			for (
				const invalid of [
					FORTY_A.toUpperCase(),
					FORTY_41,
					FORTY_A.slice(0, 39,),
					"not a revision",
					"",
					42,
				]
			) {
				writeFileSync(metadataPath, JSON.stringify({ buildRevision: invalid, },),);
				expect(buildMetadataRevision(root,),).toBeUndefined();
			}
			writeFileSync(metadataPath, "not json",);
			expect(buildMetadataRevision(root,),).toBeUndefined();
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});
});

/* ------------------------------------------------------------------ */
/*  dss version subprocess: provenance in the actual CLI               */
/* ------------------------------------------------------------------ */

describe("dss version provenance", () => {
	const runtime = process.versions.bun ? "bun" : "node";

	it("reports source provenance with no build revision and no paths", async () => {
		const { stdout, stderr, } = await dss(["version",],);
		expect(stderr,).toBe("",);
		const payload = JSON.parse(stdout,) as Record<string, string | boolean | null>;
		expect(payload.source,).toBe("source",);
		expect(payload.runtime,).toBe(runtime,);
		expect(payload.buildRevision,).toBeNull();
		expect(payload.staleBuild,).toBe(false,);
		expect(payload.gitRevision,).toEqual(expect.any(String,),);
		// No absolute user paths anywhere in the report.
		const serialized = JSON.stringify(payload,);
		expect(serialized,).not.toContain(SDK_ROOT,);
		expect(serialized,).not.toMatch(/"\/Users\//,);
		expect(serialized,).not.toMatch(/"\/home\//,);
		expect(serialized,).not.toContain("file://",);
	});

	it("retains the dist build revision and flags a stale dist", async () => {
		const fakeBuild = "f".repeat(40,);
		const { stdout, } = await dss(["version",], {
			env: {
				...process.env,
				DSS_LOAD_SOURCE: "dist",
				DSS_BUILD_REVISION: fakeBuild,
			},
		},);
		const payload = JSON.parse(stdout,) as Record<string, string | boolean | null>;
		expect(payload.source,).toBe("dist",);
		expect(payload.buildRevision,).toBe(fakeBuild,);
		expect(payload.staleBuild,).toBe(true,);
	});
});
/* ------------------------------------------------------------------ */
/*  dss version provenance: corrupt or inherited build revisions       */
/* ------------------------------------------------------------------ */

describe("dss version provenance rejects corrupt revisions", () => {
	it("never claims a corrupt or inherited revision as build provenance", async () => {
		for (const corrupt of ["garbage", "A".repeat(40,), "a".repeat(41,), "0".repeat(39,),]) {
			const { stdout, } = await dss(["version",], {
				env: {
					...process.env,
					DSS_LOAD_SOURCE: "dist",
					DSS_BUILD_REVISION: corrupt,
				},
			},);
			const payload = JSON.parse(stdout,) as Record<string, string | boolean | null>;
			expect(payload.source,).toBe("dist",);
			expect(payload.buildRevision,).toBeNull();
			expect(payload.staleBuild,).toBe(false,);
		}
	});
});

/* ------------------------------------------------------------------ */
/*  bin/dss.js: build revision forwarding                              */
/* ------------------------------------------------------------------ */

describe("bin/dss.js build revision forwarding", () => {
	async function runLauncher(
		metadata: string | null,
		inherited: string | undefined,
	): Promise<{ revision: string | null; source: string | null; }> {
		const root = mkdtempSync(join(tmpdir(), "dss-bin-",),);
		try {
			mkdirSync(join(root, "bin",), { recursive: true, },);
			mkdirSync(join(root, "dist", "src",), { recursive: true, },);
			writeFileSync(
				join(root, "bin", "dss.js",),
				readFileSync(join(SDK_ROOT, "bin", "dss.js",), "utf-8",),
			);
			writeFileSync(
				join(root, "dist", "src", "cli.js",),
				"process.stdout.write(JSON.stringify({revision: process.env.DSS_BUILD_REVISION ?? null, source: process.env.DSS_LOAD_SOURCE ?? null}));",
			);
			if (metadata !== null) {
				writeFileSync(join(root, "dist", "build-metadata.json",), metadata,);
			}
			const env: NodeJS.ProcessEnv = { ...process.env, };
			delete env.DSS_BUILD_REVISION;
			if (inherited !== undefined) env.DSS_BUILD_REVISION = inherited;
			const { stdout, } = await exec(BUN, [join(root, "bin", "dss.js",), "version",], {
				cwd: root,
				env,
			},);
			return JSON.parse(stdout,) as { revision: string | null; source: string | null; };
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	}

	it("forwards a full lowercase hexadecimal metadata revision", async () => {
		const child = await runLauncher(
			JSON.stringify({ buildRevision: FORTY_A, },),
			"inherited-garbage",
		);
		expect(child.source,).toBe("dist",);
		expect(child.revision,).toBe(FORTY_A,);
	});

	it("rejects corrupt metadata revisions instead of forwarding them", async () => {
		const child = await runLauncher(
			JSON.stringify({ buildRevision: "a".repeat(41,), },),
			"inherited-garbage",
		);
		expect(child.source,).toBe("dist",);
		expect(child.revision,).toBeNull();
	});

	it("clears inherited revisions when dist metadata is absent", async () => {
		const child = await runLauncher(null, "inherited-garbage",);
		expect(child.source,).toBe("dist",);
		expect(child.revision,).toBeNull();
	});
});
