import { afterEach, describe, expect, it, } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { doctorEnvironment, type DoctorVersionInfo, } from "../../src/cli/doctor.js";
import {
	buildVersionPayload,
	cliVersionResult,
	detectRuntime,
	gitFullRevision,
	PACKAGE_ROOT,
	sourceTreeNewerThanBuild,
} from "../../src/cli/version.js";
import { DataikuClient, } from "../../src/client.js";
import { cliEnv, dss, SDK_ROOT, withCliServer, } from "./_harness.js";

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
