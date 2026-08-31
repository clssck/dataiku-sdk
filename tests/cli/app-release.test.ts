import { describe, expect, it, } from "bun:test";
import { projectIncarnationHash, } from "../../src/utils/project-incarnation.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readBody,
	readFileExists,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
} from "./_harness.js";
import type { IncomingMessage, ServerResponse, } from "./_harness.js";

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
const PERMISSIONS = { permissions: [{ user: { login: "alice", }, admin: true, },], };
const DONE_FUTURE = {
	jobId: "job-9",
	hasResult: true,
	alive: false,
	result: { projectKey: "NEW_INSTANCE", },
};
const NEW_INSTANCE_DETAILS = {
	projectKey: "NEW_INSTANCE",
	name: "New instance",
	projectAppType: "APP_INSTANCE",
	creationTag: { versionNumber: 1, lastModifiedOn: 1_700_000_000_000, },
};
const NEW_INSTANCE_INCARNATION_HASH = projectIncarnationHash(
	"NEW_INSTANCE",
	NEW_INSTANCE_DETAILS,
)!;

describe("app manifest-version and set-manifest-version", () => {
	it("reads the raw manifest version state of a project", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["app", "manifest-version", "--project-key", "MYAPP_TEMPLATE",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				projectKey: "MYAPP_TEMPLATE",
				projectAppType: "APP_TEMPLATE",
				version: "2.0.0",
				versionNotes: null,
			},);
			expect(typeof result.manifestHash,).toBe("string",);
			expect(result.appVersion,).toBeUndefined();
			expect(result.version,).not.toBe("N/A",);
		},);
	});

	it("PUTs the whole manifest and reports the public transport without a publish claim", async () => {
		let putBody: Record<string, unknown> | undefined;
		let gets = 0;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				gets += 1;
				sendJson(
					res,
					gets === 1
						? { ...TEMPLATE_MANIFEST, version: "1.0.0", versionNotes: "Draft", }
						: putBody,
				);
				return;
			}
			if (
				req.method === "PUT" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				putBody = JSON.parse((await readBody(req,)) || "{}",) as Record<string, unknown>;
				sendJson(res, {},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						[
							"app",
							"set-manifest-version",
							"--manifest-version",
							"2.0.0",
							"--version-notes",
							"",
							"--project-key",
							"MYAPP_TEMPLATE",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				persisted: true,
				changed: true,
				publicationTransport: "public-app-manifest",
				uiPublicationVerified: false,
			},);
			expect(result.published,).toBeUndefined();
			expect(result.ready,).toBeUndefined();
			expect(putBody?.version,).toBe("2.0.0",);
			expect(putBody?.versionNotes,).toBe("",);
			expect(putBody?.homepageSections,).toEqual([],);
			expect(putBody?.projectKey,).toBe("MYAPP_TEMPLATE",);
		},);
	});

	it("surfaces an indeterminate version write as exit 2 with the result in error details", async () => {
		let gets = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				gets += 1;
				if (gets >= 2) {
					sendJson(res, { message: "post-write read failure", }, 500,);
					return;
				}
				sendJson(res, { ...TEMPLATE_MANIFEST, version: "1.0.0", },);
				return;
			}
			if (
				req.method === "PUT" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				sendJson(res, {},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"set-manifest-version",
					"--manifest-version",
					"2.0.0",
					"--project-key",
					"MYAPP_TEMPLATE",
					"--retries",
					"1",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(2,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				category: string;
				exitCode: number;
				details: { result: Record<string, unknown>; };
			};
			expect(report.code,).toBe("ambiguous_outcome",);
			expect(report.category,).toBe("dss",);
			expect(report.exitCode,).toBe(2,);
			expect(report.details.result,).toMatchObject({
				outcome: "indeterminate",
				persisted: null,
				after: null,
				publicationTransport: "public-app-manifest",
				uiPublicationVerified: false,
			},);
			expect(report.details.result.published,).toBeUndefined();
		},);
	});

	it("dry-run reads once and never issues a PUT", async () => {
		let puts = 0;
		let gets = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/MYAPP_TEMPLATE/app-manifest"
			) {
				gets += 1;
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "PUT") {
				puts += 1;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						[
							"app",
							"set-manifest-version",
							"--manifest-version",
							"3.0.0",
							"--dry-run",
							"--project-key",
							"MYAPP_TEMPLATE",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result.dryRun,).toBe(true,);
			expect(result.persisted,).toBe(false,);
			expect(gets,).toBe(1,);
			expect(puts,).toBe(0,);
		},);
	});

	it("rejects a call with no update flag before any request", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "set-manifest-version", "--project-key", "MYAPP_TEMPLATE",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("At least one of --manifest-version or --version-notes",);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("plans a manifest version write offline with a lowercased stale-read guard", async () => {
		let requestCount = 0;
		const expectHash = "ABCDEF0123456789".repeat(4,);
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(
					await dss(
						[
							"app",
							"set-manifest-version",
							"--manifest-version",
							"3.0.0",
							"--expect-hash",
							expectHash,
							"--project-key",
							"MYAPP_TEMPLATE",
							"--plan",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "set-manifest-version",
				projectKey: "MYAPP_TEMPLATE",
				method: "PUT",
				endpoint: "/public/api/projects/MYAPP_TEMPLATE/app-manifest",
				expectHash: expectHash.toLowerCase(),
				concurrencyControl: "client-side-non-atomic-stale-read-check",
				staleReadCheck: "client-side-expect-hash-compare-before-put",
				idempotency: "none",
				async: "none",
			},);
			// The plan advertises the patch it would merge, never a whole manifest
			// body: the PUT payload is only knowable after the pre-write read.
			expect(plan.payloadPatch,).toEqual({ version: "3.0.0", },);
			expect(plan.payload,).toBeUndefined();
			expect(plan.plannedAndDryRun,).toBeUndefined();
			expect(plan.exitCodesOnFailure,).toEqual({ usage: 1, error: 2, transient: 3, },);
		},);
		expect(requestCount,).toBe(0,);
	});
});

describe("app verify-instance API readiness", () => {
	it("verifies the required API checks and marks the UI gate as pending", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, INSTANCE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				apiReady: true,
				status: "API_VERIFIED_UI_PENDING",
				appId: "MYAPP",
				projectKey: "RELEASE_INSTANCE",
				valid: true,
				instanceVersion: "2.0.0",
				templateVersion: "2.0.0",
				expectedVersionSource: "template-manifest",
			},);
			expect(result.ready,).toBeUndefined();
			expect(result.published,).toBeUndefined();
			expect(
				(result.checks as Array<{ check: string; status: string; }>).map((entry,) =>
					`${entry.check}:${entry.status}`
				),
			).toEqual([
				"project-type:ok",
				"app-registration:ok",
				"manifest-version:ok",
				"manifest-references:ok",
			],);
			expect(result.requiredExternalGates,).toEqual([
				expect.objectContaining({
					gate: "visual-ui",
					status: "required",
					requiredAuthentication: "pre-authenticated-sso-or-dedicated-ui-test-identity",
					evidenceRequired:
						"Open the instance in the Dataiku Apps UI and exercise the affected tiles, forms, and actions to confirm the release behaves as intended.",
				},),
			],);
		},);
	});

	it("fails version verification against --expect-version without claiming readiness", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, INSTANCE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"verify-instance",
					"MYAPP",
					"--project-key",
					"RELEASE_INSTANCE",
					"--expect-version",
					"3.0.0",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				details: {
					result: {
						expectedVersionSource: string;
						checks: Array<{ check: string; status: string; expected: unknown; actual: unknown; }>;
					};
				};
			};
			expect(report.code,).toBe("validation_failed",);
			expect(report.details.result.expectedVersionSource,).toBe("expect-version",);
			const versionCheck = report.details.result.checks.find((entry,) =>
				entry.check === "manifest-version"
			);
			expect(versionCheck,).toMatchObject({ status: "failed", expected: "3.0.0", actual: "2.0.0", },);
			expect(failure.stdout,).not.toContain("apiReady",);
			expect(failure.stdout,).not.toContain('"ready":true',);
		},);
	});

	it("treats a missing template version as a failed required check", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, {
					projectKey: "MYAPP_TEMPLATE",
					projectAppType: "APP_TEMPLATE",
					homepageSections: [],
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, INSTANCE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: { checks: Array<{ check: string; status: string; expected: unknown; }>; }; };
			};
			const versionCheck = report.details.result.checks.find((entry,) =>
				entry.check === "manifest-version"
			);
			expect(versionCheck,).toMatchObject({ status: "failed", expected: null, },);
		},);
	});

	it("reads one manifest snapshot and never fetches project details when the snapshot carries its type", async () => {
		let manifestReads = 0;
		let projectDetailReads = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				manifestReads += 1;
				sendJson(res, INSTANCE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/") {
				projectDetailReads += 1;
				sendJson(res, { projectKey: "RELEASE_INSTANCE", projectAppType: "APP_INSTANCE", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(result.valid,).toBe(true,);
			expect(result.instanceManifestHash,).toBeTruthy();
		},);
		expect(manifestReads,).toBe(1,);
		expect(projectDetailReads,).toBe(0,);
	});

	it("fetches project details exactly once when the manifest snapshot omits its type", async () => {
		let manifestReads = 0;
		let projectDetailReads = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				manifestReads += 1;
				const { projectAppType: _omitted, ...rest } = INSTANCE_MANIFEST as Record<string, unknown>;
				sendJson(res, rest,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/") {
				projectDetailReads += 1;
				sendJson(res, {
					projectKey: "RELEASE_INSTANCE",
					name: "Release",
					projectAppType: "APP_INSTANCE",
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(await dss(["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",], {
					env: cliEnv(url,),
				},)).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				valid: true,
				projectAppType: "APP_INSTANCE",
				instanceVersion: "2.0.0",
			},);
		},);
		expect(manifestReads,).toBe(1,);
		expect(projectDetailReads,).toBe(1,);
	});
	for (
		const scenario of [
			{
				label: "the project is not registered to the app",
				instances: [] as Array<{ projectKey: string; }>,
				manifest: INSTANCE_MANIFEST,
				check: "app-registration",
				expected: "MYAPP",
				actual: null,
			},
			{
				label: "the manifest identifies an app template instead of an instance",
				instances: [{ projectKey: "RELEASE_INSTANCE", },],
				manifest: { ...INSTANCE_MANIFEST, projectAppType: "APP_TEMPLATE", },
				check: "project-type",
				expected: "APP_INSTANCE",
				actual: "APP_TEMPLATE",
			},
		]
	) {
		it(`fails required verification when ${scenario.label}`, async () => {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
					sendJson(res, TEMPLATE_MANIFEST,);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
					sendJson(res, scenario.instances,);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
				) {
					sendJson(res, scenario.manifest,);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					code: string;
					details: {
						result: {
							valid: boolean;
							checks: Array<{
								check: string;
								status: string;
								expected: unknown;
								actual: unknown;
							}>;
						};
					};
				};
				expect(report.code,).toBe("validation_failed",);
				expect(report.details.result.valid,).toBe(false,);
				expect(
					report.details.result.checks.find((entry,) => entry.check === scenario.check),
				).toMatchObject({
					status: "failed",
					expected: scenario.expected,
					actual: scenario.actual,
				},);
			},);
		});
	}

	it("fails verification when the instance manifest contains a missing reference", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, {
					...INSTANCE_MANIFEST,
					homepageSections: [{
						tiles: [{ type: "SCENARIO_RUN", scenarioId: "MISSING_SCENARIO", },],
					},],
				},);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/RELEASE_INSTANCE/scenarios/"
			) {
				sendJson(res, [],);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: {
					result: {
						valid: boolean;
						referenceValidation: {
							valid: boolean;
							errors: Array<{ code: string; path: string; }>;
						};
						checks: Array<{ check: string; status: string; actual: unknown; }>;
					};
				};
			};
			const result = report.details.result;
			expect(result.valid,).toBe(false,);
			expect(result.referenceValidation.valid,).toBe(false,);
			expect(result.referenceValidation.errors,).toContainEqual(
				expect.objectContaining({
					code: "MISSING_SCENARIO",
					path: '$["homepageSections"][0]["tiles"][0]["scenarioId"]',
				},),
			);
			expect(
				result.checks.find((entry,) => entry.check === "manifest-references"),
			).toMatchObject({ status: "failed", actual: false, },);
		},);
	});

	it("reports null snapshot checks when DSS cannot read the instance manifest", async () => {
		let projectDetailReads = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET"
				&& url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, { message: "Not an app project", }, 400,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/") {
				projectDetailReads += 1;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "verify-instance", "MYAPP", "--project-key", "RELEASE_INSTANCE",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: {
					result: {
						valid: boolean;
						projectAppType: unknown;
						instanceManifestHash: unknown;
						referenceValidation: unknown;
						checks: Array<{ check: string; status: string; actual: unknown; }>;
					};
				};
			};
			const result = report.details.result;
			expect(result,).toMatchObject({
				valid: false,
				projectAppType: null,
				instanceManifestHash: null,
				referenceValidation: null,
			},);
			expect(
				result.checks.filter((entry,) =>
					entry.check === "project-type" || entry.check === "manifest-references"
				),
			).toEqual([
				expect.objectContaining({ check: "project-type", status: "failed", actual: null, },),
				expect.objectContaining({ check: "manifest-references", status: "failed", actual: null, },),
			],);
		},);
		expect(projectDetailReads,).toBe(0,);
	});

	it("fails a whitespace-only template version even when --expect-version is supplied", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, { ...TEMPLATE_MANIFEST, version: "   ", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "RELEASE_INSTANCE", },],);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/RELEASE_INSTANCE/app-manifest"
			) {
				sendJson(res, INSTANCE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"verify-instance",
					"MYAPP",
					"--project-key",
					"RELEASE_INSTANCE",
					"--expect-version",
					"2.0.0",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: {
					result: {
						templateVersion: string | null;
						expectedVersion: string | null;
						checks: Array<{
							check: string;
							status: string;
							expected: unknown;
							actual: unknown;
							reason?: string;
						}>;
					};
				};
			};
			expect(report.details.result.templateVersion,).toBeNull();
			expect(report.details.result.expectedVersion,).toBe("2.0.0",);
			const versionCheck = report.details.result.checks.find((entry,) =>
				entry.check === "manifest-version"
			);
			expect(versionCheck,).toMatchObject({
				status: "failed",
				expected: "2.0.0",
				actual: "2.0.0",
				reason: "template-version-missing",
			},);
		},);
	});
});

describe("app create-successor-instance", () => {
	type Route = (req: IncomingMessage, res: ServerResponse,) => void;

	/** Standard successor routes; overrides replace one route for failure staging. */
	function successorServer(
		overrides: {
			created?: boolean;
			template?: Record<string, unknown>;
			instanceManifest?: Record<string, unknown>;
			post?: Route;
			future?: Route;
			targetGet?: Route;
			projectsList?: Route;
			sourceManifest?: Route;
			scenarios?: Route;
			permissionsRead?: Route;
			sourcePermissionsRead?: Route;
			permissionsPut?: Route;
		} = {},
	): Route {
		const {
			created = true,
			template = TEMPLATE_MANIFEST,
			instanceManifest = { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", },
		} = overrides;
		let targetGets = 0;
		let instanceListGets = 0;
		return (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				instanceListGets += 1;
				sendJson(
					res,
					created && instanceListGets > 1
						? [{ projectKey: "OLD_INSTANCE", }, { projectKey: "NEW_INSTANCE", },]
						: [{ projectKey: "OLD_INSTANCE", },],
				);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				if (overrides.sourceManifest) {
					overrides.sourceManifest(req, res,);
					return;
				}
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", version: "1.0.0", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/permissions") {
				if (overrides.sourcePermissionsRead) {
					overrides.sourcePermissionsRead(req, res,);
					return;
				}
				sendJson(res, PERMISSIONS,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/") {
				if (overrides.projectsList) {
					overrides.projectsList(req, res,);
					return;
				}
				sendJson(res, [],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				if (overrides.targetGet) {
					overrides.targetGet(req, res,);
					return;
				}
				targetGets += 1;
				if (targetGets === 1) {
					sendJson(res, { message: "Project not found", }, 404,);
				} else {
					sendJson(res, NEW_INSTANCE_DETAILS,);
				}
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, template,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/MYAPP/scenarios/") {
				if (overrides.scenarios) {
					overrides.scenarios(req, res,);
					return;
				}
			}
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				if (overrides.post) {
					overrides.post(req, res,);
					return;
				}
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
				sendJson(res, instanceManifest,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/permissions") {
				if (overrides.permissionsRead) {
					overrides.permissionsRead(req, res,);
					return;
				}
				sendJson(res, {},);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/NEW_INSTANCE/permissions") {
				if (overrides.permissionsPut) {
					overrides.permissionsPut(req, res,);
					return;
				}
				sendJson(res, {},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		};
	}

	/** Extract the failed-wait result from the CLI error report on stdout. */
	function failedResult(failure: { stdout: string; },): Record<string, unknown> {
		const report = JSON.parse(failure.stdout,) as {
			details: { result: Record<string, unknown>; };
		};
		return report.details.result;
	}
	/** Extract the CLI error-report envelope printed on stdout for a refused command. */
	function errorReport(failure: { stdout: string; },): {
		code: string;
		category: string;
		hint?: string;
		details?: Record<string, unknown>;
	} {
		return JSON.parse(failure.stdout,) as {
			code: string;
			category: string;
			hint?: string;
			details?: Record<string, unknown>;
		};
	}

	it("dry-run completes preflight without POSTing and reports the additive plan", async () => {
		let posts = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") posts += 1;
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(res, { message: "Project not found", }, 404,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--dry-run",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				dryRun: true,
				additive: true,
				sourcePreserved: true,
				uiPublicationVerified: false,
				templateVersion: "2.0.0",
				preflight: "passed",
				source: { projectKey: "OLD_INSTANCE", projectAppType: "APP_INSTANCE", },
				target: { projectKey: "NEW_INSTANCE", exists: false, },
			},);
			expect(result.projectKey,).toBeUndefined();
			expect(posts,).toBe(0,);
		},);
	});
	it("preflights the release before any version or creation mutation", async () => {
		let mutationRequests = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" || req.method === "PUT" || req.method === "DELETE") {
				mutationRequests += 1;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(res, { message: "Project not found", }, 404,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/permissions") {
				sendJson(res, PERMISSIONS,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						[
							"app",
							"successor-preflight",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--name",
							"Release 2",
							"--copy-permissions",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				action: "successor-preflight",
				preflight: "passed",
				preflightExecuted: true,
				creationPostAttempted: false,
				versionMutationAttempted: false,
				appId: "MYAPP",
				source: { projectKey: "OLD_INSTANCE", projectAppType: "APP_INSTANCE", },
				target: { projectKey: "NEW_INSTANCE", name: "Release 2", exists: false, },
				targetPreflight: "confirmed-absent",
				template: {
					projectKey: "MYAPP",
					version: "2.0.0",
					referenceValidation: { valid: true, },
				},
				copyPermissions: true,
			},);
			const template = result.template as { manifestHash: string; };
			const next = result.next as { setVersion: string; createSuccessor: string; };
			expect(template.manifestHash,).toMatch(/^[0-9a-f]{64}$/,);
			expect(next.setVersion,).toContain(`--expect-hash ${template.manifestHash}`,);
			expect(next.createSuccessor,).toContain("--copy-permissions",);
			expect(mutationRequests,).toBe(0,);
		},);
	});
	for (
		const scenario of [
			{
				label: "cannot read the predecessor as an app project",
				sourceManifest: (_req: IncomingMessage, res: ServerResponse,) => {
					sendJson(res, { message: "Project not found", }, 404,);
				},
				expectedDetails: { sourceProjectKey: "OLD_INSTANCE", },
				errorFragment: "could not be read as an app project",
			},
			{
				label: "finds a template where an app instance is required",
				sourceManifest: (_req: IncomingMessage, res: ServerResponse,) => {
					sendJson(res, {
						...INSTANCE_MANIFEST,
						projectKey: "OLD_INSTANCE",
						projectAppType: "APP_TEMPLATE",
						version: "1.0.0",
					},);
				},
				expectedDetails: {
					sourceProjectKey: "OLD_INSTANCE",
					projectAppType: "APP_TEMPLATE",
				},
				errorFragment: "not a classic Dataiku App instance",
			},
		]
	) {
		it(`refuses preflight when it ${scenario.label}`, async () => {
			let mutationRequests = 0;
			const route = successorServer({ sourceManifest: scenario.sourceManifest, },);
			await withCliServer((req, res,) => {
				if (req.method === "POST" || req.method === "PUT" || req.method === "DELETE") {
					mutationRequests += 1;
				}
				route(req, res,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"successor-preflight",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				const report = errorReport(failure,);
				expect(report,).toMatchObject({
					code: "validation_failed",
					category: "usage",
					details: scenario.expectedDetails,
				},);
				expect(failure.stdout,).toContain(scenario.errorFragment,);
			},);
			expect(mutationRequests,).toBe(0,);
		});
	}

	it("refuses invalid template references before creating a successor", async () => {
		let mutationRequests = 0;
		const route = successorServer({
			template: {
				...TEMPLATE_MANIFEST,
				homepageSections: [{
					tiles: [{ type: "SCENARIO_RUN", scenarioId: "MISSING_SCENARIO", },],
				},],
			},
			scenarios: (_req, res,) => {
				sendJson(res, [],);
			},
		},);
		await withCliServer((req, res,) => {
			if (req.method === "POST" || req.method === "PUT" || req.method === "DELETE") {
				mutationRequests += 1;
			}
			route(req, res,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = errorReport(failure,);
			expect(report,).toMatchObject({
				code: "validation_failed",
				category: "usage",
				details: {
					appId: "MYAPP",
					preflightExecuted: true,
					creationPostAttempted: false,
				},
			},);
			expect(report.details?.manifestHash,).toMatch(/^[0-9a-f]{64}$/,);
			expect(report.details?.errors,).toContainEqual(
				expect.objectContaining({
					code: "MISSING_SCENARIO",
					path: '$["homepageSections"][0]["tiles"][0]["scenarioId"]',
				},),
			);
		},);
		expect(mutationRequests,).toBe(0,);
	});

	it("fails permission snapshot preflight before any mutation", async () => {
		let mutationRequests = 0;
		const route = successorServer({
			sourcePermissionsRead: (_req, res,) => {
				sendJson(res, { message: "permission snapshot unavailable", }, 500,);
			},
		},);
		await withCliServer((req, res,) => {
			if (req.method === "POST" || req.method === "PUT" || req.method === "DELETE") {
				mutationRequests += 1;
			}
			route(req, res,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"successor-preflight",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
					"--copy-permissions",
					"--retries",
					"1",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(3,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				category: string;
				status: number;
				retryable: boolean;
			};
			expect(report,).toMatchObject({
				code: "transient",
				category: "dss",
				status: 500,
				retryable: true,
			},);
		},);
		expect(mutationRequests,).toBe(0,);
	});

	it("refuses --from == --to before any request", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"OLD_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("different project keys",);
		},);
		expect(requestCount,).toBe(0,);
	});
	it("does not expose an unproven server-atomic target bypass", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
					"--target-absence",
					"server-atomic",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("--target-absence",);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("refuses an unregistered --from project without POSTing", async () => {
		let posts = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") posts += 1;
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OTHER_INSTANCE", },],);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("not registered",);
			expect(posts,).toBe(0,);
		},);
	});

	it("refuses an existing --to project without POSTing", async () => {
		let posts = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") posts += 1;
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(res, { projectKey: "NEW_INSTANCE", name: "Already there", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("already exists",);
			expect(posts,).toBe(0,);
		},);
	});

	it("refuses a template without a manifest version without POSTing", async () => {
		let posts = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") posts += 1;
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(res, { message: "Project not found", }, 404,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, {
					projectKey: "MYAPP_TEMPLATE",
					projectAppType: "APP_TEMPLATE",
					homepageSections: [],
				},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("no manifest version",);
			expect(posts,).toBe(0,);
		},);
	});

	it("creates, waits, verifies, and copies permissions while preserving the predecessor", async () => {
		let created = false;
		let oldMutations = 0;
		let postBody: Record<string, unknown> | undefined;
		let permissionsPut = false;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				(req.method === "PUT" || req.method === "DELETE") && url.pathname.includes("OLD_INSTANCE",)
			) {
				oldMutations += 1;
			}
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
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", version: "1.0.0", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/permissions") {
				sendJson(res, PERMISSIONS,);
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
				postBody = JSON.parse((await readBody(req,)) || "{}",) as Record<string, unknown>;
				created = true;
				sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-9") {
				sendJson(res, DONE_FUTURE,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/permissions") {
				sendJson(res, permissionsPut ? PERMISSIONS : {},);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/projects/NEW_INSTANCE/permissions") {
				permissionsPut = true;
				sendJson(res, {},);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--name",
							"Release 2",
							"--copy-permissions",
							"--timeout",
							"60000",
							"--poll-interval",
							"1000",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(postBody,).toEqual({
				targetProjectKey: "NEW_INSTANCE",
				targetProjectName: "Release 2",
			},);
			expect(result,).toMatchObject({
				success: true,
				state: "DONE",
				projectKey: "NEW_INSTANCE",
				appId: "MYAPP",
				additive: true,
				sourcePreserved: true,
				uiPublicationVerified: false,
				templateVersion: "2.0.0",
				instanceVersion: "2.0.0",
				source: { projectKey: "OLD_INSTANCE", },
				target: { projectKey: "NEW_INSTANCE", },
				permissions: { requested: true, copied: true, verified: true, },
			},);
			expect(result.pollCount,).toBe(1,);
			expect(result.published,).toBeUndefined();
			expect(result.ready,).toBeUndefined();
			expect(result.requiredExternalGates,).toEqual([
				expect.objectContaining({ gate: "visual-ui", status: "required", },),
			],);
			expect(oldMutations,).toBe(0,);
		},);
	});

	it("exits 4 with an indeterminate, ledger-free shape when no future ID is returned", async () => {
		const dir = join(tmpdir(), `dss-app-successor-indeterminate-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
					sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
					sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
					sendJson(res, { message: "Project not found", }, 404,);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
					sendJson(res, TEMPLATE_MANIFEST,);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", },);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--record-cleanup",
						ledger,
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				// An ambiguous POST with neither a future ID nor a verified incarnation
				// is indeterminate: it authorizes no cleanup and records no ledger entry.
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "INDETERMINATE",
					outcome: "indeterminate",
					stage: "future-wait",
					projectKey: "NEW_INSTANCE",
					creationPostAttempted: true,
					cleanupEligible: false,
					recoveryCommands: ["dss app instances MYAPP",],
				},);
			},);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("exits 4 without throwing when the creation future fails", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				sendJson(res, { message: "Project not found", }, 404,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
				sendJson(res, TEMPLATE_MANIFEST,);
				return;
			}
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-9") {
				sendJson(res, { jobId: "job-9", hasResult: false, alive: false, },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("FAILED",);
		},);
	});

	it("keeps failed-future cleanup unresolved without project-incarnation proof", async () => {
		const dir = join(tmpdir(), `dss-app-successor-failed-target-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
					sendJson(res, [{ projectKey: "OLD_INSTANCE", },],);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
					sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
					sendJson(res, { message: "Project not found", }, 404,);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/") {
					sendJson(res, TEMPLATE_MANIFEST,);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/futures/job-9") {
					sendJson(res, {
						jobId: "job-9",
						hasResult: false,
						alive: false,
						result: { targetProjectKey: "NEW_INSTANCE", },
					},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure([
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
					"--record-cleanup",
					ledger,
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = JSON.parse(failure.stdout,) as {
					details?: { result?: Record<string, unknown>; };
				};
				expect(result.details?.result,).toMatchObject({
					state: "FAILED",
					futureTargetVerified: true,
					cleanupEligible: true,
				},);
			},);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				cleanup?: { argv?: string[]; };
			};
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--unconfirmed-creation",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("exits 4 with a verification failure when the successor version does not match the template", async () => {
		let created = false;
		let oldMutations = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				(req.method === "PUT" || req.method === "DELETE") && url.pathname.includes("OLD_INSTANCE",)
			) {
				oldMutations += 1;
			}
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
				created = true;
				sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-9") {
				sendJson(res, DONE_FUTURE,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", version: "1.9.0", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("VERIFICATION_FAILED",);
			expect(oldMutations,).toBe(0,);
		},);
	});
	it("refuses a whitespace-only template version without POSTing", async () => {
		let posts = 0;
		await withCliServer(
			successorServer({
				created: false,
				template: { ...TEMPLATE_MANIFEST, version: "   ", },
				post: (req, res,) => {
					posts += 1;
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
			},
		);
		expect(posts,).toBe(0,);
	});

	it("returns an unbound INDETERMINATE shape for an empty 2xx creation response", async () => {
		await withCliServer(
			successorServer({
				post: (_req, res,) => {
					res.statusCode = 200;
					res.end();
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "INDETERMINATE",
					projectKey: "NEW_INSTANCE",
					responseKind: "empty",
					outcome: "indeterminate",
					creationPostAttempted: true,
					cleanupEligible: false,
					elapsedMs: 0,
					pollCount: 0,
				},);
			},
		);
	});

	it("returns an unbound INDETERMINATE shape for a JSON-null 2xx creation response", async () => {
		await withCliServer(
			successorServer({
				post: (_req, res,) => {
					sendJson(res, null,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result,).toMatchObject({
					success: false,
					state: "INDETERMINATE",
					projectKey: "NEW_INSTANCE",
					responseKind: "null",
					outcome: "indeterminate",
					creationPostAttempted: true,
					cleanupEligible: false,
				},);
				expect(result.instance,).toBeUndefined();
			},
		);
	});

	it("fails safely and stays pinned to --to when the response echoes a different target key", async () => {
		let futureGets = 0;
		await withCliServer(
			successorServer({
				post: (_req, res,) => {
					sendJson(res, { appId: "MYAPP", projectKey: "SOMEONE_ELSE", jobId: "job-9", },);
				},
				future: (_req, res,) => {
					futureGets += 1;
					sendJson(res, DONE_FUTURE,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "VERIFICATION_FAILED",
					stage: "create",
					projectKey: "NEW_INSTANCE",
					expected: { projectKey: "NEW_INSTANCE", },
					actual: { projectKey: "SOMEONE_ELSE", },
					cleanupEligible: true,
					futureId: "job-9",
					jobId: "job-9",
				},);
				expect(futureGets,).toBe(0,);
			},
		);
	});

	it("reports template drift as VERIFICATION_FAILED against the preflight version", async () => {
		const template = { ...TEMPLATE_MANIFEST, };
		await withCliServer(
			successorServer({
				template,
				post: (_req, res,) => {
					template.version = "2.1.0";
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "VERIFICATION_FAILED",
					stage: "verification",
					projectKey: "NEW_INSTANCE",
					templateVersionDrift: true,
					templateManifestDrift: true,
					expectedTemplateVersion: "2.0.0",
					actualTemplateVersion: "2.1.0",
					cleanupEligible: true,
				},);
			},
		);
	});

	it("fails on a future-reported target mismatch, stays pinned to --to, and never redirects any request", async () => {
		const dir = join(tmpdir(), `dss-app-successor-future-target-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const seen: string[] = [];
		try {
			await withCliServer(
				successorServer({
					// Live-shaped future result: `projectKey` names the source
					// template while `targetProjectKey` names the successor.
					future: (req, res,) => {
						seen.push(new URL(req.url ?? "/", "http://localhost",).pathname,);
						sendJson(res, {
							...DONE_FUTURE,
							result: { projectKey: "MYAPP_TEMPLATE", targetProjectKey: "SOMEONE_ELSE", },
						},);
					},
					targetGet: (req, res,) => {
						seen.push(new URL(req.url ?? "/", "http://localhost",).pathname,);
						sendJson(res, { message: "Project not found", }, 404,);
					},
					permissionsRead: (req, res,) => {
						seen.push(new URL(req.url ?? "/", "http://localhost",).pathname,);
						sendJson(res, {},);
					},
					permissionsPut: (req, res,) => {
						seen.push(new URL(req.url ?? "/", "http://localhost",).pathname,);
						sendJson(res, {},);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--copy-permissions",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "VERIFICATION_FAILED",
						stage: "future-target",
						projectKey: "NEW_INSTANCE",
						futureTargetMismatch: true,
						futureTargetField: "targetProjectKey",
						expected: { projectKey: "NEW_INSTANCE", },
						actual: { projectKey: "SOMEONE_ELSE", },
						cleanupEligible: true,
					},);
					expect(failedResult(failure,).futureTargetVerified,).toBeUndefined();
					// Verification, the permission copy and the cleanup ledger all
					// keep the requested key; DSS's stray keys never see a request.
					expect(
						seen.some((path,) => path.includes("SOMEONE_ELSE",) || path.includes("MYAPP_TEMPLATE",)),
					).toBe(false,);
					expect(
						seen.filter((path,) => path === "/public/api/projects/NEW_INSTANCE/app-manifest").length,
					).toBe(0,);
					expect(seen.filter((path,) => path.endsWith("/permissions",)).length,).toBe(0,);
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
			expect(JSON.stringify(entry,),).not.toContain("SOMEONE_ELSE",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("carries futureTargetVerified from a terminal target-matched future through a later verification failure", async () => {
		const dir = join(tmpdir(), `dss-app-successor-target-verified-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(
				successorServer({
					future: (_req, res,) => {
						sendJson(res, {
							...DONE_FUTURE,
							result: { projectKey: "MYAPP_TEMPLATE", targetProjectKey: "NEW_INSTANCE", },
						},);
					},
					instanceManifest: { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", version: "9.9.9", },
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "VERIFICATION_FAILED",
						stage: "verification",
						projectKey: "NEW_INSTANCE",
						futureTargetVerified: true,
						cleanupEligible: true,
					},);
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
						"--expect-project-incarnation",
						NEW_INSTANCE_INCARNATION_HASH,
					],
				},
			},);
			expect(JSON.stringify(entry,),).not.toContain("--future-id",);
			expect(JSON.stringify(entry,),).not.toContain("--unconfirmed-creation",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("keeps a verification failure future-gated when the terminal future named no target", async () => {
		const dir = join(tmpdir(), `dss-app-successor-target-missing-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(
				successorServer({
					future: (_req, res,) => {
						sendJson(res, { ...DONE_FUTURE, result: { outcome: "SUCCESS", }, },);
					},
					instanceManifest: { ...INSTANCE_MANIFEST, projectKey: "NEW_INSTANCE", version: "9.9.9", },
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "VERIFICATION_FAILED",
						projectKey: "NEW_INSTANCE",
						cleanupEligible: true,
					},);
					expect(failedResult(failure,).futureTargetVerified,).toBeUndefined();
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
						"--future-id",
						"job-9",
						"--expect-project-incarnation",
						NEW_INSTANCE_INCARNATION_HASH,
					],
				},
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("accepts a live-shaped future result whose projectKey names the source template", async () => {
		await withCliServer(
			successorServer({
				future: (_req, res,) => {
					sendJson(res, {
						...DONE_FUTURE,
						result: { projectKey: "MYAPP_TEMPLATE", targetProjectKey: "NEW_INSTANCE", },
					},);
				},
			},),
			async (url,) => {
				const result = JSON.parse(
					(
						await dss(
							[
								"app",
								"create-successor-instance",
								"MYAPP",
								"--from",
								"OLD_INSTANCE",
								"--to",
								"NEW_INSTANCE",
							],
							{ env: cliEnv(url,), },
						)
					).stdout,
				) as Record<string, unknown>;
				expect(result,).toMatchObject({
					success: true,
					state: "DONE",
					projectKey: "NEW_INSTANCE",
					target: { projectKey: "NEW_INSTANCE", },
					futureTargetVerified: true,
				},);
			},
		);
	});

	it("fails same-version manifest content drift without mislabeling it as a version change", async () => {
		const template: Record<string, unknown> = { ...TEMPLATE_MANIFEST, };
		await withCliServer(
			successorServer({
				template,
				post: (_req, res,) => {
					// Content moves while the version marker stays put.
					template.homepageSections = [{ id: "added-after-acceptance", },];
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result,).toMatchObject({
					success: false,
					state: "VERIFICATION_FAILED",
					stage: "verification",
					projectKey: "NEW_INSTANCE",
					templateVersionDrift: false,
					templateManifestDrift: true,
					expectedTemplateVersion: "2.0.0",
					actualTemplateVersion: "2.0.0",
					cleanupEligible: true,
				},);
				expect(result.expectedTemplateManifestHash,).toBeTypeOf("string",);
				expect(result.actualTemplateManifestHash,).toBeTypeOf("string",);
				expect(result.actualTemplateManifestHash,).not.toBe(result.expectedTemplateManifestHash,);
			},
		);
	});

	it("marks a definitive 409 create rejection cleanupEligible false and records no ledger entry", async () => {
		const dir = join(tmpdir(), `dss-app-successor-rejected-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						sendJson(res, { message: "Project key already exists", }, 409,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "CREATE_FAILED",
						projectKey: "NEW_INSTANCE",
						cleanupEligible: false,
					},);
				},
			);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
	it("reports a 5xx create failure as indeterminate without recording cleanup", async () => {
		const dir = join(tmpdir(), `dss-app-successor-5xx-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						sendJson(res, { message: "boom", }, 500,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
							"--retries",
							"1",
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "INDETERMINATE",
						projectKey: "NEW_INSTANCE",
						outcome: "indeterminate",
						creationPostAttempted: true,
						cleanupEligible: false,
					},);
				},
			);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reports not-attempted when the permission before-read fails", async () => {
		let puts = 0;
		await withCliServer(
			successorServer({
				permissionsRead: (_req, res,) => {
					sendJson(res, { message: "boom", }, 500,);
				},
				permissionsPut: (_req, res,) => {
					puts += 1;
					sendJson(res, {},);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
						"--retries",
						"1",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "not-attempted",
					verified: false,
				},);
				expect(puts,).toBe(0,);
			},
		);
	});

	it("reports rejected when DSS definitively refuses the permission PUT", async () => {
		let puts = 0;
		await withCliServer(
			successorServer({
				permissionsPut: (_req, res,) => {
					puts += 1;
					sendJson(res, { message: "not allowed", }, 403,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "rejected",
					verified: false,
				},);
				expect(puts,).toBe(1,);
			},
		);
	});

	it("reports unknown when the permission PUT outcome is indeterminate", async () => {
		await withCliServer(
			successorServer({
				permissionsPut: (_req, res,) => {
					sendJson(res, { message: "boom", }, 500,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
						"--retries",
						"1",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				const permissions = result.permissions as Record<string, unknown>;
				expect(permissions,).toMatchObject({
					requested: true,
					state: "unknown",
					verified: false,
				},);
				expect(permissions.copied,).toBeNull();
			},
		);
	});

	it("reports rejected with copied false when the resolved PUT does not verify", async () => {
		let reads = 0;
		await withCliServer(
			successorServer({
				permissionsRead: (_req, res,) => {
					reads += 1;
					sendJson(
						res,
						reads === 1
							? {}
							: { permissions: [{ user: { login: "bob", }, admin: false, },], },
					);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
						"--retries",
						"1",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "rejected",
					reason: "verified-hash-mismatch",
					verified: false,
				},);
				expect(reads,).toBe(2,);
			},
		);
	});

	it("reports completed-unverified when the verification read after a resolved PUT fails", async () => {
		let reads = 0;
		await withCliServer(
			successorServer({
				permissionsRead: (_req, res,) => {
					reads += 1;
					if (reads === 1) {
						sendJson(res, {},);
						return;
					}
					sendJson(res, { message: "gone", }, 403,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: null,
					state: "completed-unverified",
					verified: false,
				},);
				expect(reads,).toBe(2,);
			},
		);
	});
	it("completes an inline hasResult creation without polling the future", async () => {
		let futureGets = 0;
		await withCliServer(
			successorServer({
				post: (_req, res,) => {
					sendJson(res, {
						appId: "MYAPP",
						projectKey: "NEW_INSTANCE",
						jobId: "job-9",
						hasResult: true,
						result: { targetProjectKey: "NEW_INSTANCE", outcome: "SUCCESS", },
					},);
				},
				future: () => {
					futureGets += 1;
					throw new Error("inline completions must not poll the future",);
				},
			},),
			async (url,) => {
				const result = JSON.parse(
					(
						await dss([
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
						], { env: cliEnv(url,), },)
					).stdout,
				) as Record<string, unknown>;
				expect(result,).toMatchObject({
					success: true,
					state: "DONE",
					projectKey: "NEW_INSTANCE",
					hasResult: true,
					pollCount: 0,
					instanceVersion: "2.0.0",
					permissions: { requested: false, copied: false, state: "not-requested", },
				},);
				expect(Array.isArray(result.checks,),).toBe(true,);
				expect(result.checks as unknown[],).not.toHaveLength(0,);
				expect(futureGets,).toBe(0,);
			},
		);
	});

	it("uses unconfirmed cleanup when an inline successor result has no target proof or future ID", async () => {
		const dir = join(tmpdir(), `dss-app-successor-inline-unconfirmed-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let futureGets = 0;
		try {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						sendJson(res, {
							appId: "MYAPP",
							projectKey: "NEW_INSTANCE",
							hasResult: true,
							result: { outcome: "SUCCESS", },
						},);
					},
					future: () => {
						futureGets += 1;
						throw new Error("inline completions must not poll a synthetic future",);
					},
				},),
				async (url,) => {
					await dss([
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--record-cleanup",
						ledger,
					], { env: cliEnv(url,), },);
				},
			);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				cleanup?: { argv?: string[]; };
			};
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--unconfirmed-creation",
			],);
			expect(JSON.stringify(entry,),).not.toContain("inline",);
			expect(futureGets,).toBe(0,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("rejects source ACL drift between preflight and the target write without a target PUT", async () => {
		let sourceReads = 0;
		let targetPuts = 0;
		await withCliServer(
			successorServer({
				sourcePermissionsRead: (_req, res,) => {
					sourceReads += 1;
					sendJson(
						res,
						sourceReads === 1
							? PERMISSIONS
							: { permissions: [{ user: { login: "carol", }, admin: true, },], },
					);
				},
				permissionsPut: () => {
					targetPuts += 1;
					throw new Error("a stale predecessor snapshot must never reach the target",);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "not-attempted",
					sourceDrift: true,
					verified: false,
				},);
				expect(sourceReads,).toBe(2,);
				expect(targetPuts,).toBe(0,);
			},
		);
	});

	it("rejects source ACL drift even when the successor already has the stale hash", async () => {
		let sourceReads = 0;
		let targetPuts = 0;
		await withCliServer(
			successorServer({
				sourcePermissionsRead: (_req, res,) => {
					sourceReads += 1;
					sendJson(
						res,
						sourceReads === 1
							? PERMISSIONS
							: { permissions: [{ user: { login: "carol", }, admin: true, },], },
					);
				},
				permissionsRead: (_req, res,) => {
					sendJson(res, PERMISSIONS,);
				},
				permissionsPut: () => {
					targetPuts += 1;
					throw new Error("a stale predecessor snapshot must never be accepted as unchanged",);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "not-attempted",
					sourceDrift: true,
					verified: false,
				},);
				expect(sourceReads,).toBe(2,);
				expect(targetPuts,).toBe(0,);
			},
		);
	});

	it("rejects target key replacement after multi-read verification", async () => {
		let targetGets = 0;
		await withCliServer(
			successorServer({
				targetGet: (_req, res,) => {
					targetGets += 1;
					if (targetGets === 1) {
						sendJson(res, { message: "Project not found", }, 404,);
						return;
					}
					sendJson(
						res,
						targetGets === 2
							? NEW_INSTANCE_DETAILS
							: {
								...NEW_INSTANCE_DETAILS,
								creationTag: {
									versionNumber: 1,
									lastModifiedOn: 1_800_000_000_000,
								},
							},
					);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result,).toMatchObject({
					state: "VERIFICATION_FAILED",
					stage: "verification",
				},);
				expect(result.error,).toContain("changed incarnation during successor verification",);
				expect(targetGets,).toBe(3,);
			},
		);
	});

	it("rejects target key replacement after the successor permission read without a PUT", async () => {
		let targetGets = 0;
		let permissionReads = 0;
		let targetPuts = 0;
		await withCliServer(
			successorServer({
				targetGet: (req, res,) => {
					targetGets += 1;
					if (targetGets === 1) {
						sendJson(res, { message: "Project not found", }, 404,);
						return;
					}
					sendJson(
						res,
						targetGets <= 3
							? NEW_INSTANCE_DETAILS
							: {
								...NEW_INSTANCE_DETAILS,
								creationTag: {
									versionNumber: 1,
									lastModifiedOn: 1_800_000_000_000,
								},
							},
					);
				},
				permissionsRead: (_req, res,) => {
					permissionReads += 1;
					sendJson(res, {},);
				},
				permissionsPut: () => {
					targetPuts += 1;
					throw new Error("a replacement successor must never receive the predecessor ACL",);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--copy-permissions",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result.state,).toBe("PERMISSIONS_FAILED",);
				expect(result.permissions,).toMatchObject({
					requested: true,
					copied: false,
					state: "not-attempted",
					verified: false,
				},);
				expect(targetGets,).toBe(4,);
				expect(permissionReads,).toBe(1,);
				expect(targetPuts,).toBe(0,);
			},
		);
	});

	it("rejects a 403-hidden target that the accessible project listing contains", async () => {
		let posts = 0;
		let listGets = 0;
		await withCliServer(
			successorServer({
				targetGet: (_req, res,) => {
					sendJson(res, { message: "Forbidden", }, 403,);
				},
				projectsList: (_req, res,) => {
					listGets += 1;
					sendJson(
						res,
						[
							{ projectKey: "OTHER_INSTANCE", name: "Other", },
							{ projectKey: "NEW_INSTANCE", name: "Taken", },
						],
					);
				},
				post: (_req, res,) => {
					posts += 1;
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
			},
		);
		expect(listGets,).toBe(1,);
		expect(posts,).toBe(0,);
	});

	it("rejects an unlisted 403-hidden target as unconfirmed without POSTing", async () => {
		let posts = 0;
		await withCliServer(
			successorServer({
				targetGet: (_req, res,) => {
					sendJson(res, { message: "Forbidden", }, 403,);
				},
				projectsList: (_req, res,) => {
					sendJson(res, [{ projectKey: "OTHER_INSTANCE", name: "Other", },],);
				},
				post: (_req, res,) => {
					posts += 1;
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--dry-run",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("Could not confirm",);
				expect(errorReport(failure,),).toMatchObject({
					code: "target_absence_unverifiable",
					category: "permission_or_environment",
					details: {
						targetProjectKey: "NEW_INSTANCE",
						targetFlag: "--to",
						directTargetProbe: 403,
						projectListProbe: 200,
						targetVisibleInProjectList: false,
						appInstancesProbe: 200,
						targetVisibleInAppInstances: false,
						preflightExecuted: true,
						creationPostAttempted: false,
						targetProbe: "forbidden-and-not-listable",
						supportedRecoveryModes: ["grant-global-project-visibility",],
						unavailableRecoveryModes: [
							"use-supported-key-availability-endpoint",
							"server-atomic-create",
						],
					},
				},);
			},
		);
		expect(posts,).toBe(0,);
	});

	it("rejects an unlisted 403-hidden target before live creation", async () => {
		let posts = 0;
		await withCliServer(
			successorServer({
				targetGet: (_req, res,) => {
					sendJson(res, { message: "Forbidden", }, 403,);
				},
				projectsList: (_req, res,) => {
					sendJson(res, [{ projectKey: "OTHER_INSTANCE", name: "Other", },],);
				},
				post: (_req, res,) => {
					posts += 1;
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("Could not confirm",);
				expect(errorReport(failure,),).toMatchObject({
					code: "target_absence_unverifiable",
					category: "permission_or_environment",
					details: {
						targetProjectKey: "NEW_INSTANCE",
						targetFlag: "--to",
						directTargetProbe: 403,
						projectListProbe: 200,
						targetVisibleInProjectList: false,
						appInstancesProbe: 200,
						targetVisibleInAppInstances: false,
						preflightExecuted: true,
						creationPostAttempted: false,
						targetProbe: "forbidden-and-not-listable",
						supportedRecoveryModes: ["grant-global-project-visibility",],
						unavailableRecoveryModes: [
							"use-supported-key-availability-endpoint",
							"server-atomic-create",
						],
					},
				},);
			},
		);
		expect(posts,).toBe(0,);
	});

	it("propagates non-403 target probe errors before POSTing", async () => {
		let posts = 0;
		await withCliServer(
			successorServer({
				targetGet: (_req, res,) => {
					sendJson(res, { message: "boom", }, 500,);
				},
				post: (_req, res,) => {
					posts += 1;
					sendJson(res, { appId: "MYAPP", projectKey: "NEW_INSTANCE", jobId: "job-9", },);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--retries",
						"1",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(3,);
				expect(failure.stderr,).toBe("",);
			},
		);
		expect(posts,).toBe(0,);
	});

	it("reports a creation transport failure as indeterminate without authorizing cleanup", async () => {
		await withCliServer(
			successorServer({
				post: (req,) => {
					req.socket.destroy();
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
						"--retries",
						"1",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "INDETERMINATE",
					projectKey: "NEW_INSTANCE",
					outcome: "indeterminate",
					creationPostAttempted: true,
					cleanupEligible: false,
					recoveryCommands: ["dss app instances MYAPP",],
				},);
			},
		);
	});

	it("binds a confirmed successor cleanup entry to the created project incarnation", async () => {
		const dir = join(tmpdir(), `dss-app-successor-bound-ledger-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		try {
			await withCliServer(successorServer(), async (url,) => {
				const result = JSON.parse(
					(
						await dss(
							[
								"app",
								"create-successor-instance",
								"MYAPP",
								"--from",
								"OLD_INSTANCE",
								"--to",
								"NEW_INSTANCE",
								"--record-cleanup",
								ledger,
							],
							{ env: cliEnv(url,), },
						)
					).stdout,
				) as Record<string, unknown>;
				expect(result,).toMatchObject({
					success: true,
					state: "DONE",
					projectKey: "NEW_INSTANCE",
					futureTargetVerified: true,
					projectIncarnationHash: NEW_INSTANCE_INCARNATION_HASH,
				},);
			},);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				projectKey?: string;
				name?: string;
				cleanup?: { argv?: string[]; };
			};
			// A settled, incarnation-bound creation needs no unconfirmed escape
			// hatch and no future gate: the recorded hash alone authorizes the
			// delete, and only for the successor project.
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--expect-project-incarnation",
				NEW_INSTANCE_INCARNATION_HASH,
			],);
			expect(entry.projectKey,).toBe("NEW_INSTANCE",);
			expect(entry.name,).toBe("NEW_INSTANCE",);
			const serialized = JSON.stringify(entry,);
			expect(serialized,).not.toContain("--future-id",);
			expect(serialized,).not.toContain("--unconfirmed-creation",);
			// The predecessor is never a cleanup target.
			expect(serialized,).not.toContain("OLD_INSTANCE",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses incarnation binding when the successor project details omit creationTag", async () => {
		const dir = join(tmpdir(), `dss-app-successor-no-creation-tag-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let targetGets = 0;
		try {
			await withCliServer(
				successorServer({
					targetGet: (_req, res,) => {
						targetGets += 1;
						if (targetGets === 1) {
							sendJson(res, { message: "Project not found", }, 404,);
							return;
						}
						// Terminal creation for the requested key, but DSS answers
						// without the immutable creationTag identity.
						sendJson(res, {
							projectKey: "NEW_INSTANCE",
							name: "New instance",
							projectAppType: "APP_INSTANCE",
						},);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					const result = failedResult(failure,);
					expect(result,).toMatchObject({
						success: false,
						state: "INCARNATION_UNVERIFIED",
						stage: "project-incarnation",
						projectKey: "NEW_INSTANCE",
						futureTargetVerified: true,
						cleanupEligible: true,
					},);
					expect(result.projectIncarnationHash,).toBeUndefined();
				},
			);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				cleanup?: { argv?: string[]; };
			};
			// A terminal target match alone cannot bind cleanup to an incarnation,
			// so the entry stays explicitly unconfirmed instead of inventing a hash.
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--unconfirmed-creation",
			],);
			expect(JSON.stringify(entry,),).not.toContain("--expect-project-incarnation",);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("reports WAIT_FAILED with future and instance recovery commands when the future read fails", async () => {
		const dir = join(tmpdir(), `dss-app-successor-wait-failed-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let futureGets = 0;
		try {
			await withCliServer(
				successorServer({
					future: (_req, res,) => {
						futureGets += 1;
						sendJson(res, { message: "boom", }, 500,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--retries",
							"1",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					const result = failedResult(failure,);
					expect(result,).toMatchObject({
						success: false,
						state: "WAIT_FAILED",
						stage: "future-wait",
						projectKey: "NEW_INSTANCE",
						jobId: "job-9",
						cleanupEligible: true,
						recoveryCommands: ["dss future wait job-9", "dss app instances MYAPP",],
					},);
					expect(result.futureTargetVerified,).toBeUndefined();
					expect(result.projectIncarnationHash,).toBeUndefined();
					expect(futureGets,).toBe(1,);
				},
			);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				cleanup?: { argv?: string[]; };
			};
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEW_INSTANCE",
				"--unconfirmed-creation",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses a --to already listed as an app instance before probing the target or POSTing", async () => {
		let posts = 0;
		let targetGets = 0;
		let projectListGets = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") posts += 1;
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEW_INSTANCE/") {
				targetGets += 1;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/") projectListGets += 1;
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				sendJson(res, [{ projectKey: "OLD_INSTANCE", }, { projectKey: "NEW_INSTANCE", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/OLD_INSTANCE/app-manifest") {
				sendJson(res, { ...INSTANCE_MANIFEST, projectKey: "OLD_INSTANCE", version: "1.0.0", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-successor-instance",
					"MYAPP",
					"--from",
					"OLD_INSTANCE",
					"--to",
					"NEW_INSTANCE",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("already exists as an app instance",);
			const report = errorReport(failure,);
			expect(report.code,).toBe("validation_failed",);
			expect(report.category,).toBe("usage",);
			// The app-instance list already proves the collision, so the refusal
			// reports exactly which probe answered and that nothing was created.
			expect(report.details,).toMatchObject({
				targetProjectKey: "NEW_INSTANCE",
				targetFlag: "--to",
				directTargetProbe: "not-executed",
				targetVisibleInAppInstances: true,
				appInstancesProbe: 200,
				preflightExecuted: true,
				creationPostAttempted: false,
			},);
			expect(report.details?.targetVisibleInProjectList,).toBeNull();
		},);
		expect(posts,).toBe(0,);
		expect(targetGets,).toBe(0,);
		expect(projectListGets,).toBe(0,);
	});

	it("refuses at create when the creation response names another project without a future ID", async () => {
		const dir = join(tmpdir(), `dss-app-successor-echo-unbound-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let futureGets = 0;
		try {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						sendJson(res, { appId: "MYAPP", targetProjectKey: "SOMEONE_ELSE", },);
					},
					future: (_req, res,) => {
						futureGets += 1;
						sendJson(res, DONE_FUTURE,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					const result = failedResult(failure,);
					// A stray key with no future ID authorizes nothing: neither the
					// named project nor the requested one may be deleted.
					expect(result,).toMatchObject({
						success: false,
						state: "VERIFICATION_FAILED",
						stage: "create",
						projectKey: "NEW_INSTANCE",
						expected: { projectKey: "NEW_INSTANCE", },
						actual: { projectKey: "SOMEONE_ELSE", },
						cleanupEligible: false,
					},);
					expect(result.futureId,).toBeUndefined();
					expect(result.jobId,).toBeUndefined();
					expect(result.futureTargetVerified,).toBeUndefined();
					expect(futureGets,).toBe(0,);
				},
			);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("refuses at future-target when an inline creation result names another project", async () => {
		let futureGets = 0;
		await withCliServer(
			successorServer({
				post: (_req, res,) => {
					sendJson(res, {
						appId: "MYAPP",
						targetProjectKey: "NEW_INSTANCE",
						jobId: "job-9",
						hasResult: true,
						result: { targetProjectKey: "SOMEONE_ELSE", },
					},);
				},
				future: (_req, res,) => {
					futureGets += 1;
					sendJson(res, DONE_FUTURE,);
				},
			},),
			async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-successor-instance",
						"MYAPP",
						"--from",
						"OLD_INSTANCE",
						"--to",
						"NEW_INSTANCE",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const result = failedResult(failure,);
				expect(result,).toMatchObject({
					success: false,
					state: "VERIFICATION_FAILED",
					stage: "future-target",
					projectKey: "NEW_INSTANCE",
					futureTargetMismatch: true,
					futureTargetField: "targetProjectKey",
					expected: { projectKey: "NEW_INSTANCE", },
					actual: { projectKey: "SOMEONE_ELSE", },
					cleanupEligible: true,
				},);
				expect(result.futureTargetVerified,).toBeUndefined();
				// The inline verdict arrived with the POST: no future is polled.
				expect(futureGets,).toBe(0,);
			},
		);
	});

	for (
		const scenario of [
			{ label: "array", body: [{ projectKey: "NEW_INSTANCE", },], responseKind: "array", },
			{ label: "scalar", body: "NEW_INSTANCE", responseKind: "scalar", },
		]
	) {
		it(`treats a ${scenario.label} creation response as indeterminate without cleanup`, async () => {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						sendJson(res, scenario.body,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					const result = failedResult(failure,);
					expect(result,).toMatchObject({
						success: false,
						state: "INDETERMINATE",
						stage: "future-wait",
						projectKey: "NEW_INSTANCE",
						outcome: "indeterminate",
						creationPostAttempted: true,
						cleanupEligible: false,
						responseKind: scenario.responseKind,
						recoveryCommands: ["dss app instances MYAPP",],
					},);
					expect(result.instance,).toBeUndefined();
					expect(result.futureId,).toBeUndefined();
					expect(result.jobId,).toBeUndefined();
				},
			);
		});
	}

	it("treats an HTTP 425 creation response as indeterminate rather than a definitive rejection", async () => {
		const dir = join(tmpdir(), `dss-app-successor-too-early-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let posts = 0;
		try {
			await withCliServer(
				successorServer({
					post: (_req, res,) => {
						posts += 1;
						sendJson(res, { message: "Too early", }, 425,);
					},
				},),
				async (url,) => {
					const failure = await dssFailure(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--record-cleanup",
							ledger,
						],
						{ env: cliEnv(url,), },
					);
					expect(failure.code,).toBe(4,);
					expect(failure.stderr,).toBe("",);
					// 409 proves nothing was created; a retryable 425 does not, so
					// the outcome stays indeterminate and authorizes no cleanup.
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "INDETERMINATE",
						stage: "create",
						projectKey: "NEW_INSTANCE",
						outcome: "indeterminate",
						creationPostAttempted: true,
						cleanupEligible: false,
						recoveryCommands: ["dss app instances MYAPP",],
					},);
				},
			);
			expect(posts,).toBe(1,);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("plans a successor creation offline with its full preflight request disclosure", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const plan = JSON.parse(
				(
					await dss(
						[
							"app",
							"create-successor-instance",
							"MYAPP",
							"--from",
							"OLD_INSTANCE",
							"--to",
							"NEW_INSTANCE",
							"--name",
							"Release 2",
							"--plan",
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "create-successor-instance",
				appId: "MYAPP",
				sourceProjectKey: "OLD_INSTANCE",
				targetProjectKey: "NEW_INSTANCE",
				targetProjectName: "Release 2",
				copyPermissions: false,
				preflightExecuted: false,
				preflightWillRunDuringApply: true,
				incarnationControl: "client-side-non-atomic-future-target-and-creation-tag-join",
				method: "POST",
				endpoint: "/public/api/apps/MYAPP/instances",
				wait: true,
				idempotency: "none",
				async: "future",
			},);
			expect(plan.payload,).toEqual({
				targetProjectKey: "NEW_INSTANCE",
				targetProjectName: "Release 2",
			},);
			expect(
				(plan.exitCodesOnFailure as Record<string, number>).longRunningFailure,
			).toBe(4,);
			const preflightRequests = plan.preflightRequests as Array<
				{ method: string; endpoint: string; when: string; }
			>;
			expect(
				preflightRequests.map((entry,) => `${entry.method} ${entry.endpoint} ${entry.when}`),
			).toEqual([
				"GET /public/api/apps/MYAPP/instances/ before-create",
				"GET /public/api/projects/OLD_INSTANCE/app-manifest before-create",
				"GET /public/api/projects/NEW_INSTANCE/ before-create",
				"GET /public/api/projects/ conditional",
			],);
			expect(plan.postFutureRequests,).toEqual([
				expect.objectContaining({
					method: "GET",
					endpoint: "/public/api/projects/NEW_INSTANCE/",
				},),
			],);
			// No ACL copy was requested, so no permission traffic is advertised.
			expect(plan.permissionRequests,).toBeUndefined();
			expect(plan.permissionConcurrencyControl,).toBeUndefined();
			expect(plan.plannedAndDryRun,).toBeUndefined();
		},);
		expect(requestCount,).toBe(0,);
	});
});
