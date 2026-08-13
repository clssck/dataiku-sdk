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
			const report = JSON.parse(failure.stderr,) as {
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
			expect(failure.stderr,).toContain("At least one of --manifest-version or --version-notes",);
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
			const report = JSON.parse(failure.stderr,) as {
				code: string;
				details: {
					result: {
						checks: Array<{ check: string; status: string; expected: unknown; actual: unknown; }>;
					};
				};
			};
			expect(report.code,).toBe("validation_failed",);
			const versionCheck = report.details.result.checks.find((entry,) =>
				entry.check === "manifest-version"
			);
			expect(versionCheck,).toMatchObject({ status: "failed", expected: "3.0.0", actual: "2.0.0", },);
			expect(failure.stderr,).not.toContain("apiReady",);
			expect(failure.stderr,).not.toContain('"ready":true',);
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
			const report = JSON.parse(failure.stderr,) as {
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
			const report = JSON.parse(failure.stderr,) as {
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
		return (req, res,) => {
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

	/** Extract the failed-wait result from the CLI error report on stderr. */
	function failedResult(failure: { stderr: string; },): Record<string, unknown> {
		return (JSON.parse(failure.stderr,) as { details: { result: Record<string, unknown>; }; }).details
			.result;
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
			expect(failure.stderr,).toContain("different project keys",);
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
			expect(failure.stderr,).toContain("not registered",);
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
			expect(failure.stderr,).toContain("already exists",);
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
			expect(failure.stderr,).toContain("no manifest version",);
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

	it("exits 4 with an addressable failed-wait shape when no future ID is returned", async () => {
		const dir = join(tmpdir(), `dss-app-successor-untrackable-${Date.now()}`,);
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
				expect(failure.stderr,).toContain("UNTRACKABLE",);
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
						"--unconfirmed-creation",
					],
				},
			},);
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
			expect(failure.stderr,).toContain("FAILED",);
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
				const result = JSON.parse(failure.stderr,) as {
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
			expect(failure.stderr,).toContain("VERIFICATION_FAILED",);
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
			},
		);
		expect(posts,).toBe(0,);
	});

	it("returns an addressable UNTRACKABLE shape for an empty 2xx creation response", async () => {
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
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "UNTRACKABLE",
					projectKey: "NEW_INSTANCE",
					responseKind: "empty",
					cleanupEligible: true,
					elapsedMs: 0,
					pollCount: 0,
				},);
			},
		);
	});

	it("returns an addressable UNTRACKABLE shape for a JSON-null 2xx creation response", async () => {
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
				const result = failedResult(failure,);
				expect(result,).toMatchObject({
					success: false,
					state: "UNTRACKABLE",
					projectKey: "NEW_INSTANCE",
					responseKind: "null",
					cleanupEligible: true,
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
		const template = { ...TEMPLATE_MANIFEST, };
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

	it("retains cleanup eligibility for a 5xx create failure and records the successor target", async () => {
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
					expect(failedResult(failure,),).toMatchObject({
						success: false,
						state: "CREATE_FAILED",
						projectKey: "NEW_INSTANCE",
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
						"--unconfirmed-creation",
					],
				},
			},);
			expect(JSON.stringify(entry,),).not.toContain("OLD_INSTANCE",);
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
				expect(failure.stderr,).toContain("Could not confirm",);
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
				expect(failure.stderr,).toContain("Could not confirm",);
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
			},
		);
		expect(posts,).toBe(0,);
	});

	it("retains cleanup eligibility for a transport failure on the creation POST", async () => {
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
				expect(failedResult(failure,),).toMatchObject({
					success: false,
					state: "CREATE_FAILED",
					projectKey: "NEW_INSTANCE",
					cleanupEligible: true,
				},);
			},
		);
	});
});
