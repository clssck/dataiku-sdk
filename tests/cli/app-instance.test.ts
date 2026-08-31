import { describe, expect, it, } from "bun:test";
import { cleanupLedgerEntry, } from "../../src/cli/helpers/cleanup.js";
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
	writeFileSync,
} from "./_harness.js";

const hermetic = { PATH: process.env.PATH, HOME: process.env.HOME, } as NodeJS.ProcessEnv;
const DONE_FUTURE = {
	jobId: "job-1",
	hasResult: true,
	alive: false,
	result: { projectKey: "NEWPROJ", },
};

const PROJECT_DETAILS = {
	projectKey: "NEWPROJ",
	name: "New project",
	projectAppType: "APP_INSTANCE",
	creationTag: { versionNumber: 1, lastModifiedOn: 1_700_000_000_000, },
};
const PROJECT_INCARNATION_HASH = projectIncarnationHash("NEWPROJ", PROJECT_DETAILS,)!;
const REPLACEMENT_PROJECT_DETAILS = {
	...PROJECT_DETAILS,
	creationTag: { versionNumber: 1, lastModifiedOn: 1_800_000_000_000, },
};

function absentCreationTarget() {
	let reads = 0;
	return (
		method: string | undefined,
		url: URL,
		res: Parameters<typeof sendJson>[0],
	): boolean => {
		if (method !== "GET" || url.pathname !== "/public/api/projects/NEWPROJ/") return false;
		reads += 1;
		if (reads === 1) {
			sendJson(res, { message: "Project not found", }, 404,);
		} else {
			sendJson(res, PROJECT_DETAILS,);
		}
		return true;
	};
}

describe("app create-instance wait, plans, and cleanup", () => {
	it("polls the returned instance future with --wait and returns creation plus wait result", async () => {
		const answerTarget = absentCreationTarget();
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (answerTarget(req.method, url, res,)) return;
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				sendJson(res, { appId: "MYAPP", projectKey: "NEWPROJ", jobId: "job-1", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				expect(url.searchParams.get("peek",),).toBe("false",);
				sendJson(res, DONE_FUTURE,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss([
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
						"--timeout",
						"60000",
						"--poll-interval",
						"1000",
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				success: true,
				futureId: "job-1",
				state: "DONE",
				jobId: "job-1",
				projectKey: "NEWPROJ",
				instance: { appId: "MYAPP", projectKey: "NEWPROJ", jobId: "job-1", },
				futureTargetVerified: true,
			},);
			expect(result.pollCount,).toBe(1,);
			expect(typeof result.elapsedMs,).toBe("number",);
		},);
	});

	it("returns the instance reference without polling when --wait is omitted", async () => {
		const answerTarget = absentCreationTarget();
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (answerTarget(req.method, url, res,)) return;
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				sendJson(res, { appId: "MYAPP", projectKey: "NEWPROJ", jobId: "job-1", },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss([
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				projectKey: "NEWPROJ",
				futureId: "job-1",
				jobId: "job-1",
				instance: { appId: "MYAPP", projectKey: "NEWPROJ", jobId: "job-1", },
			},);
		},);
	});

	it("does not trust DSS response fields that control cleanup recording", async () => {
		const dir = join(tmpdir(), `dss-app-untrusted-controls-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const answerTarget = absentCreationTarget();
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, {
						appId: "MYAPP",
						projectKey: "NEWPROJ",
						jobId: "job-1",
						skipped: true,
						cleanupEligible: false,
						futureTargetVerified: true,
					},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const result = JSON.parse(
					(
						await dss([
							"app",
							"create-instance",
							"MYAPP",
							"--data",
							'{"targetProjectKey":"NEWPROJ"}',
							"--record-cleanup",
							ledger,
						], { env: cliEnv(url,), },)
					).stdout,
				) as Record<string, unknown>;
				expect(result,).toMatchObject({
					projectKey: "NEWPROJ",
					futureId: "job-1",
					instance: {
						skipped: true,
						cleanupEligible: false,
						futureTargetVerified: true,
					},
				},);
				expect(result.skipped,).toBeUndefined();
				expect(result.cleanupEligible,).toBeUndefined();
				expect(result.futureTargetVerified,).toBeUndefined();
			},);
			const entry = JSON.parse(readFileSync(ledger, "utf-8",),) as {
				cleanup?: { argv?: string[]; };
			};
			expect(entry.cleanup?.argv,).toEqual([
				"app",
				"delete-instance",
				"--project-key",
				"NEWPROJ",
				"--unconfirmed-creation",
			],);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("falls back to the request payload target project key for future results", async () => {
		const answerTarget = absentCreationTarget();
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (answerTarget(req.method, url, res,)) return;
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				// DSS omits the project key here; only the payload carries the target.
				sendJson(res, { appId: "MYAPP", jobId: "job-1", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				sendJson(res, DONE_FUTURE,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss([
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				success: true,
				projectKey: "NEWPROJ",
				futureTargetVerified: true,
			},);
		},);
	});

	it("rejects a missing target project key before creating an instance", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "create-instance", "MYAPP", "--data", "{}",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("targetProjectKey",);
			const planFailure = await dssFailure(
				["app", "create-instance", "MYAPP", "--data", "{}", "--plan",],
				{ env: cliEnv(url,), },
			);
			expect(planFailure.code,).toBe(1,);
			expect(planFailure.stderr,).toBe("",);
			expect(planFailure.stdout,).toContain("targetProjectKey",);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("requires confirmed target absence before direct creation", async () => {
		for (const mode of ["existing", "app-instance-collision", "masked",] as const) {
			let posts = 0;
			let appInstanceProbes = 0;
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
					if (mode === "existing") {
						sendJson(res, { projectKey: "NEWPROJ", name: "Existing", },);
					} else {
						sendJson(res, { message: "Forbidden", }, 403,);
					}
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/projects/") {
					sendJson(res, [],);
					return;
				}
				// Only reached when the direct probe was masked: the app-instance
				// list is the last permission-scoped witness of the target key,
				// and it can prove a collision but never prove absence.
				if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
					appInstanceProbes += 1;
					sendJson(
						res,
						mode === "app-instance-collision"
							? [{ appId: "MYAPP", projectKey: "NEWPROJ", },]
							: [],
					);
					return;
				}
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					posts += 1;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					code: string;
					category: string;
					retryable?: boolean;
					hint?: string;
					details: Record<string, unknown>;
				};
				if (mode === "masked") {
					// Unverifiable absence is an environment problem, never a
					// bad flag: it must not be reported as ordinary usage.
					expect(report.code,).toBe("target_absence_unverifiable",);
					expect(report.category,).toBe("permission_or_environment",);
					expect(report.retryable,).toBe(false,);
					expect(failure.stdout,).toContain("Could not confirm",);
					expect(report.details,).toMatchObject({
						targetProjectKey: "NEWPROJ",
						targetFlag: "targetProjectKey",
						directTargetProbe: 403,
						appInstancesProbe: 200,
						targetVisibleInProjectList: false,
						targetVisibleInAppInstances: false,
						preflightExecuted: true,
						creationPostAttempted: false,
						targetProbe: "forbidden-and-not-listable",
						deploymentMasksUnknownProjects: "possible-but-unproven",
						supportedRecoveryModes: ["grant-global-project-visibility",],
					},);
					// No bypass is offered: the only supported recovery is a
					// wider-visibility identity, and no server-side atomic
					// create is claimed as an alternative.
					expect(report.details.unavailableRecoveryModes,).toEqual([
						"use-supported-key-availability-endpoint",
						"server-atomic-create",
					],);
					expect(report.hint,).toContain("global project visibility",);
				} else {
					expect(report.code,).toBe("validation_failed",);
					expect(report.category,).toBe("usage",);
					expect(failure.stdout,).toContain("already exists",);
				}
				if (mode === "app-instance-collision") {
					// Presence in the app-instance list is proof of collision,
					// so the masked direct probe stops being decisive.
					expect(report.details,).toMatchObject({
						directTargetProbe: 403,
						targetVisibleInProjectList: false,
						targetVisibleInAppInstances: true,
						creationPostAttempted: false,
					},);
				}
				expect(appInstanceProbes,).toBe(mode === "existing" ? 0 : 1,);
				expect(posts,).toBe(0,);
			},);
		}
	});
	it("refuses direct creation when the app-instance witness is also masked", async () => {
		let posts = 0;
		let projectListProbes = 0;
		let appInstanceProbes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") {
				posts += 1;
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, { message: "Forbidden", }, 403,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/") {
				projectListProbes += 1;
				// Answered, but the target key is not among the visible keys:
				// this proves no collision, never absence.
				sendJson(res, [{ projectKey: "OTHER", name: "Other project", },],);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				appInstanceProbes += 1;
				// The last permission-scoped witness refuses to answer, so no
				// observation distinguishes an unused key from a hidden project.
				sendJson(res, { message: "Forbidden", }, 403,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-instance",
					"MYAPP",
					"--data",
					'{"targetProjectKey":"NEWPROJ"}',
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				category: string;
				retryable?: boolean;
				hint?: string;
				details: Record<string, unknown>;
			};
			// A masked witness is an environment problem, never a bad flag.
			expect(report.code,).toBe("target_absence_unverifiable",);
			expect(report.category,).toBe("permission_or_environment",);
			expect(report.retryable,).toBe(false,);
			expect(failure.stdout,).toContain("Could not confirm",);
			expect(report.details,).toMatchObject({
				targetProjectKey: "NEWPROJ",
				targetFlag: "targetProjectKey",
				directTargetProbe: 403,
				appInstancesProbe: 403,
				targetVisibleInProjectList: false,
				preflightExecuted: true,
				creationPostAttempted: false,
				targetProbe: "forbidden-and-not-listable",
				deploymentMasksUnknownProjects: "possible-but-unproven",
				supportedRecoveryModes: ["grant-global-project-visibility",],
			},);
			// A refused app-instance list yields no verdict at all: reporting
			// `false` would claim the key was observed to be free, which is
			// exactly the unproven assumption this refusal exists to block.
			expect(report.details.targetVisibleInAppInstances,).toBeNull();
			expect(report.details.unavailableRecoveryModes,).toEqual([
				"use-supported-key-availability-endpoint",
				"server-atomic-create",
			],);
			expect(report.hint,).toContain("global project visibility",);
			// Both fallback probes ran exactly once and neither proved anything.
			expect(projectListProbes,).toBe(1,);
			expect(appInstanceProbes,).toBe(1,);
			expect(posts,).toBe(0,);
		},);
	});

	it("folds a masked global project listing into the strict absence refusal", async () => {
		let posts = 0;
		let projectListProbes = 0;
		let appInstanceProbes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST") {
				posts += 1;
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, { message: "Forbidden", }, 403,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/") {
				projectListProbes += 1;
				sendJson(res, { message: "Forbidden", }, 403,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/apps/MYAPP/instances/") {
				appInstanceProbes += 1;
				sendJson(res, [],);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-instance",
					"MYAPP",
					"--data",
					'{"targetProjectKey":"NEWPROJ"}',
				],
				{ env: cliEnv(url,), },
			);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				category: string;
				retryable?: boolean;
				exitCode?: number;
				status?: number;
				hint?: string;
				details: Record<string, unknown>;
			};
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(report,).toMatchObject({
				code: "target_absence_unverifiable",
				category: "permission_or_environment",
				retryable: false,
			},);
			expect(report.details,).toMatchObject({
				targetProjectKey: "NEWPROJ",
				targetFlag: "targetProjectKey",
				directTargetProbe: 403,
				projectListProbe: 403,
				targetVisibleInProjectList: null,
				appInstancesProbe: 200,
				targetVisibleInAppInstances: false,
				preflightExecuted: true,
				creationPostAttempted: false,
				targetProbe: "forbidden-and-not-listable",
				supportedRecoveryModes: ["grant-global-project-visibility",],
			},);
			expect(report.hint,).toContain("global project visibility",);
			expect(projectListProbes,).toBe(1,);
			expect(appInstanceProbes,).toBe(1,);
			expect(posts,).toBe(0,);
		},);
	});

	it("exits nonzero with structured details for an invalid app manifest", async () => {
		await withCliServer((req, res,) => {
			if (req.method === "GET" && req.url === "/public/api/projects/TEST/scenarios/") {
				sendJson(res, [],);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"validate-manifest",
					"--data",
					'{"homepageSections":[{"tiles":[{"type":"SCENARIO_RUN","scenarioId":"MISSING"}]}]}',
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				code: string;
				details: { result: { valid: boolean; errors: Array<{ code: string; }>; }; };
			};
			expect(report.code,).toBe("validation_failed",);
			expect(report.details.result.valid,).toBe(false,);
			expect(report.details.result.errors,).toContainEqual(
				expect.objectContaining({ code: "MISSING_SCENARIO", },),
			);
		},);
	});

	it("requires an explicit compare-manifest target despite an environment default", async () => {
		let requestCount = 0;
		await withCliServer((req, res,) => {
			requestCount += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "compare-manifest", "MYAPP",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("--project-key",);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("does not record cleanup when --wait cannot track the created instance", async () => {
		const dir = join(tmpdir(), `dss-app-untrackable-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const answerTarget = absentCreationTarget();
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, { appId: "MYAPP", projectKey: "NEWPROJ", },);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
						"--record-cleanup",
						ledger,
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("INDETERMINATE",);
				expect(failure.stdout,).toContain("may still have been created",);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(report.details.result,).toMatchObject({
					state: "INDETERMINATE",
					projectKey: "NEWPROJ",
					outcome: "indeterminate",
					creationPostAttempted: true,
					cleanupEligible: false,
				},);
			},);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("advertises exact plans for app manifest, instance, and permission mutations", async () => {
		await withCliServer(async () => {
			throw new Error("plan mode must not call the server",);
		}, async () => {
			const manifestPlan = JSON.parse(
				(
					await dss(
						[
							"app",
							"save-instance-manifest",
							"--data",
							'{"homepageSections":[]}',
							"--project-key",
							"TEMPLATE",
							"--plan",
						],
						{ env: hermetic, },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(manifestPlan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "save-instance-manifest",
				method: "PUT",
				endpoint: "/public/api/projects/TEMPLATE/app-manifest",
				projectKey: "TEMPLATE",
				payload: { homepageSections: [], },
			},);

			const createPlan = JSON.parse(
				(
					await dss(
						[
							"app",
							"create-instance",
							"MYAPP",
							"--data",
							'{"targetProjectKey":"NEWPROJ"}',
							"--plan",
						],
						{ env: hermetic, },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(createPlan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "create-instance",
				method: "POST",
				endpoint: "/public/api/apps/MYAPP/instances",
				appId: "MYAPP",
				payload: { targetProjectKey: "NEWPROJ", },
				wait: false,
				async: "future",
			},);

			const waitPlan = JSON.parse(
				(
					await dss(
						[
							"app",
							"create-instance",
							"MYAPP",
							"--data",
							'{"targetProjectKey":"NEWPROJ"}',
							"--wait",
							"--plan",
						],
						{ env: hermetic, },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(waitPlan.wait,).toBe(true,);
			expect(waitPlan.incarnationControl,).toBe(
				"client-side-non-atomic-future-target-and-creation-tag-join",
			);
			expect(waitPlan.incarnationObservationRequests,).toEqual([
				expect.objectContaining({
					method: "GET",
					endpoint: "/public/api/projects/NEWPROJ/",
					when: "after-terminal-future-target",
				},),
			],);
			expect(createPlan.incarnationControl,).toBe(
				"client-side-non-atomic-future-target-and-creation-tag-join",
			);
			expect(createPlan.incarnationObservationRequests,).toEqual([
				expect.objectContaining({
					method: "GET",
					endpoint: "/public/api/projects/NEWPROJ/",
					when: "conditional-inline-hasResult-target",
				},),
			],);
			expect(createPlan.exitCodesOnFailure,).toMatchObject({ longRunningFailure: 4, },);

			const deletePlan = JSON.parse(
				(
					await dss(
						["app", "delete-instance", "--project-key", "NEWPROJ", "--plan",],
						{ env: hermetic, },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(deletePlan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "delete-instance",
				method: "DELETE",
				endpoint: "/public/api/projects/NEWPROJ",
				projectKey: "NEWPROJ",
			},);

			const restorePlan = JSON.parse(
				(
					await dss(
						["app", "permissions-restore", "--file", "snap.json", "--project-key", "NEWPROJ", "--plan",],
						{ env: hermetic, },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(restorePlan,).toMatchObject({
				plan: true,
				resource: "app",
				action: "permissions-restore",
				method: "PUT",
				endpoint: "/public/api/projects/NEWPROJ/permissions",
				file: "snap.json",
				projectKey: "NEWPROJ",
				incarnationControl: "client-side-non-atomic-stale-identity-check",
			},);
			expect(restorePlan.preflightRequests,).toHaveLength(3,);
			expect(restorePlan.conditionalWrite,).toMatchObject({
				method: "PUT",
				when: "permissions-differ-and-dry-run-is-false",
			},);
			expect(restorePlan.verificationRequests,).toHaveLength(2,);
		},);
	});

	it("records an app delete-instance cleanup ledger entry from the created instance", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-instance",
			["MYAPP",],
			{},
			{
				appId: "MYAPP",
				projectKey: "NEWPROJ",
				jobId: "job-1",
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			undefined,
		);
		expect(entry,).toMatchObject({
			resource: "app",
			action: "create-instance",
			name: "NEWPROJ",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"NEWPROJ",
					"--future-id",
					"job-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);
		expect(typeof entry?.ts,).toBe("string",);
	});

	it("extracts a mismatched creation future ID from the nested response", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-instance",
			["MYAPP",],
			{ "project-key": "TEMPLATE", },
			{
				success: false,
				projectKey: "NEWPROJ",
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
				instance: { projectKey: "SOMEONE_ELSE", jobId: "job-1", },
			},
			"TEMPLATE",
		);
		expect(entry,).toMatchObject({
			projectKey: "NEWPROJ",
			name: "NEWPROJ",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"NEWPROJ",
					"--future-id",
					"job-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);
		expect(JSON.stringify(entry,),).not.toContain("TEMPLATE",);
		expect(JSON.stringify(entry,),).not.toContain("SOMEONE_ELSE",);
	});

	it("skips the cleanup ledger entry when no target project key resolved", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-instance",
			["MYAPP",],
			{},
			{ appId: "MYAPP", },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("records successor cleanup targeting the successor project, never the predecessor", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{
				success: true,
				state: "DONE",
				sourceProjectKey: "FROMKEY",
				projectKey: "TOKEY",
				futureTargetVerified: true,
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			"FROMKEY",
		);
		expect(entry,).toMatchObject({
			resource: "app",
			action: "create-successor-instance",
			name: "TOKEY",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);
		expect(JSON.stringify(entry,),).not.toContain("FROMKEY",);
	});

	it("treats futureTargetVerified as settled creation even when later verification failed", () => {
		const verified = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{
				success: false,
				state: "VERIFICATION_FAILED",
				projectKey: "TOKEY",
				futureId: "job-1",
				futureTargetVerified: true,
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			"FROMKEY",
		);
		expect(verified,).toMatchObject({
			resource: "app",
			action: "create-successor-instance",
			name: "TOKEY",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);
		expect(JSON.stringify(verified,),).not.toContain("--future-id",);
		expect(JSON.stringify(verified,),).not.toContain("--unconfirmed-creation",);

		const mismatched = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{
				success: false,
				state: "VERIFICATION_FAILED",
				projectKey: "TOKEY",
				futureId: "job-1",
				futureTargetMismatch: true,
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			"FROMKEY",
		);
		expect(mismatched,).toMatchObject({
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--future-id",
					"job-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);

		// A terminal result alone proves nothing: no target was named, so the
		// delete still future-gates on the recorded future ID.
		const targetless = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{
				success: true,
				state: "DONE",
				hasResult: true,
				projectKey: "TOKEY",
				futureId: "job-1",
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			"FROMKEY",
		);
		expect(targetless,).toMatchObject({
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--future-id",
					"job-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
			},
		},);

		const targetlessInline = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{
				success: true,
				state: "DONE",
				hasResult: true,
				projectKey: "TOKEY",
				projectIncarnationHash: PROJECT_INCARNATION_HASH,
			},
			"FROMKEY",
		);
		expect(targetlessInline,).toMatchObject({
			cleanup: {
				argv: ["app", "delete-instance", "--project-key", "TOKEY", "--unconfirmed-creation",],
			},
		},);

		const noFuture = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{ success: false, state: "INDETERMINATE", projectKey: "TOKEY", },
			"FROMKEY",
		);
		expect(noFuture,).toMatchObject({
			cleanup: {
				argv: ["app", "delete-instance", "--project-key", "TOKEY", "--unconfirmed-creation",],
			},
		},);
	});

	it("records successor cleanup from the failed-wait result shape", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{ success: false, state: "FAILED", projectKey: "TOKEY", },
			undefined,
		);
		expect(entry,).toMatchObject({
			resource: "app",
			action: "create-successor-instance",
			name: "TOKEY",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--unconfirmed-creation",
				],
			},
		},);
	});

	it("skips successor cleanup only when the result explicitly marks cleanupEligible false", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{ success: false, state: "CREATE_FAILED", projectKey: "TOKEY", cleanupEligible: false, },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("records successor cleanup for cleanupEligible true results", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{ success: false, state: "WAIT_FAILED", projectKey: "TOKEY", cleanupEligible: true, },
			undefined,
		);
		expect(entry,).toMatchObject({
			resource: "app",
			action: "create-successor-instance",
			name: "TOKEY",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--unconfirmed-creation",
				],
			},
		},);
	});

	it("treats an absent cleanupEligible as eligible and never targets the predecessor", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", },
			{ success: false, state: "WAIT_FAILED", projectKey: "TOKEY", },
			undefined,
		);
		expect(entry,).toMatchObject({
			resource: "app",
			action: "create-successor-instance",
			name: "TOKEY",
			cleanup: {
				argv: [
					"app",
					"delete-instance",
					"--project-key",
					"TOKEY",
					"--unconfirmed-creation",
				],
			},
		},);
		expect(JSON.stringify(entry,),).not.toContain("FROMKEY",);
	});

	it("skips successor cleanup for dry-run results without a target project key", () => {
		const entry = cleanupLedgerEntry(
			"app",
			"create-successor-instance",
			["MYAPP",],
			{ from: "FROMKEY", to: "TOKEY", "dry-run": true, },
			{ dryRun: true, },
			undefined,
		);
		expect(entry,).toBeUndefined();
	});

	it("previews and applies the recorded cleanup entry through dss cleanup", async () => {
		const dir = join(tmpdir(), "dss-app-instance-cleanup",);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		writeFileSync(
			ledger,
			`${
				JSON.stringify({
					ts: "2026-08-12T00:00:00.000Z",
					action: "create-instance",
					resource: "app",
					name: "NEWPROJ",
					cleanup: { argv: ["app", "delete-instance", "--project-key", "NEWPROJ",], },
				},)
			}\n`,
		);

		const preview = JSON.parse(
			(await dss(["cleanup", "--file", ledger,], { env: hermetic, },)).stdout,
		) as { dryRun: boolean; steps: Array<Record<string, unknown>>; };
		expect(preview.dryRun,).toBe(true,);
		expect(preview.steps,).toHaveLength(1,);
		expect(preview.steps[0],).toMatchObject({
			resource: "app",
			action: "create-instance",
			dssUrl: null,
			cleanup: { argv: ["app", "delete-instance", "--project-key", "NEWPROJ",], },
		},);

		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, PROJECT_DETAILS,);
				return;
			}
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest"
			) {
				sendJson(res, { projectAppType: "APP_INSTANCE", },);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			writeFileSync(
				ledger,
				`${
					JSON.stringify({
						ts: "2026-08-12T00:00:00.000Z",
						action: "create-instance",
						resource: "app",
						name: "NEWPROJ",
						dssUrl: url,
						cleanup: {
							argv: [
								"app",
								"delete-instance",
								"--project-key",
								"NEWPROJ",
								"--expect-project-incarnation",
								PROJECT_INCARNATION_HASH,
							],
						},
					},)
				}\n`,
			);
			const applied = JSON.parse(
				(
					await dss(["cleanup", "--file", ledger, "--apply",], { env: cliEnv(url,), },)
				).stdout,
			) as { applied: boolean; results: Array<Record<string, unknown>>; failures: unknown[]; };
			expect(applied.applied,).toBe(true,);
			expect(applied.failures,).toHaveLength(0,);
			expect(applied.results,).toHaveLength(1,);
			expect(applied.results[0],).toMatchObject({
				cleanup: {
					argv: [
						"app",
						"delete-instance",
						"--project-key",
						"NEWPROJ",
						"--expect-project-incarnation",
						PROJECT_INCARNATION_HASH,
					],
				},
			},);
		},);

		rmSync(dir, { recursive: true, force: true, },);
	});

	it("preflights every app cleanup binding before applying the first step", async () => {
		const dir = join(tmpdir(), `dss-app-instance-mixed-cleanup-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		let requests = 0;
		try {
			await withCliServer((_req, res,) => {
				requests += 1;
				res.statusCode = 500;
				res.end("cleanup must not start",);
			}, async (url,) => {
				const common = {
					ts: "2026-08-13T00:00:00.000Z",
					action: "create-instance",
					resource: "app",
					name: "NEWPROJ",
					dssUrl: url,
				};
				writeFileSync(
					ledger,
					[
						{
							...common,
							cleanup: {
								argv: ["app", "delete-instance", "--project-key", "NEWPROJ",],
							},
						},
						{
							...common,
							cleanup: {
								argv: [
									"app",
									"delete-instance",
									"--project-key",
									"NEWPROJ",
									"--expect-project-incarnation",
									PROJECT_INCARNATION_HASH,
								],
							},
						},
					].map((entry,) => JSON.stringify(entry,)).join("\n",) + "\n",
				);
				const failure = await dssFailure(
					["cleanup", "--file", ledger, "--apply",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(2,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					applied: boolean;
					lifecycleError: Record<string, unknown>;
				};
				expect(report.applied,).toBe(false,);
				expect(report.lifecycleError,).toMatchObject({
					entryIndex: 1,
					resource: "app",
					action: "delete-instance",
					reason: "missing",
				},);
				expect(requests,).toBe(0,);
			},);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("completes an inline hasResult creation with --wait without polling", async () => {
		let futureGets = 0;
		const answerTarget = absentCreationTarget();
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (answerTarget(req.method, url, res,)) return;
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				sendJson(res, {
					appId: "MYAPP",
					projectKey: "NEWPROJ",
					jobId: "job-1",
					hasResult: true,
					result: { projectKey: "NEWPROJ", },
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				futureGets += 1;
				res.statusCode = 500;
				res.end("inline completions must not poll",);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss([
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
					], { env: cliEnv(url,), },)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				success: true,
				state: "DONE",
				hasResult: true,
				projectKey: "NEWPROJ",
				pollCount: 0,
				futureTargetVerified: true,
			},);
			expect(futureGets,).toBe(0,);
		},);
	});
	it("rejects target identities contradicted by inline and polled future results", async () => {
		for (const inline of [true, false,]) {
			const answerTarget = absentCreationTarget();
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(
						res,
						inline
							? {
								appId: "MYAPP",
								projectKey: "NEWPROJ",
								jobId: "job-1",
								hasResult: true,
								result: { targetProjectKey: "SOMEONE_ELSE", },
							}
							: { appId: "MYAPP", projectKey: "NEWPROJ", jobId: "job-1", },
					);
					return;
				}
				if (!inline && req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
					sendJson(res, {
						jobId: "job-1",
						hasResult: true,
						alive: false,
						result: { targetProjectKey: "SOMEONE_ELSE", },
					},);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(report.details.result,).toMatchObject({
					success: false,
					state: "VERIFICATION_FAILED",
					projectKey: "NEWPROJ",
					futureId: "job-1",
					jobId: "job-1",
					cleanupEligible: true,
					expected: { projectKey: "NEWPROJ", },
					actual: { projectKey: "SOMEONE_ELSE", field: "targetProjectKey", },
				},);
			},);
		}
	});

	it("does not record cleanup for an empty 2xx creation response", async () => {
		const dir = join(tmpdir(), `dss-app-empty-response-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const ledger = join(dir, "cleanup.jsonl",);
		const answerTarget = absentCreationTarget();
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, null,);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--record-cleanup",
						ledger,
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(failure.stdout,).toContain("INDETERMINATE",);
				expect(report.details.result,).toMatchObject({
					state: "INDETERMINATE",
					projectKey: "NEWPROJ",
					responseKind: "null",
					creationPostAttempted: true,
					cleanupEligible: false,
				},);
			},);
			expect(readFileExists(ledger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});
	it("fails a creation response that names a different target project and keeps the requested key", async () => {
		let futureGets = 0;
		const answerTarget = absentCreationTarget();
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (answerTarget(req.method, url, res,)) return;
			if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
				sendJson(res, { appId: "MYAPP", projectKey: "SOMEONE_ELSE", jobId: "job-1", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/job-1") {
				futureGets += 1;
				res.statusCode = 500;
				res.end("mismatched responses must fail before polling",);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"create-instance",
					"MYAPP",
					"--data",
					'{"targetProjectKey":"NEWPROJ"}',
					"--wait",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("VERIFICATION_FAILED",);
			const details = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(details.details.result,).toMatchObject({
				projectKey: "NEWPROJ",
				expected: { projectKey: "NEWPROJ", },
				actual: { projectKey: "SOMEONE_ELSE", },
				cleanupEligible: true,
			},);
			expect(futureGets,).toBe(0,);
			expect(details.details.result,).toMatchObject({ futureId: "job-1", jobId: "job-1", },);
		},);
	});

	it("does not record cleanup for an indeterminate 5xx create failure or a definitive rejection", async () => {
		const dir = join(tmpdir(), `dss-app-create-failures-${Date.now()}`,);
		mkdirSync(dir, { recursive: true, },);
		const indeterminateLedger = join(dir, "indeterminate.jsonl",);
		const rejectedLedger = join(dir, "rejected.jsonl",);
		const answerIndeterminateTarget = absentCreationTarget();
		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerIndeterminateTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, { message: "boom", }, 500,);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
						"--record-cleanup",
						indeterminateLedger,
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.stdout,).toContain("INDETERMINATE",);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(report.details.result,).toMatchObject({
					state: "INDETERMINATE",
					projectKey: "NEWPROJ",
					outcome: "indeterminate",
					creationPostAttempted: true,
					cleanupEligible: false,
				},);
			},);
			expect(readFileExists(indeterminateLedger,),).toBe(false,);

			const answerRejectedTarget = absentCreationTarget();
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (answerRejectedTarget(req.method, url, res,)) return;
				if (req.method === "POST" && url.pathname === "/public/api/apps/MYAPP/instances") {
					sendJson(res, { message: "Project key already exists", }, 400,);
					return;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					[
						"app",
						"create-instance",
						"MYAPP",
						"--data",
						'{"targetProjectKey":"NEWPROJ"}',
						"--wait",
						"--record-cleanup",
						rejectedLedger,
					],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				expect(failure.stdout,).toContain("CREATE_FAILED",);
			},);
			expect(readFileExists(rejectedLedger,),).toBe(false,);
		} finally {
			rmSync(dir, { recursive: true, force: true, },);
		}
	});

	it("waits without aborting and blocks deletion while the creation future is live", async () => {
		let futureDeletes = 0;
		let projectDeletes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", },);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
				sendJson(res, { jobId: "f-1", hasResult: false, alive: true, },);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/futures/f-1") {
				futureDeletes += 1;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				projectDeletes += 1;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"delete-instance",
					"--project-key",
					"NEWPROJ",
					"--future-id",
					"f-1",
					"--timeout",
					"300",
					"--poll-interval",
					"50",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const details = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(details.details.result,).toMatchObject({
				success: false,
				state: "FUTURE_STILL_RUNNING",
				projectKey: "NEWPROJ",
				futureId: "f-1",
				terminal: false,
				deletePerformed: false,
			},);
			expect(futureDeletes,).toBe(0,);
			expect(projectDeletes,).toBe(0,);
		},);
	});

	it("deletes only after a terminal future reports the requested target", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
				sendJson(res, {
					jobId: "f-1",
					hasResult: true,
					alive: false,
					result: { targetProjectKey: "NEWPROJ", },
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, PROJECT_DETAILS,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", },);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				res.statusCode = 204;
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
							"delete-instance",
							"--project-key",
							"NEWPROJ",
							"--future-id",
							"f-1",
							"--expect-project-incarnation",
							PROJECT_INCARNATION_HASH,
						],
						{ env: cliEnv(url,), },
					)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toMatchObject({
				deleted: true,
				success: true,
				projectKey: "NEWPROJ",
				futureId: "f-1",
				futureState: "DONE",
			},);
		},);
	});

	it("refuses to delete a replacement project after the creation future settles", async () => {
		let projectDeletes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
				sendJson(res, {
					jobId: "f-1",
					hasResult: true,
					alive: false,
					result: { targetProjectKey: "NEWPROJ", },
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, REPLACEMENT_PROJECT_DETAILS,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", },);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				projectDeletes += 1;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"delete-instance",
					"--project-key",
					"NEWPROJ",
					"--future-id",
					"f-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain("not the project incarnation authorized for deletion",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				success: false,
				state: "DELETE_FAILED",
				projectKey: "NEWPROJ",
				futureId: "f-1",
				deletePerformed: false,
			},);
			expect(projectDeletes,).toBe(0,);
		},);
	});
	it("refuses absent or mismatched future identities without deleting the project", async () => {
		for (const futureMode of ["absent", "mismatch",] as const) {
			let projectDeletes = 0;
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
					sendJson(res, { projectAppType: "APP_INSTANCE", },);
					return;
				}
				if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
					if (futureMode === "absent") {
						sendJson(res, { message: "Future not found", }, 404,);
					} else {
						sendJson(res, {
							jobId: "f-1",
							hasResult: true,
							alive: false,
							result: { targetProjectKey: "SOMEONE_ELSE", },
						},);
					}
					return;
				}
				if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
					projectDeletes += 1;
				}
				res.statusCode = 500;
				res.end(`unexpected ${req.method} ${url.pathname}`,);
			}, async (url,) => {
				const failure = await dssFailure(
					["app", "delete-instance", "--project-key", "NEWPROJ", "--future-id", "f-1",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(4,);
				expect(failure.stderr,).toBe("",);
				const report = JSON.parse(failure.stdout,) as {
					details: { result: Record<string, unknown>; };
				};
				expect(report.details.result,).toMatchObject({
					success: false,
					state: futureMode === "absent" ? "FUTURE_UNVERIFIABLE" : "VERIFICATION_FAILED",
					projectKey: "NEWPROJ",
					futureId: "f-1",
					deletePerformed: false,
				},);
				expect(projectDeletes,).toBe(0,);
			},);
		}
	});

	it("rejects an invalid delete target before touching its supplied future", async () => {
		let futureRequests = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/TEMPLATE/app-manifest") {
				sendJson(res, { projectAppType: "APP_TEMPLATE", },);
				return;
			}
			if (url.pathname === "/public/api/futures/f-1") futureRequests += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "delete-instance", "--project-key", "TEMPLATE", "--future-id", "f-1",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(futureRequests,).toBe(0,);
		},);
	});

	it("classifies an ordinary-project 400 before touching its supplied future", async () => {
		let futureRequests = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/ORDINARY/app-manifest") {
				sendJson(res, { message: "neither an app template nor an app instance", }, 400,);
				return;
			}
			if (url.pathname === "/public/api/futures/f-1") futureRequests += 1;
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "delete-instance", "--project-key", "ORDINARY", "--future-id", "f-1",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(1,);
			expect(failure.stderr,).toBe("",);
			expect(failure.stdout,).toContain(
				"Only classic Dataiku App instance projects can be deleted",
			);
			expect(failure.stdout,).toContain("use `dss project delete`",);
			expect(futureRequests,).toBe(0,);
		},);
	});

	it("treats a missing no-future cleanup target as already absent", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/GONE/app-manifest") {
				sendJson(res, { message: "Project not found", }, 404,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const result = JSON.parse(
				(
					await dss(["app", "delete-instance", "--project-key", "GONE",], {
						env: cliEnv(url,),
					},)
				).stdout,
			) as Record<string, unknown>;
			expect(result,).toEqual({ deleted: false, alreadyAbsent: true, projectKey: "GONE", },);
		},);
	});

	it("keeps unconfirmed cleanup unresolved without touching DSS", async () => {
		let requests = 0;
		await withCliServer((_req, res,) => {
			requests += 1;
			res.statusCode = 500;
			res.end("unconfirmed cleanup must not touch DSS",);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "delete-instance", "--project-key", "NEWPROJ", "--unconfirmed-creation",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				success: false,
				state: "UNCONFIRMED_CREATION",
				projectKey: "NEWPROJ",
				deletePerformed: false,
				cleanupResolved: false,
			},);
			expect(requests,).toBe(0,);
		},);
	});

	it("reports an ambiguous bare project delete as unresolved", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				sendJson(res, { projectAppType: "APP_INSTANCE", },);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				sendJson(res, { message: "post-effect failure", }, 500,);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "delete-instance", "--project-key", "NEWPROJ",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				success: false,
				state: "DELETE_FAILED",
				projectKey: "NEWPROJ",
				deletePerformed: null,
			},);
		},);
	});

	it("reports no delete was attempted when the bare pre-delete manifest GET fails", async () => {
		let projectDeletes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				sendJson(res, { message: "manifest read failure", }, 503,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				projectDeletes += 1;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				["app", "delete-instance", "--project-key", "NEWPROJ", "--retries", "1",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				success: false,
				state: "DELETE_FAILED",
				projectKey: "NEWPROJ",
				deletePerformed: false,
			},);
			expect(report.details.result.remediation,).toContain("Deletion was not attempted",);
			expect(projectDeletes,).toBe(0,);
		},);
	});

	it("reports no delete was attempted when the future-gated pre-delete manifest GET fails", async () => {
		let manifestGets = 0;
		let projectDeletes = 0;
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
				sendJson(res, {
					jobId: "f-1",
					hasResult: true,
					alive: false,
					result: { targetProjectKey: "NEWPROJ", },
				},);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/") {
				sendJson(res, PROJECT_DETAILS,);
				return;
			}
			if (req.method === "GET" && url.pathname === "/public/api/projects/NEWPROJ/app-manifest") {
				manifestGets += 1;
				if (manifestGets === 1) {
					sendJson(res, { projectAppType: "APP_INSTANCE", },);
					return;
				}
				sendJson(res, { message: "manifest read failure", }, 503,);
				return;
			}
			if (req.method === "DELETE" && url.pathname === "/public/api/projects/NEWPROJ") {
				projectDeletes += 1;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const failure = await dssFailure(
				[
					"app",
					"delete-instance",
					"--project-key",
					"NEWPROJ",
					"--future-id",
					"f-1",
					"--expect-project-incarnation",
					PROJECT_INCARNATION_HASH,
					"--retries",
					"1",
				],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).toBe(4,);
			expect(failure.stderr,).toBe("",);
			const report = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(report.details.result,).toMatchObject({
				success: false,
				state: "DELETE_FAILED",
				projectKey: "NEWPROJ",
				futureId: "f-1",
				deletePerformed: false,
			},);
			expect(report.details.result.remediation,).toContain("Deletion was not attempted",);
			expect(projectDeletes,).toBe(0,);
		},);
	});

	it("rejects blank project and future IDs before any DSS request", async () => {
		let requests = 0;
		await withCliServer((_req, res,) => {
			requests += 1;
			res.statusCode = 500;
			res.end("validation must happen before DSS access",);
		}, async (url,) => {
			const blankProject = await dssFailure(
				["app", "delete-instance", "--project-key", " ",],
				{ env: cliEnv(url,), },
			);
			expect(blankProject.code,).toBe(1,);
			expect(blankProject.stderr,).toBe("",);
			const blankFuture = await dssFailure(
				["app", "delete-instance", "--project-key", "NEWPROJ", "--future-id", " ",],
				{ env: cliEnv(url,), },
			);
			expect(blankFuture.code,).toBe(1,);
			expect(blankFuture.stderr,).toBe("",);
			expect(requests,).toBe(0,);
		},);
	});

	it("honors a wait timeout smaller than the poll interval", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "GET" && url.pathname === "/public/api/futures/f-1") {
				sendJson(res, { jobId: "f-1", hasResult: false, alive: true, },);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${url.pathname}`,);
		}, async (url,) => {
			const startedAt = Date.now();
			const failure = await dssFailure(
				["future", "wait", "f-1", "--timeout", "300", "--poll-interval", "60000",],
				{ env: cliEnv(url,), },
			);
			const details = JSON.parse(failure.stdout,) as {
				details: { result: Record<string, unknown>; };
			};
			expect(details.details.result,).toMatchObject({
				success: false,
				timedOut: true,
				futureId: "f-1",
			},);
			expect(
				details.details.result.elapsedMs,
				"wait budget must not be stretched to a full poll interval",
			).toBeLessThan(5_000,);
			expect(Date.now() - startedAt,).toBeLessThan(5_000,);
		},);
	});
});
