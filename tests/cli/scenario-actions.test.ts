import { describe, expect, it, } from "bun:test";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	readBody,
	readFileSync,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

type Sent = { method: string; path: string; body: unknown; };

const SCENARIOS = "/public/api/projects/TEST/scenarios";
const WEBHOOK = "https://hooks.slack.com/services/T000/B000/SECRET-WEBHOOK-TOKEN";

/**
 * Mock DSS for the scenario actions: `answer` maps `METHOD path?query` to a
 * response body; every request (with its parsed JSON body) is recorded. A
 * request nobody answers gets a 404, which fails the command under test.
 */
async function withScenarioCli(
	answer: (request: Sent,) => unknown,
	run: (env: NodeJS.ProcessEnv, sent: Sent[],) => Promise<void>,
): Promise<void> {
	const sent: Sent[] = [];
	await withCliServer(async (req, res,) => {
		const text = await readBody(req,);
		const request = {
			method: req.method ?? "",
			path: req.url ?? "",
			body: text ? JSON.parse(text,) : undefined,
		};
		sent.push(request,);
		const body = answer(request,);
		if (body === undefined) sendJson(res, { message: "unrouted", }, 404,);
		else sendJson(res, body,);
	}, async (url,) => {
		await run(cliEnv(url,), sent,);
	},);
}

function requestLines(sent: Sent[],): string[] {
	return sent.map((request,) => `${request.method} ${request.path}`);
}

/** A scenario that keeps what was PUT, like DSS (without the id/stamp assignment it adds to new elements). */
function storedScenario(initial: Record<string, unknown>,) {
	let stored = initial;
	return {
		answer: (request: Sent,) => {
			if (request.path !== `${SCENARIOS}/s1/`) return undefined;
			if (request.method === "PUT" && typeof request.body === "object" && request.body !== null) {
				stored = { ...request.body, };
				return { msg: "ok", };
			}
			return request.method === "GET" ? stored : undefined;
		},
	};
}

const SLACK_REPORTER = {
	id: "r-slack",
	name: "slack",
	active: true,
	phase: "END",
	messaging: { type: "slack-channel", configuration: { channelId: "ops", webhookUrl: WEBHOOK, }, },
};
const MAIL_REPORTER = {
	id: "r-mail",
	name: "mail",
	active: true,
	phase: "START",
	runConditionEnabled: true,
	runCondition: "outcome == 'FAILED'",
	messaging: {
		type: "mail-scenario",
		configuration: { channelId: "smtp", recipient: "a@x.com", subject: "S", },
	},
};
const TRIGGER_A = { id: "t-a", name: "nightly", type: "temporal", active: true, params: {}, };
const TRIGGER_B = { id: "t-b", name: "ds", type: "dataset_modified", active: true, params: {}, };

describe("scenario run params and trigger-run identity", () => {
	it("run --params sends the JSON as the run body and points to get-run-for-trigger", async () => {
		await withScenarioCli((request,) => {
			if (request.method === "POST" && request.path === `${SCENARIOS}/s1/run/`) {
				return { trigger: { id: "manual", }, runId: "tr-1", };
			}
			return undefined;
		}, async (env, sent,) => {
			const out = JSON.parse(
				(await dss(["scenario", "run", "s1", "--params", '{"region":"EU"}',], { env, },)).stdout,
			);
			expect(sent,).toEqual([{
				method: "POST",
				path: `${SCENARIOS}/s1/run/`,
				body: { region: "EU", },
			},],);
			expect(out,).toMatchObject({
				triggerId: "manual",
				triggerRunId: "tr-1",
				cancelled: false,
				next: [["dss", "scenario", "get-run-for-trigger", "s1", "tr-1",],],
			},);
			expect(out,).not.toHaveProperty("runId",);
		},);
	});

	it("run --params-file reads the body from the file; both flags together are a usage error", async () => {
		const file = join(tmpdir(), `dss-scenario-params-${process.pid}.json`,);
		writeFileSync(file, '{"region":"US","limit":3}',);
		try {
			await withScenarioCli((request,) => {
				if (request.method === "POST") return { trigger: { id: "manual", }, runId: "tr-2", };
				return undefined;
			}, async (env, sent,) => {
				await dss(["scenario", "run", "s1", "--params-file", file,], { env, },);
				expect(sent[0]?.body,).toEqual({ region: "US", limit: 3, },);
				const failure = await dssFailure(
					["scenario", "run", "s1", "--params", "{}", "--params-file", file,],
					{ env, },
				);
				expect(failure.code,).toBe(1,);
				expect(failure.stdout,).toContain("not both",);
				expect(sent,).toHaveLength(1,);
			},);
		} finally {
			rmSync(file, { force: true, },);
		}
	});

	it("run --dry-run sends nothing and shows the params", async () => {
		await withScenarioCli(() => undefined, async (env, sent,) => {
			const out = JSON.parse(
				(await dss(["scenario", "run", "s1", "--params", '{"a":1}', "--dry-run",], { env, },))
					.stdout,
			);
			expect(out,).toMatchObject({ dryRun: true, method: "POST", params: { a: 1, }, },);
			expect(sent,).toEqual([],);
		},);
	});

	it("run-and-wait --params sends the body too", async () => {
		await withScenarioCli((request,) => {
			if (request.method === "POST") return { trigger: { id: "manual", }, runId: "tr-3", };
			if (request.path.startsWith(`${SCENARIOS}/s1/get-run-for-trigger`,)) {
				return { scenarioRun: { runId: "run-3", result: { outcome: "SUCCESS", }, }, };
			}
			return undefined;
		}, async (env, sent,) => {
			const out = JSON.parse(
				(await dss(
					["scenario", "run-and-wait", "s1", "--params", '{"k":"v"}', "--timeout", "5000",],
					{ env, },
				)).stdout,
			);
			expect(sent[0]?.body,).toEqual({ k: "v", },);
			expect(out,).toMatchObject({ runId: "run-3", success: true, },);
		},);
	});
});

describe("cancelled trigger fire", () => {
	for (const argv of [["run-and-wait", "s1",], ["run", "s1", "--wait",],]) {
		it(`scenario ${argv.join(" ",)} reports CANCELLED with a message, not TIMEOUT`, async () => {
			await withScenarioCli((request,) => {
				if (request.method === "POST") {
					return { trigger: { id: "manual", }, runId: "tr-c", cancelled: true, };
				}
				return undefined;
			}, async (env, sent,) => {
				const failure = await dssFailure(["scenario", ...argv,], { env, },);
				expect(failure.code,).toBe(4,);
				const out = JSON.parse(failure.stdout,);
				expect(out,).toMatchObject({
					code: "long_running_failure",
					exitCode: 4,
					details: {
						result: { outcome: "CANCELLED", success: false, cancelled: true, triggerRunId: "tr-c", },
					},
				},);
				expect(out.error,).toContain("already running",);
				expect(failure.stdout,).not.toContain("TIMEOUT",);
				expect(requestLines(sent,),).toEqual([`POST ${SCENARIOS}/s1/run/`,],);
			},);
		});
	}
});

describe("scenario get-run-for-trigger", () => {
	const poll = `${SCENARIOS}/s1/get-run-for-trigger?triggerId=manual&triggerRunId=tr-1`;
	const fire = `${SCENARIOS}/trigger/s1/manual?triggerRunId=tr-1`;

	it("resolves a finished fire to its run id, outcome and the follow-up reads", async () => {
		await withScenarioCli((request,) => {
			if (request.path === poll) {
				return { scenarioRun: { runId: "run-9", result: { outcome: "FAILED", }, }, };
			}
			return undefined;
		}, async (env,) => {
			const out = JSON.parse(
				(await dss(["scenario", "get-run-for-trigger", "s1", "tr-1",], { env, },)).stdout,
			);
			expect(out,).toMatchObject({
				state: "FINISHED",
				runId: "run-9",
				outcome: "FAILED",
				next: [
					["dss", "scenario", "get-run", "s1", "run-9",],
					["dss", "scenario", "log", "s1", "run-9",],
				],
			},);
		},);
	});

	it("reports a cancelled fire and a still pending one, passing --trigger-id through", async () => {
		await withScenarioCli((request,) => {
			if (request.path.startsWith(`${SCENARIOS}/s1/get-run-for-trigger`,)) return {};
			if (request.path === fire) return { cancelled: true, };
			if (request.path === `${SCENARIOS}/trigger/s1/nightly?triggerRunId=tr-2`) {
				return { cancelled: false, };
			}
			return undefined;
		}, async (env,) => {
			const cancelled = JSON.parse(
				(await dss(["scenario", "get-run-for-trigger", "s1", "tr-1",], { env, },)).stdout,
			);
			expect(cancelled,).toMatchObject({ state: "CANCELLED", cancelled: true, },);
			expect(cancelled.hint,).toContain("already running",);
			expect(cancelled,).not.toHaveProperty("runId",);
			const pending = JSON.parse(
				(await dss(
					["scenario", "get-run-for-trigger", "s1", "tr-2", "--trigger-id", "nightly",],
					{ env, },
				)).stdout,
			);
			expect(pending,).toMatchObject({
				state: "PENDING",
				next: [[
					"dss",
					"scenario",
					"get-run-for-trigger",
					"s1",
					"tr-2",
					"--trigger-id",
					"nightly",
				],],
			},);
		},);
	});
});

describe("scenario run history", () => {
	it("last-runs --only-finished asks DSS for finished runs only", async () => {
		await withScenarioCli((request,) => {
			if (request.path === `${SCENARIOS}/s1/get-last-runs/?limit=1&onlyFinishedRuns=true`) {
				return [{ runId: "r1", result: { outcome: "SUCCESS", }, },];
			}
			return undefined;
		}, async (env,) => {
			const out = JSON.parse(
				(await dss(["scenario", "last-runs", "s1", "--only-finished", "--limit", "1",], { env, },))
					.stdout,
			);
			expect(out,).toHaveLength(1,);
			expect(out[0],).toMatchObject({ runId: "r1", },);
		},);
	});

	it("runs-by-date sends the day bounds, defaulting --to to tomorrow", async () => {
		await withScenarioCli(() => [], async (env, sent,) => {
			await dss([
				"scenario",
				"runs-by-date",
				"s1",
				"--from",
				"2026-10-01T08:00:00Z",
				"--to",
				"2026-10-31",
			], { env, },);
			expect(requestLines(sent,),).toEqual([
				`GET ${SCENARIOS}/s1/get-runs-by-date?fromDate=2026-10-01&toDate=2026-10-31`,
			],);
			sent.length = 0;
			await dss(["scenario", "runs-by-date", "s1", "--from", "2026-10-01",], { env, },);
			const query = new URL(sent[0]?.path ?? "", "http://x",).searchParams;
			expect(query.get("fromDate",),).toBe("2026-10-01",);
			expect(query.get("toDate",),).toMatch(/^\d{4}-\d{2}-\d{2}$/,);
			expect((query.get("toDate",) ?? "") > "2026-10-01",).toBe(true,);
		},);
	});

	it("runs-by-date requires --from and rejects a malformed date before any request", async () => {
		await withScenarioCli(() => [], async (env, sent,) => {
			const missing = await dssFailure(["scenario", "runs-by-date", "s1",], { env, },);
			expect(missing.code,).toBe(1,);
			expect(missing.stdout,).toContain("--from is required",);
			const bad = await dssFailure(
				["scenario", "runs-by-date", "s1", "--from", "last tuesday",],
				{ env, },
			);
			expect(bad.code,).toBe(1,);
			expect(sent,).toEqual([],);
		},);
	});
});

describe("scenario run-report", () => {
	const xml = '<testsuites><testsuite name="s"><testcase name="t"/></testsuite></testsuites>';

	it("prints the report, targets one step with --step-id, and writes --output", async () => {
		const file = join(tmpdir(), `dss-scenario-report-${process.pid}.xml`,);
		await withCliServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			res.setHeader("content-type", "application/xml",);
			res.end(xml,);
		}, async (url,) => {
			const env = cliEnv(url,);
			try {
				expect((await dss(["scenario", "run-report", "s1", "r1",], { env, },)).stdout,).toContain(
					"<testcase",
				);
				const written = JSON.parse(
					(await dss(["scenario", "run-report", "s1", "r1", "--output", file,], { env, },)).stdout,
				);
				expect(written,).toMatchObject({ truncated: false, },);
				expect(readFileSync(file, "utf8",),).toBe(xml,);
			} finally {
				rmSync(file, { force: true, },);
			}
		},);
	});

	it("requests the step route with an encoded step id", async () => {
		const paths: string[] = [];
		await withCliServer((req, res,) => {
			paths.push(req.url ?? "",);
			res.end("<testsuites/>",);
		}, async (url,) => {
			const env = cliEnv(url,);
			await dss(["scenario", "run-report", "s1", "r1",], { env, },);
			await dss(["scenario", "run-report", "s1", "r1", "--step-id", "run tests",], { env, },);
		},);
		expect(paths,).toEqual([
			`${SCENARIOS}/s1/r1/scenario-run-report`,
			`${SCENARIOS}/s1/r1/step-run-report?stepId=run%20tests`,
		],);
	});
});

describe("scenario create --data", () => {
	it("merges the JSON into the create body and previews it with --dry-run", async () => {
		await withScenarioCli((request,) => {
			if (request.method === "GET" && request.path === `${SCENARIOS}/`) return [];
			if (request.method === "POST" && request.path === `${SCENARIOS}/`) return {};
			return undefined;
		}, async (env, sent,) => {
			const argv = ["scenario", "create", "nightly", "Nightly", "--data", '{"markedAsTest":true}',];
			const preview = JSON.parse((await dss([...argv, "--dry-run",], { env, },)).stdout,);
			expect(preview.payload,).toMatchObject({
				scenarioId: "nightly",
				data: { markedAsTest: true, },
			},);
			expect(sent.filter((request,) => request.method === "POST"),).toEqual([],);
			await dss(argv, { env, },);
			expect(sent.at(-1,),).toMatchObject({
				method: "POST",
				body: { id: "nightly", name: "Nightly", markedAsTest: true, },
			},);
		},);
	});
});

describe("scenario trigger-add / trigger-list / trigger-remove", () => {
	const settings = {
		id: "s1",
		name: "S1",
		params: { steps: [], },
		triggers: [TRIGGER_A, TRIGGER_B,],
	};

	it("trigger-add appends a built schedule and keeps the other triggers and fields", async () => {
		const store = storedScenario(settings,);
		await withScenarioCli(store.answer, async (env, sent,) => {
			const out = JSON.parse(
				(await dss([
					"scenario",
					"trigger-add",
					"s1",
					"--frequency",
					"daily",
					"--hour",
					"6",
					"--minute",
					"30",
					"--start-date",
					"2026-11-02",
					"--name",
					"six",
				], { env, },)).stdout,
			);
			expect(out,).toMatchObject({
				added: "trigger",
				index: 2,
				count: 3,
				trigger: { name: "six", params: { frequency: "Daily", hour: 6, minute: 30, }, },
				next: [["dss", "scenario", "trigger-list", "s1",],],
			},);
			expect(requestLines(sent,),).toEqual([
				`GET ${SCENARIOS}/s1/`,
				`PUT ${SCENARIOS}/s1/`,
				`GET ${SCENARIOS}/s1/`,
			],);
			expect(sent[1]?.body,).toMatchObject({
				id: "s1",
				params: { steps: [], },
				triggers: [TRIGGER_A, TRIGGER_B, { name: "six", type: "temporal", },],
			},);
		},);
	});

	it("trigger-add --data appends a raw trigger; --dry-run previews without a PUT", async () => {
		const store = storedScenario(settings,);
		await withScenarioCli(store.answer, async (env, sent,) => {
			const data = '{"type":"dataset_modified","active":true,"params":{"datasetName":"orders"}}';
			const preview = JSON.parse(
				(await dss(["scenario", "trigger-add", "s1", "--data", data, "--dry-run",], { env, },))
					.stdout,
			);
			expect(preview,).toMatchObject({
				dryRun: true,
				action: "trigger-add",
				index: 2,
				count: 3,
				trigger: { type: "dataset_modified", },
			},);
			expect(sent.map((request,) => request.method),).toEqual(["GET",],);
			await dss(["scenario", "trigger-add", "s1", "--data", data,], { env, },);
			expect(sent.filter((request,) => request.method === "PUT"),).toHaveLength(1,);
		},);
	});

	it("trigger-add rejects a missing schedule, a clash, and a bad value before writing", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const none = await dssFailure(["scenario", "trigger-add", "s1",], { env, },);
			expect(none.code,).toBe(1,);
			expect(none.stdout,).toContain("--frequency",);
			const both = await dssFailure(
				["scenario", "trigger-add", "s1", "--frequency", "daily", "--data", "{}",],
				{ env, },
			);
			expect(both.code,).toBe(1,);
			const hour = await dssFailure(
				["scenario", "trigger-add", "s1", "--frequency", "daily", "--hour", "25",],
				{ env, },
			);
			expect(hour.code,).toBe(1,);
			expect(sent.filter((request,) => request.method === "PUT"),).toEqual([],);
		},);
	});

	it("trigger-list shows the 0-based index; trigger-remove drops by name or by index", async () => {
		const store = storedScenario(settings,);
		await withScenarioCli(store.answer, async (env, sent,) => {
			const listed = JSON.parse(
				(await dss(["scenario", "trigger-list", "s1",], { env, },)).stdout,
			);
			expect(listed,).toMatchObject([
				{ index: 0, id: "t-a", name: "nightly", },
				{ index: 1, id: "t-b", name: "ds", },
			],);
			const byName = JSON.parse(
				(await dss(["scenario", "trigger-remove", "s1", "--name", "nightly",], { env, },)).stdout,
			);
			expect(byName,).toMatchObject({ removed: "trigger", index: 0, count: 1, },);
			const afterName = sent.findLast((request,) => request.method === "PUT");
			expect(afterName?.body,).toMatchObject({ triggers: [TRIGGER_B,], },);
			await dss(["scenario", "trigger-remove", "s1", "--index", "0",], { env, },);
			const afterIndex = sent.findLast((request,) => request.method === "PUT");
			expect(afterIndex?.body,).toMatchObject({ triggers: [], },);
		},);
	});

	it("trigger-remove --dry-run shows the target and sends no PUT; a bad selector fails", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const preview = JSON.parse(
				(await dss(
					["scenario", "trigger-remove", "s1", "--name", "ds", "--dry-run",],
					{ env, },
				)).stdout,
			);
			expect(preview,).toMatchObject({
				dryRun: true,
				action: "trigger-remove",
				index: 1,
				count: 1,
				trigger: { id: "t-b", },
			},);
			const missing = await dssFailure(["scenario", "trigger-remove", "s1",], { env, },);
			expect(missing.code,).toBe(1,);
			const outside = await dssFailure(
				["scenario", "trigger-remove", "s1", "--index", "9",],
				{ env, },
			);
			expect(outside.code,).toBe(1,);
			expect(sent.filter((request,) => request.method === "PUT"),).toEqual([],);
		},);
	});

	it("--plan describes the read-modify-write without sending anything", async () => {
		await withScenarioCli(() => undefined, async (env, sent,) => {
			const plan = JSON.parse(
				(await dss(
					["scenario", "trigger-add", "s1", "--frequency", "hourly", "--plan",],
					{ env, },
				)).stdout,
			);
			expect(plan,).toMatchObject({ method: "PUT", endpoint: `${SCENARIOS}/s1/`, exact: false, },);
			expect(sent,).toEqual([],);
		},);
	});
});

describe("scenario reporter-add / reporter-list / reporter-remove", () => {
	const settings = {
		id: "s1",
		name: "S1",
		triggers: [TRIGGER_A,],
		reporters: [MAIL_REPORTER, SLACK_REPORTER,],
	};

	it("reporter-list is compact by default and redacts credentials under --full", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env,) => {
			const compact = (await dss(["scenario", "reporter-list", "s1",], { env, },)).stdout;
			expect(JSON.parse(compact,),).toMatchObject([
				{
					index: 0,
					id: "r-mail",
					phase: "START",
					runCondition: "outcome == 'FAILED'",
					type: "mail-scenario",
					channel: "smtp",
					recipient: "a@x.com",
					subject: "S",
				},
				{ index: 1, id: "r-slack", type: "slack-channel", channel: "ops", },
			],);
			expect(compact,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			const full = (await dss(["scenario", "reporter-list", "s1", "--full",], { env, },)).stdout;
			expect(full,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			expect(JSON.parse(full,)[1].messaging.configuration,).toEqual({
				channelId: "ops",
				webhookUrl: "[redacted]",
			},);
		},);
	});

	it("reporter-add builds a mail reporter, appends it after the existing ones, and verifies", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const out = JSON.parse(
				(await dss([
					"scenario",
					"reporter-add",
					"s1",
					"--channel",
					"smtp",
					"--recipient",
					"ops@example.com",
					"--subject",
					"Failed",
					"--message",
					"See DSS",
					"--condition",
					"outcome == 'FAILED'",
					"--phase",
					"END",
				], { env, },)).stdout,
			);
			expect(out,).toMatchObject({
				added: "reporter",
				index: 2,
				count: 3,
				reporter: {
					phase: "END",
					runConditionEnabled: true,
					runCondition: "outcome == 'FAILED'",
					messaging: {
						configuration: { channelId: "smtp", recipient: "ops@example.com", subject: "Failed", },
					},
				},
				next: [["dss", "scenario", "reporter-list", "s1",],],
			},);
			expect(sent.filter((request,) => request.method === "PUT"),).toHaveLength(1,);
			const put = sent.find((request,) => request.method === "PUT");
			expect(put?.body,).toMatchObject({
				triggers: [TRIGGER_A,],
				reporters: [MAIL_REPORTER, SLACK_REPORTER, { phase: "END", },],
			},);
		},);
	});

	it("reporter-add --data sends a raw reporter but never prints its webhook secret", async () => {
		const raw = JSON.stringify({
			active: true,
			phase: "END",
			messaging: { type: "teams-channel", configuration: { channelId: "t", webhookUrl: WEBHOOK, }, },
		},);
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const added = await dss(["scenario", "reporter-add", "s1", "--data", raw,], { env, },);
			expect(added.stdout,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			expect(added.stderr,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			expect(JSON.stringify(sent.find((request,) => request.method === "PUT")?.body,),).toContain(
				"SECRET-WEBHOOK-TOKEN",
			);
			const preview = await dss(
				["scenario", "reporter-add", "s1", "--data", raw, "--dry-run",],
				{ env, },
			);
			expect(JSON.parse(preview.stdout,),).toMatchObject({ dryRun: true, index: 3, count: 4, },);
			expect(preview.stdout,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			const plan = await dss(["scenario", "reporter-add", "s1", "--data", raw, "--plan",], { env, },);
			expect(plan.stdout,).not.toContain("SECRET-WEBHOOK-TOKEN",);
		},);
	});

	it("reporter-add refuses mail flags next to --data and incomplete mail flags", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const clash = await dssFailure(
				["scenario", "reporter-add", "s1", "--data", "{}", "--channel", "smtp",],
				{ env, },
			);
			expect(clash.code,).toBe(1,);
			expect(clash.stdout,).toContain("--channel",);
			const incomplete = await dssFailure(
				["scenario", "reporter-add", "s1", "--channel", "smtp",],
				{ env, },
			);
			expect(incomplete.code,).toBe(1,);
			expect(incomplete.stdout,).toContain("--recipient is required",);
			expect(sent,).toEqual([],);
		},);
	});

	it("reporter-remove drops the selected reporter and keeps the rest; its output has no secret", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const out = await dss(["scenario", "reporter-remove", "s1", "--name", "slack",], { env, },);
			expect(JSON.parse(out.stdout,),).toMatchObject({ removed: "reporter", index: 1, count: 1, },);
			expect(out.stdout,).not.toContain("SECRET-WEBHOOK-TOKEN",);
			expect(sent.find((request,) => request.method === "PUT")?.body,).toMatchObject({
				reporters: [MAIL_REPORTER,],
				triggers: [TRIGGER_A,],
			},);
		},);
	});
});

describe("scenario run-as-set", () => {
	const settings = {
		id: "s1",
		name: "S1",
		runAsUser: "svc",
		versionTag: { lastModifiedBy: { login: "editor", }, },
	};

	it("--user writes runAsUser and reports the effective run-as", async () => {
		await withScenarioCli(
			storedScenario({ ...settings, runAsUser: undefined, },).answer,
			async (env, sent,) => {
				const out = JSON.parse(
					(await dss(["scenario", "run-as-set", "s1", "--user", "robot",], { env, },)).stdout,
				);
				expect(out,).toMatchObject({
					updated: "s1",
					part: "runAsUser",
					runAsUser: "robot",
					effectiveRunAs: "robot",
				},);
				expect(sent.find((request,) => request.method === "PUT")?.body,).toMatchObject({
					id: "s1",
					runAsUser: "robot",
				},);
			},
		);
	});

	it("--clear omits runAsUser from the PUT and falls back to the last editor", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const out = JSON.parse(
				(await dss(["scenario", "run-as-set", "s1", "--clear",], { env, },)).stdout,
			);
			expect(out,).toMatchObject({
				runAsUser: null,
				effectiveRunAs: "editor",
				previousRunAsUser: "svc",
			},);
			expect(sent.find((request,) => request.method === "PUT")?.body,).not.toHaveProperty(
				"runAsUser",
			);
		},);
	});

	it("--dry-run reads only and shows the current value", async () => {
		await withScenarioCli(storedScenario(settings,).answer, async (env, sent,) => {
			const out = JSON.parse(
				(await dss(["scenario", "run-as-set", "s1", "--user", "robot", "--dry-run",], { env, },))
					.stdout,
			);
			expect(out,).toMatchObject({
				dryRun: true,
				runAsUser: "robot",
				currentRunAsUser: "svc",
				method: "PUT",
			},);
			expect(sent.map((request,) => request.method),).toEqual(["GET",],);
		},);
	});

	it("needs exactly one of --user and --clear", async () => {
		await withScenarioCli(() => undefined, async (env, sent,) => {
			for (const extra of [[], ["--user", "robot", "--clear",],]) {
				const failure = await dssFailure(["scenario", "run-as-set", "s1", ...extra,], { env, },);
				expect(failure.code,).toBe(1,);
				expect(failure.stdout,).toContain("exactly one of --user LOGIN or --clear",);
			}
			expect(sent,).toEqual([],);
		},);
	});
});

describe("future list --scenarios-only", () => {
	it("asks DSS for scenario futures only, and plain list does not", async () => {
		await withScenarioCli(
			(request,) => request.path.startsWith("/public/api/futures/",) ? [] : undefined,
			async (env, sent,) => {
				await dss(["future", "list", "--scenarios-only",], { env, },);
				await dss(["future", "list",], { env, },);
				const queries = sent.map((request,) => new URL(request.path, "http://x",).searchParams);
				expect(queries[0]?.get("withScenarios",),).toBe("true",);
				expect(queries[0]?.get("withNotScenarios",),).toBe("false",);
				expect(queries[1]?.has("withNotScenarios",),).toBe(false,);
			},
		);
	});
});
