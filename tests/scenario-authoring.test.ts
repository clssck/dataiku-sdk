import { describe, expect, it, } from "bun:test";
import { DataikuClient, } from "../src/client.js";
import {
	mailReporter,
	selectScenarioListIndex,
	temporalTrigger,
} from "../src/resources/scenario-authoring.js";
import { isRecord, } from "../src/utils/records.js";

type Recorded = { method: string; path: string; body: unknown; };

/** Serves `routes` by `METHOD path?query`, recording every request; an unrouted request fails the test via 404. */
async function withScenarioServer(
	routes: Record<string, (body: unknown,) => unknown>,
	run: (client: DataikuClient, requests: Recorded[],) => Promise<void>,
): Promise<void> {
	const requests: Recorded[] = [];
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch: async (request,) => {
			const url = new URL(request.url,);
			const text = await request.text();
			const body: unknown = text ? JSON.parse(text,) : undefined;
			requests.push({ method: request.method, path: `${url.pathname}${url.search}`, body, },);
			const route = routes[`${request.method} ${url.pathname}${url.search}`];
			return route ? Response.json(route(body,),) : new Response("unrouted", { status: 404, },);
		},
	},);
	try {
		await run(
			new DataikuClient({ url: server.url.href, apiKey: "test", projectKey: "P", },),
			requests,
		);
	} finally {
		server.stop(true,);
	}
}

const SCENARIO = "/public/api/projects/P/scenarios/s1/";
const SCENARIO_BASE = "/public/api/projects/P/scenarios/s1";

describe("temporalTrigger", () => {
	const now = new Date(2026, 9, 13, 15, 30,);

	it("builds the dataikuapi shapes for each frequency", () => {
		expect(temporalTrigger({ frequency: "minutely", every: 15, },),).toEqual({
			active: true,
			type: "temporal",
			params: { frequency: "Minutely", repeatFrequency: 15, },
		},);
		expect(temporalTrigger({ frequency: "minutely", },).params,).toEqual({
			frequency: "Minutely",
			repeatFrequency: 5,
		},);
		const hourly = temporalTrigger({ frequency: "hourly", minute: 15, every: 2, }, now,);
		expect(hourly,).toMatchObject({
			active: true,
			type: "temporal",
			params: {
				frequency: "Hourly",
				hour: 0,
				minute: 15,
				repeatFrequency: 2,
				timezone: "SERVER",
			},
		},);
		expect(Object.keys(hourly.params as object,).sort(),).toEqual([
			"frequency",
			"hour",
			"minute",
			"repeatFrequency",
			"startingFrom",
			"timezone",
		],);
		expect(temporalTrigger({ frequency: "daily", }, now,).params,).toMatchObject({
			frequency: "Daily",
			hour: 2,
			minute: 0,
			repeatFrequency: 1,
			timezone: "SERVER",
		},);
		expect(
			temporalTrigger({ frequency: "weekly", days: ["Monday", "Friday",], hour: 8, }, now,).params,
		).toMatchObject({ frequency: "Weekly", hour: 8, daysOfWeek: ["Monday", "Friday",], },);
		expect(
			temporalTrigger({ frequency: "monthly", runOn: "LAST_WEEK", timezone: "Europe/Paris", }, now,)
				.params,
		).toMatchObject({
			frequency: "Monthly",
			monthlyRunOn: "LAST_WEEK",
			hour: 2,
			minute: 0,
			repeatFrequency: 1,
			timezone: "Europe/Paris",
		},);
	});

	it("starts at local midnight with milliseconds and a +HHMM offset, like dataikuapi", () => {
		expect(temporalTrigger({ frequency: "daily", }, now,).params?.["startingFrom"],).toMatch(
			/^2026-10-13T00:00:00\.000[+-]\d{4}$/,
		);
		expect(
			temporalTrigger({ frequency: "daily", startDate: "2026-11-02", }, now,).params?.["startingFrom"],
		)
			.toMatch(/^2026-11-02T00:00:00\.000[+-]\d{4}$/,);
		// Monthly schedules start on January 1st (or the chosen day), not today.
		expect(temporalTrigger({ frequency: "monthly", }, now,).params?.["startingFrom"],).toMatch(
			/^2026-01-01T00:00:00\.000/,
		);
		expect(temporalTrigger({ frequency: "monthly", dayOfMonth: 5, }, now,).params?.["startingFrom"],)
			.toMatch(
				/^2026-01-05T00:00:00\.000/,
			);
	});

	it("names the trigger and honours active=false", () => {
		expect(temporalTrigger({ frequency: "minutely", name: "poll", active: false, },),).toEqual({
			name: "poll",
			active: false,
			type: "temporal",
			params: { frequency: "Minutely", repeatFrequency: 5, },
		},);
	});

	it("rejects options that do not apply to the frequency", () => {
		expect(() => temporalTrigger({ frequency: "weekly", },)).toThrow("--days is required",);
		expect(() => temporalTrigger({ frequency: "weekly", days: ["Funday",], },)).toThrow("Funday",);
		expect(() => temporalTrigger({ frequency: "daily", days: ["Monday",], },)).toThrow(
			"weekly triggers only",
		);
		expect(() => temporalTrigger({ frequency: "daily", runOn: "LAST_WEEK", },)).toThrow(
			"monthly triggers only",
		);
		expect(() => temporalTrigger({ frequency: "minutely", hour: 3, },)).toThrow(
			"does not apply to minutely",
		);
		expect(() => temporalTrigger({ frequency: "monthly", runOn: "SOMEDAY", },)).toThrow("SOMEDAY",);
		expect(() => temporalTrigger({ frequency: "monthly", startDate: "2026-02-01", dayOfMonth: 3, },))
			.toThrow("mutually exclusive",);
		expect(() => temporalTrigger({ frequency: "daily", hour: 24, },)).toThrow("--hour",);
		expect(() => temporalTrigger({ frequency: "daily", minute: 60, },)).toThrow("--minute",);
		expect(() => temporalTrigger({ frequency: "daily", every: 0, },)).toThrow("--every",);
		expect(() => temporalTrigger({ frequency: "daily", startDate: "2026-02-30", },)).toThrow(
			"not a calendar date",
		);
		expect(() => temporalTrigger({ frequency: "daily", startDate: "soon", },)).toThrow(
			"YYYY-MM-DD",
		);
	});
});

describe("mailReporter", () => {
	it("builds the mail-scenario reporter DSS stores", () => {
		expect(
			mailReporter({ channel: "smtp", recipient: "a@x.com,b@x.com", subject: "S", message: "M", },),
		).toEqual({
			active: true,
			phase: "END",
			runConditionEnabled: false,
			runCondition: "true",
			messaging: {
				type: "mail-scenario",
				configuration: {
					channelId: "smtp",
					recipient: "a@x.com,b@x.com",
					subject: "S",
					message: "M",
					messageSource: "INLINE",
					templateFormat: "FREEMARKER",
					sendAsHTML: false,
					attachments: [],
				},
			},
		},);
	});

	it("enables the run condition only when one is given", () => {
		expect(
			mailReporter({
				channel: "smtp",
				recipient: "a@x.com",
				subject: "S",
				message: "M",
				condition: "outcome == 'FAILED'",
				phase: "START",
				name: "on failure",
				active: false,
			},),
		).toMatchObject({
			name: "on failure",
			active: false,
			phase: "START",
			runConditionEnabled: true,
			runCondition: "outcome == 'FAILED'",
		},);
	});

	it("rejects an unknown phase and empty required text", () => {
		const base = { channel: "smtp", recipient: "a@x.com", subject: "S", message: "M", };
		expect(() => mailReporter({ ...base, phase: "MIDDLE", },)).toThrow("START or END",);
		expect(() => mailReporter({ ...base, recipient: " ", },)).toThrow("--recipient",);
		expect(() => mailReporter({ ...base, channel: "", },)).toThrow("--channel",);
		expect(() => mailReporter({ ...base, condition: "", },)).toThrow("--condition",);
	});
});

describe("selectScenarioListIndex", () => {
	const items = [{ name: "a", }, { name: "b", }, { name: "a", }, "not an object",];

	it("selects by 0-based index or by unique name", () => {
		expect(selectScenarioListIndex(items, { index: 1, }, "trigger",),).toBe(1,);
		expect(selectScenarioListIndex(items, { name: "b", }, "trigger",),).toBe(1,);
	});

	it("refuses missing, out-of-range, unknown, and ambiguous selections", () => {
		expect(() => selectScenarioListIndex(items, {}, "trigger",)).toThrow("exactly one",);
		expect(() => selectScenarioListIndex(items, { index: 0, name: "a", }, "trigger",)).toThrow(
			"exactly one",
		);
		expect(() => selectScenarioListIndex(items, { index: 4, }, "reporter",)).toThrow(
			"outside the 4 reporter(s)",
		);
		expect(() => selectScenarioListIndex(items, { index: -1, }, "trigger",)).toThrow("outside",);
		expect(() => selectScenarioListIndex(items, { name: "zzz", }, "trigger",)).toThrow(
			"No trigger is named",
		);
		expect(() => selectScenarioListIndex(items, { name: "a", }, "trigger",)).toThrow(
			"indexes 0, 2",
		);
	});
});

describe("ScenariosResource.run", () => {
	it("sends params as the POST body and returns the trigger fire, not a scenario run", async () => {
		await withScenarioServer({
			[`POST ${SCENARIO_BASE}/run/`]: () => ({
				runId: "2026-10-13-10-00-00-001",
				trigger: { id: "manual", },
				cancelled: false,
			}),
		}, async (client, requests,) => {
			const fire = await client.scenarios.run("s1", { params: { region: "EU", n: 3, }, },);
			expect(fire,).toEqual({
				triggerId: "manual",
				triggerRunId: "2026-10-13-10-00-00-001",
				cancelled: false,
			},);
			expect(requests,).toEqual([
				{
					method: "POST",
					path: `${SCENARIO_BASE}/run/`,
					body: { region: "EU", n: 3, },
				},
			],);
		},);
	});

	it("sends an empty object without params and reports a cancelled fire at once", async () => {
		await withScenarioServer({
			[`POST ${SCENARIO_BASE}/run/`]: () => ({
				runId: "t-run",
				trigger: { id: "api", },
				cancelled: true,
			}),
		}, async (client, requests,) => {
			expect(await client.scenarios.run("s1",),).toEqual({
				triggerId: "api",
				triggerRunId: "t-run",
				cancelled: true,
			},);
			expect(requests[0]?.body,).toEqual({},);
		},);
	});
});

describe("ScenariosResource.runAndWait cancellation", () => {
	it("reports CANCELLED without polling when the POST already says the fire was cancelled", async () => {
		await withScenarioServer({
			[`POST ${SCENARIO_BASE}/run/`]: () => ({
				runId: "t-run",
				trigger: { id: "manual", },
				cancelled: true,
			}),
		}, async (client, requests,) => {
			const result = await client.scenarios.runAndWait("s1", { params: { a: 1, }, },);
			expect(result,).toMatchObject({
				scenarioId: "s1",
				outcome: "CANCELLED",
				success: false,
				cancelled: true,
				triggerRunId: "t-run",
				pollCount: 0,
			},);
			expect(result.timedOut,).toBeUndefined();
			expect(requests.map((request,) => request.method),).toEqual(["POST",],);
			expect(requests[0]?.body,).toEqual({ a: 1, },);
		},);
	});

	it("reports CANCELLED, not TIMEOUT, when DSS cancels a fire that never gets a run", async () => {
		const poll = "get-run-for-trigger?triggerId=manual&triggerRunId=t-run";
		await withScenarioServer({
			[`POST ${SCENARIO_BASE}/run/`]: () => ({
				runId: "t-run",
				trigger: { id: "manual", },
				cancelled: false,
			}),
			[`GET ${SCENARIO_BASE}/${poll}`]: () => ({}),
			"GET /public/api/projects/P/scenarios/trigger/s1/manual?triggerRunId=t-run": () => ({
				cancelled: true,
			}),
		}, async (client, requests,) => {
			const result = await client.scenarios.runAndWait("s1", { timeoutMs: 5_000, },);
			expect(result,).toMatchObject({
				outcome: "CANCELLED",
				success: false,
				cancelled: true,
				triggerRunId: "t-run",
				pollCount: 1,
			},);
			expect(result.timedOut,).toBeUndefined();
			expect(requests.map((request,) => `${request.method} ${request.path}`),).toEqual([
				`POST ${SCENARIO_BASE}/run/`,
				`GET ${SCENARIO_BASE}/${poll}`,
				"GET /public/api/projects/P/scenarios/trigger/s1/manual?triggerRunId=t-run",
			],);
		},);
	});

	it("keeps waiting while the fire is merely pending, then returns the run outcome", async () => {
		const poll = `GET ${SCENARIO_BASE}/get-run-for-trigger?triggerId=manual&triggerRunId=t-run`;
		let polls = 0;
		await withScenarioServer({
			[`POST ${SCENARIO_BASE}/run/`]: () => ({
				runId: "t-run",
				trigger: { id: "manual", },
				cancelled: false,
			}),
			[poll]: () => {
				polls += 1;
				return polls === 1
					? {}
					: { scenarioRun: { runId: "real-run", result: { outcome: "SUCCESS", }, }, };
			},
			"GET /public/api/projects/P/scenarios/trigger/s1/manual?triggerRunId=t-run": () => ({
				cancelled: false,
			}),
			[`GET ${SCENARIO_BASE}/real-run/`]: () => ({ stepRuns: [], }),
		}, async (client,) => {
			// pollIntervalMs: 1 keeps the one real poll sleep negligible; there is no fake-timer seam.
			const result = await client.scenarios.runAndWait("s1", { pollIntervalMs: 1, },);
			expect(result,).toMatchObject({
				runId: "real-run",
				outcome: "SUCCESS",
				success: true,
				triggerRunId: "t-run",
				pollCount: 2,
			},);
			expect(result.cancelled,).toBeUndefined();
		},);
	});
});

describe("ScenariosResource.getRunForTrigger", () => {
	const poll = `GET ${SCENARIO_BASE}/get-run-for-trigger?triggerId=manual&triggerRunId=t-run`;
	const fire = "GET /public/api/projects/P/scenarios/trigger/s1/manual?triggerRunId=t-run";

	it("resolves a finished run to its scenario run id and outcome", async () => {
		await withScenarioServer({
			[poll]: () => ({
				scenarioRun: { runId: "real-run", start: 5, result: { outcome: "FAILED", }, },
			}),
		}, async (client, requests,) => {
			expect(await client.scenarios.getRunForTrigger("s1", "t-run",),).toEqual({
				triggerId: "manual",
				triggerRunId: "t-run",
				cancelled: false,
				state: "FINISHED",
				runId: "real-run",
				outcome: "FAILED",
				start: 5,
			},);
			expect(requests,).toHaveLength(1,);
		},);
	});

	it("reports RUNNING while the run has no outcome yet", async () => {
		await withScenarioServer({
			[poll]: () => ({ scenarioRun: { runId: "real-run", end: 0, }, }),
		}, async (client,) => {
			expect(await client.scenarios.getRunForTrigger("s1", "t-run",),).toMatchObject({
				state: "RUNNING",
				runId: "real-run",
			},);
		},);
	});

	it("tells a pending fire from a cancelled one by reading the trigger fire", async () => {
		await withScenarioServer(
			{ [poll]: () => ({}), [fire]: () => ({ cancelled: false, }), },
			async (client,) => {
				expect(await client.scenarios.getRunForTrigger("s1", "t-run",),).toEqual({
					triggerId: "manual",
					triggerRunId: "t-run",
					cancelled: false,
					state: "PENDING",
				},);
			},
		);
		await withScenarioServer(
			{ [poll]: () => ({}), [fire]: () => ({ cancelled: true, }), },
			async (client,) => {
				expect(await client.scenarios.getRunForTrigger("s1", "t-run",),).toMatchObject({
					cancelled: true,
					state: "CANCELLED",
				},);
			},
		);
	});

	it("passes a non-manual trigger id through both reads", async () => {
		await withScenarioServer({
			[`GET ${SCENARIO_BASE}/get-run-for-trigger?triggerId=daily%2F1&triggerRunId=t-run`]: () => ({}),
			"GET /public/api/projects/P/scenarios/trigger/s1/daily%2F1?triggerRunId=t-run": () => ({
				cancelled: false,
			}),
		}, async (client,) => {
			expect(
				await client.scenarios.getRunForTrigger("s1", "t-run", { triggerId: "daily/1", },),
			).toMatchObject({ triggerId: "daily/1", state: "PENDING", },);
		},);
	});
});

describe("run history reads", () => {
	it("getLastRuns adds onlyFinishedRuns only when asked", async () => {
		await withScenarioServer({
			[`GET ${SCENARIO_BASE}/get-last-runs/?limit=3&onlyFinishedRuns=true`]: () => [],
			[`GET ${SCENARIO_BASE}/get-last-runs/?limit=3`]: () => [],
			[`GET ${SCENARIO_BASE}/get-last-runs/`]: () => [],
		}, async (client, requests,) => {
			await client.scenarios.getLastRuns("s1", { limit: 3, onlyFinished: true, },);
			await client.scenarios.getLastRuns("s1", { limit: 3, onlyFinished: false, },);
			await client.scenarios.getLastRuns("s1",);
			expect(requests.map((request,) => request.path),).toEqual([
				`${SCENARIO_BASE}/get-last-runs/?limit=3&onlyFinishedRuns=true`,
				`${SCENARIO_BASE}/get-last-runs/?limit=3`,
				`${SCENARIO_BASE}/get-last-runs/`,
			],);
		},);
	});

	it("getRunsByDate sends YYYY-MM-DD days and maps the runs", async () => {
		await withScenarioServer({
			[`GET ${SCENARIO_BASE}/get-runs-by-date?fromDate=2026-10-01&toDate=2026-10-31`]: () => [
				{ runId: "r1", start: 1, end: 2, result: { outcome: "SUCCESS", }, },
			],
		}, async (client,) => {
			expect(
				await client.scenarios.getRunsByDate("s1", {
					from: "2026-10-01T08:30:00Z",
					to: "2026-10-31",
				},),
			).toEqual([{ runId: "r1", start: 1, end: 2, result: { outcome: "SUCCESS", }, },],);
		},);
	});

	it("getRunsByDate defaults the exclusive end day to tomorrow so today's runs are included", async () => {
		const tomorrow = new Date(Date.now() + 86_400_000,).toLocaleDateString("en-CA",);
		await withScenarioServer({
			[`GET ${SCENARIO_BASE}/get-runs-by-date?fromDate=2026-10-01&toDate=${tomorrow}`]: () => [],
		}, async (client, requests,) => {
			await client.scenarios.getRunsByDate("s1", { from: "2026-10-01", },);
			expect(requests,).toHaveLength(1,);
		},);
	});

	it("getRunsByDate rejects bad dates before any request", async () => {
		await withScenarioServer({}, async (client, requests,) => {
			await expect(client.scenarios.getRunsByDate("s1", { from: "yesterday", },),).rejects.toThrow(
				"from must be a YYYY-MM-DD date",
			);
			await expect(
				client.scenarios.getRunsByDate("s1", { from: "2026-10-01", to: "2026-13-01", },),
			).rejects.toThrow("to must be",);
			expect(requests,).toEqual([],);
		},);
	});
});

describe("ScenariosResource.getRunReport", () => {
	it("reads the run report and the step report from their own routes", async () => {
		const run = `${SCENARIO_BASE}/r1`;
		await withScenarioServer({
			[`GET ${run}/scenario-run-report`]: () => "<testsuites/>",
			[`GET ${run}/step-run-report?stepId=step%201`]: () => "<testsuite name='step'/>",
		}, async (client, requests,) => {
			expect(await client.scenarios.getRunReport("s1", "r1",),).toEqual({
				text: '"<testsuites/>"',
				truncated: false,
				maxBytes: 1_048_576,
			},);
			expect(
				await client.scenarios.getRunReport("s1", "r1", { stepId: "step 1", maxBytes: 8, },),
			).toEqual({ text: '"<testsu', truncated: true, maxBytes: 8, },);
			expect(requests.map((request,) => request.path),).toEqual([
				`${run}/scenario-run-report`,
				`${run}/step-run-report?stepId=step%201`,
			],);
		},);
	});
});

/** Narrows parsed JSON to an object, failing the test on any other shape. */
function asObject(value: unknown,): Record<string, unknown> {
	if (!isRecord(value,)) throw new Error(`expected a JSON object, got ${JSON.stringify(value,)}`,);
	return value;
}

/** The array under `field` of the scenario a recorded PUT sent. */
function sentList(request: Recorded | undefined, field: string,): unknown[] {
	const list = asObject(request?.body,)[field];
	if (!Array.isArray(list,)) throw new Error(`PUT body has no ${field} array`,);
	return list;
}

/** A scenario object as DSS returns it: ids assigned to new triggers/reporters, `lastUpdate` stamped on every save. */
function dssScenarioStore(initial: Record<string, unknown>,) {
	let stored = initial;
	let stamp = 100;
	return {
		get: () => stored,
		put: (body: unknown,) => {
			const next = asObject(body,);
			stamp += 1;
			stored = {
				...next,
				...(Array.isArray(next.triggers,)
					? {
						triggers: next.triggers.map((trigger, index,) => {
							const fields = asObject(trigger,);
							return {
								id: `t${index}`,
								...fields,
								params: { ...asObject(fields.params,), lastUpdate: stamp, },
							};
						},),
					}
					: {}),
			};
			return { msg: "ok", };
		},
	};
}

describe("trigger and reporter list edits", () => {
	const daily = temporalTrigger({
		frequency: "daily",
		hour: 6,
		startDate: "2026-11-02",
		name: "six",
	},);
	const minutely = {
		id: "t0",
		type: "temporal",
		params: { frequency: "Minutely", repeatFrequency: 5, },
	};
	const datasetTrigger = {
		id: "t1",
		type: "dataset_modified",
		name: "ds",
		params: { datasetName: "orders", },
	};
	const base = {
		id: "s1",
		name: "S1",
		type: "step_based",
		params: { steps: [{ id: "keep-me", },], },
		reporters: [{ id: "r0", name: "mail", },],
		triggers: [{ ...minutely, params: { ...minutely.params, lastUpdate: 7, }, }, datasetTrigger,],
	};

	it("addItem appends, keeps every other field, drops the server stamp, and verifies", async () => {
		const store = dssScenarioStore(base,);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => store.get(),
			[`PUT ${SCENARIO}`]: (body,) => store.put(body,),
		}, async (client, requests,) => {
			const added = await client.scenarios.addItem("trigger", "s1", daily,);
			expect(added,).toMatchObject({
				kind: "trigger",
				index: 2,
				count: 3,
				item: { name: "six", params: { frequency: "Daily", hour: 6, }, },
			},);
			expect(requests.map((request,) => request.method),).toEqual(["GET", "PUT", "GET",],);
			expect(sentList(requests[1], "triggers",),).toEqual([minutely, datasetTrigger, daily,],);
			expect(sentList(requests[1], "reporters",),).toEqual(base.reporters,);
			expect(asObject(requests[1]?.body,).params,).toEqual(base.params,);
		},);
	});

	it("addItem on reporters leaves triggers untouched", async () => {
		const store = dssScenarioStore(base,);
		const reporter = mailReporter({
			channel: "smtp",
			recipient: "a@x.com",
			subject: "S",
			message: "M",
		},);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => store.get(),
			[`PUT ${SCENARIO}`]: (body,) => store.put(body,),
		}, async (client, requests,) => {
			const added = await client.scenarios.addItem("reporter", "s1", reporter,);
			expect(added,).toMatchObject({ kind: "reporter", index: 1, count: 2, },);
			expect(sentList(requests[1], "reporters",),).toEqual([base.reporters[0], reporter,],);
			expect(sentList(requests[1], "triggers",),).toEqual(base.triggers,);
		},);
	});

	it("removeItem drops only the selected element, by name or index", async () => {
		const byName = dssScenarioStore(base,);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => byName.get(),
			[`PUT ${SCENARIO}`]: (body,) => byName.put(body,),
		}, async (client, requests,) => {
			const removed = await client.scenarios.removeItem("trigger", "s1", { name: "ds", },);
			expect(removed,).toMatchObject({ kind: "trigger", index: 1, count: 1, item: datasetTrigger, },);
			expect(sentList(requests[1], "triggers",),).toEqual([minutely,],);
		},);
		const byIndex = dssScenarioStore(base,);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => byIndex.get(),
			[`PUT ${SCENARIO}`]: (body,) => byIndex.put(body,),
		}, async (client, requests,) => {
			await client.scenarios.removeItem("reporter", "s1", { index: 0, },);
			expect(sentList(requests[1], "reporters",),).toEqual([],);
		},);
	});

	it("removeItem sends nothing when the selection is invalid", async () => {
		await withScenarioServer({ [`GET ${SCENARIO}`]: () => base, }, async (client, requests,) => {
			await expect(client.scenarios.removeItem("trigger", "s1", { index: 5, },),).rejects.toThrow(
				"outside",
			);
			expect(requests.map((request,) => request.method),).toEqual(["GET",],);
		},);
	});

	it("reports a trigger list DSS did not store instead of claiming success", async () => {
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => base,
			[`PUT ${SCENARIO}`]: () => ({ msg: "ok", }),
		}, async (client,) => {
			await expect(client.scenarios.addItem("trigger", "s1", daily,),).rejects.toMatchObject({
				details: { mismatches: [{ path: "triggers", },], },
			},);
		},);
	});

	it("listItems returns an empty list for a scenario without the field", async () => {
		await withScenarioServer({ [`GET ${SCENARIO}`]: () => ({ id: "s1", }), }, async (client,) => {
			expect(await client.scenarios.listItems("reporter", "s1",),).toEqual([],);
		},);
	});
});

describe("ScenariosResource.setRunAs", () => {
	const editor = { versionTag: { lastModifiedBy: { login: "editor", }, }, };

	it("sets runAsUser and reports the effective run-as", async () => {
		const store = dssScenarioStore({ id: "s1", ...editor, },);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => store.get(),
			[`PUT ${SCENARIO}`]: (body,) => store.put(body,),
		}, async (client, requests,) => {
			expect(await client.scenarios.setRunAs("s1", "svc",),).toEqual({
				scenarioId: "s1",
				runAsUser: "svc",
				effectiveRunAs: "svc",
			},);
			expect(requests.map((request,) => request.method),).toEqual(["GET", "PUT", "GET",],);
			expect(requests[1]?.body,).toMatchObject({ id: "s1", runAsUser: "svc", },);
		},);
	});

	it("clears runAsUser by omitting the key, and falls back to the last editor", async () => {
		const store = dssScenarioStore({ id: "s1", runAsUser: "svc", ...editor, },);
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => store.get(),
			[`PUT ${SCENARIO}`]: (body,) => store.put(body,),
		}, async (client, requests,) => {
			expect(await client.scenarios.setRunAs("s1", null,),).toEqual({
				scenarioId: "s1",
				runAsUser: null,
				effectiveRunAs: "editor",
				previousRunAsUser: "svc",
			},);
			expect(requests[1]?.body,).not.toHaveProperty("runAsUser",);
		},);
	});

	it("fails verification without echoing the requested login", async () => {
		await withScenarioServer({
			[`GET ${SCENARIO}`]: () => ({ id: "s1", }),
			[`PUT ${SCENARIO}`]: () => ({ msg: "ok", }),
		}, async (client,) => {
			const error = await client.scenarios.setRunAs("s1", "secret-login",).catch((e: unknown,) => e);
			expect(error,).toMatchObject({ details: { mismatches: [{ path: "runAsUser", },], }, },);
			expect(JSON.stringify(error,),).not.toContain("secret-login",);
			expect(String(error,),).not.toContain("secret-login",);
		},);
	});
});

describe("FuturesResource.list", () => {
	it("asks for running scenarios only with withNotScenarios=false", async () => {
		await withScenarioServer({
			"GET /public/api/futures/?allUsers=false&withScenarios=true&withNotScenarios=false": () => [],
			"GET /public/api/futures/?allUsers=true&withScenarios=false": () => [],
		}, async (client, requests,) => {
			await client.futures.list({ scenariosOnly: true, },);
			await client.futures.list({ allUsers: true, },);
			expect(requests.map((request,) => request.path),).toEqual([
				"/public/api/futures/?allUsers=false&withScenarios=true&withNotScenarios=false",
				"/public/api/futures/?allUsers=true&withScenarios=false",
			],);
		},);
	});
});
