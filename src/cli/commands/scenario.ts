import { resolve, } from "node:path";
import type { DataikuClient, } from "../../client.js";
import {
	mailReporter,
	SCENARIO_TRIGGER_FREQUENCIES,
	selectScenarioListIndex,
	temporalTrigger,
} from "../../resources/scenario-authoring.js";
import type {
	ScenarioListKind,
	ScenarioTriggerFire,
	ScenarioTriggerRunState,
} from "../../resources/scenarios.js";
import type { ScenarioWaitResult, } from "../../schemas.js";
import {
	messagingChannelSecretsFromBody,
	sanitizeMessagingChannelError,
	sanitizeMessagingChannelSecrets,
} from "../../utils/messaging-channel-secrets.js";
import { asRecord, } from "../../utils/records.js";
import {
	json,
	jsonInput,
	num,
	parseBooleanOption,
	parseJsonObject,
	readInputText,
	splitCsvFlag,
} from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, enqueueCliWarning, readIfExists, skipResult, } from "../output.js";
import {
	type FailedActivity,
	scenarioRunDiagnosis,
	shapeCommandResult,
} from "../result-shapes.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/**
 * The recipe a failed job was running and the datasets it read, from one
 * `job get` (its FAILED activity). Undefined when the job cannot be read or
 * names no failed recipe: the diagnosis then stays job-level.
 */
async function failedActivity(
	c: DataikuClient,
	jobId: unknown,
	projectKey: string | undefined,
): Promise<FailedActivity | undefined> {
	if (typeof jobId !== "string") return undefined;
	try {
		const job = asRecord(await c.jobs.get(jobId, projectKey,),);
		const activities = asRecord(asRecord(job?.["baseStatus"],)?.["activities"],) ?? {};
		for (const value of Object.values(activities,)) {
			const activity = asRecord(value,);
			if (activity?.["state"] !== "FAILED" || typeof activity["recipeName"] !== "string") continue;
			const sources = Array.isArray(activity["sources"],) ? activity["sources"] : [];
			const inputs = sources.flatMap((source,) => {
				const id = asRecord(source,)?.["id"];
				if (typeof id !== "string") return [];
				// Same-project inputs are named bare; foreign ones keep PROJECT.name.
				const dot = id.indexOf(".",);
				return [dot > 0 && id.slice(0, dot,) === projectKey ? id.slice(dot + 1,) : id,];
			},);
			return { recipe: activity["recipeName"], inputs, };
		}
	} catch {
		// A diagnosis aid only: the run report itself is already in hand.
	}
	return undefined;
}

/** Diagnosis of a raw run report, located at the failed recipe when its job names one. */
async function locatedDiagnosis(
	c: DataikuClient,
	details: unknown,
	projectKey: string | undefined,
) {
	const plain = scenarioRunDiagnosis(details,);
	const runProject = asRecord(asRecord(asRecord(details,)?.["scenarioRun"],)?.["trigger"],)
		?.["projectKey"];
	const pk = projectKey ?? (typeof runProject === "string" ? runProject : undefined);
	const activity = await failedActivity(c, plain.failure?.["jobId"], pk,);
	return activity ? scenarioRunDiagnosis(details, activity,) : plain;
}

/**
 * Run a scenario and wait; a failed (not timed-out) run gains the first
 * failure, the recipe that failed, and the next commands. Extra reads happen
 * only on failure; a read error leaves the wait result untouched. A cancelled
 * trigger fire has no run to diagnose: it says why instead.
 */
async function runAndWaitWithFailure(
	c: DataikuClient,
	scenarioId: string,
	options: {
		params?: Record<string, unknown>;
		pollIntervalMs?: number;
		timeoutMs?: number;
		projectKey?: string;
	},
) {
	const result: ScenarioWaitResult = await c.scenarios.runAndWait(scenarioId, options,);
	if (result.cancelled === true) {
		return {
			...result,
			failure: {
				message:
					`DSS cancelled this run before it started (trigger run ${result.triggerRunId}): the scenario was already running, or another trigger fire replaced this one. No scenario run exists; wait for or abort the current run (scenario status, scenario abort), then run again.`,
			},
			next: [["dss", "scenario", "status", scenarioId, ...projectKeyArgs(options.projectKey,),],],
		};
	}
	if (result.success || result.timedOut === true || !result.runId) return result;
	try {
		const details = await c.scenarios.getRunDetails(scenarioId, result.runId, {
			projectKey: options.projectKey,
		},);
		return { ...result, ...await locatedDiagnosis(c, details, options.projectKey,), };
	} catch {
		return result;
	}
}

function projectKeyArgs(projectKey: string | undefined,): string[] {
	return projectKey === undefined ? [] : ["--project-key", projectKey,];
}

/** `scenario run` result: the trigger fire, with the explicit pointer to the scenario run it will start. */
function triggerRunResult(
	fire: ScenarioTriggerFire,
	scenarioId: string,
	projectKey: string | undefined,
) {
	if (fire.cancelled) {
		return {
			...fire,
			hint:
				"DSS cancelled this trigger fire: the scenario was already running (or another fire replaced it), so no scenario run exists for this triggerRunId.",
			next: [["dss", "scenario", "status", scenarioId, ...projectKeyArgs(projectKey,),],],
		};
	}
	return {
		...fire,
		hint:
			"triggerRunId identifies this trigger firing, not a scenario run: get-run and log need the scenario run id that get-run-for-trigger returns (or use --wait).",
		next: [[
			"dss",
			"scenario",
			"get-run-for-trigger",
			scenarioId,
			fire.triggerRunId,
			...(fire.triggerId === "manual" ? [] : ["--trigger-id", fire.triggerId,]),
			...projectKeyArgs(projectKey,),
		],],
	};
}

/** `scenario get-run-for-trigger` result: the fire's state and the commands that follow from it. */
function triggerRunStateResult(
	state: ScenarioTriggerRunState,
	scenarioId: string,
	projectKey: string | undefined,
) {
	const pk = projectKeyArgs(projectKey,);
	const next = state.runId !== undefined
		? [
			["dss", "scenario", "get-run", scenarioId, state.runId, ...pk,],
			["dss", "scenario", "log", scenarioId, state.runId, ...pk,],
		]
		: state.state === "PENDING"
		? [[
			"dss",
			"scenario",
			"get-run-for-trigger",
			scenarioId,
			state.triggerRunId,
			...(state.triggerId === "manual" ? [] : ["--trigger-id", state.triggerId,]),
			...pk,
		],]
		: [["dss", "scenario", "status", scenarioId, ...pk,],];
	return {
		...state,
		...(state.cancelled
			? {
				hint:
					"DSS cancelled this trigger fire (the scenario was already running, or another fire replaced it); no scenario run will exist.",
			}
			: {}),
		next,
	};
}

/** Run parameters from `--params JSON` or `--params-file PATH` (the POST /run body). */
export function scenarioRunParams(
	flags: Record<string, string | boolean>,
): Record<string, unknown> | undefined {
	const file = flags["params-file"];
	if (flags["params"] !== undefined && file !== undefined) {
		throw new UsageError("Pass --params or --params-file, not both.",);
	}
	return typeof file === "string"
		? parseJsonObject(readInputText(file, "--params-file",), "--params-file",)
		: json(flags["params"], "--params",);
}

/** The trigger `trigger-add` appends: a raw `--data` object, or a schedule built from the --frequency flags. */
export function scenarioTriggerFromFlags(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const usage = commandUsage("scenario", "trigger-add",);
	const raw = jsonInput(flags,);
	if (raw !== undefined) {
		if (flags["frequency"] !== undefined) {
			throw new UsageError(
				`--frequency builds a schedule; pass it or --data/--data-file/--stdin, not both. Usage: ${usage}`,
			);
		}
		return raw;
	}
	const frequency = SCENARIO_TRIGGER_FREQUENCIES.find((candidate,) =>
		candidate === flags["frequency"]
	);
	if (frequency === undefined) {
		throw new UsageError(
			`--frequency ${
				SCENARIO_TRIGGER_FREQUENCIES.join("|",)
			} (or a raw trigger via --data) is required. Usage: ${usage}`,
		);
	}
	const text = (name: string,) => typeof flags[name] === "string" ? flags[name] : undefined;
	return temporalTrigger({
		frequency,
		every: num(flags["every"], "--every",),
		hour: num(flags["hour"], "--hour",),
		minute: num(flags["minute"], "--minute",),
		days: flags["days"] === undefined ? undefined : splitCsvFlag(flags["days"],),
		dayOfMonth: num(flags["day-of-month"], "--day-of-month",),
		runOn: text("run-on",),
		startDate: text("start-date",),
		timezone: text("timezone",),
		name: text("name",),
		active: parseBooleanOption(flags["active"], "--active",),
	},);
}

/** The reporter `reporter-add` appends: a raw `--data` object, or a mail reporter built from the --channel flags. */
export function scenarioReporterFromFlags(
	flags: Record<string, string | boolean>,
): Record<string, unknown> {
	const usage = commandUsage("scenario", "reporter-add",);
	const raw = jsonInput(flags,);
	const mailFlags = ["channel", "recipient", "subject", "message", "condition", "phase",];
	if (raw !== undefined) {
		const clash = mailFlags.find((name,) => flags[name] !== undefined);
		if (clash !== undefined) {
			throw new UsageError(
				`--${clash} builds a mail reporter; pass it or --data/--data-file/--stdin, not both. Usage: ${usage}`,
			);
		}
		return raw;
	}
	const text = (name: string,) => typeof flags[name] === "string" ? flags[name] : undefined;
	const [channel, recipient, subject, message,] = ["channel", "recipient", "subject", "message",]
		.map((name,) => {
			const value = text(name,);
			if (value === undefined) {
				throw new UsageError(
					`--${name} is required (or pass a raw reporter via --data). Usage: ${usage}`,
				);
			}
			return value;
		},);
	return mailReporter({
		channel,
		recipient,
		subject,
		message,
		condition: text("condition",),
		phase: text("phase",),
		name: text("name",),
		active: parseBooleanOption(flags["active"], "--active",),
	},);
}

/** Reporter reduced to what a list needs: identity, phase, condition, channel, recipient, subject. */
function compactReporter(reporter: unknown, index: number,) {
	const fields = asRecord(reporter,);
	const messaging = asRecord(fields?.["messaging"],);
	const configuration = asRecord(messaging?.["configuration"],);
	return {
		index,
		id: fields?.["id"],
		name: fields?.["name"],
		active: fields?.["active"],
		phase: fields?.["phase"],
		runCondition: fields?.["runConditionEnabled"] === true ? fields["runCondition"] : undefined,
		type: messaging?.["type"],
		channel: configuration?.["channelId"],
		recipient: configuration?.["recipient"],
		subject: configuration?.["subject"],
	};
}

/** Shared `trigger-add`/`reporter-add`: dry-run previews the appended element, otherwise it is written and verified. */
async function addScenarioItem(
	c: DataikuClient,
	kind: ScenarioListKind,
	scenarioId: string,
	item: Record<string, unknown>,
	flags: Record<string, string | boolean>,
) {
	const projectKey = flags["project-key"] as string | undefined;
	const listAction = `${kind}-list`;
	if (executionMode(flags,).dryRun) {
		const items = await c.scenarios.listItems(kind, scenarioId, { projectKey, },);
		return {
			dryRun: true,
			action: `${kind}-add`,
			resource: "scenario",
			id: scenarioId,
			[kind]: sanitizeMessagingChannelSecrets(item,),
			index: items.length,
			count: items.length + 1,
		};
	}
	// A raw reporter can carry a webhook URL or token that DSS may echo back in an error.
	const secrets = messagingChannelSecretsFromBody(item,);
	const result = await c.scenarios.addItem(kind, scenarioId, item, { projectKey, },).catch(
		(error: unknown,) => {
			throw sanitizeMessagingChannelError(error, secrets,);
		},
	);
	return {
		added: kind,
		resource: "scenario",
		id: scenarioId,
		index: result.index,
		[kind]: sanitizeMessagingChannelSecrets(result.item,),
		count: result.count,
		next: [["dss", "scenario", listAction, scenarioId, ...projectKeyArgs(projectKey,),],],
	};
}

/** Shared `trigger-remove`/`reporter-remove`: the element is chosen by --index or --name; the others stay. */
async function removeScenarioItem(
	c: DataikuClient,
	kind: ScenarioListKind,
	scenarioId: string,
	flags: Record<string, string | boolean>,
) {
	const projectKey = flags["project-key"] as string | undefined;
	const selector = {
		index: num(flags["index"], "--index",),
		name: flags["name"] as string | undefined,
	};
	if (executionMode(flags,).dryRun) {
		const items = await c.scenarios.listItems(kind, scenarioId, { projectKey, },);
		const index = selectScenarioListIndex(items, selector, kind,);
		return {
			dryRun: true,
			action: `${kind}-remove`,
			resource: "scenario",
			id: scenarioId,
			index,
			[kind]: sanitizeMessagingChannelSecrets(items[index],),
			count: items.length - 1,
		};
	}
	const result = await c.scenarios.removeItem(kind, scenarioId, selector, { projectKey, },);
	return {
		removed: kind,
		resource: "scenario",
		id: scenarioId,
		index: result.index,
		[kind]: sanitizeMessagingChannelSecrets(result.item,),
		count: result.count,
	};
}

export const scenarioCommands: Record<string, CommandMeta> = withUsage("scenario", {
	list: {
		handler: (c, _a, f,) => c.scenarios.list(f["project-key"] as string | undefined,),
		description: "List all scenarios in a project.",
		examples: ["dss scenario list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "get",),);
			return c.scenarios.get(a[0], { projectKey: f["project-key"] as string | undefined, },);
		},
		description:
			"Get raw scenario definition. For step-based scenario edits, patch params.steps; rawParams.params is DSS echo data.",
		examples: ["dss scenario get my_scenario",],
	},
	run: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "run",),);
			const pk = f["project-key"] as string | undefined;
			const params = scenarioRunParams(f,);
			const options = {
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
			};
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "run",
					resource: "scenario",
					id: a[0],
					...options,
					...(params === undefined ? {} : { params, }),
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/run/`,
					),
					method: "POST",
				};
			}
			if (f["wait"] === true) {
				return runAndWaitWithFailure(c, a[0], { ...options, params, projectKey: pk, },);
			}
			return triggerRunResult(await c.scenarios.run(a[0], { params, projectKey: pk, },), a[0], pk,);
		},
		description:
			"Trigger a scenario run; --wait waits for it. --params JSON/--params-file PATH are the run parameters. Without --wait the result is the trigger fire: triggerRunId is NOT a scenario run id, resolve it with get-run-for-trigger (see `next`). cancelled: true means the scenario was already running; no run starts.",
		examples: [
			"dss scenario run my_scenario",
			"dss scenario run my_scenario --wait",
			'dss scenario run my_scenario --params \'{"region":"EU"}\' --wait',
		],
	},
	"run-and-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "run-and-wait",),);
			const pk = f["project-key"] as string | undefined;
			const params = scenarioRunParams(f,);
			const options = {
				pollIntervalMs: num(f["poll-interval"], "--poll-interval",),
				timeoutMs: num(f["timeout"], "--timeout",),
			};
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "run-and-wait",
					resource: "scenario",
					id: a[0],
					...options,
					...(params === undefined ? {} : { params, }),
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/run/`,
					),
					method: "POST",
				};
			}
			return runAndWaitWithFailure(c, a[0], { ...options, params, projectKey: pk, },);
		},
		description:
			"Run a scenario and wait for completion (--params JSON / --params-file PATH as run parameters). Outcome CANCELLED (exit 4) means DSS cancelled the trigger fire because the scenario was already running; TIMEOUT means the wait ran out while the run continues.",
		examples: [
			"dss scenario run-and-wait my_scenario",
			"dss scenario run-and-wait my_scenario --timeout 300000",
			"dss scenario run-and-wait my_scenario --params-file params.json",
		],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "status",),);
			return c.scenarios.status(a[0], f["project-key"] as string | undefined,);
		},
		description: "Get the current run status of a scenario.",
		examples: ["dss scenario status my_scenario",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "delete",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() => c.scenarios.get(a[0], { projectKey: pk, },));
				if (!current) return skipResult("scenario", a[0], "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "scenario", id: a[0], current, };
				}
			}
			await c.scenarios.delete(a[0], pk,);
			return { deleted: a[0], resource: "scenario", };
		},
		description: "Delete a scenario.",
		examples: ["dss scenario delete my_scenario", "dss scenario delete my_scenario --if-exists",],
	},
	create: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "create",),);
			const pk = f["project-key"] as string | undefined;
			const data = jsonInput(f,);
			const payload = {
				scenarioId: a[0],
				name: a[1],
				scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
				projectKey: pk,
				...(data === undefined ? {} : { data, }),
			};
			if (f["if-not-exists"] === true || executionMode(f,).dryRun) {
				const list = await c.scenarios.list(pk,);
				const existing = list.find((s,) => s.id === a[0]);
				if (existing && f["if-not-exists"] === true && !executionMode(f,).dryRun) {
					return skipResult("scenario", a[0], "exists", { current: existing, },);
				}
				if (executionMode(f,).dryRun) {
					return {
						dryRun: true,
						action: "create",
						resource: "scenario",
						id: a[0],
						payload,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			await c.scenarios.create(a[0], a[1], {
				scenarioType: payload.scenarioType,
				data,
				projectKey: pk,
			},);
			return { created: a[0], name: a[1], resource: "scenario", };
		},
		description:
			"Create a new scenario. --data/--data-file/--stdin merge extra top-level fields into the create body (e.g. triggers, reporters, params.steps, runAsUser).",
		examples: [
			'dss scenario create my_scenario "My Scenario"',
			'dss scenario create my_scenario "My Scenario" --type custom_python --dry-run',
			'dss scenario create nightly "Nightly" --data \'{"triggers":[{"active":true,"type":"temporal","params":{"frequency":"Daily","hour":2,"minute":0,"startingFrom":"2026-01-01T00:00:00.000+0000","repeatFrequency":1,"timezone":"SERVER"}}]}\' --dry-run',
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "update",),);
			const data = jsonInput(f,);
			if (data === undefined) {
				throw new UsageError(
					`--data, --data-file, or --stdin is required. Usage: ${commandUsage("scenario", "update",)}`,
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				const current = await c.scenarios.get(a[0], { projectKey: pk, },);
				const { scenarioUpdatePreview, } = await import("../../resources/scenarios.js");
				const preview = scenarioUpdatePreview(current, data,);
				return {
					dryRun: true,
					action: "update",
					resource: "scenario",
					id: a[0],
					canonicalEditableFields: preview.canonicalEditableFields,
					normalization: preview.normalization,
					normalizedData: preview.normalizedData,
					changes: preview.changes,
					unchangedPaths: preview.unchangedPaths,
					current: preview.current,
					next: preview.next,
				};
			}
			const result = await c.scenarios.update(a[0], data, pk,);
			return {
				updated: a[0],
				resource: "scenario",
				verified: result.verified,
				changed: result.changes.length > 0,
				canonicalEditableFields: result.canonicalEditableFields,
				normalization: result.normalization,
				...(result.normalization.length > 0 ? { normalizedData: result.normalizedData, } : {}),
				changes: result.changes,
				unchangedPaths: result.unchangedPaths,
			};
		},
		description:
			"Update scenario settings via JSON merge; edit step-based scenario steps at params.steps, not rawParams.params.steps.",
		examples: [
			'dss scenario update my_scenario --data \'{"params":{"steps":[]}}\' --dry-run',
			"dss scenario update my_scenario --data-file settings.json --dry-run",
		],
	},
	abort: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "abort",),);
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "abort",
					resource: "scenario",
					id: a[0],
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/abort`,
					),
					method: "POST",
				};
			}
			await c.scenarios.abort(a[0], pk,);
			return { aborted: a[0], resource: "scenario", };
		},
		description:
			"Abort a running scenario. Returns when DSS accepts the abort; the scenario may take time to stop.",
		examples: ["dss scenario abort my_scenario",],
	},
	"last-runs": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "last-runs",),);
			return c.scenarios.getLastRuns(a[0], {
				limit: num(f["limit"], "--limit",),
				onlyFinished: f["only-finished"] === true,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			"List a scenario's most recent runs, newest first (compact: run id, outcome, timing, trigger, failure; --full for DSS objects). --only-finished skips in-progress (and, observed live, aborted) runs: `--only-finished --limit 1` is the last finished run; read its outcome.",
		examples: [
			"dss scenario last-runs my_scenario --limit 10",
			"dss scenario last-runs my_scenario --only-finished --limit 1",
		],
	},
	"get-run": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "get-run",),);
			const projectKey = f["project-key"] as string | undefined;
			const details = await c.scenarios.getRunDetails(a[0], a[1], { projectKey, },);
			if (f["full"] === true || typeof f["fields"] === "string") return details;
			// Compact view, located at the failed recipe (one extra job read, only on failure).
			// The shaped object has no `scenarioRun`, so the dispatcher's shaping passes it through.
			const shaped = asRecord(shapeCommandResult("scenario", "get-run", details, f,),);
			if (!shaped?.["failure"]) return shaped ?? details;
			return { ...shaped, ...await locatedDiagnosis(c, details, projectKey,), };
		},
		description:
			"Get a scenario run: outcome, timing, trigger, per-step datasets and jobs, plus `failure` {stepId, jobId, dataset, recipe, inputs, message} and `next` argv (recipe code, input schemas, job errors) for the first failure (compact by default; --full returns the raw DSS run report).",
		examples: ["dss scenario get-run my_scenario 2024-09-01-01-02-03-123",],
	},
	log: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "log",),);
			const result = await c.scenarios.getRunLog(a[0], a[1], {
				stepId: f["step-id"] as string | undefined,
				maxLogBytes: num(f["max-log-bytes"], "--max-log-bytes",),
				projectKey: f["project-key"] as string | undefined,
			},);
			const outputFile = (f["output"] as string | undefined)
				?? (f["output-file"] as string | undefined);
			if (outputFile) {
				const outputPath = resolve(outputFile,);
				await Bun.write(outputPath, result.text,);
				return { path: outputPath, truncated: result.truncated, };
			}
			return result.text;
		},
		description:
			"Get the log of a scenario run (or one step via --step-id). Byte-bounded at --max-log-bytes (default 1 MiB); --output PATH writes it to a file (stdout returns the path).",
		examples: [
			"dss scenario log my_scenario 2024-09-01-01-02-03-123",
			"dss scenario log my_scenario RUN_ID --step-id prepare_1 --max-log-bytes 100000",
		],
	},
	"payload-get": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "payload-get",),);
			return c.scenarios.getPayload(a[0], { projectKey: f["project-key"] as string | undefined, },);
		},
		description: "Get the payload of a custom scenario (e.g. its Python script).",
		examples: ["dss scenario payload-get my_custom_scenario",],
	},
	"payload-set": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("scenario", "payload-set",),
			);
			const payload = jsonInput(f,);
			if (payload === undefined) {
				throw new UsageError(
					`--data, --data-file, or --stdin is required. Usage: ${
						commandUsage("scenario", "payload-set",)
					}`,
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "payload-set",
					resource: "scenario",
					id: a[0],
					payload,
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/payload`,
					),
					method: "PUT",
				};
			}
			await c.scenarios.setPayload(a[0], payload, { projectKey: pk, },);
			return { updated: a[0], resource: "scenario", part: "payload", };
		},
		description: "Update the payload of a custom scenario (e.g. replace its Python script).",
		examples: [
			'dss scenario payload-set my_custom_scenario --data \'{"script":"print(1)"}\' --dry-run',
			"dss scenario payload-set my_custom_scenario --data-file payload.json",
		],
	},
	"active-set": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "active-set",),);
			const active = parseBooleanOption(a[1], "active",);
			if (active === undefined) {
				throw new UsageError(
					`active must be 'true' or 'false'. Usage: ${commandUsage("scenario", "active-set",)}`,
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "active-set",
					resource: "scenario",
					id: a[0],
					active,
					endpoint: encodedProjectEndpoint(c, pk, `/scenarios/${encodeURIComponent(a[0],)}/light`,),
					method: "PUT",
				};
			}
			return c.scenarios.setActive(a[0], active, { projectKey: pk, },);
		},
		description:
			"Activate or deactivate a scenario (light update; deactivated scenarios ignore triggers).",
		examples: [
			"dss scenario active-set my_scenario false",
			"dss scenario active-set my_scenario true --dry-run",
		],
	},
	"get-run-for-trigger": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "get-run-for-trigger",),);
			const pk = f["project-key"] as string | undefined;
			const state = await c.scenarios.getRunForTrigger(a[0], a[1], {
				triggerId: f["trigger-id"] as string | undefined,
				projectKey: pk,
			},);
			return triggerRunStateResult(state, a[0], pk,);
		},
		description:
			"Resolve a trigger run (the triggerRunId `scenario run` returns) to its scenario run: state PENDING (no run yet; repeat), RUNNING, FINISHED (outcome), or CANCELLED (DSS cancelled the fire because the scenario was already running; no run will exist). `runId` is what get-run and log take.",
		examples: ["dss scenario get-run-for-trigger my_scenario 2024-09-01-01-02-03-123",],
	},
	"runs-by-date": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "runs-by-date",),);
			const from = f["from"];
			if (typeof from !== "string") {
				throw new UsageError(
					`--from is required. Usage: ${commandUsage("scenario", "runs-by-date",)}`,
				);
			}
			return c.scenarios.getRunsByDate(a[0], {
				from,
				to: f["to"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			"List the runs of a scenario that started from --from (inclusive) to --to (exclusive: DSS compares the start of that day, so --from D --to D is empty), each YYYY-MM-DD or an ISO date-time (time dropped). --to defaults to tomorrow, i.e. up to now. Compact like last-runs; --full returns the DSS run objects.",
		examples: ["dss scenario runs-by-date my_scenario --from 2026-10-01 --to 2026-10-31",],
	},
	"run-report": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "run-report",),);
			const result = await c.scenarios.getRunReport(a[0], a[1], {
				stepId: f["step-id"] as string | undefined,
				maxBytes: num(f["max-bytes"], "--max-bytes",),
				projectKey: f["project-key"] as string | undefined,
			},);
			const outputFile = (f["output"] as string | undefined)
				?? (f["output-file"] as string | undefined);
			if (outputFile) {
				const outputPath = resolve(outputFile,);
				await Bun.write(outputPath, result.text,);
				return { path: outputPath, truncated: result.truncated, };
			}
			return result.text;
		},
		description:
			"Get the JUnit XML report of a test scenario run (or of one step via --step-id). Only scenarios marked as test (markedAsTest) have reports. Byte-bounded at --max-bytes (default 1 MiB); --output PATH writes it to a file (stdout returns the path).",
		examples: [
			"dss scenario run-report my_test_scenario RUN_ID --output report.xml",
			"dss scenario run-report my_test_scenario RUN_ID --step-id run_tests",
		],
	},
	"trigger-list": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "trigger-list",),);
			const triggers = await c.scenarios.listItems("trigger", a[0], {
				projectKey: f["project-key"] as string | undefined,
			},);
			return triggers.map((trigger, index,) => ({ index, ...asRecord(trigger,), }));
		},
		description:
			"List the automatic triggers of a scenario with their 0-based index (the --index of trigger-remove). Triggers only fire while the scenario is active (scenario active-set).",
		examples: ["dss scenario trigger-list my_scenario",],
	},
	"trigger-add": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "trigger-add",),);
			return addScenarioItem(c, "trigger", a[0], scenarioTriggerFromFlags(f,), f,);
		},
		description:
			"Append a trigger, keeping the others. Schedule: --frequency (dataikuapi builders) with --every, --hour, --minute, --days (weekly), --day-of-month/--run-on (monthly), --start-date, --timezone (default SERVER), --name, --active. Other trigger types: JSON via --data.",
		examples: [
			"dss scenario trigger-add my_scenario --frequency daily --hour 6 --minute 30",
			"dss scenario trigger-add my_scenario --frequency weekly --days Monday,Friday --hour 8",
			"dss scenario trigger-add my_scenario --frequency monthly --day-of-month 5 --run-on ON_THE_DAY --dry-run",
			'dss scenario trigger-add my_scenario --data \'{"type":"temporal","active":true,"params":{"frequency":"Minutely","repeatFrequency":15}}\'',
		],
	},
	"trigger-remove": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "trigger-remove",),);
			return removeScenarioItem(c, "trigger", a[0], f,);
		},
		description:
			"Remove one trigger, selected by --index (from trigger-list) or --name, keeping the others.",
		examples: [
			"dss scenario trigger-remove my_scenario --index 0",
			"dss scenario trigger-remove my_scenario --name nightly --dry-run",
		],
	},
	"reporter-list": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "reporter-list",),);
			const reporters = await c.scenarios.listItems("reporter", a[0], {
				projectKey: f["project-key"] as string | undefined,
			},);
			// Channel credentials live on the channel, but reporters of other types may carry their own.
			return reporters.map((reporter, index,) =>
				f["full"] === true
					? { index, ...asRecord(sanitizeMessagingChannelSecrets(reporter,),), }
					: compactReporter(reporter, index,)
			);
		},
		description:
			"List the reporters (notifications) of a scenario with their 0-based index: id, phase, active, channel, recipient, subject (compact by default; --full returns the DSS objects with credential-looking values redacted).",
		examples: ["dss scenario reporter-list my_scenario",],
	},
	"reporter-add": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "reporter-add",),);
			const reporter = scenarioReporterFromFlags(f,);
			// DSS saves a reporter with any channel id; an unknown one never sends. Warn, don't fail:
			// a channel may be hidden from this key yet usable by the scenario's run-as user.
			const channel = f["channel"];
			if (typeof channel === "string") {
				const known = (await c.messagingChannels.list()).map((entry,) => entry.id);
				if (!known.includes(channel,)) {
					enqueueCliWarning({
						code: "reporter_channel_unknown",
						channel,
						knownChannels: known,
						hint:
							"No messaging channel with this id is visible: the reporter will not send. Check `dss messaging-channel list`.",
					},);
				}
			}
			return addScenarioItem(c, "reporter", a[0], reporter, f,);
		},
		description:
			"Append a reporter, keeping the others. Mail: --channel ID (messaging-channel list), --recipient, --subject, --message (required), --condition EXPR, --phase START|END (default END). Slack, Teams and other channel types: pass the reporter JSON with --data.",
		examples: [
			'dss scenario reporter-add my_scenario --channel smtp --recipient ops@example.com --subject "Scenario done" --message "See DSS"',
			"dss scenario reporter-add my_scenario --channel smtp --recipient ops@example.com --subject Failed --message Failed --condition \"outcome == 'FAILED'\" --dry-run",
		],
	},
	"reporter-remove": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "reporter-remove",),);
			return removeScenarioItem(c, "reporter", a[0], f,);
		},
		description:
			"Remove one reporter, selected by --index (from reporter-list) or --name, keeping the others.",
		examples: ["dss scenario reporter-remove my_scenario --index 0",],
	},
	"run-as-set": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "run-as-set",),);
			const user = f["user"];
			const clear = f["clear"] === true;
			if ((typeof user === "string") === clear) {
				throw new UsageError(
					`Pass exactly one of --user LOGIN or --clear. Usage: ${
						commandUsage("scenario", "run-as-set",)
					}`,
				);
			}
			const runAsUser = typeof user === "string" ? user : null;
			const pk = f["project-key"] as string | undefined;
			if (executionMode(f,).dryRun) {
				const current = asRecord(await c.scenarios.get(a[0], { projectKey: pk, },),);
				return {
					dryRun: true,
					action: "run-as-set",
					resource: "scenario",
					id: a[0],
					runAsUser,
					currentRunAsUser: current?.["runAsUser"] ?? null,
					endpoint: encodedProjectEndpoint(c, pk, `/scenarios/${encodeURIComponent(a[0],)}/`,),
					method: "PUT",
				};
			}
			const result = await c.scenarios.setRunAs(a[0], runAsUser, { projectKey: pk, },);
			return {
				updated: a[0],
				resource: "scenario",
				part: "runAsUser",
				runAsUser: result.runAsUser,
				effectiveRunAs: result.effectiveRunAs,
				...(result.previousRunAsUser === undefined
					? {}
					: { previousRunAsUser: result.previousRunAsUser, }),
			};
		},
		description:
			"Set the user a scenario runs as (--user LOGIN, administrators only) or clear it (--clear: it then runs as its last editor). The result reports the effective run-as; scenario status shows runAsUser.",
		examples: [
			"dss scenario run-as-set my_scenario --user service_account",
			"dss scenario run-as-set my_scenario --clear --dry-run",
		],
	},
},);
