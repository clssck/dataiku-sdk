import type { DataikuClient, } from "../../client.js";
import type { ScenarioWaitResult, } from "../../schemas.js";
import { asRecord, } from "../../utils/records.js";
import { jsonInput, num, parseBooleanOption, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
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
 * only on failure; a read error leaves the wait result untouched.
 */
async function runAndWaitWithFailure(
	c: DataikuClient,
	scenarioId: string,
	options: { pollIntervalMs?: number; timeoutMs?: number; projectKey?: string; },
) {
	const result: ScenarioWaitResult = await c.scenarios.runAndWait(scenarioId, options,);
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
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/run/`,
					),
					method: "POST",
				};
			}
			if (f["wait"] === true) {
				return runAndWaitWithFailure(c, a[0], { ...options, projectKey: pk, },);
			}
			return c.scenarios.run(a[0], pk,);
		},
		description: "Trigger a scenario run, optionally waiting for completion.",
		examples: ["dss scenario run my_scenario", "dss scenario run my_scenario --wait",],
	},
	"run-and-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("scenario", "run-and-wait",),);
			const pk = f["project-key"] as string | undefined;
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
					endpoint: encodedProjectEndpoint(
						c,
						pk,
						`/scenarios/${encodeURIComponent(a[0],)}/run/`,
					),
					method: "POST",
				};
			}
			return runAndWaitWithFailure(c, a[0], { ...options, projectKey: pk, },);
		},
		description: "Run a scenario and wait for completion.",
		examples: [
			"dss scenario run-and-wait my_scenario",
			"dss scenario run-and-wait my_scenario --timeout 300000",
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
			const payload = {
				scenarioId: a[0],
				name: a[1],
				scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
				projectKey: pk,
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
				projectKey: pk,
			},);
			return { created: a[0], name: a[1], resource: "scenario", };
		},
		description: "Create a new scenario.",
		examples: [
			'dss scenario create my_scenario "My Scenario"',
			'dss scenario create my_scenario "My Scenario" --type custom_python --dry-run',
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
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			"List the most recent runs of a scenario (compact by default: run id, outcome, start/end, trigger, failure message; --full returns the DSS run objects).",
		examples: ["dss scenario last-runs my_scenario --limit 10",],
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
				const { resolve, } = await import("node:path");
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
},);
