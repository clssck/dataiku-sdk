import { jsonInput, num, parseBooleanOption, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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
				return c.scenarios.runAndWait(a[0], { ...options, projectKey: pk, },);
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
			return c.scenarios.runAndWait(a[0], { ...options, projectKey: pk, },);
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
		description: "List the most recent runs of a scenario (run id, start/end, outcome).",
		examples: ["dss scenario last-runs my_scenario --limit 10",],
	},
	"get-run": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, commandUsage("scenario", "get-run",),);
			return c.scenarios.getRunDetails(a[0], a[1], {
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "Get the details of a specific scenario run, including per-step outcomes.",
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
				const { mkdir, writeFile, } = await import("node:fs/promises");
				const { dirname, resolve, } = await import("node:path");
				const outputPath = resolve(outputFile,);
				await mkdir(dirname(outputPath,), { recursive: true, },);
				await writeFile(outputPath, result.text, "utf-8",);
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
