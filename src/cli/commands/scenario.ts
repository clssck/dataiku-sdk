import { scenarioUpdatePreview, } from "../../resources/scenarios.js";
import { jsonInput, num, } from "../coerce.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const scenarioCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.scenarios.list(f["project-key"] as string | undefined,),
		usage: "dss scenario list [--project-key KEY]",
		description: "List all scenarios in a project.",
		examples: ["dss scenario list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss scenario get <id>",);
			return c.scenarios.get(a[0], { projectKey: f["project-key"] as string | undefined, },);
		},
		usage: "dss scenario get <id> [--project-key KEY]",
		description:
			"Get raw scenario definition. For step-based scenario edits, patch params.steps; rawParams.params is DSS echo data.",
		examples: ["dss scenario get my_scenario",],
	},
	run: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss scenario run <id>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				pollIntervalMs: num(f["poll-interval"],),
				timeoutMs: num(f["timeout"],),
			};
			if (f["dry-run"] === true) {
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
		usage:
			"dss scenario run <id> [--wait] [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
		description: "Trigger a scenario run, optionally waiting for completion.",
		examples: ["dss scenario run my_scenario", "dss scenario run my_scenario --wait",],
	},
	"run-and-wait": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss scenario run-and-wait <id>",);
			const pk = f["project-key"] as string | undefined;
			const options = {
				pollIntervalMs: num(f["poll-interval"],),
				timeoutMs: num(f["timeout"],),
			};
			if (f["dry-run"] === true) {
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
		usage:
			"dss scenario run-and-wait <id> [--timeout MS] [--poll-interval MS] [--dry-run] [--project-key KEY]",
		description: "Run a scenario and wait for completion.",
		examples: [
			"dss scenario run-and-wait my_scenario",
			"dss scenario run-and-wait my_scenario --timeout 300000",
		],
	},
	status: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss scenario status <id>",);
			return c.scenarios.status(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss scenario status <id> [--project-key KEY]",
		description: "Get the current run status of a scenario.",
		examples: ["dss scenario status my_scenario",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss scenario delete <id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.scenarios.get(a[0], { projectKey: pk, },));
				if (!current) return skipResult("scenario", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "scenario", id: a[0], current, };
				}
			}
			await c.scenarios.delete(a[0], pk,);
			return { deleted: a[0], resource: "scenario", };
		},
		usage: "dss scenario delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a scenario.",
		examples: ["dss scenario delete my_scenario", "dss scenario delete my_scenario --if-exists",],
	},
	create: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss scenario create <id> <name>",);
			const pk = f["project-key"] as string | undefined;
			const payload = {
				scenarioId: a[0],
				name: a[1],
				scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
				projectKey: pk,
			};
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const list = await c.scenarios.list(pk,);
				const existing = list.find((s,) => s.id === a[0]);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("scenario", a[0], "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
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
		usage:
			"dss scenario create <id> <name> [--type step_based|custom_python] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a new scenario.",
		examples: [
			'dss scenario create my_scenario "My Scenario"',
			'dss scenario create my_scenario "My Scenario" --type custom_python --dry-run',
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",);
			const data = jsonInput(f,);
			if (data === undefined) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required. Usage: dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",
				);
			}
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await c.scenarios.get(a[0], { projectKey: pk, },);
				const preview = scenarioUpdatePreview(current as unknown as Record<string, unknown>, data,);
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
		usage:
			"dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
		description:
			"Update scenario settings via JSON merge; edit step-based scenario steps at params.steps, not rawParams.params.steps.",
		examples: [
			'dss scenario update my_scenario --data \'{"params":{"steps":[]}}\' --dry-run',
			"dss scenario update my_scenario --data-file settings.json --dry-run",
		],
	},
};
