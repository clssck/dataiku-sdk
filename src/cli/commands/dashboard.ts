import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, } from "../coerce.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const dashboardCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.dashboards.list(f["project-key"] as string | undefined,),
		usage: "dss dashboard list [--project-key KEY]",
		description: "List project dashboards.",
		examples: ["dss dashboard list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss dashboard get <id>",);
			return c.dashboards.get(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss dashboard get <id> [--project-key KEY]",
		description: "Get dashboard definition.",
		examples: ["dss dashboard get DASHBOARD_ID",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const name = f["name"] as string | undefined;
			if (!name) throw new UsageError("--name is required. Usage: dss dashboard create --name NAME",);
			const settings = jsonInput(f,);
			const pk = f["project-key"] as string | undefined;
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = (await c.dashboards.list(pk,)).find((dashboard,) => dashboard.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("dashboard", existing.id, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "dashboard",
						name,
						payload: settings ?? { pages: [], },
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.dashboards.create({
				name,
				settings,
				projectKey: pk,
			},);
			return { created: created.id, resource: "dashboard", ...created, };
		},
		usage:
			"dss dashboard create --name NAME [--data JSON|--data-file PATH|--stdin] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a dashboard. Defaults to an empty pages array.",
		examples: ["dss dashboard create --name 'Agent dashboard' --dry-run",],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dashboard update <id> [--name NAME|--data JSON]",);
			const name = f["name"] as string | undefined;
			const data = jsonInput(f,);
			if (!name && !data) {
				throw new UsageError("--name, --data, --data-file, or --stdin is required.",);
			}
			if (f["dry-run"] === true) {
				const current = await c.dashboards.get(a[0], f["project-key"] as string | undefined,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
				if (name !== undefined) next.name = name;
				return { dryRun: true, action: "update", resource: "dashboard", id: a[0], current, next, };
			}
			return c.dashboards.update(a[0], {
				name,
				data,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss dashboard update <id> [--name NAME|--data JSON|--data-file PATH|--stdin] [--dry-run] [--project-key KEY]",
		description: "Update dashboard settings via merge.",
		examples: ["dss dashboard update DASHBOARD_ID --name 'New name' --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dashboard delete <id>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.dashboards.get(a[0], pk,));
				if (!current) return skipResult("dashboard", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "dashboard", id: a[0], current, };
				}
			}
			await c.dashboards.delete(a[0], pk,);
			return { deleted: a[0], resource: "dashboard", };
		},
		usage: "dss dashboard delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a dashboard.",
		examples: ["dss dashboard delete DASHBOARD_ID --dry-run",],
	},
};
