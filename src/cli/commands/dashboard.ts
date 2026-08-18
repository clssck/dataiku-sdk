import * as fs from "node:fs";
import { ClientValidationError, } from "../../errors.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, parseBooleanOption, } from "../coerce.js";
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
	export: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss dashboard export <id> --output PATH [--paper-size SIZE] [--orientation PORTRAIT|LANDSCAPE] [--file-type PDF] [--slide-index N] [--project-key KEY]",
			);
			const outputPath = f["output"];
			if (typeof outputPath !== "string" || !outputPath.trim()) {
				throw new UsageError("--output PATH is required.", "missing_required_flag",);
			}

			const paperSizeFlag = f["paper-size"];
			if (paperSizeFlag !== undefined && typeof paperSizeFlag !== "string") {
				throw new UsageError("--paper-size requires a value.",);
			}
			const paperSize = (paperSizeFlag ?? "A4").trim();
			if (!paperSize) throw new UsageError("--paper-size must not be empty.",);

			const orientationFlag = f["orientation"];
			if (orientationFlag !== undefined && typeof orientationFlag !== "string") {
				throw new UsageError("--orientation requires a value.",);
			}
			const orientation = (orientationFlag ?? "LANDSCAPE").trim().toUpperCase();
			if (orientation !== "PORTRAIT" && orientation !== "LANDSCAPE") {
				throw new UsageError("--orientation must be PORTRAIT or LANDSCAPE.", "invalid_enum",);
			}

			const fileTypeFlag = f["file-type"];
			if (fileTypeFlag !== undefined && typeof fileTypeFlag !== "string") {
				throw new UsageError("--file-type requires a value.",);
			}
			const fileType = (fileTypeFlag ?? "PDF").trim().toUpperCase();
			if (fileType !== "PDF") {
				throw new UsageError("--file-type must be PDF.", "invalid_enum",);
			}

			const slideIndexFlag = f["slide-index"];
			if (
				slideIndexFlag !== undefined
				&& (typeof slideIndexFlag !== "string" || !/^(0|[1-9]\d*)$/u.test(slideIndexFlag,))
			) {
				throw new UsageError("--slide-index must be a non-negative integer.",);
			}
			const slideIndex = slideIndexFlag === undefined ? 0 : Number(slideIndexFlag,);

			const response = await c.dashboards.export(a[0], {
				paperSize,
				orientation,
				fileType,
				slideIndex,
				projectKey: f["project-key"] as string | undefined,
			},);
			const bytes = Buffer.from(await response.arrayBuffer(),);
			const isPdf = bytes.length >= 5
				&& bytes[0] === 0x25
				&& bytes[1] === 0x50
				&& bytes[2] === 0x44
				&& bytes[3] === 0x46
				&& bytes[4] === 0x2d;
			if (!isPdf) {
				throw new ClientValidationError(
					"DSS dashboard export did not return a PDF.",
					"validation_failed",
					"Confirm DSS graphics export is configured and retry the export.",
				);
			}
			fs.writeFileSync(outputPath, bytes,);
			return {
				path: outputPath,
				bytes: bytes.length,
				contentType: response.headers.get("content-type",) ?? undefined,
				dashboardId: a[0],
				paperSize,
				orientation,
				fileType,
				slideIndex,
			};
		},
		usage:
			"dss dashboard export <id> --output PATH [--paper-size SIZE] [--orientation PORTRAIT|LANDSCAPE] [--file-type PDF] [--slide-index N] [--project-key KEY]",
		description: "Render a dashboard page as PDF. Requires DSS graphics export to be configured.",
		examples: [
			"dss dashboard export DASHBOARD_ID --output dashboard.pdf",
			"dss dashboard export DASHBOARD_ID --output dashboard.pdf --orientation LANDSCAPE --slide-index 0",
		],
	},
	create: {
		handler: async (c, _a, f,) => {
			const settings = jsonInput(f,);
			const flagName = f["name"] as string | undefined;
			const settingsName = settings?.["name"];
			const name = flagName ?? (typeof settingsName === "string" ? settingsName : undefined);
			const listed = parseBooleanOption(f["listed"], "--listed",);
			if (!name) {
				throw new UsageError(
					"--name or dashboard settings containing a string name are required. Usage: dss dashboard create --name NAME",
				);
			}
			const payload: Record<string, unknown> = { ...(settings ?? { pages: [], }), name, };
			if (listed !== undefined) payload.listed = listed;
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
						payload,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.dashboards.create({
				name,
				listed,
				settings,
				projectKey: pk,
			},);
			return { created: created.id, resource: "dashboard", ...created, };
		},
		usage:
			"dss dashboard create (--name NAME|--data JSON|--data-file PATH|--stdin) [--listed true|false] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description:
			"Create a dashboard from raw settings or minimal name/listed fields. Defaults to an empty pages array.",
		examples: [
			"dss dashboard create --name 'Agent dashboard' --listed true --dry-run",
			'dss dashboard create --data \'{"name":"Agent dashboard","pages":[]}\'',
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss dashboard update <id> [--name NAME|--listed true|false|--data JSON]",
			);
			const name = f["name"] as string | undefined;
			const listed = parseBooleanOption(f["listed"], "--listed",);
			const data = jsonInput(f,);
			if (name === undefined && listed === undefined && !data) {
				throw new UsageError("--name, --listed, --data, --data-file, or --stdin is required.",);
			}
			if (f["dry-run"] === true) {
				const current = await c.dashboards.get(a[0], f["project-key"] as string | undefined,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data ?? {},);
				if (name !== undefined) next.name = name;
				if (listed !== undefined) next.listed = listed;
				return { dryRun: true, action: "update", resource: "dashboard", id: a[0], current, next, };
			}
			return c.dashboards.update(a[0], {
				name,
				listed,
				data,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage:
			"dss dashboard update <id> (--name NAME|--listed true|false|--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Update dashboard settings via merge.",
		examples: ["dss dashboard update DASHBOARD_ID --listed true --dry-run",],
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
