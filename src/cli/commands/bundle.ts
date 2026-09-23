import { unexpectedResponseError, } from "../../errors.js";
import { writeResponseToFile, } from "../../utils/response-file.js";
import { json, parseBooleanOption, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const bundleCommands: Record<string, CommandMeta> = withUsage("bundle", {
	"list-exported": {
		handler: (c, _a, f,) => c.bundles.listExported(f["project-key"] as string | undefined,),
		description: "List bundles exported from a project on the Design node.",
		examples: ["dss bundle list-exported",],
	},
	export: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("bundle", "export",);
			requireArgs(a, 1, usage,);
			await c.bundles.exportBundle(a[0], f["project-key"] as string | undefined, {
				...(typeof f["release-notes"] === "string" ? { releaseNotes: f["release-notes"], } : {}),
				...(f["evaluate-standards-checks"] !== undefined
					? {
						evaluateProjectStandardsChecks: parseBooleanOption(
							f["evaluate-standards-checks"],
							"--evaluate-standards-checks",
						) ?? true,
					}
					: {}),
			},);
			return { exported: a[0], };
		},
		description:
			"Create an exported Design-node bundle. --release-notes forwards the documented releaseNotes parameter; --evaluate-standards-checks toggles the documented Project Standards Checks evaluation (default true; this instance forbids disabling it). DSS refuses re-exporting an existing bundle id — delete-exported first to replace one.",
		examples: [
			"dss bundle export v1",
			"dss bundle export v1 --release-notes='Adds churn model' --evaluate-standards-checks=false",
		],
	},
	"delete-exported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "delete-exported",),);
			await c.bundles.deleteExported(a[0], f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		description: "Delete an exported Design-node bundle.",
		examples: ["dss bundle delete-exported v1",],
	},
	"download-exported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "download-exported",),);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const res = await c.bundles.downloadExportedArchive(
				a[0],
				f["project-key"] as string | undefined,
			);
			if (!res.body) {
				throw unexpectedResponseError(
					"bundles.downloadExportedArchive response did not include a body",
				);
			}
			const bytes = await writeResponseToFile(out, res,);
			return { path: out, bytes, };
		},
		description: "Download an exported Design-node bundle archive to a local file.",
		examples: ["dss bundle download-exported v1 --output ./bundle.zip",],
	},
	publish: {
		handler: (c, a, f,) => {
			const usage = commandUsage("bundle", "publish",);
			requireArgs(a, 1, usage,);
			return c.bundles.publish(
				a[0],
				f["project-key"] as string | undefined,
				typeof f["published-project-key"] === "string"
					? { publishedProjectKey: f["published-project-key"], }
					: {},
			);
		},
		description:
			"Publish a Design-node bundle to the Project Deployer; --published-project-key forwards the documented publishedProjectKey query parameter (a new published project is created when no match exists; the source project's key is the server-side default).",
		examples: [
			"dss bundle publish v1",
			"dss bundle publish v1 --published-project-key=PROD-CHURN",
		],
	},
	"list-imported": {
		handler: (c, _a, f,) => c.bundles.listImported(f["project-key"] as string | undefined,),
		description: "List bundles imported into a project on the Automation node.",
		examples: ["dss bundle list-imported",],
	},
	"import-from-archive": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "import-from-archive",),);
			return c.bundles.importFromArchive(a[0], f["project-key"] as string | undefined,);
		},
		description: "Import a server-side bundle archive into an Automation-node project.",
		examples: ["dss bundle import-from-archive /data/bundles/v1.zip",],
	},
	"import-from-stream": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "import-from-stream",),);
			await c.bundles.importFromStream(a[0], f["project-key"] as string | undefined,);
			return { imported: true, };
		},
		description: "Upload and import a local bundle archive into an Automation-node project.",
		examples: ["dss bundle import-from-stream ./v1.zip",],
	},
	activate: {
		handler: (c, a, f,) => {
			const usage = commandUsage("bundle", "activate",);
			requireArgs(a, 1, usage,);
			const rawScenarios = f["scenarios"];
			let scenariosToEnable: Record<string, boolean> | undefined;
			if (rawScenarios !== undefined && rawScenarios !== false) {
				const parsed = json(rawScenarios,);
				if (
					typeof parsed !== "object"
					|| parsed === null
					|| Array.isArray(parsed,)
					|| !Object.values(parsed,).every((value,) => typeof value === "boolean")
				) {
					throw new UsageError(
						`--scenarios must be a JSON object mapping scenario IDs to true|false. Usage: ${
							commandUsage("bundle", "activate",)
						}`,
					);
				}
				scenariosToEnable = parsed as Record<string, boolean>;
			}
			return c.bundles.activate(
				a[0],
				f["project-key"] as string | undefined,
				scenariosToEnable !== undefined ? { scenariosToEnable, } : {},
			);
		},
		description:
			"Activate an imported Automation-node bundle; --scenarios forwards the documented scenario-ID-to-enabled dict as the scenariosActiveOnActivation body.",
		examples: [
			"dss bundle activate v1",
			'dss bundle activate v1 --scenarios=\'{"daily_build":true,"hourly_retrain":false}\'',
		],
	},
	preload: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "preload",),);
			return c.bundles.preload(a[0], f["project-key"] as string | undefined,);
		},
		description: "Preload an imported Automation-node bundle (stage data/models).",
		examples: ["dss bundle preload v1",],
	},
	"delete-imported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("bundle", "delete-imported",),);
			await c.bundles.deleteImported(a[0], f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		description: "Delete an imported Automation-node bundle.",
		examples: ["dss bundle delete-imported v1",],
	},
},);
