import { json, parseBooleanOption, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const bundleCommands: Record<string, CommandMeta> = {
	"list-exported": {
		handler: (c, _a, f,) => c.bundles.listExported(f["project-key"] as string | undefined,),
		usage: "dss bundle list-exported [--project-key KEY]",
		description: "List bundles exported from a project on the Design node.",
		examples: ["dss bundle list-exported",],
	},
	export: {
		handler: async (c, a, f,) => {
			const usage =
				"dss bundle export <bundleId> [--release-notes TEXT] [--evaluate-standards-checks true|false] [--project-key KEY]";
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
		usage:
			"dss bundle export <bundleId> [--release-notes TEXT] [--evaluate-standards-checks true|false] [--project-key KEY]",
		description:
			"Create (or overwrite) an exported Design-node bundle. --release-notes forwards the documented releaseNotes parameter; --evaluate-standards-checks toggles the documented Project Standards Checks evaluation (default true).",
		examples: [
			"dss bundle export v1",
			"dss bundle export v1 --release-notes='Adds churn model' --evaluate-standards-checks=false",
		],
	},
	"delete-exported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss bundle delete-exported <bundleId> [--project-key KEY]",);
			await c.bundles.deleteExported(a[0], f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		usage: "dss bundle delete-exported <bundleId> [--project-key KEY]",
		description: "Delete an exported Design-node bundle.",
		examples: ["dss bundle delete-exported v1",],
	},
	"download-exported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss bundle download-exported <bundleId> --output PATH [--project-key KEY]",);
			const out = f["output"] as string | undefined;
			if (!out) throw new UsageError("--output PATH is required.", "missing_required_flag",);
			const res = await c.bundles.downloadExportedArchive(
				a[0],
				f["project-key"] as string | undefined,
			);
			if (!res.body) {
				throw new Error("bundles.downloadExportedArchive response did not include a body",);
			}
			const bytes = await Bun.write(out, new Response(res.body,), { createPath: false, },);
			return { path: out, bytes, };
		},
		usage: "dss bundle download-exported <bundleId> --output PATH [--project-key KEY]",
		description: "Download an exported Design-node bundle archive to a local file.",
		examples: ["dss bundle download-exported v1 --output ./bundle.zip",],
	},
	publish: {
		handler: (c, a, f,) => {
			const usage = "dss bundle publish <bundleId> [--published-project-key KEY] [--project-key KEY]";
			requireArgs(a, 1, usage,);
			return c.bundles.publish(
				a[0],
				f["project-key"] as string | undefined,
				typeof f["published-project-key"] === "string"
					? { publishedProjectKey: f["published-project-key"], }
					: {},
			);
		},
		usage: "dss bundle publish <bundleId> [--published-project-key KEY] [--project-key KEY]",
		description:
			"Publish a Design-node bundle to the Project Deployer; --published-project-key forwards the documented publishedProjectKey query parameter (a new published project is created when no match exists; the source project's key is the server-side default).",
		examples: [
			"dss bundle publish v1",
			"dss bundle publish v1 --published-project-key=PROD-CHURN",
		],
	},
	"list-imported": {
		handler: (c, _a, f,) => c.bundles.listImported(f["project-key"] as string | undefined,),
		usage: "dss bundle list-imported [--project-key KEY]",
		description: "List bundles imported into a project on the Automation node.",
		examples: ["dss bundle list-imported",],
	},
	"import-from-archive": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss bundle import-from-archive <serverArchivePath> [--project-key KEY]",);
			return c.bundles.importFromArchive(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss bundle import-from-archive <serverArchivePath> [--project-key KEY]",
		description: "Import a server-side bundle archive into an Automation-node project.",
		examples: ["dss bundle import-from-archive /data/bundles/v1.zip",],
	},
	"import-from-stream": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss bundle import-from-stream <filePath> [--project-key KEY]",);
			await c.bundles.importFromStream(a[0], f["project-key"] as string | undefined,);
			return { imported: true, };
		},
		usage: "dss bundle import-from-stream <filePath> [--project-key KEY]",
		description: "Upload and import a local bundle archive into an Automation-node project.",
		examples: ["dss bundle import-from-stream ./v1.zip",],
	},
	activate: {
		handler: (c, a, f,) => {
			const usage = "dss bundle activate <bundleId> [--scenarios JSON] [--project-key KEY]";
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
						"--scenarios must be a JSON object mapping scenario IDs to true|false. Usage: dss bundle activate <bundleId> --scenarios '{\"daily_build\":true}'",
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
		usage: "dss bundle activate <bundleId> [--scenarios JSON] [--project-key KEY]",
		description:
			"Activate an imported Automation-node bundle; --scenarios forwards the documented scenario-ID-to-enabled dict as the scenariosActiveOnActivation body.",
		examples: [
			"dss bundle activate v1",
			'dss bundle activate v1 --scenarios=\'{"daily_build":true,"hourly_retrain":false}\'',
		],
	},
	preload: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss bundle preload <bundleId> [--project-key KEY]",);
			return c.bundles.preload(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss bundle preload <bundleId> [--project-key KEY]",
		description: "Preload an imported Automation-node bundle (stage data/models).",
		examples: ["dss bundle preload v1",],
	},
	"delete-imported": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss bundle delete-imported <bundleId> [--project-key KEY]",);
			await c.bundles.deleteImported(a[0], f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		usage: "dss bundle delete-imported <bundleId> [--project-key KEY]",
		description: "Delete an imported Automation-node bundle.",
		examples: ["dss bundle delete-imported v1",],
	},
};
