import { writeFileSync, } from "node:fs";
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
			requireArgs(a, 1, "dss bundle export <bundleId> [--project-key KEY]",);
			await c.bundles.exportBundle(a[0], f["project-key"] as string | undefined,);
			return { exported: a[0], };
		},
		usage: "dss bundle export <bundleId> [--project-key KEY]",
		description: "Create (or overwrite) an exported Design-node bundle.",
		examples: ["dss bundle export v1",],
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
			const buf = Buffer.from(await res.arrayBuffer(),);
			writeFileSync(out, buf,);
			return { path: out, bytes: buf.length, };
		},
		usage: "dss bundle download-exported <bundleId> --output PATH [--project-key KEY]",
		description: "Download an exported Design-node bundle archive to a local file.",
		examples: ["dss bundle download-exported v1 --output ./bundle.zip",],
	},
	publish: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss bundle publish <bundleId> [--project-key KEY]",);
			return c.bundles.publish(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss bundle publish <bundleId> [--project-key KEY]",
		description: "Publish a Design-node bundle to the Project Deployer.",
		examples: ["dss bundle publish v1",],
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
			requireArgs(a, 1, "dss bundle activate <bundleId> [--project-key KEY]",);
			return c.bundles.activate(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss bundle activate <bundleId> [--project-key KEY]",
		description: "Activate an imported Automation-node bundle.",
		examples: ["dss bundle activate v1",],
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
