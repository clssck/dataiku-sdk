import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, } from "../usage.js";

export const appCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.applications.listApps(),
		usage: "dss app list",
		description: "List all Dataiku App templates.",
		examples: ["dss app list",],
	},
	manifest: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss app manifest <appId>",);
			return c.applications.getAppManifest(a[0],);
		},
		usage: "dss app manifest <appId>",
		description: "Get the manifest of a Dataiku App template.",
		examples: ["dss app manifest my-app",],
	},
	instances: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss app instances <appId>",);
			return c.applications.listInstances(a[0],);
		},
		usage: "dss app instances <appId>",
		description: "List instances created from a Dataiku App template.",
		examples: ["dss app instances my-app",],
	},
	"create-instance": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss app create-instance <appId> (--data JSON|--data-file PATH|--stdin)",);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (instance creation payload).",
			);
			return c.applications.createInstance(a[0], body,);
		},
		usage: "dss app create-instance <appId> (--data JSON|--data-file PATH|--stdin)",
		description: "Create an app instance from a Dataiku App template.",
		examples: ['dss app create-instance my-app --data \'{"targetProjectKey":"NEWPROJ"}\'',],
	},
	"instance-manifest": {
		handler: (c, a, f,) => {
			requireNoArgs(a, "dss app instance-manifest [--project-key KEY]",);
			return c.applications.getInstanceManifest(f["project-key"] as string | undefined,);
		},
		usage: "dss app instance-manifest [--project-key KEY]",
		description: "Get the app manifest of an app-instance project.",
		examples: ["dss app instance-manifest --project-key MYINSTANCE",],
	},
	"save-instance-manifest": {
		handler: (c, a, f,) => {
			requireNoArgs(
				a,
				"dss app save-instance-manifest (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const manifest = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (manifest JSON).",
			);
			return c.applications.saveInstanceManifest(manifest, f["project-key"] as string | undefined,);
		},
		usage:
			"dss app save-instance-manifest (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description:
			"Save the app manifest of an app-instance project (homepage sections, use-as-recipe settings).",
		examples: ["dss app save-instance-manifest --data-file manifest.json --project-key MYINSTANCE",],
	},
	"delete-instance": {
		handler: async (c, a, f,) => {
			requireNoArgs(a, "dss app delete-instance [--project-key KEY]",);
			await c.applications.deleteInstance(f["project-key"] as string | undefined,);
			return { deleted: true, };
		},
		usage: "dss app delete-instance [--project-key KEY]",
		description: "Delete an app-instance project (destructive: removes the instance project).",
		examples: ["dss app delete-instance --project-key MYINSTANCE",],
	},
	"business-app-instance-permissions": {
		handler: (c, a,) => {
			requireArgs(
				a,
				3,
				"dss app business-app-instance-permissions <businessAppId> <instanceProjectKey> <userLogin>",
			);
			return c.applications.getBusinessAppInstanceUserPermissions(a[0], a[1], a[2],);
		},
		usage:
			"dss app business-app-instance-permissions <businessAppId> <instanceProjectKey> <userLogin>",
		description: "Get a user's effective permissions on a Business App instance.",
		examples: ["dss app business-app-instance-permissions my-bapp INSTANCEPROJ alice",],
	},
};
