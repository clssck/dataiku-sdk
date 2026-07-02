import { requiredJsonInput, requiredStringFlag, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, requireNoArgs, UsageError, } from "../usage.js";

export const appCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.applications.listApps(),
		usage: "dss app list",
		description: "List all Dataiku App templates.",
		examples: ["dss app list",],
	},
	manifest: {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss app manifest <appId>",
			);
			const projectKey = f["project-key"] as string | undefined;
			switch (a[0]) {
				case "get":
					requireNoArgs(a.slice(1,), "dss app manifest get [--project-key KEY]",);
					return c.applications.getInstanceManifest(projectKey,);
				case "update": {
					requireNoArgs(
						a.slice(1,),
						"dss app manifest update (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
					);
					const patch = requiredJsonInput(
						f,
						"--data, --data-file, or --stdin is required (manifest patch JSON).",
					);
					return c.applications.updateInstanceManifest(patch, projectKey,);
				}
				case "export-resource": {
					requireNoArgs(
						a.slice(1,),
						"dss app manifest export-resource --managed-folder FOLDER [--project-key KEY]",
					);
					const folder = requiredStringFlag(
						f,
						"managed-folder",
						"dss app manifest export-resource --managed-folder FOLDER [--project-key KEY]",
					);
					return c.applications.exportManagedFolderResource(folder, projectKey,);
				}
				default:
					requireNoArgs(a.slice(1,), "dss app manifest <appId>",);
					if (
						f["data"] !== undefined || f["data-file"] !== undefined || f["stdin"] === true
						|| f["managed-folder"] !== undefined
					) {
						throw new UsageError(
							"app manifest <appId> does not accept manifest mutation flags. Use `dss app manifest update` or `dss app manifest export-resource`.",
							"usage_error",
						);
					}
					return c.applications.getAppManifest(a[0],);
			}
		},
		usage: "dss app manifest <appId>",
		description: "Get the manifest of a Dataiku App template. Nested manifest subcommands remain supported for compatibility; registry-safe aliases are manifest-get, manifest-update, and manifest-export-resource.",
		examples: ["dss app manifest my-app",],
		optionalFlags: ["managed-folder", "data", "data-file", "stdin",],
	},
	"manifest-get": {
		handler: (c, a, f,) => {
			requireNoArgs(a, "dss app manifest-get [--project-key KEY]",);
			return c.applications.getInstanceManifest(f["project-key"] as string | undefined,);
		},
		usage: "dss app manifest-get [--project-key KEY]",
		description: "Registry-safe alias for `dss app manifest get`.",
		examples: ["dss app manifest-get --project-key MYAPP_TEMPLATE",],
	},
	"manifest-update": {
		handler: (c, a, f,) => {
			requireNoArgs(
				a,
				"dss app manifest-update (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const patch = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (manifest patch JSON).",
			);
			return c.applications.updateInstanceManifest(patch, f["project-key"] as string | undefined,);
		},
		usage: "dss app manifest-update (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Registry-safe alias for `dss app manifest update`.",
		examples: ["dss app manifest-update --data-file manifest.patch.json --project-key MYAPP_TEMPLATE",],
	},
	"manifest-export-resource": {
		handler: (c, a, f,) => {
			requireNoArgs(
				a,
				"dss app manifest-export-resource --managed-folder FOLDER [--project-key KEY]",
			);
			const folder = requiredStringFlag(
				f,
				"managed-folder",
				"dss app manifest-export-resource --managed-folder FOLDER [--project-key KEY]",
			);
			return c.applications.exportManagedFolderResource(folder, f["project-key"] as string | undefined,);
		},
		usage: "dss app manifest-export-resource --managed-folder FOLDER [--project-key KEY]",
		description: "Registry-safe alias for `dss app manifest export-resource`.",
		examples: ["dss app manifest-export-resource --managed-folder output --project-key MYAPP_TEMPLATE",],
	},
	homepage: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss app homepage <add-project-variable-tile|add-scenario-tile|add-managed-folder-tile>",);
			const projectKey = f["project-key"] as string | undefined;
			switch (a[0]) {
				case "add-project-variable-tile":
					requireNoArgs(
						a.slice(1,),
						"dss app homepage add-project-variable-tile --variable NAME --label LABEL --button-text TEXT [--project-key KEY]",
					);
					return c.applications.addProjectVariableHomepageTile(
						requiredStringFlag(
							f,
							"variable",
							"dss app homepage add-project-variable-tile --variable NAME --label LABEL --button-text TEXT [--project-key KEY]",
						),
						requiredStringFlag(
							f,
							"label",
							"dss app homepage add-project-variable-tile --variable NAME --label LABEL --button-text TEXT [--project-key KEY]",
						),
						requiredStringFlag(
							f,
							"button-text",
							"dss app homepage add-project-variable-tile --variable NAME --label LABEL --button-text TEXT [--project-key KEY]",
						),
						projectKey,
					);
				case "add-scenario-tile":
					requireNoArgs(
						a.slice(1,),
						"dss app homepage add-scenario-tile --scenario ID --button-text TEXT [--project-key KEY]",
					);
					return c.applications.addScenarioHomepageTile(
						requiredStringFlag(
							f,
							"scenario",
							"dss app homepage add-scenario-tile --scenario ID --button-text TEXT [--project-key KEY]",
						),
						requiredStringFlag(
							f,
							"button-text",
							"dss app homepage add-scenario-tile --scenario ID --button-text TEXT [--project-key KEY]",
						),
						projectKey,
					);
				case "add-managed-folder-tile":
					requireNoArgs(
						a.slice(1,),
						"dss app homepage add-managed-folder-tile --folder FOLDER --prompt TEXT [--project-key KEY]",
					);
					const folder = typeof f["folder"] === "string" && f["folder"].trim().length > 0
						? f["folder"].trim()
						: typeof f["managed-folder"] === "string" && f["managed-folder"].trim().length > 0
						? f["managed-folder"].trim()
						: undefined;
					if (!folder) {
						throw new UsageError(
							"--folder is required. Usage: dss app homepage add-managed-folder-tile --folder FOLDER --prompt TEXT [--project-key KEY]",
							"missing_required_flag",
						);
					}
					return c.applications.addManagedFolderHomepageTile(
						folder,
						requiredStringFlag(
							f,
							"prompt",
							"dss app homepage add-managed-folder-tile --folder FOLDER --prompt TEXT [--project-key KEY]",
						),
						projectKey,
					);
				default:
					throw new UsageError(
						`Unknown app homepage helper: ${a[0]}. Use add-project-variable-tile, add-scenario-tile, or add-managed-folder-tile.`,
						"invalid_enum",
					);
			}
		},
		usage:
			"dss app homepage add-project-variable-tile|add-scenario-tile|add-managed-folder-tile [--variable NAME] [--label LABEL] [--button-text TEXT] [--scenario ID] [--folder FOLDER] [--managed-folder FOLDER] [--prompt TEXT] [--project-key KEY]",
		description:
			"Add source-backed app homepage tiles or fail clearly when a raw homepageSections tile schema is unavailable.",
		examples: [
			'dss app homepage add-project-variable-tile --variable gosilico_workbook_id --label "Workbook ID" --button-text "Workbook ID" --project-key GOSILICO',
			'dss app homepage add-scenario-tile --scenario GENERATE_GOSILICO_INPUT --button-text "Generate" --project-key GOSILICO',
			'dss app homepage add-managed-folder-tile --folder output --prompt "Download generated workbook" --project-key GOSILICO',
		],
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
		description: "Get the app manifest of a Dataiku App template or app-instance project.",
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
			"Save the app manifest of a Dataiku App template project (homepage sections, use-as-recipe settings). Classic app-instance project manifests are read-only through this endpoint.",
		examples: [
			"dss app save-instance-manifest --data-file manifest.json --project-key MYAPP_TEMPLATE",
		],
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
