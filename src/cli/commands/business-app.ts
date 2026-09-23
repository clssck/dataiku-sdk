import { requiredJsonInput, } from "../coerce.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const businessAppCommands: Record<string, CommandMeta> = withUsage("business-app", {
	list: {
		handler: (c,) => c.applications.listBusinessApps(),
		description: "List all Business Apps.",
		examples: ["dss business-app list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("business-app", "get",),);
			return c.applications.getBusinessApp(a[0],);
		},
		description: "Get Business App details.",
		examples: ["dss business-app get my-bapp",],
	},
	settings: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("business-app", "settings",),);
			return c.applications.getBusinessAppSettings(a[0],);
		},
		description: "Get Business App settings.",
		examples: ["dss business-app settings my-bapp",],
	},
	"save-settings": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("business-app", "save-settings",),
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (settings JSON).",
			);
			return c.applications.saveBusinessAppSettings(a[0], body,);
		},
		description: "Save Business App settings (admin only; includes connection remapping).",
		examples: ["dss business-app save-settings my-bapp --data-file settings.json",],
	},
	instances: {
		handler: (c, a,) => {
			requireArgs(a, 1, commandUsage("business-app", "instances",),);
			return c.applications.listBusinessAppInstances(a[0],);
		},
		description: "List instances of a Business App.",
		examples: ["dss business-app instances my-bapp",],
	},
	"create-instance": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				commandUsage("business-app", "create-instance",),
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (instance payload).",
			);
			return c.applications.createBusinessAppInstance(a[0], body,);
		},
		description: "Create an instance of a Business App.",
		examples: ["dss business-app create-instance my-bapp --data '{}'",],
	},
	"upgrade-instance": {
		handler: (c, a,) => {
			requireArgs(a, 2, commandUsage("business-app", "upgrade-instance",),);
			return c.applications.upgradeBusinessAppInstance(a[0], a[1],);
		},
		description: "Upgrade a Business App instance to the latest version.",
		examples: ["dss business-app upgrade-instance my-bapp INSTANCEPROJ",],
	},
	"install-from-archive": {
		handler: async (c, a,) => {
			requireArgs(a, 1, commandUsage("business-app", "install-from-archive",),);
			await c.applications.installBusinessAppFromArchive(a[0],);
			return { installed: true, };
		},
		description: "Install or upgrade a Business App from a zip archive (admin only).",
		examples: ["dss business-app install-from-archive ./my-bapp.zip",],
	},
},);
