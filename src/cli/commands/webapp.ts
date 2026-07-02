import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const webappCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.webapps.list(f["project-key"] as string | undefined,),
		usage: "dss webapp list [--project-key KEY]",
		description: "List webapps in a project.",
		examples: ["dss webapp list",],
	},
	"get-settings": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss webapp get-settings <webappId> [--project-key KEY]",);
			return c.webapps.getSettings(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss webapp get-settings <webappId> [--project-key KEY]",
		description: "Get a webapp's settings.",
		examples: ["dss webapp get-settings WEBAPP_ID",],
	},
	create: {
		handler: (c, _a, f,) => {
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (webapp definition).",
			);
			return c.webapps.create(body, f["project-key"] as string | undefined,);
		},
		usage: "dss webapp create (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Create a webapp from a JSON definition.",
		examples: ["dss webapp create --data-file webapp.json",],
	},
	"update-settings": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss webapp update-settings <webappId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (webapp settings).",
			);
			return c.webapps.updateSettings(a[0], body, f["project-key"] as string | undefined,);
		},
		usage:
			"dss webapp update-settings <webappId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a webapp's settings.",
		examples: ["dss webapp update-settings WEBAPP_ID --data-file webapp.json",],
	},
	"stop-backend": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss webapp stop-backend <webappId> [--project-key KEY]",);
			await c.webapps.stopBackend(a[0], f["project-key"] as string | undefined,);
			return { stopped: true, };
		},
		usage: "dss webapp stop-backend <webappId> [--project-key KEY]",
		description: "Stop a webapp's backend.",
		examples: ["dss webapp stop-backend WEBAPP_ID",],
	},
	"restart-backend": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss webapp restart-backend <webappId> [--project-key KEY]",);
			await c.webapps.startOrRestartBackend(a[0], f["project-key"] as string | undefined,);
			return { restarted: true, };
		},
		usage: "dss webapp restart-backend <webappId> [--project-key KEY]",
		description: "Start or restart a webapp's backend.",
		examples: ["dss webapp restart-backend WEBAPP_ID",],
	},
	"backend-state": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss webapp backend-state <webappId> [--project-key KEY]",);
			return c.webapps.getBackendState(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss webapp backend-state <webappId> [--project-key KEY]",
		description: "Get a webapp backend's runtime state.",
		examples: ["dss webapp backend-state WEBAPP_ID",],
	},
};
