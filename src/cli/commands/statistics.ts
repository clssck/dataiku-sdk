import { requiredJsonInput, } from "../coerce.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

export const statisticsCommands: Record<string, CommandMeta> = {
	"list-worksheets": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss statistics list-worksheets <dataset> [--project-key KEY]",);
			return c.statistics.listWorksheets(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss statistics list-worksheets <dataset> [--project-key KEY]",
		description: "List EDA statistics worksheets for a dataset.",
		examples: ["dss statistics list-worksheets customers",],
	},
	"get-worksheet": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, "dss statistics get-worksheet <dataset> <worksheetId> [--project-key KEY]",);
			return c.statistics.getWorksheet(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss statistics get-worksheet <dataset> <worksheetId> [--project-key KEY]",
		description: "Get one statistics worksheet definition.",
		examples: ["dss statistics get-worksheet customers ws_1",],
	},
	"create-worksheet": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss statistics create-worksheet <dataset> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (worksheet definition).",
			);
			return c.statistics.createWorksheet(a[0], body, f["project-key"] as string | undefined,);
		},
		usage:
			"dss statistics create-worksheet <dataset> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Create a statistics worksheet from a JSON definition.",
		examples: ["dss statistics create-worksheet customers --data-file ws.json",],
	},
	"update-worksheet": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics update-worksheet <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (worksheet definition).",
			);
			return c.statistics.updateWorksheet(a[0], a[1], body, f["project-key"] as string | undefined,);
		},
		usage:
			"dss statistics update-worksheet <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Replace a statistics worksheet definition.",
		examples: ["dss statistics update-worksheet customers ws_1 --data-file ws.json",],
	},
	"delete-worksheet": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics delete-worksheet <dataset> <worksheetId> [--project-key KEY]",
			);
			await c.statistics.deleteWorksheet(a[0], a[1], f["project-key"] as string | undefined,);
			return { deleted: a[1], };
		},
		usage: "dss statistics delete-worksheet <dataset> <worksheetId> [--project-key KEY]",
		description: "Delete a statistics worksheet.",
		examples: ["dss statistics delete-worksheet customers ws_1",],
	},
	"run-worksheet": {
		handler: (c, a, f,) => {
			requireArgs(a, 2, "dss statistics run-worksheet <dataset> <worksheetId> [--project-key KEY]",);
			return c.statistics.runWorksheet(a[0], a[1], f["project-key"] as string | undefined,);
		},
		usage: "dss statistics run-worksheet <dataset> <worksheetId> [--project-key KEY]",
		description: "Run a statistics worksheet and return the DSS future response.",
		examples: ["dss statistics run-worksheet customers ws_1",],
	},
	"run-card": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics run-card <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const card = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (card settings).",
			);
			return c.statistics.runCard(a[0], a[1], card, f["project-key"] as string | undefined,);
		},
		usage:
			"dss statistics run-card <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Run a single card in a worksheet context.",
		examples: ["dss statistics run-card customers ws_1 --data-file card.json",],
	},
	"run-computation": {
		handler: (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics run-computation <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
			);
			const computation = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (computation settings).",
			);
			return c.statistics.runComputation(
				a[0],
				a[1],
				computation,
				f["project-key"] as string | undefined,
			);
		},
		usage:
			"dss statistics run-computation <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--project-key KEY]",
		description: "Run a single computation in a worksheet context.",
		examples: ["dss statistics run-computation customers ws_1 --data-file comp.json",],
	},
};
