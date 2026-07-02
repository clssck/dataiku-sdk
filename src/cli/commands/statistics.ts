import type { DataikuClient, } from "../../client.js";
import { requiredJsonInput, } from "../coerce.js";
import { encodedProjectEndpoint, planResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

const STATISTICS_PLAN_EXIT_CODES = { usage: 1, error: 2, transient: 3, };
const STATISTICS_FUTURE_PLAN_EXIT_CODES = {
	...STATISTICS_PLAN_EXIT_CODES,
	longRunningFailure: 4,
};

interface StatisticsPlanOptions {
	asyncKind?: "none" | "future";
	endpoint: string;
	identifiers: Record<string, unknown>;
	method: string;
	payload?: unknown;
}

function statisticsWorksheetsEndpoint(
	client: DataikuClient,
	datasetName: string,
	projectKey: string | undefined,
): string {
	return encodedProjectEndpoint(
		client,
		projectKey,
		`/datasets/${encodeURIComponent(datasetName,)}/statistics/worksheets/`,
	);
}

function statisticsWorksheetEndpoint(
	client: DataikuClient,
	datasetName: string,
	worksheetId: string,
	projectKey: string | undefined,
): string {
	return `${statisticsWorksheetsEndpoint(client, datasetName, projectKey,)}${
		encodeURIComponent(worksheetId,)
	}`;
}

function statisticsPlan(action: string, options: StatisticsPlanOptions,): Record<string, unknown> {
	const asyncKind = options.asyncKind ?? "none";
	return planResult("statistics", action, {
		method: options.method,
		endpoint: options.endpoint,
		identifiers: options.identifiers,
		...(options.payload !== undefined ? { payload: options.payload, } : {}),
		idempotency: "none",
		asyncKind,
		exitCodesOnFailure: asyncKind === "future"
			? STATISTICS_FUTURE_PLAN_EXIT_CODES
			: STATISTICS_PLAN_EXIT_CODES,
		plannedAndDryRun: true,
	},);
}

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
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss statistics create-worksheet <dataset> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (worksheet definition).",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("create-worksheet", {
					method: "POST",
					endpoint: statisticsWorksheetsEndpoint(c, a[0], projectKey,),
					identifiers: { dataset: a[0], },
					payload: body,
				},);
			}
			return c.statistics.createWorksheet(a[0], body, projectKey,);
		},
		usage:
			"dss statistics create-worksheet <dataset> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Create a statistics worksheet from a JSON definition.",
		examples: [
			'dss statistics create-worksheet customers --data \'{"name":"Customer stats","dataSpec":{"inputDatasetSmartName":"customers","datasetSelection":{"partitionSelectionMethod":"ALL","maxRecords":30000,"samplingMethod":"FULL"}}}\'',
		],
	},
	"update-worksheet": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics update-worksheet <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (worksheet definition).",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("update-worksheet", {
					method: "PUT",
					endpoint: statisticsWorksheetEndpoint(c, a[0], a[1], projectKey,),
					identifiers: { dataset: a[0], worksheetId: a[1], },
					payload: body,
				},);
			}
			return c.statistics.updateWorksheet(a[0], a[1], body, projectKey,);
		},
		usage:
			"dss statistics update-worksheet <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Replace a statistics worksheet definition.",
		examples: ["dss statistics update-worksheet customers ws_1 --data-file ws.json",],
	},
	"delete-worksheet": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics delete-worksheet <dataset> <worksheetId> [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("delete-worksheet", {
					method: "DELETE",
					endpoint: statisticsWorksheetEndpoint(c, a[0], a[1], projectKey,),
					identifiers: { dataset: a[0], worksheetId: a[1], },
				},);
			}
			await c.statistics.deleteWorksheet(a[0], a[1], projectKey,);
			return { deleted: a[1], };
		},
		usage: "dss statistics delete-worksheet <dataset> <worksheetId> [--dry-run] [--project-key KEY]",
		description: "Delete a statistics worksheet.",
		examples: ["dss statistics delete-worksheet customers ws_1",],
	},
	"run-worksheet": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics run-worksheet <dataset> <worksheetId> [--dry-run] [--project-key KEY]",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("run-worksheet", {
					method: "POST",
					endpoint: `${statisticsWorksheetEndpoint(c, a[0], a[1], projectKey,)}/actions/run-card`,
					identifiers: { dataset: a[0], worksheetId: a[1], },
					asyncKind: "future",
				},);
			}
			return c.statistics.runWorksheet(a[0], a[1], projectKey,);
		},
		usage: "dss statistics run-worksheet <dataset> <worksheetId> [--dry-run] [--project-key KEY]",
		description: "Run a statistics worksheet and return the DSS future response.",
		examples: ["dss statistics run-worksheet customers ws_1",],
	},
	"run-card": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics run-card <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
			);
			const card = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (card settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("run-card", {
					method: "POST",
					endpoint: `${statisticsWorksheetEndpoint(c, a[0], a[1], projectKey,)}/actions/run-card`,
					identifiers: { dataset: a[0], worksheetId: a[1], },
					payload: card,
					asyncKind: "future",
				},);
			}
			return c.statistics.runCard(a[0], a[1], card, projectKey,);
		},
		usage:
			"dss statistics run-card <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Run a single card in a worksheet context.",
		examples: ["dss statistics run-card customers ws_1 --data-file card.json",],
	},
	"run-computation": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss statistics run-computation <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
			);
			const computation = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (computation settings).",
			);
			const projectKey = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				return statisticsPlan("run-computation", {
					method: "POST",
					endpoint: `${statisticsWorksheetEndpoint(c, a[0], a[1], projectKey,)}/actions/run-computation`,
					identifiers: { dataset: a[0], worksheetId: a[1], },
					payload: computation,
					asyncKind: "future",
				},);
			}
			return c.statistics.runComputation(
				a[0],
				a[1],
				computation,
				projectKey,
			);
		},
		usage:
			"dss statistics run-computation <dataset> <worksheetId> (--data JSON|--data-file PATH|--stdin) [--dry-run] [--project-key KEY]",
		description: "Run a single computation in a worksheet context.",
		examples: ["dss statistics run-computation customers ws_1 --data-file comp.json",],
	},
};
