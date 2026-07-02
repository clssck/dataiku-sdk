import { UsageError, } from "../cli/usage.js";
import { BaseResource, } from "./base.js";

export type StatisticsCardSettings = Record<string, unknown>;
export type StatisticsComputationSettings = Record<string, unknown>;

export interface StatisticsDatasetSelection {
	partitionSelectionMethod?: string;
	maxRecords?: number;
	samplingMethod?: string;
	[key: string]: unknown;
}

export interface StatisticsWorksheetDataSpec {
	inputDatasetSmartName?: string;
	datasetSelection?: StatisticsDatasetSelection;
	[key: string]: unknown;
}

export interface StatisticsWorksheetCreateRequest {
	projectKey?: string;
	name?: string;
	dataSpec?: StatisticsWorksheetDataSpec;
	rootCard?: StatisticsCardSettings;
	[key: string]: unknown;
}

export interface StatisticsWorksheetUpdateRequest extends StatisticsWorksheetCreateRequest {
	id?: string;
}

export interface StatisticsWorksheet {
	id: string;
	projectKey?: string;
	name?: string;
	dataSpec?: StatisticsWorksheetDataSpec;
	rootCard?: StatisticsCardSettings;
	[key: string]: unknown;
}

export interface StatisticsFutureResponse {
	jobId?: string | null;
	[key: string]: unknown;
}

function isRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

function validateWorksheetCreateRequest(body: StatisticsWorksheetCreateRequest,): void {
	const bodyRecord = isRecord(body,) ? body : undefined;
	const dataSpec = bodyRecord?.dataSpec;
	if (!isRecord(dataSpec,) || !isRecord(dataSpec.datasetSelection,)) {
		throw new UsageError(
			"statistics.createWorksheet requires body.dataSpec.datasetSelection.",
			"validation_failed",
			"Include dataSpec.datasetSelection in the worksheet definition.",
			{ requiredField: "dataSpec.datasetSelection", },
		);
	}
}

export class StatisticsResource extends BaseResource {
	/** List statistics worksheets associated with a dataset. */
	async listWorksheets(datasetName: string, projectKey?: string,): Promise<StatisticsWorksheet[]> {
		return this.client.get<StatisticsWorksheet[]>(
			this.worksheetsPath(datasetName, projectKey,),
		);
	}

	/** Get one statistics worksheet definition. */
	async getWorksheet(
		datasetName: string,
		worksheetId: string,
		projectKey?: string,
	): Promise<StatisticsWorksheet> {
		return this.client.get<StatisticsWorksheet>(
			this.worksheetPath(datasetName, worksheetId, projectKey,),
		);
	}

	/** Create a statistics worksheet for a dataset. */
	async createWorksheet(
		datasetName: string,
		body: StatisticsWorksheetCreateRequest,
		projectKey?: string,
	): Promise<StatisticsWorksheet> {
		validateWorksheetCreateRequest(body,);
		return this.client.post<StatisticsWorksheet>(
			this.worksheetsPath(datasetName, projectKey,),
			body,
		);
	}

	/** Replace a statistics worksheet definition. */
	async updateWorksheet(
		datasetName: string,
		worksheetId: string,
		body: StatisticsWorksheetUpdateRequest,
		projectKey?: string,
	): Promise<StatisticsWorksheet> {
		return this.client.put<StatisticsWorksheet>(
			this.worksheetPath(datasetName, worksheetId, projectKey,),
			body,
		);
	}

	/** Delete a statistics worksheet. */
	async deleteWorksheet(
		datasetName: string,
		worksheetId: string,
		projectKey?: string,
	): Promise<void> {
		await this.client.del(
			this.worksheetPath(datasetName, worksheetId, projectKey,),
		);
	}

	/** Run the root card of a worksheet, returning the DSS future response. */
	async runWorksheet(
		datasetName: string,
		worksheetId: string,
		projectKey?: string,
	): Promise<StatisticsFutureResponse> {
		const worksheet = await this.getWorksheet(datasetName, worksheetId, projectKey,);
		if (!isRecord(worksheet.rootCard,)) {
			throw new Error("statistics.runWorksheet requires worksheet.rootCard",);
		}
		return this.runCard(datasetName, worksheetId, worksheet.rootCard, projectKey,);
	}

	/** Run a card in the context of a worksheet, returning the DSS future response. */
	async runCard(
		datasetName: string,
		worksheetId: string,
		card: StatisticsCardSettings,
		projectKey?: string,
	): Promise<StatisticsFutureResponse> {
		return this.client.post<StatisticsFutureResponse>(
			`${this.worksheetPath(datasetName, worksheetId, projectKey,)}/actions/run-card`,
			card,
		);
	}

	/** Run a computation in the context of a worksheet, returning the DSS future response. */
	async runComputation(
		datasetName: string,
		worksheetId: string,
		computation: StatisticsComputationSettings,
		projectKey?: string,
	): Promise<StatisticsFutureResponse> {
		return this.client.post<StatisticsFutureResponse>(
			`${this.worksheetPath(datasetName, worksheetId, projectKey,)}/actions/run-computation`,
			computation,
		);
	}

	private worksheetsPath(datasetName: string, projectKey?: string,): string {
		const project = this.enc(projectKey,);
		const dataset = encodeURIComponent(datasetName,);
		return `/public/api/projects/${project}/datasets/${dataset}/statistics/worksheets/`;
	}

	private worksheetPath(datasetName: string, worksheetId: string, projectKey?: string,): string {
		const worksheet = encodeURIComponent(worksheetId,);
		return `${this.worksheetsPath(datasetName, projectKey,)}${worksheet}`;
	}
}
