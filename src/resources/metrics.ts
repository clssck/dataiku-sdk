import { BaseResource, } from "./base.js";

export interface MetricDescriptor {
	id: string;
	[key: string]: unknown;
}

export interface MetricLastValue {
	partition?: string;
	value?: string | number | boolean | null;
	dataType?: string;
	[key: string]: unknown;
}

export interface MetricWithLastValues {
	metric: MetricDescriptor;
	lastValues: MetricLastValue[];
	[key: string]: unknown;
}

export interface MetricValues {
	metrics: MetricWithLastValues[];
	[key: string]: unknown;
}

export interface MetricHistoryValue {
	time?: number;
	value?: string | number | boolean | null;
	dataType?: string;
	[key: string]: unknown;
}

export interface MetricHistory {
	metricId?: string;
	values: MetricHistoryValue[];
	[key: string]: unknown;
}

export interface MetricComputationReport {
	[key: string]: unknown;
}

function datasetPath(encodedProjectKey: string, datasetName: string,): string {
	return `/public/api/projects/${encodedProjectKey}/datasets/${encodeURIComponent(datasetName,)}`;
}

function folderPath(encodedProjectKey: string, folderId: string,): string {
	return `/public/api/projects/${encodedProjectKey}/managedfolders/${encodeURIComponent(folderId,)}`;
}

export class MetricsResource extends BaseResource {
	/** Get the last metric values for a dataset's global partition. */
	async getDatasetMetrics(datasetName: string, projectKey?: string,): Promise<MetricValues> {
		const pk = this.enc(projectKey,);
		return this.client.get<MetricValues>(`${datasetPath(pk, datasetName,)}/metrics/last/NP`,);
	}

	/** Compute the dataset metrics configured in DSS for the default partition. */
	async computeDatasetMetrics(
		datasetName: string,
		projectKey?: string,
	): Promise<MetricComputationReport> {
		const pk = this.enc(projectKey,);
		return this.client.post<MetricComputationReport>(
			`${datasetPath(pk, datasetName,)}/actions/computeMetrics?partition=`,
		);
	}

	/** Get the last metric values for a managed folder. */
	async getFolderMetrics(folderId: string, projectKey?: string,): Promise<MetricValues> {
		const pk = this.enc(projectKey,);
		return this.client.get<MetricValues>(`${folderPath(pk, folderId,)}/metrics/last`,);
	}

	/** Get the metric history for a dataset's global partition. */
	async getDatasetMetricHistory(
		datasetName: string,
		metricId: string,
		projectKey?: string,
	): Promise<MetricHistory> {
		const pk = this.enc(projectKey,);
		const params = new URLSearchParams();
		params.set("metricLookup", metricId,);
		return this.client.get<MetricHistory>(
			`${datasetPath(pk, datasetName,)}/metrics/history/NP?${params.toString()}`,
		);
	}
}
