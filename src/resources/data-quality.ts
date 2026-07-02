import type {
	DataQualityComputeResult,
	DataQualityProjectStatus,
	DataQualityRule,
	DataQualityRuleResult,
	DataQualityRules,
	DataQualityStatus,
	DataQualityStatusByPartition,
	DataQualityTimeline,
	FutureWaitResult,
} from "../schemas.js";
import {
	DataQualityComputeResultSchema,
	DataQualityProjectStatusSchema,
	DataQualityRuleArraySchema,
	DataQualityRuleResultArraySchema,
	DataQualityRuleSchema,
	DataQualityRulesSchema,
	DataQualityStatusByPartitionSchema,
	DataQualityStatusSchema,
	DataQualityTimelineSchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface DataQualityProjectOptions {
	projectKey?: string;
}

export interface DataQualityPartitionOptions extends DataQualityProjectOptions {
	partition?: string;
}

export interface DataQualityStatusByPartitionOptions extends DataQualityProjectOptions {
	includeAllPartitions?: boolean;
}

export interface DataQualityHistoryOptions extends DataQualityPartitionOptions {
	minTimestamp?: number;
	maxTimestamp?: number;
	resultsPerPage?: number;
	page?: number;
	ruleId?: string;
}

export interface DataQualityRuleCreateOptions extends DataQualityProjectOptions {
	config: Record<string, unknown>;
}

export interface DataQualityRuleUpdateOptions extends DataQualityProjectOptions {
	data: Record<string, unknown>;
}

export interface DataQualityComputeOptions extends DataQualityPartitionOptions {
	ruleId?: string;
}

export interface DataQualityComputeAndWaitOptions extends DataQualityComputeOptions {
	pollIntervalMs?: number;
	timeoutMs?: number;
}

export interface DataQualityProjectStatusOptions extends DataQualityProjectOptions {
	onlyMonitored?: boolean;
}

export interface DataQualityProjectTimelineOptions extends DataQualityProjectOptions {
	minTimestamp?: number;
	maxTimestamp?: number;
}

function datasetPath(projectKey: string, datasetName: string,): string {
	return `/public/api/projects/${encodeURIComponent(projectKey,)}/datasets/${
		encodeURIComponent(datasetName,)
	}/data-quality`;
}

function addQuery(
	params: URLSearchParams,
	name: string,
	value: string | number | boolean | undefined,
): void {
	if (value !== undefined) params.set(name, String(value,),);
}

function queryString(params: URLSearchParams,): string {
	const raw = params.toString();
	return raw ? `?${raw}` : "";
}

function partitionValue(partition: string | undefined,): string {
	return partition && partition.trim().length > 0 ? partition : "NP";
}

function projectPath(projectKey: string,): string {
	return `/public/api/projects/${encodeURIComponent(projectKey,)}/data-quality`;
}

export class DataQualityResource extends BaseResource {
	async rules(datasetName: string, projectKey?: string,): Promise<DataQualityRules> {
		const pk = this.resolveProjectKey(projectKey,);
		const raw = await this.client.get<unknown>(`${datasetPath(pk, datasetName,)}/rules`,);
		return this.client.safeParse(DataQualityRulesSchema, raw, "dataQuality.rules",);
	}

	async listRules(datasetName: string, projectKey?: string,): Promise<DataQualityRule[]> {
		const rules = await this.rules(datasetName, projectKey,);
		return this.client.safeParse(DataQualityRuleArraySchema, rules.checks, "dataQuality.listRules",);
	}

	async createRule(
		datasetName: string,
		opts: DataQualityRuleCreateOptions,
	): Promise<DataQualityRule> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const raw = await this.client.post<unknown>(
			`${datasetPath(pk, datasetName,)}/rules`,
			opts.config,
		);
		return this.client.safeParse(DataQualityRuleSchema, raw, "dataQuality.createRule",);
	}

	async updateRule(
		datasetName: string,
		ruleId: string,
		opts: DataQualityRuleUpdateOptions,
	): Promise<DataQualityRule> {
		const current = await this.getRule(datasetName, ruleId, opts.projectKey,);
		const next = deepMerge(
			current as unknown as Record<string, unknown>,
			opts.data,
		) as DataQualityRule;
		const pk = this.resolveProjectKey(opts.projectKey,);
		await this.client.putVoid(
			`${datasetPath(pk, datasetName,)}/rules/${encodeURIComponent(ruleId,)}`,
			next,
		);
		return this.client.safeParse(DataQualityRuleSchema, next, "dataQuality.updateRule",);
	}

	async deleteRule(datasetName: string, ruleId: string, projectKey?: string,): Promise<void> {
		const pk = this.resolveProjectKey(projectKey,);
		const params = new URLSearchParams();
		params.set("ruleId", ruleId,);
		await this.client.del(
			`${datasetPath(pk, datasetName,)}/rules/${encodeURIComponent(ruleId,)}${queryString(params,)}`,
		);
	}

	async getRule(
		datasetName: string,
		ruleId: string,
		projectKey?: string,
	): Promise<DataQualityRule> {
		const rules = await this.listRules(datasetName, projectKey,);
		const rule = rules.find((candidate,) => candidate.id === ruleId);
		if (!rule) throw new Error(`Data quality rule not found: ${ruleId}`,);
		return rule;
	}

	async status(datasetName: string, projectKey?: string,): Promise<DataQualityStatus> {
		const pk = this.resolveProjectKey(projectKey,);
		const raw = await this.client.get<unknown>(
			`${datasetPath(pk, datasetName,)}/status-by-partition`,
		);
		return this.client.safeParse(DataQualityStatusSchema, raw, "dataQuality.status",);
	}

	async statusByPartition(
		datasetName: string,
		opts: DataQualityStatusByPartitionOptions = {},
	): Promise<DataQualityStatusByPartition> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "includeAllPartitions", opts.includeAllPartitions,);
		const raw = await this.client.get<unknown>(
			`${datasetPath(pk, datasetName,)}/status-by-partition${queryString(params,)}`,
		);
		return this.client.safeParse(
			DataQualityStatusByPartitionSchema,
			raw,
			"dataQuality.statusByPartition",
		);
	}

	async lastResults(
		datasetName: string,
		opts: DataQualityHistoryOptions = {},
	): Promise<DataQualityRuleResult[]> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "partition", partitionValue(opts.partition,),);
		addQuery(params, "ruleId", opts.ruleId,);
		const raw = await this.client.get<unknown>(
			`${datasetPath(pk, datasetName,)}/last-rules-result${queryString(params,)}`,
		);
		return this.client.safeParse(DataQualityRuleResultArraySchema, raw, "dataQuality.lastResults",);
	}

	async history(
		datasetName: string,
		opts: DataQualityHistoryOptions = {},
	): Promise<DataQualityRuleResult[]> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "minTimestamp", opts.minTimestamp,);
		addQuery(params, "maxTimestamp", opts.maxTimestamp,);
		addQuery(params, "resultsPerPage", opts.resultsPerPage,);
		addQuery(params, "page", opts.page,);
		addQuery(params, "ruleId", opts.ruleId,);
		const raw = await this.client.get<unknown>(
			`${datasetPath(pk, datasetName,)}/rules-history${queryString(params,)}`,
		);
		return this.client.safeParse(DataQualityRuleResultArraySchema, raw, "dataQuality.history",);
	}

	async computeRules(
		datasetName: string,
		opts: DataQualityComputeOptions = {},
	): Promise<DataQualityComputeResult> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "partition", partitionValue(opts.partition,),);
		addQuery(params, "ruleId", opts.ruleId,);
		const raw = await this.client.post<unknown>(
			`${datasetPath(pk, datasetName,)}/actions/compute-rules${queryString(params,)}`,
		);
		return this.client.safeParse(DataQualityComputeResultSchema, raw, "dataQuality.computeRules",);
	}

	async computeRulesAndWait(
		datasetName: string,
		opts: DataQualityComputeAndWaitOptions = {},
	): Promise<FutureWaitResult> {
		const future = await this.computeRules(datasetName, opts,);
		if (!future.jobId) throw new Error("Data quality compute did not return a future jobId.",);
		return this.client.futures.wait(future.jobId, {
			pollIntervalMs: opts.pollIntervalMs,
			timeoutMs: opts.timeoutMs,
		},);
	}

	async projectStatus(
		opts: DataQualityProjectStatusOptions = {},
	): Promise<DataQualityProjectStatus> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "onlyMonitored", opts.onlyMonitored,);
		const raw = await this.client.get<unknown>(`${projectPath(pk,)}/status${queryString(params,)}`,);
		return this.client.safeParse(DataQualityProjectStatusSchema, raw, "dataQuality.projectStatus",);
	}

	async projectTimeline(
		opts: DataQualityProjectTimelineOptions = {},
	): Promise<DataQualityTimeline> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		addQuery(params, "minTimestamp", opts.minTimestamp,);
		addQuery(params, "maxTimestamp", opts.maxTimestamp,);
		const raw = await this.client.get<unknown>(
			`${projectPath(pk,)}/timeline${queryString(params,)}`,
		);
		return this.client.safeParse(DataQualityTimelineSchema, raw, "dataQuality.projectTimeline",);
	}
}
