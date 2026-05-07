import type { InsightDetails, InsightSummary, } from "../schemas.js";
import { InsightDetailsSchema, InsightSummaryArraySchema, } from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface InsightCreateOptions {
	name?: string;
	type?: string;
	listed?: boolean;
	params?: Record<string, unknown>;
	data?: Record<string, unknown>;
	contentType?: string;
	payload?: string;
	projectKey?: string;
}

export interface InsightUpdateOptions {
	name?: string;
	listed?: boolean;
	params?: Record<string, unknown>;
	data?: Record<string, unknown>;
	contentType?: string;
	payload?: string;
	projectKey?: string;
}

function hasCreateShape(data: Record<string, unknown>,): boolean {
	return typeof data.name === "string" && typeof data.type === "string";
}

function applyInsightFields(
	base: Record<string, unknown>,
	fields: Pick<InsightCreateOptions, "name" | "type" | "listed" | "params">,
): Record<string, unknown> {
	const next = { ...base, };
	if (fields.name !== undefined) next.name = fields.name;
	if (fields.type !== undefined) next.type = fields.type;
	if (fields.listed !== undefined) next.listed = fields.listed;
	if (fields.params !== undefined) {
		const currentParams = next.params;
		next.params = currentParams && typeof currentParams === "object" && !Array.isArray(currentParams,)
			? deepMerge(currentParams as Record<string, unknown>, fields.params,)
			: fields.params;
	}
	return next;
}

export class InsightsResource extends BaseResource {
	async list(projectKey?: string,): Promise<InsightSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/insights/`,
		);
		return this.client.safeParse(InsightSummaryArraySchema, raw, "insights.list",);
	}

	async get(insightId: string, projectKey?: string,): Promise<InsightDetails> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/insights/${encodeURIComponent(insightId,)}/`,
		);
		return this.client.safeParse(InsightDetailsSchema, raw, "insights.get",);
	}

	async create(opts: InsightCreateOptions,): Promise<InsightDetails> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const prototype = applyInsightFields(opts.data ?? {}, opts,);
		if (!hasCreateShape(prototype,)) {
			throw new Error("Insight create requires name and type, either as options or in data.",);
		}
		if (prototype.projectKey === undefined) prototype.projectKey = pk;
		const created = await this.client.post<{ id: string; }>(
			`/public/api/projects/${encodeURIComponent(pk,)}/insights/`,
			{
				insightPrototype: prototype,
				...(opts.contentType !== undefined ? { contentType: opts.contentType, } : {}),
				...(opts.payload !== undefined ? { payload: opts.payload, } : {}),
			},
		);
		if (typeof created.id !== "string" || created.id.length === 0) {
			throw new Error("Insight create response did not include an id.",);
		}
		return this.get(created.id, pk,);
	}

	async update(insightId: string, opts: InsightUpdateOptions,): Promise<InsightDetails> {
		const current = await this.get(insightId, opts.projectKey,);
		const next = applyInsightFields(
			deepMerge(current as unknown as Record<string, unknown>, opts.data ?? {},),
			opts,
		);
		const pk = this.resolveProjectKey(opts.projectKey,);
		await this.client.post<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/insights/${encodeURIComponent(insightId,)}/`,
			{
				insight: next,
				...(opts.contentType !== undefined ? { contentType: opts.contentType, } : {}),
				...(opts.payload !== undefined ? { payload: opts.payload, } : {}),
			},
		);
		return this.get(insightId, pk,);
	}

	async delete(insightId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/insights/${encodeURIComponent(insightId,)}/`,
		);
	}
}
