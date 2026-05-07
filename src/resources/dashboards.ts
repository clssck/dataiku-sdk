import type { DashboardDetails, DashboardSummary, } from "../schemas.js";
import { DashboardDetailsSchema, DashboardSummaryArraySchema, } from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface DashboardCreateOptions {
	name: string;
	settings?: Record<string, unknown>;
	projectKey?: string;
}

export interface DashboardUpdateOptions {
	name?: string;
	data?: Record<string, unknown>;
	projectKey?: string;
}

export class DashboardsResource extends BaseResource {
	async list(projectKey?: string,): Promise<DashboardSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/dashboards/`,
		);
		return this.client.safeParse(DashboardSummaryArraySchema, raw, "dashboards.list",);
	}

	async get(dashboardId: string, projectKey?: string,): Promise<DashboardDetails> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/dashboards/${encodeURIComponent(dashboardId,)}/`,
		);
		return this.client.safeParse(DashboardDetailsSchema, raw, "dashboards.get",);
	}

	async create(opts: DashboardCreateOptions,): Promise<DashboardDetails> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const body = { ...(opts.settings ?? { pages: [], }), name: opts.name, };
		const created = await this.client.post<{ id: string; }>(
			`/public/api/projects/${encodeURIComponent(pk,)}/dashboards/`,
			body,
		);
		return this.get(created.id, pk,);
	}

	async update(dashboardId: string, opts: DashboardUpdateOptions,): Promise<DashboardDetails> {
		const current = await this.get(dashboardId, opts.projectKey,);
		const next = deepMerge(
			current as unknown as Record<string, unknown>,
			opts.data ?? {},
		) as DashboardDetails;
		if (opts.name !== undefined) next.name = opts.name;
		const raw = await this.client.put<unknown>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/dashboards/${
				encodeURIComponent(dashboardId,)
			}/`,
			next,
		);
		return this.client.safeParse(DashboardDetailsSchema, raw, "dashboards.update",);
	}

	async delete(dashboardId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/dashboards/${encodeURIComponent(dashboardId,)}/`,
		);
	}
}
