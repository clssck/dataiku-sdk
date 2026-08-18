import { ClientValidationError, DataikuError, } from "../errors.js";
import type { DashboardDetails, DashboardSummary, } from "../schemas.js";
import {
	DashboardDetailsSchema,
	DashboardSummaryArraySchema,
	InsightDetailsSchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface DashboardCreateOptions {
	name: string;
	listed?: boolean;
	settings?: Record<string, unknown>;
	projectKey?: string;
}

export interface DashboardUpdateOptions {
	name?: string;
	listed?: boolean;
	data?: Record<string, unknown>;
	projectKey?: string;
}

export type DashboardExportOrientation = "PORTRAIT" | "LANDSCAPE";
export type DashboardExportFileType = "PDF";

export interface DashboardExportOptions {
	paperSize?: string;
	orientation?: DashboardExportOrientation;
	fileType?: DashboardExportFileType;
	slideIndex?: number;
	projectKey?: string;
}

const REFERENCE_VALIDATION_CONCURRENCY = 8;

async function validateInBatches<T,>(
	items: readonly T[],
	validate: (item: T,) => Promise<void>,
): Promise<void> {
	for (let offset = 0; offset < items.length; offset += REFERENCE_VALIDATION_CONCURRENCY) {
		const batch = items.slice(offset, offset + REFERENCE_VALIDATION_CONCURRENCY,);
		const results = await Promise.allSettled(batch.map(validate,),);
		const failure = results.find(
			(result,): result is PromiseRejectedResult => result.status === "rejected",
		);
		if (failure) throw failure.reason;
	}
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

	private referencedInsightIds(settings: Record<string, unknown>,): string[] {
		const pages = settings.pages;
		if (pages === undefined) return [];
		if (!Array.isArray(pages,)) {
			throw new ClientValidationError("Dashboard pages must be an array.",);
		}

		const insightIds = new Set<string>();
		for (const [pageIndex, page,] of pages.entries()) {
			if (!page || typeof page !== "object" || Array.isArray(page,)) {
				throw new ClientValidationError(
					`Dashboard page ${pageIndex} must be an object.`,
				);
			}
			const grid = (page as Record<string, unknown>).grid;
			if (grid === undefined) continue;
			if (!grid || typeof grid !== "object" || Array.isArray(grid,)) {
				throw new ClientValidationError(
					`Dashboard page ${pageIndex} grid must be an object.`,
				);
			}
			const tiles = (grid as Record<string, unknown>).tiles;
			if (tiles === undefined) continue;
			if (!Array.isArray(tiles,)) {
				throw new ClientValidationError(
					`Dashboard page ${pageIndex} grid tiles must be an array.`,
				);
			}

			for (const [tileIndex, tile,] of tiles.entries()) {
				if (!tile || typeof tile !== "object" || Array.isArray(tile,)) {
					throw new ClientValidationError(
						`Dashboard page ${pageIndex} tile ${tileIndex} must be an object.`,
					);
				}
				const record = tile as Record<string, unknown>;
				const insightId = record.insightId ?? undefined;
				const targetInsightId = record.targetInsightId ?? undefined;
				const isInsight = record.tileType === "INSIGHT"
					|| insightId !== undefined
					|| targetInsightId !== undefined;
				if (!isInsight) continue;
				if (
					insightId !== undefined
					&& (typeof insightId !== "string" || insightId.trim().length === 0)
				) {
					throw new ClientValidationError(
						`Dashboard page ${pageIndex} tile ${tileIndex} insightId must be a non-empty string.`,
					);
				}
				if (
					targetInsightId !== undefined
					&& (typeof targetInsightId !== "string" || targetInsightId.trim().length === 0)
				) {
					throw new ClientValidationError(
						`Dashboard page ${pageIndex} tile ${tileIndex} targetInsightId must be a non-empty string.`,
					);
				}
				const normalizedInsightId = typeof insightId === "string" ? insightId.trim() : undefined;
				const normalizedTargetId = typeof targetInsightId === "string"
					? targetInsightId.trim()
					: undefined;
				if (
					normalizedInsightId !== undefined
					&& normalizedTargetId !== undefined
					&& normalizedInsightId !== normalizedTargetId
				) {
					throw new ClientValidationError(
						`Dashboard page ${pageIndex} tile ${tileIndex} has mismatched insightId and targetInsightId.`,
					);
				}
				const reference = normalizedInsightId ?? normalizedTargetId;
				if (!reference) {
					throw new ClientValidationError(
						`Dashboard page ${pageIndex} tile ${tileIndex} requires insightId or targetInsightId.`,
					);
				}
				insightIds.add(reference,);
			}
		}
		return [...insightIds,];
	}

	private async validateReferences(
		settings: Record<string, unknown>,
		projectKey: string,
	): Promise<void> {
		const insightIds = this.referencedInsightIds(settings,);
		const datasetNames = new Set<string>();
		await validateInBatches(insightIds, async (insightId,) => {
			let raw: unknown;
			try {
				raw = await this.client.get<unknown>(
					`/public/api/projects/${encodeURIComponent(projectKey,)}/insights/${
						encodeURIComponent(insightId,)
					}/`,
				);
			} catch (error) {
				if (error instanceof DataikuError && error.status === 404) {
					throw new ClientValidationError(
						`Dashboard references missing insight ${insightId}.`,
						"validation_failed",
						"Create or restore the insight, then update the tile reference before saving the dashboard.",
						{ insightId, },
					);
				}
				throw error;
			}
			const insight = this.client.safeParse(
				InsightDetailsSchema,
				raw,
				`dashboards.validateReferences.insight.${insightId}`,
			);
			const params = raw && typeof raw === "object" && !Array.isArray(raw,)
				? (raw as Record<string, unknown>).params
				: undefined;
			const datasetSmartName = params && typeof params === "object" && !Array.isArray(params,)
				? (params as Record<string, unknown>).datasetSmartName
				: undefined;
			if (
				insight.type === "dataset_table" && (
					typeof datasetSmartName !== "string" || datasetSmartName.trim().length === 0
				)
			) {
				throw new ClientValidationError(
					`Dashboard insight ${insightId} does not name a backing dataset.`,
					"validation_failed",
					"Retarget the dataset-table insight before saving the dashboard.",
					{ insightId, },
				);
			}
			if (typeof datasetSmartName === "string" && datasetSmartName.trim().length > 0) {
				datasetNames.add(datasetSmartName.trim(),);
			}
		},);

		await validateInBatches([...datasetNames,], async (datasetSmartName,) => {
			const separatorIndex = datasetSmartName.indexOf(".",);
			if (separatorIndex === 0 || separatorIndex === datasetSmartName.length - 1) {
				throw new ClientValidationError(
					`Dashboard insight has invalid dataset smart name ${datasetSmartName}.`,
					"validation_failed",
					"Use DATASET for the current project or PROJECT.DATASET for a cross-project dataset.",
					{ datasetSmartName, },
				);
			}
			const datasetProjectKey = separatorIndex > 0
				? datasetSmartName.slice(0, separatorIndex,)
				: projectKey;
			const datasetName = separatorIndex > 0
				? datasetSmartName.slice(separatorIndex + 1,)
				: datasetSmartName;
			try {
				await this.client.get<unknown>(
					`/public/api/projects/${encodeURIComponent(datasetProjectKey,)}/datasets/${
						encodeURIComponent(datasetName,)
					}/`,
				);
			} catch (error) {
				if (error instanceof DataikuError && error.status === 403) {
					throw new ClientValidationError(
						`Dashboard cannot validate inaccessible dataset ${datasetSmartName}.`,
						"validation_failed",
						"Grant the API key read access to the dataset project before saving the dashboard.",
						{ datasetSmartName, datasetProjectKey, datasetName, },
					);
				}
				if (error instanceof DataikuError && error.status === 404) {
					throw new ClientValidationError(
						`Dashboard insight references missing dataset ${datasetSmartName}.`,
						"validation_failed",
						"Create or restore the dataset, or retarget the insight before saving the dashboard.",
						{ datasetSmartName, datasetProjectKey, datasetName, },
					);
				}
				throw error;
			}
		},);
	}

	async create(opts: DashboardCreateOptions,): Promise<DashboardDetails> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const body: Record<string, unknown> = { ...(opts.settings ?? { pages: [], }), name: opts.name, };
		if (opts.listed !== undefined) body.listed = opts.listed;
		await this.validateReferences(body, pk,);
		const created = await this.client.post<{ id: string; }>(
			`/public/api/projects/${encodeURIComponent(pk,)}/dashboards/`,
			body,
		);
		return this.get(created.id, pk,);
	}

	async update(dashboardId: string, opts: DashboardUpdateOptions,): Promise<DashboardDetails> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const current = await this.get(dashboardId, pk,);
		const next = deepMerge(
			current as unknown as Record<string, unknown>,
			opts.data ?? {},
		) as DashboardDetails;
		if (opts.name !== undefined) next.name = opts.name;
		if (opts.listed !== undefined) next.listed = opts.listed;
		await this.validateReferences(next as unknown as Record<string, unknown>, pk,);
		await this.client.put<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/dashboards/${
				encodeURIComponent(dashboardId,)
			}/`,
			next,
		);
		return this.get(dashboardId, pk,);
	}

	async export(dashboardId: string, opts: DashboardExportOptions = {},): Promise<Response> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const paperSize = (opts.paperSize ?? "A4").trim();
		if (!paperSize) throw new ClientValidationError("Dashboard export paperSize must not be empty.",);

		const orientation = (opts.orientation ?? "LANDSCAPE").trim().toUpperCase();
		if (orientation !== "PORTRAIT" && orientation !== "LANDSCAPE") {
			throw new ClientValidationError(
				"Dashboard export orientation must be PORTRAIT or LANDSCAPE.",
			);
		}

		const fileType = (opts.fileType ?? "PDF").trim().toUpperCase();
		if (fileType !== "PDF") {
			throw new ClientValidationError("Dashboard export fileType must be PDF.",);
		}

		const slideIndex = opts.slideIndex ?? 0;
		if (!Number.isInteger(slideIndex,) || slideIndex < 0) {
			throw new ClientValidationError(
				"Dashboard export slideIndex must be a non-negative integer.",
			);
		}

		return this.client.postStream(
			`/public/api/projects/${encodeURIComponent(pk,)}/dashboards/${
				encodeURIComponent(dashboardId,)
			}/action/export`,
			{ paperSize, orientation, fileType, slideIndex, },
		);
	}

	async delete(dashboardId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/dashboards/${encodeURIComponent(dashboardId,)}/`,
		);
	}
}
