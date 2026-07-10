import { UsageError, } from "../cli/usage.js";
import { BaseResource, } from "./base.js";

export interface AnalysisListItem extends Record<string, unknown> {
	id?: string;
	analysisId?: string;
	name?: string;
	inputDataset?: string;
}

export interface AnalysisDefinition extends Record<string, unknown> {
	id?: string;
	analysisId?: string;
	name?: string;
	projectKey?: string;
	inputDataset?: string;
	script?: Record<string, unknown>;
}

export interface AnalysisCreateOptions {
	inputDataset: string;
	projectKey?: string;
}

export interface AnalysisCreateResult extends Record<string, unknown> {
	id?: string;
	analysisId?: string;
}

function requireNonEmpty(value: string, name: string,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`${name} must be a non-empty string.`, "validation_failed",);
	}
	return value;
}

export class AnalysesResource extends BaseResource {
	/** List visual analyses in a project. */
	async list(projectKey?: string,): Promise<AnalysisListItem[]> {
		return this.client.get<AnalysisListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/lab/`,
		);
	}

	/** Get the definition of a visual analysis. */
	async get(analysisId: string, projectKey?: string,): Promise<AnalysisDefinition> {
		return this.client.get<AnalysisDefinition>(
			`${this.analysisPath(analysisId, projectKey,)}/`,
		);
	}

	/** Create a visual analysis for an input dataset. */
	async create(opts: AnalysisCreateOptions,): Promise<AnalysisCreateResult> {
		const inputDataset = requireNonEmpty(opts.inputDataset, "inputDataset",);
		return this.client.post<AnalysisCreateResult>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/lab/`,
			{ inputDataset, },
		);
	}

	/** Delete a visual analysis. */
	async delete(analysisId: string, projectKey?: string,): Promise<void> {
		await this.client.del(`${this.analysisPath(analysisId, projectKey,)}/`,);
	}

	private analysisPath(analysisId: string, projectKey?: string,): string {
		const id = encodeURIComponent(requireNonEmpty(analysisId, "analysisId",),);
		return `/public/api/projects/${this.enc(projectKey,)}/lab/${id}`;
	}
}
