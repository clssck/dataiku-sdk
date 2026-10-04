import { BaseResource, requireArrayResponse, requireNonEmpty, requireObject, } from "./base.js";
import type { MlTaskListItem, } from "./ml-tasks.js";

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

	/**
	 * Replace a visual analysis definition (script, charts, name, tags): the
	 * object from {@link get}, edited (PUT /lab/{analysisId}/).
	 */
	async update(
		analysisId: string,
		definition: AnalysisDefinition,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		return this.client.put<Record<string, unknown>>(
			`${this.analysisPath(analysisId, projectKey,)}/`,
			requireObject(definition, "definition",),
		);
	}

	/** The ML tasks of one visual analysis (GET /lab/{analysisId}/models/). */
	async listMlTasks(analysisId: string, projectKey?: string,): Promise<MlTaskListItem[]> {
		const res = await this.client.get<{ mlTasks?: MlTaskListItem[]; }>(
			`${this.analysisPath(analysisId, projectKey,)}/models/`,
		);
		return requireArrayResponse<MlTaskListItem>(res.mlTasks, "analyses.listMlTasks",);
	}

	private analysisPath(analysisId: string, projectKey?: string,): string {
		const id = encodeURIComponent(requireNonEmpty(analysisId, "analysisId",),);
		return `/public/api/projects/${this.enc(projectKey,)}/lab/${id}`;
	}
}
