import { BaseResource, } from "./base.js";

export interface ContinuousActivityListItem extends Record<string, unknown> {
	projectKey: string;
	recipeId: string;
}

export interface ContinuousActivityLoopParams extends Record<string, unknown> {
	abortAfterCrashes?: number;
	initialRestartDelayMS?: number;
	restartDelayIncMS?: number;
	maxRestartDelayMS?: number;
}

export interface ContinuousActivityStatus extends Record<string, unknown> {
	desiredState?: string;
	mainLoopState?: Record<string, unknown> | null;
}

export type ContinuousActivityStartResult = Record<string, unknown>;

export class ContinuousActivitiesResource extends BaseResource {
	/** List continuous activities in a project, including their definitions and states. */
	async list(projectKey?: string,): Promise<ContinuousActivityListItem[]> {
		return this.client.get<ContinuousActivityListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/continuous-activities/`,
		);
	}

	/** Get the current status of a continuous recipe activity. */
	async getStatus(recipeId: string, projectKey?: string,): Promise<ContinuousActivityStatus> {
		return this.client.get<ContinuousActivityStatus>(
			`${this.activityPath(recipeId, projectKey,)}/`,
		);
	}

	/** Start a continuous recipe activity. */
	async start(
		recipeId: string,
		loop?: ContinuousActivityLoopParams,
		projectKey?: string,
	): Promise<ContinuousActivityStartResult> {
		return this.client.post<ContinuousActivityStartResult>(
			`${this.activityPath(recipeId, projectKey,)}/start`,
			loop ?? {},
		);
	}

	/** Stop a continuous recipe activity. */
	async stop(recipeId: string, projectKey?: string,): Promise<void> {
		await this.getStatus(recipeId, projectKey,);
		await this.client.postText(`${this.activityPath(recipeId, projectKey,)}/stop`,);
	}

	private activityPath(recipeId: string, projectKey?: string,): string {
		return `/public/api/projects/${this.enc(projectKey,)}/continuous-activities/${
			encodeURIComponent(recipeId,)
		}`;
	}
}
