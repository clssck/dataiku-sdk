import type { ProjectVariables, } from "../schemas.js";
import { ProjectVariablesSchema, } from "../schemas.js";
import { BaseResource, } from "./base.js";

export class VariablesResource extends BaseResource {
	async get(projectKey?: string,): Promise<ProjectVariables> {
		const enc = this.enc(projectKey,);
		const raw = await this.client.get<unknown>(`/public/api/projects/${enc}/variables/`,);
		return this.client.safeParse(ProjectVariablesSchema, raw, "variables.get",);
	}

	async set(opts: {
		standard?: Record<string, unknown>;
		local?: Record<string, unknown>;
		replace?: boolean;
		projectKey?: string;
	},): Promise<ProjectVariables> {
		const enc = this.enc(opts.projectKey,);

		if (opts.replace === true) {
			const replaced: ProjectVariables = {
				standard: opts.standard ?? {},
				local: opts.local ?? {},
			};

			await this.client.putVoid(`/public/api/projects/${enc}/variables/`, replaced,);
			return replaced;
		}

		if (opts.standard === undefined && opts.local === undefined) {
			throw new Error("At least one of standard or local must be provided",);
		}

		const existing = await this.get(opts.projectKey,);
		const merged: ProjectVariables = {
			standard: { ...existing.standard, ...opts.standard, },
			local: { ...existing.local, ...opts.local, },
		};

		await this.client.putVoid(`/public/api/projects/${enc}/variables/`, merged,);
		return merged;
	}
}
