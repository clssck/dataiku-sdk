import { BaseResource, } from "./base.js";

/**
 * Optional, documented parameters of the bundle-export endpoint:
 * - `releaseNotes`: important changes introduced in the bundle;
 * - `evaluateProjectStandardsChecks`: whether the Project Standards Checks
 *   applying to the project should run (defaults to true server-side, and the
 *   official client always sends it).
 */
export interface BundleExportOptions {
	releaseNotes?: string;
	evaluateProjectStandardsChecks?: boolean;
}

/**
 * Optional, documented parameter of the bundle-publish endpoint: the key of
 * the published project on the Project Deployer where the bundle is deployed
 * (a new published project is created when no match exists; when omitted, the
 * server falls back to the source project's key).
 */
export interface BundlePublishOptions {
	publishedProjectKey?: string;
}

/**
 * Optional, documented parameter of the bundle-activation endpoint: scenario
 * IDs mapped to whether each scenario should be enabled or disabled upon
 * activation.
 */
export interface BundleActivateOptions {
	scenariosToEnable?: Record<string, boolean>;
}

export class BundlesResource extends BaseResource {
	/** List bundles exported from a project on the Design node. */
	async listExported(projectKey?: string,): Promise<Record<string, unknown>[]> {
		const res = await this.client.get<{ bundles?: Record<string, unknown>[]; }>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported`,
		);
		return res.bundles ?? [];
	}

	/**
	 * Create or overwrite an exported Design-node bundle. The documented
	 * optional parameters are forwarded as query parameters:
	 * `releaseNotes` when provided, and `evaluateProjectStandardsChecks`
	 * (defaults to true, matching the official client which always sends it).
	 */
	async exportBundle(
		bundleId: string,
		projectKey?: string,
		options: BundleExportOptions = {},
	): Promise<void> {
		const params = new URLSearchParams();
		if (options.releaseNotes !== undefined) params.set("releaseNotes", options.releaseNotes,);
		params.set(
			"evaluateProjectStandardsChecks",
			String(options.evaluateProjectStandardsChecks ?? true,),
		);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported/${
				encodeURIComponent(bundleId,)
			}?${params.toString()}`,
			{},
		);
	}

	/** Delete an exported Design-node bundle. */
	async deleteExported(bundleId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported/${
				encodeURIComponent(bundleId,)
			}`,
		);
	}

	/** Download an exported Design-node bundle archive. */
	async downloadExportedArchive(bundleId: string, projectKey?: string,): Promise<Response> {
		return this.client.stream(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported/${
				encodeURIComponent(bundleId,)
			}/archive`,
		);
	}

	/**
	 * Publish a Design-node bundle to the Project Deployer. The documented
	 * optional `publishedProjectKey` parameter selects the published project
	 * the bundle is deployed to (a new published project is created when no
	 * match exists; when omitted, the server falls back to the source
	 * project's key). It is forwarded as the `publishedProjectKey` query
	 * parameter when provided.
	 */
	async publish(
		bundleId: string,
		projectKey?: string,
		options: BundlePublishOptions = {},
	): Promise<Record<string, unknown>> {
		const params = new URLSearchParams();
		if (options.publishedProjectKey !== undefined) {
			params.set("publishedProjectKey", options.publishedProjectKey,);
		}
		const query = params.size > 0 ? `?${params.toString()}` : "";
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/${
				encodeURIComponent(bundleId,)
			}/publish${query}`,
			{},
		);
	}

	/** List bundles imported into a project on the Automation node. */
	async listImported(projectKey?: string,): Promise<Record<string, unknown>[]> {
		const res = await this.client.get<{ bundles?: Record<string, unknown>[]; }>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported`,
		);
		return res.bundles ?? [];
	}

	/** Import a server-side bundle archive into an Automation-node project. */
	async importFromArchive(
		archivePath: string,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${
				this.enc(projectKey,)
			}/bundles/imported/actions/importFromArchive?archivePath=${encodeURIComponent(archivePath,)}`,
			{},
		);
	}

	/** Import a local bundle archive stream into an Automation-node project. */
	async importFromStream(filePath: string, projectKey?: string,): Promise<void> {
		await this.client.upload(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported/actions/importFromStream`,
			filePath,
		);
	}

	/**
	 * Activate an imported Automation-node bundle. The documented optional
	 * `scenariosToEnable` dict (scenario ID → enabled/disabled upon
	 * activation) is forwarded as the `scenariosActiveOnActivation` request
	 * body when provided.
	 */
	async activate(
		bundleId: string,
		projectKey?: string,
		options: BundleActivateOptions = {},
	): Promise<Record<string, unknown>> {
		const body = options.scenariosToEnable !== undefined
				&& Object.keys(options.scenariosToEnable,).length > 0
			? { scenariosActiveOnActivation: options.scenariosToEnable, }
			: {};
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported/${
				encodeURIComponent(bundleId,)
			}/actions/activate`,
			body,
		);
	}

	/** Preload an imported Automation-node bundle. */
	async preload(bundleId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported/${
				encodeURIComponent(bundleId,)
			}/actions/preload`,
			{},
		);
	}

	/** Delete an imported Automation-node bundle. */
	async deleteImported(bundleId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported/${
				encodeURIComponent(bundleId,)
			}`,
		);
	}
}

export class ProjectDeployerResource extends BaseResource {
	/** List published projects in the Project Deployer. */
	async listProjects(): Promise<Record<string, unknown>[]> {
		return this.client.get<Record<string, unknown>[]>("/public/api/project-deployer/projects",);
	}

	/** Create a Project Deployer published project. */
	async createProject(body: Record<string, unknown>,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			"/public/api/project-deployer/projects",
			body,
		);
	}

	/** Upload a project bundle archive to the Project Deployer. */
	async uploadBundle(filePath: string,): Promise<void> {
		await this.client.upload("/public/api/project-deployer/projects/bundles", filePath,);
	}

	/** Get a Project Deployer published project's status. */
	async getProjectStatus(publishedProjectKey: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/project-deployer/projects/${encodeURIComponent(publishedProjectKey,)}`,
		);
	}

	/** List Project Deployer deployments. */
	async listDeployments(): Promise<Record<string, unknown>[]> {
		return this.client.get<Record<string, unknown>[]>(
			"/public/api/project-deployer/deployments",
		);
	}

	/** Create a Project Deployer deployment. */
	async createDeployment(body: Record<string, unknown>,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			"/public/api/project-deployer/deployments",
			body,
		);
	}

	/** Get a Project Deployer deployment. */
	async getDeployment(deploymentId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/project-deployer/deployments/${encodeURIComponent(deploymentId,)}`,
		);
	}

	/** Get a Project Deployer deployment's full status. */
	async getDeploymentStatus(deploymentId: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`/public/api/project-deployer/deployments/${encodeURIComponent(deploymentId,)}/status`,
		);
	}

	/** Save Project Deployer deployment settings. */
	async saveDeploymentSettings(
		deploymentId: string,
		body: Record<string, unknown>,
	): Promise<void> {
		await this.client.putVoid(
			`/public/api/project-deployer/deployments/${encodeURIComponent(deploymentId,)}/settings`,
			body,
		);
	}

	/** Start a Project Deployer deployment update. */
	async startUpdate(deploymentId: string,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			`/public/api/project-deployer/deployments/${encodeURIComponent(deploymentId,)}/actions/update`,
			{},
		);
	}

	/** Delete a Project Deployer deployment. */
	async deleteDeployment(deploymentId: string,): Promise<void> {
		await this.client.del(
			`/public/api/project-deployer/deployments/${encodeURIComponent(deploymentId,)}`,
		);
	}

	/** List Project Deployer infrastructures. */
	async listInfras(): Promise<Record<string, unknown>[]> {
		return this.client.get<Record<string, unknown>[]>("/public/api/project-deployer/infras",);
	}

	/** Create a Project Deployer infrastructure. */
	async createInfra(body: Record<string, unknown>,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			"/public/api/project-deployer/infras",
			body,
		);
	}
}
