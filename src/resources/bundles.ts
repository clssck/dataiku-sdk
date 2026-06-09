import { BaseResource, } from "./base.js";

export class BundlesResource extends BaseResource {
	/** List bundles exported from a project on the Design node. */
	async listExported(projectKey?: string,): Promise<Record<string, unknown>[]> {
		const res = await this.client.get<{ bundles?: Record<string, unknown>[]; }>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported`,
		);
		return res.bundles ?? [];
	}

	/** Create or overwrite an exported Design-node bundle. */
	async exportBundle(bundleId: string, projectKey?: string,): Promise<void> {
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/exported/${
				encodeURIComponent(bundleId,)
			}`,
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

	/** Publish a Design-node bundle to the Project Deployer. */
	async publish(bundleId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/${encodeURIComponent(bundleId,)}/publish`,
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

	/** Activate an imported Automation-node bundle. */
	async activate(bundleId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/bundles/imported/${
				encodeURIComponent(bundleId,)
			}/actions/activate`,
			{},
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
