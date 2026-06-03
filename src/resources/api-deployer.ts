import { BaseResource, } from "./base.js";

export interface ApiDeployerInfra extends Record<string, unknown> {
	id?: string;
}

export interface ApiDeployerStage extends Record<string, unknown> {
	id?: string;
}

export interface ApiDeployerService extends Record<string, unknown> {
	id?: string;
	publishedServiceId?: string;
}

export interface ApiDeployerDeployment extends Record<string, unknown> {
	id?: string;
	deploymentId?: string;
}

export interface ApiDeployerDeploymentStatus extends Record<string, unknown> {
	id?: string;
	deploymentId?: string;
}

export interface ApiDeployerDeploymentSettings extends Record<string, unknown> {
	id?: string;
	deploymentId?: string;
}

export interface ApiDeployerActionResult extends Record<string, unknown> {
	jobId?: string;
}

export class ApiDeployerResource extends BaseResource {
	/** List API Deployer infrastructures. */
	async listInfras(): Promise<ApiDeployerInfra[]> {
		return this.client.get<ApiDeployerInfra[]>("/public/api/api-deployer/infras",);
	}

	/** Create an API Deployer infrastructure. */
	async createInfra(body: Record<string, unknown>,): Promise<ApiDeployerInfra> {
		return this.client.post<ApiDeployerInfra>("/public/api/api-deployer/infras", body,);
	}

	/** Get an API Deployer infrastructure. */
	async getInfra(infraId: string,): Promise<ApiDeployerInfra> {
		return this.client.get<ApiDeployerInfra>(
			`/public/api/api-deployer/infras/${encodeURIComponent(infraId,)}`,
		);
	}

	/** Delete an API Deployer infrastructure. */
	async deleteInfra(infraId: string,): Promise<void> {
		await this.client.del(`/public/api/api-deployer/infras/${encodeURIComponent(infraId,)}`,);
	}

	/** List API Deployer stages. */
	async listStages(): Promise<ApiDeployerStage[]> {
		return this.client.get<ApiDeployerStage[]>("/public/api/api-deployer/stages",);
	}

	/** List published API Deployer services. */
	async listServices(): Promise<ApiDeployerService[]> {
		return this.client.get<ApiDeployerService[]>("/public/api/api-deployer/services",);
	}

	/** Create a published API Deployer service. */
	async createService(body: Record<string, unknown>,): Promise<ApiDeployerService> {
		return this.client.post<ApiDeployerService>("/public/api/api-deployer/services", body,);
	}

	/** Get a published API Deployer service. */
	async getService(serviceId: string,): Promise<ApiDeployerService> {
		return this.client.get<ApiDeployerService>(
			`/public/api/api-deployer/services/${encodeURIComponent(serviceId,)}`,
		);
	}

	/** Delete a published API Deployer service. */
	async deleteService(serviceId: string,): Promise<void> {
		await this.client.del(`/public/api/api-deployer/services/${encodeURIComponent(serviceId,)}`,);
	}

	/** Publish a service version archive to the API Deployer. */
	async publishServiceVersion(serviceId: string, filePath: string,): Promise<void> {
		await this.client.upload(
			`/public/api/api-deployer/services/${encodeURIComponent(serviceId,)}/versions`,
			filePath,
		);
	}

	/** Delete a published service version. */
	async deleteServiceVersion(serviceId: string, version: string,): Promise<void> {
		await this.client.del(
			`/public/api/api-deployer/services/${encodeURIComponent(serviceId,)}/versions/${
				encodeURIComponent(version,)
			}`,
		);
	}

	/** List API Deployer deployments. */
	async listDeployments(): Promise<ApiDeployerDeployment[]> {
		return this.client.get<ApiDeployerDeployment[]>("/public/api/api-deployer/deployments",);
	}

	/** Create an API Deployer deployment. */
	async createDeployment(body: Record<string, unknown>,): Promise<ApiDeployerDeployment> {
		return this.client.post<ApiDeployerDeployment>(
			"/public/api/api-deployer/deployments",
			body,
		);
	}

	/** Get an API Deployer deployment. */
	async getDeployment(deploymentId: string,): Promise<ApiDeployerDeployment> {
		return this.client.get<ApiDeployerDeployment>(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}`,
		);
	}

	/** Get an API Deployer deployment's full status. */
	async getDeploymentStatus(deploymentId: string,): Promise<ApiDeployerDeploymentStatus> {
		return this.client.get<ApiDeployerDeploymentStatus>(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}/status`,
		);
	}

	/** Get API Deployer deployment settings. */
	async getDeploymentSettings(deploymentId: string,): Promise<ApiDeployerDeploymentSettings> {
		return this.client.get<ApiDeployerDeploymentSettings>(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}/settings`,
		);
	}

	/** Save API Deployer deployment settings. */
	async saveDeploymentSettings(
		deploymentId: string,
		body: Record<string, unknown>,
	): Promise<void> {
		await this.client.putVoid(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}/settings`,
			body,
		);
	}

	/** Start an API Deployer deployment update. */
	async startDeploymentUpdate(deploymentId: string,): Promise<ApiDeployerActionResult> {
		return this.client.post<ApiDeployerActionResult>(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}/actions/update`,
			{},
		);
	}

	/** Delete an API Deployer deployment. */
	async deleteDeployment(deploymentId: string,): Promise<void> {
		await this.client.del(
			`/public/api/api-deployer/deployments/${encodeURIComponent(deploymentId,)}`,
		);
	}
}
