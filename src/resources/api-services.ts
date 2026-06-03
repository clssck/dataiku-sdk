import { BaseResource, } from "./base.js";

export interface ApiServiceListItem extends Record<string, unknown> {
	id?: string;
	serviceId?: string;
	name?: string;
}

export interface ApiServicePackageListItem extends Record<string, unknown> {
	id?: string;
	packageId?: string;
	serviceId?: string;
}

export type ApiServiceActionResult = Record<string, unknown> | undefined;

export class ApiServicesResource extends BaseResource {
	/** List API services in a project. */
	async list(projectKey?: string,): Promise<ApiServiceListItem[]> {
		return this.client.get<ApiServiceListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/apiservices/`,
		);
	}

	/** Create an empty API service shell. */
	async create(serviceId: string, projectKey?: string,): Promise<ApiServiceActionResult> {
		return this.client.post<ApiServiceActionResult>(
			this.servicePath(serviceId, projectKey,),
			{},
		);
	}

	/** Get API service settings, including endpoint definitions. */
	async getSettings(serviceId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`${this.servicePath(serviceId, projectKey,)}/settings`,
		);
	}

	/** Save API service settings. */
	async saveSettings(
		serviceId: string,
		body: Record<string, unknown>,
		projectKey?: string,
	): Promise<ApiServiceActionResult> {
		return this.client.put<ApiServiceActionResult>(
			`${this.servicePath(serviceId, projectKey,)}/settings`,
			body,
		);
	}

	/** List deployable packages for an API service. */
	async listPackages(serviceId: string, projectKey?: string,): Promise<ApiServicePackageListItem[]> {
		return this.client.get<ApiServicePackageListItem[]>(
			`${this.servicePath(serviceId, projectKey,)}/packages`,
		);
	}

	/** Get an API service package summary. */
	async getPackageSummary(
		serviceId: string,
		packageId: string,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		return this.client.get<Record<string, unknown>>(
			`${this.packagePath(serviceId, packageId, projectKey,)}/summary`,
		);
	}

	/** Create a deployable package from the current service state. */
	async createPackage(
		serviceId: string,
		packageId: string,
		projectKey?: string,
	): Promise<ApiServiceActionResult> {
		return this.client.post<ApiServiceActionResult>(
			this.packagePath(serviceId, packageId, projectKey,),
			{},
		);
	}

	/** Delete an API service package. */
	async deletePackage(serviceId: string, packageId: string, projectKey?: string,): Promise<void> {
		await this.client.del(this.packagePath(serviceId, packageId, projectKey,),);
	}

	/** Download an API service package archive. */
	async downloadPackageArchive(
		serviceId: string,
		packageId: string,
		projectKey?: string,
	): Promise<Response> {
		return this.client.stream(`${this.packagePath(serviceId, packageId, projectKey,)}/archive`,);
	}

	/** Publish an API service package to the API Deployer. */
	async publishPackage(
		serviceId: string,
		packageId: string,
		projectKey?: string,
	): Promise<ApiServiceActionResult> {
		return this.client.post<ApiServiceActionResult>(
			`${this.packagePath(serviceId, packageId, projectKey,)}/publish`,
			{},
		);
	}

	private servicePath(serviceId: string, projectKey?: string,): string {
		return `/public/api/projects/${this.enc(projectKey,)}/apiservices/${
			encodeURIComponent(serviceId,)
		}`;
	}

	private packagePath(serviceId: string, packageId: string, projectKey?: string,): string {
		return `${this.servicePath(serviceId, projectKey,)}/packages/${encodeURIComponent(packageId,)}`;
	}
}
