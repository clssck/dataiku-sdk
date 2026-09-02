import { ClientValidationError, } from "../errors.js";
import { stableHash, } from "../utils/stable-hash.js";
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

/**
 * 64-hex SHA-256 hash of the service settings as last fetched, used as a
 * non-atomic stale-read guard: the write is refused when the settings already
 * differ at the verification read. DSS exposes no conditional write, so a
 * writer committing between that read and the PUT is still not detected.
 */
export interface ApiServiceSaveOptions {
	expectHash?: string;
}

/** Optional, documented parameters of the package-generation endpoint. */
export interface ApiServiceCreatePackageOptions {
	releaseNotes?: string;
}

/**
 * Optional, documented parameter of the package-publish endpoint: the id of
 * the published service on the API Deployer where the package is deployed
 * (defaults to the service's own id server-side).
 */
export interface ApiServicePublishOptions {
	publishedServiceId?: string;
}

const EXPECT_HASH_PATTERN = /^[0-9a-fA-F]{64}$/;

function assertExpectedSettingsHash(
	serviceId: string,
	expectHash: string,
	current: Record<string, unknown>,
	projectKey: string | undefined,
): void {
	if (!EXPECT_HASH_PATTERN.test(expectHash,)) {
		throw new ClientValidationError(
			"Expected API service settings hash must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			"Use the hash value returned by a previous read.",
			{ projectKey, serviceId, },
		);
	}
	const actual = stableHash(current,);
	if (actual !== expectHash.toLowerCase()) {
		throw new ClientValidationError(
			`The API service ${JSON.stringify(serviceId,)} changed since it was read.`,
			"validation_failed",
			"Re-read the service settings and retry with the current hash value.",
			{
				projectKey,
				serviceId,
				expectedHash: expectHash.toLowerCase(),
				actualHash: actual,
			},
		);
	}
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

	/**
	 * Save API service settings with explicit replacement semantics: the body
	 * is the complete settings object (fetched via get-settings) and is PUT
	 * as-is. With `expectHash` armed, the settings are read first and their
	 * stable hash must match, or the write is refused before any PUT.
	 */
	async saveSettings(
		serviceId: string,
		body: Record<string, unknown>,
		projectKey?: string,
		options: ApiServiceSaveOptions = {},
	): Promise<ApiServiceActionResult> {
		if (options.expectHash !== undefined) {
			const current = await this.getSettings(serviceId, projectKey,);
			assertExpectedSettingsHash(serviceId, options.expectHash, current, projectKey,);
		}
		return this.client.put<ApiServiceActionResult>(
			`${this.servicePath(serviceId, projectKey,)}/settings`,
			body,
		);
	}

	/** Add a standard prediction endpoint backed by a saved model. */
	async addPredictionEndpoint(
		serviceId: string,
		endpointId: string,
		savedModelId: string,
		projectKey?: string,
	): Promise<ApiServiceActionResult> {
		const settings = await this.getSettings(serviceId, projectKey,);
		const endpoints = Array.isArray(settings.endpoints,) ? settings.endpoints : [];
		settings.endpoints = [
			...endpoints,
			{ id: endpointId, type: "STD_PREDICTION", modelRef: savedModelId, },
		];
		return this.saveSettings(serviceId, settings, projectKey,);
	}

	/** List deployable packages for an API service. */
	async listPackages(serviceId: string, projectKey?: string,): Promise<ApiServicePackageListItem[]> {
		await this.getSettings(serviceId, projectKey,);
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

	/**
	 * Create a deployable package from the current service state. The
	 * documented optional `releaseNotes` parameter is forwarded as the
	 * `releaseNotes` query parameter when provided.
	 */
	async createPackage(
		serviceId: string,
		packageId: string,
		projectKey?: string,
		options: ApiServiceCreatePackageOptions = {},
	): Promise<{ message: string; }> {
		const params = new URLSearchParams();
		if (options.releaseNotes !== undefined) params.set("releaseNotes", options.releaseNotes,);
		const query = params.size > 0 ? `?${params.toString()}` : "";
		const message = await this.client.postText(
			`${this.packagePath(serviceId, packageId, projectKey,)}${query}`,
			{},
		);
		return { message, };
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

	/**
	 * Publish an API service package to the API Deployer. The documented
	 * optional `publishedServiceId` parameter selects the published service
	 * the package is deployed to (a new published service is created when no
	 * match exists; when omitted, the server falls back to the service's own
	 * id). It is forwarded as the `publishedServiceId` query parameter when
	 * provided.
	 */
	async publishPackage(
		serviceId: string,
		packageId: string,
		projectKey?: string,
		options: ApiServicePublishOptions = {},
	): Promise<ApiServiceActionResult> {
		const params = new URLSearchParams();
		if (options.publishedServiceId !== undefined) {
			params.set("publishedServiceId", options.publishedServiceId,);
		}
		const query = params.size > 0 ? `?${params.toString()}` : "";
		return this.client.post<ApiServiceActionResult>(
			`${this.packagePath(serviceId, packageId, projectKey,)}/publish${query}`,
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
