import { ClientValidationError, } from "../errors.js";
import type { FutureWaitResult, } from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { SHA256_HEX_PATTERN, stableHash, } from "../utils/stable-hash.js";
import { BaseResource, } from "./base.js";

export interface WebappListItem {
	id?: string;
	webAppId?: string;
	name?: string;
	type?: string;
	projectKey?: string;
}

export interface WebappCreateResult {
	webAppId: string;
}

/**
 * 64-hex SHA-256 hash of the webapp object as last fetched, used as a
 * non-atomic stale-read guard: the write is refused when the object already
 * differs at the verification read. DSS exposes no conditional write (no
 * ETag, no If-Match), so a writer committing between that read and the PUT
 * is still not detected.
 */
export interface WebappUpdateOptions {
	expectHash?: string;
}

export interface WebappWaitOptions {
	pollIntervalMs?: number;
	timeoutMs?: number;
}

function assertExpectedHash(
	id: string,
	expectHash: string,
	current: Record<string, unknown>,
	projectKey: string | undefined,
): void {
	if (!SHA256_HEX_PATTERN.test(expectHash,)) {
		throw new ClientValidationError(
			"Expected webapp hash must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			"Use the hash value returned by a previous read.",
			{ projectKey, id, },
		);
	}
	const actual = stableHash(current,);
	if (actual !== expectHash.toLowerCase()) {
		throw new ClientValidationError(
			`The webapp ${JSON.stringify(id,)} changed since it was read.`,
			"validation_failed",
			"Re-read the webapp settings and retry with the current hash value.",
			{
				projectKey,
				id,
				expectedHash: expectHash.toLowerCase(),
				actualHash: actual,
			},
		);
	}
}

export class WebappsResource extends BaseResource {
	/** List all webapps in a project. */
	async list(projectKey?: string,): Promise<WebappListItem[]> {
		return this.client.get<WebappListItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/`,
		);
	}

	/** Get one webapp's settings. */
	async getSettings(webappId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}`,
		);
	}

	/** Create a webapp. */
	async create(body: Record<string, unknown>, projectKey?: string,): Promise<WebappCreateResult> {
		return this.client.post<WebappCreateResult>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/`,
			body,
		);
	}

	/**
	 * Update one webapp's settings via GET-merge-PUT. The public API requires
	 * the object passed to a PUT call to have been obtained from a GET call at
	 * the same URL, so the current object is fetched first, the supplied
	 * fields are merged over it (fields absent from `body` are preserved), and
	 * the whole merged object is PUT back.
	 *
	 * With `expectHash` armed, the fresh GET doubles as the stale-read guard:
	 * the stable hash of the fetched object must match, or the write is
	 * refused before any PUT.
	 */
	async updateSettings(
		webappId: string,
		body: Record<string, unknown>,
		projectKey?: string,
		options: WebappUpdateOptions = {},
	): Promise<Record<string, unknown>> {
		const current = await this.getSettings(webappId, projectKey,);
		if (options.expectHash !== undefined) {
			assertExpectedHash(webappId, options.expectHash, current, projectKey,);
		}
		const next = deepMerge(current, body,);
		const id = encodeURIComponent(webappId,);
		return this.client.put<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}`,
			next,
		);
	}

	/** Stop a webapp backend. */
	async stopBackend(webappId: string, projectKey?: string,): Promise<void> {
		const id = encodeURIComponent(webappId,);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/actions/stop`,
			{},
		);
	}

	/**
	 * Start or restart a webapp backend. The documented endpoint returns a
	 * reference to a future, which is surfaced as-is; settle it with
	 * {@link restartBackendAndWait} or `client.futures.wait`. DSS 15 returns
	 * an empty body (no future) when no async job was spawned — the official
	 * Python client tolerates that (`DSSFuture.from_resp` accepts a missing
	 * jobId), so an empty response surfaces as `{}` rather than crashing.
	 */
	async startOrRestartBackend(
		webappId: string,
		projectKey?: string,
	): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		const future = await this.client.put<Record<string, unknown> | undefined>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/actions/restart`,
			{},
		);
		return future ?? {};
	}

	/**
	 * Restart a webapp backend and wait for the returned future to settle.
	 * The documented endpoint returns a reference to a future; with a real
	 * jobId this polls that future to completion. An empty response carries
	 * NO future to wait on (observed on DSS 15 for a webapp whose backend is
	 * not enabled — a no-op restart, not a supported wait target), so the
	 * wait reports the missing future explicitly instead of fabricating a
	 * result for it. Enable the backend (params.backendEnabled with the
	 * params.python code, exactly as the DSS editor does) to get a real
	 * restart future.
	 */
	async restartBackendAndWait(
		webappId: string,
		projectKey?: string,
		options: WebappWaitOptions = {},
	): Promise<FutureWaitResult> {
		const future = await this.startOrRestartBackend(webappId, projectKey,);
		const jobId = future.jobId;
		if (typeof jobId !== "string" || jobId.length === 0) {
			throw new ClientValidationError(
				"Webapp backend restart returned no future jobId, so there is nothing to wait on.",
				"validation_failed",
				"The restart endpoint returns a future reference only when it starts an async job (a webapp with an enabled Python backend). Enable the backend before restarting with --wait.",
				{ webappId, restartResponseKeys: Object.keys(future,), },
			);
		}
		return this.client.futures.wait(jobId, {
			pollIntervalMs: options.pollIntervalMs,
			timeoutMs: options.timeoutMs,
		},);
	}

	/** Get a webapp backend's state. */
	async getBackendState(webappId: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const id = encodeURIComponent(webappId,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/webapps/${id}/backend/state`,
		);
	}
}
