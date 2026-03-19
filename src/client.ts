import { type Static, type TSchema, } from "@sinclair/typebox";
import { Value, } from "@sinclair/typebox/value";
import { type SafeParseResult, safeParseSchema, } from "./schemas.js";

import { classifyDataikuError, DataikuError, type DataikuRetryMetadata, } from "./errors.js";

import { CodeEnvsResource, } from "./resources/code-envs.js";
import { ConnectionsResource, } from "./resources/connections.js";
import { DatasetsResource, } from "./resources/datasets.js";
import { FoldersResource, } from "./resources/folders.js";
import { JobsResource, } from "./resources/jobs.js";
import { NotebooksResource, } from "./resources/notebooks.js";
import { ProjectsResource, } from "./resources/projects.js";
import { RecipesResource, } from "./resources/recipes.js";
import { ScenariosResource, } from "./resources/scenarios.js";
import { SqlResource, } from "./resources/sql.js";
import { VariablesResource, } from "./resources/variables.js";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const DEFAULT_RETRY_MAX_ATTEMPTS = 4;
const MAX_RETRY_ATTEMPTS_CAP = 10;
const BASE_DELAY_MS = 2_000;
const MAX_BACKOFF_DELAY_MS = 30_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/* ------------------------------------------------------------------ */
/*  Config                                                             */
/* ------------------------------------------------------------------ */

export interface DataikuClientConfig {
	/** DSS base URL (e.g. https://dss.example.com) */
	url: string;
	/** API key for authentication */
	apiKey: string;
	/** Default project key — used when a resource method omits projectKey */
	projectKey?: string;
	/** Per-request timeout in milliseconds (default 30 000) */
	requestTimeoutMs?: number;
	/** Max retry attempts for idempotent requests (default 4, capped at 10) */
	retryMaxAttempts?: number;
	/** Emit HTTP request/response logs to stderr for CLI debugging. */
	verbose?: boolean;
	/**
	 * Called when an API response fails schema validation but data is still usable.
	 * Default: writes to stderr. Set to a throwing function for strict mode.
	 * @param method - resource method that triggered the warning (e.g. "datasets.list")
	 * @param errors - human-readable validation error strings
	 */
	onValidationWarning?: (method: string, errors: string[],) => void;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function defaultValidationWarning(method: string, errors: string[],): void {
	process.stderr.write(
		`[dataiku-sdk] Schema validation warning in ${method}:\n  ${errors.join("\n  ",)}\n`,
	);
}

function sleep(ms: number,): Promise<void> {
	return new Promise((r,) => setTimeout(r, ms,));
}

function computeBackoffDelayMs(retryNumber: number,): number {
	const cap = Math.min(MAX_BACKOFF_DELAY_MS, BASE_DELAY_MS * 2 ** Math.max(0, retryNumber - 1,),);
	return Math.floor(Math.random() * (cap + 1),);
}

function isTransientError(status: number, body: string,): boolean {
	return classifyDataikuError(status, body,).category === "transient";
}

function shouldRetryMethod(method: string,): boolean {
	return method.toUpperCase() === "GET";
}

function buildRetryMetadata(
	method: string,
	enabled: boolean,
	maxAttempts: number,
	attempts: number,
	delaysMs: number[],
	timedOut: boolean,
): DataikuRetryMetadata {
	return {
		method,
		enabled,
		maxAttempts,
		attempts,
		retries: Math.max(0, attempts - 1,),
		delaysMs,
		timedOut,
	};
}

/* ------------------------------------------------------------------ */
/*  Client                                                             */
/* ------------------------------------------------------------------ */

export class DataikuClient {
	private readonly baseUrl: string;
	private readonly apiKey: string;
	private readonly defaultProjectKey: string | undefined;
	private readonly requestTimeoutMs: number;
	private readonly retryMaxAttempts: number;
	private readonly verbose: boolean;
	private readonly onValidationWarning: (method: string, errors: string[],) => void;

	/* Resource namespaces — lazily initialized to break circular imports */
	private _projects?: ProjectsResource;
	private _datasets?: DatasetsResource;
	private _recipes?: RecipesResource;
	private _jobs?: JobsResource;
	private _scenarios?: ScenariosResource;
	private _folders?: FoldersResource;
	private _variables?: VariablesResource;
	private _connections?: ConnectionsResource;
	private _codeEnvs?: CodeEnvsResource;
	private _sql?: SqlResource;
	private _notebooks?: NotebooksResource;

	get projects(): ProjectsResource {
		return (this._projects ??= new ProjectsResource(this,));
	}
	get datasets(): DatasetsResource {
		return (this._datasets ??= new DatasetsResource(this,));
	}
	get recipes(): RecipesResource {
		return (this._recipes ??= new RecipesResource(this,));
	}
	get jobs(): JobsResource {
		return (this._jobs ??= new JobsResource(this,));
	}
	get scenarios(): ScenariosResource {
		return (this._scenarios ??= new ScenariosResource(this,));
	}
	get folders(): FoldersResource {
		return (this._folders ??= new FoldersResource(this,));
	}
	get variables(): VariablesResource {
		return (this._variables ??= new VariablesResource(this,));
	}
	get connections(): ConnectionsResource {
		return (this._connections ??= new ConnectionsResource(this,));
	}
	get codeEnvs(): CodeEnvsResource {
		return (this._codeEnvs ??= new CodeEnvsResource(this,));
	}
	get sql(): SqlResource {
		return (this._sql ??= new SqlResource(this,));
	}
	get notebooks(): NotebooksResource {
		return (this._notebooks ??= new NotebooksResource(this,));
	}

	constructor(config: DataikuClientConfig,) {
		const url = config.url?.trim();
		if (!url) throw new Error("DataikuClientConfig.url is required and must not be empty",);
		const apiKey = config.apiKey?.trim();
		if (!apiKey) throw new Error("DataikuClientConfig.apiKey is required and must not be empty",);

		this.baseUrl = url.replace(/\/+$/, "",);
		this.apiKey = apiKey;
		this.defaultProjectKey = config.projectKey?.trim() || undefined;
		this.requestTimeoutMs = config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;

		const rawMax = config.retryMaxAttempts ?? DEFAULT_RETRY_MAX_ATTEMPTS;
		this.retryMaxAttempts = Math.min(Math.max(1, rawMax,), MAX_RETRY_ATTEMPTS_CAP,);
		this.verbose = config.verbose === true;
		this.onValidationWarning = config.onValidationWarning ?? defaultValidationWarning;
	}

	/* ---- public: project key resolution ---- */

	resolveProjectKey(paramValue?: string,): string {
		const pk = paramValue?.trim();
		if (pk) return pk;
		if (this.defaultProjectKey) return this.defaultProjectKey;
		throw new Error(
			"projectKey is required — pass it as a parameter or set projectKey in DataikuClientConfig",
		);
	}

	/* ---- public: HTTP verbs ---- */

	async get<T = unknown,>(path: string,): Promise<T> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getHeaders(),
		},);
		return this.parseJsonResponse<T>(res,);
	}

	async getText(path: string,): Promise<string> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		return res.text();
	}

	async post<T = unknown,>(path: string, body?: unknown,): Promise<T> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: this.getHeaders(),
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		},);
		return this.parseJsonResponse<T>(res,);
	}

	async put<T = unknown,>(path: string, body: unknown,): Promise<T> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "PUT",
			headers: this.getHeaders(),
			body: JSON.stringify(body,),
		},);
		return this.parseJsonResponse<T>(res,);
	}

	async del(path: string,): Promise<void> {
		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "DELETE",
			headers: this.getHeaders(),
		},);
	}

	async putVoid(path: string, body: unknown,): Promise<void> {
		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "PUT",
			headers: this.getHeaders(),
			body: JSON.stringify(body,),
		},);
	}

	async upload(path: string, filePath: string,): Promise<void> {
		const { openAsBlob, } = await import("node:fs");
		const { basename, } = await import("node:path");

		const fileBlob = await openAsBlob(filePath,);
		const fileName = basename(filePath,);

		const formData = new FormData();
		formData.append("file", fileBlob, fileName,);

		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${this.apiKey}`, },
			body: formData,
		},);
	}

	async stream(path: string,): Promise<Response> {
		return this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
	}

	/* ---- private: headers ---- */

	private getHeaders(): Record<string, string> {
		return {
			Authorization: `Bearer ${this.apiKey}`,
			Accept: "application/json",
			"Content-Type": "application/json",
		};
	}

	private getAnyHeaders(): Record<string, string> {
		return {
			Authorization: `Bearer ${this.apiKey}`,
			Accept: "*/*",
		};
	}

	private logVerbose(message: string,): void {
		if (this.verbose) process.stderr.write(`[dss] ${message}\n`,);
	}

	/* ---- public: schema-validated parsing ---- */

	/**
	 * Validate raw data against a TypeBox schema, throwing on structural mismatch.
	 * Resources call this instead of bare `as T` casts for validated responses.
	 * Extra DSS fields (additionalProperties) are preserved in the returned data.
	 */
	parse<S extends TSchema,>(schema: S, data: unknown,): Static<S> {
		Value.Assert(schema, data,);
		return data as Static<S>;
	}

	/**
	 * Validate raw data against a TypeBox schema without throwing.
	 * Always returns the data. On mismatch, fires onValidationWarning callback
	 * with the method name and error details.
	 */
	safeParse<S extends TSchema,>(schema: S, data: unknown, method: string,): Static<S> {
		const result: SafeParseResult<Static<S>> = safeParseSchema(schema, data,);
		if (!result.success) {
			this.onValidationWarning(method, result.errors,);
		}
		return result.data;
	}

	/** Emit a validation warning via the configured callback. */
	warn(method: string, errors: string[],): void {
		this.onValidationWarning(method, errors,);
	}

	/* ---- private: JSON parsing ---- */

	private async parseJsonResponse<T,>(res: Response,): Promise<T> {
		const text = await res.text();
		// SAFETY: Empty 2xx responses from DSS are surfaced to callers as undefined
		// cast to T. This keeps existing call sites stable, but callers that rely on
		// an object shape must guard explicitly before dereferencing the result.
		if (!text) return undefined as T;
		try {
			return JSON.parse(text,) as T;
		} catch {
			const summary = text.length > 300 ? `${text.slice(0, 300,)}…` : text;
			throw new DataikuError(
				res.status,
				res.statusText || "Invalid JSON response",
				`Expected JSON response body but got non-JSON content: ${summary}`,
			);
		}
	}

	/* ---- private: retry loop ---- */

	private async fetchWithRetry(url: string, init: RequestInit,): Promise<Response> {
		const method = (init.method ?? "GET").toUpperCase();
		const retryEnabled = shouldRetryMethod(method,);
		const maxAttempts = retryEnabled ? this.retryMaxAttempts : 1;
		const delaysMs: number[] = [];

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			let timedOut = false;
			const startedAt = Date.now();
			const controller = new AbortController();
			const timeout = setTimeout(() => {
				timedOut = true;
				controller.abort();
			}, this.requestTimeoutMs,);

			this.logVerbose(`${method} ${url}`,);

			try {
				const res = await fetch(url, { ...init, method, signal: controller.signal, },);
				this.logVerbose(`${method} ${url} → ${res.status} (${Date.now() - startedAt}ms)`,);
				if (!res.ok) {
					const text = await res.text();
					const canRetry = retryEnabled && attempt < maxAttempts && isTransientError(res.status, text,);
					if (canRetry) {
						const delayMs = computeBackoffDelayMs(attempt,);
						delaysMs.push(delayMs,);
						await sleep(delayMs,);
						continue;
					}
					throw new DataikuError(
						res.status,
						res.statusText,
						text,
						buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, false,),
					);
				}
				return res;
			} catch (error) {
				if (error instanceof DataikuError) throw error;
				const canRetry = retryEnabled && attempt < maxAttempts;
				if (canRetry) {
					const delayMs = computeBackoffDelayMs(attempt,);
					delaysMs.push(delayMs,);
					await sleep(delayMs,);
					continue;
				}
				const detail = timedOut
					? `Request timed out after ${this.requestTimeoutMs}ms`
					: error instanceof Error
					? error.message
					: "Unknown transport error";
				this.logVerbose(`${method} ${url} → ERROR (${Date.now() - startedAt}ms) ${detail}`,);
				const statusText = timedOut ? "Request Timeout" : "Network Error";
				throw new DataikuError(
					0,
					statusText,
					detail,
					buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, timedOut,),
				);
			} finally {
				clearTimeout(timeout,);
			}
		}

		// Unreachable in practice — the loop always throws or returns.
		throw new DataikuError(
			0,
			"Network Error",
			"Request failed before receiving a response.",
			buildRetryMetadata(method, false, 1, 1, [], false,),
		);
	}
}
