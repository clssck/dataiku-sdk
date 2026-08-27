import { openAsBlob, readFileSync, } from "node:fs";
import { basename, } from "node:path";
import { getCACertificates, } from "node:tls";

import { type Static, type TSchema, } from "@sinclair/typebox";
import { Value, } from "@sinclair/typebox/value";
import { type SafeParseResult, safeParseSchema, } from "./schemas.js";

import {
	classifyDataikuError,
	ClientValidationError,
	DataikuError,
	type DataikuRetryMetadata,
} from "./errors.js";

import { AnalysesResource, } from "./resources/analyses.js";
import { ApiDeployerResource, } from "./resources/api-deployer.js";
import { ApiServicesResource, } from "./resources/api-services.js";
import { ApplicationsResource, } from "./resources/applications.js";
import { BundlesResource, ProjectDeployerResource, } from "./resources/bundles.js";
import { CodeEnvsResource, } from "./resources/code-envs.js";
import { ConnectionsResource, } from "./resources/connections.js";
import { ContinuousActivitiesResource, } from "./resources/continuous-activities.js";
import { DashboardsResource, } from "./resources/dashboards.js";
import { DataQualityResource, } from "./resources/data-quality.js";
import { DatasetsResource, } from "./resources/datasets.js";
import { DiscussionsResource, } from "./resources/discussions.js";
import { FlowZonesResource, } from "./resources/flow-zones.js";
import { FoldersResource, } from "./resources/folders.js";
import { FuturesResource, } from "./resources/futures.js";
import { InsightsResource, } from "./resources/insights.js";
import { JobsResource, } from "./resources/jobs.js";
import { MeaningsResource, } from "./resources/meanings.js";
import { MetricsResource, } from "./resources/metrics.js";
import { MlTasksResource, } from "./resources/ml-tasks.js";
import { ModelEvaluationStoresResource, } from "./resources/model-evaluation-stores.js";
import { NotebooksResource, } from "./resources/notebooks.js";
import { ProjectGitResource, } from "./resources/project-git.js";
import { ProjectLibraryResource, } from "./resources/project-library.js";
import { ProjectsResource, } from "./resources/projects.js";
import { RecipesResource, } from "./resources/recipes.js";
import { SavedModelsResource, } from "./resources/saved-models.js";
import { ScenariosResource, } from "./resources/scenarios.js";
import { SqlResource, } from "./resources/sql.js";
import { StatisticsResource, } from "./resources/statistics.js";
import { StreamingEndpointsResource, } from "./resources/streaming-endpoints.js";
import { VariablesResource, } from "./resources/variables.js";
import { WebappsResource, } from "./resources/webapps.js";
import { WikiResource, } from "./resources/wiki.js";
import { WorkspacesResource, } from "./resources/workspaces.js";

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

export interface DataikuClientTraceEvent {
	type: "trace";
	phase: "request" | "response" | "error";
	method: string;
	url: string;
	attempt: number;
	maxAttempts: number;
	status?: number;
	elapsedMs?: number;
	detail?: string;
}

/**
 * Selected response metadata captured alongside a public API response body.
 * A fixed whitelist of non-sensitive headers: authorization data is never
 * returned. Null means the header was absent.
 */
export interface DataikuClientResponseMeta {
	/** `DSS-Version` — version of the DSS backend answering the request. */
	dssVersion: string | null;
	/** `DSS-API-Version` — version of the API server handling the request. */
	dssApiVersion: string | null;
	/** `Date` — origin server timestamp, when present. */
	date: string | null;
	/** Server-provided request id, when present. */
	requestId: string | null;
}

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
	/** Emit HTTP request/response trace events. Defaults to JSONL on stderr when verbose is true. */
	verbose?: boolean;
	onTrace?: (event: DataikuClientTraceEvent,) => void;
	/** Override TLS certificate verification for HTTPS requests. */
	tlsRejectUnauthorized?: boolean;
	/** Extra PEM CA bundle to trust in addition to Bun's default trust store. */
	caCertPath?: string;
	/**
	 * Called when an API response fails schema validation but data is still usable.
	 * Default: ignored. Set to a recording or throwing function for strict mode.
	 * @param method - resource method that triggered the warning (e.g. "datasets.list")
	 * @param errors - human-readable validation error strings
	 */
	onValidationWarning?: (method: string, errors: string[],) => void;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function defaultTrace(event: DataikuClientTraceEvent,): void {
	process.stderr.write(`${JSON.stringify(event,)}\n`,);
}

function defaultValidationWarning(_method: string, _errors: string[],): void {
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

function concatBytes(chunks: Uint8Array[], total: number,): Uint8Array {
	const out = new Uint8Array(total,);
	let offset = 0;
	for (const chunk of chunks) {
		out.set(chunk, offset,);
		offset += chunk.byteLength;
	}
	return out;
}

/**
 * Drop a trailing incomplete UTF-8 multibyte sequence so the bytes end on a
 * character boundary. Guarantees the decoded string re-encodes to no more bytes
 * than the input, so a byte cap is never exceeded by a replacement character.
 */
function trimToUtf8Boundary(bytes: Uint8Array,): Uint8Array {
	let i = bytes.length - 1;
	let continuation = 0;
	while (i >= 0 && (bytes[i] & 0xC0) === 0x80) {
		i--;
		continuation++;
	}
	if (i < 0) return bytes;
	const lead = bytes[i];
	let expected: number;
	if ((lead & 0x80) === 0x00) expected = 1;
	else if ((lead & 0xE0) === 0xC0) expected = 2;
	else if ((lead & 0xF0) === 0xE0) expected = 3;
	else if ((lead & 0xF8) === 0xF0) expected = 4;
	else return bytes;
	return continuation + 1 < expected ? bytes.subarray(0, i,) : bytes;
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

type FetchTlsOptions = {
	rejectUnauthorized?: boolean;
	ca?: string[];
};

function buildFetchTlsOptions(config: DataikuClientConfig,): FetchTlsOptions | undefined {
	const rejectUnauthorized = config.tlsRejectUnauthorized;
	const caCertPath = config.caCertPath?.trim();
	if (rejectUnauthorized === undefined && !caCertPath) return undefined;

	const tls: FetchTlsOptions = {};
	if (rejectUnauthorized !== undefined) tls.rejectUnauthorized = rejectUnauthorized;

	if (caCertPath) {
		try {
			tls.ca = [...getCACertificates("default",), readFileSync(caCertPath, "utf-8",),];
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error,);
			throw new Error(`Unable to read CA certificate bundle at ${caCertPath}: ${message}`, {
				cause: error,
			},);
		}
	}

	return tls;
}

/**
 * True when the URL embeds userinfo (`https://user:password@host`). Embedded
 * credentials are rejected up front so they can never reach the canonical base
 * URL that gets persisted in recorded artifacts (cleanup ledgers, permission
 * snapshots) or echoed in diagnostics. Malformed URLs fall through to the
 * existing request-time failure.
 */
function hasEmbeddedUserinfo(url: string,): boolean {
	try {
		const parsed = new URL(url,);
		return parsed.username !== "" || parsed.password !== "";
	} catch {
		return false;
	}
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
	private readonly tlsOptions: FetchTlsOptions | undefined;
	private readonly onTrace: (event: DataikuClientTraceEvent,) => void;
	private readonly onValidationWarning: (method: string, errors: string[],) => void;

	/* Resource namespaces — lazily initialized to break circular imports */
	private projectsResource?: ProjectsResource;
	private datasetsResource?: DatasetsResource;
	private recipesResource?: RecipesResource;
	private dashboardsResource?: DashboardsResource;
	private dataQualityResource?: DataQualityResource;
	private jobsResource?: JobsResource;
	private futuresResource?: FuturesResource;
	private scenariosResource?: ScenariosResource;
	private foldersResource?: FoldersResource;
	private flowZonesResource?: FlowZonesResource;
	private variablesResource?: VariablesResource;
	private connectionsResource?: ConnectionsResource;
	private codeEnvsResource?: CodeEnvsResource;
	private insightsResource?: InsightsResource;
	private sqlResource?: SqlResource;
	private notebooksResource?: NotebooksResource;
	private wikiResource?: WikiResource;
	private applicationsResource?: ApplicationsResource;
	private webappsResource?: WebappsResource;
	private apiServicesResource?: ApiServicesResource;
	private apiDeployerResource?: ApiDeployerResource;
	private bundlesResource?: BundlesResource;
	private projectDeployerResource?: ProjectDeployerResource;
	private projectLibraryResource?: ProjectLibraryResource;
	private projectGitResource?: ProjectGitResource;
	private streamingEndpointsResource?: StreamingEndpointsResource;
	private continuousActivitiesResource?: ContinuousActivitiesResource;
	private statisticsResource?: StatisticsResource;
	private discussionsResource?: DiscussionsResource;
	private workspacesResource?: WorkspacesResource;
	private metricsResource?: MetricsResource;
	private meaningsResource?: MeaningsResource;
	private analysesResource?: AnalysesResource;
	private mlTasksResource?: MlTasksResource;
	private savedModelsResource?: SavedModelsResource;
	private modelEvaluationStoresResource?: ModelEvaluationStoresResource;

	get projects(): ProjectsResource {
		return (this.projectsResource ??= new ProjectsResource(this,));
	}
	get datasets(): DatasetsResource {
		return (this.datasetsResource ??= new DatasetsResource(this,));
	}
	get dashboards(): DashboardsResource {
		return (this.dashboardsResource ??= new DashboardsResource(this,));
	}
	get dataQuality(): DataQualityResource {
		return (this.dataQualityResource ??= new DataQualityResource(this,));
	}
	get recipes(): RecipesResource {
		return (this.recipesResource ??= new RecipesResource(this,));
	}
	get jobs(): JobsResource {
		return (this.jobsResource ??= new JobsResource(this,));
	}
	get futures(): FuturesResource {
		return (this.futuresResource ??= new FuturesResource(this,));
	}
	get scenarios(): ScenariosResource {
		return (this.scenariosResource ??= new ScenariosResource(this,));
	}
	get folders(): FoldersResource {
		return (this.foldersResource ??= new FoldersResource(this,));
	}
	get flowZones(): FlowZonesResource {
		return (this.flowZonesResource ??= new FlowZonesResource(this,));
	}
	get variables(): VariablesResource {
		return (this.variablesResource ??= new VariablesResource(this,));
	}
	get connections(): ConnectionsResource {
		return (this.connectionsResource ??= new ConnectionsResource(this,));
	}
	get codeEnvs(): CodeEnvsResource {
		return (this.codeEnvsResource ??= new CodeEnvsResource(this,));
	}
	get insights(): InsightsResource {
		return (this.insightsResource ??= new InsightsResource(this,));
	}
	get sql(): SqlResource {
		return (this.sqlResource ??= new SqlResource(this,));
	}
	get notebooks(): NotebooksResource {
		return (this.notebooksResource ??= new NotebooksResource(this,));
	}
	get wiki(): WikiResource {
		return (this.wikiResource ??= new WikiResource(this,));
	}
	get applications(): ApplicationsResource {
		return (this.applicationsResource ??= new ApplicationsResource(this,));
	}
	get webapps(): WebappsResource {
		return (this.webappsResource ??= new WebappsResource(this,));
	}
	get apiServices(): ApiServicesResource {
		return (this.apiServicesResource ??= new ApiServicesResource(this,));
	}
	get apiDeployer(): ApiDeployerResource {
		return (this.apiDeployerResource ??= new ApiDeployerResource(this,));
	}
	get bundles(): BundlesResource {
		return (this.bundlesResource ??= new BundlesResource(this,));
	}
	get projectDeployer(): ProjectDeployerResource {
		return (this.projectDeployerResource ??= new ProjectDeployerResource(this,));
	}
	get projectLibrary(): ProjectLibraryResource {
		return (this.projectLibraryResource ??= new ProjectLibraryResource(this,));
	}
	get projectGit(): ProjectGitResource {
		return (this.projectGitResource ??= new ProjectGitResource(this,));
	}
	get streamingEndpoints(): StreamingEndpointsResource {
		return (this.streamingEndpointsResource ??= new StreamingEndpointsResource(this,));
	}
	get continuousActivities(): ContinuousActivitiesResource {
		return (this.continuousActivitiesResource ??= new ContinuousActivitiesResource(this,));
	}
	get statistics(): StatisticsResource {
		return (this.statisticsResource ??= new StatisticsResource(this,));
	}
	get discussions(): DiscussionsResource {
		return (this.discussionsResource ??= new DiscussionsResource(this,));
	}
	get workspaces(): WorkspacesResource {
		return (this.workspacesResource ??= new WorkspacesResource(this,));
	}
	get metrics(): MetricsResource {
		return (this.metricsResource ??= new MetricsResource(this,));
	}
	get meanings(): MeaningsResource {
		return (this.meaningsResource ??= new MeaningsResource(this,));
	}
	get analyses(): AnalysesResource {
		return (this.analysesResource ??= new AnalysesResource(this,));
	}
	get mlTasks(): MlTasksResource {
		return (this.mlTasksResource ??= new MlTasksResource(this,));
	}
	get savedModels(): SavedModelsResource {
		return (this.savedModelsResource ??= new SavedModelsResource(this,));
	}
	get modelEvaluationStores(): ModelEvaluationStoresResource {
		return (this.modelEvaluationStoresResource ??= new ModelEvaluationStoresResource(this,));
	}

	constructor(config?: DataikuClientConfig,) {
		const envUrl = process.env["DATAIKU_URL"]?.trim();
		const envApiKey = process.env["DATAIKU_API_KEY"]?.trim();
		const url = config?.url?.trim() || envUrl;
		const apiKey = config?.apiKey?.trim() || envApiKey;
		if (!url || !apiKey) {
			throw new Error(
				"Dataiku URL and API key are required: pass {url, apiKey} or set DATAIKU_URL/DATAIKU_API_KEY",
			);
		}
		if (hasEmbeddedUserinfo(url,)) {
			throw new ClientValidationError(
				"Dataiku URL must not contain embedded credentials (userinfo). Authenticate with an API key instead.",
				"validation_failed",
				"Pass the DSS base URL without a username or password.",
				{ urlHasEmbeddedUserinfo: true, },
			);
		}

		this.baseUrl = url.replace(/\/+$/, "",);
		this.apiKey = apiKey;
		this.defaultProjectKey = config?.projectKey?.trim() || undefined;
		this.requestTimeoutMs = config?.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;

		const rawMax = config?.retryMaxAttempts ?? DEFAULT_RETRY_MAX_ATTEMPTS;
		this.retryMaxAttempts = Math.min(Math.max(1, rawMax,), MAX_RETRY_ATTEMPTS_CAP,);
		this.verbose = config?.verbose === true;
		this.tlsOptions = config ? buildFetchTlsOptions(config,) : undefined;
		this.onTrace = config?.onTrace ?? defaultTrace;
		this.onValidationWarning = config?.onValidationWarning ?? defaultValidationWarning;
	}

	/**
	 * Canonical request base URL: the exact prefix every HTTP verb concatenates
	 * paths onto (trimmed, trailing slashes stripped). Callers that must bind a
	 * recorded artifact to its originating DSS server compare against this.
	 */
	getBaseUrl(): string {
		return this.baseUrl;
	}

	getRequestTimeoutMs(): number {
		return this.requestTimeoutMs;
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
	/**
	 * GET returning the parsed JSON body plus selected response metadata
	 * (documented DSS version headers, server date, request id). Authorization
	 * headers are never included.
	 */
	async getWithMetadata<T = unknown,>(
		path: string,
	): Promise<{ data: T; meta: DataikuClientResponseMeta; }> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getHeaders(),
		},);
		const data = await this.parseJsonResponse<T>(res,);
		return { data, meta: this.responseMeta(res.headers,), };
	}

	async getText(path: string,): Promise<string> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		return res.text();
	}

	async getTextLimited(
		path: string,
		maxBytes: number,
	): Promise<{ text: string; truncated: boolean; }> {
		const limit = Math.max(0, Math.floor(maxBytes,),);
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);

		if (!res.body) return { text: "", truncated: false, };

		const reader = res.body.getReader();
		const chunks: Uint8Array[] = [];
		let bytesRead = 0;
		let truncated = false;

		try {
			while (bytesRead < limit) {
				const { done, value, } = await reader.read();
				if (done) break;
				const room = limit - bytesRead;
				if (value.byteLength > room) {
					chunks.push(value.slice(0, room,),);
					bytesRead += room;
					truncated = true;
					break;
				}
				chunks.push(value,);
				bytesRead += value.byteLength;
			}
			if (!truncated && bytesRead >= limit) {
				// Buffer filled exactly on a chunk boundary; peek whether more data remains.
				const { done, } = await reader.read();
				if (!done) truncated = true;
			}
		} finally {
			await reader.cancel();
		}

		const collected = concatBytes(chunks, bytesRead,);
		// On truncation, drop any trailing partial UTF-8 character so the decoded
		// text never re-encodes beyond the byte cap (a stream-flush would otherwise
		// emit a 3-byte replacement char for a split multibyte sequence).
		const usable = truncated ? trimToUtf8Boundary(collected,) : collected;
		return { text: new TextDecoder().decode(usable,), truncated, };
	}

	/**
	 * POST with optional transient retry. Only opt in when repeating the request
	 * is safe: the server may have accepted an attempt whose response was lost.
	 */
	async post<T = unknown,>(
		path: string,
		body?: unknown,
		options?: { retryMaxAttempts?: number; },
	): Promise<T> {
		const retryMaxAttempts = options?.retryMaxAttempts;
		if (
			retryMaxAttempts !== undefined
			&& (!Number.isInteger(retryMaxAttempts,) || retryMaxAttempts < 1)
		) {
			throw new ClientValidationError("retryMaxAttempts must be a positive integer.",);
		}
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: this.getHeaders(),
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		}, retryMaxAttempts,);
		return this.parseJsonResponse<T>(res,);
	}

	async postText(path: string, body?: unknown,): Promise<string> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: this.getHeaders(),
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		},);
		return res.text();
	}

	async postStream(path: string, body?: unknown,): Promise<Response> {
		return this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: { ...this.getAnyHeaders(), "Content-Type": "application/json", },
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		},);
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

	private async uploadResponse(
		path: string,
		filePath: string,
		fileName?: string,
	): Promise<Response> {
		const fileBlob = await openAsBlob(filePath,);
		const formData = new FormData();
		formData.append("file", fileBlob, fileName ?? basename(filePath,),);

		return this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${this.apiKey}`, },
			body: formData,
		},);
	}

	async upload(path: string, filePath: string, fileName?: string,): Promise<void> {
		await this.uploadResponse(path, filePath, fileName,);
	}

	async uploadJson<T,>(path: string, filePath: string, fileName?: string,): Promise<T> {
		const response = await this.uploadResponse(path, filePath, fileName,);
		return this.parseJsonResponse<T>(response,);
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
	private responseMeta(headers: Headers,): DataikuClientResponseMeta {
		// Fixed non-sensitive whitelist. `Headers.get` is case-insensitive.
		return {
			dssVersion: headers.get("dss-version",),
			dssApiVersion: headers.get("dss-api-version",),
			date: headers.get("date",),
			requestId: this.requestIdFromHeaders(headers,) ?? null,
		};
	}

	private logTrace(event: Omit<DataikuClientTraceEvent, "type">,): void {
		if (this.verbose) this.onTrace({ type: "trace", ...event, },);
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
	 * Validate raw data against a TypeBox schema without throwing, even when
	 * mismatched values are not JSON-serializable. Always returns the original
	 * data, and on mismatch emits onValidationWarning with the method name and
	 * error details. If the callback throws, that error still propagates.
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

	private requestIdFromHeaders(headers: Headers,): string | undefined {
		for (
			const name of [
				"x-request-id",
				"x-dku-request-id",
				"x-dataiku-request-id",
				"x-correlation-id",
				"x-amzn-requestid",
			]
		) {
			const value = headers.get(name,);
			if (value) return value;
		}
		return undefined;
	}

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
				undefined,
				this.requestIdFromHeaders(res.headers,),
			);
		}
	}

	/* ---- private: retry loop ---- */

	private async fetchWithRetry(
		url: string,
		init: RequestInit,
		retryMaxAttempts?: number,
	): Promise<Response> {
		const method = (init.method ?? "GET").toUpperCase();
		const retryEnabled = shouldRetryMethod(method,) || retryMaxAttempts !== undefined;
		const maxAttempts = retryMaxAttempts === undefined
			? retryEnabled
				? this.retryMaxAttempts
				: 1
			: Math.min(retryMaxAttempts, MAX_RETRY_ATTEMPTS_CAP,);
		const delaysMs: number[] = [];

		for (let attempt = 1; attempt <= maxAttempts; attempt++) {
			let timedOut = false;
			const startedAt = Date.now();
			const controller = new AbortController();
			const timeout = setTimeout(() => {
				timedOut = true;
				controller.abort();
			}, this.requestTimeoutMs,);

			this.logTrace({ phase: "request", method, url, attempt, maxAttempts, },);

			try {
				const requestInit: RequestInit & { tls?: FetchTlsOptions; } = {
					...init,
					method,
					signal: controller.signal,
				};
				if (this.tlsOptions) requestInit.tls = this.tlsOptions;
				const res = await fetch(url, requestInit,);
				this.logTrace({
					phase: "response",
					method,
					url,
					attempt,
					maxAttempts,
					status: res.status,
					elapsedMs: Date.now() - startedAt,
				},);
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
						this.requestIdFromHeaders(res.headers,),
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
				this.logTrace({
					phase: "error",
					method,
					url,
					attempt,
					maxAttempts,
					elapsedMs: Date.now() - startedAt,
					detail,
				},);
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
