import { readFileSync, } from "node:fs";
import { basename, } from "node:path";

import { type Static, type TSchema, } from "@sinclair/typebox";
import { Value, } from "@sinclair/typebox/value";
import { type SafeParseResult, safeParseSchema, } from "./schemas.js";

import {
	classifyDataikuError,
	ClientValidationError,
	DataikuError,
	type DataikuRetryMetadata,
	nonJsonResponseBody,
} from "./errors.js";

import type { AnalysesResource, } from "./resources/analyses.js";
import type { ApiDeployerResource, } from "./resources/api-deployer.js";
import type { ApiServicesResource, } from "./resources/api-services.js";
import type { ApplicationsResource, } from "./resources/applications.js";
import type { BundlesResource, ProjectDeployerResource, } from "./resources/bundles.js";
import type { CodeEnvsResource, } from "./resources/code-envs.js";
import type { ConnectionsResource, } from "./resources/connections.js";
import type { ContinuousActivitiesResource, } from "./resources/continuous-activities.js";
import type { DashboardsResource, } from "./resources/dashboards.js";
import type { DataCollectionsResource, } from "./resources/data-collections.js";
import type { DataQualityResource, } from "./resources/data-quality.js";
import type { DatasetsResource, } from "./resources/datasets.js";
import type { DiscussionsResource, } from "./resources/discussions.js";
import type { FlowZonesResource, } from "./resources/flow-zones.js";
import type { FoldersResource, } from "./resources/folders.js";
import type { FuturesResource, } from "./resources/futures.js";
import type { GroupsResource, } from "./resources/groups.js";
import type { InsightsResource, } from "./resources/insights.js";
import type { JobsResource, } from "./resources/jobs.js";
import type { KnowledgeBanksResource, } from "./resources/knowledge-banks.js";
import type { LlmsResource, } from "./resources/llms.js";
import type { MacrosResource, } from "./resources/macros.js";
import type { MeaningsResource, } from "./resources/meanings.js";
import type { MetricsResource, } from "./resources/metrics.js";
import type { MlTasksResource, } from "./resources/ml-tasks.js";
import type { ModelEvaluationStoresResource, } from "./resources/model-evaluation-stores.js";
import type { NotebooksResource, } from "./resources/notebooks.js";
import type { PluginsResource, } from "./resources/plugins.js";
import type { ProjectFoldersResource, } from "./resources/project-folders.js";
import type { ProjectGitResource, } from "./resources/project-git.js";
import type { ProjectLibraryResource, } from "./resources/project-library.js";
import type { ProjectsResource, } from "./resources/projects.js";
import type { RecipesResource, } from "./resources/recipes.js";
import type { SavedModelsResource, } from "./resources/saved-models.js";
import type { ScenariosResource, } from "./resources/scenarios.js";
import type { SqlResource, } from "./resources/sql.js";
import type { StatisticsResource, } from "./resources/statistics.js";
import type { StreamingEndpointsResource, } from "./resources/streaming-endpoints.js";
import type { UsersResource, } from "./resources/users.js";
import type { VariablesResource, } from "./resources/variables.js";
import type { WebappsResource, } from "./resources/webapps.js";
import type { WikiResource, } from "./resources/wiki.js";
import type { WorkspacesResource, } from "./resources/workspaces.js";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const DEFAULT_RETRY_MAX_ATTEMPTS = 4;
const MAX_RETRY_ATTEMPTS_CAP = 10;
const BASE_DELAY_MS = 2_000;
const MAX_BACKOFF_DELAY_MS = 30_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
/**
 * Default cap on bytes buffered from a single response body by the text/JSON
 * consumers (getText, postText, parseJsonResponse). Bodies larger than this
 * are rejected with a DataikuError after the stream is cancelled; raw
 * streaming methods (stream, postStream) are unaffected.
 */
const DEFAULT_MAX_BUFFERED_BODY_BYTES = 50 * 1024 * 1024;
const RESPONSE_TOO_LARGE_STATUS_TEXT = "Response Too Large";

/** One part of a {@link DataikuClient.uploadForm} multipart/form-data body. */
export interface UploadFormPart {
	/** Form field name. */
	name: string;
	/** Text field value. Mutually exclusive with `blob`/`fileName` file parts. */
	value?: string;
	/** File content. Omit (or null) together with `fileName` for a zero-byte file part. */
	blob?: Blob | null;
	/** Filename for file parts. */
	fileName?: string;
}

/**
 * Multipart part modes for {@link DataikuClient.uploadForm}:
 * - `value`: a form text field;
 * - neither `blob` nor `fileName`: a filename-less, zero-length field part —
 *   the official Python client's files={"file": (None, None)} shape required
 *   by the managed-folder MLflow import (a Blob would add `filename=`, even
 *   when empty);
 * - `blob` (optionally with `fileName`): a file part.
 */

/** Per-call overrides for {@link DataikuClient.get}. */
export interface DataikuGetOptions {
	/**
	 * TOTAL duration budget in milliseconds for this call, measured from call
	 * start: it bounds every retry attempt's fetch, the backoff sleeps between
	 * attempts, and the response body read. When the budget is exhausted the
	 * request is aborted and a timeout DataikuError is thrown. The client-level
	 * requestTimeoutMs still caps each attempt and buffered body read; this
	 * budget may shorten those limits but never extends them.
	 */
	timeoutMs?: number;
	/**
	 * Issue exactly one transport attempt (no retries). Independent of
	 * `timeoutMs`: when both are supplied the total budget above still applies
	 * and aborts the attempt; when no budget is supplied the attempt is capped
	 * by the client-level `requestTimeoutMs`. Wait loops use this for the one
	 * observation whose wait budget is already spent, so it still reaches the
	 * server once instead of failing before any transport attempt.
	 */
	noRetry?: boolean;
}

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
	/** Request/header timeout and buffered-body budget; idle timeout per raw stream read (default 30 000 ms). */
	requestTimeoutMs?: number;
	/** Maximum total attempts, initial request included, for idempotent requests (default 4, capped at 10; 1 = no retry) */
	retryMaxAttempts?: number;
	/**
	 * Maximum bytes retained from a single response body by text/JSON consumers
	 * (default 50 MiB). Oversized successful responses are rejected. Error
	 * responses retain a bounded prefix with DataikuError.bodyTruncated=true.
	 */
	maxResponseBodyBytes?: number;
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

/** Server-directed waits never bypass the existing automatic backoff cap. */
function retryAfterDelayMs(value: string | null,): number {
	if (value === null) return 0;
	const text = value.trim();
	if (/^\d+$/.test(text,)) return Number(text,) * 1_000;
	// Accept HTTP dates, not Date.parse's permissive numeric date shorthand.
	if (!/^[A-Za-z]{3,9},? /.test(text,)) return 0;
	const date = Date.parse(text,);
	return Number.isFinite(date,) ? Math.max(0, date - Date.now(),) : 0;
}

function isTransientError(status: number, body: string,): boolean {
	return classifyDataikuError(status, body,).category === "transient";
}

function shouldRetryMethod(method: string,): boolean {
	return method.toUpperCase() === "GET";
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

/**
 * Drop leading UTF-8 continuation bytes. A UTF-8 character never *starts*
 * with a continuation byte, so any leading continuation bytes are the remains
 * of a character split by a byte cut at the start of the retained range.
 * Dropping them keeps the decoded text free of replacement characters.
 */
function trimFromUtf8BoundaryStart(bytes: Uint8Array,): Uint8Array {
	let i = 0;
	while (i < bytes.length && (bytes[i] & 0xC0) === 0x80) {
		i++;
	}
	return i > 0 ? bytes.subarray(i,) : bytes;
}

function buildBodyReadTimeoutError(timeoutMs: number,): DataikuError {
	return new DataikuError(
		0,
		"Request Timeout",
		`Request timed out after ${timeoutMs}ms while reading the response body.`,
	);
}

/**
 * Read the next chunk of a response body under a hard deadline.
 * When `remainingMs` elapses, the stream is cancelled and the caller's
 * promise rejects with a DataikuError, so a stalled body cannot hang the
 * consumers after headers already arrived.
 */
async function readChunkWithDeadline(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	remainingMs: number,
	timeoutMs: number,
): Promise<Bun.ReadableStreamDefaultReadResult<Uint8Array>> {
	const { promise, resolve, reject, } = Promise.withResolvers<
		Bun.ReadableStreamDefaultReadResult<Uint8Array>
	>();
	const timer = setTimeout(() => {
		void reader.cancel(buildBodyReadTimeoutError(timeoutMs,),).catch(() => {},);
		reject(buildBodyReadTimeoutError(timeoutMs,),);
	}, remainingMs,);
	reader.read().then(
		(result,) => {
			clearTimeout(timer,);
			resolve(result,);
		},
		(error,) => {
			clearTimeout(timer,);
			reject(error,);
		},
	);
	return promise;
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
			// node:tls costs ~12 ms to load; only a custom CA bundle needs it.
			const { getCACertificates, } = require("node:tls",) as typeof import("node:tls");
			tls.ca = [...getCACertificates("default",), readFileSync(caCertPath, "utf-8",),];
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error,);
			throw new ClientValidationError(
				`Unable to read CA certificate bundle at ${caCertPath}: ${message}`,
				"invalid_flag_value",
				undefined,
				undefined,
				{
					cause: error,
				},
			);
		}
	}

	return tls;
}

/**
 * Resolve the total-budget request deadline timestamp for a per-call GET
 * budget: `Date.now() + timeoutMs`. Undefined when no budget is supplied, in
 * which case the client-level per-attempt timeout applies unchanged.
 */
function resolveGetDeadlineAt(
	options?: DataikuGetOptions,
): number | undefined {
	const timeoutMs = options?.timeoutMs;
	if (timeoutMs === undefined) return undefined;
	if (!Number.isInteger(timeoutMs,) || timeoutMs < 1) {
		throw new ClientValidationError("timeoutMs must be a positive integer.",);
	}
	return Date.now() + timeoutMs;
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
	private readonly maxResponseBodyBytes: number;
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
	private projectFoldersResource?: ProjectFoldersResource;
	private dataCollectionsResource?: DataCollectionsResource;
	private llmsResource?: LlmsResource;
	private knowledgeBanksResource?: KnowledgeBanksResource;
	private macrosResource?: MacrosResource;
	private pluginsResource?: PluginsResource;
	private usersResource?: UsersResource;
	private groupsResource?: GroupsResource;

	// Resource modules load on first access (like commands/index.ts): a command
	// that touches one resource does not parse the other forty-odd.
	get projects(): ProjectsResource {
		if (!this.projectsResource) {
			const { ProjectsResource: Resource, } = require(
				"./resources/projects.js",
			) as typeof import("./resources/projects.js");
			this.projectsResource = new Resource(this,);
		}
		return this.projectsResource;
	}
	get datasets(): DatasetsResource {
		if (!this.datasetsResource) {
			const { DatasetsResource: Resource, } = require(
				"./resources/datasets.js",
			) as typeof import("./resources/datasets.js");
			this.datasetsResource = new Resource(this,);
		}
		return this.datasetsResource;
	}
	get dashboards(): DashboardsResource {
		if (!this.dashboardsResource) {
			const { DashboardsResource: Resource, } = require(
				"./resources/dashboards.js",
			) as typeof import("./resources/dashboards.js");
			this.dashboardsResource = new Resource(this,);
		}
		return this.dashboardsResource;
	}
	get dataQuality(): DataQualityResource {
		if (!this.dataQualityResource) {
			const { DataQualityResource: Resource, } = require(
				"./resources/data-quality.js",
			) as typeof import("./resources/data-quality.js");
			this.dataQualityResource = new Resource(this,);
		}
		return this.dataQualityResource;
	}
	get recipes(): RecipesResource {
		if (!this.recipesResource) {
			const { RecipesResource: Resource, } = require(
				"./resources/recipes.js",
			) as typeof import("./resources/recipes.js");
			this.recipesResource = new Resource(this,);
		}
		return this.recipesResource;
	}
	get jobs(): JobsResource {
		if (!this.jobsResource) {
			const { JobsResource: Resource, } = require(
				"./resources/jobs.js",
			) as typeof import("./resources/jobs.js");
			this.jobsResource = new Resource(this,);
		}
		return this.jobsResource;
	}
	get futures(): FuturesResource {
		if (!this.futuresResource) {
			const { FuturesResource: Resource, } = require(
				"./resources/futures.js",
			) as typeof import("./resources/futures.js");
			this.futuresResource = new Resource(this,);
		}
		return this.futuresResource;
	}
	get scenarios(): ScenariosResource {
		if (!this.scenariosResource) {
			const { ScenariosResource: Resource, } = require(
				"./resources/scenarios.js",
			) as typeof import("./resources/scenarios.js");
			this.scenariosResource = new Resource(this,);
		}
		return this.scenariosResource;
	}
	get folders(): FoldersResource {
		if (!this.foldersResource) {
			const { FoldersResource: Resource, } = require(
				"./resources/folders.js",
			) as typeof import("./resources/folders.js");
			this.foldersResource = new Resource(this,);
		}
		return this.foldersResource;
	}
	get flowZones(): FlowZonesResource {
		if (!this.flowZonesResource) {
			const { FlowZonesResource: Resource, } = require(
				"./resources/flow-zones.js",
			) as typeof import("./resources/flow-zones.js");
			this.flowZonesResource = new Resource(this,);
		}
		return this.flowZonesResource;
	}
	get variables(): VariablesResource {
		if (!this.variablesResource) {
			const { VariablesResource: Resource, } = require(
				"./resources/variables.js",
			) as typeof import("./resources/variables.js");
			this.variablesResource = new Resource(this,);
		}
		return this.variablesResource;
	}
	get connections(): ConnectionsResource {
		if (!this.connectionsResource) {
			const { ConnectionsResource: Resource, } = require(
				"./resources/connections.js",
			) as typeof import("./resources/connections.js");
			this.connectionsResource = new Resource(this,);
		}
		return this.connectionsResource;
	}
	get codeEnvs(): CodeEnvsResource {
		if (!this.codeEnvsResource) {
			const { CodeEnvsResource: Resource, } = require(
				"./resources/code-envs.js",
			) as typeof import("./resources/code-envs.js");
			this.codeEnvsResource = new Resource(this,);
		}
		return this.codeEnvsResource;
	}
	get insights(): InsightsResource {
		if (!this.insightsResource) {
			const { InsightsResource: Resource, } = require(
				"./resources/insights.js",
			) as typeof import("./resources/insights.js");
			this.insightsResource = new Resource(this,);
		}
		return this.insightsResource;
	}
	get sql(): SqlResource {
		if (!this.sqlResource) {
			const { SqlResource: Resource, } = require(
				"./resources/sql.js",
			) as typeof import("./resources/sql.js");
			this.sqlResource = new Resource(this,);
		}
		return this.sqlResource;
	}
	get notebooks(): NotebooksResource {
		if (!this.notebooksResource) {
			const { NotebooksResource: Resource, } = require(
				"./resources/notebooks.js",
			) as typeof import("./resources/notebooks.js");
			this.notebooksResource = new Resource(this,);
		}
		return this.notebooksResource;
	}
	get wiki(): WikiResource {
		if (!this.wikiResource) {
			const { WikiResource: Resource, } = require(
				"./resources/wiki.js",
			) as typeof import("./resources/wiki.js");
			this.wikiResource = new Resource(this,);
		}
		return this.wikiResource;
	}
	get applications(): ApplicationsResource {
		if (!this.applicationsResource) {
			const { ApplicationsResource: Resource, } = require(
				"./resources/applications.js",
			) as typeof import("./resources/applications.js");
			this.applicationsResource = new Resource(this,);
		}
		return this.applicationsResource;
	}
	get webapps(): WebappsResource {
		if (!this.webappsResource) {
			const { WebappsResource: Resource, } = require(
				"./resources/webapps.js",
			) as typeof import("./resources/webapps.js");
			this.webappsResource = new Resource(this,);
		}
		return this.webappsResource;
	}
	get apiServices(): ApiServicesResource {
		if (!this.apiServicesResource) {
			const { ApiServicesResource: Resource, } = require(
				"./resources/api-services.js",
			) as typeof import("./resources/api-services.js");
			this.apiServicesResource = new Resource(this,);
		}
		return this.apiServicesResource;
	}
	get apiDeployer(): ApiDeployerResource {
		if (!this.apiDeployerResource) {
			const { ApiDeployerResource: Resource, } = require(
				"./resources/api-deployer.js",
			) as typeof import("./resources/api-deployer.js");
			this.apiDeployerResource = new Resource(this,);
		}
		return this.apiDeployerResource;
	}
	get bundles(): BundlesResource {
		if (!this.bundlesResource) {
			const { BundlesResource: Resource, } = require(
				"./resources/bundles.js",
			) as typeof import("./resources/bundles.js");
			this.bundlesResource = new Resource(this,);
		}
		return this.bundlesResource;
	}
	get projectDeployer(): ProjectDeployerResource {
		if (!this.projectDeployerResource) {
			const { ProjectDeployerResource: Resource, } = require(
				"./resources/bundles.js",
			) as typeof import("./resources/bundles.js");
			this.projectDeployerResource = new Resource(this,);
		}
		return this.projectDeployerResource;
	}
	get projectLibrary(): ProjectLibraryResource {
		if (!this.projectLibraryResource) {
			const { ProjectLibraryResource: Resource, } = require(
				"./resources/project-library.js",
			) as typeof import("./resources/project-library.js");
			this.projectLibraryResource = new Resource(this,);
		}
		return this.projectLibraryResource;
	}
	get projectGit(): ProjectGitResource {
		if (!this.projectGitResource) {
			const { ProjectGitResource: Resource, } = require(
				"./resources/project-git.js",
			) as typeof import("./resources/project-git.js");
			this.projectGitResource = new Resource(this,);
		}
		return this.projectGitResource;
	}
	get streamingEndpoints(): StreamingEndpointsResource {
		if (!this.streamingEndpointsResource) {
			const { StreamingEndpointsResource: Resource, } = require(
				"./resources/streaming-endpoints.js",
			) as typeof import("./resources/streaming-endpoints.js");
			this.streamingEndpointsResource = new Resource(this,);
		}
		return this.streamingEndpointsResource;
	}
	get continuousActivities(): ContinuousActivitiesResource {
		if (!this.continuousActivitiesResource) {
			const { ContinuousActivitiesResource: Resource, } = require(
				"./resources/continuous-activities.js",
			) as typeof import("./resources/continuous-activities.js");
			this.continuousActivitiesResource = new Resource(this,);
		}
		return this.continuousActivitiesResource;
	}
	get statistics(): StatisticsResource {
		if (!this.statisticsResource) {
			const { StatisticsResource: Resource, } = require(
				"./resources/statistics.js",
			) as typeof import("./resources/statistics.js");
			this.statisticsResource = new Resource(this,);
		}
		return this.statisticsResource;
	}
	get discussions(): DiscussionsResource {
		if (!this.discussionsResource) {
			const { DiscussionsResource: Resource, } = require(
				"./resources/discussions.js",
			) as typeof import("./resources/discussions.js");
			this.discussionsResource = new Resource(this,);
		}
		return this.discussionsResource;
	}
	get workspaces(): WorkspacesResource {
		if (!this.workspacesResource) {
			const { WorkspacesResource: Resource, } = require(
				"./resources/workspaces.js",
			) as typeof import("./resources/workspaces.js");
			this.workspacesResource = new Resource(this,);
		}
		return this.workspacesResource;
	}
	get metrics(): MetricsResource {
		if (!this.metricsResource) {
			const { MetricsResource: Resource, } = require(
				"./resources/metrics.js",
			) as typeof import("./resources/metrics.js");
			this.metricsResource = new Resource(this,);
		}
		return this.metricsResource;
	}
	get meanings(): MeaningsResource {
		if (!this.meaningsResource) {
			const { MeaningsResource: Resource, } = require(
				"./resources/meanings.js",
			) as typeof import("./resources/meanings.js");
			this.meaningsResource = new Resource(this,);
		}
		return this.meaningsResource;
	}
	get analyses(): AnalysesResource {
		if (!this.analysesResource) {
			const { AnalysesResource: Resource, } = require(
				"./resources/analyses.js",
			) as typeof import("./resources/analyses.js");
			this.analysesResource = new Resource(this,);
		}
		return this.analysesResource;
	}
	get mlTasks(): MlTasksResource {
		if (!this.mlTasksResource) {
			const { MlTasksResource: Resource, } = require(
				"./resources/ml-tasks.js",
			) as typeof import("./resources/ml-tasks.js");
			this.mlTasksResource = new Resource(this,);
		}
		return this.mlTasksResource;
	}
	get savedModels(): SavedModelsResource {
		if (!this.savedModelsResource) {
			const { SavedModelsResource: Resource, } = require(
				"./resources/saved-models.js",
			) as typeof import("./resources/saved-models.js");
			this.savedModelsResource = new Resource(this,);
		}
		return this.savedModelsResource;
	}
	get modelEvaluationStores(): ModelEvaluationStoresResource {
		if (!this.modelEvaluationStoresResource) {
			const { ModelEvaluationStoresResource: Resource, } = require(
				"./resources/model-evaluation-stores.js",
			) as typeof import("./resources/model-evaluation-stores.js");
			this.modelEvaluationStoresResource = new Resource(this,);
		}
		return this.modelEvaluationStoresResource;
	}
	get projectFolders(): ProjectFoldersResource {
		if (!this.projectFoldersResource) {
			const { ProjectFoldersResource: Resource, } = require(
				"./resources/project-folders.js",
			) as typeof import("./resources/project-folders.js");
			this.projectFoldersResource = new Resource(this,);
		}
		return this.projectFoldersResource;
	}
	get dataCollections(): DataCollectionsResource {
		if (!this.dataCollectionsResource) {
			const { DataCollectionsResource: Resource, } = require(
				"./resources/data-collections.js",
			) as typeof import("./resources/data-collections.js");
			this.dataCollectionsResource = new Resource(this,);
		}
		return this.dataCollectionsResource;
	}
	get llms(): LlmsResource {
		if (!this.llmsResource) {
			const { LlmsResource: Resource, } = require(
				"./resources/llms.js",
			) as typeof import("./resources/llms.js");
			this.llmsResource = new Resource(this,);
		}
		return this.llmsResource;
	}
	get knowledgeBanks(): KnowledgeBanksResource {
		if (!this.knowledgeBanksResource) {
			const { KnowledgeBanksResource: Resource, } = require(
				"./resources/knowledge-banks.js",
			) as typeof import("./resources/knowledge-banks.js");
			this.knowledgeBanksResource = new Resource(this,);
		}
		return this.knowledgeBanksResource;
	}
	get macros(): MacrosResource {
		if (!this.macrosResource) {
			const { MacrosResource: Resource, } = require(
				"./resources/macros.js",
			) as typeof import("./resources/macros.js");
			this.macrosResource = new Resource(this,);
		}
		return this.macrosResource;
	}
	get plugins(): PluginsResource {
		if (!this.pluginsResource) {
			const { PluginsResource: Resource, } = require(
				"./resources/plugins.js",
			) as typeof import("./resources/plugins.js");
			this.pluginsResource = new Resource(this,);
		}
		return this.pluginsResource;
	}
	get users(): UsersResource {
		if (!this.usersResource) {
			const { UsersResource: Resource, } = require(
				"./resources/users.js",
			) as typeof import("./resources/users.js");
			this.usersResource = new Resource(this,);
		}
		return this.usersResource;
	}
	get groups(): GroupsResource {
		if (!this.groupsResource) {
			const { GroupsResource: Resource, } = require(
				"./resources/groups.js",
			) as typeof import("./resources/groups.js");
			this.groupsResource = new Resource(this,);
		}
		return this.groupsResource;
	}

	constructor(config?: DataikuClientConfig,) {
		/* oxlint-disable dss/no-direct-process-env -- SDK constructor defaults (DATAIKU_URL/DATAIKU_API_KEY) for library callers */
		const envUrl = process.env["DATAIKU_URL"]?.trim();
		const envApiKey = process.env["DATAIKU_API_KEY"]?.trim();
		/* oxlint-enable dss/no-direct-process-env */
		const url = config?.url?.trim() || envUrl;
		const apiKey = config?.apiKey?.trim() || envApiKey;
		if (!url || !apiKey) {
			throw new ClientValidationError(
				"Dataiku URL and API key are required: pass {url, apiKey} or set DATAIKU_URL/DATAIKU_API_KEY",
				"missing_required_flag",
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
		this.maxResponseBodyBytes = config?.maxResponseBodyBytes ?? DEFAULT_MAX_BUFFERED_BODY_BYTES;

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

	/**
	 * The configured maximum number of bytes buffered from a single response
	 * body by the text/JSON consumer methods (default 50 MiB).
	 */
	getMaxResponseBodyBytes(): number {
		return this.maxResponseBodyBytes;
	}

	/* ---- public: project key resolution ---- */

	resolveProjectKey(paramValue?: string,): string {
		const pk = paramValue?.trim();
		if (pk) return pk;
		if (this.defaultProjectKey) return this.defaultProjectKey;
		throw new ClientValidationError(
			"projectKey is required — pass it as a parameter or set projectKey in DataikuClientConfig",
			"missing_required_flag",
			"Pass --project-key or set DATAIKU_PROJECT_KEY.",
		);
	}

	/* ---- public: HTTP verbs ---- */

	async get<T = unknown,>(path: string, options?: DataikuGetOptions,): Promise<T> {
		const deadlineAt = resolveGetDeadlineAt(options,);
		const res = await this.fetchWithRetry(
			`${this.baseUrl}${path}`,
			{ method: "GET", headers: this.getHeaders(false,), },
			options?.noRetry === true ? 1 : undefined,
			deadlineAt,
		);
		return this.parseJsonResponse<T>(res, deadlineAt,);
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
			headers: this.getHeaders(false,),
		},);
		const data = await this.parseJsonResponse<T>(res,);
		return { data, meta: this.responseMeta(res.headers,), };
	}

	/**
	 * GET returning the entire text body. The body is buffered with a hard
	 * byte cap (`maxResponseBodyBytes`, default 50 MiB) and read under the
	 * request deadline: a response exceeding the cap throws a DataikuError
	 * instead of being silently truncated, and a stalled body times out.
	 */
	async getText(path: string,): Promise<string> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		const { text, truncated, } = await this.readBoundedBodyText(res, this.maxResponseBodyBytes,);
		if (truncated) throw this.buildResponseTooLargeError(res, this.maxResponseBodyBytes,);
		return text;
	}

	/**
	 * GET returning at most the first `maxBytes` bytes of a text body.
	 * When the response exceeds the cap the stream is cancelled, only the
	 * collected prefix is returned and `truncated` is true. The read runs
	 * under the request deadline: a stalled body throws a DataikuError.
	 */
	async getTextLimited(
		path: string,
		maxBytes: number,
	): Promise<{ text: string; truncated: boolean; }> {
		const limit = Math.max(0, Math.floor(maxBytes,),);
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		return this.readBoundedBodyText(res, limit,);
	}

	/**
	 * GET returning at most the last `maxBytes` bytes of a text body.
	 * When the response exceeds the cap, earlier bytes are discarded, only
	 * the tail is kept and `truncated` is true — so callers that need the end
	 * of a large document (e.g. job logs) stay bounded without losing the most
	 * recent content. The read runs under the request deadline.
	 */
	async getTextTailLimited(
		path: string,
		maxBytes: number,
	): Promise<{ text: string; truncated: boolean; }> {
		const limit = Math.max(0, Math.floor(maxBytes,),);
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		return this.readBodyTextTail(res, limit,);
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
			headers: this.getHeaders(body !== undefined,),
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		}, retryMaxAttempts,);
		return this.parseJsonResponse<T>(res,);
	}

	/**
	 * POST returning the text body, under the same bounded buffering rules as
	 * {@link getText} (byte cap `maxResponseBodyBytes`, request deadline).
	 */
	async postText(path: string, body?: unknown,): Promise<string> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: this.getHeaders(body !== undefined,),
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		},);
		const { text, truncated, } = await this.readBoundedBodyText(res, this.maxResponseBodyBytes,);
		if (truncated) throw this.buildResponseTooLargeError(res, this.maxResponseBodyBytes,);
		return text;
	}

	/** Send raw file contents through the shared authenticated transport. */
	async postRawBody(path: string, body: string | Uint8Array,): Promise<void> {
		const payload = typeof body === "string"
			? body
			: body.buffer instanceof ArrayBuffer
			? new Uint8Array(body.buffer, body.byteOffset, body.byteLength,)
			: new Uint8Array(body,);
		const response = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: this.getAnyHeaders(),
			body: payload,
		},);
		await response.text();
	}

	/** Git endpoints require Basic API-key authentication, unlike the bearer API. */
	async requestGit(
		method: string,
		path: string,
		body?: unknown,
		deadlineAt?: number,
		noRetry = false,
	): Promise<Response> {
		return this.fetchWithRetry(
			`${this.baseUrl}${path}`,
			{
				method,
				headers: {
					Authorization: `Basic ${Buffer.from(`${this.apiKey}:`, "utf8",).toString("base64",)}`,
					Accept: "application/json",
					"Content-Type": "application/json",
				},
				body: body === undefined ? undefined : JSON.stringify(body,),
			},
			noRetry ? 1 : undefined,
			deadlineAt,
		);
	}

	async postStream(path: string, body?: unknown,): Promise<Response> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "POST",
			headers: { ...this.getAnyHeaders(), "Content-Type": "application/json", },
			body: body !== undefined ? JSON.stringify(body,) : undefined,
		},);
		return this.withBodyDeadline(res,);
	}

	async put<T = unknown,>(path: string, body: unknown,): Promise<T> {
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "PUT",
			headers: this.getHeaders(true,),
			body: JSON.stringify(body,),
		},);
		return this.parseJsonResponse<T>(res,);
	}

	async del(path: string,): Promise<void> {
		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "DELETE",
			headers: this.getHeaders(false,),
		},);
	}

	async putVoid(path: string, body: unknown,): Promise<void> {
		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "PUT",
			headers: this.getHeaders(true,),
			body: JSON.stringify(body,),
		},);
	}

	/**
	 * PUT without a request body. A few DSS endpoints (bundle export) reject
	 * an empty JSON object body with 400 validation, and the official Python
	 * client sends no body at all for them.
	 */
	async putVoidNoBody(path: string,): Promise<void> {
		await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "PUT",
			headers: this.getHeaders(false,),
		},);
	}

	/**
	 * POST a multipart/form-data body built from explicit parts, with optional
	 * query parameters appended to the path, returning the parsed JSON body.
	 *
	 * A part with `value` becomes a form text field; a part with `blob`
	 * (or a `fileName` with no blob) becomes a file part — a `fileName` without
	 * blob yields a zero-byte file part, which some DSS import endpoints
	 * (e.g. saved-model MLflow version import) require for metadata-only parts.
	 * The `Content-Type` header is left to the runtime so the multipart
	 * boundary is set correctly.
	 */
	async uploadForm<T = unknown,>(
		path: string,
		parts: UploadFormPart[],
		query?: URLSearchParams,
	): Promise<T> {
		const formData = new FormData();
		for (const part of parts) {
			if (part.value !== undefined) {
				formData.append(part.name, part.value,);
				continue;
			}
			if ((part.blob ?? null) === null && (part.fileName ?? null) === null) {
				// Filename-less, zero-length field part — the shape the official
				// Python client produces with files={"file": (None, None)} and
				// the one DSS's managed-folder MLflow import requires (a Blob
				// would always add a `filename=` parameter, even when empty).
				formData.append(part.name, "",);
				continue;
			}
			const blob = part.blob ?? new Blob([],);
			formData.append(part.name, blob, part.fileName,);
		}
		const suffix = query && [...query.keys(),].length > 0 ? `?${query.toString()}` : "";
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}${suffix}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${this.apiKey}`, },
			body: formData,
		},);
		return this.parseJsonResponse<T>(res,);
	}

	private async uploadResponse(
		path: string,
		filePath: string,
		fileName?: string,
	): Promise<Response> {
		const fileBlob = Bun.file(filePath,);
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
		const res = await this.fetchWithRetry(`${this.baseUrl}${path}`, {
			method: "GET",
			headers: this.getAnyHeaders(),
		},);
		return this.withBodyDeadline(res,);
	}

	/** Bound each pending read, not total transfer time; preserve large downloads and backpressure. */
	private withBodyDeadline(res: Response,): Response {
		if (!res.body) return res;
		const reader = res.body.getReader();
		const timeoutMs = this.requestTimeoutMs;
		const body = new ReadableStream<Uint8Array>({
			async pull(controller,) {
				try {
					const { done, value, } = await readChunkWithDeadline(reader, timeoutMs, timeoutMs,);
					if (done) {
						controller.close();
						reader.releaseLock();
					} else {
						controller.enqueue(value,);
					}
				} catch (error) {
					controller.error(error,);
					void reader.cancel(error,).catch(() => {},);
				}
			},
			cancel(reason,) {
				void reader.cancel(reason,).catch(() => {},);
			},
		},);
		const response = new Response(body, {
			status: res.status,
			statusText: res.statusText,
			headers: res.headers,
		},);
		// ResponseInit has no fields for these fetch response properties.
		Object.defineProperties(response, {
			url: { value: res.url, },
			redirected: { value: res.redirected, },
			type: { value: res.type, },
		},);
		return response;
	}

	/* ---- private: headers ---- */

	/** JSON request headers; Content-Type only when a body is sent. */
	private getHeaders(withBody: boolean,): Record<string, string> {
		return {
			Authorization: `Bearer ${this.apiKey}`,
			Accept: "application/json",
			...(withBody ? { "Content-Type": "application/json", } : {}),
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

	private async parseJsonResponse<T,>(res: Response, deadlineAt?: number,): Promise<T> {
		const { text, truncated, } = await this.readBoundedBodyText(
			res,
			this.maxResponseBodyBytes,
			deadlineAt,
		);
		if (truncated) throw this.buildResponseTooLargeError(res, this.maxResponseBodyBytes,);
		// SAFETY: Empty 2xx responses from DSS are surfaced to callers as undefined
		// cast to T. This keeps existing call sites stable, but callers that rely on
		// an object shape must guard explicitly before dereferencing the result.
		if (!text) return undefined as T;
		try {
			return JSON.parse(text,) as T;
		} catch {
			throw new DataikuError(
				res.status,
				res.statusText || "Invalid JSON response",
				nonJsonResponseBody(text,),
				undefined,
				this.requestIdFromHeaders(res.headers,),
			);
		}
	}

	/* ---- private: bounded response body reading ---- */

	/**
	 * Read at most `maxBytes` of a response body as UTF-8 text.
	 *
	 * The body is consumed with the request deadline still active (measured
	 * from the first read): a stalled body throws a DataikuError instead of
	 * hanging. When the stream yields more than the cap, the reader cancels the
	 * body and returns the collected prefix with `truncated: true`; truncated
	 * text is trimmed to a UTF-8 character boundary so it never decodes beyond
	 * the byte cap.
	 */
	private async readBoundedBodyText(
		res: Response,
		maxBytes: number,
		deadlineAt?: number,
	): Promise<{ text: string; truncated: boolean; }> {
		const limit = Math.max(0, Math.floor(maxBytes,),);
		if (!res.body) return { text: "", truncated: false, };
		// Total-budget mode: the read shares the call's absolute deadline, so
		// the budget already spent on attempts/backoff is not re-credited here.
		const bodyTimeoutMs = deadlineAt === undefined
			? this.requestTimeoutMs
			: Math.min(this.requestTimeoutMs, Math.max(1, deadlineAt - Date.now(),),);

		const reader = res.body.getReader();
		const decoder = new TextDecoder();
		const parts: string[] = [];
		// Keep the final UTF-8 sequence undecoded until truncation is known.
		// Four bytes also cover malformed sequences handled by trimToUtf8Boundary.
		const tail = new Uint8Array(4,);
		let tailLength = 0;
		const append = (chunk: Uint8Array,): void => {
			if (chunk.byteLength >= tail.length) {
				if (tailLength > 0) {
					parts.push(decoder.decode(tail.subarray(0, tailLength,), { stream: true, },),);
				}
				const end = chunk.byteLength - tail.length;
				if (end > 0) parts.push(decoder.decode(chunk.subarray(0, end,), { stream: true, },),);
				tail.set(chunk.subarray(end,),);
				tailLength = tail.length;
			} else {
				const excess = Math.max(0, tailLength + chunk.byteLength - tail.length,);
				if (excess > 0) {
					parts.push(decoder.decode(tail.subarray(0, excess,), { stream: true, },),);
					tail.copyWithin(0, excess, tailLength,);
					tailLength -= excess;
				}
				tail.set(chunk, tailLength,);
				tailLength += chunk.byteLength;
			}
		};
		let bytesRead = 0;
		let truncated = false;
		const startedAt = Date.now();

		try {
			while (bytesRead < limit) {
				const remainingMs = bodyTimeoutMs - (Date.now() - startedAt);
				if (remainingMs <= 0) throw buildBodyReadTimeoutError(bodyTimeoutMs,);
				const { done, value, } = await readChunkWithDeadline(
					reader,
					remainingMs,
					bodyTimeoutMs,
				);
				if (done) break;
				const room = limit - bytesRead;
				if (value.byteLength > room) {
					append(value.subarray(0, room,),);
					bytesRead += room;
					truncated = true;
					break;
				}
				append(value,);
				bytesRead += value.byteLength;
			}
			if (!truncated && bytesRead >= limit) {
				// Buffer filled exactly on a chunk boundary; peek whether more data remains.
				const remainingMs = bodyTimeoutMs - (Date.now() - startedAt);
				if (remainingMs <= 0) throw buildBodyReadTimeoutError(bodyTimeoutMs,);
				const { done, } = await readChunkWithDeadline(
					reader,
					remainingMs,
					bodyTimeoutMs,
				);
				if (!done) truncated = true;
			}
		} finally {
			void reader.cancel().catch(() => {},);
		}

		const remaining = tail.subarray(0, tailLength,);
		parts.push(decoder.decode(truncated ? trimToUtf8Boundary(remaining,) : remaining,),);
		return { text: parts.join("",), truncated, };
	}

	/**
	 * Read a 2xx response body keeping only the last `maxBytes` bytes.
	 * Earlier bytes are discarded as the stream advances, so memory stays
	 * bounded by the cap plus one chunk even for arbitrarily large bodies.
	 * Returns `truncated: true` when bytes had to be dropped. Bytes that fall
	 * in the middle of a UTF-8 character are dropped from both ends so the
	 * decoded text never contains a replacement character.
	 */
	private async readBodyTextTail(
		res: Response,
		maxBytes: number,
	): Promise<{ text: string; truncated: boolean; }> {
		const limit = Math.max(0, Math.floor(maxBytes,),);
		if (!res.body) return { text: "", truncated: false, };

		const reader = res.body.getReader();
		const ring = new Uint8Array(limit,);
		let writeOffset = 0;
		let retainedBytes = 0;
		let truncated = false;
		const startedAt = Date.now();

		try {
			while (true) {
				const remainingMs = this.requestTimeoutMs - (Date.now() - startedAt);
				if (remainingMs <= 0) throw buildBodyReadTimeoutError(this.requestTimeoutMs,);
				const { done, value, } = await readChunkWithDeadline(
					reader,
					remainingMs,
					this.requestTimeoutMs,
				);
				if (done) break;
				if (limit === 0) {
					truncated = value.byteLength > 0;
					if (truncated) break;
					continue;
				}

				if (value.byteLength >= limit) {
					ring.set(value.subarray(value.byteLength - limit,),);
					truncated = truncated || retainedBytes > 0 || value.byteLength > limit;
					retainedBytes = limit;
					writeOffset = 0;
					continue;
				}

				const firstLength = Math.min(value.byteLength, limit - writeOffset,);
				ring.set(value.subarray(0, firstLength,), writeOffset,);
				const remainingLength = value.byteLength - firstLength;
				if (remainingLength > 0) {
					ring.set(value.subarray(firstLength,),);
				}
				writeOffset = (writeOffset + value.byteLength) % limit;
				truncated = truncated || retainedBytes + value.byteLength > limit;
				retainedBytes = Math.min(limit, retainedBytes + value.byteLength,);
			}
		} finally {
			void reader.cancel().catch(() => {},);
		}

		let collected: Uint8Array;
		if (retainedBytes === 0) {
			collected = new Uint8Array(0,);
		} else if (retainedBytes < limit) {
			collected = ring.slice(0, retainedBytes,);
		} else if (writeOffset === 0) {
			collected = ring;
		} else {
			collected = new Uint8Array(retainedBytes,);
			const suffixLength = limit - writeOffset;
			collected.set(ring.subarray(writeOffset,),);
			collected.set(ring.subarray(0, writeOffset,), suffixLength,);
		}
		// A byte cut inside a multi-byte sequence can leave continuation bytes at
		// the start (and possibly the end) of the tail; drop both so the decoded
		// text never starts or ends with a U+FFFD replacement character.
		const usable = truncated
			? trimToUtf8Boundary(trimFromUtf8BoundaryStart(collected,),)
			: collected;
		return { text: new TextDecoder().decode(usable,), truncated, };
	}

	private buildResponseTooLargeError(res: Response, limit: number,): DataikuError {
		return new DataikuError(
			res.status,
			RESPONSE_TOO_LARGE_STATUS_TEXT,
			`Response body exceeded the ${limit}-byte limit; the body was cancelled after ${limit} bytes.`,
			undefined,
			this.requestIdFromHeaders(res.headers,),
		);
	}

	/* ---- private: retry loop ---- */

	private async fetchWithRetry(
		url: string,
		init: RequestInit,
		retryMaxAttempts?: number,
		deadlineAt?: number,
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
			// An overall deadline may shorten, but never extend, the attempt cap.
			const remainingMs = deadlineAt === undefined
				? undefined
				: deadlineAt - startedAt;
			if (remainingMs !== undefined && remainingMs <= 0) {
				throw new DataikuError(
					0,
					"Request Timeout",
					`Request deadline exceeded after ${
						attempt - 1
					} attempt(s) and ${delaysMs.length} backoff delay(s).`,
					buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, true,),
				);
			}
			const attemptTimeoutMs = remainingMs === undefined
				? this.requestTimeoutMs
				: Math.min(remainingMs, this.requestTimeoutMs,);
			const controller = new AbortController();
			const timeout = setTimeout(() => {
				timedOut = true;
				controller.abort();
			}, attemptTimeoutMs,);
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
					// The status is already known; a body that misses the deadline must not
					// turn a real 5xx/4xx into a bare transport timeout.
					let text = "";
					let truncated = false;
					try {
						({ text, truncated, } = await this.readBoundedBodyText(
							res,
							this.maxResponseBodyBytes,
							startedAt + attemptTimeoutMs,
						));
					} catch (bodyError) {
						// Oversized bodies keep their own classified error; any other read
						// failure (deadline abort, dropped connection) keeps the known status.
						if (bodyError instanceof DataikuError && bodyError.status !== 0) throw bodyError;
						text = `(response body not received within ${attemptTimeoutMs}ms)`;
					}
					const canRetry = retryEnabled && attempt < maxAttempts && isTransientError(res.status, text,);
					const serverDelayMs = canRetry ? retryAfterDelayMs(res.headers.get("retry-after",),) : 0;
					// A longer server wait declines automatic retry; never clamp it into an early request.
					if (canRetry && serverDelayMs <= MAX_BACKOFF_DELAY_MS) {
						const delayMs = Math.max(computeBackoffDelayMs(attempt,), serverDelayMs,);
						// Do not start a retry whose backoff cannot fit in the budget.
						if (deadlineAt === undefined || Date.now() + delayMs <= deadlineAt) {
							delaysMs.push(delayMs,);
							await sleep(delayMs,);
							continue;
						}
						await sleep(Math.max(0, deadlineAt - Date.now(),),);
						throw new DataikuError(
							0,
							"Request Timeout",
							`Request deadline exceeded before backoff retry ${attempt + 1}.`,
							buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, true,),
						);
					}
					throw new DataikuError(
						res.status,
						res.statusText,
						text,
						buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, false,),
						this.requestIdFromHeaders(res.headers,),
						truncated ? { bodyTruncated: true, } : undefined,
					);
				}
				return res;
			} catch (error) {
				if (error instanceof DataikuError) {
					// A bounded error-body read is still part of this transport attempt.
					// Preserve the same retry/deadline policy as a fetch body abort.
					if (
						error.status !== 0 || error.statusText !== "Request Timeout" || error.retry !== undefined
					) {
						throw error;
					}
					timedOut = true;
				}
				const canRetry = retryEnabled && attempt < maxAttempts;
				if (canRetry) {
					const delayMs = computeBackoffDelayMs(attempt,);
					if (deadlineAt === undefined || Date.now() + delayMs <= deadlineAt) {
						delaysMs.push(delayMs,);
						await sleep(delayMs,);
						continue;
					}
					await sleep(Math.max(0, deadlineAt - Date.now(),),);
					throw new DataikuError(
						0,
						"Request Timeout",
						`Request deadline exceeded before backoff retry ${attempt + 1}.`,
						buildRetryMetadata(method, retryEnabled, maxAttempts, attempt, delaysMs, true,),
					);
				}
				const detail = timedOut
					? `Request timed out after ${attemptTimeoutMs}ms`
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
