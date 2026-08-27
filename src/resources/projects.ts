import { createWriteStream, } from "node:fs";
import { Writable, } from "node:stream";
import { ClientValidationError, DataikuError, } from "../errors.js";

import {
	ProjectDetailsSchema,
	ProjectMetadataSchema,
	ProjectSummaryArraySchema,
} from "../schemas.js";
import type {
	FlowMapOptions,
	ProjectDetails,
	ProjectMetadata,
	ProjectSummary,
} from "../schemas.js";
import type {
	NormalizedFlowEdge,
	NormalizedFlowMap,
	NormalizedFlowNode,
} from "../utils/flow-map.js";
import { normalizeFlowGraph, } from "../utils/flow-map.js";
import { assertImportableProjectArchive, } from "../utils/project-archive.js";
import { projectIncarnationHash, } from "../utils/project-incarnation.js";
import { BaseResource, } from "./base.js";

// ---------------------------------------------------------------------------
// Timeout helper for optional metadata fetches
// ---------------------------------------------------------------------------

interface OptionalMetadataResult<T,> {
	value?: T;
	warning?: string;
}

function fetchWithTimeout<T,>(
	label: string,
	timeoutMs: number,
	fetcher: () => Promise<T>,
): Promise<OptionalMetadataResult<T>> {
	return new Promise((resolve,) => {
		let settled = false;
		const timer = setTimeout(() => {
			if (settled) return;
			settled = true;
			resolve({
				warning: `${label} metadata timed out after ${timeoutMs}ms; continuing without it.`,
			},);
		}, timeoutMs,);

		fetcher().then(
			(value,) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer,);
				resolve({ value, },);
			},
			(error: unknown,) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer,);
				const detail = error instanceof Error ? error.message : String(error,);
				resolve({
					warning: `${label} metadata unavailable: ${detail}`,
				},);
			},
		);
	},);
}

// ---------------------------------------------------------------------------
// Flow map truncation
// ---------------------------------------------------------------------------

interface FlowMapTruncationSummary {
	truncated: boolean;
	maxNodes: number | null;
	maxEdges: number | null;
	nodeCountBefore: number;
	nodeCountAfter: number;
	edgeCountBefore: number;
	edgeCountAfter: number;
}

function computeRootsAndLeaves(
	nodes: NormalizedFlowNode[],
	edges: NormalizedFlowEdge[],
): { roots: string[]; leaves: string[]; } {
	const inDegree = new Map<string, number>();
	const outDegree = new Map<string, number>();

	for (const node of nodes) {
		inDegree.set(node.id, 0,);
		outDegree.set(node.id, 0,);
	}

	for (const edge of edges) {
		inDegree.set(edge.to, (inDegree.get(edge.to,) ?? 0) + 1,);
		outDegree.set(edge.from, (outDegree.get(edge.from,) ?? 0) + 1,);
	}

	const roots = nodes
		.filter((node,) => (inDegree.get(node.id,) ?? 0) === 0)
		.map((node,) => node.id)
		.sort((a, b,) => a.localeCompare(b,));

	const leaves = nodes
		.filter((node,) => (outDegree.get(node.id,) ?? 0) === 0)
		.map((node,) => node.id)
		.sort((a, b,) => a.localeCompare(b,));

	return { roots, leaves, };
}

function truncateFlowMap(
	normalized: NormalizedFlowMap,
	maxNodes: number | undefined,
	maxEdges: number | undefined,
): { map: NormalizedFlowMap; truncation: FlowMapTruncationSummary; } {
	const nodes = maxNodes === undefined ? normalized.nodes : normalized.nodes.slice(0, maxNodes,);
	const nodeIds = new Set(nodes.map((node,) => node.id),);
	const edgesWithinNodes = normalized.edges.filter(
		(edge,) => nodeIds.has(edge.from,) && nodeIds.has(edge.to,),
	);
	const edges = maxEdges === undefined ? edgesWithinNodes : edgesWithinNodes.slice(0, maxEdges,);

	const { roots, leaves, } = computeRootsAndLeaves(nodes, edges,);
	const truncation: FlowMapTruncationSummary = {
		truncated: nodes.length < normalized.nodes.length || edges.length < normalized.edges.length,
		maxNodes: maxNodes ?? null,
		maxEdges: maxEdges ?? null,
		nodeCountBefore: normalized.nodes.length,
		nodeCountAfter: nodes.length,
		edgeCountBefore: normalized.edges.length,
		edgeCountAfter: edges.length,
	};

	const warnings = truncation.truncated
		? [
			...normalized.warnings,
			`Flow map truncated (nodes ${truncation.nodeCountAfter}/${truncation.nodeCountBefore}, edges ${truncation.edgeCountAfter}/${truncation.edgeCountBefore}).`,
		]
		: normalized.warnings;

	return {
		map: {
			...normalized,
			nodes,
			edges,
			roots,
			leaves,
			warnings,
			stats: {
				nodeCount: nodes.length,
				edgeCount: edges.length,
				datasets: nodes.filter((node,) => node.kind === "dataset").length,
				recipes: nodes.filter((node,) => node.kind === "recipe").length,
				roots: roots.length,
				leaves: leaves.length,
			},
		},
		truncation,
	};
}

// ---------------------------------------------------------------------------
// Public interfaces
// ---------------------------------------------------------------------------

export interface FlowMapResult {
	map: NormalizedFlowMap;
	truncation: FlowMapTruncationSummary;
	raw?: unknown;
}

type DataikuClientHttpInternals = {
	baseUrl: string;
	apiKey: string;
	fetchWithRetry(url: string, init: RequestInit,): Promise<Response>;
};

export type ProjectLifecycleSettings = Record<string, unknown>;

export type ProjectDuplicationMode = "MINIMAL" | "SHARING" | "FULL" | "NONE" | (string & {});

export interface ProjectDuplicateOptions {
	duplicationMode?: ProjectDuplicationMode;
	exportAnalysisModels?: boolean;
	exportSavedModels?: boolean;
	exportGitRepository?: boolean | null;
	exportInsightsData?: boolean;
	remapping?: Record<string, unknown>;
	targetProjectFolderId?: string;
}

export interface ProjectExportOptions {
	exportUploads?: boolean;
	exportManagedFS?: boolean;
	exportAnalysisModels?: boolean;
	exportSavedModels?: boolean;
	exportModelEvaluationStores?: boolean;
	exportManagedFolders?: boolean;
	exportAllInputDatasets?: boolean;
	exportAllDatasets?: boolean;
	exportAllInputManagedFolders?: boolean;
	exportGitRepository?: boolean;
	exportInsightsData?: boolean;
	exportPromptStudioHistories?: boolean;
	[key: string]: unknown;
}

export interface ProjectPermissionRule {
	group?: string;
	user?: string;
	admin?: boolean;
	readProjectContent?: boolean;
	writeProjectContent?: boolean;
	readDashboards?: boolean;
	writeDashboards?: boolean;
	runScenarios?: boolean;
	manageDashboardAuthorizations?: boolean;
	manageExposedElements?: boolean;
	moderateDashboards?: boolean;
	[key: string]: unknown;
}

export interface ProjectPermissions {
	owner?: string;
	permissions?: ProjectPermissionRule[];
	[key: string]: unknown;
}

export interface ProjectImportUploadResult {
	id: string;
	[key: string]: unknown;
}
export interface ProjectImportSettings {
	targetProjectKey?: string;
	remapping?: Record<string, unknown>;
	[key: string]: unknown;
}

export interface ProjectImportProcessResult {
	/** Present on definitive responses; a missing or non-boolean value makes the outcome indeterminate. */
	success?: boolean;
	/** Project key DSS actually created (after remapping). Definitive success includes it. */
	usedProjectKey?: string;
	[key: string]: unknown;
}

export interface ProjectImportResult extends ProjectImportProcessResult {
	importId: string;
	/** Key the import was bound to: the explicit settings target, else the archive manifest's key. Known on success. */
	requestedProjectKey?: string;
	/** True when usedProjectKey differs from requestedProjectKey (set only when both are known). */
	remapped?: boolean;
	/** SHA-256 fingerprint of the landed project's creationTag. Present on verified success. */
	projectIncarnationHash?: string;
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

const DEFAULT_MAX_NODES = 300;
const DEFAULT_MAX_EDGES = 600;
const DEFAULT_METADATA_TIMEOUT_MS = 1_500;
const PROJECT_IMPORT_AMBIGUOUS_REMEDIATION =
	"Import processing left DSS state indeterminate. Inspect the archive with `dss project inspect-archive <file>` and the live projects with `dss project list` before retrying the import.";

export class ProjectsResource extends BaseResource {
	private httpInternals(): DataikuClientHttpInternals {
		return this.client as unknown as DataikuClientHttpInternals;
	}

	private async postStream(path: string, body: unknown,): Promise<Response> {
		const http = this.httpInternals();
		return http.fetchWithRetry(`${http.baseUrl}${path}`, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${http.apiKey}`,
				Accept: "*/*",
				"Content-Type": "application/json",
			},
			body: JSON.stringify(body,),
		},);
	}

	/** Create a new project. */
	async createProject(
		projectKey: string,
		name: string,
		ownerLogin: string,
		settings?: ProjectLifecycleSettings | null,
	): Promise<Record<string, unknown>> {
		return this.client.post<Record<string, unknown>>("/public/api/projects/", {
			projectKey,
			name,
			owner: ownerLogin,
			settings: settings ?? null,
			description: null,
			permissions: [],
			tags: [],
		},);
	}

	/** Delete a project using the lifecycle endpoint. */
	async deleteProject(projectKey: string, dropData = false,): Promise<void> {
		const enc = encodeURIComponent(projectKey,);
		await this.client.del(
			`/public/api/projects/${enc}?clearManagedDatasets=${
				String(dropData,)
			}&clearOutputManagedFolders=false&clearJobAndScenarioLogs=true&wait=true`,
		);
	}

	/** Duplicate a project. */
	async duplicate(
		projectKey: string,
		targetProjectKey: string,
		targetProjectName: string,
		options?: ProjectDuplicateOptions,
	): Promise<Record<string, unknown>> {
		const body = {
			targetProjectName,
			targetProjectKey,
			duplicationMode: options?.duplicationMode ?? "MINIMAL",
			exportAnalysisModels: options?.exportAnalysisModels ?? true,
			exportSavedModels: options?.exportSavedModels ?? true,
			exportGitRepository: options?.exportGitRepository ?? null,
			exportInsightsData: options?.exportInsightsData ?? true,
			remapping: options?.remapping ?? {},
			...(options?.targetProjectFolderId !== undefined
				? { targetProjectFolderId: options.targetProjectFolderId, }
				: {}),
		};
		return this.client.post<Record<string, unknown>>(
			`/public/api/projects/${encodeURIComponent(projectKey,)}/duplicate/`,
			body,
		);
	}

	/** Export a project archive to a local file. */
	async exportArchive(
		projectKey: string,
		filePath: string,
		options?: ProjectExportOptions,
	): Promise<void> {
		const res = await this.postStream(
			`/public/api/projects/${encodeURIComponent(projectKey,)}/export`,
			options ?? {},
		);
		if (!res.body) throw new Error("projects.exportArchive response did not include a body",);

		const file = createWriteStream(filePath,);
		try {
			await res.body.pipeTo(Writable.toWeb(file,) as WritableStream<Uint8Array>,);
		} catch (error) {
			file.destroy();
			throw error;
		}
	}

	/** Upload a project archive and return its temporary import handle. */
	async prepareProjectImport(filePath: string,): Promise<ProjectImportUploadResult> {
		const upload = await this.client.uploadJson<ProjectImportUploadResult>(
			"/public/api/projects/import/upload",
			filePath,
			"tmp-import.zip",
		);
		if (!upload || typeof upload.id !== "string" || upload.id.trim() === "") {
			throw new ClientValidationError(
				"DSS accepted the project archive upload but did not return a temporary import id.",
				"ambiguous_outcome",
				"The archive may remain in temporary DSS storage, but no project import was started. Inspect DSS before retrying.",
			);
		}
		return upload;
	}

	/** Process an uploaded archive into a DSS project. */
	async processProjectImport(
		importId: string,
		settings: ProjectImportSettings = {},
	): Promise<ProjectImportProcessResult> {
		const body = Object.keys(settings,).length === 0 ? { _: "_", } : settings;
		try {
			const response = await this.client.post<ProjectImportProcessResult>(
				`/public/api/projects/import/${encodeURIComponent(importId,)}/process`,
				body,
			);
			// An empty or null 2xx body carries no definitive outcome: DSS may
			// have created the project, partially processed it, or done nothing.
			if (!response) {
				throw new ClientValidationError(
					"DSS accepted the import process request but returned no response body: the project may or may not have been created.",
					"ambiguous_outcome",
					PROJECT_IMPORT_AMBIGUOUS_REMEDIATION,
					{
						importId,
						...(typeof settings.targetProjectKey === "string"
								&& settings.targetProjectKey.trim() !== ""
							? { targetProjectKey: settings.targetProjectKey, }
							: {}),
					},
				);
			}
			return response;
		} catch (error) {
			// A transport timeout or server-side 5xx leaves the import outcome
			// unknowable: the project may or may not exist. Surface that as an
			// ambiguous outcome — never as a definitive failure. A 2xx
			// response that carried a non-JSON body has the same property.
			if (
				error instanceof DataikuError
				&& (
					error.status === 0
					|| error.status >= 500
					|| (error.status >= 200 && error.status < 300)
				)
			) {
				throw new ClientValidationError(
					error.status >= 200 && error.status < 300
						? "DSS returned a non-JSON process response: the import outcome is indeterminate and the project may or may not have been created."
						: "DSS returned no definitive import result: the project may or may not have been created.",
					"ambiguous_outcome",
					PROJECT_IMPORT_AMBIGUOUS_REMEDIATION,
					{
						importId,
						...(typeof settings.targetProjectKey === "string"
								&& settings.targetProjectKey.trim() !== ""
							? { targetProjectKey: settings.targetProjectKey, }
							: {}),
					},
				);
			}
			throw error;
		}
	}

	/**
	 * Upload and process a project archive.
	 *
	 * The archive is validated locally before anything is uploaded. Definitive
	 * failures surface the process response verbatim (with `importId`); every
	 * indeterminate path throws ClientValidationError(`ambiguous_outcome`).
	 * Verified success carries the actually-used project key, the requested
	 * key when known (with explicit `remapped` reporting), and the landed
	 * project's incarnation hash for identity-bound cleanup and guarded deletes.
	 */
	async importProjectFromArchive(
		filePath: string,
		settings: ProjectImportSettings = {},
	): Promise<ProjectImportResult> {
		// Fatal archive problems abort before any DSS call or upload.
		const inspection = await assertImportableProjectArchive(filePath,);
		const requestedProjectKey = typeof settings.targetProjectKey === "string"
				&& settings.targetProjectKey.trim() !== ""
			? settings.targetProjectKey
			: inspection.sourceProjectKey;

		const upload = await this.prepareProjectImport(filePath,);
		const result = await this.processProjectImport(upload.id, settings,);
		const importId = upload.id;

		if (typeof result.success !== "boolean") {
			throw new ClientValidationError(
				"DSS returned a process response without a boolean success field: the import outcome is indeterminate.",
				"ambiguous_outcome",
				PROJECT_IMPORT_AMBIGUOUS_REMEDIATION,
				{
					importId,
					...(requestedProjectKey !== undefined
						? { targetProjectKey: requestedProjectKey, }
						: {}),
				},
			);
		}
		if (result.success === false) {
			// Definitive DSS-side failure: keep the response authoritative and
			// let the command layer report it as a plain command failure.
			return { ...result, success: false, importId, };
		}

		const usedProjectKey = typeof result.usedProjectKey === "string"
				&& result.usedProjectKey.trim() !== ""
			? result.usedProjectKey
			: undefined;
		if (usedProjectKey === undefined) {
			throw new ClientValidationError(
				"DSS reported import success but did not name the project that was created.",
				"ambiguous_outcome",
				PROJECT_IMPORT_AMBIGUOUS_REMEDIATION,
				{
					importId,
					...(requestedProjectKey !== undefined
						? { targetProjectKey: requestedProjectKey, }
						: {}),
				},
			);
		}

		// Verify the claimed landing and capture identity for cleanup binding.
		let details: ProjectDetails;
		try {
			details = await this.get(usedProjectKey,);
		} catch (error) {
			if (
				error instanceof DataikuError
				&& (error.status === 404 || error.status === 0 || error.status >= 500)
			) {
				throw new ClientValidationError(
					`DSS reported import success for project ${usedProjectKey}, but the project could not be re-read for verification.`,
					"ambiguous_outcome",
					PROJECT_IMPORT_AMBIGUOUS_REMEDIATION,
					{ importId, usedProjectKey, },
				);
			}
			throw error;
		}
		const incarnation = projectIncarnationHash(usedProjectKey, details,);
		if (incarnation === undefined) {
			throw new ClientValidationError(
				`DSS imported project ${usedProjectKey}, but returned no creationTag identity.`,
				"ambiguous_outcome",
				"Refusing to claim identity-bound success without a project-incarnation hash. Verify the project identity before any cleanup or guarded delete.",
				{ importId, usedProjectKey, },
			);
		}

		return {
			...result,
			success: true,
			importId,
			usedProjectKey,
			projectIncarnationHash: incarnation,
			...(requestedProjectKey === undefined
				? {}
				: {
					requestedProjectKey,
					remapped: requestedProjectKey !== usedProjectKey,
				}),
		};
	}

	/** Get project permissions. */
	async getPermissions(projectKey?: string,): Promise<ProjectPermissions> {
		return this.client.get<ProjectPermissions>(
			`/public/api/projects/${this.enc(projectKey,)}/permissions`,
		);
	}

	/** Set project permissions. */
	async setPermissions(
		projectKey: string | undefined,
		permissions: ProjectPermissions,
	): Promise<void> {
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/permissions`,
			permissions,
		);
	}

	/** Get project settings. */
	async getSettings(projectKey?: string,): Promise<ProjectLifecycleSettings> {
		return this.client.get<ProjectLifecycleSettings>(
			`/public/api/projects/${this.enc(projectKey,)}/settings`,
		);
	}

	/** Set project settings. */
	async setSettings(projectKey: string | undefined, body: ProjectLifecycleSettings,): Promise<void> {
		await this.client.putVoid(
			`/public/api/projects/${this.enc(projectKey,)}/settings`,
			body,
		);
	}

	/** List all projects visible to the API key. */
	async list(): Promise<ProjectSummary[]> {
		const raw = await this.client.get<unknown>("/public/api/projects/",);
		return this.client.safeParse(ProjectSummaryArraySchema, raw, "projects.list",);
	}

	/** Get details for a single project. */
	async get(projectKey?: string,): Promise<ProjectDetails> {
		const enc = this.enc(projectKey,);
		const raw = await this.client.get<unknown>(`/public/api/projects/${enc}/`,);
		return this.client.safeParse(ProjectDetailsSchema, raw, "projects.get",);
	}

	/** Get metadata (tags, custom fields, checklists) for a project. */
	async metadata(projectKey?: string,): Promise<ProjectMetadata> {
		const enc = this.enc(projectKey,);
		const raw = await this.client.get<unknown>(`/public/api/projects/${enc}/metadata`,);
		return this.client.safeParse(ProjectMetadataSchema, raw, "projects.metadata",);
	}

	/** Get the raw flow graph for a project. */
	async flow(projectKey?: string,): Promise<unknown> {
		const enc = this.enc(projectKey,);
		return this.client.get<unknown>(`/public/api/projects/${enc}/flow/graph/`,);
	}

	/**
	 * Build a normalized, optionally truncated flow map for a project.
	 *
	 * Fetches the flow graph and supplementary metadata (datasets, recipes,
	 * managed folders) in parallel. Folder name resolution uses a timeout
	 * to avoid blocking when the folders endpoint is slow.
	 */
	async map(opts?: FlowMapOptions & { projectKey?: string; },): Promise<FlowMapResult> {
		const enc = this.enc(opts?.projectKey,);
		const pk = this.resolveProjectKey(opts?.projectKey,);
		const timeoutMs = DEFAULT_METADATA_TIMEOUT_MS;

		const [rawGraph, foldersMeta, datasetsMeta, recipesMeta,] = await Promise.all([
			this.client.get<unknown>(`/public/api/projects/${enc}/flow/graph/`,),
			fetchWithTimeout(
				"Managed folders",
				timeoutMs,
				() =>
					this.client.get<Array<{ id?: string; name?: string; }>>(
						`/public/api/projects/${enc}/managedfolders/`,
					),
			),
			fetchWithTimeout(
				"Datasets",
				timeoutMs,
				() => this.client.get<Array<{ name?: string; }>>(`/public/api/projects/${enc}/datasets/`,),
			),
			fetchWithTimeout(
				"Recipes",
				timeoutMs,
				() => this.client.get<Array<{ name?: string; }>>(`/public/api/projects/${enc}/recipes/`,),
			),
		],);

		// Build folder name lookup
		const folderNamesById: Record<string, string> = {};
		const allFolderIds: string[] = [];
		for (const f of foldersMeta.value ?? []) {
			if (!f.id || f.id.length === 0) continue;
			allFolderIds.push(f.id,);
			folderNamesById[f.id] = f.name ?? f.id;
		}

		const allDatasetNames = (datasetsMeta.value ?? [])
			.map((d,) => d.name)
			.filter((n,): n is string => typeof n === "string" && n.length > 0);

		const allRecipeNames = (recipesMeta.value ?? [])
			.map((r,) => r.name)
			.filter((n,): n is string => typeof n === "string" && n.length > 0);

		// Normalize the flow graph
		const normalizedBase = normalizeFlowGraph(rawGraph, pk, {
			folderNamesById,
			allDatasetNames,
			allRecipeNames,
			allFolderIds,
		},);

		// Append any metadata fetch warnings
		const metadataWarnings = [
			foldersMeta.warning,
			datasetsMeta.warning,
			recipesMeta.warning,
		].filter((w,): w is string => typeof w === "string" && w.length > 0);

		const normalized = metadataWarnings.length > 0
			? {
				...normalizedBase,
				warnings: [...normalizedBase.warnings, ...metadataWarnings,],
			}
			: normalizedBase;

		// Truncate
		const effectiveMaxNodes = opts?.maxNodes ?? DEFAULT_MAX_NODES;
		const effectiveMaxEdges = opts?.maxEdges ?? DEFAULT_MAX_EDGES;
		const { map, truncation, } = truncateFlowMap(normalized, effectiveMaxNodes, effectiveMaxEdges,);

		const result: FlowMapResult = { map, truncation, };
		if (opts?.includeRaw) {
			result.raw = rawGraph;
		}
		return result;
	}
}
