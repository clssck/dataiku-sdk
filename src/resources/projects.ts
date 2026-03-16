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

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

const DEFAULT_MAX_NODES = 300;
const DEFAULT_MAX_EDGES = 600;
const DEFAULT_METADATA_TIMEOUT_MS = 1_500;

export class ProjectsResource extends BaseResource {
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
