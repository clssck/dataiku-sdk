export type FlowNodeKind = "dataset" | "recipe" | "folder" | "other";
export type FlowEdgeRelation = "reads" | "writes" | "depends_on" | "unknown";

export interface NormalizedFlowNode {
	id: string;
	kind: FlowNodeKind;
	name?: string;
	subtype?: string;
	connection?: string;
}

export interface NormalizedFlowEdge {
	from: string;
	to: string;
	relation: FlowEdgeRelation;
}

export interface NormalizedFlowMap {
	projectKey: string;
	nodes: NormalizedFlowNode[];
	edges: NormalizedFlowEdge[];
	stats: {
		nodeCount: number;
		edgeCount: number;
		datasets: number;
		recipes: number;
		roots: number;
		leaves: number;
	};
	roots: string[];
	leaves: string[];
	warnings: string[];
}

export interface NormalizeFlowGraphOptions {
	folderNamesById?: Record<string, string>;
	allDatasetNames?: string[];
	allRecipeNames?: string[];
	allFolderIds?: string[];
}

interface InternalNode {
	id: string;
	kind: FlowNodeKind;
	name?: string;
	subtype?: string;
	connection?: string;
	predecessors: string[];
	successors: string[];
}

function asRecord(value: unknown,): Record<string, unknown> | undefined {
	if (value && typeof value === "object" && !Array.isArray(value,)) {
		return value as Record<string, unknown>;
	}
	return undefined;
}

function asString(value: unknown,): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function asStringArray(value: unknown, warnings: string[], context: string,): string[] {
	if (value === undefined) return [];
	if (!Array.isArray(value,)) {
		warnings.push(`Skipped non-array "${context}" field.`,);
		return [];
	}
	const out: string[] = [];
	for (const item of value) {
		if (typeof item === "string" && item.length > 0) out.push(item,);
		else warnings.push(`Skipped non-string item in "${context}".`,);
	}
	return out;
}

function inferKind(type: string | undefined,): FlowNodeKind {
	if (!type) return "other";
	const upper = type.toUpperCase();
	if (upper.includes("RECIPE",)) return "recipe";
	if (upper.includes("DATASET",)) return "dataset";
	if (upper.includes("FOLDER",)) return "folder";
	return "other";
}

function inferSubtypeFromType(type: string | undefined,): string | undefined {
	if (!type) return undefined;
	const upper = type.toUpperCase();
	if (upper.includes("IMPLICIT_RECIPE",)) return "implicit";
	return undefined;
}

function relationRank(relation: FlowEdgeRelation,): number {
	switch (relation) {
		case "reads":
		case "writes":
			return 3;
		case "depends_on":
			return 2;
		default:
			return 1;
	}
}

function inferRelation(fromKind: FlowNodeKind, toKind: FlowNodeKind,): FlowEdgeRelation {
	if ((fromKind === "dataset" || fromKind === "folder") && toKind === "recipe") {
		return "reads";
	}
	if (fromKind === "recipe" && (toKind === "dataset" || toKind === "folder")) {
		return "writes";
	}
	if (fromKind === "other" || toKind === "other") return "unknown";
	return "depends_on";
}

function preferredKind(current: FlowNodeKind, incoming: FlowNodeKind,): FlowNodeKind {
	if (current === incoming) return current;
	if (current === "other") return incoming;
	return current;
}

function getConnection(record: Record<string, unknown>,): string | undefined {
	const direct = asString(record.connection,);
	if (direct) return direct;
	const params = asRecord(record.params,);
	return asString(params?.connection,);
}

function mergeUnique(...lists: Array<string[] | undefined>): string[] {
	const out = new Set<string>();
	for (const list of lists) {
		if (!list) continue;
		for (const item of list) {
			if (item) out.add(item,);
		}
	}
	return [...out,];
}

export function normalizeFlowGraph(
	raw: unknown,
	projectKey: string,
	options: NormalizeFlowGraphOptions = {},
): NormalizedFlowMap {
	const warnings: string[] = [];
	const root = asRecord(raw,);

	if (!root) {
		return {
			projectKey,
			nodes: [],
			edges: [],
			stats: {
				nodeCount: 0,
				edgeCount: 0,
				datasets: 0,
				recipes: 0,
				roots: 0,
				leaves: 0,
			},
			roots: [],
			leaves: [],
			warnings: ["Flow graph response was not an object.",],
		};
	}

	const nodeMap = new Map<string, InternalNode>();
	const aliasMap = new Map<string, string>();

	function upsertNode(node: Omit<InternalNode, "predecessors" | "successors">,) {
		const existing = nodeMap.get(node.id,);
		if (!existing) {
			nodeMap.set(node.id, {
				...node,
				predecessors: [],
				successors: [],
			},);
			return;
		}

		existing.kind = preferredKind(existing.kind, node.kind,);
		if (!existing.name && node.name) existing.name = node.name;
		if (!existing.subtype && node.subtype) existing.subtype = node.subtype;
		if (!existing.connection && node.connection) existing.connection = node.connection;
	}

	function ensureNode(id: string,): InternalNode {
		const existing = nodeMap.get(id,);
		if (existing) return existing;
		const placeholder: InternalNode = {
			id,
			kind: "other",
			name: id,
			predecessors: [],
			successors: [],
		};
		nodeMap.set(id, placeholder,);
		warnings.push(`Added placeholder node for "${id}" referenced by an edge.`,);
		return placeholder;
	}

	function addAlias(alias: string | undefined, id: string,) {
		if (!alias) return;
		aliasMap.set(alias, id,);
	}

	const rawNodes = root.nodes;
	if (rawNodes !== undefined && !asRecord(rawNodes,)) {
		warnings.push('Skipped "nodes" because it was not an object.',);
	}

	for (const [nodeKey, nodeValue,] of Object.entries(asRecord(rawNodes,) ?? {},)) {
		const nodeObj = asRecord(nodeValue,);
		if (!nodeObj) {
			warnings.push(`Skipped node "${nodeKey}" because it was not an object.`,);
			continue;
		}

		const ref = asString(nodeObj.ref,);
		const id = ref ?? nodeKey;
		const kind = inferKind(asString(nodeObj.type,),);
		const subtype = asString(nodeObj.subType,)
			?? asString(nodeObj.subtype,)
			?? inferSubtypeFromType(asString(nodeObj.type,),);
		const fallbackName = asString(nodeObj.name,) ?? asString(nodeObj.label,) ?? ref ?? nodeKey;
		const folderName = kind === "folder" ? asString(options.folderNamesById?.[id],) : undefined;
		const name = folderName ?? fallbackName;
		const connection = getConnection(nodeObj,);

		upsertNode({
			id,
			kind,
			name,
			subtype,
			connection,
		},);

		addAlias(nodeKey, id,);
		addAlias(ref, id,);
		addAlias(asString(nodeObj.id,), id,);

		const node = ensureNode(id,);
		node.predecessors = asStringArray(
			nodeObj.predecessors,
			warnings,
			`nodes.${nodeKey}.predecessors`,
		);
		node.successors = asStringArray(nodeObj.successors, warnings, `nodes.${nodeKey}.successors`,);
	}

	const datasets = mergeUnique(
		asStringArray(root.datasets, warnings, "datasets",),
		options.allDatasetNames,
	);
	for (const dataset of datasets) {
		upsertNode({ id: dataset, kind: "dataset", name: dataset, },);
		addAlias(dataset, dataset,);
	}

	const recipes = mergeUnique(
		asStringArray(root.recipes, warnings, "recipes",),
		options.allRecipeNames,
	);
	for (const recipe of recipes) {
		upsertNode({ id: recipe, kind: "recipe", name: recipe, },);
		addAlias(recipe, recipe,);
	}

	const folders = mergeUnique(
		asStringArray(root.folders, warnings, "folders",),
		options.allFolderIds,
	);
	for (const folder of folders) {
		upsertNode({
			id: folder,
			kind: "folder",
			name: asString(options.folderNamesById?.[folder],) ?? folder,
		},);
		addAlias(folder, folder,);
	}

	// Best-effort rename of folder nodes using managed folder metadata lookup.
	for (const [id, node,] of nodeMap.entries()) {
		if (node.kind !== "folder") continue;
		const friendlyName = asString(options.folderNamesById?.[id],);
		if (!friendlyName) continue;
		if (!node.name || node.name === id) node.name = friendlyName;
	}

	function resolveAlias(ref: string,): string {
		return aliasMap.get(ref,) ?? ref;
	}

	const edgeMap = new Map<string, NormalizedFlowEdge>();

	function addEdge(fromRef: string, toRef: string,) {
		const from = resolveAlias(fromRef,);
		const to = resolveAlias(toRef,);
		const fromNode = ensureNode(from,);
		const toNode = ensureNode(to,);
		const relation = inferRelation(fromNode.kind, toNode.kind,);
		const key = `${from}::${to}`;
		const existing = edgeMap.get(key,);
		if (!existing || relationRank(relation,) > relationRank(existing.relation,)) {
			edgeMap.set(key, { from, to, relation, },);
		}
	}

	for (const node of nodeMap.values()) {
		for (const predecessor of node.predecessors) {
			addEdge(predecessor, node.id,);
		}
		for (const successor of node.successors) {
			addEdge(node.id, successor,);
		}
	}

	const nodes = [...nodeMap.values(),]
		.map<NormalizedFlowNode>((node,) => ({
			id: node.id,
			kind: node.kind,
			name: node.name,
			subtype: node.subtype,
			connection: node.connection,
		}))
		.sort((a, b,) => a.id.localeCompare(b.id,));

	const edges = [...edgeMap.values(),].sort((a, b,) => {
		const byFrom = a.from.localeCompare(b.from,);
		if (byFrom !== 0) return byFrom;
		const byTo = a.to.localeCompare(b.to,);
		if (byTo !== 0) return byTo;
		return a.relation.localeCompare(b.relation,);
	},);

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

	const datasetsCount = nodes.filter((node,) => node.kind === "dataset").length;
	const recipesCount = nodes.filter((node,) => node.kind === "recipe").length;

	return {
		projectKey,
		nodes,
		edges,
		stats: {
			nodeCount: nodes.length,
			edgeCount: edges.length,
			datasets: datasetsCount,
			recipes: recipesCount,
			roots: roots.length,
			leaves: leaves.length,
		},
		roots,
		leaves,
		warnings,
	};
}
