import type { FlowZone, FlowZoneItem, } from "../schemas.js";
import type { NormalizedFlowEdge, NormalizedFlowMap, NormalizedFlowNode, } from "./flow-map.js";
import { compareStrings, stableHash, } from "./stable-hash.js";

export type FlowRenderFormat = "ascii" | "mermaid";
export type FlowDiagnosticSeverity = "info" | "warning";

export interface AnalyzedFlowNode extends NormalizedFlowNode {
	componentId: string;
	layer: number;
	zoneId: string;
	zoneName: string;
}

export interface FlowMapZone {
	id: string;
	name: string;
	color?: string;
	position?: { x: number; y: number; };
	itemCount: number;
	nodeIds: string[];
}

export interface FlowMapComponent {
	id: string;
	nodeIds: string[];
	rootIds: string[];
	leafIds: string[];
	cyclic: boolean;
}

export interface FlowMapDiagnostic {
	code: string;
	severity: FlowDiagnosticSeverity;
	message: string;
	nodeIds?: string[];
	componentIds?: string[];
	edgeCount?: number;
}

export interface AnalyzedFlowMap extends Omit<NormalizedFlowMap, "nodes"> {
	nodes: AnalyzedFlowNode[];
	zones: FlowMapZone[];
	components: FlowMapComponent[];
	diagnostics: FlowMapDiagnostic[];
	topologyFingerprint: string;
}

export interface FlowMapRendering {
	format: FlowRenderFormat;
	content: string;
}

interface StrongComponent {
	id: number;
	nodeIds: string[];
	cyclic: boolean;
}

function sorted(values: Iterable<string>,): string[] {
	return [...values,].sort(compareStrings,);
}

function validPosition(zone: FlowZone,): { x: number; y: number; } | undefined {
	const position = zone.position;
	return position && Number.isFinite(position.x,) && Number.isFinite(position.y,)
		? { x: position.x, y: position.y, }
		: undefined;
}

function zoneItems(zone: FlowZone,): FlowZoneItem[] {
	return [...(zone.items ?? []), ...(zone.shared ?? []),];
}

function resolveZoneItemNodeId(
	item: FlowZoneItem,
	projectKey: string,
	nodeIds: Set<string>,
): string | undefined {
	if (item.projectKey && item.projectKey !== projectKey) {
		const qualified = `${item.projectKey}.${item.objectId}`;
		if (nodeIds.has(qualified,)) return qualified;
	}
	return nodeIds.has(item.objectId,) ? item.objectId : undefined;
}

function flowAdjacency(map: NormalizedFlowMap,): {
	outgoing: Map<string, string[]>;
	undirected: Map<string, string[]>;
	selfLoops: Set<string>;
} {
	const outgoingSets = new Map<string, Set<string>>();
	const undirectedSets = new Map<string, Set<string>>();
	const selfLoops = new Set<string>();
	for (const node of map.nodes) {
		outgoingSets.set(node.id, new Set(),);
		undirectedSets.set(node.id, new Set(),);
	}
	for (const edge of map.edges) {
		if (!outgoingSets.has(edge.from,) || !outgoingSets.has(edge.to,)) continue;
		outgoingSets.get(edge.from,)!.add(edge.to,);
		undirectedSets.get(edge.from,)!.add(edge.to,);
		undirectedSets.get(edge.to,)!.add(edge.from,);
		if (edge.from === edge.to) selfLoops.add(edge.from,);
	}
	return {
		outgoing: new Map([...outgoingSets,].map(([id, values,],) => [id, sorted(values,),]),),
		undirected: new Map([...undirectedSets,].map(([id, values,],) => [id, sorted(values,),]),),
		selfLoops,
	};
}

function strongComponents(
	nodeIds: string[],
	outgoing: Map<string, string[]>,
	selfLoops: Set<string>,
): StrongComponent[] {
	const incoming = new Map(nodeIds.map((nodeId,) => [nodeId, [] as string[],]),);
	for (const [nodeId, successors,] of outgoing) {
		for (const successor of successors) incoming.get(successor,)?.push(nodeId,);
	}
	for (const predecessors of incoming.values()) predecessors.sort(compareStrings,);

	const visited = new Set<string>();
	const finished: string[] = [];
	for (const start of nodeIds) {
		if (visited.has(start,)) continue;
		visited.add(start,);
		const stack: Array<{ nodeId: string; next: number; }> = [{ nodeId: start, next: 0, },];
		while (stack.length > 0) {
			const frame = stack[stack.length - 1]!;
			const successors = outgoing.get(frame.nodeId,) ?? [];
			const successor = successors[frame.next];
			if (successor !== undefined) {
				frame.next += 1;
				if (visited.has(successor,)) continue;
				visited.add(successor,);
				stack.push({ nodeId: successor, next: 0, },);
				continue;
			}
			finished.push(frame.nodeId,);
			stack.pop();
		}
	}

	const assigned = new Set<string>();
	const components: StrongComponent[] = [];
	for (let index = finished.length - 1; index >= 0; index -= 1) {
		const start = finished[index]!;
		if (assigned.has(start,)) continue;
		const members: string[] = [];
		const stack = [start,];
		assigned.add(start,);
		while (stack.length > 0) {
			const nodeId = stack.pop()!;
			members.push(nodeId,);
			for (const predecessor of incoming.get(nodeId,) ?? []) {
				if (assigned.has(predecessor,)) continue;
				assigned.add(predecessor,);
				stack.push(predecessor,);
			}
		}
		members.sort(compareStrings,);
		components.push({
			id: -1,
			nodeIds: members,
			cyclic: members.length > 1 || selfLoops.has(members[0],),
		},);
	}
	components.sort((a, b,) => compareStrings(a.nodeIds[0], b.nodeIds[0],));
	components.forEach((component, index,) => component.id = index);
	return components;
}

function componentLayers(
	strong: StrongComponent[],
	edges: NormalizedFlowEdge[],
): Map<string, number> {
	const strongByNode = new Map<string, number>();
	const outgoing = new Map<number, Set<number>>();
	const indegree = new Map<number, number>();
	const layers = new Map<number, number>();
	for (const component of strong) {
		outgoing.set(component.id, new Set(),);
		indegree.set(component.id, 0,);
		layers.set(component.id, 0,);
		for (const nodeId of component.nodeIds) strongByNode.set(nodeId, component.id,);
	}
	for (const edge of edges) {
		const from = strongByNode.get(edge.from,);
		const to = strongByNode.get(edge.to,);
		if (from === undefined || to === undefined || from === to || outgoing.get(from,)!.has(to,)) {
			continue;
		}
		outgoing.get(from,)!.add(to,);
		indegree.set(to, indegree.get(to,)! + 1,);
	}
	const ready = strong.filter((component,) => indegree.get(component.id,) === 0).map((component,) =>
		component.id
	);
	while (ready.length > 0) {
		const current = ready.pop()!;
		for (const successor of outgoing.get(current,)!) {
			layers.set(successor, Math.max(layers.get(successor,)!, layers.get(current,)! + 1,),);
			indegree.set(successor, indegree.get(successor,)! - 1,);
			if (indegree.get(successor,) === 0) ready.push(successor,);
		}
	}
	return new Map([...strongByNode,].map(([nodeId, id,],) => [nodeId, layers.get(id,) ?? 0,]),);
}

function weakComponents(
	map: NormalizedFlowMap,
	undirected: Map<string, string[]>,
	cyclicNodes: Set<string>,
): { components: FlowMapComponent[]; componentByNode: Map<string, string>; } {
	const seen = new Set<string>();
	const components: FlowMapComponent[] = [];
	const componentByNode = new Map<string, string>();
	const rootSet = new Set(map.roots,);
	const leafSet = new Set(map.leaves,);
	for (const start of map.nodes.map((node,) => node.id)) {
		if (seen.has(start,)) continue;
		const queue = [start,];
		const nodeIds: string[] = [];
		seen.add(start,);
		for (let index = 0; index < queue.length; index += 1) {
			const current = queue[index];
			nodeIds.push(current,);
			for (const adjacent of undirected.get(current,) ?? []) {
				if (seen.has(adjacent,)) continue;
				seen.add(adjacent,);
				queue.push(adjacent,);
			}
		}
		nodeIds.sort(compareStrings,);
		const id = `component-${components.length + 1}`;
		for (const nodeId of nodeIds) componentByNode.set(nodeId, id,);
		components.push({
			id,
			nodeIds,
			rootIds: nodeIds.filter((nodeId,) => rootSet.has(nodeId,)),
			leafIds: nodeIds.filter((nodeId,) => leafSet.has(nodeId,)),
			cyclic: nodeIds.some((nodeId,) => cyclicNodes.has(nodeId,)),
		},);
	}
	return { components, componentByNode, };
}

export function flowTopologyFingerprint(map: NormalizedFlowMap,): string {
	return stableHash({
		nodes: [...map.nodes,]
			.map((node,) => ({ id: node.id, kind: node.kind, }))
			.sort((a, b,) => compareStrings(a.id, b.id,)),
		edges: [...map.edges,]
			.map((edge,) => ({ from: edge.from, to: edge.to, relation: edge.relation, }))
			.sort((a, b,) =>
				compareStrings(a.from, b.from,) || compareStrings(a.to, b.to,)
				|| compareStrings(a.relation, b.relation,)
			),
	},);
}

export function analyzeFlowMap(
	map: NormalizedFlowMap,
	flowZones: FlowZone[],
	options: { topologyFingerprint?: string; truncated?: boolean; } = {},
): AnalyzedFlowMap {
	const nodeIds = new Set(map.nodes.map((node,) => node.id),);
	const diagnostics: FlowMapDiagnostic[] = [];
	const sortedZones = [...flowZones,].sort((a, b,) => {
		if (a.id === "default") return b.id === "default" ? 0 : 1;
		if (b.id === "default") return -1;
		const ap = validPosition(a,);
		const bp = validPosition(b,);
		if (ap && bp) return ap.x - bp.x || ap.y - bp.y || compareStrings(a.name, b.name,);
		if (ap) return -1;
		if (bp) return 1;
		return compareStrings(a.name, b.name,) || compareStrings(a.id, b.id,);
	},);
	if (!sortedZones.some((zone,) => zone.id === "default")) {
		sortedZones.push({ id: "default", name: "Default", items: [], },);
	}
	const zoneByNode = new Map<string, FlowZone>();
	for (const zone of sortedZones) {
		for (const item of zoneItems(zone,)) {
			const nodeId = resolveZoneItemNodeId(item, map.projectKey, nodeIds,);
			if (!nodeId) continue;
			const previous = zoneByNode.get(nodeId,);
			if (previous && previous.id !== zone.id) {
				diagnostics.push({
					code: "duplicate_zone_assignment",
					severity: "warning",
					message: `Flow node ${nodeId} appears in zones ${previous.name} and ${zone.name}.`,
					nodeIds: [nodeId,],
				},);
				continue;
			}
			zoneByNode.set(nodeId, zone,);
		}
	}
	const defaultZone = sortedZones.find((zone,) => zone.id === "default")!;
	for (const nodeId of nodeIds) {
		if (!zoneByNode.has(nodeId,)) zoneByNode.set(nodeId, defaultZone,);
	}

	const adjacency = flowAdjacency(map,);
	const nodeIdList = sorted(nodeIds,);
	const strong = strongComponents(nodeIdList, adjacency.outgoing, adjacency.selfLoops,);
	const layers = componentLayers(strong, map.edges,);
	const cyclicNodes = new Set(
		strong.filter((component,) => component.cyclic).flatMap((component,) => component.nodeIds),
	);
	const { components, componentByNode, } = weakComponents(map, adjacency.undirected, cyclicNodes,);
	for (const component of strong.filter((candidate,) => candidate.cyclic)) {
		diagnostics.push({
			code: "cycle_detected",
			severity: "warning",
			message: `Flow cycle contains ${component.nodeIds.length} node(s).`,
			nodeIds: component.nodeIds,
		},);
	}
	if (components.length > 1) {
		diagnostics.push({
			code: "disconnected_components",
			severity: "info",
			message: `Flow contains ${components.length} disconnected components.`,
			componentIds: components.map((component,) => component.id),
		},);
	}
	const isolated = nodeIdList.filter((nodeId,) =>
		(adjacency.undirected.get(nodeId,) ?? []).length === 0
	);
	if (isolated.length > 0) {
		diagnostics.push({
			code: "isolated_nodes",
			severity: "info",
			message: `Flow contains ${isolated.length} isolated node(s).`,
			nodeIds: isolated,
		},);
	}
	const defaultNodes = nodeIdList.filter((nodeId,) => zoneByNode.get(nodeId,)!.id === "default");
	if (defaultNodes.length > 0) {
		diagnostics.push({
			code: "default_zone_items",
			severity: "info",
			message: `${defaultNodes.length} visible flow node(s) remain in the default zone.`,
			nodeIds: defaultNodes,
		},);
	}
	const crossZoneEdges = map.edges.filter((edge,) => {
		const fromZone = zoneByNode.get(edge.from,);
		const toZone = zoneByNode.get(edge.to,);
		return fromZone !== undefined && toZone !== undefined && fromZone.id !== toZone.id;
	},).length;
	if (crossZoneEdges > 0) {
		diagnostics.push({
			code: "cross_zone_edges",
			severity: "info",
			message: `Flow contains ${crossZoneEdges} cross-zone edge(s).`,
			edgeCount: crossZoneEdges,
		},);
	}
	if (options.truncated === true) {
		diagnostics.push({
			code: "analysis_truncated",
			severity: "warning",
			message: "Layers, components, zones, and diagnostics cover only the returned truncated map.",
		},);
	}
	diagnostics.sort((a, b,) =>
		compareStrings(a.code, b.code,) || compareStrings(a.message, b.message,)
	);

	const analyzedNodes = map.nodes.map<AnalyzedFlowNode>((node,) => {
		const zone = zoneByNode.get(node.id,)!;
		return {
			...node,
			componentId: componentByNode.get(node.id,)!,
			layer: layers.get(node.id,) ?? 0,
			zoneId: zone.id,
			zoneName: zone.name,
		};
	},);
	const visibleByZone = new Map<string, string[]>();
	for (const node of analyzedNodes) {
		const ids = visibleByZone.get(node.zoneId,) ?? [];
		ids.push(node.id,);
		visibleByZone.set(node.zoneId, ids,);
	}
	const zones = sortedZones.map<FlowMapZone>((zone,) => {
		const position = validPosition(zone,);
		return {
			id: zone.id,
			name: zone.name,
			...(zone.color ? { color: zone.color, } : {}),
			...(position ? { position, } : {}),
			itemCount: zoneItems(zone,).length,
			nodeIds: sorted(visibleByZone.get(zone.id,) ?? [],),
		};
	},);

	return {
		...map,
		nodes: analyzedNodes,
		zones,
		components,
		diagnostics,
		topologyFingerprint: options.topologyFingerprint ?? flowTopologyFingerprint(map,),
	};
}

function readableNode(node: AnalyzedFlowNode,): string {
	return `${node.kind}:${node.name ?? node.id}`;
}

function mermaidLabel(value: string,): string {
	return value.replaceAll("&", "&amp;",).replaceAll('"', "&quot;",).replaceAll("<", "&lt;",)
		.replaceAll(">", "&gt;",).replaceAll(/\s+/g, " ",).trim();
}

function renderMermaid(map: AnalyzedFlowMap,): string {
	const idByNode = new Map(map.nodes.map((node, index,) => [node.id, `n${index}`,]),);
	const nodeById = new Map(map.nodes.map((node,) => [node.id, node,]),);
	const lines = ["flowchart LR",];
	for (const [zoneIndex, zone,] of map.zones.entries()) {
		if (zone.nodeIds.length === 0) continue;
		lines.push(`  subgraph z${zoneIndex}["${mermaidLabel(zone.name,)}"]`,);
		for (const nodeId of zone.nodeIds) {
			const node = nodeById.get(nodeId,)!;
			lines.push(`    ${idByNode.get(nodeId,)}["${mermaidLabel(readableNode(node,),)}"]`,);
		}
		lines.push("  end",);
	}
	for (const edge of map.edges) {
		const from = idByNode.get(edge.from,);
		const to = idByNode.get(edge.to,);
		if (from && to) lines.push(`  ${from} --> ${to}`,);
	}
	return lines.join("\n",);
}

function renderAscii(map: AnalyzedFlowMap,): string {
	const nodeById = new Map(map.nodes.map((node,) => [node.id, node,]),);
	const lines: string[] = [];
	for (const component of map.components) {
		lines.push(`${component.id}${component.cyclic ? " (cyclic)" : ""}`,);
		const layers = new Map<number, string[]>();
		for (const nodeId of component.nodeIds) {
			const node = nodeById.get(nodeId,)!;
			const entries = layers.get(node.layer,) ?? [];
			entries.push(`${readableNode(node,)} [${node.zoneName}]`,);
			layers.set(node.layer, entries,);
		}
		for (const [layer, entries,] of [...layers,].sort(([a,], [b,],) => a - b)) {
			lines.push(`  L${layer}: ${entries.sort(compareStrings,).join(" | ",)}`,);
		}
	}
	if (map.edges.length > 0) {
		lines.push("edges",);
		for (const edge of map.edges) lines.push(`  ${edge.from} -> ${edge.to} (${edge.relation})`,);
	}
	return lines.join("\n",);
}

export function renderFlowMap(map: AnalyzedFlowMap, format: FlowRenderFormat,): FlowMapRendering {
	return { format, content: format === "mermaid" ? renderMermaid(map,) : renderAscii(map,), };
}
