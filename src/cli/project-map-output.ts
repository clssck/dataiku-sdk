import type { FlowMapResult, } from "../resources/projects.js";
import type {
	AnalyzedFlowMap,
	AnalyzedFlowNode,
	FlowMapComponent,
	FlowMapZone,
} from "../utils/flow-analysis.js";

/**
 * `dss project map` output: the SDK `FlowMapResult` with derivable duplicates
 * removed at the CLI boundary. Lossless — every dropped field can be rebuilt:
 * node `name` defaults to `id`, `zoneName` comes from `zones[]`, zone/component
 * membership comes from each node's `zoneId`/`componentId`, an absent
 * `warnings` means none, and an absent `truncation` means nothing was cut.
 * `topologyFingerprint` is passed through untouched.
 */
export type CompactFlowMapNode = Omit<AnalyzedFlowNode, "zoneName">;
export type CompactFlowMapZone = Omit<FlowMapZone, "nodeIds">;
export type CompactFlowMapComponent = Omit<FlowMapComponent, "nodeIds">;

export interface CompactFlowMap
	extends Omit<AnalyzedFlowMap, "nodes" | "zones" | "components" | "warnings">
{
	nodes: CompactFlowMapNode[];
	zones: CompactFlowMapZone[];
	components: CompactFlowMapComponent[];
	warnings?: string[];
}

export interface CompactFlowMapResult extends Omit<FlowMapResult, "map" | "truncation"> {
	map: CompactFlowMap;
	truncation?: FlowMapResult["truncation"];
}

function compactNode(node: AnalyzedFlowNode,): CompactFlowMapNode {
	const { zoneName: _zoneName, name, ...rest } = node;
	return name === undefined || name === node.id ? rest : { ...rest, name, };
}

export function compactProjectMapOutput(result: FlowMapResult,): CompactFlowMapResult {
	const { nodes, zones, components, warnings, ...mapRest } = result.map;
	const map: CompactFlowMap = {
		...mapRest,
		nodes: nodes.map(compactNode,),
		zones: zones.map(({ nodeIds: _nodeIds, ...zone },) => zone),
		components: components.map(({ nodeIds: _nodeIds, ...component },) => component),
	};
	if (warnings.length > 0) map.warnings = warnings;
	const { map: _map, truncation, ...resultRest } = result;
	return {
		map,
		...(truncation.truncated ? { truncation, } : {}),
		...resultRest,
	};
}
