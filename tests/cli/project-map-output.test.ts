import { describe, expect, it, } from "bun:test";
import { compactProjectMapOutput, } from "../../src/cli/project-map-output.js";
import type { FlowMapResult, } from "../../src/resources/projects.js";
import { analyzeFlowMap, flowTopologyFingerprint, } from "../../src/utils/flow-analysis.js";
import { normalizeFlowGraph, } from "../../src/utils/flow-map.js";

function sdkResult(truncated: boolean,): FlowMapResult {
	const normalized = normalizeFlowGraph({
		nodes: {
			raw: { type: "COMPUTABLE_DATASET", name: "raw", successors: ["prepare",], },
			prepare: { type: "RECIPE", name: "Prepare raw", successors: ["clean",], },
			clean: { type: "COMPUTABLE_DATASET", name: "clean", },
		},
	}, "TEST",);
	const map = analyzeFlowMap(normalized, [{
		id: "raw-zone",
		name: "Raw",
		items: [{ objectType: "DATASET", objectId: "raw", },],
	},],);
	return {
		map,
		truncation: {
			truncated,
			maxNodes: null,
			maxEdges: null,
			nodeCountBefore: 3,
			nodeCountAfter: 3,
			edgeCountBefore: 2,
			edgeCountAfter: 2,
		},
	};
}

describe("compactProjectMapOutput", () => {
	it("drops derivable duplicates while keeping membership recoverable from node ids", () => {
		const full = sdkResult(false,);
		const compact = compactProjectMapOutput(full,);
		const byId = new Map(compact.map.nodes.map((node,) => [node.id, node,]),);

		expect("name" in byId.get("raw",)!,).toBe(false,);
		expect(byId.get("prepare",)?.name,).toBe("Prepare raw",);
		expect(compact.map.nodes.some((node,) => "zoneName" in node),).toBe(false,);
		expect(byId.get("raw",)?.zoneId,).toBe("raw-zone",);

		expect(compact.map.zones.map((zone,) => zone.name).sort(),).toEqual(["Default", "Raw",],);
		for (const zone of compact.map.zones) expect("nodeIds" in zone,).toBe(false,);
		for (const component of compact.map.components) expect("nodeIds" in component,).toBe(false,);
		expect(compact.map.components[0]?.rootIds,).toEqual(["raw",],);
		expect(compact.map.components[0]?.leafIds,).toEqual(["clean",],);

		// Membership stays derivable.
		const zoneMembers = full.map.zones.map((zone,) => [
			zone.id,
			compact.map.nodes.filter((node,) => node.zoneId === zone.id).map((node,) => node.id).sort(),
		]);
		for (const [zoneId, members,] of zoneMembers) {
			expect(members,).toEqual(full.map.zones.find((zone,) => zone.id === zoneId)!.nodeIds,);
		}
	});

	it("omits empty warnings and an untruncated truncation summary", () => {
		const compact = compactProjectMapOutput(sdkResult(false,),);
		expect("warnings" in compact.map,).toBe(false,);
		expect("truncation" in compact,).toBe(false,);
	});

	it("keeps warnings and truncation when the map was cut", () => {
		const cut = sdkResult(true,);
		cut.map.warnings = ["Flow map truncated.",];
		const compact = compactProjectMapOutput(cut,);
		expect(compact.map.warnings,).toEqual(["Flow map truncated.",],);
		expect(compact.truncation?.truncated,).toBe(true,);
	});

	it("leaves the topology fingerprint and the SDK model untouched", () => {
		const full = sdkResult(false,);
		const before = JSON.stringify(full,);
		const compact = compactProjectMapOutput(full,);
		expect(compact.map.topologyFingerprint,).toBe(full.map.topologyFingerprint,);
		expect(full.map.topologyFingerprint,).toBe(
			flowTopologyFingerprint(
				normalizeFlowGraph({
					nodes: {
						raw: { type: "COMPUTABLE_DATASET", name: "raw", successors: ["prepare",], },
						prepare: { type: "RECIPE", name: "Prepare raw", successors: ["clean",], },
						clean: { type: "COMPUTABLE_DATASET", name: "clean", },
					},
				}, "TEST",),
			),
		);
		expect(JSON.stringify(full,),).toBe(before,);
	});
});
