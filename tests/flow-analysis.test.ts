import { describe, expect, it, } from "bun:test";
import {
	analyzeFlowMap,
	flowTopologyFingerprint,
	renderFlowMap,
} from "../src/utils/flow-analysis.js";
import { normalizeFlowGraph, } from "../src/utils/flow-map.js";

function analyzedFixture() {
	const normalized = normalizeFlowGraph({
		nodes: {
			raw: { type: "COMPUTABLE_DATASET", name: "raw", successors: ["prepare",], },
			prepare: { type: "RECIPE", name: "prepare", successors: ["clean",], },
			clean: { type: "COMPUTABLE_DATASET", name: "clean", },
			isolated: { type: "COMPUTABLE_DATASET", name: "isolated", },
		},
	}, "TEST",);
	return analyzeFlowMap(normalized, [
		{
			id: "raw-zone",
			name: "Raw",
			color: "#64748b",
			position: { x: 10, y: 20, },
			items: [{ objectType: "DATASET", objectId: "raw", },],
		},
		{
			id: "prepared-zone",
			name: "Prepared",
			items: [
				{ objectType: "RECIPE", objectId: "prepare", },
				{ objectType: "DATASET", objectId: "clean", },
			],
		},
	],);
}

describe("flow map analysis", () => {
	it("joins zones and computes layers, components, and diagnostics", () => {
		const result = analyzedFixture();
		const byId = new Map(result.nodes.map((node,) => [node.id, node,]),);

		expect(byId.get("raw",),).toMatchObject({ layer: 0, zoneId: "raw-zone", zoneName: "Raw", },);
		expect(byId.get("prepare",),).toMatchObject({ layer: 1, zoneId: "prepared-zone", },);
		expect(byId.get("clean",),).toMatchObject({ layer: 2, zoneId: "prepared-zone", },);
		expect(byId.get("isolated",),).toMatchObject({
			layer: 0,
			zoneId: "default",
			zoneName: "Default",
		},);
		expect(result.zones.find((zone,) => zone.id === "raw-zone"),).toMatchObject({
			color: "#64748b",
			position: { x: 10, y: 20, },
			nodeIds: ["raw",],
		},);
		expect(result.components,).toHaveLength(2,);
		expect(result.components[0]?.nodeIds,).toEqual(["clean", "prepare", "raw",],);
		expect(result.diagnostics.map((diagnostic,) => diagnostic.code),).toEqual([
			"cross_zone_edges",
			"default_zone_items",
			"disconnected_components",
			"isolated_nodes",
		],);
	});

	it("collapses cycles before assigning topological layers", () => {
		const normalized = normalizeFlowGraph({
			nodes: {
				a: { type: "RECIPE", successors: ["b",], },
				b: { type: "RECIPE", successors: ["a", "out",], },
				out: { type: "COMPUTABLE_DATASET", },
			},
		}, "TEST",);
		const result = analyzeFlowMap(normalized, [],);
		const byId = new Map(result.nodes.map((node,) => [node.id, node,]),);

		expect(byId.get("a",)?.layer,).toBe(0,);
		expect(byId.get("b",)?.layer,).toBe(0,);
		expect(byId.get("out",)?.layer,).toBe(1,);
		expect(result.components,).toEqual([
			expect.objectContaining({ cyclic: true, nodeIds: ["a", "b", "out",], },),
		],);
		expect(result.diagnostics,).toContainEqual(
			expect.objectContaining({ code: "cycle_detected", nodeIds: ["a", "b",], },),
		);
	});

	it("fingerprints topology independently from visual metadata", () => {
		const normalized = normalizeFlowGraph({
			nodes: {
				a: { type: "COMPUTABLE_DATASET", successors: ["r",], },
				r: { type: "RECIPE", },
			},
		}, "TEST",);
		const first = analyzeFlowMap(normalized, [{
			id: "zone",
			name: "Zone A",
			position: { x: 1, y: 2, },
			items: [{ objectType: "DATASET", objectId: "a", },],
		},],);
		const second = analyzeFlowMap(normalized, [{
			id: "zone",
			name: "Renamed",
			position: { x: 500, y: -30, },
			items: [{ objectType: "RECIPE", objectId: "r", },],
		},],);
		const changed = normalizeFlowGraph({
			nodes: {
				a: { type: "COMPUTABLE_DATASET", successors: ["r",], },
				r: { type: "RECIPE", successors: ["out",], },
				out: { type: "COMPUTABLE_DATASET", },
			},
		}, "TEST",);

		expect(first.topologyFingerprint,).toBe(second.topologyFingerprint,);
		expect(first.topologyFingerprint,).toBe(flowTopologyFingerprint(normalized,),);
		expect(flowTopologyFingerprint(changed,),).not.toBe(first.topologyFingerprint,);
	});

	it("renders optional Mermaid and ASCII views", () => {
		const result = analyzedFixture();
		result.nodes.find((node,) => node.id === "raw")!.name = 'raw & "quoted"';
		const mermaid = renderFlowMap(result, "mermaid",);
		const ascii = renderFlowMap(result, "ascii",);

		expect(mermaid.format,).toBe("mermaid",);
		expect(mermaid.content,).toContain("flowchart LR",);
		expect(mermaid.content,).toContain("raw &amp; &quot;quoted&quot;",);
		expect(ascii.format,).toBe("ascii",);
		expect(ascii.content,).toContain("L0:",);
		expect(ascii.content,).toContain('dataset:raw & "quoted" [Raw]',);
		expect(ascii.content,).toContain("raw -> prepare (reads)",);
	});

	it("handles deep flows without recursive graph traversal", () => {
		const count = 12_000;
		const nodes: Record<string, { type: string; successors?: string[]; }> = {};
		for (let index = 0; index < count; index += 1) {
			const id = `node-${index.toString().padStart(5, "0",)}`;
			nodes[id] = {
				type: "RECIPE",
				...(index + 1 < count
					? { successors: [`node-${(index + 1).toString().padStart(5, "0",)}`,], }
					: {}),
			};
		}
		const result = analyzeFlowMap(normalizeFlowGraph({ nodes, }, "TEST",), [],);
		expect(result.nodes,).toHaveLength(count,);
		expect(result.nodes.at(-1,)?.layer,).toBe(count - 1,);
	});

	it("ignores dangling edges consistently during diagnostics", () => {
		const normalized = normalizeFlowGraph(
			{ nodes: { only: { type: "COMPUTABLE_DATASET", }, }, },
			"TEST",
		);
		normalized.edges.push({ from: "only", to: "missing", relation: "unknown", },);

		const result = analyzeFlowMap(normalized, [],);
		expect(result.diagnostics,).not.toContainEqual(
			expect.objectContaining({ code: "cross_zone_edges", },),
		);
	});

	it("marks analysis derived from a truncated map", () => {
		const normalized = normalizeFlowGraph(
			{ nodes: { only: { type: "COMPUTABLE_DATASET", }, }, },
			"TEST",
		);
		const result = analyzeFlowMap(normalized, [], { truncated: true, },);
		expect(result.diagnostics,).toContainEqual(
			expect.objectContaining({ code: "analysis_truncated", },),
		);
	});
});
