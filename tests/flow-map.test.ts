import { describe, expect, it, } from "bun:test";
import { normalizeFlowGraph, } from "../src/utils/flow-map.js";

describe("normalizeFlowGraph", () => {
	describe("empty/invalid input", () => {
		it("returns empty graph with warning for null", () => {
			const result = normalizeFlowGraph(null, "PROJECT",);
			expect(result.nodes,).toEqual([],);
			expect(result.edges,).toEqual([],);
			expect(result.roots,).toEqual([],);
			expect(result.leaves,).toEqual([],);
			expect(result.projectKey,).toBe("PROJECT",);
			expect(result.warnings,).toHaveLength(1,);
			expect(result.warnings[0],).toBe("Flow graph response was not an object.",);
			expect(result.stats,).toEqual({
				nodeCount: 0,
				edgeCount: 0,
				datasets: 0,
				recipes: 0,
				roots: 0,
				leaves: 0,
			},);
		});

		it("returns empty graph with warning for undefined", () => {
			const result = normalizeFlowGraph(undefined, "P",);
			expect(result.warnings,).toHaveLength(1,);
			expect(result.warnings[0],).toBe("Flow graph response was not an object.",);
		});

		it("returns empty graph with warning for a string", () => {
			const result = normalizeFlowGraph("string", "P",);
			expect(result.warnings,).toHaveLength(1,);
			expect(result.warnings[0],).toBe("Flow graph response was not an object.",);
		});

		it("returns empty graph with no warnings for empty object", () => {
			const result = normalizeFlowGraph({}, "P",);
			expect(result.nodes,).toEqual([],);
			expect(result.edges,).toEqual([],);
			expect(result.warnings,).toEqual([],);
			expect(result.stats.nodeCount,).toBe(0,);
		});

		it("returns empty graph with warning for array input", () => {
			const result = normalizeFlowGraph([], "P",);
			expect(result.warnings[0],).toBe("Flow graph response was not an object.",);
		});
	});

	describe("minimal graph", () => {
		it("normalizes a single dataset node", () => {
			const result = normalizeFlowGraph(
				{
					nodes: { ds1: { type: "COMPUTABLE_DATASET", name: "my_dataset", }, },
					datasets: ["ds1",],
				},
				"PROJ",
			);

			expect(result.nodes,).toHaveLength(1,);
			expect(result.nodes[0].id,).toBe("ds1",);
			expect(result.nodes[0].kind,).toBe("dataset",);
			expect(result.nodes[0].name,).toBe("my_dataset",);
			expect(result.edges,).toHaveLength(0,);
			expect(result.roots,).toEqual(["ds1",],);
			expect(result.leaves,).toEqual(["ds1",],);
			expect(result.warnings,).toEqual([],);
		});

		it("normalizes a single recipe node", () => {
			const result = normalizeFlowGraph(
				{
					nodes: { r1: { type: "RECIPE", name: "my_recipe", }, },
					recipes: ["r1",],
				},
				"PROJ",
			);

			expect(result.nodes,).toHaveLength(1,);
			expect(result.nodes[0].kind,).toBe("recipe",);
			expect(result.nodes[0].name,).toBe("my_recipe",);
		});
	});

	describe("recipe with inputs/outputs", () => {
		it("creates edges from predecessor/successor relationships", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						ds_in: { type: "COMPUTABLE_DATASET", name: "input", },
						compute_r: {
							type: "RECIPE",
							name: "recipe_1",
							predecessors: ["ds_in",],
							successors: ["ds_out",],
						},
						ds_out: { type: "COMPUTABLE_DATASET", name: "output", },
					},
					datasets: ["ds_in", "ds_out",],
					recipes: ["compute_r",],
				},
				"PROJ",
			);

			expect(result.nodes,).toHaveLength(3,);
			expect(result.edges,).toHaveLength(2,);

			// Edges are sorted by from, then to
			const readEdge = result.edges.find((e,) => e.from === "ds_in");
			expect(readEdge,).toBeTruthy();
			expect(readEdge!.to,).toBe("compute_r",);
			expect(readEdge!.relation,).toBe("reads",);

			const writeEdge = result.edges.find((e,) => e.from === "compute_r");
			expect(writeEdge,).toBeTruthy();
			expect(writeEdge!.to,).toBe("ds_out",);
			expect(writeEdge!.relation,).toBe("writes",);

			expect(result.roots,).toEqual(["ds_in",],);
			expect(result.leaves,).toEqual(["ds_out",],);

			expect(result.stats,).toEqual({
				nodeCount: 3,
				edgeCount: 2,
				datasets: 2,
				recipes: 1,
				roots: 1,
				leaves: 1,
			},);
		});
	});

	describe("edge relation inference", () => {
		it("infers 'reads' for dataset→recipe", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						d: { type: "COMPUTABLE_DATASET", name: "d", successors: ["r",], },
						r: { type: "RECIPE", name: "r", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "d" && e.to === "r");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("reads",);
		});

		it("infers 'writes' for recipe→dataset", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						r: { type: "RECIPE", name: "r", successors: ["d",], },
						d: { type: "COMPUTABLE_DATASET", name: "d", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "r" && e.to === "d");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("writes",);
		});

		it("infers 'depends_on' for recipe→recipe", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						r1: { type: "RECIPE", name: "r1", successors: ["r2",], },
						r2: { type: "RECIPE", name: "r2", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "r1" && e.to === "r2");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("depends_on",);
		});

		it("infers 'unknown' when either node is 'other'", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						x: { type: "UNKNOWN_THING", name: "x", successors: ["d",], },
						d: { type: "COMPUTABLE_DATASET", name: "d", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "x" && e.to === "d");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("unknown",);
		});

		it("infers 'reads' for folder→recipe", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						f: { type: "MANAGED_FOLDER", name: "f", successors: ["r",], },
						r: { type: "RECIPE", name: "r", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "f" && e.to === "r");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("reads",);
		});

		it("infers 'writes' for recipe→folder", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						r: { type: "RECIPE", name: "r", successors: ["f",], },
						f: { type: "MANAGED_FOLDER", name: "f", },
					},
				},
				"P",
			);

			const edge = result.edges.find((e,) => e.from === "r" && e.to === "f");
			expect(edge,).toBeTruthy();
			expect(edge!.relation,).toBe("writes",);
		});
	});

	describe("folder name resolution", () => {
		it("uses friendly name from folderNamesById", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						abc123: { type: "MANAGED_FOLDER", name: "abc123", },
					},
					folders: ["abc123",],
				},
				"P",
				{ folderNamesById: { abc123: "My Reports", }, },
			);

			const node = result.nodes.find((n,) => n.id === "abc123");
			expect(node,).toBeTruthy();
			expect(node!.name,).toBe("My Reports",);
			expect(node!.kind,).toBe("folder",);
		});

		it("falls back to id when folderNamesById has no entry", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						f1: { type: "MANAGED_FOLDER", },
					},
				},
				"P",
				{ folderNamesById: {}, },
			);

			const node = result.nodes.find((n,) => n.id === "f1");
			expect(node,).toBeTruthy();
			expect(node!.name,).toBe("f1",);
		});
	});

	describe("warnings for bad data", () => {
		it("warns and skips non-object node value", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						good: { type: "COMPUTABLE_DATASET", name: "good", },
						bad: "not_an_object",
					},
				},
				"P",
			);

			expect(result.nodes,).toHaveLength(1,);
			expect(result.nodes[0].id,).toBe("good",);
			const warn = result.warnings.find((w,) => w.includes("bad",));
			expect(warn,).toBeTruthy();
			expect(warn!.includes("not an object",),).toBe(true,);
		});

		it("warns for non-array predecessors", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						r: { type: "RECIPE", name: "r", predecessors: "not_an_array", },
					},
				},
				"P",
			);

			const warn = result.warnings.find((w,) => w.includes("predecessors",));
			expect(warn,).toBeTruthy();
			expect(warn!.includes("non-array",),).toBe(true,);
		});

		it("warns for non-object nodes field", () => {
			const result = normalizeFlowGraph(
				{ nodes: "bad", },
				"P",
			);

			const warn = result.warnings.find((w,) => w.includes("nodes",));
			expect(warn,).toBeTruthy();
		});
	});

	describe("stats accuracy", () => {
		it("stats match actual array lengths", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						d1: { type: "COMPUTABLE_DATASET", name: "d1", },
						d2: { type: "COMPUTABLE_DATASET", name: "d2", },
						r1: {
							type: "RECIPE",
							name: "r1",
							predecessors: ["d1",],
							successors: ["d2",],
						},
					},
					datasets: ["d1", "d2",],
					recipes: ["r1",],
				},
				"P",
			);

			expect(result.stats.nodeCount,).toBe(result.nodes.length,);
			expect(result.stats.edgeCount,).toBe(result.edges.length,);
			expect(result.stats.roots,).toBe(result.roots.length,);
			expect(result.stats.leaves,).toBe(result.leaves.length,);
			expect(result.stats.datasets,).toBe(
				result.nodes.filter((n,) => n.kind === "dataset").length,
			);
			expect(result.stats.recipes,).toBe(
				result.nodes.filter((n,) => n.kind === "recipe").length,
			);
		});
	});

	describe("deduplication", () => {
		it("node in both nodes object and datasets array appears once", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						ds1: { type: "COMPUTABLE_DATASET", name: "my_dataset", },
					},
					datasets: ["ds1",],
				},
				"P",
			);

			const matches = result.nodes.filter((n,) => n.id === "ds1");
			expect(matches,).toHaveLength(1,);
			expect(matches[0].name,).toBe("my_dataset",);
			expect(matches[0].kind,).toBe("dataset",);
		});

		it("duplicate edges are deduplicated", () => {
			// Both predecessor and successor point to the same relationship
			const result = normalizeFlowGraph(
				{
					nodes: {
						d: {
							type: "COMPUTABLE_DATASET",
							name: "d",
							successors: ["r",],
						},
						r: {
							type: "RECIPE",
							name: "r",
							predecessors: ["d",],
						},
					},
				},
				"P",
			);

			const edges = result.edges.filter(
				(e,) => e.from === "d" && e.to === "r",
			);
			expect(edges,).toHaveLength(1,);
			expect(edges[0].relation,).toBe("reads",);
		});
	});

	describe("projectKey passthrough", () => {
		it("preserves the projectKey in output", () => {
			const result = normalizeFlowGraph({}, "MY_PROJECT",);
			expect(result.projectKey,).toBe("MY_PROJECT",);
		});
	});

	describe("node kind inference", () => {
		it("infers folder kind from type containing FOLDER", () => {
			const result = normalizeFlowGraph(
				{
					nodes: { f: { type: "MANAGED_FOLDER", name: "f", }, },
				},
				"P",
			);
			expect(result.nodes[0].kind,).toBe("folder",);
		});

		it("infers other for unrecognized types", () => {
			const result = normalizeFlowGraph(
				{
					nodes: { x: { type: "STREAMING_ENDPOINT", name: "x", }, },
				},
				"P",
			);
			expect(result.nodes[0].kind,).toBe("other",);
		});

		it("infers implicit subtype from IMPLICIT_RECIPE type", () => {
			const result = normalizeFlowGraph(
				{
					nodes: { r: { type: "IMPLICIT_RECIPE", name: "r", }, },
				},
				"P",
			);
			expect(result.nodes[0].subtype,).toBe("implicit",);
		});
	});

	describe("sorting", () => {
		it("nodes are sorted by id", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						z_node: { type: "COMPUTABLE_DATASET", name: "z", },
						a_node: { type: "COMPUTABLE_DATASET", name: "a", },
						m_node: { type: "COMPUTABLE_DATASET", name: "m", },
					},
				},
				"P",
			);

			expect(
				result.nodes.map((n,) => n.id),
			).toEqual(["a_node", "m_node", "z_node",],);
		});

		it("roots and leaves are sorted alphabetically", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						z: { type: "COMPUTABLE_DATASET", name: "z", },
						a: { type: "COMPUTABLE_DATASET", name: "a", },
					},
				},
				"P",
			);

			expect(result.roots,).toEqual(["a", "z",],);
			expect(result.leaves,).toEqual(["a", "z",],);
		});
	});

	describe("placeholder nodes", () => {
		it("creates placeholder when edge references missing node", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						r: { type: "RECIPE", name: "r", successors: ["missing_ds",], },
					},
				},
				"P",
			);

			const placeholder = result.nodes.find((n,) => n.id === "missing_ds");
			expect(placeholder,).toBeTruthy();
			expect(placeholder!.kind,).toBe("other",);
			expect(placeholder!.name,).toBe("missing_ds",);

			const warn = result.warnings.find((w,) => w.includes("missing_ds",));
			expect(warn,).toBeTruthy();
			expect(warn!.includes("placeholder",),).toBe(true,);
		});
	});

	describe("connection extraction", () => {
		it("extracts direct connection field", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						d: { type: "COMPUTABLE_DATASET", name: "d", connection: "pg_prod", },
					},
				},
				"P",
			);
			expect(result.nodes[0].connection,).toBe("pg_prod",);
		});

		it("extracts connection from params.connection", () => {
			const result = normalizeFlowGraph(
				{
					nodes: {
						d: {
							type: "COMPUTABLE_DATASET",
							name: "d",
							params: { connection: "s3_raw", },
						},
					},
				},
				"P",
			);
			expect(result.nodes[0].connection,).toBe("s3_raw",);
		});
	});
});
