import { describe, expect, it, } from "bun:test";
import { deepMerge, } from "../src/utils/deep-merge.js";

describe("deepMerge", () => {
	it("combines non-overlapping keys", () => {
		expect(
			deepMerge({ a: 1, }, { b: 2, },),
		).toEqual({ a: 1, b: 2, },);
	});

	it("overrides primitive values", () => {
		expect(
			deepMerge({ a: 1, }, { a: 2, },),
		).toEqual({ a: 2, },);
	});

	it("recursively merges nested plain objects", () => {
		expect(
			deepMerge({ a: { x: 1, y: 2, }, }, { a: { y: 3, z: 4, }, },),
		).toEqual({ a: { x: 1, y: 3, z: 4, }, },);
	});

	it("replaces arrays instead of merging them", () => {
		expect(
			deepMerge({ a: [1, 2,], }, { a: [3,], },),
		).toEqual({ a: [3,], },);
	});

	it("replaces object with null", () => {
		expect(
			deepMerge({ a: { x: 1, }, }, { a: null, },),
		).toEqual({ a: null, },);
	});

	it("replaces value with undefined", () => {
		const result = deepMerge({ a: 1, }, { a: undefined, },);
		expect("a" in result,).toBeTruthy();
		expect(result.a,).toBe(undefined,);
	});

	it("handles 3 levels of nesting", () => {
		expect(
			deepMerge(
				{ a: { b: { c: 1, d: 2, }, e: 3, }, },
				{ a: { b: { c: 10, f: 4, }, g: 5, }, },
			),
		).toEqual({ a: { b: { c: 10, d: 2, f: 4, }, e: 3, g: 5, }, },);
	});

	it("returns shallow copy of base when patch is empty", () => {
		const base = { a: 1, };
		const result = deepMerge(base, {},);
		expect(result,).toEqual({ a: 1, },);
		expect(result,).not.toBe(base,);
	});

	it("returns patch content when base is empty", () => {
		expect(
			deepMerge({}, { a: 1, },),
		).toEqual({ a: 1, },);
	});

	it("does not mutate the base object", () => {
		const base = { a: { x: 1, y: 2, }, b: 3, };
		const baseCopy = structuredClone(base,);
		deepMerge(base, { a: { y: 99, }, b: 100, },);
		expect(base,).toEqual(baseCopy,);
	});

	it("replaces base object with array from patch", () => {
		expect(
			deepMerge({ a: { x: 1, }, }, { a: [1, 2,], },),
		).toEqual({ a: [1, 2,], },);
	});
});
