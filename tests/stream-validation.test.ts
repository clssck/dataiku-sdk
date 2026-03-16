import { describe, expect, it, } from "bun:test";
import { validateStreamColumns, } from "../src/resources/datasets.js";

describe("validateStreamColumns", () => {
	const expected = [
		{ name: "id", },
		{ name: "name", },
		{ name: "amount", },
	];

	it("returns no warnings when columns match exactly", () => {
		const warnings = validateStreamColumns(["id", "name", "amount",], expected,);
		expect(warnings,).toHaveLength(0,);
	});

	it("returns no warnings when columns match in different order", () => {
		const warnings = validateStreamColumns(["amount", "id", "name",], expected,);
		expect(warnings,).toHaveLength(0,);
	});

	it("detects missing expected columns", () => {
		const warnings = validateStreamColumns(["id", "name",], expected,);
		expect(warnings,).toHaveLength(1,);
		expect(warnings[0],).toContain("Missing expected column",);
		expect(warnings[0],).toContain("amount",);
	});

	it("detects unexpected columns in stream", () => {
		const warnings = validateStreamColumns(["id", "name", "amount", "extra",], expected,);
		expect(warnings,).toHaveLength(1,);
		expect(warnings[0],).toContain("Unexpected column",);
		expect(warnings[0],).toContain("extra",);
	});

	it("detects both missing and unexpected simultaneously", () => {
		const warnings = validateStreamColumns(["id", "extra", "other",], expected,);
		const missing = warnings.filter((w,) => w.includes("Missing",));
		const unexpected = warnings.filter((w,) => w.includes("Unexpected",));
		expect(missing,).toHaveLength(2,); // name, amount
		expect(unexpected,).toHaveLength(2,); // extra, other
	});

	it("handles empty header row", () => {
		const warnings = validateStreamColumns([], expected,);
		expect(warnings,).toHaveLength(3,); // all 3 expected columns missing
	});

	it("handles empty expected columns", () => {
		const warnings = validateStreamColumns(["id", "name",], [],);
		expect(warnings,).toHaveLength(2,); // both are unexpected
	});

	it("handles both empty", () => {
		const warnings = validateStreamColumns([], [],);
		expect(warnings,).toHaveLength(0,);
	});
});
