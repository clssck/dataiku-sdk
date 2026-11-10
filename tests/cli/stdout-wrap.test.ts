import { afterEach, beforeEach, describe, expect, it, } from "bun:test";
import { STDOUT_LINE_BUDGET_BYTES, stdoutJson, } from "../../src/cli/output.js";
import { dss, dssFailure, } from "./_harness.js";

const budget = STDOUT_LINE_BUDGET_BYTES;
const bytes = (text: string,) => Buffer.byteLength(text,);

function lineBytes(output: string,): number[] {
	return output.replace(/\n$/, "",).split("\n",).map(bytes,);
}

/** Lines over budget that hold more than one scalar-sized piece, i.e. should have been broken. */
function overBudgetLines(output: string,): string[] {
	return output.replace(/\n$/, "",).split("\n",).filter((line,) => bytes(line,) > budget);
}

const longStringValue = "x".repeat(5_000,);

const rows = Array.from({ length: 200, }, (_, i,) => [i, `name_${i}`, i % 2 === 0, null, i / 7,],);

function nested(depth: number,): unknown {
	let node: unknown = { leaf: "y".repeat(300,), other: [1, 2, 3,], };
	for (let level = 0; level < depth; level++) {
		node = {
			[`level_${level}`]: node,
			params: { a: "a".repeat(120,), b: "b".repeat(120,), c: { d: "d".repeat(200,), }, },
			list: Array.from({ length: 30, }, (_, i,) => ({ id: i, label: `label ${i}`, }),),
		};
	}
	return node;
}

// "é" is 2 bytes, "日" 3, "😀" 4: UTF-16 length understates the byte length.
const nonAscii = {
	two: "é".repeat(370,),
	three: "日".repeat(240,),
	four: "😀".repeat(180,),
	mixed: Array.from({ length: 40, }, (_, i,) => `${"日本語".repeat(10,)}-${i}`,),
};

const largeValues: Array<[string, unknown,]> = [
	["200 row arrays", rows,],
	["deeply nested settings", nested(6,),],
	["object with a 5 KB string", { id: "a", description: longStringValue, tail: [1, 2,], },],
	["non-ASCII strings near the budget", nonAscii,],
	["array of long strings", Array.from({ length: 50, }, (_, i,) => `${"s".repeat(100,)}${i}`,),],
];

const savedPretty = process.env.DSS_PRETTY;
beforeEach(() => {
	delete process.env.DSS_PRETTY;
},);
afterEach(() => {
	if (savedPretty === undefined) delete process.env.DSS_PRETTY;
	else process.env.DSS_PRETTY = savedPretty;
},);

describe("stdoutJson default layout", () => {
	it("keeps a result under the budget on one byte-identical line", () => {
		for (
			const value of [
				{ ok: true, },
				[1, 2, 3,],
				"short",
				null,
				{ a: [{ b: "é".repeat(100,), },], },
				{ big: "z".repeat(budget - 20,), },
			]
		) {
			expect(stdoutJson(value,),).toBe(`${JSON.stringify(value,)}\n`,);
		}
	});

	it("wraps by UTF-8 bytes, not UTF-16 units", () => {
		// 400 two-byte characters: 400 UTF-16 units, 800 bytes plus quotes.
		const value = ["é".repeat(300,), "é".repeat(100,),];
		const compact = JSON.stringify(value,);
		expect(compact.length,).toBeLessThan(budget,);
		expect(bytes(compact,),).toBeGreaterThan(budget,);
		const output = stdoutJson(value,);
		expect(output.split("\n",).length,).toBeGreaterThan(2,);
		expect(JSON.parse(output,),).toEqual(value,);
	});

	for (const [name, value,] of largeValues) {
		it(`wraps ${name} losslessly within the line budget`, () => {
			const compact = JSON.stringify(value,);
			const output = stdoutJson(value,);
			expect(JSON.parse(output,),).toEqual(JSON.parse(compact,),);
			expect(output.endsWith("\n",),).toBe(true,);
			for (const line of overBudgetLines(output,)) {
				// Only a single scalar may exceed the budget: `"key":"…long string…",` or the bare string.
				expect(line,).toMatch(/^ *(?:"(?:[^"\\]|\\.)*":)?"(?:[^"\\]|\\.)*",?$/,);
			}
			expect(bytes(output,),).toBeLessThanOrEqual(bytes(compact,) * 1.15,);
			const pretty = JSON.stringify(value, null, 2,);
			expect(bytes(output,),).toBeLessThan(bytes(pretty,),);
		});
	}

	it("keeps lines within budget when no scalar is oversized", () => {
		for (const value of [rows, nested(6,), nonAscii.mixed,]) {
			expect(Math.max(...lineBytes(stdoutJson(value,),),),).toBeLessThanOrEqual(budget,);
		}
	});

	it("indents one space per level and keeps fitting children compact", () => {
		const value = {
			a: "a".repeat(400,),
			b: { c: "c".repeat(400,), e: "e".repeat(400,), d: [1, 2,], },
		};
		expect(stdoutJson(value,),).toBe(
			`{\n "a":"${"a".repeat(400,)}",\n "b":{\n  "c":"${"c".repeat(400,)}",\n  "e":"${
				"e".repeat(400,)
			}",\n  "d":[1,2]\n }\n}\n`,
		);
	});

	it("applies the budget to the key, indent and trailing comma", () => {
		const key = "k".repeat(10,);
		// `{"kkkkkkkkkk":"…"}` at one space indent: 1 + 13 + value + 1 comma.
		const fits = "v".repeat(budget - 1 - (key.length + 3) - 1 - 2,);
		const value = { [key]: fits, next: "x".repeat(budget,), };
		const lines = stdoutJson(value,).split("\n",);
		expect(lines[1],).toBe(` "${key}":"${fits}",`,);
		expect(bytes(lines[1]!,),).toBe(budget,);
		const over = { [key]: `${fits}v`, next: 1, };
		// Compact parent is over budget; the child line itself is `<indent><key>:"…",` one byte too long.
		expect(bytes(stdoutJson(over,).split("\n",)[1]!,),).toBe(budget + 1,);
	});

	it("matches JSON.parse(JSON.stringify(value)) for toJSON, undefined and non-finite numbers", () => {
		const value = {
			when: new Date(0,),
			skipped: undefined,
			bad: [Number.NaN, Number.POSITIVE_INFINITY, undefined, () => 1,],
			custom: { toJSON: () => ({ replaced: "r".repeat(900,), }), },
			n: -0,
			order: { 2: "two", b: "b", 1: "one", a: "a".repeat(800,), },
		};
		const expected = JSON.parse(JSON.stringify(value,),);
		expect(JSON.parse(stdoutJson(value,),),).toEqual(expected,);
		expect(Object.keys(JSON.parse(stdoutJson(value,),).order,),).toEqual(
			Object.keys(expected.order,),
		);
	});

	it("handles top-level values that serialize to undefined as before", () => {
		expect(stdoutJson(undefined,),).toBe("undefined\n",);
		expect(stdoutJson(() => 1),).toBe("undefined\n",);
	});

	it("prints one over-budget string whole", () => {
		const output = stdoutJson(longStringValue,);
		expect(output,).toBe(`"${longStringValue}"\n`,);
	});

	it("stays linear on tens of megabytes", () => {
		const big = Array.from({ length: 200_000, }, (_, i,) => ({
			id: i,
			name: `row ${i}`,
			tags: ["a", "b", "c",],
			nested: { x: i, y: [i, i + 1,], },
		}),);
		const started = performance.now();
		const output = stdoutJson(big,);
		const elapsed = performance.now() - started;
		expect(JSON.parse(output,),).toEqual(big,);
		expect(Math.max(...lineBytes(output,),),).toBeLessThanOrEqual(budget,);
		expect(elapsed,).toBeLessThan(10_000,);
	});
});

describe("stdoutJson DSS_PRETTY modes", () => {
	it("DSS_PRETTY=0 is always one compact line", () => {
		process.env.DSS_PRETTY = "0";
		for (const [, value,] of largeValues) {
			expect(stdoutJson(value,),).toBe(`${JSON.stringify(value,)}\n`,);
		}
	});

	it("DSS_PRETTY=1 is the 2-space indented form", () => {
		process.env.DSS_PRETTY = "1";
		const value = { a: [1, 2,], b: { c: null, }, };
		expect(stdoutJson(value,),).toBe(`${JSON.stringify(value, null, 2,)}\n`,);
		expect(stdoutJson(rows,),).toBe(`${JSON.stringify(rows, null, 2,)}\n`,);
	});

	it("any other DSS_PRETTY value behaves as the default", () => {
		for (const other of ["", "true", "2", "yes",]) {
			process.env.DSS_PRETTY = other;
			expect(stdoutJson(rows,),).toBe(stdoutJsonDefault(rows,),);
		}
	});
});

function stdoutJsonDefault(value: unknown,): string {
	const saved = process.env.DSS_PRETTY;
	delete process.env.DSS_PRETTY;
	try {
		return stdoutJson(value,);
	} finally {
		if (saved !== undefined) process.env.DSS_PRETTY = saved;
	}
}

describe("real CLI stdout", () => {
	it("prints `commands run` as valid JSON with no over-budget line", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		const value = JSON.parse(stdout,);
		expect(value,).toBeTruthy();
		expect(Math.max(...lineBytes(stdout,),),).toBeLessThanOrEqual(budget,);
		expect(stdout.split("\n",).length,).toBeGreaterThan(2,);
		expect(stderr,).toBe("",);
	});

	it("prints an over-budget error envelope as valid wrapped JSON", async () => {
		const failure = await dssFailure([],);
		expect(failure.code,).toBe(1,);
		expect(JSON.parse(failure.stdout,),).toMatchObject({ type: "error", },);
		expect(Math.max(...lineBytes(failure.stdout,),),).toBeLessThanOrEqual(budget,);
		expect(failure.stdout,).toStartWith('{\n "type":"error",\n "error":',);
	});

	it("keeps stderr warning events one line each", async () => {
		const { stdout, stderr, } = await dss([
			"commands",
			"run",
			"--fields",
			"dataset.list.usage,datsaet.list.usage",
		],);
		expect(Object.keys(JSON.parse(stdout,),),).toEqual(["dataset.list.usage",],);
		const lines = stderr.trimEnd().split("\n",);
		expect(lines.length,).toBe(1,);
		expect(JSON.parse(lines[0]!,),).toMatchObject({ type: "warning", },);
	});
});
