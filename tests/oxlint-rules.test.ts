import { expect, it, } from "bun:test";
import { mkdtemp, rm, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { basename, join, resolve, } from "node:path";

async function lint(rule: string, cases: Record<string, string>,): Promise<string[]> {
	const directory = await mkdtemp(join(tmpdir(), "dss-lint-",),);
	try {
		const config = join(directory, "config.json",);
		await writeFile(
			config,
			JSON.stringify({
				categories: { correctness: "off", },
				jsPlugins: [{ name: "dss", specifier: resolve("tools/oxlint/index.ts",), },],
				rules: { ["dss/" + rule]: "error", },
			},),
		);
		for (const [name, code,] of Object.entries(cases,)) {
			await writeFile(join(directory, name + ".ts",), code,);
		}
		const child = Bun.spawn([
			process.execPath,
			resolve("node_modules/oxlint/bin/oxlint",),
			"--config",
			config,
			"--format",
			"json",
			"--no-ignore",
			directory,
		], { stdout: "pipe", stderr: "pipe", },);
		const [stdout, stderr, exit,] = await Promise.all([
			new Response(child.stdout,).text(),
			new Response(child.stderr,).text(),
			child.exited,
		],);
		expect(stderr,).toBe("",);
		expect(exit,).toBe(1,);
		const report = JSON.parse(stdout,) as { diagnostics: { filename: string; severity: string; }[]; };
		for (const diagnostic of report.diagnostics) expect(diagnostic.severity,).toBe("error",);
		return report.diagnostics.map(diagnostic => basename(diagnostic.filename,)).sort();
	} finally {
		await rm(directory, { recursive: true, force: true, },);
	}
}

it("rejects nested assertions while allowing a single validated boundary", async () => {
	expect(
		await lint("no-chained-type-assertions", {
			bad: "declare const value: string; export const result = (value as unknown) as number;",
			angle: "declare const value: string; export const result = <number><unknown>value;",
			good:
				"declare const value: unknown; export const result = typeof value === 'string' ? value : undefined;",
			single: "declare const value: unknown; export const result = value as string;",
		},),
	).toEqual(["angle.ts", "bad.ts",],);
});

it("tracks widened bindings without confusing shadowed or runtime-built values", async () => {
	expect(
		await lint("no-widen-then-assert", {
			bad:
				"export function f(input: string) { const widened: unknown = input; return widened as string; }",
			shadow:
				"export function f(input: string) { const widened: unknown = input; return function(widened: unknown) { return widened as string; }; }",
			boundary:
				"export function f(input: unknown) { const value: unknown = input; return value as string; }",
			built:
				"export function f(input: object) { const out: Record<string, unknown> = {}; Object.assign(out, input); return out as typeof input; }",
		},),
	).toEqual(["bad.ts",],);
});

it("detects repeated accumulator copies but permits mutation and shadowed names", async () => {
	expect(
		await lint("no-reduce-accumulator-copy", {
			concat: "export const result = [1].reduce((acc, x) => acc.concat(x), []);",
			assign: "export const result = [1].reduce((acc, x) => Object.assign({}, acc, {x}), {});",
			from: "export const result = [1].reduce((acc, x) => Array.from(acc), []);",
			slice: "export const result = [1].reduceRight((acc, x) => acc['slice'](), []);",
			good: "export const result = [1].reduce((acc, x) => { acc.push(x); return acc; }, []);",
			shadow:
				"export const result = [1].reduce((acc, x) => { const copy = (acc) => acc.slice(); acc.push(copy([x])); return acc; }, []);",
			mutate: "export const result = [1].reduce((acc, x) => Object.assign(acc, {x}), {});",
		},),
	).toEqual(["assign.ts", "concat.ts", "from.ts", "slice.ts",],);
});
