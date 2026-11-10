import { expect, it, } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile, } from "node:fs/promises";
import { tmpdir, } from "node:os";
import { basename, dirname, join, resolve, } from "node:path";

async function lintWith(
	rules: Record<string, unknown>,
	cases: Record<string, string>,
	overrides: unknown[] = [],
): Promise<string[]> {
	const directory = await mkdtemp(join(tmpdir(), "dss-lint-",),);
	try {
		const config = join(directory, "config.json",);
		await writeFile(
			config,
			JSON.stringify({
				categories: { correctness: "off", },
				jsPlugins: [{ name: "dss", specifier: resolve("tools/oxlint/index.ts",), },],
				rules,
				overrides,
			},),
		);
		for (const [name, code,] of Object.entries(cases,)) {
			const file = join(directory, `${name}.ts`,);
			await mkdir(dirname(file,), { recursive: true, },);
			await writeFile(file, code,);
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
		return report.diagnostics.map(diagnostic => basename(diagnostic.filename,),).sort();
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

it("requires coded errors in src, including promise rejections", async () => {
	expect(
		await lint("no-uncoded-errors", {
			"src/thrown": "export function f() { throw new Error('x'); }",
			"src/typed": "export function f() { throw new TypeError('x'); }",
			"src/rejected": "export const p = new Promise((_r, reject) => reject(new Error('x')));",
			"src/coded": "class Coded extends Error {} export function f() { throw new Coded('x'); }",
			"src/wrapped": "export const e = (x: unknown) => x instanceof Error ? x : new Error(String(x));",
			"src/promise": "export const p = Promise.reject(new RangeError('x'));",
			"src/callback": "export function f(cb: (e: Error) => void) { cb(new Error('x')); }",
			"src/check": "export const isErr = (x: unknown) => x instanceof Error;",
			"outside": "export function f() { throw new Error('x'); }",
		},),
	).toEqual(["callback.ts", "promise.ts", "rejected.ts", "thrown.ts", "typed.ts", "wrapped.ts",],);
});

it("allows JSON.parse only in the try block of a try/catch in resources", async () => {
	expect(
		await lint("no-raw-json-parse-in-resources", {
			"src/resources/bare": "export const v = JSON.parse('1');",
			"src/resources/guarded":
				"export function f() { try { return JSON.parse('1'); } catch { return 0; } }",
			"src/resources/incatch":
				"export function f() { try { return 1; } catch { return JSON.parse('1'); } }",
			"src/resources/finallyonly":
				"export function f() { try { return JSON.parse('1'); } finally { void 0; } }",
			"src/cli/elsewhere": "export const v = JSON.parse('1');",
		},),
	).toEqual(["bare.ts", "finallyonly.ts", "incatch.ts",],);
});

it("confines process.env reads to the environment-owning modules", async () => {
	expect(
		await lint("no-direct-process-env", {
			"src/cli/env": "export const v = process.env.DATAIKU_URL;",
			"src/cli/other": "export const v = process.env.DATAIKU_URL;",
			"src/cli/disabled":
				"// oxlint-disable-next-line dss/no-direct-process-env -- reason\nexport const v = process.env.X;",
		},),
	).toEqual(["other.ts",],);
});

function lint(rule: string, cases: Record<string, string>,): Promise<string[]> {
	return lintWith({ [`dss/${rule}`]: "error", }, cases,);
}

it("renders usage from the syntax table instead of hand-written lines", async () => {
	expect(
		await lint("no-usage-literals", {
			"src/message": "export const m = 'Usage: dss foo bar';",
			"src/template": "export const m = (x: string) => `Usage: dss ${x}`;",
			"src/argument":
				"export const check = (requireArgs: (...a: unknown[]) => void) => requireArgs([], 1, 'dss foo bar <id>');",
			"src/binding": "export const FOO_USAGE = 'dss foo bar';",
			"src/plain": "export const hint = 'dss foo bar';",
			"outside": "export const m = 'Usage: dss foo bar';",
		},),
	).toEqual(["argument.ts", "binding.ts", "message.ts", "template.ts",],);
});

it("sends every flag-derived number through the validating coercion helpers", async () => {
	expect(
		await lint("no-raw-number-coercion", {
			"src/cli/direct": "export function h(f: Record<string, string>) { return Number(f['x']); }",
			"src/cli/parse":
				"export function h(flags: Record<string, string>) { return parseInt(flags['x'], 10); }",
			"src/cli/parsefloat":
				"export function h(f: Record<string, string>) { return Number.parseFloat(f.x); }",
			"src/cli/unary": "export function h(f: Record<string, string>) { return +f['x']; }",
			"src/cli/viaconst":
				"export function h(f: Record<string, string>) { const raw = f['x'] as string; return Number(raw); }",
			"src/cli/helper":
				"export function h(f: Record<string, string>, num: (v: string, n: string) => number) { return num(f['x'], '--x'); }",
			"src/cli/plain": "export function h(value: string) { return Number(value); }",
			"src/cli/otherfn":
				"export function h(f: Record<string, string>) { const raw = f['x']; return () => Number(raw); }",
			"src/cli/coerce": "export function h(f: Record<string, string>) { return Number(f['x']); }",
			"src/other/outside": "export function h(f: Record<string, string>) { return Number(f['x']); }",
		},),
	).toEqual(["direct.ts", "parse.ts", "parsefloat.ts", "unary.ts", "viaconst.ts",],);
});

it("keeps buffered response reads on the client's bounded readers", async () => {
	expect(
		await lint("no-unbounded-response-body", {
			"src/resources/text": "export const f = (res: Response) => res.text();",
			"src/resources/json": "export const f = (response: Response) => response.json();",
			"src/resources/bytes": "export const f = () => new Response('x').arrayBuffer();",
			"src/cli/blob": "export const f = (r: Response) => r.blob();",
			"src/resources/file": "export const f = () => Bun.file('p').text();",
			"src/resources/memory": "export const f = () => new Blob(['x']).text();",
			"src/resources/arguments":
				"export const f = (res: { text(x: string): string; }) => res.text('x');",
			"src/resources/client":
				"export const f = (c: { getText(p: string): Promise<string>; }) => c.getText('/p');",
			"src/client": "export const f = (res: Response) => res.text();",
			"src/utils/raw": "export const f = (res: Response) => res.text();",
		},),
	).toEqual(["blob.ts", "bytes.ts", "json.ts", "text.ts",],);
});

it("reads stdin only behind an explicit stdin test or a content-source requirement", async () => {
	const imports = "import { readStdinText, requireContentSource } from '../coerce.js';\n";
	expect(
		await lint("no-implicit-stdin", {
			"src/cli/commands/bare": `${imports}export const f = () => readStdinText();`,
			"src/cli/commands/fallback":
				`${imports}export function h(f: Record<string, unknown>) { return typeof f['file'] === 'string' ? 'file' : readStdinText(); }`,
			"src/cli/commands/late":
				`${imports}export function h(f: Record<string, unknown>) { const t = readStdinText(); requireContentSource(f, 'u'); return t; }`,
			"src/cli/commands/otherfn":
				`${imports}export function a(f: Record<string, unknown>) { requireContentSource(f, 'u'); }\nexport function b() { return readStdinText(); }`,
			"src/cli/commands/reference": `${imports}export const sources = [{ read: readStdinText, }];`,
			"src/cli/helpers/unguarded": `${imports}export const f = () => readStdinText();`,
			"src/cli/commands/flag":
				`${imports}export function h(f: Record<string, unknown>) { if (f['stdin'] === true) { return readStdinText(); } return ''; }`,
			"src/cli/commands/marker":
				`${imports}export function h(v: string) { return v === '-' ? readStdinText() : v; }`,
			"src/cli/commands/required":
				`${imports}export function h(f: Record<string, unknown>) { requireContentSource(f, 'u'); return typeof f['file'] === 'string' ? 'file' : readStdinText(); }`,
			"src/cli/commands/refguard":
				`${imports}export function h(f: Record<string, unknown>, sources: unknown[]) { if (f['stdin'] === true) sources.push({ read: readStdinText, }); }`,
			"src/cli/commands/named": "export function readStdinText() { return ''; }",
			"src/cli/coerce": `${imports}export const f = () => readStdinText();`,
		},),
	).toEqual(["bare.ts", "fallback.ts", "late.ts", "otherfn.ts", "reference.ts", "unguarded.ts",],);
});

it("rejects chmod to an owner-only mode after a write without a mode option", async () => {
	expect(
		await lint("no-chmod-after-write", {
			"src/bad":
				"import { chmodSync, writeFileSync } from 'node:fs'; export function save(p: string, d: string) { writeFileSync(p, d); chmodSync(p, 0o600); }",
			"src/promises":
				"import { chmod, writeFile } from 'node:fs/promises'; export async function save(p: string, d: string) { await writeFile(p, d); await chmod(p, 0o700); }",
			"src/bunwrite":
				"import * as fs from 'node:fs'; export async function save(p: string, d: string) { await Bun.write(p, d); fs.chmodSync(p, 0o400); }",
			"src/fd":
				"import { fchmodSync, writeFileSync } from 'node:fs'; export function save(fd: number, d: string) { writeFileSync('p', d); fchmodSync(fd, 0o600); }",
			"src/moded":
				"import { chmodSync, writeFileSync } from 'node:fs'; export function save(p: string, d: string) { writeFileSync(p, d, { mode: 0o600, }); chmodSync(p, 0o600); }",
			"src/chmodonly":
				"import { chmodSync } from 'node:fs'; export function save(p: string) { chmodSync(p, 0o600); }",
			"src/wide":
				"import { chmodSync, writeFileSync } from 'node:fs'; export function save(p: string, d: string) { writeFileSync(p, d); chmodSync(p, 0o644); }",
			"src/split":
				"import { chmodSync, writeFileSync } from 'node:fs'; export function a(p: string, d: string) { writeFileSync(p, d); }\nexport function b(p: string) { chmodSync(p, 0o600); }",
			"elsewhere/late":
				"import { chmodSync, writeFileSync } from 'node:fs'; export function save(p: string, d: string) { writeFileSync(p, d); chmodSync(p, 0o600); }",
		},),
	).toEqual(["bad.ts", "bunwrite.ts", "fd.ts", "promises.ts",],);
});

it("routes network access through DataikuClient using the repository's restricted-globals override", async () => {
	const config = await Bun.file(resolve(".oxlintrc.json",),).json() as {
		overrides: { files: string[]; rules?: Record<string, unknown>; }[];
	};
	const overrides = config.overrides.filter((override,) =>
		override.rules?.["no-restricted-globals"] !== undefined,
	);
	expect(overrides.length,).toBe(2,);
	expect(
		await lintWith(
			{},
			{
				"src/direct": "export const r = fetch('http://example.test');",
				"src/xhr": "export const r = new XMLHttpRequest();",
				"src/socket": "export const r = new WebSocket('ws://example.test');",
				"src/client": "export const r = fetch('http://example.test');",
				"src/member": "export const r = (c: { fetch(): void; }) => c.fetch();",
				"src/shadow": "export function f(fetch: () => void) { fetch(); }",
				"tests/outside": "export const r = fetch('http://example.test');",
			},
			overrides,
		),
	).toEqual(["direct.ts", "socket.ts", "xhr.ts",],);
});
