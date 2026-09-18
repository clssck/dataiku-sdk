// Contract tests for the generated discovery output-schema metadata
// (src/generated/action-output-schemas.json).
//
// Guarantees under test:
//   1. Discovery serves exactly the canonical output schemas: every mapped
//      action's registry output is byte-identical to the canonical TypeBox map,
//      and no mapped action is missing from the registry.
//   2. The committed metadata is fresh: regeneration from the canonical builder
//      is byte-identical, and `--check` fails on a stale committed file.
import { describe, expect, it, } from "bun:test";
import {
	copyFileSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { typeBoxCommandOutputSchemas, } from "../../src/cli/output-schemas.js";
import { BUN, exec, SDK_ROOT, } from "./_harness.js";

const canonicalSchemas = typeBoxCommandOutputSchemas();
const GENERATOR_SCRIPT = join(SDK_ROOT, "scripts", "generate-action-output-schemas.mjs",);
const GENERATED_PATH = join(SDK_ROOT, "src", "generated", "action-output-schemas.json",);

type RunResult = { code: number | null; stdout: string; stderr: string; };

async function run(file: string, args: string[], cwd?: string,): Promise<RunResult> {
	try {
		const { stdout, stderr, } = await exec(BUN, ["--no-env-file", "run", file, ...args,], {
			cwd: cwd ?? SDK_ROOT,
			env: process.env,
		},);
		return { code: 0, stdout, stderr, };
	} catch (error: unknown) {
		const failure = error as { code?: number | null; stdout?: string; stderr?: string; };
		return {
			code: failure.code ?? null,
			stdout: failure.stdout ?? "",
			stderr: failure.stderr ?? "",
		};
	}
}

describe("generated discovery output schemas", () => {
	it("serves the canonical output schema for every mapped action", () => {
		const registry = buildCommandRegistry();
		for (const key of Object.keys(canonicalSchemas,)) {
			const [resource, action,] = key.split(".",);
			const output = registry[resource]?.[action]?.schemas?.output;
			expect(output, `registry entry for ${key}`,).toBeDefined();
			expect(JSON.stringify(output,), `discovery output for ${key}`,).toBe(
				JSON.stringify(canonicalSchemas[key],),
			);
		}
	});

	it("regenerates the committed metadata byte-identically and fails --check when stale", async () => {
		const tree = mkdtempSync(join(tmpdir(), "dss-schema-generator-",),);
		try {
			mkdirSync(join(tree, "scripts",), { recursive: true, },);
			mkdirSync(join(tree, "src", "cli",), { recursive: true, },);
			mkdirSync(join(tree, "src", "generated",), { recursive: true, },);
			mkdirSync(join(tree, "packages", "types", "src",), { recursive: true, },);
			copyFileSync(GENERATOR_SCRIPT, join(tree, "scripts", "generate-action-output-schemas.mjs",),);
			copyFileSync(
				join(SDK_ROOT, "src", "cli", "output-schemas.ts",),
				join(tree, "src", "cli", "output-schemas.ts",),
			);
			copyFileSync(join(SDK_ROOT, "src", "schemas.ts",), join(tree, "src", "schemas.ts",),);
			copyFileSync(
				join(SDK_ROOT, "packages", "types", "src", "index.ts",),
				join(tree, "packages", "types", "src", "index.ts",),
			);
			symlinkSync(join(SDK_ROOT, "node_modules",), join(tree, "node_modules",), "junction",);

			const generatedPath = join(tree, "src", "generated", "action-output-schemas.json",);
			writeFileSync(generatedPath, `{\n\t"format": 1,\n\t"schemas": {}\n}\n`, "utf-8",);

			const staleCheck = await run(join(tree, "scripts", "generate-action-output-schemas.mjs",), [
				"--check",
			],);
			expect(staleCheck.code,).toBe(1,);
			expect(staleCheck.stderr,).toContain("stale",);

			const regenerated = await run(
				join(tree, "scripts", "generate-action-output-schemas.mjs",),
				[],
			);
			expect(regenerated.code,).toBe(0,);
			expect(readFileSync(generatedPath, "utf-8",),).toBe(readFileSync(GENERATED_PATH, "utf-8",),);

			const freshCheck = await run(join(tree, "scripts", "generate-action-output-schemas.mjs",), [
				"--check",
			],);
			expect(freshCheck.code,).toBe(0,);
		} finally {
			rmSync(tree, { recursive: true, force: true, },);
		}
	});
});
