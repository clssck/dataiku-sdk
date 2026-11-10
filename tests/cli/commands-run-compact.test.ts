import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, compactCommandEntry, } from "../../src/cli/contract.js";
import { dss, dssFailure, join, mkdirSync, readFileSync, rmSync, tmpdir, } from "./_harness.js";

type Entry = Record<string, unknown>;

/** Keys a compact entry never carries: they restate `usage` or are the same for every command. */
const OMITTED_KEYS = [
	"resource",
	"action",
	"agentContractVersion",
	"exitCodes",
	"flags",
	"optionalFlags",
	"requiredFlags",
	"requiredOneOf",
	"requiredInputGroups",
	"positionalArguments",
	"structuredExamples",
	"payloadSchema",
	"outputShape",
	"requiresAuth",
	"requiresProject",
	"mutatesDss",
];

const registry = buildCommandRegistry();

function fullEntry(id: string,): Entry {
	const [resource, action,] = id.split(".",) as [string, string,];
	return JSON.parse(JSON.stringify(registry[resource][action],),) as Entry;
}

async function scoped(args: string[],): Promise<Record<string, Entry>> {
	const { stdout, stderr, } = await dss(["commands", "run", ...args,],);
	expect(stderr,).toBe("",);
	return JSON.parse(stdout,) as Record<string, Entry>;
}

describe("commands run --fields RESOURCE.ACTION compact entries", () => {
	const REPRESENTATIVE = [
		"recipe.set-payload",
		"recipe.create",
		"dataset.get",
		"scenario.update",
		"project.delete",
		"messaging-channel.create",
		"recipe.get-payload",
	];

	it("keeps usage, description, examples and sideEffect and drops the machine restatements", async () => {
		const result = await scoped(["--fields", REPRESENTATIVE.join(",",),],);
		expect(Object.keys(result,),).toEqual(REPRESENTATIVE,);
		for (const id of REPRESENTATIVE) {
			const entry = result[id];
			const full = fullEntry(id,);
			for (const key of ["usage", "description", "examples", "sideEffect",]) {
				expect(entry[key], `${id}.${key}`,).toEqual(full[key],);
			}
			for (const key of OMITTED_KEYS) expect(entry, `${id} omits ${key}`,).not.toHaveProperty(key,);
			const schemas = entry.schemas as Record<string, unknown> | undefined;
			if (schemas) expect(Object.keys(schemas,), `${id}.schemas`,).toEqual(["input",],);
			for (const [key, value,] of Object.entries(entry,)) {
				const expected = key === "schemas" ? { input: (full.schemas as Entry).input, } : full[key];
				expect(value, `${id}.${key} equals the full entry`,).toEqual(expected,);
			}
		}
	});

	it("keeps facts that differ from the default for the command kind", async () => {
		const result = await scoped(["--fields", REPRESENTATIVE.join(",",),],);
		// Write defaults: reversible, dry-run supported, idempotency "none", synchronous.
		expect(result["recipe.set-payload"],).toMatchObject({
			sideEffect: "write",
			idempotency: "convergent",
		},);
		expect(result["recipe.set-payload"],).not.toHaveProperty("destructive",);
		expect(result["recipe.set-payload"],).not.toHaveProperty("dryRun",);
		expect(result["project.delete"],).toMatchObject({
			sideEffect: "write",
			destructive: "destructive",
			idempotency: "if-exists",
		},);
		// Read defaults: not destructive, no dry-run, safe, synchronous, no local file.
		expect(result["dataset.get"],).toEqual({
			usage: expect.any(String,),
			description: expect.any(String,),
			examples: expect.any(Array,),
			sideEffect: "read",
		},);
		expect(result["recipe.get-payload"],).toMatchObject({
			sideEffect: "read",
			producesLocalFile: true,
			unsafeOutputs: fullEntry("recipe.get-payload",).unsafeOutputs,
		},);
		expect(result["messaging-channel.create"],).toMatchObject({
			cleanupCommand: "dss messaging-channel delete <channelId> --if-exists",
			cleanupHint: expect.stringContaining("dss messaging-channel delete",),
		},);
	});

	it("keeps the payload contract of commands that take a JSON body", async () => {
		const result = await scoped(["--fields", "scenario.update,recipe.update,dataset.get",],);
		for (const id of ["scenario.update", "recipe.update",]) {
			const full = fullEntry(id,);
			expect(result[id], id,).toMatchObject({
				inputContract: { stdin: true, dataFlag: true, dataFileFlag: true, },
				schemas: { input: (full.schemas as Entry).input, },
				examplePayload: full.examplePayload,
			},);
		}
		expect(result["dataset.get"],).not.toHaveProperty("inputContract",);
		expect(result["dataset.get"],).not.toHaveProperty("schemas",);
	});

	it("--full returns the registry entry unchanged", async () => {
		const result = await scoped(["--fields", REPRESENTATIVE.join(",",), "--full",],);
		for (const id of REPRESENTATIVE) expect(result[id], id,).toEqual(fullEntry(id,),);
	});

	it("resolves deep selectors against the full entry", async () => {
		const { stdout, stderr, } = await dss([
			"commands",
			"run",
			"--fields",
			"recipe.create.flags,recipe.create.schemas.argv,dataset.get.schemas.output,recipe.create.exitCodes,recipe.create",
		],);
		expect(stderr,).toBe("",);
		const result = JSON.parse(stdout,) as Record<string, unknown>;
		const create = fullEntry("recipe.create",);
		expect(result["recipe.create.flags"],).toEqual(create.flags,);
		expect(result["recipe.create.schemas.argv"],).toEqual((create.schemas as Entry).argv,);
		expect(result["dataset.get.schemas.output"],).toEqual(
			(fullEntry("dataset.get",).schemas as Entry).output,
		);
		expect(result["recipe.create.exitCodes"],).toEqual(create.exitCodes,);
		expect(result["recipe.create"],).toEqual(compactCommandEntry(registry.recipe.create,),);
	});

	it("exports full entries to --output and never expands --fields RESOURCE or the summary", async () => {
		const directory = join(tmpdir(), `dss-compact-output-${crypto.randomUUID()}`,);
		mkdirSync(directory, { recursive: true, },);
		const path = join(directory, "entries.json",);
		try {
			const { stdout, } = await dss([
				"commands",
				"run",
				"--fields",
				"recipe.get-payload,recipe.set-payload",
				"--output",
				path,
			],);
			expect(JSON.parse(stdout,),).toEqual({ path, },);
			const exported = JSON.parse(readFileSync(path, "utf8",),) as Record<string, Entry>;
			expect(exported["recipe.get-payload"],).toEqual(fullEntry("recipe.get-payload",),);
			expect(exported["recipe.set-payload"],).toEqual(fullEntry("recipe.set-payload",),);
		} finally {
			rmSync(directory, { recursive: true, force: true, },);
		}
		const resource = await scoped(["--fields", "dataset",],);
		expect(Object.keys(resource.dataset.get as Entry,).sort(),).toEqual([
			"description",
			"sideEffect",
			"usage",
		],);
		const summary = JSON.parse((await dss(["commands", "run",],)).stdout,) as Record<
			string,
			string[]
		>;
		expect(summary.dataset,).toContain("get",);
	});

	it("accepts --full only with --fields selectors, not --contains", async () => {
		const failure = await dssFailure(["commands", "run", "--contains", "dataset", "--full",],);
		expect(failure.code,).toBe(1,);
		expect(JSON.parse(failure.stdout,),).toMatchObject({
			code: "usage_error",
			error: "--contains cannot be combined with --fields, --output or --full.",
		},);
	});

	it("advertises --full in the commands run usage and description", () => {
		const entry = registry.commands.run;
		expect(entry.usage,).toContain("[--full]",);
		expect(entry.description,).toContain("--full",);
		expect(entry.flags.map((flag,) => flag.name,),).toContain("full",);
	});
});

describe("compactCommandEntry over the whole registry", () => {
	const all = Object.entries(registry,).flatMap(([resource, actions,],) =>
		Object.keys(actions,).map((action,) => `${resource}.${action}`,),
	);

	it("is a strict subset of the full entry with identical values for the kept keys", () => {
		expect(all.length,).toBeGreaterThan(500,);
		for (const id of all) {
			const [resource, action,] = id.split(".",) as [string, string,];
			const compact = JSON.parse(
				JSON.stringify(compactCommandEntry(registry[resource][action],),),
			) as Entry;
			const full = fullEntry(id,);
			expect(Object.keys(compact,).length, `${id} is smaller than the full entry`,).toBeLessThan(
				Object.keys(full,).length,
			);
			for (const key of OMITTED_KEYS) expect(compact, `${id} omits ${key}`,).not.toHaveProperty(key,);
			for (const [key, value,] of Object.entries(compact,)) {
				if (key === "schemas") {
					expect(value, `${id}.schemas`,).toEqual({ input: (full.schemas as Entry).input, },);
				} else {
					expect(value, `${id}.${key}`,).toEqual(full[key],);
				}
			}
			expect(JSON.stringify(compact,).length, `${id} compact size`,).toBeLessThan(
				JSON.stringify(full,).length,
			);
		}
	});

	it("keeps side effects, destructiveness, dry-run gaps, async, local files and cleanup visible", () => {
		for (const id of all) {
			const [resource, action,] = id.split(".",) as [string, string,];
			const entry = registry[resource][action];
			const compact = compactCommandEntry(entry,);
			expect(compact.sideEffect, id,).toBe(entry.sideEffect,);
			if (entry.sideEffect === "write") {
				if (entry.destructive === "destructive") {
					expect(compact.destructive, `${id} destructive`,).toBe("destructive",);
				}
				if (!entry.dryRun) expect(compact.dryRun, `${id} lacks --dry-run`,).toBe(false,);
			} else if (entry.dryRun) {
				expect(compact.dryRun, `${id} read with --dry-run`,).toBe(true,);
			}
			if (entry.async !== "none") expect(compact.async, `${id} async`,).toBe(entry.async,);
			if (entry.producesLocalFile) expect(compact.producesLocalFile, id,).toBe(true,);
			expect(compact.unsafeOutputs, id,).toEqual(entry.unsafeOutputs,);
			expect(compact.cleanupCommand, id,).toBe(entry.cleanupCommand,);
			expect(compact.cleanupHint, id,).toBe(entry.cleanupHint,);
			if (Object.keys(entry.inputContract,).length > 0) {
				expect(compact.inputContract, `${id} payload contract`,).toEqual(entry.inputContract,);
			}
			if (entry.examplePayload !== undefined) {
				expect(compact.examplePayload, id,).toEqual(entry.examplePayload,);
			}
		}
	});
});
