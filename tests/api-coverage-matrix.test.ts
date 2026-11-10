import { expect, it, } from "bun:test";
import { readFileSync, } from "node:fs";
import { join, } from "node:path";
import { buildCommandRegistry, } from "../src/cli/contract.js";
import * as sdk from "../src/index.js";

/**
 * docs/API-COVERAGE-DSS-15.{md,json} describe the same rows and claim every
 * mapping is machine-checked. This is that check: the JSON counts agree with
 * its rows, every CLI mapping names a registered action, and every SDK
 * mapping names a public method on an exported resource class.
 */
type Row = { status: string; sdk: string[]; cli: string[]; };
const docs = join(import.meta.dir, "..", "docs",);
const matrix = JSON.parse(readFileSync(join(docs, "API-COVERAGE-DSS-15.json",), "utf-8",),) as {
	meta: {
		officialEndpointRows: number;
		implementedOfficialRows: number;
		notImplementedOfficialRows: number;
		cliResources: number;
		cliActions: number;
		cliApiResourceSubset: { resources: number; actions: number; metaCommands: string[]; };
	};
	sections: Record<string, Row[]>;
};
const rows = Object.values(matrix.sections,).flat();

it("keeps the coverage matrix counts, CLI actions, and SDK methods real", () => {
	expect(rows.length,).toBe(matrix.meta.officialEndpointRows,);
	expect(rows.filter((row,) => row.status === "implemented").length,).toBe(
		matrix.meta.implementedOfficialRows,
	);
	expect(rows.length - matrix.meta.implementedOfficialRows,).toBe(
		matrix.meta.notImplementedOfficialRows,
	);

	const registry = buildCommandRegistry();
	const unknownCli = rows.flatMap((row,) => row.cli).filter((command,) => {
		const [, resource, action,] = command.split(" ",);
		return !resource || !action || registry[resource]?.[action] === undefined;
	},);
	expect([...new Set(unknownCli,),],).toEqual([],);

	const exported = sdk as unknown as Record<string, { prototype?: Record<string, unknown>; }>;
	const unknownSdk = rows.flatMap((row,) => row.sdk).filter((method,) => {
		const [className, name,] = method.split(".",);
		return typeof exported[className!]?.prototype?.[name!] !== "function";
	},);
	expect([...new Set(unknownSdk,),],).toEqual([],);

	const markdown = readFileSync(join(docs, "API-COVERAGE-DSS-15.md",), "utf-8",);
	expect(markdown,).toContain(`**${matrix.meta.officialEndpointRows} entries`,);
	expect(markdown,).toContain(`**${matrix.meta.implementedOfficialRows} implemented**`,);

	// The catalogue totals both files quote are the registry's, not a dated snapshot.
	const { metaCommands, } = matrix.meta.cliApiResourceSubset;
	expect(metaCommands.filter((name,) => registry[name] === undefined),).toEqual([],);
	const counted = { cliResources: 0, cliActions: 0, api: { resources: 0, actions: 0, }, };
	for (const [name, actions,] of Object.entries(registry,)) {
		const actionCount = Object.keys(actions,).length;
		counted.cliResources += 1;
		counted.cliActions += actionCount;
		if (!metaCommands.includes(name,)) {
			counted.api.resources += 1;
			counted.api.actions += actionCount;
		}
	}
	expect({
		cliResources: matrix.meta.cliResources,
		cliActions: matrix.meta.cliActions,
		api: {
			resources: matrix.meta.cliApiResourceSubset.resources,
			actions: matrix.meta.cliApiResourceSubset.actions,
		},
	},).toEqual(counted,);
	expect(markdown,).toContain(
		`**${matrix.meta.cliResources} resources / ${matrix.meta.cliActions} actions**`,
	);
	expect(markdown,).toContain(
		`**${matrix.meta.cliApiResourceSubset.resources} resources / ${matrix.meta.cliApiResourceSubset.actions} actions**`,
	);
});
