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
});
