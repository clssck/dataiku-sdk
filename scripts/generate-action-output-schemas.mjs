#!/usr/bin/env bun
// Canonical discovery output-schema metadata generator.
//
// Serializes the map returned by `typeBoxCommandOutputSchemas()`
// (src/cli/output-schemas.ts) into a committed JSON data file that discovery
// reads as plain data: `src/generated/action-output-schemas.json`. Discovery
// (`dss commands run`, `dss agent contract`) therefore never loads the TypeBox
// schema graph, while SDK validation keeps using the TypeBox schema objects
// from packages/types directly.
//
// Modes:
//   (default)  regenerate the committed file from the canonical builder
//   --check    fail when the committed file is stale (`bun run verify`, publish)
//
// Determinism: the output depends only on the schemas in that module — no
// timestamps, no ambient state, action ids sorted, per-schema key order
// preserved from the schema objects themselves. Two runs over the same
// checkout produce byte-identical files.
//
// Formatting contract: the emitted JSON is dprint-clean under this repo's
// config (tab indent, same plugin set), so `dprint format:check` leaves the
// committed file untouched and the two gates cannot fight: if a formatter ever
// rewrote it, `--check` and the discovery tests fail loudly instead of the
// build shipping silently stale bytes. `bun run build` regenerates before
// compiling and `prepack`/`prepublishOnly` run `--check`, so a stale committed
// file can neither be compiled nor published.
import { join, resolve, } from "node:path";

const root = resolve(import.meta.dir, "..",);
const outputPath = join(root, "src", "generated", "action-output-schemas.json",);

const { typeBoxCommandOutputSchemas, } = await import(
	new URL("../src/cli/output-schemas.ts", import.meta.url,).href
);

const schemas = typeBoxCommandOutputSchemas();
const metadata = {
	format: 1,
	schemas: Object.fromEntries(
		Object.entries(schemas,).sort(([left,], [right,],) => (left < right ? -1 : left > right ? 1 : 0)),
	),
};
const serialized = `${JSON.stringify(metadata, null, "\t",)}\n`;

if (process.argv.includes("--check",)) {
	const existing = Bun.file(outputPath,);
	if (!(await existing.exists()) || await existing.text() !== serialized) {
		process.stderr.write(
			"src/generated/action-output-schemas.json is stale. Run `bun scripts/generate-action-output-schemas.mjs` and commit the result.\n",
		);
		process.exitCode = 1;
	} else {
		process.stdout.write("action-output-schemas.json is up to date\n",);
	}
} else {
	await Bun.write(outputPath, serialized,);
	process.stdout.write(
		`Wrote ${outputPath} (${Object.keys(metadata.schemas,).length} actions)\n`,
	);
}
