// Runs from the npm `version` lifecycle (after package.json is bumped): moves the
// `## Unreleased` entries under a `## <version>` heading so every published
// version has its own section. `--check` fails when the current version has no
// section (or when released entries are still sitting under Unreleased).
import * as fs from "node:fs";

const changelogUrl = new URL("../CHANGELOG.md", import.meta.url,);
const { version, } = JSON.parse(
	fs.readFileSync(new URL("../package.json", import.meta.url,), "utf-8",),
);
const text = fs.readFileSync(changelogUrl, "utf-8",);
const heading = `## ${version}`;
const unreleased = /^## Unreleased\n\n([\s\S]*?)(?=^## |(?![\s\S]))/m;

const match = text.match(unreleased,);
if (!match) throw new Error("CHANGELOG.md must contain a `## Unreleased` section.",);
const pending = match[1].trim();

if (process.argv.includes("--check",)) {
	if (!text.split("\n",).includes(heading,)) {
		console.error(`CHANGELOG.md has no ${heading} section.`,);
		process.exitCode = 1;
	}
} else {
	if (text.split("\n",).includes(heading,)) throw new Error(`CHANGELOG.md already has ${heading}.`,);
	if (pending === "") throw new Error(`No Unreleased entries to release as ${version}.`,);
	const cut = text.replace(unreleased, `## Unreleased\n\n${heading}\n\n${pending}\n\n`,);
	fs.writeFileSync(changelogUrl, cut, "utf-8",);
}
