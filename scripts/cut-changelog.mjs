// Runs from the npm `version` lifecycle (after package.json is bumped): moves the
// `## Unreleased` entries under a `## <version>` heading so every published
// version has its own section. `--check` fails when the current version has no
// section (or when released entries are still sitting under Unreleased).
const changelogUrl = new URL("../CHANGELOG.md", import.meta.url,);
const { version, } = await Bun.file(new URL("../package.json", import.meta.url,),).json();
const text = await Bun.file(changelogUrl,).text();
const heading = `## ${version}`;
const unreleased = /^## Unreleased\n\n([\s\S]*?)(?=^## |(?![\s\S]))/m;

const match = text.match(unreleased,);
if (!match) throw new Error("CHANGELOG.md must contain a `## Unreleased` section.",);
const pending = match[1].trim();

if (process.argv.includes("--check",)) {
	if (!text.split("\n",).includes(heading,)) {
		process.stderr.write(`CHANGELOG.md has no ${heading} section.\n`,);
		process.exitCode = 1;
	}
} else {
	if (text.split("\n",).includes(heading,)) throw new Error(`CHANGELOG.md already has ${heading}.`,);
	if (pending === "") throw new Error(`No Unreleased entries to release as ${version}.`,);
	const cut = text.replace(unreleased, `## Unreleased\n\n${heading}\n\n${pending}\n\n`,);
	await Bun.write(Bun.file(changelogUrl,), cut,);
}
