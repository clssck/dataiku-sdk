#!/usr/bin/env bun
// Deterministic build metadata for the published dist/ tree.
//
// Writes `build-metadata.json` into the target directory (default: `<repo>/dist`).
// The output depends only on the repo checkout's HEAD revision: two runs over the
// same checkout produce byte-identical files (no timestamps or ambient state).
// The packaged dist uses this to report its build revision; the source CLI reads
// no build metadata and never claims a build revision.
import { join, resolve, } from "node:path";

const root = resolve(import.meta.dir, "..",);
const targetDir = process.argv[2] ? resolve(root, process.argv[2],) : join(root, "dist",);

async function revisionFromDotGit() {
	try {
		let gitDir = join(root, ".git",);
		const dotGit = join(root, ".git",);
		if ((await Bun.file(dotGit,).stat()).isFile()) {
			const content = (await Bun.file(dotGit,).text()).trim();
			if (content.startsWith("gitdir:",)) {
				gitDir = resolve(root, content.slice("gitdir:".length,).trim(),);
			}
		}
		const head = (await Bun.file(join(gitDir, "HEAD",),).text()).trim();
		if (!head.startsWith("ref:",)) return head;
		return (await Bun.file(join(gitDir, head.slice("ref:".length,).trim(),),).text()).trim();
	} catch {
		return undefined;
	}
}

const git = Bun.spawnSync(["git", "rev-parse", "HEAD",], { cwd: root, },);
const candidateRevision = (git.exitCode === 0 ? git.stdout.toString().trim() : "").trim()
	|| await revisionFromDotGit();
const buildRevision = candidateRevision && /^[0-9a-f]{40,}$/.test(candidateRevision,)
	? candidateRevision
	: undefined;

const outputPath = join(targetDir, "build-metadata.json",);
const metadata = {
	format: 1,
	...(buildRevision ? { buildRevision, } : {}),
};
await Bun.write(outputPath, `${JSON.stringify(metadata,)}\n`,);
process.stdout.write(
	buildRevision
		? `Wrote ${outputPath} (buildRevision=${buildRevision})\n`
		: `Wrote ${outputPath} without a build revision (source snapshot has no Git metadata)\n`,
);
