#!/usr/bin/env node
// Deterministic build metadata for the published dist/ tree.
//
// Writes `build-metadata.json` into the target directory (default: `<repo>/dist`).
// The output depends only on the repo checkout's HEAD revision: two runs over the
// same checkout produce byte-identical files (no timestamps or ambient state).
// The packaged dist uses this to report its build revision; the source CLI reads
// no build metadata and never claims a build revision.
import { spawnSync, } from "node:child_process";
import { mkdirSync, readFileSync, statSync, writeFileSync, } from "node:fs";
import { dirname, join, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";

const here = dirname(fileURLToPath(import.meta.url,),);
const root = resolve(here, "..",);
const targetDir = process.argv[2] ? resolve(root, process.argv[2],) : join(root, "dist",);

function revisionFromDotGit() {
	try {
		let gitDir = join(root, ".git",);
		const dotGit = join(root, ".git",);
		if (statSync(dotGit,).isFile()) {
			const content = readFileSync(dotGit, "utf-8",).trim();
			if (content.startsWith("gitdir:",)) {
				gitDir = resolve(root, content.slice("gitdir:".length,).trim(),);
			}
		}
		const head = readFileSync(join(gitDir, "HEAD",), "utf-8",).trim();
		if (!head.startsWith("ref:",)) return head;
		return readFileSync(join(gitDir, head.slice("ref:".length,).trim(),), "utf-8",).trim();
	} catch {
		return undefined;
	}
}

const git = spawnSync("git", ["rev-parse", "HEAD",], { cwd: root, encoding: "utf-8", },);
const candidateRevision = (git.status === 0 ? git.stdout.trim() : "").trim()
	|| revisionFromDotGit();
const buildRevision = candidateRevision && /^[0-9a-f]{40,}$/.test(candidateRevision,)
	? candidateRevision
	: undefined;

mkdirSync(targetDir, { recursive: true, },);
const outputPath = join(targetDir, "build-metadata.json",);
const metadata = {
	format: 1,
	...(buildRevision ? { buildRevision, } : {}),
};
writeFileSync(outputPath, `${JSON.stringify(metadata,)}\n`,);
process.stdout.write(
	buildRevision
		? `Wrote ${outputPath} (buildRevision=${buildRevision})\n`
		: `Wrote ${outputPath} without a build revision (source snapshot has no Git metadata)\n`,
);
