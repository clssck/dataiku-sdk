import { createHash, } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
const packageName = "dataiku-sdk";

function run(argv, cwd = root,) {
	const result = Bun.spawnSync(argv, { cwd, stdout: "pipe", stderr: "inherit", },);
	if (result.exitCode !== 0) {
		throw new Error(`Command failed (${result.exitCode}): ${argv.join(" ",)}`,);
	}
	return result.stdout.toString().trim();
}
function integrity(bytes,) {
	return "sha512-" + createHash("sha512",).update(bytes,).digest("base64",);
}

export function readCandidate(directory,) {
	const manifestPath = path.join(directory, "manifest.json",);
	if (fs.statSync(manifestPath,).size > 4_096) throw new Error("Oversized release manifest",);
	const value = JSON.parse(fs.readFileSync(manifestPath, "utf8",),);
	if (
		!value || value.name !== packageName || !/^\d+\.\d+\.\d+$/.test(value.version,)
		|| ![value.version, value.commit, value.baseRevision, value.integrity,].every((field,) =>
			typeof field === "string"
		)
		|| !/^[a-f0-9]{40}$/.test(value.commit,) || !/^[a-f0-9]{40}$/.test(value.baseRevision,)
		|| !/^sha512-[A-Za-z0-9+/]{86}==$/.test(value.integrity,)
		|| !Number.isSafeInteger(value.bytes,) || value.bytes < 1
	) {
		throw new Error("Invalid release candidate manifest",);
	}
	return value;
}

export function verifyCandidate(directory,) {
	const manifest = readCandidate(directory,);
	const tarball = path.join(directory, "package.tgz",);
	if (
		fs.statSync(tarball,).size !== manifest.bytes
		|| integrity(fs.readFileSync(tarball,),) !== manifest.integrity
	) {
		throw new Error("Release candidate tarball integrity mismatch",);
	}
	return manifest;
}

function prepareCandidate(bump, directory,) {
	if (!["patch", "minor", "major",].includes(bump,)) {
		throw new Error("Expected patch, minor, or major",);
	}
	if (run(["git", "status", "--porcelain",],)) {
		throw new Error("Release preparation requires a clean checkout",);
	}
	fs.mkdirSync(directory, { recursive: true, },);
	if (fs.readdirSync(directory,).length) throw new Error("Candidate directory must be empty",);
	const baseRevision = run(["git", "rev-parse", "HEAD",],);
	run(["npm", "version", bump, "-m", "v%s",],);
	const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json",), "utf8",),);
	if (pkg.name !== packageName) throw new Error("Unexpected package name",);
	const commit = run(["git", "rev-parse", "HEAD",],);
	run([process.execPath, "run", "build",],);
	// Build once, then pack without lifecycle scripts rebuilding the candidate.
	run(["npm", "pack", "--ignore-scripts", "--pack-destination", directory,],);
	const packed = path.join(directory, `${packageName}-${pkg.version}.tgz`,);
	const tarball = path.join(directory, "package.tgz",);
	fs.renameSync(packed, tarball,);
	const bytes = fs.readFileSync(tarball,);
	const manifest = {
		name: packageName,
		version: pkg.version,
		baseRevision,
		commit,
		bytes: bytes.length,
		integrity: integrity(bytes,),
	};
	fs.writeFileSync(path.join(directory, "manifest.json",), JSON.stringify(manifest,) + "\n",);
	run([
		"git",
		"bundle",
		"create",
		path.join(directory, "source.bundle",),
		"HEAD",
		`refs/tags/v${pkg.version}`,
		`^${baseRevision}`,
	],);
	verifyCandidate(directory,);
	console.log(JSON.stringify(manifest,),);
}

function restoreCandidate(directory,) {
	const manifest = verifyCandidate(directory,);
	if (run(["git", "rev-parse", "HEAD",],) !== manifest.baseRevision) {
		throw new Error("Candidate base differs from the checked-out revision",);
	}
	run([
		"git",
		"fetch",
		path.join(directory, "source.bundle",),
		"HEAD",
		`refs/tags/v${manifest.version}:refs/tags/v${manifest.version}`,
	],);
	run(["git", "checkout", "--detach", "FETCH_HEAD",],);
	if (
		run(["git", "rev-parse", "HEAD",],) !== manifest.commit
		|| run(["git", "rev-parse", `v${manifest.version}^{commit}`,],) !== manifest.commit
		|| run(["git", "rev-parse", "HEAD^",],) !== manifest.baseRevision
	) {
		throw new Error("Candidate source/tag does not match its manifest",);
	}
	console.log(JSON.stringify(manifest,),);
}

class PublicationMismatch extends Error {}

async function registryJson(response,) {
	if (!response.body) throw new PublicationMismatch("Registry metadata body is missing",);
	const chunks = [];
	let size = 0;
	for await (const chunk of response.body) {
		size += chunk.length;
		if (size > 1_048_576) throw new PublicationMismatch("Registry metadata exceeds 1 MiB",);
		chunks.push(chunk,);
	}
	try {
		return JSON.parse(Buffer.concat(chunks, size,).toString("utf8",),);
	} catch {
		throw new PublicationMismatch("Invalid registry metadata JSON",);
	}
}

/** Read-only, bounded verification. Never publishes or retries publication. */
export async function verifyPublishedCandidate(directory, {
	registry = "https://registry.npmjs.org/",
	timeoutMs = 600_000,
	pollMs = 5_000,
} = {},) {
	if (!Number.isFinite(timeoutMs,) || timeoutMs <= 0 || !Number.isFinite(pollMs,) || pollMs < 0) {
		throw new Error("Invalid publication verification timing",);
	}
	const manifest = verifyCandidate(directory,);
	const registryUrl = new URL(registry,);
	if (
		!["http:", "https:",].includes(registryUrl.protocol,) || registryUrl.username
		|| registryUrl.password
	) {
		throw new Error("Registry must be an HTTP(S) URL without credentials",);
	}
	const metadataUrl = new URL(`${packageName}/${manifest.version}`, registryUrl,);
	const tarballUrl = new URL(
		`${packageName}/-/${packageName}-${manifest.version}.tgz`,
		registryUrl,
	);
	const deadline = Date.now() + timeoutMs;
	let lastFailure = "not yet available";
	while (Date.now() < deadline) {
		try {
			const options = {
				signal: AbortSignal.timeout(Math.max(1, Math.min(30_000, deadline - Date.now(),),),),
				redirect: "error",
				headers: { "cache-control": "no-cache", },
			};
			const response = await fetch(metadataUrl, options,);
			if (!response.ok) {
				await response.body?.cancel();
				throw new Error(`Metadata HTTP ${response.status}`,);
			}
			const metadata = await registryJson(response,);
			if (
				metadata === null || metadata.name !== manifest.name || metadata.version !== manifest.version
				|| metadata.dist?.integrity !== manifest.integrity || metadata.dist?.tarball !== tarballUrl.href
				|| (metadata.gitHead !== undefined && metadata.gitHead !== manifest.commit)
			) {
				throw new PublicationMismatch(
					"Published version has different content or source identity; do not republish",
				);
			}
			const tarball = await fetch(tarballUrl, options,);
			if (!tarball.ok) {
				await tarball.body?.cancel();
				throw new Error(`Tarball HTTP ${tarball.status}`,);
			}
			if (!tarball.body) throw new PublicationMismatch("Published tarball body is missing",);
			const hash = createHash("sha512",);
			let size = 0;
			for await (const chunk of tarball.body) {
				size += chunk.length;
				if (size > manifest.bytes) {
					throw new PublicationMismatch("Published tarball exceeds candidate size",);
				}
				hash.update(chunk,);
			}
			if (size !== manifest.bytes || "sha512-" + hash.digest("base64",) !== manifest.integrity) {
				throw new PublicationMismatch("Published tarball integrity mismatch",);
			}
			return manifest;
		} catch (error) {
			if (error instanceof PublicationMismatch) throw error;
			lastFailure = error instanceof Error ? error.message : String(error,);
		}
		await Bun.sleep(Math.max(0, Math.min(pollMs, deadline - Date.now(),),),);
	}
	throw new Error(
		`Publication not confirmed: ${lastFailure}. Preserve the candidate and investigate npm; do not blindly republish.`,
	);
}

if (import.meta.main) {
	const [command, first, second,] = process.argv.slice(2,);
	if (command === "prepare" && first && second) prepareCandidate(first, path.resolve(second,),);
	else if (command === "restore" && first) restoreCandidate(path.resolve(first,),);
	else if (command === "verify" && first) {
		console.log(JSON.stringify(await verifyPublishedCandidate(path.resolve(first,),),),);
	} else {throw new Error(
			"Usage: release-candidate.mjs prepare BUMP DIRECTORY | restore DIRECTORY | verify DIRECTORY",
		);}
}
