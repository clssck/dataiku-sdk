import { readdirSync, readFileSync, statSync, } from "node:fs";
import { dirname, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";

export function findPackageRoot(): string | undefined {
	let dir = dirname(fileURLToPath(import.meta.url,),);
	for (let i = 0; i < 5; i++) {
		try {
			readFileSync(resolve(dir, "package.json",), "utf-8",);
			return dir;
		} catch {
			dir = dirname(dir,);
		}
	}
	return undefined;
}

export function packageVersion(packageRoot: string | undefined,): string {
	if (!packageRoot) return "unknown";
	try {
		return (JSON.parse(readFileSync(resolve(packageRoot, "package.json",), "utf-8",),) as {
			version: string;
		}).version;
	} catch {
		return "unknown";
	}
}

export function gitDirectory(packageRoot: string,): string {
	try {
		const gitFile = readFileSync(resolve(packageRoot, ".git",), "utf-8",).trim();
		if (gitFile.startsWith("gitdir:",)) {
			return resolve(packageRoot, gitFile.slice("gitdir:".length,).trim(),);
		}
	} catch {
		// Normal checkouts have a .git directory, not a .git file.
	}
	return resolve(packageRoot, ".git",);
}

/**
 * A full lowercase hexadecimal revision is the only provenance claim trusted.
 * Git is never invoked from runtime version reporting; revisions are resolved
 * directly from the filesystem layouts Git documents.
 */
const FULL_REVISION_PATTERN = /^[0-9a-f]{40}$/;

export function isFullRevision(value: unknown,): boolean {
	return typeof value === "string" && FULL_REVISION_PATTERN.test(value,);
}

function readGitFile(base: string, relative: string,): string | undefined {
	try {
		return readFileSync(resolve(base, relative,), "utf-8",).trim();
	} catch {
		return undefined;
	}
}

function isSafeRefName(ref: string,): boolean {
	return (
		ref.length > 0
		&& !ref.startsWith("/",)
		&& !ref.endsWith("/",)
		&& !ref.includes("\\",)
		&& !ref.split("/",).includes("..",)
	);
}

/** Resolve an entry from packed-refs; peeled `^` lines never match a ref. */
function packedRefFile(base: string, ref: string,): string | undefined {
	let contents: string;
	try {
		contents = readFileSync(resolve(base, "packed-refs",), "utf-8",);
	} catch {
		return undefined;
	}
	for (const line of contents.split("\n",)) {
		const trimmed = line.trim();
		if (trimmed.length === 0 || trimmed.startsWith("#",) || trimmed.startsWith("^",)) continue;
		const splitAt = trimmed.indexOf(" ",);
		if (splitAt <= 0) continue;
		const hash = trimmed.slice(0, splitAt,);
		if (isFullRevision(hash,) && trimmed.slice(splitAt + 1,).trim() === ref) return hash;
	}
	return undefined;
}

/**
 * The shared git dir behind a linked worktree or submodule checkout: the
 * `commondir` file inside the worktree's git dir, resolved relative to that
 * dir. Normal checkouts (no `commondir` file) share their own git dir.
 */
function gitCommonDirectory(gitDir: string,): string | undefined {
	try {
		const commondir = readFileSync(resolve(gitDir, "commondir",), "utf-8",).trim();
		return commondir.length > 0 ? resolve(gitDir, commondir,) : undefined;
	} catch {
		return undefined;
	}
}

/**
 * Full checkout HEAD revision (40-lowercase-hex) — the basis for staleness
 * comparison. Resolution follows Git's own layouts: a detached revision, then
 * loose refs in the worktree git dir, then the shared store reached through
 * `commondir`, then packed-refs in each. Anything that is not a full
 * lowercase hexadecimal revision is rejected.
 */
export function gitFullRevision(packageRoot: string | undefined,): string | undefined {
	if (!packageRoot) return undefined;
	const gitDir = gitDirectory(packageRoot,);
	const head = readGitFile(gitDir, "HEAD",);
	if (!head) return undefined;
	if (isFullRevision(head,)) return head;
	if (!head.startsWith("ref:",)) return undefined;
	const ref = head.slice("ref:".length,).trim();
	if (!isSafeRefName(ref,)) return undefined;
	for (const base of [gitDir, gitCommonDirectory(gitDir,),]) {
		if (!base) continue;
		const loose = readGitFile(base, ref,);
		if (loose && isFullRevision(loose,)) return loose;
		const packed = packedRefFile(base, ref,);
		if (packed) return packed;
	}
	return undefined;
}

export function gitRevision(packageRoot: string | undefined,): string | undefined {
	const full = gitFullRevision(packageRoot,);
	return full ? full.slice(0, 7,) : undefined;
}

export type CliLoadSource = "source" | "dist";
export type CliRuntime = "bun" | "node";

export function detectLoadSource(modulePath = fileURLToPath(import.meta.url,),): CliLoadSource {
	return modulePath.replaceAll("\\", "/",).includes("/dist/",) ? "dist" : "source";
}

export function buildMetadataRevision(packageRoot: string | undefined,): string | undefined {
	if (!packageRoot) return undefined;
	try {
		const metadata = JSON.parse(
			readFileSync(resolve(packageRoot, "dist", "build-metadata.json",), "utf-8",),
		) as { buildRevision?: unknown; };
		return typeof metadata.buildRevision === "string"
				&& isFullRevision(metadata.buildRevision,)
			? metadata.buildRevision
			: undefined;
	} catch {
		return undefined;
	}
}

/**
 * Whether compiled CLI output predates any source input. The build metadata
 * file is written after `tsc`, so its mtime is the build-completion marker.
 * Installed packages omit `src/`; without both sides there is no drift claim.
 */
export function sourceTreeNewerThanBuild(packageRoot: string | undefined,): boolean {
	if (!packageRoot) return false;
	try {
		const buildMtimeMs = statSync(
			resolve(packageRoot, "dist", "build-metadata.json",),
		).mtimeMs;
		const pending = [resolve(packageRoot, "src",),];
		while (pending.length > 0) {
			const path = pending.pop();
			if (!path) break;
			const stat = statSync(path,);
			if (stat.mtimeMs > buildMtimeMs) return true;
			if (!stat.isDirectory()) continue;
			for (const entry of readdirSync(path, { withFileTypes: true, },)) {
				if (entry.isDirectory() || entry.isFile()) {
					pending.push(resolve(path, entry.name,),);
				}
			}
		}
		return false;
	} catch {
		return false;
	}
}

/**
 * CLI provenance payload. `source` distinguishes a source checkout run from a
 * published dist run; `runtime` names the executing runtime. `buildRevision`
 * is only ever claimed by a dist build that carries build metadata — source
 * execution never claims one. `staleBuild` is true only when the running dist
 * build revision differs from the checkout or a source input is newer than the
 * build-completion marker.
 */
export interface CliVersionPayload {
	version: string;
	gitRevision: string | null;
	source: CliLoadSource;
	runtime: CliRuntime;
	buildRevision: string | null;
	staleBuild: boolean;
}

export function detectRuntime(): CliRuntime {
	return process.versions.bun ? "bun" : "node";
}

export function buildVersionPayload(input: {
	packageVersion: string;
	checkoutRevision: string | undefined;
	checkoutFullRevision: string | undefined;
	loadSource: string | undefined;
	buildRevision: string | undefined;
	sourceNewerThanBuild?: boolean;
	runtime: CliRuntime;
},): CliVersionPayload {
	const isDist = input.loadSource === "dist";
	// Source execution never claims a build revision, even if the environment
	// happens to carry one.
	const buildRevision = isDist && input.buildRevision ? input.buildRevision : null;
	const staleBuild = isDist
		&& (
			(buildRevision !== null
				&& input.checkoutFullRevision !== undefined
				&& input.checkoutFullRevision !== buildRevision)
			|| input.sourceNewerThanBuild === true
		);
	return {
		version: input.packageVersion,
		gitRevision: input.checkoutRevision ?? null,
		source: isDist ? "dist" : "source",
		runtime: input.runtime,
		buildRevision,
		staleBuild,
	};
}

export const PACKAGE_ROOT = findPackageRoot();
export const CLI_VERSION = packageVersion(PACKAGE_ROOT,);
export const CLI_GIT_REVISION = gitRevision(PACKAGE_ROOT,);
export const AGENT_CONTRACT_VERSION = 2;
export const JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema";
export const AGENT_CONTRACT_SCHEMA_ID =
	"https://clssck.github.io/dataiku-sdk/schemas/agent-contract-v2.json";

export function cliVersionResult(): CliVersionPayload {
	const loadSource = process.env["DSS_LOAD_SOURCE"] ?? detectLoadSource();
	const envBuildRevision = process.env["DSS_BUILD_REVISION"];
	return buildVersionPayload({
		packageVersion: CLI_VERSION,
		checkoutRevision: CLI_GIT_REVISION,
		checkoutFullRevision: gitFullRevision(PACKAGE_ROOT,),
		loadSource,
		buildRevision: isFullRevision(envBuildRevision,)
			? envBuildRevision
			: (loadSource === "dist" ? buildMetadataRevision(PACKAGE_ROOT,) : undefined),
		sourceNewerThanBuild: loadSource === "dist"
			? sourceTreeNewerThanBuild(PACKAGE_ROOT,)
			: false,
		runtime: detectRuntime(),
	},);
}
