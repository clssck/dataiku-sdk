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
 * Full checkout HEAD revision (40-hex) — the basis for staleness comparison.
 * The short display revision is available via `gitRevision`.
 */
export function gitFullRevision(packageRoot: string | undefined,): string | undefined {
	if (!packageRoot) return undefined;
	try {
		const gitDir = gitDirectory(packageRoot,);
		const head = readFileSync(resolve(gitDir, "HEAD",), "utf-8",).trim();
		if (!head.startsWith("ref:",)) return head;
		const ref = head.slice("ref:".length,).trim();
		return readFileSync(resolve(gitDir, ref,), "utf-8",).trim();
	} catch {
		return undefined;
	}
}

export function gitRevision(packageRoot: string | undefined,): string | undefined {
	return gitFullRevision(packageRoot,)?.slice(0, 7,);
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
				&& /^[0-9a-f]{40,}$/.test(metadata.buildRevision,)
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
	return buildVersionPayload({
		packageVersion: CLI_VERSION,
		checkoutRevision: CLI_GIT_REVISION,
		checkoutFullRevision: gitFullRevision(PACKAGE_ROOT,),
		loadSource,
		buildRevision: process.env["DSS_BUILD_REVISION"]
			?? (loadSource === "dist" ? buildMetadataRevision(PACKAGE_ROOT,) : undefined),
		sourceNewerThanBuild: loadSource === "dist"
			? sourceTreeNewerThanBuild(PACKAGE_ROOT,)
			: false,
		runtime: detectRuntime(),
	},);
}
