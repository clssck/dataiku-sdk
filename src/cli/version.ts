import { readFileSync, } from "node:fs";
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

export function gitRevision(packageRoot: string | undefined,): string | undefined {
	if (!packageRoot) return undefined;
	try {
		const gitDir = gitDirectory(packageRoot,);
		const head = readFileSync(resolve(gitDir, "HEAD",), "utf-8",).trim();
		if (!head.startsWith("ref:",)) return head.slice(0, 7,);
		const ref = head.slice("ref:".length,).trim();
		const full = readFileSync(resolve(gitDir, ref,), "utf-8",).trim();
		return full.slice(0, 7,);
	} catch {
		return undefined;
	}
}

export const PACKAGE_ROOT = findPackageRoot();
export const CLI_VERSION = packageVersion(PACKAGE_ROOT,);
export const CLI_GIT_REVISION = gitRevision(PACKAGE_ROOT,);
export const AGENT_CONTRACT_VERSION = 1;
export const JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema";
export const AGENT_CONTRACT_SCHEMA_ID =
	"https://clssck.github.io/dataiku-sdk/schemas/agent-contract-v1.json";
export function cliVersionResult(): { version: string; gitRevision: string | null; } {
	return { version: CLI_VERSION, gitRevision: CLI_GIT_REVISION ?? null, };
}
