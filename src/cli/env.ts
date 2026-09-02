import { readFileSync, } from "node:fs";
import { dirname, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";

const projectEnvVars = new Set<string>();
const installEnvVars = new Set<string>();

export type EnvFileProvenance = "project" | "install";

/**
 * Provenance of an environment variable injected by loadEnvFile:
 * - "project": set from the invocation directory's `.env` (working tree, untrusted)
 * - "install": set from the CLI install/root directory's `.env` (shipped defaults)
 * Native environment variables (present before loadEnvFile runs) are not
 * recorded here and resolve to their default source.
 */
export function envFileProvenance(name: string,): EnvFileProvenance | undefined {
	if (projectEnvVars.has(name,)) return "project";
	if (installEnvVars.has(name,)) return "install";
	return undefined;
}

export function dataikuEnvironmentEnabled(): boolean {
	return process.env.DATAIKU_DISABLE_ENV !== "1";
}

export function loadEnvFile(): void {
	if (!dataikuEnvironmentEnabled()) return;
	// The invocation cwd takes precedence over the CLI install/root directory, so a
	// project-local .env where `dss` is invoked overrides defaults shipped beside the
	// CLI. First writer wins below, so cwd must be listed first.
	const dirs = [
		process.cwd(),
		resolve(dirname(fileURLToPath(import.meta.url,),), "..", "..",),
	];
	for (const [index, dir,] of dirs.entries()) {
		try {
			const content = readFileSync(resolve(dir, ".env",), "utf-8",);
			for (const line of content.split("\n",)) {
				const trimmed = line.trim();
				if (!trimmed || trimmed.startsWith("#",)) continue;
				const eq = trimmed.indexOf("=",);
				if (eq === -1) continue;
				const key = trimmed.slice(0, eq,).trim();
				const val = trimmed.slice(eq + 1,).trim().replace(/^['"]|['"]$/g, "",);
				if (!process.env[key]) {
					process.env[key] = val;
					(index === 0 ? projectEnvVars : installEnvVars).add(key,);
				}
			}
		} catch {
			// no .env file — fine
		}
	}
}
