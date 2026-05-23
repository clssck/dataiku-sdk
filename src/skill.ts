import { execFileSync, } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync, } from "node:fs";
import { homedir, } from "node:os";
import { dirname, join, } from "node:path";

// ---------------------------------------------------------------------------
// Agent definitions
// ---------------------------------------------------------------------------

export interface AgentDef {
	/** Display name */
	name: string;
	/** CLI binary name (for `which` detection) */
	binary: string;
	/** Config directory relative to HOME (for fallback detection) */
	configDir: string;
	/** Require config dir to exist even when binary is found (disambiguates shared binary names) */
	configDirRequired?: boolean;
	/** Skill path relative to HOME (global install) */
	globalPath: (home: string,) => string;
	/** Skill path relative to CWD (project install). null = not supported. */
	projectPath: string | null;
	/** File to write inside the skill directory */
	filename: string;
	/** Content generator: standard SKILL.md or Cursor MDC */
	content: () => string;
}

const SKILL_BODY = `# Dataiku DSS agent CLI

Use \`dss\` when an agent needs to inspect or change Dataiku DSS resources: projects, datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables, code envs, and connections.
If the installed \`dss\` binary is unavailable but the repository checkout is the current workspace, use \`./bin/dss ...\` or \`bun --no-env-file src/cli.ts ...\` with the same arguments; from another working directory, call \`/path/to/dataiku-sdk/bin/dss ...\`.
\`--no-env-file\` disables Bun's automatic preloading only; the CLI still applies its documented \`.env\` handling unless \`DATAIKU_DISABLE_ENV=1\` is set.

## Contract

- Success writes exactly one JSON result to stdout.
- Failure writes exactly one JSON error envelope to stderr with \`ok:false\`, \`error\`, \`code\`, and \`exitCode\`.
- \`--verbose\` may add HTTP trace lines to stderr.
- No prompts, help screens, tables, banners, or prose output are part of the contract.
- Exit codes: 0 success, 1 usage/configuration error, 2 DSS or internal error, 3 transient/retryable DSS error, 4 completed command with failed long-running DSS work.
- \`--raw\` is the only stdout escape hatch: recipe payload commands emit raw bytes, not JSON.

## Discover commands

\`\`\`bash
dss commands run
\`\`\`

The registry is the canonical schema for resources, actions, flags, positional arguments, side effects, auth requirements, output shape, idempotency, dry-run support, payload schemas, cleanup hints, and exit codes. Use it before choosing command syntax.
Credential lookup order is flags first, then \`DATAIKU_*\` environment variables, then saved credentials.
Set \`DATAIKU_DISABLE_ENV=1\` when a test must ignore both \`.env\` files and \`DATAIKU_*\` environment variables.
When \`.env\` loading is enabled, the CLI reads \`.env\` from the CLI build/root directory and from the command's current working directory; put test-specific \`.env\` files in the directory where you invoke \`dss\`.
For disposable agent tests, set \`DSS_CONFIG_DIR\` to a temporary directory so saved credentials never touch the real profile.

## Authentication

Prefer environment variables for ephemeral agent runs:

\`\`\`bash
export DATAIKU_URL=https://dss.example.com
export DATAIKU_API_KEY=your-api-key
export DATAIKU_PROJECT_KEY=MYPROJ
\`\`\`

To persist credentials for later invocations:

\`\`\`bash
dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ
\`\`\`

The command saves credentials and returns \`{"saved":true,"path":"..."}\`. Credentials are saved to \`~/.config/dataiku/credentials.json\` unless \`DSS_CONFIG_DIR\` or platform config env vars redirect the path.
\`auth login\` validates by listing accessible projects before saving credentials, so the API key must be allowed to call DSS project-list APIs.

TLS flags: \`--insecure\` disables certificate verification; \`--ca-cert PATH\` adds a PEM CA bundle. Environment equivalents: \`NODE_TLS_REJECT_UNAUTHORIZED\`, \`NODE_EXTRA_CA_CERTS\`.

## Common workflows

\`\`\`bash
dss version
dss project list
dss doctor --fast
dss dataset list --project-key MYPROJ
dss dataset preview orders --max-rows 10 --project-key MYPROJ
dss recipe get-payload compute_orders --project-key MYPROJ
dss recipe get-payload compute_orders --raw --project-key MYPROJ
dss recipe diff compute_orders --file code.py --project-key MYPROJ
dss recipe set-payload compute_orders --file code.py --project-key MYPROJ
dss job build-and-wait orders --include-logs --project-key MYPROJ
dss scenario run daily_build --project-key MYPROJ
dss sql query --connection analytics --sql "select 1" --project-key MYPROJ
\`\`\`
For fake-DSS smoke tests, return project lists as JSON arrays such as \`[{"projectKey":"MYPROJ","name":"My Project"}]\` from \`/public/api/projects/\`; recipe payload commands read \`/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true\` and expect a JSON object shaped like \`{"recipe":{"name":"<NAME>","type":"python"},"payload":"..."}\`.

## Error envelope

Parse stderr as JSON when exit code is non-zero:

\`\`\`json
{
  "ok": false,
  "error": "Missing API key.",
  "code": "usage_error",
  "category": "usage",
  "message": "Missing API key.",
  "exitCode": 1,
  "resource": "dataset",
  "action": "list"
}
\`\`\`

Use \`code\`, \`category\`, \`exitCode\`, \`retryable\`, \`status\`, and \`details\` for recovery logic. Do not scrape message text when a structured field is available.
`;

const SKILL_FRONTMATTER = `---
name: dataiku-dss
description: >-
  Agent-only JSON CLI for Dataiku DSS. Use to inspect or mutate DSS projects,
  datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables,
  code envs, and connections. Discover the full machine-readable surface with
  dss commands run.
---

`;

function skillContent(): string {
	return SKILL_FRONTMATTER + SKILL_BODY;
}

export const AGENTS: Record<string, AgentDef> = {
	claude: {
		name: "Claude Code",
		binary: "claude",
		configDir: ".claude",
		globalPath: (home,) => join(home, ".claude", "skills", "dataiku-dss",),
		projectPath: ".claude/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	codex: {
		name: "Codex",
		binary: "codex",
		configDir: ".codex",
		globalPath: (home,) => join(home, ".codex", "skills", "dataiku-dss",),
		projectPath: ".codex/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	cursor: {
		name: "Cursor",
		binary: "cursor",
		configDir: ".cursor",
		globalPath: (home,) => join(home, ".cursor", "skills", "dataiku-dss",),
		projectPath: ".cursor/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	pi: {
		name: "Pi",
		binary: "pi",
		configDir: ".pi",
		globalPath: (home,) => join(home, ".pi", "agent", "skills", "dataiku-dss",),
		projectPath: ".pi/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	omp: {
		name: "OhMyPi",
		binary: "omp",
		configDir: join(".omp", "agent",),
		configDirRequired: true,
		globalPath: (home,) => join(home, ".omp", "agent", "skills", "dataiku-dss",),
		projectPath: ".omp/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
};

// ---------------------------------------------------------------------------
// Agent detection
// ---------------------------------------------------------------------------

function binaryExists(name: string,): boolean {
	const cmd = process.platform === "win32" ? "where" : "which";
	try {
		execFileSync(cmd, [name,], { stdio: "pipe", },);
		return true;
	} catch {
		return false;
	}
}

export interface DetectedAgent {
	id: string;
	def: AgentDef;
	via: "binary" | "config-dir" | "flag";
}

export function detectAgents(): DetectedAgent[] {
	const home = homedir();
	const found: DetectedAgent[] = [];
	for (const [id, def,] of Object.entries(AGENTS,)) {
		const hasBinary = binaryExists(def.binary,);
		const hasConfigDir = existsSync(join(home, def.configDir,),);
		if (hasBinary && (!def.configDirRequired || hasConfigDir)) {
			found.push({ id, def, via: "binary", },);
		} else if (hasConfigDir) {
			found.push({ id, def, via: "config-dir", },);
		}
	}
	return found;
}

// ---------------------------------------------------------------------------
// Skill installation
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Workspace root detection
// ---------------------------------------------------------------------------

const WORKSPACE_MARKERS = [".git",];

/**
 * Walk upward from startDir looking for strong project markers.
 * Agent config directories are install targets, not workspace roots.
 */
export function findWorkspaceRoot(startDir: string,): string {
	let dir = startDir;
	for (let i = 0; i < 20; i++) {
		for (const marker of WORKSPACE_MARKERS) {
			if (existsSync(join(dir, marker,),)) return dir;
		}
		const parent = dirname(dir,);
		if (parent === dir) break;
		dir = parent;
	}
	return startDir;
}

export interface InstallResult {
	agent: string;
	path: string;
	via: DetectedAgent["via"];
}

export function planSkillInstalls(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const home = homedir();
	const results: InstallResult[] = [];

	for (const { id, def, via, } of agents) {
		const dir = opts.global
			? def.globalPath(home,)
			: def.projectPath
			? join(opts.cwd, def.projectPath,)
			: undefined;
		if (!dir) continue;
		results.push({ agent: id, path: join(dir, def.filename,), via, },);
	}

	return results;
}

export function installSkill(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const results = planSkillInstalls(agents, opts,);

	for (const result of results) {
		const def = AGENTS[result.agent];
		if (!def) continue;
		mkdirSync(dirname(result.path,), { recursive: true, },);
		writeFileSync(result.path, def.content(), "utf-8",);
	}

	return results;
}
