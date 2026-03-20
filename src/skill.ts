import { execFileSync, } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync, } from "node:fs";
import { homedir, } from "node:os";
import { join, } from "node:path";

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

const SKILL_BODY = `# Dataiku DSS CLI

The \`dss\` CLI (npm: dataiku-sdk) manages Dataiku DSS resources from the terminal.

## When to use

- Query, create, or modify DSS projects, datasets, recipes, jobs, or scenarios.
- Build datasets or run scenarios and wait for completion.
- Download or upload recipe code, dataset data, or managed folder files.
- Run SQL queries against DSS connections.
- Inspect project flows, job logs, or dataset schemas.

## Installation

Requires [Bun](https://bun.sh) runtime.

\`\`\`bash
bun add -g dataiku-sdk              # global install \u2014 provides the \`dss\` command
\`\`\`

Or run without installing:

\`\`\`bash
bunx dataiku-sdk <command>           # e.g. bunx dataiku-sdk auth login
\`\`\`

## Authentication

\`\`\`bash
dss auth login                       # interactive: prompts for URL, API key, project key
dss auth login --url https://dss.example.com --api-key YOUR_KEY
dss auth status                      # verify connection
\`\`\`

Credentials are saved to \`~/.dss/credentials.json\`. Alternatively set environment variables:

\`\`\`bash
export DATAIKU_URL=https://dss.example.com
export DATAIKU_API_KEY=your-api-key
export DATAIKU_PROJECT_KEY=MYPROJ    # optional default project
\`\`\`

## Workflows

### Inspect a project

\`\`\`bash
dss project list                              # find the project key
dss dataset list --project-key MYPROJ         # list its datasets
dss dataset preview orders --max-rows 10      # peek at data
dss dataset schema orders                     # inspect columns
\`\`\`

### Edit recipe code

\`\`\`bash
dss recipe download-code my-recipe -o code.py # download
# ... edit code.py ...
dss recipe diff my-recipe --file code.py      # review changes
dss recipe set-payload my-recipe --file code.py  # upload
\`\`\`

### Build and monitor

\`\`\`bash
dss job build-and-wait my-dataset --include-logs  # build + wait + stream logs
dss job list                                      # recent jobs
dss job log <job-id>                               # full log output
\`\`\`

### Run a scenario

\`\`\`bash
dss scenario run my-scenario
dss scenario status my-scenario               # check if finished
\`\`\`

## Command reference

\`\`\`
dss <resource> <action> [args...] [--flags]

Resources: project, dataset, recipe, job, scenario, folder, notebook,
           variable, code-env, connection, sql, auth, install-skill
\`\`\`

Use \`dss <resource> --help\` to see all actions and flags for any resource.

## Key flags

\`\`\`
-f, --format FORMAT    json (default) | tsv | table | quiet
-o, --output PATH      write output to file instead of stdout
-v, --verbose          log HTTP requests to stderr
    --project-key KEY  override default project for any command
    --timeout MS       request timeout (default: 30000)
    --stdin            read JSON input from stdin
\`\`\`

## Gotchas

- **Most commands need a project key.** Set it once via \`dss auth login\` or \`DATAIKU_PROJECT_KEY\` to avoid passing \`--project-key\` on every call.
- **Output is JSON by default.** Use \`-f table\` when showing results to a user; use \`-f tsv\` when piping to scripts.
- **\`dss job build\` returns immediately.** Use \`dss job build-and-wait\` to block until the build finishes. Add \`--include-logs\` to stream log output.
- **Folder commands accept names or IDs.** If a folder name contains spaces, quote it. The CLI resolves names to IDs automatically.
- **Recipe set-payload overwrites the entire payload.** Always download first, edit, diff, then upload.
- **Transient errors exit code 3, API errors exit code 2, usage errors exit code 1.** Check exit codes to distinguish retriable failures.
`;

const SKILL_FRONTMATTER = `---
name: dataiku-dss
description: >-
  Interact with Dataiku DSS from the command line \u2014 list projects, query datasets,
  download and upload recipe code, build datasets, run scenarios, and manage jobs.
  Use when the user wants to work with Dataiku DSS resources, inspect a DSS project,
  modify recipes, trigger builds, check job logs, or run SQL against DSS connections,
  even if they don't explicitly mention the dss CLI.
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

export interface InstallResult {
	agent: string;
	path: string;
}

export function installSkill(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const home = homedir();
	const results: InstallResult[] = [];

	for (const { id, def, } of agents) {
		let dir: string;
		if (opts.global) {
			const globalDir = def.globalPath(home,);
			if (!globalDir) {
				process.stderr.write(`  ${def.name}: skipped (no global path available)\n`,);
				continue;
			}
			dir = globalDir;
		} else {
			if (!def.projectPath) {
				process.stderr.write(`  ${def.name}: skipped (no project path available)\n`,);
				continue;
			}
			dir = join(opts.cwd, def.projectPath,);
		}

		mkdirSync(dir, { recursive: true, },);
		const filePath = join(dir, def.filename,);
		writeFileSync(filePath, def.content(), "utf-8",);
		results.push({ agent: id, path: filePath, },);
	}

	return results;
}
