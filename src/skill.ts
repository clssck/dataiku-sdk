import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

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

const SKILL_URLS = [
	new URL("../skills/dataiku-dss/SKILL.md", import.meta.url,),
	new URL("../../skills/dataiku-dss/SKILL.md", import.meta.url,),
];

function skillContent(): string {
	for (const skillUrl of SKILL_URLS) {
		if (fs.existsSync(skillUrl,)) return fs.readFileSync(skillUrl, "utf-8",);
	}
	throw new Error(
		`Bundled Dataiku skill not found. Checked: ${SKILL_URLS.map((url,) => url.pathname).join(", ",)}`,
	);
}

export const AGENTS: Record<string, AgentDef> = {
	claude: {
		name: "Claude Code",
		binary: "claude",
		configDir: ".claude",
		globalPath: (home,) => path.join(home, ".claude", "skills", "dataiku-dss",),
		projectPath: ".claude/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	codex: {
		name: "Codex",
		binary: "codex",
		configDir: ".codex",
		globalPath: (home,) => path.join(home, ".codex", "skills", "dataiku-dss",),
		projectPath: ".codex/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	cursor: {
		name: "Cursor",
		binary: "cursor",
		configDir: ".cursor",
		globalPath: (home,) => path.join(home, ".cursor", "skills", "dataiku-dss",),
		projectPath: ".cursor/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	pi: {
		name: "Pi",
		binary: "pi",
		configDir: ".pi",
		globalPath: (home,) => path.join(home, ".pi", "agent", "skills", "dataiku-dss",),
		projectPath: ".pi/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
	omp: {
		name: "OhMyPi",
		binary: "omp",
		configDir: path.join(".omp", "agent",),
		configDirRequired: true,
		globalPath: (home,) => path.join(home, ".omp", "agent", "skills", "dataiku-dss",),
		projectPath: ".omp/skills/dataiku-dss",
		filename: "SKILL.md",
		content: skillContent,
	},
};

// ---------------------------------------------------------------------------
// Agent detection
// ---------------------------------------------------------------------------

export interface DetectedAgent {
	id: string;
	def: AgentDef;
	via: "binary" | "config-dir" | "flag";
}

export function detectAgents(): DetectedAgent[] {
	const home = os.homedir();
	const found: DetectedAgent[] = [];
	for (const [id, def,] of Object.entries(AGENTS,)) {
		const hasBinary = Bun.which(def.binary,) !== null;
		const hasConfigDir = fs.existsSync(path.join(home, def.configDir,),);
		if (hasBinary && (!def.configDirRequired || hasConfigDir)) {
			found.push({ id, def, via: "binary", },);
		} else if (hasConfigDir) {
			found.push({ id, def, via: "config-dir", },);
		}
	}
	return found;
}

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
			if (fs.existsSync(path.join(dir, marker,),)) return dir;
		}
		const parent = path.dirname(dir,);
		if (parent === dir) break;
		dir = parent;
	}
	return startDir;
}

// ---------------------------------------------------------------------------
// Skill installation
// ---------------------------------------------------------------------------
export interface InstallResult {
	agent: string;
	path: string;
	via: DetectedAgent["via"];
}

export function planSkillInstalls(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const home = os.homedir();
	const results: InstallResult[] = [];

	for (const { id, def, via, } of agents) {
		const dir = opts.global
			? def.globalPath(home,)
			: def.projectPath
			? path.join(opts.cwd, def.projectPath,)
			: undefined;
		if (!dir) continue;
		results.push({ agent: id, path: path.join(dir, def.filename,), via, },);
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
		fs.mkdirSync(path.dirname(result.path,), { recursive: true, },);
		fs.writeFileSync(result.path, def.content(), "utf-8",);
	}

	return results;
}
