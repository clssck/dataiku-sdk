import { createHash, } from "node:crypto";
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

export type SkillStatus = "missing" | "stale" | "current";

export interface InstallResult {
	agent: string;
	path: string;
	via: DetectedAgent["via"];
	/** Deterministic state of the destination file relative to the canonical skill content. */
	status: SkillStatus;
	/** Whether an install run would (or did) write the destination file. */
	changed: boolean;
	/** SHA-256 hex of the canonical skill bytes this installation writes. */
	expectedSha256: string;
	/** SHA-256 hex of the destination file when it exists; absent when status is "missing". */
	actualSha256?: string;
}

function sha256Hex(value: string | Buffer,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

function skillState(
	target: string,
	expectedSha256: string,
): Pick<InstallResult, "status" | "actualSha256"> {
	if (!fs.existsSync(target,)) return { status: "missing", };
	const actualSha256 = sha256Hex(fs.readFileSync(target,),);
	return {
		status: actualSha256 === expectedSha256 ? "current" : "stale",
		actualSha256,
	};
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
		const target = path.join(dir, def.filename,);
		const expectedSha256 = sha256Hex(def.content(),);
		const state = skillState(target, expectedSha256,);
		results.push({
			agent: id,
			path: target,
			via,
			...state,
			changed: state.status !== "current",
			expectedSha256,
		},);
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
		if (!def || !result.changed) continue;
		const content = def.content();
		const dir = path.dirname(result.path,);
		fs.mkdirSync(dir, { recursive: true, },);
		const tmpPath = path.join(
			dir,
			`.${path.basename(result.path,)}.tmp-${process.pid}-${Date.now().toString(36,)}`,
		);
		fs.writeFileSync(tmpPath, content, "utf-8",);
		try {
			fs.renameSync(tmpPath, result.path,);
		} catch (error) {
			fs.rmSync(tmpPath, { force: true, },);
			throw error;
		}
	}

	return results;
}
