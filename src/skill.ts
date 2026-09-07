import { createHash, } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";

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

function skillDirectory(): URL {
	for (const skillUrl of SKILL_URLS) {
		if (fs.existsSync(skillUrl,)) return new URL("./", skillUrl,);
	}
	throw new Error(
		`Bundled Dataiku skill not found. Checked: ${SKILL_URLS.map((url,) => url.pathname).join(", ",)}`,
	);
}

function skillContent(): string {
	return fs.readFileSync(new URL("SKILL.md", skillDirectory(),), "utf-8",);
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

export interface InstalledSkillFile {
	relativePath: string;
	path: string;
	status: SkillStatus;
	changed: boolean;
	expectedSha256: string;
	actualSha256?: string;
}

export interface InstallResult {
	agent: string;
	path: string;
	via: DetectedAgent["via"];
	/** Missing entrypoint, incomplete/different bundle, or entirely current bundle. */
	status: SkillStatus;
	/** Whether any bundled file would (or did) change. Unmanaged files are preserved. */
	changed: boolean;
	/** SHA-256 of the canonical SKILL.md entrypoint, not the whole bundle. */
	expectedSha256: string;
	/** Hash of the existing entrypoint; absent when that file is missing. */
	actualSha256?: string;
	files: InstalledSkillFile[];
}

function sha256Hex(value: string | Buffer,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

function skillState(
	target: string,
	expectedSha256: string,
): Pick<InstalledSkillFile, "status" | "actualSha256"> {
	if (!fs.existsSync(target,)) return { status: "missing", };
	const actualSha256 = sha256Hex(fs.readFileSync(target,),);
	return {
		status: actualSha256 === expectedSha256 ? "current" : "stale",
		actualSha256,
	};
}

/** Load the packaged tree, keeping portable relative paths in the report. */
function skillFiles(): Map<string, Buffer> {
	const root = fileURLToPath(skillDirectory(),);
	const files = new Map<string, Buffer>();
	function visit(relativeDir: string,): void {
		const dir = path.join(root, relativeDir,);
		for (
			const entry of fs.readdirSync(dir, { withFileTypes: true, },).sort((a, b,) =>
				a.name < b.name ? -1 : a.name > b.name ? 1 : 0
			)
		) {
			const relativePath = relativeDir ? `${relativeDir}/${entry.name}` : entry.name;
			if (entry.isDirectory()) visit(relativePath,);
			else if (entry.isFile()) {
				files.set(relativePath, fs.readFileSync(path.join(root, relativePath,),),);
			} else throw new Error(`Unsupported bundled skill entry: ${relativePath}`,);
		}
	}
	visit("",);
	return files;
}

function skillInstallPlan(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
	bundle: Map<string, Buffer>,
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
		const files: InstalledSkillFile[] = [];
		for (const [sourcePath, source,] of bundle) {
			const relativePath = sourcePath === "SKILL.md" ? def.filename : sourcePath;
			const target = path.join(dir, relativePath,);
			const expectedSha256 = sha256Hex(sourcePath === "SKILL.md" ? def.content() : source,);
			const state = skillState(target, expectedSha256,);
			files.push({
				relativePath,
				path: target,
				...state,
				changed: state.status !== "current",
				expectedSha256,
			},);
		}
		const entrypoint = files.find((file,) => file.relativePath === def.filename)!;
		const changed = files.some((file,) => file.changed);
		results.push({
			agent: id,
			path: entrypoint.path,
			via,
			status: entrypoint.status === "missing" ? "missing" : changed ? "stale" : "current",
			changed,
			expectedSha256: entrypoint.expectedSha256,
			...(entrypoint.actualSha256 !== undefined ? { actualSha256: entrypoint.actualSha256, } : {}),
			files,
		},);
	}
	return results;
}

export function planSkillInstalls(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	return skillInstallPlan(agents, opts, skillFiles(),);
}

function writeSkillFile(file: InstalledSkillFile, content: string | Buffer,): void {
	if (!file.changed) return;
	const dir = path.dirname(file.path,);
	fs.mkdirSync(dir, { recursive: true, },);
	const tmpPath = path.join(
		dir,
		`.${path.basename(file.path,)}.tmp-${process.pid}-${Date.now().toString(36,)}`,
	);
	try {
		fs.writeFileSync(tmpPath, content,);
		fs.renameSync(tmpPath, file.path,);
	} finally {
		fs.rmSync(tmpPath, { force: true, },);
	}
}

export function installSkill(
	agents: DetectedAgent[],
	opts: { global: boolean; cwd: string; },
): InstallResult[] {
	const bundle = skillFiles();
	const results = skillInstallPlan(agents, opts, bundle,);
	for (const result of results) {
		if (!result.changed) continue;
		const def = agents.find((agent,) => agent.id === result.agent)!.def;
		// Publish referenced files first; never expose a new entrypoint with missing references.
		for (const file of result.files) {
			if (file.relativePath !== def.filename) writeSkillFile(file, bundle.get(file.relativePath,)!,);
		}
		writeSkillFile(result.files.find((file,) => file.relativePath === def.filename)!, def.content(),);
	}
	return results;
}
