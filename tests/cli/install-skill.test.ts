import { describe, expect, it, } from "bun:test";
import { createHash, } from "node:crypto";
import { statSync, utimesSync, writeFileSync, } from "node:fs";
import {
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileExists,
	readFileSync,
	rmSync,
	SDK_ROOT,
	tmpdir,
} from "./_harness.js";
interface InstalledEntry {
	agent: string;
	path: string;
	via: string;
	status: string;
	changed: boolean;
	expectedSha256: string;
	actualSha256?: string;
	files: Array<
		{
			relativePath: string;
			path: string;
			status: string;
			changed: boolean;
			expectedSha256: string;
			actualSha256?: string;
		}
	>;
}

function firstEntryOf(stdout: string,): InstalledEntry {
	const parsed: unknown = JSON.parse(stdout,);
	if (typeof parsed !== "object" || parsed === null || !("installed" in parsed)) {
		throw new Error("install-skill output has no installed array",);
	}
	const installed: unknown = (parsed as { installed: unknown; }).installed;
	if (!Array.isArray(installed,) || installed.length === 0) {
		throw new Error("install-skill installed array is empty",);
	}
	return installed[0] as InstalledEntry;
}

describe("CLI install-skill command", () => {
	it("dss install-skill --dry-run emits JSON without writing files", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-dry-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stdout, stderr, } = await dss([
				"install-skill",
				"--agent",
				"claude",
				"--target",
				tmpDir,
				"--dry-run=true",
			],);
			expect(stderr,).toBe("",);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			expect(JSON.parse(stdout,),).toMatchObject({
				scope: "project",
				target: tmpDir,
				dryRun: true,
				installed: [{ agent: "claude", path: skillPath, via: "flag", },],
			},);
			expect(readFileExists(skillPath,),).toBe(false,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --list-agents emits JSON", async () => {
		const { stdout, stderr, } = await dss(["install-skill", "--agent", "omp", "--list-agents",],);
		expect(stderr,).toBe("",);
		const result = JSON.parse(stdout,) as { agents: Array<Record<string, unknown>>; };
		expect(result.agents,).toEqual([{ id: "omp", name: "OhMyPi", via: "flag", },],);
	});

	it("dry-run reports status missing, changed true, and the expected hash without writing", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-status-missing-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stdout, stderr, } = await dss([
				"install-skill",
				"--agent",
				"claude",
				"--target",
				tmpDir,
				"--dry-run",
			],);
			expect(stderr,).toBe("",);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const entry = firstEntryOf(stdout,);
			expect(entry,).toMatchObject({
				agent: "claude",
				path: skillPath,
				via: "flag",
				status: "missing",
				changed: true,
			},);
			expect(entry.expectedSha256,).toMatch(/^[0-9a-f]{64}$/,);
			expect(entry,).not.toHaveProperty("actualSha256",);
			// Plan/dry-run preserved: no file, no directories left behind.
			expect(readFileExists(skillPath,),).toBe(false,);
			expect(readFileExists(join(tmpDir, ".claude",),),).toBe(false,);
			const canonicalSkill = readFileSync(
				join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",),
				"utf-8",
			);
			expect(entry.expectedSha256,).toBe(
				createHash("sha256",).update(canonicalSkill,).digest(
					"hex",
				),
			);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dry-run detects a stale copy as stale without refreshing it", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-status-stale-${Date.now()}`,);
		const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
		mkdirSync(join(tmpDir, ".claude", "skills", "dataiku-dss",), { recursive: true, },);
		writeFileSync(skillPath, "# outdated skill copy\n", "utf-8",);
		try {
			const { stdout, stderr, } = await dss([
				"install-skill",
				"--agent",
				"claude",
				"--target",
				tmpDir,
				"--dry-run",
			],);
			expect(stderr,).toBe("",);
			const entry = firstEntryOf(stdout,);
			expect(entry,).toMatchObject({
				agent: "claude",
				status: "stale",
				changed: true,
			},);
			const actualSha256 = entry.actualSha256;
			expect(actualSha256,).toMatch(/^[0-9a-f]{64}$/,);
			expect(actualSha256,).not.toBe(entry.expectedSha256,);
			expect(actualSha256,).toBe(
				createHash("sha256",).update(readFileSync(skillPath,),).digest("hex",),
			);
			// Dry-run must not refresh the stale copy.
			expect(readFileSync(skillPath, "utf-8",),).toBe("# outdated skill copy\n",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("first install reports changed true and second install is current and writes nothing", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-status-current-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const canonicalSkill = readFileSync(
				join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",),
				"utf-8",
			);
			const first = await dss(["install-skill", "--agent", "claude", "--target", tmpDir,],);
			expect(first.stderr,).toBe("",);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const firstEntry = firstEntryOf(first.stdout,);
			expect(firstEntry,).toMatchObject({
				agent: "claude",
				path: skillPath,
				via: "flag",
				status: "missing",
				changed: true,
			},);
			expect(statSync(skillPath,).isFile(),).toBe(true,);
			// Backdate the first write deterministically, so a rewrite during the
			// second install is observable as an mtime change without wall-clock waits.
			const past = new Date(Date.now() - 3_600_000,);
			utimesSync(skillPath, past, past,);
			const afterFirst = statSync(skillPath,).mtimeMs;
			const second = await dss(["install-skill", "--agent", "claude", "--target", tmpDir,],);
			expect(second.stderr,).toBe("",);
			const secondEntry = firstEntryOf(second.stdout,);
			expect(secondEntry,).toMatchObject({
				agent: "claude",
				path: skillPath,
				status: "current",
				changed: false,
			},);
			expect(secondEntry.expectedSha256,).toBe(firstEntry.expectedSha256,);
			expect(secondEntry.actualSha256,).toBe(firstEntry.expectedSha256,);
			// Byte-identical files are skipped: no rewrite happened.
			expect(statSync(skillPath,).mtimeMs,).toBe(afterFirst,);
			expect(readFileSync(skillPath, "utf-8",),).toBe(canonicalSkill,);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent claude writes SKILL.md to project dir", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stdout, stderr, } = await dss(["install-skill", "--agent", "claude",], {
				cwd: tmpDir,
			},);
			expect(stderr,).toBe("",);
			const result = JSON.parse(stdout,) as {
				scope: string;
				installed: Array<{ agent: string; path: string; via: string; }>;
			};
			expect(result.scope,).toBe("project",);
			expect(result.installed,).toHaveLength(1,);
			expect(result.installed[0],).toMatchObject({ agent: "claude", via: "flag", },);
			expect(result.installed[0]!.path,).toEndWith(
				join(".claude", "skills", "dataiku-dss", "SKILL.md",),
			);
			const skillPath = result.installed[0]!.path;

			const content = readFileSync(skillPath, "utf-8",);
			const canonicalSkill = readFileSync(
				join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",),
				"utf-8",
			);
			// Installed bytes are canonical, and the reported expected hash agrees;
			// the install ran on a missing destination, so changed is true here.
			expect(content,).toBe(canonicalSkill,);
			const installedEntry = firstEntryOf(stdout,);
			expect(installedEntry.status,).toBe("missing",);
			expect(installedEntry.changed,).toBe(true,);
			expect(installedEntry.expectedSha256,).toBe(
				createHash("sha256",).update(canonicalSkill,).digest("hex",),
			);
			expect(installedEntry.actualSha256,).toBeUndefined();
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent codex writes to .codex/skills/", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-codex-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "codex",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".codex", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent cursor writes to .cursor/skills/", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-cursor-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "cursor",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".cursor", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent unknown fails with UsageError", async () => {
		const failure = await dssFailure(["install-skill", "--agent", "unknown",],);
		expect(failure.code,).toBe(1,);
		expect(failure.stderr,).toBe("",);
		expect(JSON.parse(failure.stdout,) as Record<string, unknown>,).toMatchObject({
			code: "usage_error",
			category: "usage",
			exitCode: 1,
		},);
	});

	it("dss install-skill --target writes to specified directory", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-target-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const { stdout, stderr, } = await dss([
				"install-skill",
				"--agent",
				"claude",
				"--target",
				tmpDir,
			],);
			expect(stderr,).toBe("",);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			expect(JSON.parse(stdout,),).toMatchObject({
				target: tmpDir,
				installed: [{ agent: "claude", path: skillPath, via: "flag", },],
			},);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("workspace detection finds .git parent for project installs", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-ws-${Date.now()}`,);
		const subdir = join(workspace, "sub",);
		mkdirSync(join(workspace, ".git",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "claude",], { cwd: subdir, },);
			const skillPath = join(workspace, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("workspace detection ignores nested agent config parents for project installs", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-pi-${Date.now()}`,);
		const subdir = join(workspace, "nested", "deeper",);
		mkdirSync(join(workspace, ".pi",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "pi",], { cwd: subdir, },);
			const skillPath = join(subdir, ".pi", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("workspace detection ignores nested .omp agent parents for project installs", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-omp-${Date.now()}`,);
		const subdir = join(workspace, "nested", "deeper",);
		mkdirSync(join(workspace, ".omp", "agent",), { recursive: true, },);
		mkdirSync(subdir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "omp",], { cwd: subdir, },);
			const skillPath = join(subdir, ".omp", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
		}
	});

	it("--target overrides workspace detection", async () => {
		const workspace = join(tmpdir(), `dss-cli-skill-override-${Date.now()}`,);
		const target = join(tmpdir(), `dss-cli-skill-target2-${Date.now()}`,);
		mkdirSync(join(workspace, ".git",), { recursive: true, },);
		mkdirSync(target, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "claude", "--target", target,], { cwd: workspace, },);
			const skillPath = join(target, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(workspace, { recursive: true, force: true, },);
			rmSync(target, { recursive: true, force: true, },);
		}
	});

	it("commands run lists install-skill as a resource", async () => {
		const { stdout, stderr, } = await dss(["commands", "run",],);
		expect(stderr,).toBe("",);
		const registry = JSON.parse(stdout,) as Record<string, unknown>;
		expect(registry,).toHaveProperty("install-skill",);
	});

	it("repairs missing or stale references without rewriting current files or removing user files", async () => {
		const tmpDir = join(tmpdir(), `dss-skill-bundle-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			const args = ["install-skill", "--agent", "claude", "--target", tmpDir,];
			const first = firstEntryOf((await dss(args,)).stdout,);
			const references = first.files.filter((file,) => file.relativePath.startsWith("references/",));
			const stale = references[0]!;
			const missing = references[1]!;
			writeFileSync(stale.path, "outdated reference",);
			rmSync(missing.path,);
			const userFile = join(tmpDir, ".claude", "skills", "dataiku-dss", "notes.txt",);
			writeFileSync(userFile, "user content",);
			const past = new Date(Date.now() - 3_600_000,);
			utimesSync(first.path, past, past,);
			const mtime = statSync(first.path,).mtimeMs;
			const plan = firstEntryOf((await dss([...args, "--dry-run",],)).stdout,);
			expect(plan,).toMatchObject({
				status: "stale",
				changed: true,
				actualSha256: first.expectedSha256,
			},);
			expect(plan.files.find((file,) => file.path === stale.path),).toMatchObject({
				status: "stale",
				changed: true,
			},);
			expect(plan.files.find((file,) => file.path === missing.path),).toMatchObject({
				status: "missing",
				changed: true,
			},);
			expect(readFileSync(stale.path, "utf-8",),).toBe("outdated reference",);
			expect(readFileExists(missing.path,),).toBe(false,);
			await dss(args,);
			for (const file of first.files) {
				const content = readFileSync(file.path,);
				expect(content,).toEqual(
					readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", file.relativePath,),),
				);
				expect(createHash("sha256",).update(content,).digest("hex",),).toBe(file.expectedSha256,);
			}
			const installedSkill = readFileSync(first.path, "utf-8",);
			for (const match of installedSkill.matchAll(/\]\((references\/[^)]+)\)/g,)) {
				expect(readFileExists(join(tmpDir, ".claude", "skills", "dataiku-dss", match[1]!,),),).toBe(
					true,
				);
			}
			expect(readFileSync(userFile, "utf-8",),).toBe("user content",);
			expect(statSync(first.path,).mtimeMs,).toBe(mtime,);
			expect(firstEntryOf((await dss([...args, "--dry-run",],)).stdout,),).toMatchObject({
				status: "current",
				changed: false,
			},);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("reports the home target for a global install independently of cwd", async () => {
		const home = join(tmpdir(), `dss-skill-global-${Date.now()}`,);
		mkdirSync(home, { recursive: true, },);
		try {
			const env = {
				...process.env,
				HOME: home,
				USERPROFILE: home,
				DATAIKU_DISABLE_ENV: "1",
				DSS_CONFIG_DIR: join(home, "config",),
			};
			const args = ["install-skill", "--agent", "claude", "--global",];
			const preview = JSON.parse((await dss([...args, "--dry-run",], { env, },)).stdout,);
			expect(preview.target,).toBe(home,);
			expect(readFileExists(preview.installed[0].path,),).toBe(false,);
			const installed = JSON.parse((await dss(args, { env, },)).stdout,);
			expect(installed.target,).toBe(home,);
			expect(installed.installed[0].path,).toBe(
				join(home, ".claude", "skills", "dataiku-dss", "SKILL.md",),
			);
			expect(readFileSync(installed.installed[0].path, "utf-8",),).toBe(
				readFileSync(join(SDK_ROOT, "skills", "dataiku-dss", "SKILL.md",), "utf-8",),
			);
		} finally {
			rmSync(home, { recursive: true, force: true, },);
		}
	});
});
