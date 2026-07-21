import { describe, expect, it, } from "bun:test";
import {
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileExists,
	readFileSync,
	rmSync,
	tmpdir,
} from "./_harness.js";

describe("CLI install-skill command", () => {
	it("error envelope example in the skill matches a real envelope", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-parity-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const jsonBlock = readFileSync(skillPath, "utf-8",).match(/```json\n([\s\S]*?)\n```/,);
			expect(jsonBlock,).not.toBeNull();
			const documented = JSON.parse(jsonBlock![1]!,) as Record<string, unknown>;
			const failure = await dssFailure(["dataset", "frobnicate",], {
				env: { PATH: process.env.PATH ?? "", DATAIKU_DISABLE_ENV: "1", DSS_CONFIG_DIR: tmpDir, },
			},);
			const real = JSON.parse(failure.stderr,) as Record<string, unknown>;
			for (const key of Object.keys(documented,)) {
				expect(real, `skill envelope documents "${key}" but a real envelope omits it`,)
					.toHaveProperty(key,);
			}
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

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
				"--dry-run",
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
			expect(content,).toContain("name: dataiku-dss",);
			expect(content,).toContain("dss agent contract",);
			expect(content,).toContain('type:"error"',);
			expect(content,).toContain('type:"trace"',);
			expect(content,).toContain("dss commands run",);
			expect(content,).toContain('sideEffect:"write"',);
			expect(content,).toContain("exact argv with `--plan`",);
			expect(content,).toContain("`dryRun:true`",);
			expect(content,).toContain("`--record-cleanup cleanup.jsonl`",);
			expect(content,).toContain("`dss cleanup --file cleanup.jsonl`",);
			expect(content,).toContain("`inputContract`",);
			expect(content,).toContain("`--data-file PATH` or `--stdin`",);
			expect(content,).toContain("`--request-timeout MS` and `--retries N`",);
			expect(content,).toContain("`dss fixtures --json`",);
			expect(content,).toContain("dss auth login --url",);
			expect(content,).toContain("~/.config/dataiku/credentials.json",);
			expect(content,).toContain("For disposable agent tests, set `DSS_CONFIG_DIR`",);
			expect(content,).toContain("node ./bin/dss.js",);
			expect(content,).toContain("bun --no-env-file src/cli.ts",);
			expect(content,).toContain("bun --no-env-file ./bin/dss.js",);
			expect(content,).toContain("PowerShell:\n\n```powershell\n$env:DATAIKU_URL",);
			expect(content,).toContain('Windows Command Prompt:\n\n```bat\nset "DATAIKU_URL=',);
			expect(content,).not.toContain("/path/to/dataiku-sdk/bin/dss",);
			expect(content,).toContain("command's current working directory",);
			expect(content,).toContain('[{"projectKey":"MYPROJ","name":"My Project"}]',);
			expect(content,).toContain('{"recipe":{"name":"<NAME>","type":"python"},"payload":"..."}',);
			expect(content,).toContain(
				"dss recipe get-payload compute_orders --raw --output code.py --project-key MYPROJ",
			);
			expect(content,).toContain("stdout is the JSON string equal to `PATH`",);
			expect(content,).toContain("dataset_download_default_location",);
			expect(content,).toContain("syncOutputSchemaPropagated",);
			expect(content,).toContain("first verifies the parent service through its settings",);
			expect(content,).toContain("preserving markdown and raw cells unchanged",);
			expect(content,).toContain(
				"dataset metadata `404` and `403` errors propagate as `not_found` and `permission_denied`",
			);
			expect(content,).toContain("Treat `details.body` as sanitized metadata only",);
			expect(content,).toContain(
				"`details.statusText` is canonical text derived from the numeric status",
			);
			expect(content,).not.toContain("--help",);
			expect(content,).not.toContain("--report-json",);
			expect(content,).not.toContain("dss auth status",);
			expect(content,).not.toContain("dss auth logout",);
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
			expect(content,).toContain("name: dataiku-dss",);
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
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill is idempotent", async () => {
		const tmpDir = join(tmpdir(), `dss-cli-skill-idem-${Date.now()}`,);
		mkdirSync(tmpDir, { recursive: true, },);
		try {
			await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			await dss(["install-skill", "--agent", "claude",], { cwd: tmpDir, },);
			const skillPath = join(tmpDir, ".claude", "skills", "dataiku-dss", "SKILL.md",);
			const content = readFileSync(skillPath, "utf-8",);
			expect(content,).toContain("name: dataiku-dss",);
		} finally {
			rmSync(tmpDir, { recursive: true, force: true, },);
		}
	});

	it("dss install-skill --agent unknown fails with UsageError", async () => {
		const failure = await dssFailure(["install-skill", "--agent", "unknown",],);
		expect(failure.stderr,).toContain("Unknown agent: unknown",);
		expect(failure.code,).toBe(1,);
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
			expect(content,).toContain("name: dataiku-dss",);
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
			expect(content,).toContain("name: dataiku-dss",);
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
			expect(content,).toContain("name: dataiku-dss",);
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
			expect(content,).toContain("name: dataiku-dss",);
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
			expect(content,).toContain("name: dataiku-dss",);
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
});
