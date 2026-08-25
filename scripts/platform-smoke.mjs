import { execFileSync, spawnSync, } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath, } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url,),), "..",);
const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-sdk-platform-",),);

function assert(condition, message,) {
	if (!condition) throw new Error(message,);
}

function run(command, args, options = {},) {
	return execFileSync(command, args, {
		cwd: root,
		encoding: "utf8",
		stdio: ["ignore", "pipe", "inherit",],
		...options,
	},);
}

try {
	const packDir = path.join(tempRoot, "packed artifact",);
	const installDir = path.join(tempRoot, "installed package",);
	const skillTarget = path.join(tempRoot, "skill target",);
	fs.mkdirSync(packDir, { recursive: true, },);
	fs.mkdirSync(installDir, { recursive: true, },);
	fs.writeFileSync(path.join(installDir, "package.json",), '{"private":true}\n',);

	const packOutput = run(process.execPath, [
		"pm",
		"pack",
		`--destination=${packDir}`,
		"--quiet",
	],);
	const packedPath = packOutput.trim().split(/\r?\n/,).at(-1,);
	assert(typeof packedPath === "string" && packedPath.length > 0, "Bun pack omitted filename",);
	const tarball = path.isAbsolute(packedPath,) ? packedPath : path.join(packDir, packedPath,);
	assert(fs.existsSync(tarball,), `packed artifact is missing: ${tarball}`,);

	run(process.execPath, [
		"add",
		"--cwd",
		installDir,
		"--no-save",
		"--ignore-scripts",
		tarball,
	],);

	const cli = path.join(installDir, "node_modules", "dataiku-sdk", "bin", "dss.js",);
	assert(fs.existsSync(cli,), `installed CLI entrypoint is missing: ${cli}`,);

	const version = JSON.parse(
		run(process.execPath, ["--no-env-file", cli, "version",], { cwd: installDir, },),
	);
	const nodeVersion = JSON.parse(run("node", [cli, "version",], { cwd: installDir, },),);
	assert(typeof version.version === "string", "Bun packaged CLI version output is invalid",);
	assert(nodeVersion.version === version.version, "Node and Bun reported different CLI versions",);

	const skillResult = JSON.parse(
		run(process.execPath, [
			"--no-env-file",
			cli,
			"install-skill",
			"--agent",
			"claude",
			"--target",
			skillTarget,
		], { cwd: installDir, },),
	);
	const skillPath = path.join(skillTarget, ".claude", "skills", "dataiku-dss", "SKILL.md",);
	assert(skillResult.installed?.[0]?.path === skillPath, "skill installer returned the wrong path",);
	const skill = fs.readFileSync(skillPath, "utf8",);
	assert(skill.includes("node ./bin/dss.js",), "skill omits the portable checkout launcher",);
	assert(
		skill.includes("bun --no-env-file ./bin/dss.js",),
		"skill omits the Bun checkout launcher",
	);
	assert(skill.includes("PowerShell",), "skill omits PowerShell environment syntax",);
	assert(
		skill.includes("Windows Command Prompt",),
		"skill omits Command Prompt environment syntax",
	);

	const failure = spawnSync("node", [cli, "not-a-resource",], {
		cwd: installDir,
		encoding: "utf8",
		stdio: ["ignore", "pipe", "pipe",],
	},);
	assert(failure.status === 1, `packaged CLI returned unexpected error status: ${failure.status}`,);
	assert(failure.stderr === "", "packaged CLI wrote error output to stderr",);
	const errorLines = failure.stdout.trim().split("\n",);
	assert(
		errorLines.length === 1,
		"packaged CLI error output was truncated or was not one JSONL event",
	);
	const error = JSON.parse(errorLines[0],);
	assert(error.type === "error" && error.exitCode === 1, "packaged CLI error envelope is invalid",);

	process.stdout.write(`${
		JSON.stringify({
			ok: true,
			platform: process.platform,
			arch: process.arch,
			bun: globalThis.Bun.version,
			node: run("node", ["--version",],).trim(),
			version: version.version,
		},)
	}\n`,);
} finally {
	fs.rmSync(tempRoot, { recursive: true, force: true, maxRetries: 3, },);
}
