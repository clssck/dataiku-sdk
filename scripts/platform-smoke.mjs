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
	const result = Bun.spawnSync([command, ...args,], {
		cwd: root,
		stdout: "pipe",
		stderr: "inherit",
		...options,
	},);
	if (result.exitCode !== 0) {
		throw new Error(
			`${command} ${
				args.join(" ",)
			} failed with exit code ${result.exitCode}: ${result.stdout.toString()}`,
		);
	}
	return result.stdout.toString();
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
	assert(typeof version.version === "string", "Bun packaged CLI version output is invalid",);
	const bunxVersion = JSON.parse(
		run(process.execPath, ["x", "dss", "version",], { cwd: installDir, },),
	);
	assert(bunxVersion.version === version.version, "bunx reported the wrong packaged CLI version",);
	assert(bunxVersion.runtime === "bun", "bunx did not execute the packaged CLI under Bun",);

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
	assert(
		skill.includes("bun --no-env-file ./bin/dss.js",),
		"skill omits the Bun checkout launcher",
	);
	assert(skill.includes("PowerShell",), "skill omits PowerShell environment syntax",);
	assert(
		skill.includes("Windows Command Prompt",),
		"skill omits Command Prompt environment syntax",
	);

	const failure = Bun.spawnSync([process.execPath, "--no-env-file", cli, "not-a-resource",], {
		cwd: installDir,
		stdout: "pipe",
		stderr: "pipe",
	},);
	assert(
		failure.exitCode === 1,
		`packaged CLI returned unexpected error status: ${failure.exitCode}`,
	);
	assert(failure.stderr.toString() === "", "packaged CLI wrote error output to stderr",);
	const errorLines = failure.stdout.toString().trim().split("\n",);
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
			version: version.version,
		},)
	}\n`,);
} finally {
	fs.rmSync(tempRoot, { recursive: true, force: true, maxRetries: 3, },);
}
