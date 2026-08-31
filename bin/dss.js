#!/usr/bin/env node
// npm bin for the dss CLI. Bun is the only runtime that executes CLI code.
// A Node-launched npm shim only bootstraps Bun with automatic .env loading
// disabled; `bunx --bun` enters Bun's Node-compatibility mode, which disables
// that loading before this module runs.
import { spawnSync, } from "node:child_process";
import * as path from "node:path";
import { fileURLToPath, pathToFileURL, } from "node:url";

const runningUnderBun = typeof process.versions.bun === "string";
const bunNodeCompatibilityMode = runningUnderBun
	&& path.basename(process.argv0,).toLowerCase().startsWith("node",);
const envFileAutoloadDisabled = process.execArgv.includes("--no-env-file",)
	|| bunNodeCompatibilityMode;

if (!runningUnderBun) {
	const result = spawnSync(
		"bun",
		["--no-env-file", fileURLToPath(import.meta.url,), ...process.argv.slice(2,),],
		{ stdio: "inherit", env: process.env, },
	);
	if (result.error) {
		process.stdout.write(`${
			JSON.stringify({
				type: "error",
				ok: false,
				error: "Unable to start the required Bun runtime.",
				code: "internal_error",
				category: "internal",
				exitCode: 2,
				hint: "Install Bun >= 1.4.0 and ensure `bun` is on PATH.",
			},)
		}\n`,);
		process.exitCode = 2;
	} else if (result.signal) {
		process.kill(process.pid, result.signal,);
	} else {
		process.exitCode = result.status ?? 1;
	}
} else if (!envFileAutoloadDisabled) {
	process.stdout.write(`${
		JSON.stringify({
			type: "error",
			ok: false,
			error: "Bun automatic .env loading must be disabled for the dss launcher.",
			code: "env_autoload_enabled",
			category: "usage",
			exitCode: 1,
			hint: "Use `bunx --bun dataiku-sdk` or pass `--no-env-file` before the script path.",
		},)
	}\n`,);
	process.exitCode = 1;
} else {
	const here = import.meta.dir;
	const distCliPath = path.resolve(here, "../dist/src/cli.js",);
	const sourceCliPath = path.resolve(here, "../src/cli.ts",);
	const usesDistCli = await Bun.file(distCliPath,).exists();
	const cliPath = usesDistCli ? distCliPath : sourceCliPath;

	// Published dist carries dist/build-metadata.json with the revision it was
	// built from. Hand it to the CLI so provenance can report dist source and its
	// build revision; only a full lowercase hexadecimal revision is accepted, so an
	// inherited or corrupt variable never stands in for packaged metadata.
	if (usesDistCli) {
		process.env.DSS_LOAD_SOURCE = "dist";
		delete process.env.DSS_BUILD_REVISION;
		try {
			const metadata = await Bun.file(path.resolve(here, "../dist/build-metadata.json",),).json();
			const revision = metadata.buildRevision;
			if (typeof revision === "string" && /^[0-9a-f]{40}$/.test(revision,)) {
				process.env.DSS_BUILD_REVISION = revision;
			}
		} catch {
			// Dist without build metadata: provenance still reports dist source.
		}
	}

	// Same process, same argv: the CLI sees process.argv.slice(2) exactly as before.
	await import(pathToFileURL(cliPath,).href);
}
