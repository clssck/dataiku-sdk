#!/usr/bin/env -S bun --no-env-file
// npm bin for the dss CLI. Bun is the only supported runtime, so this launcher
// runs the CLI in-process instead of spawning a second interpreter.
//
// `--no-env-file` must reach Bun on the command line: Bun preloads .env before
// any user code runs, and the CLI's own loader (src/cli/env.ts) owns .env
// precedence and DATAIKU_DISABLE_ENV. `env -S` splits the argument on POSIX
// hosts, and the in-process guard below fails closed if an intermediary
// such as a package runner drops the shebang argument.
import * as path from "node:path";
import { pathToFileURL, } from "node:url";

const envFileAutoloadDisabled = process.execArgv.includes("--no-env-file",);

if (!envFileAutoloadDisabled) {
	process.stdout.write(`${
		JSON.stringify({
			type: "error",
			ok: false,
			error: "Bun automatic .env loading must be disabled for the dss launcher.",
			code: "env_autoload_enabled",
			category: "usage",
			exitCode: 1,
			hint:
				"Use `bunx dataiku-sdk` or pass `--no-env-file` before the script path. Do not use `bunx --bun`.",
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
