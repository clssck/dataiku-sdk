#!/usr/bin/env node
// npm bin for the dss CLI. Bun is the only runtime that executes CLI code.
// A Node-launched npm shim only bootstraps Bun with automatic .env loading
// disabled; `bunx --bun` enters Bun's Node-compatibility mode, which disables
// that loading before this module runs.
import { spawnSync, } from "node:child_process";
import { readFileSync, } from "node:fs";
import * as path from "node:path";
import { fileURLToPath, pathToFileURL, } from "node:url";

const runningUnderBun = typeof process.versions.bun === "string";
const bunNodeCompatibilityMode = runningUnderBun
	&& (
		path.basename(process.argv0,).toLowerCase().startsWith("node",)
		|| (
			process.env.npm_lifecycle_event === "bunx"
			&& ["dss", "dataiku-sdk",].includes(process.env.npm_lifecycle_script ?? "",)
			&& /^bun(?:\.exe)?$/i.test(path.basename(process.env.npm_execpath ?? "",),)
		)
	);
const envFileAutoloadDisabled = process.execArgv.includes("--no-env-file",)
	|| bunNodeCompatibilityMode;
// The one source of the minimum is package.json engines.bun (">=X.Y.Z").
const MINIMUM_BUN = JSON.parse(
	readFileSync(fileURLToPath(new URL("../package.json", import.meta.url,),), "utf-8",),
).engines.bun.replace(/^>=/, "",);

/** Whether dotted version `actual` is >= `minimum` (numeric per part). */
function versionAtLeast(actual, minimum,) {
	// Compare the release part only: canary builds print e.g. 1.4.3-canary.12+abc.
	const a = actual.split(/[-+]/,)[0].split(".",).map(Number,);
	const m = minimum.split(".",).map(Number,);
	for (let i = 0; i < m.length; i++) {
		if ((a[i] ?? 0) !== m[i]) return (a[i] ?? 0) > m[i];
	}
	return true;
}

if (!runningUnderBun) {
	// Check before spawning: an old Bun may reject --no-env-file during argument
	// parsing, before any launcher code could report a JSON error.
	const probe = spawnSync("bun", ["--version",], { encoding: "utf-8", },);
	const found = probe.error ? undefined : probe.stdout.trim();
	const result = found !== undefined && versionAtLeast(found, MINIMUM_BUN,)
		? spawnSync(
			"bun",
			["--no-env-file", fileURLToPath(import.meta.url,), ...process.argv.slice(2,),],
			{ stdio: "inherit", env: process.env, },
		)
		: {
			error: new Error(
				found ? `Bun ${found} is older than the required ${MINIMUM_BUN}.` : "bun not found",
			),
		};
	if (result.error) {
		process.stdout.write(`${
			JSON.stringify({
				type: "error",
				ok: false,
				error: `Unable to start the required Bun runtime: ${result.error.message}`,
				code: "internal_error",
				category: "internal",
				exitCode: 2,
				hint: found
					? `Update Bun to ${MINIMUM_BUN} or newer: run \`bun upgrade\`.`
					: `Install Bun ${MINIMUM_BUN} or newer (https://bun.sh) and ensure \`bun\` is on PATH.`,
			},)
		}\n`,);
		process.exitCode = 2;
	} else if (result.signal) {
		process.kill(process.pid, result.signal,);
	} else {
		process.exitCode = result.status ?? 1;
	}
} else if (!Bun.semver.satisfies(Bun.version, `>=${MINIMUM_BUN}`,)) {
	process.stdout.write(`${
		JSON.stringify({
			type: "error",
			ok: false,
			error: `Bun ${Bun.version} is older than the required ${MINIMUM_BUN}.`,
			code: "internal_error",
			category: "internal",
			exitCode: 2,
			hint: `Update Bun to ${MINIMUM_BUN} or newer: run \`bun upgrade\`.`,
		},)
	}\n`,);
	process.exitCode = 2;
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
	// A source checkout always runs its source, like bin/dss; a leftover dist/
	// would otherwise silently shadow every edit. The published package ships
	// no src/, so installs run dist.
	const usesDistCli = !(await Bun.file(sourceCliPath,).exists())
		&& await Bun.file(distCliPath,).exists();
	const cliPath = usesDistCli ? distCliPath : sourceCliPath;

	// Published dist carries dist/build-metadata.json with the revision it was
	// built from. Hand it to the CLI so provenance can report dist source and its
	// build revision; only a full lowercase hexadecimal revision is accepted, so an
	// inherited or corrupt variable never stands in for packaged metadata.
	if (usesDistCli) {
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
	} else {
		// Only a dist run may carry a build revision; never trust an inherited one.
		delete process.env.DSS_BUILD_REVISION;
	}

	// Same process, same argv: the CLI sees process.argv.slice(2) exactly as before.
	await import(pathToFileURL(cliPath,).href);
}
