#!/usr/bin/env node
import { spawnSync, } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath, pathToFileURL, } from "node:url";

const args = process.argv.slice(2,);
const optionArgs = args.includes("--",) ? args.slice(0, args.indexOf("--",),) : args;
const here = path.dirname(fileURLToPath(import.meta.url,),);
const distCliPath = path.resolve(here, "../dist/src/cli.js",);
const sourceCliPath = path.resolve(here, "../src/cli.ts",);
const cliPath = fs.existsSync(distCliPath,) ? distCliPath : sourceCliPath;
const cliUrl = pathToFileURL(cliPath,).href;

function flagValue(names,) {
	for (let i = 0; i < optionArgs.length; i++) {
		const arg = optionArgs[i];
		for (const name of names) {
			if (arg === name) return optionArgs[i + 1];
			if (arg.startsWith(`${name}=`,)) return arg.slice(name.length + 1,);
		}
	}
	return undefined;
}

function hasFlag(names,) {
	return names.some((name,) => optionArgs.includes(name,));
}

async function loadSavedTlsSettings() {
	try {
		const { loadCredentials, } = await import("../dist/src/config.js");
		const creds = loadCredentials();
		return {
			caCertPath: creds?.caCertPath,
			tlsRejectUnauthorized: creds?.tlsRejectUnauthorized,
		};
	} catch {
		return {};
	}
}

const savedTls = await loadSavedTlsSettings();
const env = { ...process.env, };
const explicitCaCert = flagValue(["--ca-cert", "--extra-ca-certs",],);
if (explicitCaCert) {
	env.NODE_EXTRA_CA_CERTS = explicitCaCert;
} else if (!env.NODE_EXTRA_CA_CERTS && savedTls.caCertPath) {
	env.NODE_EXTRA_CA_CERTS = savedTls.caCertPath;
}

if (hasFlag(["--insecure", "--skip-tls-verify",],)) {
	env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
} else if (
	env.NODE_TLS_REJECT_UNAUTHORIZED === undefined && savedTls.tlsRejectUnauthorized === false
) {
	env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}
const runningOnBun = Boolean(process.versions.bun,);
const usesSourceCli = cliPath === sourceCliPath;

// Published dist carries dist/build-metadata.json with the revision it was
// built from. Forward it to the CLI so provenance can report dist source and
// its build revision; only a full lowercase hexadecimal revision is accepted,
// so an inherited or corrupt variable never stands in for packaged metadata.
const distMetadataPath = path.resolve(here, "../dist/build-metadata.json",);
if (!usesSourceCli) {
	env.DSS_LOAD_SOURCE = "dist";
	delete env.DSS_BUILD_REVISION;
	try {
		const metadata = JSON.parse(fs.readFileSync(distMetadataPath, "utf-8",),);
		const revision = metadata.buildRevision;
		if (typeof revision === "string" && /^[0-9a-f]{40}$/.test(revision,)) {
			env.DSS_BUILD_REVISION = revision;
		}
	} catch {
		// Dist without build metadata: provenance still reports dist source.
	}
}

const runtimeBin = runningOnBun ? process.execPath : usesSourceCli ? "bun" : process.execPath;
const runtimeArgs = runningOnBun || usesSourceCli ? ["--no-env-file",] : ["--use-system-ca",];
const result = spawnSync(
	runtimeBin,
	[...runtimeArgs, cliPath, ...args,],
	{
		stdio: "inherit",
		env,
	},
);

if (result.error) {
	const message = usesSourceCli
		? `Unable to start Bun runtime for source dss CLI (${result.error.message}).`
		: `Unable to start ${
			runningOnBun ? "Bun" : "Node"
		} runtime for packaged dss CLI (${result.error.message}); falling back to the current runtime.`;
	if (usesSourceCli) {
		process.stdout.write(`${
			JSON.stringify({
				type: "error",
				ok: false,
				error: message,
				code: "internal_error",
				category: "internal",
				exitCode: 2,
			},)
		}\n`,);
		process.exitCode = 2;
	} else {
		await import(cliUrl);
	}
} else if (result.signal) {
	process.kill(process.pid, result.signal,);
} else {
	process.exit(result.status ?? 1,);
}
