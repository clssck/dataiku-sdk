#!/usr/bin/env node
import { spawnSync, } from "node:child_process";
import { existsSync, } from "node:fs";
import { dirname, resolve, } from "node:path";
import { fileURLToPath, pathToFileURL, } from "node:url";

const args = process.argv.slice(2,);
const here = dirname(fileURLToPath(import.meta.url,),);
const distCliPath = resolve(here, "../dist/src/cli.js",);
const sourceCliPath = resolve(here, "../src/cli.ts",);
const cliPath = existsSync(distCliPath,) ? distCliPath : sourceCliPath;
const cliUrl = pathToFileURL(cliPath,).href;

function flagValue(names,) {
	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		for (const name of names) {
			if (arg === name) return args[i + 1];
			if (arg.startsWith(`${name}=`,)) return arg.slice(name.length + 1,);
		}
	}
	return undefined;
}

function hasFlag(names,) {
	return names.some((name,) => args.includes(name,));
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

function supportsSystemCa(nodeBin,) {
	const probe = spawnSync(nodeBin, ["--use-system-ca", "-e", "",], { stdio: "ignore", },);
	return !probe.error && probe.status === 0;
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

const nodeBin = process.versions.bun ? "node" : process.execPath;
const usesSourceCli = cliPath === sourceCliPath;
const nodeArgs = !usesSourceCli && supportsSystemCa(nodeBin,) ? ["--use-system-ca",] : [];
const result = spawnSync(
	usesSourceCli ? "bun" : nodeBin,
	[...(usesSourceCli ? ["--no-env-file",] : nodeArgs), cliPath, ...args,],
	{
		stdio: "inherit",
		env,
	},
);

if (result.error) {
	const message = usesSourceCli
		? `Unable to start Bun runtime for source dss CLI (${result.error.message}).`
		: `Unable to start Node runtime for packaged dss CLI (${result.error.message}); falling back to current runtime without Node system CA bootstrap.`;
	if (usesSourceCli) {
		process.stderr.write(`${
			JSON.stringify(
				{
					ok: false,
					error: message,
					code: "internal_error",
					category: "internal",
					message,
					exitCode: 2,
				},
				null,
				2,
			)
		}\n`,);
		process.exit(2,);
	}
	await import(cliUrl);
} else if (result.signal) {
	process.kill(process.pid, result.signal,);
} else {
	process.exit(result.status ?? 1,);
}
