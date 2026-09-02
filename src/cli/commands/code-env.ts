import { readFileSync, writeFileSync, } from "node:fs";
import { ClientValidationError, } from "../../errors.js";
import { stableHash, } from "../../utils/stable-hash.js";
import { json, jsonInput, num, numFlag, parseBooleanOption, } from "../coerce.js";
import { enqueueCliWarning, isNotFoundError, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const CODE_ENV_LANGS = ["PYTHON", "R",];

/** Validate a positional `<lang>` argument: required, must be PYTHON or R. */
function requireEnvLang(value: string | boolean | undefined, usage: string,): "PYTHON" | "R" {
	if (value !== "PYTHON" && value !== "R") {
		throw new UsageError(
			`env language must be one of ${CODE_ENV_LANGS.join(", ",)}. Usage: ${usage}`,
			"invalid_enum",
			undefined,
			{ value, allowed: CODE_ENV_LANGS, },
		);
	}
	return value;
}

function definitionHashMismatch(
	envLang: string,
	envName: string,
	expected: string,
	current: string | undefined,
): ClientValidationError {
	return new ClientValidationError(
		`Code env ${envLang}/${envName} changed since the expected definition hash was captured; refusing to overwrite it.`,
		"validation_failed",
		"Re-read the definition with dss code-env get-definition (definitionHash comes from code-env get), review the diff, and retry with the fresh hash.",
		{
			envLang,
			envName,
			expectedDefinitionHash: expected,
			...(current !== undefined ? { currentDefinitionHash: current, } : {}),
		},
	);
}

function validateExpectHash(value: string | boolean | undefined,): string | undefined {
	if (value === undefined || value === false) return undefined;
	if (typeof value !== "string" || !/^[0-9a-fA-F]{64}$/.test(value,)) {
		throw new ClientValidationError(
			"Expected code env definition hash must be a 64-character SHA-256 hex digest.",
			"validation_failed",
			"Use the definitionHash value returned by dss code-env get.",
		);
	}
	return value;
}

function codeEnvWait(flags: Record<string, string | boolean>,): boolean {
	return flags["no-wait"] !== true;
}

function codeEnvParams(flags: Record<string, string | boolean>,): Record<string, unknown> {
	const params = json(flags["params"],) ?? jsonInput(flags,) ?? {};
	if (typeof flags["python-interpreter"] === "string") {
		params.pythonInterpreter = flags["python-interpreter"];
	}
	return params;
}

function splitPackageSpec(raw: string,): string[] {
	return raw.split(/\r?\n/,).map((line,) => line.trim()).filter((line,) => line.length > 0);
}

function codeEnvPackageList(flags: Record<string, string | boolean>,): string[] {
	const packages: string[] = [];
	if (typeof flags["file"] === "string") {
		packages.push(...splitPackageSpec(readFileSync(flags["file"], "utf-8",),),);
	}
	if (typeof flags["packages"] === "string") {
		packages.push(...splitPackageSpec(flags["packages"],),);
	}
	if (typeof flags["package"] === "string") {
		packages.push(...splitPackageSpec(flags["package"],),);
	}
	if (packages.length === 0) {
		throw new UsageError(
			"--packages, --package, or --file is required. Use newline-separated package specs for version constraints.",
		);
	}
	return packages;
}

export const codeEnvCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => {
			const usage = "dss code-env list [--lang PYTHON|R]";
			const lang = f["lang"];
			return c.codeEnvs.list({
				envLang: typeof lang === "string"
					? requireEnvLang(lang, usage,)
					: undefined,
			},);
		},
		usage: "dss code-env list [--lang PYTHON|R]",
		description: "List code environments, optionally filtered to one language.",
		examples: ["dss code-env list", "dss code-env list --lang PYTHON",],
	},
	get: {
		handler: (c, a,) => {
			const usage = "dss code-env get <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			return c.codeEnvs.get(envLang, a[1],);
		},
		usage: "dss code-env get <lang> <name>",
		description:
			"Get code environment details: requested vs installed packages, Python interpreter, deployment mode, and the stable definition hash (definitionHash) used by set-definition --expect-hash.",
		examples: ["dss code-env get PYTHON my_env",],
	},
	"get-definition": {
		handler: (c, a,) => {
			const usage = "dss code-env get-definition <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			return c.codeEnvs.getDefinition(envLang, a[1],);
		},
		usage: "dss code-env get-definition <lang> <name>",
		description:
			"Get the raw code environment definition exactly as DSS stores it; capture its hash (definitionHash from code-env get) before replacing it with set-definition --expect-hash.",
		examples: ["dss code-env get-definition PYTHON my_env",],
	},
	"list-logs": {
		handler: (c, a,) => {
			const usage = "dss code-env list-logs <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			return c.codeEnvs.listLogs(envLang, a[1],);
		},
		usage: "dss code-env list-logs <lang> <name>",
		description: "List the build log files DSS keeps for a code environment.",
		examples: ["dss code-env list-logs PYTHON my_env",],
	},
	"get-log": {
		handler: async (c, a, f,) => {
			const usage =
				"dss code-env get-log <lang> <name> <logName> [--max-lines N|--max-log-lines N] [--max-log-bytes N] [--output PATH]";
			requireArgs(a, 3, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const maxLines = numFlag(f, ["max-lines", "max-log-lines",],);
			const maxBytes = num(f["max-log-bytes"], "--max-log-bytes",);
			const result = await c.codeEnvs.getLog(envLang, a[1], a[2], {
				...(maxLines !== undefined ? { maxLines, } : {}),
				...(maxBytes !== undefined ? { maxBytes, } : {}),
			},);
			const output = f["output"] as string | undefined;
			if (typeof output === "string" && output.trim().length > 0) {
				writeFileSync(output, result.log, "utf-8",);
				if (result.truncated || result.tailed) {
					const limits = [
						result.tailed ? `tail limited to ${maxLines ?? 500} lines` : undefined,
						result.truncated ? "byte cap reached" : undefined,
					].filter((part,) => part !== undefined);
					enqueueCliWarning({
						code: "code_env_log_truncated",
						message: `Code env ${envLang}/${a[1]} log '${a[2]}' was truncated (${
							limits.join("; ",)
						},). Pass --max-log-bytes 0 and --max-lines 0 for the full log, or refetch with higher limits.`,
						envLang,
						envName: a[1],
						logName: a[2],
						truncated: result.truncated,
						tailed: result.tailed,
					},);
				}
				return {
					path: output,
					bytes: result.bytes,
					truncated: result.truncated,
					tailed: result.tailed,
					envLang,
					envName: a[1],
					logName: a[2],
				};
			}
			return { ...result, envLang, envName: a[1], logName: a[2], };
		},
		usage:
			"dss code-env get-log <lang> <name> <logName> [--max-lines N|--max-log-lines N] [--max-log-bytes N] [--output PATH]",
		description:
			"Fetch one code-env build log, bounded for safe output: the last --max-lines lines (default 500, 0 for all) and at most --max-log-bytes bytes (default 10 MiB, 0 for all). --output PATH writes the kept content to a file; stdout then carries the path metadata. A truncation warning names which cap hit.",
		examples: [
			"dss code-env get-log PYTHON my_env install.log --max-lines 200",
			"dss code-env get-log PYTHON my_env install.log --output ./install.log --max-log-bytes 0",
		],
	},
	version: {
		handler: (c, a,) => {
			const usage = "dss code-env version <lang> <name> <projectKey>";
			requireArgs(a, 3, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			return c.codeEnvs.getVersionForProject(envLang, a[1], a[2],);
		},
		usage: "dss code-env version <lang> <name> <projectKey>",
		description:
			"Resolve the code environment version a project uses (versioned automation environments); empty version for unversioned environments.",
		examples: ["dss code-env version PYTHON my_env MY_PROJ",],
	},
	create: {
		handler: async (c, a, f,) => {
			const usage = "dss code-env create <lang> <name> --deployment-mode MODE";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const deploymentMode = f["deployment-mode"] as string | undefined;
			if (!deploymentMode) {
				throw new UsageError(
					"--deployment-mode is required. Usage: dss code-env create <lang> <name> --deployment-mode MODE",
				);
			}
			const params = codeEnvParams(f,);
			const wait = codeEnvWait(f,);
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = (await c.codeEnvs.list({ envLang, },))
					.find((env,) => env.envName === a[1]);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("code-env", a[1], "exists", { envLang, current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "code-env",
						envLang,
						envName: a[1],
						payload: {
							deploymentMode,
							params,
						},
						wait,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.codeEnvs.create({
				envLang,
				envName: a[1],
				deploymentMode,
				params,
				wait,
			},);
			return { created: a[1], resource: "code-env", envLang, ...created, };
		},
		usage:
			"dss code-env create <lang> <name> --deployment-mode MODE [--params JSON|--data JSON|--data-file PATH|--stdin] [--python-interpreter PYTHON311] [--no-wait] [--if-not-exists] [--dry-run]",
		description:
			"Create a code environment; --no-wait hands back a DSS future payload you can settle with dss future wait.",
		examples: [
			"dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --python-interpreter PYTHON311",
			'dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --params \'{"pythonInterpreter":"PYTHON311"}\' --dry-run',
		],
	},
	"set-definition": {
		handler: async (c, a, f,) => {
			const usage =
				"dss code-env set-definition <lang> <name> (--data JSON|--data-file PATH|--stdin) [--expect-hash SHA256]";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const definition = jsonInput(f,);
			if (!definition) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required. Usage: dss code-env set-definition <lang> <name> --data JSON",
				);
			}
			const expectHash = validateExpectHash(f["expect-hash"],);
			const definitionHash = stableHash(definition,);
			if (f["dry-run"] === true) {
				const current = await readIfExists(() => c.codeEnvs.getDefinition(envLang, a[1],));
				const currentHash = current ? stableHash(current,) : undefined;
				if (expectHash !== undefined && currentHash !== expectHash) {
					throw definitionHashMismatch(envLang, a[1], expectHash, currentHash,);
				}
				return {
					dryRun: true,
					action: "set-definition",
					resource: "code-env",
					envLang,
					envName: a[1],
					definition,
					definitionHash,
					...(currentHash !== undefined
						? { currentDefinitionHash: currentHash, changed: currentHash !== definitionHash, }
						: {}),
					...(expectHash !== undefined ? { expectHash, provenanceVerified: true, } : {}),
				};
			}
			return c.codeEnvs.setDefinition(envLang, a[1], definition, { expectHash, },);
		},
		usage:
			"dss code-env set-definition <lang> <name> (--data JSON|--data-file PATH|--stdin) [--expect-hash SHA256] [--dry-run]",
		description:
			"Replace a code environment definition previously fetched from DSS (get-definition). --expect-hash SHA256 refuses the PUT unless the current definition still hashes to the hash captured with code-env get, so concurrent edits are never clobbered; --dry-run also fetches the current definition and reports both hashes plus a changed flag.",
		examples: [
			"dss code-env set-definition PYTHON my_env --data-file code-env.json --expect-hash $HASH --dry-run",
		],
	},
	"set-packages": {
		handler: async (c, a, f,) => {
			const usage =
				"dss code-env set-packages <lang> <name> (--packages PKGS|--package PKG|--file PATH)";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const packages = codeEnvPackageList(f,);
			const installCorePackages = parseBooleanOption(
				f["install-core-packages"],
				"--install-core-packages",
			);
			const expectHash = validateExpectHash(f["expect-hash"],);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-packages",
					resource: "code-env",
					envLang,
					envName: a[1],
					packages,
					installCorePackages,
					...(expectHash !== undefined ? { expectHash, } : {}),
				};
			}
			return c.codeEnvs.setPackages(envLang, a[1], packages, { installCorePackages, expectHash, },);
		},
		usage:
			"dss code-env set-packages <lang> <name> (--packages PKGS|--package PKG|--file PATH) [--install-core-packages true|false] [--expect-hash SHA256] [--dry-run]",
		description:
			"Fetch the current definition, merge the requested package specs (and --install-core-packages) into it, then PUT the merged definition, so fields outside the package list are never dropped; --expect-hash refuses the merge when DSS changed since the hash was captured.",
		examples: [
			"dss code-env set-packages PYTHON my_env --packages 'tabulate\\nnameparser' --dry-run",
			"dss code-env set-packages PYTHON my_env --file requirements.txt",
		],
	},
	"update-packages": {
		handler: async (c, a, f,) => {
			const usage = "dss code-env update-packages <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const wait = codeEnvWait(f,);
			const versionToUpdate = typeof f["env-version"] === "string"
				? f["env-version"]
				: typeof f["version"] === "string"
				? f["version"]
				: undefined;
			const opts = {
				forceRebuildEnv: f["force-rebuild"] === true,
				versionToUpdate,
				wait,
			};
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "update-packages",
					resource: "code-env",
					envLang,
					envName: a[1],
					...opts,
				};
			}
			return c.codeEnvs.updatePackages(envLang, a[1], opts,);
		},
		usage:
			"dss code-env update-packages <lang> <name> [--force-rebuild] [--env-version VERSION] [--no-wait] [--dry-run]",
		description:
			"Rebuild or update code environment packages to match the requested specs; --no-wait hands back a DSS future payload you can settle with dss future wait.",
		examples: ["dss code-env update-packages PYTHON my_env --force-rebuild --dry-run",],
	},
	"update-images": {
		handler: async (c, a, f,) => {
			const usage = "dss code-env update-images <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const wait = codeEnvWait(f,);
			const envVersion = typeof f["env-version"] === "string" ? f["env-version"] : undefined;
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "update-images",
					resource: "code-env",
					envLang,
					envName: a[1],
					envVersion,
					wait,
				};
			}
			return c.codeEnvs.updateImages(envLang, a[1], { envVersion, wait, },);
		},
		usage: "dss code-env update-images <lang> <name> [--env-version VERSION] [--no-wait] [--dry-run]",
		description:
			"Rebuild the Docker image(s) of a code environment to match its settings; --env-version targets one version of a versioned environment and --no-wait hands back a DSS future payload you can settle with dss future wait.",
		examples: [
			"dss code-env update-images PYTHON my_env --dry-run",
			"dss code-env update-images PYTHON my_env --env-version v3 --no-wait",
		],
	},
	"set-jupyter": {
		handler: async (c, a, f,) => {
			const usage = "dss code-env set-jupyter <lang> <name> --active true|false";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const active = parseBooleanOption(f["active"], "--active",);
			if (active === undefined) {
				throw new UsageError(
					"--active is required. Usage: dss code-env set-jupyter <lang> <name> --active true|false",
				);
			}
			const wait = codeEnvWait(f,);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-jupyter",
					resource: "code-env",
					envLang,
					envName: a[1],
					active,
					wait,
				};
			}
			return c.codeEnvs.setJupyterSupport(envLang, a[1], active, { wait, },);
		},
		usage: "dss code-env set-jupyter <lang> <name> --active true|false [--no-wait] [--dry-run]",
		description:
			"Enable or disable Jupyter support for a code environment; --no-wait hands back a DSS future payload you can settle with dss future wait.",
		examples: ["dss code-env set-jupyter PYTHON my_env --active true --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			const usage = "dss code-env delete <lang> <name>";
			requireArgs(a, 2, usage,);
			const envLang = requireEnvLang(a[0], usage,);
			const wait = codeEnvWait(f,);
			if (f["dry-run"] === true) {
				const current = await readIfExists(() => c.codeEnvs.get(envLang, a[1],));
				if (!current) return skipResult("code-env", a[1], "missing", { envLang, },);
				return {
					dryRun: true,
					action: "delete",
					resource: "code-env",
					envLang,
					envName: a[1],
					wait,
					current,
				};
			}
			if (f["if-exists"] === true) {
				return await c.codeEnvs.delete(envLang, a[1], { wait, },).catch((error: unknown,) => {
					if (isNotFoundError(error,)) {
						return skipResult("code-env", a[1], "missing", { envLang, },);
					}
					throw error;
				},);
			}
			return c.codeEnvs.delete(envLang, a[1], { wait, },);
		},
		usage: "dss code-env delete <lang> <name> [--no-wait] [--if-exists] [--dry-run]",
		description:
			"Delete a code environment; --no-wait hands back a DSS future payload you can settle with dss future wait.",
		examples: ["dss code-env delete PYTHON my_env --dry-run",],
	},
	usages: {
		handler: (c, a,) => {
			if (a.length === 0) return c.codeEnvs.listUsages();
			if (a.length === 2) return c.codeEnvs.listUsages(a[0], a[1],);
			throw new UsageError("Usage: dss code-env usages [<lang> <name>]",);
		},
		usage: "dss code-env usages [<lang> <name>]",
		description: "List code environment usages globally or for one environment.",
		examples: ["dss code-env usages", "dss code-env usages PYTHON my_env",],
	},
};
