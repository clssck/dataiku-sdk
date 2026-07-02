import { readFileSync, } from "node:fs";
import { json, jsonInput, parseBooleanOption, } from "../coerce.js";
import { isNotFoundError, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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
		handler: (c, _a, f,) =>
			c.codeEnvs.list({
				envLang: f["lang"] as "PYTHON" | "R" | undefined,
			},),
		usage: "dss code-env list [--lang LANG]",
		description: "List code environments.",
		examples: ["dss code-env list", "dss code-env list --lang PYTHON",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 2, "dss code-env get <lang> <name>",);
			return c.codeEnvs.get(a[0], a[1],);
		},
		usage: "dss code-env get <lang> <name>",
		description: "Get code environment details.",
		examples: ["dss code-env get PYTHON my_env",],
	},
	"get-definition": {
		handler: (c, a,) => {
			requireArgs(a, 2, "dss code-env get-definition <lang> <name>",);
			return c.codeEnvs.getDefinition(a[0], a[1],);
		},
		usage: "dss code-env get-definition <lang> <name>",
		description: "Get raw code environment definition.",
		examples: ["dss code-env get-definition PYTHON my_env",],
	},
	create: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env create <lang> <name> --deployment-mode MODE",);
			const deploymentMode = f["deployment-mode"] as string | undefined;
			if (!deploymentMode) {
				throw new UsageError(
					"--deployment-mode is required. Usage: dss code-env create <lang> <name> --deployment-mode MODE",
				);
			}
			const params = codeEnvParams(f,);
			const wait = codeEnvWait(f,);
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const existing = (await c.codeEnvs.list({ envLang: a[0] as "PYTHON" | "R", },))
					.find((env,) => env.envName === a[1]);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("code-env", a[1], "exists", { envLang: a[0], current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "code-env",
						envLang: a[0],
						envName: a[1],
						payload: {
							deploymentMode,
							params,
							wait,
						},
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.codeEnvs.create({
				envLang: a[0],
				envName: a[1],
				deploymentMode,
				params,
				wait,
			},);
			return { created: a[1], resource: "code-env", envLang: a[0], ...created, };
		},
		usage:
			"dss code-env create <lang> <name> --deployment-mode MODE [--params JSON|--data JSON|--data-file PATH|--stdin] [--python-interpreter PYTHON311] [--no-wait] [--if-not-exists] [--dry-run]",
		description: "Create a code environment.",
		examples: [
			"dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --python-interpreter PYTHON311",
			'dss code-env create PYTHON my_env --deployment-mode DESIGN_MANAGED --params \'{"pythonInterpreter":"PYTHON311"}\' --dry-run',
		],
	},
	"set-definition": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env set-definition <lang> <name> --data JSON",);
			const definition = jsonInput(f,);
			if (!definition) {
				throw new UsageError(
					"--data, --data-file, or --stdin is required. Usage: dss code-env set-definition <lang> <name> --data JSON",
				);
			}
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-definition",
					resource: "code-env",
					envLang: a[0],
					envName: a[1],
					definition,
				};
			}
			return c.codeEnvs.setDefinition(a[0], a[1], definition,);
		},
		usage:
			"dss code-env set-definition <lang> <name> [--data JSON|--data-file PATH|--stdin] [--dry-run]",
		description: "Replace a code environment definition previously fetched from DSS.",
		examples: ["dss code-env set-definition PYTHON my_env --data-file code-env.json --dry-run",],
	},
	"set-packages": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env set-packages <lang> <name> --packages PKGS",);
			const packages = codeEnvPackageList(f,);
			const installCorePackages = parseBooleanOption(
				f["install-core-packages"],
				"--install-core-packages",
			);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-packages",
					resource: "code-env",
					envLang: a[0],
					envName: a[1],
					packages,
					installCorePackages,
				};
			}
			return c.codeEnvs.setPackages(a[0], a[1], packages, { installCorePackages, },);
		},
		usage:
			"dss code-env set-packages <lang> <name> [--packages PKGS|--package PKG|--file PATH] [--install-core-packages true|false] [--dry-run]",
		description: "Update requested package specs without rebuilding packages.",
		examples: [
			"dss code-env set-packages PYTHON my_env --packages 'tabulate\\nnameparser' --dry-run",
			"dss code-env set-packages PYTHON my_env --file requirements.txt",
		],
	},
	"update-packages": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env update-packages <lang> <name>",);
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
					envLang: a[0],
					envName: a[1],
					...opts,
				};
			}
			return c.codeEnvs.updatePackages(a[0], a[1], opts,);
		},
		usage:
			"dss code-env update-packages <lang> <name> [--force-rebuild] [--env-version VERSION] [--no-wait] [--dry-run]",
		description: "Rebuild or update code environment packages to match the requested specs.",
		examples: ["dss code-env update-packages PYTHON my_env --force-rebuild --dry-run",],
	},
	"set-jupyter": {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env set-jupyter <lang> <name> --active true|false",);
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
					envLang: a[0],
					envName: a[1],
					active,
					wait,
				};
			}
			return c.codeEnvs.setJupyterSupport(a[0], a[1], active, { wait, },);
		},
		usage: "dss code-env set-jupyter <lang> <name> --active true|false [--no-wait] [--dry-run]",
		description: "Enable or disable Jupyter support for a code environment.",
		examples: ["dss code-env set-jupyter PYTHON my_env --active true --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss code-env delete <lang> <name>",);
			const wait = codeEnvWait(f,);
			if (f["dry-run"] === true) {
				const current = await readIfExists(() => c.codeEnvs.get(a[0], a[1],));
				if (!current) return skipResult("code-env", a[1], "missing", { envLang: a[0], },);
				return {
					dryRun: true,
					action: "delete",
					resource: "code-env",
					envLang: a[0],
					envName: a[1],
					wait,
					current,
				};
			}
			if (f["if-exists"] === true) {
				return await c.codeEnvs.delete(a[0], a[1], { wait, },).catch((error: unknown,) => {
					if (isNotFoundError(error,)) {
						return skipResult("code-env", a[1], "missing", { envLang: a[0], },);
					}
					throw error;
				},);
			}
			return c.codeEnvs.delete(a[0], a[1], { wait, },);
		},
		usage: "dss code-env delete <lang> <name> [--no-wait] [--if-exists] [--dry-run]",
		description: "Delete a code environment.",
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
