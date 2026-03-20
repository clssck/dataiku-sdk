#!/usr/bin/env node

import { readFileSync, } from "node:fs";
import { writeFile, } from "node:fs/promises";
import { dirname, resolve, } from "node:path";
import { createInterface, } from "node:readline";
import { Writable, } from "node:stream";
import { fileURLToPath, } from "node:url";
import { validateCredentials, } from "./auth.js";
import { DataikuClient, } from "./client.js";
import {
	deleteCredentials,
	getCredentialsPath,
	loadCredentials,
	maskApiKey,
	saveCredentials,
} from "./config.js";
import { DataikuError, } from "./errors.js";
import type { BuildMode, } from "./schemas.js";
import { AGENTS, detectAgents, installSkill, } from "./skill.js";

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

const CLI_VERSION: string = (() => {
	try {
		let dir = dirname(fileURLToPath(import.meta.url,),);
		for (let i = 0; i < 5; i++) {
			const candidate = resolve(dir, "package.json",);
			try {
				return (JSON.parse(readFileSync(candidate, "utf-8",),) as { version: string; }).version;
			} catch {
				dir = dirname(dir,);
			}
		}
		return "unknown";
	} catch {
		return "unknown";
	}
})();
function num(v: string | boolean | undefined,): number | undefined {
	if (typeof v !== "string") return undefined;
	const n = Number(v,);
	return Number.isFinite(n,) ? n : undefined;
}

function json(v: string | boolean | undefined,): Record<string, unknown> | undefined {
	if (typeof v !== "string") return undefined;
	return JSON.parse(v,) as Record<string, unknown>;
}

type OutputFormat = "json" | "quiet" | "table" | "tsv";

function jsonInput(flags: Record<string, string | boolean>,): Record<string, unknown> | undefined {
	if (flags["stdin"] === true) {
		return JSON.parse(readFileSync(0, "utf-8",),) as Record<string, unknown>;
	}
	if (typeof flags["data-file"] === "string") {
		return JSON.parse(readFileSync(flags["data-file"], "utf-8",),) as Record<string, unknown>;
	}
	if (typeof flags["data"] === "string") {
		return JSON.parse(flags["data"],) as Record<string, unknown>;
	}
	return undefined;
}

async function resolveFolderId(
	client: DataikuClient,
	nameOrId: string,
	flags: Record<string, string | boolean>,
): Promise<string> {
	return client.folders.resolveId(nameOrId, flags["project-key"] as string | undefined,);
}

function formatLineDiff(
	remoteName: string,
	localPath: string,
	remoteContent: string,
	localContent: string,
): string {
	if (localContent === remoteContent) {
		return "No differences.";
	}

	const localLines = localContent.split("\n",);
	const remoteLines = remoteContent.split("\n",);
	const lines: string[] = [`--- remote:${remoteName}`, `+++ local:${localPath}`, "",];
	const maxLen = Math.max(localLines.length, remoteLines.length,);

	for (let i = 0; i < maxLen; i++) {
		const remoteLine = remoteLines[i];
		const localLine = localLines[i];
		if (remoteLine === localLine) continue;

		if (remoteLine !== undefined && localLine !== undefined) {
			lines.push(`@@ line ${String(i + 1,)} @@`,);
			lines.push(`- ${remoteLine}`,);
			lines.push(`+ ${localLine}`,);
			continue;
		}

		if (remoteLine !== undefined) {
			lines.push(`- ${remoteLine}`,);
			continue;
		}

		lines.push(`+ ${localLine}`,);
	}

	return lines.join("\n",);
}

function parseOutputFormat(v: string | boolean | undefined,): OutputFormat {
	if (v === undefined) return "json";
	if (v === "json" || v === "quiet" || v === "table" || v === "tsv") return v;
	throw new UsageError(`Invalid --format value: ${String(v,)}. Use json, tsv, table, or quiet.`,);
}

function writeTable(items: Record<string, unknown>[],): void {
	if (items.length === 0) return;
	const keys = Object.keys(items[0],);
	const maxWidths = keys.map((k,) => {
		const values = items.map((item,) => String(item[k] ?? "",));
		return Math.min(40, Math.max(k.length, ...values.map((v,) => v.length),),);
	},);
	process.stdout.write(`${keys.map((k, i,) => k.padEnd(maxWidths[i],)).join("  ",)}\n`,);
	process.stdout.write(`${maxWidths.map((w,) => "-".repeat(w,)).join("  ",)}\n`,);
	for (const item of items) {
		const row = keys.map((k, i,) => {
			const val = String(item[k] ?? "",);
			return (val.length > maxWidths[i]
				? `${val.slice(0, maxWidths[i] - 1,)}\u2026`
				: val).padEnd(maxWidths[i],);
		},);
		process.stdout.write(`${row.join("  ",)}\n`,);
	}
}

function writeCommandResult(result: unknown, format: OutputFormat,): void {
	if (result === undefined || result === null) {
		if (format !== "quiet") {
			process.stdout.write(`${JSON.stringify({ ok: true, }, null, 2,)}\n`,);
		}
		return;
	}
	if (typeof result === "string") {
		if (format !== "quiet") {
			process.stdout.write(result,);
			if (!result.endsWith("\n",)) process.stdout.write("\n",);
		}
		return;
	}
	if (format === "quiet") return;
	const isArrayOfObjects = Array.isArray(result,)
		&& result.every((item,) => item !== null && typeof item === "object" && !Array.isArray(item,));
	if (format === "tsv" && isArrayOfObjects) {
		const items = result as Record<string, unknown>[];
		if (items.length === 0) return;
		const keys = Object.keys(items[0],);
		process.stdout.write(`${keys.join("\t",)}\n`,);
		for (const item of items) {
			process.stdout.write(`${keys.map((key,) => String(item[key] ?? "",)).join("\t",)}\n`,);
		}
		return;
	}
	if (format === "table" && isArrayOfObjects) {
		writeTable(result as Record<string, unknown>[],);
		return;
	}
	process.stdout.write(`${JSON.stringify(result, null, 2,)}\n`,);
}

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

const BOOLEAN_FLAGS = new Set(["help", "verbose", "version", "stdin", "global", "list-agents",],);

const SHORT_FLAGS: Record<string, string> = {
	h: "help",
	v: "verbose",
	V: "version",
	f: "format",
	o: "output",
};

interface ParsedArgs {
	positional: string[];
	flags: Record<string, string | boolean>;
}

function parseArgs(argv: string[],): ParsedArgs {
	const positional: string[] = [];
	const flags: Record<string, string | boolean> = {};
	let i = 0;
	while (i < argv.length) {
		const arg = argv[i];
		if (arg === "--") {
			positional.push(...argv.slice(i + 1,),);
			break;
		}
		if (arg.startsWith("--",)) {
			const eqIdx = arg.indexOf("=",);
			if (eqIdx !== -1) {
				flags[arg.slice(2, eqIdx,)] = arg.slice(eqIdx + 1,);
			} else {
				const flagName = arg.slice(2,);
				if (BOOLEAN_FLAGS.has(flagName,)) {
					flags[flagName] = true;
				} else {
					const next = argv[i + 1];
					if (next !== undefined && !next.startsWith("-",)) {
						flags[flagName] = next;
						i++;
					} else {
						flags[flagName] = true;
					}
				}
			}
		} else if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long) {
				if (BOOLEAN_FLAGS.has(long,)) {
					flags[long] = true;
				} else {
					const next = argv[i + 1];
					if (next !== undefined && !next.startsWith("-",)) {
						flags[long] = next;
						i++;
					} else {
						flags[long] = true;
					}
				}
			} else {
				positional.push(arg,);
			}
		} else {
			positional.push(arg,);
		}
		i++;
	}
	return { positional, flags, };
}

// ---------------------------------------------------------------------------
// Command registry
// ---------------------------------------------------------------------------

type CommandHandler = (
	client: DataikuClient,
	args: string[],
	flags: Record<string, string | boolean>,
) => Promise<unknown>;

interface CommandMeta {
	handler: CommandHandler;
	usage: string;
}

const commands: Record<string, Record<string, CommandMeta>> = {
	project: {
		list: {
			handler: (c,) => c.projects.list(),
			usage: "dss project list",
		},
		get: {
			handler: (c, _a, f,) => c.projects.get(f["project-key"] as string | undefined,),
			usage: "dss project get [--project-key KEY]",
		},
		metadata: {
			handler: (c, _a, f,) => c.projects.metadata(f["project-key"] as string | undefined,),
			usage: "dss project metadata [--project-key KEY]",
		},
		flow: {
			handler: (c, _a, f,) => c.projects.flow(f["project-key"] as string | undefined,),
			usage: "dss project flow [--project-key KEY]",
		},
		map: {
			handler: (c, _a, f,) =>
				c.projects.map({
					maxNodes: num(f["max-nodes"],),
					maxEdges: num(f["max-edges"],),
					includeRaw: f["include-raw"] === true,
				},),
			usage: "dss project map [--max-nodes N] [--max-edges N] [--include-raw]",
		},
	},

	dataset: {
		list: {
			handler: (c, _a, f,) => c.datasets.list(f["project-key"] as string | undefined,),
			usage: "dss dataset list [--project-key KEY]",
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset get <name>",);
				return c.datasets.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset get <name> [--project-key KEY]",
		},
		schema: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset schema <name>",);
				return c.datasets.schema(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset schema <name> [--project-key KEY]",
		},
		preview: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset preview <name>",);
				return c.datasets.preview(a[0], {
					maxRows: num(f["max-rows"],),
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss dataset preview <name> [--max-rows N] [--project-key KEY]",
		},
		metadata: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset metadata <name>",);
				return c.datasets.metadata(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset metadata <name> [--project-key KEY]",
		},
		download: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset download <name>",);
				return c.datasets.download(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss dataset download <name> [--output PATH] [--project-key KEY]",
		},
		create: {
			handler: (c, _a, f,) =>
				c.datasets.create({
					datasetName: f["name"] as string,
					connection: f["connection"] as string,
					dsType: f["type"] as string,
					projectKey: f["project-key"] as string | undefined,
				},),
			usage: "dss dataset create --name NAME --connection CONN --type TYPE [--project-key KEY]",
		},
		delete: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset delete <name>",);
				return c.datasets.delete(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset delete <name> [--project-key KEY]",
		},
		update: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				return c.datasets.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage:
				"dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
		},
	},

	recipe: {
		list: {
			handler: (c, _a, f,) => c.recipes.list(f["project-key"] as string | undefined,),
			usage: "dss recipe list [--project-key KEY]",
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe get <name>",);
				return c.recipes.get(a[0], {
					includePayload: f["include-payload"] === true,
				},);
			},
			usage: "dss recipe get <name> [--include-payload]",
		},
		delete: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe delete <name>",);
				return c.recipes.delete(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss recipe delete <name> [--project-key KEY]",
		},
		download: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe download <name>",);
				return c.recipes.download(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe download <name> [--output PATH] [--project-key KEY]",
		},
		"download-code": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe download-code <name>",);
				return c.recipes.downloadCode(a[0], {
					outputPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe download-code <name> [--output PATH] [--project-key KEY]",
		},
		create: {
			handler: (c, _a, f,) => {
				const type = f["type"] as string;
				if (!type) {
					throw new UsageError(
						"--type is required. Usage: dss recipe create --type TYPE --input DS --output DS",
					);
				}
				const outputDataset = f["output"] as string | undefined;
				if (!outputDataset) {
					throw new UsageError(
						"--output is required. Usage: dss recipe create --type TYPE --input DS --output DS",
					);
				}
				return c.recipes.create({
					type,
					name: f["name"] as string | undefined,
					inputDatasets: f["input"] ? [f["input"] as string,] : undefined,
					outputDataset,
					outputConnection: f["output-connection"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss recipe create --type TYPE --input DS --output DS [--output-connection CONN] [--project-key KEY]",
		},
		diff: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe diff <name> --file PATH",);
				const filePath = f["file"] as string | undefined;
				if (!filePath) {
					throw new UsageError("--file is required. Usage: dss recipe diff <name> --file PATH",);
				}
				const result = await c.recipes.get(a[0], {
					includePayload: true,
					projectKey: f["project-key"] as string | undefined,
				},);
				if (!result.payload) {
					throw new Error(`Recipe "${a[0]}" has no code payload to diff.`,);
				}
				const localContent = readFileSync(filePath, "utf-8",);
				return formatLineDiff(a[0], filePath, result.payload, localContent,);
			},
			usage: "dss recipe diff <name> --file PATH [--project-key KEY]",
		},

		update: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				return c.recipes.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage:
				"dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
		},
		"get-payload": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe get-payload <name>",);
				const payload = await c.recipes.getPayload(a[0], {
					projectKey: f["project-key"] as string | undefined,
				},);
				if (typeof f["output"] === "string") {
					await writeFile(f["output"], payload, "utf-8",);
					return f["output"];
				}
				return payload;
			},
			usage: "dss recipe get-payload <name> [--output PATH] [--project-key KEY]",
		},
		"set-payload": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe set-payload <name> --file PATH",);
				const filePath = f["file"] as string;
				if (!filePath) throw new UsageError("--file is required.",);
				const content = readFileSync(filePath, "utf-8",);
				await c.recipes.setPayload(a[0], content, {
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss recipe set-payload <name> --file PATH [--project-key KEY]",
		},
	},

	job: {
		list: {
			handler: (c, _a, f,) => c.jobs.list(f["project-key"] as string | undefined,),
			usage: "dss job list [--project-key KEY]",
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job get <id>",);
				return c.jobs.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss job get <id> [--project-key KEY]",
		},
		log: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job log <id>",);
				return c.jobs.log(a[0], {
					activity: f["activity"] as string | undefined,
					maxLogLines: num(f["max-lines"],),
				},);
			},
			usage: "dss job log <id> [--activity NAME] [--max-lines N]",
		},
		build: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job build <dataset>",);
				return c.jobs.build(a[0], {
					buildMode: f["build-mode"] as BuildMode | undefined,
				},);
			},
			usage: "dss job build <dataset> [--build-mode MODE]",
		},
		"build-and-wait": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job build-and-wait <dataset>",);
				return c.jobs.buildAndWait(a[0], {
					buildMode: f["build-mode"] as BuildMode | undefined,
					includeLogs: f["include-logs"] === true,
					timeoutMs: num(f["timeout"],),
				},);
			},
			usage: "dss job build-and-wait <dataset> [--build-mode MODE] [--include-logs] [--timeout MS]",
		},
		wait: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job wait <id>",);
				return c.jobs.wait(a[0], {
					includeLogs: f["include-logs"] === true,
					timeoutMs: num(f["timeout"],),
				},);
			},
			usage: "dss job wait <id> [--include-logs] [--timeout MS]",
		},
		abort: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job abort <id>",);
				return c.jobs.abort(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss job abort <id> [--project-key KEY]",
		},
	},

	scenario: {
		list: {
			handler: (c, _a, f,) => c.scenarios.list(f["project-key"] as string | undefined,),
			usage: "dss scenario list [--project-key KEY]",
		},
		get: {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss scenario get <id>",);
				return c.scenarios.get(a[0],);
			},
			usage: "dss scenario get <id>",
		},
		run: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario run <id>",);
				return c.scenarios.run(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario run <id> [--project-key KEY]",
		},
		status: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario status <id>",);
				return c.scenarios.status(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario status <id> [--project-key KEY]",
		},
		delete: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario delete <id>",);
				return c.scenarios.delete(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario delete <id> [--project-key KEY]",
		},
		create: {
			handler: (c, a, f,) => {
				requireArgs(a, 2, "dss scenario create <id> <name>",);
				return c.scenarios.create(a[0], a[1], {
					scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss scenario create <id> <name> [--type step_based|custom_python] [--project-key KEY]",
		},
		update: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				return c.scenarios.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage:
				"dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
		},
	},

	folder: {
		list: {
			handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
			usage: "dss folder list [--project-key KEY]",
		},
		get: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder get <name-or-id>",);
				return c.folders.get(
					await resolveFolderId(c, a[0], f,),
					f["project-key"] as string | undefined,
				);
			},
			usage: "dss folder get <name-or-id> [--project-key KEY]",
		},
		contents: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder contents <name-or-id>",);
				return c.folders.contents(await resolveFolderId(c, a[0], f,), {
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss folder contents <name-or-id> [--project-key KEY]",
		},
		download: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder download <name-or-id> <path>",);
				return c.folders.download(await resolveFolderId(c, a[0], f,), a[1], {
					localPath: f["output"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss folder download <name-or-id> <path> [--output PATH] [--project-key KEY]",
		},
		upload: {
			handler: async (c, a, f,) => {
				requireArgs(a, 3, "dss folder upload <name-or-id> <path> <localPath>",);
				return c.folders.upload(
					await resolveFolderId(c, a[0], f,),
					a[1],
					a[2],
					f["project-key"] as string | undefined,
				);
			},
			usage: "dss folder upload <name-or-id> <path> <localPath> [--project-key KEY]",
		},
		"delete-file": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder delete-file <name-or-id> <path>",);
				return c.folders.deleteFile(
					await resolveFolderId(c, a[0], f,),
					a[1],
					f["project-key"] as string | undefined,
				);
			},
			usage: "dss folder delete-file <name-or-id> <path> [--project-key KEY]",
		},
	},

	variable: {
		get: {
			handler: (c, _a, f,) => c.variables.get(f["project-key"] as string | undefined,),
			usage: "dss variable get [--project-key KEY]",
		},
		set: {
			handler: (c, _a, f,) =>
				c.variables.set({
					standard: json(f["standard"],),
					local: json(f["local"],),
					replace: f["replace"] === true,
					projectKey: f["project-key"] as string | undefined,
				},),
			usage:
				'dss variable set --standard \'{"k":"v"}\' --local \'{"k":"v"}\' [--replace] [--project-key KEY]',
		},
	},

	connection: {
		list: {
			handler: (c,) => c.connections.list(),
			usage: "dss connection list",
		},
		infer: {
			handler: (c, _a, f,) =>
				c.connections.infer({
					mode: f["mode"] as "fast" | "rich" | undefined,
				},),
			usage: "dss connection infer [--mode fast|rich]",
		},
	},

	"code-env": {
		list: {
			handler: (c, _a, f,) =>
				c.codeEnvs.list({
					envLang: f["lang"] as "PYTHON" | "R" | undefined,
				},),
			usage: "dss code-env list [--lang LANG]",
		},
		get: {
			handler: (c, a,) => {
				requireArgs(a, 2, "dss code-env get <lang> <name>",);
				return c.codeEnvs.get(a[0], a[1],);
			},
			usage: "dss code-env get <lang> <name>",
		},
	},
	sql: {
		query: {
			handler: (c, _a, f,) => {
				const query = f["sql"] as string;
				if (!query) throw new UsageError("--sql is required. Usage: dss sql query --sql 'SELECT ...'",);
				return c.sql.query({
					query,
					connection: f["connection"] as string | undefined,
					datasetFullName: f["dataset"] as string | undefined,
					database: f["database"] as string | undefined,
				},);
			},
			usage:
				"dss sql query --sql 'SELECT ...' [--connection CONN] [--dataset FULL_NAME] [--database DB]",
		},
	},
	notebook: {
		"list-jupyter": {
			handler: (c, _a, f,) => c.notebooks.listJupyter(f["project-key"] as string | undefined,),
			usage: "dss notebook list-jupyter [--project-key KEY]",
		},
		"get-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook get-jupyter <name>",);
				return c.notebooks.getJupyter(a[0],);
			},
			usage: "dss notebook get-jupyter <name>",
		},
		"delete-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook delete-jupyter <name>",);
				return c.notebooks.deleteJupyter(a[0],);
			},
			usage: "dss notebook delete-jupyter <name>",
		},
		"clear-jupyter-outputs": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook clear-jupyter-outputs <name>",);
				return c.notebooks.clearJupyterOutputs(a[0],);
			},
			usage: "dss notebook clear-jupyter-outputs <name>",
		},
		"sessions-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook sessions-jupyter <name>",);
				return c.notebooks.listJupyterSessions(a[0],);
			},
			usage: "dss notebook sessions-jupyter <name>",
		},
		"unload-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 2, "dss notebook unload-jupyter <name> <sessionId>",);
				return c.notebooks.unloadJupyter(a[0], a[1],);
			},
			usage: "dss notebook unload-jupyter <name> <sessionId>",
		},
		"list-sql": {
			handler: (c, _a, f,) => c.notebooks.listSql(f["project-key"] as string | undefined,),
			usage: "dss notebook list-sql [--project-key KEY]",
		},
		"get-sql": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook get-sql <id>",);
				return c.notebooks.getSql(a[0],);
			},
			usage: "dss notebook get-sql <id>",
		},
		"delete-sql": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook delete-sql <id>",);
				return c.notebooks.deleteSql(a[0],);
			},
			usage: "dss notebook delete-sql <id>",
		},
		"history-sql": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook history-sql <id>",);
				return c.notebooks.getSqlHistory(a[0],);
			},
			usage: "dss notebook history-sql <id>",
		},
		"save-jupyter": {
			handler: (c, a, f,) => {
				requireArgs(
					a,
					1,
					"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin]",
				);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required (notebook JSON content).",
					);
				}
				return c.notebooks.saveJupyter(a[0], data as never, f["project-key"] as string | undefined,);
			},
			usage:
				"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
		},
		"save-sql": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required (SQL notebook content JSON).",
					);
				}
				return c.notebooks.saveSql(a[0], data as never, f["project-key"] as string | undefined,);
			},
			usage:
				"dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
		},
		"clear-sql-history": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook clear-sql-history <id>",);
				return c.notebooks.clearSqlHistory(a[0], {
					cellId: f["cell-id"] as string | undefined,
					numRunsToRetain: num(f["retain"],),
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss notebook clear-sql-history <id> [--cell-id CID] [--retain N] [--project-key KEY]",
		},
	},
};

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

const RESOURCE_NAMES = [...Object.keys(commands,), "auth", "install-skill",].sort();

function printTopLevelHelp(): void {
	const lines = [
		"Usage: dss <resource> <action> [args...] [--flags]",
		"",
		"Global flags:",
		"  -h, --help               Show help",
		"  -v, --verbose            Log HTTP requests to stderr",
		"  -V, --version            Show version",
		"  -f, --format FORMAT      Output format: json|tsv|table|quiet",
		"  -o, --output PATH        Write output to file (recipe get-payload)",
		"      --url URL            Dataiku DSS base URL (env: DATAIKU_URL)",
		"      --api-key KEY        API key              (env: DATAIKU_API_KEY)",
		"      --project-key KEY    Default project key   (env: DATAIKU_PROJECT_KEY)",
		"      --timeout MS         Request timeout in ms  (default: 30000)",
		"",
		"Resources:",
		...RESOURCE_NAMES.map((r,) => `  ${r}`),
		"",
		"Quick start:",
		"  dss auth login                         Save DSS credentials",
		"  dss auth status                        Verify connection",
		"  dss project list                       List accessible projects",
		"  dss dataset list                       List datasets in default project",
		"  dss dataset preview <name>             Preview dataset rows as CSV",
		"  dss recipe get-payload <name>          Print recipe code to stdout",
		"  dss recipe download-code <name>        Download recipe code to a file",
		"  dss job log <id>                       View job log output",
		"  dss install-skill                      Install agent skill for coding agents",
	];
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

function printResourceHelp(resource: string,): void {
	const actions = commands[resource];
	if (!actions) return;
	const lines = [
		`Usage: dss ${resource} <action> [args...] [--flags]`,
		"",
		"Actions:",
		...Object.entries(actions,).map(([name, meta,],) => `  ${name}  →  ${meta.usage}`),
	];
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

function printActionHelp(resource: string, action: string,): void {
	const meta = commands[resource]?.[action];
	if (!meta) return;
	process.stderr.write(`Usage: ${meta.usage}\n`,);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

class UsageError extends Error {
	constructor(message: string,) {
		super(message,);
		this.name = "UsageError";
	}
}

function requireArgs(args: string[], count: number, usage: string,): void {
	if (args.length < count) {
		throw new UsageError(`Expected ${count} argument(s), got ${args.length}.\nUsage: ${usage}`,);
	}
}

// ---------------------------------------------------------------------------
// .env auto-loading
// ---------------------------------------------------------------------------

function loadEnvFile(): void {
	const dirs = [
		resolve(dirname(fileURLToPath(import.meta.url,),), "..",),
		process.cwd(),
	];
	for (const dir of dirs) {
		try {
			const content = readFileSync(resolve(dir, ".env",), "utf-8",);
			for (const line of content.split("\n",)) {
				const trimmed = line.trim();
				if (!trimmed || trimmed.startsWith("#",)) continue;
				const eq = trimmed.indexOf("=",);
				if (eq === -1) continue;
				const key = trimmed.slice(0, eq,).trim();
				const val = trimmed.slice(eq + 1,).trim().replace(/^['"]|['"]$/g, "",);
				if (!process.env[key]) process.env[key] = val;
			}
		} catch {
			// no .env file — fine
		}
	}
}

// ---------------------------------------------------------------------------
// Auth commands (run before client creation)
// ---------------------------------------------------------------------------

const AUTH_ACTIONS: Record<string, {
	handler: (flags: Record<string, string | boolean>,) => Promise<void>;
	usage: string;
}> = {
	login: {
		handler: async (flags,) => {
			let { url, apiKey, projectKey, } = resolveCredentials(flags,);

			if (!url || !apiKey) {
				if (!process.stdin.isTTY) {
					throw new UsageError(
						"Missing --url and/or --api-key. Provide them as flags or run interactively.",
					);
				}
				if (!url) url = await promptLine("DSS URL: ",);
				if (!apiKey) apiKey = await promptSecret("API key: ",);
				if (!projectKey) projectKey = (await promptLine("Project key (optional): ",)) || undefined;
			}

			if (!url) throw new UsageError("URL is required.",);
			if (!apiKey) throw new UsageError("API key is required.",);
			process.stderr.write("Validating credentials... ",);
			const result = await validateCredentials(url, apiKey,);
			if (!result.valid) {
				process.stderr.write(`✗ Failed\n`,);
				throw new DataikuError(
					0,
					"Authentication Failed",
					result.error ?? "Credential validation failed",
				);
			}
			process.stderr.write("\u2713 Connected\n",);

			saveCredentials({ url, apiKey, projectKey, },);
			process.stderr.write(`Credentials saved to ${getCredentialsPath()}\n`,);
		},
		usage: "dss auth login [--url URL] [--api-key KEY] [--project-key KEY]",
	},
	status: {
		handler: async (_flags,) => {
			const creds = loadCredentials();
			if (!creds) {
				process.stderr.write("No saved credentials. Run: dss auth login\n",);
				return;
			}
			const lines = [
				`URL:         ${creds.url}`,
				`API key:     ${maskApiKey(creds.apiKey,)}`,
				`Project key: ${creds.projectKey ?? "(not set)"}`,
			];
			for (const line of lines) process.stderr.write(`${line}\n`,);

			const result = await validateCredentials(creds.url, creds.apiKey,);
			if (result.valid) {
				process.stderr.write("Connection:  \u2713 Valid\n",);
			} else {
				process.stderr.write(`Connection:  \u2717 Failed (${result.error ?? "unknown error"})\n`,);
			}
			process.stderr.write(`Config:      ${getCredentialsPath()}\n`,);
		},
		usage: "dss auth status",
	},
	logout: {
		handler: async (_flags,) => {
			deleteCredentials();
			process.stderr.write("Credentials removed.\n",);
		},
		usage: "dss auth logout",
	},
};

// ---------------------------------------------------------------------------
// Interactive prompts
// ---------------------------------------------------------------------------

function promptLine(label: string,): Promise<string> {
	return new Promise((res, rej,) => {
		const rl = createInterface({ input: process.stdin, output: process.stderr, },);
		rl.on("close", () => rej(new UsageError("Input closed before a value was provided.",),),);
		rl.question(label, (answer,) => {
			rl.close();
			res(answer.trim(),);
		},);
	},);
}

function promptSecret(label: string,): Promise<string> {
	return new Promise((res, rej,) => {
		const muted = new Writable({
			write(_chunk, _encoding, cb,) {
				cb();
			},
		},);
		const rl = createInterface({ input: process.stdin, output: muted, terminal: true, },);
		rl.on("close", () => rej(new UsageError("Input closed before a value was provided.",),),);
		process.stderr.write(label,);
		rl.question("", (answer,) => {
			rl.close();
			process.stderr.write("\n",);
			res(answer.trim(),);
		},);
	},);
}

// ---------------------------------------------------------------------------
// Credential resolution
// ---------------------------------------------------------------------------

function resolveCredentials(flags: Record<string, string | boolean>,): {
	url: string;
	apiKey: string;
	projectKey?: string;
} {
	let url = flags["url"] as string | undefined;
	let apiKey = flags["api-key"] as string | undefined;
	let projectKey = flags["project-key"] as string | undefined;

	url ??= process.env.DATAIKU_URL;
	apiKey ??= process.env.DATAIKU_API_KEY;
	projectKey ??= process.env.DATAIKU_PROJECT_KEY;

	if (!url || !apiKey) {
		const saved = loadCredentials();
		if (saved) {
			url ||= saved.url;
			apiKey ||= saved.apiKey;
			projectKey ??= saved.projectKey;
		}
	}

	return { url: url ?? "", apiKey: apiKey ?? "", projectKey, };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	loadEnvFile();
	const { positional, flags, } = parseArgs(process.argv.slice(2,),);

	// --version
	if (flags["version"] === true) {
		process.stdout.write(`${CLI_VERSION}\n`,);
		process.exit(0,);
	}

	// Top-level help
	if (positional.length === 0 || (positional.length === 0 && flags["help"])) {
		printTopLevelHelp();
		if (flags["help"]) process.exit(0,);
		process.exit(1,);
	}

	const resource = positional[0];

	// Auth commands — dispatched before client creation
	if (resource === "auth") {
		const action = positional[1];
		if (!action || flags["help"] === true) {
			const lines = [
				"Usage: dss auth <action> [--flags]",
				"",
				"Actions:",
				...Object.entries(AUTH_ACTIONS,).map(
					([name, meta,],) => `  ${name}  \u2192  ${meta.usage}`,
				),
			];
			process.stderr.write(`${lines.join("\n",)}\n`,);
			process.exit(flags["help"] === true ? 0 : 1,);
		}
		const authMeta = AUTH_ACTIONS[action];
		if (!authMeta) {
			process.stderr.write(
				`Unknown action: auth ${action}\nAvailable: ${Object.keys(AUTH_ACTIONS,).join(", ",)}\n`,
			);
			process.exit(1,);
		}
		await authMeta.handler(flags,);
		return;
	}

	// install-skill — dispatched before client creation
	if (resource === "install-skill") {
		if (flags["help"] === true) {
			const lines = [
				"Usage: dss install-skill [--global] [--agent NAME] [--list-agents]",
				"",
				"Install the dataiku-dss agent skill for detected coding agents.",
				"",
				"Flags:",
				"  --global         Install to user-level global scope (default: project)",
				"  --agent NAME     Target a specific agent: claude, codex, cursor, pi, omp",
				"  --list-agents    Print detected agents and exit",
			];
			process.stderr.write(`${lines.join("\n",)}\n`,);
			process.exit(0,);
		}

		const listOnly = flags["list-agents"] === true;
		const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
		const isGlobal = flags["global"] === true;

		// Resolve target agents
		let targets;
		if (agentFilter) {
			const def = AGENTS[agentFilter];
			if (!def) {
				throw new UsageError(
					`Unknown agent: ${agentFilter}. Available: ${Object.keys(AGENTS,).join(", ",)}`,
				);
			}
			targets = [{ id: agentFilter, def, via: "flag" as const, },];
		} else {
			targets = detectAgents();
		}

		if (listOnly) {
			if (targets.length === 0) {
				process.stderr.write("No coding agents detected.\n",);
			} else {
				process.stderr.write("Detected agents:\n",);
				for (const t of targets) {
					process.stderr.write(`  ${t.id}  (${t.def.name}, via ${t.via})\n`,);
				}
			}
			process.exit(0,);
		}

		if (targets.length === 0) {
			throw new UsageError(
				"No coding agents detected. Install one (claude, codex, cursor, pi, omp) or use --agent NAME.",
			);
		}

		const scope = isGlobal ? "global" : "project";
		process.stderr.write(`Installing dataiku-dss skill (${scope} scope):\n`,);
		const results = installSkill(targets, { global: isGlobal, cwd: process.cwd(), },);

		for (const r of results) {
			process.stderr.write(`  ${r.agent}  \u2192  ${r.path}\n`,);
		}
		if (results.length > 0) {
			process.stderr.write(`\nDone. ${results.length} skill(s) installed.\n`,);
		}
		return;
	}

	// Unknown resource
	if (!commands[resource]) {
		if (flags["help"]) {
			printTopLevelHelp();
			process.exit(0,);
		}
		process.stderr.write(
			`Unknown resource: ${resource} \nAvailable: ${RESOURCE_NAMES.join(", ",)} \n`,
		);
		process.exit(1,);
	}

	// Resource-level help
	if (positional.length === 1 || flags["help"] === true) {
		if (positional.length === 1) {
			printResourceHelp(resource,);
			if (flags["help"]) process.exit(0,);
			process.exit(1,);
		}
	}

	const action = positional[1];
	const actionMeta = commands[resource][action];

	// Unknown action
	if (!actionMeta) {
		process.stderr.write(
			`Unknown action: ${resource} ${action} \nAvailable actions for ${resource}: ${
				Object.keys(commands[resource],).join(", ",)
			} \n`,
		);
		process.exit(1,);
	}

	// Action-level help
	if (flags["help"] === true) {
		printActionHelp(resource, action,);
		process.exit(0,);
	}

	// Resolve credentials: flags > env > saved > .env
	const { url, apiKey, projectKey, } = resolveCredentials(flags,);

	if (!url) {
		throw new UsageError("Missing Dataiku URL. Set DATAIKU_URL, pass --url, or run: dss auth login",);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY, pass --api-key, or run: dss auth login",
		);
	}

	const requestTimeoutMs = num(flags["timeout"],) ?? undefined;

	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
	},);

	const args = positional.slice(2,);
	const format = parseOutputFormat(flags["format"],);
	const result = await actionMeta.handler(client, args, flags,);
	writeCommandResult(result, format,);
}

main().catch((err: unknown,) => {
	if (err instanceof UsageError) {
		process.stderr.write(`${err.message} \n`,);
		process.exit(1,);
	}
	if (err instanceof DataikuError) {
		const payload: Record<string, unknown> = {
			error: err.message,
			category: err.category,
			retryable: err.retryable,
		};
		if (err.retryHint) payload.retryHint = err.retryHint;
		process.stderr.write(`${JSON.stringify(payload, null, 2,)} \n`,);
		process.exit(err.category === "transient" ? 3 : 2,);
	}
	const message = err instanceof Error ? err.message : String(err,);
	process.stderr.write(`${JSON.stringify({ error: message, }, null, 2,)} \n`,);
	process.exit(1,);
},);
