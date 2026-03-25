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
	type DssCredentials,
	getCredentialsPath,
	loadCredentials,
	maskApiKey,
	saveCredentials,
} from "./config.js";
import { DataikuError, } from "./errors.js";
import type { BuildMode, } from "./schemas.js";
import { AGENTS, detectAgents, findWorkspaceRoot, installSkill, } from "./skill.js";

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

type TlsSettings = Pick<DssCredentials, "tlsRejectUnauthorized" | "caCertPath">;

const SQL_QUERY_USAGE =
	"dss sql query [SQL | --sql QUERY | --sql-file PATH | --sql - | --stdin] (--connection CONN | --dataset FULL_NAME) [--database DB] [--project-key KEY]";

function readStdinText(): string {
	return readFileSync(0, "utf-8",);
}

function jsonInput(flags: Record<string, string | boolean>,): Record<string, unknown> | undefined {
	if (flags["stdin"] === true) {
		return JSON.parse(readStdinText(),) as Record<string, unknown>;
	}
	if (typeof flags["data-file"] === "string") {
		return JSON.parse(readFileSync(flags["data-file"], "utf-8",),) as Record<string, unknown>;
	}
	if (typeof flags["data"] === "string") {
		return JSON.parse(flags["data"],) as Record<string, unknown>;
	}
	return undefined;
}

function parseTlsRejectUnauthorizedEnv(value: string | undefined,): boolean | undefined {
	if (value === undefined) return undefined;
	const normalized = value.trim().toLowerCase();
	if (normalized === "0" || normalized === "false" || normalized === "no") return false;
	if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
	return undefined;
}

function resolveTlsSettings(
	flags: Record<string, string | boolean>,
	saved?: TlsSettings,
): TlsSettings {
	let tlsRejectUnauthorized = flags["insecure"] === true ? false : undefined;
	let caCertPath = flags["ca-cert"] as string | undefined;

	tlsRejectUnauthorized ??= parseTlsRejectUnauthorizedEnv(process.env.NODE_TLS_REJECT_UNAUTHORIZED,);
	caCertPath ??= process.env.NODE_EXTRA_CA_CERTS;

	if (tlsRejectUnauthorized === undefined) {
		tlsRejectUnauthorized = saved?.tlsRejectUnauthorized;
	}
	caCertPath ??= saved?.caCertPath;

	return { tlsRejectUnauthorized, caCertPath, };
}

function resolveSqlInput(args: string[], flags: Record<string, string | boolean>,): string {
	const sources: Array<{ label: string; read: () => string; }> = [];

	if (typeof flags["sql"] === "string") {
		sources.push({
			label: flags["sql"] === "-" ? "--sql -" : "--sql",
			read: () => flags["sql"] === "-" ? readStdinText() : String(flags["sql"],),
		},);
	}
	if (typeof flags["sql-file"] === "string") {
		sources.push({
			label: "--sql-file",
			read: () => readFileSync(flags["sql-file"] as string, "utf-8",),
		},);
	}
	if (flags["stdin"] === true) {
		sources.push({ label: "--stdin", read: readStdinText, },);
	}
	if (args.length > 1) {
		throw new UsageError(
			`Expected at most one positional SQL argument. Quote the SQL or use --sql-file/--stdin.\nUsage: ${SQL_QUERY_USAGE}`,
		);
	}
	if (args[0] !== undefined) {
		sources.push({ label: "positional SQL", read: () => args[0], },);
	}

	if (sources.length === 0) {
		throw new UsageError(`SQL input is required. Usage: ${SQL_QUERY_USAGE}`,);
	}
	if (sources.length > 1) {
		throw new UsageError(
			`Choose exactly one SQL input source: --sql, --sql-file, --stdin, or one positional SQL argument. Usage: ${SQL_QUERY_USAGE}`,
		);
	}

	const query = sources[0]!.read();
	if (query.trim().length === 0) {
		throw new UsageError(
			`SQL input from ${sources[0]!.label} must not be empty. Usage: ${SQL_QUERY_USAGE}`,
		);
	}
	return query;
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

const BOOLEAN_FLAGS = new Set([
	"help",
	"verbose",
	"version",
	"stdin",
	"insecure",
	"global",
	"list-agents",
	"include-raw",
	"include-payload",
	"include-logs",
	"replace",
	"dry-run",
	"if-not-exists",
],);

const SHORT_FLAGS: Record<string, string> = {
	h: "help",
	v: "verbose",
	V: "version",
	f: "format",
	o: "output",
};

/** Long-flag aliases: these are normalized to the canonical name in parseArgs. */
const FLAG_ALIASES: Record<string, string> = {
	project: "project-key",
	"skip-tls-verify": "insecure",
	"extra-ca-certs": "ca-cert",
};

function isNegativeNumberToken(value: string,): boolean {
	return value.startsWith("-",) && Number.isFinite(Number(value,),);
}

function requireFlagValue(
	flagLabel: string,
	next: string | undefined,
): string {
	if (next === undefined || (next.startsWith("-",) && !isNegativeNumberToken(next,))) {
		throw new UsageError(`Flag ${flagLabel} requires a value.`,);
	}
	return next;
}

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
				const raw = arg.slice(2, eqIdx,);
				flags[FLAG_ALIASES[raw] ?? raw] = arg.slice(eqIdx + 1,);
			} else {
				const rawFlagName = arg.slice(2,);
				const flagName = FLAG_ALIASES[rawFlagName] ?? rawFlagName;
				if (BOOLEAN_FLAGS.has(flagName,)) {
					flags[flagName] = true;
				} else {
					const next = requireFlagValue(`--${rawFlagName}`, argv[i + 1],);
					flags[flagName] = next;
					i++;
				}
			}
		} else if (arg.length === 2 && arg[0] === "-" && arg[1] !== "-") {
			const long = SHORT_FLAGS[arg[1]!];
			if (long) {
				if (BOOLEAN_FLAGS.has(long,)) {
					flags[long] = true;
				} else {
					const next = requireFlagValue(`-${arg[1]}`, argv[i + 1],);
					flags[long] = next;
					i++;
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
	description?: string;
	examples?: string[];
}

const commands: Record<string, Record<string, CommandMeta>> = {
	project: {
		list: {
			handler: (c,) => c.projects.list(),
			usage: "dss project list",
			description: "List all accessible projects.",
			examples: ["dss project list", "dss project list -f table",],
		},
		get: {
			handler: (c, _a, f,) => c.projects.get(f["project-key"] as string | undefined,),
			usage: "dss project get [--project-key KEY]",
			description: "Get project settings and metadata.",
			examples: ["dss project get", "dss project get --project-key MYPROJ",],
		},
		metadata: {
			handler: (c, _a, f,) => c.projects.metadata(f["project-key"] as string | undefined,),
			usage: "dss project metadata [--project-key KEY]",
			description: "Get project-level metadata (tags, labels, custom fields).",
			examples: ["dss project metadata", "dss project metadata --project-key MYPROJ",],
		},
		flow: {
			handler: (c, _a, f,) => c.projects.flow(f["project-key"] as string | undefined,),
			usage: "dss project flow [--project-key KEY]",
			description: "Get the raw flow graph (all datasets, recipes, and edges).",
			examples: ["dss project flow", "dss project flow --project-key MYPROJ -f quiet",],
		},
		map: {
			handler: (c, _a, f,) =>
				c.projects.map({
					maxNodes: num(f["max-nodes"],),
					maxEdges: num(f["max-edges"],),
					includeRaw: f["include-raw"] === true,
				},),
			usage: "dss project map [--max-nodes N] [--max-edges N] [--include-raw]",
			description: "Get a summarized, truncated flow map.",
			examples: [
				"dss project map",
				"dss project map --max-nodes 50 --max-edges 100",
				"dss project map --include-raw",
			],
		},
	},

	dataset: {
		list: {
			handler: (c, _a, f,) => c.datasets.list(f["project-key"] as string | undefined,),
			usage: "dss dataset list [--project-key KEY]",
			description: "List all datasets in a project.",
			examples: [
				"dss dataset list",
				"dss dataset list -f table",
				"dss dataset list --project-key MYPROJ",
			],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset get <name>",);
				return c.datasets.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset get <name> [--project-key KEY]",
			description: "Get full settings for a dataset.",
			examples: ["dss dataset get orders", "dss dataset get orders --project-key MYPROJ",],
		},
		schema: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset schema <name>",);
				return c.datasets.schema(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset schema <name> [--project-key KEY]",
			description: "Show the column schema of a dataset.",
			examples: ["dss dataset schema orders", "dss dataset schema orders -f table",],
		},
		preview: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset preview <name>",);
				return c.datasets.preview(a[0], {
					maxRows: num(f["max-rows"],),
					projectKey: f["project-key"] as string | undefined,
					timeoutMs: num(f["timeout"],),
				},);
			},
			usage: "dss dataset preview <name> [--max-rows N] [--project-key KEY] [--timeout MS]",
			description: "Preview dataset rows.",
			examples: ["dss dataset preview orders", "dss dataset preview orders --max-rows 5",],
		},
		metadata: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss dataset metadata <name>",);
				return c.datasets.metadata(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss dataset metadata <name> [--project-key KEY]",
			description: "Get dataset-level metadata.",
			examples: ["dss dataset metadata orders",],
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
			description: "Download dataset contents as CSV.",
			examples: ["dss dataset download orders", "dss dataset download orders --output ./data/",],
		},
		create: {
			handler: async (c, _a, f,) => {
				const pk = f["project-key"] as string | undefined;
				const name = f["name"] as string;
				if (f["if-not-exists"] === true) {
					const list = await c.datasets.list(pk,);
					const existing = list.find((d,) => d.name === name);
					if (existing) return { exists: true, ...existing, };
				}
				return c.datasets.create({
					datasetName: name,
					connection: f["connection"] as string,
					dsType: f["type"] as string,
					projectKey: pk,
				},);
			},
			usage: "dss dataset create --name NAME --connection CONN --type TYPE [--project-key KEY]",
			description: "Create a new dataset.",
			examples: ["dss dataset create --name orders --connection filesystem --type Filesystem",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset delete <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.datasets.get(a[0], pk,);
					return { dryRun: true, action: "delete", resource: "dataset", name: a[0], current, };
				}
				await c.datasets.delete(a[0], pk,);
				return { deleted: a[0], resource: "dataset", };
			},
			usage: "dss dataset delete <name> [--project-key KEY]",
			description: "Delete a dataset.",
			examples: ["dss dataset delete orders",],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				await c.datasets.update(a[0], data, f["project-key"] as string | undefined,);
				return { updated: a[0], resource: "dataset", };
			},
			usage:
				"dss dataset update <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
			description: "Update dataset settings via JSON merge.",
			examples: [
				'dss dataset update orders --data \'{"tags":["production"]}\'',
				"echo '{\"tags\":[]}' | dss dataset update orders --stdin",
			],
		},
	},

	recipe: {
		list: {
			handler: (c, _a, f,) => c.recipes.list(f["project-key"] as string | undefined,),
			usage: "dss recipe list [--project-key KEY]",
			description: "List all recipes in a project.",
			examples: ["dss recipe list", "dss recipe list -f table",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe get <name>",);
				return c.recipes.get(a[0], {
					includePayload: f["include-payload"] === true,
				},);
			},
			usage: "dss recipe get <name> [--include-payload]",
			description: "Get recipe settings.",
			examples: ["dss recipe get compute_orders", "dss recipe get compute_orders --include-payload",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe delete <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.recipes.get(a[0],);
					return { dryRun: true, action: "delete", resource: "recipe", name: a[0], current, };
				}
				await c.recipes.delete(a[0], pk,);
				return { deleted: a[0], resource: "recipe", };
			},
			usage: "dss recipe delete <name> [--project-key KEY]",
			description: "Delete a recipe.",
			examples: ["dss recipe delete compute_orders",],
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
			description: "Download recipe definition as JSON.",
			examples: [
				"dss recipe download compute_orders",
				"dss recipe download compute_orders -o recipe.json",
			],
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
			description: "Download the code payload of a recipe.",
			examples: [
				"dss recipe download-code compute_orders",
				"dss recipe download-code compute_orders -o code.py",
			],
		},
		create: {
			handler: async (c, _a, f,) => {
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
				const name = f["name"] as string | undefined;
				const pk = f["project-key"] as string | undefined;
				if (f["if-not-exists"] === true && name) {
					const list = await c.recipes.list(pk,);
					const existing = list.find((r,) => r.name === name);
					if (existing) return { exists: true, ...existing, };
				}
				return c.recipes.create({
					type,
					name,
					inputDatasets: f["input"] ? [f["input"] as string,] : undefined,
					outputDataset,
					outputConnection: f["output-connection"] as string | undefined,
					projectKey: pk,
				},);
			},
			usage:
				"dss recipe create --type TYPE --input DS --output DS [--output-connection CONN] [--project-key KEY]",
			description: "Create a new recipe.",
			examples: [
				"dss recipe create --type python --input orders --output orders_clean",
				"dss recipe create --type python --input orders --output orders_clean --output-connection filesystem",
			],
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
			description: "Show differences between local file and remote recipe code.",
			examples: ["dss recipe diff compute_orders --file code.py",],
		},

		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				await c.recipes.update(a[0], data, f["project-key"] as string | undefined,);
				return { updated: a[0], resource: "recipe", };
			},
			usage:
				"dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
			description: "Update recipe settings via JSON merge.",
			examples: [
				"dss recipe update compute_orders --data-file settings.json",
				"cat settings.json | dss recipe update compute_orders --stdin",
			],
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
			description: "Print the recipe code payload to stdout.",
			examples: [
				"dss recipe get-payload compute_orders",
				"dss recipe get-payload compute_orders -o code.py",
			],
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
				return { updated: a[0], resource: "recipe", file: filePath, };
			},
			usage: "dss recipe set-payload <name> --file PATH [--project-key KEY]",
			description: "Upload recipe code from a local file.",
			examples: ["dss recipe set-payload compute_orders --file code.py",],
		},
	},

	job: {
		list: {
			handler: (c, _a, f,) => c.jobs.list(f["project-key"] as string | undefined,),
			usage: "dss job list [--project-key KEY]",
			description: "List recent jobs.",
			examples: ["dss job list", "dss job list -f table",],
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job get <id>",);
				return c.jobs.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss job get <id> [--project-key KEY]",
			description: "Get job details.",
			examples: ["dss job get JOB_ID",],
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
			description: "Get log output for a job.",
			examples: ["dss job log JOB_ID", "dss job log JOB_ID --activity main --max-lines 200",],
		},
		build: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job build <dataset>",);
				return c.jobs.build(a[0], {
					buildMode: f["build-mode"] as BuildMode | undefined,
				},);
			},
			usage: "dss job build <dataset> [--build-mode MODE]",
			description: "Start a dataset build (returns immediately).",
			examples: ["dss job build orders", "dss job build orders --build-mode RECURSIVE_BUILD",],
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
			description: "Build a dataset and wait for completion.",
			examples: [
				"dss job build-and-wait orders",
				"dss job build-and-wait orders --include-logs",
				"dss job build-and-wait orders --timeout 300000",
			],
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
			description: "Wait for an existing job to complete.",
			examples: ["dss job wait JOB_ID", "dss job wait JOB_ID --include-logs --timeout 60000",],
		},
		abort: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss job abort <id>",);
				return c.jobs.abort(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss job abort <id> [--project-key KEY]",
			description: "Abort a running job.",
			examples: ["dss job abort JOB_ID",],
		},
	},

	scenario: {
		list: {
			handler: (c, _a, f,) => c.scenarios.list(f["project-key"] as string | undefined,),
			usage: "dss scenario list [--project-key KEY]",
			description: "List all scenarios in a project.",
			examples: ["dss scenario list", "dss scenario list -f table",],
		},
		get: {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss scenario get <id>",);
				return c.scenarios.get(a[0],);
			},
			usage: "dss scenario get <id>",
			description: "Get scenario definition.",
			examples: ["dss scenario get my_scenario",],
		},
		run: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario run <id>",);
				return c.scenarios.run(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario run <id> [--project-key KEY]",
			description: "Trigger a scenario run (returns immediately).",
			examples: ["dss scenario run my_scenario",],
		},
		"run-and-wait": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario run-and-wait <id>",);
				return c.scenarios.runAndWait(a[0], {
					timeoutMs: num(f["timeout"],),
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss scenario run-and-wait <id> [--timeout MS] [--project-key KEY]",
			description: "Run a scenario and wait for completion.",
			examples: [
				"dss scenario run-and-wait my_scenario",
				"dss scenario run-and-wait my_scenario --timeout 300000",
			],
		},
		status: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss scenario status <id>",);
				return c.scenarios.status(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss scenario status <id> [--project-key KEY]",
			description: "Get the current run status of a scenario.",
			examples: ["dss scenario status my_scenario",],
		},
		delete: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario delete <id>",);
				const pk = f["project-key"] as string | undefined;
				if (f["dry-run"] === true) {
					const current = await c.scenarios.get(a[0],);
					return { dryRun: true, action: "delete", resource: "scenario", id: a[0], current, };
				}
				await c.scenarios.delete(a[0], pk,);
				return { deleted: a[0], resource: "scenario", };
			},
			usage: "dss scenario delete <id> [--project-key KEY]",
			description: "Delete a scenario.",
			examples: ["dss scenario delete my_scenario",],
		},
		create: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss scenario create <id> <name>",);
				const pk = f["project-key"] as string | undefined;
				if (f["if-not-exists"] === true) {
					const list = await c.scenarios.list(pk,);
					const existing = list.find((s,) => s.id === a[0]);
					if (existing) return { exists: true, ...existing, };
				}
				await c.scenarios.create(a[0], a[1], {
					scenarioType: f["type"] as "step_based" | "custom_python" | undefined,
					projectKey: pk,
				},);
				return { created: a[0], name: a[1], resource: "scenario", };
			},
			usage: "dss scenario create <id> <name> [--type step_based|custom_python] [--project-key KEY]",
			description: "Create a new scenario.",
			examples: [
				'dss scenario create my_scenario "My Scenario"',
				'dss scenario create my_scenario "My Scenario" --type custom_python',
			],
		},
		update: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required. Usage: dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin]",
					);
				}
				await c.scenarios.update(a[0], data, f["project-key"] as string | undefined,);
				return { updated: a[0], resource: "scenario", };
			},
			usage:
				"dss scenario update <id> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
			description: "Update scenario settings via JSON merge.",
			examples: ["dss scenario update my_scenario --data-file settings.json",],
		},
	},

	folder: {
		list: {
			handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
			usage: "dss folder list [--project-key KEY]",
			description: "List managed folders in a project.",
			examples: ["dss folder list", "dss folder list -f table",],
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
			description: "Get managed folder settings.",
			examples: ["dss folder get my_folder",],
		},
		contents: {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss folder contents <name-or-id>",);
				return c.folders.contents(await resolveFolderId(c, a[0], f,), {
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: "dss folder contents <name-or-id> [--project-key KEY]",
			description: "List files in a managed folder.",
			examples: ["dss folder contents my_folder",],
		},
		download: {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder download <name-or-id> <remote-path> [local-path]",);
				const localPath = (a[2] as string | undefined) ?? (f["output"] as string | undefined);
				return c.folders.download(await resolveFolderId(c, a[0], f,), a[1], {
					localPath,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss folder download <name-or-id> <remote-path> [local-path] [--output PATH] [--project-key KEY]",
			description: "Download a file from a managed folder.",
			examples: [
				"dss folder download my_folder /data/report.csv",
				"dss folder download my_folder /data/report.csv ./report.csv",
			],
		},
		upload: {
			handler: async (c, a, f,) => {
				requireArgs(a, 3, "dss folder upload <name-or-id> <path> <localPath>",);
				await c.folders.upload(
					await resolveFolderId(c, a[0], f,),
					a[1],
					a[2],
					f["project-key"] as string | undefined,
				);
				return { uploaded: a[1], folder: a[0], localPath: a[2], resource: "folder", };
			},
			usage: "dss folder upload <name-or-id> <path> <localPath> [--project-key KEY]",
			description: "Upload a local file to a managed folder.",
			examples: ["dss folder upload my_folder /data/report.csv ./report.csv",],
		},
		"delete-file": {
			handler: async (c, a, f,) => {
				requireArgs(a, 2, "dss folder delete-file <name-or-id> <path>",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete-file", resource: "folder", folder: a[0], path: a[1], };
				}
				await c.folders.deleteFile(
					await resolveFolderId(c, a[0], f,),
					a[1],
					f["project-key"] as string | undefined,
				);
				return { deleted: a[1], folder: a[0], resource: "folder", };
			},
			usage: "dss folder delete-file <name-or-id> <path> [--project-key KEY]",
			description: "Delete a file from a managed folder.",
			examples: ["dss folder delete-file my_folder /data/report.csv",],
		},
	},

	variable: {
		get: {
			handler: (c, _a, f,) => c.variables.get(f["project-key"] as string | undefined,),
			usage: "dss variable get [--project-key KEY]",
			description: "Get project variables (standard and local).",
			examples: ["dss variable get", "dss variable get --project-key MYPROJ",],
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
				`dss variable set --standard '{"k":"v"}' --local '{"k":"v"}' [--replace] [--project-key KEY]`,
			description: "Set project variables via JSON merge (or full replace with --replace).",
			examples: [
				'dss variable set --standard \'{"env":"staging"}\'',
				"dss variable set --local '{\"debug\":true}' --replace",
			],
		},
	},

	connection: {
		list: {
			handler: (c,) => c.connections.list(),
			usage: "dss connection list",
			description: "List all connection names.",
			examples: ["dss connection list",],
		},
		infer: {
			handler: (c, _a, f,) =>
				c.connections.infer({
					mode: f["mode"] as "fast" | "rich" | undefined,
				},),
			usage: "dss connection infer [--mode fast|rich]",
			description: "List connections with inferred types and metadata.",
			examples: ["dss connection infer", "dss connection infer --mode rich",],
		},
	},

	"code-env": {
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
	},
	sql: {
		query: {
			handler: (c, a, f,) => {
				const query = resolveSqlInput(a, f,);
				const connection = f["connection"] as string | undefined;
				const datasetFullName = f["dataset"] as string | undefined;
				if ((connection ? 1 : 0) + (datasetFullName ? 1 : 0) !== 1) {
					throw new UsageError(
						`Pass exactly one of --connection or --dataset. Usage: ${SQL_QUERY_USAGE}`,
					);
				}
				return c.sql.query({
					query,
					connection,
					datasetFullName,
					database: f["database"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage: SQL_QUERY_USAGE,
			description: "Run a SQL query against a DSS connection or dataset.",
			examples: [
				"dss sql query 'SELECT * FROM orders LIMIT 10' --connection my_pg",
				"dss sql query --sql-file query.sql --connection my_pg",
				"echo 'SELECT 1' | dss sql query --stdin --dataset MYPROJ.orders",
			],
		},
	},
	notebook: {
		"list-jupyter": {
			handler: (c, _a, f,) => c.notebooks.listJupyter(f["project-key"] as string | undefined,),
			usage: "dss notebook list-jupyter [--project-key KEY]",
			description: "List Jupyter notebooks.",
			examples: ["dss notebook list-jupyter",],
		},
		"get-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook get-jupyter <name>",);
				return c.notebooks.getJupyter(a[0],);
			},
			usage: "dss notebook get-jupyter <name>",
			description: "Get a Jupyter notebook.",
			examples: ["dss notebook get-jupyter my_notebook",],
		},
		"delete-jupyter": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook delete-jupyter <name>",);
				if (f["dry-run"] === true) {
					const current = await c.notebooks.getJupyter(a[0],);
					return { dryRun: true, action: "delete", resource: "jupyter-notebook", name: a[0], current, };
				}
				await c.notebooks.deleteJupyter(a[0],);
				return { deleted: a[0], resource: "jupyter-notebook", };
			},
			usage: "dss notebook delete-jupyter <name>",
			description: "Delete a Jupyter notebook.",
			examples: ["dss notebook delete-jupyter my_notebook",],
		},
		"clear-jupyter-outputs": {
			handler: async (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook clear-jupyter-outputs <name>",);
				await c.notebooks.clearJupyterOutputs(a[0],);
				return { cleared: a[0], resource: "jupyter-notebook", };
			},
			usage: "dss notebook clear-jupyter-outputs <name>",
			description: "Clear all cell outputs from a Jupyter notebook.",
			examples: ["dss notebook clear-jupyter-outputs my_notebook",],
		},
		"sessions-jupyter": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook sessions-jupyter <name>",);
				return c.notebooks.listJupyterSessions(a[0],);
			},
			usage: "dss notebook sessions-jupyter <name>",
			description: "List active kernel sessions for a Jupyter notebook.",
			examples: ["dss notebook sessions-jupyter my_notebook",],
		},
		"unload-jupyter": {
			handler: async (c, a, _f,) => {
				requireArgs(a, 2, "dss notebook unload-jupyter <name> <sessionId>",);
				await c.notebooks.unloadJupyter(a[0], a[1],);
				return { unloaded: a[0], sessionId: a[1], resource: "jupyter-notebook", };
			},
			usage: "dss notebook unload-jupyter <name> <sessionId>",
			description: "Unload a Jupyter notebook kernel session.",
			examples: ["dss notebook unload-jupyter my_notebook SESSION_ID",],
		},
		"list-sql": {
			handler: (c, _a, f,) => c.notebooks.listSql(f["project-key"] as string | undefined,),
			usage: "dss notebook list-sql [--project-key KEY]",
			description: "List SQL notebooks.",
			examples: ["dss notebook list-sql",],
		},
		"get-sql": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook get-sql <id>",);
				return c.notebooks.getSql(a[0],);
			},
			usage: "dss notebook get-sql <id>",
			description: "Get a SQL notebook.",
			examples: ["dss notebook get-sql my_sql_notebook",],
		},
		"delete-sql": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook delete-sql <id>",);
				if (f["dry-run"] === true) {
					const current = await c.notebooks.getSql(a[0],);
					return { dryRun: true, action: "delete", resource: "sql-notebook", id: a[0], current, };
				}
				await c.notebooks.deleteSql(a[0],);
				return { deleted: a[0], resource: "sql-notebook", };
			},
			usage: "dss notebook delete-sql <id>",
			description: "Delete a SQL notebook.",
			examples: ["dss notebook delete-sql my_sql_notebook",],
		},
		"history-sql": {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss notebook history-sql <id>",);
				return c.notebooks.getSqlHistory(a[0],);
			},
			usage: "dss notebook history-sql <id>",
			description: "Get query history for a SQL notebook.",
			examples: ["dss notebook history-sql my_sql_notebook",],
		},
		"save-jupyter": {
			handler: async (c, a, f,) => {
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
				await c.notebooks.saveJupyter(a[0], data as never, f["project-key"] as string | undefined,);
				return { saved: a[0], resource: "jupyter-notebook", };
			},
			usage:
				"dss notebook save-jupyter <name> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
			description: "Save content to a Jupyter notebook.",
			examples: [
				"dss notebook save-jupyter my_notebook --data-file notebook.json",
				"cat notebook.json | dss notebook save-jupyter my_notebook --stdin",
			],
		},
		"save-sql": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin]",);
				const data = jsonInput(f,);
				if (!data) {
					throw new UsageError(
						"--data, --data-file, or --stdin is required (SQL notebook content JSON).",
					);
				}
				await c.notebooks.saveSql(a[0], data as never, f["project-key"] as string | undefined,);
				return { saved: a[0], resource: "sql-notebook", };
			},
			usage:
				"dss notebook save-sql <id> [--data '{...}' | --data-file PATH | --stdin] [--project-key KEY]",
			description: "Save content to a SQL notebook.",
			examples: ["dss notebook save-sql my_sql_notebook --data-file content.json",],
		},
		"clear-sql-history": {
			handler: async (c, a, f,) => {
				requireArgs(a, 1, "dss notebook clear-sql-history <id>",);
				await c.notebooks.clearSqlHistory(a[0], {
					cellId: f["cell-id"] as string | undefined,
					numRunsToRetain: num(f["retain"],),
					projectKey: f["project-key"] as string | undefined,
				},);
				return { cleared: a[0], resource: "sql-notebook", };
			},
			usage: "dss notebook clear-sql-history <id> [--cell-id CID] [--retain N] [--project-key KEY]",
			description: "Clear query history for a SQL notebook.",
			examples: [
				"dss notebook clear-sql-history my_sql_notebook",
				"dss notebook clear-sql-history my_sql_notebook --cell-id CELL1 --retain 5",
			],
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
		"      --timeout MS         Operation timeout (build-and-wait, run-and-wait)",
		"      --request-timeout MS HTTP request timeout in ms (default: 30000)",
		"      --dry-run            Preview destructive actions without executing",
		"      --if-not-exists      Skip create if resource already exists",
		"      --insecure           Disable TLS certificate verification",
		"      --ca-cert PATH       Extra PEM CA bundle (env: NODE_EXTRA_CA_CERTS)",
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
	const maxName = Math.max(...Object.keys(actions,).map((n,) => n.length),);
	const lines = [
		`Usage: dss ${resource} <action> [args...] [--flags]`,
		"",
		"Actions:",
		...Object.entries(actions,).map(
			([name, meta,],) => `  ${name.padEnd(maxName + 2,)}${meta.description ?? meta.usage}`,
		),
		"",
		`Run 'dss ${resource} <action> --help' for details and examples.`,
	];
	process.stderr.write(`${lines.join("\n",)}\n`,);
}

function printActionHelp(resource: string, action: string,): void {
	const meta = commands[resource]?.[action];
	if (!meta) return;
	const lines: string[] = [];
	if (meta.description) lines.push(meta.description, "",);
	lines.push(`Usage: ${meta.usage}`,);
	if (meta.examples && meta.examples.length > 0) {
		lines.push("", "Examples:",);
		for (const ex of meta.examples) lines.push(`  ${ex}`,);
	}
	process.stderr.write(`${lines.join("\n",)}\n`,);
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
	description?: string;
	examples?: string[];
}> = {
	login: {
		handler: async (flags,) => {
			const tlsSettings = resolveTlsSettings(flags,);
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
			const result = await validateCredentials(url, apiKey, tlsSettings,);
			if (!result.valid) {
				process.stderr.write(`✗ Failed\n`,);
				if (result.dataikuError) throw result.dataikuError;
				throw new DataikuError(
					0,
					"Authentication Failed",
					result.error ?? "Credential validation failed",
				);
			}
			process.stderr.write("✓ Connected\n",);

			saveCredentials({ url, apiKey, projectKey, ...tlsSettings, },);
			process.stderr.write(`Credentials saved to ${getCredentialsPath()}\n`,);
		},
		usage:
			"dss auth login [--url URL] [--api-key KEY] [--project-key KEY] [--insecure] [--ca-cert PATH]",
		description: "Save DSS credentials (interactive or via flags).",
		examples: [
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY",
			"dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ",
		],
	},
	status: {
		handler: async (flags,) => {
			const creds = loadCredentials();
			if (!creds) {
				process.stderr.write("No saved credentials. Run: dss auth login\n",);
				process.exit(1,);
			}
			const tlsSettings = resolveTlsSettings(flags, creds,);
			const lines = [
				`URL:         ${creds.url}`,
				`API key:     ${maskApiKey(creds.apiKey,)}`,
				`Project key: ${creds.projectKey ?? "(not set)"}`,
				`TLS verify:  ${tlsSettings.tlsRejectUnauthorized === false ? "disabled" : "strict"}`,
				`CA cert:     ${tlsSettings.caCertPath ?? "(default trust store)"}`,
			];
			for (const line of lines) process.stderr.write(`${line}\n`,);

			const result = await validateCredentials(creds.url, creds.apiKey, tlsSettings,);
			if (result.valid) {
				process.stderr.write("Connection:  ✓ Valid\n",);
			} else {
				process.stderr.write(`Connection:  ✗ Failed (${result.error ?? "unknown error"})\n`,);
				process.stderr.write(`Config:      ${getCredentialsPath()}\n`,);
				process.exit(1,);
			}
			process.stderr.write(`Config:      ${getCredentialsPath()}\n`,);
		},
		usage: "dss auth status [--insecure] [--ca-cert PATH]",
		description: "Show saved credentials and verify the connection.",
		examples: ["dss auth status",],
	},
	logout: {
		handler: async (_flags,) => {
			deleteCredentials();
			process.stderr.write("Credentials removed.\n",);
		},
		usage: "dss auth logout",
		description: "Remove saved credentials.",
		examples: ["dss auth logout",],
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
	tlsRejectUnauthorized?: boolean;
	caCertPath?: string;
} {
	let url = flags["url"] as string | undefined;
	let apiKey = flags["api-key"] as string | undefined;
	let projectKey = flags["project-key"] as string | undefined;
	const saved = loadCredentials();

	url ??= process.env.DATAIKU_URL;
	apiKey ??= process.env.DATAIKU_API_KEY;
	projectKey ??= process.env.DATAIKU_PROJECT_KEY;

	if (saved) {
		url ||= saved.url;
		apiKey ||= saved.apiKey;
		projectKey ??= saved.projectKey;
	}

	return {
		url: url ?? "",
		apiKey: apiKey ?? "",
		projectKey,
		...resolveTlsSettings(flags, saved ?? undefined,),
	};
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
		if (!action) {
			const maxName = Math.max(...Object.keys(AUTH_ACTIONS,).map((n,) => n.length),);
			const lines = [
				"Usage: dss auth <action> [--flags]",
				"",
				"Actions:",
				...Object.entries(AUTH_ACTIONS,).map(
					([name, meta,],) => `  ${name.padEnd(maxName + 2,)}${meta.description ?? meta.usage}`,
				),
				"",
				"Run 'dss auth <action> --help' for details and examples.",
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
		if (flags["help"] === true) {
			const lines: string[] = [];
			if (authMeta.description) lines.push(authMeta.description, "",);
			lines.push(`Usage: ${authMeta.usage}`,);
			if (authMeta.examples && authMeta.examples.length > 0) {
				lines.push("", "Examples:",);
				for (const ex of authMeta.examples) lines.push(`  ${ex}`,);
			}
			process.stderr.write(`${lines.join("\n",)}\n`,);
			process.exit(0,);
		}
		await authMeta.handler(flags,);
		return;
	}

	// install-skill — dispatched before client creation
	if (resource === "install-skill") {
		if (flags["help"] === true) {
			const lines = [
				"Usage: dss install-skill [--global] [--agent NAME] [--target PATH] [--list-agents]",
				"",
				"Install the dataiku-dss agent skill for detected coding agents.",
				"",
				"Flags:",
				"  --global         Install to user-level global scope (default: project)",
				"  --agent NAME     Target a specific agent: claude, codex, cursor, pi, omp",
				"  --target PATH    Project directory to install into (default: workspace root)",
				"  --list-agents    Print detected agents and exit",
			];
			process.stderr.write(`${lines.join("\n",)}\n`,);
			process.exit(0,);
		}

		const listOnly = flags["list-agents"] === true;
		const agentFilter = typeof flags["agent"] === "string" ? flags["agent"] : undefined;
		const isGlobal = flags["global"] === true;
		const targetDir = typeof flags["target"] === "string" ? flags["target"] : undefined;

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
		const cwd = targetDir ?? (isGlobal ? process.cwd() : findWorkspaceRoot(process.cwd(),));
		process.stderr.write(`Installing dataiku-dss skill (${scope} scope):\n`,);
		const results = installSkill(targets, { global: isGlobal, cwd, },);

		for (const r of results) {
			process.stderr.write(`  ${r.agent}  \u2192  ${r.path}\n`,);
		}
		if (results.length > 0) {
			process.stderr.write(`\nDone. ${results.length} skill(s) installed.\n`,);
		}
		return;
	}

	// commands — machine-readable introspection (no auth needed)
	if (resource === "commands") {
		const registry: Record<
			string,
			Record<string, { usage: string; description?: string; examples?: string[]; }>
		> = {};
		for (const [res, actions,] of Object.entries(commands,)) {
			registry[res] = {};
			for (const [act, meta,] of Object.entries(actions,)) {
				registry[res][act] = {
					usage: meta.usage,
					description: meta.description,
					examples: meta.examples,
				};
			}
		}
		registry["auth"] = {};
		for (const [act, meta,] of Object.entries(AUTH_ACTIONS,)) {
			registry["auth"][act] = {
				usage: meta.usage,
				description: meta.description,
				examples: meta.examples,
			};
		}
		process.stdout.write(`${JSON.stringify(registry, null, 2,)}\n`,);
		process.exit(0,);
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
	const { url, apiKey, projectKey, tlsRejectUnauthorized, caCertPath, } = resolveCredentials(flags,);

	if (!url) {
		throw new UsageError("Missing Dataiku URL. Set DATAIKU_URL, pass --url, or run: dss auth login",);
	}
	if (!apiKey) {
		throw new UsageError(
			"Missing API key. Set DATAIKU_API_KEY, pass --api-key, or run: dss auth login",
		);
	}

	const requestTimeoutMs = num(flags["request-timeout"],) ?? num(flags["timeout"],) ?? undefined;

	const client = new DataikuClient({
		url,
		apiKey,
		projectKey,
		verbose: flags["verbose"] === true,
		requestTimeoutMs,
		tlsRejectUnauthorized,
		caCertPath,
	},);

	const args = positional.slice(2,);
	const format = parseOutputFormat(flags["format"],);
	const result = await actionMeta.handler(client, args, flags,);
	writeCommandResult(result, format,);
}

main().catch((err: unknown,) => {
	if (err instanceof UsageError) {
		process.stderr.write(`${JSON.stringify({ error: err.message, code: "usage", }, null, 2,)}\n`,);
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
