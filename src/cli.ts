#!/usr/bin/env node

import { readFileSync, } from "node:fs";
import { dirname, resolve, } from "node:path";
import { fileURLToPath, } from "node:url";
import { DataikuClient, } from "./client.js";
import { DataikuError, } from "./errors.js";
import type { BuildMode, } from "./schemas.js";

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function num(v: string | boolean | undefined,): number | undefined {
	if (typeof v !== "string") return undefined;
	const n = Number(v,);
	return Number.isFinite(n,) ? n : undefined;
}

function json(v: string | boolean | undefined,): Record<string, unknown> | undefined {
	if (typeof v !== "string") return undefined;
	return JSON.parse(v,) as Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Arg parsing
// ---------------------------------------------------------------------------

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
				const next = argv[i + 1];
				if (next !== undefined && !next.startsWith("--",)) {
					flags[arg.slice(2,)] = next;
					i++;
				} else {
					flags[arg.slice(2,)] = true;
				}
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
				requireArgs(a, 1, "dss dataset update <name> --data '{...}'",);
				const data = json(f["data"],);
				if (!data) {
					throw new UsageError("--data is required. Usage: dss dataset update <name> --data '{...}'",);
				}
				return c.datasets.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage: "dss dataset update <name> --data '{...}' [--project-key KEY]",
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
				},);
			},
			usage: "dss recipe download <name> [--output PATH]",
		},
		create: {
			handler: (c, _a, f,) => {
				const type = f["type"] as string;
				if (!type) {
					throw new UsageError("--type is required. Usage: dss recipe create --type TYPE --input DS",);
				}
				return c.recipes.create({
					type,
					name: f["name"] as string | undefined,
					inputDatasets: f["input"] ? [f["input"] as string,] : undefined,
					outputDataset: f["output"] as string | undefined,
					outputConnection: f["output-connection"] as string | undefined,
					projectKey: f["project-key"] as string | undefined,
				},);
			},
			usage:
				"dss recipe create --type TYPE --input DS [--output DS] [--output-connection CONN] [--project-key KEY]",
		},
		update: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss recipe update <name> --data '{...}'",);
				const data = json(f["data"],);
				if (!data) {
					throw new UsageError("--data is required. Usage: dss recipe update <name> --data '{...}'",);
				}
				return c.recipes.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage: "dss recipe update <name> --data '{...}' [--project-key KEY]",
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
				requireArgs(a, 1, "dss scenario update <id> --data '{...}'",);
				const data = json(f["data"],);
				if (!data) {
					throw new UsageError("--data is required. Usage: dss scenario update <id> --data '{...}'",);
				}
				return c.scenarios.update(a[0], data, f["project-key"] as string | undefined,);
			},
			usage: "dss scenario update <id> --data '{...}' [--project-key KEY]",
		},
	},

	folder: {
		list: {
			handler: (c, _a, f,) => c.folders.list(f["project-key"] as string | undefined,),
			usage: "dss folder list [--project-key KEY]",
		},
		get: {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss folder get <id>",);
				return c.folders.get(a[0], f["project-key"] as string | undefined,);
			},
			usage: "dss folder get <id> [--project-key KEY]",
		},
		contents: {
			handler: (c, a, _f,) => {
				requireArgs(a, 1, "dss folder contents <id>",);
				return c.folders.contents(a[0],);
			},
			usage: "dss folder contents <id>",
		},
		download: {
			handler: (c, a, f,) => {
				requireArgs(a, 2, "dss folder download <id> <path>",);
				return c.folders.download(a[0], a[1], {
					localPath: f["output"] as string | undefined,
				},);
			},
			usage: "dss folder download <id> <path> [--output PATH]",
		},
		upload: {
			handler: (c, a, f,) => {
				requireArgs(a, 3, "dss folder upload <id> <path> <localPath>",);
				return c.folders.upload(a[0], a[1], a[2], f["project-key"] as string | undefined,);
			},
			usage: "dss folder upload <id> <path> <localPath> [--project-key KEY]",
		},
		"delete-file": {
			handler: (c, a, f,) => {
				requireArgs(a, 2, "dss folder delete-file <id> <path>",);
				return c.folders.deleteFile(a[0], a[1], f["project-key"] as string | undefined,);
			},
			usage: "dss folder delete-file <id> <path> [--project-key KEY]",
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
				},),
			usage: 'dss variable set --standard \'{"k":"v"}\' --local \'{"k":"v"}\'',
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
				requireArgs(a, 1, "dss notebook save-jupyter <name> --data '{...}'",);
				const data = json(f["data"],);
				if (!data) throw new UsageError("--data is required (notebook JSON content)",);
				return c.notebooks.saveJupyter(a[0], data as never, f["project-key"] as string | undefined,);
			},
			usage: "dss notebook save-jupyter <name> --data '{...}' [--project-key KEY]",
		},
		"save-sql": {
			handler: (c, a, f,) => {
				requireArgs(a, 1, "dss notebook save-sql <id> --data '{...}'",);
				const data = json(f["data"],);
				if (!data) throw new UsageError("--data is required (SQL notebook content JSON)",);
				return c.notebooks.saveSql(a[0], data as never, f["project-key"] as string | undefined,);
			},
			usage: "dss notebook save-sql <id> --data '{...}' [--project-key KEY]",
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

const RESOURCE_NAMES = Object.keys(commands,).sort();

function printTopLevelHelp(): void {
	const lines = [
		"Usage: dss <resource> <action> [args...] [--flags]",
		"",
		"Global flags:",
		"  --url URL            Dataiku DSS base URL (env: DATAIKU_URL)",
		"  --api-key KEY        API key              (env: DATAIKU_API_KEY)",
		"  --project-key KEY    Default project key   (env: DATAIKU_PROJECT_KEY)",
		"  --help               Show help",
		"",
		"Resources:",
		...RESOURCE_NAMES.map((r,) => `  ${r}`),
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
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	loadEnvFile();
	const { positional, flags, } = parseArgs(process.argv.slice(2,),);

	// Top-level help
	if (positional.length === 0 || (positional.length === 0 && flags["help"])) {
		printTopLevelHelp();
		if (flags["help"]) process.exit(0,);
		process.exit(1,);
	}

	const resource = positional[0];

	// Unknown resource
	if (!commands[resource]) {
		if (flags["help"]) {
			printTopLevelHelp();
			process.exit(0,);
		}
		process.stderr.write(
			`Unknown resource: ${resource}\nAvailable: ${RESOURCE_NAMES.join(", ",)}\n`,
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
			`Unknown action: ${resource} ${action}\nAvailable actions for ${resource}: ${
				Object.keys(commands[resource],).join(", ",)
			}\n`,
		);
		process.exit(1,);
	}

	// Action-level help
	if (flags["help"] === true) {
		printActionHelp(resource, action,);
		process.exit(0,);
	}

	// Validate config
	const url = (flags["url"] as string | undefined) ?? process.env.DATAIKU_URL ?? "";
	const apiKey = (flags["api-key"] as string | undefined) ?? process.env.DATAIKU_API_KEY ?? "";

	if (!url) {
		process.stderr.write(
			`${
				JSON.stringify({ error: "Missing Dataiku URL. Set DATAIKU_URL or pass --url.", }, null, 2,)
			}\n`,
		);
		process.exit(1,);
	}
	if (!apiKey) {
		process.stderr.write(
			`${
				JSON.stringify({ error: "Missing API key. Set DATAIKU_API_KEY or pass --api-key.", }, null, 2,)
			}\n`,
		);
		process.exit(1,);
	}

	const client = new DataikuClient({
		url,
		apiKey,
		projectKey: (flags["project-key"] as string | undefined) ?? process.env.DATAIKU_PROJECT_KEY,
	},);

	const args = positional.slice(2,);
	const result = await actionMeta.handler(client, args, flags,);
	process.stdout.write(`${JSON.stringify(result, null, 2,)}\n`,);
}

main().catch((err: unknown,) => {
	if (err instanceof UsageError) {
		process.stderr.write(`${err.message}\n`,);
		process.exit(1,);
	}
	if (err instanceof DataikuError) {
		const payload: Record<string, unknown> = {
			error: err.message,
			category: err.category,
			retryable: err.retryable,
		};
		if (err.retryHint) payload.retryHint = err.retryHint;
		process.stderr.write(`${JSON.stringify(payload, null, 2,)}\n`,);
		process.exit(1,);
	}
	const message = err instanceof Error ? err.message : String(err,);
	process.stderr.write(`${JSON.stringify({ error: message, }, null, 2,)}\n`,);
	process.exit(1,);
},);
