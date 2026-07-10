import { readFileSync, } from "node:fs";
import { mkdir, writeFile, } from "node:fs/promises";
import { join, } from "node:path";
import type { BuildMode, } from "../../schemas.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import {
	jobLogFilterFromFlag,
	jsonInput,
	maxLogLinesFromFlags,
	normalizeLineEndings,
	num,
	recipeInputDatasetsFromFlags,
	requiredStringFlag,
	rewritePairsFromFlags,
	sha256Hex,
	splitCsvFlag,
	stableHash,
} from "../coerce.js";
import { moveCreatedItemsToZone, resolveFlowZoneIdFromFlags, } from "../helpers/flow-zone.js";
import {
	readRecipeBackup,
	recipeBackupDocument,
	recipeBackupPath,
	recipeCodeEnv,
	recipeGraph,
	recipeInputItemRef,
	recipeRoleInputItems,
	recipeRunShouldWait,
} from "../helpers/recipe.js";
import { encodedProjectEndpoint, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

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

export const recipeCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.recipes.list(f["project-key"] as string | undefined,),
		usage: "dss recipe list [--project-key KEY]",
		description: "List all recipes in a project.",
		examples: ["dss recipe list",],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss recipe get <name>",);
			return c.recipes.get(a[0], {
				includePayload: f["include-payload"] === true && f["no-payload"] !== true,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: "dss recipe get <name> [--include-payload|--no-payload] [--project-key KEY]",
		description: "Get compact recipe settings unless --include-payload is set.",
		examples: [
			"dss recipe get compute_orders",
			"dss recipe get compute_orders --no-payload",
			"dss recipe get compute_orders --include-payload",
		],
	},
	"validate-graph": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss recipe validate-graph <name>",);
			return c.recipes.validateGraph(a[0], {
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: "dss recipe validate-graph <name> [--project-key KEY]",
		description: "Validate declared recipe input/output graph references before building.",
		examples: ["dss recipe validate-graph compute_orders",],
	},
	run: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss recipe run <name>",);
			const pk = f["project-key"] as string | undefined;
			const wait = recipeRunShouldWait(f,);
			const options = {
				buildMode: f["build-mode"] as BuildMode | undefined,
				includeLogs: f["include-logs"] === true,
				logFilter: jobLogFilterFromFlag(f["log-filter"],),
				maxLogLines: maxLogLinesFromFlags(f,),
				partition: f["partition"] as string | undefined,
				pollIntervalMs: num(f["poll-interval"],),
				projectKey: pk,
				timeoutMs: num(f["timeout"],),
				summary: f["summary"] === true,
				wait,
			};
			if (f["dry-run"] === true) {
				const outputs = await c.recipes.resolveRunOutputs(a[0], {
					partition: options.partition,
					projectKey: pk,
				},);
				return {
					dryRun: true,
					action: "run",
					resource: "recipe",
					recipe: a[0],
					outputs,
					...options,
					endpoint: encodedProjectEndpoint(c, pk, "/jobs/",),
					method: "POST",
				};
			}
			return c.recipes.run(a[0], options,);
		},
		usage:
			"dss recipe run <name> [--wait|--no-wait] [--build-mode MODE] [--include-logs] [--log-filter stdout|stderr|user|errors] [--summary] [--max-log-lines N] [--timeout MS] [--poll-interval MS] [--partition PARTITION] [--dry-run] [--project-key KEY]",
		description:
			"Run a recipe by resolving its outputs and submitting the correct dataset or managed-folder build job.",
		examples: [
			"dss recipe run compute_orders --wait",
			"dss recipe run compute_exports --include-logs --log-filter stdout --summary --timeout 600000",
			"dss recipe run compute_exports --dry-run",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss recipe delete <name>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() =>
					c.recipes.get(a[0], { projectKey: pk, includePayload: true, },)
				);
				if (!current) return skipResult("recipe", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "recipe", name: a[0], current, };
				}
			}
			await c.recipes.delete(a[0], pk,);
			return { deleted: a[0], resource: "recipe", };
		},
		usage: "dss recipe delete <name> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a recipe.",
		examples: ["dss recipe delete compute_orders", "dss recipe delete compute_orders --if-exists",],
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
					"--type is required. Usage: dss recipe create --type TYPE --input DS (--output DS | --output-folder FOLDER_ID)",
				);
			}
			const outputDataset = f["output"] as string | undefined;
			const outputFolder = f["output-folder"] as string | undefined;
			if (outputDataset && outputFolder) {
				throw new UsageError("--output and --output-folder are mutually exclusive.",);
			}
			if (!outputDataset && !outputFolder) {
				throw new UsageError(
					"--output or --output-folder is required. Usage: dss recipe create --type TYPE --input DS (--output DS | --output-folder FOLDER_ID)",
				);
			}
			if (outputFolder && !f["output-connection"]) {
				throw new UsageError("--output-connection is required when using --output-folder.",);
			}
			const name = f["name"] as string | undefined;
			const pk = f["project-key"] as string | undefined;
			const inputDatasets = recipeInputDatasetsFromFlags(f,);
			const joinColumns = splitCsvFlag(f["join-on"],);
			const fuzzyColumns = splitCsvFlag(f["fuzzy-on"],);
			const rawFuzzyDistance = f["fuzzy-distance"];
			let fuzzyDistance: string | undefined;
			if (typeof rawFuzzyDistance === "string") {
				const normalized = rawFuzzyDistance.trim().toUpperCase();
				if (
					![
						"DAMERAU_LEVENSHTEIN",
						"HAMMING",
						"JACCARD",
						"COSINE",
						"EUCLIDEAN",
					].includes(normalized,)
				) {
					throw new UsageError(
						"--fuzzy-distance must be one of DAMERAU_LEVENSHTEIN, HAMMING, JACCARD, COSINE, or EUCLIDEAN.",
						"invalid_enum",
					);
				}
				fuzzyDistance = normalized;
			}
			const fuzzyThreshold = num(f["fuzzy-threshold"],);
			if (typeof f["fuzzy-threshold"] === "string" && fuzzyThreshold === undefined) {
				throw new UsageError("--fuzzy-threshold must be a finite number.", "validation_failed",);
			}
			const payload = {
				type,
				name,
				inputDatasets,
				outputDataset,
				outputFolder,
				outputConnection: f["output-connection"] as string | undefined,
				...(joinColumns.length > 0 ? { joinOn: joinColumns, } : {}),
				...(typeof f["join-type"] === "string" ? { joinType: f["join-type"], } : {}),
				...(fuzzyColumns.length > 0 ? { fuzzyOn: fuzzyColumns, } : {}),
				...(fuzzyDistance ? { fuzzyDistance, } : {}),
				...(fuzzyThreshold !== undefined ? { fuzzyThreshold, } : {}),
				fuzzyNormalize: f["normalize"] === true,
				projectKey: pk,
			};
			const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
			const zoneMove = zoneId && name
				? [{ objectId: name, objectType: "RECIPE" as const, },]
				: undefined;
			if ((f["if-not-exists"] === true || f["dry-run"] === true) && name) {
				const list = await c.recipes.list(pk,);
				const existing = list.find((r,) => r.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("recipe", name, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "recipe",
						name,
						payload,
						...(zoneId ? { zoneId, zoneMove, } : {}),
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "create",
					resource: "recipe",
					payload,
					...(zoneId ? { zoneId, zoneMove, } : {}),
				};
			}
			const created = await c.recipes.create(payload,);
			const createdName = created.recipeName;
			const moved = await moveCreatedItemsToZone(c, f, [{
				objectId: createdName,
				objectType: "RECIPE",
			},], pk,);
			return { created: createdName, resource: "recipe", ...created, ...moved, };
		},
		usage:
			"dss recipe create --type TYPE --input DS[,DS2] (--output DS | --output-folder FOLDER_ID) [--name NAME] [--output-connection CONN] [--zone ZONE|--zone-id ID] [--if-not-exists] [--dry-run] [--join-on COL|LEFT=RIGHT[,...]] [--join-type LEFT|INNER|RIGHT|FULL] [--fuzzy-on COL|LEFT=RIGHT[,...]] [--fuzzy-distance DAMERAU_LEVENSHTEIN|HAMMING|JACCARD|COSINE|EUCLIDEAN] [--fuzzy-threshold N] [--normalize] [--project-key KEY]",
		description: "Create a recipe with one or more inputs and a dataset or managed-folder output.",
		examples: [
			"dss recipe create --type python --input raw_orders,lookup --output orders_clean",
			"dss recipe create --type python --input orders --input customers --output orders_clean --zone Experiments",
			"dss recipe create --type python --input orders --output-folder LT7TUHJ8 --output-connection filesystem --dry-run",
		],
	},
	clone: {
		handler: async (c, a, f,) => {
			const usage =
				"dss recipe clone [source|--from SOURCE] (--name NAME|--to NAME) [--replace-input FROM=TO] [--replace-output FROM=TO] [--replace-payload-text FROM=TO] [--output DATASET] [--copy-output-settings] [--path PATH] [--metastore-table TABLE] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]";
			const fromFlag = typeof f["from"] === "string" ? f["from"].trim() : "";
			const sourceName = a[0] ?? fromFlag;
			if (!sourceName) {
				throw new UsageError(`Source recipe is required. Usage: ${usage}`, "missing_required_flag",);
			}
			if (a[0] && fromFlag && a[0] !== fromFlag) {
				throw new UsageError(
					"Positional source and --from must match when both are provided.",
					"invalid_enum",
				);
			}
			const pk = f["project-key"] as string | undefined;
			const toFlag = typeof f["to"] === "string" ? f["to"].trim() : "";
			const nameFlag = typeof f["name"] === "string" ? f["name"].trim() : "";
			const name = toFlag || nameFlag;
			if (!name) {
				throw new UsageError(`--name or --to is required. Usage: ${usage}`, "missing_required_flag",);
			}
			const inputRewrites = rewritePairsFromFlags(f, "replace-input",);
			const outputRewrites = rewritePairsFromFlags(f, "replace-output",);
			const payloadTextRewrites = rewritePairsFromFlags(f, "replace-payload-text",);
			const opts = {
				projectKey: pk,
				name,
				outputDataset: f["output"] as string | undefined,
				outputRewrites,
				inputRewrites,
				payloadTextRewrites,
				copyOutputSettings: f["copy-output-settings"] === true,
				outputPath: f["path"] as string | undefined,
				metastoreTableName: f["metastore-table"] as string | undefined,
			};
			const source = await c.recipes.get(sourceName, { includePayload: true, projectKey: pk, },);
			const outputItems = Object.values(
				(source.recipe.outputs ?? {}) as Record<
					string,
					{ items?: Array<{ ref?: string; type?: string; }>; }
				>,
			).flatMap((role,) => role.items ?? []).filter((item,) => typeof item.ref === "string");
			const plannedOutputRewrites = { ...outputRewrites, };
			if (opts.outputDataset !== undefined && outputItems.length === 1) {
				plannedOutputRewrites[outputItems[0]!.ref!] = opts.outputDataset;
			}
			if (
				opts.copyOutputSettings === true
				&& Object.keys(plannedOutputRewrites,).length > 1
				&& (opts.outputPath !== undefined || opts.metastoreTableName !== undefined)
			) {
				throw new UsageError(
					"Cannot reuse --path or --metastore-table for multiple cloned output datasets.",
					"invalid_enum",
				);
			}
			const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "clone",
					resource: "recipe",
					source: sourceName,
					target: name,
					inputRewrites,
					outputRewrites: plannedOutputRewrites,
					copyOutputSettings: opts.copyOutputSettings,
					payloadTextRewrites,
					current: source,
					...(zoneId ? { zoneId, zoneMove: [{ objectId: name, objectType: "RECIPE", },], } : {}),
				};
			}
			const cloned = await c.recipes.clone(sourceName, opts,).catch((error: unknown,) => {
				if (
					!opts.copyOutputSettings
					&& error instanceof Error
					&& /need to create output dataset/i.test(error.message,)
				) {
					throw new UsageError(
						`Clone output dataset does not exist. Pass --copy-output-settings to clone the source output settings, or create the output dataset first. Usage: ${usage}`,
						"missing_required_flag",
					);
				}
				throw error;
			},);
			const moved = await moveCreatedItemsToZone(
				c,
				f,
				[{ objectId: name, objectType: "RECIPE", },],
				pk,
			);
			return { ...cloned, resource: "recipe", ...moved, };
		},
		usage:
			"dss recipe clone [source|--from SOURCE] (--name NAME|--to NAME) [--replace-input FROM=TO] [--replace-output FROM=TO] [--replace-payload-text FROM=TO] [--output DATASET] [--copy-output-settings] [--path PATH] [--metastore-table TABLE] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]",
		description: "Clone a recipe graph/settings/payload into a separate experiment recipe.",
		examples: [
			"dss recipe clone compute_orders --name compute_orders_opt --output orders_opt --copy-output-settings --dry-run",
			"dss recipe clone compute_orders --name compute_orders_opt --output orders_opt --zone Experiments",
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
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await c.recipes.get(a[0], { projectKey: pk, includePayload: true, },);
				const currentRecipe = current.recipe as Record<string, unknown>;
				const next = {
					...current,
					...data,
					recipe: deepMerge(
						currentRecipe,
						(data.recipe && typeof data.recipe === "object" && !Array.isArray(data.recipe,))
							? data.recipe as Record<string, unknown>
							: {},
					),
				};
				return { dryRun: true, action: "update", resource: "recipe", name: a[0], current, next, };
			}
			await c.recipes.update(a[0], data, pk,);
			return { updated: a[0], resource: "recipe", };
		},
		usage:
			"dss recipe update <name> [--data '{...}' | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]",
		description:
			"Update recipe settings via JSON merge. Recipe definition fields must be nested under a top-level recipe key.",
		examples: [
			"dss recipe update compute_orders --data-file settings.json --dry-run",
			'dss recipe update compute_orders --data \'{"recipe":{"params":{"envSelection":{"envMode":"EXPLICIT_ENV","envName":"python39"}}}}\'',
			"cat settings.json | dss recipe update compute_orders --stdin",
		],
	},
	"add-input": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss recipe add-input <recipe> <dataset> [--role ROLE] [--if-not-exists] [--dry-run] [--project-key KEY]",
			);
			const role = (f["role"] as string | undefined) ?? "main";
			const pk = f["project-key"] as string | undefined;
			const { recipe, } = await c.recipes.get(a[0], { projectKey: pk, },);
			const items = recipeRoleInputItems(recipe, role,);
			const present = items.some((item,) => recipeInputItemRef(item,) === a[1]);
			if (present) {
				if (f["if-not-exists"] === true) {
					return skipResult("recipe", a[0], "exists", { dataset: a[1], role, },);
				}
				throw new UsageError(
					`Dataset "${a[1]}" is already a "${role}" input of recipe "${a[0]}".`,
					"validation_failed",
				);
			}
			const nextItems = [...items, { ref: a[1], deps: [], },];
			const inputs = nextItems.map(recipeInputItemRef,).filter((ref,): ref is string => Boolean(ref,));
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "add-input",
					resource: "recipe",
					recipe: a[0],
					dataset: a[1],
					role,
					inputs,
				};
			}
			await c.recipes.update(
				a[0],
				{ recipe: { inputs: { [role]: { items: nextItems, }, }, }, },
				pk,
			);
			return { updated: a[0], resource: "recipe", action: "add-input", role, dataset: a[1], inputs, };
		},
		usage:
			"dss recipe add-input <recipe> <dataset> [--role ROLE] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description:
			"Add a dataset as a recipe input by appending one item to the current inputs (no need to resend the whole list).",
		examples: [
			"dss recipe add-input compute_orders extra_lookup",
			"dss recipe add-input compute_orders extra_lookup --if-not-exists --dry-run",
		],
	},
	"remove-input": {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				2,
				"dss recipe remove-input <recipe> <dataset> [--role ROLE] [--if-exists] [--dry-run] [--project-key KEY]",
			);
			const role = (f["role"] as string | undefined) ?? "main";
			const pk = f["project-key"] as string | undefined;
			const { recipe, } = await c.recipes.get(a[0], { projectKey: pk, },);
			const items = recipeRoleInputItems(recipe, role,);
			const present = items.some((item,) => recipeInputItemRef(item,) === a[1]);
			if (!present) {
				if (f["if-exists"] === true) {
					return skipResult("recipe", a[0], "missing", { dataset: a[1], role, },);
				}
				throw new UsageError(
					`Dataset "${a[1]}" is not a "${role}" input of recipe "${a[0]}".`,
					"validation_failed",
				);
			}
			const nextItems = items.filter((item,) => recipeInputItemRef(item,) !== a[1]);
			const inputs = nextItems.map(recipeInputItemRef,).filter((ref,): ref is string => Boolean(ref,));
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "remove-input",
					resource: "recipe",
					recipe: a[0],
					dataset: a[1],
					role,
					inputs,
				};
			}
			await c.recipes.update(
				a[0],
				{ recipe: { inputs: { [role]: { items: nextItems, }, }, }, },
				pk,
			);
			return {
				updated: a[0],
				resource: "recipe",
				action: "remove-input",
				role,
				dataset: a[1],
				inputs,
			};
		},
		usage:
			"dss recipe remove-input <recipe> <dataset> [--role ROLE] [--if-exists] [--dry-run] [--project-key KEY]",
		description:
			"Remove a dataset from a recipe's inputs by dropping one item from the current inputs.",
		examples: [
			"dss recipe remove-input compute_orders stale_lookup",
			"dss recipe remove-input compute_orders stale_lookup --if-exists --dry-run",
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
		usage: "dss recipe get-payload <name> [--raw] [--output PATH] [--project-key KEY]",
		description: "Print the recipe code payload as JSON; use --raw for raw bytes, not JSON.",
		examples: [
			"dss recipe get-payload compute_orders --raw",
			"dss recipe get-payload compute_orders -o code.py",
		],
	},
	cat: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss recipe cat <name> [--raw]",);
			return c.recipes.getPayload(a[0], {
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: "dss recipe cat <name> [--raw] [--project-key KEY]",
		description: "Print a recipe code payload as JSON; use --raw for raw bytes, not JSON.",
		examples: ["dss recipe cat compute_orders --raw",],
	},
	"set-payload": {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss recipe set-payload <name> --file PATH",);
			const filePath = f["file"] as string;
			if (!filePath) throw new UsageError("--file is required.",);
			const content = readFileSync(filePath, "utf-8",);
			const pk = f["project-key"] as string | undefined;
			const shouldBackup = f["no-backup"] !== true;
			const backupDir = shouldBackup
				? (f["backup-dir"] as string | undefined) ?? join(process.cwd(), ".dss-backups", "recipes",)
				: undefined;
			const backupPath = backupDir ? recipeBackupPath(a[0], backupDir,) : undefined;
			const current = await c.recipes.get(a[0], {
				projectKey: pk,
				includePayload: true,
			},);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "set-payload",
					resource: "recipe",
					name: a[0],
					file: filePath,
					current,
					next: { ...current, payload: content, },
					...(backupPath ? { backupPath, backup: recipeBackupDocument(a[0], pk, current,), } : {}),
				};
			}
			if (backupDir && backupPath) {
				await mkdir(backupDir, { recursive: true, },);
				await writeFile(
					backupPath,
					`${JSON.stringify(recipeBackupDocument(a[0], pk, current,), null, 2,)}\n`,
					"utf-8",
				);
			}
			await c.recipes.replace(a[0], { ...current, payload: content, }, pk,);
			return {
				updated: a[0],
				resource: "recipe",
				file: filePath,
				backupCreated: backupPath !== undefined,
				...(backupPath ? { backupPath, } : {}),
			};
		},
		usage:
			"dss recipe set-payload <name> --file PATH [--backup-dir DIR|--no-backup] [--dry-run] [--project-key KEY]",
		description:
			"Upload recipe code from a local file, backing up payload, graph, settings, and version metadata by default.",
		examples: [
			"dss recipe set-payload compute_orders --file code.py --dry-run",
			"dss recipe set-payload compute_orders --file code.py --backup-dir ./backups",
			"dss recipe set-payload compute_orders --file code.py --no-backup",
		],
	},
	restore: {
		handler: async (c, a, f,) => {
			const usage =
				"dss recipe restore <name> --backup FILE [--payload-only] [--dry-run] [--project-key KEY]";
			requireArgs(a, 1, usage,);
			const backupPath = requiredStringFlag(f, "backup", usage,);
			const backup = readRecipeBackup(backupPath,);
			const payload = typeof backup.payload === "string" ? backup.payload : "";
			const pk = f["project-key"] as string | undefined;
			const current = await c.recipes.get(a[0], { includePayload: true, projectKey: pk, },);
			const backupRecipe =
				backup.recipe && typeof backup.recipe === "object" && !Array.isArray(backup.recipe,)
					? backup.recipe as Record<string, unknown>
					: undefined;
			const restoredRecipe = backupRecipe
				? { ...backupRecipe, name: a[0], ...(pk ? { projectKey: pk, } : {}), }
				: undefined;
			const next = f["payload-only"] === true || !restoredRecipe
				? { ...current, payload, }
				: { ...current, recipe: restoredRecipe, payload, };
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "restore",
					resource: "recipe",
					name: a[0],
					backupPath,
					current,
					next,
				};
			}
			await c.recipes.replace(a[0], next as Record<string, unknown>, pk,);
			return {
				restored: a[0],
				resource: "recipe",
				backupPath,
				payloadOnly: f["payload-only"] === true,
			};
		},
		usage: "dss recipe restore <name> --backup FILE [--payload-only] [--dry-run] [--project-key KEY]",
		description: "Restore a recipe from a set-payload backup.",
		examples: [
			"dss recipe restore compute_orders --backup .dss-backups/recipes/backup.recipe-backup.json --dry-run",
		],
	},
	"assert-unchanged": {
		handler: async (c, a, f,) => {
			const usage = "dss recipe assert-unchanged <name> --since BACKUP [--project-key KEY]";
			requireArgs(a, 1, usage,);
			const backupPath = requiredStringFlag(f, "since", usage,);
			const backup = readRecipeBackup(backupPath,);
			const current = await c.recipes.get(a[0], {
				includePayload: true,
				projectKey: f["project-key"] as string | undefined,
			},);
			const payloadHash = sha256Hex(current.payload ?? "",);
			const normalizedPayloadHash = sha256Hex(normalizeLineEndings(current.payload ?? "",),);
			const expectedPayloadHash = typeof backup.payloadHash === "string"
				? backup.payloadHash
				: undefined;
			const expectedNormalizedPayloadHash = typeof backup.normalizedPayloadHash === "string"
				? backup.normalizedPayloadHash
				: typeof backup.payload === "string"
				? sha256Hex(normalizeLineEndings(backup.payload,),)
				: undefined;
			const checks = [
				{
					name: "payload",
					expected: expectedPayloadHash,
					actual: payloadHash,
					unchanged: expectedPayloadHash === payloadHash
						|| (
							expectedNormalizedPayloadHash !== undefined
							&& expectedNormalizedPayloadHash === normalizedPayloadHash
						),
					normalizedExpected: expectedNormalizedPayloadHash,
					normalizedActual: normalizedPayloadHash,
				},
				{
					name: "graph",
					expected: backup.graphHash,
					actual: stableHash(recipeGraph(current.recipe,),),
					unchanged: backup.graphHash === stableHash(recipeGraph(current.recipe,),),
				},
				{
					name: "codeEnv",
					expected: backup.codeEnvHash,
					actual: stableHash(recipeCodeEnv(current.recipe,),),
					unchanged: backup.codeEnvHash === stableHash(recipeCodeEnv(current.recipe,),),
				},
			].filter((check,) => typeof check.expected === "string");
			const failures = checks.filter((check,) => !check.unchanged);
			return {
				unchanged: failures.length === 0,
				resource: "recipe",
				name: a[0],
				backupPath,
				checks,
				failures,
			};
		},
		usage: "dss recipe assert-unchanged <name> --since BACKUP [--project-key KEY]",
		description: "Compare current recipe payload, graph, and code env against a backup.",
		examples: [
			"dss recipe assert-unchanged compute_orders --since .dss-backups/recipes/backup.recipe-backup.json",
		],
	},
};
