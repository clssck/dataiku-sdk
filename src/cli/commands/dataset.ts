import { buildDatasetCloneSettings, } from "../../resources/datasets.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { jsonInput, num, schemaColumnsInput, } from "../coerce.js";
import { datasetSourceSummary, } from "../helpers/dataset.js";
import { moveCreatedItemsToZone, resolveFlowZoneIdFromFlags, } from "../helpers/flow-zone.js";
import { enqueueCliWarning, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const datasetCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.datasets.list(f["project-key"] as string | undefined,),
		usage: "dss dataset list [--project-key KEY]",
		description: "List all datasets in a project.",
		examples: ["dss dataset list", "dss dataset list --project-key MYPROJ",],
	},
	rename: {
		handler: async (c, a, f,) => {
			requireArgs(a, 2, "dss dataset rename <oldName> <newName> [--project-key KEY]",);
			await c.datasets.rename(a[0], a[1], f["project-key"] as string | undefined,);
			return { renamed: a[0], to: a[1], };
		},
		usage: "dss dataset rename <oldName> <newName> [--project-key KEY]",
		description: "Rename a dataset (updates downstream flow references).",
		examples: ["dss dataset rename old_ds new_ds",],
	},
	"list-partitions": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss dataset list-partitions <name> [--project-key KEY]",);
			return c.datasets.listPartitions(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss dataset list-partitions <name> [--project-key KEY]",
		description: "List the partitions of a partitioned dataset.",
		examples: ["dss dataset list-partitions events",],
	},
	clear: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dataset clear <name> [--partitions SPEC] [--project-key KEY]",);
			const partitions = f["partitions"] as string | undefined;
			await c.datasets.clear(a[0], partitions, f["project-key"] as string | undefined,);
			return { cleared: a[0], partitions: partitions ?? "ALL", };
		},
		usage: "dss dataset clear <name> [--partitions SPEC] [--project-key KEY]",
		description: "Clear a dataset's data (all, or a partition spec); keeps the dataset.",
		examples: ["dss dataset clear staging", "dss dataset clear events --partitions 2024-01",],
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
		examples: ["dss dataset schema orders",],
	},
	source: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dataset source <name>",);
			return datasetSourceSummary(
				await c.datasets.get(a[0], f["project-key"] as string | undefined,),
			);
		},
		usage: "dss dataset source <name> [--project-key KEY]",
		description: "Show backing connection, catalog/schema/table, path, and format for a dataset.",
		examples: ["dss dataset source orders",],
	},
	"refresh-schema": {
		handler: async (c, a, f,) => {
			const usage =
				"dss dataset refresh-schema <name> [--data JSON | --data-file PATH | --stdin] [--dry-run] [--project-key KEY]";
			requireArgs(a, 1, usage,);
			const columns = schemaColumnsInput(f, usage,);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await c.datasets.schema(a[0], pk,);
				return {
					dryRun: true,
					action: "refresh-schema",
					resource: "dataset",
					name: a[0],
					current,
					next: { columns, },
				};
			}
			await c.datasets.updateSchema(a[0], columns, pk,);
			return { updated: a[0], resource: "dataset", schema: { columns, }, };
		},
		usage:
			"dss dataset refresh-schema <name> (--data JSON | --data-file PATH | --stdin) [--dry-run] [--project-key KEY]",
		description: "Replace a dataset schema through the DSS schema endpoint.",
		examples: [
			`dss dataset refresh-schema orders --data '{"columns":[{"name":"id","type":"bigint"}]}' --dry-run`,
		],
	},
	"validate-build": {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss dataset validate-build <name>",);
			return c.datasets.validateBuildSettings(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss dataset validate-build <name> [--project-key KEY]",
		description: "Check common dataset settings that can make file-backed builds fail.",
		examples: ["dss dataset validate-build orders",],
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
		usage: "dss dataset preview <name> [--max-rows N] [--rows N] [--project-key KEY] [--timeout MS]",
		description: "Preview dataset rows (--rows is an alias for --max-rows).",
		examples: ["dss dataset preview orders", "dss dataset preview orders --rows 5",],
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
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dataset download <name>",);
			const result = await c.datasets.download(a[0], {
				outputPath: f["output"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
				limit: num(f["limit"],),
			},);
			if (result.truncated) {
				enqueueCliWarning({
					code: "dataset_download_truncated",
					message:
						`Download of '${a[0]}' stopped at the ${result.limit}-row cap; the dataset has more rows. `
						+ "Re-run with --limit N for more, or read inside a recipe (get_dataframe) for the full data.",
					dataset: a[0],
					rows: result.rows,
					limit: result.limit,
					path: result.path,
				},);
			}
			if (!f["output"]) {
				enqueueCliWarning({
					code: "dataset_download_default_location",
					message:
						`No --output was given, so '${
							a[0]
						}' was written to '${result.path}' in the current directory. `
						+ "Pass --output PATH to control the destination and avoid writing into your working tree.",
					dataset: a[0],
					path: result.path,
				},);
			}
			return result;
		},
		usage: "dss dataset download <name> [--output PATH] [--limit N] [--project-key KEY]",
		description:
			"Download up to --limit rows (default 100k) as CSV; returns { path, rows, truncated, limit }. When truncated, a dataset_download_truncated warning is written to stderr; when --output is omitted, a dataset_download_default_location warning names the file's path in the working directory so the write is never silent.",
		examples: ["dss dataset download orders", "dss dataset download orders --output ./data/",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const name = f["name"] as string | undefined;
			const connection = f["connection"] as string | undefined;
			const dsType = f["type"] as string | undefined;
			if (!name || !connection || !dsType) {
				throw new UsageError(
					"--name, --connection, and --type are required. Usage: dss dataset create --name NAME --connection CONN --type TYPE",
				);
			}
			const payload = {
				datasetName: name,
				connection,
				dsType,
				projectKey: pk,
			};
			const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const list = await c.datasets.list(pk,);
				const existing = list.find((d,) => d.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("dataset", name, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
					return {
						dryRun: true,
						action: "create",
						resource: "dataset",
						name,
						payload,
						...(existing ? { current: existing, } : {}),
						...(zoneId ? { zoneId, zoneMove: [{ objectId: name, objectType: "DATASET", },], } : {}),
					};
				}
			}
			await c.datasets.create(payload,);
			const moved = await moveCreatedItemsToZone(
				c,
				f,
				[{ objectId: name, objectType: "DATASET", },],
				pk,
			);
			return { created: name, resource: "dataset", ...moved, };
		},
		usage:
			"dss dataset create --name NAME --connection CONN --type TYPE [--zone ZONE|--zone-id ID] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a new dataset.",
		examples: [
			"dss dataset create --name orders --connection filesystem --type Filesystem",
			"dss dataset create --name orders --connection filesystem --type Filesystem --zone Experiments --dry-run",
		],
	},
	clone: {
		handler: async (c, a, f,) => {
			const usage =
				"dss dataset clone <source> <target> [--path PATH] [--table TABLE] [--metastore-table TABLE] [--allow-same-path] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]";
			requireArgs(a, 2, usage,);
			const pk = f["project-key"] as string | undefined;
			const opts = {
				projectKey: pk,
				path: f["path"] as string | undefined,
				table: f["table"] as string | undefined,
				metastoreTableName: f["metastore-table"] as string | undefined,
				allowSamePath: f["allow-same-path"] === true,
			};
			const current = await c.datasets.get(a[0], pk,);
			const next = buildDatasetCloneSettings(current, a[1], pk ?? c.resolveProjectKey(pk,), opts,);
			const zoneId = await resolveFlowZoneIdFromFlags(c, f, pk,);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "clone",
					resource: "dataset",
					source: a[0],
					target: a[1],
					current,
					next,
					...(zoneId ? { zoneId, zoneMove: [{ objectId: a[1], objectType: "DATASET", },], } : {}),
				};
			}
			const cloned = await c.datasets.clone(a[0], a[1], opts,);
			const moved = await moveCreatedItemsToZone(
				c,
				f,
				[{ objectId: a[1], objectType: "DATASET", },],
				pk,
			);
			return { ...cloned, resource: "dataset", ...moved, };
		},
		usage:
			"dss dataset clone <source> <target> [--path PATH] [--table TABLE] [--metastore-table TABLE] [--allow-same-path] [--zone ZONE|--zone-id ID] [--dry-run] [--project-key KEY]",
		description: "Clone dataset settings into a new dataset, with storage/table overrides.",
		examples: [
			"dss dataset clone source_ds experiment_ds --path /dataiku/TEST/experiment_ds --dry-run",
			"dss dataset clone source_ds experiment_ds --allow-same-path",
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss dataset delete <name>",);
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.datasets.get(a[0], pk,));
				if (!current) return skipResult("dataset", a[0], "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "dataset", name: a[0], current, };
				}
			}
			await c.datasets.delete(a[0], pk,);
			return { deleted: a[0], resource: "dataset", };
		},
		usage: "dss dataset delete <name> [--if-exists] [--dry-run] [--project-key KEY]",
		description: "Delete a dataset.",
		examples: ["dss dataset delete orders", "dss dataset delete orders --if-exists",],
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
			const pk = f["project-key"] as string | undefined;
			if (f["dry-run"] === true) {
				const current = await c.datasets.get(a[0], pk,);
				const next = deepMerge(current as unknown as Record<string, unknown>, data,);
				return { dryRun: true, action: "update", resource: "dataset", name: a[0], current, next, };
			}
			await c.datasets.update(a[0], data, pk,);
			return { updated: a[0], resource: "dataset", };
		},
		usage:
			"dss dataset update <name> (--data '{...}' | --data-file PATH | --stdin) [--dry-run] [--project-key KEY]",
		description: "Update dataset settings via JSON merge.",
		examples: [
			'dss dataset update orders --data \'{"tags":["production"]}\' --dry-run',
			"echo '{\"tags\":[]}' | dss dataset update orders --stdin",
		],
	},
};
