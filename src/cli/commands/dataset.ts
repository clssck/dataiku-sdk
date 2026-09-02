import { buildDatasetCloneSettings, } from "../../resources/datasets.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { compareStrings, stableHash, } from "../../utils/stable-hash.js";
import { jsonInput, num, schemaColumnsInput, unknownJsonInput, } from "../coerce.js";
import { datasetSourceSummary, } from "../helpers/dataset.js";
import { moveCreatedItemsToZone, resolveFlowZoneIdFromFlags, } from "../helpers/flow-zone.js";
import { enqueueCliWarning, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

// ---------------------------------------------------------------------------
// Assertions: deterministic schema comparison
// ---------------------------------------------------------------------------

export interface DatasetSchemaDifference {
	/** Path from the schema root in dot/bracket notation, e.g. `columns[2].type`. */
	path: string;
	kind: "added" | "removed" | "changed";
	/** SHA-256 of the deterministic JSON rendering of the expected subtree. */
	expectedHash?: string;
	/** SHA-256 of the deterministic JSON rendering of the actual subtree. */
	actualHash?: string;
}

export interface DatasetSchemaComparison {
	equal: boolean;
	expectedHash: string;
	actualHash: string;
	differences: DatasetSchemaDifference[];
	totalDifferences: number;
	differencesTruncated: boolean;
}

const MAX_SCHEMA_DIFFERENCES = 50;

function isPlainObject(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

function appendSchemaPath(parent: string, key: string,): string {
	return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key,)
		? (parent ? `${parent}.${key}` : key)
		: `${parent}[${JSON.stringify(key,)}]`;
}

function collectSchemaDifferences(
	expected: unknown,
	actual: unknown,
	path: string,
	acc: { items: DatasetSchemaDifference[]; total: number; },
): void {
	// Hash equality is the deterministic equivalence test: structurally equal
	// subtrees (any key order) match exactly and are never descended into.
	if (stableHash(expected,) === stableHash(actual,)) return;

	if (isPlainObject(expected,) && isPlainObject(actual,)) {
		const keys = [...new Set([...Object.keys(expected,), ...Object.keys(actual,),],),].sort(
			compareStrings,
		);
		for (const key of keys) {
			const nextPath = appendSchemaPath(path, key,);
			const hasExpected = key in expected;
			const hasActual = key in actual;
			if (hasExpected && hasActual) {
				collectSchemaDifferences(expected[key], actual[key], nextPath, acc,);
				continue;
			}
			acc.total += 1;
			if (acc.items.length < MAX_SCHEMA_DIFFERENCES) {
				acc.items.push({
					path: nextPath,
					kind: hasActual ? "added" : "removed",
					...(hasActual
						? { actualHash: stableHash(actual[key],), }
						: { expectedHash: stableHash(expected[key],), }),
				},);
			}
		}
		return;
	}

	if (Array.isArray(expected,) && Array.isArray(actual,)) {
		const shared = Math.min(expected.length, actual.length,);
		for (let i = 0; i < shared; i++) {
			collectSchemaDifferences(expected[i], actual[i], `${path}[${i}]`, acc,);
		}
		for (let i = shared; i < expected.length; i++) {
			acc.total += 1;
			if (acc.items.length < MAX_SCHEMA_DIFFERENCES) {
				acc.items.push({
					path: `${path}[${i}]`,
					kind: "removed",
					expectedHash: stableHash(expected[i],),
				},);
			}
		}
		for (let i = shared; i < actual.length; i++) {
			acc.total += 1;
			if (acc.items.length < MAX_SCHEMA_DIFFERENCES) {
				acc.items.push({
					path: `${path}[${i}]`,
					kind: "added",
					actualHash: stableHash(actual[i],),
				},);
			}
		}
		return;
	}

	acc.total += 1;
	if (acc.items.length < MAX_SCHEMA_DIFFERENCES) {
		acc.items.push({
			path: path || "$",
			kind: "changed",
			expectedHash: stableHash(expected,),
			actualHash: stableHash(actual,),
		},);
	}
}

/**
 * Compare two schema values deterministically — object key order never matters
 * — and report each structural difference as a concise per-path entry carrying
 * subtree hashes instead of subtree values.
 */
export function compareDatasetSchemas(
	expected: unknown,
	actual: unknown,
): DatasetSchemaComparison {
	const acc = { items: [], total: 0, } as {
		items: DatasetSchemaDifference[];
		total: number;
	};
	collectSchemaDifferences(expected, actual, "", acc,);
	return {
		equal: acc.items.length === 0,
		expectedHash: stableHash(expected,),
		actualHash: stableHash(actual,),
		differences: acc.items,
		totalDifferences: acc.total,
		differencesTruncated: acc.items.length < acc.total,
	};
}

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
	files: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss dataset files <name> [--project-key KEY]",);
			return c.datasets.listUploadedFiles(a[0], f["project-key"] as string | undefined,);
		},
		usage: "dss dataset files <name> [--project-key KEY]",
		description: "List files stored by an UploadedFiles dataset.",
		examples: ["dss dataset files uploaded_input",],
	},
	"upload-file": {
		handler: (c, a, f,) => {
			const usage = "dss dataset upload-file <name> <localPath> --file-name NAME [--project-key KEY]";
			requireArgs(a, 2, usage,);
			const fileName = f["file-name"] as string | undefined;
			if (!fileName || fileName.trim() === "") {
				throw new UsageError(`--file-name is required. Usage: ${usage}`,);
			}
			return c.datasets.uploadDatasetFile(a[0], a[1], {
				fileName,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		usage: "dss dataset upload-file <name> <localPath> --file-name NAME [--project-key KEY]",
		description: "Add one new file to an UploadedFiles dataset and verify the stored byte length.",
		examples: [
			"dss dataset upload-file uploaded_input ./input.csv --file-name input.csv",
		],
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
		handler: async (c, a, f,) => {
			const previewUsage =
				"dss dataset preview <name> [--max-rows N] [--rows N] [--project-key KEY] [--timeout MS]";
			requireArgs(a, 1, previewUsage,);
			const maxRows = num(f["max-rows"], "--max-rows",);
			if (
				maxRows !== undefined && (!Number.isSafeInteger(maxRows,) || maxRows <= 0 || maxRows > 500)
			) {
				throw new UsageError(
					`--max-rows must be a positive integer no greater than 500 (got "${String(f["max-rows"],)}"). `
						+ `Usage: ${previewUsage}`,
					"validation_failed",
				);
			}
			const result = await c.datasets.preview(a[0], {
				maxRows,
				projectKey: f["project-key"] as string | undefined,
				timeoutMs: num(f["timeout"], "--timeout",),
			},);
			if (result.truncated) {
				enqueueCliWarning({
					code: "dataset_preview_truncated",
					message: `Preview of '${
						a[0]
					}' stopped at the ${result.limit}-row cap; the dataset has more rows. ${
						result.limit >= 500
							? "The preview cap of 500 rows is the public maximum; use `dss dataset download` for more rows."
							: "Re-run with --max-rows N (up to 500) for more rows."
					}`,
					dataset: a[0],
					rows: result.rowCount,
					limit: result.limit,
				},);
			}
			return result;
		},
		usage: "dss dataset preview <name> [--max-rows N] [--rows N] [--project-key KEY] [--timeout MS]",
		description:
			"Preview dataset rows (--rows is an alias for --max-rows). Returns { columns, rows, rowCount, truncated, limit }; when the dataset has more rows than the cap, truncated is true and a dataset_preview_truncated warning is written to stderr.",
		examples: ["dss dataset preview orders", "dss dataset preview orders --rows 5",],
	},
	"assert-count": {
		handler: async (c, a, f,) => {
			const assertUsage = "dss dataset assert-count <dataset> --expected N [--project-key KEY]";
			requireArgs(a, 1, assertUsage,);
			const expected = num(f["expected"], "--expected",);
			if (expected === undefined || !Number.isSafeInteger(expected,) || expected < 0) {
				throw new UsageError(
					`--expected must be a non-negative integer (got "${String(f["expected"],)}"). `
						+ `Usage: ${assertUsage}`,
					"validation_failed",
				);
			}
			const result = await c.datasets.assertRowCount(
				a[0],
				expected,
				f["project-key"] as string | undefined,
			);
			return { ...result, dataset: a[0], };
		},
		usage: "dss dataset assert-count <dataset> --expected N [--project-key KEY]",
		description:
			"Stream the dataset and assert it holds exactly --expected rows, probing at most N+1 rows. Returns { satisfied, expected, count, exact }; a mismatch exits 4 with assertion_failed.",
		examples: ["dss dataset assert-count orders --expected 1000",],
	},
	"assert-schema": {
		handler: async (c, a, f,) => {
			const assertUsage =
				"dss dataset assert-schema <dataset> (--data JSON | --data-file PATH | --stdin) [--project-key KEY]";
			requireArgs(a, 1, assertUsage,);
			const expected = unknownJsonInput(f,);
			if (expected === undefined) {
				throw new UsageError(
					`--data, --data-file, or --stdin is required. Usage: ${assertUsage}`,
				);
			}
			const actual = await c.datasets.getSchemaObject(
				a[0],
				f["project-key"] as string | undefined,
			);
			const comparison = compareDatasetSchemas(expected, actual,);
			return {
				satisfied: comparison.equal,
				dataset: a[0],
				expectedHash: comparison.expectedHash,
				actualHash: comparison.actualHash,
				totalDifferences: comparison.totalDifferences,
				...(comparison.differencesTruncated ? { differencesTruncated: true, } : {}),
				differences: comparison.differences,
			};
		},
		usage:
			"dss dataset assert-schema <dataset> (--data JSON | --data-file PATH | --stdin) [--project-key KEY]",
		description:
			"Assert the dataset's full schema equals the expected schema object; reports concise per-path differences with sha256 subtree hashes. A mismatch exits 4 with assertion_failed.",
		examples: [
			`dss dataset assert-schema orders --data-file expected-schema.json`,
			`dss dataset assert-schema orders --data '{"columns":[]}'`,
		],
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
				limit: num(f["limit"], "--limit",),
				rawData: f["raw-data"] === true,
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
		usage: "dss dataset download <name> [--output PATH] [--limit N] [--raw-data] [--project-key KEY]",
		description:
			"Download up to --limit rows (default 100k) as CSV and return { path, rows, truncated, limit }. Formula-like cells are neutralized for spreadsheets; --raw-data preserves exact bytes. Warnings report truncation and the default output path.",
		examples: [
			"dss dataset download orders",
			"dss dataset download orders --output ./data/",
			"dss dataset download orders --raw-data",
		],
	},
	create: {
		handler: async (c, _a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const name = f["name"] as string | undefined;
			const connection = f["connection"] as string | undefined;
			const dsType = f["type"] as string | undefined;
			if (!name || !dsType) {
				throw new UsageError(
					"--name and --type are required. Usage: dss dataset create --name NAME --type TYPE [--connection CONN]",
				);
			}
			if (!connection && dsType.toLowerCase() !== "uploadedfiles") {
				throw new UsageError("--connection is required unless --type is UploadedFiles.",);
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
			"dss dataset create --name NAME --type TYPE [--connection CONN] [--zone ZONE|--zone-id ID] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description:
			"Create a new dataset. UploadedFiles uses the server's default upload connection when omitted.",
		examples: [
			"dss dataset create --name uploads --type UploadedFiles",
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
