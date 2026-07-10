import * as fs from "node:fs";
import * as nodePath from "node:path";
import { Readable, Transform, } from "node:stream";
import { pipeline, } from "node:stream/promises";
import { createGzip, } from "node:zlib";
import { DataikuError, } from "../errors.js";
import {
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";

import type {
	DatasetCreateOptions,
	DatasetDetails,
	DatasetSchema,
	DatasetSummary,
} from "../schemas.js";

export interface DatasetBuildValidationResult {
	valid: boolean;
	datasetName: string;
	projectKey: string;
	type: string | null;
	path: string | null;
	formatType: string | null;
	warnings: string[];
	validationScope: "configuration";
	materializationChecked: false;
	note: string;
}

export interface DatasetSchemaColumnInput {
	name: string;
	type: string;
	comment?: string;
}

export interface DatasetCloneOptions {
	projectKey?: string;
	path?: string;
	table?: string;
	metastoreTableName?: string;
	overrides?: Record<string, unknown>;
	allowSamePath?: boolean;
}

export interface DatasetCloneResult {
	source: string;
	target: string;
	projectKey: string;
	created: Record<string, unknown>;
	settings: Record<string, unknown>;
}
// ---------------------------------------------------------------------------
// Helpers: TSV → CSV streaming conversion
// ---------------------------------------------------------------------------

function csvEscape(field: string,): string {
	if (
		field.includes(",",)
		|| field.includes('"',)
		|| field.includes("\n",)
		|| field.includes("\r",)
		|| field.includes("\t",)
	) {
		return `"${field.replace(/"/g, '""',)}"`;
	}
	return field;
}

interface TsvStreamState {
	currentField: string;
	currentRow: string[];
	inQuotes: boolean;
	pendingQuoteInQuotes: boolean;
}

function createTsvStreamState(): TsvStreamState {
	return {
		currentField: "",
		currentRow: [],
		inQuotes: false,
		pendingQuoteInQuotes: false,
	};
}

function consumeTsvChunk(
	text: string,
	state: TsvStreamState,
	onRow: (row: string[],) => void,
): void {
	let i = 0;

	if (state.pendingQuoteInQuotes) {
		state.pendingQuoteInQuotes = false;
		const first = text[0];
		if (first === '"') {
			state.currentField += '"';
			i = 1;
		} else if (first === "\t" || first === "\n" || first === "\r") {
			state.inQuotes = false;
		} else if (first !== undefined) {
			// Ambiguous terminal quote from previous chunk; keep it as data.
			state.currentField += '"';
		}
	}

	for (; i < text.length; i++) {
		const ch = text[i];

		if (state.inQuotes) {
			if (ch === '"') {
				const next = text[i + 1];
				if (next === '"') {
					state.currentField += '"';
					i++;
					continue;
				}
				if (next === undefined) {
					state.pendingQuoteInQuotes = true;
					continue;
				}
				if (next === "\t" || next === "\n" || next === "\r") {
					state.inQuotes = false;
					continue;
				}
				// Quote in the middle of quoted field text — keep it literal.
				state.currentField += '"';
				continue;
			}
			state.currentField += ch;
			continue;
		}

		if (ch === '"' && state.currentField.length === 0) {
			state.inQuotes = true;
			continue;
		}
		if (ch === "\t") {
			state.currentRow.push(state.currentField,);
			state.currentField = "";
			continue;
		}
		if (ch === "\n") {
			state.currentRow.push(state.currentField,);
			state.currentField = "";
			const row = state.currentRow;
			state.currentRow = [];
			onRow(row,);
			continue;
		}
		if (ch === "\r") {
			continue;
		}

		state.currentField += ch;
	}
}

function flushTsvStream(state: TsvStreamState, onRow: (row: string[],) => void,): void {
	if (state.pendingQuoteInQuotes) {
		state.currentField += '"';
		state.pendingQuoteInQuotes = false;
	}
	if (state.currentField.length === 0 && state.currentRow.length === 0) return;
	state.currentRow.push(state.currentField,);
	state.currentField = "";
	const row = state.currentRow;
	state.currentRow = [];
	onRow(row,);
}

function rowToCsv(row: string[],): string {
	return row.map((field,) => csvEscape(field,)).join(",",);
}

function isBlankRow(row: string[],): boolean {
	return row.length === 1 && row[0].length === 0;
}

/**
 * Compare streamed TSV header columns against a known dataset schema.
 * Returns an array of warning strings (empty if all columns match).
 */
export function validateStreamColumns(
	headerRow: string[],
	expectedColumns: { name: string; }[],
): string[] {
	const warnings: string[] = [];
	const headerSet = new Set(headerRow,);
	const expectedSet = new Set(expectedColumns.map((c,) => c.name),);

	for (const col of expectedColumns) {
		if (!headerSet.has(col.name,)) {
			warnings.push(`Missing expected column: "${col.name}"`,);
		}
	}
	for (const col of headerRow) {
		if (!expectedSet.has(col,)) {
			warnings.push(`Unexpected column in stream: "${col}"`,);
		}
	}
	return warnings;
}

function buildPreviewTimeoutError(timeoutMs: number,): DataikuError {
	return new DataikuError(
		0,
		"Request Timeout",
		`Dataset preview timed out after ${timeoutMs}ms while waiting for rows.`,
	);
}

async function readChunkWithTimeout(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	remainingMs: number,
	timeoutMs: number,
): Promise<Awaited<ReturnType<ReadableStreamDefaultReader<Uint8Array>["read"]>>> {
	return new Promise((resolveChunk, rejectChunk,) => {
		const timer = setTimeout(() => {
			void reader.cancel(buildPreviewTimeoutError(timeoutMs,),).catch(() => {},);
			rejectChunk(buildPreviewTimeoutError(timeoutMs,),);
		}, remainingMs,);
		reader.read().then(
			(result,) => {
				clearTimeout(timer,);
				resolveChunk(result,);
			},
			(error,) => {
				clearTimeout(timer,);
				rejectChunk(error,);
			},
		);
	},);
}

async function collectPreviewRows(
	body: ReadableStream<Uint8Array>,
	maxDataRows: number,
	timeoutMs: number,
	onHeader?: (headerRow: string[],) => void,
): Promise<{ columns: string[]; rows: string[][]; }> {
	const state = createTsvStreamState();
	let columns: string[] | undefined;
	const rows: string[][] = [];
	let done = false;
	const startedAt = Date.now();
	const reader = body.getReader();

	const handleRow = (row: string[],): void => {
		if (done || isBlankRow(row,)) return;
		if (columns === undefined) {
			columns = row;
			onHeader?.(row,);
			return;
		}
		if (rows.length >= maxDataRows) {
			done = true;
			return;
		}
		rows.push(row,);
		if (rows.length >= maxDataRows) done = true;
	};

	try {
		while (true) {
			if (done) {
				void reader.cancel().catch(() => {},);
				break;
			}
			const remainingMs = timeoutMs - (Date.now() - startedAt);
			if (remainingMs <= 0) throw buildPreviewTimeoutError(timeoutMs,);
			const result = await readChunkWithTimeout(reader, remainingMs, timeoutMs,);
			if (result.done) break;
			consumeTsvChunk(Buffer.from(result.value,).toString("utf-8",), state, handleRow,);
		}

		if (!done) flushTsvStream(state, handleRow,);

		return { columns: columns ?? [], rows, };
	} finally {
		reader.releaseLock();
	}
}

function tsvToCsvTransform(
	maxDataRows: number,
	stats: { rows: number; truncated: boolean; },
	onHeader?: (headerRow: string[],) => void,
): Transform {
	const state = createTsvStreamState();
	const maxRows = Math.max(1, maxDataRows,);
	let headerSeen = false;
	let done = false;

	const handleRow = (row: string[], push: (line: string,) => void,): void => {
		if (done || isBlankRow(row,)) return;
		if (!headerSeen) {
			headerSeen = true;
			onHeader?.(row,);
			push(`${rowToCsv(row,)}\n`,);
			return;
		}
		if (stats.rows >= maxRows) {
			stats.truncated = true;
			done = true;
			return;
		}
		push(`${rowToCsv(row,)}\n`,);
		stats.rows += 1;
	};

	return new Transform({
		transform(chunk: Buffer, _encoding, callback,) {
			if (done) {
				callback();
				return;
			}
			consumeTsvChunk(
				chunk.toString("utf-8",),
				state,
				(row,) => handleRow(row, (line,) => this.push(line,),),
			);
			if (done) this.push(null,);
			callback();
		},
		flush(callback,) {
			if (!done) {
				flushTsvStream(state, (row,) => handleRow(row, (line,) => this.push(line,),),);
			}
			callback();
		},
	},);
}

// ---------------------------------------------------------------------------
// Helpers: dataset creation
// ---------------------------------------------------------------------------

const DEFAULT_DATABASE_DATASET_TYPE = "Snowflake";
const DEFAULT_FILESYSTEM_DATASET_TYPE = "Filesystem";

function shouldRetryWithConnectionInferredType(error: unknown,): error is DataikuError {
	if (!(error instanceof DataikuError)) return false;
	if (error.category !== "validation" && error.category !== "unknown") return false;
	const detail = `${error.statusText ?? ""}\n${error.body ?? ""}`.toLowerCase();
	return (
		detail.includes("connection",)
		|| detail.includes("dataset type",)
		|| detail.includes("invalid type",)
		|| detail.includes("illegal argument",)
	);
}

function buildDatasetCreateBody(opts: {
	projectKey: string;
	datasetName: string;
	connection: string;
	dsType: string;
	table?: string;
	dbSchema?: string;
	catalog?: string;
	formatType?: string;
	formatParams?: Record<string, unknown>;
	managed?: boolean;
},): Record<string, unknown> {
	if (opts.table) {
		const params: Record<string, unknown> = {
			connection: opts.connection,
			mode: "table",
			table: opts.table,
		};
		if (opts.dbSchema) params.schema = opts.dbSchema;
		if (opts.catalog) params.catalog = opts.catalog;

		return {
			projectKey: opts.projectKey,
			name: opts.datasetName,
			type: opts.dsType,
			params,
			managed: opts.managed ?? false,
		};
	}

	return {
		projectKey: opts.projectKey,
		name: opts.datasetName,
		type: opts.dsType,
		params: {
			connection: opts.connection,
			path: `/dataiku/${opts.projectKey}/${opts.datasetName}`,
		},
		formatType: opts.formatType ?? "csv",
		formatParams: opts.formatParams ?? {
			style: "excel",
			charset: "utf8",
			separator: "\t",
			quoteChar: '"',
			escapeChar: "\\",
			dateSerializationFormat: "ISO",
			arrayMapFormat: "json",
			parseHeaderRow: true,
			compress: "gz",
		},
		managed: opts.managed ?? true,
	};
}

const DATASET_CLONE_PARAM_KEYS = [
	"connection",
	"uploadConnection",
	"path",
	"table",
	"schema",
	"catalog",
	"folderSmartId",
	"metastoreTableName",
	"mode",
] as const;

function cloneDatasetParams(params: DatasetDetails["params"],): Record<string, unknown> {
	const sourceParams = params && typeof params === "object" && !Array.isArray(params,)
		? params as Record<string, unknown>
		: {};
	const cloned: Record<string, unknown> = {};
	for (const key of DATASET_CLONE_PARAM_KEYS) {
		const value = sourceParams[key];
		if (value !== undefined) cloned[key] = value;
	}
	return cloned;
}

export function buildDatasetCloneSettings(
	source: DatasetDetails,
	targetName: string,
	projectKey: string,
	opts: DatasetCloneOptions,
): Record<string, unknown> {
	const params = {
		...cloneDatasetParams(source.params,),
		...(opts.path !== undefined ? { path: opts.path, } : {}),
		...(opts.table !== undefined ? { table: opts.table, mode: "table", } : {}),
		...(opts.metastoreTableName !== undefined
			? { metastoreTableName: opts.metastoreTableName, }
			: {}),
	};
	const cloned: Record<string, unknown> = {
		name: targetName,
		projectKey,
		...(source.type !== undefined ? { type: source.type, } : {}),
		...(source.managed !== undefined ? { managed: source.managed, } : {}),
		...(Object.keys(params,).length > 0 ? { params, } : {}),
		...(source.formatType !== undefined ? { formatType: source.formatType, } : {}),
		...(source.formatParams !== undefined ? { formatParams: source.formatParams, } : {}),
		...(source.schema !== undefined ? { schema: source.schema, } : {}),
	};
	const settings = opts.overrides ? deepMerge(cloned, opts.overrides,) : cloned;
	const settingsParams =
		settings.params && typeof settings.params === "object" && !Array.isArray(settings.params,)
			? settings.params as Record<string, unknown>
			: {};
	const sourcePath = typeof source.params?.path === "string" ? source.params.path : undefined;
	if (
		opts.allowSamePath !== true
		&& source.managed === true
		&& sourcePath !== undefined
		&& settingsParams.path === sourcePath
	) {
		throw new Error(
			`Refusing to clone managed dataset "${source.name}" with the same storage path. Pass a new path or allowSamePath: true.`,
		);
	}
	return settings;
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

export class DatasetsResource extends BaseResource {
	/** List all datasets in a project. */
	async list(projectKey?: string,): Promise<DatasetSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/`,
		);
		return this.client.safeParse(DatasetSummaryArraySchema, raw, "datasets.list",);
	}

	/** Get full dataset details. */
	async get(datasetName: string, projectKey?: string,): Promise<DatasetDetails> {
		const dsEnc = encodeURIComponent(datasetName,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}`,
		);
		return this.client.safeParse(DatasetDetailsSchema, raw, "datasets.get",);
	}

	/** Get dataset schema (column names and types). */
	async schema(datasetName: string, projectKey?: string,): Promise<DatasetSchema> {
		const dsEnc = encodeURIComponent(datasetName,);
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/schema`,
		);
		return this.client.safeParse(DatasetSchemaSchema, raw, "datasets.schema",);
	}

	/** Replace dataset schema columns directly through the schema endpoint. */
	async updateSchema(
		datasetName: string,
		columns: DatasetSchemaColumnInput[],
		projectKey?: string,
	): Promise<void> {
		const dsEnc = encodeURIComponent(datasetName,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/schema`,
			{ columns, },
		);
	}

	/**
	 * Preview dataset rows as structured data: column names plus row arrays,
	 * mirroring the sql query shape ({ columns, rows, rowCount }). Streams TSV
	 * from the API and returns up to `maxRows` data rows.
	 *
	 * If `validateColumns` is provided, the first TSV row (header) is checked
	 * against the column names. Mismatches emit a warning via onValidationWarning.
	 */
	async preview(
		datasetName: string,
		opts?: {
			maxRows?: number;
			projectKey?: string;
			validateColumns?: { name: string; }[];
			timeoutMs?: number;
		},
	): Promise<{ columns: Array<{ name: string; }>; rows: string[][]; rowCount: number; }> {
		const maxRows = Math.max(1, Math.min(opts?.maxRows ?? 50, 500,),);
		const timeoutMs = Math.max(1, opts?.timeoutMs ?? this.client.getRequestTimeoutMs(),);
		const dsEnc = encodeURIComponent(datasetName,);
		const res = await this.client.stream(
			`/public/api/projects/${
				this.enc(opts?.projectKey,)
			}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${maxRows}`,
		);
		const onHeader = opts?.validateColumns
			? (headerRow: string[],) => {
				const warnings = validateStreamColumns(headerRow, opts.validateColumns!,);
				if (warnings.length > 0) {
					this.client.warn(`datasets.preview(${datasetName})`, warnings,);
				}
			}
			: undefined;
		if (!res.body) return { columns: [], rows: [], rowCount: 0, };
		const { columns, rows, } = await collectPreviewRows(
			res.body as ReadableStream<Uint8Array>,
			maxRows,
			timeoutMs,
			onHeader,
		);
		return { columns: columns.map((name,) => ({ name, })), rows, rowCount: rows.length, };
	}

	/** Get dataset metadata (tags, custom fields, checklists). */
	async metadata(datasetName: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const dsEnc = encodeURIComponent(datasetName,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/metadata`,
		);
	}

	/**
	 * Download dataset rows as a gzipped (or .csv) file, capped at `limit` rows
	 * (default 100k). Returns the written path, the number of data rows written,
	 * whether the dataset had more rows than the cap (truncated), and the limit
	 * used — so callers can detect truncation instead of silently getting a sample.
	 */
	async download(
		datasetName: string,
		opts?: {
			outputPath?: string;
			projectKey?: string;
			validateColumns?: { name: string; }[];
			limit?: number;
		},
	): Promise<{ path: string; rows: number; truncated: boolean; limit: number; }> {
		const limit = Math.max(1, opts?.limit ?? 100_000,);
		const dsEnc = encodeURIComponent(datasetName,);
		const res = await this.client.stream(
			`/public/api/projects/${
				this.enc(opts?.projectKey,)
			}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${limit + 1}`,
		);

		const safeDatasetName = sanitizeFileName(datasetName, "dataset",);
		const filePath = opts?.outputPath?.endsWith(".gz",) || opts?.outputPath?.endsWith(".csv",)
			? nodePath.resolve(opts.outputPath,)
			: nodePath.resolve(opts?.outputPath ?? process.cwd(), `${safeDatasetName}.csv.gz`,);

		const onHeader = opts?.validateColumns
			? (headerRow: string[],) => {
				const warnings = validateStreamColumns(headerRow, opts.validateColumns!,);
				if (warnings.length > 0) {
					this.client.warn(`datasets.download(${datasetName})`, warnings,);
				}
			}
			: undefined;

		const shouldGzip = filePath.endsWith(".gz",);
		const stats = { rows: 0, truncated: false, };
		const nodeStream = Readable.fromWeb(res.body as unknown as import("stream/web").ReadableStream,);
		const csvTransform = tsvToCsvTransform(limit, stats, onHeader,);
		fs.mkdirSync(nodePath.dirname(filePath,), { recursive: true, },);
		const fileOut = fs.createWriteStream(filePath,);

		if (shouldGzip) {
			const gzip = createGzip();
			await pipeline(nodeStream, csvTransform, gzip, fileOut,);
		} else {
			await pipeline(nodeStream, csvTransform, fileOut,);
		}

		return { path: filePath, rows: stats.rows, truncated: stats.truncated, limit, };
	}

	/**
	 * Create a new dataset.
	 *
	 * If `dsType` is not provided, a default is inferred from whether `table`
	 * is specified. On failure, the method retries once using a type inferred
	 * from existing datasets on the same connection.
	 */
	async create(opts: DatasetCreateOptions,): Promise<Record<string, unknown>> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const enc = encodeURIComponent(pk,);

		const explicitType = opts.dsType;
		let dsType = explicitType
			?? (opts.table ? DEFAULT_DATABASE_DATASET_TYPE : DEFAULT_FILESYSTEM_DATASET_TYPE);

		let body = buildDatasetCreateBody({
			projectKey: pk,
			datasetName: opts.datasetName,
			connection: opts.connection,
			dsType,
			table: opts.table,
			dbSchema: opts.dbSchema,
			catalog: opts.catalog,
			formatType: opts.formatType,
			formatParams: opts.formatParams,
			managed: opts.managed,
		},);

		try {
			return await this.client.post<Record<string, unknown>>(
				`/public/api/projects/${enc}/datasets/`,
				body,
			);
		} catch (error) {
			if (explicitType || !shouldRetryWithConnectionInferredType(error,)) {
				throw error;
			}

			// Infer type from existing datasets on the same connection.
			const existing = await this.client.get<
				Array<{ type?: string; params?: { connection?: string; }; }>
			>(`/public/api/projects/${enc}/datasets/`,);

			const inferredType = existing.find(
				(d,) => d.params?.connection === opts.connection && d.type,
			)?.type;

			if (!inferredType || inferredType === dsType) {
				throw error;
			}

			dsType = inferredType;
			body = buildDatasetCreateBody({
				projectKey: pk,
				datasetName: opts.datasetName,
				connection: opts.connection,
				dsType,
				table: opts.table,
				dbSchema: opts.dbSchema,
				catalog: opts.catalog,
				formatType: opts.formatType,
				formatParams: opts.formatParams,
				managed: opts.managed,
			},);

			return this.client.post<Record<string, unknown>>(
				`/public/api/projects/${enc}/datasets/`,
				body,
			);
		}
	}

	/** Validate common build blockers before running a dataset build. */
	async validateBuildSettings(
		datasetName: string,
		projectKey?: string,
	): Promise<DatasetBuildValidationResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const details = await this.get(datasetName, pk,);
		const params = details.params ?? {};
		const type = details.type ?? null;
		const path = typeof params.path === "string" && params.path.trim().length > 0
			? params.path
			: null;
		const table = typeof params.table === "string" && params.table.trim().length > 0
			? params.table
			: null;
		const normalizedType = (type ?? "").toLowerCase();
		const fileBacked = !table
			&& (normalizedType.includes("filesystem",)
				|| normalizedType.includes("uploaded",)
				|| normalizedType.includes("s3",)
				|| path !== null);
		const formatType = details.formatType ?? null;
		const warnings: string[] = [];

		if (fileBacked && !path) {
			warnings.push("File-backed dataset has no writable storage path configured.",);
		}
		if (fileBacked && !formatType) {
			warnings.push("File-backed dataset has no formatType configured.",);
		}

		return {
			valid: warnings.length === 0,
			datasetName,
			projectKey: pk,
			type,
			path,
			formatType,
			warnings,
			validationScope: "configuration",
			materializationChecked: false,
			note:
				"Validates build configuration only; it does not verify storage file existence or materialized data.",
		};
	}

	/** Clone dataset settings, preserving connection/storage, format, and schema fields. */
	async clone(
		sourceName: string,
		targetName: string,
		opts: DatasetCloneOptions = {},
	): Promise<DatasetCloneResult> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const settings = buildDatasetCloneSettings(
			await this.get(sourceName, pk,),
			targetName,
			pk,
			opts,
		);
		const created = await this.client.post<Record<string, unknown>>(
			`/public/api/projects/${encodeURIComponent(pk,)}/datasets/`,
			settings,
		);
		return { source: sourceName, target: targetName, projectKey: pk, created, settings, };
	}

	/** Update a dataset by deep-merging a patch into the current definition. */
	async update(
		datasetName: string,
		data: Record<string, unknown>,
		projectKey?: string,
	): Promise<void> {
		const dsEnc = encodeURIComponent(datasetName,);
		const pkEnc = this.enc(projectKey,);
		const current = await this.client.get<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/datasets/${dsEnc}`,
		);
		const merged = deepMerge(current, data,);
		await this.client.put<Record<string, unknown>>(
			`/public/api/projects/${pkEnc}/datasets/${dsEnc}`,
			merged,
		);
	}

	/** Delete a dataset. */
	async delete(datasetName: string, projectKey?: string,): Promise<void> {
		const dsEnc = encodeURIComponent(datasetName,);
		await this.client.del(`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}`,);
	}

	/** List all partitions of a partitioned dataset. */
	async listPartitions(datasetName: string, projectKey?: string,): Promise<string[]> {
		const dsEnc = encodeURIComponent(datasetName,);
		return this.client.get<string[]>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/partitions`,
		);
	}

	/** Clear a dataset's data. Omit `partitions` to clear the whole dataset. */
	async clear(datasetName: string, partitions?: string, projectKey?: string,): Promise<void> {
		const dsEnc = encodeURIComponent(datasetName,);
		const query = partitions ? `?partitions=${encodeURIComponent(partitions,)}` : "";
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/data${query}`,
		);
	}

	/** Rename a dataset, updating downstream flow references. */
	async rename(oldName: string, newName: string, projectKey?: string,): Promise<void> {
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/actions/renameDataset`,
			{ oldName, newName, },
		);
	}
}
