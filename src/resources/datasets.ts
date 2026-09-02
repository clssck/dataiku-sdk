import * as fs from "node:fs";
import * as nodePath from "node:path";
import { Readable, Transform, } from "node:stream";
import { pipeline, } from "node:stream/promises";
import { StringDecoder, } from "node:string_decoder";
import { createGzip, } from "node:zlib";
import { ClientValidationError, DataikuError, } from "../errors.js";
import {
	DatasetDetailsSchema,
	DatasetSchemaSchema,
	DatasetSummaryArraySchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { sanitizeFileName, } from "../utils/sanitize.js";
import { BaseResource, } from "./base.js";
import { resolveAdminManagedStorageConnection, } from "./connections.js";

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
export interface UploadedFileMetadata {
	filename: string;
	path?: string;
	length?: number;
	mime?: string;
	weird?: boolean;
	[key: string]: unknown;
}

export interface UploadDatasetFileOptions {
	projectKey?: string;
	fileName: string;
}

export interface UploadDatasetFileResult {
	datasetName: string;
	projectKey: string;
	fileName: string;
	bytes: number;
	after: UploadedFileMetadata;
}
// ---------------------------------------------------------------------------
// Helpers: TSV → CSV streaming conversion
// ---------------------------------------------------------------------------

/**
 * Decoded-byte budgets for the streaming TSV parser. Each budget fails closed
 * — the response is cancelled and any partial download removed — so a dataset
 * holding one oversized field, an unterminated quoted row, or a huge response
 * can never accumulate unbounded memory or CPU before row limits apply.
 */
const TSV_MAX_FIELD_BYTES = 16 * 1024 * 1024; // 16 MiB per decoded field
const TSV_MAX_ROW_BYTES = 64 * 1024 * 1024; // 64 MiB per decoded row
const TSV_MAX_RESPONSE_BYTES = 16 * 1024 * 1024 * 1024; // 16 GiB per decoded response

interface TsvLimits {
	maxFieldBytes: number;
	maxRowBytes: number;
	maxResponseBytes: number;
}

function formatLimitBytes(bytes: number,): string {
	if (bytes >= 1024 * 1024 * 1024) return `${bytes / (1024 * 1024 * 1024)} GiB`;
	if (bytes >= 1024 * 1024) return `${bytes / (1024 * 1024)} MiB`;
	return `${bytes} B`;
}

function tsvLimitError(what: "field" | "row" | "response", limit: number,): DataikuError {
	return new DataikuError(
		0,
		"Dataset TSV Limit",
		`Dataset TSV ${what} exceeded ${
			formatLimitBytes(limit,)
		}; streaming was aborted to guard against unbounded memory use.`,
	);
}

/** Characters that evaluate a spreadsheet cell as a formula. */
const FORMULA_SIGILS = "=+-@";

/**
 * True when the first effective character of `field` — the first character
 * that is not leading whitespace/control — is a spreadsheet formula sigil.
 * Spreadsheet applications evaluate such cells even when CSV-quoted, so an
 * untrusted dataset producer could plant formulas that execute on open.
 */
function hasFormulaPrefix(field: string,): boolean {
	for (let i = 0; i < field.length; i++) {
		const code = field.charCodeAt(i,);
		if (code <= 0x20 || (code >= 0x7f && code <= 0x9f)) continue;
		return FORMULA_SIGILS.includes(field[i],);
	}
	return false;
}

/**
 * Escape one TSV field for CSV output. In spreadsheet-safe mode (the default
 * for dataset downloads) a field whose first effective character is a formula
 * sigil gets a leading apostrophe so spreadsheet importers show text instead
 * of executing it; `rawData: true` (CLI `--raw-data`) preserves exact bytes.
 */
function csvEscape(field: string, spreadsheetSafe: boolean,): string {
	if (spreadsheetSafe && hasFormulaPrefix(field,)) {
		field = `'${field}`;
	}
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
	fieldBytes: number;
	rowBytes: number;
	responseBytes: number;
	limits: TsvLimits;
}

function createTsvStreamState(
	limits: TsvLimits = {
		maxFieldBytes: TSV_MAX_FIELD_BYTES,
		maxRowBytes: TSV_MAX_ROW_BYTES,
		maxResponseBytes: TSV_MAX_RESPONSE_BYTES,
	},
): TsvStreamState {
	return {
		currentField: "",
		currentRow: [],
		inQuotes: false,
		pendingQuoteInQuotes: false,
		fieldBytes: 0,
		rowBytes: 0,
		responseBytes: 0,
		limits,
	};
}

/** Append decoded field data, bounding field/row bytes and failing closed. */
function appendFieldText(state: TsvStreamState, text: string,): void {
	if (text.length === 0) return;
	const bytes = Buffer.byteLength(text, "utf-8",);
	state.fieldBytes += bytes;
	state.rowBytes += bytes;
	if (state.fieldBytes > state.limits.maxFieldBytes) {
		throw tsvLimitError("field", state.limits.maxFieldBytes,);
	}
	if (state.rowBytes > state.limits.maxRowBytes) {
		throw tsvLimitError("row", state.limits.maxRowBytes,);
	}
	state.currentField += text;
}

/** Close a field at a tab/newline delimiter and reset the field byte count. */
function endField(state: TsvStreamState,): void {
	state.currentRow.push(state.currentField,);
	state.currentField = "";
	state.fieldBytes = 0;
	state.rowBytes += 1; // count the delimiter byte
}

function consumeTsvChunk(
	text: string,
	state: TsvStreamState,
	onRow: (row: string[],) => void,
): void {
	// Response budget: bound total decoded bytes regardless of row structure.
	state.responseBytes += Buffer.byteLength(text, "utf-8",);
	if (state.responseBytes > state.limits.maxResponseBytes) {
		throw tsvLimitError("response", state.limits.maxResponseBytes,);
	}

	let i = 0;

	if (state.pendingQuoteInQuotes) {
		state.pendingQuoteInQuotes = false;
		const first = text[0];
		if (first === '"') {
			appendFieldText(state, '"',);
			i = 1;
		} else if (first === "\t" || first === "\n" || first === "\r") {
			state.inQuotes = false;
		} else if (first !== undefined) {
			// Ambiguous terminal quote from previous chunk; keep it as data.
			appendFieldText(state, '"',);
		}
	}

	while (i < text.length) {
		if (state.inQuotes) {
			const quoteIdx = text.indexOf('"', i,);
			if (quoteIdx === -1) {
				appendFieldText(state, text.slice(i,),);
				break;
			}
			if (quoteIdx > i) appendFieldText(state, text.slice(i, quoteIdx,),);
			const next = text[quoteIdx + 1];
			if (next === '"') {
				appendFieldText(state, '"',);
				i = quoteIdx + 2;
				continue;
			}
			if (next === undefined) {
				state.pendingQuoteInQuotes = true;
				break;
			}
			if (next === "\t" || next === "\n" || next === "\r") {
				state.inQuotes = false;
				i = quoteIdx + 1;
				continue;
			}
			// Quote in the middle of quoted field text — keep it literal.
			appendFieldText(state, '"',);
			i = quoteIdx + 1;
			continue;
		}

		// Scan ahead in whole runs to the next structural character, so field
		// data is appended in slices instead of one character at a time.
		let special = -1;
		for (let j = i; j < text.length; j++) {
			const ch = text[j];
			if (ch === '"' || ch === "\t" || ch === "\n" || ch === "\r") {
				special = j;
				break;
			}
		}
		const runEnd = special === -1 ? text.length : special;
		if (runEnd > i) {
			appendFieldText(state, text.slice(i, runEnd,),);
			i = runEnd;
		}
		if (special === -1) break;
		const ch = text[special];
		if (ch === '"') {
			if (state.currentField.length === 0) {
				state.inQuotes = true;
			} else {
				// Quote in the middle of unquoted field text — keep it literal.
				appendFieldText(state, '"',);
			}
			i = special + 1;
			continue;
		}
		if (ch === "\t") {
			endField(state,);
			i = special + 1;
			continue;
		}
		if (ch === "\n") {
			endField(state,);
			i = special + 1;
			const row = state.currentRow;
			state.currentRow = [];
			state.rowBytes = 0;
			onRow(row,);
			continue;
		}
		// "\r" — carriage return: skipped, as in the original parser.
		i = special + 1;
	}
}

function flushTsvStream(state: TsvStreamState, onRow: (row: string[],) => void,): void {
	if (state.pendingQuoteInQuotes) {
		appendFieldText(state, '"',);
		state.pendingQuoteInQuotes = false;
	}
	if (state.currentField.length === 0 && state.currentRow.length === 0) return;
	state.currentRow.push(state.currentField,);
	state.currentField = "";
	state.fieldBytes = 0;
	const row = state.currentRow;
	state.currentRow = [];
	state.rowBytes = 0;
	onRow(row,);
}

function rowToCsv(row: string[], spreadsheetSafe: boolean,): string {
	return row.map((field,) => csvEscape(field, spreadsheetSafe,)).join(",",);
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

function buildStreamTimeoutError(timeoutMs: number, action: string,): DataikuError {
	return new DataikuError(
		0,
		"Request Timeout",
		`Dataset ${action} timed out after ${timeoutMs}ms while waiting for rows.`,
	);
}

async function readChunkWithTimeout(
	reader: ReadableStreamDefaultReader<Uint8Array>,
	remainingMs: number,
	timeoutMs: number,
	action: string,
): Promise<Bun.ReadableStreamDefaultReadResult<Uint8Array>> {
	const { promise, resolve, reject, } = Promise.withResolvers<
		Bun.ReadableStreamDefaultReadResult<Uint8Array>
	>();
	const timer = setTimeout(() => {
		void reader.cancel(buildStreamTimeoutError(timeoutMs, action,),).catch(() => {},);
		reject(buildStreamTimeoutError(timeoutMs, action,),);
	}, remainingMs,);
	reader.read().then(
		(result,) => {
			clearTimeout(timer,);
			resolve(result,);
		},
		(error,) => {
			clearTimeout(timer,);
			reject(error,);
		},
	);
	return promise;
}

async function collectPreviewRows(
	body: ReadableStream<Uint8Array>,
	maxDataRows: number,
	timeoutMs: number,
	onHeader?: (headerRow: string[],) => void,
): Promise<{ columns: string[]; rows: string[][]; truncated: boolean; }> {
	const state = createTsvStreamState();
	const decoder = new StringDecoder("utf-8",);
	let columns: string[] | undefined;
	const rows: string[][] = [];
	let done = false;
	let truncated = false;
	const startedAt = Date.now();
	const reader = body.getReader();

	let columnCount = 0;
	const handleRow = (row: string[],): void => {
		if (done) return;
		if (columns === undefined) {
			if (isBlankRow(row,)) return;
			columns = row;
			columnCount = row.length;
			onHeader?.(row,);
			return;
		}
		if (isBlankRow(row,) && columnCount !== 1) return;
		if (rows.length >= maxDataRows) {
			truncated = true;
			done = true;
			return;
		}
		rows.push(row,);
	};

	try {
		while (true) {
			if (done) {
				void reader.cancel().catch(() => {},);
				break;
			}
			const remainingMs = timeoutMs - (Date.now() - startedAt);
			if (remainingMs <= 0) throw buildStreamTimeoutError(timeoutMs, "preview",);
			const result = await readChunkWithTimeout(reader, remainingMs, timeoutMs, "preview",);
			if (result.done) break;
			consumeTsvChunk(decoder.write(result.value,), state, handleRow,);
		}

		if (!done) {
			const tail = decoder.end();
			if (tail !== "") consumeTsvChunk(tail, state, handleRow,);
			flushTsvStream(state, handleRow,);
		}

		return { columns: columns ?? [], rows, truncated, };
	} catch (error) {
		// Fail closed on TSV byte-limit overflow: cancel the response instead
		// of retaining partial decoded data.
		void reader.cancel().catch(() => {},);
		throw error;
	} finally {
		reader.releaseLock();
	}
}

/**
 * Count data rows in a streamed TSV response (header row excluded), stopping
 * as soon as `stopAfter` rows have been seen so a mismatched dataset is never
 * fully scanned. Returns the exact count when the stream ends before the cap,
 * or `stopAfter` — a lower bound — once the cap is reached.
 */
async function collectTsvRowCount(
	body: ReadableStream<Uint8Array>,
	stopAfter: number,
	timeoutMs: number,
): Promise<{ count: number; bounded: boolean; }> {
	const state = createTsvStreamState();
	const decoder = new StringDecoder("utf-8",);
	let headerSeen = false;
	let count = 0;
	let bounded = false;
	const startedAt = Date.now();
	const reader = body.getReader();

	let columnCount = 0;
	const handleRow = (row: string[],): void => {
		if (bounded) return;
		if (!headerSeen) {
			if (isBlankRow(row,)) return;
			headerSeen = true;
			columnCount = row.length;
			return;
		}
		// An empty line is structurally invalid in a multi-column TSV (blank
		// rows are tolerated and skipped), but for single-column datasets an
		// empty line is a real data row carrying an empty string value and
		// must be counted. The stream parser only emits rows for real line
		// terminators, so the synthetic trailing newline never produces one.
		if (isBlankRow(row,) && columnCount !== 1) return;
		count += 1;
		if (count >= stopAfter) bounded = true;
	};

	try {
		while (true) {
			if (bounded) {
				void reader.cancel().catch(() => {},);
				break;
			}
			const remainingMs = timeoutMs - (Date.now() - startedAt);
			if (remainingMs <= 0) {
				throw buildStreamTimeoutError(timeoutMs, "row count assertion",);
			}
			const result = await readChunkWithTimeout(
				reader,
				remainingMs,
				timeoutMs,
				"row count assertion",
			);
			if (result.done) break;
			consumeTsvChunk(decoder.write(result.value,), state, handleRow,);
		}

		if (!bounded) {
			const tail = decoder.end();
			if (tail !== "") consumeTsvChunk(tail, state, handleRow,);
			flushTsvStream(state, handleRow,);
		}

		return { count, bounded, };
	} catch (error) {
		// Fail closed on TSV byte-limit overflow: cancel the response instead
		// of continuing to consume unbounded decoded data.
		void reader.cancel().catch(() => {},);
		throw error;
	} finally {
		reader.releaseLock();
	}
}

function tsvToCsvTransform(
	maxDataRows: number,
	stats: { rows: number; truncated: boolean; },
	onHeader?: (headerRow: string[],) => void,
	spreadsheetSafe = true,
): Transform {
	const state = createTsvStreamState();
	const decoder = new StringDecoder("utf-8",);
	const maxRows = Math.max(1, maxDataRows,);
	let headerSeen = false;
	let done = false;

	let columnCount = 0;
	const handleRow = (row: string[], push: (line: string,) => void,): void => {
		if (done) return;
		if (!headerSeen) {
			if (isBlankRow(row,)) return;
			headerSeen = true;
			columnCount = row.length;
			onHeader?.(row,);
			push(`${rowToCsv(row, spreadsheetSafe,)}\n`,);
			return;
		}
		if (isBlankRow(row,) && columnCount !== 1) return;
		if (stats.rows >= maxRows) {
			stats.truncated = true;
			done = true;
			return;
		}
		push(`${rowToCsv(row, spreadsheetSafe,)}\n`,);
		stats.rows += 1;
	};

	return new Transform({
		transform(chunk: Buffer, _encoding, callback,) {
			if (done) {
				callback();
				return;
			}
			try {
				consumeTsvChunk(
					decoder.write(chunk,),
					state,
					(row,) => handleRow(row, (line,) => this.push(line,),),
				);
			} catch (error) {
				// Byte-limit overflow: destroy the transform so the pipeline
				// cancels the response and the partial file is removed.
				callback(error instanceof Error ? error : new Error(String(error,),),);
				return;
			}
			if (done) this.push(null,);
			callback();
		},
		flush(callback,) {
			if (done) {
				callback();
				return;
			}
			try {
				const tail = decoder.end();
				if (tail !== "") {
					consumeTsvChunk(
						tail,
						state,
						(row,) => handleRow(row, (line,) => this.push(line,),),
					);
				}
				flushTsvStream(state, (row,) => handleRow(row, (line,) => this.push(line,),),);
			} catch (error) {
				callback(error instanceof Error ? error : new Error(String(error,),),);
				return;
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
function isMissingUploadedFilesTargetConnection(error: unknown,): error is DataikuError {
	if (!(error instanceof DataikuError)) return false;
	const detail = error.body.toLowerCase();
	return (
		detail.includes("without a target connection",)
		|| detail.includes("target connection is required",)
	);
}

function buildDatasetCreateBody(opts: {
	projectKey: string;
	datasetName: string;
	connection?: string;
	dsType: string;
	table?: string;
	dbSchema?: string;
	catalog?: string;
	formatType?: string;
	formatParams?: Record<string, unknown>;
	managed?: boolean;
},): Record<string, unknown> {
	if (opts.dsType.toLowerCase() === "uploadedfiles") {
		return {
			projectKey: opts.projectKey,
			name: opts.datasetName,
			type: opts.dsType,
			params: opts.connection ? { uploadConnection: opts.connection, } : {},
		};
	}
	if (!opts.connection) {
		throw new ClientValidationError("connection is required unless dsType is UploadedFiles.",);
	}
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
function parseUploadedFiles(raw: unknown, datasetName: string,): UploadedFileMetadata[] {
	if (!Array.isArray(raw,)) {
		throw new ClientValidationError(
			`DSS returned an invalid uploaded-files list for dataset ${datasetName}.`,
			"validation_failed",
			"Expected an array from the UploadedFiles dataset file endpoint.",
		);
	}
	return raw.map((item, index,) => {
		if (
			!item
			|| typeof item !== "object"
			|| Array.isArray(item,)
			|| typeof (item as Record<string, unknown>).filename !== "string"
			|| (item as Record<string, unknown>).filename === ""
		) {
			throw new ClientValidationError(
				`DSS returned invalid metadata for uploaded file ${String(index,)} in dataset ${datasetName}.`,
				"validation_failed",
				"Expected every uploaded file to include a non-empty filename.",
			);
		}
		return item as UploadedFileMetadata;
	},);
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
	 * Get the raw dataset schema object exactly as the DSS schema endpoint
	 * returns it, without schema validation — used for deterministic comparison.
	 */
	async getSchemaObject(datasetName: string, projectKey?: string,): Promise<unknown> {
		const dsEnc = encodeURIComponent(datasetName,);
		return this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/schema`,
		);
	}

	/**
	 * Preview dataset rows as structured data: column names plus row arrays,
	 * mirroring the sql query shape ({ columns, rows, rowCount }). Streams TSV
	 * from the API and returns up to `maxRows` data rows.
	 *
	 * If `validateColumns` is provided, the first TSV row (header) is checked
	 * against the column names. Mismatches emit a warning via onValidationWarning.
	 *
	 * The stream is byte-bounded (16 MiB per field, 64 MiB per row, 16 GiB per
	 * response): an oversized field or unterminated quoted row aborts the
	 * preview, cancels the response, and fails with a DataikuError.
	 */
	async preview(
		datasetName: string,
		opts?: {
			maxRows?: number;
			projectKey?: string;
			validateColumns?: { name: string; }[];
			timeoutMs?: number;
		},
	): Promise<{
		columns: Array<{ name: string; }>;
		rows: string[][];
		rowCount: number;
		truncated: boolean;
		limit: number;
	}> {
		const maxRows = Math.max(1, Math.min(opts?.maxRows ?? 50, 500,),);
		const timeoutMs = Math.max(1, opts?.timeoutMs ?? this.client.getRequestTimeoutMs(),);
		const dsEnc = encodeURIComponent(datasetName,);
		// Probe one row past the cap so callers learn whether the dataset holds
		// more rows than requested without materializing the full dataset.
		const res = await this.client.stream(
			`/public/api/projects/${
				this.enc(opts?.projectKey,)
			}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${maxRows + 1}`,
		);
		const onHeader = opts?.validateColumns
			? (headerRow: string[],) => {
				const warnings = validateStreamColumns(headerRow, opts.validateColumns!,);
				if (warnings.length > 0) {
					this.client.warn(`datasets.preview(${datasetName})`, warnings,);
				}
			}
			: undefined;
		if (!res.body) {
			return { columns: [], rows: [], rowCount: 0, truncated: false, limit: maxRows, };
		}
		const { columns, rows, truncated, } = await collectPreviewRows(
			res.body as ReadableStream<Uint8Array>,
			maxRows,
			timeoutMs,
			onHeader,
		);
		return {
			columns: columns.map((name,) => ({ name, })),
			rows,
			rowCount: rows.length,
			truncated,
			limit: maxRows,
		};
	}

	/**
	 * Count dataset rows by streaming the TSV data endpoint and stopping as soon
	 * as `expected + 1` rows have been seen, so a mismatched dataset is never
	 * fully scanned. Returns the observed count together with `exact` (false when
	 * the probe cap was reached, i.e. the count is a lower bound) and whether the
	 * count satisfies the assertion.
	 *
	 * Streaming is byte-bounded (16 MiB per field, 64 MiB per row, 16 GiB per
	 * response); oversized fields abort the count, cancel the response, and
	 * fail with a DataikuError.
	 */
	async assertRowCount(
		datasetName: string,
		expected: number,
		projectKey?: string,
	): Promise<{ expected: number; count: number; exact: boolean; satisfied: boolean; }> {
		if (!Number.isSafeInteger(expected,) || expected < 0) {
			throw new ClientValidationError(
				"expected row count must be a non-negative safe integer",
				"validation_failed",
			);
		}
		const target = expected;
		const probe = target + 1;
		const timeoutMs = Math.max(1, this.client.getRequestTimeoutMs(),);
		const dsEnc = encodeURIComponent(datasetName,);
		const res = await this.client.stream(
			`/public/api/projects/${
				this.enc(projectKey,)
			}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${probe}`,
		);
		if (!res.body) {
			return { expected: target, count: 0, exact: true, satisfied: target === 0, };
		}
		const { count, bounded, } = await collectTsvRowCount(
			res.body as ReadableStream<Uint8Array>,
			probe,
			timeoutMs,
		);
		return { expected: target, count, exact: !bounded, satisfied: !bounded && count === target, };
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
	 *
	 * Exported cells are spreadsheet-safe by default: headers/values whose
	 * first effective character is a spreadsheet formula sigil (`=`, `+`, `-`,
	 * `@`, including leading whitespace/control variants) are neutralized with
	 * a leading apostrophe so the file never executes formulas on open. Pass
	 * `rawData: true` to preserve exact bytes.
	 *
	 * Streaming is byte-bounded (16 MiB per field, 64 MiB per row, 16 GiB per
	 * response); on overflow the response is cancelled, the error is thrown,
	 * and any partially written download file is removed.
	 */
	async download(
		datasetName: string,
		opts?: {
			outputPath?: string;
			projectKey?: string;
			validateColumns?: { name: string; }[];
			limit?: number;
			/** Preserve exact cell bytes (no spreadsheet-safe formula neutralization), default false. */
			rawData?: boolean;
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
		const csvTransform = tsvToCsvTransform(limit, stats, onHeader, opts?.rawData !== true,);
		fs.mkdirSync(nodePath.dirname(filePath,), { recursive: true, },);
		const fileOut = fs.createWriteStream(filePath,);

		try {
			if (shouldGzip) {
				const gzip = createGzip();
				await pipeline(nodeStream, csvTransform, gzip, fileOut,);
			} else {
				await pipeline(nodeStream, csvTransform, fileOut,);
			}
		} catch (error) {
			// A failed export (byte-limit overflow, network error) must never
			// leave a partial file that looks like a complete download.
			fs.rmSync(filePath, { force: true, },);
			throw error;
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
			if (
				explicitType?.toLowerCase() === "uploadedfiles"
				&& !opts.connection
				&& isMissingUploadedFilesTargetConnection(error,)
			) {
				let uploadConnection = await resolveAdminManagedStorageConnection(
					this.client,
					"allowManagedDatasets",
					"filesystem_managed",
				);
				if (!uploadConnection) {
					const existing = await this.list(pk,);
					uploadConnection = [
						...new Set(
							existing
								.filter((dataset,) => dataset.managed === true)
								.map((dataset,) => dataset.params?.connection)
								.filter((connection,): connection is string =>
									typeof connection === "string" && connection.length > 0
								),
						),
					].sort()[0];
				}
				if (!uploadConnection) throw error;

				body = buildDatasetCreateBody({
					projectKey: pk,
					datasetName: opts.datasetName,
					connection: uploadConnection,
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
		const uploadedFiles = normalizedType.includes("uploaded",);
		const fileBacked = !table
			&& (normalizedType.includes("filesystem",)
				|| normalizedType.includes("uploaded",)
				|| normalizedType.includes("s3",)
				|| path !== null);
		const formatType = details.formatType ?? null;
		const warnings: string[] = [];

		if (fileBacked && !uploadedFiles && !path) {
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
	/** List files stored by an UploadedFiles dataset. */
	async listUploadedFiles(
		datasetName: string,
		projectKey?: string,
	): Promise<UploadedFileMetadata[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${
				encodeURIComponent(datasetName,)
			}/uploaded/files`,
		);
		return parseUploadedFiles(raw, datasetName,);
	}

	/** Add a new file to an UploadedFiles dataset and verify its byte length. */
	async uploadDatasetFile(
		datasetName: string,
		localPath: string,
		opts: UploadDatasetFileOptions,
	): Promise<UploadDatasetFileResult> {
		const fileName = opts.fileName.trim();
		if (fileName === "") {
			throw new ClientValidationError("fileName must be a non-empty uploaded filename.",);
		}
		const localFile = await fs.promises.stat(localPath,);
		if (!localFile.isFile()) {
			throw new ClientValidationError(`${localPath} is not a regular file.`,);
		}

		const projectKey = this.resolveProjectKey(opts.projectKey,);
		const details = await this.get(datasetName, projectKey,);
		if (details.type?.toLowerCase() !== "uploadedfiles") {
			throw new ClientValidationError(
				`Dataset ${datasetName} is ${details.type ?? "an unknown type"}, not UploadedFiles.`,
				"validation_failed",
				"Use dataset upload-file only with an UploadedFiles dataset.",
			);
		}

		const existing = (await this.listUploadedFiles(datasetName, projectKey,))
			.filter((file,) => file.filename === fileName);
		if (existing.length !== 0) {
			throw new ClientValidationError(
				`Uploaded file ${fileName} already exists in dataset ${datasetName}.`,
				"validation_failed",
				"DSS 14.7's public API can add uploaded files but cannot replace or delete them. Use a new filename or import a successor project archive containing the replacement.",
			);
		}

		const endpoint = `/public/api/projects/${encodeURIComponent(projectKey,)}/datasets/${
			encodeURIComponent(datasetName,)
		}/uploaded/files`;
		await this.client.upload(endpoint, localPath, fileName,);

		const afterMatches = (await this.listUploadedFiles(datasetName, projectKey,))
			.filter((file,) => file.filename === fileName);
		const after = afterMatches[0];
		if (
			afterMatches.length !== 1
			|| after === undefined
			|| after.length !== localFile.size
		) {
			throw new ClientValidationError(
				`DSS did not verify upload of ${fileName} to dataset ${datasetName}.`,
				"ambiguous_outcome",
				"The upload returned successfully, but the uploaded-files listing did not show exactly one target with the local byte length. Inspect the dataset before retrying.",
				{
					expectedBytes: localFile.size,
					matchingFiles: afterMatches,
				},
			);
		}

		return {
			datasetName,
			projectKey,
			fileName,
			bytes: localFile.size,
			after,
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
