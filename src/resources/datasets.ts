import { createWriteStream, } from "node:fs";
import { resolve, } from "node:path";
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

function emitCsvLineWithLimit(
	row: string[],
	maxDataRows: number,
	emittedRows: { value: number; },
	onLine: (line: string,) => void,
	onHeader?: (headerRow: string[],) => void,
): boolean {
	if (isBlankRow(row,)) return false;

	const isHeader = emittedRows.value === 0;
	if (isHeader && onHeader) onHeader(row,);
	if (!isHeader && emittedRows.value - 1 >= maxDataRows) {
		return true;
	}

	onLine(rowToCsv(row,),);
	emittedRows.value += 1;

	if (!isHeader && emittedRows.value - 1 >= maxDataRows) {
		return true;
	}
	return false;
}

async function collectPreviewCsv(
	body: ReadableStream<Uint8Array>,
	maxDataRows: number,
	onHeader?: (headerRow: string[],) => void,
): Promise<string> {
	const state = createTsvStreamState();
	const emittedRows = { value: 0, };
	const lines: string[] = [];
	let done = false;

	const nodeStream = Readable.fromWeb(body as unknown as import("stream/web").ReadableStream,);
	for await (const chunk of nodeStream) {
		if (done) break;
		consumeTsvChunk(Buffer.from(chunk,).toString("utf-8",), state, (row,) => {
			if (done) return;
			done = emitCsvLineWithLimit(row, maxDataRows, emittedRows, (line,) => {
				lines.push(line,);
			}, onHeader,);
		},);
		if (done) {
			nodeStream.destroy();
			break;
		}
	}

	if (!done) {
		flushTsvStream(state, (row,) => {
			if (done) return;
			done = emitCsvLineWithLimit(row, maxDataRows, emittedRows, (line,) => {
				lines.push(line,);
			}, onHeader,);
		},);
	}

	return lines.join("\n",);
}

function tsvToCsvTransform(
	maxDataRows: number,
	onHeader?: (headerRow: string[],) => void,
): Transform {
	const state = createTsvStreamState();
	const emittedRows = { value: 0, };
	let done = false;
	const maxRows = Math.max(1, maxDataRows,);

	return new Transform({
		transform(chunk: Buffer, _encoding, callback,) {
			if (done) {
				callback();
				return;
			}

			consumeTsvChunk(chunk.toString("utf-8",), state, (row,) => {
				if (done) return;
				done = emitCsvLineWithLimit(row, maxRows, emittedRows, (line,) => {
					this.push(`${line}\n`,);
				}, onHeader,);
			},);

			if (done) {
				this.push(null,);
			}
			callback();
		},
		flush(callback,) {
			if (done) {
				callback();
				return;
			}

			flushTsvStream(state, (row,) => {
				if (done) return;
				done = emitCsvLineWithLimit(row, maxRows, emittedRows, (line,) => {
					this.push(`${line}\n`,);
				}, onHeader,);
			},);
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
			path: `${opts.projectKey}/${opts.datasetName}`,
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

	/**
	 * Preview dataset data as CSV text.
	 * Streams TSV from the API, converts to CSV, and returns up to `maxRows`
	 * data rows (plus header).
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
		},
	): Promise<string> {
		const maxRows = Math.max(1, Math.min(opts?.maxRows ?? 50, 500,),);
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
		return collectPreviewCsv(res.body as ReadableStream<Uint8Array>, maxRows, onHeader,);
	}

	/** Get dataset metadata (tags, custom fields, checklists). */
	async metadata(datasetName: string, projectKey?: string,): Promise<Record<string, unknown>> {
		const dsEnc = encodeURIComponent(datasetName,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${this.enc(projectKey,)}/datasets/${dsEnc}/metadata`,
		);
	}

	/**
	 * Download dataset data as a gzipped CSV file.
	 * Returns the absolute path of the written file.
	 */
	async download(
		datasetName: string,
		opts?: {
			outputPath?: string;
			projectKey?: string;
			validateColumns?: { name: string; }[];
		},
	): Promise<string> {
		const downloadLimit = 100_000;
		const dsEnc = encodeURIComponent(datasetName,);
		const res = await this.client.stream(
			`/public/api/projects/${
				this.enc(opts?.projectKey,)
			}/datasets/${dsEnc}/data/?format=tsv-excel-header&limit=${downloadLimit}`,
		);

		const safeDatasetName = sanitizeFileName(datasetName, "dataset",);
		const filePath = opts?.outputPath?.endsWith(".gz",) || opts?.outputPath?.endsWith(".csv",)
			? resolve(opts.outputPath,)
			: resolve(opts?.outputPath ?? process.cwd(), `${safeDatasetName}.csv.gz`,);

		const onHeader = opts?.validateColumns
			? (headerRow: string[],) => {
				const warnings = validateStreamColumns(headerRow, opts.validateColumns!,);
				if (warnings.length > 0) {
					this.client.warn(`datasets.download(${datasetName})`, warnings,);
				}
			}
			: undefined;

		const shouldGzip = filePath.endsWith(".gz",);
		const nodeStream = Readable.fromWeb(res.body as unknown as import("stream/web").ReadableStream,);
		const csvTransform = tsvToCsvTransform(downloadLimit, onHeader,);
		const fileOut = createWriteStream(filePath,);

		if (shouldGzip) {
			const gzip = createGzip();
			await pipeline(nodeStream, csvTransform, gzip, fileOut,);
		} else {
			await pipeline(nodeStream, csvTransform, fileOut,);
		}

		return filePath;
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
}
