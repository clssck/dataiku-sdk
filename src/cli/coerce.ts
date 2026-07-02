import { createHash, } from "node:crypto";
import { readFileSync, } from "node:fs";
import type { JobBuildTargetType, JobLogFilter, } from "../resources/jobs.js";
import { UsageError, } from "./usage.js";

export function num(v: string | boolean | undefined,): number | undefined {
	if (typeof v !== "string") return undefined;
	const n = Number(v,);
	return Number.isFinite(n,) ? n : undefined;
}

export function jobBuildTargetType(v: string | boolean | undefined,): JobBuildTargetType {
	if (v === undefined) return "DATASET";
	if (typeof v !== "string") {
		throw new UsageError(
			"Invalid job target type. Use dataset or managed-folder.",
			"invalid_enum",
		);
	}
	const normalized = v.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "DATASET" || normalized === "MANAGED_FOLDER") return normalized;
	throw new UsageError(
		"Invalid job target type. Use dataset or managed-folder.",
		"invalid_enum",
	);
}

export function jobBuildTargetTypeFromFlags(
	flags: Record<string, string | boolean>,
): JobBuildTargetType {
	return jobBuildTargetType(flags["target-type"] ?? flags["type"],);
}

export function maxLogLinesFromFlags(flags: Record<string, string | boolean>,): number | undefined {
	return num(flags["max-log-lines"] ?? flags["max-lines"],);
}

export function jobLogFilterFromFlag(v: string | boolean | undefined,): JobLogFilter | undefined {
	if (v === undefined) return undefined;
	if (typeof v !== "string") {
		throw new UsageError(
			"Invalid --log-filter value. Use stdout, stderr, user, or errors.",
			"invalid_enum",
		);
	}
	const normalized = v.trim().toLowerCase();
	if (
		normalized === "stdout" || normalized === "stderr" || normalized === "user"
		|| normalized === "errors"
	) {
		return normalized;
	}
	throw new UsageError(
		"Invalid --log-filter value. Use stdout, stderr, user, or errors.",
		"invalid_enum",
	);
}

export function sha256Hex(value: string,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

export function normalizeLineEndings(value: string,): string {
	return value.replace(/\r\n/g, "\n",);
}

export function stableJson(value: unknown,): string {
	if (value === undefined) return "undefined";
	if (value === null || typeof value !== "object") return JSON.stringify(value,);
	if (Array.isArray(value,)) return `[${value.map((item,) => stableJson(item,)).join(",",)}]`;
	const entries = Object.entries(value as Record<string, unknown>,).sort(([a,], [b,],) =>
		a.localeCompare(b,)
	);
	return `{${
		entries.map(([key, item,],) => `${JSON.stringify(key,)}:${stableJson(item,)}`).join(",",)
	}}`;
}

export function stableHash(value: unknown,): string {
	return sha256Hex(stableJson(value,),);
}

export function splitCsvFlag(v: string | boolean | undefined,): string[] {
	if (typeof v !== "string") return [];
	return v.split(",",).map((item,) => item.trim()).filter((item,) => item.length > 0);
}

export function recipeInputDatasetsFromFlags(
	flags: Record<string, string | boolean>,
): string[] | undefined {
	const inputs = splitCsvFlag(flags["input"],);
	return inputs.length > 0 ? inputs : undefined;
}

export function rewritePairsFromFlags(
	flags: Record<string, string | boolean>,
	flagName: string,
): Record<string, string> {
	const rewrites: Record<string, string> = {};
	for (const spec of splitCsvFlag(flags[flagName],)) {
		const idx = spec.indexOf("=",);
		if (idx <= 0 || idx === spec.length - 1) {
			throw new UsageError(`--${flagName} values must use FROM=TO.`, "invalid_enum",);
		}
		const from = spec.slice(0, idx,).trim();
		const to = spec.slice(idx + 1,).trim();
		if (!from || !to) throw new UsageError(`--${flagName} values must use FROM=TO.`, "invalid_enum",);
		rewrites[from] = to;
	}
	return rewrites;
}

export function plainRecord(value: unknown,): Record<string, unknown> | undefined {
	return value && typeof value === "object" && !Array.isArray(value,)
		? value as Record<string, unknown>
		: undefined;
}

export function requiredStringFlag(
	flags: Record<string, string | boolean>,
	name: string,
	usage: string,
): string {
	const value = flags[name];
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(`--${name} is required. Usage: ${usage}`, "missing_required_flag",);
	}
	return value.trim();
}

export function optionalStringField(
	record: Record<string, unknown>,
	keys: string[],
): string | undefined {
	for (const key of keys) {
		const value = record[key];
		if (typeof value === "string" && value.trim().length > 0) return value.trim();
	}
	return undefined;
}

export function requiredStringArray(value: unknown, source: string,): string[] {
	if (!Array.isArray(value,)) {
		throw new UsageError(`${source} must be an array of strings.`, "validation_failed",);
	}
	return value.map((item, index,) => {
		if (typeof item !== "string" || item.trim().length === 0) {
			throw new UsageError(`${source}[${index}] must be a non-empty string.`, "validation_failed",);
		}
		return item.trim();
	},);
}

export function finiteNumberField(
	record: Record<string, unknown>,
	key: string,
	source: string,
): number {
	const value = record[key];
	if (typeof value !== "number" || !Number.isFinite(value,)) {
		throw new UsageError(`${source}.${key} must be a finite number.`, "validation_failed",);
	}
	return value;
}

export function stripUtf8Bom(text: string,): string {
	return text.charCodeAt(0,) === 0xfeff ? text.slice(1,) : text;
}

export function parseJsonValue(text: string, source: string,): unknown {
	try {
		return JSON.parse(stripUtf8Bom(text,),) as unknown;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error,);
		throw new UsageError(`Invalid JSON in ${source}: ${message}`, "validation_failed",);
	}
}

export function expectJsonObject(value: unknown, source: string,): Record<string, unknown> {
	if (value && typeof value === "object" && !Array.isArray(value,)) {
		return value as Record<string, unknown>;
	}
	throw new UsageError(`Expected JSON object in ${source}.`, "validation_failed",);
}

export function parseJsonObject(text: string, source: string,): Record<string, unknown> {
	return expectJsonObject(parseJsonValue(text, source,), source,);
}

export function json(
	v: string | boolean | undefined,
	source = "JSON flag",
): Record<string, unknown> | undefined {
	if (typeof v !== "string") return undefined;
	return parseJsonObject(v, source,);
}

export function readStdinText(): string {
	return readFileSync(0, "utf-8",);
}

export function jsonInput(
	flags: Record<string, string | boolean>,
): Record<string, unknown> | undefined {
	if (flags["stdin"] === true) return parseJsonObject(readStdinText(), "stdin",);
	if (typeof flags["data-file"] === "string") {
		return parseJsonObject(readFileSync(flags["data-file"], "utf-8",), flags["data-file"],);
	}
	if (typeof flags["data"] === "string") return parseJsonObject(flags["data"], "--data",);
	return undefined;
}

export function unknownJsonInput(flags: Record<string, string | boolean>,): unknown {
	if (flags["stdin"] === true) return parseJsonValue(readStdinText(), "stdin",);
	if (typeof flags["data-file"] === "string") {
		return parseJsonValue(readFileSync(flags["data-file"], "utf-8",), flags["data-file"],);
	}
	if (typeof flags["data"] === "string") return parseJsonValue(flags["data"], "--data",);
	return undefined;
}

export function schemaColumnsInput(flags: Record<string, string | boolean>, usage: string,): Array<{
	comment?: string;
	name: string;
	type: string;
}> {
	const input = unknownJsonInput(flags,);
	if (input === undefined) {
		throw new UsageError(`--data, --data-file, or --stdin is required. Usage: ${usage}`,);
	}
	const columns = Array.isArray(input,)
		? input
		: input && typeof input === "object" && Array.isArray((input as { columns?: unknown; }).columns,)
		? (input as { columns: unknown[]; }).columns
		: undefined;
	if (!columns) {
		throw new UsageError(
			"Schema input must be an array of columns or an object with a columns array.",
		);
	}
	return columns.map((column, index,) => {
		if (!column || typeof column !== "object" || Array.isArray(column,)) {
			throw new UsageError(`Schema column at index ${index} must be an object.`,);
		}
		const record = column as Record<string, unknown>;
		if (typeof record.name !== "string" || record.name.length === 0) {
			throw new UsageError(`Schema column at index ${index} is missing string field "name".`,);
		}
		if (typeof record.type !== "string" || record.type.length === 0) {
			throw new UsageError(`Schema column "${record.name}" is missing string field "type".`,);
		}
		return {
			...record,
			name: record.name,
			type: record.type,
			...(typeof record.comment === "string" ? { comment: record.comment, } : {}),
		};
	},);
}

export function textInput(flags: Record<string, string | boolean>,): string | undefined {
	if (typeof flags["content"] === "string") return flags["content"];
	if (typeof flags["file"] === "string") return readFileSync(flags["file"], "utf-8",);
	return undefined;
}

export function requiredJsonInput(
	flags: Record<string, string | boolean>,
	message: string,
): Record<string, unknown> {
	const data = jsonInput(flags,);
	if (data === undefined) throw new UsageError(message,);
	return data;
}

export function parseBooleanOption(
	value: string | boolean | undefined,
	flagName: string,
): boolean | undefined {
	if (value === undefined) return undefined;
	if (typeof value === "boolean") return value;
	const normalized = value.trim().toLowerCase();
	if (["1", "true", "yes", "y", "on",].includes(normalized,)) return true;
	if (["0", "false", "no", "n", "off",].includes(normalized,)) return false;
	throw new UsageError(`${flagName} must be true or false.`, "invalid_enum",);
}

export function stringField(
	record: Record<string, unknown>,
	fields: string[],
): string | undefined {
	for (const field of fields) {
		const value = record[field];
		if (typeof value === "string" && value.length > 0) return value;
	}
	return undefined;
}

export function resultRecord(result: unknown,): Record<string, unknown> {
	return result !== null && typeof result === "object" && !Array.isArray(result,)
		? result as Record<string, unknown>
		: {};
}
