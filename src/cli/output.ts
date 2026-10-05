import type { DataikuClient, } from "../client.js";
import { DataikuError, type StableErrorCode, } from "../errors.js";

let outputFieldProjection: string[] | undefined;

export function setOutputFieldProjection(fields: string[] | undefined,): void {
	outputFieldProjection = fields;
}

/**
 * Non-fatal diagnostics queued during a command and flushed to stderr as one
 * JSON envelope, so agents get a loud, machine-parseable signal (e.g. truncated
 * exports) without polluting the stdout result contract.
 */
let pendingCliWarnings: Record<string, unknown>[] = [];

export function enqueueCliWarning(warning: Record<string, unknown>,): void {
	pendingCliWarnings.push(warning,);
}

/** Emit queued warnings as one JSONL warning event on stderr. Idempotent. */
export function flushCliWarnings(): void {
	if (pendingCliWarnings.length === 0) return;
	const warnings = pendingCliWarnings;
	pendingCliWarnings = [];
	process.stderr.write(`${JSON.stringify({ type: "warning", warnings, },)}\n`,);
}

interface ResolvedFieldPath {
	found: boolean;
	value: unknown;
}

function resolveFieldPath(source: Record<string, unknown>, field: string,): ResolvedFieldPath {
	let current: unknown = source;
	for (const segment of field.split(".",)) {
		if (current === null || typeof current !== "object" || Array.isArray(current,)) {
			return { found: false, value: null, };
		}
		const record = current as Record<string, unknown>;
		if (!Object.prototype.hasOwnProperty.call(record, segment,)) {
			return { found: false, value: null, };
		}
		current = record[segment];
	}
	return { found: true, value: current ?? null, };
}

/**
 * Keys available at the deepest parent object a dotted field actually reaches.
 * A miss like `recipe.params` is ambiguous — the path may be unsupported or the
 * key may merely be absent — so listing sibling keys under the resolved parent
 * (`recipe.type`, `recipe.name`, ...) shows nested paths work and points at
 * real alternatives; a plain top-level miss keeps listing top-level keys.
 */
function availableFieldsFor(
	records: Array<Record<string, unknown>>,
	field: string,
): string[] {
	const segments = field.split(".",);
	const parentPath = segments.slice(0, -1,).join(".",);
	const keys = new Set<string>();
	for (const record of records) {
		const parent = parentPath === ""
			? record
			: resolveFieldPath(record, parentPath,).value;
		if (parent === null || typeof parent !== "object" || Array.isArray(parent,)) continue;
		for (const key of Object.keys(parent as Record<string, unknown>,)) {
			keys.add(parentPath === "" ? key : `${parentPath}.${key}`,);
		}
	}
	return [...keys,].sort();
}

export function pickResultFields(item: unknown, fields: string[],): unknown {
	if (!item || typeof item !== "object" || Array.isArray(item,)) return item;
	const source = item as Record<string, unknown>;
	const picked: Record<string, unknown> = {};
	for (const field of fields) picked[field] = resolveFieldPath(source, field,).value;
	return picked;
}

function projectionRecords(result: unknown,): Array<Record<string, unknown>> {
	const items = Array.isArray(result,) ? result : [result,];
	return items.filter((item,) =>
		item !== null && typeof item === "object" && !Array.isArray(item,)
	) as Array<Record<string, unknown>>;
}

function warnUnknownProjectionFields(
	result: unknown,
	fields: string[],
	availableFieldNames?: () => string[],
): void {
	const records = projectionRecords(result,);
	if (records.length === 0) return;
	const unknownFields = fields.filter((field,) =>
		!records.some((record,) => resolveFieldPath(record, field,).found)
	);
	if (unknownFields.length === 0) return;
	const explicitNames = availableFieldNames?.();
	const availableFields = explicitNames
		?? [...new Set(unknownFields.flatMap((field,) => availableFieldsFor(records, field,)),),].sort();
	enqueueCliWarning({
		code: "field_projection_missing",
		fields: unknownFields,
		availableFields,
		hint: "Use fields from the command's schemas.output contract or an unprojected result.",
	},);
}

/**
 * Project the top-level fields callers asked for via --fields. Arrays are mapped
 * element-wise; scalars and string results pass through untouched. Requested keys
 * that are absent become null so every row keeps a stable, predictable shape.
 */
export function projectResultFields(
	result: unknown,
	fields: string[],
	availableFieldNames?: () => string[],
): unknown {
	warnUnknownProjectionFields(result, fields, availableFieldNames,);
	if (Array.isArray(result,)) return result.map((item,) => pickResultFields(item, fields,));
	return pickResultFields(result, fields,);
}

export function writeCommandResult(result: unknown,): void {
	const projected = outputFieldProjection
		? projectResultFields(result, outputFieldProjection,)
		: result;
	flushCliWarnings();
	process.stdout.write(
		`${JSON.stringify(projected ?? { ok: true, },)}\n`,
	);
}

export function addTransientTargetContext(
	error: unknown,
	target: string,
	elapsedMs: number,
): never {
	if (error instanceof DataikuError && error.category === "transient") {
		throw new DataikuError(
			error.status,
			error.statusText,
			error.body,
			error.retry,
			error.requestId,
			{ target, elapsedMs, bodyTruncated: error.bodyTruncated, },
		);
	}
	throw error;
}

export function isFailedWaitResult(result: unknown,): boolean {
	if (result === null || typeof result !== "object" || Array.isArray(result,)) return false;
	const record = result as Record<string, unknown>;
	if (record.success !== false) return false;
	if (typeof record.elapsedMs !== "number" || typeof record.pollCount !== "number") return false;
	// Job/future wait results name the terminal state as a string; macro wait
	// results identify themselves by their runnable identity fields with the
	// poll state attached as an object (or omitted entirely).
	if (typeof record.state === "string" || typeof record.outcome === "string") return true;
	return typeof record.runnableType === "string" && typeof record.runId === "string";
}

export function isAssertionFailureResult(result: unknown,): boolean {
	if (result === null || typeof result !== "object" || Array.isArray(result,)) return false;
	const record = result as Record<string, unknown>;
	return record.unchanged === false || record.satisfied === false;
}

function isNestedAssertionFailureResult(result: unknown,): boolean {
	if (result === null || typeof result !== "object" || Array.isArray(result,)) return false;
	const steps = (result as Record<string, unknown>).steps;
	if (!Array.isArray(steps,)) return false;
	for (const step of steps) {
		if (step === null || typeof step !== "object" || Array.isArray(step,)) continue;
		const stepRecord = step as Record<string, unknown>;
		if (stepRecord.ok !== false) continue;
		const error = stepRecord.error;
		return error !== null
			&& typeof error === "object"
			&& !Array.isArray(error,)
			&& (error as Record<string, unknown>).code === "assertion_failed";
	}
	return false;
}

export function commandFailureExitCode(result: unknown,): number | undefined {
	if (isFailedWaitResult(result,) || isAssertionFailureResult(result,)) return 4;
	return undefined;
}

/**
 * Stable error code for a command-level failure report. A failed synchronous
 * assertion (exit 4) is a distinct outcome from a failed long-running remote
 * operation (also exit 4), so agents must never see `long_running_failure`
 * for a result that finished synchronously with `unchanged: false` or
 * `satisfied: false`.
 */
export function commandFailureCode(result: unknown, exitCode: number,): StableErrorCode {
	if (isAssertionFailureResult(result,) || isNestedAssertionFailureResult(result,)) {
		return "assertion_failed";
	}
	if (isFailedWaitResult(result,)) return "long_running_failure";
	return exitCode === 4 ? "long_running_failure" : "command_result_failure";
}

export class CommandResultFailure extends Error {
	readonly result: unknown;
	readonly exitCode: number;
	readonly code: StableErrorCode;

	constructor(result: unknown, exitCode: number, code?: StableErrorCode,) {
		super(commandFailureMessage(result,),);
		this.name = "CommandResultFailure";
		this.result = result;
		this.exitCode = exitCode;
		this.code = code ?? commandFailureCode(result, exitCode,);
	}
}

export function commandFailureMessage(result: unknown,): string {
	if (isFailedWaitResult(result,)) {
		const record = result as Record<string, unknown>;
		const failure = record.failure;
		if (failure !== null && typeof failure === "object" && !Array.isArray(failure,)) {
			const message = (failure as Record<string, unknown>).message;
			if (typeof message === "string" && message.length > 0) return message;
		}
		const state = typeof record.state === "string"
			? record.state
			: typeof record.outcome === "string"
			? record.outcome
			: typeof record.timedOut === "boolean"
			? "timed out"
			: undefined;
		return `Command completed with failed long-running result${state ? `: ${state}` : ""}.`;
	}
	if (isAssertionFailureResult(result,)) {
		const record = result as Record<string, unknown>;
		return record.unchanged === false
			? "Command completed with failed assertion result."
			: "Command completed with unsatisfied assertion result.";
	}
	if (isNestedAssertionFailureResult(result,)) {
		return "Command completed with nested failed assertion result.";
	}
	return "Command completed with failed result.";
}

export function isNotFoundError(error: unknown,): boolean {
	if (error instanceof DataikuError) return error.category === "not_found";
	if (error instanceof Error) return /not found|does not exist|unknown/i.test(error.message,);
	return false;
}

export async function readIfExists<T,>(reader: () => Promise<T>,): Promise<T | undefined> {
	try {
		return await reader();
	} catch (error) {
		if (isNotFoundError(error,)) return undefined;
		throw error;
	}
}

export function skipResult(
	resource: string,
	id: string,
	reason: "exists" | "missing",
	extra: Record<string, unknown> = {},
): Record<string, unknown> {
	return { skipped: id, reason, resource, ...extra, };
}

export function planResult(
	resource: string,
	action: string,
	options: {
		asyncKind: string;
		endpoint?: string;
		exact?: boolean;
		exitCodesOnFailure: Record<string, number>;
		identifiers?: Record<string, unknown>;
		idempotency: string;
		method?: string;
		payload?: unknown;
		localWrites?: unknown;
		plannedAndDryRun?: boolean;
		reason?: string;
		requests?: unknown;
		wait?: unknown;
	},
): Record<string, unknown> {
	return {
		plan: true,
		action,
		resource,
		...(options.plannedAndDryRun ? { plannedAndDryRun: true, } : {}),
		...(options.exact !== undefined ? { exact: options.exact, } : {}),
		...(options.reason !== undefined ? { reason: options.reason, } : {}),
		...options.identifiers,
		...(options.method ? { method: options.method, } : {}),
		...(options.endpoint ? { endpoint: options.endpoint, } : {}),
		...(options.payload !== undefined ? { payload: options.payload, } : {}),
		...(options.localWrites !== undefined ? { localWrites: options.localWrites, } : {}),
		...(options.requests !== undefined ? { requests: options.requests, } : {}),
		...(options.wait !== undefined ? { wait: options.wait, } : {}),
		idempotency: options.idempotency,
		async: options.asyncKind,
		exitCodesOnFailure: options.exitCodesOnFailure,
	};
}

export function encodedProjectEndpoint(
	client: DataikuClient,
	projectKey: string | undefined,
	suffix: string,
): string {
	return `/public/api/projects/${
		encodeURIComponent(client.resolveProjectKey(projectKey,),)
	}${suffix}`;
}

export function encodedProjectEndpointForPlan(projectKey: string, suffix: string,): string {
	return `/public/api/projects/${encodeURIComponent(projectKey,)}${suffix}`;
}
