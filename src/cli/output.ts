import type { DataikuClient, } from "../client.js";
import { DataikuError, type StableErrorCode, } from "../errors.js";

let outputFieldProjection: string[] | undefined;
let compactJsonOutput = false;

export function setCompactJsonOutput(compact: boolean,): void {
	compactJsonOutput = compact;
}

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
function flushCliWarnings(): void {
	if (pendingCliWarnings.length === 0) return;
	const warnings = pendingCliWarnings;
	pendingCliWarnings = [];
	process.stderr.write(`${JSON.stringify({ type: "warning", warnings, },)}\n`,);
}

function resolveFieldPath(source: Record<string, unknown>, field: string,): unknown {
	let current: unknown = source;
	for (const segment of field.split(".",)) {
		if (current === null || typeof current !== "object" || Array.isArray(current,)) return null;
		current = (current as Record<string, unknown>)[segment];
	}
	return current ?? null;
}

export function pickResultFields(item: unknown, fields: string[],): unknown {
	if (!item || typeof item !== "object" || Array.isArray(item,)) return item;
	const source = item as Record<string, unknown>;
	const picked: Record<string, unknown> = {};
	for (const field of fields) picked[field] = resolveFieldPath(source, field,);
	return picked;
}

/**
 * Project the top-level fields callers asked for via --fields. Arrays are mapped
 * element-wise; scalars and string results pass through untouched. Requested keys
 * that are absent become null so every row keeps a stable, predictable shape.
 */
export function projectResultFields(result: unknown, fields: string[],): unknown {
	if (Array.isArray(result,)) return result.map((item,) => pickResultFields(item, fields,));
	return pickResultFields(result, fields,);
}

export function writeCommandResult(result: unknown,): void {
	flushCliWarnings();
	const projected = outputFieldProjection
		? projectResultFields(result, outputFieldProjection,)
		: result;
	process.stdout.write(
		`${JSON.stringify(projected ?? { ok: true, }, null, compactJsonOutput ? undefined : 2,)}\n`,
	);
}

export function transientBodyWithTargetContext(
	body: string,
	target: string,
	elapsedMs: number,
): string {
	try {
		const parsed = JSON.parse(body,) as unknown;
		if (parsed && typeof parsed === "object" && !Array.isArray(parsed,)) {
			const record = parsed as Record<string, unknown>;
			const message = typeof record.message === "string" && record.message.length > 0
				? `Target: ${target}\nElapsed: ${elapsedMs}ms\n${record.message}`
				: `Target: ${target}\nElapsed: ${elapsedMs}ms`;
			return JSON.stringify({ ...record, message, target, elapsedMs, },);
		}
	} catch {
		// Non-JSON DSS bodies are wrapped as text below.
	}
	return `Target: ${target}\nElapsed: ${elapsedMs}ms\n${body}`;
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
			transientBodyWithTargetContext(error.body, target, elapsedMs,),
			error.retry,
			error.requestId,
			{ target, elapsedMs, },
		);
	}
	throw error;
}

export function isFailedWaitResult(result: unknown,): boolean {
	if (result === null || typeof result !== "object" || Array.isArray(result,)) return false;
	const record = result as Record<string, unknown>;
	return record.success === false
		&& typeof record.elapsedMs === "number"
		&& typeof record.pollCount === "number"
		&& (typeof record.state === "string" || typeof record.outcome === "string");
}

export function commandFailureExitCode(result: unknown,): number | undefined {
	if (isFailedWaitResult(result,)) return 4;
	if (
		result && typeof result === "object" && (result as Record<string, unknown>).unchanged === false
	) return 4;
	return undefined;
}

export class CommandResultFailure extends Error {
	readonly result: unknown;
	readonly exitCode: number;
	readonly code?: StableErrorCode;

	constructor(result: unknown, exitCode: number, code?: StableErrorCode,) {
		super(commandFailureMessage(result,),);
		this.name = "CommandResultFailure";
		this.result = result;
		this.exitCode = exitCode;
		this.code = code;
	}
}

export function commandFailureMessage(result: unknown,): string {
	if (isFailedWaitResult(result,)) {
		const record = result as Record<string, unknown>;
		const state = typeof record.state === "string" ? record.state : record.outcome;
		return `Command completed with failed long-running result${state ? `: ${state}` : ""}.`;
	}
	if (
		result && typeof result === "object" && (result as Record<string, unknown>).unchanged === false
	) {
		return "Command completed with failed assertion result.";
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
		exitCodesOnFailure: Record<string, number>;
		identifiers?: Record<string, unknown>;
		idempotency: string;
		method?: string;
		payload?: unknown;
		localWrites?: unknown;
		plannedAndDryRun?: boolean;
		wait?: unknown;
	},
): Record<string, unknown> {
	return {
		plan: true,
		action,
		resource,
		...(options.plannedAndDryRun ? { plannedAndDryRun: true, } : {}),
		...options.identifiers,
		...(options.method ? { method: options.method, } : {}),
		...(options.endpoint ? { endpoint: options.endpoint, } : {}),
		...(options.payload !== undefined ? { payload: options.payload, } : {}),
		...(options.localWrites !== undefined ? { localWrites: options.localWrites, } : {}),
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
