import { ClientValidationError, DataikuError, } from "../errors.js";
import type {
	MacroDefinition,
	MacroRunStart,
	MacroState,
	MacroSummary,
	MacroWaitResult,
} from "../schemas.js";
import {
	MacroDefinitionSchema,
	MacroRunStartSchema,
	MacroStateSchema,
	MacroSummaryArraySchema,
	MacroWaitResultSchema,
} from "../schemas.js";
import { computeNextPollDelayMs, isRequestDeadlineError, } from "../utils/polling.js";
import { BaseResource, requireNonEmpty, } from "./base.js";

const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_TIMEOUT_MS = 120_000;
/** Hard floor for the result download cap; keeps a caller typo from hanging the read. */
const MIN_RESULT_MAX_BYTES = 1;
const MAX_RESULT_MAX_BYTES = 2 * 1024 * 1024 * 1024;

export interface MacroRunOptions {
	/**
	 * Server-side wait: when true the POST call blocks until the run finishes
	 * (docs: `POST /runnables/{runnableType}?wait=true`). Default false.
	 */
	wait?: boolean;
	projectKey?: string;
	/**
	 * Macro parameter values keyed by the param names declared in the macro
	 * definition (`params` array). Sent as the POST body. Values must be
	 * JSON-serializable primitives or structures matching the declared types.
	 */
	params?: Record<string, unknown>;
	/**
	 * Admin-only parameter values keyed by the adminParams names declared in
	 * the macro definition. Sent as the POST body's adminParams entry.
	 */
	adminParams?: Record<string, unknown>;
}

export interface MacroWaitOptions {
	pollIntervalMs?: number;
	timeoutMs?: number;
	projectKey?: string;
	/** Macro parameter values (see {@link MacroRunOptions.params}). */
	params?: Record<string, unknown>;
	/** Admin-only parameter values (see {@link MacroRunOptions.adminParams}). */
	adminParams?: Record<string, unknown>;
}

export interface MacroResultOptions {
	projectKey?: string;
	/**
	 * Cap on the number of bytes buffered from the result body. Macro results
	 * can be arbitrary documents (HTML, text, JSON); the default 8 MiB guards
	 * against oversized bodies while remaining generous for typical reports.
	 */
	maxBytes?: number;
}

/**
 * Normalize a byte cap coming from user input (CLI `--max-bytes`) into the
 * inclusive range [1, 2 GiB], rejecting non-integers before any DSS request.
 */
export function macroResultMaxBytes(maxBytes: number | undefined,): number {
	const raw = maxBytes ?? 8 * 1024 * 1024;
	if (!Number.isInteger(raw,) || raw < MIN_RESULT_MAX_BYTES) {
		throw new ClientValidationError(
			`maxBytes must be a positive integer >= ${String(MIN_RESULT_MAX_BYTES,)} (got ${String(raw,)},)`,
		);
	}
	return Math.min(raw, MAX_RESULT_MAX_BYTES,);
}

function isRecord(value: unknown,): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value,);
}

/**
 * Run result of {@link MacrosResource.run} when the caller opted into
 * `wait: false` (the default): a run id for abort/state/result tracking.
 */
export type MacroRunHandle = MacroRunStart;

/**
 * When `wait: true`, DSS blocks server-side until the run finishes and then
 * returns the run id plus the terminal poll state (resultType, errors, …).
 */
export type MacroRunWaitedResult = MacroRunStart & { state?: MacroState; };

/**
 * Build the run POST body from params/adminParams. DSS macro runs accept
 * `params` keyed by the macro's declared param names and `adminParams` keyed
 * by its declared adminParams names; both are optional.
 */
function macroRunBody(
	params?: Record<string, unknown>,
	adminParams?: Record<string, unknown>,
): Record<string, unknown> {
	const body: Record<string, unknown> = {};
	if (params !== undefined) body["params"] = params;
	if (adminParams !== undefined) body["adminParams"] = adminParams;
	return body;
}

/**
 * Error payload surfaced by DSS on a failed macro run, from either
 * `resultError` or `storedError` in the poll state.
 */
export interface MacroRunFailure {
	source: "resultError" | "storedError";
	details: unknown;
}

function macroRunFailure(state: Record<string, unknown>,): MacroRunFailure | undefined {
	if (state["resultError"] !== undefined && state["resultError"] !== null) {
		return { source: "resultError", details: state["resultError"], };
	}
	if (state["storedError"] !== undefined && state["storedError"] !== null) {
		return { source: "storedError", details: state["storedError"], };
	}
	return undefined;
}

/**
 * DSS Macros — the plugin "runnable" surface under
 * `/public/api/projects/{pk}/runnables`.
 *
 * A macro run executes arbitrary plugin code on the DSS backend with
 * side effects that DSS cannot predict; every method here that starts or
 * stops a run is a write. Result bodies may be plain text or HTML rather
 * than JSON, so {@link MacrosResource.result} returns the raw body.
 */
export class MacrosResource extends BaseResource {
	/** List the macros (runnables) available in a project. */
	async list(projectKey?: string,): Promise<MacroSummary[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/runnables`,
		);
		return this.client.safeParse(MacroSummaryArraySchema, raw, "macros.list",);
	}

	/** Get the definition of a macro: params, result type, owning plugin. */
	async definition(
		runnableType: string,
		opts?: { projectKey?: string; },
	): Promise<MacroDefinition> {
		requireNonEmpty(runnableType, "runnableType",);
		const raw = await this.client.get<unknown>(
			`${this.base(runnableType, opts?.projectKey,)}`,
		);
		return this.client.safeParse(MacroDefinitionSchema, raw, "macros.definition",);
	}

	/**
	 * Start a macro run. Executes arbitrary plugin code: classify as
	 * destructive. With `wait: true` the server call blocks until completion
	 * and the resolved state is attached to the returned run handle.
	 */
	async run(
		runnableType: string,
		opts?: MacroRunOptions,
	): Promise<MacroRunHandle | MacroRunWaitedResult> {
		requireNonEmpty(runnableType, "runnableType",);
		const wait = opts?.wait === true;
		const query = wait ? "?wait=true" : "?wait=false";
		const raw = await this.client.post<unknown>(
			`${this.base(runnableType, opts?.projectKey,)}${query}`,
			macroRunBody(opts?.params, opts?.adminParams,),
		);
		const run = this.client.safeParse(MacroRunStartSchema, raw, "macros.run",);
		if (typeof run?.runId !== "string" || run.runId.trim().length === 0) {
			throw new DataikuError(
				200,
				"Unexpected Response",
				"macros.run: DSS did not return a run identifier.",
			);
		}
		if (!wait) return run;
		try {
			const state = await this.state(runnableType, run.runId, {
				projectKey: opts?.projectKey,
			},);
			return { ...run, state, };
		} catch {
			// The run started; a failed post-wait state probe must not lose the runId.
			return run;
		}
	}

	/** Request abort of a running macro run (204 response). */
	async abort(runnableType: string, runId: string, projectKey?: string,): Promise<void> {
		requireNonEmpty(runnableType, "runnableType",);
		requireNonEmpty(runId, "runId",);
		await this.client.post(
			`${this.base(runnableType, projectKey,)}/abort/${encodeURIComponent(runId,)}`,
			{},
		);
	}

	/** Get the poll state of a macro run (running flag, progress, errors). */
	async state(
		runnableType: string,
		runId: string,
		opts?: {
			projectKey?: string;
			/**
			 * TOTAL duration budget in ms for this state GET, measured from
			 * call start (bounds fetch, retries, backoff, and body read).
			 * Polling loops pass their remaining wait budget so a stalled
			 * state endpoint cannot outlive the wait deadline.
			 */
			timeoutMs?: number;
		},
	): Promise<MacroState> {
		requireNonEmpty(runnableType, "runnableType",);
		requireNonEmpty(runId, "runId",);
		const remainingMs = opts?.timeoutMs;
		const raw = await this.client.get<unknown>(
			`${this.base(runnableType, opts?.projectKey,)}/state/${encodeURIComponent(runId,)}`,
			...(remainingMs !== undefined ? [{ timeoutMs: remainingMs, },] : []),
		);
		return this.client.safeParse(MacroStateSchema, raw, "macros.state",);
	}

	/**
	 * Download the result of a macro run. Macro results are documents of the
	 * macro's declared `resultType` (HTML, text, JSON, …) and are frequently
	 * NOT JSON: the raw body is returned verbatim. JSON bodies are returned
	 * as parsed values so programmatic callers can read fields directly.
	 */
	async result(
		runnableType: string,
		runId: string,
		opts?: MacroResultOptions,
	): Promise<unknown> {
		requireNonEmpty(runnableType, "runnableType",);
		requireNonEmpty(runId, "runId",);
		const maxBytes = macroResultMaxBytes(opts?.maxBytes,);
		const raw = await this.client.getTextLimited(
			`${this.base(runnableType, opts?.projectKey,)}/result/${encodeURIComponent(runId,)}`,
			maxBytes,
		);
		if (raw.truncated) {
			throw new Error(
				`Macro result exceeded ${String(maxBytes,)} bytes; retry with a larger maxBytes.`,
			);
		}
		const text = raw.text;
		const trimmed = text.trim();
		if (
			trimmed.startsWith("{",) && trimmed.endsWith("}",)
			|| trimmed.startsWith("[",) && trimmed.endsWith("]",)
		) {
			try {
				return JSON.parse(text,) as unknown;
			} catch {
				// Not JSON after all — fall through and return the text.
			}
		}
		return text;
	}

	/**
	 * Run a macro and poll its state until it finishes or times out, using a
	 * bounded client-side loop on the state endpoint. Returns
	 * `{ success: false, timedOut: true }` on timeout rather than throwing.
	 */
	async runAndWait(
		runnableType: string,
		opts?: MacroWaitOptions,
	): Promise<MacroWaitResult> {
		requireNonEmpty(runnableType, "runnableType",);
		// Validate the wait knobs BEFORE the run POST: an invalid budget must be
		// a caller error, not a mutation whose wait behavior is undefined.
		if (opts?.pollIntervalMs !== undefined) {
			if (!Number.isFinite(opts.pollIntervalMs,) || opts.pollIntervalMs < 1) {
				throw new ClientValidationError("pollIntervalMs must be a finite number >= 1.",);
			}
		}
		if (opts?.timeoutMs !== undefined) {
			if (!Number.isFinite(opts.timeoutMs,) || opts.timeoutMs < 0) {
				throw new ClientValidationError("timeoutMs must be a finite number >= 0.",);
			}
		}
		const baseIntervalMs = Math.max(1, opts?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,);
		const adaptivePolling = opts?.pollIntervalMs === undefined;
		// One state observation always happens regardless of budget (poll
		// precedes deadline check), but never round a budget up to a full poll.
		const timeoutMs = Math.max(0, opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS,);
		const startedAt = Date.now();

		const run = await this.run(runnableType, {
			wait: false,
			projectKey: opts?.projectKey,
			params: opts?.params,
			adminParams: opts?.adminParams,
		},);
		const runId = run.runId;

		let pollCount = 0;
		let lastState: MacroState | undefined;
		const timedOutResult = (
			elapsedMs: number,
			state: MacroState | undefined,
		): MacroWaitResult =>
			this.client.safeParse(MacroWaitResultSchema, {
				runnableType,
				runId,
				running: true,
				success: false,
				timedOut: true,
				elapsedMs,
				pollCount,
				...(state ? { state, } : {}),
			}, "macros.runAndWait",);
		while (true) {
			pollCount += 1;
			try {
				lastState = await this.state(runnableType, runId, {
					projectKey: opts?.projectKey,
					timeoutMs: Math.max(1, timeoutMs - (Date.now() - startedAt),),
				},);
			} catch (error) {
				if (!isRequestDeadlineError(error, startedAt + timeoutMs,)) throw error;
				return timedOutResult(Date.now() - startedAt, lastState,);
			}
			const running = lastState.running === true;
			if (running !== true) {
				return this.client.safeParse(MacroWaitResultSchema, {
					runnableType,
					runId,
					running: false,
					success: macroRunFailure(lastState as Record<string, unknown>,) === undefined
						&& lastState.exists !== false,
					elapsedMs: Date.now() - startedAt,
					pollCount,
					...(macroRunFailure(lastState as Record<string, unknown>,)
						? { failure: macroRunFailure(lastState as Record<string, unknown>,), }
						: {}),
					state: lastState,
				}, "macros.runAndWait",);
			}
			const elapsedMs = Date.now() - startedAt;
			if (elapsedMs >= timeoutMs) {
				return timedOutResult(elapsedMs, lastState,);
			}
			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled: adaptivePolling,
			},);
			const { promise, resolve, } = Promise.withResolvers<void>();
			setTimeout(
				resolve,
				Math.min(nextDelayMs, Math.max(1, timeoutMs - (Date.now() - startedAt),),),
			);
			await promise;
		}
	}

	private base(runnableType: string, projectKey?: string,): string {
		return `/public/api/projects/${this.enc(projectKey,)}/runnables/${
			encodeURIComponent(runnableType,)
		}`;
	}
}

// Re-exported for CLI/plan use: failure extraction from a poll state record.
export { isRecord as isMacroStateRecord, macroRunFailure, };
