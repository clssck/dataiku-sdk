/** Default fixed poll interval and overall wait budget for long-running DSS operations. */
export const DEFAULT_POLL_INTERVAL_MS = 2_000;
export const DEFAULT_TIMEOUT_MS = 120_000;

import { ClientValidationError, DataikuError, } from "../errors.js";

/**
 * Shared polling helpers used by every long-running wait loop (jobs, futures,
 * scenarios, macros, Visual ML training, Project Git futures).
 */

/** Hard ceiling for one adaptive backoff step (ms). */
export const MAX_POLL_INTERVAL_MS = 10_000;

interface ComputeNextPollDelayMsOptions {
	pollCount: number;
	baseIntervalMs: number;
	adaptiveEnabled: boolean;
}

/**
 * Compute the next poll delay.
 * When adaptive polling is enabled, the interval doubles every 3 polls,
 * capped at MAX_POLL_INTERVAL_MS (or baseIntervalMs if it's larger).
 */
export function computeNextPollDelayMs({
	pollCount,
	baseIntervalMs,
	adaptiveEnabled,
}: ComputeNextPollDelayMsOptions,): number {
	if (!adaptiveEnabled) {
		return baseIntervalMs;
	}
	const step = Math.max(0, Math.floor((pollCount - 1) / 3,),);
	const interval = baseIntervalMs * 2 ** step;
	return Math.min(interval, Math.max(baseIntervalMs, MAX_POLL_INTERVAL_MS,),);
}

/**
 * Validate a caller's wait budget. Call it before any request or mutation of
 * the operation, so a malformed budget can never leave started work behind a
 * thrown error. `undefined` (use the default) passes through.
 */
export function validateWaitBudgetMs(timeoutMs: number | undefined,): void {
	if (timeoutMs !== undefined && (!Number.isFinite(timeoutMs,) || timeoutMs < 0)) {
		throw new ClientValidationError("timeoutMs must be a finite non-negative number.",);
	}
}

/**
 * Request options for one status poll under a wait budget. GET budgets are
 * whole milliseconds >= 1, so a fractional remainder (a fractional caller
 * budget, or a fractional clock) rounds up; a spent budget still issues
 * exactly one transport attempt (no retries) so the first observation
 * reaches the server.
 */
export function pollRequestOptions(
	remainingMs: number,
): { timeoutMs: number; } | { noRetry: true; } {
	return remainingMs > 0 ? { timeoutMs: Math.ceil(remainingMs,), } : { noRetry: true, };
}

/** Distinguish an exhausted wait deadline from an earlier request timeout. */
export function isRequestDeadlineError(error: unknown, deadlineAt: number,): boolean {
	return Date.now() >= deadlineAt
		&& error instanceof DataikuError
		&& error.status === 0
		&& (error.retry?.timedOut === true || error.statusText === "Request Timeout");
}
