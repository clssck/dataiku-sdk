import { DataikuError, } from "../errors.js";

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

/** Distinguish an exhausted wait deadline from an earlier request timeout. */
export function isRequestDeadlineError(error: unknown, deadlineAt: number,): boolean {
	return Date.now() >= deadlineAt
		&& error instanceof DataikuError
		&& error.status === 0
		&& (error.retry?.timedOut === true || error.statusText === "Request Timeout");
}
