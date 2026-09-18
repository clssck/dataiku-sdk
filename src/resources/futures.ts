import type { FutureState, FutureWaitResult, } from "../schemas.js";
import { FutureStateSchema, FutureWaitResultSchema, } from "../schemas.js";
import { computeNextPollDelayMs, isRequestDeadlineError, } from "../utils/polling.js";
import { BaseResource, } from "./base.js";

const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_TIMEOUT_MS = 120_000;

export interface FutureWaitOptions {
	pollIntervalMs?: number;
	timeoutMs?: number;
}

function sleep(ms: number,): Promise<void> {
	return new Promise((resolve,) => setTimeout(resolve, ms,));
}

function isFinished(state: FutureState,): boolean {
	return state.hasResult === true
		|| state.aborted === true
		|| state.alive === false
		|| state.unknown === true;
}

function waitState(state: FutureState,): string {
	if (state.hasResult === true) return "DONE";
	if (state.aborted === true) return "ABORTED";
	if (state.unknown === true) return "UNKNOWN";
	if (state.alive === false) return "FAILED";
	return "RUNNING";
}

export class FuturesResource extends BaseResource {
	async get(
		futureId: string,
		opts: { timeoutMs?: number; noRetry?: boolean; } = {},
	): Promise<FutureState> {
		return this.state(futureId, {
			peek: false,
			timeoutMs: opts.timeoutMs,
			noRetry: opts.noRetry,
		},);
	}

	async peek(futureId: string,): Promise<FutureState> {
		return this.state(futureId, { peek: true, },);
	}

	async state(
		futureId: string,
		opts: { peek?: boolean; timeoutMs?: number; noRetry?: boolean; } = {},
	): Promise<FutureState> {
		const params = new URLSearchParams();
		params.set("peek", String(opts.peek === true,),);
		const raw = await this.client.get<unknown>(
			`/public/api/futures/${encodeURIComponent(futureId,)}?${params.toString()}`,
			{ timeoutMs: opts.timeoutMs, noRetry: opts.noRetry, },
		);
		return this.client.safeParse(FutureStateSchema, raw, "futures.state",);
	}

	async abort(futureId: string,): Promise<void> {
		await this.client.del(`/public/api/futures/${encodeURIComponent(futureId,)}`,);
	}

	async wait(futureId: string, opts: FutureWaitOptions = {},): Promise<FutureWaitResult> {
		// Adaptive backoff is the default; an explicit pollIntervalMs is an
		// exact contract the caller chose, so it is never adapted.
		const explicitIntervalMs = opts.pollIntervalMs;
		const baseIntervalMs = Math.max(
			1,
			explicitIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
		);
		const adaptiveEnabled = explicitIntervalMs === undefined;
		// A caller's budget is never rounded up to a whole poll interval: a 5ms
		// wait must answer in about 5ms. One state observation always happens
		// regardless, because the poll precedes the deadline check.
		const timeoutMs = Math.max(0, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,);
		const startedAt = Date.now();
		let pollCount = 0;
		let lastState: FutureState | undefined;

		while (true) {
			const elapsedBeforeMs = Date.now() - startedAt;
			// The first observation always happens, even when the budget is
			// already spent. A later poll is never started after the deadline:
			// the loop reports the structured timeout from the last observed
			// state instead of issuing a request the budget cannot cover.
			if (lastState !== undefined && elapsedBeforeMs >= timeoutMs) {
				return this.client.safeParse(
					FutureWaitResultSchema,
					{
						futureId,
						jobId: lastState.jobId,
						state: waitState(lastState,),
						elapsedMs: elapsedBeforeMs,
						pollCount,
						success: false,
						timedOut: true,
						hasResult: lastState.hasResult === true,
						alive: lastState.alive,
						aborted: lastState.aborted,
						unknown: lastState.unknown,
					},
					"futures.wait",
				);
			}
			pollCount += 1;
			// Requests issued while budget remains are bounded by the remaining
			// time; a spent budget still issues exactly one transport attempt
			// (no retries, client requestTimeoutMs cap) so the first observation
			// reaches the server instead of failing before any attempt.
			const remainingMs = timeoutMs - elapsedBeforeMs;
			let state: FutureState;
			try {
				state = await this.get(
					futureId,
					remainingMs > 0 ? { timeoutMs: remainingMs, } : { noRetry: true, },
				);
			} catch (error) {
				// The poll budget ran out mid-request: report the structured
				// timeout instead of letting the transport deadline error escape.
				if (!isRequestDeadlineError(error, startedAt + timeoutMs,)) throw error;
				return this.client.safeParse(
					FutureWaitResultSchema,
					{
						futureId,
						state: "RUNNING",
						elapsedMs: Date.now() - startedAt,
						pollCount,
						success: false,
						timedOut: true,
					},
					"futures.wait",
				);
			}
			lastState = state;
			const elapsedMs = Date.now() - startedAt;
			const status = waitState(state,);

			if (isFinished(state,)) {
				const result = {
					futureId,
					jobId: state.jobId,
					state: status,
					elapsedMs,
					pollCount,
					success: state.hasResult === true,
					hasResult: state.hasResult === true,
					alive: state.alive,
					aborted: state.aborted,
					unknown: state.unknown,
					...(state.result !== undefined ? { result: state.result, } : {}),
				};
				return this.client.safeParse(FutureWaitResultSchema, result, "futures.wait",);
			}

			// Deadline reached: the guard at the top of the loop reports the
			// timeout from this observation without issuing another request.
			if (elapsedMs >= timeoutMs) continue;

			// Only the remaining budget is slept: a longer sleep would report an
			// elapsed time the caller never authorized.
			const nextDelayMs = computeNextPollDelayMs({
				pollCount,
				baseIntervalMs,
				adaptiveEnabled,
			},);
			await sleep(Math.min(nextDelayMs, timeoutMs - elapsedMs,),);
		}
	}
}
