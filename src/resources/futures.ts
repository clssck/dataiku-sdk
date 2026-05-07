import type { FutureState, FutureWaitResult, } from "../schemas.js";
import { FutureStateSchema, FutureWaitResultSchema, } from "../schemas.js";
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
	async get(futureId: string,): Promise<FutureState> {
		return this.state(futureId, { peek: false, },);
	}

	async peek(futureId: string,): Promise<FutureState> {
		return this.state(futureId, { peek: true, },);
	}

	async state(futureId: string, opts: { peek?: boolean; } = {},): Promise<FutureState> {
		const params = new URLSearchParams();
		params.set("peek", String(opts.peek === true,),);
		const raw = await this.client.get<unknown>(
			`/public/api/futures/${encodeURIComponent(futureId,)}?${params.toString()}`,
		);
		return this.client.safeParse(FutureStateSchema, raw, "futures.state",);
	}

	async abort(futureId: string,): Promise<void> {
		await this.client.del(`/public/api/futures/${encodeURIComponent(futureId,)}`,);
	}

	async wait(futureId: string, opts: FutureWaitOptions = {},): Promise<FutureWaitResult> {
		const baseIntervalMs = Math.max(1, opts.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,);
		const timeoutMs = Math.max(baseIntervalMs, opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,);
		const startedAt = Date.now();
		let pollCount = 0;

		while (true) {
			pollCount += 1;
			const state = await this.get(futureId,);
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

			if (elapsedMs >= timeoutMs) {
				const result = {
					futureId,
					jobId: state.jobId,
					state: status,
					elapsedMs,
					pollCount,
					success: false,
					timedOut: true,
					hasResult: state.hasResult === true,
					alive: state.alive,
					aborted: state.aborted,
					unknown: state.unknown,
				};
				return this.client.safeParse(FutureWaitResultSchema, result, "futures.wait",);
			}

			await sleep(baseIntervalMs,);
		}
	}
}
