import { describe, expect, it, } from "bun:test";
import { computeNextPollDelayMs, } from "../src/resources/jobs.js";

describe("computeNextPollDelayMs", () => {
	describe("non-adaptive", () => {
		it("returns baseIntervalMs regardless of pollCount", () => {
			expect(
				computeNextPollDelayMs({ pollCount: 1, baseIntervalMs: 2000, adaptiveEnabled: false, },),
			).toBe(2000,);
		});

		it("returns baseIntervalMs even at high pollCount", () => {
			expect(
				computeNextPollDelayMs({ pollCount: 100, baseIntervalMs: 2000, adaptiveEnabled: false, },),
			).toBe(2000,);
		});
	});

	describe("adaptive with base=2000", () => {
		const base = 2000;
		const cases: [number, number,][] = [
			[1, 2000,], // step=0, 2000*1=2000
			[2, 2000,], // step=0
			[3, 2000,], // step=0
			[4, 4000,], // step=1, 2000*2=4000
			[5, 4000,], // step=1
			[6, 4000,], // step=1
			[7, 8000,], // step=2, 2000*4=8000
			[10, 10000,], // step=3, 2000*8=16000, capped at 10000
			[100, 10000,], // huge interval, capped at 10000
		];

		for (const [pollCount, expected,] of cases) {
			it(`pollCount=${pollCount} → ${expected}ms`, () => {
				expect(
					computeNextPollDelayMs({ pollCount, baseIntervalMs: base, adaptiveEnabled: true, },),
				).toBe(expected,);
			});
		}
	});

	describe("adaptive with base=500", () => {
		const base = 500;
		const cases: [number, number,][] = [
			[1, 500,], // step=0, 500*1=500
			[7, 2000,], // step=2, 500*4=2000
			[10, 4000,], // step=3, 500*8=4000
			[16, 10000,], // step=5, 500*32=16000, capped at 10000
		];

		for (const [pollCount, expected,] of cases) {
			it(`pollCount=${pollCount} → ${expected}ms`, () => {
				expect(
					computeNextPollDelayMs({ pollCount, baseIntervalMs: base, adaptiveEnabled: true, },),
				).toBe(expected,);
			});
		}
	});

	describe("adaptive with base larger than MAX_POLL_INTERVAL_MS", () => {
		const base = 20000;

		it("pollCount=1 returns base even when above 10000", () => {
			expect(
				computeNextPollDelayMs({ pollCount: 1, baseIntervalMs: base, adaptiveEnabled: true, },),
			).toBe(20000,);
		});

		it("pollCount=4 caps at base when base exceeds MAX", () => {
			expect(
				computeNextPollDelayMs({ pollCount: 4, baseIntervalMs: base, adaptiveEnabled: true, },),
			).toBe(20000,);
		});
	});

	describe("edge cases", () => {
		it("pollCount=0 treats step as 0 and returns base", () => {
			expect(
				computeNextPollDelayMs({ pollCount: 0, baseIntervalMs: 2000, adaptiveEnabled: true, },),
			).toBe(2000,);
		});
	});
});
