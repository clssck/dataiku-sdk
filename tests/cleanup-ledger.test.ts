import { afterEach, describe, expect, it, } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync, } from "node:fs";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { ClientValidationError, } from "../src/errors.js";
import { reserveCleanupLedgerDssUrl, } from "../src/utils/cleanup-ledger.js";

const tempDirs: string[] = [];

function tempLedger(): { dir: string; ledger: string; } {
	const dir = mkdtempSync(join(tmpdir(), "dss-ledger-reserve-",),);
	tempDirs.push(dir,);
	return { dir, ledger: join(dir, "cleanup.jsonl",), };
}

afterEach(() => {
	for (const dir of tempDirs.splice(0,)) rmSync(dir, { recursive: true, force: true, },);
},);

describe("reserveCleanupLedgerDssUrl", () => {
	it("lets many concurrent same-server reservations all succeed", async () => {
		for (let round = 0; round < 10; round++) {
			const { dir, ledger, } = tempLedger();
			const results = await Promise.allSettled(
				Array.from(
					{ length: 24, },
					() => reserveCleanupLedgerDssUrl(ledger, "https://dss.example.test/",),
				),
			);
			expect(results.filter((result,) => result.status === "rejected",),).toEqual([],);
			expect(readFileSync(`${ledger}.dss-url`, "utf-8",).trim(),).toBe("https://dss.example.test",);
			// Only the ledger binding remains: no temporary publication file.
			expect(readdirSync(dir,),).toEqual(["cleanup.jsonl.dss-url",],);
		}
	});

	it("keeps the binding owner-only", async () => {
		const { ledger, } = tempLedger();
		await reserveCleanupLedgerDssUrl(ledger, "https://dss.example.test",);
		if (process.platform !== "win32") {
			expect(statSync(`${ledger}.dss-url`,).mode & 0o777,).toBe(0o600,);
		}
	});

	it("rejects a different server and keeps the first binding", async () => {
		const { dir, ledger, } = tempLedger();
		await reserveCleanupLedgerDssUrl(ledger, "https://dss.example.test",);
		await expect(reserveCleanupLedgerDssUrl(ledger, "https://other.example.test",),).rejects
			.toBeInstanceOf(ClientValidationError,);
		expect(readFileSync(`${ledger}.dss-url`, "utf-8",).trim(),).toBe("https://dss.example.test",);
		expect(readdirSync(dir,),).toEqual(["cleanup.jsonl.dss-url",],);
	});

	it("lets exactly one of several concurrent different servers bind the ledger", async () => {
		const { ledger, } = tempLedger();
		const hosts = ["a", "b", "c", "d", "e", "f",].map((name,) => `https://${name}.example.test`,);
		const results = await Promise.allSettled(
			hosts.map((host,) => reserveCleanupLedgerDssUrl(ledger, host,),),
		);
		expect(results.filter((result,) => result.status === "fulfilled",),).toHaveLength(1,);
		const bound = readFileSync(`${ledger}.dss-url`, "utf-8",).trim();
		expect(hosts,).toContain(bound,);
	});
});
