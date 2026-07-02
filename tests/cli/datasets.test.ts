import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI dataset validation", () => {
	it("reports file-backed dataset build blockers", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/broken_output",);
			sendJson(res, {
				name: "broken_output",
				type: "Filesystem",
				params: { connection: "filesystem_managed", },
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["dataset", "validate-build", "broken_output",], {
				env: cliEnv(url,),
			},);
			const result = JSON.parse(stdout,) as { valid: boolean; warnings: string[]; };
			expect(result.valid,).toBe(false,);
			expect(result.warnings,).toContain(
				"File-backed dataset has no writable storage path configured.",
			);
			expect(result.warnings,).toContain("File-backed dataset has no formatType configured.",);
		},);
	});

	it("refreshes dataset schema through schema endpoint", async () => {
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("PUT",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/schema",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, {},);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"refresh-schema",
				"orders",
				"--data",
				JSON.stringify({ columns: [{ name: "id", type: "bigint", },], },),
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				updated: "orders",
				resource: "dataset",
				schema: { columns: [{ name: "id", type: "bigint", },], },
			},);
		},);
		expect(requestBody,).toEqual({ columns: [{ name: "id", type: "bigint", },], },);
	});
});
