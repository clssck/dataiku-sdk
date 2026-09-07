import { describe, expect, it, } from "bun:test";
import { readdirSync, } from "node:fs";
import { apiServiceCommands, } from "../../src/cli/commands/api-service.js";
import { bundleCommands, } from "../../src/cli/commands/bundle.js";
import type { DataikuClient, } from "../../src/client.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	mkdirSync,
	readFileExists,
	readFileSync,
	rmSync,
	tmpdir,
	withCliServer,
} from "./_harness.js";

describe("CLI archive downloads", () => {
	it("rejects bodyless archive responses without creating output files", async () => {
		const root = join(tmpdir(), "dss-bodyless-" + Date.now(),);
		mkdirSync(root, { recursive: true, },);
		try {
			const client = {
				apiServices: { downloadPackageArchive: async () => new Response(null, { status: 204, },), },
				bundles: { downloadExportedArchive: async () => new Response(null, { status: 204, },), },
			} as unknown as DataikuClient;
			const cases = [
				{ command: apiServiceCommands["download-package"]!, args: ["service", "package",], },
				{ command: bundleCommands["download-exported"]!, args: ["bundle",], },
			];
			for (const { command, args, } of cases) {
				const output = join(root, args[0] + ".zip",);
				await expect(command.handler(client, args, { output, },),).rejects.toBeInstanceOf(Error,);
				expect(readFileExists(output,),).toBe(false,);
			}
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});

	it("exports a progressing archive beyond the timeout and fails a stalled body", async () => {
		const root = join(tmpdir(), "dss-archive-timeout-" + Date.now(),);
		mkdirSync(root, { recursive: true, },);
		let stall = false;
		try {
			await withCliServer(async (_req, res,) => {
				res.writeHead(200, { "Content-Type": "application/zip", },);
				res.write("one",);
				if (stall) return;
				await Bun.sleep(120,);
				res.write("two",);
				await Bun.sleep(120,);
				res.end("three",);
			}, async (url,) => {
				const output = join(root, "project.zip",);
				const argv = [
					"project",
					"export",
					"TEST",
					"--output",
					output,
					"--request-timeout",
					"200",
					"--retries",
					"1",
				];
				await dss(argv, { env: cliEnv(url,), },);
				expect(readFileSync(output, "utf-8",),).toBe("onetwothree",);
				stall = true;
				const failure = await dssFailure(argv, { env: cliEnv(url,), },);
				expect(failure.code,).toBe(3,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "transient",
					retryable: true,
					status: 0,
				},);
				expect(readFileSync(output, "utf-8",),).toBe("onetwothree",);
				const missingOutput = join(root, "new.zip",);
				const failedNew = await dssFailure(
					argv.map((arg, index,) => index === 4 ? missingOutput : arg),
					{ env: cliEnv(url,), },
				);
				expect(failedNew.code,).toBe(3,);
				expect(readFileExists(missingOutput,),).toBe(false,);
				expect(readdirSync(root,),).toEqual(["project.zip",],);
			},);
		} finally {
			rmSync(root, { recursive: true, force: true, },);
		}
	});
});
