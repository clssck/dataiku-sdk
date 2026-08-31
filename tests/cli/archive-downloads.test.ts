import { describe, expect, it, } from "bun:test";
import { apiServiceCommands, } from "../../src/cli/commands/api-service.js";
import { bundleCommands, } from "../../src/cli/commands/bundle.js";
import type { DataikuClient, } from "../../src/client.js";

async function rejectedError(work: () => Promise<unknown>,): Promise<Error> {
	try {
		await work();
	} catch (error) {
		return error instanceof Error ? error : new Error(String(error,),);
	}
	throw new Error("expected operation to reject",);
}

describe("CLI archive downloads", () => {
	it("rejects an API service package response without a body", async () => {
		const client = {
			apiServices: {
				downloadPackageArchive: async () => new Response(null, { status: 204, },),
			},
		} as unknown as DataikuClient;

		const error = await rejectedError(() =>
			apiServiceCommands["download-package"]!.handler(
				client,
				["service", "package",],
				{ output: "/tmp/unused-api-service-package.zip", },
			)
		);
		expect(error.message,).toBe(
			"apiServices.downloadPackageArchive response did not include a body",
		);
	});

	it("rejects an exported bundle response without a body", async () => {
		const client = {
			bundles: {
				downloadExportedArchive: async () => new Response(null, { status: 204, },),
			},
		} as unknown as DataikuClient;

		const error = await rejectedError(() =>
			bundleCommands["download-exported"]!.handler(
				client,
				["bundle",],
				{ output: "/tmp/unused-exported-bundle.zip", },
			)
		);
		expect(error.message,).toBe("bundles.downloadExportedArchive response did not include a body",);
	});
});
