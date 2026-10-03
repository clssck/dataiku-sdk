import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, sendJson, withCliServer, } from "./_harness.js";

describe("field projection warnings", () => {
	it("lists sibling keys under the resolved parent for a missing nested field", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/one",);
			sendJson(res, {
				type: "S3",
				name: "one",
				projectKey: "TEST",
				params: { path: "/data/one", connection: "s3", },
			},);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "get", "one", "--fields", "params.nope",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({ "params.nope": null, },);
			const event = JSON.parse(stderr,) as {
				type: string;
				warnings: Array<Record<string, unknown>>;
			};
			expect(event.type,).toBe("warning",);
			expect(event.warnings,).toEqual([
				{
					code: "field_projection_missing",
					fields: ["params.nope",],
					availableFields: ["params.connection", "params.path",],
					hint: "Use fields from the command's schemas.output contract or an unprojected result.",
				},
			],);
		},);
	});

	it("keeps top-level keys for a missing top-level field", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { type: "S3", name: "one", params: { path: "/data/one", }, },);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "get", "one", "--fields", "nope",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({ nope: null, },);
			const event = JSON.parse(stderr,) as { warnings: Array<Record<string, unknown>>; };
			expect(event.warnings,).toEqual([
				{
					code: "field_projection_missing",
					fields: ["nope",],
					availableFields: ["name", "params", "type",],
					hint: "Use fields from the command's schemas.output contract or an unprojected result.",
				},
			],);
		},);
	});

	it("reports no available fields when the dotted parent is itself missing", async () => {
		await withCliServer((_req, res,) => {
			sendJson(res, { type: "S3", name: "one", },);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "get", "one", "--fields", "params.path",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual({ "params.path": null, },);
			const event = JSON.parse(stderr,) as { warnings: Array<Record<string, unknown>>; };
			expect(event.warnings,).toEqual([
				{
					code: "field_projection_missing",
					fields: ["params.path",],
					availableFields: [],
					hint: "Use fields from the command's schemas.output contract or an unprojected result.",
				},
			],);
		},);
	});
});
