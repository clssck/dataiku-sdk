import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, sendJson, withCliServer, } from "./_harness.js";

const datasets = [
	{
		name: "orders",
		type: "S3",
		managed: true,
		params: { connection: "s3_managed", path: "/p/orders", formatParams: { separator: ",", }, },
		schema: { columns: [{ name: "id", type: "bigint", },], },
		creationTag: { versionNumber: 1, },
	},
	{
		name: "order_items",
		type: "Snowflake",
		managed: false,
		params: { connection: "sf", table: "ITEMS", },
	},
	{ name: "customers", type: "S3", managed: true, params: { connection: "s3_managed", }, },
];

async function listDatasets(args: string[],): Promise<{ stdout: string; stderr: string; }> {
	let result = { stdout: "", stderr: "", };
	await withCliServer((req, res,) => {
		expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
			"/public/api/projects/TEST/datasets/",
		);
		sendJson(res, datasets,);
	}, async (url,) => {
		result = await dss(["dataset", "list", ...args,], { env: cliEnv(url,), },);
	},);
	return result;
}

describe("list output shaping", () => {
	it("prints compact items by default and the DSS objects with --full", async () => {
		const compact = await listDatasets([],);
		expect(JSON.parse(compact.stdout,),).toEqual([
			{ name: "orders", type: "S3", managed: true, connection: "s3_managed", },
			{ name: "order_items", type: "Snowflake", managed: false, connection: "sf", },
			{ name: "customers", type: "S3", managed: true, connection: "s3_managed", },
		],);
		const full = await listDatasets(["--full",],);
		expect(JSON.parse(full.stdout,),).toEqual(datasets,);
	});

	it("projects --fields from the DSS objects", async () => {
		const { stdout, } = await listDatasets(["--fields", "name,params.path",],);
		expect(JSON.parse(stdout,),).toEqual([
			{ name: "orders", "params.path": "/p/orders", },
			{ name: "order_items", "params.path": null, },
			{ name: "customers", "params.path": null, },
		],);
	});

	it("filters with --contains before --limit and reports the cut on stderr", async () => {
		const { stdout, stderr, } = await listDatasets(["--contains", "ORDER", "--limit", "1",],);
		expect(JSON.parse(stdout,),).toEqual([{
			name: "orders",
			type: "S3",
			managed: true,
			connection: "s3_managed",
		},],);
		expect(JSON.parse(stderr,),).toMatchObject({
			type: "warning",
			warnings: [{ code: "list_truncated", shown: 1, total: 2, },],
		},);
	});
});
