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

	it("keeps whether each plugin is a development plugin in the compact plugin list", async () => {
		let stdout = "";
		await withCliServer((req, res,) => {
			expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe("/public/api/plugins/",);
			sendJson(res, [
				{ id: "dev-plugin", version: "0.0.1", isDev: true, meta: { label: "Dev", icon: "icon-x", }, },
				{ id: "store-plugin", version: "2.3.1", isDev: false, meta: { label: "Store", }, },
			],);
		}, async (url,) => {
			({ stdout, } = await dss(["plugin", "list",], { env: cliEnv(url,), },));
		},);
		expect(JSON.parse(stdout,),).toEqual([
			{ id: "dev-plugin", version: "0.0.1", label: "Dev", isDev: true, },
			{ id: "store-plugin", version: "2.3.1", label: "Store", isDev: false, },
		],);
	});

	it("reports in-progress jobs, which DSS lists without a state, as RUNNING and filters them", async () => {
		// DSS 15 /jobs/ shape: in-progress jobs carry stableState:false and no state.
		const jobs = [
			{
				def: { id: "build_b", name: "Build b", type: "RECURSIVE_BUILD", },
				stableState: false,
				startTime: 2,
				endTime: 0,
			},
			{
				def: { id: "build_a", name: "Build a", type: "RECURSIVE_BUILD", },
				stableState: true,
				state: "DONE",
				startTime: 1,
				endTime: 3,
			},
		];
		const listJobs = async (args: string[],): Promise<unknown> => {
			let stdout = "";
			await withCliServer((req, res,) => {
				expect(new URL(req.url ?? "/", "http://localhost",).pathname,).toBe(
					"/public/api/projects/TEST/jobs/",
				);
				sendJson(res, jobs,);
			}, async (url,) => {
				({ stdout, } = await dss(["job", "list", ...args,], { env: cliEnv(url,), },));
			},);
			return JSON.parse(stdout,);
		};
		const running = {
			id: "build_b",
			name: "Build b",
			type: "RECURSIVE_BUILD",
			state: "RUNNING",
			startTime: 2,
			endTime: 0,
		};
		expect(await listJobs([],),).toEqual([
			running,
			{
				id: "build_a",
				name: "Build a",
				type: "RECURSIVE_BUILD",
				state: "DONE",
				startTime: 1,
				endTime: 3,
			},
		],);
		expect(await listJobs(["--state", "running",],),).toEqual([running,],);
		expect(await listJobs(["--full",],),).toEqual(jobs,);
	});
});
