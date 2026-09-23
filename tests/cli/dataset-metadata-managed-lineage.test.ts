import { describe, expect, it, } from "bun:test";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI dataset metadata-set", () => {
	it("replaces metadata through PUT and returns the server object", async () => {
		let method = "";
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "PUT" && url.pathname === "/public/api/projects/TEST/datasets/orders/metadata"
			) {
				method = req.method;
				requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
				sendJson(res, requestBody,);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"metadata-set",
				"orders",
				"--data",
				JSON.stringify({ label: "Orders", tags: ["prod",], checklists: { items: [], }, },),
			], { env: cliEnv(url,), },);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toMatchObject({
				updated: "orders",
				resource: "dataset",
				metadata: { label: "Orders", tags: ["prod",], checklists: { items: [], }, },
			},);
		},);
		// Faithful replace: the exact input object, no merge with server state.
		expect(method,).toBe("PUT",);
		expect(requestBody,).toEqual({
			label: "Orders",
			tags: ["prod",],
			checklists: { items: [], },
		},);
	});

	it("dry-runs with zero HTTP and echoes the validated replacement", async () => {
		let requestCount = 0;
		await withCliServer((_req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end("dry-run must not contact DSS",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"metadata-set",
				"orders",
				"--data",
				'{"label":"new","tags":["x"]}',
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				dryRun: true,
				action: "metadata-set",
				resource: "dataset",
				name: "orders",
				next: { label: "new", tags: ["x",], },
			},);
		},);
		expect(requestCount,).toBe(0,);
	});
	it("rejects missing JSON input before any request", async () => {
		let requestCount = 0;
		await withCliServer((_req, res,) => {
			requestCount++;
			res.end("{}",);
		}, async (url,) => {
			const failure = await dssFailure(["dataset", "metadata-set", "orders",], {
				env: cliEnv(url,),
			},);
			expect(failure.code,).toBe(1,);
			expect(failure.stdout,).toContain("--data, --data-file, or --stdin is required",);
		},);
		expect(requestCount,).toBe(0,);
	});
});

describe("CLI dataset create-managed", () => {
	it("posts server-derived creationSettings and reports creation", async () => {
		let method = "";
		let path = "";
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			method = req.method ?? "";
			path = url.pathname;
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const { stdout, stderr, } = await dss([
				"dataset",
				"create-managed",
				"--name",
				"orders_copy",
				"--connection",
				"filesystem_managed",
				"--format-option-id",
				"PARQUET_HIVE",
			], { env: cliEnv(url,), },);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toEqual({
				created: "orders_copy",
				resource: "dataset",
				managed: true,
				projectKey: "TEST",
				creationSettings: {
					connectionId: "filesystem_managed",
					specificSettings: { formatOptionId: "PARQUET_HIVE", },
				},
			},);
		},);
		expect(method,).toBe("POST",);
		expect(path,).toBe("/public/api/projects/TEST/datasets/managed",);
		expect(requestBody,).toEqual({
			name: "orders_copy",
			creationSettings: {
				connectionId: "filesystem_managed",
				specificSettings: { formatOptionId: "PARQUET_HIVE", },
			},
		},);
	});

	it("copies partitioning from a dataset or folder", async () => {
		const bodies: Record<string, unknown>[] = [];
		await withCliServer(async (req, res,) => {
			bodies.push(JSON.parse(await readBody(req,),) as Record<string, unknown>,);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			await dss([
				"dataset",
				"create-managed",
				"--name",
				"p1",
				"--connection",
				"c",
				"--copy-partitioning-from",
				"raw_events",
			], { env: cliEnv(url,), },);
			await dss([
				"dataset",
				"create-managed",
				"--name",
				"p2",
				"--connection",
				"c",
				"--copy-partitioning-from",
				"fold1",
				"--partitioning-folder",
			], { env: cliEnv(url,), },);
		},);
		expect(bodies[0],).toEqual({
			name: "p1",
			creationSettings: {
				connectionId: "c",
				specificSettings: { partitioningOptionId: "copy:dataset:raw_events", },
			},
		},);
		expect(bodies[1],).toEqual({
			name: "p2",
			creationSettings: {
				connectionId: "c",
				specificSettings: { partitioningOptionId: "copy:folder:fold1", },
			},
		},);
	});

	it("dry-runs without contacting DSS and carries the exact payload", async () => {
		let requestCount = 0;
		await withCliServer((_req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end("dry-run must not contact DSS",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"create-managed",
				"--name",
				"planned",
				"--connection",
				"s3_conn",
				"--type-option-id",
				"HDFS",
				"--format-option-id",
				"ORC",
				"--dry-run",
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toEqual({
				dryRun: true,
				action: "create-managed",
				resource: "dataset",
				name: "planned",
				connection: "s3_conn",
				typeOptionId: "HDFS",
				formatOptionId: "ORC",
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("validates required flags as usage errors", async () => {
		await withCliServer((_req, res,) => {
			res.end();
		}, async (url,) => {
			const missingName = await dssFailure(["dataset", "create-managed", "--connection", "c",], {
				env: cliEnv(url,),
			},);
			expect(missingName.code,).toBe(1,);
			expect(missingName.stdout,).toContain("--name is required",);

			const missingConnection = await dssFailure(
				["dataset", "create-managed", "--name", "n",],
				{ env: cliEnv(url,), },
			);
			expect(missingConnection.code,).toBe(1,);
			expect(missingConnection.stdout,).toContain("--connection is required",);
		},);
	});
});

describe("CLI dataset info", () => {
	it("returns the full opaque info object", async () => {
		const info = {
			name: "orders",
			type: "Filesystem",
			schema: { columns: [{ name: "id", type: "bigint", },], },
			lastBuild: { state: "OK", },
		};
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/info",);
			sendJson(res, info,);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(["dataset", "info", "orders",], {
				env: cliEnv(url,),
			},);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toEqual(info,);
		},);
	});
});

describe("CLI dataset column-lineage", () => {
	it("queries the per-dataset route with columnName and optional maxDatasetCount", async () => {
		const lineage = [{
			inputColumn: "mycol1",
			inputDataset: "myproject1.mydataset1",
			outputColumn: "mycol2",
			outputDataset: "myproject1.mydataset2",
		},];
		const paths: string[] = [];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			paths.push(url.pathname + url.search,);
			sendJson(res, lineage,);
		}, async (url,) => {
			const { stdout, stderr, } = await dss(
				["dataset", "column-lineage", "orders", "mycol1", "--max-dataset-count", "2000",],
				{ env: cliEnv(url,), },
			);
			expect(stderr,).toBe("",);
			expect(JSON.parse(stdout,),).toEqual(lineage,);
			await dss(["dataset", "column-lineage", "orders", "mycol1",], { env: cliEnv(url,), },);
		},);
		expect(paths,).toEqual([
			"/public/api/projects/TEST/datasets/orders/column-lineage?columnName=mycol1&maxDatasetCount=2000",
			"/public/api/projects/TEST/datasets/orders/column-lineage?columnName=mycol1",
		],);
	});

	it("validates maxDatasetCount as a positive integer usage error", async () => {
		await withCliServer((_req, res,) => {
			res.end("[]",);
		}, async (url,) => {
			const failure = await dssFailure([
				"dataset",
				"column-lineage",
				"orders",
				"col",
				"--max-dataset-count",
				"0",
			], { env: cliEnv(url,), },);
			expect(failure.code,).toBe(1,);
			expect(failure.stdout,).toContain("--max-dataset-count must be a positive integer",);
		},);
	});
});
