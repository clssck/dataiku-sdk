import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type Server, type ServerResponse, } from "node:http";
import { DataikuClient, } from "../src/client.js";

function createClient(url: string,): DataikuClient {
	return new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
}

async function readBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) body += chunk.toString();
	return body;
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => void | Promise<void>,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server: Server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch(() => {
			res.statusCode = 500;
			res.end();
		},);
	},);
	await new Promise<void>((resolve,) => server.listen(0, "127.0.0.1", () => resolve(),));
	const addr = server.address();
	const url = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
	try {
		await run(url,);
	} finally {
		server.close();
	}
}

describe("DatasetsResource metadata PUT", () => {
	it("replaces metadata verbatim, preserving optional server fields the caller sends back", async () => {
		let method = "";
		let path = "";
		let body: Record<string, unknown> | undefined;
		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (
				req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/orders/metadata"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ label: "old", tags: ["a",], checklists: { items: [], }, },),);
				return;
			}
			method = req.method ?? "";
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/metadata",);
			body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(body,),);
		}, async (url,) => {
			const client = createClient(url,);
			const current = await client.datasets.metadata("orders",);
			// Caller edits the full object (documented GET -> edit -> PUT flow).
			const next = { ...current, label: "new", tags: ["prod",], };
			const updated = await client.datasets.updateMetadata("orders", next,);
			expect(updated,).toEqual(next,);
		},);
		expect(method,).toBe("PUT",);
		// Faithful replace: the body is the full edited object, no merge artifacts.
		expect(body,).toEqual({
			label: "new",
			tags: ["prod",],
			checklists: { items: [], },
		},);
	});

	it("URL-encodes the dataset name", async () => {
		let path = "";
		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			path = url.pathname;
			res.end(JSON.stringify({},),);
		}, async (url,) => {
			await createClient(url,).datasets.updateMetadata("my ds", { label: "x", },);
		},);
		expect(path,).toBe("/public/api/projects/TEST/datasets/my%20ds/metadata",);
	});

	it("rejects non-object metadata before any request", async () => {
		let requestCount = 0;
		await withServer((_req, res,) => {
			requestCount++;
			res.end("{}",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.datasets.updateMetadata("orders", null as unknown as Record<string, unknown>,),
			).rejects.toThrow("metadata must be a JSON object",);
			await expect(
				client.datasets.updateMetadata(
					"orders",
					[1, 2,] as unknown as Record<string, unknown>,
				),
			).rejects.toThrow("metadata must be a JSON object",);
		},);
		expect(requestCount,).toBe(0,);
	});
});

describe("DatasetsResource.createManaged", () => {
	it("posts name and connection-derived creationSettings to the managed endpoint", async () => {
		let method = "";
		let path = "";
		let body: Record<string, unknown> | undefined;
		await withServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			method = req.method ?? "";
			path = url.pathname;
			body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const result = await createClient(url,).datasets.createManaged({
				name: "orders_copy",
				connection: "filesystem_managed",
			},);
			expect(result,).toEqual({
				datasetName: "orders_copy",
				projectKey: "TEST",
				creationSettings: { connectionId: "filesystem_managed", },
			},);
		},);
		expect(method,).toBe("POST",);
		expect(path,).toBe("/public/api/projects/TEST/datasets/managed",);
		// Only server-documented fields; no fabricated defaults.
		expect(body,).toEqual({
			name: "orders_copy",
			creationSettings: { connectionId: "filesystem_managed", },
		},);
	});

	it("maps optional type, format, and copied partitioning options", async () => {
		let body: Record<string, unknown> | undefined;
		await withServer(async (req, res,) => {
			body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			await createClient(url,).datasets.createManaged({
				name: "events_parquet",
				connection: "s3_conn",
				typeOptionId: "HDFS",
				formatOptionId: "PARQUET_HIVE",
				copyPartitioningFrom: { ref: "raw_events", },
			},);
		},);
		expect(body,).toEqual({
			name: "events_parquet",
			creationSettings: {
				connectionId: "s3_conn",
				typeOptionId: "HDFS",
				specificSettings: {
					formatOptionId: "PARQUET_HIVE",
					partitioningOptionId: "copy:dataset:raw_events",
				},
			},
		},);
	});

	it("builds a folder partitioning copy id from copyPartitioningFrom.type", async () => {
		let body: Record<string, unknown> | undefined;
		await withServer(async (req, res,) => {
			body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			await createClient(url,).datasets.createManaged({
				name: "p2",
				connection: "c",
				copyPartitioningFrom: { ref: "fold1", type: "FOLDER", },
			},);
		},);
		expect(
			(body!.creationSettings as Record<string, unknown>).specificSettings,
		).toEqual({ partitioningOptionId: "copy:folder:fold1", },);
	});

	it("accepts a raw partitioningOptionId unchanged", async () => {
		let body: Record<string, unknown> | undefined;
		await withServer(async (req, res,) => {
			body = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			await createClient(url,).datasets.createManaged({
				name: "p3",
				connection: "c",
				partitioningOptionId: "copy:dataset:raw_events",
			},);
		},);
		expect(
			(body!.creationSettings as Record<string, unknown>).specificSettings,
		).toEqual({ partitioningOptionId: "copy:dataset:raw_events", },);
	});

	it("validates required fields and conflicting options before any request", async () => {
		let requestCount = 0;
		await withServer((_req, res,) => {
			requestCount++;
			res.end();
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.datasets.createManaged({ name: "", connection: "c", },),)
				.rejects.toThrow("name must be a non-empty dataset name",);
			await expect(client.datasets.createManaged({ name: "n", connection: "  ", },),)
				.rejects.toThrow("connection must be a non-empty connection name",);
			await expect(client.datasets.createManaged({
				name: "n",
				connection: "c",
				partitioningOptionId: "x",
				copyPartitioningFrom: { ref: "y", },
			},),).rejects.toThrow("Pass either partitioningOptionId or copyPartitioningFrom",);
			await expect(client.datasets.createManaged({
				name: "n",
				connection: "c",
				copyPartitioningFrom: { ref: "", },
			},),).rejects.toThrow("copyPartitioningFrom.ref must be a non-empty",);
		},);
		expect(requestCount,).toBe(0,);
	});
});

describe("DatasetsResource.info", () => {
	it("gets the full opaque info object", async () => {
		const infoPayload = {
			name: "orders",
			type: "Filesystem",
			schema: { columns: [{ name: "id", type: "bigint", },], },
			lastBuild: { state: "OK", },
		};
		let requestCount = 0;
		await withServer((req, res,) => {
			requestCount++;
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/info",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(infoPayload,),);
		}, async (url,) => {
			await expect(createClient(url,).datasets.info("orders",),).resolves.toEqual(infoPayload,);
		},);
		expect(requestCount,).toBe(1,);
	});
});

describe("DatasetsResource.getColumnLineage", () => {
	it("queries the per-dataset column-lineage route with columnName and maxDatasetCount", async () => {
		const lineage = [{
			inputColumn: "mycol1",
			inputDataset: "myproject1.mydataset1",
			outputColumn: "mycol2",
			outputDataset: "myproject1.mydataset2",
		},];
		let path = "";
		await withServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			path = url.pathname + url.search;
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(lineage,),);
		}, async (url,) => {
			await expect(
				createClient(url,).datasets.getColumnLineage("orders", "mycol1", { maxDatasetCount: 2000, },),
			).resolves.toEqual(lineage,);
		},);
		// Per the official Python client (dataikuapi.dss.dataset.DSSDataset.get_column_lineage):
		// GET /projects/{projectKey}/datasets/{datasetName}/column-lineage.
		expect(path,).toBe(
			"/public/api/projects/TEST/datasets/orders/column-lineage?columnName=mycol1&maxDatasetCount=2000",
		);
	});

	it("omits maxDatasetCount when not requested and URL-encodes the column", async () => {
		let path = "";
		await withServer((req, res,) => {
			path = new URL(req.url ?? "/", "http://localhost",).search;
			res.setHeader("Content-Type", "application/json",);
			res.end("[]",);
		}, async (url,) => {
			await createClient(url,).datasets.getColumnLineage("orders", "my col",);
		},);
		expect(path,).toBe("?columnName=my%20col",);
	});

	it("rejects empty columns and non-positive maxDatasetCount before any request", async () => {
		let requestCount = 0;
		await withServer((_req, res,) => {
			requestCount++;
			res.end("[]",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.datasets.getColumnLineage("orders", "",),).rejects.toThrow(
				"column must be a non-empty column name",
			);
			await expect(client.datasets.getColumnLineage("orders", "   ",),).rejects.toThrow(
				"column must be a non-empty column name",
			);
			for (const bad of [0, -1, 1.5, Number.NaN,]) {
				await expect(
					client.datasets.getColumnLineage("orders", "c", { maxDatasetCount: bad, },),
				).rejects.toThrow("maxDatasetCount must be a positive integer",);
			}
		},);
		expect(requestCount,).toBe(0,);
	});
});
