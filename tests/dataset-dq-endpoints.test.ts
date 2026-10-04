import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../src/client.js";

interface Seen {
	method: string;
	url: string;
	body: unknown;
}

async function recordRequests(
	respond: (req: IncomingMessage, res: ServerResponse,) => void,
	run: (client: DataikuClient,) => Promise<void>,
): Promise<Seen[]> {
	const seen: Seen[] = [];
	const server = createServer(async (req, res,) => {
		let text = "";
		for await (const chunk of req) text += chunk.toString();
		seen.push({
			method: req.method ?? "",
			url: req.url ?? "",
			body: text ? JSON.parse(text,) : undefined,
		},);
		respond(req, res,);
	},);
	const listening = Promise.withResolvers<void>();
	server.listen(0, "127.0.0.1", () => listening.resolve(),);
	await listening.promise;
	const { port, } = server.address() as AddressInfo;
	try {
		await run(
			new DataikuClient({
				url: `http://127.0.0.1:${port}`,
				apiKey: "k",
				projectKey: "TEST",
				retryMaxAttempts: 1,
			},),
		);
	} finally {
		const closed = Promise.withResolvers<void>();
		server.close(() => closed.resolve());
		await closed.promise;
	}
	return seen;
}

const json = (res: ServerResponse, body: unknown,) => {
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
};

describe("dataset data selection", () => {
	it("reads selected columns and partitions through the POST data endpoint with a row cap", async () => {
		const seen = await recordRequests(
			(_req, res,) => res.end("id\tamount\n1\t2\n",),
			async (client,) => {
				const preview = await client.datasets.preview("orders", {
					maxRows: 5,
					columns: ["id", "amount",],
					partitions: "2026-01",
				},);
				expect(preview.rows,).toEqual([["1", "2",],],);
				await client.datasets.preview("orders", { maxRows: 5, },);
			},
		);
		expect(seen,).toEqual([
			{
				method: "POST",
				url: "/public/api/projects/TEST/datasets/orders/data/",
				body: {
					format: "tsv-excel-header",
					columns: ["id", "amount",],
					partitions: "2026-01",
					sampling: { samplingMethod: "HEAD_SEQUENTIAL", maxRecords: 6, },
				},
			},
			// Without a selection the GET endpoint stays in use.
			{
				method: "GET",
				url: "/public/api/projects/TEST/datasets/orders/data/?format=tsv-excel-header&limit=6",
				body: undefined,
			},
		],);
	});
});

describe("dataset actions, checks, and data quality", () => {
	it("calls the documented routes", async () => {
		const seen = await recordRequests((req, res,) => {
			if (req.method === "DELETE" || req.url?.includes("Hive",)) {
				res.statusCode = 204;
				res.end();
				return;
			}
			json(res, {},);
		}, async (client,) => {
			await client.datasets.synchronizeHiveMetastore("logs",);
			await client.datasets.updateFromHive("logs",);
			await client.metrics.runDatasetChecks("logs", {
				partitions: "2026-01",
				checks: [{ type: "numericRange", },],
			},);
			await client.dataQuality.instanceStatus();
			await client.dataQuality.partitionsStatus("logs",);
			await client.dataQuality.partitionsStatus("logs", { partitions: ["a", "b",], },);
			await client.dataQuality.deleteHistory("logs", { partition: "ALL", },);
		},);
		expect(seen.map(({ method, url, },) => `${method} ${url}`),).toEqual([
			"POST /public/api/projects/TEST/datasets/logs/actions/synchronizeHiveMetastore",
			"POST /public/api/projects/TEST/datasets/logs/actions/updateFromHive",
			"POST /public/api/projects/TEST/datasets/logs/actions/runChecks/?partitions=2026-01",
			"GET /public/api/data-quality/status",
			// DSS 15 throws on a parameterless call; NP is the non-partitioned partition.
			"POST /public/api/projects/TEST/datasets/logs/data-quality/get-partitions-status?partitions=NP",
			"POST /public/api/projects/TEST/datasets/logs/data-quality/get-partitions-status?partitions=a&partitions=b",
			"DELETE /public/api/projects/TEST/datasets/logs/data-quality/history/ALL",
		],);
		expect(seen[2]!.body,).toEqual({ checks: [{ type: "numericRange", },], },);
	});
});
