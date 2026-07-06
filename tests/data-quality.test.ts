import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { DataikuClient, } from "../src/client.js";
import { DataikuError, } from "../src/errors.js";
import { DataQualityResource, } from "../src/resources/data-quality.js";

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
	},);
}

async function withServer(
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void> | void,
	run: (url: string,) => Promise<void>,
): Promise<void> {
	const server = createServer((req, res,) => {
		void Promise.resolve(handler(req, res,),).catch((error: unknown,) => {
			res.statusCode = 500;
			res.end(error instanceof Error ? error.message : String(error,),);
		},);
	},);

	await new Promise<void>((resolvePromise, rejectPromise,) => {
		server.listen(0, "127.0.0.1", (error?: Error,) => {
			if (error) {
				rejectPromise(error,);
				return;
			}
			resolvePromise();
		},);
	},);

	const address = server.address();
	if (!address || typeof address === "string") {
		throw new Error("Test server did not bind to a TCP address.",);
	}
	const url = `http://127.0.0.1:${String(address.port,)}`;
	try {
		await run(url,);
	} finally {
		await new Promise<void>((resolvePromise, rejectPromise,) => {
			server.close((error,) => {
				if (error) {
					rejectPromise(error,);
					return;
				}
				resolvePromise();
			},);
		},);
	}
}

describe("DataQualityResource", () => {
	it("gets dataset status from the status-by-partition endpoint", async () => {
		const status = { outcome: "SUCCESS", partition: "NP", };
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, status,);
		}, async (url,) => {
			const resource = new DataQualityResource(createClient(url,),);
			await expect(resource.status("orders/table", "ALT/PROJECT",),).resolves.toEqual(status,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/ALT%2FPROJECT/datasets/orders%2Ftable/data-quality/status-by-partition",
		],);
	});

	it("returns the matching data quality rule from the rules collection", async () => {
		const rule = {
			id: "rule-1",
			type: "RecordCountInRangeRule",
			displayName: "Has rows",
			softMinimum: 1,
			softMinimumEnabled: true,
		};
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, {
				monitor: { enabled: true, },
				checks: [rule,],
				displayedState: { status: "OK", },
			},);
		}, async (url,) => {
			const resource = new DataQualityResource(createClient(url,),);
			await expect(resource.getRule("orders", "rule-1",),).resolves.toEqual(rule,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders/data-quality/rules",
		],);
	});

	it("classifies absent data quality rules as not_found", async () => {
		const requests: string[] = [];

		await withServer((req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			sendJson(res, {
				monitor: { enabled: true, },
				checks: [{ id: "other-rule", type: "RecordCountInRangeRule", displayName: "Other", },],
				displayedState: { status: "OK", },
			},);
		}, async (url,) => {
			const resource = new DataQualityResource(createClient(url,),);
			const error = await resource.getRule("orders", "missing-rule",).catch((caught: unknown,) =>
				caught
			);
			expect(error,).toBeInstanceOf(DataikuError,);
			if (!(error instanceof DataikuError)) throw error;
			expect(error.category,).toBe("not_found",);
			expect(error.status,).toBe(404,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/datasets/orders/data-quality/rules",
		],);
	});
});
