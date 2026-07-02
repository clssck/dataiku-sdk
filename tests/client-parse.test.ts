import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, type DataikuClientConfig, } from "../src/client.js";
import { ProjectSummaryArraySchema, ProjectSummarySchema, } from "../src/schemas.js";

const client = new DataikuClient({ url: "http://localhost:0", apiKey: "test", },);

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
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

	const { port, } = server.address() as AddressInfo;
	const url = `http://127.0.0.1:${String(port,)}`;
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

async function withEnv(
	vars: Record<string, string | undefined>,
	run: () => Promise<void> | void,
): Promise<void> {
	const previous: Record<string, string | undefined> = {};
	for (const key of Object.keys(vars,)) {
		previous[key] = process.env[key];
		const value = vars[key];
		if (value === undefined) delete process.env[key];
		else process.env[key] = value;
	}
	try {
		await run();
	} finally {
		for (const [key, value,] of Object.entries(previous,)) {
			if (value === undefined) delete process.env[key];
			else process.env[key] = value;
		}
	}
}

describe("DataikuClient.parse()", () => {
	it("returns valid object unchanged", () => {
		const input = { projectKey: "X", name: "Y", };
		const result = client.parse(ProjectSummarySchema, input,);
		expect(result,).toEqual(input,);
	});

	it("preserves extra fields not in the schema", () => {
		const input = { projectKey: "X", name: "Y", extra: 1, };
		const result = client.parse(ProjectSummarySchema, input,);
		expect((result as Record<string, unknown>).extra,).toBe(1,);
	});

	it("throws on invalid data (wrong type for field)", () => {
		expect(
			() => client.parse(ProjectSummarySchema, { projectKey: 123, },),
		).toThrow();
	});

	it("returns valid array", () => {
		const input = [{ projectKey: "X", name: "Y", },];
		const result = client.parse(ProjectSummaryArraySchema, input,);
		expect(result,).toEqual(input,);
	});

	it("accepts empty array as valid", () => {
		const result = client.parse(ProjectSummaryArraySchema, [],);
		expect(result,).toEqual([],);
	});

	it("throws when data is a string instead of object", () => {
		expect(
			() => client.parse(ProjectSummarySchema, "string",),
		).toThrow();
	});

	it("throws when data is null", () => {
		expect(
			() => client.parse(ProjectSummarySchema, null,),
		).toThrow();
	});
});

describe("DataikuClient constructor configuration", () => {
	it("uses trimmed DATAIKU_URL and DATAIKU_API_KEY environment credentials", async () => {
		let observedPath = "";
		let observedAuthorization = "";

		await withServer((req, res,) => {
			observedPath = req.url ?? "";
			observedAuthorization = req.headers.authorization ?? "";
			sendJson(res, [],);
		}, async (url,) => {
			await withEnv({
				DATAIKU_URL: ` ${url}/// `,
				DATAIKU_API_KEY: " env-key ",
			}, async () => {
				const envClient = new DataikuClient();
				await expect(envClient.projects.list(),).resolves.toEqual([],);
			},);
		},);

		expect(observedPath,).toBe("/public/api/projects/",);
		expect(observedAuthorization,).toBe("Bearer env-key",);
	});

	it("throws a clear configuration error when URL and API key are unavailable", async () => {
		await withEnv({
			DATAIKU_URL: undefined,
			DATAIKU_API_KEY: undefined,
		}, () => {
			expect(() => new DataikuClient()).toThrow(
				"Dataiku URL and API key are required: pass {url, apiKey} or set DATAIKU_URL/DATAIKU_API_KEY",
			);
		},);
	});

	it("prefers explicit credentials while allowing one missing field to come from env", async () => {
		const authorizations: string[] = [];
		const paths: string[] = [];

		await withServer((req, res,) => {
			authorizations.push(req.headers.authorization ?? "",);
			paths.push(req.url ?? "",);
			sendJson(res, [],);
		}, async (url,) => {
			await withEnv({
				DATAIKU_URL: `${url}/env-should-not-be-used`,
				DATAIKU_API_KEY: "env-key",
			}, async () => {
				const explicitClient = new DataikuClient({
					url,
					apiKey: "explicit-key",
				},);
				await expect(explicitClient.projects.list(),).resolves.toEqual([],);

				const urlFallbackClient = new DataikuClient({
					apiKey: "url-fallback-key",
				} as DataikuClientConfig,);
				await expect(urlFallbackClient.projects.list(),).resolves.toEqual([],);

				const apiKeyFallbackClient = new DataikuClient({
					url,
				} as DataikuClientConfig,);
				await expect(apiKeyFallbackClient.projects.list(),).resolves.toEqual([],);
			},);
		},);

		expect(authorizations,).toEqual([
			"Bearer explicit-key",
			"Bearer url-fallback-key",
			"Bearer env-key",
		],);
		expect(paths,).toEqual([
			"/public/api/projects/",
			"/env-should-not-be-used/public/api/projects/",
			"/public/api/projects/",
		],);
	});
});
