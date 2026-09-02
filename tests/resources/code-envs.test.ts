import { describe, expect, it, } from "bun:test";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { DataikuClient, } from "../../src/client.js";
import { ClientValidationError, } from "../../src/errors.js";
import { stableHash, } from "../../src/utils/stable-hash.js";

function createClient(url: string,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
		retryMaxAttempts: 1,
	},);
}

async function withTestServer(
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

async function readJsonBody(req: IncomingMessage,): Promise<unknown> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) {
		chunks.push(Buffer.isBuffer(chunk,) ? chunk : Buffer.from(chunk,),);
	}
	const body = Buffer.concat(chunks,).toString("utf8",);
	return body.length > 0 ? JSON.parse(body,) : undefined;
}

const DEFINITION = {
	envName: "my_env",
	envLang: "PYTHON",
	desc: { pythonInterpreter: "PYTHON311", installCorePackages: false, },
	permissions: { owner: "admin", },
	specPackageList: "polars",
};

describe("CodeEnvsResource log lifecycle", () => {
	it("listLogs GETs the logs listing endpoint and parses descriptors", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${decodeURIComponent(req.url ?? "",)}`,);
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/admin/code-envs/PYTHON/my_env/logs",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify([
				{ name: "install.log", size: 1204, },
				{ name: "update-3.log", },
			],),);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(client.codeEnvs.listLogs("PYTHON", "my_env",),).resolves.toEqual([
				{ name: "install.log", size: 1204, },
				{ name: "update-3.log", },
			],);
		},);

		expect(requests,).toEqual(["GET /public/api/admin/code-envs/PYTHON/my_env/logs",],);
	});

	it("getLog respects the byte cap and reports truncation", async () => {
		const fullLog = Array.from({ length: 40, }, (_, i,) => `line-${String(i,).padStart(3, "0",)}`,)
			.join("\n",);

		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/admin/code-envs/PYTHON/my_env/logs/install.log",);
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.codeEnvs.getLog("PYTHON", "my_env", "install.log", {
				maxBytes: 40,
			},);
			expect(result.truncated,).toBe(true,);
			expect(result.tailed,).toBe(false,);
			expect(result.bytes,).toBeLessThanOrEqual(40,);
			expect(result.log.length,).toBeLessThan(fullLog.length,);
		},);
	});

	it("getLog tails the requested number of lines by default-free opts", async () => {
		const fullLog = ["l1", "l2", "l3", "l4", "l5",].join("\n",);

		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			res.setHeader("Content-Type", "text/plain",);
			res.end(fullLog,);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.codeEnvs.getLog("PYTHON", "my_env", "install.log", {
				maxLines: 2,
				maxBytes: 0,
			},);
			expect(result.tailed,).toBe(true,);
			expect(result.truncated,).toBe(false,);
			expect(result.log,).toBe("l4\nl5",);
			expect(result.bytes,).toBe(Buffer.byteLength(fullLog,),);
		},);
	});

	it("getLog with maxBytes 0 returns the whole log untruncated", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			res.setHeader("Content-Type", "text/plain",);
			res.end("whole\nlog\n",);
		}, async (url,) => {
			const client = createClient(url,);
			const result = await client.codeEnvs.getLog("PYTHON", "my_env", "install.log", {
				maxBytes: 0,
			},);
			expect(result.log,).toBe("whole\nlog\n",);
			expect(result.truncated,).toBe(false,);
			expect(result.tailed,).toBe(false,);
		},);
	});

	it("encodes log names with special characters", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(decodeURIComponent(req.url ?? "",),).toBe(
				"/public/api/admin/code-envs/PYTHON/my_env/logs/my log 2.log",
			);
			res.setHeader("Content-Type", "text/plain",);
			res.end("ok",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.getLog("PYTHON", "my_env", "my log 2.log",),
			).resolves.toMatchObject({ log: "ok", truncated: false, tailed: false, },);
		},);
	});
});

describe("CodeEnvsResource image and version lifecycle", () => {
	it("updateImages posts to the images endpoint with wait and optional version", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			requests.push(`${req.method ?? "UNKNOWN"} ${req.url ?? ""}`,);
			expect(req.method,).toBe("POST",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify({ messages: { success: true, }, },),);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.updateImages("PYTHON", "my_env", { envVersion: "v2", wait: false, },),
			).resolves.toEqual({ messages: { success: true, }, },);
		},);

		expect(requests,).toEqual([
			"POST /public/api/admin/code-envs/PYTHON/my_env/images?envVersion=v2&wait=false",
		],);
	});

	it("updateImages omits envVersion by default and sends wait=true", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("POST",);
			expect(req.url,).toBe("/public/api/admin/code-envs/R/my_env/images?wait=true",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify({ messages: { success: true, }, },),);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.updateImages("R", "my_env",),
			).resolves.toEqual({ messages: { success: true, }, },);
		},);
	});

	it("getVersionForProject resolves the project-pinned version", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/admin/code-envs/PYTHON/my_env/MY_PROJ/version",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify({ version: "bundle-v7", bundleId: "b-123", },),);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.getVersionForProject("PYTHON", "my_env", "MY_PROJ",),
			).resolves.toEqual({ version: "bundle-v7", bundleId: "b-123", },);
		},);
	});
});

describe("CodeEnvsResource definition provenance", () => {
	it("get exposes deploymentMode and a stable canonical definitionHash", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toBe("/public/api/admin/code-envs/PYTHON/my_env/",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify(DEFINITION,),);
		}, async (url,) => {
			const client = createClient(url,);
			const details = await client.codeEnvs.get("PYTHON", "my_env",);
			expect(details.deploymentMode,).toBeUndefined();
			expect(details.definitionHash,).toBe(stableHash(DEFINITION,),);
		},);
	});

	it("setDefinition verifies the expect-hash guard before PUT", async () => {
		const requests: string[] = [];
		let putBody: Record<string, unknown> | undefined;

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "UNKNOWN"} ${url.pathname}`,);
			if (
				req.method === "GET"
				&& ["/public/api/admin/code-envs/PYTHON/my_env", "/public/api/admin/code-envs/PYTHON/my_env/",]
					.includes(url.pathname,)
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(DEFINITION,),);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/my_env") {
				const rawBody = await readJsonBody(req,);
				putBody = rawBody as Record<string, unknown>;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ updated: true, },),);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.setDefinition("PYTHON", "my_env", DEFINITION, {
					expectHash: stableHash(DEFINITION,),
				},),
			).resolves.toEqual({ updated: true, },);
			expect(putBody,).toEqual(DEFINITION,);
		},);

		expect(requests,).toEqual([
			"GET /public/api/admin/code-envs/PYTHON/my_env",
			"PUT /public/api/admin/code-envs/PYTHON/my_env",
		],);
	});

	it("setDefinition refuses the PUT when the hash is stale", async () => {
		const requests: string[] = [];

		await withTestServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "UNKNOWN"} ${url.pathname}`,);
			expect(req.method,).toBe("GET",);
			res.setHeader("Content-Type", "application/json",);
			res.end(JSON.stringify({ ...DEFINITION, specPackageList: "polars>=1.0", },),);
		}, async (url,) => {
			const client = createClient(url,);
			const error = await client.codeEnvs.setDefinition("PYTHON", "my_env", DEFINITION, {
				expectHash: stableHash(DEFINITION,),
			},).catch((caught: unknown,) => caught);
			expect(error,).toBeInstanceOf(ClientValidationError,);
			const validation = error as ClientValidationError;
			expect(validation.code,).toBe("validation_failed",);
			expect(validation.details,).toMatchObject({
				envLang: "PYTHON",
				envName: "my_env",
				expectedDefinitionHash: stableHash(DEFINITION,),
				currentDefinitionHash: stableHash({ ...DEFINITION, specPackageList: "polars>=1.0", },),
			},);
		},);

		expect(requests,).toEqual(["GET /public/api/admin/code-envs/PYTHON/my_env",],);
	});

	it("setPackages GET-merge-PUT preserves other fields and honors the guard", async () => {
		const requests: string[] = [];
		let putBody: Record<string, unknown> | undefined;

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			requests.push(`${req.method ?? "UNKNOWN"} ${url.pathname}`,);
			if (
				req.method === "GET"
				&& ["/public/api/admin/code-envs/PYTHON/my_env", "/public/api/admin/code-envs/PYTHON/my_env/",]
					.includes(url.pathname,)
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(DEFINITION,),);
				return;
			}
			if (req.method === "PUT" && url.pathname === "/public/api/admin/code-envs/PYTHON/my_env") {
				const rawBody = await readJsonBody(req,);
				putBody = rawBody as Record<string, unknown>;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ updated: true, },),);
				return;
			}
			res.statusCode = 404;
			res.end("unexpected",);
		}, async (url,) => {
			const client = createClient(url,);
			await expect(
				client.codeEnvs.setPackages("PYTHON", "my_env", ["polars==1.40.1", "tabulate",], {
					installCorePackages: true,
					expectHash: stableHash(DEFINITION,),
				},),
			).resolves.toEqual({ updated: true, },);
			expect(putBody,).toEqual({
				...DEFINITION,
				specPackageList: "polars==1.40.1\ntabulate",
				desc: { ...DEFINITION.desc, installCorePackages: true, },
			},);
		},);

		expect(requests,).toEqual([
			"GET /public/api/admin/code-envs/PYTHON/my_env",
			"PUT /public/api/admin/code-envs/PYTHON/my_env",
		],);
	});
});
