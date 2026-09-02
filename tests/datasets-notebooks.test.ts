import { describe, expect, it, } from "bun:test";

import * as fs from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { gunzipSync, } from "node:zlib";
import { DataikuError, } from "../src/errors.js";
import {
	DataikuClient,
	type JupyterNotebookContent,
	type SqlNotebookContent,
} from "../src/index.js";
import { buildDatasetCloneSettings, DatasetsResource, } from "../src/resources/datasets.js";
import { stableHash, } from "../src/utils/stable-hash.js";

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

async function readRequestBody(req: IncomingMessage,): Promise<string> {
	let body = "";
	for await (const chunk of req) {
		body += chunk.toString();
	}
	return body;
}

describe("DatasetsResource.preview", () => {
	it("returns structured preview rows", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			// One probe row beyond the requested cap is streamed to detect
			// truncation; a dataset with exactly maxRows rows still reports
			// truncated:false and the probe row is discarded.
			expect(req.url,).toContain(
				"/public/api/projects/TEST/datasets/sample/data/?format=tsv-excel-header&limit=3",
			);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcity\nAlice\tParis\nBob\tBerlin\n",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const preview = await client.datasets.preview("sample", { maxRows: 2, },);
			expect(preview,).toEqual({
				columns: [{ name: "name", }, { name: "city", },],
				rows: [["Alice", "Paris",], ["Bob", "Berlin",],],
				rowCount: 2,
				truncated: false,
				limit: 2,
			},);
		},);
	});

	it("reports truncated when the dataset holds more rows than requested", async () => {
		await withTestServer((req, res,) => {
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcity\nAlice\tParis\nBob\tBerlin\nCarole\tLyon\n",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const preview = await client.datasets.preview("sample", { maxRows: 2, },);
			expect(preview,).toEqual({
				columns: [{ name: "name", }, { name: "city", },],
				rows: [["Alice", "Paris",], ["Bob", "Berlin",],],
				rowCount: 2,
				truncated: true,
				limit: 2,
			},);
		},);
	});

	it("times out stalled preview bodies after headers", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toContain("/public/api/projects/TEST/datasets/sample/data/",);
			res.writeHead(200, { "Content-Type": "text/tab-separated-values; charset=utf-8", },);
			res.flushHeaders();
			setTimeout(() => res.end("",), 200,);
		}, async (url,) => {
			const client = new DataikuClient({
				url,
				apiKey: "test-key",
				projectKey: "TEST",
				requestTimeoutMs: 50,
			},);
			const error = await client.datasets.preview("sample",).catch((caught: unknown,) => caught);
			expect(error,).toBeInstanceOf(DataikuError,);
			expect((error as DataikuError).status,).toBe(0,);
			expect((error as Error).message,).toContain("timed out after 50ms",);
		},);
	});
});

describe("DatasetsResource.download", () => {
	it("writes an uncompressed CSV when the output path ends in .csv", async () => {
		const warnings: { method: string; errors: string[]; }[] = [];

		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toContain("/public/api/projects/TEST/datasets/sample/data/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcity\nAlice\tParis\n",);
		}, async (url,) => {
			const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-dataset-download-",),);
			const outputPath = path.join(tempDir, "sample.csv",);

			try {
				const client = new DataikuClient({
					url,
					apiKey: "test-key",
					projectKey: "TEST",
					onValidationWarning: (method, errors,) => {
						warnings.push({ method, errors, },);
					},
				},);

				const { path: writtenPath, } = await client.datasets.download("sample", {
					outputPath,
					validateColumns: [{ name: "name", }, { name: "city", },],
				},);
				const fileBuffer = fs.readFileSync(writtenPath,);

				expect(path.resolve(writtenPath,),).toBe(path.resolve(outputPath,),);
				expect(fileBuffer[0],).not.toBe(0x1f,);
				expect(fileBuffer[1],).not.toBe(0x8b,);
				expect(fileBuffer.toString("utf8",),).toBe("name,city\nAlice,Paris\n",);
				expect(warnings,).toEqual([],);
			} finally {
				fs.rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);
	});

	it("emits validation warnings when downloaded columns do not match the header row", async () => {
		const warnings: { method: string; errors: string[]; }[] = [];

		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toContain("/public/api/projects/TEST/datasets/sample/data/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcountry\nAlice\tFrance\n",);
		}, async (url,) => {
			const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-dataset-download-",),);
			const outputPath = path.join(tempDir, "sample.csv",);

			try {
				const client = new DataikuClient({
					url,
					apiKey: "test-key",
					projectKey: "TEST",
					onValidationWarning: (method, errors,) => {
						warnings.push({ method, errors, },);
					},
				},);

				const { path: writtenPath, } = await client.datasets.download("sample", {
					outputPath,
					validateColumns: [{ name: "name", }, { name: "city", },],
				},);

				expect(fs.readFileSync(writtenPath, "utf8",),).toBe("name,country\nAlice,France\n",);
			} finally {
				fs.rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);

		expect(warnings,).toEqual([
			{
				method: "datasets.download(sample)",
				errors: ['Missing expected column: "city"', 'Unexpected column in stream: "country"',],
			},
		],);
	});

	it("keeps the default .csv.gz naming and gzip compression for directory outputs", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toContain("/public/api/projects/TEST/datasets/sample%20dataset/data/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcity\nAlice\tParis\n",);
		}, async (url,) => {
			const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-dataset-download-",),);

			try {
				const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
				const { path: writtenPath, } = await client.datasets.download("sample dataset", {
					outputPath: tempDir,
				},);
				const fileBuffer = fs.readFileSync(writtenPath,);

				expect(writtenPath,).toBe(path.join(tempDir, "sample dataset.csv.gz",),);
				expect(fileBuffer[0],).toBe(0x1f,);
				expect(fileBuffer[1],).toBe(0x8b,);
				expect(gunzipSync(fileBuffer,).toString("utf8",),).toBe("name,city\nAlice,Paris\n",);
			} finally {
				fs.rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);
	});

	it("reports truncated=true and the written row count when data exceeds the limit", async () => {
		await withTestServer((_req, res,) => {
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\nA\nB\nC\nD\nE\n",);
		}, async (url,) => {
			const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-dataset-download-",),);
			try {
				const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
				const result = await client.datasets.download("sample", {
					outputPath: path.join(tempDir, "out.csv",),
					limit: 2,
				},);
				expect(result.truncated,).toBe(true,);
				expect(result.rows,).toBe(2,);
				expect(result.limit,).toBe(2,);
				expect(fs.readFileSync(result.path, "utf-8",),).toBe("name\nA\nB\n",);
			} finally {
				fs.rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);
	});

	it("creates a missing output directory before writing the download", async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dataiku-dataset-download-",),);
		const outputPath = path.join(tempDir, "nested", "missing", "sample.csv",);
		let observedUrl = "";

		try {
			expect(fs.existsSync(path.dirname(outputPath,),),).toBe(false,);
			const resource = new DatasetsResource({
				resolveProjectKey: (projectKey?: string,) => projectKey ?? "TEST",
				stream: async (url: string,) => {
					observedUrl = url;
					return {
						body: new ReadableStream<Uint8Array>({
							start(controller,) {
								controller.enqueue(new TextEncoder().encode("name\tcity\nAlice\tParis\n",),);
								controller.close();
							},
						},),
					};
				},
			} as unknown as DataikuClient,);

			const result = await resource.download("sample", { outputPath, projectKey: "TEST", },);

			expect(observedUrl,).toBe(
				"/public/api/projects/TEST/datasets/sample/data/?format=tsv-excel-header&limit=100001",
			);
			expect(result,).toEqual({
				path: path.resolve(outputPath,),
				rows: 1,
				truncated: false,
				limit: 100_000,
			},);
			expect(fs.readFileSync(outputPath, "utf8",),).toBe("name,city\nAlice,Paris\n",);
		} finally {
			fs.rmSync(tempDir, { recursive: true, force: true, },);
		}
	});
});

describe("DatasetsResource.create", () => {
	it("uses a non-root project path for managed filesystem datasets", async () => {
		let createBody: Record<string, unknown> | undefined;

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				createBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ name: "output_ds", },),);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.datasets.create({
				datasetName: "output_ds",
				connection: "s3_conn",
			},);
		},);

		expect(createBody,).toMatchObject({
			projectKey: "TEST",
			name: "output_ds",
			type: "Filesystem",
			params: {
				connection: "s3_conn",
				path: "/dataiku/TEST/output_ds",
			},
			managed: true,
		},);
	});

	it("uses the DSS uploaded-files request shape", async () => {
		let createBody: Record<string, unknown> | undefined;

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				createBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ name: "uploaded_input", },),);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.datasets.create({
				datasetName: "uploaded_input",
				connection: "Default (in DSS data dir.)",
				dsType: "UploadedFiles",
			},);
		},);

		expect(createBody,).toEqual({
			projectKey: "TEST",
			name: "uploaded_input",
			type: "UploadedFiles",
			params: { uploadConnection: "Default (in DSS data dir.)", },
		},);
	});
	it("uses the server default when an uploaded-files connection is omitted", async () => {
		let createBody: Record<string, unknown> | undefined;

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				createBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ name: "uploaded_input", },),);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.datasets.create({
				datasetName: "uploaded_input",
				dsType: "UploadedFiles",
			},);
		},);

		expect(createBody,).toEqual({
			projectKey: "TEST",
			name: "uploaded_input",
			type: "UploadedFiles",
			params: {},
		},);
	});
	it("resolves managed storage when DSS has no default upload target", async () => {
		const createBodies: Record<string, unknown>[] = [];

		await withTestServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			if (req.method === "POST" && url.pathname === "/public/api/projects/TEST/datasets/") {
				createBodies.push(JSON.parse(await readRequestBody(req,),) as Record<string, unknown>,);
				res.setHeader("Content-Type", "application/json",);
				if (createBodies.length === 1) {
					res.statusCode = 500;
					res.end(JSON.stringify({
						message: "Cannot create dataset TEST.uploaded_input without a target connection",
					},),);
					return;
				}
				res.statusCode = 200;
				res.end(JSON.stringify({ name: "uploaded_input", },),);
				return;
			}

			if (req.method === "GET" && url.pathname === "/public/api/admin/connections/") {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({
					"archive": {
						allowWrite: true,
						allowManagedDatasets: false,
					},
					"dataiku-managed-storage": {
						allowWrite: true,
						allowManagedDatasets: true,
					},
				},),);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.datasets.create({
				datasetName: "uploaded_input",
				dsType: "UploadedFiles",
			},);
		},);

		expect(createBodies,).toEqual([
			{
				projectKey: "TEST",
				name: "uploaded_input",
				type: "UploadedFiles",
				params: {},
			},
			{
				projectKey: "TEST",
				name: "uploaded_input",
				type: "UploadedFiles",
				params: { uploadConnection: "dataiku-managed-storage", },
			},
		],);
	});
});

describe("buildDatasetCloneSettings", () => {
	it("omits server-managed params while preserving cloneable storage fields", () => {
		const settings = buildDatasetCloneSettings(
			{
				name: "source_ds",
				type: "Filesystem",
				projectKey: "TEST",
				managed: true,
				params: {
					connection: "filesystem",
					path: "/dataiku/TEST/source_ds",
					table: "source_table",
					schema: "analytics",
					catalog: "prod",
					folderSmartId: "folder-id",
					metastoreTableName: "source_ds",
					mode: "table",
					internalX: "server-managed",
				} as Record<string, unknown>,
				formatType: "csv",
			},
			"target_ds",
			"TEST",
			{
				path: "/dataiku/TEST/target_ds",
				metastoreTableName: "target_ds",
			},
		);

		expect(settings.params,).toEqual({
			connection: "filesystem",
			path: "/dataiku/TEST/target_ds",
			table: "source_table",
			schema: "analytics",
			catalog: "prod",
			folderSmartId: "folder-id",
			metastoreTableName: "target_ds",
			mode: "table",
		},);
	});

	it("preserves UploadedFiles upload connections while cloning dataset params", () => {
		const settings = buildDatasetCloneSettings(
			{
				name: "source_uploads",
				type: "UploadedFiles",
				projectKey: "TEST",
				managed: false,
				params: {
					uploadConnection: "uploads-connection",
					internalX: "server-managed",
				} as Record<string, unknown>,
			},
			"target_uploads",
			"TEST",
			{},
		);

		expect(settings.params,).toEqual({ uploadConnection: "uploads-connection", },);
	});
});

describe("NotebooksResource.create", () => {
	it("posts new Jupyter notebooks to the encoded notebook path", async () => {
		const requests: string[] = [];
		let observedBody: JupyterNotebookContent | undefined;
		const notebookName = "analysis notebook";
		const notebookPath = "/public/api/projects/TEST/jupyter-notebooks/"
			+ encodeURIComponent(notebookName,);
		const notebook: JupyterNotebookContent = {
			metadata: { kernelspec: { name: "python3", }, },
			nbformat: 4,
			nbformat_minor: 5,
			cells: [
				{
					cell_type: "code",
					source: ["print('created')\n",],
					metadata: {},
					outputs: [],
					execution_count: null,
				},
			],
		};

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			observedBody = JSON.parse(await readRequestBody(req,),) as JupyterNotebookContent;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(client.notebooks.createJupyter(notebookName, notebook,),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual([`POST ${notebookPath}`,],);
		expect(observedBody,).toEqual(notebook,);
	});

	it("posts new SQL notebooks to the project SQL notebook collection", async () => {
		const requests: string[] = [];
		let observedBody: Record<string, unknown> | undefined;
		const notebook: SqlNotebookContent = {
			connection: "postgres",
			cells: [{ id: "cell-1", type: "QUERY", code: "select 1", },],
		};

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			observedBody = JSON.parse(await readRequestBody(req,),) as Record<string, unknown>;
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(client.notebooks.createSql("sql/slash", notebook,),).resolves.toBeUndefined();
		},);

		expect(requests,).toEqual(["POST /public/api/projects/TEST/sql-notebooks/",],);
		expect(observedBody,).toEqual({
			...notebook,
			id: "sql/slash",
			projectKey: "TEST",
		},);
	});
});

describe("NotebooksResource.clearJupyterOutputs", () => {
	it("clears outputs with the official single DELETE of the outputs endpoint", async () => {
		const requests: string[] = [];
		const notebookName = "analysis notebook";
		const outputsPath = "/public/api/projects/TEST/jupyter-notebooks/"
			+ `${encodeURIComponent(notebookName,)}/outputs`;

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "DELETE" && req.url === outputsPath) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(client.notebooks.clearJupyterOutputs(notebookName,),).resolves.toBeUndefined();
		},);

		// Atomic: exactly one request, no GET/PUT round trip.
		expect(requests,).toEqual([`DELETE ${outputsPath}`,],);
	});

	it("rejects the legacy GET-strip-PUT path: no notebook read or write occurs", async () => {
		const notebookName = "analysis notebook";
		const notebookPath = "/public/api/projects/TEST/jupyter-notebooks/"
			+ encodeURIComponent(notebookName,);
		const outputsPath = `${notebookPath}/outputs`;
		const requests: string[] = [];

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "DELETE" && req.url === outputsPath) {
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 404;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.notebooks.clearJupyterOutputs(notebookName,);
		},);

		expect(requests.some((request,) => request.startsWith(`GET ${notebookPath}`,)),).toBe(
			false,
		);
		expect(requests.some((request,) => request.startsWith(`PUT ${notebookPath}`,)),).toBe(
			false,
		);
	});
});

describe("NotebooksResource.listJupyter active filter", () => {
	it("requests the official ?active=true filter when active is true", async () => {
		const requests: string[] = [];

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (
				req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/?active=true"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([{ name: "running", projectKey: "TEST", language: "python", },],),);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const list = await client.notebooks.listJupyter(undefined, { active: true, },);
			expect(list,).toEqual([{ name: "running", projectKey: "TEST", language: "python", },],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/jupyter-notebooks/?active=true",
		],);
	});

	it("passes ?active=false for the inactive filter and omits the query without a filter", async () => {
		const requests: string[] = [];

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (
				req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/?active=false"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([],),);
				return;
			}
			if (req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/") {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([{ name: "any", projectKey: "TEST", language: "python", },],),);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			expect(await client.notebooks.listJupyter(undefined, { active: false, },),).toEqual([],);
			expect(await client.notebooks.listJupyter(),).toEqual([
				{ name: "any", projectKey: "TEST", language: "python", },
			],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/jupyter-notebooks/?active=false",
			"GET /public/api/projects/TEST/jupyter-notebooks/",
		],);
	});
});

describe("NotebooksResource.unloadJupyterAll", () => {
	it("unloads every session of every active notebook through per-session DELETEs", async () => {
		const requests: string[] = [];

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (
				req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/?active=true"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([
					{ name: "busy", projectKey: "TEST", language: "python", },
					{ name: "idle", projectKey: "TEST", language: "python", },
				],),);
				return;
			}
			if (
				req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/busy/sessions"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([
					{ sessionId: "s1", notebookName: "busy", },
					{ sessionId: "s2", notebookName: "busy", },
				],),);
				return;
			}
			if (
				req.method === "GET" && req.url === "/public/api/projects/TEST/jupyter-notebooks/idle/sessions"
			) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify([],),);
				return;
			}
			if (
				req.method === "DELETE"
				&& req.url === "/public/api/projects/TEST/jupyter-notebooks/busy/sessions/s1"
			) {
				res.statusCode = 204;
				res.end();
				return;
			}
			if (
				req.method === "DELETE"
				&& req.url === "/public/api/projects/TEST/jupyter-notebooks/busy/sessions/s2"
			) {
				res.statusCode = 404;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify({ message: "Session s2 not found", },),);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			// A session that vanished between listing and unloading counts as
			// already unloaded instead of failing the sweep.
			expect(await client.notebooks.unloadJupyterAll(),).toEqual([
				{ name: "busy", unloadedSessionIds: ["s1",], },
				{ name: "idle", unloadedSessionIds: [], },
			],);
		},);

		expect(requests,).toEqual([
			"GET /public/api/projects/TEST/jupyter-notebooks/?active=true",
			"GET /public/api/projects/TEST/jupyter-notebooks/busy/sessions",
			"DELETE /public/api/projects/TEST/jupyter-notebooks/busy/sessions/s1",
			"DELETE /public/api/projects/TEST/jupyter-notebooks/busy/sessions/s2",
			"GET /public/api/projects/TEST/jupyter-notebooks/idle/sessions",
		],);
	});
});

describe("NotebooksResource.saveOrCreateJupyter", () => {
	const notebookName = "analysis notebook";
	const notebookPath = "/public/api/projects/TEST/jupyter-notebooks/"
		+ encodeURIComponent(notebookName,);
	const content: JupyterNotebookContent = {
		metadata: { kernelspec: { name: "python3", }, },
		nbformat: 4,
		nbformat_minor: 5,
		cells: [{
			cell_type: "code",
			source: "print('saved')",
			metadata: {},
			outputs: [],
			execution_count: null,
		},],
	};

	function notebookServer(
		requests: string[],
		state: { stored?: JupyterNotebookContent; },
	): (req: IncomingMessage, res: ServerResponse,) => Promise<void> {
		return async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "GET" && req.url === notebookPath) {
				if (state.stored === undefined) {
					res.statusCode = 404;
					res.setHeader("Content-Type", "application/json",);
					res.end(JSON.stringify({ message: "Jupyter notebook not found", },),);
					return;
				}
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(state.stored,),);
				return;
			}
			if (req.method === "POST" && req.url === notebookPath) {
				state.stored = JSON.parse(await readRequestBody(req,),) as JupyterNotebookContent;
				res.statusCode = 204;
				res.end();
				return;
			}
			if (req.method === "PUT" && req.url === notebookPath) {
				state.stored = JSON.parse(await readRequestBody(req,),) as JupyterNotebookContent;
				res.statusCode = 204;
				res.end();
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		};
	}

	it("creates a missing notebook, exposes created, and hashes the persisted content", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = {};

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const result = await client.notebooks.saveOrCreateJupyter(notebookName, content,);
			expect(result.created,).toBe(true,);
			expect(result.hash,).toMatch(/^[0-9a-f]{64}$/,);
			expect(result.hash,).toBe(stableHash(state.stored,),);
			// Fresh read decides create vs update; POST then a confirming read.
			expect(requests,).toEqual([
				`GET ${notebookPath}`,
				`POST ${notebookPath}`,
				`GET ${notebookPath}`,
			],);
			expect(state.stored,).toEqual(content,);
		},);
	});

	it("updates an existing notebook and reports created=false with a stable hash", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = { stored: content, };

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const first = await client.notebooks.saveOrCreateJupyter(notebookName, content,);
			const second = await client.notebooks.saveOrCreateJupyter(notebookName, content,);
			expect(first.created,).toBe(false,);
			expect(second.created,).toBe(false,);
			// Deterministic: identical persisted content yields the same hash.
			expect(first.hash,).toBe(second.hash,);
		},);
	});

	it("aborts with a stale-read error when --expect-hash does not match the stored content", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = { stored: content, };

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(
				client.notebooks.saveOrCreateJupyter(notebookName, content, undefined, {
					expectHash: "0".repeat(64,),
				},),
			).rejects.toThrow(/changed since it was read/,);
			// Only the fresh read happened: no PUT, no create.
			expect(requests,).toEqual([`GET ${notebookPath}`,],);
		},);
	});

	it("verifies the stored hash before saving when --expect-hash matches", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = { stored: content, };

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const result = await client.notebooks.saveOrCreateJupyter(notebookName, content, undefined, {
				expectHash: stableHash(state.stored,),
			},);
			expect(result.created,).toBe(false,);
			expect(requests,).toEqual([
				`GET ${notebookPath}`,
				`PUT ${notebookPath}`,
				`GET ${notebookPath}`,
			],);
		},);
	});

	it("rejects a malformed --expect-hash before any request", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = {};

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(
				client.notebooks.saveOrCreateJupyter(notebookName, content, undefined, {
					expectHash: "not-a-hash",
				},),
			).rejects.toThrow(/64-character SHA-256 hex digest/,);
			expect(requests,).toEqual([],);
		},);
	});

	it("treats an expect-hash on a missing notebook as a stale read and never creates", async () => {
		const requests: string[] = [];
		const state: { stored?: JupyterNotebookContent; } = {};

		await withTestServer(notebookServer(requests, state,), async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await expect(
				client.notebooks.saveOrCreateJupyter(notebookName, content, undefined, {
					expectHash: "a".repeat(64,),
				},),
			).rejects.toThrow(/changed since it was read/,);
			expect(requests,).toEqual([`GET ${notebookPath}`,],);
			expect(state.stored,).toBeUndefined();
		},);
	});
});

describe("NotebooksResource.getJupyter cell source forms", () => {
	it("accepts valid nbformat cells whose source is a single string or an array", async () => {
		const requests: string[] = [];
		const notebookPath = "/public/api/projects/TEST/jupyter-notebooks/nb";
		const notebook = {
			metadata: {},
			nbformat: 4,
			nbformat_minor: 5,
			cells: [
				{ cell_type: "code", source: "print('string source')", metadata: {}, },
				{ cell_type: "markdown", source: ["# Title\n",], metadata: {}, },
			],
		};

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);
			if (req.method === "GET" && req.url === notebookPath) {
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(notebook,),);
				return;
			}
			res.statusCode = 500;
			res.end(`unexpected ${req.method} ${req.url}`,);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const parsed = await client.notebooks.getJupyter("nb",);
			expect(parsed.cells[0]?.source,).toBe("print('string source')",);
			expect(parsed.cells[1]?.source,).toEqual(["# Title\n",],);
		},);

		expect(requests,).toEqual([`GET ${notebookPath}`,],);
	});
});
