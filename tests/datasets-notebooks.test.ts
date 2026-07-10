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
			expect(req.url,).toContain(
				"/public/api/projects/TEST/datasets/sample/data/?format=tsv-excel-header&limit=2",
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
	it("fetches the notebook, strips outputs, and saves the updated content", async () => {
		const requests: string[] = [];
		let savedNotebook: JupyterNotebookContent | undefined;
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
					source: ["print('hello')\n",],
					metadata: { collapsed: false, },
					outputs: [{ output_type: "stream", text: ["hello\n",], },],
					execution_count: 7,
				},
				{
					cell_type: "markdown",
					source: ["# Title\n",],
					metadata: { tag: "intro", },
				},
			],
		};

		await withTestServer(async (req, res,) => {
			requests.push(`${req.method} ${req.url}`,);

			if (req.method === "GET" && req.url === notebookPath) {
				res.statusCode = 200;
				res.setHeader("Content-Type", "application/json",);
				res.end(JSON.stringify(notebook,),);
				return;
			}

			if (req.method === "PUT" && req.url === notebookPath) {
				savedNotebook = JSON.parse(await readRequestBody(req,),) as JupyterNotebookContent;
				res.statusCode = 204;
				res.end();
				return;
			}

			if (req.url?.endsWith("/outputs",)) {
				res.statusCode = 404;
				res.end("unexpected outputs endpoint",);
				return;
			}

			res.statusCode = 404;
			res.end("unexpected request",);
		}, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			await client.notebooks.clearJupyterOutputs(notebookName,);
		},);

		expect(requests,).toEqual([
			`GET ${notebookPath}`,
			`PUT ${notebookPath}`,
		],);
		expect(savedNotebook,).toEqual({
			...notebook,
			cells: [
				{
					...notebook.cells[0],
					outputs: [],
					execution_count: null,
				},
				{
					...notebook.cells[1],
					outputs: [],
					execution_count: null,
				},
			],
		},);
	});
});
