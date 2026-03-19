import { mkdtempSync, readFileSync, rmSync, } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, resolve, } from "node:path";
import { gunzipSync, } from "node:zlib";
import { DataikuClient, type JupyterNotebookContent, } from "../src/index.js";

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
			const tempDir = mkdtempSync(join(tmpdir(), "dataiku-dataset-download-",),);
			const outputPath = join(tempDir, "sample.csv",);

			try {
				const client = new DataikuClient({
					url,
					apiKey: "test-key",
					projectKey: "TEST",
					onValidationWarning: (method, errors,) => {
						warnings.push({ method, errors, },);
					},
				},);

				const writtenPath = await client.datasets.download("sample", {
					outputPath,
					validateColumns: [{ name: "name", }, { name: "city", },],
				},);
				const fileBuffer = readFileSync(writtenPath,);

				expect(resolve(writtenPath,),).toBe(resolve(outputPath,),);
				expect(fileBuffer[0],).not.toBe(0x1f,);
				expect(fileBuffer[1],).not.toBe(0x8b,);
				expect(fileBuffer.toString("utf8",),).toBe("name,city\nAlice,Paris\n",);
				expect(warnings,).toEqual([],);
			} finally {
				rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);
	});

	it("keeps the default .csv.gz naming and gzip compression for directory outputs", async () => {
		await withTestServer((req, res,) => {
			expect(req.method,).toBe("GET",);
			expect(req.url,).toContain("/public/api/projects/TEST/datasets/sample%20dataset/data/",);
			res.statusCode = 200;
			res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
			res.end("name\tcity\nAlice\tParis\n",);
		}, async (url,) => {
			const tempDir = mkdtempSync(join(tmpdir(), "dataiku-dataset-download-",),);

			try {
				const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
				const writtenPath = await client.datasets.download("sample dataset", {
					outputPath: tempDir,
				},);
				const fileBuffer = readFileSync(writtenPath,);

				expect(writtenPath,).toBe(join(tempDir, "sample dataset.csv.gz",),);
				expect(fileBuffer[0],).toBe(0x1f,);
				expect(fileBuffer[1],).toBe(0x8b,);
				expect(gunzipSync(fileBuffer,).toString("utf8",),).toBe("name,city\nAlice,Paris\n",);
			} finally {
				rmSync(tempDir, { recursive: true, force: true, },);
			}
		},);
	});
});

describe("NotebooksResource.clearJupyterOutputs", () => {
	it("fetches the notebook, strips outputs, and saves the updated content", async () => {
		const requests: string[] = [];
		let savedNotebook: JupyterNotebookContent | undefined;
		const notebookName = "analysis notebook";
		const notebookPath = `/public/api/projects/TEST/jupyter-notebooks/${
			encodeURIComponent(notebookName,)
		}`;
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
					id: "code-1",
				},
				{
					cell_type: "markdown",
					source: ["# Title\n",],
					metadata: { tag: "intro", },
					id: "md-1",
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
