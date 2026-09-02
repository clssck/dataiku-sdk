import { describe, expect, it, } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { cliEnv, dss, dssFailure, readBody, sendJson, withCliServer, } from "./_harness.js";

describe("CLI dataset validation", () => {
	it("reports file-backed dataset build blockers", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/broken_output",);
			sendJson(res, {
				name: "broken_output",
				type: "Filesystem",
				params: { connection: "filesystem_managed", },
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["dataset", "validate-build", "broken_output",], {
				env: cliEnv(url,),
			},);
			const result = JSON.parse(stdout,) as { valid: boolean; warnings: string[]; };
			expect(result.valid,).toBe(false,);
			expect(result.warnings,).toContain(
				"File-backed dataset has no writable storage path configured.",
			);
			expect(result.warnings,).toContain("File-backed dataset has no formatType configured.",);
		},);
	});

	it("refreshes dataset schema through schema endpoint", async () => {
		let requestBody: unknown;
		await withCliServer(async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("PUT",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/orders/schema",);
			requestBody = JSON.parse(await readBody(req,),) as Record<string, unknown>;
			sendJson(res, {},);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"refresh-schema",
				"orders",
				"--data",
				JSON.stringify({ columns: [{ name: "id", type: "bigint", },], },),
			], { env: cliEnv(url,), },);
			expect(JSON.parse(stdout,),).toMatchObject({
				updated: "orders",
				resource: "dataset",
				schema: { columns: [{ name: "id", type: "bigint", },], },
			},);
		},);
		expect(requestBody,).toEqual({ columns: [{ name: "id", type: "bigint", },], },);
	});

	it("does not require a storage path for UploadedFiles datasets", async () => {
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe("/public/api/projects/TEST/datasets/uploaded_input",);
			sendJson(res, {
				name: "uploaded_input",
				type: "UploadedFiles",
				params: { uploadConnection: "Default (in DSS data dir.)", },
				formatType: "csv",
			},);
		}, async (url,) => {
			const { stdout, } = await dss(["dataset", "validate-build", "uploaded_input",], {
				env: cliEnv(url,),
			},);
			const result = JSON.parse(stdout,) as { valid: boolean; warnings: string[]; };
			expect(result.valid,).toBe(true,);
			expect(result.warnings,).toEqual([],);
		},);
	});

	it("lists UploadedFiles dataset files", async () => {
		const files = [{
			filename: "input.csv",
			path: "/input.csv",
			length: 3,
			mime: "text/csv",
			weird: false,
		},];
		await withCliServer((req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			expect(req.method,).toBe("GET",);
			expect(url.pathname,).toBe(
				"/public/api/projects/TEST/datasets/uploaded_input/uploaded/files",
			);
			sendJson(res, files,);
		}, async (url,) => {
			const { stdout, } = await dss(["dataset", "files", "uploaded_input",], {
				env: cliEnv(url,),
			},);
			expect(JSON.parse(stdout,),).toEqual(files,);
		},);
	});

	it("plans UploadedFiles upload as a mutation without contacting DSS", async () => {
		let requestCount = 0;
		await withCliServer((_req, res,) => {
			requestCount++;
			res.statusCode = 500;
			res.end("plan must not contact DSS",);
		}, async (url,) => {
			const { stdout, } = await dss([
				"dataset",
				"upload-file",
				"uploaded_input",
				"./new-input.csv",
				"--file-name",
				"input.csv",
				"--plan",
			], { env: cliEnv(url,), },);
			const plan = JSON.parse(stdout,) as Record<string, unknown>;
			expect(plan,).toMatchObject({
				plan: true,
				resource: "dataset",
				action: "upload-file",
				method: "POST",
				endpoint: "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files",
				payload: {
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: "./new-input.csv",
					fileName: "input.csv",
				},
			},);
		},);
		expect(requestCount,).toBe(0,);
	});

	it("uploads one new file and verifies the stored byte length", async () => {
		const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dss-upload-file-",),);
		const localPath = path.join(tempDir, "replacement.csv",);
		await fs.promises.writeFile(localPath, "new-data",);
		let fileListCount = 0;
		let uploadBody = "";

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (
					req.method === "GET"
					&& url.pathname === "/public/api/projects/TEST/datasets/uploaded_input"
				) {
					sendJson(res, {
						name: "uploaded_input",
						type: "UploadedFiles",
						params: { uploadConnection: "Default (in DSS data dir.)", },
					},);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname
						=== "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files"
				) {
					fileListCount++;
					sendJson(
						res,
						fileListCount === 1
							? [{
								filename: "existing.csv",
								path: "/existing.csv",
								length: 3,
								mime: "text/csv",
								weird: false,
							},]
							: [{
								filename: "input.csv",
								path: "/input.csv",
								length: 8,
								mime: "text/csv",
								weird: false,
							},],
					);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname
						=== "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files"
				) {
					uploadBody = await readBody(req,);
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const { stdout, } = await dss([
					"dataset",
					"upload-file",
					"uploaded_input",
					localPath,
					"--file-name",
					"input.csv",
				], { env: cliEnv(url,), },);
				expect(JSON.parse(stdout,),).toMatchObject({
					datasetName: "uploaded_input",
					projectKey: "TEST",
					fileName: "input.csv",
					bytes: 8,
					after: { length: 8, },
				},);
			},);
		} finally {
			await fs.promises.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(fileListCount,).toBe(2,);
		expect(uploadBody,).toContain('name="file"',);
		expect(uploadBody,).toContain('filename="input.csv"',);
		expect(uploadBody,).toContain("new-data",);
	});
	it("reports post-upload verification ambiguity as a DSS failure", async () => {
		const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dss-upload-ambiguous-",),);
		const localPath = path.join(tempDir, "input.csv",);
		await fs.promises.writeFile(localPath, "new-data",);
		let fileListCount = 0;

		try {
			await withCliServer(async (req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/uploaded_input"
				) {
					sendJson(res, { name: "uploaded_input", type: "UploadedFiles", params: {}, },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname
						=== "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files"
				) {
					fileListCount++;
					sendJson(
						res,
						fileListCount === 1
							? []
							: [{ filename: "input.csv", path: "/input.csv", length: 7, },],
					);
					return;
				}
				if (
					req.method === "POST"
					&& url.pathname
						=== "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files"
				) {
					await readBody(req,);
					res.statusCode = 204;
					res.end();
					return;
				}
				res.statusCode = 404;
				res.end("unexpected request",);
			}, async (url,) => {
				const failure = await dssFailure([
					"dataset",
					"upload-file",
					"uploaded_input",
					localPath,
					"--file-name",
					"input.csv",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(2,);
				expect(JSON.parse(failure.stdout,),).toMatchObject({
					code: "ambiguous_outcome",
					category: "dss",
					exitCode: 2,
					details: {
						expectedBytes: 8,
						matchingFiles: [{ filename: "input.csv", length: 7, },],
					},
				},);
			},);
		} finally {
			await fs.promises.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(fileListCount,).toBe(2,);
	});

	it("refuses an existing filename before issuing an upload", async () => {
		const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dss-upload-conflict-",),);
		const localPath = path.join(tempDir, "input.csv",);
		await fs.promises.writeFile(localPath, "new-data",);
		let uploadCount = 0;

		try {
			await withCliServer((req, res,) => {
				const url = new URL(req.url ?? "/", "http://localhost",);
				if (
					req.method === "GET" && url.pathname === "/public/api/projects/TEST/datasets/uploaded_input"
				) {
					sendJson(res, { name: "uploaded_input", type: "UploadedFiles", params: {}, },);
					return;
				}
				if (
					req.method === "GET"
					&& url.pathname
						=== "/public/api/projects/TEST/datasets/uploaded_input/uploaded/files"
				) {
					sendJson(res, [{ filename: "input.csv", path: "/input.csv", length: 3, },],);
					return;
				}
				if (req.method === "POST") uploadCount++;
				res.statusCode = 500;
				res.end("unexpected request",);
			}, async (url,) => {
				const failure = await dssFailure([
					"dataset",
					"upload-file",
					"uploaded_input",
					localPath,
					"--file-name",
					"input.csv",
				], { env: cliEnv(url,), },);
				expect(failure.code,).toBe(1,);
				expect(failure.stdout,).toContain(
					"DSS 14.7's public API can add uploaded files but cannot replace or delete them.",
				);
			},);
		} finally {
			await fs.promises.rm(tempDir, { recursive: true, force: true, },);
		}

		expect(uploadCount,).toBe(0,);
	});
});

describe("CLI dataset download export sanitation", () => {
	const tsv = "=name\tcity\n=1+2\tParis\n";

	it("writes spreadsheet-safe exports by default and exact bytes with --raw-data", async () => {
		const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dss-dataset-download-sanitize-",),);
		const safePath = path.join(tempDir, "safe.csv",);
		const rawPath = path.join(tempDir, "raw.csv",);
		try {
			await withCliServer((_req, res,) => {
				res.statusCode = 200;
				res.setHeader("Content-Type", "text/tab-separated-values; charset=utf-8",);
				res.end(tsv,);
			}, async (url,) => {
				const { stdout: safeOut, } = await dss([
					"dataset",
					"download",
					"orders",
					"--output",
					safePath,
				], { env: cliEnv(url,), },);
				expect(JSON.parse(safeOut,),).toMatchObject({ path: safePath, rows: 1, },);
				expect(fs.readFileSync(safePath, "utf8",),).toBe("'=name,city\n'=1+2,Paris\n",);

				const { stdout: rawOut, } = await dss([
					"dataset",
					"download",
					"orders",
					"--output",
					rawPath,
					"--raw-data",
				], { env: cliEnv(url,), },);
				expect(JSON.parse(rawOut,),).toMatchObject({ path: rawPath, rows: 1, },);
				expect(fs.readFileSync(rawPath, "utf8",),).toBe("=name,city\n=1+2,Paris\n",);
			},);
		} finally {
			await fs.promises.rm(tempDir, { recursive: true, force: true, },);
		}
	});
});
