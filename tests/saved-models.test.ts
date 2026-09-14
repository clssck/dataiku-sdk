import { describe, expect, it, } from "bun:test";
import { mkdtemp, readFile, rm, } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse, } from "node:http";
import { type AddressInfo, } from "node:net";
import { tmpdir, } from "node:os";
import { join, } from "node:path";
import { DataikuClient, } from "../src/client.js";
import { SavedModelsResource, } from "../src/resources/saved-models.js";
import { writeResponseToFile, } from "../src/utils/response-file.js";

async function readBody(req: IncomingMessage,): Promise<Buffer> {
	const chunks: Buffer[] = [];
	for await (const chunk of req) {
		chunks.push(chunk as Buffer,);
	}
	return Buffer.concat(chunks,);
}

function sendJson(res: ServerResponse, body: unknown, status = 200,): void {
	res.statusCode = status;
	res.setHeader("Content-Type", "application/json",);
	res.end(JSON.stringify(body,),);
}

function createClient(url: string, retries = 4,): DataikuClient {
	return new DataikuClient({
		url,
		apiKey: "test-key",
		projectKey: "TEST",
		retryMaxAttempts: retries,
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

async function withTempDir(run: (dir: string,) => Promise<void>,): Promise<void> {
	const dir = await mkdtemp(join(tmpdir(), "dss-savedmodel-test-",),);
	try {
		await run(dir,);
	} finally {
		await rm(dir, { recursive: true, force: true, },);
	}
}

const SM_PATH = "/public/api/projects/TEST/savedmodels/SM1";

/**
 * Focused regressions for genuinely uncertain edges only: the destructive
 * delete-versions transport (official-client POST, never retried), the
 * multipart shape of MLflow version import (archive bytes / zero-byte part +
 * folderRef/path query), and binary streaming exports written atomically.
 * Straightforward per-endpoint CRUD is covered by throwaway smoke, not here.
 */
describe("SavedModelsResource", () => {
	it("deletes versions via the official-client POST body exactly once despite client retries", async () => {
		const requests: string[] = [];
		let observedBody: unknown;
		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			observedBody = JSON.parse((await readBody(req,)).toString(),);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			// Client configured with 4 retries: deleteVersions must still send exactly one attempt.
			const resource = new SavedModelsResource(createClient(url, 4,),);
			await expect(resource.deleteVersions(["v1", "v2",], { removeIntermediate: false, }, "SM1",),)
				.resolves.toBeUndefined();
		},);
		expect(requests,).toEqual([`POST ${SM_PATH}/actions/delete-versions`,],);
		expect(observedBody,).toEqual({ versions: ["v1", "v2",], removeIntermediate: false, },);
	});

	it("defaults removeIntermediate to true and rejects an empty version list", async () => {
		let observedBody: unknown;
		await withServer(async (req, res,) => {
			observedBody = JSON.parse((await readBody(req,)).toString(),);
			res.statusCode = 204;
			res.end();
		}, async (url,) => {
			const resource = new SavedModelsResource(createClient(url,),);
			await expect(resource.deleteVersions(["v9",], undefined, "SM1",),).resolves.toBeUndefined();
			await expect(resource.deleteVersions([], undefined, "SM1",),).rejects.toThrow("versions",);
		},);
		expect(observedBody,).toEqual({ versions: ["v9",], removeIntermediate: true, },);
	});

	it("uploads a local MLflow archive as multipart with the documented query parameters", async () => {
		await withTempDir(async (dir,) => {
			const archive = join(dir, "model.zip",);
			const archiveBytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04, 1, 2, 3,],);
			await Bun.write(archive, archiveBytes,);
			const requests: string[] = [];
			let observedQuery = "";
			let observedContentType = "";
			let observedBody = Buffer.alloc(0,);

			await withServer(async (req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				observedContentType = req.headers["content-type"] ?? "";
				observedQuery = req.url ?? "";
				observedBody = await readBody(req,);
				sendJson(res, { imported: true, },);
			}, async (url,) => {
				const resource = new SavedModelsResource(createClient(url,),);
				await expect(resource.importMlflowVersion(archive, "v1", {
					codeEnvName: "py-env",
					containerExecConfigName: "NONE",
					setActive: false,
					binaryClassificationThreshold: 0.3,
				}, "SM1",),).resolves.toEqual({ imported: true, },);
			},);

			expect(requests,).toEqual([
				`POST ${SM_PATH}/versions/v1?codeEnvName=py-env&containerExecConfigName=NONE&setActive=false&binaryClassificationThreshold=0.3`,
			],);
			expect(observedContentType,).toContain("multipart/form-data",);
			const bodyText = observedBody.toString("latin1",);
			expect(bodyText,).toContain('filename="model.zip"',);
			// The archive bytes must survive the multipart framing intact.
			expect(observedBody.includes(Buffer.from(archiveBytes,),),).toBe(true,);
		},);
	});

	it("rejects a local archive path that does not exist without sending a request", async () => {
		const requests: string[] = [];
		await withServer((_req, res,) => {
			requests.push(`${_req.method ?? ""} ${_req.url ?? ""}`,);
			sendJson(res, {},);
		}, async (url,) => {
			const resource = new SavedModelsResource(createClient(url,),);
			await expect(
				resource.importMlflowVersion("/nonexistent/path.zip", "v1", undefined, "SM1",),
			).rejects.toThrow("not found",);
		},);
		expect(requests,).toEqual([],);
	});

	it("imports from a managed folder with a zero-byte file part and folderRef/path query", async () => {
		const requests: string[] = [];
		let observedQuery = "";
		let observedBody = Buffer.alloc(0,);
		let observedContentType = "";

		await withServer(async (req, res,) => {
			requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
			observedQuery = req.url ?? "";
			observedContentType = req.headers["content-type"] ?? "";
			observedBody = await readBody(req,);
			sendJson(res, { imported: true, },);
		}, async (url,) => {
			const resource = new SavedModelsResource(createClient(url,),);
			await expect(resource.importMlflowVersionFromFolder(
				"OTHER.FOLDERID",
				"/artifacts/model",
				"v2",
				{ codeEnvName: "INHERIT", },
				"SM1",
			),).resolves.toEqual({ imported: true, },);
		},);

		expect(requests,).toEqual([
			`POST ${SM_PATH}/versions/v2?codeEnvName=INHERIT&containerExecConfigName=INHERIT&setActive=true&binaryClassificationThreshold=0.5&folderRef=OTHER.FOLDERID&path=%2Fartifacts%2Fmodel`,
		],);
		expect(observedContentType,).toContain("multipart/form-data",);
		const bodyText = observedBody.toString("latin1",);
		// Backend-mandated multipart: one file part, zero bytes, matching the
		// official Python client's files={"file": (None, None)}.
		expect(bodyText,).toContain('name="file"',);
		const partMatch = bodyText.match(/name="file"[^\r\n]*(?:\r\n[^\r\n]+)*\r\n\r\n/,);
		expect(partMatch,).not.toBeNull();
		// The part body is empty: after the header terminator comes a CRLF then
		// the closing boundary — i.e. zero content bytes were sent.
		const afterPart = bodyText.slice(bodyText.indexOf(partMatch![0]!,) + partMatch![0]!.length,);
		expect(afterPart.startsWith("\r\n--",),).toBe(true,);
	});

	it("streams the scoring jar through multipart-safe binary framing to a file", async () => {
		// Bytes chosen to include a CRLF pair and boundary-ish content so a
		// corrupting re-framing would corrupt the output.
		const jarBytes = new Uint8Array([0x50, 0x4b, 0x01, 0x02, 0x0d, 0x0a, 0xff, 0x00, 0xab,],);
		const requests: string[] = [];
		await withTempDir(async (dir,) => {
			await withServer((req, res,) => {
				requests.push(`${req.method ?? ""} ${req.url ?? ""}`,);
				res.setHeader("Content-Type", "application/java-archive",);
				res.end(Buffer.from(jarBytes,),);
			}, async (url,) => {
				const resource = new SavedModelsResource(createClient(url,),);
				const res2 = await resource.downloadScoringJar(
					{ fullClassName: "model.Model", includeLibs: false, },
					"SM1",
					"v1",
				);
				expect(res2.status,).toBe(200,);
				const out = join(dir, "scoring.jar",);
				const bytes = await writeResponseToFile(out, res2,);
				expect(bytes,).toBe(jarBytes.byteLength,);
				// Byte-exact: binary export must not be re-encoded.
				const written = await readFile(out,);
				expect([...written,],).toEqual([...jarBytes,],);
			},);
		},);
		expect(requests,).toEqual([
			`GET ${SM_PATH}/versions/v1/scoring-jar?fullClassName=model.Model&includeLibs=false`,
		],);
	});

	it("streams the scoring PMML to a file", async () => {
		const pmml = Buffer.from('<?xml version="1.0"?><PMML/>', "utf-8",);
		await withTempDir(async (dir,) => {
			await withServer((req, res,) => {
				res.setHeader("Content-Type", "application/xml",);
				res.end(pmml,);
			}, async (url,) => {
				const resource = new SavedModelsResource(createClient(url,),);
				const res2 = await resource.downloadScoringPmml("SM1", "v1",);
				const out = join(dir, "model.pmml",);
				await writeResponseToFile(out, res2,);
				expect((await readFile(out,)).toString(),).toBe(pmml.toString(),);
			},);
		},);
	});
});
