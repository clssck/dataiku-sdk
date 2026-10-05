import { describe, expect, it, } from "bun:test";
import { mkdtempSync, } from "node:fs";
import type { IncomingMessage, ServerResponse, } from "node:http";
import { DataikuClient, } from "../../src/client.js";
import { ClientValidationError, } from "../../src/errors.js";
import {
	cliEnv,
	dss,
	dssFailure,
	join,
	readBody,
	rmSync,
	sendJson,
	tmpdir,
	withCliServer,
	writeFileSync,
} from "./_harness.js";

const ARTICLE_PATH = "/public/api/projects/TEST/wiki/article-1";
const UPLOAD_PATH = `${ARTICLE_PATH}/upload`;

interface Attachment {
	attachmentType: string;
	smartId: string;
	details: Record<string, unknown>;
}

interface ArticleBody {
	article: Record<string, unknown> & { attachments: Attachment[]; };
	payload: string;
}

interface AttachOutput {
	attached: string;
	articleId: string;
	fileName: string;
	bytes: number;
	attachment: Attachment;
	attachments: Attachment[];
}

interface DetachOutput {
	articleId: string;
	detached: Attachment;
	attachments: Attachment[];
}

interface DryRunOutput {
	dryRun: boolean;
	action: string;
	file?: unknown;
	current: unknown;
}

interface PlanOutput {
	plan: boolean;
	method: string;
	endpoint: string;
	payload?: unknown;
}

interface SkippedOutput {
	skipped: string;
	reason: string;
	article: string;
}

interface ErrorOutput {
	code: string;
	error: string;
	details?: unknown;
}

interface MockWiki {
	requests: string[];
	multipart: string[];
	putBodies: ArticleBody[];
	attachments: Attachment[];
	handler: (req: IncomingMessage, res: ServerResponse,) => Promise<void>;
}

/** Parse CLI JSON stdout into the named output shape the test asserts on. */
function parse<T,>(stdout: string,): T {
	const value: T = JSON.parse(stdout,);
	return value;
}

function ids(attachments: Attachment[],): string[] {
	return attachments.map((entry,) => entry.smartId);
}

function fileAttachment(smartId: string, name: string, size: number,): Attachment {
	return {
		attachmentType: "FILE",
		smartId,
		details: { objectDisplayName: name, size, mimeType: "image/png", },
	};
}

function articleBody(attachments: Attachment[],): ArticleBody {
	return {
		article: {
			id: "article-1",
			name: "Article 1",
			projectKey: "TEST",
			attachments,
			versionTag: { versionNumber: 3, },
		},
		payload: "# body",
	};
}

/**
 * Stateful wiki article: upload registers a FILE attachment named after the
 * multipart filename (unless `dropUploads`), PUT stores the posted attachments
 * (unless `ignorePut`).
 */
function mockWiki(
	initial: Attachment[],
	opts: { dropUploads?: boolean; ignorePut?: boolean; } = {},
): MockWiki {
	const state: MockWiki = {
		requests: [],
		multipart: [],
		putBodies: [],
		attachments: [...initial,],
		handler: async (req, res,) => {
			const url = new URL(req.url ?? "/", "http://localhost",);
			state.requests.push(`${req.method} ${url.pathname}`,);
			if (req.method === "GET" && url.pathname === ARTICLE_PATH) {
				sendJson(res, articleBody(state.attachments,),);
				return;
			}
			if (req.method === "POST" && url.pathname === UPLOAD_PATH) {
				const body = await readBody(req,);
				state.multipart.push(`${req.headers["content-type"] ?? ""}\n${body}`,);
				const filename = /filename="([^"]*)"/u.exec(body,)?.[1] ?? "";
				if (opts.dropUploads !== true) {
					state.attachments.push(fileAttachment(`new-${state.attachments.length}`, filename, 4,),);
				}
				sendJson(res, articleBody(state.attachments,).article,);
				return;
			}
			if (req.method === "PUT" && url.pathname === ARTICLE_PATH) {
				const body = parse<ArticleBody>(await readBody(req,),);
				state.putBodies.push(body,);
				if (opts.ignorePut !== true) state.attachments = body.article.attachments;
				sendJson(res, articleBody(state.attachments,),);
				return;
			}
			sendJson(res, { message: "Not found", }, 404,);
		},
	};
	return state;
}

function withTempFile(name: string, content: string,): { path: string; cleanup: () => void; } {
	const dir = mkdtempSync(join(tmpdir(), "dss-wiki-attach-",),);
	const path = join(dir, name,);
	writeFileSync(path, content,);
	return { path, cleanup: () => rmSync(dir, { recursive: true, force: true, },), };
}

describe("wiki attach", () => {
	it("uploads the file as multipart and returns the new attachment verified by re-reading the article", async () => {
		const file = withTempFile("diagram.png", "PNG!",);
		const wiki = mockWiki([fileAttachment("old-1", "old.png", 9,),],);
		try {
			await withCliServer(wiki.handler, async (url,) => {
				const { stdout, } = await dss([
					"wiki",
					"attach",
					"article-1",
					file.path,
					"--project-key",
					"TEST",
				], {
					env: cliEnv(url,),
				},);
				const result = parse<AttachOutput>(stdout,);
				expect(result.attached,).toBe("new-1",);
				expect(result.articleId,).toBe("article-1",);
				expect(result.fileName,).toBe("diagram.png",);
				expect(result.bytes,).toBe(4,);
				expect(result.attachment.smartId,).toBe("new-1",);
				expect(result.attachment.details["objectDisplayName"],).toBe("diagram.png",);
				expect(ids(result.attachments,),).toEqual(["old-1", "new-1",],);
			},);
		} finally {
			file.cleanup();
		}
		expect(wiki.requests,).toEqual([
			`GET ${ARTICLE_PATH}`,
			`POST ${UPLOAD_PATH}`,
			`GET ${ARTICLE_PATH}`,
		],);
		expect(wiki.multipart,).toHaveLength(1,);
		const upload = wiki.multipart[0] ?? "";
		expect(upload.startsWith("multipart/form-data; boundary=",),).toBe(true,);
		expect(upload,).toContain('name="file"',);
		expect(upload,).toContain('filename="diagram.png"',);
		expect(upload,).toContain("PNG!",);
	});

	it("fails before any DSS request when the local file is missing or not a regular file", async () => {
		const wiki = mockWiki([],);
		await withCliServer(wiki.handler, async (url,) => {
			const missing = join(tmpdir(), `dss-wiki-attach-missing-${Date.now()}.png`,);
			for (const extra of [[], ["--dry-run",],]) {
				const failure = await dssFailure(
					["wiki", "attach", "article-1", missing, "--project-key", "TEST", ...extra,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				const report = parse<ErrorOutput>(failure.stdout,);
				expect(report.code,).toBe("not_found",);
				expect(report.error,).toContain(missing,);
			}
			const dir = mkdtempSync(join(tmpdir(), "dss-wiki-attach-dir-",),);
			try {
				const failure = await dssFailure(
					["wiki", "attach", "article-1", dir, "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				);
				expect(parse<ErrorOutput>(failure.stdout,).code,).toBe("validation_failed",);
			} finally {
				rmSync(dir, { recursive: true, force: true, },);
			}
		},);
		expect(wiki.requests,).toEqual([],);
	});

	it("--dry-run reads the article but never uploads; --plan makes no request at all", async () => {
		const file = withTempFile("note.txt", "hello",);
		const wiki = mockWiki([fileAttachment("old-1", "old.png", 9,),],);
		try {
			await withCliServer(wiki.handler, async (url,) => {
				const dry = parse<DryRunOutput>(
					(await dss(
						["wiki", "attach", "article-1", file.path, "--dry-run", "--project-key", "TEST",],
						{ env: cliEnv(url,), },
					)).stdout,
				);
				expect(dry.dryRun,).toBe(true,);
				expect(dry.action,).toBe("attach",);
				expect(dry.file,).toEqual({ path: file.path, fileName: "note.txt", bytes: 5, },);
				expect(dry.current,).toEqual([fileAttachment("old-1", "old.png", 9,),],);
				expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`,],);

				const plan = parse<PlanOutput>(
					(await dss(
						["wiki", "attach", "article-1", file.path, "--plan", "--project-key", "TEST",],
						{ env: cliEnv(url,), },
					)).stdout,
				);
				expect(plan.plan,).toBe(true,);
				expect(plan.method,).toBe("POST",);
				expect(plan.endpoint,).toBe(UPLOAD_PATH,);
				expect(plan.payload,).toEqual({
					contentType: "multipart/form-data",
					fileField: "file",
					filePath: file.path,
					fileName: "note.txt",
				},);
				expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`,],);
			},);
		} finally {
			file.cleanup();
		}
	});

	it("reports an ambiguous outcome when the article shows no new attachment after the upload", async () => {
		const file = withTempFile("lost.png", "PNG!",);
		const wiki = mockWiki([], { dropUploads: true, },);
		try {
			await withCliServer(wiki.handler, async (url,) => {
				const failure = await dssFailure(
					["wiki", "attach", "article-1", file.path, "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).not.toBe(0,);
				const report = parse<ErrorOutput>(failure.stdout,);
				expect(report.code,).toBe("ambiguous_outcome",);
				expect(report.error,).toContain("lost.png",);
			},);
		} finally {
			file.cleanup();
		}
		expect(wiki.requests,).toEqual([
			`GET ${ARTICLE_PATH}`,
			`POST ${UPLOAD_PATH}`,
			`GET ${ARTICLE_PATH}`,
		],);
	});

	it("SDK attach keeps the file basename (spaces included) as the multipart filename", async () => {
		const file = withTempFile("a b.png", "PNG!",);
		const wiki = mockWiki([],);
		try {
			await withCliServer(wiki.handler, async (url,) => {
				const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
				const result = await client.wiki.attach("article-1", file.path,);
				expect(result.attachment.smartId,).toBe("new-0",);
				expect(result.fileName,).toBe("a b.png",);
			},);
		} finally {
			file.cleanup();
		}
		expect(wiki.multipart[0],).toContain('filename="a b.png"',);
	});
});

describe("wiki detach", () => {
	const keep = fileAttachment("keep-1", "keep.png", 3,);
	const drop = fileAttachment("drop-1", "drop.png", 4,);

	it("PUTs the article without exactly the given attachment and reports it", async () => {
		const wiki = mockWiki([keep, drop, fileAttachment("keep-2", "drop.png", 4,),],);
		await withCliServer(wiki.handler, async (url,) => {
			const { stdout, } = await dss(
				["wiki", "detach", "article-1", "drop-1", "--project-key", "TEST",],
				{ env: cliEnv(url,), },
			);
			const result = parse<DetachOutput>(stdout,);
			expect(result.detached.smartId,).toBe("drop-1",);
			expect(result.articleId,).toBe("article-1",);
			expect(ids(result.attachments,),).toEqual(["keep-1", "keep-2",],);
		},);
		expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`, `PUT ${ARTICLE_PATH}`,],);
		expect(wiki.putBodies,).toHaveLength(1,);
		const body = wiki.putBodies[0];
		expect(body?.payload,).toBe("# body",);
		expect(ids(body?.article.attachments ?? [],),).toEqual(["keep-1", "keep-2",],);
		expect(body?.article["name"],).toBe("Article 1",);
		expect(body?.article["versionTag"],).toEqual({ versionNumber: 3, },);
	});

	it("fails with a coded not_found listing the real smart ids, without writing", async () => {
		const wiki = mockWiki([keep, drop,],);
		await withCliServer(wiki.handler, async (url,) => {
			for (const extra of [[], ["--dry-run",],]) {
				const failure = await dssFailure(
					["wiki", "detach", "article-1", "nope", "--project-key", "TEST", ...extra,],
					{ env: cliEnv(url,), },
				);
				expect(failure.code,).toBe(1,);
				const report = parse<ErrorOutput>(failure.stdout,);
				expect(report.code,).toBe("not_found",);
				expect(report.details,).toEqual({
					articleId: "article-1",
					smartId: "nope",
					availableSmartIds: ["keep-1", "drop-1",],
				},);
			}
		},);
		expect(wiki.requests.every((request,) => request.startsWith("GET ",)),).toBe(true,);
	});

	it("--if-exists turns a missing attachment or a missing article into a skipped success", async () => {
		const wiki = mockWiki([keep,],);
		await withCliServer(wiki.handler, async (url,) => {
			const missingAttachment = parse<SkippedOutput>(
				(await dss(
					["wiki", "detach", "article-1", "gone", "--if-exists", "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				)).stdout,
			);
			expect(missingAttachment.skipped,).toBe("gone",);
			expect(missingAttachment.reason,).toBe("missing",);

			const missingArticle = parse<SkippedOutput>(
				(await dss(
					["wiki", "detach", "no-such-article", "gone", "--if-exists", "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				)).stdout,
			);
			expect(missingArticle.skipped,).toBe("gone",);
			expect(missingArticle.article,).toBe("no-such-article",);

			const missingArticleStrict = await dssFailure(
				["wiki", "detach", "no-such-article", "gone", "--project-key", "TEST",],
				{ env: cliEnv(url,), },
			);
			expect(missingArticleStrict.code,).not.toBe(0,);
		},);
		expect(wiki.requests.some((request,) => request.startsWith("PUT ",)),).toBe(false,);
		expect(wiki.attachments,).toEqual([keep,],);
	});

	it("--if-exists still removes an attachment that is present", async () => {
		const wiki = mockWiki([keep, drop,],);
		await withCliServer(wiki.handler, async (url,) => {
			const result = parse<DetachOutput>(
				(await dss(
					["wiki", "detach", "article-1", "drop-1", "--if-exists", "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				)).stdout,
			);
			expect(result.detached.smartId,).toBe("drop-1",);
		},);
		expect(ids(wiki.attachments,),).toEqual(["keep-1",],);
	});

	it("--dry-run previews the attachment without a write and --plan makes no request", async () => {
		const wiki = mockWiki([keep, drop,],);
		await withCliServer(wiki.handler, async (url,) => {
			const dry = parse<DryRunOutput>(
				(await dss(
					["wiki", "detach", "article-1", "drop-1", "--dry-run", "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				)).stdout,
			);
			expect(dry.dryRun,).toBe(true,);
			expect(dry.action,).toBe("detach",);
			expect(dry.current,).toEqual(drop,);
			expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`,],);

			const plan = parse<PlanOutput>(
				(await dss(
					["wiki", "detach", "article-1", "drop-1", "--plan", "--project-key", "TEST",],
					{ env: cliEnv(url,), },
				)).stdout,
			);
			expect(plan.plan,).toBe(true,);
			expect(plan.method,).toBe("PUT",);
			expect(plan.endpoint,).toBe(ARTICLE_PATH,);
			expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`,],);
		},);
		expect(wiki.attachments,).toHaveLength(2,);
	});

	it("reports an ambiguous outcome when DSS saves but the attachment is still there", async () => {
		const wiki = mockWiki([keep, drop,], { ignorePut: true, },);
		await withCliServer(wiki.handler, async (url,) => {
			const failure = await dssFailure(
				["wiki", "detach", "article-1", "drop-1", "--project-key", "TEST",],
				{ env: cliEnv(url,), },
			);
			expect(failure.code,).not.toBe(0,);
			expect(parse<ErrorOutput>(failure.stdout,).code,).toBe("ambiguous_outcome",);
		},);
		expect(wiki.putBodies,).toHaveLength(1,);
	});

	it("is advertised as a write with the right destructiveness and idempotency", async () => {
		const { stdout, } = await dss(["commands", "run", "--fields", "wiki.attach,wiki.detach",],);
		const registry = parse<Record<string, Record<string, unknown>>>(stdout,);
		expect(registry["wiki.attach"]?.["sideEffect"],).toBe("write",);
		expect(registry["wiki.attach"]?.["destructive"],).toBe("reversible",);
		expect(registry["wiki.detach"]?.["sideEffect"],).toBe("write",);
		expect(registry["wiki.detach"]?.["destructive"],).toBe("destructive",);
		expect(registry["wiki.detach"]?.["idempotency"],).toBe("if-exists",);
	});
});

describe("wiki SDK attach/detach", () => {
	it("attach rejects a missing local file with code not_found before any request", async () => {
		const wiki = mockWiki([],);
		await withCliServer(wiki.handler, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const missing = join(tmpdir(), `dss-wiki-sdk-missing-${Date.now()}.png`,);
			const error = await client.wiki.attach("article-1", missing,).catch((cause: unknown,) => cause);
			expect(error instanceof ClientValidationError ? error.code : String(error,),).toBe(
				"not_found",
			);
		},);
		expect(wiki.requests,).toEqual([],);
	});

	it("detach rejects an unknown smartId with code not_found and never PUTs", async () => {
		const wiki = mockWiki([fileAttachment("keep-1", "keep.png", 3,),],);
		await withCliServer(wiki.handler, async (url,) => {
			const client = new DataikuClient({ url, apiKey: "test-key", projectKey: "TEST", },);
			const error = await client.wiki.detach("article-1", "nope",).catch((cause: unknown,) => cause);
			expect(error instanceof ClientValidationError ? error.code : String(error,),).toBe(
				"not_found",
			);
		},);
		expect(wiki.requests,).toEqual([`GET ${ARTICLE_PATH}`,],);
	});
});
