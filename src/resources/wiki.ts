import fs from "node:fs";
import path from "node:path";
import { ClientValidationError, } from "../errors.js";
import type { WikiArticleAttachment, WikiArticleData, WikiSettings, } from "../schemas.js";
import {
	WikiArticleDataArraySchema,
	WikiArticleDataSchema,
	WikiSettingsSchema,
} from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export interface WikiArticleCreateOptions {
	name: string;
	parent?: string;
	content?: string;
	projectKey?: string;
}

export interface WikiArticleUpdateOptions {
	name?: string;
	content?: string;
	data?: Record<string, unknown>;
	projectKey?: string;
}

export interface WikiAttachResult {
	articleId: string;
	projectKey: string;
	fileName: string;
	bytes: number;
	/** The attachment DSS created, as re-read from the article. */
	attachment: WikiArticleAttachment;
	/** All attachments of the article after the upload. */
	attachments: WikiArticleAttachment[];
}

export interface WikiDetachResult {
	articleId: string;
	projectKey: string;
	/** The attachment that was removed. */
	detached: WikiArticleAttachment;
	/** Attachments left on the article. */
	attachments: WikiArticleAttachment[];
}

export interface LocalWikiAttachmentFile {
	path: string;
	fileName: string;
	bytes: number;
}

/** Attachments of an article; DSS omits the field when there are none. */
export function wikiAttachments(data: WikiArticleData,): WikiArticleAttachment[] {
	return data.article.attachments ?? [];
}

/** The attachment with exactly this smart id, or undefined. */
export function findWikiAttachment(
	data: WikiArticleData,
	smartId: string,
): WikiArticleAttachment | undefined {
	return wikiAttachments(data,).find((attachment,) => attachment.smartId === smartId);
}

/** Coded not_found error for a smart id that is not attached to the article. */
export function wikiAttachmentNotFound(
	data: WikiArticleData,
	smartId: string,
): ClientValidationError {
	const available = wikiAttachments(data,).flatMap((attachment,) =>
		attachment.smartId === undefined ? [] : [attachment.smartId,]
	);
	return new ClientValidationError(
		`Attachment ${smartId} not found on wiki article ${data.article.id}.`,
		"not_found",
		"List attachments with `dss wiki get ARTICLE_ID` (article.attachments[].smartId), or pass --if-exists to treat absence as success.",
		{ articleId: data.article.id, smartId, availableSmartIds: available, },
	);
}

/** Check that a local path is a readable regular file before any DSS call. */
export async function statWikiAttachmentFile(localPath: string,): Promise<LocalWikiAttachmentFile> {
	let stat: fs.Stats;
	try {
		stat = await fs.promises.stat(localPath,);
	} catch (error) {
		throw new ClientValidationError(
			`Could not read attachment file ${localPath}: ${
				error instanceof Error ? error.message : String(error,)
			}.`,
			"not_found",
			"Verify the file path and that it is readable.",
		);
	}
	if (!stat.isFile()) {
		throw new ClientValidationError(`${localPath} is not a regular file.`,);
	}
	return { path: localPath, fileName: path.basename(localPath,), bytes: stat.size, };
}

const WIKI_LIST_CONCURRENCY = 4;

function taxonomyIds(nodes: unknown[] | undefined,): string[] {
	const ids: string[] = [];
	for (const node of nodes ?? []) {
		if (!node || typeof node !== "object" || Array.isArray(node,)) continue;
		const record = node as Record<string, unknown>;
		if (typeof record.id === "string" && record.id.length > 0) ids.push(record.id,);
		if (Array.isArray(record.children,)) ids.push(...taxonomyIds(record.children,),);
	}
	return ids;
}

async function mapWithConcurrency<T, U,>(
	items: T[],
	limit: number,
	mapper: (item: T,) => Promise<U>,
): Promise<U[]> {
	const results: U[] = [];
	let nextIndex = 0;
	async function worker(): Promise<void> {
		while (nextIndex < items.length) {
			const index = nextIndex;
			nextIndex++;
			results[index] = await mapper(items[index]!,);
		}
	}
	await Promise.all(
		Array.from({ length: Math.min(limit, items.length,), }, () => worker(),),
	);
	return results;
}

export class WikiResource extends BaseResource {
	async settings(projectKey?: string,): Promise<WikiSettings> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/wiki/`,
		);
		return this.client.safeParse(WikiSettingsSchema, raw, "wiki.settings",);
	}

	/**
	 * Replace the wiki properties (home article and taxonomy, PUT /wiki/) with
	 * an object obtained from `settings`, edited.
	 */
	async updateSettings(settings: Record<string, unknown>, projectKey?: string,): Promise<void> {
		await this.client.putVoid(`/public/api/projects/${this.enc(projectKey,)}/wiki/`, settings,);
	}

	async list(projectKey?: string,): Promise<WikiArticleData[]> {
		const settings = await this.settings(projectKey,);
		const ids = taxonomyIds(settings.taxonomy,);
		const articles = await mapWithConcurrency(
			ids,
			WIKI_LIST_CONCURRENCY,
			(id,) => this.get(id, projectKey,),
		);
		return this.client.safeParse(WikiArticleDataArraySchema, articles, "wiki.list",);
	}

	async get(articleIdOrName: string, projectKey?: string,): Promise<WikiArticleData> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/wiki/${encodeURIComponent(articleIdOrName,)}`,
		);
		return this.client.safeParse(WikiArticleDataSchema, raw, "wiki.get",);
	}

	async create(opts: WikiArticleCreateOptions,): Promise<WikiArticleData> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/wiki/`,
			{
				projectKey: pk,
				name: opts.name,
				parent: opts.parent ?? null,
			},
		);
		const created = this.client.safeParse(WikiArticleDataSchema, raw, "wiki.create",);
		if (opts.content === undefined) return created;
		return this.update(created.article.id, { content: opts.content, projectKey: pk, },);
	}

	async update(articleIdOrName: string, opts: WikiArticleUpdateOptions,): Promise<WikiArticleData> {
		const current = await this.get(articleIdOrName, opts.projectKey,);
		const patch: Record<string, unknown> = opts.data ?? {};
		const next = deepMerge(current, patch,);
		if (opts.name !== undefined) next.article = { ...next.article, name: opts.name, };
		if (opts.content !== undefined) next.payload = opts.content;
		const raw = await this.client.put<unknown>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/wiki/${
				encodeURIComponent(current.article.id,)
			}`,
			next,
		);
		return this.client.safeParse(WikiArticleDataSchema, raw, "wiki.update",);
	}

	async delete(articleIdOrName: string, projectKey?: string,): Promise<void> {
		const current = await this.get(articleIdOrName, projectKey,);
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/wiki/${encodeURIComponent(current.article.id,)}`,
		);
	}

	/**
	 * Upload a local file and attach it to an article (multipart POST to
	 * /wiki/{id}/upload). DSS names the attachment after the file's basename and
	 * types it from the extension. The article is re-read to identify the new
	 * attachment.
	 */
	async attach(
		articleIdOrName: string,
		localPath: string,
		projectKey?: string,
	): Promise<WikiAttachResult> {
		const file = await statWikiAttachmentFile(localPath,);
		const pk = this.resolveProjectKey(projectKey,);
		const before = await this.get(articleIdOrName, pk,);
		const articleId = before.article.id;
		await this.client.uploadJson<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/wiki/${encodeURIComponent(articleId,)}/upload`,
			localPath,
			file.fileName,
		);
		const after = await this.get(articleId, pk,);
		const knownIds = new Set(wikiAttachments(before,).map((attachment,) => attachment.smartId),);
		let created = wikiAttachments(after,).filter((attachment,) => !knownIds.has(attachment.smartId,));
		if (created.length > 1) {
			created = created.filter((attachment,) =>
				attachment.details?.["objectDisplayName"] === file.fileName
				&& attachment.details?.["size"] === file.bytes
			);
		}
		const attachment = created[0];
		if (created.length !== 1 || attachment === undefined || attachment.smartId === undefined) {
			throw new ClientValidationError(
				`DSS did not verify upload of ${file.fileName} to wiki article ${articleId}.`,
				"ambiguous_outcome",
				"The upload returned successfully, but the article did not show exactly one new attachment. Inspect the article (dss wiki get) before retrying.",
				{ articleId, expectedBytes: file.bytes, newAttachments: created, },
			);
		}
		return {
			articleId,
			projectKey: pk,
			fileName: file.fileName,
			bytes: file.bytes,
			attachment,
			attachments: wikiAttachments(after,),
		};
	}

	/**
	 * Remove exactly one attachment (by smart id) from an article: the article is
	 * read, the entry filtered out of `article.attachments`, and the whole
	 * article PUT back. Throws a coded `not_found` error when the smart id is not
	 * attached.
	 */
	async detach(
		articleIdOrName: string,
		smartId: string,
		projectKey?: string,
	): Promise<WikiDetachResult> {
		const pk = this.resolveProjectKey(projectKey,);
		const current = await this.get(articleIdOrName, pk,);
		const detached = findWikiAttachment(current, smartId,);
		if (detached === undefined) throw wikiAttachmentNotFound(current, smartId,);
		const remaining = wikiAttachments(current,).filter((attachment,) =>
			attachment.smartId !== smartId
		);
		const raw = await this.client.put<unknown>(
			`/public/api/projects/${encodeURIComponent(pk,)}/wiki/${
				encodeURIComponent(current.article.id,)
			}`,
			{ ...current, article: { ...current.article, attachments: remaining, }, },
		);
		const saved = this.client.safeParse(WikiArticleDataSchema, raw, "wiki.detach",);
		const after = wikiAttachments(saved,);
		if (
			findWikiAttachment(saved, smartId,) !== undefined
			|| after.length !== remaining.length
		) {
			throw new ClientValidationError(
				`DSS did not verify removal of attachment ${smartId} from wiki article ${current.article.id}.`,
				"ambiguous_outcome",
				"The save returned successfully, but the article's attachments are not the expected remainder. Inspect the article (dss wiki get) before retrying.",
				{
					articleId: current.article.id,
					smartId,
					expectedRemaining: remaining.length,
					actualRemaining: after.length,
				},
			);
		}
		return { articleId: current.article.id, projectKey: pk, detached, attachments: after, };
	}
}
