import type { WikiArticleData, WikiSettings, } from "../schemas.js";
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
		const next = deepMerge(current as unknown as Record<string, unknown>, patch,) as WikiArticleData;
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
}
