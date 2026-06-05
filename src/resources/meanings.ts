import { BaseResource, } from "./base.js";

export type MeaningType =
	| "DECLARATIVE"
	| "VALUES_LIST"
	| "VALUES_MAPPING"
	| "PATTERN"
	| (string & {});

export type MeaningNormalizationMode = "EXACT" | "LOWERCASE" | "NORMALIZED" | (string & {});

export interface MeaningEntry extends Record<string, unknown> {
	value?: unknown;
	color?: string | null;
}

export interface MeaningMapping extends Record<string, unknown> {
	from?: unknown;
	to?: MeaningEntry | null;
}

export interface MeaningDefinition extends Record<string, unknown> {
	id?: string;
	label?: string;
	type?: MeaningType;
	description?: string | null;
	entries?: MeaningEntry[] | null;
	mappings?: MeaningMapping[] | null;
	pattern?: string | null;
	normalizationMode?: MeaningNormalizationMode | null;
	detectable?: boolean;
}

export interface MeaningCreateBody extends Record<string, unknown> {
	description?: string | null;
	entries?: MeaningEntry[] | null;
	mappings?: MeaningMapping[] | null;
	pattern?: string | null;
	normalizationMode?: MeaningNormalizationMode | null;
	detectable?: boolean;
}

export class MeaningsResource extends BaseResource {
	/** List all user-defined meanings on the DSS instance. */
	async list(): Promise<MeaningDefinition[]> {
		return this.client.get<MeaningDefinition[]>("/public/api/meanings/",);
	}

	/** Get a user-defined meaning definition. */
	async get(meaningId: string,): Promise<MeaningDefinition> {
		return this.client.get<MeaningDefinition>(this.meaningPath(meaningId,),);
	}

	/** Create a user-defined meaning. DSS returns a text confirmation. */
	async create(
		id: string,
		label: string,
		type: MeaningType,
		body: MeaningCreateBody = {},
	): Promise<string> {
		return this.client.postText("/public/api/meanings/", {
			...body,
			id,
			label,
			type,
			description: body.description ?? null,
			entries: body.entries ?? null,
			mappings: body.mappings ?? null,
			pattern: body.pattern ?? null,
			normalizationMode: body.normalizationMode ?? null,
			detectable: body.detectable ?? false,
		},);
	}

	/** Replace a user-defined meaning definition. */
	async update(meaningId: string, body: MeaningDefinition,): Promise<MeaningDefinition> {
		return this.client.put<MeaningDefinition>(this.meaningPath(meaningId,), body,);
	}

	private meaningPath(meaningId: string,): string {
		return `/public/api/meanings/${encodeURIComponent(meaningId,)}`;
	}
}
