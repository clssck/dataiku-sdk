import type {
	FlowZone,
	FlowZoneCreateOptions,
	FlowZoneItem,
	FlowZoneObjectType,
	FlowZoneUpdateOptions,
} from "../schemas.js";
import { FlowZoneArraySchema, FlowZoneSchema, } from "../schemas.js";
import { deepMerge, } from "../utils/deep-merge.js";
import { BaseResource, } from "./base.js";

export type FlowZoneItemInput = {
	objectId: string;
	objectType: FlowZoneObjectType;
	projectKey?: string;
};

function normalizeZoneItem(item: FlowZoneItemInput,): FlowZoneItem {
	return {
		objectId: item.objectId,
		objectType: item.objectType,
		...(item.projectKey ? { projectKey: item.projectKey, } : {}),
	};
}

export class FlowZonesResource extends BaseResource {
	/** List all flow zones in a project. */
	async list(projectKey?: string,): Promise<FlowZone[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones`,
		);
		return this.client.safeParse(FlowZoneArraySchema, raw, "flowZones.list",);
	}

	/** Get one flow zone by id. */
	async get(zoneId: string, projectKey?: string,): Promise<FlowZone> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}`,
		);
		return this.client.safeParse(FlowZoneSchema, raw, "flowZones.get",);
	}

	/** Create a flow zone. */
	async create(opts: FlowZoneCreateOptions,): Promise<FlowZone> {
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/flow/zones`,
			{
				name: opts.name,
				color: opts.color ?? "#2ab1ac",
			},
		);
		return this.client.safeParse(FlowZoneSchema, raw, "flowZones.create",);
	}

	/** Update flow zone settings such as name and color. */
	async update(zoneId: string, opts: FlowZoneUpdateOptions,): Promise<FlowZone> {
		const current = await this.get(zoneId, opts.projectKey,);
		const merged = deepMerge(current as unknown as Record<string, unknown>, {
			...(opts.name !== undefined ? { name: opts.name, } : {}),
			...(opts.color !== undefined ? { color: opts.color, } : {}),
		},);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(opts.projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}`,
			merged,
		);
		return this.get(zoneId, opts.projectKey,);
	}

	/** Delete a flow zone. DSS moves its items back to the default zone. */
	async delete(zoneId: string, projectKey?: string,): Promise<void> {
		await this.client.del(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}`,
		);
	}

	/** Move items into a flow zone. */
	async moveItems(
		zoneId: string,
		items: FlowZoneItemInput[],
		projectKey?: string,
	): Promise<FlowZone> {
		if (items.length === 0) throw new Error("flowZones.moveItems requires at least one item",);
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones/${
				encodeURIComponent(zoneId,)
			}/add-items`,
			items.map(normalizeZoneItem,),
		);
		return this.client.safeParse(FlowZoneSchema, raw, "flowZones.moveItems",);
	}

	/** Move a single item into a flow zone. */
	async moveItem(zoneId: string, item: FlowZoneItemInput, projectKey?: string,): Promise<FlowZone> {
		return this.moveItems(zoneId, [item,], projectKey,);
	}

	/** Get the graph for a single flow zone. */
	async graph(zoneId: string, projectKey?: string,): Promise<unknown> {
		return this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}/graph`,
		);
	}
}
