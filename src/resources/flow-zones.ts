import type { DataikuGetOptions, } from "../client.js";
import { ClientValidationError, } from "../errors.js";
import type {
	FlowZone,
	FlowZoneCreateOptions,
	FlowZoneItem,
	FlowZoneObjectType,
	FlowZonePosition,
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

export type FlowZoneRecreateOptions = {
	position: FlowZonePosition;
	name?: string;
	color?: string;
	projectKey?: string;
};

export type FlowZoneRecreateResult = {
	zone: FlowZone;
	replacedZoneId: string;
};

function normalizeZoneItem(item: FlowZoneItemInput,): FlowZoneItem {
	return {
		objectId: item.objectId,
		objectType: item.objectType,
		...(item.projectKey ? { projectKey: item.projectKey, } : {}),
	};
}

export class FlowZonesResource extends BaseResource {
	/**
	 * List all flow zones in a project. Pass `timeoutMs` to bound the whole
	 * request (attempts, backoff, and body read) by a total budget.
	 */
	async list(projectKey?: string, options?: DataikuGetOptions,): Promise<FlowZone[]> {
		const raw = await this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones`,
			options,
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

	/**
	 * Create a flow zone. DSS stores `position` only while the project's manual
	 * zone positioning is off; with it on, DSS auto-places the zone.
	 */
	async create(opts: FlowZoneCreateOptions,): Promise<FlowZone> {
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${this.enc(opts.projectKey,)}/flow/zones`,
			{
				name: opts.name,
				color: opts.color ?? "#2ab1ac",
				...(opts.position !== undefined ? { position: opts.position, } : {}),
			},
		);
		return this.client.safeParse(FlowZoneSchema, raw, "flowZones.create",);
	}

	/**
	 * Update flow zone name and color. DSS's public API ignores `position` on
	 * this endpoint; use {@link recreate} to move an existing zone.
	 */
	async update(zoneId: string, opts: FlowZoneUpdateOptions,): Promise<FlowZone> {
		const current = await this.get(zoneId, opts.projectKey,);
		const merged = deepMerge(current, {
			...(opts.name !== undefined ? { name: opts.name, } : {}),
			...(opts.color !== undefined ? { color: opts.color, } : {}),
		},);
		await this.client.putVoid(
			`/public/api/projects/${this.enc(opts.projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}`,
			merged,
		);
		return this.get(zoneId, opts.projectKey,);
	}

	/**
	 * Replace a zone with a new one at `position`: DSS honors a zone position
	 * only on creation, and only while the project's manual zone positioning is
	 * off (otherwise it auto-places the new zone). The replacement receives the
	 * old zone's name, color, metadata (tags, custom fields, checklists, ...),
	 * items, and shared items; the old zone is deleted once it is empty. The zone
	 * id changes, so anything referencing the old id (for example scenario zone
	 * builds) must be updated.
	 */
	async recreate(zoneId: string, opts: FlowZoneRecreateOptions,): Promise<FlowZoneRecreateResult> {
		const base = `/public/api/projects/${this.enc(opts.projectKey,)}/flow/zones`;
		const old = await this.get(zoneId, opts.projectKey,);
		const created = await this.create({
			name: opts.name ?? old.name,
			color: opts.color ?? old.color,
			position: opts.position,
			projectKey: opts.projectKey,
		},);
		const completed: string[] = ["create",];
		try {
			const { id: _id, items, shared, position: _position, ...metadata } = old;
			await this.client.putVoid(`${base}/${encodeURIComponent(created.id,)}`, {
				...created,
				...metadata,
				id: created.id,
				name: created.name,
				color: created.color,
				position: created.position,
			},);
			completed.push("metadata",);
			if (items && items.length > 0) {
				await this.moveItems(created.id, items, opts.projectKey,);
			}
			completed.push("items",);
			for (const item of shared ?? []) {
				await this.client.post<unknown>(
					`${base}/${encodeURIComponent(created.id,)}/shared`,
					normalizeZoneItem(item,),
				);
			}
			completed.push("shared",);
			await this.delete(old.id, opts.projectKey,);
		} catch (error) {
			throw new ClientValidationError(
				`Recreating flow zone ${old.id} at a new position stopped after ${
					completed.at(-1,)
				}; both zones may now exist.`,
				"ambiguous_outcome",
				"Inspect both zones with dss flow-zone list, then move remaining items and delete the zone you no longer need.",
				{ replacedZoneId: old.id, zoneId: created.id, completed, },
				{ cause: error, },
			);
		}
		return { zone: await this.get(created.id, opts.projectKey,), replacedZoneId: old.id, };
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
		if (items.length === 0) {
			throw new ClientValidationError(
				"flowZones.moveItems requires at least one item",
				"validation_failed",
			);
		}
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

	/** Get the full flow graph, or the graph for a single non-default flow zone. */
	async graph(zoneId?: string, projectKey?: string,): Promise<unknown> {
		if (zoneId === undefined || zoneId === "default") {
			return this.client.get<unknown>(`/public/api/projects/${this.enc(projectKey,)}/flow/graph/`,);
		}
		return this.client.get<unknown>(
			`/public/api/projects/${this.enc(projectKey,)}/flow/zones/${encodeURIComponent(zoneId,)}/graph`,
		);
	}
}
