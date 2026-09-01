import { readFileSync, } from "node:fs";
import type { DataikuClient, } from "../../client.js";
import type { FlowZoneItemInput, } from "../../resources/flow-zones.js";
import type { FlowZone, FlowZoneObjectType, FlowZonePosition, } from "../../schemas.js";
import { compareStrings, } from "../../utils/stable-hash.js";
import {
	finiteNumberField,
	jsonInput,
	optionalStringField,
	parseJsonObject,
	plainRecord,
	requiredStringArray,
	splitCsvFlag,
} from "../coerce.js";
import { UsageError, } from "../usage.js";

export function flowZoneId(value: string,): string {
	const trimmed = value.trim();
	if (!trimmed) throw new UsageError("Flow zone id must not be empty.",);
	return trimmed;
}

export function flowZoneName(value: string | boolean | undefined,): string {
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			"--name is required. Usage: dss flow-zone create --name NAME [--color #RRGGBB]",
		);
	}
	return value.trim();
}

export function flowZoneColor(value: string | boolean | undefined,): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !/^#[0-9a-fA-F]{6}$/.test(value.trim(),)) {
		throw new UsageError("--color must be a hex color like #2ab1ac.",);
	}
	return value.trim();
}

export function flowZoneObjectType(value: string,): FlowZoneObjectType {
	const normalized = value.trim().toUpperCase().replace(/-/g, "_",);
	if (normalized === "FOLDER") return "MANAGED_FOLDER";
	if (normalized === "MODEL_EVAL_STORE") return "MODEL_EVALUATION_STORE";
	if (normalized === "KNOWLEDGE_BANK") return "RETRIEVABLE_KNOWLEDGE";
	if (
		normalized === "DATASET"
		|| normalized === "MANAGED_FOLDER"
		|| normalized === "SAVED_MODEL"
		|| normalized === "RECIPE"
		|| normalized === "MODEL_EVALUATION_STORE"
		|| normalized === "STREAMING_ENDPOINT"
		|| normalized === "LABELING_TASK"
		|| normalized === "RETRIEVABLE_KNOWLEDGE"
	) {
		return normalized;
	}
	throw new UsageError(
		`Invalid flow zone object type: ${value}. Use DATASET, RECIPE, MANAGED_FOLDER, SAVED_MODEL, MODEL_EVALUATION_STORE, STREAMING_ENDPOINT, LABELING_TASK, or RETRIEVABLE_KNOWLEDGE.`,
		"invalid_enum",
	);
}

export function addFlowZoneFlagItems(
	items: FlowZoneItemInput[],
	flags: Record<string, string | boolean>,
	flagName: string,
	objectType: FlowZoneObjectType,
): void {
	for (const objectId of splitCsvFlag(flags[flagName],)) {
		items.push({ objectId, objectType, },);
	}
}

export function parseFlowZoneObject(value: string,): FlowZoneItemInput {
	const parts = value.split(":",).map((part,) => part.trim()).filter((part,) => part.length > 0);
	if (parts.length === 2) {
		return { objectType: flowZoneObjectType(parts[0],), objectId: parts[1], };
	}
	if (parts.length === 3) {
		return { projectKey: parts[0], objectType: flowZoneObjectType(parts[1],), objectId: parts[2], };
	}
	throw new UsageError(
		`Invalid --object value: ${value}. Use TYPE:ID or PROJECT_KEY:TYPE:ID.`,
	);
}

export function flowZoneMoveItems(flags: Record<string, string | boolean>,): FlowZoneItemInput[] {
	const items: FlowZoneItemInput[] = [];
	addFlowZoneFlagItems(items, flags, "dataset", "DATASET",);
	addFlowZoneFlagItems(items, flags, "recipe", "RECIPE",);
	addFlowZoneFlagItems(items, flags, "folder", "MANAGED_FOLDER",);
	addFlowZoneFlagItems(items, flags, "saved-model", "SAVED_MODEL",);
	addFlowZoneFlagItems(items, flags, "model-evaluation-store", "MODEL_EVALUATION_STORE",);
	addFlowZoneFlagItems(items, flags, "streaming-endpoint", "STREAMING_ENDPOINT",);
	addFlowZoneFlagItems(items, flags, "labeling-task", "LABELING_TASK",);
	addFlowZoneFlagItems(items, flags, "knowledge-bank", "RETRIEVABLE_KNOWLEDGE",);
	for (const value of splitCsvFlag(flags["object"],)) {
		items.push(parseFlowZoneObject(value,),);
	}
	return items;
}

export function flowZoneItems(zone: FlowZone,): FlowZoneItemInput[] {
	return [...(zone.items ?? []), ...(zone.shared ?? []),];
}

export function flowZoneContains(zone: FlowZone, object: FlowZoneItemInput,): boolean {
	return flowZoneItems(zone,).some((item,) =>
		item.objectId === object.objectId
		&& item.objectType === object.objectType
		&& (object.projectKey === undefined || item.projectKey === object.projectKey)
	);
}

export function flowZoneSummary(
	zone: FlowZone,
	object?: FlowZoneItemInput,
): Record<string, unknown> {
	const items = flowZoneItems(zone,);
	return {
		id: zone.id,
		name: zone.name,
		itemCount: items.length,
		...(object ? { containsMatchingObject: flowZoneContains(zone, object,), } : {}),
	};
}

export function flowZoneDetailSummary(zone: FlowZone,): Record<string, unknown> {
	return {
		...flowZoneSummary(zone,),
		items: flowZoneItems(zone,),
	};
}

export interface FlowZoneOrganizeZonePlan {
	id?: string;
	name?: string;
	color?: string;
	position?: FlowZonePosition;
	items: FlowZoneItemInput[];
}

export interface FlowZoneOrganizePlan {
	topologyFingerprint?: string;
	zones: FlowZoneOrganizeZonePlan[];
}

export interface FlowZoneOrganizeValidationIssue {
	zone: string;
	objectId: string;
	objectType: FlowZoneObjectType;
	projectKey?: string;
	reason: string;
}

export interface FlowZoneOrganizeValidationResult {
	valid: boolean;
	missing: FlowZoneOrganizeValidationIssue[];
}

export interface FlowZoneOrganizeValidationIndex {
	projectKey: string;
	all: Set<string>;
	datasets: Set<string>;
	recipes: Set<string>;
	folders: Set<string>;
}

export function flowZonePlanColor(value: unknown, source: string,): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !/^#[0-9a-fA-F]{6}$/.test(value.trim(),)) {
		throw new UsageError(`${source} must be a hex color like #2ab1ac.`, "validation_failed",);
	}
	return value.trim();
}

export function flowZonePlanPosition(
	value: unknown,
	source: string,
): FlowZonePosition | undefined {
	if (value === undefined) return undefined;
	const record = plainRecord(value,);
	if (!record) {
		throw new UsageError(`${source} must be an object with x and y.`, "validation_failed",);
	}
	return {
		x: finiteNumberField(record, "x", source,),
		y: finiteNumberField(record, "y", source,),
	};
}

export function flowZoneCurrentPosition(zone: FlowZone,): FlowZonePosition | undefined {
	const record = zone as unknown as Record<string, unknown>;
	const position = plainRecord(record.position,);
	if (!position) return undefined;
	const x = position.x;
	const y = position.y;
	return typeof x === "number" && Number.isFinite(x,) && typeof y === "number" && Number.isFinite(y,)
		? { x, y, }
		: undefined;
}

export function flowZoneSamePosition(
	a: FlowZonePosition | undefined,
	b: FlowZonePosition | undefined,
): boolean {
	if (a === undefined || b === undefined) return a === b;
	return a.x === b.x && a.y === b.y;
}

export function parseFlowZonePlanItem(value: unknown, source: string,): FlowZoneItemInput {
	if (typeof value === "string") return parseFlowZoneObject(value,);
	const record = plainRecord(value,);
	if (!record) {
		throw new UsageError(`${source} must be TYPE:ID or an object.`, "validation_failed",);
	}
	const object = optionalStringField(record, ["object",],);
	if (object) return parseFlowZoneObject(object,);
	const objectType = optionalStringField(record, ["objectType", "type",],);
	const objectId = optionalStringField(record, ["objectId", "id", "name",],);
	if (!objectType || !objectId) {
		throw new UsageError(
			`${source} must include objectType/type and objectId/id, or object as TYPE:ID.`,
			"validation_failed",
		);
	}
	const projectKey = optionalStringField(record, ["projectKey", "project",],);
	return {
		objectType: flowZoneObjectType(objectType,),
		objectId,
		...(projectKey ? { projectKey, } : {}),
	};
}

export function addFlowZonePlanTypedItems(
	items: FlowZoneItemInput[],
	record: Record<string, unknown>,
	key: string,
	objectType: FlowZoneObjectType,
	source: string,
): void {
	if (record[key] === undefined) return;
	for (const objectId of requiredStringArray(record[key], `${source}.${key}`,)) {
		items.push({ objectType, objectId, },);
	}
}

export function flowZoneItemKey(item: FlowZoneItemInput,): string {
	return `${item.projectKey ?? ""}\0${item.objectType}\0${item.objectId}`;
}

export function flowZonePlanLabel(plan: FlowZoneOrganizeZonePlan,): string {
	return plan.id ?? plan.name ?? "<unknown>";
}

export function dedupeFlowZonePlanItems(items: FlowZoneItemInput[],): FlowZoneItemInput[] {
	const seen = new Set<string>();
	const result: FlowZoneItemInput[] = [];
	for (const item of items) {
		const key = flowZoneItemKey(item,);
		if (seen.has(key,)) continue;
		seen.add(key,);
		result.push(item,);
	}
	return result;
}

export function flowZonePlanItemKeys(plan: FlowZoneOrganizePlan,): Set<string> {
	const keys = new Set<string>();
	for (const zone of plan.zones) {
		for (const item of zone.items) keys.add(flowZoneItemKey(item,),);
	}
	return keys;
}

export function validateUniqueFlowZoneAssignments(plan: FlowZoneOrganizePlan,): void {
	const seen = new Map<string, string>();
	for (const zone of plan.zones) {
		const label = flowZonePlanLabel(zone,);
		for (const item of zone.items) {
			const key = flowZoneItemKey(item,);
			const previous = seen.get(key,);
			if (previous) {
				throw new UsageError(
					`Flow object ${item.objectType}:${item.objectId} is assigned to both "${previous}" and "${label}".`,
					"validation_failed",
				);
			}
			seen.set(key, label,);
		}
	}
}

export function parseFlowZoneOrganizePlan(input: Record<string, unknown>,): FlowZoneOrganizePlan {
	const zones = input.zones;
	if (!Array.isArray(zones,)) {
		throw new UsageError(
			"Flow zone organize plan must include a zones array.",
			"validation_failed",
		);
	}
	const topologyFingerprint = optionalStringField(input, ["topologyFingerprint",],);
	if (topologyFingerprint !== undefined && !/^[0-9a-f]{64}$/.test(topologyFingerprint,)) {
		throw new UsageError(
			"topologyFingerprint must be a lowercase SHA-256 hash.",
			"validation_failed",
		);
	}
	const plan: FlowZoneOrganizePlan = {
		...(topologyFingerprint ? { topologyFingerprint, } : {}),
		zones: zones.map((value, index,) => {
			const source = `zones[${index}]`;
			const record = plainRecord(value,);
			if (!record) throw new UsageError(`${source} must be an object.`, "validation_failed",);
			const id = optionalStringField(record, ["id", "zoneId",],);
			const name = optionalStringField(record, ["name",],);
			if (!id && !name) {
				throw new UsageError(`${source} must include name or id.`, "validation_failed",);
			}
			const items: FlowZoneItemInput[] = [];
			const rawItems = record.items ?? record.objects;
			if (rawItems !== undefined) {
				if (!Array.isArray(rawItems,)) {
					throw new UsageError(`${source}.items must be an array.`, "validation_failed",);
				}
				rawItems.forEach((item, itemIndex,) => {
					items.push(parseFlowZonePlanItem(item, `${source}.items[${itemIndex}]`,),);
				},);
			}
			addFlowZonePlanTypedItems(items, record, "datasets", "DATASET", source,);
			addFlowZonePlanTypedItems(items, record, "recipes", "RECIPE", source,);
			addFlowZonePlanTypedItems(items, record, "folders", "MANAGED_FOLDER", source,);
			addFlowZonePlanTypedItems(items, record, "savedModels", "SAVED_MODEL", source,);
			addFlowZonePlanTypedItems(
				items,
				record,
				"modelEvaluationStores",
				"MODEL_EVALUATION_STORE",
				source,
			);
			addFlowZonePlanTypedItems(
				items,
				record,
				"streamingEndpoints",
				"STREAMING_ENDPOINT",
				source,
			);
			addFlowZonePlanTypedItems(items, record, "labelingTasks", "LABELING_TASK", source,);
			addFlowZonePlanTypedItems(items, record, "knowledgeBanks", "RETRIEVABLE_KNOWLEDGE", source,);
			return {
				...(id ? { id, } : {}),
				...(name ? { name, } : {}),
				...(record.color !== undefined
					? { color: flowZonePlanColor(record.color, `${source}.color`,), }
					: {}),
				...(record.position !== undefined
					? { position: flowZonePlanPosition(record.position, `${source}.position`,), }
					: {}),
				items: dedupeFlowZonePlanItems(items,),
			};
		},),
	};
	validateUniqueFlowZoneAssignments(plan,);
	return plan;
}

export function readFlowZoneOrganizePlan(
	flags: Record<string, string | boolean>,
	usage: string,
): FlowZoneOrganizePlan {
	const data = typeof flags["file"] === "string"
		? parseJsonObject(readFileSync(flags["file"], "utf-8",), flags["file"],)
		: jsonInput(flags,);
	if (!data) {
		throw new UsageError(
			`--data, --data-file, --file, or --stdin is required. Usage: ${usage}`,
			"missing_required_flag",
		);
	}
	return parseFlowZoneOrganizePlan(data,);
}

export function findFlowZoneForPlan(
	zones: FlowZone[],
	plan: FlowZoneOrganizeZonePlan,
): FlowZone | undefined {
	if (plan.id) {
		const byId = zones.find((zone,) => zone.id === plan.id);
		if (byId) return byId;
	}
	if (!plan.name) return undefined;
	const byName = zones.filter((zone,) => zone.name === plan.name);
	if (byName.length > 1) {
		throw new UsageError(
			`Multiple flow zones named "${plan.name}" exist; use id.`,
			"validation_failed",
		);
	}
	return byName[0];
}

export function ensureFlowZonePlanTarget(
	plan: FlowZoneOrganizeZonePlan,
	existing: FlowZone | undefined,
): void {
	if (existing || plan.name) return;
	throw new UsageError(
		`Flow zone ${plan.id ?? "<unknown>"} was not found and cannot be created without name.`,
		"validation_failed",
	);
}

export function flowZoneExplicitItems(zone: FlowZone,): FlowZoneItemInput[] {
	return (zone.items ?? []).map((item,) => ({
		objectId: item.objectId,
		objectType: item.objectType,
		...(item.projectKey ? { projectKey: item.projectKey, } : {}),
	}));
}

export function flowZonePlanFromZones(
	zones: FlowZone[],
	topologyFingerprint: string,
): FlowZoneOrganizePlan {
	return {
		topologyFingerprint,
		zones: [...zones,]
			.sort((a, b,) => compareStrings(a.id, b.id,))
			.map((zone,) => {
				const position = flowZoneCurrentPosition(zone,);
				return {
					id: zone.id,
					name: zone.name,
					...(zone.color ? { color: zone.color, } : {}),
					...(position ? { position, } : {}),
					items: flowZoneExplicitItems(zone,).sort((a, b,) =>
						compareStrings(flowZoneItemKey(a,), flowZoneItemKey(b,),)
					),
				};
			},),
	};
}

export function flowZoneMoveDelta(
	existing: FlowZone | undefined,
	plannedItems: FlowZoneItemInput[],
): FlowZoneItemInput[] {
	if (!existing) return plannedItems;
	const existingKeys = new Set(flowZoneExplicitItems(existing,).map(flowZoneItemKey,),);
	return plannedItems.filter((item,) => !existingKeys.has(flowZoneItemKey(item,),));
}

export function flowZonePruneItems(
	existing: FlowZone | undefined,
	plannedItemKeys: Set<string>,
): FlowZoneItemInput[] {
	if (!existing) return [];
	return flowZoneExplicitItems(existing,).filter((item,) =>
		!plannedItemKeys.has(flowZoneItemKey(item,),)
	);
}

export function flowZoneOrganizeStep(
	plan: FlowZoneOrganizeZonePlan,
	existing: FlowZone | undefined,
	sync: boolean,
	plannedItemKeys: Set<string>,
): Record<string, unknown> {
	ensureFlowZonePlanTarget(plan, existing,);
	const update: Record<string, unknown> = {};
	if (existing && plan.name && plan.name !== existing.name) update.name = plan.name;
	if (existing && plan.color && plan.color !== existing.color) update.color = plan.color;
	if (
		existing && plan.position !== undefined
		&& !flowZoneSamePosition(flowZoneCurrentPosition(existing,), plan.position,)
	) {
		update.position = plan.position;
	}
	const pruneItems = sync ? flowZonePruneItems(existing, plannedItemKeys,) : [];
	const moveItems = flowZoneMoveDelta(existing, plan.items,);
	return {
		target: {
			...(plan.id ? { id: plan.id, } : {}),
			...(plan.name ? { name: plan.name, } : {}),
			...(plan.color ? { color: plan.color, } : {}),
			...(plan.position ? { position: plan.position, } : {}),
		},
		...(existing ? { existing: flowZoneSummary(existing,), } : { create: true, }),
		...(Object.keys(update,).length > 0 ? { update, } : {}),
		moveItems,
		...(pruneItems.length > 0 ? { pruneItems, } : {}),
	};
}

export function flowZoneValidationBucket(
	index: FlowZoneOrganizeValidationIndex,
	objectType: FlowZoneObjectType,
): Set<string> {
	switch (objectType) {
		case "DATASET":
			return index.datasets;
		case "RECIPE":
			return index.recipes;
		case "MANAGED_FOLDER":
			return index.folders;
		case "SAVED_MODEL":
		case "MODEL_EVALUATION_STORE":
		case "STREAMING_ENDPOINT":
		case "LABELING_TASK":
		case "RETRIEVABLE_KNOWLEDGE":
			return index.all;
	}
}

export async function flowZoneValidationIndex(
	client: DataikuClient,
	projectKey: string | undefined,
): Promise<FlowZoneOrganizeValidationIndex> {
	const result = await client.projects.map({
		projectKey,
		maxNodes: 100_000,
		maxEdges: 100_000,
	},);
	const index: FlowZoneOrganizeValidationIndex = {
		projectKey: result.map.projectKey,
		all: new Set(),
		datasets: new Set(),
		recipes: new Set(),
		folders: new Set(),
	};
	for (const node of result.map.nodes) {
		index.all.add(node.id,);
		switch (node.kind) {
			case "dataset":
				index.datasets.add(node.id,);
				break;
			case "recipe":
				index.recipes.add(node.id,);
				break;
			case "folder":
				index.folders.add(node.id,);
				break;
			case "other":
				break;
		}
	}
	return index;
}

export async function validateFlowZoneOrganizeObjects(
	client: DataikuClient,
	plan: FlowZoneOrganizePlan,
	projectKey: string | undefined,
): Promise<FlowZoneOrganizeValidationResult> {
	const indexes = new Map<string, FlowZoneOrganizeValidationIndex>();
	const missing: FlowZoneOrganizeValidationIssue[] = [];

	const getIndex = async (itemProjectKey: string | undefined,) => {
		const requestedProjectKey = itemProjectKey ?? projectKey;
		const cacheKey = requestedProjectKey ?? "";
		const cached = indexes.get(cacheKey,);
		if (cached) return cached;
		const index = await flowZoneValidationIndex(client, requestedProjectKey,);
		indexes.set(cacheKey, index,);
		return index;
	};

	for (const zone of plan.zones) {
		for (const item of zone.items) {
			const index = await getIndex(item.projectKey,);
			const bucket = flowZoneValidationBucket(index, item.objectType,);
			if (bucket.has(item.objectId,)) continue;
			missing.push({
				zone: flowZonePlanLabel(zone,),
				objectId: item.objectId,
				objectType: item.objectType,
				...(item.projectKey ? { projectKey: item.projectKey, } : {}),
				reason: `Object not found in project ${item.projectKey ?? index.projectKey}.`,
			},);
		}
	}

	return { valid: missing.length === 0, missing, };
}

export function throwFlowZoneValidationError(validation: FlowZoneOrganizeValidationResult,): void {
	if (validation.valid) return;
	const first = validation.missing[0];
	const suffix = validation.missing.length > 1 ? ` and ${validation.missing.length - 1} more` : "";
	throw new UsageError(
		`Flow zone organize validation failed: ${first?.objectType}:${first?.objectId} in zone "${first?.zone}" was not found${suffix}.`,
		"validation_failed",
	);
}

export async function resolveFlowZoneIdFromFlags(
	client: DataikuClient,
	flags: Record<string, string | boolean>,
	projectKey?: string,
): Promise<string | undefined> {
	const zoneId = typeof flags["zone-id"] === "string" ? flags["zone-id"].trim() : "";
	if (zoneId) return zoneId;
	const zone = typeof flags["zone"] === "string" ? flags["zone"].trim() : "";
	if (!zone) return undefined;
	const zones = await client.flowZones.list(projectKey,);
	const match = zones.find((candidate,) => candidate.id === zone || candidate.name === zone);
	if (!match) throw new UsageError(`Flow zone not found: ${zone}`, "invalid_enum",);
	return match.id;
}

export async function moveCreatedItemsToZone(
	client: DataikuClient,
	flags: Record<string, string | boolean>,
	items: FlowZoneItemInput[],
	projectKey?: string,
): Promise<{ zoneId?: string; moved?: FlowZoneItemInput[]; }> {
	const zoneId = await resolveFlowZoneIdFromFlags(client, flags, projectKey,);
	if (!zoneId || items.length === 0) return {};
	await client.flowZones.moveItems(zoneId, items, projectKey,);
	return { zoneId, moved: items, };
}
