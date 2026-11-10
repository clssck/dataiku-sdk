import type { DataikuClient, } from "../../client.js";
import { ClientValidationError, } from "../../errors.js";
import type { FlowZoneItemInput, } from "../../resources/flow-zones.js";
import type { FlowZone, FlowZonePosition, } from "../../schemas.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import { asRecord, } from "../../utils/records.js";
import { executionMode, } from "../flags.js";
import {
	applyFlowZoneUnits,
	ensureFlowZonePlanTarget,
	findFlowZoneForPlan,
	flowZoneColor,
	flowZoneContains,
	flowZoneCurrentPosition,
	flowZoneDetailSummary,
	flowZoneId,
	flowZoneMoveDelta,
	flowZoneMoveItems,
	flowZoneName,
	flowZoneOrganizeStep,
	flowZonePlanFromZones,
	flowZonePlanItemKeys,
	flowZonePruneItems,
	flowZoneRepositionTarget,
	flowZoneSamePosition,
	flowZoneSummary,
	flowZoneUnassignedItems,
	readFlowZoneOrganizePlan,
	resolveFlowZoneIdFromFlags,
	throwFlowZoneValidationError,
	validateFlowZoneOrganizeObjects,
} from "../helpers/flow-zone.js";
import { enqueueCliWarning, readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

const ZONE_OBJECT_IDS = { type: "array", items: { type: "string", minLength: 1, }, };

/** JSON Schema of the `flow-zone organize` plan, as parsed by `parseFlowZoneOrganizePlan`. */
const FLOW_ZONE_ORGANIZE_PLAN_SCHEMA: Record<string, unknown> = {
	type: "object",
	required: ["zones",],
	properties: {
		topologyFingerprint: {
			type: "string",
			pattern: "^[0-9a-f]{64}$",
			description:
				"From dss flow-zone plan; organize refuses to apply if the flow topology changed since.",
		},
		zones: {
			type: "array",
			items: {
				type: "object",
				anyOf: [{ required: ["id",], }, { required: ["name",], },],
				properties: {
					id: { type: "string", description: "Existing zone id (alias zoneId); matched before name.", },
					name: { type: "string", description: "Zone name; required to create a zone.", },
					color: { type: "string", pattern: "^#[0-9a-fA-F]{6}$", },
					position: {
						type: "object",
						required: ["x", "y",],
						properties: { x: { type: "number", }, y: { type: "number", }, },
						description:
							"Applied when the zone is created; moving an existing zone requires --recreate-on-move.",
					},
					items: {
						type: "array",
						description:
							"Alias objects. Each entry is TYPE:ID or {objectType|type, objectId|id|name, projectKey?}.",
						items: {
							anyOf: [
								{ type: "string", pattern: "^[A-Z_]+:.+$", },
								{
									type: "object",
									properties: {
										object: { type: "string", },
										objectType: { type: "string", },
										type: { type: "string", },
										objectId: { type: "string", },
										id: { type: "string", },
										name: { type: "string", },
										projectKey: { type: "string", },
									},
								},
							],
						},
					},
					datasets: ZONE_OBJECT_IDS,
					recipes: ZONE_OBJECT_IDS,
					folders: ZONE_OBJECT_IDS,
					savedModels: ZONE_OBJECT_IDS,
					modelEvaluationStores: ZONE_OBJECT_IDS,
					streamingEndpoints: ZONE_OBJECT_IDS,
					labelingTasks: ZONE_OBJECT_IDS,
					knowledgeBanks: ZONE_OBJECT_IDS,
				},
			},
		},
	},
};

/**
 * DSS stores a zone `position` sent on creation only while the project's
 * manual zone positioning is off (with it on, DSS auto-places the new zone),
 * and draws stored positions only while it is on.
 */
async function readManualPositioning(
	c: DataikuClient,
	projectKey: string | undefined,
): Promise<{ manual?: boolean; error?: string; }> {
	try {
		const settings = asRecord(await c.projects.getSettings(projectKey,),);
		const display = asRecord(asRecord(settings?.settings,)?.flowDisplaySettings,);
		return { manual: display?.zonesManualPositioning === true, };
	} catch (error) {
		return { error: error instanceof Error ? error.message.split("\n",)[0] : String(error,), };
	}
}

/** Read-modify-write of `flowDisplaySettings.zonesManualPositioning`, keeping every other setting. */
async function setManualPositioning(
	c: DataikuClient,
	projectKey: string | undefined,
	manual: boolean,
): Promise<void> {
	const current = await c.projects.getSettings(projectKey,);
	const settings = asRecord(current.settings,) ?? {};
	await c.projects.setSettings(projectKey, {
		...current,
		settings: {
			...settings,
			flowDisplaySettings: {
				...asRecord(settings.flowDisplaySettings,),
				zonesManualPositioning: manual,
			},
		},
	},);
}

function enableManualPositioningCommand(projectKey: string | undefined,): string {
	return `dss project settings-set --data '{"settings":{"flowDisplaySettings":{"zonesManualPositioning":true}}}'${
		projectKey ? ` --project-key ${projectKey}` : ""
	}`;
}

export const flowZoneCommands: Record<string, CommandMeta> = withUsage("flow-zone", {
	list: {
		handler: async (c, _a, f,) => {
			const zones = await c.flowZones.list(f["project-key"] as string | undefined,);
			if (f["summary"] !== true) return zones;
			const objects = flowZoneMoveItems(f,);
			const object = objects.length === 1 ? objects[0] : undefined;
			return zones.map((zone,) => flowZoneSummary(zone, object,));
		},
		description: "List flow zones in a project, optionally as compact summaries.",
		examples: ["dss flow-zone list", "dss flow-zone list --summary --object RECIPE:compute_orders",],
	},
	find: {
		handler: async (c, a, f,) => {
			const zones = await c.flowZones.list(f["project-key"] as string | undefined,);
			const objects = flowZoneMoveItems(f,);
			const query = a[0]?.trim();
			if (query && objects.length === 0) {
				const normalized = query.toLowerCase();
				return zones
					.filter((zone,) =>
						zone.id.toLowerCase().includes(normalized,)
						|| zone.name.toLowerCase().includes(normalized,)
					)
					.map((zone,) => flowZoneDetailSummary(zone,));
			}
			if (objects.length !== 1) {
				throw new UsageError(
					"Exactly one zone name/id or object is required. Use <name>, --object TYPE:ID, --dataset DS, or --recipe R.",
				);
			}
			const object = objects[0]!;
			return zones
				.filter((zone,) => flowZoneContains(zone, object,))
				.map((zone,) => flowZoneSummary(zone, object,));
		},
		description: "Find flow zones by name/id or by contained flow object.",
		examples: [
			"dss flow-zone find ATH_SNW_MAP_FRG49",
			"dss flow-zone find --object RECIPE:compute_orders",
			"dss flow-zone find --dataset orders",
		],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, commandUsage("flow-zone", "get",),);
			return c.flowZones.get(flowZoneId(a[0],), f["project-key"] as string | undefined,);
		},
		description: "Get a flow zone by id.",
		examples: ["dss flow-zone get ZONE_ID",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const name = flowZoneName(f["name"],);
			const payload = {
				name,
				color: flowZoneColor(f["color"],),
				projectKey: pk,
			};
			if (f["if-not-exists"] === true || executionMode(f,).dryRun) {
				const list = await c.flowZones.list(pk,);
				const existing = list.find((zone,) => zone.name === name);
				if (existing && f["if-not-exists"] === true && !executionMode(f,).dryRun) {
					return skipResult("flow-zone", existing.id, "exists", { current: existing, },);
				}
				if (executionMode(f,).dryRun) {
					return {
						dryRun: true,
						action: "create",
						resource: "flow-zone",
						name,
						payload,
						...(existing ? { current: existing, } : {}),
					};
				}
			}
			const created = await c.flowZones.create(payload,);
			return { created: created.id, resource: "flow-zone", ...created, };
		},
		description: "Create a flow zone.",
		examples: [
			"dss flow-zone create --name Exports",
			"dss flow-zone create --name Exports --color '#2ab1ac' --dry-run",
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("flow-zone", "update",),);
			if (typeof f["name"] !== "string" && typeof f["color"] !== "string") {
				throw new UsageError("--name and/or --color is required.",);
			}
			const zoneId = flowZoneId(a[0],);
			const pk = f["project-key"] as string | undefined;
			const patch = {
				name: typeof f["name"] === "string" ? flowZoneName(f["name"],) : undefined,
				color: flowZoneColor(f["color"],),
				projectKey: pk,
			};
			if (executionMode(f,).dryRun) {
				const current = await c.flowZones.get(zoneId, pk,);
				const next = deepMerge(current, patch,);
				return { dryRun: true, action: "update", resource: "flow-zone", id: zoneId, current, next, };
			}
			return c.flowZones.update(zoneId, patch,);
		},
		description: "Update flow zone settings.",
		examples: ["dss flow-zone update ZONE_ID --name Exports --color '#2ab1ac' --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, commandUsage("flow-zone", "delete",),);
			const zoneId = flowZoneId(a[0],);
			if (executionMode(f,).dryRun || f["if-exists"] === true) {
				const current = await readIfExists(() =>
					c.flowZones.get(zoneId, f["project-key"] as string | undefined,)
				);
				if (!current) return skipResult("flow-zone", zoneId, "missing",);
				if (executionMode(f,).dryRun) {
					return { dryRun: true, action: "delete", resource: "flow-zone", id: zoneId, current, };
				}
			}
			await c.flowZones.delete(zoneId, f["project-key"] as string | undefined,);
			return { deleted: zoneId, resource: "flow-zone", };
		},
		description: "Delete a flow zone. DSS moves zone items back to the default zone.",
		examples: [
			"dss flow-zone delete ZONE_ID --dry-run",
			"dss flow-zone delete ZONE_ID --if-exists",
		],
	},
	move: {
		handler: async (c, a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const zoneId = a[0] ? flowZoneId(a[0],) : await resolveFlowZoneIdFromFlags(c, f, pk,);
			if (!zoneId) {
				throw new UsageError(
					`A zone id or --zone/--zone-id is required. Usage: ${commandUsage("flow-zone", "move",)}`,
				);
			}
			const items = flowZoneMoveItems(f,);
			if (items.length === 0) {
				throw new UsageError(
					"At least one object is required. Use --dataset, --recipe, --folder, or --object TYPE:ID.",
				);
			}

			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "move",
					resource: "flow-zone",
					id: zoneId,
					items,
				};
			}
			return c.flowZones.moveItems(zoneId, items, pk,);
		},
		description:
			"Move datasets, recipes, managed folders, or other flow objects into a zone by id or --zone name.",
		examples: [
			"dss flow-zone move ZONE_ID --dataset orders --dry-run",
			"dss flow-zone move --zone ATH_SNW_MAP_FRG49 --dataset raw_orders,clean_orders --recipe prepare_orders",
			"dss flow-zone move ZONE_ID --folder FOLDER_ID",
			"dss flow-zone move ZONE_ID --object SAVED_MODEL:model_id",
		],
	},
	plan: {
		handler: async (c, _a, f,) => {
			const pk = f["project-key"] as string | undefined;
			const [zones, { graph, topologyFingerprint, },] = await Promise.all([
				c.flowZones.list(pk,),
				c.projects.flowTopology(pk,),
			],);
			return {
				...flowZonePlanFromZones(zones, topologyFingerprint,),
				unassigned: flowZoneUnassignedItems(graph, zones,),
			};
		},
		description:
			"Export current visual flow-zone organization as an organize-compatible plan. `unassigned` lists default-zone recipes and source objects still to place (recipe outputs follow their recipe); organize ignores it.",
		examples: ["dss flow-zone plan --project-key MYPROJ",],
	},
	organize: {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("flow-zone", "organize",);
			const pk = f["project-key"] as string | undefined;
			const parsed = readFlowZoneOrganizePlan(f, usage,);
			const sync = f["sync"] === true;
			const validateObjects = f["validate-objects"] === true;
			const recreateOnMove = f["recreate-on-move"] === true;
			const [zones, { graph, topologyFingerprint, },] = await Promise.all([
				c.flowZones.list(pk,),
				c.projects.flowTopology(pk,),
			],);
			if (parsed.topologyFingerprint && parsed.topologyFingerprint !== topologyFingerprint) {
				throw new ClientValidationError(
					"Flow topology changed after this visual organization plan was generated.",
					"assertion_failed",
					"Regenerate the plan with dss flow-zone plan, then review it before applying.",
					{ expected: parsed.topologyFingerprint, actual: topologyFingerprint, },
				);
			}
			const plan = applyFlowZoneUnits(parsed, graph, zones, pk,);
			const plannedItemKeys = flowZonePlanItemKeys(plan,);
			const targets = plan.zones.map((zonePlan,) => ({
				zonePlan,
				existing: findFlowZoneForPlan(zones, zonePlan,),
			}));
			const planned = targets.map(({ zonePlan, existing, },) =>
				flowZoneOrganizeStep(zonePlan, existing, sync, plannedItemKeys,)
			);
			const repositions = targets.flatMap(({ zonePlan, existing, },) => {
				const requestedPosition = existing && flowZoneRepositionTarget(zonePlan, existing,);
				return requestedPosition
					? [{ ...flowZoneSummary(existing,), position: existing.position, requestedPosition, },]
					: [];
			},);
			if (repositions.length > 0 && !recreateOnMove) {
				throw new ClientValidationError(
					`DSS cannot move ${repositions.length} existing flow zone(s): its API applies a zone position only when the zone is created.`,
					"validation_failed",
					"Drop position from those zones, or pass --recreate-on-move to replace each with a new zone at the requested position (items, shared items, and metadata carry over; the zone id changes, so update scenarios that build the zone by id).",
					{ zones: repositions, },
				);
			}
			const validation = validateObjects
				? await validateFlowZoneOrganizeObjects(c, plan, pk,)
				: undefined;
			if (validation) throwFlowZoneValidationError(validation,);
			const writesPositions = repositions.length > 0
				|| targets.some(({ zonePlan, existing, },) => !existing && zonePlan.position !== undefined);
			const positioning = writesPositions ? await readManualPositioning(c, pk,) : undefined;
			if (positioning?.error !== undefined) {
				enqueueCliWarning({
					code: "zone_manual_positioning_unverified",
					message:
						`Could not read project settings (${positioning.error}). DSS stores a zone position only when manual zone positioning is off at creation, and draws it only when it is on.`,
					hint: enableManualPositioningCommand(pk,),
				},);
			} else if (positioning?.manual === false) {
				enqueueCliWarning({
					code: "zone_manual_positioning_disabled",
					message:
						"This plan sets zone positions, but the project's manual zone positioning is off, so DSS lays zones out automatically and the positions do not show.",
					hint: enableManualPositioningCommand(pk,),
				},);
			}
			// With manual positioning on, DSS auto-places new zones and drops the
			// requested position, so it is switched off while zones are created.
			const suspendManualPositioning = positioning?.manual === true;
			const positionReport = positioning?.error === undefined && positioning
				? { manualPositioning: positioning.manual, suspendManualPositioning, }
				: {};
			const itemCount = plan.zones.reduce((count, zonePlan,) => count + zonePlan.items.length, 0,);
			const pruneItemCount = planned.reduce((count, step,) => {
				const pruneItems = Array.isArray(step.pruneItems,) ? step.pruneItems : [];
				return count + pruneItems.length;
			}, 0,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "organize",
					resource: "flow-zone",
					projectKey: pk,
					sync,
					validateObjects,
					recreateOnMove,
					topologyFingerprint,
					zoneCount: plan.zones.length,
					itemCount,
					pruneItemCount,
					...positionReport,
					...(validation ? { validation, } : {}),
					planned,
				};
			}

			const currentZones = [...zones,];
			const created: FlowZone[] = [];
			const updated: FlowZone[] = [];
			const recreated: Array<{ replacedZoneId: string; zone: FlowZone; }> = [];
			const moved: Array<{ zoneId: string; name: string; items: FlowZoneItemInput[]; }> = [];
			const pruned: Array<
				{ zoneId: "default"; fromZoneId: string; name: string; items: FlowZoneItemInput[]; }
			> = [];
			const positionsNotApplied: Array<
				{ zoneId: string; name: string; requested: FlowZonePosition; stored: unknown; }
			> = [];
			const checkPosition = (zone: FlowZone, requested: FlowZonePosition | undefined,) => {
				if (requested && !flowZoneSamePosition(flowZoneCurrentPosition(zone,), requested,)) {
					positionsNotApplied.push({
						zoneId: zone.id,
						name: zone.name,
						requested,
						stored: zone.position,
					},);
				}
			};

			let failure: unknown;
			if (suspendManualPositioning) await setManualPositioning(c, pk, false,);
			try {
				for (const zonePlan of plan.zones) {
					let zone = findFlowZoneForPlan(currentZones, zonePlan,);
					ensureFlowZonePlanTarget(zonePlan, zone,);
					const moveItems = flowZoneMoveDelta(zone, zonePlan.items,);
					const pruneItems = sync ? flowZonePruneItems(zone, plannedItemKeys,) : [];
					if (!zone) {
						zone = await c.flowZones.create({
							name: zonePlan.name!,
							color: zonePlan.color,
							position: zonePlan.position,
							projectKey: pk,
						},);
						currentZones.push(zone,);
						created.push(zone,);
						checkPosition(zone, zonePlan.position,);
					} else {
						const name = zonePlan.name && zonePlan.name !== zone.name ? zonePlan.name : undefined;
						const color = zonePlan.color && zonePlan.color !== zone.color ? zonePlan.color : undefined;
						const replacedId = zone.id;
						const position = flowZoneRepositionTarget(zonePlan, zone,);
						if (position) {
							const result = await c.flowZones.recreate(replacedId, {
								position,
								name,
								color,
								projectKey: pk,
							},);
							zone = result.zone;
							recreated.push(result,);
							checkPosition(zone, position,);
						} else if (name !== undefined || color !== undefined) {
							zone = await c.flowZones.update(replacedId, { name, color, projectKey: pk, },);
							updated.push(zone,);
						}
						const index = currentZones.findIndex((candidate,) => candidate.id === replacedId);
						if (index !== -1) currentZones[index] = zone;
					}

					if (moveItems.length > 0) {
						await c.flowZones.moveItems(zone.id, moveItems, pk,);
						moved.push({ zoneId: zone.id, name: zone.name, items: moveItems, },);
					}
					if (pruneItems.length > 0) {
						await c.flowZones.moveItems("default", pruneItems, pk,);
						pruned.push({ zoneId: "default", fromZoneId: zone.id, name: zone.name, items: pruneItems, },);
					}
				}
			} catch (error) {
				failure = error;
			}
			if (suspendManualPositioning) {
				try {
					await setManualPositioning(c, pk, true,);
				} catch (restoreError) {
					throw new ClientValidationError(
						"Manual zone positioning was switched off to place zones and could not be switched back on.",
						"ambiguous_outcome",
						enableManualPositioningCommand(pk,),
						failure instanceof Error ? { organizeError: failure.message, } : undefined,
						{ cause: restoreError, },
					);
				}
			}
			if (failure !== undefined) throw failure;

			const topologyFingerprintAfter = await c.projects.topologyFingerprint(pk,);
			if (topologyFingerprintAfter !== topologyFingerprint) {
				throw new ClientValidationError(
					"Visual flow-zone changes modified flow topology.",
					"assertion_failed",
					"Inspect the flow before applying any further organization changes.",
					{ before: topologyFingerprint, after: topologyFingerprintAfter, },
				);
			}
			if (positionsNotApplied.length > 0) {
				throw new ClientValidationError(
					`DSS did not store the requested position for ${positionsNotApplied.length} flow zone(s).`,
					"assertion_failed",
					"Items were organized; inspect the zones with dss flow-zone list.",
					{
						positionsNotApplied,
						created: created.map((zone,) => zone.id),
						recreated: recreated.map(({ replacedZoneId, zone, },) => ({
							replacedZoneId,
							zoneId: zone.id,
						})),
					},
				);
			}

			return {
				organized: true,
				action: "organize",
				resource: "flow-zone",
				projectKey: pk,
				sync,
				validateObjects,
				recreateOnMove,
				topologyFingerprint,
				topologyUnchanged: true,
				zoneCount: plan.zones.length,
				itemCount,
				pruneItemCount,
				...positionReport,
				created,
				updated,
				recreated,
				moved,
				pruned,
			};
		},
		description:
			"Create/update flow zones and move objects from a declarative plan. A recipe's outputs share its zone: listing either places both (impliedItems); splitting them is rejected. DSS stores a zone position only at creation: moving an existing zone needs --recreate-on-move (new id); manual zone positioning is switched off while zones are placed, then restored.",
		examples: [
			"dss flow-zone organize --file flow-zones.json --dry-run",
			`dss flow-zone organize --data '{"zones":[{"name":"Raw","color":"#64748b","datasets":["raw_orders"]}]}'`,
			"dss flow-zone organize --file flow-zones.json --recreate-on-move --dry-run",
		],
		payloadSchema: {
			stdin: true,
			dataFlag: true,
			dataFileFlag: true,
			jsonShape: "object",
			jsonSchema: FLOW_ZONE_ORGANIZE_PLAN_SCHEMA,
		},
	},
	graph: {
		handler: (c, a, f,) => {
			const id = a[0] === undefined ? undefined : flowZoneId(a[0],);
			return c.flowZones.graph(id, f["project-key"] as string | undefined,);
		},
		description: "Get the full flow graph or the graph for a single flow zone.",
		examples: ["dss flow-zone graph", "dss flow-zone graph ZONE_ID",],
	},
},);
