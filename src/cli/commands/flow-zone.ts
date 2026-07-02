import type { FlowZoneItemInput, } from "../../resources/flow-zones.js";
import type { FlowZone, } from "../../schemas.js";
import { deepMerge, } from "../../utils/deep-merge.js";
import {
	ensureFlowZonePlanTarget,
	findFlowZoneForPlan,
	flowZoneColor,
	flowZoneContains,
	flowZoneCurrentPosition,
	flowZoneDetailSummary,
	flowZoneId,
	flowZoneMoveItems,
	flowZoneName,
	flowZoneOrganizeStep,
	flowZonePlanItemKeys,
	flowZonePruneItems,
	flowZoneSamePosition,
	flowZoneSummary,
	readFlowZoneOrganizePlan,
	resolveFlowZoneIdFromFlags,
	throwFlowZoneValidationError,
	validateFlowZoneOrganizeObjects,
} from "../helpers/flow-zone.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

export const flowZoneCommands: Record<string, CommandMeta> = {
	list: {
		handler: async (c, _a, f,) => {
			const zones = await c.flowZones.list(f["project-key"] as string | undefined,);
			if (f["summary"] !== true) return zones;
			const objects = flowZoneMoveItems(f,);
			const object = objects.length === 1 ? objects[0] : undefined;
			return zones.map((zone,) => flowZoneSummary(zone, object,));
		},
		usage: "dss flow-zone list [--summary] [--object TYPE:ID] [--project-key KEY]",
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
		usage:
			"dss flow-zone find [name-or-id] [--object TYPE:ID | --dataset DS | --recipe R | --folder F] [--project-key KEY]",
		description: "Find flow zones by name/id or by contained flow object.",
		examples: [
			"dss flow-zone find ATH_SNW_MAP_FRG49",
			"dss flow-zone find --object RECIPE:compute_orders",
			"dss flow-zone find --dataset orders",
		],
	},
	get: {
		handler: (c, a, f,) => {
			requireArgs(a, 1, "dss flow-zone get <id>",);
			return c.flowZones.get(flowZoneId(a[0],), f["project-key"] as string | undefined,);
		},
		usage: "dss flow-zone get <id> [--project-key KEY]",
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
			if (f["if-not-exists"] === true || f["dry-run"] === true) {
				const list = await c.flowZones.list(pk,);
				const existing = list.find((zone,) => zone.name === name);
				if (existing && f["if-not-exists"] === true && f["dry-run"] !== true) {
					return skipResult("flow-zone", existing.id, "exists", { current: existing, },);
				}
				if (f["dry-run"] === true) {
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
		usage:
			"dss flow-zone create --name NAME [--color #RRGGBB] [--if-not-exists] [--dry-run] [--project-key KEY]",
		description: "Create a flow zone.",
		examples: [
			"dss flow-zone create --name Exports",
			"dss flow-zone create --name Exports --color '#2ab1ac' --dry-run",
		],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss flow-zone update <id> [--name NAME] [--color #RRGGBB]",);
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
			if (f["dry-run"] === true) {
				const current = await c.flowZones.get(zoneId, pk,);
				const next = deepMerge(current as unknown as Record<string, unknown>, patch,);
				return { dryRun: true, action: "update", resource: "flow-zone", id: zoneId, current, next, };
			}
			return c.flowZones.update(zoneId, patch,);
		},
		usage:
			"dss flow-zone update <id> [--name NAME] [--color #RRGGBB] [--dry-run] [--project-key KEY]",
		description: "Update flow zone settings.",
		examples: ["dss flow-zone update ZONE_ID --name Exports --color '#2ab1ac' --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss flow-zone delete <id>",);
			const zoneId = flowZoneId(a[0],);
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() =>
					c.flowZones.get(zoneId, f["project-key"] as string | undefined,)
				);
				if (!current) return skipResult("flow-zone", zoneId, "missing",);
				if (f["dry-run"] === true) {
					return { dryRun: true, action: "delete", resource: "flow-zone", id: zoneId, current, };
				}
			}
			await c.flowZones.delete(zoneId, f["project-key"] as string | undefined,);
			return { deleted: zoneId, resource: "flow-zone", };
		},
		usage: "dss flow-zone delete <id> [--if-exists] [--dry-run] [--project-key KEY]",
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
					"A zone id or --zone/--zone-id is required. Usage: dss flow-zone move <id> [--dataset DS] [--recipe R] [--folder F] [--object TYPE:ID]",
				);
			}
			const items = flowZoneMoveItems(f,);
			if (items.length === 0) {
				throw new UsageError(
					"At least one object is required. Use --dataset, --recipe, --folder, or --object TYPE:ID.",
				);
			}

			if (f["dry-run"] === true) {
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
		usage:
			"dss flow-zone move [id] [--zone ZONE|--zone-id ID] [--dataset DS[,DS2]] [--recipe R] [--folder F] [--object TYPE:ID] [--dry-run] [--project-key KEY]",
		description:
			"Move datasets, recipes, managed folders, or other flow objects into a zone by id or --zone name.",
		examples: [
			"dss flow-zone move ZONE_ID --dataset orders --dry-run",
			"dss flow-zone move --zone ATH_SNW_MAP_FRG49 --dataset raw_orders,clean_orders --recipe prepare_orders",
			"dss flow-zone move ZONE_ID --folder FOLDER_ID",
			"dss flow-zone move ZONE_ID --object SAVED_MODEL:model_id",
		],
	},
	organize: {
		handler: async (c, _a, f,) => {
			const usage =
				"dss flow-zone organize (--data JSON|--data-file PATH|--file PATH|--stdin) [--sync] [--validate-objects] [--dry-run] [--project-key KEY]";
			const pk = f["project-key"] as string | undefined;
			const plan = readFlowZoneOrganizePlan(f, usage,);
			const sync = f["sync"] === true;
			const validateObjects = f["validate-objects"] === true;
			const zones = await c.flowZones.list(pk,);
			const plannedItemKeys = flowZonePlanItemKeys(plan,);
			const planned = plan.zones.map((zonePlan,) =>
				flowZoneOrganizeStep(zonePlan, findFlowZoneForPlan(zones, zonePlan,), sync, plannedItemKeys,)
			);
			const validation = validateObjects
				? await validateFlowZoneOrganizeObjects(c, plan, pk,)
				: undefined;
			if (validation) throwFlowZoneValidationError(validation,);
			const itemCount = plan.zones.reduce((count, zonePlan,) => count + zonePlan.items.length, 0,);
			const pruneItemCount = planned.reduce((count, step,) => {
				const pruneItems = Array.isArray(step.pruneItems,) ? step.pruneItems : [];
				return count + pruneItems.length;
			}, 0,);
			if (f["dry-run"] === true) {
				return {
					dryRun: true,
					action: "organize",
					resource: "flow-zone",
					projectKey: pk,
					sync,
					validateObjects,
					zoneCount: plan.zones.length,
					itemCount,
					pruneItemCount,
					...(validation ? { validation, } : {}),
					planned,
				};
			}

			const currentZones = [...zones,];
			const created: FlowZone[] = [];
			const updated: FlowZone[] = [];
			const moved: Array<{ zoneId: string; name: string; items: FlowZoneItemInput[]; }> = [];
			const pruned: Array<
				{ zoneId: "default"; fromZoneId: string; name: string; items: FlowZoneItemInput[]; }
			> = [];

			for (const zonePlan of plan.zones) {
				let zone = findFlowZoneForPlan(currentZones, zonePlan,);
				ensureFlowZonePlanTarget(zonePlan, zone,);
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
				} else {
					const patch = {
						...(zonePlan.name && zonePlan.name !== zone.name ? { name: zonePlan.name, } : {}),
						...(zonePlan.color && zonePlan.color !== zone.color ? { color: zonePlan.color, } : {}),
						...(zonePlan.position !== undefined
								&& !flowZoneSamePosition(flowZoneCurrentPosition(zone,), zonePlan.position,)
							? { position: zonePlan.position, }
							: {}),
						projectKey: pk,
					};
					if (patch.name !== undefined || patch.color !== undefined || patch.position !== undefined) {
						zone = await c.flowZones.update(zone.id, patch,);
						const index = currentZones.findIndex((candidate,) => candidate.id === zone!.id);
						if (index !== -1) currentZones[index] = zone;
						updated.push(zone,);
					}
				}

				if (zonePlan.items.length > 0) {
					await c.flowZones.moveItems(zone.id, zonePlan.items, pk,);
					moved.push({ zoneId: zone.id, name: zone.name, items: zonePlan.items, },);
				}
				if (pruneItems.length > 0) {
					await c.flowZones.moveItems("default", pruneItems, pk,);
					pruned.push({ zoneId: "default", fromZoneId: zone.id, name: zone.name, items: pruneItems, },);
				}
			}

			return {
				organized: true,
				action: "organize",
				resource: "flow-zone",
				projectKey: pk,
				sync,
				validateObjects,
				zoneCount: plan.zones.length,
				itemCount,
				pruneItemCount,
				created,
				updated,
				moved,
				pruned,
			};
		},
		usage:
			"dss flow-zone organize (--data JSON|--data-file PATH|--file PATH|--stdin) [--sync] [--validate-objects] [--dry-run] [--project-key KEY]",
		description:
			"Create/update flow zones and move objects from a declarative visual organization plan.",
		examples: [
			"dss flow-zone organize --file flow-zones.json --dry-run",
			`dss flow-zone organize --data '{"zones":[{"name":"Raw","color":"#64748b","datasets":["raw_orders"]}]}'`,
		],
	},
	graph: {
		handler: (c, a, f,) => {
			const id = a[0] === undefined ? undefined : flowZoneId(a[0],);
			return c.flowZones.graph(id, f["project-key"] as string | undefined,);
		},
		usage: "dss flow-zone graph [<id>] [--project-key KEY]",
		description: "Get the full flow graph or the graph for a single flow zone.",
		examples: ["dss flow-zone graph", "dss flow-zone graph ZONE_ID",],
	},
};
