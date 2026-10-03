import { num, } from "./coerce.js";
import { COMPACT_LIST_FIELDS, type CompactListField, } from "./list-shapes.js";
import { enqueueCliWarning, } from "./output.js";

type Item = Record<string, unknown>;

function record(value: unknown,): Item {
	return value && typeof value === "object" && !Array.isArray(value,) ? value as Item : {};
}

function compactValue(item: Item, spec: CompactListField,): unknown {
	let value: unknown = item;
	for (const key of spec.path.split(".",)) value = record(value,)[key];
	if (spec.roleRefs !== true) return value;
	// Dataset refs across every role of recipe inputs/outputs (`{main:{items:[{ref}]}}`).
	return Object.values(record(value,),).flatMap((role,) => {
		const items = record(role,).items;
		return Array.isArray(items,)
			? items.flatMap((entry,) => {
				const ref = record(entry,).ref;
				return typeof ref === "string" ? [ref,] : [];
			},)
			: [];
	},);
}

/** Identifier fields `--contains` matches; string items match themselves. */
const IDENTIFIER_FIELDS = [
	"id",
	"name",
	"projectKey",
	"envName",
	"login",
	"displayName",
	"label",
	"runnableType",
] as const;

/** `job list` filters by --contains/--limit itself (job name, state, outputs). */
const LISTS_WITH_OWN_FILTERS: Record<string, true> = { job: true, };

function itemMatches(item: unknown, needle: string,): boolean {
	if (typeof item === "string") return item.toLowerCase().includes(needle,);
	const fields = record(item,);
	return IDENTIFIER_FIELDS.some((field,) => {
		const value = fields[field];
		return typeof value === "string" && value.toLowerCase().includes(needle,);
	},);
}

/**
 * Shape a `*.list` result for agents: `--contains` filters on identifier
 * fields, `--limit` caps the count (a `list_truncated` warning reports the
 * total), and large DSS objects compact to their decision fields unless
 * `--full` or `--fields` asks for the DSS objects.
 */
export function shapeListResult(
	resource: string,
	result: unknown,
	flags: Record<string, string | boolean>,
): unknown {
	if (!Array.isArray(result,)) return result;
	let items: unknown[] = result;
	if (LISTS_WITH_OWN_FILTERS[resource] !== true) {
		const contains = flags["contains"];
		if (typeof contains === "string" && contains.length > 0) {
			const needle = contains.toLowerCase();
			items = items.filter((item,) => itemMatches(item, needle,));
		}
		const limit = num(flags["limit"], "--limit",);
		if (limit !== undefined && items.length > limit) {
			enqueueCliWarning({
				code: "list_truncated",
				shown: limit,
				total: items.length,
				hint: "Raise --limit or narrow with --contains.",
			},);
			items = items.slice(0, Math.max(0, limit,),);
		}
	}
	const fields = COMPACT_LIST_FIELDS[resource];
	if (!fields || flags["full"] === true || typeof flags["fields"] === "string") return items;
	return items.map((item,) =>
		Object.fromEntries(
			Object.entries(fields,).map(([name, spec,],) => [name, compactValue(record(item,), spec,),]),
		)
	);
}
