import { jsonInput, requiredJsonInput, } from "../coerce.js";
import { planResult, readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, } from "../usage.js";

const MEANING_PLAN_FAILURE_CODES = { usage: 1, error: 2, transient: 3, };

function meaningEndpoint(meaningId: string,): string {
	return `/public/api/meanings/${encodeURIComponent(meaningId,)}`;
}

function meaningCreatePayload(
	id: string,
	label: string,
	type: string,
	body: Record<string, unknown>,
): Record<string, unknown> {
	return {
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
	};
}

export const meaningCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c,) => c.meanings.list(),
		usage: "dss meaning list",
		description: "List user-defined meanings (column semantic types) on the instance.",
		examples: ["dss meaning list",],
	},
	get: {
		handler: (c, a,) => {
			requireArgs(a, 1, "dss meaning get <meaningId>",);
			return c.meanings.get(a[0],);
		},
		usage: "dss meaning get <meaningId>",
		description: "Get a user-defined meaning definition.",
		examples: ["dss meaning get customer_type",],
	},
	create: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				3,
				"dss meaning create <id> <label> <type> [--data JSON|--data-file PATH|--stdin] [--dry-run]",
			);
			const body = jsonInput(f,) ?? {};
			const payload = meaningCreatePayload(a[0], a[1], a[2], body,);
			if (f["dry-run"] === true) {
				return planResult("meaning", "create", {
					asyncKind: "none",
					method: "POST",
					endpoint: "/public/api/meanings/",
					identifiers: { id: a[0], },
					payload,
					idempotency: "none",
					exitCodesOnFailure: MEANING_PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.meanings.create(a[0], a[1], a[2], body,);
		},
		usage:
			"dss meaning create <id> <label> <type> [--data JSON|--data-file PATH|--stdin] [--dry-run]",
		description: "Create a user-defined meaning (type e.g. VALUES_LIST, VALUES_MAPPING, PATTERN).",
		examples: ["dss meaning create vip VIP VALUES_LIST --dry-run",],
	},
	update: {
		handler: async (c, a, f,) => {
			requireArgs(
				a,
				1,
				"dss meaning update <meaningId> (--data JSON|--data-file PATH|--stdin) [--dry-run]",
			);
			const body = requiredJsonInput(
				f,
				"--data, --data-file, or --stdin is required (meaning definition).",
			);
			if (f["dry-run"] === true) {
				return planResult("meaning", "update", {
					asyncKind: "none",
					method: "PUT",
					endpoint: meaningEndpoint(a[0],),
					identifiers: { id: a[0], },
					payload: body,
					idempotency: "none",
					exitCodesOnFailure: MEANING_PLAN_FAILURE_CODES,
					plannedAndDryRun: true,
				},);
			}
			return c.meanings.update(a[0], body,);
		},
		usage: "dss meaning update <meaningId> (--data JSON|--data-file PATH|--stdin) [--dry-run]",
		description: "Replace a user-defined meaning definition.",
		examples: ["dss meaning update vip --data-file meaning.json --dry-run",],
	},
	delete: {
		handler: async (c, a, f,) => {
			requireArgs(a, 1, "dss meaning delete <meaningId>",);
			if (f["dry-run"] === true || f["if-exists"] === true) {
				const current = await readIfExists(() => c.meanings.get(a[0],));
				if (!current) return skipResult("meaning", a[0], "missing",);
				if (f["dry-run"] === true) {
					return planResult("meaning", "delete", {
						asyncKind: "none",
						method: "DELETE",
						endpoint: meaningEndpoint(a[0],),
						identifiers: { id: a[0], },
						idempotency: "if-exists",
						exitCodesOnFailure: MEANING_PLAN_FAILURE_CODES,
						plannedAndDryRun: true,
					},);
				}
			}
			await c.meanings.delete(a[0],);
			return { deleted: a[0], resource: "meaning", };
		},
		usage: "dss meaning delete <meaningId> [--if-exists] [--dry-run]",
		description: "Delete a user-defined meaning definition.",
		examples: ["dss meaning delete vip --dry-run",],
	},
};
