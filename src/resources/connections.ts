import type { DataikuClient, } from "../client.js";
import type { ConnectionSummary, } from "../schemas.js";
import { BaseResource, } from "./base.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalizeConnectionNames(value: unknown,): string[] {
	if (!Array.isArray(value,)) return [];
	return (value as unknown[])
		.filter((v,): v is string => typeof v === "string" && v.length > 0)
		.sort();
}

async function inferRichConnectionsFromDatasets(
	client: DataikuClient,
	projectEnc: string,
): Promise<ConnectionSummary[]> {
	const datasets = await client.get<unknown[]>(`/public/api/projects/${projectEnc}/datasets/`,);

	const map = new Map<string, { types: Set<string>; managed: boolean; dbSchemas: Set<string>; }>();

	for (const ds of datasets) {
		const d = ds as Record<string, unknown>;
		const params = d["params"] as Record<string, unknown> | undefined;
		const connection = params?.["connection"];
		if (typeof connection !== "string" || connection.length === 0) continue;

		if (!map.has(connection,)) {
			map.set(connection, { types: new Set(), managed: false, dbSchemas: new Set(), },);
		}
		const entry = map.get(connection,)!;

		const dsType = d["type"];
		if (typeof dsType === "string" && dsType.length > 0) entry.types.add(dsType,);

		if (d["managed"] === true) entry.managed = true;

		const schema = params?.["schema"];
		if (typeof schema === "string" && schema.length > 0) entry.dbSchemas.add(schema,);
	}

	return [...map.entries(),]
		.sort(([a,], [b,],) => a.localeCompare(b,))
		.map(([name, { types, managed, dbSchemas, },],) => ({
			name,
			types: [...types,].sort(),
			managed,
			dbSchemas: [...dbSchemas,].sort(),
		}));
}

// ---------------------------------------------------------------------------
// Resource
// ---------------------------------------------------------------------------

export class ConnectionsResource extends BaseResource {
	/**
	 * Returns sorted list of all connection names visible to the current user.
	 */
	async list(): Promise<string[]> {
		const raw = await this.client.get<unknown>(`/public/api/connections/get-names/`,);
		return normalizeConnectionNames(raw,);
	}

	/**
	 * Infers available connections.
	 *
	 * - fast (default): fetches the connection name list and maps to ConnectionSummary.
	 *   Falls back to rich mode on any failure or empty result set.
	 * - rich: inspects project datasets to derive connection metadata
	 *   (types, managed flag, db schemas).
	 */
	async infer(opts?: {
		mode?: "fast" | "rich";
		projectKey?: string;
	},): Promise<ConnectionSummary[]> {
		const mode = opts?.mode ?? "fast";
		const projectEnc = this.enc(opts?.projectKey,);

		if (mode === "rich") {
			return inferRichConnectionsFromDatasets(this.client, projectEnc,);
		}

		// fast — attempt name list, fall back to rich on any error or empty result
		try {
			const names = await this.list();
			if (names.length > 0) {
				return names.map((name,) => ({ name, }));
			}
		} catch {
			// Fall through to rich inference.
		}

		return inferRichConnectionsFromDatasets(this.client, projectEnc,);
	}
}
