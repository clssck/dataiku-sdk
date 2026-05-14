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

export interface ConnectionSchemaListOptions {
	connection: string;
	projectKey?: string;
}

export interface ConnectionTableListOptions extends ConnectionSchemaListOptions {
	catalog?: string;
	schema?: string;
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
	async list(opts?: { type?: string; },): Promise<string[]> {
		const type = opts?.type?.trim();
		const query = type ? `?type=${encodeURIComponent(type,)}` : "";
		const raw = await this.client.get<unknown>(`/public/api/connections/get-names/${query}`,);
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

	async schemas(opts: ConnectionSchemaListOptions,): Promise<string[]> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		params.set("connectionName", opts.connection,);
		return this.client.get<string[]>(
			`/public/api/projects/${
				encodeURIComponent(pk,)
			}/datasets/tables-import/actions/list-schemas?${params.toString()}`,
		);
	}

	async tables(opts: ConnectionTableListOptions,): Promise<Record<string, unknown>> {
		const pk = this.resolveProjectKey(opts.projectKey,);
		const params = new URLSearchParams();
		params.set("connectionName", opts.connection,);
		if (opts.catalog !== undefined) params.set("catalogName", opts.catalog,);
		if (opts.schema !== undefined) params.set("schemaName", opts.schema,);
		return this.client.get<Record<string, unknown>>(
			`/public/api/projects/${
				encodeURIComponent(pk,)
			}/datasets/tables-import/actions/list-tables?${params.toString()}`,
		);
	}
}
