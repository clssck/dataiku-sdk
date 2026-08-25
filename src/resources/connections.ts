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
export async function resolveAdminManagedStorageConnection(
	client: DataikuClient,
	capability: "allowManagedDatasets" | "allowManagedFolders",
	preferredName?: string,
): Promise<string | undefined> {
	try {
		const raw = await client.get<unknown>("/public/api/admin/connections/",);
		if (!raw || typeof raw !== "object" || Array.isArray(raw,)) return undefined;

		const candidates: string[] = [];
		for (const [name, value,] of Object.entries(raw,)) {
			if (!value || typeof value !== "object" || Array.isArray(value,)) continue;
			const details = value as Record<string, unknown>;
			if (details["allowWrite"] === true && details[capability] === true) {
				candidates.push(name,);
			}
		}
		candidates.sort();
		return preferredName && candidates.includes(preferredName,) ? preferredName : candidates[0];
	} catch {
		return undefined;
	}
}

export interface ConnectionSchemaListOptions {
	connection: string;
	projectKey?: string;
}

export interface ConnectionTableListOptions extends ConnectionSchemaListOptions {
	catalog?: string;
	schema?: string;
}

async function inferRichConnections(
	client: DataikuClient,
	projectEnc: string,
): Promise<ConnectionSummary[]> {
	const datasetsRaw = await client.get<unknown>(`/public/api/projects/${projectEnc}/datasets/`,);
	const datasets = Array.isArray(datasetsRaw,) ? datasetsRaw : [];
	const map = new Map<string, { types: Set<string>; managed: boolean; dbSchemas: Set<string>; }>();

	const recordConnection = (
		connection: unknown,
		type: unknown,
		managed: boolean,
		schema?: unknown,
	): void => {
		if (typeof connection !== "string" || connection.length === 0) return;
		let entry = map.get(connection,);
		if (!entry) {
			entry = { types: new Set(), managed: false, dbSchemas: new Set(), };
			map.set(connection, entry,);
		}
		if (typeof type === "string" && type.length > 0) entry.types.add(type,);
		if (managed) entry.managed = true;
		if (typeof schema === "string" && schema.length > 0) entry.dbSchemas.add(schema,);
	};

	for (const dataset of datasets) {
		if (!dataset || typeof dataset !== "object" || Array.isArray(dataset,)) continue;
		const details = dataset as Record<string, unknown>;
		const params = details["params"];
		if (!params || typeof params !== "object" || Array.isArray(params,)) continue;
		const datasetParams = params as Record<string, unknown>;
		recordConnection(
			datasetParams["connection"],
			details["type"],
			details["managed"] === true,
			datasetParams["schema"],
		);
		recordConnection(datasetParams["uploadConnection"], details["type"], false,);
	}

	try {
		const foldersRaw = await client.get<unknown>(
			`/public/api/projects/${projectEnc}/managedfolders/`,
		);
		const folders = Array.isArray(foldersRaw,) ? foldersRaw : [];
		const folderDetails = await Promise.all(
			folders.map(async (folder,) => {
				if (!folder || typeof folder !== "object" || Array.isArray(folder,)) return folder;
				const summary = folder as Record<string, unknown>;
				const params = summary["params"];
				if (params && typeof params === "object" && !Array.isArray(params,)) return summary;
				const id = summary["id"];
				if (typeof id !== "string" || id.length === 0) return summary;
				try {
					return await client.get<unknown>(
						`/public/api/projects/${projectEnc}/managedfolders/${encodeURIComponent(id,)}`,
					);
				} catch {
					return summary;
				}
			},),
		);
		for (const folder of folderDetails) {
			if (!folder || typeof folder !== "object" || Array.isArray(folder,)) continue;
			const details = folder as Record<string, unknown>;
			const params = details["params"];
			if (!params || typeof params !== "object" || Array.isArray(params,)) continue;
			recordConnection(
				(params as Record<string, unknown>)["connection"],
				details["type"],
				true,
			);
		}
	} catch {
		// Dataset-derived inference remains useful when managed folders are inaccessible.
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
		const type = opts?.type?.trim() || "all";
		const raw = await this.client.get<unknown>(
			`/public/api/connections/get-names/?type=${encodeURIComponent(type,)}`,
		);
		return normalizeConnectionNames(raw,);
	}

	/**
	 * Infers available connections.
	 *
	 * - fast (default): fetches the connection name list and maps to ConnectionSummary.
	 *   Falls back to rich mode on any failure or empty result set.
	 * - rich: inspects project datasets and managed folders to derive connection metadata
	 *   (types, managed flag, db schemas).
	 */
	async infer(opts?: {
		mode?: "fast" | "rich";
		projectKey?: string;
	},): Promise<ConnectionSummary[]> {
		const mode = opts?.mode ?? "fast";
		const projectEnc = this.enc(opts?.projectKey,);

		if (mode === "rich") {
			return inferRichConnections(this.client, projectEnc,);
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

		return inferRichConnections(this.client, projectEnc,);
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
