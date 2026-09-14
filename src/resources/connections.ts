import type { DataikuClient, } from "../client.js";
import { ClientValidationError, } from "../errors.js";
import { type ConnectionSummary, type FutureState, FutureStateSchema, } from "../schemas.js";
import { BaseResource, requireNonEmpty, } from "./base.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalizeConnectionNames(value: unknown,): string[] {
	if (!Array.isArray(value,)) return [];
	return (value as unknown[])
		.filter((v,): v is string => typeof v === "string" && v.length > 0)
		.sort();
}

/**
 * Connection definition for admin create/update. `name` and `type` are
 * required on create; params are connection-type specific per the docs and may
 * carry credentials (e.g. `params.password`), which this SDK forwards verbatim
 * over TLS and never logs.
 */
export interface ConnectionAdminInput extends Record<string, unknown> {
	name?: string;
	type?: string;
	params?: Record<string, unknown>;
	allowWrite?: boolean;
	allowManagedDatasets?: boolean;
	usableBy?: string;
	allowedGroups?: string[];
}

function validateConnectionAdminInput(
	body: ConnectionAdminInput,
	method: string,
): ConnectionAdminInput {
	if (body === null || body === undefined || typeof body !== "object" || Array.isArray(body,)) {
		throw new ClientValidationError(`${method}: body must be a Connection object.`,);
	}
	return body;
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

	/* ----------------------------------------------------------------- */
	/*  Admin connection management (GET/POST/PUT/DELETE /admin/connections) */
	/* ----------------------------------------------------------------- */

	/**
	 * Lists all connections on the DSS instance (Admin required). Returns the
	 * documented dictionary of connection name to Connection object. Connection
	 * params may contain credentials (e.g. `params.password`); the SDK returns
	 * the server payload faithfully, and CLI output/plan layers redact them.
	 */
	async adminList(): Promise<Record<string, Record<string, unknown>>> {
		const raw = await this.client.get<unknown>("/public/api/admin/connections",);
		if (raw === undefined || raw === null || typeof raw !== "object" || Array.isArray(raw,)) {
			return {};
		}
		return raw as Record<string, Record<string, unknown>>;
	}

	/** Gets one connection by name (Admin required). */
	async adminGet(connectionName: string,): Promise<Record<string, unknown>> {
		const enc = requireNonEmpty(connectionName, "connectionName",).trim();
		return this.client.get<Record<string, unknown>>(
			`/public/api/admin/connections/${encodeURIComponent(enc,)}`,
		);
	}

	/**
	 * Creates a connection (Admin required). Body is the Connection definition
	 * (`{name, type, params, ...}`); params are connection-type specific per
	 * the docs. DSS answers 200 with an empty body.
	 */
	async adminCreate(body: ConnectionAdminInput,): Promise<Record<string, unknown>> {
		const validated = validateConnectionAdminInput(
			body,
			"connections.adminCreate",
		);
		const raw = await this.client.post<unknown>(
			"/public/api/admin/connections",
			validated,
		);
		return raw !== undefined && raw !== null && typeof raw === "object" && !Array.isArray(raw,)
			? raw as Record<string, unknown>
			: {};
	}

	/**
	 * Updates a connection (Admin required). Per the docs the body MUST have
	 * been obtained from a prior GET at the same URL; `type` and `name` may not
	 * be modified and undocumented attributes should pass through unchanged.
	 * DSS answers 204 with an empty body.
	 */
	async adminUpdate(
		connectionName: string,
		body: ConnectionAdminInput,
	): Promise<Record<string, unknown>> {
		const enc = requireNonEmpty(connectionName, "connectionName",).trim();
		const validated = validateConnectionAdminInput(
			body,
			"connections.adminUpdate",
		);
		const raw = await this.client.put<unknown>(
			`/public/api/admin/connections/${encodeURIComponent(enc,)}`,
			validated,
		);
		return raw !== undefined && raw !== null && typeof raw === "object" && !Array.isArray(raw,)
			? raw as Record<string, unknown>
			: {};
	}

	/**
	 * Deletes a connection (Admin required). Per the docs DSS performs no check
	 * that the connection is not in use by a dataset, so this is destructive.
	 */
	async adminDelete(connectionName: string,): Promise<{ deleted: string; }> {
		const enc = requireNonEmpty(connectionName, "connectionName",).trim();
		await this.client.del(`/public/api/admin/connections/${encodeURIComponent(enc,)}`,);
		return { deleted: enc, };
	}

	/**
	 * Tests whether a connection is available. Routed like the official
	 * dataikuapi Python client (`DSSConnection.test()`):
	 * `GET /connections/{connectionName}/test` — a pure availability test, not
	 * a settings read. Returns an error when testing is not supported for the
	 * connection type; the result carries `connectionOK` (true when available).
	 * The REST reference documents a same-URL "Test connection" variant of
	 * `GET /admin/connections/{name}`; this route is preferred because it
	 * cannot be confused with a settings retrieval.
	 */
	async adminTest(connectionName: string,): Promise<Record<string, unknown>> {
		const enc = requireNonEmpty(connectionName, "connectionName",).trim();
		const raw = await this.client.get<unknown>(
			`/public/api/connections/${encodeURIComponent(enc,)}/test`,
		);
		return raw !== undefined && raw !== null && typeof raw === "object" && !Array.isArray(raw,)
			? raw as Record<string, unknown>
			: {};
	}

	/* ----------------------------------------------------------------- */
	/*  Tables import (prepare-from-keys / execute-from-candidates)      */
	/* ----------------------------------------------------------------- */

	/**
	 * Prepares the import of selected SQL or Hive tables (WRITE_CONF required).
	 * `keys` entries carry `{connectionName, name}` plus optional
	 * `catalog`/`schema`; Hive keys use the `@virtual(hive-jdbc):...`
	 * connection-name form documented by DSS. Returns a future reference; poll
	 * with `client.futures` when the result is not inline.
	 */
	async prepareTablesImport(
		opts: {
			keys: Array<Record<string, unknown>>;
			projectKey?: string;
		},
	): Promise<FutureState> {
		if (
			!Array.isArray(opts?.keys,) || opts.keys.length === 0
			|| opts.keys.some((k,) => k === null || typeof k !== "object" || Array.isArray(k,))
		) {
			throw new ClientValidationError(
				"connections.prepareTablesImport: keys must be a non-empty array of key objects.",
			);
		}
		for (const key of opts.keys) {
			const connectionName = (key as Record<string, unknown>)["connectionName"];
			const name = (key as Record<string, unknown>)["name"];
			if (
				typeof connectionName !== "string" || connectionName.trim().length === 0
				|| typeof name !== "string" || name.trim().length === 0
			) {
				throw new ClientValidationError(
					"connections.prepareTablesImport: each key must carry non-empty string connectionName and name per the DSS tables-import contract.",
				);
			}
		}
		const pk = this.resolveProjectKey(opts.projectKey,);
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${
				encodeURIComponent(pk,)
			}/datasets/tables-import/actions/prepare-from-keys`,
			{ keys: opts.keys, },
		);
		return this.client.safeParse(FutureStateSchema, raw, "connections.prepareTablesImport",);
	}

	/**
	 * Performs an import from table candidates (WRITE_CONF required). At least
	 * one of `sqlImportCandidates` or `hiveImportCandidates` must be supplied;
	 * shapes follow the DSS SQL/Hive import candidate objects. Returns a future
	 * reference; poll with `client.futures`.
	 */
	async executeTablesImport(
		opts: {
			sqlImportCandidates?: Array<Record<string, unknown>>;
			hiveImportCandidates?: Array<Record<string, unknown>>;
			projectKey?: string;
		},
	): Promise<FutureState> {
		const sql = opts?.sqlImportCandidates;
		const hive = opts?.hiveImportCandidates;
		if (
			(sql !== undefined && (!Array.isArray(sql,) || sql.length === 0))
			|| (hive !== undefined && (!Array.isArray(hive,) || hive.length === 0))
			|| (sql === undefined && hive === undefined)
		) {
			throw new ClientValidationError(
				"connections.executeTablesImport: sqlImportCandidates or hiveImportCandidates must be a non-empty array (at least one required).",
			);
		}
		const pk = this.resolveProjectKey(opts.projectKey,);
		const body: Record<string, unknown> = {};
		if (sql !== undefined) body.sqlImportCandidates = sql;
		if (hive !== undefined) body.hiveImportCandidates = hive;
		const raw = await this.client.post<unknown>(
			`/public/api/projects/${
				encodeURIComponent(pk,)
			}/datasets/tables-import/actions/execute-from-candidates`,
			body,
		);
		return this.client.safeParse(FutureStateSchema, raw, "connections.executeTablesImport",);
	}
}
