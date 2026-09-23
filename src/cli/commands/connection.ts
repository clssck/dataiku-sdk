import { sanitizeErrorSecrets, sanitizeSecrets, } from "../../utils/secret-sanitize.js";
import { requiredJsonInput, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { readIfExists, skipResult, } from "../output.js";
import { commandUsage, withUsage, } from "../syntax.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/**
 * Keys of a connection payload that carry credentials. CLI plan/error/output
 * layers replace these with `[redacted]`; the SDK resource layer forwards
 * values verbatim to DSS (over TLS) and returns server responses unmodified.
 * Keys are matched after stripping `-`/`_` and lowercasing. The connection
 * policy is deliberately broader than the user/project-git tables.
 */
const SENSITIVE_CONNECTION_KEYS: Record<string, true> = {
	accesstoken: true,
	accesskey: true,
	apikey: true,
	apitoken: true,
	apisecret: true,
	authorization: true,
	clientsecret: true,
	credential: true,
	credentials: true,
	password: true,
	privatekey: true,
	refreshtoken: true,
	secret: true,
	secretkey: true,
	sharesecret: true,
	token: true,
};

export function sanitizeConnectionSecrets<T,>(value: T, secrets: string[] = [],): T {
	return sanitizeSecrets(value, { sensitiveKeys: SENSITIVE_CONNECTION_KEYS, secrets, },);
}

/** Extract exact secret values (e.g. `params.password`) for error scrubbing. */
export function connectionSecretsFromBody(body: unknown,): string[] {
	if (body === undefined || body === null || typeof body !== "object" || Array.isArray(body,)) {
		return [];
	}
	const found: string[] = [];
	const visit = (value: unknown,): void => {
		if (Array.isArray(value,)) {
			value.forEach(visit,);
			return;
		}
		if (value !== null && typeof value === "object") {
			for (const [key, item,] of Object.entries(value,)) {
				const normalizedKey = key.replace(/[-_]/g, "",).toLowerCase();
				if (SENSITIVE_CONNECTION_KEYS[normalizedKey] === true && typeof item === "string") {
					found.push(item,);
				} else {
					visit(item,);
				}
			}
		}
	};
	visit(body,);
	return found;
}

/**
 * DSS validation errors can echo the submitted connection body (including
 * credentials). Scrub error text and body before it reaches stderr or a
 * structured failure.
 */
export function sanitizeConnectionError(error: unknown, secrets: string[],): unknown {
	return sanitizeErrorSecrets(error, { sensitiveKeys: SENSITIVE_CONNECTION_KEYS, secrets, },);
}

function requiredConnectionJson(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	return requiredJsonInput(
		flags,
		`--data, --data-file, or --stdin is required (connection definition). Usage: ${usage}`,
	);
}

/** Connection identifiers must be non-empty before any request or dry-run preview. */
function requireConnectionName(value: string | undefined, usage: string,): string {
	const trimmed = value?.trim() ?? "";
	if (trimmed.length === 0) {
		throw new UsageError(
			`connection name must be a non-empty string.\nUsage: ${usage}`,
			"validation_failed",
		);
	}
	return trimmed;
}
export const connectionCommands: Record<string, CommandMeta> = withUsage("connection", {
	list: {
		handler: (c, _a, f,) =>
			c.connections.list({
				type: f["type"] as string | undefined,
			},),
		description: "List all connection names, optionally filtered by connection type.",
		examples: ["dss connection list", "dss connection list --type Filesystem",],
	},
	infer: {
		handler: (c, _a, f,) =>
			c.connections.infer({
				mode: f["mode"] as "fast" | "rich" | undefined,
				projectKey: f["project-key"] as string | undefined,
			},),
		description: "List connections with inferred types and metadata.",
		examples: ["dss connection infer", "dss connection infer --mode rich",],
	},

	get: {
		handler: async (c, a,) => {
			const usage = commandUsage("connection", "get",);
			requireArgs(a, 1, usage,);
			const details = await c.connections.adminGet(requireConnectionName(a[0], usage,),);
			return sanitizeConnectionSecrets(details,);
		},
		description:
			"Get a connection by name (admin). Credential-bearing params (e.g. params.password) are redacted in output.",
		examples: ["dss connection get postgres",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("connection", "create",);
			const body = requiredConnectionJson(f, usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "create",
					resource: "connection",
					connectionName: typeof body.name === "string" ? body.name : undefined,
					method: "POST",
					endpoint: "/public/api/admin/connections",
					payload: sanitizeConnectionSecrets(body,),
				};
			}
			try {
				return await c.connections.adminCreate(
					body as Parameters<typeof c.connections.adminCreate>[0],
				);
			} catch (error) {
				throw sanitizeConnectionError(error, connectionSecretsFromBody(body,),);
			}
		},
		description:
			'Create a connection (admin). Body is the Connection definition, e.g. {"name":"new-connection","type":"PostgreSQL","params":{"host":"...","user":"...","password":"..."}}. Secrets are never echoed in errors or output.',
		examples: [
			`dss connection create --data '{"name":"new-connection","type":"PostgreSQL","params":{"db":"dbname","user":"myuser","password":"s3cret"}}'`,
		],
	},
	update: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("connection", "update",);
			requireArgs(a, 1, usage,);
			const name = requireConnectionName(a[0], usage,);
			const body = requiredConnectionJson(f, usage,);
			const secrets = connectionSecretsFromBody(body,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "update",
					resource: "connection",
					name,
					method: "PUT",
					endpoint: `/public/api/admin/connections/${encodeURIComponent(name,)}`,
					payload: sanitizeConnectionSecrets(body,),
					note:
						"PUT replaces the whole Connection object; the body must come from `dss connection get`. No GET was issued in dry-run.",
				};
			}
			try {
				const result = await c.connections.adminUpdate(
					name,
					body as Parameters<typeof c.connections.adminUpdate>[1],
				);
				return sanitizeConnectionSecrets(result, secrets,);
			} catch (error) {
				throw sanitizeConnectionError(error, secrets,);
			}
		},
		description:
			"Update a connection (admin). The body MUST be the Connection object obtained from `dss connection get` (PUT semantics); type and name cannot be modified. Use --dry-run to preview the merged result without writing.",
		examples: [
			`dss connection update postgres --data '{"type":"PostgreSQL","name":"postgres","allowWrite":true,"params":{"db":"dbname","user":"myuser","password":"s3cret"}}'`,
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			const usage = commandUsage("connection", "delete",);
			requireArgs(a, 1, usage,);
			const name = requireConnectionName(a[0], usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "delete",
					resource: "connection",
					name,
					method: "DELETE",
					endpoint: `/public/api/admin/connections/${encodeURIComponent(name,)}`,
					...(f["if-exists"] === true
						? { note: "Existence not checked in dry-run; live --if-exists issues a GET probe first.", }
						: {}),
				};
			}
			if (f["if-exists"] === true) {
				const current = await readIfExists(() => c.connections.adminGet(name,));
				if (!current) return skipResult("connection", name, "missing",);
			}
			return c.connections.adminDelete(name,);
		},
		description:
			"Delete a connection (admin). Destructive: per the DSS docs no check is performed that the connection is not in use by a dataset.",
		examples: ["dss connection delete old-conn", "dss connection delete old-conn --if-exists",],
	},
	test: {
		handler: async (c, a,) => {
			const usage = commandUsage("connection", "test",);
			requireArgs(a, 1, usage,);
			const result = await c.connections.adminTest(requireConnectionName(a[0], usage,),);
			return sanitizeConnectionSecrets(result,);
		},
		description:
			"Test whether a connection is available (uses the same GET /connections/{name}/test route as the official Python client; returns connectionOK, errors when testing is unsupported for the type). Credential-bearing fields are redacted in output.",
		examples: ["dss connection test postgres",],
	},
	schemas: {
		handler: (c, _a, f,) => {
			const connection = f["connection"] as string | undefined;
			if (!connection) {
				throw new UsageError(
					`--connection is required. Usage: ${commandUsage("connection", "schemas",)}`,
				);
			}
			return c.connections.schemas({
				connection,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description: "List schemas in a SQL connection.",
		examples: ["dss connection schemas --connection ATHENA_CONN --project-key MYPROJ",],
	},
	tables: {
		handler: (c, _a, f,) => {
			const connection = f["connection"] as string | undefined;
			if (!connection) {
				throw new UsageError(
					`--connection is required. Usage: ${commandUsage("connection", "tables",)}`,
				);
			}
			return c.connections.tables({
				connection,
				catalog: f["catalog"] as string | undefined,
				schema: f["schema"] as string | undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			"List importable tables in a SQL connection, optionally scoped by catalog and schema.",
		examples: [
			"dss connection tables --connection ATHENA_CONN --schema analytics --project-key MYPROJ",
		],
	},
	"prepare-import": {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("connection", "prepare-import",);
			const body = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (tables-import request). Usage: ${usage}`,
			);
			const keys = body["keys"];
			if (!Array.isArray(keys,) || keys.length === 0) {
				throw new UsageError(
					'Body must include a non-empty keys array (e.g. [{"connectionName":"postgres","name":"my_table"}]).',
					"validation_failed",
					`Usage: ${usage}`,
				);
			}
			for (const key of keys) {
				const k = key as Record<string, unknown>;
				const connectionName = k["connectionName"];
				const name = k["name"];
				if (
					typeof connectionName !== "string" || connectionName.trim().length === 0
					|| typeof name !== "string" || name.trim().length === 0
				) {
					throw new UsageError(
						'Each key must carry non-empty string connectionName and name (e.g. {"connectionName":"postgres","name":"my_table"}).',
						"validation_failed",
						`Usage: ${usage}`,
					);
				}
			}
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "prepare-import",
					resource: "connection",
					keyCount: keys.length,
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(c.resolveProjectKey(f["project-key"] as string | undefined,),)
					}/datasets/tables-import/actions/prepare-from-keys`,
					payload: { keys, },
				};
			}
			return c.connections.prepareTablesImport({
				keys: keys as Array<Record<string, unknown>>,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			'Prepare the import of selected SQL or Hive tables (WRITE_CONF). Body is {"keys":[...]} per the DSS tables-import docs. Returns a DSS future; poll with `dss future wait <jobId>`.',
		examples: [
			`dss connection prepare-import --data '{"keys":[{"connectionName":"postgres","name":"my_table"}]}' --project-key MYPROJ`,
		],
	},
	"execute-import": {
		handler: async (c, _a, f,) => {
			const usage = commandUsage("connection", "execute-import",);
			const body = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (tables-import request). Usage: ${usage}`,
			);
			const sql = body["sqlImportCandidates"];
			const hive = body["hiveImportCandidates"];
			const hasSql = Array.isArray(sql,) && sql.length > 0;
			const hasHive = Array.isArray(hive,) && hive.length > 0;
			if (!hasSql && !hasHive) {
				throw new UsageError(
					'Body must include sqlImportCandidates and/or hiveImportCandidates arrays (e.g. {"sqlImportCandidates":[{"connectionName":"pgsql","table":"t","checked":false,"datasetName":"imported","existingDatasetsNames":[]}]}).',
					"validation_failed",
					`Usage: ${usage}`,
				);
			}
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "execute-import",
					resource: "connection",
					sqlCandidates: hasSql ? (sql as Array<Record<string, unknown>>).length : undefined,
					hiveCandidates: hasHive ? (hive as Array<Record<string, unknown>>).length : undefined,
					method: "POST",
					endpoint: `/public/api/projects/${
						encodeURIComponent(c.resolveProjectKey(f["project-key"] as string | undefined,),)
					}/datasets/tables-import/actions/execute-from-candidates`,
					payload: {
						...(hasSql ? { sqlImportCandidates: sql, } : {}),
						...(hasHive ? { hiveImportCandidates: hive, } : {}),
					},
				};
			}
			return c.connections.executeTablesImport({
				sqlImportCandidates: hasSql ? sql as Array<Record<string, unknown>> : undefined,
				hiveImportCandidates: hasHive ? hive as Array<Record<string, unknown>> : undefined,
				projectKey: f["project-key"] as string | undefined,
			},);
		},
		description:
			"Perform an import from SQL/Hive table candidates (WRITE_CONF). Body carries sqlImportCandidates and/or hiveImportCandidates per the DSS tables-import docs. Returns a DSS future.",
		examples: [
			`dss connection execute-import --data '{"sqlImportCandidates":[{"connectionName":"pgsql","table":"my_table","checked":false,"datasetName":"imported_from_db","existingDatasetsNames":[]}]}' --project-key MYPROJ`,
		],
	},
},);
