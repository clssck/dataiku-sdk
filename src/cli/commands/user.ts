import { normalizeSecretKey, sanitizeSecrets, } from "../../utils/secret-sanitize.js";
import { requiredJsonInput, splitCsvFlag, } from "../coerce.js";
import { executionMode, } from "../flags.js";
import { readIfExists, skipResult, } from "../output.js";
import type { CommandMeta, } from "../types.js";
import { requireArgs, UsageError, } from "../usage.js";

/** Credential-bearing user keys, matched after stripping -/_ and lowercasing. */
const SENSITIVE_USER_NORMALIZED_KEYS: Record<string, true> = {
	password: true,
	userpassword: true,
};

/**
 * Key check for the user secrets family: `secrets` entries (DSS user settings
 * carry a secrets array of credential values) and password-like keys are
 * redacted regardless of nesting depth.
 */
function isSensitiveUserKey(normalizedKey: string,): boolean {
	return SENSITIVE_USER_NORMALIZED_KEYS[normalizedKey] === true || normalizedKey === "secrets";
}

export function sanitizeUserSecrets<T,>(value: T, secrets: string[] = [],): T {
	return sanitizeSecrets(value, {
		sensitiveKeys: SENSITIVE_USER_NORMALIZED_KEYS,
		isSensitiveKey: (normalizedKey,) => normalizedKey === "secrets",
		secrets,
	},);
}

/**
 * Extract every exact password value in a user body (top-level or nested) so
 * errors echoing the payload can be scrubbed.
 */
export function userSecretsFromBody(body: unknown,): string[] {
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
				const normalizedKey = normalizeSecretKey(key,);
				if (isSensitiveUserKey(normalizedKey,)) {
					// `secrets` may be an array of credential values or a single
					// string; passwords are single strings.
					if (typeof item === "string") found.push(item,);
					else if (Array.isArray(item,)) {
						for (const entry of item) {
							if (typeof entry === "string") found.push(entry,);
							else visit(entry,);
						}
					} else visit(item,);
				} else {
					visit(item,);
				}
			}
		}
	};
	visit(body,);
	return found;
}

function requiredUserJson(
	flags: Record<string, string | boolean>,
	usage: string,
): Record<string, unknown> {
	const body = requiredJsonInput(
		flags,
		`--data, --data-file, or --stdin is required (user definition). Usage: ${usage}`,
	);
	return body;
}
/** Login identifiers must be non-empty before any request or dry-run preview. */
function requireLogin(value: string | undefined, usage: string,): string {
	const trimmed = value?.trim() ?? "";
	if (trimmed.length === 0) {
		throw new UsageError(
			`login must be a non-empty string.\nUsage: ${usage}`,
			"validation_failed",
		);
	}
	return trimmed;
}

export const userCommands: Record<string, CommandMeta> = {
	list: {
		handler: (c, _a, f,) => c.users.list(f["connected"] === true ? { connected: true, } : undefined,),
		usage: "dss user list [--connected]",
		description:
			"List DSS users (admin). --connected reports currently connected users via WebSockets, which may under-report when WebSockets are disabled.",
		examples: ["dss user list", "dss user list --connected",],
	},
	get: {
		handler: (c, a,) => {
			const usage = "dss user get <login>";
			requireArgs(a, 1, usage,);
			return c.users.get(requireLogin(a[0], usage,),);
		},
		usage: "dss user get <login>",
		description: "Get a DSS user (admin).",
		examples: ["dss user get admin",],
	},
	create: {
		handler: async (c, _a, f,) => {
			const usage = "dss user create (--data JSON|--data-file PATH|--stdin)";
			const body = requiredUserJson(f, usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "create",
					resource: "user",
					login: typeof body.login === "string" ? body.login : undefined,
					endpoint: "/public/api/admin/users",
					method: "POST",
					payload: sanitizeUserSecrets(body,),
				};
			}
			const result = await c.users.create(body as Parameters<typeof c.users.create>[0],);
			return sanitizeUserSecrets(result, userSecretsFromBody(body,),);
		},
		usage: "dss user create (--data JSON|--data-file PATH|--stdin) [--dry-run]",
		description:
			'Create a DSS user (admin). The JSON body follows the DSS User schema, e.g. {"login":"UserA","sourceType":"LOCAL","displayName":"User A","groups":[],"userProfile":"DATA_ANALYST","password":"..."}. Secrets are never echoed in output.',
		examples: [
			`dss user create --data '{"login":"UserA","sourceType":"LOCAL","displayName":"User A","groups":[],"userProfile":"DATA_ANALYST","password":"s3cret"}'`,
		],
	},
	update: {
		handler: async (c, a, f,) => {
			const usage = "dss user update <login> (--data JSON|--data-file PATH|--stdin) [--dry-run]";
			requireArgs(a, 1, usage,);
			const login = requireLogin(a[0], usage,);
			const body = requiredUserJson(f, usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "update",
					resource: "user",
					login,
					method: "PUT",
					endpoint: `/public/api/admin/users/${encodeURIComponent(login,)}`,
					payload: sanitizeUserSecrets(body,),
					note:
						"PUT replaces the whole User object; the body must come from `dss user get`. No GET was issued in dry-run.",
				};
			}
			const result = await c.users.update(login, body as Parameters<typeof c.users.update>[1],);
			return sanitizeUserSecrets(result, userSecretsFromBody(body,),);
		},
		usage: "dss user update <login> (--data JSON|--data-file PATH|--stdin) [--dry-run]",
		description:
			"Update a DSS user (admin). The body MUST be the User object obtained from `dss user get` (PUT semantics); pass undocumented attributes through unchanged. Use --dry-run to preview the merged result without writing.",
		examples: [
			`dss user update UserA --data '{"login":"UserA","displayName":"User A2","groups":["administrators"],"codeAllowed":true}'`,
		],
	},
	delete: {
		handler: async (c, a, f,) => {
			const usage = "dss user delete <login> [--if-exists] [--dry-run]";
			requireArgs(a, 1, usage,);
			const login = requireLogin(a[0], usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "delete",
					resource: "user",
					login,
					method: "DELETE",
					endpoint: `/public/api/admin/users/${encodeURIComponent(login,)}`,
					...(f["if-exists"] === true
						? { note: "Existence not checked in dry-run; live --if-exists issues a GET probe first.", }
						: {}),
				};
			}
			if (f["if-exists"] === true) {
				const current = await readIfExists(() => c.users.get(login,));
				if (!current) return skipResult("user", login, "missing",);
			}
			return c.users.delete(login,);
		},
		usage: "dss user delete <login> [--if-exists] [--dry-run]",
		description: "Delete a DSS user (admin). Destructive and irreversible.",
		examples: ["dss user delete UserA", "dss user delete UserA --if-exists",],
	},
	resync: {
		handler: async (c, a, f,) => {
			const usage = "dss user resync <login>";
			requireArgs(a, 1, usage,);
			const login = requireLogin(a[0], usage,);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "resync",
					resource: "user",
					login,
					method: "POST",
					endpoint: `/public/api/admin/users/${encodeURIComponent(login,)}/actions/resync`,
				};
			}
			return c.users.resync(login,);
		},
		usage: "dss user resync <login> [--dry-run]",
		description:
			"Resync one DSS user (admin). Returns a DSS future; poll with `dss future wait <jobId>` when hasResult is false.",
		examples: ["dss user resync UserA",],
	},
	"resync-multi": {
		handler: async (c, _a, f,) => {
			const logins = splitCsvFlag(f["logins"],);
			if (logins.length === 0) {
				throw new UsageError(
					"--logins is required. Usage: dss user resync-multi --logins user1,user2",
					"missing_required_flag",
				);
			}
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "resync-multi",
					resource: "user",
					count: logins.length,
					method: "POST",
					endpoint: "/public/api/admin/users/actions/resync-multi",
					payload: logins,
				};
			}
			return c.users.resyncMulti(logins,);
		},
		usage: "dss user resync-multi --logins CSV [--dry-run]",
		description: "Resync multiple DSS users (admin). Returns a DSS future.",
		examples: ["dss user resync-multi --logins user1,user2,user3",],
	},
	"external-users": {
		handler: async (c, _a, f,) => {
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "external-users",
					resource: "user",
					method: "GET",
					endpoint: "/public/api/admin/users/actions/external-users",
					note: "Mints a DSS future that enumerates the external identity supplier.",
				};
			}
			return c.users.externalUsers();
		},
		usage: "dss user external-users [--dry-run]",
		description:
			"Fetch externally sourced users and their provisioning status (admin). Returns a DSS future whose result is the external user list when hasResult is true.",
		examples: ["dss user external-users",],
	},
	"external-groups": {
		handler: async (c, _a, f,) => {
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "external-groups",
					resource: "user",
					method: "GET",
					endpoint: "/public/api/admin/users/actions/external-groups",
					note: "Mints a DSS future that enumerates the external identity supplier's groups.",
				};
			}
			return c.users.externalGroups();
		},
		usage: "dss user external-groups [--dry-run]",
		description:
			"Fetch externally sourced groups (admin). Returns a DSS future whose result is the external group name list when hasResult is true.",
		examples: ["dss user external-groups",],
	},
	provision: {
		handler: async (c, _a, f,) => {
			const usage = "dss user provision (--data JSON|--data-file PATH|--stdin)";
			const body = requiredJsonInput(
				f,
				`--data, --data-file, or --stdin is required (provisioning request). Usage: ${usage}`,
			);
			if (executionMode(f,).dryRun) {
				return {
					dryRun: true,
					action: "provision",
					resource: "user",
					userSourceType: typeof body.userSourceType === "string" ? body.userSourceType : undefined,
					method: "POST",
					endpoint: "/public/api/admin/users/actions/provision",
					payload: body,
				};
			}
			return c.users.provision(body as Parameters<typeof c.users.provision>[0],);
		},
		usage: "dss user provision (--data JSON|--data-file PATH|--stdin) [--dry-run]",
		description:
			"Provision users from an external source (admin). Body is the documented {userSourceType, users[]} request; returns a DSS future with a provisioning summary.",
		examples: [
			`dss user provision --data '{"userSourceType":"AZURE_AD","users":[{"login":"bob","displayName":"Bob","sourceGroupNames":["externalGroupA"],"dkuGroupNames":["dssGroupA"]}]}'`,
		],
	},
	activity: {
		handler: (c,) => c.users.activityAll(),
		usage: "dss user activity",
		description: "Get last activity timestamps for all DSS users (admin).",
		examples: ["dss user activity",],
	},
	"activity-get": {
		handler: (c, a,) => {
			const usage = "dss user activity-get <login>";
			requireArgs(a, 1, usage,);
			return c.users.activity(requireLogin(a[0], usage,),);
		},
		usage: "dss user activity-get <login>",
		description: "Get last activity timestamps for one DSS user (admin).",
		examples: ["dss user activity-get UserA",],
	},
};
