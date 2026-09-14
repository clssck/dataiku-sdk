import { ClientValidationError, } from "../errors.js";
import type { FutureState, } from "../schemas.js";
import { FutureStateSchema, } from "../schemas.js";
import { BaseResource, requireArrayResponse, requireNonEmpty, } from "./base.js";

/**
 * A DSS user as returned by the admin Security endpoints. The documented
 * attributes below are stable; DSS may return undocumented extra attributes
 * (per the API docs they must not be modified on update), so they are preserved
 * under `Record<string, unknown>` and are never stripped by this SDK.
 */
export interface DssUser extends Record<string, unknown> {
	login?: string;
	sourceType?: string;
	displayName?: string;
	groups?: string[];
	/** "True if the user is allowed to write native code" (string or boolean per DSS). */
	codeAllowed?: unknown;
	email?: string;
	password?: string;
	userProfile?: string;
}

/**
 * Last activity timestamps for one user. Epoch-millisecond timestamps are
 * absent for a user who has never logged in or whose session was never loaded.
 */
export interface UserActivity extends Record<string, unknown> {
	login?: string;
	lastSuccessfulLogin?: number | null;
	lastFailedLogin?: number | null;
	lastLoaded?: number | null;
}

/**
 * One entry of the external-users listing: an externally sourced user together
 * with its provisioning status against DSS groups/profiles.
 */
export interface ExternalUserEntry extends Record<string, unknown> {
	status?: string;
	userAttributes?: Record<string, unknown>;
	profile?: string;
}

/**
 * Body of `POST /admin/users` (create). `login` is required by DSS; everything
 * else is optional because DSS accepts partial definitions and fills defaults.
 * Secret attributes (e.g. `password`) are forwarded verbatim over TLS but must
 * never be logged — see the CLI-layer redaction in commands/user.ts.
 */
export interface UserCreateRequest extends Record<string, unknown> {
	login: string;
	sourceType?: string;
	displayName?: string;
	email?: string;
	groups?: string[];
	userProfile?: string;
	password?: string;
}

/**
 * Body of `POST /admin/users/actions/provision`. Each provisioned user carries
 * the source (external) group names and the DSS group names to map them to.
 */
export interface ProvisionRequest extends Record<string, unknown> {
	userSourceType: string;
	users: Array<{
		login?: string;
		displayName?: string;
		email?: string;
		sourceGroupNames?: string[];
		dkuGroupNames?: string[];
	}>;
}

function validateGroupsBody(body: Record<string, unknown>, method: string,): void {
	if (body.groups !== undefined && !Array.isArray(body.groups,)) {
		throw new ClientValidationError(`${method}: groups must be an array of group names.`,);
	}
}

export class UsersResource extends BaseResource {
	/**
	 * Retrieves the list of DSS users (Admin required).
	 *
	 * With `connected: true`, DSS additionally reports which users are currently
	 * connected. The connected status is detected over WebSockets per the docs;
	 * disabled WebSockets make some or all connected users report as offline.
	 */
	async list(opts?: { connected?: boolean; },): Promise<DssUser[]> {
		const params = new URLSearchParams();
		if (opts?.connected !== undefined) params.set("connected", String(opts.connected,),);
		const query = params.toString();
		const raw = await this.client.get<unknown>(
			`/public/api/admin/users/${query ? `?${query}` : ""}`,
		);
		return requireArrayResponse<DssUser>(raw, "users.list",);
	}

	/** Retrieves a User object describing a DSS user (Admin required). */
	async get(login: string,): Promise<DssUser> {
		const enc = requireNonEmpty(login, "login",).trim();
		return this.client.get<DssUser>(`/public/api/admin/users/${encodeURIComponent(enc,)}`,);
	}

	/**
	 * Adds a user account on DSS (Admin required). DSS answers 201 with
	 * `{msg: "Created user <login>"}`; the raw server record is returned as-is.
	 */
	async create(body: UserCreateRequest,): Promise<Record<string, unknown>> {
		if (
			typeof body?.login !== "string" || body.login.trim().length === 0
		) {
			throw new ClientValidationError("users.create: body.login is required.",);
		}
		validateGroupsBody(body, "users.create",);
		return this.client.post<Record<string, unknown>>("/public/api/admin/users", body,);
	}

	/**
	 * Updates a user (Admin required). Per the docs the body MUST have been
	 * obtained from a prior GET at the same URL; undocumented attributes should
	 * be passed through unmodified. DSS answers with `{msg: "Edited user ..."}`.
	 */
	async update(login: string, body: DssUser,): Promise<Record<string, unknown>> {
		const enc = requireNonEmpty(login, "login",).trim();
		if (body === null || body === undefined || typeof body !== "object" || Array.isArray(body,)) {
			throw new ClientValidationError("users.update: body must be a User object.",);
		}
		validateGroupsBody(body, "users.update",);
		return this.client.put<Record<string, unknown>>(
			`/public/api/admin/users/${encodeURIComponent(enc,)}`,
			body,
		);
	}

	/** Deletes a DSS user (Admin required). */
	async delete(login: string,): Promise<{ deleted: string; }> {
		const enc = requireNonEmpty(login, "login",).trim();
		await this.client.del(`/public/api/admin/users/${encodeURIComponent(enc,)}`,);
		return { deleted: enc, };
	}

	/**
	 * Resyncs one user (Admin required). Returns a DSS future reference; poll
	 * with `client.futures` when the result is not inline.
	 */
	async resync(login: string,): Promise<FutureState> {
		const enc = requireNonEmpty(login, "login",).trim();
		const raw = await this.client.post<unknown>(
			`/public/api/admin/users/${encodeURIComponent(enc,)}/actions/resync`,
		);
		return this.client.safeParse(FutureStateSchema, raw, "users.resync",);
	}

	/**
	 * Resyncs multiple users (Admin required). Body is a plain array of logins
	 * per the docs. Returns a future reference.
	 */
	async resyncMulti(logins: string[],): Promise<FutureState> {
		if (
			!Array.isArray(logins,) || logins.length === 0
			|| logins.some((l,) => typeof l !== "string" || l.trim().length === 0)
		) {
			throw new ClientValidationError(
				"users.resyncMulti: logins must be a non-empty array of logins.",
			);
		}
		const raw = await this.client.post<unknown>(
			"/public/api/admin/users/actions/resync-multi",
			logins,
		);
		return this.client.safeParse(FutureStateSchema, raw, "users.resyncMulti",);
	}

	/**
	 * Fetches externally sourced users and their provisioning status
	 * (Admin required). Returns a future whose `result` (when present) is an
	 * array of {@link ExternalUserEntry}; poll with `client.futures` when
	 * `hasResult` is false.
	 */
	async externalUsers(): Promise<FutureState> {
		const raw = await this.client.get<unknown>(
			"/public/api/admin/users/actions/external-users",
		);
		return this.client.safeParse(FutureStateSchema, raw, "users.externalUsers",);
	}

	/**
	 * Fetches externally sourced groups (Admin required). Returns a future whose
	 * `result` (when present) is an array of group-name strings; poll with
	 * `client.futures` when `hasResult` is false.
	 */
	async externalGroups(): Promise<FutureState> {
		const raw = await this.client.get<unknown>(
			"/public/api/admin/users/actions/external-groups",
		);
		return this.client.safeParse(FutureStateSchema, raw, "users.externalGroups",);
	}

	/**
	 * Provisions users from an external source (Admin required). Body is the
	 * documented `{userSourceType, users[]}` request. Returns a future whose
	 * result carries a provisioning summary; poll with `client.futures`.
	 */
	async provision(body: ProvisionRequest,): Promise<FutureState> {
		if (body === null || body === undefined || typeof body !== "object" || Array.isArray(body,)) {
			throw new ClientValidationError("users.provision: body must be an object.",);
		}
		if (typeof body.userSourceType !== "string" || body.userSourceType.trim().length === 0) {
			throw new ClientValidationError("users.provision: body.userSourceType is required.",);
		}
		if (!Array.isArray(body.users,)) {
			throw new ClientValidationError("users.provision: body.users must be an array.",);
		}
		const raw = await this.client.post<unknown>(
			"/public/api/admin/users/actions/provision",
			body,
		);
		return this.client.safeParse(FutureStateSchema, raw, "users.provision",);
	}

	/** Gets the last activity timestamps for all users (Admin required). */
	async activityAll(): Promise<UserActivity[]> {
		const raw = await this.client.get<unknown>("/public/api/admin/users-activity",);
		return requireArrayResponse<UserActivity>(raw, "users.activityAll",);
	}

	/** Gets the last activity timestamps for one user (Admin required). */
	async activity(login: string,): Promise<UserActivity> {
		const enc = requireNonEmpty(login, "login",).trim();
		return this.client.get<UserActivity>(
			`/public/api/admin/users/${encodeURIComponent(enc,)}/activity`,
		);
	}
}
