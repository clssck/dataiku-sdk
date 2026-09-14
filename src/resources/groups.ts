import { ClientValidationError, } from "../errors.js";
import { BaseResource, requireArrayResponse, requireNonEmpty, } from "./base.js";

/**
 * A DSS group as returned by the admin Security endpoints. The documented
 * attributes below are stable; DSS may return undocumented extra attributes
 * (per the API docs they must not be modified on update), so they are preserved
 * under `Record<string, unknown>` and are never stripped by this SDK.
 */
export interface DssGroup extends Record<string, unknown> {
	name?: string;
	description?: string;
	/** Whether this group gives administrative rights on DSS. */
	admin?: boolean;
	sourceType?: string;
}

/**
 * Body of `POST /admin/groups` (create) and the editable subset of the PUT
 * body. DSS requires `name` on create; `admin` and `description` are optional.
 */
export interface GroupCreateRequest extends Record<string, unknown> {
	name: string;
	description?: string;
	admin?: boolean;
}

function validateGroupBody(body: unknown, method: string,): Record<string, unknown> {
	if (body === null || body === undefined || typeof body !== "object" || Array.isArray(body,)) {
		throw new ClientValidationError(`${method}: body must be a Group object.`,);
	}
	const record = body as Record<string, unknown>;
	if (record.admin !== undefined && typeof record.admin !== "boolean") {
		throw new ClientValidationError(`${method}: admin must be a boolean.`,);
	}
	return record;
}

export class GroupsResource extends BaseResource {
	/** Retrieves the list of DSS groups (Admin required). */
	async list(): Promise<DssGroup[]> {
		const raw = await this.client.get<unknown>("/public/api/admin/groups",);
		return requireArrayResponse<DssGroup>(raw, "groups.list",);
	}

	/**
	 * Retrieves a Group object describing a DSS user group, used for access
	 * control on connections and projects (Admin required).
	 */
	async get(groupName: string,): Promise<DssGroup> {
		const enc = requireNonEmpty(groupName, "groupName",).trim();
		return this.client.get<DssGroup>(`/public/api/admin/groups/${encodeURIComponent(enc,)}`,);
	}

	/**
	 * Creates a group (Admin required). DSS answers with
	 * `{msg: "Created group <name>"}`; the raw server record is returned as-is.
	 */
	async create(body: GroupCreateRequest,): Promise<Record<string, unknown>> {
		if (
			body === null || body === undefined || typeof body !== "object" || Array.isArray(body,)
			|| typeof body.name !== "string"
			|| body.name.trim().length === 0
		) {
			throw new ClientValidationError("groups.create: body.name is required.",);
		}
		validateGroupBody(body, "groups.create",);
		return this.client.post<Record<string, unknown>>("/public/api/admin/groups", body,);
	}

	/**
	 * Updates a group (Admin required). Per the docs the body MUST have been
	 * obtained from a prior GET at the same URL; undocumented attributes should
	 * be passed through unmodified. DSS answers with `{msg: "Edited group ..."}`.
	 */
	async update(groupName: string, body: DssGroup,): Promise<Record<string, unknown>> {
		const enc = requireNonEmpty(groupName, "groupName",).trim();
		const validated = validateGroupBody(body, "groups.update",);
		return this.client.put<Record<string, unknown>>(
			`/public/api/admin/groups/${encodeURIComponent(enc,)}`,
			validated,
		);
	}

	/** Deletes a DSS users group (Admin required). */
	async delete(groupName: string,): Promise<{ deleted: string; }> {
		const enc = requireNonEmpty(groupName, "groupName",).trim();
		await this.client.del(`/public/api/admin/groups/${encodeURIComponent(enc,)}`,);
		return { deleted: enc, };
	}
}
