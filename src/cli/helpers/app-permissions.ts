import * as fs from "node:fs";
import type { ProjectPermissions, } from "../../resources/projects.js";
import { canonicalDssUrl, } from "../../utils/dss-url.js";
import { compareStrings, } from "../../utils/stable-hash.js";
import { parseJsonObject, plainRecord, stableHash, stableJson, } from "../coerce.js";
import { UsageError, } from "../usage.js";

/**
 * Version 3 binds every snapshot to both its DSS server and the concrete project
 * incarnation present at capture time. Earlier versions are refused.
 */
export const APP_PERMISSIONS_SNAPSHOT_VERSION = 3;

/** Snapshots hold access-control rules: keep them owner-only on disk. */
const SNAPSHOT_FILE_MODE = 0o600;

/**
 * Open for writing *without* `O_TRUNC`: truncation must wait until the descriptor
 * has been narrowed to owner-only. `O_NOFOLLOW`, where the platform defines it,
 * refuses a symlinked destination so a hostile link cannot redirect the snapshot —
 * or the permission change itself — onto another file.
 */
const SNAPSHOT_OPEN_FLAGS = fs.constants.O_WRONLY | fs.constants.O_CREAT
	| (typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0);

export interface AppPermissionsSnapshot {
	version: number;
	projectKey: string;
	/** Canonical base URL of the DSS server these rules were read from. */
	dssUrl: string;
	/** Hash of the concrete project incarnation these rules were read from. */
	projectIncarnationHash: string;
	capturedAt: string;
	hash: string;
	permissionsHash: string;
	permissions: ProjectPermissions;
}

export type AppPermissionsDifferenceStatus = "added" | "removed" | "changed";

export interface AppPermissionsDifference {
	path: string;
	status: AppPermissionsDifferenceStatus;
	backup?: unknown;
	current?: unknown;
}

export interface AppPermissionsDiff {
	changed: boolean;
	differences: AppPermissionsDifference[];
}

/**
 * Canonical hash of a permissions payload. Key order and unknown DSS fields are
 * normalized by `stableJson`, so re-serializing a snapshot never changes the hash.
 */
export function appPermissionsHash(permissions: unknown,): string {
	return stableHash(permissions,);
}

export function buildAppPermissionsSnapshot(
	projectKey: string,
	dssUrl: string,
	projectIncarnationHash: string,
	permissions: ProjectPermissions,
	capturedAt: string = new Date().toISOString(),
): AppPermissionsSnapshot {
	const version = APP_PERMISSIONS_SNAPSHOT_VERSION;
	const boundUrl = canonicalDssUrl(dssUrl,);
	if (boundUrl.length === 0) {
		throw new UsageError(
			"Cannot capture an app permissions snapshot without the DSS server URL it was read from.",
			"validation_failed",
		);
	}
	if (!/^[0-9a-f]{64}$/.test(projectIncarnationHash,)) {
		throw new UsageError(
			"Cannot capture an app permissions snapshot without a valid project-incarnation hash.",
			"validation_failed",
		);
	}
	const permissionsHash = appPermissionsHash(permissions,);
	const hash = stableHash(
		{
			version,
			projectKey,
			dssUrl: boundUrl,
			projectIncarnationHash,
			capturedAt,
			permissionsHash,
			permissions,
		},
	);
	return {
		version,
		projectKey,
		dssUrl: boundUrl,
		projectIncarnationHash,
		capturedAt,
		hash,
		permissionsHash,
		permissions,
	};
}

/** Drain the payload: one `writeSync` may cover only part of the buffer. */
function writeAllSync(fd: number, payload: Buffer,): void {
	let offset = 0;
	while (offset < payload.length) {
		const written = fs.writeSync(fd, payload, offset, payload.length - offset, offset,);
		if (written <= 0) {
			throw new Error(`Stalled writing app permissions snapshot at byte ${offset}.`,);
		}
		offset += written;
	}
}

/**
 * Write a snapshot as owner-only JSON. Access-control rules must never sit on disk
 * under wider permissions: `writeFileSync`'s `mode` is ignored for an existing file
 * and a trailing `chmod` would publish the payload first, so the descriptor is
 * clamped to 0600 before the file is truncated or written.
 */
export function writeAppPermissionsSnapshot(
	filePath: string,
	snapshot: AppPermissionsSnapshot,
): void {
	// Serialize before touching the filesystem: a serialization fault must not
	// truncate or half-overwrite an existing snapshot.
	const payload = Buffer.from(`${JSON.stringify(snapshot, null, 2,)}\n`, "utf-8",);
	const fd = fs.openSync(filePath, SNAPSHOT_OPEN_FLAGS, SNAPSHOT_FILE_MODE,);
	let wrote = false;
	try {
		fs.fchmodSync(fd, SNAPSHOT_FILE_MODE,);
		fs.ftruncateSync(fd, 0,);
		writeAllSync(fd, payload,);
		wrote = true;
	} finally {
		if (wrote) {
			fs.closeSync(fd,);
		} else {
			try {
				fs.closeSync(fd,);
			} catch {
				// Keep the original failure: a close fault must not mask it.
			}
		}
	}
}

function requiredSnapshotString(
	record: Record<string, unknown>,
	field: string,
	source: string,
): string {
	const value = record[field];
	if (typeof value !== "string" || value.trim().length === 0) {
		throw new UsageError(
			`${source} is not an app permissions snapshot: "${field}" must be a non-empty string.`,
			"validation_failed",
		);
	}
	return value;
}

/** Validate envelope shape, version, and hash integrity. Never echoes permission payloads. */
export function parseAppPermissionsSnapshot(text: string, source: string,): AppPermissionsSnapshot {
	const record = parseJsonObject(text, source,);
	const version = record["version"];
	if (typeof version !== "number" || !Number.isInteger(version,)) {
		throw new UsageError(
			`${source} is not an app permissions snapshot: "version" must be an integer.`,
			"validation_failed",
		);
	}
	if (version !== APP_PERMISSIONS_SNAPSHOT_VERSION) {
		throw new UsageError(
			`${source} has unsupported snapshot version ${String(version,)} (expected ${
				String(APP_PERMISSIONS_SNAPSHOT_VERSION,)
			}).`,
			"validation_failed",
			version < APP_PERMISSIONS_SNAPSHOT_VERSION
				? "Older snapshots are not bound to the concrete project incarnation they came from, so they cannot be applied safely after project-key reuse. Re-run `dss app permissions-snapshot` against that project to capture a current snapshot."
				: undefined,
		);
	}
	const projectKey = requiredSnapshotString(record, "projectKey", source,);
	const dssUrl = requiredSnapshotString(record, "dssUrl", source,);
	const projectIncarnationHash = requiredSnapshotString(
		record,
		"projectIncarnationHash",
		source,
	);
	const capturedAt = requiredSnapshotString(record, "capturedAt", source,);
	const hash = requiredSnapshotString(record, "hash", source,);
	const permissionsHash = requiredSnapshotString(record, "permissionsHash", source,);
	const permissions = plainRecord(record["permissions"],);
	if (!permissions) {
		throw new UsageError(
			`${source} is not an app permissions snapshot: "permissions" must be a JSON object.`,
			"validation_failed",
		);
	}
	const actualPermissionsHash = appPermissionsHash(permissions,);
	const actualHash = stableHash(
		{
			version,
			projectKey,
			dssUrl,
			projectIncarnationHash,
			capturedAt,
			permissionsHash,
			permissions,
		},
	);
	if (actualPermissionsHash !== permissionsHash || actualHash !== hash) {
		throw new UsageError(
			`${source} failed its integrity check. Refusing to use a corrupted or hand-edited snapshot.`,
			"validation_failed",
			"Re-run `dss app permissions-snapshot` to capture a fresh snapshot.",
		);
	}
	return {
		version,
		projectKey,
		dssUrl,
		projectIncarnationHash,
		capturedAt,
		hash,
		permissionsHash,
		permissions,
	};
}

export function readAppPermissionsSnapshot(filePath: string,): AppPermissionsSnapshot {
	let text: string;
	try {
		text = fs.readFileSync(filePath, "utf-8",);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error,);
		throw new UsageError(
			`Cannot read app permissions snapshot ${filePath}: ${message}`,
			"validation_failed",
		);
	}
	return parseAppPermissionsSnapshot(text, filePath,);
}

/**
 * Refuse to use a snapshot outside the exact project *and* DSS server it was captured
 * from. Access-control rules are server-scoped: the same project key, users, and groups
 * exist on other instances with different meaning, so replaying a snapshot elsewhere can
 * silently grant or revoke access. Runs before any live permission read or write.
 */
export function assertAppPermissionsSnapshotBinding(
	snapshot: AppPermissionsSnapshot,
	targetProjectKey: string,
	currentDssUrl: string,
	currentProjectIncarnationHash?: string,
): void {
	if (snapshot.projectKey !== targetProjectKey) {
		throw new UsageError(
			`Snapshot was captured from project ${snapshot.projectKey} but the target project is ${targetProjectKey}. Refusing to restore permissions across projects.`,
			"validation_failed",
			`Pass --project-key ${snapshot.projectKey} to restore the project the snapshot came from.`,
		);
	}
	const bound = canonicalDssUrl(snapshot.dssUrl,);
	const current = canonicalDssUrl(currentDssUrl,);
	if (bound !== current) {
		throw new UsageError(
			`Snapshot was captured from DSS server ${bound} but the configured server is ${current}. Refusing to use permissions across DSS servers.`,
			"validation_failed",
			`Point the CLI at ${bound} to use this snapshot, or capture a fresh snapshot from ${current}.`,
			{ projectKey: snapshot.projectKey, snapshotDssUrl: bound, currentDssUrl: current, },
		);
	}
	if (currentProjectIncarnationHash === undefined) return;
	if (snapshot.projectIncarnationHash === currentProjectIncarnationHash) return;
	throw new UsageError(
		`Snapshot was captured from a different incarnation of project ${targetProjectKey}. Refusing to use permissions after project-key reuse.`,
		"validation_failed",
		"Capture a fresh permissions snapshot from the current project. Do not restore access-control data from the replaced project.",
		{
			projectKey: targetProjectKey,
			snapshotProjectIncarnationHash: snapshot.projectIncarnationHash,
			currentProjectIncarnationHash,
		},
	);
}

export function appPermissionsVerificationError(
	projectKey: string,
	hashes: { beforeHash: string; desiredHash: string; verifiedHash: string; },
): UsageError {
	return new UsageError(
		`Permission restore verification failed for project ${projectKey}: expected hash ${hashes.desiredHash} after the update but DSS returned ${hashes.verifiedHash}.`,
		"validation_failed",
		"Re-read the live permissions with `dss app permissions-diff` before retrying; DSS may have rejected or rewritten part of the payload.",
		{ projectKey, ...hashes, },
	);
}

function differenceStatus(backup: unknown, current: unknown,): AppPermissionsDifferenceStatus {
	if (backup === undefined) return "added";
	if (current === undefined) return "removed";
	return "changed";
}

function pushDifference(
	out: AppPermissionsDifference[],
	path: string,
	backup: unknown,
	current: unknown,
): void {
	out.push({
		path,
		status: differenceStatus(backup, current,),
		...(backup === undefined ? {} : { backup, }),
		...(current === undefined ? {} : { current, }),
	},);
}

function collectDifferences(
	path: string,
	backup: unknown,
	current: unknown,
	out: AppPermissionsDifference[],
): void {
	if (stableJson(backup,) === stableJson(current,)) return;
	const backupRecord = plainRecord(backup,);
	const currentRecord = plainRecord(current,);
	if (backupRecord && currentRecord) {
		const keys = [...new Set([...Object.keys(backupRecord,), ...Object.keys(currentRecord,),],),]
			.sort(compareStrings,);
		for (const key of keys) {
			collectDifferences(
				path ? `${path}.${key}` : key,
				backupRecord[key],
				currentRecord[key],
				out,
			);
		}
		return;
	}
	if (Array.isArray(backup,) && Array.isArray(current,)) {
		const length = Math.max(backup.length, current.length,);
		for (let index = 0; index < length; index += 1) {
			collectDifferences(`${path}[${String(index,)}]`, backup[index], current[index], out,);
		}
		return;
	}
	pushDifference(out, path || "(root)", backup, current,);
}

/**
 * Deterministic, path-addressed differences between a snapshot payload and live
 * permissions. Paths are sorted so repeated runs emit byte-identical output.
 */
export function diffAppPermissions(backup: unknown, current: unknown,): AppPermissionsDiff {
	const differences: AppPermissionsDifference[] = [];
	collectDifferences("", backup, current, differences,);
	differences.sort((a, b,) => compareStrings(a.path, b.path,));
	return { changed: differences.length > 0, differences, };
}
