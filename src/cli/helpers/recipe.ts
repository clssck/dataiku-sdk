import * as fs from "node:fs";
import {
	join,
	parse as parsePath,
	relative as relativePath,
	resolve as resolvePath,
	sep,
} from "node:path";
import { sanitizeFileName, } from "../../utils/sanitize.js";
import { normalizeLineEndings, sha256Hex, stableHash, } from "../coerce.js";
import { UsageError, } from "../usage.js";

/** Backups may embed recipe payloads with secrets: keep them owner-only. */
const RECIPE_BACKUP_DIR_MODE = 0o700;
const RECIPE_BACKUP_FILE_MODE = 0o600;

/**
 * Backup files are created exclusively — an existing destination is a conflict
 * (a stale file, or a pre-planted symlink), never something to overwrite — and
 * O_NOFOLLOW refuses a symlink at the destination itself.
 */
const RECIPE_BACKUP_FILE_OPEN_FLAGS = fs.constants.O_WRONLY | fs.constants.O_CREAT
	| fs.constants.O_EXCL
	| (typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0);

/** Verify the backup directory through a descriptor that rejects symlinking. */
const RECIPE_BACKUP_DIR_OPEN_FLAGS = fs.constants.O_RDONLY
	| (typeof fs.constants.O_DIRECTORY === "number" ? fs.constants.O_DIRECTORY : 0)
	| (typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0);

export function recipeBackupPath(recipeName: string, backupDir: string,): string {
	const stamp = new Date().toISOString().replace(/[:.]/g, "-",);
	return join(backupDir, `${sanitizeFileName(recipeName, "recipe",)}-${stamp}.recipe-backup.json`,);
}

/**
 * Prepare the backup directory as an owner-only, symlink-free directory.
 *
 * Every existing component is checked with `lstat` and must be a real directory
 * — `mkdir { recursive: true, }` would silently traverse a symlink planted
 * along the path (e.g. `.dss-backups` pointing at an attacker-chosen home) and
 * create with ambient permissions. Missing components are created one at a time
 * with mode 0700, and the final directory is clamped to 0700 through a
 * no-follow descriptor so the verification cannot be raced by a swap.
 */
export function ensureRecipeBackupDir(backupDir: string,): void {
	const absolute = resolvePath(backupDir,);
	const { root, } = parsePath(absolute,);
	const parts = relativePath(root, absolute,).split(sep,).filter((part,) => part.length > 0);
	if (parts.length === 0) {
		throw new UsageError(
			`Recipe backup directory "${backupDir}" is the filesystem root. Choose a dedicated backup directory.`,
			"usage_error",
		);
	}
	let current = root;
	for (const part of parts) {
		current = join(current, part,);
		let stats: fs.Stats;
		try {
			stats = fs.lstatSync(current,);
		} catch {
			fs.mkdirSync(current, { mode: RECIPE_BACKUP_DIR_MODE, },);
			continue;
		}
		if (stats.isSymbolicLink()) {
			throw new UsageError(
				`Recipe backup directory "${backupDir}" traverses the symlink "${current}". Refusing to write backups through a symlink.`,
				"usage_error",
			);
		}
		if (!stats.isDirectory()) {
			throw new UsageError(
				`Recipe backup path component "${current}" is not a directory.`,
				"usage_error",
			);
		}
	}
	let dirFd: number;
	try {
		dirFd = fs.openSync(absolute, RECIPE_BACKUP_DIR_OPEN_FLAGS,);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ELOOP" || code === "ENOTDIR") {
			throw new UsageError(
				`Recipe backup directory "${backupDir}" is not a real, non-symlinked directory.`,
				"usage_error",
			);
		}
		throw error;
	}
	try {
		fs.fchmodSync(dirFd, RECIPE_BACKUP_DIR_MODE,);
		const verified = fs.fstatSync(dirFd,);
		if (!verified.isDirectory()) {
			throw new UsageError(
				`Recipe backup path "${absolute}" is not a directory.`,
				"usage_error",
			);
		}
	} finally {
		fs.closeSync(dirFd,);
	}
}

/** Drain a backup payload: one `writeSync` may cover only part of the buffer. */
function writeAllSync(fd: number, payload: Buffer,): void {
	let offset = 0;
	while (offset < payload.length) {
		const written = fs.writeSync(fd, payload, offset, payload.length - offset, offset,);
		if (written <= 0) {
			throw new Error(`Stalled writing recipe backup at byte ${offset}.`,);
		}
		offset += written;
	}
}

/**
 * Write a recipe backup file as a brand-new owner-only file.
 *
 * `O_EXCL` refuses any pre-existing destination (including a planted symlink)
 * instead of clobbering it, `O_NOFOLLOW` blocks a symlink destination, and the
 * file is created with mode 0600 — the payload must never sit on disk under
 * ambient (umask-derived) permissions.
 */
export function writeRecipeBackup(backupPath: string, content: string,): void {
	const payload = Buffer.from(content, "utf-8",);
	let fd: number;
	try {
		fd = fs.openSync(backupPath, RECIPE_BACKUP_FILE_OPEN_FLAGS, RECIPE_BACKUP_FILE_MODE,);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "EEXIST" || code === "ELOOP") {
			throw new UsageError(
				`Recipe backup destination already exists: ${backupPath}. Refusing to overwrite an existing path.`,
				"usage_error",
			);
		}
		throw error;
	}
	try {
		// O_EXCL guarantees a fresh inode; clamp the mode so the payload is never
		// published under wider permissions even if creation flags change.
		fs.fchmodSync(fd, RECIPE_BACKUP_FILE_MODE,);
		writeAllSync(fd, payload,);
	} finally {
		fs.closeSync(fd,);
	}
}

export function recipeCodeEnv(recipe: Record<string, unknown>,): unknown {
	const params = recipe.params;
	if (!params || typeof params !== "object" || Array.isArray(params,)) return undefined;
	return (params as Record<string, unknown>).envSelection;
}

export function recipeGraph(recipe: Record<string, unknown>,): Record<string, unknown> {
	return {
		inputs: recipe.inputs,
		outputs: recipe.outputs,
	};
}

export function recipeBackupDocument(
	recipeName: string,
	projectKey: string | undefined,
	current: { recipe: Record<string, unknown>; payload?: string; },
): Record<string, unknown> {
	return {
		resource: "recipe",
		recipeName,
		projectKey,
		createdAt: new Date().toISOString(),
		versionTag: current.recipe.versionTag,
		payloadHash: sha256Hex(current.payload ?? "",),
		graphHash: stableHash(recipeGraph(current.recipe,),),
		normalizedPayloadHash: sha256Hex(normalizeLineEndings(current.payload ?? "",),),
		codeEnvHash: stableHash(recipeCodeEnv(current.recipe,),),
		codeEnv: recipeCodeEnv(current.recipe,),
		recipe: current.recipe,
		payload: current.payload ?? "",
	};
}

export function readRecipeBackup(backupPath: string,): Record<string, unknown> {
	const raw = fs.readFileSync(backupPath, "utf-8",);
	try {
		const parsed = JSON.parse(raw,) as Record<string, unknown>;
		if (parsed && typeof parsed === "object" && parsed.resource === "recipe") return parsed;
	} catch {
		// Backward-compatible payload-only backups are handled below.
	}
	return {
		resource: "recipe",
		recipeName: "unknown",
		payloadHash: sha256Hex(raw,),
		payload: raw,
	};
}
export function recipeRunShouldWait(flags: Record<string, string | boolean>,): boolean {
	if (flags["wait"] === true && flags["no-wait"] === true) {
		throw new UsageError("--wait and --no-wait are mutually exclusive.", "invalid_enum",);
	}
	const waitImplied = flags["include-logs"] === true
		|| flags["summary"] === true
		|| flags["timeout"] !== undefined
		|| flags["poll-interval"] !== undefined;
	if (flags["no-wait"] === true && waitImplied) {
		throw new UsageError(
			"--include-logs, --summary, --timeout, and --poll-interval require waiting; remove --no-wait.",
			"invalid_enum",
		);
	}
	return flags["no-wait"] !== true && (flags["wait"] === true || waitImplied);
}

export function recipeRoleInputItems(recipe: Record<string, unknown>, role: string,): unknown[] {
	const inputs = recipe["inputs"];
	if (!inputs || typeof inputs !== "object") return [];
	const roleEntry = (inputs as Record<string, unknown>)[role];
	if (!roleEntry || typeof roleEntry !== "object") return [];
	const items = (roleEntry as Record<string, unknown>)["items"];
	return Array.isArray(items,) ? items : [];
}

export function recipeInputItemRef(item: unknown,): string | undefined {
	if (!item || typeof item !== "object") return undefined;
	const ref = (item as Record<string, unknown>)["ref"];
	return typeof ref === "string" && ref.length > 0 ? ref : undefined;
}
