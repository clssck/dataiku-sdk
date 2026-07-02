import { readFileSync, } from "node:fs";
import { join, } from "node:path";
import { sanitizeFileName, } from "../../utils/sanitize.js";
import { normalizeLineEndings, sha256Hex, stableHash, } from "../coerce.js";
import { UsageError, } from "../usage.js";

export function recipeBackupPath(recipeName: string, backupDir: string,): string {
	const stamp = new Date().toISOString().replace(/[:.]/g, "-",);
	return join(backupDir, `${sanitizeFileName(recipeName, "recipe",)}-${stamp}.recipe-backup.json`,);
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
	const raw = readFileSync(backupPath, "utf-8",);
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
