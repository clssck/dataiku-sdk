import { appendFile, mkdir, readFile, } from "node:fs/promises";
import { dirname, resolve, } from "node:path";

export type CleanupLedgerAction =
	| "create"
	| "upload"
	| "delete"
	| "update"
	| "set"
	| "run"
	| string;

export interface CleanupLedgerEntry {
	ts: string;
	action: CleanupLedgerAction;
	resource: string;
	id?: string;
	name?: string;
	path?: string;
	projectKey?: string;
	cleanup: { argv: string[]; };
}

function isCleanupLedgerEntry(value: unknown,): value is CleanupLedgerEntry {
	if (!value || typeof value !== "object") return false;
	const candidate = value as Record<string, unknown>;
	if (typeof candidate.ts !== "string") return false;
	if (typeof candidate.action !== "string") return false;
	if (typeof candidate.resource !== "string") return false;
	if (!candidate.cleanup || typeof candidate.cleanup !== "object") return false;
	const cleanup = candidate.cleanup as Record<string, unknown>;
	return Array.isArray(cleanup.argv,) && cleanup.argv.every((arg,) => typeof arg === "string");
}

export async function appendCleanupLedgerEntry(
	filePath: string,
	entry: CleanupLedgerEntry,
): Promise<void> {
	const resolved = resolve(filePath,);
	await mkdir(dirname(resolved,), { recursive: true, },);
	await appendFile(resolved, `${JSON.stringify(entry,)}\n`, "utf-8",);
}

export async function readCleanupLedger(filePath: string,): Promise<CleanupLedgerEntry[]> {
	const content = await readFile(resolve(filePath,), "utf-8",);
	return content
		.split(/\r?\n/,)
		.map((line,) => line.trim())
		.filter((line,) => line.length > 0)
		.map((line, index,) => {
			const parsed: unknown = JSON.parse(line,);
			if (!isCleanupLedgerEntry(parsed,)) {
				throw new Error(`Invalid cleanup ledger entry at line ${index + 1}`,);
			}
			return parsed;
		},);
}
