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
		.map((line,) => JSON.parse(line,) as CleanupLedgerEntry);
}
