import * as fs from "node:fs/promises";
import * as path from "node:path";
import { canonicalDssUrl, } from "./dss-url.js";

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
	/** Canonical DSS base URL the entry was recorded against. Absent only on legacy ledgers. */
	dssUrl?: string;
	cleanup: { argv: string[]; };
}

function isCleanupLedgerEntry(value: unknown,): value is CleanupLedgerEntry {
	if (!value || typeof value !== "object") return false;
	const candidate = value as Record<string, unknown>;
	if (typeof candidate.ts !== "string") return false;
	if (typeof candidate.action !== "string") return false;
	if (typeof candidate.resource !== "string") return false;
	if (!candidate.cleanup || typeof candidate.cleanup !== "object") return false;
	if (candidate.dssUrl !== undefined && typeof candidate.dssUrl !== "string") return false;
	const cleanup = candidate.cleanup as Record<string, unknown>;
	return Array.isArray(cleanup.argv,) && cleanup.argv.every((arg,) => typeof arg === "string");
}

/**
 * Prove that a cleanup ledger path can be written before any remote mutation.
 * A parent directory is created when missing (matching append behavior); an
 * existing ledger file is opened for append; a not-yet-existing ledger file is
 * probed by creating and removing a sibling file, so nothing is left behind.
 * Throws a plain Error with a non-secret message on any failure.
 */
export async function preflightCleanupLedgerPath(filePath: string,): Promise<void> {
	const resolved = path.resolve(filePath,);
	const parent = path.dirname(resolved,);
	await fs.mkdir(parent, { recursive: true, },);
	let stats: Awaited<ReturnType<typeof fs.stat>> | undefined;
	try {
		stats = await fs.stat(resolved,);
	} catch (error) {
		if ((error as { code?: string; }).code !== "ENOENT") {
			throw new Error(
				`Could not preflight cleanup ledger ${resolved}: ${
					error instanceof Error ? error.message : String(error,)
				}`,
				{ cause: error, },
			);
		}
	}
	if (stats?.isDirectory()) {
		throw new Error(`Cleanup ledger path is a directory, not a file: ${resolved}`,);
	}
	if (stats !== undefined) {
		try {
			const handle = await fs.open(resolved, "a",);
			await handle.close();
		} catch (error) {
			const code = (error as { code?: string; }).code;
			throw new Error(
				code === "EACCES" || code === "EPERM"
					? `Cleanup ledger file is not writable: ${resolved}`
					: `Could not preflight cleanup ledger ${resolved}: ${
						error instanceof Error ? error.message : String(error,)
					}`,
				{ cause: error, },
			);
		}
		return;
	}
	const probePrefix = `${resolved}.preflight-`;
	try {
		const probe = await fs.mkdtemp(probePrefix,);
		await fs.rm(probe, { recursive: true, force: true, },);
	} catch (error) {
		throw new Error(
			`Cleanup ledger directory is not writable: ${parent}${
				error instanceof Error ? ` (${error.message})` : ""
			}`,
			{ cause: error, },
		);
	}
}

/**
 * Atomically bind a cleanup ledger path to one DSS server before a mutation.
 * The adjacent owner-only reservation closes the empty-ledger race: processes
 * targeting different servers cannot both pass preflight and mutate before
 * either cleanup entry is appended. The reservation intentionally persists
 * even when a mutation fails, because another same-server process may already
 * rely on it.
 */
export async function reserveCleanupLedgerDssUrl(
	filePath: string,
	dssUrl: string,
): Promise<void> {
	const resolved = path.resolve(filePath,);
	const bindingPath = `${resolved}.dss-url`;
	const expected = canonicalDssUrl(dssUrl,);
	await fs.mkdir(path.dirname(resolved,), { recursive: true, },);
	try {
		await fs.writeFile(bindingPath, `${expected}\n`, {
			encoding: "utf-8",
			flag: "wx",
			mode: 0o600,
		},);
		return;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
	}
	const raw = await fs.readFile(bindingPath, "utf-8",);
	let found: string;
	try {
		found = canonicalDssUrl(raw.trim(),);
	} catch (error) {
		throw new Error("Cleanup ledger DSS binding is invalid.", { cause: error, },);
	}
	if (found !== expected) {
		throw new Error("Cleanup ledger is reserved for a different DSS server.",);
	}
}

export interface CleanupLedgerBindingViolation {
	index: number;
	resource: string;
	action: string;
	reason: "missing" | "mismatch";
	/** Canonical URL the entry was bound to, for mismatch reasons. */
	found?: string;
}

/**
 * First entry not exactly bound to `expectedDssUrl` (both canonicalized), in
 * ledger order. Legacy entries without a `dssUrl` count as missing. Intentionally
 * never exposes argv/ids: the violation is identifiable by index alone.
 */
export function findCleanupLedgerBindingViolation(
	entries: CleanupLedgerEntry[],
	expectedDssUrl: string,
): CleanupLedgerBindingViolation | undefined {
	const expected = canonicalDssUrl(expectedDssUrl,);
	for (const [index, entry,] of entries.entries()) {
		const bound = typeof entry.dssUrl === "string" ? canonicalDssUrl(entry.dssUrl,) : undefined;
		if (!bound) {
			return { index, resource: entry.resource, action: entry.action, reason: "missing", };
		}
		if (bound !== expected) {
			return {
				index,
				resource: entry.resource,
				action: entry.action,
				reason: "mismatch",
				found: bound,
			};
		}
	}
	return undefined;
}

export async function appendCleanupLedgerEntry(
	filePath: string,
	entry: CleanupLedgerEntry,
	dssUrl: string,
): Promise<void> {
	const resolved = path.resolve(filePath,);
	await fs.mkdir(path.dirname(resolved,), { recursive: true, },);
	await reserveCleanupLedgerDssUrl(resolved, dssUrl,);
	const bound: CleanupLedgerEntry = { ...entry, dssUrl: canonicalDssUrl(dssUrl,), };
	await fs.appendFile(resolved, `${JSON.stringify(bound,)}\n`, "utf-8",);
}

export async function readCleanupLedger(filePath: string,): Promise<CleanupLedgerEntry[]> {
	const content = await fs.readFile(path.resolve(filePath,), "utf-8",);
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
