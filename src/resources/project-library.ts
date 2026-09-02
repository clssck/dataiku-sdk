import { createHash, } from "node:crypto";
import { ClientValidationError, DataikuError, } from "../errors.js";
import { BaseResource, } from "./base.js";

export interface ProjectLibraryItem {
	name: string;
	/** Full library path of the item, as observed from DSS contents listings. */
	path?: string;
	/** File size in bytes, as observed from DSS contents listings. */
	size?: number;
	/** MIME type of the file content, as observed from DSS contents listings. */
	mimeType?: string;
	/** Whether the item carries data, as observed from DSS contents listings. */
	hasData?: boolean;
	/** Last modification timestamp (epoch milliseconds), as observed from DSS contents listings. */
	lastModified?: number;
	children?: ProjectLibraryItem[];
}

export interface ProjectLibraryFileContent {
	data: string;
}

export interface ProjectLibraryRenameRequest {
	oldPath: string;
	newName: string;
}

export interface ProjectLibraryMoveRequest {
	oldPath: string;
	newPath: string;
}

export interface ProjectLibraryWriteOptions {
	/**
	 * 64-character SHA-256 hex digest the caller last read from this file.
	 * Non-atomic stale-read guard: the write is refused when the remote
	 * content already differs at the verification read. It cannot prevent a
	 * concurrent write landing between that read and this write.
	 */
	expectSha256?: string;
}

export interface ProjectLibraryWriteResult {
	path: string;
	/** Number of bytes written. */
	bytes: number;
	/** SHA-256 hex digest of the exact bytes written. */
	sha256: string;
	/** SHA-256 hex digest observed before the write; present when a precondition was checked. */
	beforeSha256?: string;
}

export interface ProjectLibraryDiffOptions {
	/** Cap on reported diff lines (default 200). */
	maxLines?: number;
}

export interface ProjectLibraryDiffResult {
	path: string;
	/** True when remote and local content are byte-identical. */
	unchanged: boolean;
	/** Lines added in the local content relative to the remote file. */
	added: number;
	/** Lines removed from the remote file relative to the local content. */
	removed: number;
	/** Unified diff text; capped at `maxLines`. Empty for binary or byte-identical content. */
	diff: string;
	/** True when the diff text was truncated at `maxLines`. */
	diffTruncated: boolean;
	/** True when either side is not valid UTF-8 text; diff is never emitted for binary. */
	binary?: boolean;
	/** True when the remote file does not exist. */
	remoteAbsent?: boolean;
	remoteSha256?: string;
	remoteBytes?: number;
	localSha256: string;
	localBytes: number;
	maxLines: number;
}

type RawBodyClient = {
	baseUrl: string;
	fetchWithRetry(url: string, init: RequestInit,): Promise<Response>;
	getAnyHeaders(): Record<string, string>;
};

/**
 * The only concurrency control the public project-library API permits. DSS
 * exposes no conditional write (no ETag, no If-Match, no version token), so
 * `expectSha256` can only detect a hash that is already stale when it is
 * checked; it can never turn the subsequent POST into a conditional write.
 */
export const PROJECT_LIBRARY_CONCURRENCY_CONTROL = "client-side-non-atomic-stale-read-check";

/** Lowercase or uppercase SHA-256 hex digest. */
export const EXPECT_SHA256_PATTERN = /^[0-9a-fA-F]{64}$/;

function sha256Hex(value: string | Uint8Array,): string {
	return createHash("sha256",).update(value,).digest("hex",);
}

const CONTROL_CHARACTER_RE = /[\u0000-\u001f\u007f]/;
const BACKSLASH_RE = /\\/;
const TRAILING_AMBIGUOUS_RE = /[\s.]$/;
const WHITESPACE_ONLY_RE = /^\s+$/;

function pathValidationError(path: string, message: string,): ClientValidationError {
	return new ClientValidationError(
		message,
		"validation_failed",
		"Use a project library path relative to the library root: folders separated by '/', each segment a simple file or folder name.",
		{ path, },
	);
}

/**
 * Validate and canonicalize a project library path.
 *
 * Rejects empty paths, control characters, backslash separators, empty
 * segments (leading/trailing/doubled '/'), '.'/'..' segments (including
 * mid-path traversals), whitespace-only or edge-whitespace segments, and
 * trailing dot/space segments that differ across filesystem normalizations.
 *
 * Returns the canonical path with any leading '/' prefix stripped (the DSS
 * contents endpoints take paths relative to the library root).
 */
export function validateLibraryPath(path: string,): string {
	if (typeof path !== "string" || path.trim() === "") {
		throw pathValidationError(path, "Project library path must be a non-empty string.",);
	}
	if (CONTROL_CHARACTER_RE.test(path,)) {
		throw pathValidationError(path, "Project library path must not contain control characters.",);
	}
	const normalized = path.replace(/^\/+/, "",);
	if (normalized === "") {
		throw pathValidationError(
			path,
			"Project library path must name a file or folder below the library root.",
		);
	}
	const segments = normalized.split("/",);
	for (const segment of segments) {
		if (segment === "") {
			throw pathValidationError(
				path,
				"Project library path must not contain empty segments (leading, trailing, or doubled '/').",
			);
		}
		if (segment === "." || segment === "..") {
			throw pathValidationError(
				path,
				"Project library path must not contain '.' or '..' segments.",
			);
		}
		if (BACKSLASH_RE.test(segment,)) {
			throw pathValidationError(path, "Project library path must not contain backslashes.",);
		}
		if (segment !== segment.trim() || WHITESPACE_ONLY_RE.test(segment,)) {
			throw pathValidationError(
				path,
				"Project library path segments must not be whitespace-only, or start or end with whitespace.",
			);
		}
		if (TRAILING_AMBIGUOUS_RE.test(segment,)) {
			throw pathValidationError(
				path,
				"Project library path must not contain segments with a trailing space or dot (ambiguous across filesystem normalizations).",
			);
		}
	}
	return normalized;
}

/**
 * Validate a rename target: a single library segment (no '/'), with the same
 * character rules as path segments. Returns the validated name unchanged.
 */
export function validateLibraryName(name: string,): string {
	if (typeof name !== "string" || name.trim() === "") {
		throw new ClientValidationError(
			"Project library rename target must be a non-empty string.",
			"validation_failed",
			"Pass the new name as a single file or folder name.",
			{ newName: name, },
		);
	}
	if (CONTROL_CHARACTER_RE.test(name,)) {
		throw new ClientValidationError(
			"Project library rename target must not contain control characters.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (name.includes("/",)) {
		throw new ClientValidationError(
			"Project library rename target must be a single segment without '/'.",
			"validation_failed",
			"Rename within the current parent folder; use the move operation to change folders.",
			{ newName: name, },
		);
	}
	if (name === "." || name === "..") {
		throw new ClientValidationError(
			"Project library rename target must not be '.' or '..'.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (BACKSLASH_RE.test(name,)) {
		throw new ClientValidationError(
			"Project library rename target must not contain backslashes.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	if (name !== name.trim() || WHITESPACE_ONLY_RE.test(name,) || TRAILING_AMBIGUOUS_RE.test(name,)) {
		throw new ClientValidationError(
			"Project library rename target must not start or end with whitespace or a trailing dot.",
			"validation_failed",
			undefined,
			{ newName: name, },
		);
	}
	return name;
}

/**
 * Validate a destination folder path for a library move. '/' (the library
 * root) is valid; any other path must pass {@link validateLibraryPath}.
 * Returns the canonical destination ('/' or '/<path>').
 */
export function validateLibraryDestinationPath(path: string,): string {
	if (path === "/") return "/";
	return `/${validateLibraryPath(path,)}`;
}

/** Validate and URL-encode each segment of a library path for endpoint use. */
export function encodeLibraryPath(path: string,): string {
	const normalized = validateLibraryPath(path,);
	return normalized
		.split("/",)
		.map((segment,) => encodeURIComponent(segment,))
		.join("/",);
}

const DEFAULT_DIFF_MAX_LINES = 200;
const DIFF_CONTEXT_LINES = 3;
const LCS_WORK_LIMIT = 400_000;

function decodeUtf8(bytes: Uint8Array,): { text: string; binary: boolean; } {
	for (let i = 0; i < Math.min(bytes.length, 8192,); i++) {
		if (bytes[i] === 0) return { text: "", binary: true, };
	}
	try {
		return { text: new TextDecoder("utf-8", { fatal: true, },).decode(bytes,), binary: false, };
	} catch {
		return { text: "", binary: true, };
	}
}

function splitLines(text: string,): string[] {
	return text.split("\n",);
}

type DiffOp = { type: "equal" | "delete" | "insert"; line: string; };

/**
 * Sequence diff via LCS dynamic programming, capped at LCS_WORK_LIMIT cell
 * writes. Beyond the cap the middle block degrades to a single
 * remove-all/add-all edit script, which is always a correct (if coarser)
 * description of the change.
 */
function editScript(before: string[], after: string[],): DiffOp[] {
	const n = before.length;
	const m = after.length;
	if (n * m > LCS_WORK_LIMIT) {
		return [
			...before.map((line,) => ({ type: "delete" as const, line, })),
			...after.map((line,) => ({ type: "insert" as const, line, })),
		];
	}
	const width = m + 1;
	const table = new Uint32Array((n + 1) * width,);
	for (let i = n - 1; i >= 0; i--) {
		for (let j = m - 1; j >= 0; j--) {
			if (before[i] === after[j]) {
				table[i * width + j] = table[(i + 1) * width + j + 1] + 1;
			} else {
				table[i * width + j] = Math.max(table[(i + 1) * width + j], table[i * width + j + 1],);
			}
		}
	}
	const ops: DiffOp[] = [];
	let i = 0;
	let j = 0;
	while (i < n && j < m) {
		if (before[i] === after[j]) {
			ops.push({ type: "equal", line: before[i], },);
			i++;
			j++;
		} else if (table[(i + 1) * width + j] >= table[i * width + j + 1]) {
			ops.push({ type: "delete", line: before[i], },);
			i++;
		} else {
			ops.push({ type: "insert", line: after[j], },);
			j++;
		}
	}
	while (i < n) {
		ops.push({ type: "delete", line: before[i], },);
		i++;
	}
	while (j < m) {
		ops.push({ type: "insert", line: after[j], },);
		j++;
	}
	return ops;
}

interface DiffHunk {
	beforeStart: number;
	beforeCount: number;
	afterStart: number;
	afterCount: number;
	body: string[];
}

/**
 * Group a full edit script into unified-diff hunks: every change block is
 * surrounded by up to DIFF_CONTEXT_LINES equal lines; two change blocks whose
 * separating equal run is shorter than 2*DIFF_CONTEXT_LINES merge into one
 * hunk, matching conventional unified diff output.
 */
function buildHunks(ops: DiffOp[],): DiffHunk[] {
	type Block = { kind: "equal" | "change"; start: number; ops: DiffOp[]; };
	const blocks: Block[] = [];
	for (let z = 0; z < ops.length; z++) {
		const op = ops[z]!;
		const kind = op.type === "equal" ? "equal" : "change";
		const last = blocks[blocks.length - 1];
		if (last && last.kind === kind) last.ops.push(op,);
		else blocks.push({ kind, start: z, ops: [op,], },);
	}
	// Prefix line counters for hunk numbering: before[z]/after[z] count the
	// before/after lines consumed by ops[0..z).
	const beforePrefix = new Uint32Array(ops.length + 1,);
	const afterPrefix = new Uint32Array(ops.length + 1,);
	for (let z = 0; z < ops.length; z++) {
		const op = ops[z]!;
		beforePrefix[z + 1] = beforePrefix[z] + (op.type === "insert" ? 0 : 1);
		afterPrefix[z + 1] = afterPrefix[z] + (op.type === "delete" ? 0 : 1);
	}
	const hunks: DiffHunk[] = [];
	let index = 0;
	while (index < blocks.length) {
		const block = blocks[index]!;
		if (block.kind === "equal") {
			index++;
			continue;
		}
		// Merge with later change blocks when the separating equal run is short.
		let endBlock = index;
		while (
			endBlock + 2 < blocks.length
			&& blocks[endBlock + 1]!.kind === "equal"
			&& blocks[endBlock + 1]!.ops.length <= DIFF_CONTEXT_LINES * 2
			&& blocks[endBlock + 2]!.kind === "change"
		) {
			endBlock += 2;
		}
		const hasBeforeContext = index > 0 && blocks[index - 1]!.kind === "equal";
		const hasAfterContext = endBlock + 1 < blocks.length && blocks[endBlock + 1]!.kind === "equal";
		const beforeBlock = hasBeforeContext ? blocks[index - 1]! : undefined;
		const afterBlock = hasAfterContext ? blocks[endBlock + 1]! : undefined;
		const contextBefore = beforeBlock?.ops.slice(-DIFF_CONTEXT_LINES,) ?? [];
		const contextAfter = afterBlock?.ops.slice(0, DIFF_CONTEXT_LINES,) ?? [];
		const windowStart = (beforeBlock?.start ?? block.start)
			+ (beforeBlock ? contextBefore.length : 0);
		const windowEnd = (afterBlock?.start ?? blocks[endBlock]!.start + blocks[endBlock]!.ops.length)
			+ (afterBlock ? contextAfter.length : 0);
		const body: string[] = [];
		let beforeCount = 0;
		let afterCount = 0;
		for (let z = windowStart; z < windowEnd; z++) {
			const op = ops[z]!;
			if (op.type === "equal") {
				body.push(` ${op.line}`,);
				beforeCount++;
				afterCount++;
			} else if (op.type === "delete") {
				body.push(`-${op.line}`,);
				beforeCount++;
			} else {
				body.push(`+${op.line}`,);
				afterCount++;
			}
		}
		hunks.push({
			beforeStart: beforePrefix[windowStart] + 1,
			beforeCount,
			afterStart: afterPrefix[windowStart] + 1,
			afterCount,
			body,
		},);
		index = endBlock + 1;
	}
	return hunks;
}

function hunkHeader(hunk: DiffHunk,): string {
	const beforeRange = hunk.beforeCount === 1
		? `${hunk.beforeStart}`
		: `${hunk.beforeStart},${hunk.beforeCount}`;
	const afterRange = hunk.afterCount === 1
		? `${hunk.afterStart}`
		: `${hunk.afterStart},${hunk.afterCount}`;
	return `@@ -${beforeRange} +${afterRange} @@`;
}

function formatUnifiedDiff(
	beforeText: string,
	afterText: string,
	path: string,
	maxLines: number,
): { diff: string; diffTruncated: boolean; added: number; removed: number; } {
	const before = splitLines(beforeText,);
	const after = splitLines(afterText,);

	// Trim the common prefix and suffix so the edit script only covers the
	// changed middle (keeps the LCS working set small for large files).
	let prefix = 0;
	while (prefix < before.length && prefix < after.length && before[prefix] === after[prefix]) {
		prefix++;
	}
	let suffix = 0;
	while (
		suffix < before.length - prefix
		&& suffix < after.length - prefix
		&& before[before.length - 1 - suffix] === after[after.length - 1 - suffix]
	) {
		suffix++;
	}
	const ops: DiffOp[] = [
		...before.slice(0, prefix,).map((line,) => ({ type: "equal" as const, line, })),
		...editScript(
			before.slice(prefix, before.length - suffix,),
			after.slice(prefix, after.length - suffix,),
		),
		...before.slice(before.length - suffix,).map((line,) => ({ type: "equal" as const, line, })),
	];
	const added = ops.filter((op,) => op.type === "insert").length;
	const removed = ops.filter((op,) => op.type === "delete").length;

	const hunks = buildHunks(ops,);
	const header = `--- a/${path}\n+++ b/${path}\n`;
	const lines: string[] = [];
	for (const hunk of hunks) {
		lines.push(hunkHeader(hunk,), ...hunk.body,);
	}

	// maxLines counts every reported diff line: the two file headers, the kept
	// hunk lines, and (when truncated) the trailing "…" marker. The trailing
	// newline of the final line is not counted as a line.
	const cap = Math.max(1, maxLines,);
	const diffTruncated = lines.length + 2 + 1 > cap;
	const kept = diffTruncated ? lines.slice(0, Math.max(0, cap - 3,),) : lines;
	const diffText = kept.length > 0
		? `${header}${kept.join("\n",)}\n${diffTruncated ? "…\n" : ""}`
		: header;
	return { diff: diffText, diffTruncated, added, removed, };
}

export class ProjectLibraryResource extends BaseResource {
	/**
	 * List the full project code-library contents tree. Items carry the DSS
	 * observed fields (path, size, mimeType, hasData, lastModified) when the
	 * server reports them; folders carry children, files do not.
	 */
	async listContents(projectKey?: string,): Promise<ProjectLibraryItem[]> {
		return this.client.get<ProjectLibraryItem[]>(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents`,
		);
	}

	/** Read a project code-library file as text. */
	async getFile(path: string, projectKey?: string,): Promise<string> {
		const valid = validateLibraryPath(path,);
		const res = await this.client.get<ProjectLibraryFileContent>(
			this.contentsPath(valid, projectKey,),
		);
		return res.data;
	}

	/** Read a project code-library file as bytes. */
	async getFileBytes(path: string, projectKey?: string,): Promise<Uint8Array> {
		const valid = validateLibraryPath(path,);
		const res = await this.client.get<ProjectLibraryFileContent>(
			`${this.contentsPath(valid, projectKey,)}?dataEncoding=base64`,
		);
		return Buffer.from(res.data, "base64",);
	}

	/**
	 * True when a file or folder exists at the given library path. Existence is
	 * verified against the live contents tree read from DSS.
	 */
	async hasLibraryItem(path: string, projectKey?: string,): Promise<boolean> {
		const valid = validateLibraryPath(path,);
		const segments = valid.split("/",);
		let items = await this.listContents(projectKey,);
		for (const segment of segments) {
			const next = items.find((item,) => item.name === segment);
			if (!next) return false;
			items = next.children ?? [];
		}
		return true;
	}

	/** Create an empty project code-library file; refuses to overwrite an existing item. */
	async addFile(path: string, projectKey?: string,): Promise<void> {
		const valid = validateLibraryPath(path,);
		await this.assertLibraryFileAbsent(valid, projectKey,);
		await this.client.postText(this.contentsPath(valid, projectKey,),);
	}

	/** Create a project code-library folder; refuses to overwrite an existing item. */
	async addFolder(path: string, projectKey?: string,): Promise<void> {
		const valid = validateLibraryPath(path,);
		if (await this.hasLibraryItem(valid, projectKey,)) {
			throw this.alreadyExistsError(valid,);
		}
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/folders/${encodeLibraryPath(valid,)}`,
		);
	}

	/**
	 * Create or replace a project code-library file with raw content. `content`
	 * may be a string (UTF-8 text) or raw bytes (binary-safe); the exact bytes
	 * sent are returned as a SHA-256 digest and byte count.
	 *
	 * When `options.expectSha256` is set, the current remote content is read
	 * first and the write is refused with an `assertion_failed` error when its
	 * SHA-256 differs. The check is non-atomic (see
	 * {@link PROJECT_LIBRARY_CONCURRENCY_CONTROL}).
	 */
	async addOrUpdateFile(
		path: string,
		content: string | Uint8Array,
		projectKey?: string,
		options?: ProjectLibraryWriteOptions,
	): Promise<ProjectLibraryWriteResult> {
		const valid = validateLibraryPath(path,);
		if (typeof content !== "string" && !(content instanceof Uint8Array)) {
			throw new ClientValidationError(
				"Project library file content must be a string or raw bytes.",
				"validation_failed",
			);
		}
		const expectSha256 = options?.expectSha256;
		if (
			expectSha256 !== undefined
			&& (typeof expectSha256 !== "string" || !EXPECT_SHA256_PATTERN.test(expectSha256,))
		) {
			throw new ClientValidationError(
				"expectSha256 must be a 64-character SHA-256 hex digest.",
				"validation_failed",
				"Use the sha256 value returned by a project library read or diff.",
				{ path: valid, },
			);
		}
		let beforeSha256: string | undefined;
		if (expectSha256 !== undefined) {
			try {
				const beforeBytes = await this.getFileBytes(valid, projectKey,);
				beforeSha256 = sha256Hex(beforeBytes,);
			} catch (error) {
				if (error instanceof DataikuError && error.category === "not_found") {
					beforeSha256 = undefined;
				} else {
					throw error;
				}
			}
			if (beforeSha256 !== expectSha256.toLowerCase()) {
				throw new ClientValidationError(
					`Project library file "${valid}" does not match --expect-sha256.`,
					"assertion_failed",
					"Re-read the file's sha256 and retry with the current digest. The stale-read check is non-atomic: a concurrent writer can still land between this read and the write.",
					{
						path: valid,
						expectedSha256: expectSha256.toLowerCase(),
						actualSha256: beforeSha256 ?? null,
					},
				);
			}
		}
		const bytes = typeof content === "string" ? Buffer.from(content, "utf8",) : content;
		const sha256 = sha256Hex(bytes,);
		await this.postRawBody(this.contentsPath(valid, projectKey,), content,);
		return {
			path: valid,
			bytes: bytes.length,
			sha256,
			...(beforeSha256 !== undefined ? { beforeSha256, } : {}),
		};
	}

	/**
	 * Diff a library file against local content. The remote side is read as
	 * bytes (so binary files are detected instead of dumped); the reported
	 * unified diff is capped at `options.maxLines` (default 200) and never
	 * contains binary payloads.
	 */
	async diffFile(
		path: string,
		local: string | Uint8Array,
		projectKey?: string,
		options?: ProjectLibraryDiffOptions,
	): Promise<ProjectLibraryDiffResult> {
		const valid = validateLibraryPath(path,);
		const maxLines = Math.max(
			1,
			Math.floor(options?.maxLines ?? DEFAULT_DIFF_MAX_LINES,),
		);
		const localBytes = typeof local === "string" ? Buffer.from(local, "utf8",) : local;
		const localSha256 = sha256Hex(localBytes,);
		const localDecoded = typeof local === "string"
			? { text: local as string, binary: false, }
			: decodeUtf8(localBytes,);
		let remoteBytes: Uint8Array | undefined;
		let remoteAbsent = false;
		try {
			remoteBytes = await this.getFileBytes(valid, projectKey,);
		} catch (error) {
			if (error instanceof DataikuError && error.category === "not_found") {
				remoteAbsent = true;
			} else {
				throw error;
			}
		}
		const base: ProjectLibraryDiffResult = {
			path: valid,
			unchanged: false,
			added: 0,
			removed: 0,
			diff: "",
			diffTruncated: false,
			localSha256,
			localBytes: localBytes.length,
			maxLines,
		};
		if (remoteAbsent) {
			const rendered = localDecoded.binary
				? { added: 0, removed: 0, diff: "", diffTruncated: false, }
				: formatUnifiedDiff("", localDecoded.text, valid, maxLines,);
			return {
				...base,
				unchanged: localBytes.length === 0,
				added: rendered.added,
				removed: 0,
				diff: rendered.diff,
				diffTruncated: rendered.diffTruncated,
				remoteAbsent: true,
				...(localDecoded.binary ? { binary: true, } : {}),
			};
		}
		const remoteSha256 = sha256Hex(remoteBytes!,);
		const remoteDecoded = decodeUtf8(remoteBytes!,);
		const binary = remoteDecoded.binary || localDecoded.binary;
		const unchanged = remoteSha256 === localSha256;
		if (binary || unchanged) {
			return {
				...base,
				unchanged,
				...(binary ? { binary: true, } : {}),
				remoteSha256,
				remoteBytes: remoteBytes!.length,
			};
		}
		const rendered = formatUnifiedDiff(remoteDecoded.text, localDecoded.text, valid, maxLines,);
		return {
			...base,
			unchanged: false,
			added: rendered.added,
			removed: rendered.removed,
			diff: rendered.diff,
			diffTruncated: rendered.diffTruncated,
			remoteSha256,
			remoteBytes: remoteBytes!.length,
		};
	}

	/** Delete a project code-library file or folder. */
	async deleteFile(path: string, projectKey?: string,): Promise<void> {
		const valid = validateLibraryPath(path,);
		await this.client.del(this.contentsPath(valid, projectKey,),);
	}

	/** Rename a project code-library file or folder within its current parent folder. */
	async rename(path: string, newName: string, projectKey?: string,): Promise<void> {
		const validPath = validateLibraryPath(path,);
		const validName = validateLibraryName(newName,);
		const body: ProjectLibraryRenameRequest = {
			oldPath: `/${validPath}`,
			newName: validName,
		};
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents-actions/rename/`,
			body,
		);
	}

	/** Move a project code-library file or folder into another library folder. */
	async move(path: string, destinationFolderPath: string, projectKey?: string,): Promise<void> {
		const validPath = validateLibraryPath(path,);
		const destination = validateLibraryDestinationPath(destinationFolderPath,);
		const body: ProjectLibraryMoveRequest = {
			oldPath: `/${validPath}`,
			newPath: destination,
		};
		await this.client.postText(
			`/public/api/projects/${this.enc(projectKey,)}/libraries/contents-actions/move`,
			body,
		);
	}

	private contentsPath(path: string, projectKey?: string,): string {
		const projectKeyPart = this.enc(projectKey,);
		return `/public/api/projects/${projectKeyPart}/libraries/contents/${encodeLibraryPath(path,)}`;
	}

	private async postRawBody(path: string, body: string | Uint8Array,): Promise<void> {
		// DSS library writes use dataikuapi's raw_body; route through the client's
		// transport so auth, retries, TLS options, and DSS error handling stay aligned.
		// BodyInit rejects plain Uint8Array views, so non-Buffer bytes are copied
		// into a Buffer (an ArrayBufferView BodyInit accepts).
		const payload = body instanceof Uint8Array && !(body instanceof Buffer)
			? Buffer.from(body.buffer, body.byteOffset, body.byteLength,)
			: body;
		const rawClient = this.client as unknown as RawBodyClient;
		const res = await rawClient.fetchWithRetry(`${rawClient.baseUrl}${path}`, {
			method: "POST",
			headers: rawClient.getAnyHeaders(),
			body: payload,
		},);
		await res.text();
	}

	private alreadyExistsError(path: string,): ClientValidationError {
		return new ClientValidationError(
			`Project library item "${path}" already exists.`,
			"validation_failed",
			"Refuse to overwrite an existing item; delete it first, or use the create-or-replace update to replace an existing file.",
			{ path, },
		);
	}

	private async assertLibraryFileAbsent(path: string, projectKey?: string,): Promise<void> {
		try {
			await this.client.get<ProjectLibraryFileContent>(this.contentsPath(path, projectKey,),);
		} catch (error) {
			if (error instanceof DataikuError && error.category === "not_found") return;
			throw error;
		}
		throw this.alreadyExistsError(path,);
	}
}
