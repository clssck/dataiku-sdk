const DIFF_CONTEXT_LINES = 3;
const LCS_WORK_LIMIT = 400_000;

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
			...before.map((line,) => ({ type: "delete" as const, line, }),),
			...after.map((line,) => ({ type: "insert" as const, line, }),),
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
		const op = ops[z];
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
		const op = ops[z];
		beforePrefix[z + 1] = beforePrefix[z] + (op.type === "insert" ? 0 : 1);
		afterPrefix[z + 1] = afterPrefix[z] + (op.type === "delete" ? 0 : 1);
	}
	const hunks: DiffHunk[] = [];
	let index = 0;
	while (index < blocks.length) {
		const block = blocks[index];
		if (block.kind === "equal") {
			index++;
			continue;
		}
		// Merge with later change blocks when the separating equal run is short.
		let endBlock = index;
		while (
			endBlock + 2 < blocks.length
			&& blocks[endBlock + 1].kind === "equal"
			&& blocks[endBlock + 1].ops.length <= DIFF_CONTEXT_LINES * 2
			&& blocks[endBlock + 2].kind === "change"
		) {
			endBlock += 2;
		}
		const hasBeforeContext = index > 0 && blocks[index - 1].kind === "equal";
		const hasAfterContext = endBlock + 1 < blocks.length && blocks[endBlock + 1].kind === "equal";
		const beforeBlock = hasBeforeContext ? blocks[index - 1] : undefined;
		const afterBlock = hasAfterContext ? blocks[endBlock + 1] : undefined;
		const contextBefore = beforeBlock?.ops.slice(-DIFF_CONTEXT_LINES,) ?? [];
		const contextAfter = afterBlock?.ops.slice(0, DIFF_CONTEXT_LINES,) ?? [];
		const windowStart = beforeBlock
			? beforeBlock.start + beforeBlock.ops.length - contextBefore.length
			: block.start;
		const windowEnd = (afterBlock?.start ?? blocks[endBlock].start + blocks[endBlock].ops.length)
			+ (afterBlock ? contextAfter.length : 0);
		const body: string[] = [];
		let beforeCount = 0;
		let afterCount = 0;
		for (let z = windowStart; z < windowEnd; z++) {
			const op = ops[z];
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

/** Unified line diff of two texts (3 context lines) capped at `maxLines` output lines. */
export function formatUnifiedDiff(
	beforeText: string,
	afterText: string,
	path: string,
	maxLines: number,
): { diff: string; diffTruncated: boolean; added: number; removed: number; } {
	const before = beforeText.split("\n",);
	const after = afterText.split("\n",);

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
		...before.slice(0, prefix,).map((line,) => ({ type: "equal" as const, line, }),),
		...editScript(
			before.slice(prefix, before.length - suffix,),
			after.slice(prefix, after.length - suffix,),
		),
		...before.slice(before.length - suffix,).map((line,) => ({ type: "equal" as const, line, }),),
	];
	const added = ops.filter((op,) => op.type === "insert",).length;
	const removed = ops.filter((op,) => op.type === "delete",).length;

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
