/**
 * Pure parser for omp session logs (`<session-dir>/<timestamp>_<id>.jsonl`).
 *
 * Extracts what the agent-eval report needs: tool calls by tool, every `dss`
 * invocation (from bash `command` strings and eval `code`), failed dss calls
 * with their structured error `code`, token usage, and the final answer.
 * No I/O: callers pass the jsonl text.
 */

export interface SessionTokens {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	total: number;
	/** Provider-reported cost in USD, when the log records one. */
	cost: number;
}

export interface DssInvocation {
	tool: string;
	toolCallId: string;
	/** argv after `dss`; non-literal code arguments render as `{expr}` / `*expr`. */
	argv: string[];
}

export interface DssError {
	code: string;
	error: string;
	exitCode?: number;
}

export interface FailedDssCall extends DssError {
	tool: string;
	toolCallId: string;
	/** The failing invocation, or null when one tool call ran several and the failure is ambiguous. */
	argv: string[] | null;
	/** Every dss invocation of the tool call, when the failure is ambiguous. */
	candidates?: string[][];
}

export interface SessionMetrics {
	model?: string;
	startedAt?: string;
	endedAt?: string;
	toolCalls: number;
	toolCallsByTool: Record<string, number>;
	dssInvocations: DssInvocation[];
	failedDssCalls: FailedDssCall[];
	usageErrors: number;
	tokens: SessionTokens;
	/**
	 * Where tokens go. Every model turn re-reads the whole context, so totals
	 * are roughly turns x context: `firstTurnContextTokens` is the agent's own
	 * fixed overhead (system prompt, tools, skill), `dssOutputChars` is what the
	 * CLI added to that context across the run.
	 */
	turns: { count: number; firstTurnContextTokens: number; lastTurnContextTokens: number; };
	dssOutputChars: number;
	finalAnswer: string;
}

interface ToolCallRecord {
	id: string;
	name: string;
	invocations: string[][];
}

export function parseSessionLog(jsonl: string,): SessionMetrics {
	const metrics: SessionMetrics = {
		toolCalls: 0,
		toolCallsByTool: {},
		dssInvocations: [],
		failedDssCalls: [],
		usageErrors: 0,
		tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, cost: 0, },
		turns: { count: 0, firstTurnContextTokens: 0, lastTurnContextTokens: 0, },
		dssOutputChars: 0,
		finalAnswer: "",
	};
	const calls = new Map<string, ToolCallRecord>();
	// The eval kernel persists across calls, so a helper defined once stays callable.
	const helpers = new Set<string>();

	for (const line of jsonl.split("\n",)) {
		const entry = parseJsonObject(line,);
		if (!entry) continue;
		const timestamp = typeof entry["timestamp"] === "string" ? entry["timestamp"] : undefined;
		if (timestamp) {
			metrics.startedAt ??= timestamp;
			metrics.endedAt = timestamp;
		}
		if (entry["type"] === "model_change" && typeof entry["model"] === "string") {
			metrics.model ??= entry["model"];
			continue;
		}
		if (entry["type"] !== "message") continue;
		const message = asRecord(entry["message"],);
		if (!message) continue;
		const content = Array.isArray(message["content"],) ? message["content"] : [];

		if (message["role"] === "assistant") {
			const usage = asRecord(message["usage"],);
			addUsage(metrics.tokens, usage,);
			if (usage) {
				const context = numberOr0(usage["input"],) + numberOr0(usage["cacheRead"],)
					+ numberOr0(usage["cacheWrite"],);
				metrics.turns.count += 1;
				if (metrics.turns.count === 1) metrics.turns.firstTurnContextTokens = context;
				metrics.turns.lastTurnContextTokens = context;
			}
			const text: string[] = [];
			for (const part of content) {
				const block = asRecord(part,);
				if (!block) continue;
				if (block["type"] === "text" && typeof block["text"] === "string") text.push(block["text"],);
				if (block["type"] !== "toolCall") continue;
				const name = typeof block["name"] === "string" ? block["name"] : "unknown";
				const id = typeof block["id"] === "string" ? block["id"] : `call-${metrics.toolCalls}`;
				metrics.toolCalls += 1;
				metrics.toolCallsByTool[name] = (metrics.toolCallsByTool[name] ?? 0) + 1;
				const invocations = toolCallInvocations(asRecord(block["arguments"],) ?? {}, helpers,);
				calls.set(id, { id, name, invocations, },);
				for (const argv of invocations) {
					metrics.dssInvocations.push({ tool: name, toolCallId: id, argv, },);
				}
			}
			const answer = text.join("",).trim();
			if (answer) metrics.finalAnswer = answer;
			continue;
		}

		if (message["role"] === "toolResult") {
			const call = typeof message["toolCallId"] === "string"
				? calls.get(message["toolCallId"],)
				: undefined;
			if (!call || call.invocations.length === 0) continue;
			const text = content
				.map((part,) => asRecord(part,))
				.filter((block,) => block?.["type"] === "text" && typeof block["text"] === "string")
				.map((block,) => String(block?.["text"],))
				.join("\n",);
			metrics.dssOutputChars += text.length;
			const errors = extractDssErrors(text,);
			errors.forEach((error, index,) => {
				const failed: FailedDssCall = { ...error, tool: call.name, toolCallId: call.id, argv: null, };
				if (call.invocations.length === 1) failed.argv = call.invocations[0] ?? null;
				else if (errors.length === call.invocations.length) {
					failed.argv = call.invocations[index] ?? null;
				} else {
					// Several calls, fewer errors: keep the calls whose words (split on , . whitespace) the error text mentions.
					const mentioned = call.invocations.filter((argv,) =>
						argv.some((word,) =>
							!word.startsWith("-",)
							&& word.split(/[,.\s]+/,).some((token,) => token.length > 3 && error.error.includes(token,))
						)
					);
					if (mentioned.length === 1) failed.argv = mentioned[0] ?? null;
					else failed.candidates = call.invocations;
				}
				metrics.failedDssCalls.push(failed,);
				if (error.code === "usage_error") metrics.usageErrors += 1;
			},);
		}
	}
	return metrics;
}

function toolCallInvocations(args: Record<string, unknown>, helpers: Set<string>,): string[][] {
	const command = args["command"];
	if (typeof command === "string") return extractShellDssInvocations(command,);
	const code = args["code"];
	if (typeof code === "string") return extractCodeDssInvocations(code, helpers,);
	return [];
}

function addUsage(tokens: SessionTokens, usage: Record<string, unknown> | undefined,): void {
	if (!usage) return;
	tokens.input += numberOr0(usage["input"],);
	tokens.output += numberOr0(usage["output"],);
	tokens.cacheRead += numberOr0(usage["cacheRead"],);
	tokens.cacheWrite += numberOr0(usage["cacheWrite"],);
	tokens.total += numberOr0(usage["totalTokens"],);
	tokens.cost += numberOr0(asRecord(usage["cost"],)?.["total"],);
}

// ---------------------------------------------------------------------------
// Structured CLI errors in tool output
// ---------------------------------------------------------------------------

/**
 * The CLI error envelope starts `{"type":"error","error":MSG,"code":CODE`; match
 * it compactly printed, pretty-printed, or as a Python dict repr.
 */
const ERROR_ENVELOPE =
	/\{\s*["']type["']\s*:\s*["']error["']\s*,\s*["']error["']\s*:\s*(["'])((?:\\.|(?!\1)[^\\])*)\1\s*,\s*["']code["']\s*:\s*["']([\w.-]+)["']/g;
const EXIT_CODE = /["']exitCode["']\s*:\s*(\d+)/;

export function extractDssErrors(text: string,): DssError[] {
	const errors: DssError[] = [];
	for (const match of text.matchAll(ERROR_ENVELOPE,)) {
		const error: DssError = {
			code: match[3] ?? "",
			error: (match[2] ?? "").replace(/\\(["'\\/])/g, "$1",).replace(/\\n/g, "\n",),
		};
		const tail = text.slice(match.index + match[0].length, match.index + match[0].length + 400,);
		const nextEnvelope = tail.search(/["']type["']\s*:\s*["']error["']/,);
		const exit = EXIT_CODE.exec(nextEnvelope === -1 ? tail : tail.slice(0, nextEnvelope,),);
		if (exit) error.exitCode = Number(exit[1],);
		errors.push(error,);
	}
	return errors;
}

// ---------------------------------------------------------------------------
// Shell commands
// ---------------------------------------------------------------------------

/** Leading words that do not start a command of their own. */
const SHELL_PREFIX_WORDS: Record<string, true> = {
	"!": true,
	"{": true,
	do: true,
	else: true,
	elif: true,
	if: true,
	while: true,
	until: true,
	time: true,
	command: true,
	exec: true,
	nohup: true,
	env: true,
};

/** `dss` argv for every shell command segment that runs `dss` (or a path ending in `/dss`). */
export function extractShellDssInvocations(command: string,): string[][] {
	const invocations: string[][] = [];
	for (const words of shellSegments(command,)) {
		let index = 0;
		while (index < words.length) {
			const word = words[index] ?? "";
			if (
				SHELL_PREFIX_WORDS[word] === true || word === "then" || /^[A-Za-z_][A-Za-z0-9_]*=/.test(word,)
			) index += 1;
			else if (word === "timeout") index += 2;
			else break;
		}
		const head = words[index];
		if (head !== undefined && /(?:^|\/)dss(?:\.js)?$/.test(head,)) {
			invocations.push(words.slice(index + 1,),);
		}
	}
	return invocations;
}

/**
 * Split a shell string into command segments of unquoted words. Operators
 * (`;`, `&&`, `||`, `|`, `&`, newlines, parentheses, `$(`, backticks) end a
 * segment; redirections and their targets are dropped. Variables stay literal.
 */
export function shellSegments(command: string,): string[][] {
	const segments: string[][] = [];
	let words: string[] = [];
	let word = "";
	let inWord = false;
	let skipNextWord = false;
	const endWord = () => {
		if (inWord) {
			if (skipNextWord) skipNextWord = false;
			else words.push(word,);
		}
		word = "";
		inWord = false;
	};
	const endSegment = () => {
		endWord();
		if (words.length > 0) segments.push(words,);
		words = [];
		skipNextWord = false;
	};
	for (let index = 0; index < command.length; index += 1) {
		const char = command[index] ?? "";
		if (char === "\\") {
			const next = command[index + 1];
			index += 1;
			if (next === "\n" || next === undefined) continue;
			word += next;
			inWord = true;
		} else if (char === "'") {
			const end = command.indexOf("'", index + 1,);
			const stop = end === -1 ? command.length : end;
			word += command.slice(index + 1, stop,);
			inWord = true;
			index = stop;
		} else if (char === '"') {
			index += 1;
			while (index < command.length && command[index] !== '"') {
				if (command[index] === "\\" && /["\\$`]/.test(command[index + 1] ?? "",)) index += 1;
				word += command[index] ?? "";
				index += 1;
			}
			inWord = true;
		} else if (char === "#" && !inWord) {
			const end = command.indexOf("\n", index,);
			index = end === -1 ? command.length : end - 1;
		} else if (char === " " || char === "\t") {
			endWord();
		} else if (char === ">" || char === "<") {
			// `2>&1`, `>file`, `> file`: drop a pending fd number and the target.
			if (inWord && /^\d+$/.test(word,)) {
				word = "";
				inWord = false;
			}
			endWord();
			while (command[index + 1] === ">" || command[index + 1] === "<") index += 1;
			if (command[index + 1] === "&") {
				index += 1;
				while (/\d|-/.test(command[index + 1] ?? "",)) index += 1;
			} else {
				skipNextWord = true;
			}
		} else if (char === "$" && command[index + 1] === "(") {
			endSegment();
			index += 1;
		} else if (";&|\n()`".includes(char,)) {
			endSegment();
		} else {
			word += char;
			inWord = true;
		}
	}
	endSegment();
	return segments;
}

// ---------------------------------------------------------------------------
// Eval code (Python / JavaScript)
// ---------------------------------------------------------------------------

const DSS_LIST_LITERAL = /\[\s*(["'`])dss\1\s*[,\]]/g;
const FUNCTION_DEF = /^([ \t]*)(?:async\s+)?(?:def|function)\s+([A-Za-z_$][\w$]*)\s*\(/gm;
const LAMBDA_DEF = /\b([A-Za-z_$][\w$]*)\s*=\s*(?:lambda\b|(?:async\s*)?\([^)]*\)\s*=>)/g;
const SHELL_TEMPLATE = /\$`(dss\s[^`]*)`/g;
const SHELL_STRING = /(["'])(dss\s(?:\\.|(?!\1)[^\\\n])*)\1/g;

/**
 * `dss` argv from eval code: `["dss", ...]` list literals (subprocess /
 * Bun.spawn), calls of helpers that wrap such a list (`def dss(*a): ...`,
 * remembered across calls in `helpers`), Bun shell templates (`` $`dss ...` ``)
 * and shell strings starting with `dss `.
 */
export function extractCodeDssInvocations(
	code: string,
	helpers: Set<string> = new Set(),
): string[][] {
	const helperBodies: Array<[number, number,]> = [];
	for (const match of code.matchAll(FUNCTION_DEF,)) {
		const indent = match[1]?.length ?? 0;
		const start = match.index;
		const end = blockEnd(code, start, indent,);
		if (DSS_LIST_LITERAL.test(code.slice(start, end,),)) {
			helpers.add(match[2] ?? "",);
			helperBodies.push([start, end,],);
		}
		DSS_LIST_LITERAL.lastIndex = 0;
	}
	for (const match of code.matchAll(LAMBDA_DEF,)) {
		const lineEnd = code.indexOf("\n", match.index,);
		const end = lineEnd === -1 ? code.length : lineEnd;
		if (DSS_LIST_LITERAL.test(code.slice(match.index, end,),)) {
			helpers.add(match[1] ?? "",);
			helperBodies.push([match.index, end,],);
		}
		DSS_LIST_LITERAL.lastIndex = 0;
	}
	const inHelperBody = (offset: number,) =>
		helperBodies.some(([start, end,],) => offset >= start && offset < end);

	const found: Array<{ offset: number; argv: string[]; }> = [];
	for (const match of code.matchAll(DSS_LIST_LITERAL,)) {
		if (inHelperBody(match.index,)) continue;
		const items = splitArguments(code, match.index + 1, "]",);
		found.push({ offset: match.index, argv: items.slice(1,).map(renderArgument,), },);
	}
	for (const helper of helpers) {
		const call = new RegExp(
			`(?<![\\w$.])${helper.replace(/[.*+?^${}()|[\]\\]/g, "\\$&",)}\\s*\\(`,
			"g",
		);
		for (const match of code.matchAll(call,)) {
			if (inHelperBody(match.index,) || /(?:def|function)\s+$/.test(code.slice(0, match.index,),)) {
				continue;
			}
			const items = splitArguments(code, match.index + match[0].length, ")",)
				.filter((item,) => !/^[A-Za-z_]\w*\s*=(?!=)/.test(item,));
			if (items.length > 0) found.push({ offset: match.index, argv: items.map(renderArgument,), },);
		}
	}
	for (const pattern of [SHELL_TEMPLATE, SHELL_STRING,]) {
		for (const match of code.matchAll(pattern,)) {
			const shell = pattern === SHELL_TEMPLATE ? match[1] : match[2];
			for (const argv of extractShellDssInvocations(shell ?? "",)) {
				found.push({ offset: match.index, argv, },);
			}
		}
	}
	return found.sort((left, right,) => left.offset - right.offset).map((entry,) => entry.argv);
}

/** End offset of an indented block (Python `def`) or brace block (JS `function`). */
function blockEnd(code: string, start: number, indent: number,): number {
	const header = code.slice(
		start,
		code.indexOf("\n", start,) === -1 ? code.length : code.indexOf("\n", start,),
	);
	if (/\{\s*$/.test(header,) || /\)\s*\{/.test(header,)) {
		let depth = 0;
		for (let index = code.indexOf("{", start,); index < code.length; index += 1) {
			if (code[index] === "{") depth += 1;
			else if (code[index] === "}" && --depth === 0) return index + 1;
		}
		return code.length;
	}
	let offset = code.indexOf("\n", start,);
	while (offset !== -1) {
		const next = code.indexOf("\n", offset + 1,);
		const line = code.slice(offset + 1, next === -1 ? code.length : next,);
		const lineIndent = line.length - line.trimStart().length;
		if (line.trim() !== "" && lineIndent <= indent) return offset + 1;
		offset = next;
	}
	return code.length;
}

/** Raw top-level comma-separated items from `start` up to the matching `close`. */
function splitArguments(code: string, start: number, close: "]" | ")",): string[] {
	const items: string[] = [];
	let depth = 0;
	let current = "";
	for (let index = start; index < code.length; index += 1) {
		const char = code[index] ?? "";
		if (char === "'" || char === '"' || char === "`") {
			const triple = code.startsWith(char.repeat(3,), index,) ? char.repeat(3,) : char;
			let end = index + triple.length;
			while (end < code.length && !code.startsWith(triple, end,)) end += code[end] === "\\" ? 2 : 1;
			current += code.slice(index, end + triple.length,);
			index = end + triple.length - 1;
			continue;
		}
		if (char === "#" && depth === 0 && close === ")") {
			const lineEnd = code.indexOf("\n", index,);
			index = lineEnd === -1 ? code.length : lineEnd;
			continue;
		}
		if ("([{".includes(char,)) depth += 1;
		else if (")]}".includes(char,)) {
			if (depth === 0) {
				if (current.trim()) items.push(current.trim(),);
				return items;
			}
			depth -= 1;
		} else if (char === "," && depth === 0) {
			if (current.trim()) items.push(current.trim(),);
			current = "";
			continue;
		}
		current += char;
	}
	if (current.trim()) items.push(current.trim(),);
	return items;
}

const STRING_LITERAL = /^(?:[rRuUbBfF]{0,2})("""|'''|"|'|`)([\s\S]*)\1$/;

function renderArgument(item: string,): string {
	if (item.startsWith("*",)) return `*${item.replace(/^\*+/, "",).trim()}`;
	if (item.startsWith("...",)) return `*${item.slice(3,).trim()}`;
	const literal = STRING_LITERAL.exec(item,);
	if (literal) {
		const raw = /^[rR]/.test(item,);
		const body = literal[2] ?? "";
		return raw
			? body
			: body.replace(/\\(.)/g, (_match, escaped: string,) => escaped === "n" ? "\n" : escaped,);
	}
	if (/^-?\d+(?:\.\d+)?$/.test(item,)) return item;
	return `{${item}}`;
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

function parseJsonObject(line: string,): Record<string, unknown> | undefined {
	const trimmed = line.trim();
	if (!trimmed.startsWith("{",)) return undefined;
	try {
		return asRecord(JSON.parse(trimmed,),);
	} catch {
		return undefined;
	}
}

function asRecord(value: unknown,): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value,)
		? value as Record<string, unknown>
		: undefined;
}

function numberOr0(value: unknown,): number {
	return typeof value === "number" && Number.isFinite(value,) ? value : 0;
}

/** `resource.action` of a dss argv (first two positional words), or undefined when unresolved. */
export function dssActionOf(argv: string[],): string | undefined {
	const [resource, action,] = argv;
	if (!resource || !action || resource.startsWith("-",) || action.startsWith("-",)) return undefined;
	if (resource.startsWith("{",) || resource.startsWith("*",)) return undefined;
	return `${resource}.${action}`;
}
