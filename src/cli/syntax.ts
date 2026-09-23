import { ClientValidationError, } from "../errors.js";
import commandSyntaxSource from "./command-syntax.json" with { type: "json", };
import { FLAG_ALIASES, KNOWN_LONG_FLAGS, } from "./flags.js";
import type { CommandDefinition, CommandFlagChoice, CommandMeta, } from "./types.js";

/**
 * One element of a command's usage line. `src/cli/command-syntax.json` holds an
 * ordered node list per command; it is the source of truth for the usage text
 * (rendered by {@link commandUsage}) and for every usage-level contract field
 * (derived by {@link commandSyntax}).
 */
export type SyntaxNode =
	/** Literal word: `dss`, the resource, the action. */
	| string
	/** Positional argument, rendered `<name>` (or bare `name` when `bare`). */
	| { arg: string; bare?: boolean; }
	/** Flag, rendered `--flag`, `--flag VALUE`, or `--flag a|b` (enum), plus an optional trailing note. */
	| { flag: string; value?: string; values?: string[]; note?: string; }
	/** Optional group `[a|b]`; each alternative is a node sequence. */
	| { optional: SyntaxNode[][]; sep?: string; }
	/** Required choice `(a|b)`; each alternative is a node sequence. */
	| { oneOf: SyntaxNode[][]; sep?: string; };

export type FlagNode = Extract<SyntaxNode, { flag: string; }>;
type ArgNode = Extract<SyntaxNode, { arg: string; }>;

export interface CommandPositionalMetadata {
	name: string;
	required: boolean;
}

/** Input channels a command accepts for its JSON payload. */
export interface CommandInputContract {
	stdin?: boolean;
	dataFlag?: boolean;
	dataFileFlag?: boolean;
}

/** Usage-level contract fields derived from a command's syntax tree. */
export interface CommandSyntax {
	/** Canonical names of the flags the usage line lists, in order. */
	flags: string[];
	requiredFlags: string[];
	requiredOneOf: CommandFlagChoice[];
	valueHints: Record<string, { valueType: string; enumValues?: string[]; }>;
	inputContract: CommandInputContract;
	positionalArguments: CommandPositionalMetadata[];
	/** Alias spellings (canonical name -> raw names) the usage line uses. */
	aliases: Record<string, string[]>;
	dryRun: boolean;
	producesLocalFile: boolean;
}

const SOURCE: Record<string, SyntaxNode[]> = commandSyntaxSource.commands;

/** The syntax tree for `resource.action`, or undefined when no such command exists. */
export function commandSyntaxTree(resource: string, action: string,): SyntaxNode[] | undefined {
	return Object.hasOwn(SOURCE, `${resource}.${action}`,)
		? SOURCE[`${resource}.${action}`]
		: undefined;
}

function requiredSyntaxTree(resource: string, action: string,): SyntaxNode[] {
	const nodes = commandSyntaxTree(resource, action,);
	if (!nodes) {
		throw new ClientValidationError(
			`No syntax for ${resource} ${action} in src/cli/command-syntax.json.`,
			"internal_error",
		);
	}
	return nodes;
}

export function commandSyntaxKeys(): string[] {
	return Object.keys(SOURCE,);
}

function renderNode(node: SyntaxNode, omitOptional: boolean,): string {
	if (typeof node === "string") return node;
	if ("arg" in node) return node.bare ? node.arg : `<${node.arg}>`;
	if ("flag" in node) {
		const value = node.values ? node.values.join("|",) : node.value;
		return `--${node.flag}${value === undefined ? "" : ` ${value}`}${
			node.note ? ` ${node.note}` : ""
		}`;
	}
	if ("optional" in node) {
		return omitOptional ? "" : `[${renderAlternatives(node.optional, node.sep, omitOptional,)}]`;
	}
	return `(${renderAlternatives(node.oneOf, node.sep, omitOptional,)})`;
}

function renderAlternatives(
	alternatives: SyntaxNode[][],
	sep: string | undefined,
	omitOptional: boolean,
): string {
	return alternatives.map((alternative,) => renderNodes(alternative, omitOptional,)).join(
		sep ?? "|",
	);
}

/** Render a node sequence as usage text; `omitOptional` drops every `[...]` group. */
export function renderNodes(nodes: SyntaxNode[], omitOptional = false,): string {
	return nodes.map((node,) => renderNode(node, omitOptional,)).filter((part,) => part !== "").join(
		" ",
	);
}

const usageCache = new Map<string, string>();

/** The human usage line for `resource.action`, rendered from its syntax tree. */
export function commandUsage(resource: string, action: string,): string {
	const key = `${resource}.${action}`;
	let usage = usageCache.get(key,);
	if (usage === undefined) {
		usage = renderNodes(requiredSyntaxTree(resource, action,),);
		usageCache.set(key, usage,);
	}
	return usage;
}

/**
 * Attach each action's usage line, rendered from its syntax tree. Every command
 * module exports its definitions through this, so a command without a syntax
 * entry fails at load instead of shipping an undocumented contract.
 */
export function withUsage(
	resource: string,
	definitions: Record<string, CommandDefinition>,
): Record<string, CommandMeta> {
	const commands: Record<string, CommandMeta> = {};
	for (const [action, definition,] of Object.entries(definitions,)) {
		commands[action] = { ...definition, usage: commandUsage(resource, action,), };
	}
	return commands;
}

type Visit = (
	node: Exclude<SyntaxNode, string>,
	context: { optional: boolean; oneOf: SyntaxNode[][] | undefined; },
) => void;

function walk(
	nodes: SyntaxNode[],
	visit: Visit,
	context: { optional: boolean; oneOf: SyntaxNode[][] | undefined; } = {
		optional: false,
		oneOf: undefined,
	},
): void {
	for (const node of nodes) {
		if (typeof node === "string") continue;
		visit(node, context,);
		if ("optional" in node) {
			for (const alternative of node.optional) {
				walk(alternative, visit, { ...context, optional: true, },);
			}
		} else if ("oneOf" in node) {
			for (const alternative of node.oneOf) {
				walk(alternative, visit, { ...context, oneOf: node.oneOf, },);
			}
		}
	}
}

function flagNodes(nodes: SyntaxNode[],): FlagNode[] {
	const found: FlagNode[] = [];
	walk(nodes, (node,) => {
		if ("flag" in node) found.push(node,);
	},);
	return found;
}

/** Whether the syntax lists `--name` (the raw spelling, not an alias). */
export function syntaxHasFlag(nodes: SyntaxNode[], name: string,): boolean {
	return flagNodes(nodes,).some((node,) => node.flag === name);
}

/** Leading literal words after `dss` (resource and, when present, action). */
export function syntaxCommandWords(nodes: SyntaxNode[],): string[] {
	const words: string[] = [];
	for (const node of nodes.slice(1,)) {
		if (typeof node !== "string") break;
		words.push(node,);
	}
	return words;
}

function canonicalFlag(name: string,): string {
	return FLAG_ALIASES[name] ?? name;
}

function knownCanonicalFlags(nodes: FlagNode[],): string[] {
	return unique(nodes.map((node,) => canonicalFlag(node.flag,)),).filter((flag,) =>
		KNOWN_LONG_FLAGS.has(flag,)
	);
}

function unique(values: string[],): string[] {
	return [...new Set(values,),];
}

/** Flags of a sequence outside its optional groups (a required choice's alternative). */
function requiredFlagsOf(nodes: SyntaxNode[],): string[] {
	const found: FlagNode[] = [];
	walk(nodes, (node, context,) => {
		if ("flag" in node && !context.optional) found.push(node,);
	},);
	return knownCanonicalFlags(found,);
}

function deriveRequired(
	nodes: SyntaxNode[],
): { requiredFlags: string[]; requiredOneOf: CommandFlagChoice[]; } {
	const requiredFlags = knownCanonicalFlags(
		nodes.filter((node,): node is FlagNode => typeof node !== "string" && "flag" in node),
	);
	const requiredOneOf: CommandFlagChoice[] = [];
	for (const node of nodes) {
		if (typeof node === "string" || !("oneOf" in node)) continue;
		const alternatives = node.oneOf.map(requiredFlagsOf,);
		if (alternatives.length <= 1) {
			requiredFlags.push(...(alternatives[0] ?? []),);
			continue;
		}
		// A choice with a flagless alternative (a positional, e.g. sql query's
		// `(SQL | --sql QUERY | ...)`) cannot be expressed as a flag-only choice:
		// requiring one of the flags would be false. The handler stays the authority.
		if (alternatives.some((alternative,) => alternative.length === 0)) continue;
		requiredOneOf.push({ oneOf: alternatives, },);
	}
	return { requiredFlags: unique(requiredFlags,), requiredOneOf, };
}

const PLACEHOLDER_VALUE = /^(<[^>]+>|[A-Z][A-Za-z0-9_]*)/;

function deriveValueHints(
	flags: FlagNode[],
): Record<string, { valueType: string; enumValues?: string[]; }> {
	const hints: Record<string, { valueType: string; enumValues?: string[]; }> = {};
	// Enumerations take precedence over a placeholder elsewhere in the line.
	for (const node of flags) {
		const flag = canonicalFlag(node.flag,);
		if (node.values && hints[flag] === undefined) {
			hints[flag] = { valueType: "enum", enumValues: node.values, };
		}
	}
	for (const node of flags) {
		const flag = canonicalFlag(node.flag,);
		const placeholder = node.value?.match(PLACEHOLDER_VALUE,)?.[1];
		if (placeholder && hints[flag] === undefined) hints[flag] = { valueType: placeholder, };
	}
	return hints;
}

function derivePositionals(nodes: SyntaxNode[],): CommandPositionalMetadata[] {
	const positionals: CommandPositionalMetadata[] = [];
	walk(nodes, (node, context,) => {
		if (!("arg" in node)) return;
		const arg: ArgNode = node;
		// A positional inside a required choice that offers a flag instead is optional.
		const flagAlternative = context.oneOf?.some((alternative,) => flagNodes(alternative,).length > 0)
			?? false;
		const required = !context.optional && !flagAlternative;
		const existing = positionals.find((positional,) => positional.name === arg.arg);
		if (existing) existing.required ||= required;
		else positionals.push({ name: arg.arg, required, },);
	},);
	return positionals;
}

/** Usage-level contract fields of a syntax tree (flags, requirements, value hints, positionals, inputs). */
export function deriveSyntax(nodes: SyntaxNode[],): CommandSyntax {
	const flags = flagNodes(nodes,);
	const has = (name: string,) => flags.some((node,) => node.flag === name);
	const aliases: Record<string, string[]> = {};
	for (const [raw, canonical,] of Object.entries(FLAG_ALIASES,)) {
		if (has(raw,)) (aliases[canonical] ??= []).push(raw,);
	}
	return {
		flags: knownCanonicalFlags(flags,),
		...deriveRequired(nodes,),
		valueHints: deriveValueHints(flags,),
		inputContract: {
			...(has("stdin",) ? { stdin: true, } : {}),
			...(flags.some((node,) =>
					node.flag === "data" && (node.value !== undefined || node.values !== undefined)
				)
				? { dataFlag: true, }
				: {}),
			...(has("data-file",) ? { dataFileFlag: true, } : {}),
		},
		positionalArguments: derivePositionals(nodes,),
		aliases,
		dryRun: has("dry-run",),
		producesLocalFile: flags.some((node,) =>
			(node.flag === "output" || node.flag === "output-file") && node.value === "PATH"
		),
	};
}

const syntaxCache = new Map<string, CommandSyntax>();

/** Usage-level contract fields for `resource.action`, derived from its syntax tree. */
export function commandSyntax(resource: string, action: string,): CommandSyntax {
	const key = `${resource}.${action}`;
	let syntax = syntaxCache.get(key,);
	if (!syntax) {
		syntax = deriveSyntax(requiredSyntaxTree(resource, action,),);
		syntaxCache.set(key, syntax,);
	}
	return syntax;
}
