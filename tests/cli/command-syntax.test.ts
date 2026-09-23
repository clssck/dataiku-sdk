import { describe, expect, it, } from "bun:test";
import { buildCommandRegistry, } from "../../src/cli/contract.js";
import { FLAG_ALIASES, KNOWN_LONG_FLAGS, } from "../../src/cli/flags.js";
import {
	commandSyntaxKeys,
	commandSyntaxTree,
	deriveSyntax,
	renderNodes,
	type SyntaxNode,
} from "../../src/cli/syntax.js";

function malformed(nodes: SyntaxNode[], path: string,): string[] {
	if (nodes.length === 0) return [`${path}: empty sequence`,];
	return nodes.flatMap((node, index,): string[] => {
		const at = `${path}[${index}]`;
		if (typeof node === "string") {
			return node.length > 0 && !/\s/.test(node,)
				? []
				: [`${at}: bad word`,];
		}
		if ("arg" in node) return node.arg.length > 0 ? [] : [`${at}: empty arg`,];
		if ("flag" in node) {
			const problems: string[] = [];
			if (!KNOWN_LONG_FLAGS.has(FLAG_ALIASES[node.flag] ?? node.flag,)) {
				problems.push(`${at}: unknown --${node.flag}`,);
			}
			if (node.values && node.values.length < 2) problems.push(`${at}: enum needs two values`,);
			if (node.values && node.value !== undefined) problems.push(`${at}: both value and values`,);
			if (node.value === "") problems.push(`${at}: empty value`,);
			return problems;
		}
		const alternatives = "optional" in node ? node.optional : node.oneOf;
		if (node.sep !== undefined && node.sep !== " | ") {
			return [`${at}: separator must be "|" or " | "`,];
		}
		return alternatives.flatMap((alternative, alt,) => malformed(alternative, `${at}.${alt}`,));
	},);
}

describe("src/cli/command-syntax.json", () => {
	it("defines exactly the registered commands, each as a well-formed dss usage line", () => {
		const registry = buildCommandRegistry();
		const registered = Object.entries(registry,).flatMap(([resource, actions,],) =>
			Object.keys(actions,).map((action,) => `${resource}.${action}`)
		);
		expect(commandSyntaxKeys().sort(),).toEqual(registered.sort(),);
		const problems = registered.flatMap((key,) => {
			const dot = key.indexOf(".",);
			const nodes = commandSyntaxTree(key.slice(0, dot,), key.slice(dot + 1,),)!;
			return [
				...(nodes[0] === "dss" ? [] : [`${key}: must start with dss`,]),
				...malformed(nodes, key,),
			];
		},);
		expect(problems,).toEqual([],);
	});
});

describe("syntax derivation", () => {
	const line: SyntaxNode[] = [
		"dss",
		"thing",
		"make",
		{ arg: "name", },
		{ flag: "type", value: "TYPE", },
		{
			oneOf: [[{ flag: "data", value: "JSON", },], [{ flag: "data-file", value: "PATH", },], [{
				flag: "stdin",
			},],],
		},
		{ optional: [[{ flag: "connection", value: "CONN", },],], },
		{ optional: [[{ flag: "render", values: ["ascii", "mermaid",], },],], },
		{ optional: [[{ flag: "dry-run", },],], },
	];

	it("renders the usage line", () => {
		expect(renderNodes(line,),).toBe(
			"dss thing make <name> --type TYPE (--data JSON|--data-file PATH|--stdin) [--connection CONN] [--render ascii|mermaid] [--dry-run]",
		);
		expect(renderNodes(line, true,),).toBe(
			"dss thing make <name> --type TYPE (--data JSON|--data-file PATH|--stdin)",
		);
	});

	it("separates unconditional flags, required choices, and optional flags", () => {
		const syntax = deriveSyntax(line,);
		expect(syntax.requiredFlags,).toEqual(["type",],);
		expect(syntax.requiredOneOf,).toEqual([{ oneOf: [["data",], ["data-file",], ["stdin",],], },],);
		expect(syntax.flags,).toEqual([
			"type",
			"data",
			"data-file",
			"stdin",
			"connection",
			"render",
			"dry-run",
		],);
		expect(syntax.valueHints,).toMatchObject({
			type: { valueType: "TYPE", },
			render: { valueType: "enum", enumValues: ["ascii", "mermaid",], },
		},);
		expect(syntax.inputContract,).toEqual({ stdin: true, dataFlag: true, dataFileFlag: true, },);
		expect(syntax.dryRun,).toBe(true,);
		expect(syntax.positionalArguments,).toEqual([{ name: "name", required: true, },],);
	});

	it("never requires a flag from a choice that a positional can satisfy", () => {
		const syntax = deriveSyntax([
			"dss",
			"sql",
			"query",
			{ oneOf: [[{ arg: "SQL", bare: true, },], [{ flag: "sql", value: "QUERY", },],], sep: " | ", },
		],);
		expect(syntax.requiredFlags,).toEqual([],);
		expect(syntax.requiredOneOf,).toEqual([],);
		expect(syntax.positionalArguments,).toEqual([{ name: "SQL", required: false, },],);
	});

	it("makes positionals optional when a flag alternative replaces them", () => {
		const syntax = deriveSyntax([
			"dss",
			"notebook",
			"unload-jupyter",
			{ oneOf: [[{ arg: "name", }, { arg: "sessionId", },], [{ flag: "all", },],], },
		],);
		expect(syntax.positionalArguments,).toEqual([
			{ name: "name", required: false, },
			{ name: "sessionId", required: false, },
		],);
	});
});
