import {
	type Context,
	definePlugin,
	defineRule,
	type ESTree,
	type Variable,
} from "@oxlint/plugins";

function binding(
	context: Context,
	node: ESTree.IdentifierReference | ESTree.BindingIdentifier,
): Variable | undefined {
	for (
		let scope: ReturnType<Context["sourceCode"]["getScope"]> | null = context.sourceCode.getScope(
			node,
		);
		scope;
		scope = scope.upper
	) {
		const variable = scope.set.get(node.name,);
		if (variable) return variable;
	}
	return undefined;
}

function unwrap(node: ESTree.Expression,): ESTree.Expression {
	while (
		node.type === "ParenthesizedExpression" || node.type === "TSNonNullExpression"
		|| node.type === "TSSatisfiesExpression"
	) node = node.expression;
	return node;
}

function memberName(node: ESTree.Expression,): string | undefined {
	if (node.type !== "MemberExpression") return undefined;
	if (!node.computed && node.property.type === "Identifier") return node.property.name;
	if (node.computed && node.property.type === "Literal" && typeof node.property.value === "string") {
		return node.property.value;
	}
	return undefined;
}

const chainedAssertions = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		function inspect(node: ESTree.TSAsExpression | ESTree.TSTypeAssertion,) {
			const inner = unwrap(node.expression,);
			if (inner.type === "TSAsExpression" || inner.type === "TSTypeAssertion") {
				context.report({
					node,
					message: "Replace chained assertions with a correctly typed boundary.",
				},);
			}
		}
		return { TSAsExpression: inspect, TSTypeAssertion: inspect, };
	},
},);

function broadType(node: ESTree.TSType,): boolean {
	if (
		node.type === "TSAnyKeyword" || node.type === "TSUnknownKeyword"
		|| node.type === "TSObjectKeyword"
	) return true;
	if (node.type === "TSUnionType") return node.types.some(broadType,);
	if (node.type === "TSTypeLiteral") return node.members.length === 0;
	return node.type === "TSTypeReference" && node.typeName.type === "Identifier"
		&& node.typeName.name === "Record" && node.typeArguments?.params[1] !== undefined
		&& broadType(node.typeArguments.params[1],);
}

const widenThenAssert = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		function inspect(node: ESTree.TSAsExpression | ESTree.TSTypeAssertion,) {
			const value = unwrap(node.expression,);
			if (value.type !== "Identifier" || broadType(node.typeAnnotation,)) return;
			const variable = binding(context, value,);
			if (!variable || variable.references.some(reference => reference.isWrite() && !reference.init)) {
				return;
			}
			const declaration = variable.defs[0]?.node;
			if (
				declaration?.type !== "VariableDeclarator" || declaration.id.type !== "Identifier"
				|| !declaration.id.typeAnnotation || !declaration.init
				|| !broadType(declaration.id.typeAnnotation.typeAnnotation,)
			) return;
			const original = unwrap(declaration.init,);
			let known = original.type === "Literal";
			if (original.type === "Identifier") {
				known = binding(context, original,)?.defs.some(definition =>
					definition.name.typeAnnotation !== undefined && definition.name.typeAnnotation !== null
					&& !broadType(definition.name.typeAnnotation.typeAnnotation,)
				) ?? false;
			} else if (original.type === "TSAsExpression" || original.type === "TSTypeAssertion") {
				known = !broadType(original.typeAnnotation,);
			}
			if (known) {
				context.report({
					node,
					message: "Keep the initializer's known type instead of widening and asserting it back.",
				},);
			}
		}
		return { TSAsExpression: inspect, TSTypeAssertion: inspect, };
	},
},);

const accumulatorCopy = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		function accumulator(node: ESTree.CallExpression | ESTree.NewExpression,): Variable | undefined {
			for (let parent: ESTree.Node | null = node.parent; parent; parent = parent.parent) {
				if (parent.type !== "ArrowFunctionExpression" && parent.type !== "FunctionExpression") continue;
				const call: ESTree.Node = parent.parent;
				if (
					call.type !== "CallExpression" || call.arguments[0] !== parent
					|| !["reduce", "reduceRight",].includes(memberName(call.callee,) ?? "",)
				) continue;
				const parameter = parent.params[0];
				if (parameter?.type === "Identifier") return binding(context, parameter,);
			}
			return undefined;
		}
		function inspect(node: ESTree.CallExpression | ESTree.NewExpression,) {
			const target = accumulator(node,);
			if (!target) return;
			function isAccumulator(value: ESTree.Argument | undefined,): boolean {
				if (!value || value.type === "SpreadElement") return false;
				const expression = unwrap(value,);
				return expression.type === "Identifier" && binding(context, expression,) === target;
			}
			const callee = node.callee;
			const name = memberName(callee,);
			let copies = false;
			if (callee.type === "MemberExpression") {
				copies = ["concat", "slice", "toSpliced", "toSorted", "toReversed",].includes(name ?? "",)
					&& isAccumulator(callee.object,);
				if (
					callee.object.type === "Identifier" && context.sourceCode.isGlobalReference(callee.object,)
				) {
					if (
						(callee.object.name === "Array" && name === "from")
						|| (callee.object.name === "Object" && ["entries", "values", "keys",].includes(name ?? "",))
					) {
						copies ||= isAccumulator(node.arguments[0],);
					}
					if (callee.object.name === "Object" && name === "assign") {
						copies ||= !isAccumulator(node.arguments[0],)
							&& node.arguments.slice(1,).some(isAccumulator,);
					}
				}
			} else if (callee.type === "Identifier" && context.sourceCode.isGlobalReference(callee,)) {
				copies = (node.type === "NewExpression" && ["Map", "Set",].includes(callee.name,)
					|| node.type === "CallExpression" && callee.name === "structuredClone")
					&& isAccumulator(node.arguments[0],);
			}
			if (copies) {
				context.report({
					node,
					message:
						"Do not copy a growing reduction accumulator on every iteration; update a fresh accumulator instead.",
				},);
			}
		}
		return { CallExpression: inspect, NewExpression: inspect, };
	},
},);

function sourcePath(context: Context,): string {
	return context.filename.replaceAll("\\", "/",);
}

const BUILTIN_ERRORS: Record<string, true> = {
	Error: true,
	TypeError: true,
	RangeError: true,
	SyntaxError: true,
};

function isBuiltinErrorConstruction(node: ESTree.Expression | null | undefined,): boolean {
	return !!node && node.type === "NewExpression" && node.callee.type === "Identifier"
		&& BUILTIN_ERRORS[node.callee.name] === true;
}

/**
 * Every error the SDK or CLI raises carries a stable code: an uncoded built-in
 * error reaches agents as `internal_error` with no way to tell cause from bug.
 * Throw DataikuError (DSS responses), ClientValidationError, or UsageError.
 */
const uncodedErrors = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		if (!sourcePath(context,).includes("/src/",)) return {};
		const report = (node: ESTree.Node,) =>
			context.report({
				node,
				message:
					"Raise a coded error (DataikuError, ClientValidationError with a StableErrorCode, or UsageError), not a built-in Error.",
			},);
		return {
			// Any built-in error construction is an uncoded error, however it
			// leaves (throw, reject, Promise.reject, callbacks, stream.destroy).
			NewExpression(node,) {
				if (isBuiltinErrorConstruction(node,)) report(node,);
			},
		};
	},
},);

/** Response bodies go through the client's parser, which classifies non-JSON (proxy/login pages) as unexpected_response. */
const rawJsonParseInResources = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		if (!sourcePath(context,).includes("/src/resources/",)) return {};
		return {
			CallExpression(node,) {
				const callee = node.callee;
				if (
					callee.type !== "MemberExpression" || callee.object.type !== "Identifier"
					|| callee.object.name !== "JSON" || memberName(callee,) !== "parse"
				) return;
				// Only the `try` block of a try/catch handles a parse failure; a parse in
				// `catch`/`finally`, or under try/finally alone, still leaks SyntaxError.
				const ancestors = context.sourceCode.getAncestors(node,);
				const guarded = ancestors.some((ancestor, index,) =>
					Reflect.get(ancestor, "type",) === "TryStatement"
					&& Boolean(Reflect.get(ancestor, "handler",),)
					&& Reflect.get(ancestor, "block",) === ancestors[index + 1]
				);
				if (!guarded) {
					context.report({
						node,
						message:
							"Guard JSON.parse of DSS bodies so a non-JSON body becomes a classified error, not a SyntaxError.",
					},);
				}
			},
		};
	},
},);
const USAGE_PREFIX = /^dss [a-z][a-z-]* [a-z]/;
const USAGE_MESSAGE = /Usage: dss /;

/** Whether a literal sits where a usage line belongs: a requireArgs/requireNoArgs argument or a `usage`/`*_USAGE` binding. */
function inUsagePosition(node: ESTree.Node,): boolean {
	const parent: unknown = Reflect.get(node, "parent",);
	if (!parent || typeof parent !== "object") return false;
	const type = Reflect.get(parent, "type",);
	if (type === "CallExpression") {
		const callee: unknown = Reflect.get(parent, "callee",);
		const name = callee && typeof callee === "object" ? Reflect.get(callee, "name",) : undefined;
		return name === "requireArgs" || name === "requireNoArgs";
	}
	if (type === "VariableDeclarator") {
		const id: unknown = Reflect.get(parent, "id",);
		const name = id && typeof id === "object" ? Reflect.get(id, "name",) : undefined;
		return typeof name === "string" && /^usage$|_USAGE$/.test(name,);
	}
	return false;
}

/** Usage lines are rendered from src/cli/command-syntax.json; hand-written copies drift. */
const usageLiterals = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		if (!sourcePath(context,).includes("/src/",)) return {};
		const report = (node: ESTree.Node,) =>
			context.report({
				node,
				message: "Render usage with commandUsage(resource, action) instead of writing it by hand.",
			},);
		return {
			Literal(node,) {
				if (typeof node.value !== "string") return;
				if (
					USAGE_MESSAGE.test(node.value,) || (USAGE_PREFIX.test(node.value,) && inUsagePosition(node,))
				) {
					report(node,);
				}
			},
			TemplateElement(node,) {
				if (USAGE_MESSAGE.test(node.value.cooked ?? node.value.raw,)) report(node,);
			},
		};
	},
},);

/** Environment reads live in the modules that own .env loading, provenance, and credentials. */
const ENV_OWNERS = [
	"/src/cli/env.ts", // .env loading, provenance, DATAIKU_DISABLE_ENV
	"/src/cli/runtime.ts", // CLI credential and TLS resolution
	"/src/config.ts", // saved-credential location
];
const directProcessEnv = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		const path = sourcePath(context,);
		if (!path.includes("/src/",) || ENV_OWNERS.some((owner,) => path.endsWith(owner,))) return {};
		return {
			MemberExpression(node,) {
				if (
					node.object.type === "Identifier" && node.object.name === "process"
					&& memberName(node,) === "env"
				) {
					context.report({
						node,
						message:
							"Read environment variables through the env/runtime/config modules, not process.env directly.",
					},);
				}
			},
		};
	},
},);

export default definePlugin({
	meta: { name: "dss", },
	rules: {
		"no-chained-type-assertions": chainedAssertions,
		"no-widen-then-assert": widenThenAssert,
		"no-reduce-accumulator-copy": accumulatorCopy,
		"no-uncoded-errors": uncodedErrors,
		"no-raw-json-parse-in-resources": rawJsonParseInResources,
		"no-usage-literals": usageLiterals,
		"no-direct-process-env": directProcessEnv,
	},
},);
