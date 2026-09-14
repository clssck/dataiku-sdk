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

export default definePlugin({
	meta: { name: "dss", },
	rules: {
		"no-chained-type-assertions": chainedAssertions,
		"no-widen-then-assert": widenThenAssert,
		"no-reduce-accumulator-copy": accumulatorCopy,
	},
},);
