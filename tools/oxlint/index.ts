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
			if (
				!variable || variable.references.some(reference => reference.isWrite() && !reference.init,)
			) {
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
				known =
					binding(context, original,)?.defs.some(definition =>
						definition.name.typeAnnotation !== undefined && definition.name.typeAnnotation !== null
						&& !broadType(definition.name.typeAnnotation.typeAnnotation,),
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
					&& Reflect.get(ancestor, "block",) === ancestors[index + 1],
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
		if (!path.includes("/src/",) || ENV_OWNERS.some((owner,) => path.endsWith(owner,),)) return {};
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

const FUNCTION_TYPES: Record<string, true> = {
	FunctionDeclaration: true,
	FunctionExpression: true,
	ArrowFunctionExpression: true,
};

/** Strip parentheses, type assertions, non-null assertions, and optional-chain wrappers. */
function peel(node: ESTree.Node,): ESTree.Node {
	for (;;) {
		switch (node.type) {
			case "ParenthesizedExpression":
			case "TSNonNullExpression":
			case "TSAsExpression":
			case "TSSatisfiesExpression":
			case "TSTypeAssertion":
			case "ChainExpression":
				node = node.expression;
				break;
			default:
				return node;
		}
	}
}

/** The nearest enclosing function, or the outermost ancestor (the Program) at top level. */
function enclosingFunction(node: ESTree.Node,): ESTree.Node {
	let outermost: ESTree.Node = node;
	for (let parent: ESTree.Node | null = node.parent; parent; parent = parent.parent) {
		if (FUNCTION_TYPES[parent.type] === true) return parent;
		outermost = parent;
	}
	return outermost;
}

const FLAG_BAGS: Record<string, true> = { f: true, flags: true, };

/**
 * Whether an expression reads a CLI flag: a member or index access on `f` or
 * `flags`, directly or through one local `const` of the same function that was
 * initialized from such an access.
 */
function flagRead(context: Context, node: ESTree.Node, followConst: boolean,): boolean {
	const value = peel(node,);
	if (value.type === "LogicalExpression") {
		return flagRead(context, value.left, followConst,)
			|| flagRead(context, value.right, followConst,);
	}
	if (value.type === "MemberExpression") {
		const object = peel(value.object,);
		return object.type === "Identifier" && FLAG_BAGS[object.name] === true;
	}
	if (value.type !== "Identifier" || !followConst) return false;
	const declarator = binding(context, value,)?.defs[0]?.node;
	if (declarator?.type !== "VariableDeclarator" || !declarator.init) return false;
	const declaration = declarator.parent;
	return declaration.type === "VariableDeclaration" && declaration.kind === "const"
		&& enclosingFunction(declarator,) === enclosingFunction(value,)
		&& flagRead(context, declarator.init, false,);
}

const NUMBER_COERCERS: Record<string, true> = { Number: true, parseInt: true, parseFloat: true, };

/** A blank flag value is "" and Number("") is 0: numeric flags go through the validating helpers. */
const rawNumberCoercion = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		const path = sourcePath(context,);
		if (!path.includes("/src/cli/",) || path.endsWith("/src/cli/coerce.ts",)) return {};
		const report = (node: ESTree.Node,) =>
			context.report({
				node,
				message:
					"Coerce flag values with num() or numFlag() from src/cli/coerce.ts: Number(), parseInt(), parseFloat() and unary + turn a blank flag into 0 or NaN.",
			},);
		return {
			CallExpression(node,) {
				const callee = node.callee;
				let coerces = false;
				if (callee.type === "Identifier") {
					coerces = NUMBER_COERCERS[callee.name] === true
						&& context.sourceCode.isGlobalReference(callee,);
				} else if (
					callee.type === "MemberExpression" && callee.object.type === "Identifier"
					&& callee.object.name === "Number" && context.sourceCode.isGlobalReference(callee.object,)
				) {
					coerces = ["parseInt", "parseFloat",].includes(memberName(callee,) ?? "",);
				}
				const argument = node.arguments[0];
				if (
					coerces && argument && argument.type !== "SpreadElement" && flagRead(context, argument, true,)
				) {
					report(node,);
				}
			},
			UnaryExpression(node,) {
				if (node.operator === "+" && flagRead(context, node.argument, true,)) report(node,);
			},
		};
	},
},);

const BODY_READERS: Record<string, true> = {
	text: true,
	json: true,
	arrayBuffer: true,
	bytes: true,
	blob: true,
	formData: true,
};

/**
 * `res.text()` and friends read without a size cap or a deadline. Buffered
 * reads go through the client (getText, readBoundedBytes, ...), which holds
 * both. Local files (`Bun.file(...)`) and in-memory `Blob`/`File` are exempt.
 */
const unboundedResponseBody = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		const path = sourcePath(context,);
		if (!path.includes("/src/resources/",) && !path.includes("/src/cli/",)) return {};
		return {
			CallExpression(node,) {
				const callee = node.callee;
				if (
					node.arguments.length > 0 || callee.type !== "MemberExpression"
					|| BODY_READERS[memberName(callee,) ?? ""] !== true
				) return;
				const receiver = peel(callee.object,);
				if (
					receiver.type === "CallExpression" && receiver.callee.type === "MemberExpression"
					&& receiver.callee.object.type === "Identifier" && receiver.callee.object.name === "Bun"
					&& memberName(receiver.callee,) === "file"
				) return;
				if (
					receiver.type === "NewExpression" && receiver.callee.type === "Identifier"
					&& (receiver.callee.name === "Blob" || receiver.callee.name === "File")
				) return;
				context.report({
					node,
					message:
						"Read buffered response bodies through the client's bounded readers (getText, readBoundedBytes, ...): a bare body read has no size cap and no deadline.",
				},);
			},
		};
	},
},);

const STDIN_READERS: Record<string, true> = { readStdinText: true, };
const STDIN_TEST = /\.stdin\b|["']stdin["']|[=!]==?\s*["']-["']|["']-["']\s*[=!]==?/;

/** Whether an enclosing `if`, ternary, or `&&` tests the stdin flag or compares against the `-` marker. */
function guardedByStdinTest(context: Context, node: ESTree.Node,): boolean {
	let child: ESTree.Node = node;
	for (let parent: ESTree.Node | null = node.parent; parent; parent = parent.parent) {
		let test: ESTree.Node | undefined;
		if (
			(parent.type === "IfStatement" || parent.type === "ConditionalExpression")
			&& child !== parent.test
		) {
			test = parent.test;
		} else if (parent.type === "LogicalExpression" && child === parent.right) {
			test = parent.left;
		}
		if (test && STDIN_TEST.test(context.sourceCode.getText(test,),)) return true;
		child = parent;
	}
	return false;
}

function isWithin(node: ESTree.Node, scope: ESTree.Node,): boolean {
	for (let parent: ESTree.Node | null = node.parent; parent; parent = parent.parent) {
		if (parent === scope) return true;
	}
	return false;
}

/**
 * Stdin is read only when asked for. Falling back to it when no source flag is
 * given writes an empty file wherever stdin is closed. A use of the stdin
 * reader needs an enclosing test of the stdin flag or the `-` marker, or an
 * earlier requireContentSource() call in one of its enclosing functions.
 */
const implicitStdin = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		const path = sourcePath(context,);
		if (!path.includes("/src/cli/commands/",) && !path.includes("/src/cli/helpers/",)) return {};
		const requirements: { scope: ESTree.Node; start: number; }[] = [];
		return {
			CallExpression(node,) {
				if (node.callee.type === "Identifier" && node.callee.name === "requireContentSource") {
					requirements.push({ scope: enclosingFunction(node,), start: node.start, },);
				}
			},
			Identifier(node,) {
				const parent = node.parent;
				if (STDIN_READERS[node.name] !== true) return;
				// Uses only: not an import/export name, a declaration, a property key, or a member name.
				if (
					parent.type === "ImportSpecifier" || parent.type === "ExportSpecifier"
					|| (parent.type === "FunctionDeclaration" && parent.id === node)
					|| (parent.type === "MemberExpression" && parent.property === node && !parent.computed)
					|| (parent.type === "Property" && parent.key === node && !parent.computed && !parent.shorthand)
				) return;
				if (guardedByStdinTest(context, node,)) return;
				if (requirements.some((entry,) => entry.start < node.start && isWithin(node, entry.scope,),)) {
					return;
				}
				context.report({
					node,
					message:
						"Read stdin only when asked: guard it with a test of --stdin or the - marker, or call requireContentSource() first, so a closed stdin cannot write an empty file.",
				},);
			},
		};
	},
},);

const WRITERS: Record<string, true> = { writeFile: true, writeFileSync: true, "Bun.write": true, };
const CHMODS: Record<string, true> = {
	chmod: true,
	chmodSync: true,
	fchmod: true,
	fchmodSync: true,
};
const OWNER_ONLY_MODES = new Set([0o600, 0o700, 0o400,],);

function callName(node: ESTree.CallExpression,): string | undefined {
	const callee = node.callee;
	if (callee.type === "Identifier") return callee.name;
	if (callee.type !== "MemberExpression") return undefined;
	const name = memberName(callee,);
	return name === "write" && callee.object.type === "Identifier" && callee.object.name === "Bun"
		? "Bun.write"
		: name;
}

function hasModeOption(node: ESTree.CallExpression,): boolean {
	return node.arguments.some((argument,) =>
		argument.type === "ObjectExpression"
		&& argument.properties.some((property,) =>
			property.type === "Property" && !property.computed
			&& (property.key.type === "Identifier"
				? property.key.name === "mode"
				: property.key.type === "Literal" && property.key.value === "mode"),
		),
	);
}

/**
 * A credential file written and then chmodded is world-readable in between.
 * Owner-only files are created with the mode at open time (see src/config.ts,
 * src/utils/response-file.ts, src/cli/helpers/app-permissions.ts). Syntactic:
 * an owner-only literal mode passed to chmod in a function that also writes a
 * file without a `mode` option.
 */
const chmodAfterWrite = defineRule({
	meta: { type: "problem", schema: [], },
	create(context,) {
		if (!sourcePath(context,).includes("/src/",)) return {};
		const scopes = new Map<
			ESTree.Node,
			{ chmods: ESTree.CallExpression[]; unmodedWrite: boolean; }
		>();
		return {
			CallExpression(node,) {
				const name = callName(node,);
				if (name === undefined) return;
				const key = enclosingFunction(node,);
				let scope = scopes.get(key,);
				if (!scope) {
					scope = { chmods: [], unmodedWrite: false, };
					scopes.set(key, scope,);
				}
				if (CHMODS[name] === true) {
					const mode = node.arguments[1];
					if (
						mode?.type === "Literal" && typeof mode.value === "number"
						&& OWNER_ONLY_MODES.has(mode.value,)
					) scope.chmods.push(node,);
				} else if (WRITERS[name] === true && !hasModeOption(node,)) {
					scope.unmodedWrite = true;
				}
			},
			"Program:exit"() {
				for (const scope of scopes.values()) {
					if (!scope.unmodedWrite) continue;
					for (const call of scope.chmods) {
						context.report({
							node: call,
							message:
								"Create owner-only files with the mode at open time (writeFile mode option, or open with a mode and then write): chmod after the write leaves the file readable in between.",
						},);
					}
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
		"no-raw-number-coercion": rawNumberCoercion,
		"no-unbounded-response-body": unboundedResponseBody,
		"no-implicit-stdin": implicitStdin,
		"no-chmod-after-write": chmodAfterWrite,
	},
},);
