# Project lint rules

`index.ts` implements the `dss` Oxlint plugin locally. It is repository code, not a vendored plugin.

- `no-chained-type-assertions`: rejects nested `as` and angle-bracket assertions, including transparent expression wrappers.
- `no-widen-then-assert`: rejects direct assertions on unchanged local bindings explicitly widened to `any`, `unknown`, `object`, an empty object type, or a broad `Record`, when the initializer has explicit concrete type evidence or is a literal. External unknown values and incrementally constructed records remain valid boundaries.
- `no-reduce-accumulator-copy`: identifies the accumulator binding of inline `reduce`/`reduceRight` callbacks and rejects repeated copying through array copy methods, `Array.from`, `Object.assign` into another target, object enumeration, `structuredClone`, and `Map`/`Set` constructors. Shadowed identifiers are separate bindings.

The native `oxc/no-accumulating-spread` rule covers spread copies in reductions and loops. Ordinary immutable mapping remains allowed. All four checks are errors. These are syntax and lexical-scope checks, not TypeScript assignability analysis or exhaustive performance analysis. No automatic fixes are offered: changing a type boundary or mutation strategy requires review.

`bun run check` type-checks the plugin; `bun run lint` lints it along with the SDK and CLI. `bun test tests/oxlint-rules.test.ts` exercises the actual Oxlint process against accepted and rejected programs. Keep `oxlint` and `@oxlint/plugins` pinned to matching versions.
