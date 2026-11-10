# Project lint rules

`index.ts` implements the `dss` Oxlint plugin locally. It is repository code, not a vendored plugin.

- `no-chained-type-assertions`: rejects nested `as` and angle-bracket assertions, including transparent expression wrappers.
- `no-widen-then-assert`: rejects direct assertions on unchanged local bindings explicitly widened to `any`, `unknown`, `object`, an empty object type, or a broad `Record`, when the initializer has explicit concrete type evidence or is a literal. External unknown values and incrementally constructed records remain valid boundaries.
- `no-reduce-accumulator-copy`: identifies the accumulator binding of inline `reduce`/`reduceRight` callbacks and rejects repeated copying through array copy methods, `Array.from`, `Object.assign` into another target, object enumeration, `structuredClone`, and `Map`/`Set` constructors. Shadowed identifiers are separate bindings.
- `no-uncoded-errors` (`src/`): rejects raising a built-in `Error`; raise a coded error (`DataikuError`, `ClientValidationError` with a stable error code, or `UsageError`).
- `no-usage-literals` (`src/`): rejects hand-written usage lines; render them with `commandUsage(resource, action)` from `src/cli/command-syntax.json`.
- `no-direct-process-env` (`src/`): rejects reading `process.env` outside the env/runtime/config modules.
- `no-raw-json-parse-in-resources` (`src/resources/`): rejects an unguarded `JSON.parse` of a DSS body, so a non-JSON body becomes a classified error instead of a `SyntaxError`.

The native `oxc/no-accumulating-spread` rule covers spread copies in reductions and loops. Ordinary immutable mapping remains allowed. All of these checks are errors. They are syntax and lexical-scope checks, not TypeScript assignability analysis or exhaustive performance analysis. No automatic fixes are offered: changing a type boundary or mutation strategy requires review.

Scope lives in `.oxlintrc.json` overrides: every file with an active `dss/*` rule pays an AST transfer into the JS plugin (about 1.3 s of a 2 s run when every rule covered every file). The type-assertion and accumulator rules run on `src/`, `tools/` and `scripts/` (chained assertions not on scripts); the error, usage, and environment rules on `src/`; the JSON-parse rule on `src/resources/`. Tests are exempt. Rules that also check their own path (`no-uncoded-errors`, `no-direct-process-env`, …) keep doing so, so they behave the same when run alone. `no-restricted-imports` bans `node:string_decoder`, `node:child_process`, `fileURLToPath` from `node:url`, and `createHash` from `node:crypto` outside tests and scripts in favour of the Bun-native equivalents.

`bun run verify` type-checks the plugin; `bun run lint` lints it along with the SDK and CLI. `bun test tests/oxlint-rules.test.ts` exercises the actual Oxlint process against accepted and rejected programs. Keep `oxlint` and `@oxlint/plugins` pinned to matching versions.
