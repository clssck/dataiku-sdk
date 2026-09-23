# Changelog

## Unreleased

- Bun 1.4.2 is now the minimum supported version (`engines.bun >=1.4.2`); CI tests 1.4.2 only. On an older Bun, `dss` exits 2 with a JSON error that tells you to run `bun upgrade`.
- Fix: when `dss` runs under Bun itself (`bunx --bun`, `bun --no-env-file`), canary builds such as `1.4.3-canary.12` pass the minimum-version check again; `Bun.semver` range matching rejected every prerelease. The Node and Bun launch paths now share one release-part comparison (`bin/bun-version.js`).

## 3.6.1

- Fix: the Node-launched `dss` checks the Bun version (release part only, so canary builds pass) before spawning Bun, so an old Bun gets the JSON error instead of its own argument-parsing failure.
- `batch --dry-run` has a regression test proving it applies the same meta-command flag checks as a direct run.
- Remove the unused `DSS_LOAD_SOURCE` launcher variable; load source comes only from where the CLI module actually lives.

## 3.6.0

- Fix: a non-2xx response whose body misses the request deadline keeps its real HTTP status instead of becoming a bare network timeout.
- Fix: `target_absence_unverifiable` (a masked `403` on an explicit target key) exits 2 like other permission/environment failures; exit 1 is reserved for usage errors.
- Fix: an environment variable set to `""` counts as set everywhere, so `.env` can no longer fill it (`DATAIKU_URL=""` clears a `.env` value).
- Fix: `bin/dss.js` runs source in a checkout instead of a leftover `dist/`, never trusts an inherited `DSS_LOAD_SOURCE`/`DSS_BUILD_REVISION`, and checks the Bun version against `engines.bun` before running, with a JSON error.
- Release: the `version` lifecycle moves `## Unreleased` entries under the new version heading, and `bun run check` fails when the current version has no section; the history is split into 3.2.0–3.5.0 sections by release tag.
- Packaging: stop building and shipping the unreachable `packages/types/dist/`; `prepublishOnly` reuses `prepack`; drop the duplicate `bundledDependencies`.
- Internal: one meta-command validator for direct and batch runs; one argv positional scanner; no in-loop re-check of cleanup incarnation bindings; one `isRecord`/`asRecord`; saved-model validators live in `resources/base.ts`; `Content-Type` is sent only with a body; `retryMaxAttempts` is documented as total attempts.
- Docs: skill names `dss doctor`, drops SDK test-fixture text, and states the one empty-variable rule; README token figures defer to `bun run test:tokens`; a test now machine-checks the API coverage matrix counts and every CLI/SDK mapping; `.gitignore` covers `install-skill` project targets.

## 3.5.0

- CI tests both the minimum supported Bun (1.4.0) and the latest (1.4.2) on all three operating systems; the release workflow runs 1.4.2; `@types/bun` ^1.4.2.
- Lint: `dss/no-uncoded-errors` rejects built-in `Error`/`TypeError`/`RangeError`/`SyntaxError` throws and `reject(new Error(...))` anywhere in `src/`; `no-raw-json-parse-in-resources` requires resource `JSON.parse` to sit in the `try` block of a try/catch; `no-usage-literals` rejects hand-written usage text; `no-direct-process-env` confines environment reads to `env.ts`, `runtime.ts`, and `config.ts` (the few other reads carry a justified disable comment). `node:crypto` `createHash` is restricted to `sha256Hex` in `src/`. `bun run lint` now covers all of `tests/` and `scripts/`.
- Every error the SDK and CLI raise now carries a stable code; nothing reaches agents as an uncoded `internal_error` by accident. Caller and state problems are `validation_failed` (exit 1); a missing project key, URL, or API key from the SDK client is `missing_required_flag` (exit 1, matched by type instead of by message text); a bad CA bundle path is `invalid_flag_value`; malformed 2xx DSS responses (missing body, id, job id, recipe name, archive member) are one canonical `DataikuError` classified `unexpected_response` (exit 2) via `unexpectedResponseError`, and non-JSON bodies use the existing `nonJsonResponseBody` form; Project Git future failures and timeouts are `long_running_failure` (exit 4); genuine invariants (missing bundled skill, missing command syntax, stalled local writes) are explicit `internal_error` (exit 2).
- Fix: 113 more hand-written usage strings in `requireArgs` calls and `Usage:` messages are rendered from the command syntax; many had drifted from the real syntax.
- Security: `.env` files now supply only `DATAIKU_*`, `NODE_TLS_REJECT_UNAUTHORIZED`, and `NODE_EXTRA_CA_CERTS`; other keys are ignored, so a project `.env` can no longer redirect `DSS_CONFIG_DIR` (and with it saved credentials).
- Fix: `cleanup --dry-run --apply` is now a usage error instead of running the destructive replay; `auth login --dry-run` is rejected instead of logging in and saving credentials; meta commands (`doctor`, `auth`, `install-skill`, `agent`, `version`, `cleanup`, `fixtures`, `batch`) now reject flags they do not support.
- Fix: `--expect-hash` accepts uppercase hex in both the SDK and CLI `code-env set-definition` paths (including `--dry-run`); commands without a plan mode (e.g. `doctor`, `fixtures`) reject `--plan` instead of running live; `auth login` advertises its `--plan` preview.
- Fix: SQL query failures surface as DSS `validation_failed` errors (exit 2, true 2xx status) and non-JSON SQL result streams as `unexpected_response`, instead of `internal_error`; non-JSON bodies are summarized by one shared helper.
- Fix: `batch --dry-run` step errors report their own step's resource/action instead of a concurrently validated sibling's.
- Fix: `--plan` now matches the request actually sent for `scenario create` (default params), `scenario active-set` (merged light status, listed under `requests`), `flow-zone create` (default color, no stray `projectKey`), `dataset create` and `recipe create` (both built by the same body builders as the real request), `folder create` (full `params`), `saved-model evaluate-version` (query flags), `plugin install-from-git`/`update-from-git`/`create-dev` (null git fields), `plugin rename`/`move` (leading-slash paths), `bundle export` (no body), and `wiki create` (content is a follow-up update, listed under `requests`). A test now runs every documented write example with `--plan` and for real against a mock server and requires identical method, endpoint, and body, skipping plans marked `exact: false` and deliberately redacted payloads.
- Security: `plugin create-dev --plan` no longer echoes credentials embedded in `--repository`; the URL is validated like `install-from-git`.
- Command syntax is now structured data: `src/cli/command-syntax.json` holds a syntax tree per command (words, positionals, flags with value/enum/note, optional groups, required choices). Every `usage` line is rendered from it, and the contract fields (flags, required flags and choices, value hints, positionals, stdin/data inputs, dry-run, local-file output, aliases, project scope, idempotency flags, cleanup commands) are derived by walking it; the regex parsing of usage prose is gone, as are 281 hand-copied usage strings in handler error messages (now rendered) and 26 abbreviated `Usage:` messages that had drifted (e.g. `scenario active-set <id> <true|false>`). The registry is byte-identical except: `recipe clone` now advertises its optional `source` positional (the old parser only recognized uppercase positionals in choices), and `wiki update`/`insight create` render their required choice with uniform ` | ` separators.
- Fix: `batch --dry-run` accepts `notebook unload-jupyter --all`; the CLI's required-positional count had treated `<name> <sessionId>` as required even though `--all` replaces them, contradicting the registry.
- Plans that depend on live DSS state (`dataset clone`, `folder create` without `--connection`, `variable set`, `app create-instance` with a generated key, `saved-model import-mlflow-version-from-folder`) now say so with `exact: false` and a `reason`, like `recipe run` and `api-service add-prediction-endpoint`, instead of placeholder values.
- Remove the unused `src/utils/pagination.ts`, the stray `Product_Aggregated.csv.gz` and `VALIDATION-2026-09-14.md`; share one SHA-256 helper and case-insensitive hash pattern, one set of poll defaults, and one schema export surface (`export *`), which also restores the previously dropped `CodeEnvLogSummaryArray` and `SqlNotebookHistory` exports.
- CI runs on pull requests and pushes to `main`; SECURITY.md covers the current `3.x` line; docs no longer claim `--dry-run` never contacts DSS, list the removed `--json` flag, or deny the `dss plugin` authoring commands. CLI test harness defaults to `DATAIKU_DISABLE_ENV=1` so the checkout `.env` cannot leak into tests.
- Direct skill agents to always invoke the installed `dss` command instead of calling `bin/dss.js` through Bun; clarify source/built `.env` lookup and environment-only login, reducing skill and authentication guidance without removing safety requirements.

## 3.4.0

- Flush dataset CSV exports in bounded batches (64 KiB) at row boundaries, before truncation limits, and at EOF: byte-identical output while a 200k-row (8.96 MB) export drops from 16.6 s to 0.5 s.
- Honor `DataikuGetOptions.noRetry` in the client `get` path and Git transport, and give every futures, jobs, ML-training, and Project Git wait loop a spent-budget single observation: the first request always happens (capped by the per-request timeout), no later request starts after the budget expires, and the structured timeout reports the last observed state without clamping the first request.
- Serve CLI discovery output schemas from the committed `src/generated/action-output-schemas.json`: `build` regenerates it ahead of `tsc`, while `check` and `prepack`/`prepublishOnly` verify freshness (after and before compilation respectively); full-registry and scoped contract bytes are unchanged, source checkouts work without `dist`, and scoped discovery latency drops by about half.
- Enforce the project map metadata budget through the client's total request timeout (1500 ms), distinguishing deadline expiry from earlier per-request failures; `FlowZones.list` accepts `DataikuGetOptions`.
- Correct skill release examples to snapshot permissions before mutations and reserve cleanup for rollback; remove the blanket credential-free planning claim and document schema file input and unfiltered log export.
- Honor `Retry-After` seconds and HTTP dates within existing retry eligibility, the 30-second automatic wait cap, and total request deadlines.
- Materialize discovery entries per action and schemas on access; defer runtime-only SDK imports while preserving full registry output.
- Extract the first visible HTML error line without allocating an array for every line, retaining carriage returns, decoding, and sanitization.
- Serialize releases around one versioned, integrity-checked tarball tested on every platform; verify npm metadata and canonical download availability before pushing the candidate commit/tag atomically. Preserve candidates for safe failed-job recovery without automatic republishing.

## 3.3.0

- Use platform-aware basenames for local MLflow archive uploads, keeping Windows parent directories out of multipart filenames.
- Build scoped discovery metadata only for requested resources, generate action indexes without payload schemas, and lazily materialize bootstrap sections; preserve registry bytes and global missing-field recovery hints.
- Bound SDK HTTP error bodies using the configured response limit, preserve UTF-8 boundaries and retry/deadline rules, and expose trusted truncation state through error wrappers and CLI diagnostics.
- Route skill discovery directly to full entries for writes and resource-only action indexes; accelerate optional native token measurements while retaining pinned JS parity and validation checks.
- Validate real Application Designer download-folder and form-variable references; report deleted download folders instead of silently skipping them.
- Self-provision owned Application Designer templates in the live suite, exercise variable/scenario/download and version workflows, and retire only proven unused creation reservations without cleanup requests or project-key reuse.
- Let `app create-instance` generate a cryptographically random project key when `targetProjectKey` is omitted, preserving the display name without requiring global project visibility. Keep strict checks for explicit keys, future/cleanup identity guards, and allocation-free plans; expose fresh-key creation in masked-403 recovery guidance.
- Let `app successor-preflight` and `app create-successor-instance` omit `--to` and generate the successor target key during apply: preflight, plan, and dry-run stay targetless and allocation-free (`targetProjectKeyGeneratedDuringApply`, `targetPreflight:"not-applicable-generated-key"`), source/template/optional-ACL gates still run, the key is generated once before the single POST, cleanup binds the creation future plus `creationTag`, and an explicit `--to` keeps the strict masked-403 absence guard with no POST.
- Preserve bounded, credential-redacted DSS messages and error types in ambiguous mutation failures without enabling retries or weakening unknown-outcome handling.
- Require app-release sharing checks: snapshot and compare intended users, groups, and roles; restore approved missing access without dropping newer grants, then verify with the intended identities.
- Preserve guarded app-deletion receipts and host ownership markers across cleanup failures; remove empty private host directories without widening their permissions.
- Use owned Filesystem fixtures with two-principal inherited ACLs, DSS-selected managed-folder storage, and native-only code-environment rebuilds in live verification.
- Keep unavailable dashboard rendering and meaning deletion explicitly blocked, but optional in current-capability live runs.
- Exercise App catalog reads against owned templates and attribute successful group reads to the read-only directory case instead of blocked account creation.
- Make the app-release rollback example apply cleanup explicitly; distinguish mutation with `--apply` from the default preview.
- Enforce project-owned accumulator-copy, widen-then-assert, and chained-assertion lint rules alongside the native accumulating-spread check. Remove double assertions through typed merge results, direct schema assignments, shared raw-body/Git transport methods, and iterable response streams.
- Add 113 CLI actions across eight new resource families and expanded scenario, metadata, dataset, saved-model, and connection APIs, with SDK exports and mutation preview contracts.
- Add LLMs, knowledge banks, project folders, data collections, users, groups, macros, and plugin administration; include MLflow imports and scoring exports.
- Bound ML training and macro status waits across requests, retries, and response bodies; preserve per-request timeout caps and distinguish request expiry from overall wait expiry. Use adaptive future and Project Git polling unless an interval is explicit.
- Keep early CLI usage errors lightweight with resource-local command loading while preserving structured errors.
- Reject malformed directory and scenario-history responses and missing run identifiers; consolidate SDK identifier validation and recursive secret sanitization.
- Use `next` consistently in metadata previews. Normalize commands to `project-folder settings-get/settings-set`, `data-collection settings-set`, and `scenario payload-get/payload-set/active-set`; remove the old names.
- Remeasure and verify native cross-model discovery-token baselines after API expansion and command renames.
- Publish the DSS 15 API-to-SDK-to-CLI coverage matrix, including remaining gaps and documented endpoint discrepancies.
- Expand the self-provisioning live runner with an `--profile all` selector that widens run-case availability to every profile inside one persistent lab (distinct from the destructive `all` lifecycle verb), persistent `--state-dir` demo labs, retained trained-ML state across iterations, and new expanded case families (bundles, Project Git, scenario statistics/payloads, application manifests, webapp backend state, infrastructure reads, disposable folders/notebooks, disposable global lifecycles).
- Add isolated-disposable global case families (users, groups, meanings, workspaces, data collections, API-deployer infra/services/deployments, plugins, project folders) guarded by exact lab-manifest reservations and ownership markers (`ctx.markerFor`; code-envs bind on the server `desc.creationTag` incarnation snapshot), with per-command identity re-verification, never-delete-blind conflict handling, and no modification of pre-existing accounts or configuration.
- Record explicit per-action unavailability for licensed or disabled instance features, external deployment/auth backends, plugin-store access, and cost-bearing LLM/macro surfaces; every registered action stays enumerated as demonstrated, blocked with a recorded prerequisite, or uncovered, with no silent omissions.
- Serve `plugin git-branches` with GET: DSS 15 rejects POST on that route with 405 `rawMethodPOSTnotsupported` despite the public documentation listing POST.
- Clarify `code-env set-packages` clearing semantics: the package list is replaced wholesale, so an explicitly empty `--packages` value (or an empty `--file`) clears the requested specs, while a call with no package source flag is a usage error.
- Exercise plugin-managed code environments explicitly in the live harness: derived environments are deleted before the parent plugin (no server-side cascade) and plugin updates link the `settings.codeEnvName` association created after plugin creation; MLflow import fixtures use canonical `code`/`data`/`env` packaging inside owned runtime environments.
- Require the DSS 15 `containerExecConfigName` parameter explicitly on MLflow imports and evaluations: send `NONE` for archive imports, folder imports, and evaluation bodies (folder imports no longer inherit), with plan parity and regression coverage.

## 3.2.0

- Optimize all seven skill references, cutting their combined OpenAI text-token count by 15.2% while preserving runnable examples and safety conditions; add per-reference budgets.
- Reduce installed-skill tokens by about 12% across model encodings and shorten discovery prose, retaining safety guidance, schema contracts, and reference links; tighten the skill budget to prevent regrowth.
- Add opt-in cross-model discovery token budgets using an external OMP native module, preserving the mandatory pinned OpenAI gate and adding no production dependency.
- Preserve request IDs and trusted target/timing metadata when enriching SQL failures.
- Treat SQLite missing-column and missing-table errors as nonretryable validation failures, including when SQL start retries are explicitly enabled; preserve ambiguous mutation outcomes.
- Decode bounded HTTP text incrementally to reduce large-response allocations while retaining byte limits, read deadlines, and UTF-8 truncation behavior.
- Load CLI command dependencies lazily and build only the selected resource registry during validation, preserving full discovery contracts.
- Publish SQL JSON exports atomically after query completion, retaining existing permission bits and using the process umask for new files; preserve existing destinations on failure and avoid iterable-string file writes.
- Add a separate, self-provisioning `test:live` runner with persistent setup/run/clean, upfront case validation, optional capability profiles, guarded ownership and local outputs, failure-preserving cleanup journals, deterministic fixtures, explicit registry coverage reports, and regular type/lint gates.
- Normalize explicit boolean flag values and aliases before dispatch; share plan/dry-run resolution across direct, batch, and local commands.
- Bound stalled streaming body reads without imposing a total-duration limit on healthy archive transfers; use backpressure-aware file streaming and publish archive/folder outputs only after completion, preserving existing files on failure.
- Report unreadable JSON/text input files as structured usage errors rather than internal failures.
- Report the actual home target for global skill installation.
- Split the Dataiku skill into a short entrypoint and seven on-demand references; install and hash the complete bundle, detect incomplete reference trees, and preserve unrelated destination files.
