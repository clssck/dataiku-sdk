# Changelog

## Unreleased

- Honor `Retry-After` seconds and HTTP dates within existing retry eligibility, the 30-second automatic wait cap, and total request deadlines.
- Materialize discovery entries per action and schemas on access; defer runtime-only SDK imports while preserving full registry output.
- Extract the first visible HTML error line without allocating an array for every line, retaining carriage returns, decoding, and sanitization.
- Serialize releases around one versioned, integrity-checked tarball tested on every platform; verify npm metadata and canonical download availability before pushing the candidate commit/tag atomically. Preserve candidates for safe failed-job recovery without automatic republishing.

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

- Optimize all seven skill references, cutting their combined OpenAI text-token count by 15.2% while preserving runnable examples and safety conditions; add per-reference budgets.
- Reduce installed-skill tokens by about 12% across model encodings and shorten discovery prose, retaining safety guidance, schema contracts, and reference links; tighten the skill budget to prevent regrowth.
- Add opt-in cross-model discovery token budgets using an external OMP native module, preserving the mandatory pinned OpenAI gate and adding no production dependency.
- Preserve request IDs and trusted target/timing metadata when enriching SQL failures.
- Treat SQLite missing-column and missing-table errors as nonretryable validation failures, including when SQL start retries are explicitly enabled; preserve ambiguous mutation outcomes.
- Decode bounded HTTP text incrementally to reduce large-response allocations while retaining byte limits, read deadlines, and UTF-8 truncation behavior.
- Load CLI command dependencies lazily and build only the selected resource registry during validation, preserving full discovery contracts.
- Publish SQL JSON exports atomically after query completion, retaining existing permission bits and using the process umask for new files; preserve existing destinations on failure and avoid iterable-string file writes.

- Expand the self-provisioning live runner with an `--profile all` selector that widens run-case availability to every profile inside one persistent lab (distinct from the destructive `all` lifecycle verb), persistent `--state-dir` demo labs, retained trained-ML state across iterations, and new expanded case families (bundles, Project Git, scenario statistics/payloads, application manifests, webapp backend state, infrastructure reads, disposable folders/notebooks, disposable global lifecycles).
- Add isolated-disposable global case families (users, groups, meanings, workspaces, data collections, API-deployer infra/services/deployments, plugins, project folders) guarded by exact lab-manifest reservations and ownership markers (`ctx.markerFor`; code-envs bind on the server `desc.creationTag` incarnation snapshot), with per-command identity re-verification, never-delete-blind conflict handling, and no modification of pre-existing accounts or configuration.
- Record explicit per-action unavailability for licensed or disabled instance features, external deployment/auth backends, plugin-store access, and cost-bearing LLM/macro surfaces; every registered action stays enumerated as demonstrated, blocked with a recorded prerequisite, or uncovered, with no silent omissions.
- Serve `plugin git-branches` with GET: DSS 15 rejects POST on that route with 405 `rawMethodPOSTnotsupported` despite the public documentation listing POST.
- Clarify `code-env set-packages` clearing semantics: the package list is replaced wholesale, so an explicitly empty `--packages` value (or an empty `--file`) clears the requested specs, while a call with no package source flag is a usage error.
- Exercise plugin-managed code environments explicitly in the live harness: derived environments are deleted before the parent plugin (no server-side cascade) and plugin updates link the `settings.codeEnvName` association created after plugin creation; MLflow import fixtures use canonical `code`/`data`/`env` packaging inside owned runtime environments.
- Require the DSS 15 `containerExecConfigName` parameter explicitly on MLflow imports and evaluations: send `NONE` for archive imports, folder imports, and evaluation bodies (folder imports no longer inherit), with plan parity and regression coverage.
- Add a separate, self-provisioning `test:live` runner with persistent setup/run/clean, upfront case validation, optional capability profiles, guarded ownership and local outputs, failure-preserving cleanup journals, deterministic fixtures, explicit registry coverage reports, and regular type/lint gates.

- Normalize explicit boolean flag values and aliases before dispatch; share plan/dry-run resolution across direct, batch, and local commands.
- Bound stalled streaming body reads without imposing a total-duration limit on healthy archive transfers; use backpressure-aware file streaming and publish archive/folder outputs only after completion, preserving existing files on failure.
- Report unreadable JSON/text input files as structured usage errors rather than internal failures.
- Report the actual home target for global skill installation.
- Split the Dataiku skill into a short entrypoint and seven on-demand references; install and hash the complete bundle, detect incomplete reference trees, and preserve unrelated destination files.
