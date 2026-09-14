# Changelog

## Unreleased

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
- Record explicit per-action blockers for capabilities the lab cannot self-provision: application templates, external SQL connectivity, external Git remotes, external auth backends, plugin-store access, and cost-bearing LLM/macro surfaces; every registered action stays enumerated as demonstrated, blocked with a recorded prerequisite, or uncovered, with no silent omissions.
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
