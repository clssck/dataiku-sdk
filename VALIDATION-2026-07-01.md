# Dataiku SDK/CLI — Live Validation Report (2026-07-01)

Version validated: **0.10.1** (`ea60911`). Live DSS instance (Designer node, app-scoped key `dkuaps-`), 4 tutorial projects: `DKU_EXAM_ADV_DESIGNER`, `DKU_TUT_FUZZY_JOIN`, `QS_DATA_PREP`, `QS_ML`.

## Remediation status (2026-07-01) — ALL 28 numbered findings FIXED

All B1–B27 + G1 fixed, with regression tests; full suite **586 pass / 65 skip / 0 fail**, `tsc` clean, and key repros re-verified live against the instance. Landed as focused commits on top of a behavior-preserving refactor:
- `refactor(cli): split cli.ts monolith into per-resource modules` — `src/cli.ts` 10,153 → ~1,150 lines across `src/cli/**` (contract surfaces kept byte-identical).
- `refactor(tests): split cli.test.ts into per-topic suites` — 178 CLI cases preserved across `tests/cli/**`.
- `fix: correct dry-run safety, error taxonomy, and CLI/SDK contract gaps` — B1–B20, B22–B24, G1 (+ regression tests).
- `fix(contract): expose documented flag aliases in machine-readable flags` — completes B21 at the `flags` array level.
- `refactor(cli): remove dead code orphaned by the module split`.

Systemic issues — all resolved: **S1** (dry-run unsafe) by B1/B15 (dry-run honored + advertised, or rejected where unsupported by B9); **S2** (permanent failures misclassified transient) by B3/B17 (+ the `fix(errors)` residual for "cannot read directory as file"); **S4** (global flag leak) by B9; **S3** (the `404 Not Found: Dataiku instance not found` headline) by `fix(cli): contextualize the generic DSS 404 headline` — object-not-found now leads with command context (resource/action/project) while keeping `code:"not_found"`, exit 2, and the raw DSS body under `details.body`.

### Residual reconciliation (raw findings beyond the 28)

Re-auditing the 16 raw findings files surfaced items that were not part of the consolidated B1–B27/G1 list. Final status (suite now **655 pass / 65 skip / 0 fail**):
- **Fixed:** `project-library get <folder>` "Cannot read directory as file" 500 misclassified transient → now `validation` exit 2 (`fix(errors)`, completes S2/B3, which had missed this message). `app instance-manifest`/`save-instance-manifest`/`delete-instance` silently ignored extra positionals → now `usage_error` exit 1 via `requireNoArgs` (`fix(cli)`). `recipe clone --output <new>` without `--copy-output-settings` returned a raw DSS 400 → now a clear usage error pointing at `--copy-output-settings` (`fix(recipe)`). All three have regression tests.
- **Triaged as not-a-defect:** fresh Python recipe has no payload so `recipe get-payload` errors (expected — a new recipe has no code until `set-payload`); empty no-step `scenario run-and-wait` times out (degenerate scenario; DSS never signals completion); `dataset download` returning `{path,rows,...}`+file is self-consistent (`producesLocalFile:true`) — the README `--raw` contract is recipe-payload-only.
- **Also fixed:** commands requiring `--project-key` reported `internal_error` (exit 2) when the project was unresolved → now a usage error (`missing_required_flag`, exit 1) with a hint (`fix(cli): classify unresolved project key as a usage error`, with test).
- **S3 (now fixed):** the generic `404 … Dataiku instance not found` headline is rewritten with command context; `code:"not_found"`/exit 2 unchanged and the raw DSS body preserved under `details.body` (with regression test).


## Method & coverage
- Full machine surface enumerated via `dss commands run`: **278 commands across 42 resources**.
- **Unit baseline**: `bun test` → 559 pass / 65 skip / 0 fail (exit 0). 65 skips are integration tests gated behind `RUN_DATAIKU_INTEGRATION`.
- **Live exercise** (~580 invocations) split into 16 parallel validators. Coverage is by *reachable path*, not a full success-path test of all 278: every resource was touched, but some commands were only negative/dry-run tested or blocked by environment limits or prerequisite bugs (see Environment limitations / Gaps).
  - Phase B (10 agents): every READ command run live; every dry-run-capable mutator run with `--dry-run`; commands needing absent infra run as negative/degradation tests → 441 checks, 369 pass.
  - Phase C (6 agents): full create→modify→delete lifecycles for reachable real mutators in isolated `ZZ_SDKTEST_*` scratch projects → 140 checks, 116 pass. Success paths NOT reached live are listed under Environment limitations and Intentionally-not-exercised.
- **Safety**: all mutations confined to scratch projects. Post-run integrity audit (exact dataset/recipe/scenario name-sets + object counts for dashboards/wikis/insights/notebooks/flow-zones/folders/variables) shows the 4 tutorial projects unchanged from the pre-run baseline; all scratch projects deleted; zero global-meaning leaks. (Audit checked object identity/counts, not full per-object content bytes.)
- Per-cluster detail lives in `local://findings-{c1..c10,muta,mutb,mutc,mutd,mute,mutf}.md`.

## Verdict
The CLI is broadly solid: reads, `--fields` projection, the JSONL error contract, exit codes, idempotency flags (`--if-exists`/`--if-not-exists`), and full dataset/recipe/scenario/wiki/dashboard/insight/discussion/notebook/data-quality lifecycles work against live DSS. An end-to-end `project duplicate → job build → job get/summary/log` chain succeeded. But there are **4 systemic issues** and **~32 distinct defects**, two of which are dangerous for agent use.

---

## Systemic issues
- **S1 — `--dry-run`/`--plan` is not uniformly safe.** Several mutators accept the global `--dry-run` flag but ignore it and mutate anyway (CRITICAL, see B1). Others that do implement it emit inaccurate plans (H3). An agent cannot trust dry-run as a safety gate across the surface.
- **S2 — Error taxonomy misclassifies permanent failures as `transient`/retryable** (exit 3), triggering 3–4× exponential-backoff retries and minutes of wasted latency on deterministic errors (H2).
- **S3 — Misleading 404 headline.** Object-not-found returns the correct `code:"not_found"` but the `error` text reads `404 Not Found: Dataiku instance not found` for missing datasets/insights/jobs/flow-zones/folders/deployables (M4).
- **S4 — Global flag set leaks across commands.** Command-unsupported-but-globally-known flags are silently accepted/mis-consumed rather than rejected (M2).

---

## Bugs (severity-ordered; repro uses `./bin/dss`)

### CRITICAL
**B1. `--dry-run` accepted but ignored → real mutation.** Confirmed first-hand.
- `project-library create-file f.txt --dry-run --project-key <P>` → exit 0 `{created}` and the file **is created** (verified via follow-up `get`). Same for create-folder/put/delete/rename/move.
- `statistics create-worksheet <ds> --data <valid> --dry-run` → **persists a worksheet** despite `--dry-run`.
- Also affects `meaning create/update`. Root cause: these handlers in `src/cli.ts` call the mutating path without checking the parsed `dry-run` flag. Either honor it or reject it as unsupported.

### HIGH
**B2. `recipe clone --replace-payload-text A=B` does a broad, double-applied substring rewrite.** (prior report #2, still present & worse). `mutb_in=mutb_in2` turned `dataiku.Dataset("mutb_in")` into `dataiku.Dataset("mutb_in22")` and corrupted the comment token `mutb_input_comment` → `mutb_in2put_comment`. Needs targeted/anchored replacement and single application. `src/resources/recipes.ts` clone.

**B3. Permanent failures classified as `transient` (retryable, exit 3).** Confirmed first-hand: `project-library get <folder>` (500 "Cannot read directory as file"), `api-service package-summary`/`download-package` on a missing package (500 "Package directory does not exist" — also leaks the server datadir path), `bundle download-exported <missing>` (500 empty body), `business-app get` under license denial. All should be exit 2 (`not_found`/`validation`/`permission`), retryable:no. `src/errors.ts` taxonomy maps bare 500 → transient.

**B4. `project ... --plan` emits wrong endpoints/methods.** `project create --plan` demands an extraneous `--project-key`, then plans `POST /projects/QS_ML/projects/KEY` (real: `POST /public/api/projects/`). `delete`/`duplicate`/`export`/`import`/`settings-set` plans are all wrong vs `src/resources/projects.ts`; `export`/`import` drop the local-file semantics. (`permissions-set --plan` is correct.) Agents that plan-then-apply are misled.

**B5. SDK `new DataikuClient()` crashes without explicit config.** `undefined is not an object (evaluating 'config.url')` — no env fallback, though this is the ergonomic public entry. `new DataikuClient({url, apiKey})` works and live calls succeed. `src/client.ts:343`. Add env-based construction or a clean configuration error.

**B6. `data-quality status <valid dataset>` returns exit 2 404 `not_found`** for valid datasets (reconfirmed on a fresh scratch dataset). Likely wrong endpoint/path in `src/resources/data-quality.ts`.

**B7. `statistics` worksheet payload contract is invalid/incomplete.** The documented `rootCard: column_summary_stats` example fails; DSS requires `dataSpec.datasetSelection`, which is absent from usage/examples. Blocks worksheet creation from the documented shape.

### MEDIUM
- **B8.** `notebook save-jupyter`/`save-sql` **cannot create** a missing notebook (exit 2 404); "save" only updates. There is no `notebook create` — creation path is missing.
- **B9.** Global flag leak: `project list --name X` silently ignored; `project create KEY --name X` mis-consumes `--name` → "Expected 2 args, got 1". `src/cli.ts` `VALUE_FLAGS`.
- **B10.** `batch` cannot run meta commands (`version`, `doctor`, …) → `Unknown resource: version`; `runBatch` dispatches only the resource map.
- **B11.** `job log-url` with a malformed/param-missing URL → `internal_error` exit 2 instead of usage exit 1.
- **B12.** `folder download` negative path leaks HTML into the error text.
- **B13.** `flow-zone graph` can't return the default/full graph: no-id → usage error, default zone id → repeatable 500, while `project flow` works.
- **B14.** `project settings-set --data '{"shortDesc":"x"}'` → exit 3 DSS 500/NPE on a partial payload; a full-settings payload exits 0 but `shortDesc` does not round-trip.
- **B15.** `future abort --dry-run` peeks the future first, so it errors on an absent future instead of returning a plan.
- **B16.** Documented `--sql -` (stdin) form rejected ("Flag --sql requires a value"); only `--sql=-` works.
- **B17.** `code-env delete <missing> --dry-run` (and `--if-exists --dry-run`) → exit 3 transient 500 (`Not a file .../desc.json`) instead of a clean missing/skip result.
- **B18.** `api-service list-packages <missing service>` → exit 0 `[]`, masking a nonexistent service (peer commands 404).

### LOW
- **B19.** `project create` stdout is **double-JSON-encoded** (`"{\"msg\":...}"`): needs two `JSON.parse` calls. Isolated to `create`; `delete` returns a clean object.
- **B20.** `project create` requires `owner` (DSS 400) though usage marks `--owner` optional.
- **B21.** `dataset preview --rows` works and is documented, but the machine `flags` list only advertises `max-rows`.
- **B22.** `dataset validate-build` returns `valid:true` even when storage files are missing — the name oversells the check.
- **B23.** `job wait` machine usage omits `--project-key` though the handler accepts (and needs) it.
- **B24.** `batch --dry-run` is shallow — validates only that resource/action exist, not arg counts/required flags.
- **B25.** `get-jupyter` normalizes `execution_count` null→0, so exact cell round-trip differs (source/outputs persist; clear-outputs works).
- **B26.** `sanitizeFileName` neutralizes separators but keeps a leading `..` as literal text.
- **B27.** `streaming-endpoint create` cleanupHint advertises an unsupported `delete <id> --if-exists`.

---

## Gaps (missing functionality)
- **G1. `meaning` has no delete and no dry-run.** list/get/create/update only → a created meaning is a permanent, instance-global artifact with no CLI removal path. Not safely testable (validated by source only).
- **G2. dry-run not implemented** for `statistics *`, `meaning create/update`, `streaming-endpoint create/update-settings/delete`, `project-library *` — combined with B1 (accepted-but-ignored) this is a safety hole.
- **G3. No `notebook create`** — see B8.

---

## Confirmed FIXED since prior `bugs.md` (v0.6.0)
- `job log-url` `cat-activity-log` 401 → no longer reproduces (clean 404 now).
- `recipe clone --copy-output-settings` shared-storage override → now guarded (exit 1 `invalid_enum`).
- `dataset clone` posting the full source object → NOT reproduced; clone now returns a whitelisted key set only.
- Version drift → `package.json` is 0.10.1, current.
- Test-suite timeout regressions → baseline is 559 pass / 0 fail.

## Environment limitations (clean failures, NOT CLI bugs)
- API/Project Deployer + API-node infra not configured → deployer/bundle-publish/business-app-create degrade to clean `[]`/404 (except the B3 misclassifications).
- No SQL connection → `sql query` positive paths unverifiable.
- App-scoped key lacks permissions for `connection list` (`[]`), `folder create` (403), business-app (license denied).
- Tutorial Filesystem datasets are not materialized on storage → `preview`/`download`/`list-partitions` fail cleanly with `validation_failed`.

## Intentionally not exercised (stated scope)
- Real `code-env create`/`update-packages` builds (multi-minute conda builds on a shared instance) — validated via dry-run + source; the dry-run itself surfaced B17.
- Real infra mutators with no dry-run and no safe sandbox on a Designer node (`api-deployer`/`project-deployer` create/deploy, `bundle publish`, `business-app create-instance`).
