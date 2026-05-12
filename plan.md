# Unproven SDK Function Coverage Plan

Date: 2026-05-11

## Goal

Turn every currently unproven SDK function into one of three explicit states:

1. live-proven with a safe disposable fixture,
2. gated/admin-only with documented prerequisites and cleanup, or
3. intentionally unsupported/unsafe for shared automation.

This plan targets the functions skipped during the live audit, not the already-proven dashboard, data-quality, dataset, scenario, flow-zone create/update/delete, wiki, connection read-only, and read-only project surfaces.

## Non-goals

- Do not add project CRUD.
- Do not mutate existing user assets as fixtures.
- Do not run destructive/admin tests by default.
- Do not fake coverage with mocks for live behavior.
- Do not create compatibility wrappers that hide unsupported DSS behavior.

## Safety model

- Default test mode remains read-only plus reversible disposable objects.
- Mutating/admin coverage must be behind explicit environment gates.
- Every disposable DSS object must use a unique `OMP_` prefix and best-effort cleanup in `finally`.
- Destructive commands must keep `--dry-run`, `--if-exists`, `--if-not-exists`, `--plan`, and cleanup-ledger support where applicable.
- Verification output must distinguish `pass`, `blocked`, `skippedByPolicy`, and `unsupportedByDss`.

## Workstreams

### 1. Code environment mutations

Skipped functions:

- `codeEnvs.create`
- `codeEnvs.setDefinition`
- `codeEnvs.setPackages`
- `codeEnvs.updatePackages`
- `codeEnvs.setJupyterSupport`
- `codeEnvs.delete`

Reason skipped: admin/global mutation.

Plan:

- Add a gated live test mode: `RUN_DATAIKU_ADMIN_MUTATING=1`.
- Require a disposable code-env prefix, defaulting to `OMP_CODE_ENV_`.
- Create a minimal disposable code env with `wait=false` unless the test explicitly needs package installation.
- Exercise:
  - create disposable env,
  - read definition,
  - set definition with a reversible metadata/package-list delta,
  - set packages to a harmless minimal list,
  - call update packages only when the env is disposable,
  - toggle Jupyter support only on the disposable env,
  - delete the disposable env.
- Add cleanup-ledger support for code-env create if not already present.
- Acceptance:
  - gated suite proves all code-env mutators against a disposable env,
  - default suite never mutates admin/global state,
  - interrupted runs leave a cleanup command or ledger entry.

### 2. Job build, wait, log, and abort

Skipped functions:

- `jobs.build`
- `jobs.buildAndWait`
- `jobs.abort`

Reason skipped: would run or abort live jobs.

Plan:

- Use a disposable filesystem dataset as the build target.
- For build/wait/log:
  - create disposable dataset,
  - build it with non-recursive mode,
  - wait for terminal state,
  - fetch job details and logs,
  - delete dataset.
- For abort:
  - prefer a deliberately abortable disposable job only if DSS exposes a safe long-running fixture,
  - otherwise classify `jobs.abort` as gated/manual because aborting a shared or already-finished job is not a meaningful safety proof.
- Add audit output that separates `buildAndWait proven` from `abort blocked: no abortable disposable job`.
- Acceptance:
  - `build`, `buildAndWait`, `get`, `wait`, and `log` proven live using disposable outputs,
  - `abort` proven only if a disposable long-running job can be created; otherwise documented as requiring a dedicated fixture.

### 3. Flow-zone item movement

Skipped functions:

- `flowZones.moveItems`
- `flowZones.moveItem`

Reason skipped: would move existing flow objects.

Plan:

- Create a disposable dataset and a disposable flow zone.
- Move the disposable dataset into the disposable zone using `moveItem`.
- Move it again using `moveItems` if DSS permits moving to another disposable zone or back to default through the API.
- Validate with `flowZones.get`/`graph` or project flow/map that only the disposable object moved.
- Delete disposable zones and dataset.
- Acceptance:
  - no existing project objects are moved,
  - both single-item and multi-item movement are proven live,
  - cleanup returns the project to no disposable residue.

### 4. Project variables set

Skipped function:

- `variables.set`

Reason skipped: would rewrite project variables.

Plan:

- Add a safe merge-only live test that writes a uniquely named standard variable key, then restores the exact previous variables object.
- For replace mode:
  - run only in a gated mutating suite,
  - snapshot full variables first,
  - replace with snapshot plus disposable key,
  - restore original snapshot in `finally`.
- Add CLI ergonomics if missing:
  - `--dry-run` shows current and next variables,
  - `--merge` remains default,
  - `--replace` is explicit and warned in plan output.
- Acceptance:
  - merge mode proven live by adding/removing one disposable key,
  - replace mode either proven under gate with full restore or classified as manual-only,
  - no persistent variable residue.

### 5. Jupyter notebook mutations

Skipped functions:

- `notebooks.saveJupyter`
- `notebooks.deleteJupyter`
- `notebooks.clearJupyterOutputs`
- `notebooks.unloadJupyter`

Reason skipped: DSS public REST API exposes save/delete/session operations but no create endpoint was confirmed for disposable Jupyter notebooks.

Plan:

- Investigate whether PUT to a nonexistent Jupyter notebook creates a notebook in this DSS version.
- If PUT-create works:
  - create disposable notebook with minimal valid `.ipynb` JSON,
  - get it,
  - save an updated version,
  - clear outputs,
  - delete it.
- If PUT-create does not work:
  - require an explicit disposable fixture name via environment variable,
  - never mutate arbitrary existing notebooks,
  - classify delete and clear-output as fixture-required.
- For `unloadJupyter`:
  - only test with a disposable running session if one can be started safely,
  - otherwise classify as session-fixture-required.
- Acceptance:
  - save/clear/delete proven only against a disposable notebook,
  - unload proven only against a disposable running session,
  - no existing notebook content is modified.

### 6. SQL notebook mutations

Skipped functions:

- `notebooks.saveSql`
- `notebooks.deleteSql`
- `notebooks.getSqlHistory`
- `notebooks.clearSqlHistory`

Reason skipped: live project had no SQL notebook fixture; DSS returned the same generic `404 Dataiku instance not found` for nonexistent SQL notebook IDs while the instance was confirmed running.

Plan:

- Treat current DSS response as a missing-object response, not an instance outage.
- Investigate whether a SQL notebook can be created through public REST or another safe DSS API path.
- If create is available:
  - create disposable SQL notebook,
  - save content,
  - get content,
  - get history,
  - clear history,
  - delete notebook.
- If create is not available:
  - require an explicit disposable SQL notebook fixture id,
  - test save/history/delete only against that fixture,
  - otherwise classify these methods as fixture-required.
- Acceptance:
  - no mutation of existing non-disposable SQL notebooks,
  - generic DSS 404 is normalized in audit reporting as missing SQL notebook fixture,
  - delete is only exercised on a disposable or explicitly supplied fixture.

### 7. SQL query execution

Skipped functions:

- `sql.startQuery`
- `sql.streamResults`
- `sql.finishStreaming`
- `sql.query`

Reason skipped: no SQL-compatible live connection was confirmed; filesystem connections are not SQL-compatible.

Plan:

- Extend connection inference to identify SQL-capable connections from safe metadata only.
- Add `RUN_DATAIKU_SQL_LIVE=1` gated tests requiring one of:
  - `DATAIKU_SQL_CONNECTION`, or
  - `DATAIKU_SQL_DATASET_FULL_NAME` backed by a SQL/HDFS-compatible connection.
- Use a harmless `SELECT 1` query.
- Exercise both low-level and high-level paths:
  - `startQuery`,
  - `streamResults`,
  - `finishStreaming`,
  - `query`.
- Preserve the existing unsupported-connection diagnostic for filesystem-backed datasets.
- Acceptance:
  - SQL functions proven when a SQL-compatible fixture exists,
  - default suite reports `blocked: no SQL-compatible connection` instead of failure,
  - no query mutates data.

### 8. Insight create, update, and delete

Skipped functions:

- `insights.create`
- `insights.update`
- `insights.delete`

Reason skipped: raw insight prototypes are DSS object-type-specific and schema-sensitive.

Plan:

- Build a small set of validated insight templates instead of forcing arbitrary raw JSON:
  - text/static content insight if DSS accepts it,
  - chart insight backed by an existing chart or dataset only after validating prototype shape,
  - dashboard-linked insight only when safe.
- Prefer harvesting a valid existing insight prototype from `insights.get` in the playground, then clone it with a disposable name and `listed=false` if DSS supports that safely.
- Add template-level CLI helpers only after one live prototype is proven:
  - `insight create-template --kind text|chart ...`, or
  - documented `--data-file` examples with validated payloads.
- Exercise:
  - create disposable insight,
  - get it,
  - update metadata/params,
  - delete it.
- Acceptance:
  - at least one insight type has a live-proven disposable lifecycle,
  - unsupported insight types fail loudly with stable diagnostics,
  - dashboard aesthetic work can depend on a proven insight-template path.

## Verification matrix

Default verification:

- `bun test`
- `bun run check`
- `bun run format:check`
- `bun run lint`
- `bun run test:integration`
- `bun run test:integration:rigorous`

Gated verification:

- `RUN_DATAIKU_INTEGRATION_MUTATING=1 bun run test:integration:rigorous:mutating`
- `RUN_DATAIKU_ADMIN_MUTATING=1` for code-env mutation tests
- `RUN_DATAIKU_SQL_LIVE=1` for SQL query tests
- fixture-specific gates for Jupyter/SQL notebook session or notebook mutation tests

## Reporting requirements

- Update `observations.md` after each workstream with:
  - methods covered,
  - exact live commands or SDK calls exercised,
  - cleanup outcome,
  - skipped/blocked reason if still unproven.
- Keep `commands --json` metadata in sync with any new safety flags or cleanup commands.
- Do not mark a method as live-proven unless the live environment executed it successfully.

## Recommended order

1. Flow-zone item movement: likely easiest with disposable dataset/zone fixtures.
2. Variables set: safe with snapshot/restore if implemented carefully.
3. Jobs build/wait/log: safe with disposable dataset; abort remains fixture-dependent.
4. SQL query execution: needs SQL-compatible connection fixture.
5. Insight templates: unlocks aesthetic dashboard generation.
6. Jupyter notebook mutations: depends on disposable notebook creation or explicit fixture.
7. SQL notebook mutations: depends on disposable SQL notebook creation or explicit fixture.
8. Code-env mutations: highest blast radius; keep admin-gated and last.

## Progress update — 2026-05-11

Implemented in this pass:

- Flow-zone movement coverage now uses a disposable dataset instead of moving an existing project dataset.
- `flowZones.moveItem` is exercised through the SDK against the disposable dataset.
- `flowZones.moveItems` is exercised through the CLI `flow-zone move` path against the disposable dataset and a second disposable zone.
- Job build coverage now creates a disposable filesystem dataset and covers:
  - `jobs.build`
  - `jobs.wait`
  - `jobs.get`
  - `jobs.log`
  - `jobs.buildAndWait`
  - CLI `job build --dry-run`
  - CLI `job wait`
  - CLI `job build-and-wait --dry-run`
  - CLI `job abort --dry-run`
- Live `jobs.abort` remains intentionally unproven until there is a dedicated long-running disposable job fixture.
- Variable live coverage now verifies CLI `variable set --dry-run`, SDK merge write, CLI `variable get`, and full restore from a pre-mutation snapshot under `RUN_DATAIKU_INTEGRATION_VARIABLES=1`.
- SQL live coverage now has a fixture-gated `SELECT 1` path for `sql.query`, which exercises `startQuery`, `streamResults`, and `finishStreaming`.
- Notebook mutation coverage now has explicit fixture gates:
  - `DATAIKU_TEST_JUPYTER_NOTEBOOK` for no-loss `saveJupyter` and `clearJupyterOutputs` with restore.
  - `DATAIKU_TEST_SQL_NOTEBOOK_ID` for `saveSql`, `getSqlHistory`, and safe high-retention `clearSqlHistory`.
- Admin code-env coverage now has a `RUN_DATAIKU_ADMIN_MUTATING=1` gated disposable lifecycle for create/get/getDefinition/setDefinition/setPackages/setJupyterSupport/updatePackages/delete.
- Doctor environment reporting now surfaces:
  - `RUN_DATAIKU_ADMIN_MUTATING`
  - `RUN_DATAIKU_SQL_LIVE`
- Integration harness now has reusable gated predicates for:
  - admin mutating integration
  - SQL live integration

Verification run:

- `bun run check`
- `bun run format:check`
- `bun run lint` (passed with pre-existing no-underscore-dangle warnings in `src/client.ts`)
- `bun test tests/cli.test.ts tests/integration-rigorous.test.ts tests/connections-variables.test.ts tests/folders-jobs.test.ts tests/flow-zones.test.ts` -> 134 pass, 60 skip, 0 fail

Live DSS state during implementation:

- Initial implementation happened while DSS returned `404 Not Found: Dataiku instance not found`.
- After the playground came back, the read-only, safe mutating, and restore-safe variable suites were live-validated against `TUT_PIVOT_TABLES`.

## Live validation update — 2026-05-11

After the DSS playground became available, live validation was run against `TUT_PIVOT_TABLES`.

Passed:

- `bun run src/cli.ts doctor --capabilities --report-json`
  - connectivity ok
  - project accessible
  - project mutation, job, scenario, Jupyter-save, and connection-mutation permission probes reported `yes`
- `bun run test:integration`
  - 4 pass, 1 skip, 0 fail
- `bun run test:integration:rigorous`
  - 54 pass, 6 skip, 0 fail
- `bun run test:integration:mutating`
  - 5 pass, 0 fail
- `bun run test:integration:rigorous:mutating`
  - 60 pass, 0 fail
- `RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 bun test tests/integration-rigorous.test.ts`
  - 60 pass, 0 fail, 727 expect calls
- `RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_INTEGRATION_REPORT=1 bun test tests/integration-rigorous.test.ts`
  - 60 pass, 0 fail, 727 expect calls
  - repeated report reruns also passed: 60 pass, 0 fail, 727 expect calls

Final local verification after recording results:

- `bun run format:check`
- `bun run check`
- `bun run lint` (passed with pre-existing no-underscore-dangle warnings in `src/client.ts`)
- `bun test tests/cli.test.ts tests/integration-rigorous.test.ts tests/connections-variables.test.ts tests/folders-jobs.test.ts tests/flow-zones.test.ts`
  - 134 pass, 60 skip, 0 fail
- SDK residue scan found no disposable `sdk_cli_it`/`OMP` datasets, data-quality rules, flow zones, wiki articles, dashboards, or insights.

Live-proven in this update:

- `flowZones.moveItem`
- `flowZones.moveItems`
- `jobs.build`
- `jobs.wait`
- `jobs.get`
- `jobs.log`
- `jobs.buildAndWait`
- `variables.set`
- `variables.get`
- `insights.create`
- `insights.update`
- `insights.delete`

Still explicitly unproven/classified:

- `jobs.abort`: dry-run planning is covered; real abort still needs a long-running disposable job fixture.
- SQL query methods: require `RUN_DATAIKU_SQL_LIVE=1` plus `DATAIKU_SQL_CONNECTION` or `DATAIKU_SQL_DATASET_FULL_NAME`.
- Jupyter notebook mutations: require `DATAIKU_TEST_JUPYTER_NOTEBOOK`; `deleteJupyter` and `unloadJupyter` still need disposable notebook/session fixtures.
- SQL notebook mutations: require `DATAIKU_TEST_SQL_NOTEBOOK_ID`; `deleteSql` still needs a disposable create path or explicit disposable fixture.
- Code-env mutations: require `RUN_DATAIKU_ADMIN_MUTATING=1`; the global/admin lifecycle was not executed without that explicit gate.

## Completion update — 2026-05-11

The remaining sandbox-safe/gated coverage was implemented and live-validated after explicit approval to use disposable sandbox mutations.

Additional live-proven coverage:

- `jobs.abort`
  - SDK `jobs.abort` aborted a running disposable long-running Python-recipe build job.
  - CLI `job abort` aborted a second running disposable long-running Python-recipe build job.
  - Both jobs were waited to terminal non-success states.
- SQL query methods:
  - `sql.query`
  - `sql.startQuery`
  - `sql.streamResults`
  - `sql.finishStreaming`
  - CLI `sql query`
  - Validated with SDK and CLI `SELECT 1` probes through a disposable PostgreSQL connection created and deleted by the test under explicit SQL/admin gates.
- Jupyter notebook methods:
  - `saveJupyter`
  - `clearJupyterOutputs`
  - `listJupyterSessions`
  - `deleteJupyter`
  - CLI `save-jupyter`, `clear-jupyter-outputs`, `sessions-jupyter`, and `delete-jupyter`
  - Covered using disposable notebooks created through the public project notebook create endpoint and deleted in the same test.
- SQL notebook methods:
  - `saveSql`
  - `getSqlHistory`
  - `clearSqlHistory`
  - `deleteSql`
  - CLI `save-sql`, `history-sql`, `clear-sql-history`, and `delete-sql`
  - Covered using disposable SQL notebooks created through the public project SQL-notebook create endpoint and deleted in the same test.
- Admin code-env lifecycle:
  - The `RUN_DATAIKU_ADMIN_MUTATING=1` disposable lifecycle test was executed live and passed.

Validation:

- `RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_SQL_LIVE=1 RUN_DATAIKU_ADMIN_MUTATING=1 DATAIKU_SQL_LIVE_CREATE_CONNECTION=1 DATAIKU_SQL_LIVE_*=[configured] bun test tests/integration-rigorous.test.ts`
  - 60 pass, 0 fail, 794 expect calls
- Same command with `RUN_DATAIKU_INTEGRATION_REPORT=1`
  - 60 pass, 0 fail, 794 expect calls
- Residue scan found no disposable `sdk_cli_it`/`OMP` datasets, recipes, flow zones, insights, Jupyter notebooks, SQL notebooks, connections, code envs, dashboards, or wiki articles.

Still explicitly not live-proven:

- `notebooks.unloadJupyter`: no public API path creates a running disposable Jupyter session. A disposable notebook session-create probe returned `405 Method Not Allowed`; dry-run and session listing are covered.

## Additional project validation update — 2026-05-12

The expanded DSS playground now exposes 8 accessible projects. Cross-project validation was run without project CRUD and without mutating existing folders/datasets as fixtures.

Validated projects:

- `DKU_EXAM_DEVELOPER`
- `DKU_TUT_CODE_NOTEBOOKS`
- `TUT_BATCH`
- `TUT_GOVERNANCE`
- `TUT_PIVOT_TABLES`
- `TUT_PYTHON_PREPARE`
- `TUT_R_MARKDOWN`
- `TUT_STATIC_INSIGHTS`

Safety hardening added during this validation:

- Data-quality rule coverage now creates and deletes a disposable filesystem dataset before adding the disposable rule.
- Managed-folder file coverage now attempts disposable managed-folder creation and records a resource-gap finding if DSS forbids creation; it no longer uploads temporary files into existing folders.
- Job abort coverage now records `job-abort-needs-long-running-fixture` when a project's disposable abort recipe reaches a terminal state before abort can be issued.

Validation:

- Fixture discovery and doctor capability checks passed for all 8 projects.
- Read-only matrix passed for all 8 projects:
  - `RUN_DATAIKU_INTEGRATION=1 bun test tests/integration-playground.test.ts tests/integration-rigorous.test.ts`
  - Each project reported 58 pass, 7 skip, 0 fail.
- Mutating matrix passed for all 8 projects:
  - `RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_INTEGRATION_REPORT=1 bun test tests/integration-rigorous.test.ts`
  - Each project reported 60 pass, 0 fail.
- Residue scan found no `sdk_cli_it`/`OMP` objects across datasets, recipes, flow zones, insights, Jupyter notebooks, SQL notebooks, dashboards, wiki articles, managed folders, connections, or code envs.

Additional project-specific classifications:

- `jobs.abort` remains live-proven on projects whose disposable long-running recipe stays running long enough to abort: `DKU_EXAM_DEVELOPER`, `DKU_TUT_CODE_NOTEBOOKS`, `TUT_GOVERNANCE`, and `TUT_PIVOT_TABLES`.
- `jobs.abort` is classified as fixture-limited on `TUT_BATCH`, `TUT_PYTHON_PREPARE`, `TUT_R_MARKDOWN`, and `TUT_STATIC_INSIGHTS`; their disposable abort recipe job failed before abort could be issued.
- Managed-folder file workflow remains fixture-limited for disposable-folder-only safety; DSS returned 403 for disposable managed-folder creation on filesystem connections, so existing folders were not mutated.
- `notebooks.unloadJupyter` remains unchanged: no public API path creates a running disposable Jupyter session.
