# Dataiku SDK expanded demonstration — validation report (2026-09-14)

STATUS: FINAL — published in the repository. Basis: the accepted full unfiltered
`run --profile all` iteration-30 (145 cases; both legacy suites exit 0; integrity
verified). Suite exit is nonzero ONLY for the three honestly-reported required blocks
(dashboard export 404/unverified graphics; applications instance-lifecycle missing
template; meaning cleanup 405). The 474-action table in Appendix A classifies every
registered action: demonstrated 380 + setupBaseline 2 + offline 7 + blocked 85 = 474,
zero failed attempts, zero unrun, zero unrepresented. Historical runs 15–29 are
supplementary session-local context only (not committed; see §9); no historical
union is used.

## 1. Scope — all 474 registered actions, exact

The catalogue is `buildCommandRegistry()` (src/cli/contract.ts): **474** resource.action
entries, drift-gated by `tests/live-coverage.ts` (catalogue drift fails the offline gate).
The expanded demonstration classifies every registered action as exactly one of:

- **demonstrated** — actually executed with at least one successful (exitCode 0) command,
  even inside a blocked or failed case; passes via passed-case execution count regardless
  of expected probe exit codes (ledger policy, matching §4);
- **blocked** — declared by a case whose recorded status is blocked/unsupported, with the
  per-action prerequisite in the report;
- **hermetic-offline** — verified offline by the hermetic suite + actual CLI proofs
  (agent.contract, auth.login, batch.run, cleanup.run, commands.run, install-skill.run,
  version.run — the only 7 actions intentionally not declared in live modules);
- no silent omissions: latest declaration scan shows the declared union is 467 live cases +
  the 7 hermetic = 474, with zero live-case declaration gaps; the only remaining
  registry↔module item is `ml.setup.retained` (legitimate inline dispatch in
  live-suite.test.ts).

## 2. Lab identity and persistence

- Persistent lab root: `SDK_LIVE_94AEB85F90C1425A_CLI_ROOT_0` (retained; never cleaned).
- Lab home: `.live-tests/demo-2026-09-14/lab-13fc3204b9f64905-001/` (reports, cleanup.jsonl,
  manifest retained across iterations; `run` reuses, `setup` idempotent, only `clean`/`all`
  delete owned projects).
- State isolation: per-server hash; `--state-dir` labs created on demand, never implicitly
  cleaned.

## 3. Offline gates (FINAL, frozen modules)

- `bun test`: **1637 pass / 76 skip / 0 fail** — 1713 tests, 108 files, 33362 expects,
  149.52s. `bun run check` passed.
- Lint + format: `bun run lint` (oxlint) and `bun run format:check` (dprint) exit 0 with
  0 warnings / 0 errors on the frozen tree.
- Build + platform: build PASS (buildRevision `51d10c7`); packaged platform
  `{ok:true, linux, arm64, bun 1.4.2, version 3.2.0}`.
- Dist provenance: packaged `bun --no-env-file bin/dss.js version` reports
  `gitRevision: 51d10c7`, `buildRevision: 51d10c7fbcaf4b35e2c6c47f28366673419113c4`,
  `source: dist`, `staleBuild: false` (dist outputs retained).
- Packaged doctor: `bun --no-env-file bin/dss.js doctor` → `ok:true`, projectCount 1 on the
  final 51d10c7 build.

## 4. Final live demonstration — unfiltered iteration-30 (ACCEPTED)

Status: **145 cases — 112 passed / 33 blocked / 0 failed**; both legacy suites exit 0;
integrity verified; 1074 commands recorded. The suite exits nonzero ONLY because three
REQUIRED cases are blocked by exact known prerequisites — deliberately not downgraded and
not suppressed:

- `core.dashboard.export` — 404 unavailable route; prerequisite UNVERIFIED (the documented
  route is correct, the graphics configuration is not proven false).
- `applications.instance-lifecycle` — missing application template (the instance has zero
  apps/business apps, proven by raw `listApps`/`listBusinessApps`).
- `infrastructure.meaning` — cleanup 405 (official DSS 15 meaning API has no public delete;
  the bound owned meaning is reused and its cleanup is reported blocked).

### 474-action partition (final)

| Bucket | Count |
|---|---|
| demonstrated | 380 |
| setupBaseline | 2 |
| blocked | 85 |
| failed | 0 |
| declaredNotRun | 0 |
| offline | 7 |
| unrepresented | 0 |
| **total** | **474** (balanced) |

380 demonstrated + 2 setup-baseline + 7 offline = 389 with positive execution evidence;
the remaining 85 are blocked with their exact per-action prerequisite. Zero failed
attempts, zero declared-but-unrun, zero unrepresented. Supplementary historical passes: 0 —
no historical union is used; per-action status comes only from this run.

Classification policy (per-command ledger evidence, not case names):
- **demonstrated** — executed with at least one successful (exitCode 0) command, even
  inside a blocked/failed case; passed-case execution counts regardless of expected probe
  exit codes.
- **setupBaseline** — setup-phase evidence from the report's `setupCases`.
- **blocked** — declared by a case whose recorded status is blocked/unsupported; exact
  prerequisite preserved.
- **failedAttempt** — executed with only nonzero exits, no success, not blocked-explained
  (none in the final run).
- **offline** — the hermetic seven with source + packaged-dist proofs (§5).

### Nonzero-exit command ledger (expected probes + block probes; 14 entries)

The 14 nonzero commands are expected negative probes inside passing cases or
prerequisite-proving probes inside blocked cases; none is an unclassified failure. The
full command logs were reviewed in session-local artifacts (not committed; see §9).
Highlights: `dataset assert-count --expected 999`
(exit 4 expected), `job wait`/`job log-url` probes, `dashboard export` (exit 2, blocked
prereq), `saved-model download-scoring-jar/pmml` (exit 2, blocked), `api-service
delete-package` (exit 2, probe), `user/group create` (exit 2, 403-blocked prerequisite).

### Blocked actions (85, with owning case)

- saved-model.download-scoring-jar — ml.saved-model.scoring-jar
- saved-model.download-scoring-pmml — ml.saved-model.scoring-pmml
- app.manifest — applications.template-prerequisite
- app.instances — applications.template-prerequisite
- app.manifest-version — applications.app.template-surface
- app.verify-instance — applications.app.successor-lifecycle
- app.create-instance — applications.app.permissions-restore-cycle
- app.successor-preflight — applications.app.successor-preflight
- app.create-successor-instance — applications.app.successor-lifecycle
- app.instance-manifest — applications.app.template-surface
- app.save-instance-manifest — applications.app.instance-ops
- app.set-manifest-version — applications.app.set-manifest-version
- app.validate-manifest — applications.app.template-surface
- app.compare-manifest — applications.app.template-surface
- app.delete-instance — applications.app.instance-ops
- app.business-app-instance-permissions — applications.app.business-app-instance-permissions
- app.permissions-snapshot — applications.app.permissions-restore-cycle
- app.permissions-diff — applications.app.instance-ops
- app.permissions-restore — applications.app.permissions-restore-cycle
- business-app.get — applications.business-app.read-surface
- business-app.settings — applications.business-app.read-surface
- business-app.save-settings — applications.business-app-settings
- business-app.instances — applications.business-app.read-surface
- business-app.create-instance — applications.business-app.lifecycle
- business-app.upgrade-instance — applications.business-app.lifecycle
- business-app.install-from-archive — applications.business-app.lifecycle
- api-deployer.deploy — infrastructure.api-deployer.deploy
- bundle.list-imported — core.bundle.automation-node
- bundle.import-from-archive — core.bundle.automation-node
- bundle.import-from-stream — core.bundle.automation-node
- bundle.activate — core.bundle.automation-node
- bundle.preload — core.bundle.automation-node
- bundle.delete-imported — core.bundle.automation-node
- project-deployer.deploy — infrastructure.project-deployer.deploy
- project-git.set-remote — core.project-git.remote-sync
- project-git.remove-remote — core.project-git.remote-sync
- project-git.fetch — core.project-git.remote-sync
- project-git.pull — core.project-git.remote-sync
- project-git.push — core.project-git.remote-sync
- project-git.reset-to-upstream — core.project-git.remote-sync
- project-git.add-library — core.project-git.external-libraries
- project-git.set-library — core.project-git.external-libraries
- project-git.remove-library — core.project-git.external-libraries
- project-git.reset-library — core.project-git.external-libraries
- project-git.push-library — core.project-git.external-libraries
- project-git.push-all-libraries — core.project-git.external-libraries
- streaming-endpoint.list — infrastructure.streaming-endpoints
- streaming-endpoint.get — infrastructure.streaming-endpoints
- streaming-endpoint.create — infrastructure.continuous-activities
- streaming-endpoint.update-settings — infrastructure.streaming-endpoints
- streaming-endpoint.delete — infrastructure.continuous-activities
- continuous-activity.list — infrastructure.continuous-activities
- continuous-activity.status — infrastructure.continuous-activities
- continuous-activity.start — infrastructure.continuous-activities
- continuous-activity.stop — infrastructure.continuous-activities
- meaning.create — infrastructure.meaning
- meaning.delete — infrastructure.meaning
- dashboard.export — core.dashboard.export
- connection.prepare-import — infrastructure.connection-import-surface
- connection.execute-import — infrastructure.connection-import-surface
- user.create — infrastructure.user.external-sync
- user.update — infrastructure.user
- user.delete — infrastructure.user.external-sync
- user.resync — infrastructure.user.external-sync
- user.resync-multi — infrastructure.user.external-sync
- user.external-users — infrastructure.user.external-sync
- user.external-groups — infrastructure.user.external-sync
- user.provision — infrastructure.user.external-sync
- group.create — infrastructure.group
- group.update — infrastructure.group
- group.delete — infrastructure.group
- plugin.install-from-store — infrastructure.plugin.store
- plugin.install-from-git — infrastructure.plugin.git-install
- plugin.update-from-store — infrastructure.plugin.store
- plugin.update-from-git — infrastructure.plugin.git-install
- plugin.set-git-remote — infrastructure.plugin.git-sync
- plugin.delete-git-remote — infrastructure.plugin.git-sync
- plugin.push — infrastructure.plugin.git-sync
- plugin.pull — infrastructure.plugin.git-sync
- plugin.fetch — infrastructure.plugin.git-sync
- plugin.reset-remote — infrastructure.plugin.git-sync
- llm.completions — infrastructure.llm-probes
- llm.embeddings — infrastructure.llm-probes
- knowledge-bank.search — infrastructure.knowledge-bank
- knowledge-bank.clear — infrastructure.knowledge-bank

### Blocker catalogue (by owning case)

| case | required | actions (declared) | prerequisite / observed blocker |
|---|---|---|---|
| core.bundle.automation-node | optional | 6 | Imported-bundle actions (list-imported, import-from-archive, import-from-stream, activate, preload, delete-imported) only apply to a project on an Automation node; the lab credentials target the Design node and live cases cannot substitute server credentials. |
| core.project-git.remote-sync | optional | 6 | Remote-sync actions (set-remote, remove-remote, fetch, pull, push, reset-to-upstream) require an isolated Git remote; the lab never points owned projects at shared or external repositories. |
| core.project-git.external-libraries | optional | 8 | Git library actions attach, update, push, or reset external repository checkouts and need an external Git repository plus a running future to abort; none is reserved for this lab. |
| core.dashboard.export | REQUIRED | 4 | documented dashboard export route returned 404; graphics export prerequisite could not be verified: dashboard.export — Not found: dashboard export in project SDK_LIVE_94AEB85F90C1425A_CLI_ROOT_0 — verify the object identifier and project key. — Hint: Resource was not found (gateway returned HTML)... |
| ml.saved-model.scoring-jar | optional | 1 | saved-model.download-scoring-jar — 403 Forbidden — Error type: forbidden |
| ml.saved-model.scoring-pmml | optional | 1 | saved-model.download-scoring-pmml — 403 Forbidden — Error type: forbidden |
| applications.template-prerequisite | optional | 2 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read. The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.instance-lifecycle | REQUIRED | 5 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read. The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.business-app-settings | optional | 1 | business-app has no owned provisioning verb (install-from-archive/create-instance both need external receipts and no delete verb exists), so save-settings against a business app not owned by this run is denied; configured pre-existing business apps are read-only; business-app list returned 0 busi... |
| applications.app.template-surface | optional | 6 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read; app list returned 0 app template(s). The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.app.instance-ops | optional | 5 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read; app list returned 0 app template(s). The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.app.successor-preflight | optional | 2 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read; app list returned 0 app template(s). The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.app.successor-lifecycle | optional | 4 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read; app list returned 0 app template(s). The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.app.permissions-restore-cycle | optional | 5 | No Dataiku App template prerequisite: set DATAIKU_LIVE_APP_TEMPLATE_ID to an app template this key can read; app list returned 0 app template(s). The applications profile does not guess templates from app list and never writes to external template manifests. |
| applications.app.set-manifest-version | optional | 1 | No owned Dataiku App template: set-manifest-version publishes the persisted template version that new instances inherit, so it is denied against external templates (never written by live cases); no CLI verb creates an owned template from nothing, so the case stays blocked until an owned-template ... |
| applications.app.business-app-instance-permissions | optional | 1 | No Business App instance permission prerequisite: set DATAIKU_LIVE_BUSINESS_APP_ID (a business app id readable by this key), DATAIKU_LIVE_BAPP_INSTANCE_PROJECT (an existing instance project key of that app), and DATAIKU_LIVE_BAPP_USER (a login to query). All three are explicit opt-ins; list outpu... |
| applications.business-app.read-surface | optional | 3 | No Business App prerequisite: set DATAIKU_LIVE_BUSINESS_APP_ID to a Business App id this key can read; business-app list returned 0 business app(s). The applications profile never guesses business-app ids from list output and never writes to external business apps. |
| applications.business-app.lifecycle | optional | 3 | Business App install prerequisites missing: (1) no owned archive fixture at DATAIKU_LIVE_BAPP_ARCHIVE_PATH; (2) no source-verified cleanup contract for the created business-app shell (the official Python client exposes no delete for the shell — only instance projects are deleted), so install woul... |
| infrastructure.user | optional | 6 | user.create — 403 Forbidden — Error type: forbidden |
| infrastructure.group | optional | 5 | group.create — 403 Forbidden — Error type: forbidden |
| infrastructure.meaning | REQUIRED | 5 | meaning lifecycle cleanup is blocked: DSS exposes no public deletion route for meanings (DELETE /meanings/{id} answers 405; the DSS 15 REST reference and the official Python client document list/create/get/update only). Owned meaning sdk_live_94aeb85f90c1425a_meaning_meaning_67 is retained from a... |
| infrastructure.user.external-sync | optional | 7 | user external-users is an instance-global operation with no owned target; the live project sandbox intentionally refuses global mutations — it enumerates users from an external identity supplier, and this run has no lab-owned or explicitly authorized supplier (guard's own response: Global mutatio... |
| infrastructure.connection-import-surface | optional | 5 | No importable table was reported by connection tables for SDK_LIVE_94AEB85F90C1425A_CONNECTION_SQLIMPORT_115: the completed catalog reported zero tables (shared-memory SQLite does not persist between executor sessions; see the SQL-import proof). |
| infrastructure.llm-probes | optional | 2 | LLM completions/embeddings are cost-bearing and the catalog listing is not billing authorization: set DATAIKU_LIVE_FREE_LLM_ID to an explicitly user-authorized free/local LLM id to run the probe. No LLM is invoked without that operator designation. |
| infrastructure.knowledge-bank | optional | 2 | No knowledge bank is available: the CLI has no knowledge-bank list/create action and neither DATAIKU_LIVE_KB_ID nor a lab-owned fixtures.knowledgeBankId was provided. Configure an existing bank id to exercise search (clear runs only on lab-created banks). |
| infrastructure.streaming-endpoints | optional | 5 | No streaming-capable connection (Kafka/MQTT/stream) is configured on this instance: connection list + get type inspection found none and DATAIKU_LIVE_STREAMING_CONNECTION is unset. |
| infrastructure.continuous-activities | optional | 9 | No streaming-capable connection (Kafka/MQTT/stream) is configured on this instance: connection list + get type inspection found none and DATAIKU_LIVE_STREAMING_CONNECTION is unset. |
| infrastructure.deployer-details | optional | 13 | No API deployer or project deployer object exists on this instance (infras, services, deployments, and published projects all empty), so the detail gets have no target; the lists themselves were verified in the same case. |
| infrastructure.plugin.git-sync | optional | 11 | plugin set-git-remote/fetch/push/pull/reset-remote/delete-git-remote need a lab-owned Git remote reachable from the DSS host: set DATAIKU_LIVE_PLUGIN_GIT_REMOTE to an SSH or credential-free HTTPS URL of a repository this lab owns. No remote is guessed. |
| infrastructure.plugin.store | optional | 2 | plugin install-from-store/update-from-store need a store-published plugin owned by this lab: the Dataiku store serves only Dataiku-published plugins, so every candidate is a foreign artifact the ownership ledger can neither bind nor delete. No store request is made. |
| infrastructure.plugin.git-install | optional | 2 | plugin install-from-git/update-from-git need a lab-owned repository whose plugin.json meta.description carries this run's ownership marker; the CLI exposes no plugin commit action, so the lab cannot publish one and every reachable repository is a foreign artifact. No git request is made. |
| infrastructure.api-deployer.deploy | optional | 4 | api-deployer deploy SDK_LIVE_94AEB85F90C1425A_API_D_F8E93CE037E84071B24E1A9C1C047D26 needs an API node on infra SDK_LIVE_94AEB85F90C1425A_API_D_E1984BD97D174273B5A2FBD56A5D9C02: the owned STATIC infra was created with no settings.apiNodes entry (official add_apinode needs a node URL + admin API k... |
| infrastructure.project-deployer.deploy | optional | 4 | project-deployer deploy SDK_LIVE_94AEB85F90C1425A_PROJE_0790DBA623B44E1C8CE430B83BA22A86 needs an Automation node on infra SDK_LIVE_94AEB85F90C1425A_PROJE_8D1D4818B2B845E699B44898372E20EE: the owned infra was created with the official {id, stage, governCheckPolicy} body only, the lab has no Autom... |

### Verified source fixes (final)
- SQL notebook writes aligned with the DSS contract: create requires a matching
  `projectKey` and returns the generated id + full notebook; `cells[0].code` is a string
  (commit f14f6cb).
- `plugin git-branches` served via GET (docs list POST; DSS 15 rejects POST with 405
  `rawMethodPOSTnotsupported`) (f458e56); plugin-managed code-envs are deleted explicitly
  before the parent plugin; plugin updates require the `settings.codeEnvName` association.
- `code-env set-packages` replaces the list wholesale — an explicitly empty value/file
  clears the requested specs (f458e56).
- MLflow: canonical `code`/`data`/`env` packaging; `containerExecConfigName=NONE` defaults
  for folder imports, SDK evaluations, and archive imports; owned runtime pins (51d10c7).
  The pinned package set is a verified fixture baseline, not an isolated cause of the
  earlier kernel failures: an unpinned runtime with explicit `NONE` was not tested.
  The earlier diagnostic `versionDetails("v1", id, projectKey)` call reversed the
  SDK arguments (`savedModelId`, `versionId`, `projectKey`); its 404 is not evidence
  of an instance outage.
- Harness interruption: the first signal stops the child and releases the lab lock (no
  stale lock); regression in tests/live-runner.test.ts.
- Post-run advisory verification (focused iteration-31): the code-env lifecycle now
  executes its declared `code-env.list` action and verifies the owned environment is
  listed. The lifecycle passed, recorded the successful list command, and deleted its
  temporary environment; project integrity was verified. Iteration-30 remains the
  unfiltered 474-action accounting basis; the totals above are unchanged.

### Historical diagnostics (supplementary only)
Iterations 18–29 development chronology, root-cause probes, and incident history are
recorded in the session-local chronology (not committed; see §9). Historical pass data
is supplementary only —
never a substitute for the accepted final iteration (no historical union).

## 5. Offline-7 explicit proof (hermetic + actual)

- Hermetic suite: tests/cli/{agent-surface,auth,batch,agent-contract-accuracy,install-skill,
  doctor-version,machine-contract}.test.ts → 140 pass / 0 fail (earlier scoped run).
- Actual CLI proofs (src AND built artifact `bin/dss.js` → dist): all 7 green, exit 0,
  stderr empty — version.run (source=dist, runtime=bun), agent.contract v2, commands.run
  registry projection, install-skill actual install (SKILL.md 3076B + references/),
  batch.run 1/1, cleanup.run actual valid-ledger run, auth.login mock-local only
  (credentials only under disposable HOME). No live API calls. RE-RUN against the packaged
  contract at the final revision: all 7 green again with `version` reporting
  `source:dist`, `buildRevision:51d10c7fbcaf4b35e2c6c47f28366673419113c4`, `staleBuild:false`
  (prior re-run at f14f6cbc also green).

## 6. Known limitations and residual state (final)

- Residual retained state: the lab root project (`SDK_LIVE_94AEB85F90C1425A_CLI_ROOT_0`);
  two stopped root webapps (`4lF6yms`, `vVknyIm`; UI cleanup returns 401); the owned meaning
  `sdk_live_94aeb85f90c1425a_meaning_meaning_67` (DELETE 405 — DSS has no public meaning
  delete; reused for get/list/update; cleanup reported blocked). Everything else probed
  during development was deleted (recoveries: chronology artifact).
- Inventory: only the root project; exactly the original 4 code environments; exactly the
  original 9 plugins; run-owned account id lists are empty.
- Blocked prerequisites (85 actions across 33 cases; per-case catalogue in §4): zero
  app/business-app templates on the instance; dashboard export 404 with an unverified
  graphics prerequisite; shared-memory SQLite does not survive executor sessions (SQL
  import proven impossible); external Git remotes for project-git and plugin
  git-sync/install; plugin-store ownership; LDAP/SAML for user external sync; 403
  permission walls for user/group creation; Automation/API node targets absent for
  deployers; no streaming-capable connection; knowledge-bank/LLM authorization not
  granted.
- Platform behavior: plugin deletion is asynchronous on this instance; the harness deletes
  plugin-managed code environments explicitly before the parent plugin and awaits delete
  completion.

## 7. Incidents (detail in the chronology artifact)

Recovery events are recorded in the session-local chronology (not committed; see §9).
No unresolved incident affects the accepted
iteration-30 evidence.

## 8. Provenance

- Production SDK/CLI/resources/non-live tests/tools/docs inventory committed as d4b6fc8
  (`feat: expand DSS SDK and CLI resource coverage`). Source-fix wave commits: 59ae393
  (14 files, +424/−92), f14f6cb (8 files, +99/−12, aligning SQL notebook writes + plugin
  env requests; tsc --noEmit pass; 86 focused SQL/plugin tests pass, 263 expects),
  f458e56 (6 files, +159/−21: plugin git-branches GET + explicit-empty package clear), and
  **51d10c7** (production fix: MLflow `containerExecConfigName=NONE` defaults for folder +
  SDK evaluate, owned runtime pins; build + platform passed; 37 focused tests), and
  **08bfd5b** (live harness: 20 files, +14257/−84 — all live suites, ownership, runner).
  Production stays at 51d10c7; no pushes.
- README/CHANGELOG + this report: finalized for the separate docs commit; the harness is
  committed at 08bfd5b. README/CHANGELOG cover the final source-fix
  semantics: `plugin git-branches` GET (public docs POST is wrong), `code-env set-packages`
  explicit empty value clears specs, plugin-managed env cleanup + `settings.codeEnvName`
  association in the live harness, MLflow canonical `code`/`data`/`env` packaging with
  `containerExecConfigName=NONE` in owned runtime environments.
- Build metadata shows the previous revision until rebuilt after commits (expected; not a
  source defect). Final buildRevision: 51d10c7 (production commit
  51d10c7fbcaf4b35e2c6c47f28366673419113c4); source completeness confirmed after the two
  omission fixes landed.

## 9. Session-local evidence artifacts (not committed)

The following are session-local audit records, not repository-resolvable links.
Temporary generation files have been removed. The committed reconciliation evidence
is the blocker catalogue and the complete action table in Appendix A. Generated
`.live-tests/` reports and inventories also belong to the local lab, not this commit.

- local://dataiku-action-state.json (iteration-30 final-authoritative partition)
- local://dataiku-case-rows.json (case → actions → owners; 155 cases)
- local://dataiku-expanded-action-gaps.md (reconciliation + offline proofs + incidents)
- local://dataiku-offline-gates.md (offline gate history + selector verification)
- local://dataiku-live-incident-chronology.md (supplementary development chronology)
- local://dataiku-declaredNotRun-27.json (iteration-21 gap analysis; historical)

## 10. Post-run inventory (complete)

Evidence: `final-receipt-inventory.json` under the lab directory. Read-only checks:

- Projects: only the retained lab root project (`SDK_LIVE_94AEB85F90C1425A_CLI_ROOT_0`).
  api-deployer services: 0.
- Users/groups: user list 1 / group list 4; the run-owned id lists are exactly `[]` (no
  run-owned accounts remain).
- Meanings: exactly 1 — `sdk_live_94aeb85f90c1425a_meaning_meaning_67` (the known bound
  cleanup blocker).
- Code environments: exactly the original 4 (`default_v1` + 3 INTERNAL).
- Plugins: exactly the original 9 (`geoadmin`, `project-standards`, `local-r-dev-setup`,
  `default-samples`, `builtin-macros`, `dku-saas`, `code-studio-blocks`,
  `colorbrewer-palettes`, `k8s-metrics-utils`).
- Ledger history: 12 historical unconfirmed receipts are retained unchanged; their names
  do not appear in the current lists (meanings are case-folded) — they are receipt
  records, not 12 resources.
- Futures (58 read-only `peek` probes — `future list` is not a CLI action, so `peek` is
  the correct probe; no SDK defect): 8 returned terminal state (`alive:false`,
  `unknown:false`, `hasResult:true`, `aborted:true`); the other 50 returned HTTP 400 and
  no alive future was observed. Raw representative `WGXLS4Ru`: errorType
  `IllegalArgumentException` ("No future by that id"). Not all 50 raw bodies were checked
  and not all 58 states were independently verified.
- Bound globals: only `sdk_live_94aeb85f90c1425a_meaning_meaning_67`; bound projects: only
  root. The two already-known stopped root webapps remain (`4lF6yms`, `vVknyIm`).
- Final packaged doctor on 51d10c7: `ok:true`, projectCount 1.
- All temporary inventory scripts (Main's and /tmp) removed.

Residual retained state (truthful): lab root project, 2 stopped root webapps (`4lF6yms`,
`vVknyIm`; UI 401 cleanup), meaning `sdk_live_94aeb85f90c1425a_meaning_meaning_67`
(DELETE 405; no public DSS delete).

## Appendix A — exact 474-action table

| # | action | category | evidence |
|---|---|---|---|
| 1 | `agent.contract` | offline | hermetic suite + actual CLI proofs (§5) |
| 2 | `analysis.create` | demonstrated | ml.lifecycle, ml.clustering-task, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 3 | `analysis.delete` | demonstrated | ml.lifecycle, ml.clustering-task |
| 4 | `analysis.get` | demonstrated | ml.analysis.read |
| 5 | `analysis.list` | demonstrated | ml.analysis.read |
| 6 | `api-deployer.create-deployment` | demonstrated | infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 7 | `api-deployer.create-infra` | demonstrated | infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 8 | `api-deployer.create-service` | demonstrated | applications.api-service.publication, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 9 | `api-deployer.delete-deployment` | demonstrated | infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 10 | `api-deployer.delete-infra` | demonstrated | infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 11 | `api-deployer.delete-service` | demonstrated | applications.api-service.publication, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 12 | `api-deployer.delete-version` | demonstrated | infrastructure.api-deployer.publish-version |
| 13 | `api-deployer.deploy` | blocked | infrastructure.api-deployer.deploy (blocked) |
| 14 | `api-deployer.deployment-settings` | demonstrated | infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 15 | `api-deployer.deployment-status` | demonstrated | infrastructure.api-deployer.deploy |
| 16 | `api-deployer.get-deployment` | demonstrated | infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 17 | `api-deployer.get-infra` | demonstrated | infrastructure.api-deployer |
| 18 | `api-deployer.get-service` | demonstrated | infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 19 | `api-deployer.list-deployments` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle |
| 20 | `api-deployer.list-infras` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer |
| 21 | `api-deployer.list-services` | demonstrated | applications.api-service.publication, infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer |
| 22 | `api-deployer.list-stages` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy, infrastructure.project-deployer.infra-lifecycle, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 23 | `api-deployer.publish-version` | demonstrated | infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 24 | `api-deployer.save-deployment-settings` | demonstrated | infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 25 | `api-service.add-prediction-endpoint` | demonstrated | applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 26 | `api-service.create` | demonstrated | applications.api-service.service-crud, applications.api-service.prediction-endpoint, applications.api-service.package-lifecycle, applications.api-service.publication, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 27 | `api-service.create-package` | demonstrated | applications.api-service.package-lifecycle, applications.api-service.publication, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 28 | `api-service.delete-package` | demonstrated | applications.api-service.package-lifecycle |
| 29 | `api-service.download-package` | demonstrated | applications.api-service.package-lifecycle, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 30 | `api-service.get-settings` | demonstrated | applications.api-service.service-crud, applications.api-service.prediction-endpoint |
| 31 | `api-service.list` | demonstrated | applications.api-service.service-crud |
| 32 | `api-service.list-packages` | demonstrated | applications.api-service.package-lifecycle |
| 33 | `api-service.package-summary` | demonstrated | applications.api-service.package-lifecycle |
| 34 | `api-service.publish-package` | demonstrated | applications.api-service.publication |
| 35 | `api-service.save-settings` | demonstrated | applications.api-service.service-crud |
| 36 | `app.business-app-instance-permissions` | blocked | applications.app.business-app-instance-permissions (blocked) |
| 37 | `app.compare-manifest` | blocked | applications.app.template-surface (blocked) |
| 38 | `app.create-instance` | blocked | applications.app.permissions-restore-cycle (blocked) |
| 39 | `app.create-successor-instance` | blocked | applications.app.successor-lifecycle (blocked) |
| 40 | `app.delete-instance` | blocked | applications.app.instance-ops (blocked) |
| 41 | `app.instance-manifest` | blocked | applications.app.template-surface (blocked) |
| 42 | `app.instances` | blocked | applications.template-prerequisite (blocked) |
| 43 | `app.list` | demonstrated | applications.app-discovery, applications.app.template-surface, applications.app.instance-ops, applications.app.successor-preflight, applications.app.successor-lifecycle, applications.app.permissions-restore-cycle, applications.app.set-manifest-version |
| 44 | `app.manifest` | blocked | applications.template-prerequisite (blocked) |
| 45 | `app.manifest-version` | blocked | applications.app.template-surface (blocked) |
| 46 | `app.permissions-diff` | blocked | applications.app.instance-ops (blocked) |
| 47 | `app.permissions-restore` | blocked | applications.app.permissions-restore-cycle (blocked) |
| 48 | `app.permissions-snapshot` | blocked | applications.app.permissions-restore-cycle (blocked) |
| 49 | `app.save-instance-manifest` | blocked | applications.app.instance-ops (blocked) |
| 50 | `app.set-manifest-version` | blocked | applications.app.set-manifest-version (blocked) |
| 51 | `app.successor-preflight` | blocked | applications.app.successor-preflight (blocked) |
| 52 | `app.validate-manifest` | blocked | applications.app.template-surface (blocked) |
| 53 | `app.verify-instance` | blocked | applications.app.successor-lifecycle (blocked) |
| 54 | `auth.login` | offline | hermetic suite + actual CLI proofs (§5) |
| 55 | `batch.run` | offline | hermetic suite + actual CLI proofs (§5) |
| 56 | `bundle.activate` | blocked | core.bundle.automation-node (blocked) |
| 57 | `bundle.delete-exported` | demonstrated | core.bundle.export-lifecycle |
| 58 | `bundle.delete-imported` | blocked | core.bundle.automation-node (blocked) |
| 59 | `bundle.download-exported` | demonstrated | core.bundle.export-lifecycle, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 60 | `bundle.export` | demonstrated | core.bundle.export-lifecycle, core.bundle.publish, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 61 | `bundle.import-from-archive` | blocked | core.bundle.automation-node (blocked) |
| 62 | `bundle.import-from-stream` | blocked | core.bundle.automation-node (blocked) |
| 63 | `bundle.list-exported` | demonstrated | core.bundle.export-lifecycle |
| 64 | `bundle.list-imported` | blocked | core.bundle.automation-node (blocked) |
| 65 | `bundle.preload` | blocked | core.bundle.automation-node (blocked) |
| 66 | `bundle.publish` | demonstrated | core.bundle.publish |
| 67 | `business-app.create-instance` | blocked | applications.business-app.lifecycle (blocked) |
| 68 | `business-app.get` | blocked | applications.business-app.read-surface (blocked) |
| 69 | `business-app.install-from-archive` | blocked | applications.business-app.lifecycle (blocked) |
| 70 | `business-app.instances` | blocked | applications.business-app.read-surface (blocked) |
| 71 | `business-app.list` | demonstrated | applications.app-discovery, applications.business-app-settings, applications.app.business-app-instance-permissions, applications.business-app.read-surface, applications.business-app.lifecycle |
| 72 | `business-app.save-settings` | blocked | applications.business-app-settings (blocked) |
| 73 | `business-app.settings` | blocked | applications.business-app.read-surface (blocked) |
| 74 | `business-app.upgrade-instance` | blocked | applications.business-app.lifecycle (blocked) |
| 75 | `cleanup.run` | offline | hermetic suite + actual CLI proofs (§5) |
| 76 | `code-env.create` | demonstrated | infrastructure.code-env |
| 77 | `code-env.delete` | demonstrated | infrastructure.code-env, infrastructure.plugin.code-env |
| 78 | `code-env.get` | demonstrated | infrastructure.code-env, infrastructure.code-env-reads, infrastructure.plugin.code-env |
| 79 | `code-env.get-definition` | demonstrated | infrastructure.code-env, infrastructure.code-env-reads, infrastructure.plugin.code-env |
| 80 | `code-env.get-log` | demonstrated | infrastructure.code-env-reads |
| 81 | `code-env.list` | demonstrated | infrastructure.code-env-reads |
| 82 | `code-env.list-logs` | demonstrated | infrastructure.code-env-reads |
| 83 | `code-env.set-definition` | demonstrated | infrastructure.code-env |
| 84 | `code-env.set-jupyter` | demonstrated | infrastructure.code-env |
| 85 | `code-env.set-packages` | demonstrated | infrastructure.code-env |
| 86 | `code-env.update-images` | demonstrated | infrastructure.code-env |
| 87 | `code-env.update-packages` | demonstrated | infrastructure.code-env |
| 88 | `code-env.usages` | demonstrated | infrastructure.code-env-reads |
| 89 | `code-env.version` | demonstrated | infrastructure.code-env-reads |
| 90 | `code.run` | demonstrated | core.code.run |
| 91 | `commands.run` | offline | hermetic suite + actual CLI proofs (§5) |
| 92 | `connection.create` | demonstrated | core.notebook.sql-lifecycle, infrastructure.sql-select, infrastructure.connection, infrastructure.connection-import-surface |
| 93 | `connection.delete` | demonstrated | core.notebook.sql-lifecycle, infrastructure.sql-select, infrastructure.connection, infrastructure.connection-import-surface |
| 94 | `connection.execute-import` | blocked | infrastructure.connection-import-surface (blocked) |
| 95 | `connection.get` | demonstrated | infrastructure.connection, infrastructure.connections-reads, infrastructure.connection-import-surface, infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 96 | `connection.infer` | demonstrated | infrastructure.connections-reads |
| 97 | `connection.list` | demonstrated | infrastructure.connection, infrastructure.connections-reads, infrastructure.connection-import-surface, infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 98 | `connection.prepare-import` | blocked | infrastructure.connection-import-surface (blocked) |
| 99 | `connection.schemas` | demonstrated | infrastructure.connection-import-surface |
| 100 | `connection.tables` | demonstrated | infrastructure.connection-import-surface |
| 101 | `connection.test` | demonstrated | infrastructure.connection, infrastructure.connections-reads |
| 102 | `connection.update` | demonstrated | infrastructure.connection |
| 103 | `continuous-activity.list` | blocked | infrastructure.continuous-activities (blocked) |
| 104 | `continuous-activity.start` | blocked | infrastructure.continuous-activities (blocked) |
| 105 | `continuous-activity.status` | blocked | infrastructure.continuous-activities (blocked) |
| 106 | `continuous-activity.stop` | blocked | infrastructure.continuous-activities (blocked) |
| 107 | `dashboard.create` | demonstrated | collab.dashboard-lifecycle, core.dashboard.export, collab.setup.dashboard (setup) |
| 108 | `dashboard.delete` | demonstrated | collab.dashboard-lifecycle, core.dashboard.export |
| 109 | `dashboard.export` | blocked | core.dashboard.export (blocked) |
| 110 | `dashboard.get` | demonstrated | collab.dashboard, collab.dashboard-lifecycle, core.dashboard.export, collab.setup.dashboard (setup) |
| 111 | `dashboard.list` | demonstrated | collab.dashboard, core.dashboard.export, collab.setup.dashboard (setup) |
| 112 | `dashboard.update` | demonstrated | collab.dashboard, collab.dashboard-lifecycle |
| 113 | `data-collection.add-object` | demonstrated | infrastructure.data-collection |
| 114 | `data-collection.create` | demonstrated | infrastructure.data-collection |
| 115 | `data-collection.delete` | demonstrated | infrastructure.data-collection |
| 116 | `data-collection.get` | demonstrated | infrastructure.data-collection |
| 117 | `data-collection.list` | demonstrated | infrastructure.data-collection, infrastructure.directory-reads |
| 118 | `data-collection.list-objects` | demonstrated | infrastructure.data-collection |
| 119 | `data-collection.remove-dataset` | demonstrated | infrastructure.data-collection |
| 120 | `data-collection.settings-set` | demonstrated | infrastructure.data-collection |
| 121 | `data-quality.assert-results` | demonstrated | collab.data-quality |
| 122 | `data-quality.compute` | demonstrated | collab.data-quality, core.future.reads, core.future.abort, core.data-quality.timeline, collab.setup.data-quality (setup) |
| 123 | `data-quality.create-rule` | demonstrated | collab.data-quality, core.data-quality.timeline, collab.setup.data-quality (setup) |
| 124 | `data-quality.delete-rule` | demonstrated | collab.data-quality, core.data-quality.timeline |
| 125 | `data-quality.get-rule` | demonstrated | collab.data-quality |
| 126 | `data-quality.history` | demonstrated | collab.data-quality |
| 127 | `data-quality.last-results` | demonstrated | collab.data-quality |
| 128 | `data-quality.project-status` | demonstrated | collab.data-quality, core.data-quality.timeline |
| 129 | `data-quality.project-timeline` | demonstrated | core.data-quality.timeline |
| 130 | `data-quality.rules` | demonstrated | collab.data-quality, collab.setup.data-quality (setup) |
| 131 | `data-quality.status` | demonstrated | collab.data-quality, core.data-quality.partition-status |
| 132 | `data-quality.status-by-partition` | demonstrated | core.data-quality.partition-status |
| 133 | `data-quality.update-rule` | demonstrated | collab.data-quality |
| 134 | `dataset.assert-count` | demonstrated | core.dataset.baseline, core.recipe.runs, collab.jobs, core.job.lifecycle |
| 135 | `dataset.assert-schema` | demonstrated | core.dataset.baseline |
| 136 | `dataset.clear` | demonstrated | core.dataset.lifecycle |
| 137 | `dataset.clone` | demonstrated | core.dataset.lifecycle |
| 138 | `dataset.column-lineage` | demonstrated | core.dataset.inspect |
| 139 | `dataset.create` | demonstrated | core.dataset.lifecycle, core.flow-jobs.project, core.dataset.inspect, core.dataset.metadata, core.dataset.rename, core.dataset.partitions, core.recipe.clone, core.recipe.update, core.recipe.restore, core.flow-zone.organize, core.job.lifecycle, core.job.log-url, core.job.abort, ml.lifecycle, ml.clustering-task, ml.mlflow.evaluate, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 140 | `dataset.create-managed` | demonstrated | core.flow-jobs.project, core.dataset.partitions |
| 141 | `dataset.delete` | demonstrated | core.dataset.lifecycle, core.recipe.lifecycle |
| 142 | `dataset.download` | demonstrated | core.dataset.baseline, ml.scoring.retained-recipe |
| 143 | `dataset.files` | demonstrated | core.dataset.baseline, core.dataset.lifecycle, core.flow-jobs.project, core.dataset.inspect, core.dataset.metadata, core.dataset.rename, core.dataset.partitions, core.recipe.clone, core.recipe.update, core.recipe.restore, core.flow-zone.organize, core.job.lifecycle, core.job.log-url, core.job.abort |
| 144 | `dataset.get` | demonstrated | core.dataset.baseline, core.dataset.lifecycle, core.dataset.rename, core.dataset.partitions, core.recipe.clone |
| 145 | `dataset.info` | demonstrated | core.dataset.inspect |
| 146 | `dataset.list` | demonstrated | core.dataset.baseline, core.flow-jobs.project, core.dataset.inspect, core.dataset.rename, core.dataset.partitions, core.recipe.clone, core.recipe.update, core.flow-zone.organize, core.job.lifecycle, core.job.log-url, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 147 | `dataset.list-partitions` | demonstrated | core.dataset.partitions |
| 148 | `dataset.metadata` | demonstrated | core.dataset.baseline, core.dataset.metadata |
| 149 | `dataset.metadata-set` | demonstrated | core.dataset.metadata |
| 150 | `dataset.preview` | demonstrated | core.dataset.baseline, core.recipe.runs, ml.lifecycle, ml.clustering-task, ml.retained-fixtures, ml.analysis.read, ml.task.settings, ml.saved-model.metadata, ml.saved-model.scoring-jar, ml.saved-model.scoring-pmml, ml.scoring.retained-recipe, ml.mes.retained-evaluation, ml.mlflow.evaluate, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 151 | `dataset.refresh-schema` | demonstrated | core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.mlflow.evaluate, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 152 | `dataset.rename` | demonstrated | core.dataset.rename |
| 153 | `dataset.schema` | demonstrated | core.dataset.baseline, core.recipe.runs, core.dataset.inspect |
| 154 | `dataset.source` | demonstrated | core.dataset.baseline |
| 155 | `dataset.update` | demonstrated | core.dataset.lifecycle, core.flow-jobs.project, core.dataset.partitions, ml.lifecycle, ml.clustering-task, ml.mlflow.evaluate, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 156 | `dataset.upload-file` | demonstrated | core.dataset.lifecycle, core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.mlflow.evaluate, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 157 | `dataset.validate-build` | demonstrated | core.dataset.inspect |
| 158 | `discussion.create` | demonstrated | core.discussion.lifecycle |
| 159 | `discussion.get` | demonstrated | core.discussion.lifecycle |
| 160 | `discussion.list` | demonstrated | core.discussion.lifecycle |
| 161 | `discussion.reply` | demonstrated | core.discussion.lifecycle |
| 162 | `doctor.run` | demonstrated | core.doctor.run |
| 163 | `fixtures.run` | demonstrated | core.fixtures.run |
| 164 | `flow-zone.create` | setupBaseline | collab.setup.flow-zone (setup) |
| 165 | `flow-zone.delete` | demonstrated | core.flow-zone.organize |
| 166 | `flow-zone.find` | demonstrated | collab.flow-zone, core.flow-zone.organize |
| 167 | `flow-zone.get` | demonstrated | core.flow-zone.organize, collab.setup.flow-zone (setup) |
| 168 | `flow-zone.graph` | demonstrated | core.flow-zone.organize |
| 169 | `flow-zone.list` | demonstrated | collab.flow-zone, collab.setup.flow-zone (setup) |
| 170 | `flow-zone.move` | setupBaseline | collab.setup.flow-zone (setup) |
| 171 | `flow-zone.organize` | demonstrated | core.flow-zone.organize |
| 172 | `flow-zone.plan` | demonstrated | core.flow-zone.organize |
| 173 | `flow-zone.update` | demonstrated | collab.flow-zone |
| 174 | `folder.contents` | demonstrated | core.folder.roundtrip, core.folder.lifecycle |
| 175 | `folder.create` | demonstrated | core.folder.lifecycle, ml.mlflow.import |
| 176 | `folder.delete` | demonstrated | core.folder.lifecycle |
| 177 | `folder.delete-file` | demonstrated | core.folder.roundtrip, core.folder.lifecycle |
| 178 | `folder.download` | demonstrated | core.folder.roundtrip |
| 179 | `folder.get` | demonstrated | core.folder.lifecycle |
| 180 | `folder.list` | demonstrated | core.folder.lifecycle, ml.mlflow.import |
| 181 | `folder.update` | demonstrated | core.folder.lifecycle |
| 182 | `folder.upload` | demonstrated | core.folder.roundtrip, core.folder.lifecycle, ml.mlflow.import |
| 183 | `future.abort` | demonstrated | core.future.abort |
| 184 | `future.get` | demonstrated | core.future.reads |
| 185 | `future.peek` | demonstrated | core.future.reads, core.future.abort |
| 186 | `future.wait` | demonstrated | core.future.reads, core.statistics.computation, infrastructure.code-env |
| 187 | `group.create` | blocked | infrastructure.group (blocked) |
| 188 | `group.delete` | blocked | infrastructure.group (blocked) |
| 189 | `group.get` | demonstrated | infrastructure.directory-reads |
| 190 | `group.list` | demonstrated | infrastructure.directory-reads |
| 191 | `group.update` | blocked | infrastructure.group (blocked) |
| 192 | `insight.create` | demonstrated | collab.insight-lifecycle, collab.setup.insight (setup) |
| 193 | `insight.delete` | demonstrated | collab.insight-lifecycle |
| 194 | `insight.get` | demonstrated | collab.insight, collab.insight-lifecycle, collab.setup.insight (setup) |
| 195 | `insight.list` | demonstrated | collab.insight, collab.setup.insight (setup) |
| 196 | `insight.update` | demonstrated | collab.insight, collab.insight-lifecycle |
| 197 | `install-skill.run` | offline | hermetic suite + actual CLI proofs (§5) |
| 198 | `job.abort` | demonstrated | core.job.abort |
| 199 | `job.build` | demonstrated | core.job.lifecycle, core.job.abort |
| 200 | `job.build-and-wait` | demonstrated | collab.jobs, core.dataset.partitions, core.job.log-url |
| 201 | `job.get` | demonstrated | collab.jobs, core.job.lifecycle, core.job.log-url |
| 202 | `job.list` | demonstrated | collab.jobs |
| 203 | `job.log` | demonstrated | collab.jobs |
| 204 | `job.log-url` | demonstrated | core.job.log-url |
| 205 | `job.monitor` | demonstrated | core.job.lifecycle |
| 206 | `job.summary` | demonstrated | collab.jobs |
| 207 | `job.wait` | demonstrated | core.job.lifecycle |
| 208 | `job.watch` | demonstrated | core.job.lifecycle |
| 209 | `knowledge-bank.clear` | blocked | infrastructure.knowledge-bank (blocked) |
| 210 | `knowledge-bank.search` | blocked | infrastructure.knowledge-bank (blocked) |
| 211 | `llm.completions` | blocked | infrastructure.llm-probes (blocked) |
| 212 | `llm.embeddings` | blocked | infrastructure.llm-probes (blocked) |
| 213 | `llm.list` | demonstrated | infrastructure.llm-catalog |
| 214 | `macro.abort` | demonstrated | core.macro.abort |
| 215 | `macro.get` | demonstrated | core.macro.discovery |
| 216 | `macro.list` | demonstrated | core.macro.discovery, core.macro.run |
| 217 | `macro.result` | demonstrated | core.macro.run |
| 218 | `macro.run` | demonstrated | core.macro.run, core.macro.abort |
| 219 | `macro.run-and-wait` | demonstrated | core.macro.run |
| 220 | `macro.state` | demonstrated | core.macro.run, core.macro.abort |
| 221 | `meaning.create` | blocked | infrastructure.meaning (blocked) |
| 222 | `meaning.delete` | blocked | infrastructure.meaning (blocked) |
| 223 | `meaning.get` | demonstrated | infrastructure.meaning, infrastructure.directory-reads |
| 224 | `meaning.list` | demonstrated | infrastructure.meaning, infrastructure.directory-reads |
| 225 | `meaning.update` | demonstrated | infrastructure.meaning |
| 226 | `metrics.dataset-compute` | demonstrated | collab.metrics, collab.setup.metrics (setup) |
| 227 | `metrics.dataset-get` | demonstrated | collab.metrics, collab.setup.metrics (setup) |
| 228 | `metrics.dataset-history` | demonstrated | collab.metrics |
| 229 | `metrics.folder-get` | demonstrated | collab.metrics |
| 230 | `ml-task.create` | demonstrated | ml.lifecycle, ml.clustering-task, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 231 | `ml-task.delete` | demonstrated | ml.lifecycle, ml.clustering-task |
| 232 | `ml-task.deploy` | demonstrated | ml.lifecycle, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 233 | `ml-task.get-settings` | demonstrated | ml.lifecycle, ml.task.settings |
| 234 | `ml-task.list-models` | demonstrated | ml.lifecycle |
| 235 | `ml-task.model-details` | demonstrated | ml.lifecycle |
| 236 | `ml-task.set-settings` | demonstrated | ml.task.settings |
| 237 | `ml-task.status` | demonstrated | ml.clustering-task, ml.task.settings |
| 238 | `ml-task.train` | demonstrated | ml.lifecycle, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 239 | `model-evaluation-store.create` | demonstrated | ml.mes.lifecycle |
| 240 | `model-evaluation-store.delete` | demonstrated | ml.mes.lifecycle |
| 241 | `model-evaluation-store.get` | demonstrated | ml.mes.retained-evaluation, ml.mes.lifecycle |
| 242 | `model-evaluation-store.list` | demonstrated | ml.mes.lifecycle |
| 243 | `model-evaluation-store.list-evaluations` | demonstrated | ml.mes.retained-evaluation, ml.mes.lifecycle |
| 244 | `notebook.clear-jupyter-outputs` | demonstrated | core.notebook.jupyter-lifecycle |
| 245 | `notebook.clear-sql-history` | demonstrated | core.notebook.sql-lifecycle |
| 246 | `notebook.delete-jupyter` | demonstrated | core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions |
| 247 | `notebook.delete-sql` | demonstrated | core.notebook.sql-lifecycle |
| 248 | `notebook.get-jupyter` | demonstrated | collab.notebook, core.notebook.jupyter-lifecycle, collab.setup.notebook (setup) |
| 249 | `notebook.get-sql` | demonstrated | core.notebook.sql-lifecycle |
| 250 | `notebook.history-sql` | demonstrated | core.notebook.sql-lifecycle |
| 251 | `notebook.list-jupyter` | demonstrated | collab.notebook, core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions, collab.setup.notebook (setup) |
| 252 | `notebook.list-sql` | demonstrated | core.notebook.sql-list, core.notebook.sql-lifecycle |
| 253 | `notebook.save-jupyter` | demonstrated | collab.notebook, core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions, collab.setup.notebook (setup) |
| 254 | `notebook.save-sql` | demonstrated | core.notebook.sql-lifecycle |
| 255 | `notebook.sessions-jupyter` | demonstrated | core.notebook.jupyter-sessions |
| 256 | `notebook.unload-jupyter` | demonstrated | core.notebook.jupyter-sessions |
| 257 | `plugin.code-env-create` | demonstrated | infrastructure.plugin.code-env |
| 258 | `plugin.code-env-update` | demonstrated | infrastructure.plugin.code-env |
| 259 | `plugin.contents-delete` | demonstrated | infrastructure.plugin |
| 260 | `plugin.contents-get` | demonstrated | infrastructure.plugin, infrastructure.plugin.zip |
| 261 | `plugin.contents-list` | demonstrated | infrastructure.plugin |
| 262 | `plugin.contents-put` | demonstrated | infrastructure.plugin, infrastructure.plugin.git-remote |
| 263 | `plugin.create-dev` | demonstrated | infrastructure.plugin, infrastructure.plugin.git-remote |
| 264 | `plugin.delete` | demonstrated | core.macro.abort, infrastructure.plugin, infrastructure.plugin.code-env, infrastructure.plugin.zip, infrastructure.plugin.git-remote |
| 265 | `plugin.delete-git-remote` | blocked | infrastructure.plugin.git-sync (blocked) |
| 266 | `plugin.details` | demonstrated | infrastructure.plugin |
| 267 | `plugin.download` | demonstrated | infrastructure.plugin |
| 268 | `plugin.fetch` | blocked | infrastructure.plugin.git-sync (blocked) |
| 269 | `plugin.folder-add` | demonstrated | infrastructure.plugin |
| 270 | `plugin.get-git-remote` | demonstrated | infrastructure.plugin.git-remote |
| 271 | `plugin.git-branches` | demonstrated | infrastructure.plugin.git-remote |
| 272 | `plugin.install-from-git` | blocked | infrastructure.plugin.git-install (blocked) |
| 273 | `plugin.install-from-store` | blocked | infrastructure.plugin.store (blocked) |
| 274 | `plugin.install-from-zip` | demonstrated | core.macro.run, infrastructure.plugin.code-env, infrastructure.plugin.zip |
| 275 | `plugin.list` | demonstrated | infrastructure.plugin-reads, infrastructure.plugin, infrastructure.plugin.zip |
| 276 | `plugin.move` | demonstrated | infrastructure.plugin |
| 277 | `plugin.move-to-dev` | demonstrated | infrastructure.plugin.zip |
| 278 | `plugin.pull` | blocked | infrastructure.plugin.git-sync (blocked) |
| 279 | `plugin.push` | blocked | infrastructure.plugin.git-sync (blocked) |
| 280 | `plugin.rename` | demonstrated | infrastructure.plugin |
| 281 | `plugin.reset-local` | demonstrated | infrastructure.plugin.zip |
| 282 | `plugin.reset-remote` | blocked | infrastructure.plugin.git-sync (blocked) |
| 283 | `plugin.set-git-remote` | blocked | infrastructure.plugin.git-sync (blocked) |
| 284 | `plugin.settings-get` | demonstrated | infrastructure.plugin, infrastructure.plugin.code-env |
| 285 | `plugin.settings-set` | demonstrated | infrastructure.plugin, infrastructure.plugin.code-env |
| 286 | `plugin.update-from-git` | blocked | infrastructure.plugin.git-install (blocked) |
| 287 | `plugin.update-from-store` | blocked | infrastructure.plugin.store (blocked) |
| 288 | `plugin.update-from-zip` | demonstrated | infrastructure.plugin.zip |
| 289 | `plugin.usages` | demonstrated | infrastructure.plugin-reads, infrastructure.plugin |
| 290 | `project-deployer.create-deployment` | demonstrated | infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 291 | `project-deployer.create-infra` | demonstrated | infrastructure.project-deployer.infra-lifecycle, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 292 | `project-deployer.create-project` | demonstrated | core.bundle.publish, infrastructure.project-deployer |
| 293 | `project-deployer.delete-deployment` | demonstrated | infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 294 | `project-deployer.deploy` | blocked | infrastructure.project-deployer.deploy (blocked) |
| 295 | `project-deployer.deployment-status` | demonstrated | infrastructure.project-deployer.deploy |
| 296 | `project-deployer.get-deployment` | demonstrated | infrastructure.project-deployer.deployment-lifecycle |
| 297 | `project-deployer.list-deployments` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.project-deployer.reads, infrastructure.project-deployer.deployment-lifecycle |
| 298 | `project-deployer.list-infras` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.project-deployer.reads, infrastructure.project-deployer.infra-lifecycle, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 299 | `project-deployer.list-projects` | demonstrated | infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.project-deployer.reads, infrastructure.project-deployer, infrastructure.project-deployer.upload-bundle |
| 300 | `project-deployer.project-status` | demonstrated | core.bundle.publish, infrastructure.project-deployer, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 301 | `project-deployer.save-deployment-settings` | demonstrated | infrastructure.project-deployer.deployment-lifecycle |
| 302 | `project-deployer.upload-bundle` | demonstrated | infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 303 | `project-folder.create-child` | demonstrated | infrastructure.project-folder, infrastructure.project-folder.move-project |
| 304 | `project-folder.delete` | demonstrated | infrastructure.project-folder, infrastructure.project-folder.move-project |
| 305 | `project-folder.get` | demonstrated | infrastructure.project-folder, infrastructure.project-folder.move-project, infrastructure.directory-reads |
| 306 | `project-folder.move` | demonstrated | infrastructure.project-folder |
| 307 | `project-folder.move-project` | demonstrated | infrastructure.project-folder.move-project |
| 308 | `project-folder.root` | demonstrated | infrastructure.project-folder, infrastructure.project-folder.move-project, infrastructure.directory-reads |
| 309 | `project-folder.settings-get` | demonstrated | infrastructure.project-folder, infrastructure.directory-reads |
| 310 | `project-folder.settings-set` | demonstrated | infrastructure.project-folder |
| 311 | `project-git.add-library` | blocked | core.project-git.external-libraries (blocked) |
| 312 | `project-git.branches` | demonstrated | core.project-git.inspect, core.project-git.branches |
| 313 | `project-git.commit` | demonstrated | core.project-git.commit, core.project-git.history |
| 314 | `project-git.create-branch` | demonstrated | core.project-git.branches |
| 315 | `project-git.create-tag` | demonstrated | core.project-git.tags |
| 316 | `project-git.current-branch` | demonstrated | core.project-git.inspect, core.project-git.branches |
| 317 | `project-git.delete-branch` | demonstrated | core.project-git.branches |
| 318 | `project-git.delete-tag` | demonstrated | core.project-git.tags |
| 319 | `project-git.diff` | demonstrated | core.project-git.inspect |
| 320 | `project-git.drop-and-rebuild` | demonstrated | core.project-git.drop-and-rebuild |
| 321 | `project-git.fetch` | blocked | core.project-git.remote-sync (blocked) |
| 322 | `project-git.future-abort` | demonstrated | core.project-git.library-futures |
| 323 | `project-git.future-status` | demonstrated | core.project-git.library-futures |
| 324 | `project-git.future-wait` | demonstrated | core.project-git.library-futures |
| 325 | `project-git.get-remote` | demonstrated | core.project-git.inspect |
| 326 | `project-git.list-libraries` | demonstrated | core.project-git.inspect, core.project-git.library-futures |
| 327 | `project-git.log` | demonstrated | core.project-git.inspect, core.project-git.commit, core.project-git.history, core.project-git.drop-and-rebuild |
| 328 | `project-git.pull` | blocked | core.project-git.remote-sync (blocked) |
| 329 | `project-git.push` | blocked | core.project-git.remote-sync (blocked) |
| 330 | `project-git.push-all-libraries` | blocked | core.project-git.external-libraries (blocked) |
| 331 | `project-git.push-library` | blocked | core.project-git.external-libraries (blocked) |
| 332 | `project-git.remove-library` | blocked | core.project-git.external-libraries (blocked) |
| 333 | `project-git.remove-remote` | blocked | core.project-git.remote-sync (blocked) |
| 334 | `project-git.reset-all-libraries` | demonstrated | core.project-git.library-futures |
| 335 | `project-git.reset-library` | blocked | core.project-git.external-libraries (blocked) |
| 336 | `project-git.reset-to-head` | demonstrated | core.project-git.history |
| 337 | `project-git.reset-to-upstream` | blocked | core.project-git.remote-sync (blocked) |
| 338 | `project-git.revert-commit` | demonstrated | core.project-git.history |
| 339 | `project-git.revert-to-revision` | demonstrated | core.project-git.history |
| 340 | `project-git.set-library` | blocked | core.project-git.external-libraries (blocked) |
| 341 | `project-git.set-remote` | blocked | core.project-git.remote-sync (blocked) |
| 342 | `project-git.status` | demonstrated | core.project-git.inspect, core.project-git.commit, core.project-git.branches, core.project-git.history, core.project-git.drop-and-rebuild |
| 343 | `project-git.switch` | demonstrated | core.project-git.branches |
| 344 | `project-git.tags` | demonstrated | core.project-git.inspect, core.project-git.tags |
| 345 | `project-library.create-file` | demonstrated | collab.project-library |
| 346 | `project-library.create-folder` | demonstrated | collab.project-library, collab.setup.project-library (setup) |
| 347 | `project-library.delete` | demonstrated | collab.project-library |
| 348 | `project-library.diff` | demonstrated | collab.project-library |
| 349 | `project-library.get` | demonstrated | collab.project-library |
| 350 | `project-library.get-bytes` | demonstrated | collab.project-library |
| 351 | `project-library.list` | demonstrated | collab.project-library |
| 352 | `project-library.move` | demonstrated | collab.project-library |
| 353 | `project-library.put` | demonstrated | collab.project-library, core.project-git.commit, core.project-git.history, collab.setup.project-library (setup) |
| 354 | `project-library.rename` | demonstrated | collab.project-library |
| 355 | `project.create` | demonstrated | project.lifecycle, core.project.metadata, core.project.tags, core.project.permissions, core.discussion.lifecycle, core.bundle.export-lifecycle, core.bundle.publish, core.project-git.inspect, core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.mes.lifecycle, ml.mlflow.import, ml.mlflow.evaluate, infrastructure.sql-select, applications.api-service.service-crud, applications.api-service.prediction-endpoint, applications.api-service.package-lifecycle, applications.api-service.publication, applications.webapp.lifecycle, applications.webapp.backend, infrastructure.project-folder.move-project, infrastructure.connection-import-surface, infrastructure.llm-catalog, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 356 | `project.delete` | demonstrated | project.lifecycle, core.project.metadata, core.project.tags, core.project.permissions, core.discussion.lifecycle, core.bundle.export-lifecycle, core.bundle.publish, ml.lifecycle, ml.clustering-task, ml.mes.lifecycle, ml.mlflow.import, ml.mlflow.evaluate, infrastructure.sql-select, applications.api-service.service-crud, applications.api-service.prediction-endpoint, applications.api-service.package-lifecycle, applications.api-service.publication, applications.webapp.lifecycle, applications.webapp.backend, infrastructure.project-folder.move-project, infrastructure.connection-import-surface, infrastructure.llm-catalog, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 357 | `project.duplicate` | demonstrated | project.lifecycle |
| 358 | `project.export` | demonstrated | project.lifecycle |
| 359 | `project.flow` | demonstrated | core.project.reads |
| 360 | `project.get` | demonstrated | core.project.reads, core.project.metadata |
| 361 | `project.import` | demonstrated | project.lifecycle |
| 362 | `project.inspect-archive` | demonstrated | project.lifecycle |
| 363 | `project.list` | demonstrated | core.project.reads |
| 364 | `project.map` | demonstrated | core.project.reads |
| 365 | `project.metadata` | demonstrated | collab.project-metadata, core.project.metadata |
| 366 | `project.metadata-set` | demonstrated | core.project.metadata |
| 367 | `project.permissions-get` | demonstrated | core.project.permissions |
| 368 | `project.permissions-set` | demonstrated | core.project.permissions |
| 369 | `project.settings-get` | demonstrated | collab.project-metadata |
| 370 | `project.settings-set` | demonstrated | collab.project-metadata |
| 371 | `project.tags-get` | demonstrated | core.project.tags |
| 372 | `project.tags-set` | demonstrated | core.project.tags |
| 373 | `recipe.add-input` | demonstrated | core.recipe.lifecycle |
| 374 | `recipe.assert-unchanged` | demonstrated | core.recipe.restore |
| 375 | `recipe.cat` | demonstrated | core.recipe.lifecycle, core.recipe.restore |
| 376 | `recipe.clone` | demonstrated | core.recipe.clone |
| 377 | `recipe.create` | demonstrated | core.recipe.lifecycle, core.flow-jobs.project, core.dataset.partitions |
| 378 | `recipe.delete` | demonstrated | core.recipe.lifecycle |
| 379 | `recipe.diff` | demonstrated | core.recipe.lifecycle |
| 380 | `recipe.download` | demonstrated | core.recipe.lifecycle |
| 381 | `recipe.download-code` | demonstrated | core.recipe.lifecycle |
| 382 | `recipe.get` | demonstrated | core.recipe.graph, collab.jobs, core.dataset.rename, core.recipe.clone, core.recipe.update, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 383 | `recipe.get-payload` | demonstrated | core.recipe.lifecycle |
| 384 | `recipe.list` | demonstrated | core.recipe.graph, collab.jobs, core.flow-jobs.project, core.dataset.partitions, core.recipe.restore, core.job.abort, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 385 | `recipe.metadata` | demonstrated | core.recipe.update |
| 386 | `recipe.metadata-set` | demonstrated | core.recipe.update |
| 387 | `recipe.remove-input` | demonstrated | core.recipe.lifecycle |
| 388 | `recipe.restore` | demonstrated | core.recipe.restore |
| 389 | `recipe.run` | demonstrated | core.recipe.runs, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 390 | `recipe.set-payload` | demonstrated | core.recipe.lifecycle, core.flow-jobs.project, core.recipe.restore |
| 391 | `recipe.update` | demonstrated | core.recipe.update |
| 392 | `recipe.validate-graph` | demonstrated | core.recipe.graph, core.recipe.clone, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 393 | `saved-model.create-external` | demonstrated | ml.mlflow.import, ml.mlflow.evaluate |
| 394 | `saved-model.delete` | demonstrated | ml.lifecycle, ml.mlflow.import, ml.mlflow.evaluate |
| 395 | `saved-model.delete-versions` | demonstrated | ml.mlflow.import |
| 396 | `saved-model.download-scoring-jar` | blocked | ml.saved-model.scoring-jar (blocked) |
| 397 | `saved-model.download-scoring-pmml` | blocked | ml.saved-model.scoring-pmml (blocked) |
| 398 | `saved-model.evaluate-version` | demonstrated | ml.mlflow.evaluate |
| 399 | `saved-model.external-metadata-get` | demonstrated | ml.mlflow.import, ml.mlflow.evaluate |
| 400 | `saved-model.external-metadata-put` | demonstrated | ml.mlflow.import, ml.mlflow.evaluate |
| 401 | `saved-model.get` | demonstrated | ml.lifecycle, ml.saved-model.metadata |
| 402 | `saved-model.import-mlflow-version` | demonstrated | ml.mlflow.import, ml.mlflow.evaluate |
| 403 | `saved-model.import-mlflow-version-from-folder` | demonstrated | ml.mlflow.import |
| 404 | `saved-model.list` | demonstrated | ml.lifecycle, ml.mlflow.import, ml.mlflow.evaluate |
| 405 | `saved-model.list-versions` | demonstrated | ml.lifecycle, ml.saved-model.metadata, ml.mlflow.import, applications.api-service.prediction-endpoint, infrastructure.api-deployer.publish-version |
| 406 | `saved-model.set-active` | demonstrated | ml.saved-model.metadata |
| 407 | `saved-model.set-user-meta` | demonstrated | ml.saved-model.metadata |
| 408 | `saved-model.update-settings` | demonstrated | ml.saved-model.metadata |
| 409 | `saved-model.version-details` | demonstrated | ml.lifecycle, ml.saved-model.metadata, ml.mlflow.import, ml.mlflow.evaluate |
| 410 | `saved-model.version-snippet` | demonstrated | ml.saved-model.metadata |
| 411 | `scenario.abort` | demonstrated | core.scenario.abort |
| 412 | `scenario.active-set` | demonstrated | core.scenario.activation |
| 413 | `scenario.create` | demonstrated | core.scenario.custom-lifecycle, core.scenario.activation, core.scenario.abort, collab.setup.scenario (setup) |
| 414 | `scenario.delete` | demonstrated | core.scenario.custom-lifecycle, core.scenario.activation, core.scenario.abort |
| 415 | `scenario.get` | demonstrated | collab.scenario, core.scenario.custom-lifecycle, core.scenario.activation |
| 416 | `scenario.get-run` | demonstrated | core.scenario.custom-lifecycle, core.scenario.abort |
| 417 | `scenario.last-runs` | demonstrated | core.scenario.custom-lifecycle, core.scenario.abort |
| 418 | `scenario.list` | demonstrated | core.code.run, core.scenario.custom-lifecycle, collab.setup.scenario (setup) |
| 419 | `scenario.log` | demonstrated | core.scenario.custom-lifecycle |
| 420 | `scenario.payload-get` | demonstrated | core.scenario.custom-lifecycle |
| 421 | `scenario.payload-set` | demonstrated | core.scenario.custom-lifecycle, core.scenario.abort |
| 422 | `scenario.run` | demonstrated | collab.scenario, core.scenario.custom-lifecycle, core.scenario.abort |
| 423 | `scenario.run-and-wait` | demonstrated | collab.scenario, collab.setup.scenario (setup) |
| 424 | `scenario.status` | demonstrated | collab.scenario, core.scenario.activation, core.scenario.abort |
| 425 | `scenario.update` | demonstrated | collab.scenario, core.scenario.custom-lifecycle, core.scenario.abort |
| 426 | `sql.query` | demonstrated | infrastructure.sql-select |
| 427 | `statistics.create-worksheet` | demonstrated | core.statistics.worksheet-lifecycle, core.statistics.computation |
| 428 | `statistics.delete-worksheet` | demonstrated | core.statistics.worksheet-lifecycle, core.statistics.computation |
| 429 | `statistics.get-worksheet` | demonstrated | core.statistics.worksheet-lifecycle, core.statistics.computation |
| 430 | `statistics.list-worksheets` | demonstrated | core.statistics.worksheet-lifecycle |
| 431 | `statistics.run-card` | demonstrated | core.statistics.computation |
| 432 | `statistics.run-computation` | demonstrated | core.statistics.computation |
| 433 | `statistics.run-worksheet` | demonstrated | core.statistics.computation |
| 434 | `statistics.update-worksheet` | demonstrated | core.statistics.worksheet-lifecycle, core.statistics.computation |
| 435 | `streaming-endpoint.create` | blocked | infrastructure.continuous-activities (blocked) |
| 436 | `streaming-endpoint.delete` | blocked | infrastructure.continuous-activities (blocked) |
| 437 | `streaming-endpoint.get` | blocked | infrastructure.streaming-endpoints (blocked) |
| 438 | `streaming-endpoint.list` | blocked | infrastructure.streaming-endpoints (blocked) |
| 439 | `streaming-endpoint.update-settings` | blocked | infrastructure.streaming-endpoints (blocked) |
| 440 | `user.activity` | demonstrated | infrastructure.user.activity, infrastructure.directory-reads |
| 441 | `user.activity-get` | demonstrated | infrastructure.user.activity, infrastructure.directory-reads |
| 442 | `user.create` | blocked | infrastructure.user.external-sync (blocked) |
| 443 | `user.delete` | blocked | infrastructure.user.external-sync (blocked) |
| 444 | `user.external-groups` | blocked | infrastructure.user.external-sync (blocked) |
| 445 | `user.external-users` | blocked | infrastructure.user.external-sync (blocked) |
| 446 | `user.get` | demonstrated | infrastructure.user.activity, infrastructure.directory-reads |
| 447 | `user.list` | demonstrated | infrastructure.directory-reads |
| 448 | `user.provision` | blocked | infrastructure.user.external-sync (blocked) |
| 449 | `user.resync` | blocked | infrastructure.user.external-sync (blocked) |
| 450 | `user.resync-multi` | blocked | infrastructure.user.external-sync (blocked) |
| 451 | `user.update` | blocked | infrastructure.user (blocked) |
| 452 | `variable.get` | demonstrated | collab.variables, project.lifecycle, collab.setup.variables (setup) |
| 453 | `variable.set` | demonstrated | collab.variables, project.lifecycle, collab.setup.variables (setup) |
| 454 | `version.run` | offline | hermetic suite + actual CLI proofs (§5) |
| 455 | `webapp.backend-state` | demonstrated | applications.webapp.lifecycle, applications.webapp.backend |
| 456 | `webapp.create` | demonstrated | applications.webapp.lifecycle, applications.webapp.backend |
| 457 | `webapp.get-settings` | demonstrated | applications.webapp.lifecycle |
| 458 | `webapp.list` | demonstrated | applications.webapp.lifecycle |
| 459 | `webapp.restart-backend` | demonstrated | applications.webapp.backend |
| 460 | `webapp.stop-backend` | demonstrated | applications.webapp.lifecycle, applications.webapp.backend |
| 461 | `webapp.update-settings` | demonstrated | applications.webapp.lifecycle, applications.webapp.backend |
| 462 | `wiki.create` | demonstrated | collab.wiki-lifecycle, collab.setup.wiki (setup) |
| 463 | `wiki.delete` | demonstrated | collab.wiki-lifecycle |
| 464 | `wiki.get` | demonstrated | collab.wiki, collab.wiki-lifecycle, collab.setup.wiki (setup) |
| 465 | `wiki.list` | demonstrated | collab.wiki, collab.setup.wiki (setup) |
| 466 | `wiki.settings` | demonstrated | collab.wiki |
| 467 | `wiki.update` | demonstrated | collab.wiki, collab.wiki-lifecycle |
| 468 | `workspace.add-object` | demonstrated | infrastructure.workspace |
| 469 | `workspace.create` | demonstrated | infrastructure.workspace |
| 470 | `workspace.delete` | demonstrated | infrastructure.workspace |
| 471 | `workspace.get` | demonstrated | infrastructure.workspace |
| 472 | `workspace.list` | demonstrated | infrastructure.workspace, infrastructure.directory-reads |
| 473 | `workspace.list-objects` | demonstrated | infrastructure.workspace |
| 474 | `workspace.update-settings` | demonstrated | infrastructure.workspace |
