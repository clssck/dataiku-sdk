# Dataiku SDK expanded demonstration — validation report (2026-09-14)

## Current accepted scope

This revision supersedes the iteration-30 results previously recorded here. The user explicitly selected **stay within current capabilities**: exercise supported workflows, retain genuine platform/license/safety limitations, and never disguise them as passes.

- Full, unfiltered **iteration-49**: **145 cases — 126 passed, 19 blocked, 0 failed, 0 required failures**; 1,364 recorded commands. Both legacy phases passed. Project integrity verified.
- Final targeted **iteration-51**: **2 passed, 0 failed, 0 required failures**. Both owned-template catalog reads execute; group reads are now declared against the read-only case that actually executes them. Project integrity verified. Iteration-50 was the intermediate one-case confirmation.
- This is an explicit full-run-plus-targeted-correction evidence set, **not a claim that iteration-51 reran the entire suite**. Only four action rows use iteration-51: app.manifest, app.instances, group.list, group.get. Other rows retain iteration-49 or identified setup/offline provenance.
- All 474 registered actions are classified below: **416 demonstrated + 3 setup-baseline + 48 lifecycle-blocked + 7 hermetic-offline = 474**. No uncovered live actions. A blocked lifecycle does not imply every individual read API in that family is unavailable.
- Expected negative probes and capability failures have nonzero command exits; **zero failed cases** does not mean every individual command exited zero.
- Dashboard export and meaning cleanup were reclassified from required to optional under the explicitly approved capability scope. Their blockers were not fixed; exit zero is not an all-capabilities-green claim.
- Live owned-App creation now uses generated keys, omitting `targetProjectKey`/`--to`. Successful explicit-key creation is no longer exercised live; hermetic App tests retain strict refusal coverage. No explicit-key absence proof is bypassed.

## Verification gates

The disjoint counts below were measured before the final two live-only case edits (owned-template catalog reads and group-read declarations). Those edits received targeted live verification in iteration-51, followed by fresh `check` (including `check:live`), lint, formatting, and 32 coverage/runner tests. They are not presented as a single full-suite run at the final tree. The subsequent cleanup-example correction passed all 24 skill installation/token checks, plus fresh live types, lint, and formatting.

| Disjoint partition | Passed | Failed | Runner skips | Files |
|---|---:|---:|---:|---:|
| Offline root | 888 | 0 | 0 | 64 |
| CLI a–l | 512 | 0 | 0 | 27 |
| CLI m–z plus nested code-env resource | 278 | 0 | 0 | 13 |
| Fully enabled integration modules | 74 | 0 | 0 | 4 |

Offline total: **1,678 passed**. Integration total: **74 passed**, with owned filesystem/SQL/App fixtures and every integration opt-in enabled. The live wrapper is separate; repeated focused checks and legacy reruns are not added to these totals. Interactive Jupyter unloading remains an explicit internal unavailable-capability finding: the public API cannot start a running UI session and no authenticated UI session was accessible. No browser/UI success is claimed.

- Native cross-model tokenizer checks enabled through the external OMP native module; no tokenizer skip.
- Types: `bun run check` passed, including live/tools types and plugin-version synchronization.
- Lint and formatting: `bun run lint`, `bun run format:check` passed.
- Production: `bun run build` passed. Before the follow-up commit series, packaged Bun `bin/dss.js` reported version 3.2.0, source dist, build revision `2e65d776884ff9f9a592f941ee438cc5ed974b80`, staleBuild false. This identifies the verification base revision, not the later commits containing the verified changes.
- Packaged `doctor` returned `ok:true`; packaged project listing showed only the retained root. Targetless successor planning succeeded with an unreachable dummy DSS endpoint: no request or key allocation was needed.
- Focused generated-release regression check: 76 passed, not double-counted above.

## Completed remediations

- Generated-key App creation/successors retain source/template/ACL guards, allocate once during apply, and bind cleanup to the creation future and incarnation. Explicit keys retain strict masked-403 absence checks.
- Confirmed guarded App deletion updates ownership state; cleanup does not re-probe a deleted project through a masked-403 endpoint.
- App-release guidance snapshots sharing before changes, compares intended users/groups/roles afterward, and restores approved missing access without dropping newer grants. People absent from the originator Flow can lose access. Same-instance restore is distinct from copying permissions to a successor; intended-identity and external SSO checks remain mandatory.
- Owned Filesystem fixtures use inherited ACLs for the Python runner and DSS service identities, not world-writable roots. Managed folders use DSS-selected compatible storage. Real sync and file-content roundtrips passed.
- Integration fixtures now exercise dataset/Jupyter discovery, insight mutation, job aborts, folder I/O, notebook cleanup, and native-only code-environment rebuilds rather than reporting missing-fixture gaps.
- Host cleanup retains ownership/daemon receipts on failure and can remove empty private directories without broadening permissions. The retained historical directory was recovered through exact empty-directory removal, never by recreating its missing ownership marker.
- Owned Git, SQL import, deployer metadata/detail, Designer App variable/scenario/download, successor, version, and permission workflows passed. SSE transport was exercised before DSS proved streaming unavailable.
- Ambiguous mutation errors preserve bounded credential-redacted DSS diagnostics without adding unsafe retries.

## Remaining approved capability limits

All 19 live cases below remain explicitly blocked and optional under the approved current-capability scope; none is relabeled passed. The additional interactive Jupyter/UI limitation is described above.

| Case | Exact remaining prerequisite or safety boundary |
|---|---|
| `core.bundle.automation-node` | Design node only; no Automation-node target or credentials. |
| `core.dashboard.export` | Documented graphics-export route returns 404; Cloud graphics enablement is not verified. |
| `ml.saved-model.scoring-jar` | Scoring export is disabled by the current license; request returns 403. |
| `ml.saved-model.scoring-pmml` | Scoring export is disabled by the current license; request returns 403. |
| `applications.business-app-settings` | Business Apps are not licensed/provisioned; no owned shell with a safe cleanup contract. |
| `applications.app.business-app-instance-permissions` | No Business App or instance is available; ordinary Designer Apps are covered separately. |
| `applications.business-app.read-surface` | Business App catalog is empty; no licensed fixture is available. |
| `applications.business-app.lifecycle` | No licensed archive/owned shell, and no source-verified public shell deletion contract. |
| `infrastructure.user` | Cloud rejects DSS user creation with 403; no Launchpad credentials. |
| `infrastructure.group` | Cloud rejects DSS group creation with 403; directory reads are verified separately. |
| `infrastructure.meaning` | DSS has no public meaning deletion route (405). Preserve the previously created meaning; do not create more undeletable globals. |
| `infrastructure.user.external-sync` | No owned external directory; global authentication changes are outside the approved scope. |
| `infrastructure.llm-probes` | Actual LLM catalog is empty. |
| `infrastructure.knowledge-bank` | No available embedding model or owned searchable bank. |
| `infrastructure.streaming-endpoints` | Owned SSE fixture works, but DSS rejects endpoint creation: Streaming is not available on this instance. |
| `infrastructure.continuous-activities` | The required streaming source is disabled by DSS, not missing because of an unprovisioned broker. |
| `infrastructure.plugin.store` | No ownership-bound store package with a safe cleanup contract. |
| `infrastructure.api-deployer.deploy` | No API runtime node. Owned deployer metadata/detail lifecycles are verified. |
| `infrastructure.project-deployer.deploy` | No Automation runtime target. Owned deployer metadata/detail lifecycles are verified. |

## Ownership, cleanup, and retained state

- Protected root: `SDK_LIVE_94AEB85F90C1425A_CLI_ROOT_0`. Never cleaned or rebuilt by this verification.
- Final ownership journal: no undeleted non-root owned projects, no remaining owned host directories, no pending dependent App creations. Full and targeted run integrity checks report no missing, changed, unreadable, or unexplained projects.
- Preserve the existing root webapps `4lF6yms`, `vVknyIm` and meaning `sdk_live_94aeb85f90c1425a_meaning_meaning_67`. Meaning deletion remains unavailable; no UI cleanup is claimed.
- Eighteen unconfirmed historical/denied creation receipts remain truthfully unconfirmed: seven user, seven group, three meaning, one API-deployer service. Final read-only inventories contained none of those exact identities. Receipts were not silently converted into deletion claims.
- Confirmed bound globals retain only the known meaning. Global identities were not guessed or deleted by prefix. Plugin-managed environments are cleaned before their owning plugins; asynchronous plugin deletion is awaited.
- Task-created temporary probes removed. Existing dist, lab reports, ledgers, caches, and unrelated artifacts preserved. Verification preceded the user-authorized follow-up commit series; no repository push, publication, or deployment was performed.

## Evidence and provenance

DSS **15.0.1**, API **1**, SDK **3.2.0**. Lab: `.live-tests/demo-2026-09-14/lab-13fc3204b9f64905-001/`.

- `reports/iteration-49/report.json`: full run, command ledger, setup receipts, phases, blockers, integrity.
- `reports/iteration-51/report.json`: final App catalog and directory-read corrections.
- `reports/iteration-50/report.json`: intermediate App catalog verification only.
- Integration all-opt-in proof: 74 pass / 0 fail / 0 skip, 1,653 expectations, four files; fixtures and dependencies cleaned in ownership order.
- These reports and the manifest/cleanup ledger are local lab evidence, not newly published repository artifacts. Earlier source-fix and incident history remains in prior revisions and retained lab records; its old counts are not reused as current results.

## Exact 474-action partition

`setup-baseline` explicitly means retained setup evidence, not freshly rerun setup. `hermetic-offline` makes no live-network claim. Blocked rows refer to the lifecycle boundaries above; raw per-command results remain available in the reports.

| # | Action | Category | Evidence |
|---:|---|---|---|
| 1 | `agent.contract` | hermetic-offline | offline gates |
| 2 | `analysis.create` | demonstrated | iteration-49: ml.lifecycle, ml.clustering-task, ml.retained-fixtures, applications.api-service.prediction-endpoint |
| 3 | `analysis.delete` | demonstrated | iteration-49: ml.lifecycle |
| 4 | `analysis.get` | demonstrated | iteration-49: ml.analysis.read |
| 5 | `analysis.list` | demonstrated | iteration-49: ml.retained-fixtures, ml.analysis.read |
| 6 | `api-deployer.create-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 7 | `api-deployer.create-infra` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle |
| 8 | `api-deployer.create-service` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle |
| 9 | `api-deployer.delete-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle, infrastructure.api-deployer.deploy |
| 10 | `api-deployer.delete-infra` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle |
| 11 | `api-deployer.delete-service` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle |
| 12 | `api-deployer.delete-version` | demonstrated | iteration-49: infrastructure.api-deployer.publish-version |
| 13 | `api-deployer.deploy` | blocked | iteration-49: infrastructure.api-deployer.deploy |
| 14 | `api-deployer.deployment-settings` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle |
| 15 | `api-deployer.deployment-status` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deploy |
| 16 | `api-deployer.get-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle |
| 17 | `api-deployer.get-infra` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer |
| 18 | `api-deployer.get-service` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer, infrastructure.api-deployer.publish-version |
| 19 | `api-deployer.list-deployments` | demonstrated | iteration-49: infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle |
| 20 | `api-deployer.list-infras` | demonstrated | iteration-49: applications.api-service.publication, infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer |
| 21 | `api-deployer.list-services` | demonstrated | iteration-49: applications.api-service.publication, infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer |
| 22 | `api-deployer.list-stages` | demonstrated | iteration-49: infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.api-deployer |
| 23 | `api-deployer.publish-version` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.publish-version, infrastructure.api-deployer.deployment-lifecycle |
| 24 | `api-deployer.save-deployment-settings` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.api-deployer.deployment-lifecycle |
| 25 | `api-service.add-prediction-endpoint` | demonstrated | iteration-49: applications.api-service.prediction-endpoint |
| 26 | `api-service.create` | demonstrated | iteration-49: applications.api-service.service-crud, applications.api-service.prediction-endpoint, applications.api-service.package-lifecycle, applications.api-service.publication |
| 27 | `api-service.create-package` | demonstrated | iteration-49: applications.api-service.package-lifecycle, applications.api-service.publication |
| 28 | `api-service.delete-package` | demonstrated | iteration-49: applications.api-service.package-lifecycle |
| 29 | `api-service.download-package` | demonstrated | iteration-49: applications.api-service.package-lifecycle |
| 30 | `api-service.get-settings` | demonstrated | iteration-49: applications.api-service.service-crud, applications.api-service.prediction-endpoint |
| 31 | `api-service.list` | demonstrated | iteration-49: applications.api-service.service-crud |
| 32 | `api-service.list-packages` | demonstrated | iteration-49: applications.api-service.package-lifecycle |
| 33 | `api-service.package-summary` | demonstrated | iteration-49: applications.api-service.package-lifecycle |
| 34 | `api-service.publish-package` | demonstrated | iteration-49: applications.api-service.publication |
| 35 | `api-service.save-settings` | demonstrated | iteration-49: applications.api-service.service-crud |
| 36 | `app.business-app-instance-permissions` | blocked | iteration-49: applications.app.business-app-instance-permissions |
| 37 | `app.compare-manifest` | demonstrated | iteration-49: applications.app.template-surface, applications.instance-lifecycle |
| 38 | `app.create-instance` | demonstrated | iteration-49: applications.app.template-surface, applications.instance-lifecycle, applications.app.instance-ops, applications.app.successor-preflight, applications.app.successor-lifecycle, applications.app.permissions-restore-cycle |
| 39 | `app.create-successor-instance` | demonstrated | iteration-49: applications.app.successor-lifecycle |
| 40 | `app.delete-instance` | demonstrated | iteration-49: applications.app.instance-ops |
| 41 | `app.instance-manifest` | demonstrated | iteration-49: applications.template-prerequisite, applications.app.template-surface |
| 42 | `app.instances` | demonstrated | iteration-51: applications.template-prerequisite |
| 43 | `app.list` | demonstrated | iteration-49: applications.app-discovery |
| 44 | `app.manifest` | demonstrated | iteration-51: applications.template-prerequisite |
| 45 | `app.manifest-version` | demonstrated | iteration-49: applications.template-prerequisite, applications.app.template-surface, applications.app.set-manifest-version |
| 46 | `app.permissions-diff` | demonstrated | iteration-49: applications.app.instance-ops |
| 47 | `app.permissions-restore` | demonstrated | iteration-49: applications.app.permissions-restore-cycle |
| 48 | `app.permissions-snapshot` | demonstrated | iteration-49: applications.app.instance-ops, applications.app.permissions-restore-cycle |
| 49 | `app.save-instance-manifest` | demonstrated | iteration-49: applications.template-prerequisite, applications.app.template-surface, applications.app.instance-ops, applications.app.set-manifest-version |
| 50 | `app.set-manifest-version` | demonstrated | iteration-49: applications.app.set-manifest-version |
| 51 | `app.successor-preflight` | demonstrated | iteration-49: applications.app.successor-preflight |
| 52 | `app.validate-manifest` | demonstrated | iteration-49: applications.template-prerequisite, applications.app.template-surface |
| 53 | `app.verify-instance` | demonstrated | iteration-49: applications.app.template-surface, applications.instance-lifecycle, applications.app.successor-lifecycle |
| 54 | `auth.login` | hermetic-offline | offline gates |
| 55 | `batch.run` | hermetic-offline | offline gates |
| 56 | `bundle.activate` | blocked | iteration-49: core.bundle.automation-node |
| 57 | `bundle.delete-exported` | demonstrated | iteration-49: core.bundle.export-lifecycle |
| 58 | `bundle.delete-imported` | blocked | iteration-49: core.bundle.automation-node |
| 59 | `bundle.download-exported` | demonstrated | iteration-49: core.bundle.export-lifecycle, infrastructure.deployer-details, infrastructure.project-deployer.upload-bundle |
| 60 | `bundle.export` | demonstrated | iteration-49: core.bundle.export-lifecycle, core.bundle.publish, infrastructure.deployer-details, infrastructure.project-deployer.upload-bundle |
| 61 | `bundle.import-from-archive` | blocked | iteration-49: core.bundle.automation-node |
| 62 | `bundle.import-from-stream` | blocked | iteration-49: core.bundle.automation-node |
| 63 | `bundle.list-exported` | demonstrated | iteration-49: core.bundle.export-lifecycle |
| 64 | `bundle.list-imported` | blocked | iteration-49: core.bundle.automation-node |
| 65 | `bundle.preload` | blocked | iteration-49: core.bundle.automation-node |
| 66 | `bundle.publish` | demonstrated | iteration-49: core.bundle.publish |
| 67 | `business-app.create-instance` | blocked | iteration-49: applications.business-app.lifecycle |
| 68 | `business-app.get` | blocked | iteration-49: applications.business-app.read-surface |
| 69 | `business-app.install-from-archive` | blocked | iteration-49: applications.business-app.lifecycle |
| 70 | `business-app.instances` | blocked | iteration-49: applications.business-app.read-surface |
| 71 | `business-app.list` | demonstrated | iteration-49: applications.app-discovery |
| 72 | `business-app.save-settings` | blocked | iteration-49: applications.business-app-settings |
| 73 | `business-app.settings` | blocked | iteration-49: applications.business-app.read-surface |
| 74 | `business-app.upgrade-instance` | blocked | iteration-49: applications.business-app.lifecycle |
| 75 | `cleanup.run` | hermetic-offline | offline gates |
| 76 | `code-env.create` | demonstrated | iteration-49: infrastructure.code-env |
| 77 | `code-env.delete` | demonstrated | iteration-49: infrastructure.code-env, infrastructure.plugin.code-env |
| 78 | `code-env.get` | demonstrated | iteration-49: infrastructure.code-env, infrastructure.code-env-reads, infrastructure.plugin.code-env |
| 79 | `code-env.get-definition` | demonstrated | iteration-49: infrastructure.code-env, infrastructure.code-env-reads, infrastructure.plugin.code-env |
| 80 | `code-env.get-log` | demonstrated | iteration-49: infrastructure.code-env-reads |
| 81 | `code-env.list` | demonstrated | iteration-49: infrastructure.code-env, infrastructure.code-env-reads |
| 82 | `code-env.list-logs` | demonstrated | iteration-49: infrastructure.code-env-reads |
| 83 | `code-env.set-definition` | demonstrated | iteration-49: infrastructure.code-env |
| 84 | `code-env.set-jupyter` | demonstrated | iteration-49: infrastructure.code-env |
| 85 | `code-env.set-packages` | demonstrated | iteration-49: infrastructure.code-env |
| 86 | `code-env.update-images` | demonstrated | iteration-49: infrastructure.code-env |
| 87 | `code-env.update-packages` | demonstrated | iteration-49: infrastructure.code-env |
| 88 | `code-env.usages` | demonstrated | iteration-49: infrastructure.code-env-reads |
| 89 | `code-env.version` | demonstrated | iteration-49: infrastructure.code-env-reads |
| 90 | `code.run` | demonstrated | iteration-49: core.code.run |
| 91 | `commands.run` | hermetic-offline | offline gates |
| 92 | `connection.create` | demonstrated | iteration-49: infrastructure.connection |
| 93 | `connection.delete` | demonstrated | iteration-49: infrastructure.connection |
| 94 | `connection.execute-import` | demonstrated | iteration-49: infrastructure.connection-import-surface |
| 95 | `connection.get` | demonstrated | iteration-49: infrastructure.connection, infrastructure.connections-reads |
| 96 | `connection.infer` | demonstrated | iteration-49: infrastructure.connections-reads |
| 97 | `connection.list` | demonstrated | iteration-49: infrastructure.connection, infrastructure.connections-reads |
| 98 | `connection.prepare-import` | demonstrated | iteration-49: infrastructure.connection-import-surface |
| 99 | `connection.schemas` | demonstrated | iteration-49: infrastructure.connection-import-surface |
| 100 | `connection.tables` | demonstrated | iteration-49: infrastructure.connection-import-surface |
| 101 | `connection.test` | demonstrated | iteration-49: infrastructure.connection, infrastructure.connections-reads |
| 102 | `connection.update` | demonstrated | iteration-49: infrastructure.connection |
| 103 | `continuous-activity.list` | blocked | iteration-49: infrastructure.continuous-activities |
| 104 | `continuous-activity.start` | blocked | iteration-49: infrastructure.continuous-activities |
| 105 | `continuous-activity.status` | blocked | iteration-49: infrastructure.continuous-activities |
| 106 | `continuous-activity.stop` | blocked | iteration-49: infrastructure.continuous-activities |
| 107 | `dashboard.create` | demonstrated | iteration-49: collab.setup.dashboard, collab.dashboard-lifecycle, core.dashboard.export |
| 108 | `dashboard.delete` | demonstrated | iteration-49: collab.dashboard-lifecycle, core.dashboard.export |
| 109 | `dashboard.export` | blocked | iteration-49: core.dashboard.export |
| 110 | `dashboard.get` | demonstrated | iteration-49: collab.setup.dashboard, collab.dashboard, collab.dashboard-lifecycle, core.dashboard.export |
| 111 | `dashboard.list` | demonstrated | iteration-49: collab.dashboard |
| 112 | `dashboard.update` | demonstrated | iteration-49: collab.dashboard, collab.dashboard-lifecycle |
| 113 | `data-collection.add-object` | demonstrated | iteration-49: infrastructure.data-collection |
| 114 | `data-collection.create` | demonstrated | iteration-49: infrastructure.data-collection |
| 115 | `data-collection.delete` | demonstrated | iteration-49: infrastructure.data-collection |
| 116 | `data-collection.get` | demonstrated | iteration-49: infrastructure.data-collection, infrastructure.directory-reads |
| 117 | `data-collection.list` | demonstrated | iteration-49: infrastructure.data-collection, infrastructure.directory-reads |
| 118 | `data-collection.list-objects` | demonstrated | iteration-49: infrastructure.data-collection, infrastructure.directory-reads |
| 119 | `data-collection.remove-dataset` | demonstrated | iteration-49: infrastructure.data-collection |
| 120 | `data-collection.settings-set` | demonstrated | iteration-49: infrastructure.data-collection |
| 121 | `data-quality.assert-results` | demonstrated | iteration-49: collab.data-quality |
| 122 | `data-quality.compute` | demonstrated | iteration-49: collab.setup.data-quality, collab.data-quality, core.future.reads, core.data-quality.timeline |
| 123 | `data-quality.create-rule` | demonstrated | iteration-49: collab.setup.data-quality, collab.data-quality, core.data-quality.timeline |
| 124 | `data-quality.delete-rule` | demonstrated | iteration-49: collab.data-quality, core.data-quality.timeline |
| 125 | `data-quality.get-rule` | demonstrated | iteration-49: collab.data-quality |
| 126 | `data-quality.history` | demonstrated | iteration-49: collab.data-quality |
| 127 | `data-quality.last-results` | demonstrated | iteration-49: collab.data-quality |
| 128 | `data-quality.project-status` | demonstrated | iteration-49: collab.data-quality, core.data-quality.timeline |
| 129 | `data-quality.project-timeline` | demonstrated | iteration-49: core.data-quality.timeline |
| 130 | `data-quality.rules` | demonstrated | iteration-49: collab.data-quality |
| 131 | `data-quality.status` | demonstrated | iteration-49: collab.data-quality, core.data-quality.partition-status |
| 132 | `data-quality.status-by-partition` | demonstrated | iteration-49: core.data-quality.partition-status |
| 133 | `data-quality.update-rule` | demonstrated | iteration-49: collab.data-quality |
| 134 | `dataset.assert-count` | demonstrated | iteration-49: core.dataset.baseline, core.dataset.lifecycle, core.recipe.runs, core.flow-jobs.project, core.job.lifecycle |
| 135 | `dataset.assert-schema` | demonstrated | iteration-49: core.dataset.baseline |
| 136 | `dataset.clear` | demonstrated | iteration-49: core.dataset.lifecycle |
| 137 | `dataset.clone` | demonstrated | iteration-49: core.dataset.lifecycle |
| 138 | `dataset.column-lineage` | demonstrated | iteration-49: core.dataset.inspect |
| 139 | `dataset.create` | demonstrated | iteration-49: core.dataset.lifecycle, core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.retained-fixtures |
| 140 | `dataset.create-managed` | demonstrated | iteration-49: core.flow-jobs.project, core.dataset.partitions, infrastructure.continuous-activities |
| 141 | `dataset.delete` | demonstrated | iteration-49: core.dataset.lifecycle, infrastructure.continuous-activities |
| 142 | `dataset.download` | demonstrated | iteration-49: core.dataset.baseline, ml.scoring.retained-recipe |
| 143 | `dataset.files` | demonstrated | iteration-49: core.dataset.baseline |
| 144 | `dataset.get` | demonstrated | iteration-49: core.dataset.baseline, core.dataset.lifecycle, core.dataset.rename, core.recipe.clone |
| 145 | `dataset.info` | demonstrated | iteration-49: core.dataset.inspect |
| 146 | `dataset.list` | demonstrated | iteration-49: core.dataset.baseline, ml.retained-fixtures |
| 147 | `dataset.list-partitions` | demonstrated | iteration-49: core.dataset.partitions |
| 148 | `dataset.metadata` | demonstrated | iteration-49: core.dataset.baseline, core.dataset.metadata |
| 149 | `dataset.metadata-set` | demonstrated | iteration-49: core.dataset.metadata |
| 150 | `dataset.preview` | demonstrated | iteration-49: core.dataset.baseline, core.recipe.runs, ml.scoring.retained-recipe, infrastructure.continuous-activities |
| 151 | `dataset.refresh-schema` | demonstrated | iteration-49: core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.retained-fixtures, infrastructure.continuous-activities |
| 152 | `dataset.rename` | demonstrated | iteration-49: core.dataset.rename |
| 153 | `dataset.schema` | demonstrated | iteration-49: core.dataset.baseline, core.recipe.runs, core.dataset.inspect |
| 154 | `dataset.source` | demonstrated | iteration-49: core.dataset.baseline |
| 155 | `dataset.update` | demonstrated | iteration-49: core.dataset.lifecycle, core.flow-jobs.project, core.dataset.partitions |
| 156 | `dataset.upload-file` | demonstrated | iteration-49: core.dataset.lifecycle, core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.retained-fixtures |
| 157 | `dataset.validate-build` | demonstrated | iteration-49: core.dataset.inspect |
| 158 | `discussion.create` | demonstrated | iteration-49: core.discussion.lifecycle |
| 159 | `discussion.get` | demonstrated | iteration-49: core.discussion.lifecycle |
| 160 | `discussion.list` | demonstrated | iteration-49: core.discussion.lifecycle |
| 161 | `discussion.reply` | demonstrated | iteration-49: core.discussion.lifecycle |
| 162 | `doctor.run` | demonstrated | iteration-49: core.doctor.run |
| 163 | `fixtures.run` | demonstrated | iteration-49: core.fixtures.run |
| 164 | `flow-zone.create` | setup-baseline | retained setup receipts: collab.setup.flow-zone |
| 165 | `flow-zone.delete` | demonstrated | iteration-49: core.flow-zone.organize |
| 166 | `flow-zone.find` | demonstrated | iteration-49: core.flow-zone.organize |
| 167 | `flow-zone.get` | demonstrated | iteration-49: collab.setup.flow-zone, collab.flow-zone, core.flow-zone.organize |
| 168 | `flow-zone.graph` | demonstrated | iteration-49: core.flow-zone.organize |
| 169 | `flow-zone.list` | demonstrated | iteration-49: collab.flow-zone |
| 170 | `flow-zone.move` | setup-baseline | retained setup receipts: collab.setup.flow-zone |
| 171 | `flow-zone.organize` | demonstrated | iteration-49: core.flow-zone.organize |
| 172 | `flow-zone.plan` | demonstrated | iteration-49: core.flow-zone.organize |
| 173 | `flow-zone.update` | demonstrated | iteration-49: collab.flow-zone |
| 174 | `folder.contents` | demonstrated | iteration-49: core.folder.roundtrip, core.folder.lifecycle |
| 175 | `folder.create` | demonstrated | iteration-49: core.folder.lifecycle, applications.template-prerequisite |
| 176 | `folder.delete` | demonstrated | iteration-49: core.folder.lifecycle, applications.template-prerequisite |
| 177 | `folder.delete-file` | demonstrated | iteration-49: core.folder.roundtrip, core.folder.lifecycle |
| 178 | `folder.download` | demonstrated | iteration-49: core.folder.roundtrip, applications.template-prerequisite |
| 179 | `folder.get` | demonstrated | iteration-49: core.folder.lifecycle |
| 180 | `folder.list` | demonstrated | iteration-49: core.folder.lifecycle |
| 181 | `folder.update` | demonstrated | iteration-49: core.folder.lifecycle |
| 182 | `folder.upload` | demonstrated | iteration-49: core.folder.roundtrip, core.folder.lifecycle |
| 183 | `future.abort` | demonstrated | iteration-49: core.future.abort |
| 184 | `future.get` | demonstrated | iteration-49: core.future.reads |
| 185 | `future.peek` | demonstrated | iteration-49: core.future.reads |
| 186 | `future.wait` | demonstrated | iteration-49: core.future.reads, core.statistics.computation, infrastructure.connection-import-surface |
| 187 | `group.create` | blocked | iteration-49: infrastructure.group |
| 188 | `group.delete` | blocked | iteration-49: infrastructure.group |
| 189 | `group.get` | demonstrated | iteration-51: infrastructure.directory-reads |
| 190 | `group.list` | demonstrated | iteration-51: infrastructure.directory-reads |
| 191 | `group.update` | blocked | iteration-49: infrastructure.group |
| 192 | `insight.create` | demonstrated | iteration-49: collab.setup.insight, collab.insight-lifecycle |
| 193 | `insight.delete` | demonstrated | iteration-49: collab.insight-lifecycle |
| 194 | `insight.get` | demonstrated | iteration-49: collab.setup.insight, collab.insight, collab.insight-lifecycle |
| 195 | `insight.list` | demonstrated | iteration-49: collab.insight |
| 196 | `insight.update` | demonstrated | iteration-49: collab.insight, collab.insight-lifecycle |
| 197 | `install-skill.run` | hermetic-offline | offline gates |
| 198 | `job.abort` | demonstrated | iteration-49: core.job.abort |
| 199 | `job.build` | demonstrated | iteration-49: core.job.lifecycle, core.job.abort |
| 200 | `job.build-and-wait` | demonstrated | iteration-49: collab.jobs, core.dataset.partitions, core.job.log-url |
| 201 | `job.get` | demonstrated | iteration-49: collab.jobs, core.job.lifecycle, core.job.log-url |
| 202 | `job.list` | demonstrated | iteration-49: collab.jobs |
| 203 | `job.log` | demonstrated | iteration-49: collab.jobs |
| 204 | `job.log-url` | demonstrated | iteration-49: core.job.log-url |
| 205 | `job.monitor` | demonstrated | iteration-49: core.job.lifecycle |
| 206 | `job.summary` | demonstrated | iteration-49: collab.jobs |
| 207 | `job.wait` | demonstrated | iteration-49: core.job.lifecycle, core.job.abort |
| 208 | `job.watch` | demonstrated | iteration-49: core.job.lifecycle |
| 209 | `knowledge-bank.clear` | blocked | iteration-49: infrastructure.knowledge-bank |
| 210 | `knowledge-bank.search` | blocked | iteration-49: infrastructure.knowledge-bank |
| 211 | `llm.completions` | blocked | iteration-49: infrastructure.llm-probes |
| 212 | `llm.embeddings` | blocked | iteration-49: infrastructure.llm-probes |
| 213 | `llm.list` | demonstrated | iteration-49: infrastructure.llm-catalog |
| 214 | `macro.abort` | demonstrated | iteration-49: core.macro.abort |
| 215 | `macro.get` | demonstrated | iteration-49: core.macro.discovery |
| 216 | `macro.list` | demonstrated | iteration-49: core.macro.discovery, core.macro.run |
| 217 | `macro.result` | demonstrated | iteration-49: core.macro.run |
| 218 | `macro.run` | demonstrated | iteration-49: core.macro.run, core.macro.abort |
| 219 | `macro.run-and-wait` | demonstrated | iteration-49: core.macro.run |
| 220 | `macro.state` | demonstrated | iteration-49: core.macro.run, core.macro.abort |
| 221 | `meaning.create` | blocked | iteration-49: infrastructure.meaning |
| 222 | `meaning.delete` | blocked | iteration-49: infrastructure.meaning |
| 223 | `meaning.get` | demonstrated | iteration-49: infrastructure.meaning, infrastructure.directory-reads |
| 224 | `meaning.list` | demonstrated | iteration-49: infrastructure.meaning, infrastructure.directory-reads |
| 225 | `meaning.update` | blocked | iteration-49: infrastructure.meaning |
| 226 | `metrics.dataset-compute` | demonstrated | iteration-49: collab.setup.metrics, collab.metrics |
| 227 | `metrics.dataset-get` | demonstrated | iteration-49: collab.setup.metrics, collab.metrics |
| 228 | `metrics.dataset-history` | demonstrated | iteration-49: collab.metrics |
| 229 | `metrics.folder-get` | demonstrated | iteration-49: collab.metrics |
| 230 | `ml-task.create` | demonstrated | iteration-49: ml.lifecycle, ml.clustering-task, ml.retained-fixtures, applications.api-service.prediction-endpoint |
| 231 | `ml-task.delete` | demonstrated | iteration-49: ml.clustering-task |
| 232 | `ml-task.deploy` | demonstrated | iteration-49: ml.lifecycle, ml.retained-fixtures, applications.api-service.prediction-endpoint |
| 233 | `ml-task.get-settings` | demonstrated | iteration-49: ml.lifecycle, ml.task.settings |
| 234 | `ml-task.list-models` | demonstrated | iteration-49: ml.lifecycle |
| 235 | `ml-task.model-details` | demonstrated | iteration-49: ml.lifecycle |
| 236 | `ml-task.set-settings` | demonstrated | iteration-49: ml.task.settings |
| 237 | `ml-task.status` | demonstrated | iteration-49: ml.clustering-task, ml.task.settings |
| 238 | `ml-task.train` | demonstrated | iteration-49: ml.lifecycle, ml.retained-fixtures, applications.api-service.prediction-endpoint |
| 239 | `model-evaluation-store.create` | demonstrated | iteration-49: ml.mes.retained-evaluation, ml.mes.lifecycle |
| 240 | `model-evaluation-store.delete` | demonstrated | iteration-49: ml.mes.lifecycle |
| 241 | `model-evaluation-store.get` | demonstrated | iteration-49: ml.mes.retained-evaluation, ml.mes.lifecycle |
| 242 | `model-evaluation-store.list` | demonstrated | iteration-49: ml.mes.retained-evaluation, ml.mes.lifecycle |
| 243 | `model-evaluation-store.list-evaluations` | demonstrated | iteration-49: ml.mes.retained-evaluation, ml.mes.lifecycle |
| 244 | `notebook.clear-jupyter-outputs` | demonstrated | iteration-49: core.notebook.jupyter-lifecycle |
| 245 | `notebook.clear-sql-history` | demonstrated | iteration-49: core.notebook.sql-lifecycle |
| 246 | `notebook.delete-jupyter` | demonstrated | iteration-49: core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions |
| 247 | `notebook.delete-sql` | demonstrated | iteration-49: core.notebook.sql-lifecycle |
| 248 | `notebook.get-jupyter` | demonstrated | iteration-49: collab.setup.notebook, collab.notebook, core.notebook.jupyter-lifecycle |
| 249 | `notebook.get-sql` | demonstrated | iteration-49: core.notebook.sql-lifecycle |
| 250 | `notebook.history-sql` | demonstrated | iteration-49: core.notebook.sql-lifecycle |
| 251 | `notebook.list-jupyter` | demonstrated | iteration-49: collab.notebook, core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions |
| 252 | `notebook.list-sql` | demonstrated | iteration-49: core.notebook.sql-list, core.notebook.sql-lifecycle |
| 253 | `notebook.save-jupyter` | demonstrated | iteration-49: collab.setup.notebook, collab.notebook, core.notebook.jupyter-lifecycle, core.notebook.jupyter-sessions |
| 254 | `notebook.save-sql` | demonstrated | iteration-49: core.notebook.sql-lifecycle |
| 255 | `notebook.sessions-jupyter` | demonstrated | iteration-49: core.notebook.jupyter-sessions |
| 256 | `notebook.unload-jupyter` | demonstrated | iteration-49: core.notebook.jupyter-sessions |
| 257 | `plugin.code-env-create` | demonstrated | iteration-49: infrastructure.plugin.code-env |
| 258 | `plugin.code-env-update` | demonstrated | iteration-49: infrastructure.plugin.code-env |
| 259 | `plugin.contents-delete` | demonstrated | iteration-49: infrastructure.plugin |
| 260 | `plugin.contents-get` | demonstrated | iteration-49: infrastructure.plugin, infrastructure.plugin.zip, infrastructure.plugin.git-sync, infrastructure.plugin.git-install |
| 261 | `plugin.contents-list` | demonstrated | iteration-49: infrastructure.plugin |
| 262 | `plugin.contents-put` | demonstrated | iteration-49: infrastructure.plugin |
| 263 | `plugin.create-dev` | demonstrated | iteration-49: infrastructure.plugin, infrastructure.plugin.git-remote, infrastructure.plugin.git-sync |
| 264 | `plugin.delete` | demonstrated | iteration-49: core.macro.abort, infrastructure.plugin, infrastructure.plugin.code-env, infrastructure.plugin.zip, infrastructure.plugin.git-remote, infrastructure.plugin.git-sync, infrastructure.plugin.git-install |
| 265 | `plugin.delete-git-remote` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 266 | `plugin.details` | demonstrated | iteration-49: infrastructure.plugin |
| 267 | `plugin.download` | demonstrated | iteration-49: infrastructure.plugin |
| 268 | `plugin.fetch` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 269 | `plugin.folder-add` | demonstrated | iteration-49: infrastructure.plugin |
| 270 | `plugin.get-git-remote` | demonstrated | iteration-49: infrastructure.plugin.git-remote, infrastructure.plugin.git-sync |
| 271 | `plugin.git-branches` | demonstrated | iteration-49: infrastructure.plugin.git-remote, infrastructure.plugin.git-sync |
| 272 | `plugin.install-from-git` | demonstrated | iteration-49: infrastructure.plugin.git-install |
| 273 | `plugin.install-from-store` | blocked | iteration-49: infrastructure.plugin.store |
| 274 | `plugin.install-from-zip` | demonstrated | iteration-49: core.macro.run, infrastructure.plugin.code-env, infrastructure.plugin.zip |
| 275 | `plugin.list` | demonstrated | iteration-49: infrastructure.plugin-reads, infrastructure.plugin, infrastructure.plugin.zip |
| 276 | `plugin.move` | demonstrated | iteration-49: infrastructure.plugin |
| 277 | `plugin.move-to-dev` | demonstrated | iteration-49: infrastructure.plugin.zip, infrastructure.plugin.git-install |
| 278 | `plugin.pull` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 279 | `plugin.push` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 280 | `plugin.rename` | demonstrated | iteration-49: infrastructure.plugin |
| 281 | `plugin.reset-local` | demonstrated | iteration-49: infrastructure.plugin.zip |
| 282 | `plugin.reset-remote` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 283 | `plugin.set-git-remote` | demonstrated | iteration-49: infrastructure.plugin.git-sync |
| 284 | `plugin.settings-get` | demonstrated | iteration-49: infrastructure.plugin, infrastructure.plugin.code-env |
| 285 | `plugin.settings-set` | demonstrated | iteration-49: infrastructure.plugin, infrastructure.plugin.code-env |
| 286 | `plugin.update-from-git` | demonstrated | iteration-49: infrastructure.plugin.git-install |
| 287 | `plugin.update-from-store` | blocked | iteration-49: infrastructure.plugin.store |
| 288 | `plugin.update-from-zip` | demonstrated | iteration-49: infrastructure.plugin.zip |
| 289 | `plugin.usages` | demonstrated | iteration-49: infrastructure.plugin-reads, infrastructure.plugin |
| 290 | `project-deployer.create-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 291 | `project-deployer.create-infra` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.infra-lifecycle, infrastructure.project-deployer.upload-bundle |
| 292 | `project-deployer.create-project` | demonstrated | iteration-49: core.bundle.publish, infrastructure.project-deployer |
| 293 | `project-deployer.delete-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.deployment-lifecycle, infrastructure.project-deployer.deploy |
| 294 | `project-deployer.deploy` | blocked | iteration-49: infrastructure.project-deployer.deploy |
| 295 | `project-deployer.deployment-status` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.deploy |
| 296 | `project-deployer.get-deployment` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.deployment-lifecycle |
| 297 | `project-deployer.list-deployments` | demonstrated | iteration-49: infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.project-deployer.reads, infrastructure.project-deployer.deployment-lifecycle |
| 298 | `project-deployer.list-infras` | demonstrated | iteration-49: infrastructure.deployer-lists, infrastructure.project-deployer.reads, infrastructure.project-deployer.infra-lifecycle |
| 299 | `project-deployer.list-projects` | demonstrated | iteration-49: infrastructure.deployer-lists, infrastructure.deployer-details, infrastructure.project-deployer.reads, infrastructure.project-deployer, infrastructure.project-deployer.upload-bundle |
| 300 | `project-deployer.project-status` | demonstrated | iteration-49: core.bundle.publish, infrastructure.deployer-details, infrastructure.project-deployer, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle |
| 301 | `project-deployer.save-deployment-settings` | demonstrated | iteration-49: infrastructure.project-deployer.deployment-lifecycle |
| 302 | `project-deployer.upload-bundle` | demonstrated | iteration-49: infrastructure.deployer-details, infrastructure.project-deployer.upload-bundle, infrastructure.project-deployer.deployment-lifecycle |
| 303 | `project-folder.create-child` | demonstrated | iteration-49: infrastructure.project-folder, infrastructure.project-folder.move-project |
| 304 | `project-folder.delete` | demonstrated | iteration-49: infrastructure.project-folder, infrastructure.project-folder.move-project |
| 305 | `project-folder.get` | demonstrated | iteration-49: infrastructure.project-folder, infrastructure.directory-reads |
| 306 | `project-folder.move` | demonstrated | iteration-49: infrastructure.project-folder |
| 307 | `project-folder.move-project` | demonstrated | iteration-49: infrastructure.project-folder.move-project |
| 308 | `project-folder.root` | demonstrated | iteration-49: infrastructure.project-folder, infrastructure.project-folder.move-project, infrastructure.directory-reads |
| 309 | `project-folder.settings-get` | demonstrated | iteration-49: infrastructure.directory-reads |
| 310 | `project-folder.settings-set` | demonstrated | iteration-49: infrastructure.project-folder |
| 311 | `project-git.add-library` | demonstrated | iteration-49: core.project-git.external-libraries |
| 312 | `project-git.branches` | demonstrated | iteration-49: core.project-git.inspect, core.project-git.branches |
| 313 | `project-git.commit` | demonstrated | iteration-49: core.project-git.commit, core.project-git.history |
| 314 | `project-git.create-branch` | demonstrated | iteration-49: core.project-git.branches |
| 315 | `project-git.create-tag` | demonstrated | iteration-49: core.project-git.tags |
| 316 | `project-git.current-branch` | demonstrated | iteration-49: core.project-git.inspect, core.project-git.branches |
| 317 | `project-git.delete-branch` | demonstrated | iteration-49: core.project-git.branches |
| 318 | `project-git.delete-tag` | demonstrated | iteration-49: core.project-git.tags |
| 319 | `project-git.diff` | demonstrated | iteration-49: core.project-git.inspect |
| 320 | `project-git.drop-and-rebuild` | demonstrated | iteration-49: core.project-git.drop-and-rebuild |
| 321 | `project-git.fetch` | demonstrated | iteration-49: core.project-git.remote-sync |
| 322 | `project-git.future-abort` | demonstrated | iteration-49: core.project-git.external-libraries, core.project-git.library-futures |
| 323 | `project-git.future-status` | demonstrated | iteration-49: core.project-git.library-futures |
| 324 | `project-git.future-wait` | demonstrated | iteration-49: core.project-git.library-futures |
| 325 | `project-git.get-remote` | demonstrated | iteration-49: core.project-git.inspect |
| 326 | `project-git.list-libraries` | demonstrated | iteration-49: core.project-git.inspect |
| 327 | `project-git.log` | demonstrated | iteration-49: core.project-git.inspect, core.project-git.commit, core.project-git.history, core.project-git.drop-and-rebuild |
| 328 | `project-git.pull` | demonstrated | iteration-49: core.project-git.remote-sync |
| 329 | `project-git.push` | demonstrated | iteration-49: core.project-git.remote-sync |
| 330 | `project-git.push-all-libraries` | demonstrated | iteration-49: core.project-git.external-libraries |
| 331 | `project-git.push-library` | demonstrated | iteration-49: core.project-git.external-libraries |
| 332 | `project-git.remove-library` | demonstrated | iteration-49: core.project-git.external-libraries |
| 333 | `project-git.remove-remote` | demonstrated | iteration-49: core.project-git.remote-sync |
| 334 | `project-git.reset-all-libraries` | demonstrated | iteration-49: core.project-git.external-libraries, core.project-git.library-futures |
| 335 | `project-git.reset-library` | demonstrated | iteration-49: core.project-git.external-libraries |
| 336 | `project-git.reset-to-head` | demonstrated | iteration-49: core.project-git.history |
| 337 | `project-git.reset-to-upstream` | demonstrated | iteration-49: core.project-git.remote-sync |
| 338 | `project-git.revert-commit` | demonstrated | iteration-49: core.project-git.history |
| 339 | `project-git.revert-to-revision` | demonstrated | iteration-49: core.project-git.history |
| 340 | `project-git.set-library` | demonstrated | iteration-49: core.project-git.external-libraries |
| 341 | `project-git.set-remote` | demonstrated | iteration-49: core.project-git.remote-sync |
| 342 | `project-git.status` | demonstrated | iteration-49: core.project-git.inspect, core.project-git.commit, core.project-git.branches, core.project-git.history, core.project-git.drop-and-rebuild |
| 343 | `project-git.switch` | demonstrated | iteration-49: core.project-git.branches |
| 344 | `project-git.tags` | demonstrated | iteration-49: core.project-git.inspect, core.project-git.tags |
| 345 | `project-library.create-file` | demonstrated | iteration-49: collab.project-library |
| 346 | `project-library.create-folder` | setup-baseline | retained setup receipts: collab.setup.project-library |
| 347 | `project-library.delete` | demonstrated | iteration-49: collab.project-library |
| 348 | `project-library.diff` | demonstrated | iteration-49: collab.project-library |
| 349 | `project-library.get` | demonstrated | iteration-49: collab.project-library |
| 350 | `project-library.get-bytes` | demonstrated | iteration-49: collab.project-library |
| 351 | `project-library.list` | demonstrated | iteration-49: collab.project-library |
| 352 | `project-library.move` | demonstrated | iteration-49: collab.project-library |
| 353 | `project-library.put` | demonstrated | iteration-49: collab.setup.project-library, collab.project-library |
| 354 | `project-library.rename` | demonstrated | iteration-49: collab.project-library |
| 355 | `project.create` | demonstrated | iteration-49: project.lifecycle, core.bundle.export-lifecycle, core.bundle.publish, core.project-git.inspect, core.flow-jobs.project, ml.lifecycle, ml.clustering-task, ml.mes.lifecycle, ml.mlflow.import, ml.mlflow.evaluate, infrastructure.sql-select, applications.instance-lifecycle, infrastructure.project-folder.move-project |
| 356 | `project.delete` | demonstrated | iteration-49: project.lifecycle, core.bundle.export-lifecycle, core.bundle.publish, ml.lifecycle, ml.clustering-task, ml.mes.lifecycle, ml.mlflow.import, ml.mlflow.evaluate, infrastructure.sql-select, applications.instance-lifecycle, applications.app.successor-lifecycle, applications.app.permissions-restore-cycle, infrastructure.project-folder.move-project, infrastructure.continuous-activities |
| 357 | `project.duplicate` | demonstrated | iteration-49: project.lifecycle |
| 358 | `project.export` | demonstrated | iteration-49: project.lifecycle |
| 359 | `project.flow` | demonstrated | iteration-49: core.project.reads |
| 360 | `project.get` | demonstrated | iteration-49: core.project.reads, core.project.metadata |
| 361 | `project.import` | demonstrated | iteration-49: project.lifecycle |
| 362 | `project.inspect-archive` | demonstrated | iteration-49: project.lifecycle |
| 363 | `project.list` | demonstrated | iteration-49: core.project.reads |
| 364 | `project.map` | demonstrated | iteration-49: core.project.reads |
| 365 | `project.metadata` | demonstrated | iteration-49: collab.project-metadata, core.project.metadata |
| 366 | `project.metadata-set` | demonstrated | iteration-49: core.project.metadata |
| 367 | `project.permissions-get` | demonstrated | iteration-49: core.project.permissions |
| 368 | `project.permissions-set` | demonstrated | iteration-49: core.project.permissions, applications.app.permissions-restore-cycle |
| 369 | `project.settings-get` | demonstrated | iteration-49: collab.project-metadata |
| 370 | `project.settings-set` | demonstrated | iteration-49: collab.project-metadata, applications.app.template-surface, applications.app.instance-ops, applications.app.successor-preflight, applications.app.successor-lifecycle, applications.app.permissions-restore-cycle, applications.app.set-manifest-version |
| 371 | `project.tags-get` | demonstrated | iteration-49: core.project.tags |
| 372 | `project.tags-set` | demonstrated | iteration-49: core.project.tags |
| 373 | `recipe.add-input` | demonstrated | iteration-49: core.recipe.lifecycle |
| 374 | `recipe.assert-unchanged` | demonstrated | iteration-49: core.recipe.restore |
| 375 | `recipe.cat` | demonstrated | iteration-49: core.recipe.lifecycle, core.recipe.restore |
| 376 | `recipe.clone` | demonstrated | iteration-49: core.recipe.clone |
| 377 | `recipe.create` | demonstrated | iteration-49: core.recipe.lifecycle, core.flow-jobs.project, core.dataset.partitions, infrastructure.continuous-activities |
| 378 | `recipe.delete` | demonstrated | iteration-49: core.recipe.lifecycle, infrastructure.continuous-activities |
| 379 | `recipe.diff` | demonstrated | iteration-49: core.recipe.lifecycle |
| 380 | `recipe.download` | demonstrated | iteration-49: core.recipe.lifecycle |
| 381 | `recipe.download-code` | demonstrated | iteration-49: core.recipe.lifecycle |
| 382 | `recipe.get` | demonstrated | iteration-49: core.recipe.graph, core.dataset.rename, core.recipe.clone, core.recipe.update, infrastructure.continuous-activities |
| 383 | `recipe.get-payload` | demonstrated | iteration-49: core.recipe.lifecycle |
| 384 | `recipe.list` | demonstrated | iteration-49: core.recipe.graph |
| 385 | `recipe.metadata` | demonstrated | iteration-49: core.recipe.update |
| 386 | `recipe.metadata-set` | demonstrated | iteration-49: core.recipe.update |
| 387 | `recipe.remove-input` | demonstrated | iteration-49: core.recipe.lifecycle |
| 388 | `recipe.restore` | demonstrated | iteration-49: core.recipe.restore |
| 389 | `recipe.run` | demonstrated | iteration-49: core.recipe.runs, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 390 | `recipe.set-payload` | demonstrated | iteration-49: core.recipe.lifecycle, core.flow-jobs.project, core.recipe.restore |
| 391 | `recipe.update` | demonstrated | iteration-49: core.recipe.update |
| 392 | `recipe.validate-graph` | demonstrated | iteration-49: core.recipe.graph, core.recipe.clone, ml.scoring.retained-recipe, ml.mes.retained-evaluation |
| 393 | `saved-model.create-external` | demonstrated | iteration-49: ml.mlflow.import, ml.mlflow.evaluate |
| 394 | `saved-model.delete` | demonstrated | iteration-49: ml.mlflow.import, ml.mlflow.evaluate |
| 395 | `saved-model.delete-versions` | demonstrated | iteration-49: ml.mlflow.import |
| 396 | `saved-model.download-scoring-jar` | blocked | iteration-49: ml.saved-model.scoring-jar |
| 397 | `saved-model.download-scoring-pmml` | blocked | iteration-49: ml.saved-model.scoring-pmml |
| 398 | `saved-model.evaluate-version` | demonstrated | iteration-49: ml.mlflow.evaluate |
| 399 | `saved-model.external-metadata-get` | demonstrated | iteration-49: ml.mlflow.import |
| 400 | `saved-model.external-metadata-put` | demonstrated | iteration-49: ml.mlflow.import, ml.mlflow.evaluate |
| 401 | `saved-model.get` | demonstrated | iteration-49: ml.lifecycle, ml.saved-model.metadata |
| 402 | `saved-model.import-mlflow-version` | demonstrated | iteration-49: ml.mlflow.import, ml.mlflow.evaluate |
| 403 | `saved-model.import-mlflow-version-from-folder` | demonstrated | iteration-49: ml.mlflow.import |
| 404 | `saved-model.list` | demonstrated | iteration-49: ml.lifecycle, ml.retained-fixtures |
| 405 | `saved-model.list-versions` | demonstrated | iteration-49: ml.lifecycle, ml.retained-fixtures, ml.saved-model.metadata, ml.mlflow.import |
| 406 | `saved-model.set-active` | demonstrated | iteration-49: ml.saved-model.metadata |
| 407 | `saved-model.set-user-meta` | demonstrated | iteration-49: ml.saved-model.metadata |
| 408 | `saved-model.update-settings` | demonstrated | iteration-49: ml.saved-model.metadata |
| 409 | `saved-model.version-details` | demonstrated | iteration-49: ml.lifecycle, ml.saved-model.metadata, ml.mlflow.import, ml.mlflow.evaluate |
| 410 | `saved-model.version-snippet` | demonstrated | iteration-49: ml.saved-model.metadata |
| 411 | `scenario.abort` | demonstrated | iteration-49: core.scenario.abort |
| 412 | `scenario.active-set` | demonstrated | iteration-49: core.scenario.activation |
| 413 | `scenario.create` | demonstrated | iteration-49: collab.setup.scenario, core.scenario.custom-lifecycle, core.scenario.activation, core.scenario.abort, applications.template-prerequisite |
| 414 | `scenario.delete` | demonstrated | iteration-49: core.scenario.custom-lifecycle, core.scenario.activation, core.scenario.abort |
| 415 | `scenario.get` | demonstrated | iteration-49: collab.scenario, core.scenario.custom-lifecycle, core.scenario.activation |
| 416 | `scenario.get-run` | demonstrated | iteration-49: core.scenario.custom-lifecycle, core.scenario.abort |
| 417 | `scenario.last-runs` | demonstrated | iteration-49: core.scenario.custom-lifecycle, core.scenario.abort |
| 418 | `scenario.list` | demonstrated | iteration-49: core.code.run, core.scenario.custom-lifecycle |
| 419 | `scenario.log` | demonstrated | iteration-49: core.scenario.custom-lifecycle |
| 420 | `scenario.payload-get` | demonstrated | iteration-49: core.scenario.custom-lifecycle |
| 421 | `scenario.payload-set` | demonstrated | iteration-49: core.scenario.custom-lifecycle, core.scenario.abort, applications.template-prerequisite |
| 422 | `scenario.run` | demonstrated | iteration-49: collab.scenario, core.scenario.custom-lifecycle, core.scenario.abort, applications.template-prerequisite |
| 423 | `scenario.run-and-wait` | demonstrated | iteration-49: collab.setup.scenario, collab.scenario |
| 424 | `scenario.status` | demonstrated | iteration-49: collab.scenario, core.scenario.activation, core.scenario.abort |
| 425 | `scenario.update` | demonstrated | iteration-49: collab.scenario, core.scenario.custom-lifecycle, applications.template-prerequisite |
| 426 | `sql.query` | demonstrated | iteration-49: infrastructure.sql-select |
| 427 | `statistics.create-worksheet` | demonstrated | iteration-49: core.statistics.worksheet-lifecycle, core.statistics.computation |
| 428 | `statistics.delete-worksheet` | demonstrated | iteration-49: core.statistics.worksheet-lifecycle, core.statistics.computation |
| 429 | `statistics.get-worksheet` | demonstrated | iteration-49: core.statistics.worksheet-lifecycle, core.statistics.computation |
| 430 | `statistics.list-worksheets` | demonstrated | iteration-49: core.statistics.worksheet-lifecycle |
| 431 | `statistics.run-card` | demonstrated | iteration-49: core.statistics.computation |
| 432 | `statistics.run-computation` | demonstrated | iteration-49: core.statistics.computation |
| 433 | `statistics.run-worksheet` | demonstrated | iteration-49: core.statistics.computation |
| 434 | `statistics.update-worksheet` | demonstrated | iteration-49: core.statistics.worksheet-lifecycle, core.statistics.computation |
| 435 | `streaming-endpoint.create` | blocked | iteration-49: infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 436 | `streaming-endpoint.delete` | blocked | iteration-49: infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 437 | `streaming-endpoint.get` | blocked | iteration-49: infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 438 | `streaming-endpoint.list` | blocked | iteration-49: infrastructure.streaming-endpoints |
| 439 | `streaming-endpoint.update-settings` | blocked | iteration-49: infrastructure.streaming-endpoints, infrastructure.continuous-activities |
| 440 | `user.activity` | demonstrated | iteration-49: infrastructure.user.activity, infrastructure.directory-reads |
| 441 | `user.activity-get` | demonstrated | iteration-49: infrastructure.user, infrastructure.user.activity, infrastructure.directory-reads |
| 442 | `user.create` | blocked | iteration-49: infrastructure.user, infrastructure.user.external-sync |
| 443 | `user.delete` | blocked | iteration-49: infrastructure.user, infrastructure.user.external-sync |
| 444 | `user.external-groups` | blocked | iteration-49: infrastructure.user.external-sync |
| 445 | `user.external-users` | blocked | iteration-49: infrastructure.user.external-sync |
| 446 | `user.get` | demonstrated | iteration-49: infrastructure.user, infrastructure.user.activity, infrastructure.directory-reads |
| 447 | `user.list` | demonstrated | iteration-49: infrastructure.user, infrastructure.directory-reads |
| 448 | `user.provision` | blocked | iteration-49: infrastructure.user.external-sync |
| 449 | `user.resync` | blocked | iteration-49: infrastructure.user.external-sync |
| 450 | `user.resync-multi` | blocked | iteration-49: infrastructure.user.external-sync |
| 451 | `user.update` | blocked | iteration-49: infrastructure.user |
| 452 | `variable.get` | demonstrated | iteration-49: collab.setup.variables, collab.variables, project.lifecycle |
| 453 | `variable.set` | demonstrated | iteration-49: collab.setup.variables, collab.variables, project.lifecycle, core.project-git.commit, core.project-git.history, applications.template-prerequisite |
| 454 | `version.run` | hermetic-offline | offline gates |
| 455 | `webapp.backend-state` | demonstrated | iteration-49: applications.webapp.lifecycle, applications.webapp.backend |
| 456 | `webapp.create` | demonstrated | iteration-49: applications.webapp.lifecycle, applications.webapp.backend |
| 457 | `webapp.get-settings` | demonstrated | iteration-49: applications.webapp.lifecycle |
| 458 | `webapp.list` | demonstrated | iteration-49: applications.webapp.lifecycle |
| 459 | `webapp.restart-backend` | demonstrated | iteration-49: applications.webapp.backend |
| 460 | `webapp.stop-backend` | demonstrated | iteration-49: applications.webapp.lifecycle, applications.webapp.backend |
| 461 | `webapp.update-settings` | demonstrated | iteration-49: applications.webapp.lifecycle, applications.webapp.backend |
| 462 | `wiki.create` | demonstrated | iteration-49: collab.setup.wiki, collab.wiki-lifecycle |
| 463 | `wiki.delete` | demonstrated | iteration-49: collab.wiki-lifecycle |
| 464 | `wiki.get` | demonstrated | iteration-49: collab.setup.wiki, collab.wiki, collab.wiki-lifecycle |
| 465 | `wiki.list` | demonstrated | iteration-49: collab.wiki |
| 466 | `wiki.settings` | demonstrated | iteration-49: collab.wiki |
| 467 | `wiki.update` | demonstrated | iteration-49: collab.wiki, collab.wiki-lifecycle |
| 468 | `workspace.add-object` | demonstrated | iteration-49: infrastructure.workspace |
| 469 | `workspace.create` | demonstrated | iteration-49: infrastructure.workspace |
| 470 | `workspace.delete` | demonstrated | iteration-49: infrastructure.workspace |
| 471 | `workspace.get` | demonstrated | iteration-49: infrastructure.workspace, infrastructure.directory-reads |
| 472 | `workspace.list` | demonstrated | iteration-49: infrastructure.workspace, infrastructure.directory-reads |
| 473 | `workspace.list-objects` | demonstrated | iteration-49: infrastructure.workspace, infrastructure.directory-reads |
| 474 | `workspace.update-settings` | demonstrated | iteration-49: infrastructure.workspace |
