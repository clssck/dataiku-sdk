# DSS 15 REST API coverage matrix

Complete inventory of the official DSS 15 REST API (every `#### <name>[METHOD](#anchor)` endpoint entry in the official static docs), mapped to the actual SDK resource methods and CLI actions in this repository. Unsupported endpoints are marked honestly; no coverage percentage is claimed.

## Source & version

- **API:** Dataiku DSS 15 REST API (public API; all calls relative to `/public/api` on the DSS server).
- **Official docs:** https://doc.dataiku.com/dss/api/15/rest/
- **Snapshot:** official static HTML, retrieved **2026-09-09**; DSS major version **15**.
- **Machine-readable companion:** [docs/API-COVERAGE-DSS-15.json](./API-COVERAGE-DSS-15.json) (same rows plus `meta`).
- **CLI catalogue (actual programmatic registry):** `buildCommandRegistry()` / `commandActionSummary()` = **55 resources / 474 actions**, of which 47 resources / 466 actions are API-facing and 8 are meta-commands (`agent`, `auth`, `batch`, `cleanup`, `commands`, `fixtures`, `install-skill`, `version`).

## Counting rules (read before citing numbers)

- One row = one documented operation entry from the official docs (method + URI, identity by doc anchor): **372 entries across 37 sections**. The official page documents `GET /admin/connections/{connectionName}` twice (a known doc duplication), so the 372 entries resolve to **371 distinct (method, URI) pairs**; both connection entries are retained.
- Query-parameter variants documented on one row are not double-counted (the docs fold them into `{?param}` suffixes).
- Where the SDK reaches the same capability via a different verb or path shape, the row is marked **implemented** with a drift note (never counted twice as implemented + unsupported).
- **No parity percentage is claimed.** Published tallies are raw row counts validated against the programmatic CLI registry at snapshot time.
- Working-tree snapshot 2026-09-09: **294 implemented**, **78 not implemented** of 372 official rows.

## Mapping validation (machine-checked)

- Every nonempty CLI mapping in this matrix exists in the current catalogue (`buildCommandRegistry()` summary: 55/474); the correct action is the one whose handler actually calls the mapped SDK method (hyphenated quoted action keys parsed structurally).
- Every cited SDK method is a public method on its client resource class; private helpers are cited only through the public methods that call them.
- 8 meta-commands (`agent.contract`, `auth.login`, `batch.*`, `cleanup.*`, `commands.run`, `fixtures.*`, `install-skill.*`, `version.run`) drive no REST endpoint and never appear as coverage; the API-facing subset is 47/466.
- This document describes source-level wiring only and makes no live-instance compatibility claims.

## Legend

- Status `yes` - an SDK method exercises this endpoint. SDK column: `Class.method`; CLI column: registered actions that route through it (including pre-check calls inside handlers).
- Status `no` - no SDK method at snapshot. Honest gap; this document records it, it does not mandate implementing it.
- Notes flag documented-vs-implementation drift (verb, path shape, body/query parameters) or scope decisions.

## Per-section totals

| Section | Implemented | Total |
|---|---|---|
| Project Folders | 8 | 8 |
| Projects | 12 | 14 |
| Workspaces | 7 | 8 |
| Collections | 8 | 8 |
| Data Quality | 11 | 14 |
| Flow | 0 | 4 |
| Datasets | 21 | 26 |
| LLM Mesh | 3 | 3 |
| Knowledge Banks | 2 | 2 |
| Dataset Statistics | 7 | 7 |
| Jobs | 5 | 5 |
| Scenarios | 16 | 16 |
| Machine Learning - Lab | 10 | 32 |
| Machine Learning - Saved models | 15 | 28 |
| Machine Learning - Experiment tracking | 0 | 6 |
| Managed Folders | 9 | 9 |
| Recipes | 7 | 7 |
| Streaming endpoints | 5 | 8 |
| Continuous activities | 4 | 4 |
| Webapps | 6 | 7 |
| Notebooks | 8 | 8 |
| Macros | 6 | 6 |
| Long tasks | 2 | 3 |
| Meanings | 4 | 4 |
| Plugins | 33 | 33 |
| Libraries | 7 | 7 |
| API Services | 6 | 6 |
| Bundles, Design-side | 5 | 6 |
| Bundles, Automation-side | 5 | 7 |
| Project Deployer | 12 | 24 |
| Wiki | 4 | 5 |
| Discussions | 4 | 5 |
| Dashboards | 6 | 6 |
| Insights | 5 | 5 |
| SQL queries | 3 | 3 |
| Connections | 7 | 7 |
| Security | 21 | 21 |
| **Total** | **294** | **372** |

## Endpoint matrix

### Project Folders (8/8)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/project-folders/` | yes | ProjectFoldersResource.root | `dss project-folder root` |  |
| `GET` | `/project-folders/{folderId}` | yes | ProjectFoldersResource.get | `dss project-folder delete`, `dss project-folder get` |  |
| `GET` | `/project-folders/{folderId}/settings` | yes | ProjectFoldersResource.getSettings | `dss project-folder settings-get` |  |
| `PUT` | `/project-folders/{folderId}/settings` | yes | ProjectFoldersResource.updateSettings | `dss project-folder settings-set` |  |
| `POST` | `/project-folders/{folderId}/move{?destination}` | yes | ProjectFoldersResource.move | `dss project-folder move` |  |
| `DELETE` | `/project-folders/{folderId}` | yes | ProjectFoldersResource.delete | `dss project-folder delete` |  |
| `POST` | `/project-folders/{folderId}/children{?name}` | yes | ProjectFoldersResource.createChild | `dss project-folder create-child` |  |
| `POST` | `/project-folders/{folderId}/projects/{projectKey}/move{?destination}` | yes | ProjectFoldersResource.moveProject | `dss project-folder move-project` |  |

### Projects (12/14)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{tags}` | yes | ProjectsResource.list | `dss project list` | Docs publish the list endpoint as GET /projects/{tags} with an optional tags selector; the SDK GETs /projects/ and filters client-side. The same normalized shape also matches GET /projects/{projectKey}/ (project details), so both list and get land on this row. |
| `POST` | `/projects{?projectFolderId}` | yes | ProjectsResource.createProject | `dss project create` |  |
| `GET` | `/projects/{projectKey}/metadata` | yes | ProjectsResource.metadata | `dss project metadata` |  |
| `PUT` | `/projects/{projectKey}/metadata` | no |  |  | SDK exposes GET metadata only; no PUT wrapper at snapshot. |
| `GET` | `/projects/{projectKey}/permissions` | yes | ProjectsResource.getPermissions | `dss app permissions-diff`, `dss app permissions-restore`, `dss app permissions-snapshot`, `dss project permissions-get` |  |
| `PUT` | `/projects/{projectKey}/permissions` | yes | ProjectsResource.setPermissions | `dss app permissions-restore`, `dss project permissions-set` |  |
| `GET` | `/projects/{projectKey}/variables` | yes | VariablesResource.get, VariablesResource.set | `dss variable get`, `dss variable set` |  |
| `PUT` | `/projects/{projectKey}/variables` | yes | VariablesResource.set | `dss variable set` |  |
| `DELETE` | `/projects/{projectKey}{?dropData}` | yes | ApplicationsResource.deleteInstance, ProjectsResource.deleteProject | `dss app delete-instance`, `dss project delete` |  |
| `GET` | `/projects/{projectKey}/export{?exportUploads}{?exportManaged}{?exportAnalysisModels}{?exportSavedModels}` | yes | ProjectsResource.exportArchive | `dss project export` | Verb drift: official GET /export{?exportUploads}{?exportManaged}{?exportAnalysisModels}{?exportSavedModels}; SDK POSTs /export and streams the archive. |
| `POST` | `/projects/{projectKey}/duplicate` | yes | ProjectsResource.duplicate | `dss project duplicate` |  |
| `POST` | `/projects/{projectKey}/actions/push-to-git-remote{?remote}` | no |  |  | project-git resource drives the Git remotes/branches/tags/actions family; this one-shot action endpoint is not wrapped. |
| `GET` | `/projects/{projectKey}/tags` | yes | ProjectsResource.tags | `dss project tags-get` |  |
| `PUT` | `/projects/{projectKey}/tags` | yes | ProjectsResource.setTags | `dss project tags-set` |  |

### Workspaces (7/8)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/workspaces` | yes | WorkspacesResource.list | `dss workspace list` |  |
| `POST` | `/workspaces` | yes | WorkspacesResource.create | `dss workspace create` |  |
| `GET` | `/workspaces/{workspaceKey}` | yes | WorkspacesResource.get | `dss workspace get` |  |
| `PUT` | `/workspaces/{workspaceKey}` | yes | WorkspacesResource.updateSettings | `dss workspace update-settings` |  |
| `DELETE` | `/workspaces/{workspaceKey}` | yes | WorkspacesResource.delete | `dss workspace delete` |  |
| `GET` | `/workspaces/{workspaceKey}/objects` | yes | WorkspacesResource.listObjects | `dss workspace list-objects` |  |
| `POST` | `/workspaces/{workspaceKey}/objects` | yes | WorkspacesResource.addObject | `dss workspace add-object` |  |
| `DELETE` | `/workspaces/{workspaceKey}/objects/{workspaceObjectId}` | no |  |  |  |

### Collections (8/8)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/data-collections/` | yes | DataCollectionsResource.list | `dss data-collection list` |  |
| `POST` | `/data-collections/` | yes | DataCollectionsResource.create | `dss data-collection create` |  |
| `GET` | `/data-collections/{dataCollectionId}` | yes | DataCollectionsResource.get | `dss data-collection get` |  |
| `PUT` | `/data-collections/{dataCollectionId}` | yes | DataCollectionsResource.update | `dss data-collection settings-set` |  |
| `DELETE` | `/data-collections/{dataCollectionId}` | yes | DataCollectionsResource.delete | `dss data-collection delete` |  |
| `GET` | `/data-collections/{dataCollectionId}/objects` | yes | DataCollectionsResource.listObjects | `dss data-collection list-objects` |  |
| `POST` | `/data-collections/{dataCollectionId}/objects` | yes | DataCollectionsResource.addObject | `dss data-collection add-object` |  |
| `DELETE` | `/data-collections/{dataCollectionId}/objects/dataset/{projectKey}/{datasetName}` | yes | DataCollectionsResource.removeDataset | `dss data-collection remove-dataset` |  |

### Data Quality (11/14)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/data-quality/status` | no |  |  | Instance-wide DQ status not wrapped; DQ resource covers project and dataset level. |
| `GET` | `/projects/{projectKey}/data-quality/status{?onlyMonitored}` | yes | DataQualityResource.projectStatus | `dss data-quality project-status` |  |
| `GET` | `/projects/{projectKey}/data-quality/timeline{?minTimestamp}{?maxTimestamp}` | yes | DataQualityResource.projectTimeline | `dss data-quality project-timeline` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/rules` | yes | DataQualityResource.getRule, DataQualityResource.listRules, DataQualityResource.rules, DataQualityResource.updateRule | `dss data-quality create-rule`, `dss data-quality delete-rule`, `dss data-quality get-rule`, `dss data-quality rules`, `dss data-quality update-rule` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/rules` | yes | DataQualityResource.createRule | `dss data-quality create-rule` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/get-partitions-status` | no |  |  |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/actions/compute-rules` | yes | DataQualityResource.computeRules, DataQualityResource.computeRulesAndWait | `dss data-quality compute` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/status` | yes | DataQualityResource.status | `dss data-quality status` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/status-by-partition{?includeAllPartitions}` | yes | DataQualityResource.status, DataQualityResource.statusByPartition | `dss data-quality status`, `dss data-quality status-by-partition` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/last-rules-result{?partition}{?ruleId}` | yes | DataQualityResource.lastResults | `dss data-quality assert-results`, `dss data-quality last-results` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/rules-history{?minTimestamp}{?maxTimestamp}{?resultsPerPage}{?page}{?ruleIds}` | yes | DataQualityResource.history | `dss data-quality history` |  |
| `PUT` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/rules/{ruleId}` | yes | DataQualityResource.updateRule | `dss data-quality update-rule` |  |
| `DELETE` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/rules/{ruleId}` | yes | DataQualityResource.deleteRule | `dss data-quality delete-rule` |  |
| `DELETE` | `/projects/{projectKey}/datasets/{datasetName}/data-quality/{partition}/rules-history` | no |  |  |  |

### Flow (0/4)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `POST` | `/projects/{projectKey}/flow/documentation/generate` | no |  |  | utils/flow-analysis.ts is local graph analysis of GET /flow/graph, not flow documentation export. |
| `POST` | `/projects/{projectKey}/flow/documentation/generate-with-template` | no |  |  |  |
| `POST` | `/projects/{projectKey}/flow/documentation/generate-with-template-in-folder{?folderId}{?path}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/flow/documentation/generated/{exportId}` | no |  |  |  |

### Datasets (21/26)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/datasets/{?tags}{?foreign}` | yes | DatasetsResource.list, ProjectsResource.map | `dss dataset create`, `dss dataset list`, `dss project map` |  |
| `POST` | `/projects/{projectKey}/datasets` | yes | DatasetsResource.create, RecipesResource.create | `dss dataset create`, `dss recipe create` |  |
| `POST` | `/projects/{projectKey}/datasets/managed` | yes | DatasetsResource.createManaged | `dss dataset create-managed` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}` | yes | DatasetsResource.get | `dss dataset clone`, `dss dataset delete`, `dss dataset get`, `dss dataset source`, `dss dataset update` |  |
| `PUT` | `/projects/{projectKey}/datasets/{datasetName}` | yes | DatasetsResource.update | `dss dataset update` |  |
| `DELETE` | `/projects/{projectKey}/datasets/{datasetName}{?dropData}` | yes | DatasetsResource.delete, RecipesResource.create | `dss dataset delete`, `dss recipe create` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/metadata` | yes | DatasetsResource.metadata | `dss dataset metadata` |  |
| `PUT` | `/projects/{projectKey}/datasets/{datasetName}/metadata` | no |  |  |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/info` | yes | DatasetsResource.info | `dss dataset info` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/schema` | yes | DatasetsResource.getSchemaObject, DatasetsResource.schema | `dss dataset assert-schema`, `dss dataset refresh-schema`, `dss dataset schema` |  |
| `PUT` | `/projects/{projectKey}/datasets/{datasetName}/schema` | yes | DatasetsResource.updateSchema | `dss dataset refresh-schema` |  |
| `GET` | `/projects/datasets/column-lineage{?columnName}{?maxDatasetCount}` | yes | DatasetsResource.getColumnLineage | `dss dataset column-lineage` | Path drift: docs render a merged project-scoped route GET /projects/{pk}/datasets/column-lineage{?columnName}{?maxDatasetCount}; the SDK (matching the official Python client) calls dataset-scoped GET /projects/{pk}/datasets/{datasetName}/column-lineage?columnName=...&maxDatasetCount=... |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/data{?format}{?formatParams}{?columns}{?partitions}{?filter}{?sampling}` | yes | DatasetsResource.assertRowCount | `dss dataset assert-count` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/data` | no |  |  | Data write/upload endpoint not wrapped; SDK reads data via streaming GET only. |
| `DELETE` | `/projects/{projectKey}/datasets/{datasetName}/data/{?partitions}` | yes | DatasetsResource.clear | `dss dataset clear` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/partitions` | yes | DatasetsResource.listPartitions | `dss dataset list-partitions` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/metrics/last/{?partition}` | yes | MetricsResource.getDatasetMetrics | `dss metrics dataset-get` | SDK appends literal NP (no-partition) segment; official uses optional {?partition} query. |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/metrics/history/{?partition}{?metricLookup}` | yes | MetricsResource.getDatasetMetricHistory | `dss metrics dataset-history` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/actions/synchronizeHiveMetastore` | no |  |  |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/actions/updateFromHive` | no |  |  |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/actions/computeMetrics/{?partitions}` | yes | MetricsResource.computeDatasetMetrics | `dss metrics dataset-compute` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/actions/runChecks/{?partitions}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/datasets/tables-import/actions/list-schemas{?connectionName}` | yes | ConnectionsResource.schemas | `dss connection schemas` |  |
| `GET` | `/projects/{projectKey}/datasets/tables-import/actions/list-tables{?connectionName}{?catalogName}{?schemaName}` | yes | ConnectionsResource.tables | `dss connection tables` |  |
| `POST` | `/projects/{projectKey}/datasets/tables-import/actions/prepare-from-keys` | yes | ConnectionsResource.prepareTablesImport | `dss connection prepare-import` |  |
| `POST` | `/projects/{projectKey}/datasets/tables-import/actions/execute-from-candidates` | yes | ConnectionsResource.executeTablesImport | `dss connection execute-import` |  |

### LLM Mesh (3/3)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/llms/{?purpose}` | yes | LlmsResource.list | `dss llm list` | Optional ?purpose query forwarded; SDK path keeps the trailing slash (/llms/). |
| `POST` | `/projects/{projectKey}/llms/completions` | yes | LlmsResource.completions | `dss llm completions` |  |
| `POST` | `/projects/{projectKey}/llms/embeddings` | yes | LlmsResource.embeddings | `dss llm embeddings` |  |

### Knowledge Banks (2/2)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `POST` | `/projects/{projectKey}/knowledge-banks/{knowledgeBankId}/search` | yes | KnowledgeBanksResource.search | `dss knowledge-bank search` |  |
| `POST` | `/projects/{projectKey}/knowledge-banks/{knowledgeBankId}/clear` | yes | KnowledgeBanksResource.clear | `dss knowledge-bank clear` |  |

### Dataset Statistics (7/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets/` | yes | StatisticsResource.listWorksheets | `dss statistics list-worksheets` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets` | yes | StatisticsResource.createWorksheet | `dss statistics create-worksheet` |  |
| `GET` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets/{worksheetId}` | yes | StatisticsResource.getWorksheet, StatisticsResource.runWorksheet | `dss statistics get-worksheet`, `dss statistics run-worksheet` |  |
| `PUT` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets/{worksheetId}` | yes | StatisticsResource.updateWorksheet | `dss statistics update-worksheet` |  |
| `DELETE` | `/projects/{projectKey}/datasets/statistics/worksheets/{worksheetId}` | yes | StatisticsResource.deleteWorksheet | `dss statistics delete-worksheet` | Docs quirk: official DELETE URI omits {datasetName}; SDK scopes worksheets under the dataset path. |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets/{worksheetId}/actions/run-card` | yes | StatisticsResource.runCard | `dss statistics run-card` |  |
| `POST` | `/projects/{projectKey}/datasets/{datasetName}/statistics/worksheets/{worksheetId}/actions/run-computation` | yes | StatisticsResource.runComputation | `dss statistics run-computation` |  |

### Jobs (5/5)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/jobs/{?limit}` | yes | JobsResource.list | `dss job list` |  |
| `POST` | `/projects/{projectKey}/jobs` | yes | JobsResource.build, JobsResource.buildAndWait, JobsResource.buildOutputs | `dss job build`, `dss job build-and-wait` |  |
| `GET` | `/projects/{projectKey}/jobs/{jobId}` | yes | JobsResource.get, JobsResource.wait | `dss job get`, `dss job monitor`, `dss job wait`, `dss job watch` |  |
| `POST` | `/projects/{projectKey}/jobs/{jobId}/abort` | yes | JobsResource.abort | `dss job abort` |  |
| `GET` | `/projects/{projectKey}/jobs/{jobId}/log{?activity}` | yes | JobsResource.log | `dss job log` |  |

### Scenarios (16/16)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/scenarios` | yes | ScenariosResource.list | `dss scenario create`, `dss scenario list` |  |
| `POST` | `/projects/{projectKey}/scenarios` | yes | ScenariosResource.create, ScenariosResource.runScript | `dss code run`, `dss scenario create` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/` | yes | ScenariosResource.get, ScenariosResource.update | `dss scenario delete`, `dss scenario get`, `dss scenario update` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/light` | yes | ScenariosResource.status, ScenariosResource.setActive | `dss scenario status`, `dss scenario active-set` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/payload` | yes | ScenariosResource.getPayload | `dss scenario payload-get` |  |
| `DELETE` | `/projects/{projectKey}/scenarios/{scenarioId}/` | yes | ScenariosResource.delete | `dss scenario delete` |  |
| `POST` | `/projects/{projectKey}/scenarios/{scenarioId}/run` | yes | ScenariosResource.run, ScenariosResource.runAndWait | `dss scenario run`, `dss scenario run-and-wait` |  |
| `POST` | `/projects/{projectKey}/scenarios/{scenarioId}/abort` | yes | ScenariosResource.abort | `dss scenario abort` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/get-last-runs/{?limit}` | yes | ScenariosResource.getLastRuns | `dss scenario last-runs` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/get-run-for-trigger/{?triggerId}{?triggerRunId}` | yes | ScenariosResource.runAndWait, ScenariosResource.runScript | `dss code run`, `dss scenario run`, `dss scenario run-and-wait` | Tracking internals use GET /scenarios/{id}/get-run-for-trigger and GET /scenarios/trigger/{scenarioId}/{triggerId} through a private helper; public entry points are runAndWait/runScript. |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/{runId}` | yes | ScenariosResource.getRunDetails | `dss scenario get-run` |  |
| `GET` | `/projects/{projectKey}/scenarios/{scenarioId}/{runId}/log{?stepId}` | yes | ScenariosResource.getRunLog | `dss scenario log` |  |
| `GET` | `/projects/{projectKey}/scenarios/trigger/{scenarioId}/{triggerId}{?triggerRunId}` | yes | ScenariosResource.runAndWait, ScenariosResource.runScript | `dss code run`, `dss scenario run`, `dss scenario run-and-wait` | Same private tracking helper as get-run-for-trigger; public entry points are runAndWait/runScript. |
| `PUT` | `/projects/{projectKey}/scenarios/{scenarioId}` | yes | ScenariosResource.update | `dss scenario update` |  |
| `PUT` | `/projects/{projectKey}/scenarios/{scenarioId}/light` | yes | ScenariosResource.setActive | `dss scenario active-set` | SDK setActive verifies via GET before/after PUT of the light form. |
| `PUT` | `/projects/{projectKey}/scenarios/{scenarioId}/payload` | yes | ScenariosResource.setPayload | `dss scenario payload-set` |  |

### Machine Learning - Lab (10/32)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/lab` | yes | AnalysesResource.list | `dss analysis list` |  |
| `POST` | `/projects/{projectKey}/lab` | yes | AnalysesResource.create | `dss analysis create` |  |
| `GET` | `/projects/{projectKey}/lab/{analysisId}` | yes | AnalysesResource.get | `dss analysis delete`, `dss analysis get` |  |
| `PUT` | `/projects/{projectKey}/lab/{analysisId}` | no |  |  |  |
| `DELETE` | `/projects/{projectKey}/lab/{analysisId}` | yes | AnalysesResource.delete | `dss analysis delete` |  |
| `GET` | `/projects/{projectKey}/lab/{analysisId}/models` | no |  |  |  |
| `POST` | `/projects/{projectKey}/lab/{analysisId}/models` | yes | MlTasksResource.create | `dss ml-task create` |  |
| `GET` | `/projects/{projectKey}/models/lab` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/settings` | yes | MlTasksResource.getSettings | `dss ml-task get-settings` |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/status` | yes | MlTasksResource.listTrainedModels, MlTasksResource.status | `dss ml-task list-models`, `dss ml-task status` |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/guess{?predictionType}{?targetVariable}{?timeVariable}{?timeseriesIdentifiers}{?fullReguess}` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/reguess-with-forecasting-params` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/train` | yes | MlTasksResource.train | `dss ml-task train` |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models-snippets` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/details` | yes | MlTasksResource.trainedModelDetails | `dss ml-task model-details` |  |
| `PUT` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/user-meta` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/actions/deployToFlow` | yes | MlTasksResource.deployToFlow | `dss ml-task deploy` |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/scoring-jar{?fullClassName}{?includeLibs}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/scoring-pmml` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/subpopulation-analyses` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/subpopulation-analyses` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/partial-dependencies` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/partial-dependencies` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/timeseries-residuals` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/timeseries-residuals` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/per-timeseries-metrics` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/per-timeseries-evaluation-forecasts` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/generate-documentation-from-default-template` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/generate-documentation-from-custom-template` | no |  |  |  |
| `POST` | `/projects/{projectKey}/models/lab/{analysisId}/{mlTaskId}/models/{modelFullId}/generate-documentation-from-template-in-folder{?folderId}{?path}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/models/lab/documentations/{exportId}` | no |  |  |  |

### Machine Learning - Saved models (15/28)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/savedmodels` | yes | SavedModelsResource.list | `dss saved-model list` |  |
| `POST` | `/projects/{projectKey}/savedmodels/create-external` | yes | SavedModelsResource.createExternal | `dss saved-model create-external` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}` | yes | SavedModelsResource.get | `dss saved-model delete`, `dss saved-model get` |  |
| `PUT` | `/projects/{projectKey}/savedmodels/{savedModelId}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions` | yes | SavedModelsResource.listVersions | `dss saved-model list-versions` |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}{?codeEnvName}{?containerExecConfigName}{?folderRef}{?path}` | yes | SavedModelsResource.importMlflowVersion, SavedModelsResource.importMlflowVersionFromFolder | `dss saved-model import-mlflow-version`, `dss saved-model import-mlflow-version-from-folder` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/snippet` | yes | SavedModelsResource.versionSnippet | `dss saved-model version-snippet` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/details` | yes | SavedModelsResource.versionDetails | `dss saved-model version-details` |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/actions/setActive` | yes | SavedModelsResource.setActiveVersion | `dss saved-model set-active` |  |
| `PUT` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/user-meta` | yes | SavedModelsResource.setUserMeta | `dss saved-model set-user-meta` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/delete-versions{?versions}{?removeIntermediate}` | yes | SavedModelsResource.delete | `dss saved-model delete` | DRIFT (documented): official REST page shows mutating GET /projects/{projectKey}/savedmodels/{savedModelId}/delete-versions{?versions}{?removeIntermediate}; the official Python client and this SDK instead POST /projects/{projectKey}/savedmodels/{savedModelId}/actions/delete-versions with JSON body {versions, removeIntermediate}. Python-verified against dataiku-api-client-python master on 2026-09-09. |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/external-ml/metadata` | yes | SavedModelsResource.externalMetadataGet | `dss saved-model external-metadata-get` |  |
| `PUT` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/external-ml/metadata` | yes | SavedModelsResource.externalMetadataPut | `dss saved-model external-metadata-put` |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/external-ml/actions/evaluate` | yes | SavedModelsResource.evaluateVersion | `dss saved-model evaluate-version` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/scoring-jar{?fullClassName}{?includeLibs}` | yes | SavedModelsResource.downloadScoringJar | `dss saved-model download-scoring-jar` |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/scoring-pmml` | yes | SavedModelsResource.downloadScoringPmml | `dss saved-model download-scoring-pmml` |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/subpopulation-analyses` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/subpopulation-analyses` | no |  |  |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/partial-dependencies` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/partial-dependencies` | no |  |  |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/timeseries-residuals` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/timeseries-residuals` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/per-timeseries-metrics` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/per-timeseries-evaluation-forecasts` | no |  |  |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/generate-documentation-from-default-template` | no |  |  |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/generate-documentation-from-custom-template` | no |  |  |  |
| `POST` | `/projects/{projectKey}/savedmodels/{savedModelId}/versions/{versionId}/generate-documentation-from-template-in-folder{?folderId}{?path}` | no |  |  |  |
| `GET` | `/projects/{projectKey}/savedmodels/documentations/{exportId}` | no |  |  |  |

### Machine Learning - Experiment tracking (0/6)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/api/2.0/mlflow/extension/models/{runId}` | no |  |  |  |
| `POST` | `/api/2.0/mlflow/extension/set-run-inference-info` | no |  |  |  |
| `POST` | `/api/2.0/mlflow/extension/deploy-run` | no |  |  |  |
| `POST` | `/api/2.0/mlflow/extension/create-project-experiments-dataset` | no |  |  |  |
| `POST` | `/api/2.0/mlflow/extension/garbage-collect` | no |  |  |  |
| `DELETE` | `/api/2.0/mlflow/extension/clean-db/{projectKey}` | no |  |  |  |

### Managed Folders (9/9)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/managedfolders/` | yes | FoldersResource.list, FoldersResource.resolveId, ProjectsResource.map | `dss folder create`, `dss folder list`, `dss project map` |  |
| `POST` | `/projects/{projectKey}/managedfolders` | yes | FoldersResource.create | `dss folder create` |  |
| `GET` | `/projects/{projectKey}/managedfolders/{folderId}` | yes | FoldersResource.get, FoldersResource.update | `dss folder delete`, `dss folder get`, `dss folder update` |  |
| `PUT` | `/projects/{projectKey}/managedfolders/{folderId}` | yes | FoldersResource.update | `dss folder update` |  |
| `DELETE` | `/projects/{projectKey}/managedfolders/{folderId}` | yes | FoldersResource.delete | `dss folder delete` |  |
| `GET` | `/projects/{projectKey}/managedfolders/{folderId}/contents/` | yes | FoldersResource.contents | `dss folder contents` |  |
| `GET` | `/projects/{projectKey}/managedfolders/{folderId}/contents/{path}` | yes | FoldersResource.download | `dss folder download` |  |
| `DELETE` | `/projects/{projectKey}/managedfolders/{folderId}/contents/{path}` | yes | FoldersResource.deleteFile | `dss folder delete-file` |  |
| `POST` | `/projects/{projectKey}/managedfolders/{folderId}/contents/{path}` | yes | FoldersResource.upload | `dss folder upload` |  |

### Recipes (7/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/recipes/` | yes | ProjectsResource.map, RecipesResource.list | `dss project map`, `dss recipe create`, `dss recipe list` |  |
| `POST` | `/projects/{projectKey}/recipes` | yes | RecipesResource.create | `dss recipe create` |  |
| `GET` | `/projects/{projectKey}/recipes/{recipeName}` | yes | RecipesResource.clone, RecipesResource.download, RecipesResource.resolveRunOutputs, RecipesResource.run, RecipesResource.update, RecipesResource.validateGraph | `dss recipe add-input`, `dss recipe clone`, `dss recipe download`, `dss recipe remove-input`, `dss recipe run`, `dss recipe update`, `dss recipe validate-graph` |  |
| `PUT` | `/projects/{projectKey}/recipes/{recipeName}` | yes | RecipesResource.create, RecipesResource.replace, RecipesResource.setPayload | `dss recipe create`, `dss recipe restore`, `dss recipe set-payload` |  |
| `DELETE` | `/projects/{projectKey}/recipes/{recipeName}` | yes | RecipesResource.delete | `dss recipe delete` |  |
| `GET` | `/projects/{projectKey}/recipes/{recipeName}/metadata` | yes | RecipesResource.metadata | `dss recipe metadata` |  |
| `PUT` | `/projects/{projectKey}/recipes/{recipeName}/metadata` | yes | RecipesResource.setMetadata | `dss recipe metadata-set` |  |

### Streaming endpoints (5/8)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/streamingendpoints/` | yes | StreamingEndpointsResource.list | `dss streaming-endpoint list` |  |
| `POST` | `/projects/{projectKey}/streamingendpoints/` | yes | StreamingEndpointsResource.create | `dss streaming-endpoint create` |  |
| `POST` | `/projects/{projectKey}/streamingendpoints/managed` | no |  |  |  |
| `GET` | `/projects/{projectKey}/streamingendpoints/{streamingEndpointId}` | yes | StreamingEndpointsResource.get, StreamingEndpointsResource.getSettings | `dss streaming-endpoint get` |  |
| `PUT` | `/projects/{projectKey}/streamingendpoints/{streamingEndpointId}` | yes | StreamingEndpointsResource.updateSettings | `dss streaming-endpoint update-settings` |  |
| `DELETE` | `/projects/{projectKey}/streamingendpoints/{streamingEndpointId}` | yes | StreamingEndpointsResource.delete | `dss streaming-endpoint delete` |  |
| `GET` | `/projects/{projectKey}/streamingendpoints/{streamingEndpointId}/schema` | no |  |  |  |
| `PUT` | `/projects/{projectKey}/streamingendpoints/{streamingEndpointId}/schema` | no |  |  |  |

### Continuous activities (4/4)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/continuous-activities` | yes | ContinuousActivitiesResource.list | `dss continuous-activity list` |  |
| `GET` | `/projects/{projectKey}/continuous-activities/{recipeId}` | yes | ContinuousActivitiesResource.getStatus, ContinuousActivitiesResource.stop | `dss continuous-activity status`, `dss continuous-activity stop` |  |
| `POST` | `/projects/{projectKey}/continuous-activities/{recipeId}/start` | yes | ContinuousActivitiesResource.start | `dss continuous-activity start` |  |
| `POST` | `/projects/{projectKey}/continuous-activities/{recipeId}/stop` | yes | ContinuousActivitiesResource.stop | `dss continuous-activity stop` |  |

### Webapps (6/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/webapps/` | yes | WebappsResource.list | `dss webapp list` |  |
| `GET` | `/projects/{projectKey}/webapps/{webappId}` | yes | WebappsResource.getSettings, WebappsResource.updateSettings | `dss webapp get-settings`, `dss webapp update-settings` |  |
| `PUT` | `/projects/{projectKey}/webapps/{webappId}` | yes | WebappsResource.updateSettings | `dss webapp update-settings` |  |
| `POST` | `/projects/{projectKey}/webapps/{webappId}/actions/trust{?trustForEverybody}` | no |  |  |  |
| `PUT` | `/projects/{projectKey}/webapps/{webappId}/backend/actions/restart` | yes | WebappsResource.restartBackendAndWait, WebappsResource.startOrRestartBackend | `dss webapp restart-backend` |  |
| `PUT` | `/projects/{projectKey}/webapps/{webappId}/backend/actions/stop` | yes | WebappsResource.stopBackend | `dss webapp stop-backend` |  |
| `GET` | `/projects/{projectKey}/webapps/{webappId}/backend/state` | yes | WebappsResource.getBackendState | `dss webapp backend-state` |  |

### Notebooks (8/8)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/jupyter-notebooks/{?active}` | yes | NotebooksResource.listJupyter, NotebooksResource.unloadJupyterAll | `dss notebook list-jupyter`, `dss notebook unload-jupyter` |  |
| `GET` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}` | yes | NotebooksResource.getJupyter | `dss notebook clear-jupyter-outputs`, `dss notebook delete-jupyter`, `dss notebook get-jupyter`, `dss notebook save-jupyter` |  |
| `PUT` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}` | yes | NotebooksResource.saveJupyter |  |  |
| `POST` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}` | yes | NotebooksResource.createJupyter |  |  |
| `DELETE` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}` | yes | NotebooksResource.deleteJupyter | `dss notebook delete-jupyter` |  |
| `DELETE` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}/outputs` | yes | NotebooksResource.clearJupyterOutputs | `dss notebook clear-jupyter-outputs` |  |
| `GET` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}/sessions` | yes | NotebooksResource.listJupyterSessions | `dss notebook sessions-jupyter`, `dss notebook unload-jupyter` |  |
| `DELETE` | `/projects/{projectKey}/jupyter-notebooks/{notebookName}/sessions/{sessionId}` | yes | NotebooksResource.unloadJupyter | `dss notebook unload-jupyter` |  |

### Macros (6/6)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/runnables` | yes | MacrosResource.list | `dss macro list` |  |
| `GET` | `/projects/{projectKey}/runnables/{runnableType}` | yes | MacrosResource.definition | `dss macro get` |  |
| `POST` | `/projects/{projectKey}/runnables/{runnableType}{?wait}` | yes | MacrosResource.run | `dss macro run` |  |
| `POST` | `/projects/{projectKey}/runnables/{runnableType}/abort/{run}` | yes | MacrosResource.abort | `dss macro abort` |  |
| `GET` | `/projects/{projectKey}/runnables/{runnableType}/state/{run}` | yes | MacrosResource.state | `dss macro state` |  |
| `GET` | `/projects/{projectKey}/runnables/{runnableType}/result/{run}` | yes | MacrosResource.result | `dss macro result` |  |

### Long tasks (2/3)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/futures/{?allUsers}{?withScenarios}` | no |  |  | Futures listing (allUsers/withScenarios) not wrapped; SDK addresses futures by jobId. |
| `GET` | `/futures/{jobId}{?peek}` | yes | FuturesResource.get, FuturesResource.peek, FuturesResource.state, FuturesResource.wait | `dss app create-instance`, `dss app delete-instance`, `dss future get`, `dss future peek`, `dss future wait` |  |
| `DELETE` | `/futures/{jobId}` | yes | FuturesResource.abort | `dss future abort` |  |

### Meanings (4/4)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/meanings/` | yes | MeaningsResource.list | `dss meaning list` |  |
| `POST` | `/meanings` | yes | MeaningsResource.create | `dss meaning create` |  |
| `GET` | `/meanings/{meaningId}` | yes | MeaningsResource.get | `dss meaning delete`, `dss meaning get` |  |
| `PUT` | `/meanings/{meaningId}` | yes | MeaningsResource.update | `dss meaning update` |  |

### Plugins (33/33)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/plugins/` | yes | PluginsResource.list | `dss plugin list` |  |
| `POST` | `/plugins/actions/installFromZip` | yes | PluginsResource.installFromZip | `dss plugin install-from-zip` |  |
| `POST` | `/plugins/actions/installFromStore` | yes | PluginsResource.installFromStore | `dss plugin install-from-store` |  |
| `POST` | `/plugins/actions/installFromGit` | yes | PluginsResource.installFromGit |  |  |
| `GET` | `/plugins/{pluginId}/download` | yes | PluginsResource.download | `dss plugin download` |  |
| `POST` | `/plugins/{pluginId}/actions/updateFromZip` | yes | PluginsResource.updateFromZip | `dss plugin update-from-zip` |  |
| `POST` | `/plugins/{pluginId}/actions/updateFromStore` | yes | PluginsResource.updateFromStore | `dss plugin update-from-store` |  |
| `POST` | `/plugins/actions/updateFromGit` | yes | PluginsResource.updateFromGit | `dss plugin update-from-git` |  |
| `GET` | `/plugins/{pluginId}/settings{?projectKey}` | yes | PluginsResource.getSettings | `dss plugin settings-get` |  |
| `POST` | `/plugins/{pluginId}/settings{?projectKey}` | yes | PluginsResource.setSettings | `dss plugin settings-set` |  |
| `POST` | `/plugins/{pluginId}/code-env/actions/create` | yes | PluginsResource.createCodeEnv | `dss plugin code-env-create` |  |
| `POST` | `/plugins/{pluginId}/code-env/actions/update` | yes | PluginsResource.updateCodeEnv | `dss plugin code-env-update` |  |
| `POST` | `/plugins/{pluginId}/actions/moveToDev` | yes | PluginsResource.moveToDev | `dss plugin move-to-dev` |  |
| `GET` | `/plugins/{pluginId}/actions/listUsages{?projectKey}` | yes | PluginsResource.listUsages | `dss plugin usages` |  |
| `POST` | `/plugins/{pluginId}/actions/delete` | yes | PluginsResource.delete | `dss plugin delete` |  |
| `POST` | `/plugins/actions/createDev` | yes | PluginsResource.createDev | `dss plugin create-dev` |  |
| `GET` | `/plugins/{pluginId}/gitRemote` | yes | PluginsResource.getGitRemote | `dss plugin get-git-remote` |  |
| `POST` | `/plugins/{pluginId}/gitRemote` | yes | PluginsResource.setGitRemote | `dss plugin set-git-remote` |  |
| `DELETE` | `/plugins/{pluginId}/gitRemote` | yes | PluginsResource.deleteGitRemote | `dss plugin delete-git-remote` |  |
| `POST` | `/plugins/{pluginId}/gitBranches` | yes | PluginsResource.listGitBranches | `dss plugin git-branches` |  |
| `POST` | `/plugins/{pluginId}/actions/push` | yes | PluginsResource.push | `dss plugin push` |  |
| `POST` | `/plugins/{pluginId}/actions/pullRebase` | yes | PluginsResource.pull | `dss plugin pull` |  |
| `POST` | `/plugins/{pluginId}/actions/fetch` | yes | PluginsResource.fetch | `dss plugin fetch` |  |
| `POST` | `/plugins/{pluginId}/actions/resetToLocalHeadState` | yes | PluginsResource.resetToLocalHeadState | `dss plugin reset-local` |  |
| `POST` | `/plugins/{pluginId}/actions/resetToRemoteHeadState` | yes | PluginsResource.resetToRemoteHeadState | `dss plugin reset-remote` |  |
| `GET` | `/plugins/{pluginId}/contents/` | yes | PluginsResource.listContents | `dss plugin contents-list` |  |
| `GET` | `/plugins/{pluginId}/contents/{path}` | yes | PluginsResource.downloadFile, PluginsResource.getFile, PluginsResource.getFileBytes | `dss plugin contents-get` |  |
| `GET` | `/plugins/{pluginId}/details/{path}` | yes | PluginsResource.getFileDetails | `dss plugin details` |  |
| `POST` | `/plugins/{pluginId}/contents/{path}` | yes | PluginsResource.putFile | `dss plugin contents-put` |  |
| `DELETE` | `/plugins/{pluginId}/contents/{path}` | yes | PluginsResource.deleteFile | `dss plugin contents-delete` |  |
| `POST` | `/plugins/{pluginId}/folders/{path}` | yes | PluginsResource.addFolder | `dss plugin folder-add` |  |
| `POST` | `/plugins/{pluginId}/contents-actions/rename` | yes | PluginsResource.rename | `dss plugin rename` |  |
| `POST` | `/plugins/{pluginId}/contents-actions/move` | yes | PluginsResource.move | `dss plugin move` |  |

### Libraries (7/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/libraries/contents/` | yes | ProjectLibraryResource.addFolder, ProjectLibraryResource.hasLibraryItem, ProjectLibraryResource.listContents | `dss project-library create-file`, `dss project-library create-folder`, `dss project-library list` |  |
| `GET` | `/projects/{projectKey}/libraries/contents/{path}` | yes | ProjectLibraryResource.addFile, ProjectLibraryResource.getFile | `dss project-library create-file`, `dss project-library get` |  |
| `POST` | `/projects/{projectKey}/libraries/contents/{path}` | yes | ProjectLibraryResource.addFile, ProjectLibraryResource.addFolder, ProjectLibraryResource.addOrUpdateFile | `dss project-library create-file`, `dss project-library create-folder`, `dss project-library put` |  |
| `DELETE` | `/projects/{projectKey}/libraries/contents/{path}` | yes | ProjectLibraryResource.deleteFile | `dss project-library delete` |  |
| `POST` | `/projects/{projectKey}/libraries/folders/{path}` | yes | ProjectLibraryResource.addFolder | `dss project-library create-folder` |  |
| `POST` | `/projects/{projectKey}/libraries/contents-actions/rename` | yes | ProjectLibraryResource.rename | `dss project-library rename` |  |
| `POST` | `/projects/{projectKey}/libraries/contents-actions/move` | yes | ProjectLibraryResource.move | `dss project-library move` |  |

### API Services (6/6)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/apiservices/` | yes | ApiServicesResource.list | `dss api-service list` |  |
| `GET` | `/projects/{projectKey}/apiservices/{serviceId}/packages/` | yes | ApiServicesResource.listPackages | `dss api-service list-packages` |  |
| `GET` | `/projects/{projectKey}/apiservices/{serviceId}/packages/{packageId}/archive` | yes | ApiServicesResource.downloadPackageArchive | `dss api-service download-package` |  |
| `POST` | `/projects/{projectKey}/apiservices/{serviceId}/packages/{packageId}` | yes | ApiServicesResource.createPackage | `dss api-service create-package` |  |
| `DELETE` | `/projects/{projectKey}/apiservices/{serviceId}/packages/{packageId}` | yes | ApiServicesResource.deletePackage | `dss api-service delete-package` |  |
| `POST` | `/projects/{projectKey}/apiservices/{serviceId}/packages/{packageId}/publish{?publishedServiceId}` | yes | ApiServicesResource.publishPackage | `dss api-service publish-package` |  |

### Bundles, Design-side (5/6)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/bundles/exported` | yes | BundlesResource.listExported | `dss bundle list-exported` |  |
| `GET` | `/projects/{projectKey}/bundles/exported/{bundleId}` | no |  |  | Single exported-bundle details not wrapped; the SDK lists exported bundles and downloads their /archive streams. |
| `GET` | `/projects/{projectKey}/bundles/exported/{bundleId}/archive` | yes | BundlesResource.downloadExportedArchive | `dss bundle download-exported` |  |
| `PUT` | `/projects/{projectKey}/bundles/exported/` | yes | BundlesResource.exportBundle | `dss bundle export` | Path-shape drift: official "Create a new bundle" is PUT /projects/{projectKey}/bundles/exported/ with bundleId in the body; the SDK PUTs /bundles/exported/{bundleId} (id in path) and also forwards releaseNotes/evaluateProjectStandardsChecks query params. |
| `DELETE` | `/projects/{projectKey}/bundles/exported/` | yes | BundlesResource.deleteExported | `dss bundle delete-exported` | Path-shape drift: official "Delete an exported bundle" documents DELETE /bundles/exported/ with bundleId in the body; the SDK DELETEs /bundles/exported/{bundleId}. |
| `POST` | `/projects/{projectKey}/bundles/{bundleId}/publish{?publishedProjectKey}` | yes | BundlesResource.publish | `dss bundle publish` |  |

### Bundles, Automation-side (5/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/bundles/imported` | yes | BundlesResource.listImported | `dss bundle list-imported` |  |
| `POST` | `/projects/{projectKey}/bundles/imported/actions/importFromArchive{?archivePath}` | yes | BundlesResource.importFromArchive | `dss bundle import-from-archive` |  |
| `POST` | `/projects/{projectKey}/bundles/imported/{bundleId}/actions/preload` | yes | BundlesResource.preload | `dss bundle preload` |  |
| `POST` | `/projects/{projectKey}/bundles/imported/{bundleId}/actions/activate` | yes | BundlesResource.activate | `dss bundle activate` |  |
| `POST` | `/projectsFromBundle{?projectFolderId}` | no |  |  | Project-from-bundle creation (Automation node) not wrapped. |
| `POST` | `/projectsFromBundle/fromArchive{?archivePath,projectFolderId}` | no |  |  | Project-from-archive-bundle creation (Automation node) not wrapped. |
| `POST` | `/projects/{projectKey}/bundles/imported/actions/importFromStream` | yes | BundlesResource.importFromStream | `dss bundle import-from-stream` |  |

### Project Deployer (12/24)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/project-deployer/deployments` | yes | ProjectDeployerResource.listDeployments | `dss project-deployer list-deployments` |  |
| `GET` | `/project-deployer/deployments/{deploymentId}` | yes | ProjectDeployerResource.getDeployment | `dss project-deployer get-deployment` |  |
| `POST` | `/project-deployer/deployments` | yes | ProjectDeployerResource.createDeployment | `dss project-deployer create-deployment` |  |
| `DELETE` | `/project-deployer/deployments/{deploymentId}` | yes | BundlesResource.deleteDeployment, ProjectDeployerResource.deleteDeployment | `dss project-deployer delete-deployment` |  |
| `GET` | `/project-deployer/deployments/{deploymentId}/settings` | no |  |  |  |
| `PUT` | `/project-deployer/deployments/{deploymentId}/settings` | yes | BundlesResource.saveDeploymentSettings, ProjectDeployerResource.saveDeploymentSettings | `dss project-deployer save-deployment-settings` |  |
| `GET` | `/project-deployer/deployments/{deploymentId}/status` | yes | ProjectDeployerResource.getDeploymentStatus | `dss project-deployer deployment-status` |  |
| `GET` | `/project-deployer/deployments/{deploymentId}/governance-status` | no |  |  |  |
| `POST` | `/project-deployer/deployments/{deploymentId}/actions/update` | yes | ProjectDeployerResource.startUpdate | `dss project-deployer deploy` |  |
| `GET` | `/project-deployer/projects` | yes | ProjectDeployerResource.listProjects | `dss project-deployer list-projects` |  |
| `GET` | `/project-deployer/projects/{projectKey}` | yes | ProjectDeployerResource.getProjectStatus | `dss project-deployer project-status` |  |
| `POST` | `/project-deployer/projects` | yes | ProjectDeployerResource.createProject | `dss project-deployer create-project` |  |
| `DELETE` | `/project-deployer/projects/{projectKey}` | no |  |  |  |
| `GET` | `/project-deployer/projects/settings` | no |  |  |  |
| `PUT` | `/project-deployer/projects/{projectKey}/settings` | no |  |  |  |
| `POST` | `/project-deployer/projects/bundles{?projectKey}{?filePart}` | yes | BundlesResource.uploadBundle, ProjectDeployerResource.uploadBundle | `dss project-deployer upload-bundle` |  |
| `DELETE` | `/project-deployer/projects/{projectKey}/bundles/{bundleId}` | no |  |  |  |
| `GET` | `/project-deployer/infras/stages` | no |  |  |  |
| `GET` | `/project-deployer/infras` | yes | ProjectDeployerResource.listInfras | `dss project-deployer list-infras` |  |
| `GET` | `/project-deployer/infras/{infraId}` | no |  |  |  |
| `POST` | `/project-deployer/projects/infras` | no |  |  |  |
| `GET` | `/project-deployer/infras/{infraId}/settings` | no |  |  |  |
| `PUT` | `/project-deployer/infras/{infraId}/settings` | no |  |  |  |
| `DELETE` | `/project-deployer/infras/{infraId}` | no |  |  |  |

### Wiki (4/5)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/wiki/` | yes | WikiResource.list, WikiResource.settings | `dss wiki create`, `dss wiki list`, `dss wiki settings` |  |
| `PUT` | `/projects/{projectKey}/wiki/` | no |  |  |  |
| `POST` | `/projects/{projectKey}/wiki/` | yes | WikiResource.create | `dss wiki create` |  |
| `GET` | `/projects/{projectKey}/wiki/{articleId}` | yes | WikiResource.delete, WikiResource.get, WikiResource.update | `dss wiki delete`, `dss wiki get`, `dss wiki update` |  |
| `PUT` | `/projects/{projectKey}/wiki/{articleId}` | yes | WikiResource.update | `dss wiki update` |  |

### Discussions (4/5)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/discussions/{objectType}/{objectId}/` | yes | DiscussionsResource.list | `dss discussion list` |  |
| `POST` | `/projects/{projectKey}/discussions/{objectType}/{objectId}/` | yes | DiscussionsResource.create | `dss discussion create` |  |
| `GET` | `/projects/{projectKey}/discussions/{objectType}/{objectId}/{discussionId}` | yes | DiscussionsResource.get | `dss discussion get` |  |
| `PUT` | `/projects/{projectKey}/discussions/{objectType}/{objectId}/{discussionId}` | no |  |  |  |
| `POST` | `/projects/{projectKey}/discussions/{objectType}/{objectId}/{discussionId}/replies/` | yes | DiscussionsResource.reply | `dss discussion reply` |  |

### Dashboards (6/6)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/dashboards/` | yes | DashboardsResource.list | `dss dashboard create`, `dss dashboard list` |  |
| `POST` | `/projects/{projectKey}/dashboards/` | yes | DashboardsResource.create | `dss dashboard create` |  |
| `GET` | `/projects/{projectKey}/dashboards/{dashboardId}` | yes | DashboardsResource.get, DashboardsResource.update | `dss dashboard delete`, `dss dashboard get`, `dss dashboard update` |  |
| `PUT` | `/projects/{projectKey}/dashboards/{dashboardId}` | yes | DashboardsResource.update | `dss dashboard update` |  |
| `DELETE` | `/projects/{projectKey}/dashboards/{dashboardId}` | yes | DashboardsResource.delete | `dss dashboard delete` |  |
| `POST` | `/projects/{projectKey}/dashboards/{dashboardId}/action/export` | yes | DashboardsResource.export | `dss dashboard export` |  |

### Insights (5/5)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/projects/{projectKey}/insights/` | yes | InsightsResource.list | `dss insight create`, `dss insight list` |  |
| `POST` | `/projects/{projectKey}/insights/` | yes | InsightsResource.create | `dss insight create` |  |
| `GET` | `/projects/{projectKey}/insights/{insightId}` | yes | DashboardsResource.validateReferences, InsightsResource.get, InsightsResource.update | `dss insight delete`, `dss insight get`, `dss insight update` |  |
| `POST` | `/projects/{projectKey}/insights/{insightId}` | yes | InsightsResource.update | `dss insight update` |  |
| `DELETE` | `/projects/{projectKey}/insights/{insightId}` | yes | InsightsResource.delete | `dss insight delete` |  |

### SQL queries (3/3)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `POST` | `/sql/queries` | yes | SqlResource.query, SqlResource.startQuery | `dss sql query` |  |
| `GET` | `/sql/queries/{queryId}/stream{?format}{?formatParams}` | yes | SqlResource.streamResults |  |  |
| `GET` | `/sql/queries/{queryId}/finish-streaming` | yes | SqlResource.finishStreaming |  |  |

### Connections (7/7)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/admin/connections` | yes | ConnectionsResource.adminList |  |  |
| `POST` | `/admin/connections` | yes | ConnectionsResource.adminCreate | `dss connection create` |  |
| `GET` | `/admin/connections/{connectionName}` | yes | ConnectionsResource.adminGet | `dss connection delete`, `dss connection get`, `dss connection update` | Config read stays on the admin path per docs. TEST-ROUTE DRIFT (documented): the official REST page has no connection-test route (the GET /admin/connections/{connectionName} entries are ambiguous); the official Python client (dataikuapi/dss/connection.py, DSSConnection.test, master) uses GET /public/api/connections/{name}/test — connections-admin uses the Python-verified /test route for testing, and the admin path for config read. |
| `PUT` | `/admin/connections/{connectionName}` | yes | ConnectionsResource.adminUpdate | `dss connection update` |  |
| `DELETE` | `/admin/connections/{connectionName}` | yes | ConnectionsResource.adminDelete | `dss connection delete` |  |
| `GET` | `/admin/connections/{connectionName}` | yes | ConnectionsResource.adminGet | `dss connection delete`, `dss connection get`, `dss connection update` | Config read stays on the admin path per docs. TEST-ROUTE DRIFT (documented): the official REST page has no connection-test route (the GET /admin/connections/{connectionName} entries are ambiguous); the official Python client (dataikuapi/dss/connection.py, DSSConnection.test, master) uses GET /public/api/connections/{name}/test — connections-admin uses the Python-verified /test route for testing, and the admin path for config read. | Docs duplication: documented twice on the official page (anchors -get and -get-1); both entries kept.
| `GET` | `/connections/get-names/{?type}` | yes | ConnectionsResource.infer, ConnectionsResource.list | `dss connection infer`, `dss connection list` |  |

### Security (21/21)

| Method | Official URI | Status | SDK method(s) | CLI actions | Notes |
|---|---|---|---|---|---|
| `GET` | `/admin/users/{?connected}` | yes | UsersResource.list | `dss user list` |  |
| `POST` | `/admin/users` | yes | UsersResource.create | `dss user create` |  |
| `GET` | `/admin/users/{login}` | yes | UsersResource.get | `dss user delete`, `dss user get`, `dss user update` |  |
| `PUT` | `/admin/users/{login}` | yes | UsersResource.update | `dss user update` |  |
| `DELETE` | `/admin/users/{login}` | yes | UsersResource.delete | `dss user delete` |  |
| `POST` | `/admin/users/{login}/actions/resync` | yes | UsersResource.resync | `dss user resync` |  |
| `POST` | `/admin/users/actions/resync-multi` | yes | UsersResource.resyncMulti | `dss user resync-multi` |  |
| `GET` | `/admin/users/actions/external-users` | yes | UsersResource.externalUsers | `dss user external-users` |  |
| `GET` | `/admin/users/actions/external-groups` | yes | UsersResource.externalGroups | `dss user external-groups` |  |
| `POST` | `/admin/users/actions/provision` | yes | UsersResource.provision | `dss user provision` |  |
| `GET` | `/admin/users-activity` | yes | UsersResource.activityAll | `dss user activity` |  |
| `GET` | `/admin/users/{login}/activity` | yes | UsersResource.activity | `dss user activity-get` |  |
| `GET` | `/admin/groups` | yes | GroupsResource.list | `dss group list` |  |
| `POST` | `/admin/groups` | yes | GroupsResource.create | `dss group create` |  |
| `GET` | `/admin/groups/{groupname}` | yes | GroupsResource.get | `dss group delete`, `dss group get`, `dss group update` |  |
| `PUT` | `/admin/groups/{groupname}` | yes | GroupsResource.update | `dss group update` |  |
| `DELETE` | `/admin/groups/{groupname}` | yes | GroupsResource.delete | `dss group delete` |  |
| `GET` | `/admin/code-envs` | yes | CodeEnvsResource.list | `dss code-env create`, `dss code-env list` |  |
| `POST` | `/admin/code-envs/{envLang}/{envName}` | yes | CodeEnvsResource.create | `dss code-env create` |  |
| `GET` | `/admin/code-envs/{envLang}/{envName}` | yes | CodeEnvsResource.get, CodeEnvsResource.getDefinition, CodeEnvsResource.setDefinition, CodeEnvsResource.setPackages | `dss code-env delete`, `dss code-env get`, `dss code-env get-definition`, `dss code-env set-definition`, `dss code-env set-packages` |  |
| `PUT` | `/admin/code-envs/{envLang}/{envName}` | yes | CodeEnvsResource.setDefinition | `dss code-env set-definition` |  |

## Known documentation drift (confirmed)

1. **Saved model version deletion.** Official REST page documents a mutating `GET /projects/{projectKey}/savedmodels/{savedModelId}/delete-versions{?versions}{?removeIntermediate}`. The official Python client (`dataikuapi/dss/savedmodel.py`, `DSSSavedModel.delete_versions`, master) instead POSTs `/projects/{pk}/savedmodels/{smId}/actions/delete-versions` with JSON body `{versions, removeIntermediate}` (Python-verified 2026-09-09). This SDK follows the Python-verified POST route and pins `retryMaxAttempts: 1` (destructive, no auto-retry).
2. **Connection test.** The REST page has no unambiguous connection-test route; the official Python client (`dataikuapi/dss/connection.py`, `DSSConnection.test`, master) uses `GET /public/api/connections/{name}/test`. The SDK/CLI use the Python-verified `/test` route for testing and the documented admin path (`/admin/connections/{connectionName}`) for config read.
3. **Bundle create/delete path shape.** Docs document `PUT`/`DELETE /projects/{projectKey}/bundles/exported/` with `bundleId` in the request body; the SDK carries the id in the path (`/bundles/exported/{bundleId}`). Functionally equivalent; drift noted in the matrix rows.
4. **Dataset column lineage route.** Docs render a merged project-scoped route `GET /projects/{pk}/datasets/column-lineage{?columnName}{?maxDatasetCount}`; the SDK (matching the official Python client) calls the dataset-scoped `GET /projects/{pk}/datasets/{datasetName}/column-lineage?columnName=...&maxDatasetCount=...`.
5. **Dataset metrics partition.** Docs fold `{?partition}` into the URI; the SDK hard-codes the literal `NP` (no-partition) segment for last-metrics.
6. **Project delete query.** Docs document `?dropData`; the SDK sends `clearManagedDatasets`/`clearOutputManagedFolders`/`clearJobAndScenarioLogs`/`wait` (recorded as drift).
7. **Project list encoding.** The list endpoint is published as `GET /projects/{tags}` with the tags selector optional; the SDK GETs `/projects/` and filters client-side.

## Deliberately out of scope

- **MLflow / experiment-tracking extension** (`/api/2.0/mlflow/extension/*`, 6 rows): separate product surface.
- **Deep model-analysis family** (scoring jars, PMML, subpopulation/partial-dependency/residual analyses, model-doc generation, models-snippets): analysis screens driven by the DSS UI. Core lifecycle plus the Python-parity version ops (snippet, user-meta, external-ml metadata/evaluate, MLflow version import, create-external) are wrapped; the remaining lab-side rows are marked honestly.
- **Project Deployer deep settings** (deployment settings GET, governance status, infra settings/delete, lifecycle stages, published-bundle delete): deployments/projects/infras lifecycle is wrapped; deep settings rows are not.

---

*Generated 2026-09-09 from the official DSS 15 REST docs snapshot plus this repository working tree; CLI counts validated against the actual programmatic catalogue (`buildCommandRegistry()` = 55 resources / 474 actions; API-facing subset 47/466). This document makes no live-instance compatibility claims. JSON `meta` records the exact source of every number.*
