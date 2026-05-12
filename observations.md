# Observations

## Agent CRUD Phase 1 Verification (2026-05-07)

```json
{
  "phase": "Phase 1 - agent-readiness consistency",
  "status": "implementation_verified_with_environment_limited_live_smoke",
  "changes": [
    "Added --dry-run coverage for write commands across dataset, recipe, folder, variable, scenario, job, notebook, wiki, dashboard, insight, data-quality, flow-zone, code-env, future, and install-skill surfaces.",
    "Added --if-not-exists for create-shaped commands and --if-exists for delete-shaped commands where the current CLI surface supports lifecycle checks.",
    "Extended commands --json entries with outputShape, inputContract, destructive, producesLocalFile, mutatesDss, async, idempotency, and cleanupHint metadata.",
    "Removed emoji glyphs from auth and install-skill CLI output."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 157 pass, 0 fail",
    "unit": "bun test -> 366 pass, 60 skip, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "integrationMutating": "bun run test:integration:mutating -> 5 pass, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 50 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 55 pass, 0 fail",
    "rigorousReport": "RUN_DATAIKU_INTEGRATION_REPORT=1 with mutating rigorous suite -> 55 pass, 0 fail, emitted expected feature/opportunity findings"
  },
  "liveSmoke": {
    "dataset_update_dry_run": true,
    "recipe_update_dry_run": true,
    "variable_set_dry_run": true,
    "scenario_create_update_run_delete_cleanup_verified": true,
    "job_build_dry_run": true,
    "folder_upload_dry_run": false,
    "notebook_save_clear_delete": false,
    "notes": [
      "Direct live smoke found no managed folder in folder list, so folder upload dry-run was not exercised there.",
      "Direct live smoke found no Jupyter notebook in notebook list-jupyter; attempting to create one with save-jupyter returned DSS 404 for a non-existent notebook, so notebook clear-output live smoke was not exercised.",
      "Rigorous report also recorded folder file workflow as skipped because DATAIKU_TEST_FOLDER_ID is not configured."
    ]
  },
  "cleanup": {
    "scenario": "temporary scenario was deleted with --if-exists and scenario list verified the id absent",
    "temporaryFiles": "temporary /tmp live-smoke script removed"
  }
}
```

## Agent Ergonomics Phase 1.5.A Verification (2026-05-07)

```json
{
  "phase": "1.5.A doctor capability report",
  "status": "verified",
  "changes": [
    "Added dss doctor --capabilities permission probes with yes/no/unknown statuses and non-escalating per-probe details.",
    "Added doctor fixture discovery for default datasets, recipes, scenarios, flow zones, managed folders, and Jupyter notebooks.",
    "Added doctor environment integration flag reporting while preserving the default dss doctor output shape."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 159 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass after formatting edited files",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 50 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 55 pass, 0 fail",
    "liveSmoke": "bun run src/cli.ts doctor --capabilities -> permissions included yes values; fixtures included defaultDataset=cardholder_info, defaultRecipe=compute_transactions_joined, defaultScenario=BUILDDASHBOARD"
  },
  "cleanup": {
    "required": false,
    "notes": "No disposable DSS artifacts were created for this sub-phase."
  }
}
```

## Agent Ergonomics Phase 1.5.B Verification (2026-05-07)

```json
{
  "phase": "1.5.B richer commands registry",
  "status": "verified",
  "changes": [
    "Extended commands --json entries with dryRun, requiredFlags, optionalFlags, payloadSchema, examplePayload, cleanupCommand, and exitCodes.",
    "Derived cleanupCommand from registered delete usages for create-shaped commands with cleanup support.",
    "Extended registry surface and rigorous integration matrix assertions for the richer registry shape."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 159 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 50 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 55 pass, 0 fail",
    "liveSmoke": "bun run src/cli.ts commands --json -> dataset.create, scenario.create, and flow-zone.create cleanupCommand present; job.build exitCodes.longRunningFailure=4"
  },
  "cleanup": {
    "required": false,
    "notes": "No disposable DSS artifacts were created for this sub-phase."
  }
}
```

## Agent Ergonomics Phase 1.5.C Verification (2026-05-07)

```json
{
  "phase": "1.5.C mutation planning",
  "status": "verified",
  "changes": [
    "Added --plan with --explain alias for mutating command planning before credential resolution or DSS calls.",
    "Added structured plan output with method, endpoint, payload, identifiers, wait, idempotency, async, and failure exit code metadata.",
    "Updated commands --json to advertise the canonical plan flag for write commands."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 161 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 50 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 55 pass, 0 fail",
    "liveSmoke": "bun run src/cli.ts scenario run BUILDDASHBOARD --plan and bun run src/cli.ts job build cardholder_info --plan -> emitted POST endpoints and payloads without executing the operations"
  },
  "cleanup": {
    "required": false,
    "notes": "Plan mode does not mutate DSS; no disposable artifacts were created."
  }
}
```

## Agent Ergonomics Phase 1.5.D Verification (2026-05-07)

```json
{
  "phase": "1.5.D cleanup ledger",
  "status": "verified",
  "changes": [
    "Added append-only JSONL cleanup ledger utilities.",
    "Added --record-cleanup for supported create/upload workflows and a top-level dss cleanup command.",
    "Implemented reverse-order in-process cleanup application with dry-run default and continue-on-error support."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 164 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 50 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 55 pass, 0 fail",
    "liveSmoke": "Created disposable scenario with --record-cleanup, ran dss cleanup --apply, then scenario list verified the id absent"
  },
  "cleanup": {
    "required": true,
    "verified": true,
    "notes": "Temporary live-smoke scenario was removed by dss cleanup --apply and the temporary local ledger directory was removed."
  }
}
```

## Agent Ergonomics Phase 1.5.E Verification (2026-05-07)

```json
{
  "phase": "1.5.E report JSON errors",
  "status": "verified",
  "changes": [
    "Added --report-json and DSS_REPORT_JSON support for stable stderr error envelopes.",
    "Mapped usage, validation, permission, transient, and internal failures to stable machine-readable codes.",
    "Added report-json help output that returns the same registry entry exposed by dss commands."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 168 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass"
  },
  "cleanup": {
    "required": false,
    "verified": true,
    "notes": "No DSS mutations were performed for report-json verification."
  }
}
```

## Agent Ergonomics Phase 1.5.F Verification (2026-05-07)

```json
{
  "phase": "1.5.F fixture discovery",
  "status": "verified",
  "changes": [
    "Added dss fixtures --json with default safe type allowlist Filesystem,Inline and configurable --allow-types.",
    "Returned doctor fixture defaults, safeDataset, safeManagedFolder, safeJupyterNotebook, and unsafe rejection reasons.",
    "Updated rigorous integration coverage to discover fixtures once and use safeManagedFolder unless DATAIKU_TEST_FOLDER_ID overrides it.",
    "Stopped treating command-specific --timeout as the HTTP request timeout; --request-timeout remains the HTTP-level control."
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 171 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 51 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 56 pass, 0 fail",
    "liveSmoke": "dss fixtures --json returned safeDataset=transactions_joined for project TUT_PIVOT_TABLES"
  },
  "cleanup": {
    "required": false,
    "verified": true,
    "notes": "Fixture discovery is read-only; no DSS artifacts were created."
  }
}
```

## Agent Ergonomics Phase 1.5 Final Verification (2026-05-07)

```json
{
  "phase": "1.5 final",
  "status": "complete",
  "commits": [
    "a43ce69 feat(doctor): report capability probes",
    "ca59eb4 feat(cli): enrich command registry",
    "4b54c25 feat(cli): add mutation planning",
    "f8cc808 feat(cli): add cleanup ledger",
    "6ef2926 feat(cli): report JSON errors",
    "e43fc18 feat(cli): discover safe fixtures"
  ],
  "verification": {
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts -> 171 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 51 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 56 pass, 0 fail",
    "liveSmokes": {
      "doctorCapabilities": "dss doctor --capabilities returned populated fixtures and yes permissions for all probed capabilities",
      "registry": "dss commands --json returned cleanupCommand for dataset.create, scenario.create, flow-zone.create and longRunningFailure=4 for job.build",
      "planning": "dss scenario run BUILDDASHBOARD --plan and dss job build transactions_joined --plan returned POST endpoints without execution",
      "cleanupLedger": "Created OMP_FINAL_CLEANUP_1778161714404 with --record-cleanup, ran dss cleanup --apply, and scenario list verified it absent",
      "reportJson": "dss flow-zone list --wat yes --report-json returned code=unknown_flag",
      "fixtures": "dss fixtures --json returned projectKey=TUT_PIVOT_TABLES and safeDataset=transactions_joined"
    }
  },
  "cleanup": {
    "required": true,
    "verified": true,
    "notes": "The final disposable scenario was removed by dss cleanup --apply; no known DSS residue remains."
  }
}
```

## Managed Folder Lifecycle Follow-up (2026-05-07)

```json
{
  "scope": "managed folder update/delete",
  "status": "verified_with_permission_limited_live_smoke",
  "changes": [
    "Added FoldersResource.update and FoldersResource.delete for PUT/DELETE /projects/{projectKey}/managedfolders/{folderId}.",
    "Added dss folder update and dss folder delete with dry-run, plan, if-exists, report-json-compatible validation, and registry metadata.",
    "Enabled cleanup ledger entries for dss folder create now that dss folder delete exists.",
    "Updated CLI surface and SDK surface tests for the new managed-folder lifecycle methods."
  ],
  "verification": {
    "officialDocs": "Dataiku REST docs list PUT and DELETE /projects/{projectKey}/managedfolders/{folderId}; delete warns associated recipes are removed.",
    "focused": "bun test tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts tests/schemas.test.ts tests/folders-jobs.test.ts -> 185 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "livePlan": "folder delete/update --plan returned managedfolders DELETE/PUT endpoints without DSS mutation",
    "liveDryRun": "folder delete OMP_NONEXISTENT --dry-run --if-exists returned skipped/missing",
    "liveMutating": "attempted disposable folder create on filesystem_managed; DSS returned 403 Forbidden: may not create a managed folder on connection filesystem_managed"
  },
  "cleanup": {
    "required": false,
    "verified": true,
    "notes": "The live mutating folder create was rejected before artifact creation; no DSS folder cleanup was required."
  },
  "openRisks": [
    "Configured playground API key can list/infer filesystem_managed but cannot create managed folders on that connection, so live PUT/DELETE was not exercised against a real disposable folder."
  ]
}
```

## Connection Read-only Follow-up (2026-05-07)

```json
{
  "scope": "connection read-only ergonomics",
  "status": "verified",
  "changes": [
    "Added optional type filtering to ConnectionsResource.list and dss connection list --type TYPE using the non-admin /connections/get-names endpoint.",
    "Added --project-key support to dss connection infer so rich inference can target a specific project.",
    "Explicitly avoided admin connection CRUD/get/update/delete because Dataiku docs show admin-only endpoints can expose connection parameters and delete in-use connections without checks."
  ],
  "verification": {
    "officialDocs": "Dataiku REST docs list non-admin GET /connections/get-names/{?type}; admin /admin/connections get/update/delete are privileged and can expose params or delete in-use connections.",
    "focused": "bun test tests/connections-variables.test.ts tests/cli.test.ts tests/cli-surface.test.ts tests/sdk-surface.test.ts -> 120 pass, 0 fail",
    "typecheck": "bun run check -> pass",
    "format": "bun run format:check -> pass",
    "lint": "bun run lint -> pass with pre-existing no-underscore-dangle warnings in src/client.ts",
    "build": "bun run build -> pass",
    "liveSmoke": "dss connection list --type all returned dataiku-managed-storage, filesystem_folders, filesystem_managed; dss connection infer --mode rich --project-key TUT_PIVOT_TABLES returned filesystem_managed"
  },
  "cleanup": {
    "required": false,
    "verified": true,
    "notes": "Connection follow-up is read-only and performed no DSS mutations."
  }
}
```

## SDK Live Surface Audit (2026-05-07)

```json
{
  "scope": "SDK live method audit",
  "status": "completed_with_safe_skips",
  "safeBoundary": [
    "Project CRUD remains out of scope; project SDK was audited read-only only.",
    "Admin connection CRUD was not added or audited because Dataiku docs show it can expose connection params and delete in-use connections without checks.",
    "Existing live assets were not destructively mutated; disposable scenarios, flow zones, dashboards, wiki articles, data-quality rules, temp files, and attempted disposable datasets/folders were cleaned up or rejected before creation."
  ],
  "verification": {
    "integration": "bun run test:integration -> 4 pass, 1 skip, 0 fail",
    "rigorous": "bun run test:integration:rigorous -> 51 pass, 5 skip, 0 fail",
    "rigorousMutating": "bun run test:integration:rigorous:mutating -> 56 pass, 0 fail",
    "directSdkAudit": {
      "summary": {
        "pass": 73,
        "skip": 44,
        "blocked": 5,
        "fail": 0
      },
      "passedCategories": [
        "projects read-only",
        "connections read-only",
        "datasets read-only plus disposable create/update/delete",
        "recipes read-only",
        "scenarios disposable lifecycle",
        "flow-zone list/create/update/delete",
        "variables get",
        "data-quality read-only plus disposable rule lifecycle and compute/wait",
        "futures get/peek/state/wait via data-quality compute",
        "dashboards disposable lifecycle",
        "code-env read-only",
        "wiki disposable lifecycle"
      ],
      "fixtureLimited": {
        "method": "notebooks.saveSql",
        "observedResponse": "404 Not Found: Dataiku instance not found",
        "classification": "nonexistent SQL notebook fixture; not a confirmed SDK defect",
        "context": "Retest while DSS was confirmed running: doctor/project-get/listSql all succeeded, listSql returned [], and both saveSql to a disposable nonexistent id and getSql for a nonexistent id returned the same DSS 404 body. The message text is misleading here; the actionable constraint is that there was no existing SQL notebook fixture to overwrite safely."
      },
      "permissionBlocked": [
        "folders.create: 403 Forbidden on filesystem_managed",
        "folders.update/delete blocked because disposable managed-folder create is not permitted in this live env"
      ],
      "safetySkips": [
        "recipes create/update/setPayload/delete require a disposable recipe/output lifecycle",
        "jobs build/buildAndWait/abort would build or abort live jobs",
        "flowZones moveItems/moveItem would move existing flow objects",
        "variables.set would rewrite project variables",
        "futures.abort would abort a live future/job",
        "codeEnvs mutators are admin/global mutations",
        "Jupyter save/delete/clear/unload would mutate existing notebooks and no SDK create endpoint exists",
        "SQL query methods skipped because no SQL-compatible live connection was confirmed",
        "insight create/update/delete need a valid object-specific insight prototype"
      ]
    }
  },
  "cleanup": {
    "verified": true,
    "notes": "Audit temp script and local temp directories were removed; disposable DSS artifacts were cleaned up best-effort during each run. Folder create was rejected before creation."
  }
}
```

## Unproven SDK Function Coverage Progress (2026-05-11)

```json
{
  "scope": "unproven SDK live coverage plan execution",
  "status": "live_validated_safe_and_restore_gated_paths",
  "implemented": [
    {
      "area": "flow-zone movement",
      "change": "Rigorous mutating integration now creates a disposable dataset and moves only that dataset through SDK moveItem and CLI moveItems paths.",
      "methodsCoveredWhenLive": ["flowZones.moveItem", "flowZones.moveItems"],
      "safety": "No existing project flow objects are moved."
    },
    {
      "area": "job build coverage",
      "change": "Rigorous mutating integration now creates a disposable dataset and covers job build/wait/details/log/buildAndWait plus CLI dry-run planning.",
      "methodsCoveredWhenLive": ["jobs.build", "jobs.wait", "jobs.get", "jobs.log", "jobs.buildAndWait"],
      "remaining": "jobs.abort remains dry-run-only until a long-running disposable job fixture exists."
    },
    {
      "area": "variables",
      "change": "Variable mutation gate now verifies CLI dry-run planning, SDK merge write, CLI readback, and full snapshot restore.",
      "methodsCoveredWhenGated": ["variables.set", "variables.get"],
      "gate": "RUN_DATAIKU_INTEGRATION_VARIABLES=1"
    },
    {
      "area": "SQL query coverage",
      "change": "Added gated live SELECT 1 coverage for sql.query, which exercises startQuery, streamResults, and finishStreaming internally.",
      "methodsCoveredWhenConfigured": ["sql.query", "sql.startQuery", "sql.streamResults", "sql.finishStreaming"],
      "gate": "RUN_DATAIKU_SQL_LIVE=1 plus DATAIKU_SQL_CONNECTION or DATAIKU_SQL_DATASET_FULL_NAME"
    },
    {
      "area": "notebook mutation coverage",
      "change": "Added explicit-fixture gated no-loss Jupyter and SQL notebook mutation coverage. Jupyter save/clear restore original notebook content; SQL notebook save/history/clear requires explicit SQL notebook id.",
      "methodsCoveredWhenConfigured": ["notebooks.saveJupyter", "notebooks.clearJupyterOutputs", "notebooks.saveSql", "notebooks.getSqlHistory", "notebooks.clearSqlHistory"],
      "remaining": "deleteJupyter and deleteSql remain unproven until disposable notebook create paths or explicit disposable fixtures exist."
    },
    {
      "area": "admin code-env coverage",
      "change": "Added RUN_DATAIKU_ADMIN_MUTATING gated disposable code-env lifecycle for create/get/getDefinition/setDefinition/setPackages/setJupyterSupport/updatePackages/delete.",
      "methodsCoveredWhenConfigured": ["codeEnvs.create", "codeEnvs.get", "codeEnvs.getDefinition", "codeEnvs.setDefinition", "codeEnvs.setPackages", "codeEnvs.setJupyterSupport", "codeEnvs.updatePackages", "codeEnvs.delete"],
      "gate": "RUN_DATAIKU_ADMIN_MUTATING=1"
    },
    {
      "area": "environment reporting",
      "change": "Doctor now reports RUN_DATAIKU_ADMIN_MUTATING and RUN_DATAIKU_SQL_LIVE flags. Integration harness now exposes admin-mutating and SQL-live describe gates.",
      "newFlags": ["RUN_DATAIKU_ADMIN_MUTATING", "RUN_DATAIKU_SQL_LIVE"]
    }
  ],
  "verification": {
    "typecheck": "bun run check passed",
    "format": "bun run format:check passed",
    "lint": "bun run lint passed with pre-existing no-underscore-dangle warnings in src/client.ts",
    "focusedTests": "bun test tests/cli.test.ts tests/integration-rigorous.test.ts tests/connections-variables.test.ts tests/folders-jobs.test.ts tests/flow-zones.test.ts -> 134 pass, 60 skip, 0 fail"
  },
  "liveDss": {
    "status": "available during live validation",
    "observed": "doctor --capabilities returned ok=true for TUT_PIVOT_TABLES with yes permissions for list/read/mutate project, create folder, run jobs, create scenario, save Jupyter, and mutate connection.",
    "impact": "Safe mutating, restore-safe variable, and read-only rigorous suites were live-proven against the playground."
  },
  "liveValidation": {
    "commands": [
      "bun run test:integration -> 4 pass, 1 skip, 0 fail",
      "bun run test:integration:rigorous -> 54 pass, 6 skip, 0 fail",
      "bun run test:integration:mutating -> 5 pass, 0 fail",
      "bun run test:integration:rigorous:mutating -> 60 pass, 0 fail after adding explicit per-test timeouts for long live data-quality/job tests",
      "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 bun test tests/integration-rigorous.test.ts -> 60 pass, 0 fail, 727 expect calls",
      "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_INTEGRATION_REPORT=1 bun test tests/integration-rigorous.test.ts -> 60 pass, 0 fail, 727 expect calls"
    ],
    "cleanup": "An intermediate fixture scan observed a disposable job dataset from a timed-out run; a later residue scan across datasets, data-quality rules, flow zones, wiki articles, dashboards, and insights found no sdk_cli_it/OMP artifacts. Dataset cleanup in the affected tests now fails visibly instead of swallowing delete failures."
  },
  "next": [
    "Run fixture-gated SQL/Jupyter/admin code-env coverage when explicit SQL/Jupyter notebook fixtures or RUN_DATAIKU_ADMIN_MUTATING are intentionally configured.",
    "Add a dedicated long-running disposable job fixture before live-testing jobs.abort."
  ]
}
```

## Unproven SDK Function Coverage Live Validation (2026-05-11)

```json
{
  "scope": "live validation after DSS playground became available",
  "doctor": {
    "command": "bun run src/cli.ts doctor --capabilities --report-json",
    "result": "passed",
    "connectivity": "ok",
    "projectKey": "TUT_PIVOT_TABLES",
    "permissions": {
      "canListProjects": "yes",
      "canReadProject": "yes",
      "canMutateProject": "yes",
      "canCreateFolder": "yes",
      "canRunJobs": "yes",
      "canCreateScenario": "yes",
      "canSaveJupyter": "yes",
      "canMutateConnection": "yes"
    },
    "fixtures": {
      "defaultDataset": "cardholder_info",
      "defaultRecipe": "compute_transactions_joined",
      "defaultScenario": "BUILDDASHBOARD",
      "defaultFlowZone": null,
      "defaultManagedFolder": null,
      "defaultJupyterNotebook": null
    }
  },
  "commands": [
    {
      "command": "bun run test:integration",
      "result": "4 pass, 1 skip, 0 fail"
    },
    {
      "command": "bun run test:integration:rigorous",
      "result": "54 pass, 6 skip, 0 fail"
    },
    {
      "command": "bun run test:integration:mutating",
      "result": "5 pass, 0 fail"
    },
    {
      "command": "bun run test:integration:rigorous:mutating",
      "result": "60 pass, 0 fail",
      "notes": "An earlier run hit live-operation timeouts in the data-quality/job area; targeted reruns and a full rerun passed cleanly."
    },
    {
      "command": "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 bun test tests/integration-rigorous.test.ts",
      "result": "60 pass, 0 fail",
      "expectCalls": 727
    },
    {
      "command": "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_INTEGRATION_REPORT=1 bun test tests/integration-rigorous.test.ts",
      "result": "60 pass, 0 fail",
      "expectCalls": 727,
      "skips": [
        "sql-live-fixture-not-configured",
        "jupyter-mutation-needs-explicit-fixture",
        "sql-notebook-mutation-needs-explicit-fixture",
        "code-env-mutation-admin-gated",
        "job-abort-needs-long-running-disposable-job",
        "folder-file-workflow-needs-test-folder"
      ]
    }
  ],
  "finalVerification": {
    "format": "bun run format:check passed",
    "typecheck": "bun run check passed",
    "lint": "bun run lint passed with pre-existing no-underscore-dangle warnings in src/client.ts",
    "focusedTests": "bun test tests/cli.test.ts tests/integration-rigorous.test.ts tests/connections-variables.test.ts tests/folders-jobs.test.ts tests/flow-zones.test.ts -> 134 pass, 60 skip, 0 fail",
    "residueScan": "SDK scan found no disposable sdk_cli_it/OMP datasets, data-quality rules, flow zones, wiki articles, dashboards, or insights."
  },
  "nowLiveProven": [
    "flowZones.moveItem",
    "flowZones.moveItems",
    "jobs.build",
    "jobs.wait",
    "jobs.get",
    "jobs.log",
    "jobs.buildAndWait",
    "variables.set",
    "variables.get",
    "insights.create",
    "insights.update",
    "insights.delete"
  ],
  "stillNotLiveProven": [
    {
      "area": "jobs.abort",
      "reason": "Only dry-run planning is covered; real abort still needs a long-running disposable job fixture."
    },
    {
      "area": "SQL query methods",
      "reason": "RUN_DATAIKU_SQL_LIVE plus DATAIKU_SQL_CONNECTION or DATAIKU_SQL_DATASET_FULL_NAME is not configured."
    },
    {
      "area": "Jupyter notebook mutations",
      "reason": "DATAIKU_TEST_JUPYTER_NOTEBOOK is not configured; delete/unload still need disposable notebook/session fixtures."
    },
    {
      "area": "SQL notebook mutations",
      "reason": "DATAIKU_TEST_SQL_NOTEBOOK_ID is not configured; deleteSql still needs a disposable create path or explicit disposable fixture."
    },
    {
      "area": "admin code-env mutations",
      "reason": "RUN_DATAIKU_ADMIN_MUTATING is not configured, so global/admin code-env lifecycle was not executed."
    }
  ],
  "cleanup": {
    "verifiedByTests": true,
    "notes": "An intermediate fixture scan observed a disposable job dataset from a timed-out run; the current residue scan found no sdk_cli_it/OMP datasets, data-quality rules, flow zones, wiki articles, dashboards, or insights. New dataset cleanup no longer swallows delete failures."
  }
}
```

## Remaining SDK Function Coverage Live Completion (2026-05-11)

```json
{
  "scope": "completion pass for previously gated/unproven SDK and CLI coverage",
  "status": "remaining_safe_sandbox_paths_live_validated",
  "supersedes": "The earlier 2026-05-11 live-validation stillNotLiveProven entries for jobs.abort, SQL query methods, Jupyter delete, SQL notebook delete, and admin code-env mutations are now resolved by this completion pass. Jupyter unload remains explicitly session-fixture-gated.",
  "implemented": [
    {
      "area": "jobs.abort",
      "change": "Rigorous mutating integration now creates a disposable long-running Python recipe, builds its disposable output, aborts one running job through SDK jobs.abort, aborts a second running job through CLI job abort, waits both to terminal non-success states, then deletes the recipe and output dataset.",
      "methodsCoveredWhenLive": ["jobs.abort"],
      "cliCoveredWhenLive": ["job abort"],
      "safety": "Only sdk_cli_it_* disposable recipe/output dataset jobs are aborted."
    },
    {
      "area": "SQL query methods",
      "change": "SQL live coverage can now create a disposable PostgreSQL connection under RUN_DATAIKU_ADMIN_MUTATING=1 plus DATAIKU_SQL_LIVE_CREATE_CONNECTION=1 when DATAIKU_SQL_CONNECTION/DATAIKU_SQL_DATASET_FULL_NAME are absent; the test runs SDK and CLI SELECT 1 probes and cleans up the temporary connection.",
      "methodsCoveredWhenLive": ["sql.query", "sql.startQuery", "sql.streamResults", "sql.finishStreaming"],
      "cliCoveredWhenLive": ["sql query"],
      "gate": "RUN_DATAIKU_SQL_LIVE=1 plus either DATAIKU_SQL_CONNECTION/DATAIKU_SQL_DATASET_FULL_NAME or disposable connection env"
    },
    {
      "area": "Jupyter notebook mutations",
      "change": "Mutating integration now creates disposable Jupyter notebooks through the documented public create endpoint, covers SDK saveJupyter/clearJupyterOutputs/listJupyterSessions/deleteJupyter, covers CLI save-jupyter/clear-jupyter-outputs/sessions-jupyter/delete-jupyter, and cleans up all notebooks.",
      "methodsCoveredWhenLive": ["notebooks.saveJupyter", "notebooks.clearJupyterOutputs", "notebooks.listJupyterSessions", "notebooks.deleteJupyter"],
      "cliCoveredWhenLive": ["notebook save-jupyter", "notebook clear-jupyter-outputs", "notebook sessions-jupyter", "notebook delete-jupyter"],
      "remaining": "notebooks.unloadJupyter still requires a disposable running notebook session; public POST session probing returned 405 Method Not Allowed."
    },
    {
      "area": "SQL notebook mutations",
      "change": "Mutating integration now creates disposable SQL notebooks through the documented public create endpoint, covers SDK saveSql/getSqlHistory/clearSqlHistory/deleteSql, covers CLI save-sql/history-sql/clear-sql-history/delete-sql, and cleans up all notebooks.",
      "methodsCoveredWhenLive": ["notebooks.saveSql", "notebooks.getSqlHistory", "notebooks.clearSqlHistory", "notebooks.deleteSql"],
      "cliCoveredWhenLive": ["notebook save-sql", "notebook history-sql", "notebook clear-sql-history", "notebook delete-sql"]
    },
    {
      "area": "admin code-env mutations",
      "change": "The existing RUN_DATAIKU_ADMIN_MUTATING-gated disposable code-env lifecycle was executed live and passed.",
      "methodsCoveredWhenLive": ["codeEnvs.create", "codeEnvs.get", "codeEnvs.getDefinition", "codeEnvs.setDefinition", "codeEnvs.setPackages", "codeEnvs.setJupyterSupport", "codeEnvs.listUsages", "codeEnvs.updatePackages", "codeEnvs.delete"]
    }
  ],
  "liveValidation": {
    "commands": [
      "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_SQL_LIVE=1 RUN_DATAIKU_ADMIN_MUTATING=1 DATAIKU_SQL_LIVE_CREATE_CONNECTION=1 DATAIKU_SQL_LIVE_*=[configured] bun test tests/integration-rigorous.test.ts -> 60 pass, 0 fail, 794 expect calls",
      "same command with RUN_DATAIKU_INTEGRATION_REPORT=1 -> 60 pass, 0 fail, 794 expect calls"
    ],
    "remainingReportFindings": [
      "jupyter-unload-needs-running-session",
      "folder-file-workflow-needs-test-folder",
      "low-severity feature-opportunity probes"
    ]
  },
  "residueScan": {
    "result": "clean",
    "checked": ["datasets", "recipes", "flowZones", "insights", "jupyter notebooks", "sql notebooks", "connections", "code envs", "dashboards", "wiki articles"],
    "pattern": "sdk_cli_it/OMP"
  },
  "stillNotLiveProven": [
    {
      "area": "notebooks.unloadJupyter",
      "reason": "No public API path creates a running disposable Jupyter session; disposable notebook session create probing returned 405 Method Not Allowed. Dry-run and session listing are covered."
    }
  ]
}
```

## Additional Project Live Validation (2026-05-12)

```json
{
  "scope": "cross-project validation after additional DSS playground projects became available",
  "status": "validated_with_project_specific_fixture_gaps_classified",
  "projects": [
    "DKU_EXAM_DEVELOPER",
    "DKU_TUT_CODE_NOTEBOOKS",
    "TUT_BATCH",
    "TUT_GOVERNANCE",
    "TUT_PIVOT_TABLES",
    "TUT_PYTHON_PREPARE",
    "TUT_R_MARKDOWN",
    "TUT_STATIC_INSIGHTS"
  ],
  "testHardening": [
    "Data-quality mutation coverage now creates a disposable filesystem dataset before adding/removing a disposable rule, instead of attaching a temporary rule to an existing project dataset.",
    "Managed-folder file workflow coverage now attempts disposable managed-folder creation and classifies the resource gap if creation is forbidden, instead of uploading into an existing folder fixture.",
    "Job abort coverage now classifies projects whose disposable long-running abort fixture reaches a terminal state before abort can be issued, while still proving abort on projects where the fixture remains running."
  ],
  "validation": {
    "fixtures": "dss fixtures --project-key <project> --allow-types Filesystem,Inline was run for all 8 accessible projects.",
    "capabilities": "dss doctor --capabilities --project-key <project> --report-json returned ok=true for all 8 accessible projects.",
    "readOnlyMatrix": "RUN_DATAIKU_INTEGRATION=1 bun test tests/integration-playground.test.ts tests/integration-rigorous.test.ts passed for all 8 projects; each run reported 58 pass, 7 skip, 0 fail.",
    "mutatingMatrix": "RUN_DATAIKU_INTEGRATION=1 RUN_DATAIKU_INTEGRATION_MUTATING=1 RUN_DATAIKU_INTEGRATION_VARIABLES=1 RUN_DATAIKU_INTEGRATION_REPORT=1 bun test tests/integration-rigorous.test.ts passed for all 8 projects; each run reported 60 pass, 0 fail.",
    "focusedLocal": "bun test tests/cli.test.ts tests/integration-rigorous.test.ts tests/connections-variables.test.ts tests/folders-jobs.test.ts tests/flow-zones.test.ts -> 134 pass, 60 skip, 0 fail",
    "format": "bun run format:check passed",
    "typecheck": "bun run check passed",
    "lint": "bun run lint passed with 16 pre-existing no-underscore-dangle warnings in src/client.ts and 0 errors",
    "diffCheck": "git diff --check passed"
  },
  "projectSpecificClassifications": {
    "jobAbortFixtureGap": [
      "TUT_BATCH",
      "TUT_PYTHON_PREPARE",
      "TUT_R_MARKDOWN",
      "TUT_STATIC_INSIGHTS"
    ],
    "jobAbortStillLiveProvenOn": [
      "DKU_EXAM_DEVELOPER",
      "DKU_TUT_CODE_NOTEBOOKS",
      "TUT_GOVERNANCE",
      "TUT_PIVOT_TABLES"
    ],
    "managedFolderDisposableCreate": "Skipped where DSS returned 403 for disposable folder creation on filesystem connections; existing folders were not mutated.",
    "sqlLive": "Not rerun in the per-project matrix; SQL live remains covered by the earlier explicit SQL/admin gated completion run.",
    "adminCodeEnv": "Not rerun in the per-project matrix; global code-env lifecycle remains guarded by RUN_DATAIKU_ADMIN_MUTATING=1 and was covered by the earlier completion run.",
    "notebooksUnloadJupyter": "Still requires an externally started disposable running Jupyter session."
  },
  "residueScan": {
    "result": "clean",
    "checked": ["datasets", "recipes", "flow zones", "insights", "Jupyter notebooks", "SQL notebooks", "dashboards", "wiki articles", "managed folders", "connections", "code envs"],
    "pattern": "sdk_cli_it/OMP"
  }
}
```
