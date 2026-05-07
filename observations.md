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
