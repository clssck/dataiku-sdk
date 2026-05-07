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
