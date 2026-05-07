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
