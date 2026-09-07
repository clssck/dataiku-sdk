# Changelog

## Unreleased

- Reject empty explicit live case selections rather than accidentally running the full suite.
- Invalidate live-lab readiness before teardown and retain cleanup failure reports when a project cannot be deleted.

- Validate live case selections before provisioning, preserve authentication/transport preflight errors, include live files in regular quality gates, and share CSV settings with statically required recipe row expectations.
- Add a separate, self-provisioning `test:live` runner with persistent setup/run/clean, focused cases, optional capability profiles, guarded ownership journals, deterministic fixtures and explicit registry coverage reports.

- Normalize explicit boolean flag values and aliases before dispatch; share plan/dry-run resolution across direct, batch, and local commands.
- Bound stalled streaming body reads without imposing a total-duration limit on healthy archive transfers; use backpressure-aware file streaming and publish archive/folder outputs only after completion, preserving existing files on failure.
- Report unreadable JSON/text input files as structured usage errors rather than internal failures.
- Report the actual home target for global skill installation.
- Split the Dataiku skill into a short entrypoint and seven on-demand references; install and hash the complete bundle, detect incomplete reference trees, and preserve unrelated destination files.
