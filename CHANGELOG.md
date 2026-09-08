# Changelog

## Unreleased

- Preserve request IDs and trusted target/timing metadata when enriching SQL failures.
- Treat SQLite missing-column and missing-table errors as nonretryable validation failures, including when SQL start retries are explicitly enabled; preserve ambiguous mutation outcomes.
- Decode bounded HTTP text incrementally to reduce large-response allocations while retaining byte limits, read deadlines, and UTF-8 truncation behavior.
- Load CLI command dependencies lazily and build only the selected resource registry during validation, preserving full discovery contracts.
- Publish SQL JSON exports atomically after query completion, retaining existing permission bits and using the process umask for new files; preserve existing destinations on failure and avoid iterable-string file writes.

- Add a separate, self-provisioning `test:live` runner with persistent setup/run/clean, upfront case validation, optional capability profiles, guarded ownership and local outputs, failure-preserving cleanup journals, deterministic fixtures, explicit registry coverage reports, and regular type/lint gates.

- Normalize explicit boolean flag values and aliases before dispatch; share plan/dry-run resolution across direct, batch, and local commands.
- Bound stalled streaming body reads without imposing a total-duration limit on healthy archive transfers; use backpressure-aware file streaming and publish archive/folder outputs only after completion, preserving existing files on failure.
- Report unreadable JSON/text input files as structured usage errors rather than internal failures.
- Report the actual home target for global skill installation.
- Split the Dataiku skill into a short entrypoint and seven on-demand references; install and hash the complete bundle, detect incomplete reference trees, and preserve unrelated destination files.
