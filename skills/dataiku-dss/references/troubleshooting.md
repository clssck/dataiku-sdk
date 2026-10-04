# Troubleshooting and data access

## Platform, builds, data

- Code/SQL: use `--file`/`--sql-file`; shells, especially PowerShell, mangle quotes, `$`, and multiline text. Non-UTF-8 consoles (e.g. Windows cp1252): write non-ASCII results to UTF-8 files or `--output PATH`, not the console.
- Build errors: `dss job log <id> --errors-only` selects errors/tracebacks. For full logs, use `--output PATH` without `--errors-only`. Logs may arrive as one JVM-noisy line; `Error in Python process: At line <N>` identifies the payload source line.
- Schemas: code recipes set output schemas at run time. Visual/SQL query recipes: `recipe create` fills created or empty outputs (`outputSchemaUpdated`); `recipe set-payload` updates empty or DSS-derived ones, warns `recipe_output_schema_outdated` for hand-edited ones. Apply with `dss recipe update-schema NAME` (one recipe), `dss flow propagate-schema DATASET --wait` (every downstream recipe), or build `--auto-update-schema`. `built_dataset_has_no_columns`: the DONE build wrote nothing. Manual columns: `dss dataset refresh-schema NAME --data-file columns.json`. `dss dataset validate-build` catches file-backed misconfiguration.
- `Inline` (editable) datasets: the public API cannot write rows, so `dataset create` rejects them; use `UploadedFiles` + `dataset upload-file`, or a python recipe output. `code run` is a scenario step; some DSS versions refuse dataset writes there.
- `dss dataset download`: default 100k rows; result `{ path, rows, truncated, limit }`. If truncated, raise `--limit`. Specify `--output` or read the `dataset_download_default_location` path warning. CSV is spreadsheet-safe; `--raw-data` retains formula prefixes. For large tables, aggregate in SQL or use a recipe.
- `dss dashboard create`/`update`: before POST/PUT, every `INSIGHT` tile needs agreeing/resolvable `insightId` and `targetInsightId`; any `datasetSmartName` must resolve. Missing/stale references: `validation_failed`. Cross-project `PROJECT.DATASET` needs dataset-project read permission; `403` blocks saving an unverifiable dashboard.
- `dss api-service list-packages SERVICE` checks parent settings first. `not_found`: missing service; verify service ID/project key. Empty array: existing service, no deployable packages. Never convert `not_found` to an empty list or retry the lower-level route.
- `dss notebook clear-jupyter-outputs NAME --dry-run` plans the official output DELETE; apply uses it, never rewrites cells.

## SQL

- `dss sql query ... --dataset PROJECT.NAME` first queries via the dataset. Only when DSS rejects its connection as neither SQL nor HDFS does the CLI read metadata and retry `params.connection`, using schema/catalog as database if available. No usable connection on a readable dataset: `validation_failed`, use `--connection`. Metadata `404`/`403` propagate as `not_found`/`permission_denied`.
- `--start-retries N` enables transient query-start POST retries with exponential backoff. Lost responses can execute SQL twice: **repetition-safe SQL only**. Ordinary `--retries N` remains GET-only.
- No `--preview`: full stdout rows for compatibility. `--preview N`: bounded exploratory stdout. `--output PATH`: full rows to file.
- SQL `--output`/`--output-file`: atomic complete JSON after query completion; failures preserve the destination. Replacements retain permission bits; new files use `0666` filtered by umask. Results still accumulate in memory before export.

## Errors

Dispatch/runtime failure: one compact stdout error:
```json
{"type":"error","ok":false,"error":"Missing API key.","code":"missing_required_flag","category":"usage","exitCode":1,"resource":"dataset","action":"list"}
```

`doctor`/`batch`/`cleanup` command failures return direct stdout result objects; check nonzero exit before interpreting. Recover from structured `code`, `category`, `exitCode`, `retryable`, `status`, `details`, never message scraping.

DSS failures: `error` is `STATUS Text: DSS message` (credentials redacted, bounded); `hint` names the next check (resource, project). `details` appears only with content: `elapsedMs`/`target`, `dssErrorType`, `bodyTruncated`, and `retry` when a retry or timeout happened. Refused connections, unknown hosts, bad URLs, and untrusted certificates fail at once as `validation_failed` (exit 2); `transient` (exit 3) means automatic retries already ran.
