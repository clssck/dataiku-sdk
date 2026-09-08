# Troubleshooting and data access

## Platform, builds, data

- Code/SQL: use `--file`/`--sql-file`; shells, especially PowerShell, mangle quotes, `$`, and multiline text. Non-UTF-8 consoles (e.g. Windows cp1252): write non-ASCII results to UTF-8 files or `--output PATH`, not the console.
- Build errors: `dss job log <id> --errors-only` selects errors/tracebacks; `--output PATH` saves full logs. Logs are one JVM-noisy line; `Error in Python process: At line <N>` identifies the payload source line.
- After python/SQL/prepare output-column changes, run `dss dataset refresh-schema` or rebuild before downstream reads. Run `dss dataset validate-build` before builds to catch file-backed misconfiguration. Exception: `dss recipe create --type sync` copies input schema to schemaless output at creation (`syncOutputSchemaPropagated`); fresh sync outputs build populated without manual refresh.
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

`details.body` is sanitized metadata, **not the DSS response body**: at most `requestId`/`request_id`/`errorId`/`elapsedMs` plus locally trusted target/timing. `details.statusText` derives from numeric status, not remote reason phrases. Use top-level `code`, `category`, `status`, `retryable`, `requestId`, `hint`, and `details.dssCategory`; never assume server-body fields.
