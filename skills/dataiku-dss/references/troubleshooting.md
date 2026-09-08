# Troubleshooting and data access

## Platform & debugging notes

- Pass code and SQL via `--file`/`--sql-file`, not inline: shells (especially PowerShell) mangle quotes, `$`, and newlines in multi-line snippets.
- On a non-UTF-8 console (Windows cp1252), don't print non-ASCII results; write them to a UTF-8 file or use `--output PATH`.
- Build failures: `dss job log <id> --errors-only` surfaces error/traceback lines; `--output PATH` saves the full log. Logs are one long line with JVM noise; the `Error in Python process: At line <N>` marker names your payload's source line.
- Schema changes aren't automatic for code recipes: after changing a python/SQL/prepare recipe's output columns run `dss dataset refresh-schema` (or rebuild) before downstream reads, and `dss dataset validate-build` to catch file-backed misconfig before a build. Exception: `dss recipe create --type sync` copies the input schema onto a schemaless output at create time (`syncOutputSchemaPropagated`), so a fresh sync output builds populated without a manual refresh.
- `dss dataset download` defaults to 100k rows and returns `{ path, rows, truncated, limit }`; raise `--limit` when truncated. Set `--output` or read the `dataset_download_default_location` path warning. CSV is spreadsheet-safe by default; `--raw-data` preserves formula prefixes. For large tables, aggregate in SQL or use a recipe.
- `dss dashboard create`/`update` validate every `INSIGHT` tile before mutation: `insightId` and `targetInsightId` must agree and resolve, and any `datasetSmartName` must resolve. Missing or stale references exit with `validation_failed` before POST/PUT. Cross-project `PROJECT.DATASET` references require the API key to read that dataset project; a `403` blocks the save rather than accept an unverifiable dashboard.
- `dss api-service list-packages SERVICE` first verifies the parent service through its settings. `not_found` means the service is missing (verify the service ID and project key); an empty array means it exists but has no deployable packages. Never reinterpret `not_found` as an empty list or retry the lower-level route.
- `dss notebook clear-jupyter-outputs NAME --dry-run` plans the official output DELETE; applying it uses that endpoint rather than rewriting notebook cells.
- `dss sql query ... --dataset PROJECT.NAME` first queries through the dataset. If DSS rejects that connection as neither SQL nor HDFS, the CLI reads dataset metadata and retries with `params.connection` (schema or catalog as the database when available). A readable dataset with no usable connection exits with `validation_failed` and advises `--connection`; dataset metadata `404` and `403` errors propagate as `not_found` and `permission_denied`.
- `dss sql query ... --start-retries N` opts the query-start POST into transient retries with exponential backoff. A lost response can make DSS execute the SQL more than once, so use it only for repetition-safe SQL; ordinary `--retries N` remains GET-only.
- `dss sql query` without `--preview` returns full rows on stdout for compatibility; `--preview N` bounds stdout for exploratory reads; `--output PATH` writes full rows to a file instead.
- SQL `--output` and `--output-file` publish complete JSON atomically after query completion. Failures preserve the existing destination; replacements retain its permission bits, while new files use `0666` filtered by the process umask. Results are still collected in memory before export.

## Error envelope

Dispatch/runtime failures carry one compact error object on stdout:

```json
{
  "type": "error",
  "ok": false,
  "error": "Missing API key.",
  "code": "missing_required_flag",
  "category": "usage",
  "exitCode": 1,
  "resource": "dataset",
  "action": "list"
}
```

`doctor`, `batch`, and `cleanup` report command-level failures as their direct result object on stdout with a nonzero exit code; key off the exit code before interpreting the result.
Recover from `code`, `category`, `exitCode`, `retryable`, `status`, and `details`; never scrape message text when a structured field exists.
Treat `details.body` as sanitized metadata only: at most `requestId`/`request_id`/`errorId`/`elapsedMs` plus locally trusted target/timing fields, not the DSS response body. `details.statusText` is canonical text derived from the numeric status, not a remote reason phrase. Base recovery on top-level `code`, `category`, `status`, `retryable`, `requestId`, `hint`, and `details.dssCategory`; never parse assumed server-body fields.
