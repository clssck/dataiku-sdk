# Mutation and batch safety

## Before writes

- For `sideEffect:"write"`, first run exact argv with `--plan`: local derived operation, no credentials or DSS calls. Check `destructive`, `idempotency`, `async`, `unsafeOutputs`, `exitCodes` before execution.
- `--plan` explains; `--dry-run` simulates, only when that action advertises `dryRun:true`. Never add unsupported flags.
- For creates/uploads advertising it, pass `--record-cleanup cleanup.jsonl`. `dss cleanup --file cleanup.jsonl` previews reverse-order steps without mutation; inspect before adding `--apply`.
- JSON: follow `inputContract`, `requiredFlags`, `requiredOneOf`. Prefer advertised `--data-file PATH` / `--stdin` over inline `--data`: preserve JSON across shells; keep large/sensitive payloads out of argv.
- Authenticated actions expose `--request-timeout MS`, `--retries N` (idempotent GET only). Long-running actions expose `--timeout MS`, `--poll-interval MS`, log limits. Use only advertised flags.
- Before live mutation tests, discover compatible resources with `dss fixtures`; never guess project objects.

## Check results

Success: small JSON ack on stdout, e.g. `{"updated":"NAME","resource":"recipe"}`, exit 0. Failure: stdout error envelope, nonzero exit. Exit code is authoritative.

Prefer `dss batch` for portable multi-step writes: JSON array of argv arrays, fail-fast, one envelope with per-step `ok`/`result`/`error`, nonzero if any step fails. For separate processes, check each exit and stop on failure; chaining differs across POSIX, PowerShell 5.1/7, and Command Prompt.

Never pipe mutations into fixed-string printers or stderr-merging helpers: their exit can mask failure. Branch on exits or stdout JSON acks, not hardcoded labels.

## Workflows

```text
dss version
dss project list
dss recipe get-payload compute_orders --output code.py --project-key MYPROJ
dss recipe diff compute_orders --file code.py --project-key MYPROJ
dss recipe set-payload compute_orders --file code.py --project-key MYPROJ
dss job build-and-wait orders --include-logs --project-key MYPROJ
dss sql query --connection analytics --sql "select 1" --project-key MYPROJ
dss batch --data-file steps.json
```

Fake DSS fixtures: `/public/api/projects/` returns an array, e.g. `[{"projectKey":"MYPROJ","name":"My Project"}]`. `/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true` returns `{"recipe":{"name":"<NAME>","type":"python"},"payload":"..."}` for recipe payload commands.
