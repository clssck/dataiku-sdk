# Mutation and batch safety

## Planning, safety, and generic inputs

- Before any registry entry with `sideEffect:"write"`, run the exact argv with `--plan` first. Planning is local: it returns the derived operation without credentials or DSS calls. Check `destructive`, `idempotency`, `async`, `unsafeOutputs`, and `exitCodes` before executing.
- Use `--dry-run` only when that action's registry entry has `dryRun:true`. `--plan` explains the operation; `--dry-run` exercises the simulation path. Never add an unsupported flag.
- When a create/upload action advertises `--record-cleanup`, pass `--record-cleanup cleanup.jsonl`. `dss cleanup --file cleanup.jsonl` previews recorded steps in reverse order without mutating DSS; add `--apply` only after checking it.
- For JSON payload actions, follow `inputContract`, `requiredFlags`, and `requiredOneOf`. Prefer `--data-file PATH` or `--stdin` when advertised over inline `--data`; this preserves exact JSON across shells and keeps large or sensitive payloads out of argv.
- Authenticated actions advertise `--request-timeout MS` and `--retries N`; `--retries` applies to idempotent GET requests. Long-running actions advertise `--timeout MS`, `--poll-interval MS`, and log limits; use only the flags in that registry entry.
- Before live mutation tests, use `dss fixtures` to discover compatible test resources instead of guessing project objects.

## Confirming mutations

Mutations print a small JSON ack to stdout and exit 0 on success (e.g. `{"updated":"NAME","resource":"recipe"}`); on failure the error envelope appears on stdout with a non-zero exit. The exit code is the source of truth.

- For portable multi-step writes, prefer `dss batch` (payload: a JSON array of argv arrays): fail-fast, one envelope with per-step `ok`/`result`/`error`, non-zero exit if any step fails.
- With separate processes, inspect each exit code and stop before the next step; shell-chaining syntax differs across POSIX shells, PowerShell 5.1/7, and Command Prompt.
- Never pipe a mutation into a command that prints a fixed string or merges stderr: the helper's exit code can mask a failed mutation as success.
- Branch on the exit code or the JSON ack on stdout — never a hardcoded label.

## Common workflows

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
For fake-DSS smoke tests, return project lists as JSON arrays such as `[{"projectKey":"MYPROJ","name":"My Project"}]` from `/public/api/projects/`; recipe payload commands read `/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true` expecting `{"recipe":{"name":"<NAME>","type":"python"},"payload":"..."}`.
