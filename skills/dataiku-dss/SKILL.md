---
name: dataiku-dss
description: >-
  Agent-only JSON CLI for Dataiku DSS. Use to inspect or mutate DSS projects,
  datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables,
  code envs, and connections. Discover scoped command metadata with
  dss commands run --fields RESOURCE.ACTION.
license: LicenseRef-Dataiku-SDK-Limited-Use-1.0
compatibility: >-
  Requires the dss CLI, network access to a Dataiku DSS instance, configured
  DATAIKU_URL and DATAIKU_API_KEY credentials, and Bun >= 1.4.0.
---

# Dataiku DSS agent CLI

Use `dss` when an agent needs to inspect or change Dataiku DSS resources: projects, datasets, recipes, jobs, scenarios, folders, notebooks, SQL, variables, code envs, and connections.
If the installed `dss` binary is unavailable but the checkout is the workspace, prefer `bun --no-env-file src/cli.ts ...` or `bun --no-env-file ./bin/dss.js ...`; from another working directory, pass the checkout's absolute `bin/dss.js` path to Bun.
`--no-env-file` disables Bun's automatic preloading only; the CLI still applies its documented `.env` handling unless `DATAIKU_DISABLE_ENV=1` is set.

## Contract

- Command results write exactly one compact JSON value to stdout; void success is `{ok:true}`. `doctor`, `batch`, and `cleanup` failure reports are their direct result objects on stdout — use the exit code and result fields.
- Dispatch/runtime failures write one compact structured error object on stdout (`type:"error"`, `ok:false`, `error`, `code`, `category`, `exitCode`) with a nonzero exit code; stderr carries JSONL diagnostics only. Warnings and `--verbose` HTTP traces are JSONL stderr events (`type:"warning"` / `type:"trace"`), flushed before success and failure output, never prose.
- No prompts, help screens, tables, banners, or prose output are part of the contract. Exit codes: 0 success, 1 usage/configuration error, 2 DSS or internal error, 3 transient/retryable DSS error, 4 a failed long-running result or synchronous assertion.
- Recipe payload commands write the payload as a JSON string on stdout; with `--output PATH` the exact bytes go to a file and stdout carries the JSON string equal to `PATH`.
- `--fields a,b,c` projects those fields from object or array-of-objects results; dotted paths (`a.b.c`) drill into nested objects, missing fields become `null`; strings and scalars pass through unchanged.

## Discover commands

```text
dss commands run
dss commands run --fields dataset
dss commands run --fields dataset.create
dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
dss agent contract --fields commands.actions
```

`dss commands run` prints the compact resource/action summary — every resource keyed to its action names, about 1k tokens — and never dumps registry entries to stdout. Bootstrap with the scoped `agent contract` call above: those six fields cover protocol/schema compatibility, stream semantics, preferred discovery commands, planning rules, and compatibility guarantees in ~250 tokens; request `schemas` or `commands` only when needed.

Before choosing command syntax, look the action up in the registry: `--fields RESOURCE.ACTION` returns one complete entry, `--fields RESOURCE` every action of one resource, and appended `.FIELD` paths only the metadata you need. Prefer the four-field projection `usage,description,flags,examples` by default — the smallest self-sufficient starting point for an invocation. Each registry entry is the canonical schema for flags, positional arguments, side effects, auth requirements, output shape, idempotency, dry-run support, structured examples, payload schemas, unsafe outputs, cleanup hints, and exit codes.

`--fields` takes a comma-separated list, so request several actions in one call. Each key is echoed exactly as requested (`--fields dataset.create` returns `{"dataset.create": {...}}`); an empty `--fields` is a usage error, never a silent full dump. The full registry (~220k tokens) is exported only via `--output PATH`: `dss commands run --output PATH` writes the registry, or the selected `--fields` subset, as compact JSON to `PATH`, and stdout carries `{"path":"PATH"}` — never the registry itself. An unknown resource or action exits with code 1 and a compact JSON error object on stdout containing the valid options.
Credential lookup order is flags first, then `DATAIKU_*` environment variables, then saved credentials.
Set `DATAIKU_DISABLE_ENV=1` to ignore both `.env` files and `DATAIKU_*` variables.
With `.env` loading enabled, the CLI reads `.env` from the command's current working directory first, then the CLI root; the invocation directory wins on conflicts, so put test-specific `.env` files where you invoke `dss`.
For disposable agent tests, set `DSS_CONFIG_DIR` to a temporary directory so saved credentials never touch the real profile.

## Planning, safety, and generic inputs

- Before any registry entry with `sideEffect:"write"`, run the exact argv with `--plan` first. Planning is local: it returns the derived operation without credentials or DSS calls. Check `destructive`, `idempotency`, `async`, `unsafeOutputs`, and `exitCodes` before executing.
- Use `--dry-run` only when that action's registry entry has `dryRun:true`. `--plan` explains the operation; `--dry-run` exercises the simulation path. Never add an unsupported flag.
- When a create/upload action advertises `--record-cleanup`, pass `--record-cleanup cleanup.jsonl`. `dss cleanup --file cleanup.jsonl` previews recorded steps in reverse order without mutating DSS; add `--apply` only after checking it.
- For JSON payload actions, follow `inputContract`, `requiredFlags`, and `requiredOneOf`. Prefer `--data-file PATH` or `--stdin` when advertised over inline `--data`; this preserves exact JSON across shells and keeps large or sensitive payloads out of argv.
- Authenticated actions advertise `--request-timeout MS` and `--retries N`; `--retries` applies to idempotent GET requests. Long-running actions advertise `--timeout MS`, `--poll-interval MS`, and log limits; use only the flags in that registry entry.
- Before live mutation tests, use `dss fixtures` to discover compatible test resources instead of guessing project objects.

## Authentication

Prefer environment variables for ephemeral agent runs. Use the syntax for the active shell:

POSIX shell:

```sh
export DATAIKU_URL=https://dss.example.com
export DATAIKU_API_KEY=your-api-key
export DATAIKU_PROJECT_KEY=MYPROJ
```

PowerShell:

```powershell
$env:DATAIKU_URL = "https://dss.example.com"
$env:DATAIKU_API_KEY = "your-api-key"
$env:DATAIKU_PROJECT_KEY = "MYPROJ"
```

Windows Command Prompt:

```bat
set "DATAIKU_URL=https://dss.example.com"
set "DATAIKU_API_KEY=your-api-key"
set "DATAIKU_PROJECT_KEY=MYPROJ"
```

To persist credentials for later invocations:

```bash
dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ
```

The command saves credentials and returns `{"saved":true,"path":"..."}`. `DSS_CONFIG_DIR` wins when set; otherwise credentials use `XDG_CONFIG_HOME/dataiku/credentials.json`, the `dataiku/credentials.json` directory under `APPDATA` on Windows, or `~/.config/dataiku/credentials.json`.
`auth login` validates by listing accessible projects before saving, so the key must be allowed to call DSS project-list APIs.

TLS: `--insecure` disables certificate verification; `--ca-cert PATH` adds a PEM CA bundle. Environment equivalents: `NODE_TLS_REJECT_UNAUTHORIZED`, `NODE_EXTRA_CA_CERTS`.

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

## Application release safety

Treat app release as explicit validate, compare, version, successor, verify, and permission gates.
The public app-manifest `version` field is raw metadata: writing it is NOT a publish transaction.
New instances inherit the template's raw `version`; existing instances are never upgraded in
place — create an additive successor (the old instance is preserved), verify it, retire it
separately. Never infer private publish/recreate/rename, recipient-sharing, or UI-click operations
the public DSS API lacks.

```text
dss app validate-manifest --project-key APP_TEMPLATE
dss app compare-manifest APP_ID --project-key RELEASE_INSTANCE
dss app manifest-version --project-key APP_TEMPLATE
dss app successor-preflight APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions
dss app set-manifest-version --manifest-version 1.4.0 --expect-hash PREFLIGHT_TEMPLATE_MANIFEST_HASH --project-key APP_TEMPLATE
# targetProjectKey in instance.json must be confirmed absent
dss app create-instance APP_ID --data-file instance.json --wait --record-cleanup cleanup.jsonl
dss app create-successor-instance APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance APP_ID --project-key RELEASE_INSTANCE_V2 --expect-version 1.4.0
dss cleanup --file cleanup.jsonl
dss app permissions-snapshot --project-key RELEASE_INSTANCE --output permissions.json
dss app permissions-diff --project-key RELEASE_INSTANCE --file permissions.json
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json --dry-run
```

Run `successor-preflight` before changing the template version. It validates the template,
predecessor, target, and optional ACL snapshot, performs no mutation, and returns the template
manifest hash for the next `set-manifest-version --expect-hash` guard.

A masked `403` absent from both visible lists is rejected as
`target_absence_unverifiable` / `permission_or_environment`; lists prove collisions, not absence.
DSS exposes no permission-independent public key-availability endpoint or guaranteed
non-overwriting duplicate-key rejection, so no force or server-atomic bypass is supported; use
global project visibility. A definitive rejection writes no cleanup entry. An ambiguous POST
without a returned future ID or verified incarnation is `INDETERMINATE` and also produces no cleanup
entry. Future-addressable cleanup waits for target identity and `creationTag`; the predecessor is
never targeted. Ledgers bind the canonical DSS URL, project key, and concrete project incarnation,
rejecting legacy, mixed-server, mismatched-server, or unbound app cleanup entries. Static plans
expose `preflightExecuted:false` and `preflightWillRunDuringApply:true`.

`app set-manifest-version` reports
`concurrencyControl:"client-side-non-atomic-stale-read-check"`. `--expect-hash` is a stale-read
guard; the PUT is unconditional. Never treat the hash as a serializing lock; ambiguous writes
report `outcome:"indeterminate"`.

The API key authenticates public REST only. `app verify-instance` reports
`status:"API_VERIFIED_UI_PENDING"`; its external SSO gate requires exercising the affected tiles,
forms, and actions. Permission snapshots bind identity and permissions to the server/project
incarnation and reject mismatches.

## Flow maps and visual organization

```text
dss project map --render mermaid --project-key MYPROJ
dss flow-zone plan --project-key MYPROJ > flow-zones.json
dss flow-zone organize --file flow-zones.json --dry-run --project-key MYPROJ
dss flow-zone organize --file flow-zones.json --project-key MYPROJ
```

`project map` returns zones, SCC-safe layers, weak components, diagnostics, and a full-flow
fingerprint; rendering stays in `rendering.content`. DSS exposes zone positions only, never node
pixel coordinates.

`flow-zone plan` output feeds `flow-zone organize`; inspect the dry-run. Organize checks topology
before and after writes and skips unchanged moves. Audit recipe payloads separately; layout
commands never fetch or analyze code.

## Project Git

UAT=`create-branch --duplicate-project`; pull=rebase; checkout=switch. Remotes: DSS Git rules and
SSH/server credentials. `--password-env`; `future-wait`.

## Coding and libraries

Use `project-library` for internal `lib/`; `project-git list-libraries` for external Git repos.
Safe edit: `get-bytes` → `diff` → `put --expect-sha256 HASH`; create with
`--if-not-exists`. Paths reject traversal.

Run Python with `code run --file`; inspect/diff/backup recipes before `recipe run --dry-run`.
Notebook saves report hashes and accept `--expect-hash`; output clearing uses DSS DELETE, while
`unload-jupyter --all` composes session deletes. Inspect code-env definitions/logs before updates;
guard webapp/API settings with `--expect-hash`. `exact:false` means live state was not guessed:
use command `--dry-run`. Public APIs expose no plugin authoring, notebook execute/checkpoints, or
webapp/API delete; use project Git, code/recipes, or the DSS UI.

## Confirming mutations

Mutations print a small JSON ack to stdout and exit 0 on success (e.g. `{"updated":"NAME","resource":"recipe"}`); on failure the error envelope appears on stdout with a non-zero exit. The exit code is the source of truth.

- For portable multi-step writes, prefer `dss batch` (payload: a JSON array of argv arrays): fail-fast, one envelope with per-step `ok`/`result`/`error`, non-zero exit if any step fails.
- With separate processes, inspect each exit code and stop before the next step; shell-chaining syntax differs across POSIX shells, PowerShell 5.1/7, and Command Prompt.
- Never pipe a mutation into a command that prints a fixed string or merges stderr: the helper's exit code can mask a failed mutation as success.
- Branch on the exit code or the JSON ack on stdout — never a hardcoded label.

## Platform & debugging notes

- Pass code and SQL via `--file`/`--sql-file`, not inline: shells (especially PowerShell) mangle quotes, `$`, and newlines in multi-line snippets.
- On a non-UTF-8 console (Windows cp1252), don't print non-ASCII results; write them to a UTF-8 file or use `--output PATH`.
- Build failures: `dss job log <id> --errors-only` surfaces error/traceback lines; `--output PATH` saves the full log. Logs are one long line with JVM noise; the `Error in Python process: At line <N>` marker names your payload's source line.
- Schema changes aren't automatic for code recipes: after changing a python/SQL/prepare recipe's output columns run `dss dataset refresh-schema` (or rebuild) before downstream reads, and `dss dataset validate-build` to catch file-backed misconfig before a build. Exception: `dss recipe create --type sync` copies the input schema onto a schemaless output at create time (`syncOutputSchemaPropagated`), so a fresh sync output builds populated without a manual refresh.
- `dss dataset download` is capped (default 100k rows) and returns `{ path, rows, truncated, limit }`: check `truncated` and raise `--limit N` for more — a sample, not a guaranteed export. Without `--output` the file lands in the current working directory (a `dataset_download_default_location` warning names that path); pass `--output PATH` to control it. For very large tables, aggregate in SQL or read inside a recipe instead.
- `dss dashboard create`/`update` validate every `INSIGHT` tile before mutation: `insightId` and `targetInsightId` must agree and resolve, and any `datasetSmartName` must resolve. Missing or stale references exit with `validation_failed` before POST/PUT. Cross-project `PROJECT.DATASET` references require the API key to read that dataset project; a `403` blocks the save rather than accept an unverifiable dashboard.
- `dss api-service list-packages SERVICE` first verifies the parent service through its settings. `not_found` means the service is missing (verify the service ID and project key); an empty array means it exists but has no deployable packages. Never reinterpret `not_found` as an empty list or retry the lower-level route.
- `dss notebook clear-jupyter-outputs NAME --dry-run` plans the official output DELETE; applying it uses that endpoint rather than rewriting notebook cells.
- `dss sql query ... --dataset PROJECT.NAME` first queries through the dataset. If DSS rejects that connection as neither SQL nor HDFS, the CLI reads dataset metadata and retries with `params.connection` (schema or catalog as the database when available). A readable dataset with no usable connection exits with `validation_failed` and advises `--connection`; dataset metadata `404` and `403` errors propagate as `not_found` and `permission_denied`.
- `dss sql query ... --start-retries N` opts the query-start POST into transient retries with exponential backoff. A lost response can make DSS execute the SQL more than once, so use it only for repetition-safe SQL; ordinary `--retries N` remains GET-only.
- `dss sql query` without `--preview` returns full rows on stdout for compatibility; `--preview N` bounds stdout for exploratory reads; `--output PATH` writes full rows to a file instead.

## Error envelope

Dispatch/runtime failures carry one compact error object on stdout:

```json
{
  "type": "error",
  "ok": false,
  "error": "Missing API key.",
  "code": "usage_error",
  "category": "usage",
  "exitCode": 1,
  "resource": "dataset",
  "action": "list"
}
```

`doctor`, `batch`, and `cleanup` report command-level failures as their direct result object on stdout with a nonzero exit code; key off the exit code before interpreting the result.
Recover from `code`, `category`, `exitCode`, `retryable`, `status`, and `details`; never scrape message text when a structured field exists.
Treat `details.body` as sanitized metadata only: at most `requestId`/`request_id`/`errorId`/`elapsedMs` plus locally trusted target/timing fields, not the DSS response body. `details.statusText` is canonical text derived from the numeric status, not a remote reason phrase. Base recovery on top-level `code`, `category`, `status`, `retryable`, `requestId`, `hint`, and `details.dssCategory`; never parse assumed server-body fields.
