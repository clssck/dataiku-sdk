# dataiku-sdk

Agent-only TypeScript SDK and `dss` CLI for Dataiku DSS automation.

## Platform support

The published `dss` CLI supports Linux, macOS, and Windows under Bun >= 1.3.14 or Node.js >= 22.15.0. The release gate runs Bun 1.3.14 with Node 24 on all three operating systems, plus a lightweight minimum-Node 22.15 package smoke on Linux. The runtime dependency is pure JavaScript, so the same package also runs on runtime-supported x64 and ARM64 systems.

Run directly with Bun:

```text
bunx --bun dataiku-sdk version
```

Or install the npm binary:

```text
npm install --global dataiku-sdk
```

Bun is the primary development runtime and package manager. Examples below assume an installed `dss` binary. From a checkout, use `bun --no-env-file src/cli.ts ...` or the cross-runtime `bun --no-env-file ./bin/dss.js ...` launcher. Node users can invoke the same launcher as `node ./bin/dss.js ...`; if `dist/` is absent, it delegates to Bun. From another working directory, pass the checkout's absolute `bin/dss.js` path to Bun or Node.
`--no-env-file` disables Bun's automatic preloading only; the CLI still applies its documented `.env` handling unless `DATAIKU_DISABLE_ENV=1` is set.

## CLI contract

- Command results write exactly one compact JSON value to stdout; void success is `{ok:true}`. `doctor`, `batch`, and `cleanup` failure reports are their direct result objects on stdout with a nonzero exit code — use the exit code and result fields.
- Dispatch/runtime failures write one compact structured error object on stdout (`type:"error"`, `ok:false`, `error`, `code`, `category`, `exitCode`) with a nonzero exit code; stderr carries JSONL diagnostics only.
- Non-fatal diagnostics and `--verbose` traces are JSONL stderr events (`type:"warning"` / `type:"trace"`), flushed before both success and failure output; no prose trace lines are part of the contract.
- `--fields a,b,c` projects those fields from an object or array-of-objects result; dotted paths (`a.b.c`) drill into nested objects, and missing fields become `null`; string and scalar results pass through unchanged.
- No prompts, help screens, tables, banners, or prose output are part of the contract.
- Exit codes: `0` success, `1` usage/configuration error, `2` DSS/internal error, `3` transient DSS error, `4` a failed long-running result or synchronous assertion.
- `--retries N` controls idempotent GET retries only. `dss sql query --start-retries N` explicitly retries transient failures while starting a query; use it only when executing the SQL more than once is safe.
- The exit code is the success signal. For portable multi-step mutations, prefer `dss batch`; shell chaining and pipeline exit semantics differ across POSIX shells, Windows PowerShell, PowerShell 7, and Command Prompt.
- Recipe payload commands write the payload as a JSON string on stdout; use `--output PATH` to write the exact bytes to a file instead, with stdout carrying the JSON string equal to `PATH`.

Discover the agent contract and command surface with scoped calls:

```text
dss commands run
dss commands run --fields dataset
dss commands run --fields dataset.create
dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
dss agent contract --fields commands.actions
```

`dss commands run` prints the compact resource/action summary — every resource keyed to its action names, about 1k tokens — and never dumps registry entries to stdout. Bootstrap contract discovery with the scoped `agent contract` call above: `protocol,agentContractVersion,cli,stdio,planning,compatibility` cover protocol/schema compatibility, stdout/stderr stream semantics, preferred discovery commands, planning rules, and compatibility guarantees in ~250 tokens; request `schemas` or `commands` only when you actually need them. Before choosing syntax, scope `commands run` with `--fields`: `--fields RESOURCE.ACTION` returns one complete registry entry, `--fields RESOURCE` returns every action of one resource, and appending `.FIELD` paths projects the metadata you need — the four-field per-action projection `usage,description,flags,examples` is the smallest self-sufficient starting point for constructing an invocation. Inspect `flags`, `structuredExamples`, `schemas`, `requiresAuth`, `requiresProject`, `sideEffect`, `async`, `idempotency`, `dryRun`, `cleanupCommand`, `unsafeOutputs`, `outputShape`, `inputContract`, and `exitCodes` instead of guessing.

`--fields` takes a comma-separated list, so request several actions in one call. Each key is echoed exactly as requested (`--fields dataset.create` returns `{"dataset.create": {...}}`); an empty `--fields` is a usage error, never a silent full dump. The full registry (~220k tokens) is exported only via `--output PATH`: the command writes the registry, or the selected `--fields` subset, as compact JSON to `PATH`, and stdout carries the compact `{"path":"PATH"}` result — never the registry itself. An unknown resource or action exits with code 1 and a compact JSON error object on stdout containing the valid options.

## Agent skill installation

```text
dss install-skill --list-agents
dss install-skill --agent omp --target .
dss install-skill --agent omp --target . --dry-run
dss install-skill --global --agent omp
```

`--list-agents` only reports targetable agents; it does not write files. Auto-detection checks supported agent binaries/config directories (`claude`, `codex`, `cursor`, `pi`, `omp`). Passing `--agent NAME` forces one entry and reports `via:"flag"`.

Project installs write `SKILL.md` under the target workspace:

- Claude: `.claude/skills/dataiku-dss/SKILL.md`
- Codex: `.codex/skills/dataiku-dss/SKILL.md`
- Cursor: `.cursor/skills/dataiku-dss/SKILL.md`
- Pi: `.pi/skills/dataiku-dss/SKILL.md`
- OMP: `.omp/skills/dataiku-dss/SKILL.md`

Global installs write under the agent's home config path, for example OMP: `~/.omp/agent/skills/dataiku-dss/SKILL.md`.

## Credentials

Use environment variables for ephemeral runs. For disposable agent tests, set `DSS_CONFIG_DIR` to a temporary directory so saved credentials never touch your real profile.
Credential precedence is flags first, then `DATAIKU_*` environment variables, then saved credentials in `DSS_CONFIG_DIR` or the platform config directory.
Set `DATAIKU_DISABLE_ENV=1` when a test must ignore both `.env` files and `DATAIKU_*` environment variables.
When `.env` loading is enabled, the CLI reads `.env` from the CLI build/root directory and from the command's current working directory; put test-specific `.env` files in the directory where you invoke `dss`.

POSIX shell:

```sh
export DATAIKU_URL=https://dss.example.com
export DATAIKU_API_KEY=your-api-key
export DATAIKU_PROJECT_KEY=MYPROJ
dss project list
```

PowerShell:

```powershell
$env:DATAIKU_URL = "https://dss.example.com"
$env:DATAIKU_API_KEY = "your-api-key"
$env:DATAIKU_PROJECT_KEY = "MYPROJ"
dss project list
```

Windows Command Prompt:

```bat
set "DATAIKU_URL=https://dss.example.com"
set "DATAIKU_API_KEY=your-api-key"
set "DATAIKU_PROJECT_KEY=MYPROJ"
dss project list
```

Persist credentials when needed:

```text
dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ
```

The command saves credentials and returns `{ "saved": true, "path": "..." }`.
`auth login` validates by listing accessible projects before saving credentials, so the API key must be allowed to call DSS project-list APIs.

## Examples

```bash
dss version
dss doctor --fast
dss project list
dss project import ./project.zip --target-project-key IMPORTED_PROJECT
dss dataset list --project-key MYPROJ
dss dataset files uploaded_input --project-key MYPROJ
dss dataset upload-file uploaded_input ./new.csv --file-name new.csv --project-key MYPROJ
dss recipe get-payload compute_orders --project-key MYPROJ
dss recipe get-payload compute_orders --output code.py --project-key MYPROJ
dss install-skill --dry-run
dss app validate-manifest --project-key MYAPP_INSTANCE
dss app compare-manifest my-app --project-key MYAPP_INSTANCE
dss app create-instance my-app --data '{"targetProjectKey":"MYAPP_INSTANCE"}' --wait --record-cleanup cleanup.jsonl
dss app manifest-version --project-key MYAPP_TEMPLATE
dss app set-manifest-version --manifest-version 1.4.0 --project-key MYAPP_TEMPLATE
dss app create-successor-instance my-app --from MYAPP_INSTANCE --to MYAPP_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance my-app --project-key MYAPP_INSTANCE_V2 --expect-version 1.4.0
dss app permissions-snapshot --project-key MYAPP_INSTANCE --output permissions.json
dss app permissions-diff --project-key MYAPP_INSTANCE --file permissions.json
dss app permissions-restore --project-key MYAPP_INSTANCE --file permissions.json --dry-run
dss cleanup --file cleanup.jsonl
dss cleanup --file cleanup.jsonl --apply
```
`project import` performs both public API stages: archive upload, then import processing. It
exits nonzero when DSS returns `success:false` even if both HTTP requests succeeded.
`dataset upload-file` only adds a filename that is not already present and verifies its byte
length afterward. DSS 14.7 exposes no public UploadedFiles delete or replacement endpoint;
the command refuses an existing filename before POST instead of triggering DSS's opaque 500.
For replacement, import a successor project archive containing the desired upload.

### Application release safety

Use the app commands as explicit release gates around a stable template, instance project key,
and permission set:

```bash
# Validate source-verifiable manifest references.
dss app validate-manifest --project-key APP_TEMPLATE

# Detect governed template/instance drift. Only projectKey and projectAppType are normalized away.
dss app compare-manifest APP_ID --project-key RELEASE_INSTANCE

# Read the raw `version` from the public app manifest (string fields only; never appVersion).
dss app manifest-version --project-key APP_TEMPLATE

# Write version/versionNotes through the public app-manifest endpoint. This is NOT a publish
# transaction: it only changes the raw manifest metadata. --expect-hash SHA256 is a non-atomic
# stale-read guard, not a conditional write: it refuses the write when the manifest already
# changed since you read that hash, but DSS accepts every PUT unconditionally. This command can
# overwrite a write landing between its read and PUT, and the final read cannot detect that lost
# update when this command's payload wins. Every result reports
# concurrencyControl: "client-side-non-atomic-stale-read-check".
dss app set-manifest-version --manifest-version 1.4.0 --project-key APP_TEMPLATE

# Create an instance only after its target key is confirmed absent, wait on the DSS future, and
# record deterministic cleanup.
dss app create-instance APP_ID \
  --data '{"targetProjectKey":"RELEASE_INSTANCE","targetProjectName":"Release instance"}' \
  --wait --record-cleanup cleanup.jsonl

# Roll out a new template version to an existing instance as an additive successor: the old
# instance is never modified or deleted, and the command always waits on the DSS future
# (there is no --wait flag). The recorded cleanup entry targets only the new project key. If DSS
# accepts creation but returns no future ID, cleanup stops unresolved instead of racing creation.
dss app create-successor-instance APP_ID \
  --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 \
  --name "Release instance v2" --copy-permissions --record-cleanup cleanup.jsonl

# API readiness gate for the successor. apiReady:true with status API_VERIFIED_UI_PENDING is an
# API-verified state only — never visual verification. The API key authenticates public REST only;
# visual confirmation stays an external SSO gate (pre-authenticated SSO browser session or a
# dedicated UI test identity).
dss app verify-instance APP_ID --project-key RELEASE_INSTANCE_V2 --expect-version 1.4.0

# Retirement of the predecessor is a separate, separately guarded step after the successor is
# verified. It is never performed implicitly by the successor flow.
dss app delete-instance --project-key RELEASE_INSTANCE

# Snapshot, compare, preview, and restore project permissions.
dss app permissions-snapshot --project-key RELEASE_INSTANCE --output permissions.json
dss app permissions-diff --project-key RELEASE_INSTANCE --file permissions.json
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json --dry-run
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json
```

Permission snapshots are written with mode `0600` and an integrity hash covering the canonical DSS
URL, the concrete project incarnation (`creationTag`), capture metadata, and permissions. Diff and
restore reject snapshots from another DSS server, project key, or observed incarnation. These are
client-side, non-atomic stale-identity checks: DSS exposes no conditional permission PUT, so the
checks narrow and detect key-reuse races but cannot serialize the final check with the write.
Snapshots contain access-control identities; commit them only when repository policy permits.
`validate-manifest` checks `SCENARIO_RUN` scenario IDs, `DOWNLOAD_FILE`
managed-folder IDs, and runtime-form parameter names against supported public project APIs.

Some DSS deployments hide unknown project keys behind `403` instead of returning `404`.
Instance creation requires confirmed target absence before POST: an inaccessible target not present
in the visible project list is still unconfirmed and is rejected rather than risking cleanup
against a pre-existing project. A definitive create rejection never produces a cleanup entry.

Every new cleanup entry records the canonical DSS URL. App-instance cleanup records a
`creationTag` hash observed after the DSS future identifies the target key; unconfirmed creation
stops unresolved. The future target and later `creationTag` are independent, non-atomic
observations: the public API exposes neither an immutable project ID joined to the future nor a
conditional DELETE. Cleanup rechecks type and `creationTag` immediately before deletion, rejecting
detected key reuse, but cannot eliminate replacement in the remaining check-to-DELETE gap.
`cleanup --apply` validates the full ledger before issuing any request and rejects legacy entries
without server identity, mixed-server ledgers, entries for another DSS URL, and app cleanup entries
without a valid incarnation binding.

Saving `version`/`versionNotes` in the public app manifest is a metadata write, not a publish
transaction. If the manifest PUT or its verification read has an ambiguous transport/server
failure, the command exits non-zero with `persisted:null`, `after:null`, and
`outcome:"indeterminate"` rather than claiming the write failed or succeeded. Existing instances
are never upgraded in place: new instances inherit the template's
raw `version`, and an existing instance is rolled forward by creating an additive successor (the
old instance is preserved) followed by a separate, guarded retirement. `verify-instance` never
reports visual verification: its output is `apiReady:true` with
`status:"API_VERIFIED_UI_PENDING"` and `uiPublicationVerified:false`. Its `visual-ui` gate names
the required authentication and evidence: open the instance in a pre-authenticated SSO browser or
with a dedicated UI test identity, then exercise the affected tiles, forms, and actions. The CLI
never marks that external check complete. The public DSS APIs used by this SDK do not expose a
supported app-template publish/recreate/rename workflow, recipient-level app sharing setter, or
UI-click smoke test. The CLI does not guess private endpoints for those operations.

## Project Git source-control safety

Project Git commands require DSS 12.4.2 or newer plus read and write access to the project
content. Inspect state before changing it:
The DSS project Git API exposes status, remotes, branches, tags, branch switching, fetch,
rebase-based pull, push, log, diff, commit, revert/reset/rebuild, external-library, and future
operations. It does not expose a separate checkout operation; use `switch`.

```bash
dss project-git status --project-key MYPROJ
dss project-git branches --remote --project-key MYPROJ
dss project-git log --count 20 --project-key MYPROJ
```

Creating a branch in a duplicate project is the safe UAT path; creating it in place switches the
source project to the new branch:

```bash
dss project-git create-branch feature/orders \
  --duplicate-project \
  --target-project-key MYPROJ_ORDERS_UAT \
  --project-key MYPROJ \
  --plan
dss project-git create-branch feature/orders \
  --duplicate-project \
  --target-project-key MYPROJ_ORDERS_UAT \
  --project-key MYPROJ
```

`pull` rebases rather than merging. `reset-to-head`, `reset-to-upstream`, reverts, forced or
remote branch deletion, library-directory deletion, and `drop-and-rebuild` are destructive;
inspect their local `--plan` result first. `drop-and-rebuild` additionally requires
`--i-know-what-i-am-doing` and destroys all Git history. Repository URLs containing embedded
HTTP credentials are rejected. External-library credentials must come from
`--password-env ENV_NAME`; the value is never included in plans or command output. Library
operations that return a `jobId` can be observed with `project-git future-status` or completed
with `project-git future-wait`.
Project Git requests use `/dip/publicapi/projects/{projectKey}/git/*`; they do not configure
remote authentication. DSS documents administrator Git group rules plus either per-user SSH keys
under **Profile > Credentials > SSH** or non-interactive system-level credentials for the DSS
server account. Configure these before private-remote fetch, pull, or push. `--password-env`
applies only to external project-library operations and does not persist a remote credential.
The `project-git` resource does not manage these settings, so public/auth-less and
already-configured remotes are the supported automation paths.

For fake-DSS smoke tests, return project lists as JSON arrays such as `[{ "projectKey": "MYPROJ", "name": "My Project" }]` from `/public/api/projects/`; recipe payload commands read `/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true` and expect a JSON object shaped like `{ "recipe": { "name": "<NAME>", "type": "python" }, "payload": "..." }`.
