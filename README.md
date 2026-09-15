# dataiku-sdk

Agent-only TypeScript SDK and `dss` CLI for Dataiku DSS automation.

## API coverage

The SDK and CLI include scenario run management; project, recipe, and dataset metadata; managed datasets, lineage, and table imports; external saved models and MLflow imports; LLM completions and embeddings; knowledge banks; project folders and data collections; users, groups, connections, macros, and plugin administration. Discover exact inputs with `dss commands run` and scoped command contracts rather than assuming flags from API field names. MLflow archive/folder imports and evaluations resolve the external container config explicitly: DSS 15 requires the `containerExecConfigName` parameter, so the SDK/CLI send `NONE` rather than omitting it (the plan surface matches the executed request).

The [DSS 15 coverage matrix](docs/API-COVERAGE-DSS-15.md) maps documented operations to SDK methods and CLI actions, records documentation discrepancies, and identifies remaining gaps. A [machine-readable matrix](docs/API-COVERAGE-DSS-15.json) is also available. Coverage is not full DSS parity or live compatibility certification: availability depends on DSS version, permissions, licensing, and installed capabilities. One known documentation discrepancy: `plugin git-branches` is served by GET on DSS 15 (the same route rejects POST with 405 `rawMethodPOSTnotsupported`, contrary to the public docs).

Metadata setters replace the complete metadata object: fetch it, edit it, then submit the replacement. Metadata previews expose that replacement under `next`. New mutation previews (`--plan` and `--dry-run`) make no DSS requests; sensitive payloads may be represented by redacted summaries rather than literal request bodies. LLM completions and embeddings are cost-bearing operations, and macros execute plugin code. Review their contracts before execution.

ML training and macro waits bound status requests by the remaining wait deadline, including response bodies and retries. The client `requestTimeoutMs` still caps each request: an overall wait budget can shorten that cap, never extend it. An individual request timeout propagates as an error until the overall wait budget expires. Future and Project Git waits use adaptive polling by default; explicit polling intervals remain fixed. Early CLI usage errors load at most the selected resource definitions, not the full command runtime.

Malformed user/group listings and scenario run histories fail explicitly instead of appearing empty. Scenario and macro run responses must contain usable run identifiers; missing identifiers are server-response errors, not caller-validation errors.

## Platform support

The published `dss` CLI requires Bun >= 1.4.0 and supports Linux, macOS, and Windows. The release gate runs Bun 1.4.0 on all three operating systems. The runtime dependency is pure JavaScript, so the same package also runs on every Bun-supported x64 and ARM64 system.

Run directly with Bun:

```text
bunx --bun dataiku-sdk version
```

Or install the npm binary:

```text
npm install --global dataiku-sdk
```
The installed `dss` bin bootstraps Bun with automatic `.env` preloading disabled, so Bun >= 1.4.0 must be on `PATH`; the CLI's own `.env` handling stays authoritative. `bunx --bun` forces Bun's Node-compatibility mode, which provides the same no-preload guarantee across operating systems.

Bun is the only supported runtime and the package manager. Examples below assume an installed `dss` binary. From a checkout, use `bun --no-env-file src/cli.ts ...` or the packaged launcher `bun --no-env-file ./bin/dss.js ...`. From another working directory, pass the checkout's absolute `bin/dss.js` path to Bun.
`--no-env-file` disables Bun's automatic preloading only; the CLI still applies its documented `.env` handling unless `DATAIKU_DISABLE_ENV=1` is set.
`dss version` reports whether the running code came from `source` or `dist`, the runtime (always `bun`), the packaged build revision when available, and `staleBuild` when that revision differs from the checkout. Release builds generate `dist/build-metadata.json`; source snapshots without Git metadata still build but report no build revision.

## Self-provisioning live tests

From a checkout, `test:live` is a separate Bun script, not a `dss` command. It reads
the existing `.env` without changing it and needs project-creation access plus a
writable managed storage connection. No imported tutorials are required.

```sh
bun run test:live setup
bun run test:live run --case core.dataset.baseline
bun run test:live run --case "core.recipe.*"
bun run test:live run --profile all          # every profile's run cases in one lab
bun run test:live run --profile ml --case "ml.*"
bun run test:live run --profile all --case "applications.*,infrastructure.sql-select"
bun run test:live status
bun run test:live clean               # delete only this lab, including its root project
# CI: setup + run + cleanup, even when cases fail
bun run test:live all
```

`setup` is idempotent for a ready lab; `run` reuses its datasets, graph and collaboration
fixtures. Cases use temporary resources or restore baseline values. Cases accept exact
IDs, comma-separated lists, repeated `--case`, and trailing-prefix `*` matching; quote
wildcards. Selected runs omit the legacy suites. Unknown, setup-only or inactive-profile
or empty selections fail before credentials, state creation or provisioning; `--case` is valid only
for `run`/`all`. `run` refuses
an incomplete or cleaned lab rather than silently rebuilding it. Clean failed setup
before provisioning a replacement. Authentication and transport failures retain their
original errors; they are not evidence that the lab is missing.

`--profile all` is a selection operator, not a lifecycle command: it widens run-case
availability to every profile (core, ml, applications, infrastructure) in the same
persistent lab, and setup-only cases stay unselectable. The selector itself neither
provisions beyond the selected cases nor cleans — combined with the `all` VERB the
run still ends in the usual destructive cleanup.

Core fixtures include deterministic CSVs with nulls, duplicates, Unicode and quoting;
server-provisioned managed datasets; sync, Prepare, join, fuzzy-join, grouping and Python
recipes; folders, scenarios, variables, wiki, notebooks, dashboards, insights, libraries,
metrics and quality rules. Project export/import/duplicate cases use separate owned
projects. Managed dataset types, paths and recipe output schemas come from DSS rather
than tutorial-specific connection guesses.

Profiles add to core: `ml` trains and deploys a small decision tree and checks clustering.
`applications` template-gated cases (template surface, instance, successor and
business-app surfaces) need `DATAIKU_LIVE_APP_TEMPLATE_ID`, and business-app instance
cases may additionally use `DATAIKU_LIVE_BUSINESS_APP_ID`,
`DATAIKU_LIVE_BAPP_INSTANCE_PROJECT`, `DATAIKU_LIVE_BAPP_USER` and
`DATAIKU_LIVE_BAPP_ARCHIVE_PATH` — each missing prerequisite reports an exact blocker.
Other application and infrastructure read/CRUD cases run without these. The SQL probe
case needs `DATAIKU_SQL_CONNECTION` or `DATAIKU_SQL_DATASET_FULL_NAME` (other
infrastructure read-only cases run without either).
The SQL case permits only the fixed, table-free `SELECT 1 AS one` probe with an
explicit target and an owned project whose incarnation is checked before execution.
It does not authorize arbitrary SQL, alternate query inputs or global mutations. Missing
prerequisites are recorded as **blocked**, never passed. Required blocked cases exit
nonzero; optional blocked cases remain visible in reports. Global administration,
external Git mutations and unsupported capabilities are not silently exercised.

SQL catalog/import cases can provision an owned file-backed SQLite connection and
seed a real table. Project and plugin Git cases use owned bare repositories served on loopback
through the server's `git-http-backend`; they never change global Git rules. These
fixtures require server-side Python execution and the relevant admin/Git permissions.
Host paths and process identities are recorded in the manifest. Cleanup verifies
ownership before stopping services and deleting directories; failed cleanup retains
the runner project for recovery.

State is ignored by Git under `.live-tests/<server-hash>/`. Use `--state-dir PATH` to keep
a persistent demo or verification lab outside the default root — the directory is
created on demand, isolated per server hash, and never cleaned implicitly. For `run`,
`clean` or `status` you can alternatively select an existing lab with `--manifest PATH`
(the two flags overlap only there; `setup`/`all` require `--state-dir` instead). Keep the manifest and
`cleanup.jsonl`: cleanup requires exact project identities and creation-incarnation
hashes, never a prefix sweep. A changed server/project identity or unconfirmed creation
fails closed; investigate its journal before manual recovery. Locks prevent concurrent
lab runs; after an uncatchable process kill, confirm its recorded process is gone before
removing a stale lock. Interrupting a run preserves the lab; `all` attempts cleanup.

New globally-scoped case families create only isolated, disposable resources: users,
groups, meanings, workspaces, data collections, API-deployer infra/services/deployments,
and plugins require an explicit reservation in the lab manifest plus an exact
ownership marker written into a lab-controlled field and re-read on every GET
(`ctx.markerFor`; code-envs instead bind on the server's `desc.creationTag`
incarnation snapshot, and project folders have no nonce field — they bind on the
server-generated id plus owner binding). The lifecycle is explicit: reserve checks the
pending reservation against the manifest; create runs the command and binds the entry
only after a GET proves the marker/incarnation identity; bound-target mutations
re-verify the live object before every request. A GET that cannot prove identity
leaves the entry unconfirmed rather than bound. A failed creation is never deleted
blind and a conflicting pre-existing resource is recorded, not removed. Pre-existing
accounts, connections and configuration are never modified or deleted.

Trained-ML demonstration state is retained across iterations in the lab: the `ml`
profile keeps a real trained decision-tree model and its fixtures, so subsequent
iterations read model metadata, settings and versions without retraining.

Core fixtures include deterministic CSVs with nulls, duplicates, Unicode and quoting;
server-provisioned managed datasets; sync, Prepare, join, fuzzy-join, grouping and Python
recipes; folders, scenarios, variables, wiki, notebooks, dashboards, insights, libraries,
metrics and quality rules. Project export/import/duplicate cases use separate owned
projects. Managed dataset types, paths and recipe output schemas come from DSS rather
than tutorial-specific connection guesses. Expanded case families beyond the original
core set cover bundles (export/publish lifecycles), Project Git (inspect, commit,
remote and external-library lifecycles against owned loopback repositories), scenario statistics and
payload round-trips, application template and instance manifests, webapp backend state,
infrastructure reads (connections, users, groups, macros, code-envs, project folders,
data collections), disposable global lifecycles under the ownership-marker contract,
and disposable folders/notebooks with plugin.json receipt flows. Plugin cases that
generate a plugin-managed code environment delete that derived environment explicitly
before deleting the parent plugin (DSS does not cascade the deletion), and plugin updates
require the `settings.codeEnvName` association created after plugin creation. MLflow
import fixtures use canonical packaging (`code:'code'`, `data:'model'`, `env:'conda.yaml'`)
inside owned runtime environments with pinned scientific versions, and both archive and
folder imports/evaluations pass explicit `containerExecConfigName=NONE` (DSS 15 rejects
requests that omit it; folder imports previously inherited a default); noncanonical
model-path layouts fail with READ_META `ModuleNotFoundError`.

Some capabilities have external prerequisites the lab cannot self-provision; each
affected action records an explicit per-action blocker instead of passing or being
silently omitted: an installed application template (`DATAIKU_LIVE_APP_TEMPLATE_ID`),
external SQL connectivity for non-fixture workloads (`DATAIKU_SQL_CONNECTION`/`DATAIKU_SQL_DATASET_FULL_NAME`),
an external auth backend (LDAP/SAML) for user external-directory reads and resyncs, plugin-store
access, and any cost-bearing surface (LLM completions/embeddings bill per call; macros
execute plugin code). Missing credentials, templates, external infrastructure or billing
eligibility are reported as **blocked**, never as success.

Each iteration retains logs and `report.json` with cases, timing, executed actions,
capability reasons, coverage and external-project metadata integrity. Unavailable project
metadata is reported separately in `integrity.unreadable`, not as confirmed drift;
integrity remains unverified and the run fails until those reads succeed. `clean` retains
`cleanup-report.json`, including deletion failures; starting teardown invalidates lab
readiness even if cleanup only partially succeeds. If cleanup and integrity verification
both fail, the cleanup error remains primary and the report preserves both results.
Planned lifecycle commands retain ownership checks; local outputs stay inside the lab
for both `--output` and `--output-file`. Treat local reports and exported archives as private project
data. Coverage is an explicit inventory of every registered action, not a claim that
every action has a live test: unexercised actions remain **uncovered**, and the
expanded demonstration keeps each registered action visible as demonstrated, blocked
with a recorded prerequisite, or uncovered — never silently dropped. Add cases through
the typed catalogue in `tests/live-cases.ts`, `LiveContext.check` and guarded helpers,
then update `tests/live-coverage.ts` when the
registry changes; catalogue drift fails the offline gate. Plan/dry-run calls do not count
as executed live coverage.

Regular `bun run check` and `bun run lint` include the live runner and fixtures.
Lint includes project-owned rules in `tools/oxlint/index.ts`: accumulator-copy, widen-then-assert, and chained-assertion errors, alongside the native accumulating-spread check.
For focused verification:

```sh
bun run check:live
bun test tests/live-context.test.ts tests/live-runner.test.ts tests/live-coverage.test.ts
```

## CLI contract

The complete agent-facing contract — stdout JSON discipline, stderr JSONL diagnostics, the error
envelope, exit codes, `--fields` projection, command discovery, and planning/safety rules — is the
skill bundle rooted at [`skills/dataiku-dss/SKILL.md`](skills/dataiku-dss/SKILL.md). The short
entrypoint routes agents to focused references; the installed command registry remains the source
of truth for command syntax and schemas. Human
essentials: exactly one compact JSON value on stdout per command (void success is `{ok:true}`),
failures as one structured JSON error object on stdout with a nonzero exit code, stderr reserved
for JSONL diagnostics only, and exit codes `0` success / `1` usage error / `2` DSS or internal
error / `3` transient DSS error / `4` failed long-running result or assertion. `--retries N`
controls idempotent GET retries only; `dss sql query --start-retries N` retries transient failures
while starting a query. For portable multi-step mutations, prefer `dss batch`; shell chaining and
pipeline exit semantics differ across platforms.

Discover the command surface with scoped calls:

```text
dss commands run
dss commands run --fields dataset.create.usage,dataset.create.description,dataset.create.flags,dataset.create.examples
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
```

`dss commands run` prints the compact resource/action summary; registry entries never dump to stdout — they travel only via `--output PATH`. See the canonical skill for
the full `--fields` projection rules and planning workflow.

## Agent skill installation

```text
dss install-skill --list-agents
dss install-skill --agent omp --target .
dss install-skill --agent omp --target . --dry-run
dss install-skill --global --agent omp
```

`--list-agents` only reports targetable agents; it does not write files. Auto-detection checks supported agent binaries/config directories (`claude`, `codex`, `cursor`, `pi`, `omp`). Passing `--agent NAME` forces one entry and reports `via:"flag"`.

Project installs write `SKILL.md` and its sibling `references/` directory under the target workspace:

- Claude: `.claude/skills/dataiku-dss/SKILL.md`
- Codex: `.codex/skills/dataiku-dss/SKILL.md`
- Cursor: `.cursor/skills/dataiku-dss/SKILL.md`
- Pi: `.pi/skills/dataiku-dss/SKILL.md`
- OMP: `.omp/skills/dataiku-dss/SKILL.md`

Global installs write under the agent's home config path, for example OMP: `~/.omp/agent/skills/dataiku-dss/SKILL.md`. The reported `target` is the home directory for global installs and the resolved workspace directory for project installs; `installed[].path` names each entrypoint.

Each entry in the `installed` array reports pre-install bundle state:

- `status`: `missing` when the entrypoint is absent; `stale` when any bundled file is missing or differs; `current` when every bundled file matches.
- `changed`: whether any bundled file would (or did) change. Byte-identical files are not rewritten.
- `expectedSha256` and `actualSha256`: the canonical and existing **entrypoint** hashes. The latter is absent when the entrypoint is missing.
- `files`: each bundled file's portable `relativePath`, destination `path`, `status`, `changed`, and hashes. Missing or stale references are detected even when `SKILL.md` itself is current.

`--dry-run` and `--plan` never write. Real installs replace changed files through same-directory temporary files and rename, writing references before the entrypoint. Atomicity is per file, not a bundle-wide transaction. Unrelated files in the destination are preserved.

### Token-efficient skill structure

The entrypoint contains bootstrap/discovery, output interpretation, and universal safety rules. Seven focused files under [references/](skills/dataiku-dss/references/) cover authentication, discovery, mutations, application releases, flow maps, coding, and troubleshooting. All are linked directly from `SKILL.md` with task-specific read conditions; agents should not preload the whole directory.

The entrypoint measures **750 o200k_base tokens**, down from **851** in the previous revision (12% less) and **4,272** before progressive disclosure. The token-budget test now caps it at **825** tokens. Claude V5 reconstruction counts fell **1,420 → 1,250** and GLM5 **858 → 752**. Safety rules and all seven reference links remain. Reference content consumes context only when read; all five agent installers ship the complete bundle.

All seven references are token-budgeted (each measured `o200k_base` baseline plus 5% headroom). Per-file baselines live in [the token-budget tests](tests/cli/agent-token-budget.test.ts); run `bun run test:tokens` to check them.

References total **4,179 → 3,545 tokens (15.2% less)**. With the 750-token entrypoint, the whole bundle totals **4,295**; this is a sum of file counts, not a recommendation to preload them. All references shrink across ten encodings; whole-bundle Claude V5 reconstruction **8,200 → 7,362**, GLM5 **4,943 → 4,305**. Runnable examples and safety conditions are retained.

This uses progressive disclosure from the [Agent Skills specification](https://agentskills.io/specification) and [Anthropic's authoring guidance](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices): concise metadata/instructions, direct relative reference links, and domain-focused documents. No scripts or assets are needed here; generated command metadata is queried from the CLI rather than duplicated in the skill.

`bun run test:tokens` enforces the pinned OpenAI `o200k_base` budgets. To also check ten native encodings (OpenAI, Claude, GLM, Qwen, DeepSeek, and Kimi), point the optional development-only gate at an OMP native module:

```bash
DSS_TOKENIZER_MODULE=/absolute/path/to/oh-my-pi/packages/natives/native/index.js bun run test:tokens
```

Without this variable, only the native cross-model case is skipped. A configured missing/incompatible module fails rather than estimating tokens. The cross-model gate permits 10% growth over its recorded text baselines for bootstrap and action discovery; the pinned OpenAI gate also enforces the skill ceiling above. Claude counts use ctok reconstructions, not an official tokenizer, and all counts exclude provider message/tool framing. No native dependency is added to the CLI.

Measured full `dataset.create` discovery versus its `usage,description,flags,examples` projection: OpenAI **1,059 → 356**, Claude V5 reconstruction **1,771 → 640**, GLM5 **1,015 → 347** tokens. Use the projection when those fields suffice; fetch the full action when schema details are needed. This is selective disclosure, not lossless compression.

Boolean flags accept bare flags or explicit `=true`/`=false` values, including aliases. Invalid boolean values fail before dispatch. Direct, batch, and local commands share execution-mode resolution; `--plan --dry-run` returns a plan with `plannedAndDryRun:true` when both modes are supported. Unreadable JSON/text payload files produce `validation_failed` usage errors (exit 1) with the flag, path, and filesystem cause.

For raw streaming downloads, `--request-timeout MS` bounds each pending body read, not the total transfer duration: large progressing transfers remain supported. Buffered text/JSON consumers retain their bounded body-read budget. Request retries do not replay partially consumed streams. Project, bundle, and API-service archives and managed-folder downloads are written to private temporary files beside the destination, then renamed on success. Failures leave no partial output and preserve any existing destination.

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
dss project inspect-archive ./project.zip
dss project import ./project.zip --target-project-key IMPORTED_PROJECT --record-cleanup cleanup.jsonl
dss dataset list --project-key MYPROJ
dss dataset files uploaded_input --project-key MYPROJ
dss dataset upload-file uploaded_input ./new.csv --file-name new.csv --project-key MYPROJ
dss dataset assert-count jmp_doe_ready --expected 12 --project-key MYPROJ
dss dataset assert-schema jmp_doe_ready --data-file expected-schema.json --project-key MYPROJ
dss data-quality assert-results jmp_doe_ready --project-key MYPROJ
dss recipe get-payload compute_orders --project-key MYPROJ
dss recipe get-payload compute_orders --output code.py --project-key MYPROJ
dss install-skill --dry-run
dss app validate-manifest --project-key MYAPP_INSTANCE
dss app compare-manifest my-app --project-key MYAPP_INSTANCE
dss app create-instance my-app --data '{"targetProjectKey":"MYAPP_INSTANCE"}' --wait --record-cleanup cleanup.jsonl
dss app manifest-version --project-key MYAPP_TEMPLATE
dss app successor-preflight my-app --from MYAPP_INSTANCE --to MYAPP_INSTANCE_V2 --copy-permissions
dss app set-manifest-version --manifest-version 1.4.0 --expect-hash PREFLIGHT_TEMPLATE_MANIFEST_HASH --project-key MYAPP_TEMPLATE
dss app create-successor-instance my-app --from MYAPP_INSTANCE --to MYAPP_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance my-app --project-key MYAPP_INSTANCE_V2 --expect-version 1.4.0
dss app permissions-snapshot --project-key MYAPP_INSTANCE --output permissions.json
dss app permissions-diff --project-key MYAPP_INSTANCE --file permissions.json
dss app permissions-restore --project-key MYAPP_INSTANCE --file permissions.json --dry-run
dss cleanup --file cleanup.jsonl
dss cleanup --file cleanup.jsonl --apply
```
`project import` performs both public API stages: archive upload, then import processing. It
first validates ZIP integrity, safe unique member paths, the export manifest, and provable Flow
references locally. Verified success reports DSS's actual project key, explicit remapping, and a
`creationTag` incarnation hash; `--record-cleanup` records an incarnation-bound guarded delete.
It exits nonzero when DSS returns `success:false` even if both HTTP requests succeeded. A timeout,
5xx, malformed response, missing used key, or failed post-import identity read is
`ambiguous_outcome` and retains the temporary import ID for inspection before retry.

`dataset upload-file` only adds a filename that is not already present and verifies its byte
length afterward. DSS 14.7 exposes no public UploadedFiles delete or replacement endpoint;
the command refuses an existing filename before POST instead of triggering DSS's opaque 500.
For replacement, import a successor project archive containing the desired upload.

`dataset preview` returns `truncated` and `limit`; it probes one row past the requested cap instead
of presenting a capped sample as an exact count. `dataset assert-count`, `dataset assert-schema`,
and `data-quality assert-results` return `satisfied` and exit `4`/`assertion_failed` on a failed
postcondition, so they can be used as `batch` steps. Multi-job `job monitor` and `job watch`
aggregate child failures and also exit `4` when any watched job fails.

`doctor` reads the documented `DSS-Version` and `DSS-API-Version` response headers plus the HTTP
`Date` header from the public project-list call. Feature support remains behavioral: the CLI does
not call an undocumented feature endpoint or infer licensed capabilities from a version string.

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

# Run every read-only successor gate before changing the template version. This validates the
# template references, verifies the predecessor, proves target absence, and optionally snapshots
# the predecessor ACL. Keep template.manifestHash from the result for the next command.
dss app successor-preflight APP_ID \
  --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions

# Write version/versionNotes through the public app-manifest endpoint. This is NOT a publish
# transaction: it only changes the raw manifest metadata. --expect-hash SHA256 is a non-atomic
# stale-read guard, not a conditional write: it refuses the write when the manifest already
# changed since you read that hash, but DSS accepts every PUT unconditionally. This command can
# overwrite a write landing between its read and PUT, and the final read cannot detect that lost
# update when this command's payload wins. Every result reports
# concurrencyControl: "client-side-non-atomic-stale-read-check".
dss app set-manifest-version --manifest-version 1.4.0 \
  --expect-hash PREFLIGHT_TEMPLATE_MANIFEST_HASH --project-key APP_TEMPLATE

# Create an instance only after its target key is confirmed absent, wait on the DSS future, and
# record deterministic cleanup.
dss app create-instance APP_ID \
  --data '{"targetProjectKey":"RELEASE_INSTANCE","targetProjectName":"Release instance"}' \
  --wait --record-cleanup cleanup.jsonl

# Roll out a new template version to an existing instance as an additive successor: the old
# instance is never modified or deleted, and the command always waits on the DSS future
# (there is no --wait flag). The recorded cleanup entry targets only the new project key. If DSS
# may have accepted creation but no future ID is returned, the outcome is indeterminate and no
# unbound cleanup entry is written.
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
in the visible project and app-instance lists is still unconfirmed and is rejected with
`target_absence_unverifiable` / `permission_or_environment` rather than risking cleanup against a
pre-existing project. Choosing another key does not solve deployments that mask every unknown key.
DSS exposes no permission-independent public availability endpoint, and its published app-instance
API does not guarantee duplicate-key rejection before any write or non-overwrite behavior, so
there is no unsafe force or unconfirmed-target bypass. Use an identity with global project
visibility. A definitive create rejection never produces a cleanup entry; an ambiguous POST
without a future ID or verified
incarnation also produces no cleanup entry.

Static create plans describe, but do not execute, these reads:
`preflightExecuted:false` and `preflightWillRunDuringApply:true`. Run `successor-preflight` when
live read evidence is required before changing the template version.

Every new cleanup entry records the canonical DSS URL. App-instance cleanup records a
`creationTag` hash observed after the DSS future identifies the target key; a future-addressable
entry stops unresolved until the future names that key and its incarnation can be bound. An
ambiguous POST with no future ID produces no entry. The future target and later `creationTag` are independent, non-atomic
observations: the public API exposes neither an immutable project ID joined to the future nor a
conditional DELETE. Cleanup rechecks type and `creationTag` immediately before deletion, rejecting
detected key reuse, but cannot eliminate replacement in the remaining check-to-DELETE gap.
`cleanup --apply` validates the full ledger before issuing any request and rejects legacy entries
without server identity, mixed-server ledgers, entries for another DSS URL, and app cleanup entries
without a valid incarnation binding. Project-import cleanup uses the same rule: the ledger binds
the actual imported key and verified `creationTag` hash, and `project delete
--expect-project-incarnation` refuses a reused or unverifiable key before DELETE.

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

## Flow visualization and zone planning

`project map` returns one agent-oriented JSON view of the Flow: normalized nodes and edges,
joined zone metadata, cycle-safe topological layers, weakly connected components, diagnostics, and
a stable `topologyFingerprint`. Zone joins add `zoneId`/`zoneName` to each node; the top-level
`zones` array preserves zone color, position, item count, and visible node IDs.

```bash
dss project map --project-key MYPROJ
dss project map --render mermaid --project-key MYPROJ
dss project map --render ascii --max-nodes 100 --max-edges 200 --project-key MYPROJ
```

Rendering is optional and remains inside the JSON result as `rendering.content`; stdout never
switches to an unstructured diagram stream. Layers are computed after collapsing strongly connected
components, so cycles are reported without blocking the analysis. Diagnostics cover cycles,
disconnected components, isolated/default-zone nodes, cross-zone edges, duplicate assignments, and
truncated analysis. When limits truncate the returned nodes or edges, the topology fingerprint still
covers the full normalized Flow while layers/components/diagnostics cover the returned subset.

The public API exposes zone positions and membership, not individual node pixel coordinates. The CLI
therefore preserves only supported zone-level visual metadata and never fabricates node coordinates.
Use the declarative export/apply loop for visual organization:

```bash
dss flow-zone plan --project-key MYPROJ > flow-zones.json
dss flow-zone organize --file flow-zones.json --dry-run --project-key MYPROJ
dss flow-zone organize --file flow-zones.json --project-key MYPROJ
```

`flow-zone plan` emits JSON accepted directly by `flow-zone organize`, including every
current zone's supported metadata and explicit items. Reapplying an unchanged plan produces no
redundant moves. `organize` compares the plan's topology fingerprint before mutation and recomputes
it after mutation; a mismatch fails rather than claiming a visual-only change. Zone names, colors,
positions, and membership are deliberately excluded from the fingerprint; flow node identity/kind and
edges are included.

Recipe/code analysis is a separate audit. `project map` and `flow-zone plan` do not fetch,
hash, interpret, or grade recipe payloads. Use the recipe inspection/diff/assertion commands in a
separate workflow when code quality or semantic review is required; never mix those findings into a
layout plan.

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

## Coding, notebooks, and project libraries

`project-library` manages files in a project's internal `lib/` tree. External Git-backed
libraries are a different surface: inspect and change those with `project-git list-libraries`,
`set-library`, `push-library`, and the related future commands. Internal paths are canonicalized;
leading slashes are removed, while traversal (`..`) and empty segments are rejected.

Use a read/diff/guarded-write sequence for internal files. `get-bytes` preserves binary data and
returns its SHA-256; `diff` caps text output and detects binary files instead of dumping bytes.
`put --expect-sha256` re-reads the remote bytes and refuses a stale overwrite. New files and
folders never overwrite existing items; `--if-not-exists` turns an existing target into a skip.

```bash
dss project-library get-bytes python/mylib/utils.py --output ./utils.py --project-key MYPROJ
dss project-library diff python/mylib/utils.py --file ./utils.py --project-key MYPROJ
dss project-library put python/mylib/utils.py --file ./utils.py --expect-sha256 HASH --dry-run --project-key MYPROJ
dss project-library put python/mylib/utils.py --file ./utils.py --expect-sha256 HASH --project-key MYPROJ
```

For one-off Python, prefer a file so shell quoting cannot change the source. `code run` creates an
observable throwaway custom-Python scenario, waits with a bounded timeout, caps returned logs, and
deletes only that generated scenario unless `--keep` is set.

```bash
dss code run --file inspect.py --env py311 --project-key MYPROJ
dss recipe diff prepare_orders --file recipe.py --project-key MYPROJ
dss recipe set-payload prepare_orders --file recipe.py --dry-run --project-key MYPROJ
dss recipe set-payload prepare_orders --file recipe.py --backup-dir .dss-backups/recipes --project-key MYPROJ
dss recipe run prepare_orders --dry-run --project-key MYPROJ
```

Recipe payload diffs normalize LF/CRLF. `set-payload` backs up payload, graph, settings, code-env,
and version metadata by default; use `restore --backup FILE --dry-run` before applying a rollback.
`recipe run --dry-run` performs live output resolution without creating a job; static
`--plan` reports `exact:false` rather than inventing that live payload.

Notebook saves are save-or-create operations. Read first, preserve the full notebook object, then
use the persisted hash from save/dry-run output with `--expect-hash`. Jupyter output clearing uses
the official DELETE endpoint. `unload-jupyter --all` is deliberately composed: list active
notebooks, list their sessions, then delete each session; DSS has no unload-all endpoint.

```bash
dss notebook get-jupyter analysis.ipynb --project-key MYPROJ
dss notebook save-jupyter analysis.ipynb --data-file notebook.json --dry-run --project-key MYPROJ
dss notebook save-jupyter analysis.ipynb --data-file notebook.json --expect-hash HASH --project-key MYPROJ
dss notebook clear-jupyter-outputs analysis.ipynb --dry-run --project-key MYPROJ
dss notebook unload-jupyter --all --dry-run --project-key MYPROJ
```

Code-environment reads expose definitions, deployment mode, installed/requested packages, project
versions, usages, and bounded build logs. Fetch the definition/hash before `set-definition` or
`set-packages --expect-hash`; package, image, Jupyter, create, and delete operations can return DSS
futures with `--no-wait`. `set-packages` replaces the requested package list wholesale: an
explicitly empty `--packages ''` value (or an empty `--file`) clears the requested specs, while a
call with no package source flag at all is a usage error.

Webapp updates use GET–deep-merge–PUT; API-service `save-settings` is explicit full replacement.
Run either with `--dry-run` to inspect `currentHash`/`nextHash`, then apply with
`--expect-hash`. Webapp restart futures support `--wait`. API packages and project bundles
forward release notes and explicit Deployer target IDs.

An agent plan with `exact:false` is an honesty boundary: live state is required and no endpoint or
payload was guessed. Use the command's own `--dry-run` when it resolves that state. The supported
public DSS API exposes no plugin-authoring lifecycle, notebook execution/checkpoint operation, or
webapp/API-service delete. Use project Git for plugin source, `code run`/recipes for execution,
and the authenticated DSS UI for unsupported lifecycle operations.

For fake-DSS smoke tests, return project lists as JSON arrays such as `[{ "projectKey": "MYPROJ", "name": "My Project" }]` from `/public/api/projects/`; recipe payload commands read `/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true` and expect a JSON object shaped like `{ "recipe": { "name": "<NAME>", "type": "python" }, "payload": "..." }`.

## License

This project is source-available under the [Dataiku SDK Limited Use License 1.0](LICENSE), not an open-source license.

- Individuals acting on their own behalf may use unmodified copies without a license fee.
- Companies with total annual gross revenue below US $1,000,000 may use unmodified copies without a license fee.
- Companies at or above that threshold, and all other entities, must request and obtain a separate written license before use.
- Every modification, including private or internal changes, and every form of redistribution requires prior written approval.
- Permitted use must preserve the license notices and display: `dataiku-sdk by clssck — https://github.com/clssck/dataiku-sdk`.

Copies previously received under another license retain the rights granted for those copies. See [LICENSE](LICENSE) for the controlling terms and request path.
