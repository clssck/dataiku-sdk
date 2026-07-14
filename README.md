# dataiku-sdk

Agent-only TypeScript SDK and `dss` CLI for Dataiku DSS automation.

## Platform support

The published `dss` CLI supports Linux, macOS, and Windows under Bun >= 1.3.14 or Node.js >= 22.15.0. CI exercises Bun 1.4 and Node 22.15/24 on all three operating systems. The runtime dependency is pure JavaScript, so the same package also runs on runtime-supported x64 and ARM64 systems.

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

- Success: stdout contains one JSON result.
- Failure: stderr contains one JSONL error event with `type:"error"`, `ok:false`, `error`, `code`, and `exitCode`.
- Non-fatal diagnostics and `--verbose` traces are JSONL stderr events (`type:"warning"` / `type:"trace"`); no prose trace lines are part of the contract.
- `--fields a,b,c` projects those fields from an object or array-of-objects result; dotted paths (`a.b.c`) drill into nested objects, and missing fields become `null`; string and scalar results pass through unchanged.
- No prompts, help screens, tables, banners, or prose output are part of the contract.
- Exit codes: `0` success, `1` usage/configuration error, `2` DSS/internal error, `3` transient DSS error, `4` completed command with failed long-running DSS work.
- The exit code is the success signal. For portable multi-step mutations, prefer `dss batch`; shell chaining and pipeline exit semantics differ across POSIX shells, Windows PowerShell, PowerShell 7, and Command Prompt.
- `--raw` is only for recipe payload commands. Without `--output`, stdout is raw bytes; with `--output PATH`, stdout is the JSON string equal to `PATH` and the file contains the exact raw bytes.

Discover the agent protocol and complete machine-readable command surface:

```text
dss agent contract
dss commands run
```

Agents should parse `agent contract` once for protocol/schema compatibility, then parse `commands run` before choosing syntax; inspect `flags`, `structuredExamples`, `schemas`, `requiresAuth`, `requiresProject`, `sideEffect`, `unsafeOutputs`, and `outputShape` instead of guessing.

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
dss dataset list --project-key MYPROJ
dss recipe get-payload compute_orders --project-key MYPROJ
dss recipe get-payload compute_orders --raw --project-key MYPROJ
dss recipe get-payload compute_orders --raw --output code.py --project-key MYPROJ
dss install-skill --dry-run
```

For fake-DSS smoke tests, return project lists as JSON arrays such as `[{ "projectKey": "MYPROJ", "name": "My Project" }]` from `/public/api/projects/`; recipe payload commands read `/public/api/projects/<PROJECT>/recipes/<NAME>?includePayload=true` and expect a JSON object shaped like `{ "recipe": { "name": "<NAME>", "type": "python" }, "payload": "..." }`.
