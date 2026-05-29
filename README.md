# dataiku-sdk

Agent-only TypeScript SDK and `dss` CLI for Dataiku DSS automation.

Examples below assume the installed `dss` binary. From this checkout, use `./bin/dss ...` or `bun --no-env-file src/cli.ts ...` with the same arguments; from another working directory, call `/path/to/dataiku-sdk/bin/dss ...`.
`--no-env-file` disables Bun's automatic preloading only; the CLI still applies its documented `.env` handling unless `DATAIKU_DISABLE_ENV=1` is set.

## CLI contract

- Success: stdout contains one JSON result.
- Failure: stderr contains one JSON error envelope with `ok:false`, `error`, `code`, and `exitCode`.
- `--verbose` may add HTTP trace lines to stderr.
- `--fields a,b,c` projects those top-level fields from an object or array-of-objects result (missing fields become `null`); string and scalar results pass through unchanged.
- No prompts, help screens, tables, banners, or prose output are part of the contract.
- Exit codes: `0` success, `1` usage/configuration error, `2` DSS/internal error, `3` transient DSS error, `4` completed command with failed long-running DSS work.
- `--raw` is only for recipe payload commands. Without `--output`, stdout is raw bytes; with `--output PATH`, stdout is the JSON string equal to `PATH` and the file contains the exact raw bytes.

Discover the complete machine-readable command surface:

```bash
dss commands run
```

Agents should parse `commands run` before choosing syntax; inspect `flags`, `requiresAuth`, `requiresProject`, `sideEffect`, and `outputShape` instead of guessing.

## Agent skill installation

```bash
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

Use environment variables for ephemeral runs:
For disposable agent tests, set `DSS_CONFIG_DIR` to a temporary directory so saved credentials never touch your real profile.
Credential precedence is flags first, then `DATAIKU_*` environment variables, then saved credentials in `DSS_CONFIG_DIR` or the platform config directory.
Set `DATAIKU_DISABLE_ENV=1` when a test must ignore both `.env` files and `DATAIKU_*` environment variables.
When `.env` loading is enabled, the CLI reads `.env` from the CLI build/root directory and from the command's current working directory; put test-specific `.env` files in the directory where you invoke `dss`.

```bash
DATAIKU_URL=https://dss.example.com \
DATAIKU_API_KEY=your-api-key \
DATAIKU_PROJECT_KEY=MYPROJ \
dss project list
```

Persist credentials when needed:

```bash
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
