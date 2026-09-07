# Authentication and runtime setup

If the installed `dss` binary is unavailable but the checkout is the workspace, prefer `bun --no-env-file src/cli.ts ...` or `bun --no-env-file ./bin/dss.js ...`; from another working directory, pass the checkout's absolute `bin/dss.js` path to Bun.
`--no-env-file` disables Bun's automatic preloading only; the CLI still applies its documented `.env` handling unless `DATAIKU_DISABLE_ENV=1` is set.

Credential lookup order is flags first, then `DATAIKU_*` environment variables, then saved credentials.
Set `DATAIKU_DISABLE_ENV=1` to ignore both `.env` files and `DATAIKU_*` variables.
With `.env` loading enabled, the CLI reads `.env` from the command's current working directory first, then the CLI root; the invocation directory wins on conflicts, so put test-specific `.env` files where you invoke `dss`.
For disposable agent tests, set `DSS_CONFIG_DIR` to a temporary directory so saved credentials never touch the real profile.

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
