# Authentication and runtime

No installed `dss`? In the checkout, use `bun --no-env-file src/cli.ts ...` or `bun --no-env-file ./bin/dss.js ...`; elsewhere, pass Bun the absolute `bin/dss.js` path.

- `--no-env-file` disables only Bun preloading, not CLI `.env` handling.
- Credentials: flags → `DATAIKU_*` variables → saved credentials. `DATAIKU_DISABLE_ENV=1` ignores both `.env` and `DATAIKU_*`.
- CLI `.env` lookup: invocation directory, then CLI root; invocation values win. Put test `.env` files where you invoke `dss`.
- For disposable tests, set `DSS_CONFIG_DIR` to a temporary directory to isolate saved credentials.

Prefer environment variables for ephemeral runs; match the shell:

POSIX:
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

Command Prompt:
```bat
set "DATAIKU_URL=https://dss.example.com"
set "DATAIKU_API_KEY=your-api-key"
set "DATAIKU_PROJECT_KEY=MYPROJ"
```

Persist credentials:
```bash
dss auth login --url https://dss.example.com --api-key YOUR_KEY --project-key MYPROJ
```
`auth login` lists accessible projects before saving: the key needs project-list permission. Returns `{"saved":true,"path":"..."}`. Storage precedence: `DSS_CONFIG_DIR`, `XDG_CONFIG_HOME/dataiku/credentials.json`, `APPDATA/dataiku/credentials.json` on Windows, then `~/.config/dataiku/credentials.json`.

TLS: `--insecure` disables verification; `--ca-cert PATH` adds a PEM CA bundle. Environment equivalents: `NODE_TLS_REJECT_UNAUTHORIZED`, `NODE_EXTRA_CA_CERTS`.
