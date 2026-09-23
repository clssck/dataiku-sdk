# Authentication and runtime

Invoke the installed `dss` command; do not call package scripts through Bun directly.

- Credentials: flags → `DATAIKU_*` variables → saved credentials. `DATAIKU_DISABLE_ENV=1` ignores both `.env` and `DATAIKU_*`.
- `.env` lookup: invocation directory, then checkout root (source) or package `dist/` (built). Only `DATAIKU_*`, `NODE_TLS_REJECT_UNAUTHORIZED`, and `NODE_EXTRA_CA_CERTS` are read; other keys are ignored. Nonempty environment wins, then invocation values. Prefer invocation-local `.env`.
- If the invocation `.env` supplies the URL or TLS settings, the API key must come from it too; mixing with `--api-key` or another source fails with `conflicting_input_sources`.
- For disposable tests, set `DSS_CONFIG_DIR` to a temporary directory to isolate saved credentials.

Prefer injected environment variables; never type real keys into shell history. Placeholder examples:

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

Persist the configured environment credentials:
```bash
dss auth login
```
`auth login` lists projects before saving: the key needs project-list permission. Returns `{"saved":true,"path":"..."}`. Storage: `credentials.json` in `DSS_CONFIG_DIR`, else `XDG_CONFIG_HOME/dataiku`, Windows `APPDATA/dataiku`, or `~/.config/dataiku`.

TLS: `--insecure` disables verification; `--ca-cert PATH` adds a PEM CA bundle. Environment equivalents: `NODE_TLS_REJECT_UNAUTHORIZED`, `NODE_EXTRA_CA_CERTS`.
