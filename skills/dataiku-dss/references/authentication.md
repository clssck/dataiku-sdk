# Authentication and runtime

Invoke the installed `dss` command; do not call package scripts through Bun directly.

- Credentials: flags → `DATAIKU_*` variables → saved credentials. `DATAIKU_DISABLE_ENV=1` ignores both `.env` and `DATAIKU_*`.
- `.env`: invocation directory, then checkout root or package `dist/`; only `DATAIKU_*` and the two `NODE_*` TLS keys are read. Any already-set variable wins, even `""`.
- A URL/TLS from the invocation `.env` requires its API key from the same `.env` (else `conflicting_input_sources`).
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
