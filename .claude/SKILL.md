# Dataiku DSS SDK

CLI and TypeScript SDK for the Dataiku DSS REST API. No build step required.

## Setup

Set environment variables:
```
DATAIKU_URL=https://dss.example.com
DATAIKU_API_KEY=your-api-key
DATAIKU_PROJECT_KEY=YOUR_PROJECT   # optional default
```

Environment variables can also be set in a `.env` file in the SDK directory or current working directory. The CLI auto-loads `.env` on startup (existing env vars take precedence).

## Invocation

Run directly from any directory — no install, no build:
```bash
~/shared/dataiku-sdk/bin/dss <resource> <action> [args...] [--flags]
```

Or add an alias:
```bash
alias dss="~/shared/dataiku-sdk/bin/dss"
```

Output is always JSON to stdout. Errors go to stderr as JSON with `error`, `category`, `retryable`, `retryHint` fields.

## Resources and Actions

### project
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss project list` | List all projects |
| get | `dss project get [--project-key PK]` | Get project details |
| metadata | `dss project metadata [--project-key PK]` | Get project metadata |
| flow | `dss project flow [--project-key PK]` | Get raw flow graph |
| map | `dss project map [--max-nodes N] [--max-edges N] [--include-raw]` | Normalized flow map with truncation |

### dataset
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss dataset list` | List datasets in project |
| get | `dss dataset get <name>` | Get dataset definition |
| schema | `dss dataset schema <name>` | Get dataset schema (columns/types) |
| preview | `dss dataset preview <name> [--max-rows N]` | Preview data as CSV (default 50 rows) |
| metadata | `dss dataset metadata <name>` | Get dataset metadata |
| download | `dss dataset download <name> [--output PATH]` | Download dataset to gzipped CSV |
| create | `dss dataset create --name NAME --connection CONN [--type TYPE]` | Create a dataset |
| delete | `dss dataset delete <name>` | Delete a dataset |
| update | `dss dataset update <name> --data '{...}'` | Update dataset definition (merge) |

### recipe
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss recipe list` | List recipes in project |
| get | `dss recipe get <name> [--include-payload] [--payload-max-lines N]` | Get recipe definition |
| delete | `dss recipe delete <name>` | Delete a recipe |
| download | `dss recipe download <name> [--output PATH]` | Download recipe payload to file |
| create | `dss recipe create --type TYPE --input DS [--output DS] [--output-connection CONN]` | Create a recipe |
| update | `dss recipe update <name> --data '{...}'` | Update recipe definition (merge) |

### job
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss job list` | List jobs in project |
| get | `dss job get <id>` | Get job details |
| log | `dss job log <id> [--activity NAME] [--max-lines N]` | Get job log (tail N lines) |
| build | `dss job build <dataset> [--build-mode MODE]` | Start a build job |
| build-and-wait | `dss job build-and-wait <dataset> [--build-mode MODE] [--include-logs] [--timeout MS]` | Build and poll until complete |
| wait | `dss job wait <id> [--include-logs] [--timeout MS]` | Wait for job to finish |
| abort | `dss job abort <id>` | Abort a running job |

Build modes: `RECURSIVE_BUILD`, `NON_RECURSIVE_FORCED_BUILD`, `RECURSIVE_FORCED_BUILD`, `RECURSIVE_MISSING_ONLY_BUILD`

### scenario
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss scenario list` | List scenarios |
| get | `dss scenario get <id>` | Get scenario definition |
| run | `dss scenario run <id>` | Trigger a scenario run |
| status | `dss scenario status <id>` | Get scenario status |
| delete | `dss scenario delete <id>` | Delete a scenario |
| create | `dss scenario create <id> <name> [--type step_based\|custom_python]` | Create a scenario |
| update | `dss scenario update <id> --data '{...}'` | Update scenario definition (merge) |

### folder
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss folder list` | List managed folders |
| get | `dss folder get <id>` | Get folder details |
| contents | `dss folder contents <id>` | List folder contents |
| download | `dss folder download <id> <path> [--output PATH]` | Download a file from folder |
| upload | `dss folder upload <id> <path> <localPath>` | Upload a file to folder |
| delete-file | `dss folder delete-file <id> <path>` | Delete a file from folder |

### variable
| Action | Command | Description |
|--------|---------|-------------|
| get | `dss variable get` | Get project variables |
| set | `dss variable set --standard '{"key":"val"}' --local '{"key":"val"}'` | Set project variables (merge) |

### connection
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss connection list` | List connection names |
| infer | `dss connection infer [--mode fast\|rich]` | Infer connection details |

### code-env
| Action | Command | Description |
|--------|---------|-------------|
| list | `dss code-env list [--lang PYTHON\|R]` | List code environments |
| get | `dss code-env get <lang> <name>` | Get code environment details |


### sql
| Action | Command | Description |
|--------|---------|-------------|
| query | `dss sql query --sql 'SELECT ...' [--connection CONN] [--dataset FULL_NAME] [--database DB]` | Execute SQL query end-to-end |

### notebook
| Action | Command | Description |
|--------|---------|-------------|
| list-jupyter | `dss notebook list-jupyter` | List Jupyter notebooks |
| get-jupyter | `dss notebook get-jupyter <name>` | Get Jupyter notebook content |
| save-jupyter | `dss notebook save-jupyter <name> --data '{...}'` | Save Jupyter notebook content |
| delete-jupyter | `dss notebook delete-jupyter <name>` | Delete a Jupyter notebook |
| clear-jupyter-outputs | `dss notebook clear-jupyter-outputs <name>` | Clear cell outputs |
| sessions-jupyter | `dss notebook sessions-jupyter <name>` | List kernel sessions |
| unload-jupyter | `dss notebook unload-jupyter <name> <sessionId>` | Stop a kernel session |
| list-sql | `dss notebook list-sql` | List SQL notebooks |
| get-sql | `dss notebook get-sql <id>` | Get SQL notebook content |
| save-sql | `dss notebook save-sql <id> --data '{...}'` | Save SQL notebook content |
| delete-sql | `dss notebook delete-sql <id>` | Delete a SQL notebook |
| history-sql | `dss notebook history-sql <id>` | Get SQL execution history |
| clear-sql-history | `dss notebook clear-sql-history <id> [--cell-id CID] [--retain N]` | Clear SQL execution history |
## Global Flags

All commands accept:
- `--url URL` - Override DATAIKU_URL
- `--api-key KEY` - Override DATAIKU_API_KEY
- `--project-key KEY` - Override DATAIKU_PROJECT_KEY

## Common Workflows

**Inspect a project's data pipeline:**
```bash
dss project map --project-key MYPROJ
```

**Preview dataset contents:**
```bash
dss dataset preview my_dataset --max-rows 10
```

**Build a dataset and wait for completion:**
```bash
dss job build-and-wait my_dataset --build-mode NON_RECURSIVE_FORCED_BUILD --include-logs
```

**Run a scenario:**
```bash
dss scenario run my_scenario
dss scenario status my_scenario
```

## Programmatic Usage (TypeScript)

```typescript
import { DataikuClient } from "dataiku-sdk";

const client = new DataikuClient({
  url: "https://dss.example.com",
  apiKey: "your-api-key",
  projectKey: "MY_PROJECT",
});

const projects = await client.projects.list();
const schema = await client.datasets.schema("my_dataset");
const result = await client.jobs.buildAndWait("my_dataset", {
  buildMode: "NON_RECURSIVE_FORCED_BUILD",
  includeLogs: true,
});
```
