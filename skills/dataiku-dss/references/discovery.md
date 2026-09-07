# Discovery and output contracts

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

`--fields` takes a comma-separated list, so request several actions in one call. Each key is echoed exactly as requested (`--fields dataset.create` returns `{"dataset.create": {...}}`); an empty `--fields` is a usage error, never a silent full dump. The full registry is exported only via `--output PATH`: `dss commands run --output PATH` writes the registry, or the selected `--fields` subset, as compact JSON to `PATH`, and stdout carries `{"path":"PATH"}` — never the registry itself. An unknown resource or action exits with code 1 and a compact JSON error object on stdout containing the valid options.
