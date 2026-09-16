# Discovery and output

## Contract

- Stdout: exactly one compact JSON value; void success: `{ok:true}`. No prompts, help screens, tables, banners, or prose.
- Dispatch/runtime failure: stdout error object (`type:"error"`, `ok:false`, `error`, `code`, `category`, `exitCode`), nonzero exit. `doctor`/`batch`/`cleanup` failures instead return direct result objects; inspect exit code and fields.
- Stderr: JSONL diagnostics only; `type:"warning"` / `type:"trace"` for warnings / `--verbose` HTTP traces, flushed before success or failure output.
- Exits: 0 success, 1 usage/configuration, 2 DSS/internal, 3 transient/retryable DSS error, 4 failed long-running result/assertion.
- Recipe payload stdout: JSON string. With `--output PATH`: exact bytes to file, JSON string equal to `PATH` on stdout.
- General `--fields a,b,c`: object projection, element-wise for object arrays. Dotted paths traverse nested objects; missing fields become `null`; strings/scalars pass through.

## Discovery

```text
dss commands run
dss agent contract --fields commands.actions.dataset
dss commands run --fields dataset.create
dss commands run --fields dataset.preview.usage,dataset.preview.description,dataset.preview.flags,dataset.preview.examples
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
dss agent contract --fields commands.actions
```

`commands run` defaults to a resource→action-name summary (~1.6k tokens), never full entries. For a known resource, project `commands.actions.RESOURCE` from `agent contract` (~64 tokens for dataset). Bootstrap once with the six-field projection above (~250 tokens); fetch `schemas` or `commands` only as needed.

Look up syntax before invoking. `--fields RESOURCE` selects every full entry (potentially large); `RESOURCE.ACTION` one complete entry; append `.FIELD` for nested metadata. Reads: prefer `usage,description,flags,examples`. Writes: fetch the full entry directly once, not compact-then-full. It includes flags, positionals, side effects, auth, output, idempotency, dry-run, examples, payload schemas, unsafe outputs, cleanup, and exits.

Comma-separate selectors to batch lookups. Keys echo selectors: `--fields dataset.create` → `{"dataset.create":{...}}`. Empty `--fields` fails usage validation, never dumps everything. Unknown resources/actions exit 1 with compact JSON errors listing valid options.

Full registry: `commands run --output PATH` only. With `--fields`, export that subset instead. Files contain compact JSON; stdout is `{"path":"PATH"}`, never registry content.
