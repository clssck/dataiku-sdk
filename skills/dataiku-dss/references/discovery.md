# Discovery and output

## Contract

- Stdout: exactly one compact JSON value; void success: `{ok:true}`. No prompts, help screens, tables, banners, or prose.
- Dispatch/runtime failure: stdout error object (`type:"error"`, `ok:false`, `error`, `code`, `category`, `exitCode`), nonzero exit. `doctor`/`batch`/`cleanup` failures instead return direct result objects; inspect exit code and fields.
- Stderr: JSONL diagnostics only; `type:"warning"` / `type:"trace"` for warnings / `--verbose` HTTP traces, flushed before success or failure output.
- Exits: 0 success, 1 usage/configuration, 2 DSS/internal/permission-or-environment, 3 transient/retryable DSS error, 4 failed long-running result/assertion.
- Recipe payload stdout: JSON string. With `--output PATH`: exact bytes to file, JSON string equal to `PATH` on stdout.
- General `--fields a,b,c`: object projection, element-wise for object arrays. Dotted paths traverse nested objects; missing fields become `null`; strings/scalars pass through.
- `*.list`: compact items (ids, kind, next-step fields such as recipe `inputs`/`outputs`); `--full`/`--fields` use DSS objects. `--contains TEXT` filters ids; `--limit N` caps (`list_truncated` warning).

## Discovery

```text
dss commands run
dss commands run --contains "propagate schema"
dss agent contract --fields commands.actions.dataset
dss commands run --fields dataset.create
dss commands run --fields dataset.preview.usage,dataset.preview.description,dataset.preview.flags,dataset.preview.examples
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
dss agent contract --fields commands.actions
```

`commands run`: resource→action summary (~1.6k tokens). Unknown name: `--contains TEXT` gives the top 5 actions with usage. Known resource: `agent contract --fields commands.actions.RESOURCE`. Bootstrap once with the six-field projection (~250 tokens). Global flags (`--fields`, `--verbose`, connection/TLS) appear once in `commands.globalFlags`, not per entry.

Look up syntax before invoking: `RESOURCE.ACTION` is one entry, `.FIELD` nests, commas batch, keys echo selectors. Reads: `usage,description,flags,examples`. Writes: the full entry once (flags, side effects, idempotency, dry-run, payload schemas, cleanup, exits). Unknown flags, resources, and actions exit 1 with the usage line or valid options. Full registry: `commands run --output PATH` only; stdout is `{"path":"PATH"}`.
