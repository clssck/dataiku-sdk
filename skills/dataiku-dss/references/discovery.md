# Discovery and output

## Contract

- Stdout: exactly one JSON value (line-wrapped past 760 bytes; `DSS_PRETTY=0` forces one line); void success: `{ok:true}`. No prompts, help screens, tables, banners, or prose.
- Dispatch/runtime failure: stdout error object (`type:"error"`, `error`, `code`, `category`, `exitCode`; `projectKey` when resolved), nonzero exit. `doctor`/`batch`/`cleanup` failures instead return direct result objects; inspect exit code and fields.
- Stderr: JSONL diagnostics only; `type:"warning"` / `type:"trace"` for warnings / `--verbose` HTTP traces, flushed before success or failure output.
- Exits: 0 success, 1 usage/configuration, 2 DSS/internal/permission-or-environment, 3 transient/retryable DSS error, 4 failed long-running result/assertion.
- Recipe payload stdout: JSON string. With `--output PATH`: exact bytes to file, JSON string equal to `PATH` on stdout.
- General `--fields a,b,c`: object projection, element-wise for object arrays. Dotted paths traverse nested objects; missing fields become `null`; strings/scalars pass through.
- Compact by default: `*.list` (ids, kind, next-step fields such as recipe `inputs`/`outputs`), `job get` (no log tail), `dataset get`/`info`, `project get`, `scenario get-run`/`last-runs`; `--full`/`--fields` use DSS objects. Lists: `--contains TEXT` filters ids; `--limit N` caps (`list_truncated` warning).
- Update `--dry-run`: `changes` (`path`, `before`, `after`; long text as a `diff`), `changeCount`; `[]` means no-op. `--fields current,next` returns the objects.

## Discovery

```text
dss commands run
dss commands run --contains "propagate schema"
dss agent contract --fields commands.actions.dataset
dss commands run --fields dataset.create,dataset.update
dss commands run --fields dataset.create --full
dss commands run --output commands.json
dss agent contract --fields protocol,agentContractVersion,cli,stdio,planning,compatibility
dss agent contract --fields commands.actions
```

`commands run`: resource→action summary (~1.6k tokens). Unknown name: `--contains TEXT` gives the top 5 actions with usage. Known resource: `agent contract --fields commands.actions.RESOURCE`. Global flags (`--fields`, `--verbose`, connection/TLS) appear once in `commands.globalFlags`, not per entry.

`RESOURCE.ACTION` is one compact entry: `usage`, `description`, `examples`, `sideEffect`, the JSON-body contract (`inputContract`, `schemas.input`), and only the non-default facts (`destructive`, `dryRun:false`, `idempotency`, `async`, `producesLocalFile`, `unsafeOutputs`, cleanup). `--full` or `.FIELD` (`.flags`, `.schemas.argv`) gives the rest; commas batch; keys echo selectors. Unknown selectors among known ones are skipped (`unknown_fields` warning with `suggestions`). Unknown flags, resources, and actions exit 1 with the usage line or valid options. Full registry: `commands run --output PATH` only; stdout is `{"path":"PATH"}`.
