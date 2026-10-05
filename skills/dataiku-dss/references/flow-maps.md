# Flow maps

```text
dss project map --render mermaid --project-key MYPROJ
dss flow-zone plan --project-key MYPROJ > flow-zones.json
dss flow-zone organize --file flow-zones.json --dry-run --project-key MYPROJ
dss flow-zone organize --file flow-zones.json --project-key MYPROJ
```

`project map`: zones, SCC-safe layers, weak components, diagnostics, fingerprint; rendering in `rendering.content`.

Feed `flow-zone plan` (`unassigned`: to place) to `flow-zone organize`; dry-run first. Recipe outputs follow their recipe. DSS positions a zone only at creation; `--recreate-on-move` moves it (new id). Layout commands never read recipe code.
