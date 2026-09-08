# Flow maps

```text
dss project map --render mermaid --project-key MYPROJ
dss flow-zone plan --project-key MYPROJ > flow-zones.json
dss flow-zone organize --file flow-zones.json --dry-run --project-key MYPROJ
dss flow-zone organize --file flow-zones.json --project-key MYPROJ
```

`project map`: zones, SCC-safe layers, weak components, diagnostics, full-flow fingerprint; rendering in `rendering.content`. DSS exposes zone positions, not node pixels.

Feed `flow-zone plan` to `flow-zone organize`; inspect dry-run first. Organize checks topology before/after writes and skips unchanged moves. Audit recipe payloads separately: layout commands neither fetch nor analyze code.
