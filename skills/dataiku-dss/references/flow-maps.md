# Flow maps

## Flow maps and visual organization

```text
dss project map --render mermaid --project-key MYPROJ
dss flow-zone plan --project-key MYPROJ > flow-zones.json
dss flow-zone organize --file flow-zones.json --dry-run --project-key MYPROJ
dss flow-zone organize --file flow-zones.json --project-key MYPROJ
```

`project map` returns zones, SCC-safe layers, weak components, diagnostics, and a full-flow
fingerprint; rendering stays in `rendering.content`. DSS exposes zone positions only, never node
pixel coordinates.

`flow-zone plan` output feeds `flow-zone organize`; inspect the dry-run. Organize checks topology
before and after writes and skips unchanged moves. Audit recipe payloads separately; layout
commands never fetch or analyze code.
