# Application releases

Release gates: validate → compare → version → successor → verify → permissions.
Manifest `version` is raw metadata, **not publishing**. New instances inherit it; existing instances never upgrade in place. Create/verify an additive successor, preserve the predecessor, retire separately. No private publish/recreate/rename, recipient-sharing, or UI-click APIs may be inferred.

```text
dss app validate-manifest --project-key APP_TEMPLATE
dss app compare-manifest APP_ID --project-key RELEASE_INSTANCE
dss app manifest-version --project-key APP_TEMPLATE
dss app successor-preflight APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions
dss app set-manifest-version --manifest-version 1.4.0 --expect-hash PREFLIGHT_TEMPLATE_MANIFEST_HASH --project-key APP_TEMPLATE
# targetProjectKey in instance.json must be confirmed absent
dss app create-instance APP_ID --data-file instance.json --wait --record-cleanup cleanup.jsonl
dss app create-successor-instance APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance APP_ID --project-key RELEASE_INSTANCE_V2 --expect-version 1.4.0
dss cleanup --file cleanup.jsonl
dss app permissions-snapshot --project-key RELEASE_INSTANCE --output permissions.json
dss app permissions-diff --project-key RELEASE_INSTANCE --file permissions.json
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json --dry-run
```

- Run `successor-preflight` **before** changing the version: non-mutating validation of template, predecessor, target, optional ACL snapshot; returns the template manifest hash for `set-manifest-version --expect-hash`.
- A masked `403`, even absent from both visible lists, fails as `target_absence_unverifiable` / `permission_or_environment`: lists prove collisions, not absence. DSS has no permission-independent public key-availability endpoint or guaranteed non-overwriting duplicate-key rejection. No force/server-atomic bypass; use global project visibility.
- Definitive rejection: no cleanup entry. Ambiguous POST without a future ID or verified incarnation: `INDETERMINATE`, no cleanup entry. Future-addressable cleanup waits for target identity and `creationTag`; never targets the predecessor.
- Ledgers bind canonical DSS URL, project key, concrete incarnation; reject legacy, mixed-server, mismatched-server, or unbound app entries. Static plans: `preflightExecuted:false`, `preflightWillRunDuringApply:true`.
- `set-manifest-version` reports `concurrencyControl:"client-side-non-atomic-stale-read-check"`: `--expect-hash` guards stale reads, **not a lock**; PUT is unconditional. Ambiguous writes: `outcome:"indeterminate"`.
- API keys authenticate public REST only. `verify-instance` reports `status:"API_VERIFIED_UI_PENDING"`; complete external SSO verification by exercising affected tiles, forms, actions. Permission snapshots bind identities/permissions to server/project incarnation and reject mismatches.
