# Application releases

Snapshot sharing → validate → compare → version → successor → verify access.
Manifest `version` is raw metadata, **not publishing**. New instances inherit it; existing instances never upgrade in place. Create/verify an additive successor, preserve the predecessor, retire separately. No private publish/recreate/rename, recipient-sharing, or UI-click APIs may be inferred.

```text
dss app validate-manifest --project-key APP_TEMPLATE
dss app compare-manifest APP_ID --project-key RELEASE_INSTANCE
dss app manifest-version --project-key APP_TEMPLATE
dss app successor-preflight APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions
dss app permissions-snapshot --project-key RELEASE_INSTANCE --output permissions.json
dss app set-manifest-version --manifest-version 1.4.0 --expect-hash PREFLIGHT_TEMPLATE_MANIFEST_HASH --project-key APP_TEMPLATE
dss app create-instance APP_ID --data-file instance.json --wait --record-cleanup cleanup.jsonl
dss app create-successor-instance APP_ID --from RELEASE_INSTANCE --to RELEASE_INSTANCE_V2 --copy-permissions --record-cleanup cleanup.jsonl
dss app verify-instance APP_ID --project-key RELEASE_INSTANCE_V2 --expect-version 1.4.0
dss app permissions-diff --project-key RELEASE_INSTANCE --file permissions.json
dss app permissions-restore --project-key RELEASE_INSTANCE --file permissions.json --dry-run
```

- Run `successor-preflight` **before** changing the version: validate template, predecessor, optional ACLs, and an explicit target's absence; returns the template manifest hash for `set-manifest-version --expect-hash`. Omitting `--to` skips only target-absence checks, not source/template checks or apply-time target identity verification.
- Omit `targetProjectKey` (create) or `--to` (successor) for a cryptographically random key, no visibility probes; preserve the name (defaults to key). Preflight/plan allocate nothing and omit `target.projectKey` — generated successor results carry `targetProjectKeyGeneratedDuringApply` and `targetPreflight:"not-applicable-generated-key"`; apply generates the key once before its single POST and binds cleanup through the future target plus `creationTag`. Collision risk is negligible, not zero.
- Explicit keys/`--to` require confirmed absence. Masked `403` → `target_absence_unverifiable` / `permission_or_environment`, even if unlisted. No public availability/atomic duplicate guarantee or force bypass; exact keys need global visibility.
- Definitive rejection: no cleanup entry. Ambiguous POST without a future ID or verified incarnation: `INDETERMINATE`, no cleanup entry. Future-addressable cleanup waits for target identity and `creationTag`; never targets the predecessor.
- Ledgers bind canonical DSS URL, project key, concrete incarnation; reject legacy, mixed-server, mismatched-server, or unbound app entries. Explicit-key plans: `preflightExecuted:false`, `preflightWillRunDuringApply:true`. Cleanup is rollback, not a release step: preview `dss cleanup --file cleanup.jsonl` and run it with `--apply` only to undo a confirmed failed or abandoned creation.
- `set-manifest-version` reports `concurrencyControl:"client-side-non-atomic-stale-read-check"`: `--expect-hash` guards stale reads, **not a lock**; PUT is unconditional. Ambiguous writes: `outcome:"indeterminate"`.
- People absent from the originator flow can lose sharing across a template update, recreation, or successor. Snapshot live `user`/`group` access before, `permissions-diff` after, then restore approved missing access without removing new grants — `permissions-restore` is SAME-instance only and PUTs the whole object: preview and reconcile intervening changes first.
- API keys authenticate public REST only. `verify-instance` reports `status:"API_VERIFIED_UI_PENDING"`; complete external SSO verification by exercising affected tiles, forms, actions. Permission snapshots bind to server and project incarnation, so a predecessor snapshot cannot be restored onto a successor — copy permissions at creation (`--copy-permissions`), compare predecessor/successor rules, and verify the intended identity's access before retiring the predecessor.
