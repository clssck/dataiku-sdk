# Application releases

## Application release safety

Treat app release as explicit validate, compare, version, successor, verify, and permission gates.
The public app-manifest `version` field is raw metadata: writing it is NOT a publish transaction.
New instances inherit the template's raw `version`; existing instances are never upgraded in
place — create an additive successor (the old instance is preserved), verify it, retire it
separately. Never infer private publish/recreate/rename, recipient-sharing, or UI-click operations
the public DSS API lacks.

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

Run `successor-preflight` before changing the template version. It validates the template,
predecessor, target, and optional ACL snapshot, performs no mutation, and returns the template
manifest hash for the next `set-manifest-version --expect-hash` guard.

A masked `403` absent from both visible lists is rejected as
`target_absence_unverifiable` / `permission_or_environment`; lists prove collisions, not absence.
DSS exposes no permission-independent public key-availability endpoint or guaranteed
non-overwriting duplicate-key rejection, so no force or server-atomic bypass is supported; use
global project visibility. A definitive rejection writes no cleanup entry. An ambiguous POST
without a returned future ID or verified incarnation is `INDETERMINATE` and also produces no cleanup
entry. Future-addressable cleanup waits for target identity and `creationTag`; the predecessor is
never targeted. Ledgers bind the canonical DSS URL, project key, and concrete project incarnation,
rejecting legacy, mixed-server, mismatched-server, or unbound app cleanup entries. Static plans
expose `preflightExecuted:false` and `preflightWillRunDuringApply:true`.

`app set-manifest-version` reports
`concurrencyControl:"client-side-non-atomic-stale-read-check"`. `--expect-hash` is a stale-read
guard; the PUT is unconditional. Never treat the hash as a serializing lock; ambiguous writes
report `outcome:"indeterminate"`.

The API key authenticates public REST only. `app verify-instance` reports
`status:"API_VERIFIED_UI_PENDING"`; its external SSO gate requires exercising the affected tiles,
forms, and actions. Permission snapshots bind identity and permissions to the server/project
incarnation and reject mismatches.
