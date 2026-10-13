# Scenario notifications, messaging channels, test reports

## Email notification for a scenario, end to end

1. Find a mail channel: `dss messaging-channel list --family mail` (compact: `id`, `type`, `family`). Empty: there is none; an admin creates one (step 5).
2. `dss messaging-channel get CHANNEL_ID` shows its sender. Credentials (password, secret keys, webhook URLs, tokens) are always `[redacted]` in output, plans, and errors.
3. Attach it: `dss scenario reporter-add SCENARIO --channel CHANNEL_ID --recipient a@x.com,b@x.com --subject 'Scenario ${scenarioName}: ${outcome}' --message 'Finished with ${outcome}' --dry-run`, then apply without `--dry-run`. `--phase START|END` (default END), `--condition EXPR` limits when it fires. Other channel types (Slack, Teams, ...): pass the reporter JSON with `--data-file`.
4. Check: `dss scenario reporter-list SCENARIO`. `dss messaging-channel permissions-get CHANNEL_ID` shows which users and groups may use the channel.
5. Admin only, creating a channel: `dss messaging-channel create --type smtp --id mail-main --data-file smtp.json --dry-run` (`--type` smtp, aws-ses-mail, microsoft-graph-mail, slack, msteams, google-chat, twilio, shell; `--data` is its configuration; `--permissions '[{"group":"analysts","canUse":true}]'`, every user when omitted). Record it for cleanup with `--record-cleanup`.

Changing a channel: `dss messaging-channel settings-set ID --data '{"sender":"ops@x.com"}' --dry-run` merges into the stored settings; omit credentials you do not change. A `[redacted]` placeholder pasted back is refused. For concurrent edits pass `--expect-hash` with the `settingsHash` from `settings-get` (it covers non-credential fields only).

Send mail directly (external side effect, cannot be recalled; always `--dry-run` first, project needs Write content): `dss messaging-channel send CHANNEL_ID --to a@x.com,b@x.com --subject S --body-file body.html --attachment report.csv --project-key PROJ`. HTML unless `--plain-text`; one mail per `--to` address unless `--single-message`; `--attachment` takes comma-separated or repeated paths.

## Test scenarios

- `dss project testing-status [--bundle-id ID]`: worst outcome across the last runs of the project's test scenarios (scenarios with `markedAsTest`).
- `dss project test-report [--bundle-id ID]`: JUnit XML of the last test runs, in `report`; `--output PATH` writes the file instead; `--format html --output PATH` writes the HTML page.
- Project Deployer: `dss project-deployer testing-status DEPLOYMENT [--bundle-id ID] [--automation-node-id ID]`; `dss project-deployer run-test-scenarios DEPLOYMENT --dry-run` previews running the tests on the Automation node (blocks until they finish: raise `--request-timeout`). `--automation-node-id` is only for multi-node deployments.
- `dss scenario run-report SCENARIO RUNID [--step-id ID] [--output PATH]`: JUnit XML of one run of a test scenario (non-test scenarios: 400).

## Scenario setup and history

- Triggers: `dss scenario trigger-add SCENARIO --frequency minutely|hourly|daily|weekly|monthly [--every N --hour H --minute M --days Monday,Friday --day-of-month D --run-on ON_THE_DAY|LAST_DAY_OF_THE_MONTH|FIRST_WEEK..LAST_WEEK --start-date YYYY-MM-DD --timezone TZ --name N]`, or any trigger type with `--data`. `trigger-list` gives the 0-based `index` for `trigger-remove --index N` (or `--name X`). Each add/remove rewrites the whole list, keeping the others, and verifies it; `--dry-run` previews. Triggers fire only while the scenario is active (`scenario active-set SCENARIO true`).
- Reporters: `reporter-list`, `reporter-remove --index N|--name X` as above; `--full` shows DSS objects with credentials redacted.
- Run as: `dss scenario run-as-set SCENARIO --user LOGIN` (administrators) or `--clear` (runs as the last editor); the result's `effectiveRunAs` is the user DSS will use; `scenario status` shows `runAsUser`.
- Create with settings: `dss scenario create ID NAME --data '{"triggers":[...],"reporters":[...],"markedAsTest":true}'`.
- Run: `scenario run ID --params '{"region":"EU"}'` (or `--params-file`). Without `--wait` it returns a trigger run id (`triggerRunId`, not a run id); `scenario get-run-for-trigger ID TRIGGERRUNID` returns `state` PENDING, RUNNING, FINISHED (`runId`, `outcome`) or CANCELLED (DSS refused the fire because a run was in progress: wait or `scenario abort`, then run again). `run-and-wait` / `run --wait` end CANCELLED, exit 4, in that case.
- History: `scenario last-runs ID --only-finished --limit 1` is the last finished run (`outcome`); `scenario runs-by-date ID --from 2026-10-01 --to 2026-11-01` (`--to` exclusive; default tomorrow). `dss future list --scenarios-only` lists running scenario futures.
