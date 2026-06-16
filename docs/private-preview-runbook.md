# Private Preview Runbook

UltraSQL private preview is for controlled evidence gathering, not marketing.
It should invite a small group of operators and auditors to test a pinned
release candidate under explicit pre-production terms.

## Entry Criteria

Start private preview only when the maintainer can provide:

- pinned commit SHA,
- latest green `main` CI URL,
- build or install instructions for that commit,
- release-readiness audit link,
- benchmark artifact link with exact claim boundaries,
- operator report schema,
- external audit schema,
- incident drill schema,
- security contact path.

If any item is missing, do not start a broad preview. Fix the packet first.

## Allowed Claims

Allowed:

```text
UltraSQL is a pre-alpha database seeking independent production-readiness
evidence.
```

Allowed when linking the exact artifact:

```text
UltraSQL was fastest on the committed release-artifact DB-vs-DB scale sweep for
the measured comparable rows on the recorded host.
```

Not allowed:

```text
UltraSQL is production ready.
UltraSQL is the best database in all aspects.
UltraSQL is safe for unsupervised customer production data.
```

## Preview Rules

- Use synthetic or disposable data only.
- Pin every run to a full 40-character git commit.
- Keep raw logs and metrics.
- Report failures without negotiation.
- Treat data loss, corruption, panic, crash loop, privilege bypass, or
  unbounded resource growth as release-blocking until triaged.
- Do not merge report evidence by hand; run validators.

## Operator Packet

Send operators:

- `docs/OPERATOR_SOAK.md`,
- `docs/operator-reports.md`,
- `operator-reports/operator-report.json.example`,
- current commit SHA,
- current CI URL,
- install/build command,
- support contact and incident channel.

Ask them for:

- one 30-day continuous run,
- one signed JSON report,
- log bundle path,
- plain-language notes,
- confirmation that no production customer data was used.

## Auditor Packet

Send auditors:

- `docs/external-audits.md`,
- `external-audits/security-audit.json.example`,
- `external-audits/correctness-audit.json.example`,
- `docs/production-readiness.md`,
- `docs/release-checklist.md`,
- architecture and benchmark docs,
- current CI URL and commit SHA.

Ask them for:

- scope confirmation,
- methodology,
- timeline,
- written report URI or attachment,
- open finding counts by severity,
- sign-off identity,
- whether report can be cited in release evidence.

## Incident Drill Packet

Send incident reviewers:

- `docs/incident-drills.md`,
- `incident-drills/backup-restore.json.example`,
- `incident-drills/wal-recovery.json.example`,
- `incident-drills/disk-full.json.example`,
- backup/restore and chaos recovery docs,
- current CI URL and commit SHA.

Ask them for:

- RTO/RPO targets and actuals,
- correctness verification notes,
- alerting confirmation,
- postmortem URI or attachment,
- signed report.

## Exit Criteria

Private preview can graduate to public preview only after:

- at least one operator has completed a meaningful non-production run,
- security and correctness audit scopes are booked or in progress,
- incident-drill process has been dry-run,
- all release-gate statuses remain honest,
- no unresolved critical or high finding is hidden.

Production-ready claim still requires every gate in
`docs/release-checklist.md` to close with committed evidence.
