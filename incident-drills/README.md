# Incident Drill Intake

Place real incident drill JSON reports in this directory after a drill has run
against a pinned release commit.

Example templates use `.json.example` so release validators do not count them
as reports.

Required source docs:

- `docs/incident-drills.md`
- `docs/private-preview-runbook.md`
- `docs/production-readiness.md`

Do not commit secrets, private hostnames, customer data, or unreviewed
postmortems. The v1.0 gate requires valid `backup_restore`, `wal_recovery`, and
`disk_full` drill reports for the exact release commit.
