# Incident Drill Reports

Incident drills are a v1.0 production gate. UltraSQL needs repeated recovery
evidence before maintainers can claim production readiness.

## Required drills

The release gate requires one valid report for each drill type:

- `backup_restore` - restore from the release backup path and verify SQL
  correctness.
- `wal_recovery` - recover from a WAL/crash event and verify committed rows.
- `disk_full` - prove failed writes do not corrupt durable state and recovery
  remains possible.

These drills complement `benchmarks/chaos_recovery.sh`; they add operator
runbook, monitoring, RTO/RPO, and postmortem evidence.

Local smoke reports can be generated with:

```bash
scripts/run-incident-drills.py \
  --mode smoke \
  --commit "$(git rev-parse HEAD)" \
  --reports-dir target/incident-drills
scripts/validate-incident-drills.py \
  --reports-dir target/incident-drills \
  --required-drill-types backup_restore,wal_recovery,disk_full \
  --commit "$(git rev-parse HEAD)"
```

Smoke reports are recorded as `smoke_valid_report_count`, but do not close the
release gate. Production sign-off requires `mode: "production"` reports from a
real incident-drill environment.

## Report schema

Each report is a JSON file under `incident-drills/*.json`:

```json
{
  "schema_version": 2,
  "mode": "production",
  "drill_id": "rc1-backup-restore",
  "commit": "0123456789abcdef0123456789abcdef01234567",
  "drill_type": "backup_restore",
  "run_time_utc": "2026-02-01T00:00:00Z",
  "environment": "release-candidate staging",
  "scenario": "restore latest base backup and replay WAL",
  "operator": "ops-a",
  "rto_target_seconds": 60,
  "rto_actual_seconds": 20,
  "rpo_target_seconds": 0,
  "rpo_actual_seconds": 0,
  "data_loss_confirmed": false,
  "correctness_verified": true,
  "monitoring_alerted": true,
  "postmortem_uri": "https://example.invalid/rc1-backup-restore.md",
  "unresolved_sev0_count": 0,
  "unresolved_sev1_count": 0,
  "artifacts": {
    "manifest_path": "artifact://backup_restore_smoke_manifest.json",
    "log_bundle_path": "artifact://incident-drill-logs"
  },
  "checks": [
    {"name": "row_count_verified", "passed": true},
    {"name": "recovery_verified", "passed": true}
  ],
  "signed_off_by": "incident commander"
}
```

## Validation

Run:

```bash
scripts/validate-incident-drills.py \
  --reports-dir incident-drills \
  --required-drill-types backup_restore,wal_recovery,disk_full \
  --commit "$(git rev-parse HEAD)" \
  --out benchmarks/results/latest/incident_drill_status.json
```

For v1.0 and later releases, the release workflow runs:

```bash
scripts/validate-incident-drills.py --strict
```

The gate is ready only when:

- `backup_restore`, `wal_recovery`, and `disk_full` are covered,
- every counted report has `schema_version: 2` and `mode: "production"`,
- every valid report covers the release commit,
- `rto_actual_seconds <= rto_target_seconds`,
- `rpo_actual_seconds <= rpo_target_seconds`,
- `data_loss_confirmed` is `false`,
- `correctness_verified` is `true`,
- `monitoring_alerted` is `true`,
- `unresolved_sev0_count` and `unresolved_sev1_count` are `0`,
- every report has non-empty `artifacts.manifest_path`,
- every `checks` entry has `passed: true`,
- every report has `postmortem_uri` and `signed_off_by`.

The committed status file may say `not_ready`. That is honest evidence, not a
failure by itself for pre-1.0 prereleases.
