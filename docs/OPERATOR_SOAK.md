# UltraSQL Operator Soak

This runbook records evidence needed to close the final v1.0 operator gate:

```text
Three independent operators run UltraSQL for 30 continuous days and report.
```

## Scope

Each operator runs one UltraSQL instance for 30 continuous days using the same
released commit, records host and binary details, runs the standard mixed SQL
workload, keeps logs, and reports correctness or availability failures.
Reports are committed as `operator-reports/*.json` or attached to the release
issue and mirrored into the repository before tagging.

CI writes current status to:

```text
benchmarks/results/latest/operator_soak_status.json
```

## Commands

Short local smoke:

```bash
cargo build -p ultrasql-server --bin ultrasqld
scripts/run-operator-soak.py \
  --mode smoke \
  --commit "$(git rev-parse HEAD)" \
  --ultrasqld target/debug/ultrasqld \
  --out target/operator-soak-smoke.json \
  --duration-seconds 60 \
  --cycles 3 \
  --operator-id "$USER"
scripts/validate-operator-soak.py \
  --reports-dir target \
  --out target/operator_soak_status.json
```

Thirty-day operator run:

```bash
cargo build --profile release-ship -p ultrasql-server --bin ultrasqld
scripts/run-operator-soak.py \
  --mode 30d \
  --commit "$(git rev-parse HEAD)" \
  --ultrasqld target/release-ship/ultrasqld \
  --data-dir /var/lib/ultrasql-soak \
  --out operator-reports/<operator-hash>.json \
  --duration-seconds 2592000 \
  --operator-id "<private operator id to hash>"
scripts/validate-operator-soak.py --strict \
  --reports-dir operator-reports \
  --min-reports 3 \
  --min-days 30 \
  --commit "$(git rev-parse HEAD)"
```

Smoke reports are accepted only as non-ready development checks. They never
close the release gate. The validator records them as
`smoke_valid_report_count`.

## Required Operator Record

```text
schema_version: 2
mode: smoke | 30d
commit:
started_at:
ended_at:
duration_days:
host.id_hash:
operator.id_hash:
workload.id_hash:
db_binary.sha256:
config:
dataset_scale:
concurrency:
operations.total:
latency_ms.p50 / p95 / p99:
throughput_ops_per_sec:
errors.total:
errors.correctness:
errors.corruption:
errors.critical:
errors.high:
restart_count:
crash_recovery_count:
consistency_checks:
wal_replay_checks:
final_verdict:
log_bundle_path:
signed_off_by:
```

Do not commit credentials, raw customer data, private hostnames, or personal
contact details. Use hashed identifiers and redacted paths where possible.

## Closure Rule

The roadmap checkbox may be closed only after three independent operator
records show the same release commit, at least 30 continuous days each, zero
availability failures, zero correctness or corruption errors, zero critical or
high-severity issues, passing consistency checks, passing WAL replay checks,
and `final_verdict: "pass"`. Operator identifiers are compared from
`operator.id_hash` after trimming surrounding whitespace, and reports with
non-positive `concurrency` are rejected. Reports whose end time is in the
future are rejected. The release workflow runs
`scripts/validate-operator-soak.py --strict --commit $GITHUB_SHA`; if
`benchmarks/results/latest/operator_soak_status.json` reports `not_ready`, the
tagged release must not publish.
