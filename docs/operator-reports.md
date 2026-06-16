# Operator Reports

Three independent operators must run UltraSQL for 30 continuous days before a
v1.0 release can be signed. Reports live under `operator-reports/*.json` and
are checked by `scripts/validate-operator-soak.py`. Operators can generate the
schema v2 report with `scripts/run-operator-soak.py`.

The validator writes:

```text
benchmarks/results/latest/operator_soak_status.json
```

## Report Fields

Each report must contain:

Key identity fields are `operator.id_hash`, `workload.id_hash`, and
`db_binary.sha256`; do not record raw operator or host identifiers.
Release-blocking error counters include `errors.total`, `errors.correctness`,
`errors.corruption`, `errors.critical`, and `errors.high`.

```json
{
  "schema_version": 2,
  "mode": "30d",
  "commit": "git commit tested",
  "started_at": "2026-01-01T00:00:00Z",
  "ended_at": "2026-01-31T00:00:00Z",
  "duration_days": 30.0,
  "host": {
    "id_hash": "sha256 host identifier",
    "cpu": "CPU model and core count",
    "memory_bytes": 68719476736,
    "storage": "storage device and filesystem",
    "os": "operating system and kernel"
  },
  "operator": {
    "id_hash": "sha256 operator identifier"
  },
  "workload": {
    "id": "mixed-sql-soak-v1",
    "id_hash": "sha256 workload identifier",
    "sql_surface": ["ddl", "crud", "transactions", "indexes", "views", "jsonb", "text_search", "vector", "copy", "export_import"]
  },
  "db_binary": {
    "path": "redacted path to ultrasqld",
    "sha256": "sha256 binary digest"
  },
  "config": {
    "ultrasqld_command": "full command line",
    "data_dir": "data directory path or redacted mount name",
    "ops_endpoint": "health/ready/metrics endpoint",
    "health_check_interval": "30s"
  },
  "dataset_scale": {"rows": 1000000},
  "concurrency": 8,
  "operations": {"total": 1000000, "ddl": 10, "read": 400000, "write": 400000, "transactions": 100000, "copy": 100, "export_import": 1},
  "latency_ms": {"p50": 1.0, "p95": 5.0, "p99": 20.0},
  "throughput_ops_per_sec": 1000.0,
  "errors": {"total": 0, "availability": 0, "sql": 0, "correctness": 0, "corruption": 0, "critical": 0, "high": 0},
  "restart_count": 1,
  "crash_recovery_count": 0,
  "consistency_checks": [{"name": "row_count", "passed": true, "checksum": "sha256"}],
  "wal_replay_checks": [{"name": "clean_restart", "passed": true, "checksum": "sha256"}],
  "final_verdict": "pass",
  "log_bundle_path": "artifact, object-store path, or release issue attachment",
  "signed_off_by": "human reviewer"
}
```

Do not commit secrets, credentials, private hostnames, or raw customer data.
Use hashed identifiers and redacted paths where possible.

## Closure Rule

The release gate is ready only when:

- at least three valid reports exist,
- each report is from a distinct normalized `operator.id_hash` after
  surrounding whitespace is trimmed,
- every report covers the tagged release commit,
- every report covers at least 30 continuous days,
- `concurrency` is a positive integer,
- `ended_at` is not in the future,
- all `errors` counters are zero,
- every consistency and WAL replay check passed,
- `final_verdict` is `pass`.

Reports with `mode: "smoke"` and `final_verdict: "smoke_pass"` are useful
local checks. The validator records them as `smoke_valid_report_count`, but
they do not count toward release readiness.
