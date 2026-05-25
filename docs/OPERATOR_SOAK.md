# UltraSQL Operator Soak

This runbook records evidence needed to close the final v1.0 operator gate:

```text
Three independent operators run UltraSQL for 30 continuous days and report.
```

## Scope

Each operator runs one UltraSQL instance for 30 continuous days using the same
released commit, records host details, keeps logs, and reports correctness or
availability failures. Reports are committed as `operator-reports/*.json` or
attached to the release issue and mirrored into the repository before tagging.

CI writes current status to:

```text
benchmarks/results/latest/operator_soak_status.json
```

## Required operator record

```text
operator_id:
operator_contact_handle:
commit:
start_time_utc:
end_time_utc:
host_cpu:
host_memory:
host_storage:
os:
ultrasqld_command:
workload:
client_count:
data_dir:
ops_endpoint:
health_check_interval:
failure_count:
correctness_issue_count:
critical_issue_count:
high_issue_count:
notes:
log_bundle_path:
signed_off_by:
```

Do not commit credentials, raw customer data, private hostnames, or personal
contact details. Use redacted paths and public handles where possible.

## Minimum commands

```bash
cargo run --bin ultrasql -- --ctl initdb --data-dir target/soak-data
cargo run --bin ultrasqld -- --data-dir target/soak-data --listen 127.0.0.1:5433 --ops-listen 127.0.0.1:8080 --log-format json
cargo run --bin ultrasql -- --isready --host 127.0.0.1 --port 5433 --ops-endpoint 127.0.0.1:8080
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/ready
curl -fsS http://127.0.0.1:8080/metrics
```

## Closure rule

The roadmap checkbox may be closed only after three independent operator
records show at least 30 continuous days each, with zero correctness issues and
zero critical or high-severity issues. The release workflow runs
`scripts/validate-operator-soak.py --strict`; if
`benchmarks/results/latest/operator_soak_status.json` reports `not_ready`, the
tagged release must not publish.
