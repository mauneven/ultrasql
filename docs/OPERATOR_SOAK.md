# UltraSQL v0.9 Operator Soak

This runbook records the evidence needed to close the v0.9 external
operator milestone:

```text
Three independent operators run UltraSQL for 7 days and report.
```

## Scope

Each operator runs one UltraSQL instance for seven continuous days using
the same released commit, records host details, keeps logs, and reports
correctness or availability failures.

## Required operator record

```text
operator_id:
operator_contact:
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
notes:
log_bundle_path:
```

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

The roadmap checkbox may be closed only after three independent records
show at least seven continuous days each, with no open critical or
high-severity correctness issue.
