# Operator Reports

Three independent operators must run UltraSQL for 30 continuous days before a
v1.0 release can be signed. Reports live under `operator-reports/*.json` and
are checked by `scripts/validate-operator-soak.py`.

The validator writes:

```text
benchmarks/results/latest/operator_soak_status.json
```

## Report fields

Each report must contain:

```json
{
  "operator_id": "public-handle-or-company-identifier",
  "operator_contact_handle": "optional public contact handle",
  "commit": "git commit tested",
  "start_time_utc": "2026-01-01T00:00:00Z",
  "end_time_utc": "2026-01-31T00:00:00Z",
  "host_cpu": "CPU model and core count",
  "host_memory": "RAM size",
  "host_storage": "storage device and filesystem",
  "os": "operating system and kernel",
  "ultrasqld_command": "full command line",
  "workload": "workload description",
  "client_count": 1,
  "data_dir": "data directory path or redacted mount name",
  "ops_endpoint": "health/ready/metrics endpoint",
  "health_check_interval": "interval used by the operator",
  "failure_count": 0,
  "correctness_issue_count": 0,
  "critical_issue_count": 0,
  "high_issue_count": 0,
  "notes": "plain-language observations",
  "log_bundle_path": "artifact, object-store path, or release issue attachment",
  "signed_off_by": "human reviewer"
}
```

Do not commit secrets, credentials, private hostnames, or raw customer data.
Use a public handle or redacted identifier instead of personal contact details
when possible.

## Closure rule

The release gate is ready only when:

- at least three valid reports exist,
- each report is from a distinct normalized `operator_id` after surrounding
  whitespace is trimmed,
- every report covers the tagged release commit,
- every report covers at least 30 continuous days,
- `client_count` is a positive integer,
- `end_time_utc` is not in the future,
- `failure_count` is zero,
- `correctness_issue_count`, `critical_issue_count`, and
  `high_issue_count` are all zero.

CI can run the validator in non-strict mode to publish current status. The
release workflow runs it with `--strict --commit "$GITHUB_SHA"`, so a tag cannot
publish unless the operator soak reports satisfy the gate for that exact commit.
