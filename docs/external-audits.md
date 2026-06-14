# External Audit Reports

External audits are a v1.0 production gate. UltraSQL needs at least two
independent external audit reports before a production-ready release can be
signed. Code changes alone cannot close this gate.

## Required coverage

The release gate requires two independent external audit reports:

- `security` - threat model, authentication, authorization, file-system
  handling, release packaging, dependency risk, and secret-handling review.
- `correctness` - SQL execution, storage, WAL, crash recovery, transaction
  isolation, and upgrade/downgrade behavior review.

Extra `performance`, `operations`, or `compatibility` audits are welcome, but
they do not replace the required `security` and `correctness` reports.

## Report schema

Each report is a JSON file under `external-audits/*.json`:

```json
{
  "auditor_id": "security-lab-a",
  "auditor_org": "Security Lab A",
  "auditor_contact": "security-lab-a@example.invalid",
  "commit": "0123456789abcdef0123456789abcdef01234567",
  "audit_type": "security",
  "report_date_utc": "2026-02-01T00:00:00Z",
  "scope": "storage, WAL, SQL execution, release packaging",
  "methodology": "manual review, fuzz replay, threat modeling",
  "report_uri": "https://example.invalid/ultrasql-security-audit.pdf",
  "critical_findings_open": 0,
  "high_findings_open": 0,
  "medium_findings_open": 0,
  "low_findings_open": 0,
  "signed_off_by": "external reviewer",
  "signature": "detached-signature-or-verifiable-attestation"
}
```

`report_uri` must point to a review artifact that maintainers can inspect.
Closed, NDA-only, or unverifiable claims cannot support a production-ready
release claim.

## Validation

Run:

```bash
scripts/validate-external-audits.py \
  --reports-dir external-audits \
  --min-reports 2 \
  --required-audit-types security,correctness \
  --commit "$(git rev-parse HEAD)" \
  --out benchmarks/results/latest/external_audit_status.json
```

For v1.0 and later releases, the release workflow runs:

```bash
scripts/validate-external-audits.py --strict
```

The gate is ready only when:

- two independent external audit reports are valid,
- required audit types `security` and `correctness` are covered,
- every valid report covers the release commit,
- `critical_findings_open` is `0`,
- `high_findings_open` is `0`,
- every report has `report_uri`, `signed_off_by`, and `signature`.

The committed status file may say `not_ready`. That is honest evidence, not a
failure by itself for pre-1.0 prereleases.
