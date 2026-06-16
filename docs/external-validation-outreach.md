# External Validation Outreach

Last prepared: 2026-06-16.

This page is the non-code launch packet for the remaining production evidence.
It is intentionally not a production-ready announcement. Use it to recruit
independent operators, external auditors, and incident-drill reviewers.

## Current Position

UltraSQL can be presented as a pre-alpha database seeking independent review.
The only benchmark claim that can be made today is the committed same-host
release-artifact DB-vs-DB scale-sweep result recorded in the benchmark
artifacts. Do not claim production readiness, universal database leadership, or
general workload superiority.

## Outreach Targets

Prioritize groups that can produce public or inspectable evidence:

- Open source audit coordinators such as [OSTIF](https://ostif.org/), because
  their public role is facilitating open source security audits and reviews.
- Security review firms with Rust or systems experience, such as
  [Trail of Bits](https://trailofbits.com/) or [Cure53](https://cure53.de/).
- Database correctness and failure-analysis specialists such as
  [Jepsen](https://jepsen.io/), especially for claims around transactions,
  recovery, consistency, and operator safety.
- Independent database operators willing to run a 30-day soak from a pinned
  commit and publish a signed report.

This list is not an endorsement and not a procurement decision. Each candidate
must still be screened for independence, availability, price, conflict of
interest, and willingness to publish or share enough evidence for release
sign-off.

## Security Audit Request

Subject:

```text
UltraSQL pre-production security audit request
```

Body:

```text
Hello,

We maintain UltraSQL, a from-scratch SQL database engine in Rust. We are not
claiming production readiness yet. We are looking for an independent security
audit before any v1.0 claim.

Scope requested:
- threat model for a SQL server exposed on PostgreSQL-compatible wire protocol,
- authentication and authorization surfaces,
- filesystem and data-dir handling,
- WAL/recovery safety from a security perspective,
- packaging and release integrity,
- dependency and supply-chain risk,
- unsafe Rust and FFI review where present.

Evidence package:
- repository: https://github.com/mauneven/ultrasql
- readiness audit: docs/production-readiness.md
- release checklist: docs/release-checklist.md
- expected report schema: docs/external-audits.md

We need a written report that can be attached or referenced by release
evidence. Closed, unverifiable, or testimonial-only review cannot close the
production gate.

Can you share availability, proposed scope, methodology, timeline, and whether
you can provide a report that satisfies this evidence model?
```

## Correctness Audit Request

Subject:

```text
UltraSQL database correctness audit request
```

Body:

```text
Hello,

We maintain UltraSQL, a from-scratch SQL database engine in Rust. We are
seeking an independent correctness review before any production-ready claim.

Scope requested:
- SQL parser, binder, optimizer, and executor correctness,
- storage, page, index, MVCC, transaction, and WAL invariants,
- crash recovery and checkpoint behavior,
- transaction isolation claims,
- data export/import and backup/restore behavior,
- compatibility risks against PostgreSQL client expectations.

Evidence package:
- repository: https://github.com/mauneven/ultrasql
- readiness audit: docs/production-readiness.md
- release checklist: docs/release-checklist.md
- expected report schema: docs/external-audits.md

We need a written report with open findings counted by severity. Critical and
high findings must be resolved before v1.0 sign-off.

Can you share availability, methodology, timeline, and whether your report can
be made inspectable enough to support release evidence?
```

## Operator Soak Invite

Subject:

```text
UltraSQL 30-day operator soak invitation
```

Body:

```text
Hello,

We are recruiting independent operators for an UltraSQL pre-production soak.
This is not a production-ready release. The goal is to learn whether a pinned
release commit can run for 30 continuous days without availability failures,
correctness issues, or critical/high operational issues.

Operator commitment:
- run one pinned commit for 30 continuous days,
- use non-customer test data,
- keep logs and health/ready/metrics evidence,
- report failures honestly,
- submit a report matching docs/operator-reports.md.

The release gate needs three independent valid reports. A failed or critical
report is valuable and must not be hidden.

Can you run this in a staging or lab environment and provide a signed report?
```

## Incident Drill Request

Subject:

```text
UltraSQL incident drill reviewer request
```

Body:

```text
Hello,

We need independent incident-drill evidence for UltraSQL before any production
claim. Required drills are backup restore, WAL recovery, and disk-full
response.

Scope:
- run drills against a pinned commit,
- record RTO/RPO targets and actuals,
- verify data correctness after recovery,
- confirm alerting/monitoring behavior,
- produce report JSON matching docs/incident-drills.md.

This is pre-production validation. Negative findings should be reported, not
softened.

Can you run or review these drills in a staging environment?
```

## Intake Checklist

Before sending any outreach, attach or link:

- pinned commit SHA,
- latest green CI run URL,
- latest benchmark artifact directory,
- `docs/production-readiness.md`,
- `docs/release-checklist.md`,
- relevant report schema document,
- explicit statement that UltraSQL is not production ready yet.

Before accepting any report, verify:

- reviewer/operator is independent,
- report covers the exact commit,
- report has enough detail to inspect,
- report has no secrets or customer data,
- report has human sign-off,
- validators accept it,
- production docs still say `not_ready` unless every gate closes.

## Non-Negotiable Ethics

- Do not buy testimonials.
- Do not count private praise as an audit.
- Do not hide critical or high findings.
- Do not edit report contents except to remove secrets with reviewer approval.
- Do not call a report valid unless the repository validator accepts it.
- Do not ask operators to use production customer data.
