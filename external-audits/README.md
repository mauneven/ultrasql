# External Audit Intake

Place real external audit JSON reports in this directory only after an
independent auditor has signed them off.

Example templates use `.json.example` so release validators do not count them
as reports.

Required source docs:

- `docs/external-audits.md`
- `docs/external-validation-outreach.md`
- `docs/production-readiness.md`

Do not commit NDA-only reports, secrets, raw customer data, or unverifiable
testimonials. The v1.0 gate requires at least one valid `security` audit and
one valid `correctness` audit for the exact release commit.
