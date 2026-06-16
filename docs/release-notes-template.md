# UltraSQL @RELEASE_TAG@

These GitHub release notes are rendered by the release workflow. They are not a
production claim unless every gate below is linked to evidence.

## Release Status

- Release workflow: @RELEASE_RUN_URL@
- Operator soak status: @OPERATOR_SOAK_STATUS@
- External audit status: @EXTERNAL_AUDIT_STATUS@
- Incident drill status: @INCIDENT_DRILL_STATUS@
- Driver compatibility status: @DRIVER_COMPATIBILITY_STATUS@
- GitHub release notes: this body plus attached assets and checksums.

## Green workflow evidence

Attach these run ids before declaring the release production-ready:

- latest green CI workflow run id,
- latest green benchmark certification workflow run id,
- latest green docs workflow run id,
- release workflow run id: @RELEASE_RUN_URL@.

## 30-day operator reports

The release workflow validates `operator-reports/*.json` with
`scripts/validate-operator-soak.py --strict`. Three independent 30-day operator
reports are required. The rendered status artifact is
`operator_soak_status.json`.

## External audit reports

The release workflow validates `external-audits/*.json` with
`scripts/validate-external-audits.py --strict` for v1.0 and later releases.
Two independent external audit reports covering `security` and `correctness`
are required. The rendered status artifact is `external_audit_status.json`.

## Incident drills

The release workflow validates `incident-drills/*.json` with
`scripts/validate-incident-drills.py --strict` for v1.0 and later releases.
The required drill types are `backup_restore`, `wal_recovery`, and
`disk_full`. The rendered status artifact is `incident_drill_status.json`.

## Driver compatibility

The release workflow runs `scripts/run-driver-release-evidence.py`, which
builds `ultrasqld`, writes `target/driver-certification.json`, and validates
the result with `scripts/validate-driver-compatibility.py --strict` for v1.0
and later releases. Required clients include stock libpq, psql meta-commands,
Python, Node.js, Go, Java/JDBC, Hibernate, .NET/Npgsql, Prisma, Diesel, GUI
introspection probes, Flyway, Liquibase, and Alembic coverage. The rendered
status artifact is `driver_compatibility_status.json`.

## Assets

Release assets include:

- platform archives plus `.sha256` files,
- `SHASUMS256.txt`,
- `ultrasql.rb` Homebrew formula,
- Linux `.deb` and `.rpm` packages,
- `operator_soak_status.json`,
- `external_audit_status.json`,
- `incident_drill_status.json`,
- `driver_compatibility_status.json`.

## Known Gaps

See `CHANGELOG.md`, `ROADMAP.md`, `DONE.md`, and
`docs/known-limitations.md`.
