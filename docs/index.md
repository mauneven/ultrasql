# UltraSQL Documentation

UltraSQL is a fast SQL database in Rust. These docs cover local evaluation,
release artifacts, operational gates, and known limits.

UltraSQL is alpha. Treat every install path as an evaluation or release
candidate path until the v1.0 release checklist is closed.

## Start here

- [Getting Started](getting-started.md) builds and runs a local server.
- [Install](install.md) covers archives, npm, Docker, Homebrew, Debian, and RPM.
- [Configuration](configuration.md) lists release-relevant server knobs.
- [DESCRIBE](sql/describe.md) documents table, view, and query metadata introspection.
- [CREATE VIEW](sql/create-view.md) documents regular view creation and query
  expansion.
- [ALTER VIEW](sql/alter-view.md) documents regular view rename and schema
  moves.
- [CHECKPOINT](sql/checkpoint.md) documents the WAL checkpoint command.
- [SET VARIABLE](sql/set-variable.md) documents session-local runtime settings.
- [MERGE INTO](sql/merge.md) documents conditional table upserts, updates, and
  deletes.
- [EXPORT DATABASE / IMPORT DATABASE](sql/export-import.md) documents logical
  dump and restore.
- [PIVOT](sql/pivot.md) documents row-to-column aggregate transforms.
- [UNPIVOT](sql/unpivot.md) documents column-to-row transforms.
- [SUMMARIZE](sql/summarize.md) documents per-column table statistics.
- [AI Database Strategy](ai-database-strategy.md) maps UltraSQL's AI memory
  engine plan.
- [Production Readiness Audit](production-readiness.md) records the current
  evidence-backed readiness verdict.
- [Documentation Status Audit](documentation-status-audit.md) records the
  current alpha wording and first-party Markdown audit scope.
- [Private Preview Runbook](private-preview-runbook.md) defines the controlled
  pre-production preview packet.
- [External Validation Outreach](external-validation-outreach.md) gives audit,
  operator-soak, and incident-drill outreach templates.
- [Known Limitations](known-limitations.md) records open SQL and operations
  gaps.
- [Release Checklist](release-checklist.md) is the production readiness gate.
