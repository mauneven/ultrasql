//! Unit tests for the `ultrasqld` binary support modules.
//!
//! Grouped by the support module under test: CLI-to-config validation
//! ([`config`]), WAL archive/restore orchestration ([`wal_archive`]),
//! and the HTTP ops endpoint ([`ops`]).

mod config;
mod ops;
mod wal_archive;
