//! Support modules for the `ultrasql` client binary.
//!
//! `main.rs` keeps the entry point, argument resolution, and REPL driver;
//! the cohesive feature areas (session/formatting, connection params,
//! backup/dump, WAL shipping, ops/ctl) live here so each file stays small.

pub(crate) mod backup;
pub(crate) mod cli_args;
pub(crate) mod fileio;
pub(crate) mod server_ops;
pub(crate) mod session;
pub(crate) mod wal_ship;
pub(crate) mod waldump;

#[cfg(test)]
mod tests;
