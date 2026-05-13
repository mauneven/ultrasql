//! UltraSQL transaction manager.
//!
//! Coordinates xact lifecycle, snapshot acquisition, lock acquisition, and
//! WAL flushes. The lock manager uses a fastpath for relation-level locks
//! and a heavier wait-for graph for row-level conflicts.
//!
//! This crate's v0.5 surface ships the basics:
//!
//! - [`TransactionManager`] owns the XID allocator and the CLOG.
//! - [`Transaction`] is the handle returned by
//!   [`TransactionManager::begin`].
//! - [`IsolationLevel`] enumerates the supported isolation levels.
//! - [`TxnError`] carries the error type returned from commit / abort.
//! - [`TransactionManager`] implements [`ultrasql_mvcc::XidStatusOracle`]
//!   so visibility checks in `ultrasql-mvcc` can be served directly
//!   from the live commit log.
//!
//! The CLOG in this revision is an in-memory `DashMap`. A persistent,
//! page-backed CLOG is tracked as a follow-up; the API does not change.
//! The active-transactions snapshot is built by a full scan of the
//! in-progress CLOG entries — for the modest write concurrency v0.5 is
//! sized for this is well within budget. An optimised procarray-style
//! cache is an RFC follow-up.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod manager;

pub use manager::{IsolationLevel, Transaction, TransactionManager, TxnError};
