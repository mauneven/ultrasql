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
//! v0.4 additions:
//!
//! - [`ssi`] — Serializable Snapshot Isolation (SSI) primitives: predicate
//!   locks, rw-anti-dependency tracking, dangerous-structure detection, and
//!   `SERIALIZABLE` conflict checks via [`TransactionManager::new_with_ssi`].
//!   The server integration records column-range predicate tags for supported
//!   scalar comparisons and relation-level fallback tags for unsupported
//!   predicates.
//! - [`savepoint`] — Subtransaction / savepoint manager implementing
//!   `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`, with
//!   rolled-back subtransaction tracking for MVCC visibility.
//! - [`two_phase`] — Two-phase commit coordinator implementing
//!   `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`.
//! - [`row_lock`] — Row-level locking API for `SELECT FOR UPDATE`,
//!   `FOR SHARE`, `FOR NO KEY UPDATE`, and `FOR KEY SHARE`.
//!
//! The CLOG in this revision is an in-memory `DashMap`. A persistent,
//! page-backed CLOG is tracked as a follow-up; the API does not change.
//! The active-transactions snapshot is built by a full scan of the
//! in-progress CLOG entries — for the modest write concurrency v0.5 is
//! sized for this is well within budget. An optimised procarray-style
//! cache is an RFC follow-up.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) transaction code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod lock;
pub mod manager;
pub mod row_lock;
pub mod savepoint;
pub mod ssi;
pub mod two_phase;

pub use lock::{
    LockError, LockManager, LockMode, LockRequest, LockTableSnapshot, LockTag, LockWait,
};
pub use manager::{IsolationLevel, Transaction, TransactionManager, TxnError};
pub use row_lock::{RowLockExt, RowLockMode, RowLockRequest};
pub use savepoint::{SavepointError, Subtxn, SubtxnManager};
pub use ssi::{PredicateLock, PredicateLockTag, SsiError, SsiManager};
pub use two_phase::{PreparedTxn, TwoPhaseCoordinator, TwoPhaseError};
