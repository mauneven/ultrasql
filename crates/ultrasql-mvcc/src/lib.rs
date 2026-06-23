//! UltraSQL multi-version concurrency control.
//!
//! Tuples carry `xmin`/`xmax`/`cmin`/`cmax` headers. Snapshots are computed
//! by sampling the active transaction set. Visibility predicates implement
//! PostgreSQL `HeapTupleSatisfiesMVCC` semantics, generalized to support
//! both snapshot isolation and serializable isolation.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) MVCC code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod snapshot;
pub mod status;
pub mod tuple_header;
pub mod visibility;

pub use snapshot::Snapshot;
pub use status::{XidStatus, XidStatusOracle};
pub use tuple_header::{InfoMask, TupleHeader};
pub use visibility::{Visibility, is_visible};
