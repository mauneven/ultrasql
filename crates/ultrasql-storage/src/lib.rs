//! UltraSQL storage engine.
//!
//! Layers (bottom-up):
//!
//! 1. Page (8 KiB, 64-bit aligned). On-disk format with header + slotted body.
//! 2. Segment / file manager (mmap + direct IO toggle).
//! 3. Buffer pool (CLOCK-Pro, sharded page table).
//! 4. Heap access method (PostgreSQL-style HOT-update-friendly tuple layout).
//! 5. B+ tree index access method.
//! 6. Free-space map + visibility map.
//! 7. Checkpointer (periodic dirty-page flush driven by WAL durable LSN).
//! 8. TOAST (oversize attribute storage) — inline ≤ 2 KiB, external otherwise.
//! 9. Persistent CLOG — page-backed commit-log, 2 bits per XID.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) storage code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod access_method;
pub mod btree;
pub mod buffer_pool;
pub mod checkpointer;
pub mod checksum;
pub mod clog;
pub mod column_cache;
pub mod constraints;
pub mod fsm;
pub mod heap;
pub mod page;
pub mod segment;
pub mod sequence;
pub mod toast;
pub mod vm;
pub mod wal_applier;
pub mod wal_sink;

pub use buffer_pool::{
    BufferPool, BufferPoolError, BufferPoolStats, EVICTION_RELIEF_ROUNDS, EvictionRelief,
    PageGuard, PageLoader,
};
pub use checkpointer::{Checkpointer, CheckpointerConfig};
pub use clog::{ClogError, PersistentClog};
pub use fsm::FreeSpaceMap;
pub use page::{ItemId, ItemIdFlags, Page, PageError, PageHeader, PageKind, SlotIndex};
pub use toast::{ToastDatum, ToastError, ToastPointer, ToastTable, maybe_toast};
pub use vm::VisibilityMap;
#[cfg(any(test, feature = "testing"))]
pub use wal_sink::test_support;
pub use wal_sink::{NullWalSink, WalSink, WalSinkError, WalSinkStats};
