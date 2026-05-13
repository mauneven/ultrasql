//! Pluggable WAL sink consumed by the heap on each mutation.
//!
//! Heap operations don't own the WAL writer directly: in production
//! the writer is a long-lived background thread, while tests prefer
//! an in-memory mock for verification. The [`WalSink`] trait gives the
//! heap a narrow surface ("here is a fully-formed [`WalRecord`]") without
//! coupling it to a concrete writer type.
//!
//! # Tradeoffs
//!
//! The trait uses a shared reference (`&self`) rather than `&mut self` so
//! a single sink can be handed to multiple concurrent heap operations. This
//! means implementations must use interior mutability (e.g. a `Mutex` or
//! atomics) for state that changes per `append` call — see
//! `InMemoryWalSink` in the test module for the reference implementation.

use ultrasql_core::{Lsn, Xid};
use ultrasql_wal::WalRecord;

/// Errors that arise when a [`WalSink`] rejects a record.
#[derive(Debug, thiserror::Error)]
pub enum WalSinkError {
    /// The sink refused to accept the record. The message explains why.
    #[error("wal sink rejected record: {0}")]
    Rejected(String),
}

/// Anything that can durably accept a [`WalRecord`] and report the LSN it
/// was written at.
///
/// Implementations decide their own durability and ordering semantics; the
/// heap relies only on the contract below:
///
/// 1. `append` is called at most once per heap mutation.
/// 2. The returned `Lsn` is the assigned position of the record in the log.
/// 3. `durable_lsn` returns the highest LSN that has been flushed to durable
///    storage. Callers may use this to decide whether a page is safe to evict.
/// 4. `last_lsn_for` returns the LSN of the most recently appended record for
///    `xid`, or [`Lsn::ZERO`] if none.  Heap callers use this to fill the
///    `prev_lsn` field so records form a per-transaction linked list.
///
/// # Thread safety
///
/// `WalSink` requires `Send + Sync` so it can be stored behind an `Arc` and
/// shared across concurrent heap calls.
pub trait WalSink: Send + Sync {
    /// Append `record` to the WAL and return the assigned LSN.
    fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError>;

    /// Return the highest LSN that has been made durable (flushed). Heap
    /// callers use this to decide whether they need to flush before evicting
    /// a dirty page. A value of [`Lsn::ZERO`] means nothing has been flushed
    /// yet.
    fn durable_lsn(&self) -> Lsn;

    /// Return the LSN of the most recent record appended for `xid`, or
    /// [`Lsn::ZERO`] if no records have been appended for `xid` yet.
    ///
    /// The heap uses this as the `prev_lsn` of the next record it appends for
    /// `xid` so records form a per-transaction linked list in the WAL.
    fn last_lsn_for(&self, xid: Xid) -> Lsn;
}

// ---------------------------------------------------------------------------
// NullWalSink
// ---------------------------------------------------------------------------

/// No-op sink that silently discards every record.
///
/// Useful in tests that want to exercise heap logic without caring about WAL
/// contents, and as the default when WAL is administratively disabled (e.g.
/// in `--no-wal` test mode). Every `append` call succeeds immediately and
/// returns [`Lsn::ZERO`]; `durable_lsn` and `last_lsn_for` always return
/// [`Lsn::ZERO`].
///
/// This type is `Send + Sync` trivially because it has no shared state.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullWalSink;

impl WalSink for NullWalSink {
    fn append(&self, _record: WalRecord) -> Result<Lsn, WalSinkError> {
        Ok(Lsn::ZERO)
    }

    fn durable_lsn(&self) -> Lsn {
        Lsn::ZERO
    }

    fn last_lsn_for(&self, _xid: Xid) -> Lsn {
        Lsn::ZERO
    }
}

// ---------------------------------------------------------------------------
// InMemoryWalSink — test support
// ---------------------------------------------------------------------------

/// In-memory WAL sink for unit-test verification.
///
/// Records are appended to an in-memory buffer in the order they arrive.
/// LSNs are assigned monotonically starting at 1 (so `Lsn::ZERO` is
/// unambiguously "no record yet"). A per-XID map tracks the last LSN for
/// each transaction so the heap's `prev_lsn` chaining can be tested.
///
/// This type is exported only under `#[cfg(test)]` because it is not
/// intended for production use.
#[cfg(test)]
pub mod test_support {
    use std::collections::HashMap;

    use parking_lot::Mutex;
    use ultrasql_core::{Lsn, Xid};
    use ultrasql_wal::WalRecord;

    use super::{WalSink, WalSinkError};

    /// In-memory sink that stores every appended record and assigns
    /// monotonically increasing LSNs starting at 1.
    ///
    /// All state is guarded by a `Mutex` so the sink satisfies `Sync`.
    /// Test code can call `records()` after the mutations to inspect what
    /// was appended.
    #[derive(Debug, Default)]
    pub struct InMemoryWalSink {
        inner: Mutex<Inner>,
    }

    #[derive(Debug, Default)]
    struct Inner {
        /// All records in append order, together with their assigned LSN.
        records: Vec<(Lsn, WalRecord)>,
        /// Next LSN to hand out.
        next_lsn: u64,
        /// Per-XID last-assigned LSN for `prev_lsn` chaining.
        last_lsn: HashMap<u64, Lsn>,
    }

    impl Inner {
        fn next(&mut self) -> Lsn {
            self.next_lsn = self.next_lsn.saturating_add(1);
            Lsn::new(self.next_lsn)
        }
    }

    impl InMemoryWalSink {
        /// Construct an empty sink. LSN counter starts at 0 (first
        /// assigned LSN will be 1).
        pub fn new() -> Self {
            Self::default()
        }

        /// Return a snapshot of all appended `(lsn, record)` pairs in
        /// the order they were received.
        pub fn records(&self) -> Vec<(Lsn, WalRecord)> {
            self.inner.lock().records.clone()
        }

        /// Number of records appended so far.
        pub fn len(&self) -> usize {
            self.inner.lock().records.len()
        }

        /// `true` when no records have been appended.
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    }

    impl WalSink for InMemoryWalSink {
        // `significant_drop_tightening` would suggest dropping the guard before
        // the final `Ok(lsn)`, but `lsn` is a plain `Copy` value so keeping the
        // guard alive until function exit is safe and keeps the push+lsn
        // assignment under one lock acquisition.
        #[allow(clippy::significant_drop_tightening)]
        fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
            let mut inner = self.inner.lock();
            let lsn = inner.next();
            let xid_raw = record.header.xid.raw();
            inner.last_lsn.insert(xid_raw, lsn);
            inner.records.push((lsn, record));
            Ok(lsn)
        }

        fn durable_lsn(&self) -> Lsn {
            let inner = self.inner.lock();
            // All records in this mock are immediately "durable".
            if inner.next_lsn == 0 {
                Lsn::ZERO
            } else {
                Lsn::new(inner.next_lsn)
            }
        }

        fn last_lsn_for(&self, xid: Xid) -> Lsn {
            let inner = self.inner.lock();
            inner.last_lsn.get(&xid.raw()).copied().unwrap_or(Lsn::ZERO)
        }
    }
}
