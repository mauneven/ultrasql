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
use ultrasql_wal::{RecordType, WalRecord};

/// Live WAL counters exposed by storage sinks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalSinkStats {
    /// Number of WAL records accepted by the sink.
    pub wal_records: u64,
    /// Number of full-page-write records accepted by the sink.
    pub wal_fpi: u64,
    /// Number of serialized WAL bytes accepted by the sink.
    pub wal_bytes: u64,
    /// Number of WAL write operations observed by the sink.
    pub wal_write: u64,
}

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
/// # Failure contract
///
/// An implementation that returns `Err` from `append` **after** the caller
/// has applied a page mutation will cause the caller to poison the buffer
/// pool and return a fatal storage error. Implementations should reserve
/// `Err` for true WAL failures (disk full, closed queue, checksum writer
/// failure). Once page state has diverged from the WAL there is no safe way
/// to continue accepting work; the owning service must restart so recovery
/// starts from a consistent WAL position.
///
/// # Thread safety
///
/// `WalSink` requires `Send + Sync` so it can be stored behind an `Arc` and
/// shared across concurrent heap calls.
pub trait WalSink: Send + Sync {
    /// Append `record` to the WAL and return the assigned LSN.
    fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError>;

    /// Append a borrowed WAL record and return its assigned LSN.
    ///
    /// The default clones and delegates to [`Self::append`]. Buffered sinks
    /// should override this when they can serialize from `&WalRecord`
    /// directly; hot paths can then reuse payload allocations across many
    /// page-local records.
    fn append_ref(&self, record: &WalRecord) -> Result<Lsn, WalSinkError> {
        self.append(record.clone())
    }

    /// Append a WAL record from a borrowed payload and return its assigned LSN.
    ///
    /// The default constructs an owned [`WalRecord`] and delegates to
    /// [`Self::append`]. Buffered sinks should override this when they can
    /// serialize directly from `payload`; hot page-local paths can then reuse
    /// one payload allocation across many records.
    fn append_borrowed(
        &self,
        record_type: RecordType,
        xid: Xid,
        prev_lsn: Lsn,
        flags: u8,
        payload: &[u8],
    ) -> Result<Lsn, WalSinkError> {
        let record = WalRecord::new(record_type, xid, prev_lsn, flags, payload.to_vec())
            .map_err(|err| WalSinkError::Rejected(format!("WAL record rejected: {err}")))?;
        self.append(record)
    }

    /// Append a borrowed-payload record whose per-transaction chain link is
    /// resolved atomically with LSN assignment.
    ///
    /// `link` holds the raw LSN of the transaction's previous record (`0`
    /// for none); the record's `prev_lsn` is read from it and the assigned
    /// LSN stored back. Sinks that admit CONCURRENT appenders for one
    /// transaction (the parallel bulk-mutation paths) MUST override this so
    /// the read-link/append/store-link step is atomic with the append —
    /// otherwise two appenders can read the same link and fork the chain.
    /// The default performs the three steps non-atomically, which is correct
    /// for every single-threaded caller and test sink.
    fn append_borrowed_linked(
        &self,
        record_type: RecordType,
        xid: Xid,
        flags: u8,
        payload: &[u8],
        link: &std::sync::atomic::AtomicU64,
    ) -> Result<Lsn, WalSinkError> {
        let prev = Lsn::new(link.load(std::sync::atomic::Ordering::Acquire));
        let lsn = self.append_borrowed(record_type, xid, prev, flags, payload)?;
        link.store(lsn.raw(), std::sync::atomic::Ordering::Release);
        Ok(lsn)
    }

    /// Return `true` when [`Self::append_ref`] performs no blocking filesystem
    /// I/O or fsync and acquires no buffer-pool page latch or storage lock — so
    /// it is safe to call while holding a page's write guard.
    ///
    /// Heap paths use this to decide whether they can append a page-local WAL
    /// record while holding that page's write guard. This lets the page-local
    /// DELETE path preserve WAL-before-data ordering without a second buffer
    /// pool pin. Sinks that may touch storage during `append_ref` must keep the
    /// default `false` so heap callers release page guards before appending.
    ///
    /// A buffered sink may still *briefly park* an append for WAL backpressure
    /// when the in-memory buffer is full, waiting on the WAL writer to drain. It
    /// may return `true` regardless, because the WAL writer is independent of
    /// the page-latch world (it only writes its own segment files), so parking
    /// an append while holding a page guard cannot deadlock.
    fn appends_without_blocking_io(&self) -> bool {
        false
    }

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

    /// Return live append counters for observability views.
    fn stats(&self) -> WalSinkStats {
        WalSinkStats::default()
    }
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

    fn append_borrowed(
        &self,
        _record_type: RecordType,
        _xid: Xid,
        _prev_lsn: Lsn,
        _flags: u8,
        _payload: &[u8],
    ) -> Result<Lsn, WalSinkError> {
        Ok(Lsn::ZERO)
    }

    fn appends_without_blocking_io(&self) -> bool {
        true
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
/// This type is exported only under `#[cfg(test)]` or when the
/// `test-support` feature is enabled so that integration tests in other
/// crates (executor, recovery, …) can import it without pulling in
/// production WAL writer state.  Enable the feature in your crate's
/// dev-dependencies:
///
/// ```toml
/// [dev-dependencies]
/// ultrasql-storage = { workspace = true, features = ["testing"] }
/// ```
#[cfg(any(test, feature = "testing"))]
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
        fn next(&mut self) -> Result<Lsn, WalSinkError> {
            let next = self
                .next_lsn
                .checked_add(1)
                .ok_or_else(|| WalSinkError::Rejected("in-memory WAL LSN overflow".to_owned()))?;
            self.next_lsn = next;
            Ok(Lsn::new(next))
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
        fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
            let lsn = {
                let mut inner = self.inner.lock();
                let lsn = inner.next()?;
                let xid_raw = record.header.xid.raw();
                inner.last_lsn.insert(xid_raw, lsn);
                inner.records.push((lsn, record));
                lsn
            };
            Ok(lsn)
        }

        fn appends_without_blocking_io(&self) -> bool {
            true
        }

        fn durable_lsn(&self) -> Lsn {
            // All records in this mock are immediately "durable".
            let next_lsn = self.inner.lock().next_lsn;
            if next_lsn == 0 {
                Lsn::ZERO
            } else {
                Lsn::new(next_lsn)
            }
        }

        fn last_lsn_for(&self, xid: Xid) -> Lsn {
            self.inner
                .lock()
                .last_lsn
                .get(&xid.raw())
                .copied()
                .unwrap_or(Lsn::ZERO)
        }
    }

    /// In-memory sink whose durable LSN *lags* the appended LSN until the test
    /// explicitly advances it.
    ///
    /// Unlike [`InMemoryWalSink`] (where everything appended is immediately
    /// durable), this sink assigns LSNs monotonically from 1 but reports a
    /// `durable_lsn` that the test controls via [`Self::set_durable_lsn`]. It
    /// models a WAL writer that has accepted records into its buffer but not
    /// yet fsynced them — exactly the state the eviction-relief LSN gate must
    /// respect (a dirty page whose page-LSN exceeds `durable_lsn` must not be
    /// written).
    #[derive(Debug, Default)]
    pub struct LaggingWalSink {
        inner: Mutex<LaggingInner>,
    }

    #[derive(Debug, Default)]
    struct LaggingInner {
        next_lsn: u64,
        durable: u64,
        last_lsn: HashMap<u64, Lsn>,
    }

    impl LaggingWalSink {
        /// Construct an empty sink with `durable_lsn == 0` (nothing durable).
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Advance the reported durable LSN to `lsn` (monotonically; a lower
        /// value is ignored). Tests call this to "fsync" the WAL up to `lsn`.
        pub fn set_durable_lsn(&self, lsn: Lsn) {
            let mut inner = self.inner.lock();
            if lsn.raw() > inner.durable {
                inner.durable = lsn.raw();
            }
        }

        /// Return the highest LSN assigned so far (the next append gets
        /// `assigned + 1`).
        #[must_use]
        pub fn assigned_lsn(&self) -> Lsn {
            Lsn::new(self.inner.lock().next_lsn)
        }
    }

    impl WalSink for LaggingWalSink {
        fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
            let mut inner = self.inner.lock();
            let next = inner
                .next_lsn
                .checked_add(1)
                .ok_or_else(|| WalSinkError::Rejected("lagging WAL LSN overflow".to_owned()))?;
            inner.next_lsn = next;
            let lsn = Lsn::new(next);
            inner.last_lsn.insert(record.header.xid.raw(), lsn);
            Ok(lsn)
        }

        fn appends_without_blocking_io(&self) -> bool {
            true
        }

        fn durable_lsn(&self) -> Lsn {
            Lsn::new(self.inner.lock().durable)
        }

        fn last_lsn_for(&self, xid: Xid) -> Lsn {
            self.inner
                .lock()
                .last_lsn
                .get(&xid.raw())
                .copied()
                .unwrap_or(Lsn::ZERO)
        }
    }

    #[cfg(test)]
    mod tests {
        use ultrasql_core::{Lsn, Xid};
        use ultrasql_wal::{RecordType, WalRecord};

        use super::{InMemoryWalSink, Inner};
        use crate::WalSink;

        fn nop_record() -> WalRecord {
            WalRecord::new(RecordType::Nop, Xid::new(7), Lsn::ZERO, 0, Vec::new())
                .expect("test WAL record")
        }

        #[test]
        fn append_rejects_lsn_overflow_without_recording_duplicate() {
            let sink = InMemoryWalSink {
                inner: parking_lot::Mutex::new(Inner {
                    next_lsn: u64::MAX,
                    ..Inner::default()
                }),
            };

            let err = sink
                .append(nop_record())
                .expect_err("LSN overflow must not saturate");
            assert!(matches!(err, super::WalSinkError::Rejected(_)), "{err:?}");
            assert!(
                sink.records().is_empty(),
                "failed append must not record duplicate LSN"
            );
        }

        #[test]
        fn in_memory_sink_declares_buffered_append() {
            let sink = InMemoryWalSink::new();
            assert!(sink.appends_without_blocking_io());
        }
    }
}
