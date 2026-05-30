//! In-memory WAL append buffer.
//!
//! This is the staging area between record-emitting transactions and
//! the fsync writer. Writers append serialized records; a dedicated
//! flusher thread drains the buffer in LSN order, writes the bytes to
//! the on-disk segment, and fsyncs. Once the flusher publishes a new
//! `durable_lsn` value, every writer whose commit record's LSN is
//! `<= durable_lsn` is unblocked.
//!
//! This module provides the in-memory primitive only — segment file
//! I/O and the flusher thread will land in a follow-up that bolts on
//! `tokio::fs` / `io_uring` per platform.
//!
//! Concurrency
//! -----------
//!
//! - Appends are mutually exclusive (a single `parking_lot::Mutex`
//!   guards the buffer). The append takes microseconds; serialization
//!   is the bottleneck only at extreme throughputs and an upcoming
//!   lock-free ring will replace it.
//! - Readers (the flusher) observe a monotonically-growing
//!   `next_lsn` value via the same mutex. `durable_lsn` is published
//!   via an `AtomicU64` so observers (waiting committers) can poll it
//!   without blocking the append path.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ultrasql_core::Lsn;

use crate::record::WalRecord;

/// Errors from the WAL append path.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalBufferError {
    /// The buffer is full and the caller did not provide a wait
    /// strategy.
    #[error("wal buffer full: have {used} bytes used of {capacity}")]
    Full {
        /// Bytes currently buffered.
        used: usize,
        /// Configured capacity.
        capacity: usize,
    },
}

/// In-memory WAL append buffer.
#[derive(Debug)]
pub struct WalBuffer {
    inner: Mutex<Inner>,
    durable_lsn: AtomicU64,
    capacity: usize,
}

#[derive(Debug)]
struct Inner {
    bytes: Vec<u8>,
    next_lsn: u64,
}

impl WalBuffer {
    /// Construct a buffer with the supplied capacity in bytes. The
    /// buffer rejects appends that would overflow the capacity.
    #[must_use]
    pub fn new(capacity: usize, initial_lsn: Lsn) -> Self {
        Self {
            inner: Mutex::new(Inner {
                bytes: Vec::with_capacity(capacity),
                next_lsn: initial_lsn.raw(),
            }),
            durable_lsn: AtomicU64::new(initial_lsn.raw()),
            capacity,
        }
    }

    /// Configured capacity in bytes.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// LSN that the flusher has committed durably. Visible to every
    /// other thread via acquire ordering.
    #[must_use]
    pub fn durable_lsn(&self) -> Lsn {
        Lsn::new(self.durable_lsn.load(Ordering::Acquire))
    }

    /// Advance the buffer's next assigned LSN to at least `lsn`.
    ///
    /// Startup recovery uses this after replaying existing WAL segments and
    /// before accepting new appends, so freshly-written records cannot reuse
    /// byte positions that already exist on disk. The method is monotonic and
    /// does not truncate or drain buffered bytes.
    pub fn advance_to_lsn(&self, lsn: Lsn) {
        let raw = lsn.raw();
        {
            let mut inner = self.inner.lock();
            if inner.next_lsn < raw {
                inner.next_lsn = raw;
            }
        }
        let _ = self
            .durable_lsn
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < raw).then_some(raw)
            });
    }

    /// Append a record. Returns the LSN at which it was placed.
    ///
    /// The LSN is assigned monotonically inside the lock; callers
    /// observe a global ordering.
    pub fn append(&self, record: &WalRecord) -> Result<Lsn, WalBufferError> {
        let bytes = record.encode();
        let lsn = {
            let mut inner = self.inner.lock();
            if inner.bytes.len() + bytes.len() > self.capacity {
                return Err(WalBufferError::Full {
                    used: inner.bytes.len(),
                    capacity: self.capacity,
                });
            }
            let lsn = inner.next_lsn;
            inner.next_lsn += bytes.len() as u64;
            inner.bytes.extend_from_slice(&bytes);
            lsn
        };
        Ok(Lsn::new(lsn))
    }

    /// Drain the buffer into a single contiguous byte vector. Returns
    /// the bytes plus the LSN at which they begin and the LSN
    /// immediately after them (i.e. the next available position).
    ///
    /// In production this is called by the flusher thread on every
    /// fsync window. The buffer reuses its allocation across drains.
    pub fn drain(&self) -> DrainedBatch {
        let mut inner = self.inner.lock();
        let end_lsn = inner.next_lsn;
        let bytes = std::mem::take(&mut inner.bytes);
        drop(inner);

        let start_lsn = end_lsn - bytes.len() as u64;
        DrainedBatch {
            bytes,
            start_lsn: Lsn::new(start_lsn),
            end_lsn: Lsn::new(end_lsn),
        }
    }

    /// Record that the flusher has made all bytes up to and including
    /// `lsn` durable on disk. Subsequent calls to [`Self::durable_lsn`]
    /// observe this value under acquire ordering.
    ///
    /// `lsn` must monotonically increase across calls; callers that
    /// violate this invariant invoke a debug-build panic.
    pub fn publish_durable_lsn(&self, lsn: Lsn) {
        let prev = self.durable_lsn.load(Ordering::Relaxed);
        debug_assert!(
            lsn.raw() >= prev,
            "durable LSN must increase monotonically: {} -> {}",
            prev,
            lsn.raw()
        );
        self.durable_lsn.store(lsn.raw(), Ordering::Release);
    }

    /// Currently buffered bytes — for diagnostics and tests.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.inner.lock().bytes.len()
    }

    /// The LSN that the next appended record will receive.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        Lsn::new(self.inner.lock().next_lsn)
    }
}

/// One drain operation's worth of bytes plus the LSN range they cover.
#[derive(Debug, Clone)]
pub struct DrainedBatch {
    /// Serialized WAL record bytes, in append order.
    pub bytes: Vec<u8>,
    /// LSN of the first byte of `bytes`.
    pub start_lsn: Lsn,
    /// LSN of the first byte *after* `bytes`.
    pub end_lsn: Lsn,
}

#[cfg(test)]
mod tests {
    use ultrasql_core::Xid;

    use super::*;
    use crate::record::RecordType;

    fn rec(rt: RecordType, payload: &[u8], prev: Lsn) -> WalRecord {
        WalRecord::new(rt, Xid::new(1), prev, 0, payload.to_vec())
            .expect("test WAL record should fit size limits")
    }

    #[test]
    fn append_assigns_monotonic_lsns() {
        let buf = WalBuffer::new(64 * 1024, Lsn::new(1000));
        let a = buf
            .append(&rec(RecordType::HeapInsert, b"a", Lsn::ZERO))
            .unwrap();
        let b = buf.append(&rec(RecordType::HeapInsert, b"bb", a)).unwrap();
        let c = buf.append(&rec(RecordType::HeapInsert, b"ccc", b)).unwrap();
        assert_eq!(a, Lsn::new(1000));
        assert!(b > a);
        assert!(c > b);
    }

    #[test]
    fn advance_to_lsn_moves_next_and_durable_lsn_forward() {
        let buf = WalBuffer::new(64 * 1024, Lsn::ZERO);
        buf.advance_to_lsn(Lsn::new(4096));
        assert_eq!(buf.next_lsn(), Lsn::new(4096));
        assert_eq!(buf.durable_lsn(), Lsn::new(4096));
        let assigned = buf
            .append(&rec(RecordType::HeapInsert, b"a", Lsn::ZERO))
            .unwrap();
        assert_eq!(assigned, Lsn::new(4096));
        buf.advance_to_lsn(Lsn::new(1));
        assert!(buf.next_lsn() > Lsn::new(4096));
    }

    #[test]
    fn drain_returns_appended_bytes_in_order() {
        let buf = WalBuffer::new(64 * 1024, Lsn::new(0));
        for i in 0_u8..5 {
            buf.append(&rec(RecordType::HeapInsert, &[i], Lsn::ZERO))
                .unwrap();
        }
        let drained = buf.drain();
        // We can decode the records back out of the byte stream in
        // order to confirm the buffer is FIFO.
        let mut offset = 0;
        let mut payloads = Vec::new();
        while offset < drained.bytes.len() {
            let (rec, used) = WalRecord::decode(&drained.bytes[offset..]).unwrap();
            offset += used;
            payloads.extend_from_slice(&rec.payload);
        }
        assert_eq!(payloads, vec![0, 1, 2, 3, 4]);
        assert_eq!(drained.start_lsn, Lsn::new(0));
        assert_eq!(drained.end_lsn, Lsn::new(drained.bytes.len() as u64));
    }

    #[test]
    fn full_buffer_rejects_appends() {
        // Set capacity just under one record's worth.
        let buf = WalBuffer::new(20, Lsn::new(0));
        let err = buf
            .append(&rec(RecordType::HeapInsert, b"abc", Lsn::ZERO))
            .unwrap_err();
        assert!(matches!(err, WalBufferError::Full { .. }));
    }

    #[test]
    fn drain_resets_used_to_zero() {
        let buf = WalBuffer::new(1024, Lsn::new(0));
        buf.append(&rec(RecordType::HeapInsert, b"x", Lsn::ZERO))
            .unwrap();
        assert!(buf.buffered_bytes() > 0);
        let _ = buf.drain();
        assert_eq!(buf.buffered_bytes(), 0);
    }

    #[test]
    fn publish_durable_lsn_visible_immediately() {
        let buf = WalBuffer::new(64, Lsn::new(0));
        assert_eq!(buf.durable_lsn(), Lsn::new(0));
        buf.publish_durable_lsn(Lsn::new(42));
        assert_eq!(buf.durable_lsn(), Lsn::new(42));
    }

    #[test]
    fn next_lsn_advances_with_appends() {
        let buf = WalBuffer::new(1024, Lsn::new(100));
        assert_eq!(buf.next_lsn(), Lsn::new(100));
        let _ = buf
            .append(&rec(RecordType::HeapInsert, b"x", Lsn::ZERO))
            .unwrap();
        assert!(buf.next_lsn() > Lsn::new(100));
    }
}
