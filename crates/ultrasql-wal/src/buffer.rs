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

use crate::record::{RecordType, WalRecord, WalRecordError, append_encoded_parts_to};

/// Errors from the WAL append path.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalBufferError {
    /// The borrowed append path could not construct a valid WAL record.
    #[error("wal record rejected: {0}")]
    Record(#[from] WalRecordError),
    /// The buffer is full and the caller did not provide a wait
    /// strategy.
    #[error("wal buffer full: have {used} bytes used of {capacity}")]
    Full {
        /// Bytes currently buffered.
        used: usize,
        /// Configured capacity.
        capacity: usize,
    },
    /// The append would advance the WAL byte-position beyond `u64::MAX`.
    #[error("wal lsn overflow: current {current}, append {bytes} bytes")]
    LsnOverflow {
        /// Current next LSN.
        current: u64,
        /// Bytes in the record being appended.
        bytes: u64,
    },
    /// Buffered bytes cannot be represented as a 64-bit LSN span.
    #[error("wal buffered byte length overflow: {bytes} bytes")]
    BufferedBytesOverflow {
        /// Bytes currently being drained.
        bytes: usize,
    },
    /// The buffered byte count is larger than the recorded end LSN.
    #[error("wal lsn underflow during drain: end {end}, bytes {bytes}")]
    LsnUnderflow {
        /// Recorded end LSN.
        end: u64,
        /// Bytes being drained.
        bytes: u64,
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
        let byte_len = u64::from(record.header.total_length);
        let record_len = usize::try_from(record.header.total_length).map_err(|_| {
            WalBufferError::LsnOverflow {
                current: u64::MAX,
                bytes: byte_len,
            }
        })?;
        let lsn = {
            let mut inner = self.inner.lock();
            let used_after =
                inner
                    .bytes
                    .len()
                    .checked_add(record_len)
                    .ok_or(WalBufferError::Full {
                        used: inner.bytes.len(),
                        capacity: self.capacity,
                    })?;
            if used_after > self.capacity {
                return Err(WalBufferError::Full {
                    used: inner.bytes.len(),
                    capacity: self.capacity,
                });
            }
            let lsn = inner.next_lsn;
            inner.next_lsn =
                inner
                    .next_lsn
                    .checked_add(byte_len)
                    .ok_or(WalBufferError::LsnOverflow {
                        current: inner.next_lsn,
                        bytes: byte_len,
                    })?;
            record.append_encoded_to(&mut inner.bytes);
            lsn
        };
        Ok(Lsn::new(lsn))
    }

    /// Append a record from a borrowed payload.
    ///
    /// This produces the same bytes as [`WalRecord::new`] followed by
    /// [`Self::append`], but skips allocating an owned payload vector.
    pub fn append_borrowed(
        &self,
        record_type: RecordType,
        xid: ultrasql_core::Xid,
        prev_lsn: Lsn,
        flags: u8,
        payload: &[u8],
    ) -> Result<Lsn, WalBufferError> {
        let header =
            WalRecord::header_for_borrowed_payload(record_type, xid, prev_lsn, flags, payload)?;
        let byte_len = u64::from(header.total_length);
        let record_len =
            usize::try_from(header.total_length).map_err(|_| WalBufferError::LsnOverflow {
                current: u64::MAX,
                bytes: byte_len,
            })?;
        let lsn = {
            let mut inner = self.inner.lock();
            let used_after =
                inner
                    .bytes
                    .len()
                    .checked_add(record_len)
                    .ok_or(WalBufferError::Full {
                        used: inner.bytes.len(),
                        capacity: self.capacity,
                    })?;
            if used_after > self.capacity {
                return Err(WalBufferError::Full {
                    used: inner.bytes.len(),
                    capacity: self.capacity,
                });
            }
            let lsn = inner.next_lsn;
            inner.next_lsn =
                inner
                    .next_lsn
                    .checked_add(byte_len)
                    .ok_or(WalBufferError::LsnOverflow {
                        current: inner.next_lsn,
                        bytes: byte_len,
                    })?;
            append_encoded_parts_to(&header, payload, &mut inner.bytes);
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
    pub fn drain(&self) -> Result<DrainedBatch, WalBufferError> {
        let mut bytes = Vec::new();
        let range = self.drain_into(&mut bytes)?;
        Ok(DrainedBatch {
            bytes,
            start_lsn: range.start_lsn,
            end_lsn: range.end_lsn,
        })
    }

    /// Drain the buffer into `bytes`, swapping the caller's empty allocation
    /// back into the append path.
    ///
    /// The WAL writer owns a reusable drain buffer and calls this method on
    /// every loop. Swapping avoids leaving the foreground append vector at
    /// zero capacity after a drain, which would otherwise make the next burst
    /// of WAL appends pay allocator growth under the append mutex.
    pub fn drain_into(&self, bytes: &mut Vec<u8>) -> Result<DrainedRange, WalBufferError> {
        bytes.clear();
        let mut inner = self.inner.lock();
        let end_lsn = inner.next_lsn;
        std::mem::swap(&mut inner.bytes, bytes);
        drop(inner);

        let byte_len = u64::try_from(bytes.len())
            .map_err(|_| WalBufferError::BufferedBytesOverflow { bytes: bytes.len() })?;
        let start_lsn = end_lsn
            .checked_sub(byte_len)
            .ok_or(WalBufferError::LsnUnderflow {
                end: end_lsn,
                bytes: byte_len,
            })?;
        Ok(DrainedRange {
            start_lsn: Lsn::new(start_lsn),
            end_lsn: Lsn::new(end_lsn),
        })
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

/// LSN span of bytes drained into a caller-owned buffer.
#[derive(Debug, Clone, Copy)]
pub struct DrainedRange {
    /// LSN of the first byte of the drained byte buffer.
    pub start_lsn: Lsn,
    /// LSN of the first byte after the drained byte buffer.
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
    fn append_borrowed_payload_matches_record_append() {
        let payload = b"payload bytes";
        let record = rec(
            RecordType::HeapDeleteInPlaceRangeBatch,
            payload,
            Lsn::new(7),
        );
        let expected = record.encode();

        let buf = WalBuffer::new(64 * 1024, Lsn::new(1000));
        let lsn = buf
            .append_borrowed(
                RecordType::HeapDeleteInPlaceRangeBatch,
                Xid::new(1),
                Lsn::new(7),
                0,
                payload,
            )
            .unwrap();

        assert_eq!(lsn, Lsn::new(1000));
        assert_eq!(buf.drain().unwrap().bytes, expected);
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
        let drained = buf.drain().expect("drain succeeds");
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
        let drained_len = u64::try_from(drained.bytes.len()).expect("drained length fits u64");
        assert_eq!(drained.end_lsn, Lsn::new(drained_len));
    }

    #[test]
    fn drain_rejects_inconsistent_lsn_span_without_panicking() {
        let buf = WalBuffer::new(64 * 1024, Lsn::new(0));
        buf.append(&rec(RecordType::HeapInsert, b"x", Lsn::ZERO))
            .unwrap();
        let buffered = buf.buffered_bytes();
        {
            let mut inner = buf.inner.lock();
            inner.next_lsn = 0;
        }

        let err = buf
            .drain()
            .expect_err("corrupt buffered LSN span should return a typed error");
        assert!(
            matches!(
                err,
                WalBufferError::LsnUnderflow { end, bytes }
                    if end == 0 && bytes == u64::try_from(buffered).unwrap()
            ),
            "{err:?}"
        );
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
    fn append_rejects_lsn_overflow_without_buffering_bytes() {
        let record = rec(RecordType::HeapInsert, b"abc", Lsn::ZERO);
        let record_len = u64::try_from(record.encode().len()).expect("record length fits u64");
        let initial = Lsn::new(u64::MAX - record_len + 1);
        let buf = WalBuffer::new(64 * 1024, initial);

        let err = buf
            .append(&record)
            .expect_err("LSN overflow must return Err");
        assert!(
            matches!(
                err,
                WalBufferError::LsnOverflow { current, bytes }
                    if current == initial.raw() && bytes == record_len
            ),
            "{err:?}"
        );
        assert_eq!(
            buf.buffered_bytes(),
            0,
            "failed append must not buffer bytes"
        );
        assert_eq!(
            buf.next_lsn(),
            initial,
            "failed append must not advance LSN"
        );
    }

    #[test]
    fn drain_resets_used_to_zero() {
        let buf = WalBuffer::new(1024, Lsn::new(0));
        buf.append(&rec(RecordType::HeapInsert, b"x", Lsn::ZERO))
            .unwrap();
        assert!(buf.buffered_bytes() > 0);
        let _ = buf.drain().expect("drain succeeds");
        assert_eq!(buf.buffered_bytes(), 0);
    }

    #[test]
    fn drain_into_uses_caller_buffer_and_leaves_append_path_usable() {
        let buf = WalBuffer::new(1024, Lsn::new(0));
        let first = rec(RecordType::HeapInsert, b"x", Lsn::ZERO);
        let first_len = u64::try_from(first.encode().len()).expect("record length fits");
        buf.append(&first).unwrap();

        let mut drained = Vec::with_capacity(1024);
        let range = buf.drain_into(&mut drained).expect("drain succeeds");
        assert_eq!(range.start_lsn, Lsn::ZERO);
        assert_eq!(range.end_lsn, Lsn::new(first_len));
        assert!(!drained.is_empty());
        assert_eq!(buf.buffered_bytes(), 0);

        drained.clear();
        buf.append(&rec(RecordType::HeapInsert, b"y", Lsn::ZERO))
            .unwrap();
        assert!(buf.buffered_bytes() > 0);
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
