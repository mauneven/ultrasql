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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, MutexGuard};
use ultrasql_core::Lsn;

use crate::record::{RecordType, WalRecord, WalRecordError, append_encoded_parts_to};

/// Wakes the buffer's drainer (the WAL writer) so it frees capacity promptly
/// when an appender is blocked applying backpressure.
///
/// The WAL writer registers itself through [`WalBuffer::set_drainer`]. When an
/// append finds the buffer transiently full it calls [`Self::request_drain`]
/// before parking, so the writer drains on its next loop instead of waiting out
/// its group-commit window. The signal is best-effort: a buffer with no drainer
/// registered falls back to rejecting a full append (see [`WalBuffer::append`]),
/// which preserves the historical behaviour for bare buffers in tests.
pub trait WalDrainSignal: Send + Sync {
    /// Ask the drainer to drain the buffer without waiting out its window.
    fn request_drain(&self);
}

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
    /// The buffer was closed (the WAL writer stopped) while an append was
    /// waiting for capacity. No further records can be accepted.
    #[error("wal buffer closed: writer stopped")]
    Closed,
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
pub struct WalBuffer {
    inner: Mutex<Inner>,
    /// Signalled whenever a drain frees capacity, so appenders parked on
    /// backpressure can retry.
    space_available: Condvar,
    /// Registered drainer (the WAL writer) used to request a prompt drain when
    /// an appender is parked. `None` until a writer wires itself up; while it is
    /// `None` a full append is rejected instead of waiting.
    drainer: Mutex<Option<Arc<dyn WalDrainSignal>>>,
    /// Set once the writer has stopped; unblocks any parked appenders so they
    /// fail fast instead of waiting forever for a drain that will never come.
    closed: AtomicBool,
    durable_lsn: AtomicU64,
    capacity: usize,
}

impl std::fmt::Debug for WalBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalBuffer")
            .field("inner", &self.inner)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("durable_lsn", &self.durable_lsn())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
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
            space_available: Condvar::new(),
            drainer: Mutex::new(None),
            closed: AtomicBool::new(false),
            durable_lsn: AtomicU64::new(initial_lsn.raw()),
            capacity,
        }
    }

    /// Configured capacity in bytes.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Register the drainer (the WAL writer) that frees buffer capacity.
    ///
    /// Once a drainer is set, an append that finds the buffer transiently full
    /// applies backpressure — it parks until the drainer frees space — instead
    /// of returning [`WalBufferError::Full`]. Without a drainer (e.g. a bare
    /// buffer in a unit test) a full append is rejected as before.
    pub fn set_drainer(&self, drainer: Arc<dyn WalDrainSignal>) {
        *self.drainer.lock() = Some(drainer);
    }

    /// Mark the buffer closed and wake every parked appender.
    ///
    /// The WAL writer calls this when it stops — clean shutdown, fatal error, or
    /// panic — so appenders parked on backpressure observe
    /// [`WalBufferError::Closed`] rather than waiting forever for a drain that
    /// will never arrive. Idempotent.
    pub fn close_appends(&self) {
        self.closed.store(true, Ordering::Release);
        self.space_available.notify_all();
    }

    /// Defensive re-check interval for a parked appender. Backpressure wakeups
    /// are signalled precisely on every drain, so this bounded wait is only a
    /// backstop: it lets a parked appender re-poke the drainer and re-observe
    /// `closed` even in the (not-expected) event that a wakeup is missed.
    const BACKPRESSURE_RECHECK: Duration = Duration::from_millis(100);

    /// Acquire the append lock with at least `record_len` bytes of free
    /// capacity, applying backpressure if necessary.
    ///
    /// Returns the held [`Inner`] guard ready for the caller to assign an LSN
    /// and encode into. Behaviour when the record does not currently fit:
    ///
    /// - With a drainer registered, a record larger than the whole buffer is
    ///   admitted once the buffer is empty — written over-capacity (bounded by
    ///   the 64 MiB [`MAX_RECORD_BYTES`](crate::record::MAX_RECORD_BYTES)
    ///   record ceiling) — so a wide row or page flows through as backpressure
    ///   instead of failing with [`WalBufferError::Full`].
    /// - A *transiently* full buffer parks the caller until a drain frees space
    ///   when a drainer is registered.
    /// - With NO drainer registered, a full buffer (transient or oversized) is
    ///   rejected with [`WalBufferError::Full`]: there is nothing to drain it,
    ///   so parking would hang. This preserves the bare-buffer contract.
    /// - A buffer whose writer has stopped returns [`WalBufferError::Closed`].
    ///
    /// Backpressure performs no filesystem I/O and acquires no buffer-pool page
    /// latch or storage lock — it only waits on the WAL writer, which is
    /// independent of the page-latch world. Holding a page guard across this
    /// wait is therefore deadlock-free, which is what lets heap callers keep
    /// appending page-local records under their page guard.
    fn reserve(&self, record_len: usize) -> Result<MutexGuard<'_, Inner>, WalBufferError> {
        let mut inner = self.inner.lock();
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(WalBufferError::Closed);
            }
            let used = inner.bytes.len();
            if used
                .checked_add(record_len)
                .is_some_and(|needed| needed <= self.capacity)
            {
                return Ok(inner);
            }
            // The record does not fit in the remaining space. Only the WAL
            // writer (the registered drainer) can free space; without one,
            // preserve the historical reject-on-full contract for bare buffers.
            let drainer = self.drainer.lock().clone();
            let Some(drainer) = drainer else {
                return Err(WalBufferError::Full {
                    used,
                    capacity: self.capacity,
                });
            };
            // An empty buffer can always make progress on a single record — even
            // one wider than the whole buffer (a legal record up to
            // `MAX_RECORD_BYTES`). Admit it over-capacity rather than rejecting,
            // so a wide row or page flows through as backpressure instead of
            // failing with `Full`; the drainer writes and fsyncs it next and
            // reclaims the allocation. This is what lets a durable bulk write
            // whose largest record exceeds the buffer succeed instead of erroring.
            if used == 0 {
                return Ok(inner);
            }
            // Non-empty and the record does not fit: ask the writer to drain and
            // park until space frees. For a record wider than the buffer this
            // waits until the buffer fully empties, then the `used == 0` branch
            // above admits it. The bounded wait re-pokes/re-checks defensively.
            drainer.request_drain();
            self.space_available
                .wait_for(&mut inner, Self::BACKPRESSURE_RECHECK);
        }
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
            let mut inner = self.reserve(record_len)?;
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
            let mut inner = self.reserve(record_len)?;
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

    /// Append a record from a borrowed payload with its per-transaction
    /// chain link resolved atomically with LSN assignment.
    ///
    /// `link` holds the raw LSN of the transaction's previous record (0 for
    /// none). The header's `prev_lsn` is read from `link` and the newly
    /// assigned LSN stored back **inside the append critical section**, so
    /// any number of concurrent appenders sharing one `link` still produce a
    /// strictly linear per-transaction chain — without any lock beyond the
    /// buffer mutex every append already takes.
    pub fn append_borrowed_linked(
        &self,
        record_type: RecordType,
        xid: ultrasql_core::Xid,
        flags: u8,
        payload: &[u8],
        link: &std::sync::atomic::AtomicU64,
    ) -> Result<Lsn, WalBufferError> {
        // Compute the record length from a placeholder header: `prev_lsn`
        // does not affect the encoded length, only the header bytes.
        let probe =
            WalRecord::header_for_borrowed_payload(record_type, xid, Lsn::ZERO, flags, payload)?;
        let byte_len = u64::from(probe.total_length);
        let record_len =
            usize::try_from(probe.total_length).map_err(|_| WalBufferError::LsnOverflow {
                current: u64::MAX,
                bytes: byte_len,
            })?;
        let lsn = {
            let mut inner = self.reserve(record_len)?;
            let prev = Lsn::new(link.load(Ordering::Acquire));
            let header =
                WalRecord::header_for_borrowed_payload(record_type, xid, prev, flags, payload)?;
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
            link.store(lsn, Ordering::Release);
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

        // Freeing capacity unblocks appenders parked on backpressure. Only
        // signal when this drain actually moved bytes out; an empty drain
        // changes nothing, and no appender can be parked on an empty buffer.
        if !bytes.is_empty() {
            self.space_available.notify_all();
        }

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

    /// A drainer that only counts pokes; the test thread performs the drain
    /// that actually frees space.
    struct CountingDrainer(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl WalDrainSignal for CountingDrainer {
        fn request_drain(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn backpressure_parks_append_until_a_drain_frees_space() {
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        // 64 bytes fits one ~45-byte record but not two, so the second append
        // cannot proceed until a drain frees space.
        let buf = Arc::new(WalBuffer::new(64, Lsn::ZERO));
        let pokes = Arc::new(AtomicUsize::new(0));
        buf.set_drainer(Arc::new(CountingDrainer(Arc::clone(&pokes))));

        buf.append(&rec(
            RecordType::HeapInsert,
            b"first-record-payload",
            Lsn::ZERO,
        ))
        .expect("first append fits");
        assert!(buf.buffered_bytes() > 0);

        let appender_buf = Arc::clone(&buf);
        let appender = thread::spawn(move || {
            appender_buf
                .append(&rec(
                    RecordType::HeapInsert,
                    b"second-record-payload",
                    Lsn::ZERO,
                ))
                .map(|_| ())
        });

        // The test thread deliberately does NOT drain until the appender has
        // parked (it pokes the drainer immediately before parking), which makes
        // the park deterministic rather than timing-dependent.
        while pokes.load(Ordering::Relaxed) == 0 {
            thread::yield_now();
        }
        // Free space; loop only to be robust against a park/notify race.
        while !appender.is_finished() {
            let _ = buf.drain().expect("drain succeeds");
            thread::sleep(Duration::from_millis(1));
        }
        appender
            .join()
            .expect("appender thread joins")
            .expect("second append succeeds via backpressure rather than erroring");
    }

    #[test]
    fn close_appends_releases_parked_appender_with_closed_error() {
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let buf = Arc::new(WalBuffer::new(64, Lsn::ZERO));
        let pokes = Arc::new(AtomicUsize::new(0));
        buf.set_drainer(Arc::new(CountingDrainer(Arc::clone(&pokes))));
        buf.append(&rec(
            RecordType::HeapInsert,
            b"first-record-payload",
            Lsn::ZERO,
        ))
        .expect("first append fits");

        let appender_buf = Arc::clone(&buf);
        let appender = thread::spawn(move || {
            appender_buf.append(&rec(
                RecordType::HeapInsert,
                b"second-record-payload",
                Lsn::ZERO,
            ))
        });

        // Wait until the appender parks, then close the buffer (never drain).
        while pokes.load(Ordering::Relaxed) == 0 {
            thread::yield_now();
        }
        while !appender.is_finished() {
            buf.close_appends();
            thread::sleep(Duration::from_millis(1));
        }

        let err = appender
            .join()
            .expect("appender thread joins")
            .expect_err("append on a closed buffer must fail rather than hang");
        assert!(matches!(err, WalBufferError::Closed), "{err:?}");
    }

    #[test]
    fn concurrent_linked_appends_produce_one_strictly_linear_chain() {
        use std::sync::atomic::AtomicU64;
        use std::thread;

        // 4 workers x 64 linked appends for ONE xid, racing on one link
        // cell. Decoding the buffer afterwards must yield a single linear
        // prev_lsn chain covering every record exactly once — the invariant
        // the parallel bulk-mutation paths rely on instead of a chain mutex.
        // Non-zero initial LSN, like a real WAL: `prev == 0` is the
        // "no previous record" sentinel and must never collide with a
        // record's own address.
        let buf = Arc::new(WalBuffer::new(1024 * 1024, Lsn::new(4096)));
        let link = Arc::new(AtomicU64::new(0));
        let xid = Xid::new(9);

        thread::scope(|scope| {
            for worker in 0_u8..4 {
                let buf = Arc::clone(&buf);
                let link = Arc::clone(&link);
                scope.spawn(move || {
                    for i in 0_u8..64 {
                        buf.append_borrowed_linked(
                            RecordType::HeapInsert,
                            xid,
                            0,
                            &[worker, i],
                            &link,
                        )
                        .expect("linked append succeeds");
                    }
                });
            }
        });

        let drained = buf.drain().expect("drain");
        let mut offset = 0;
        let mut by_lsn = std::collections::BTreeMap::new();
        let mut lsn = 4096_u64;
        while offset < drained.bytes.len() {
            let (rec, used) = WalRecord::decode(&drained.bytes[offset..]).unwrap();
            by_lsn.insert(lsn, rec.header.prev_lsn.raw());
            lsn += u64::from(rec.header.total_length);
            offset += used;
        }
        assert_eq!(by_lsn.len(), 256, "every append decoded");
        // Walk the chain from the link's final head back to the start:
        // it must visit every record exactly once and terminate at 0.
        let mut cursor = link.load(Ordering::Acquire);
        let mut visited = 0_usize;
        while cursor != 0 || (visited < 256 && by_lsn.contains_key(&cursor)) {
            let Some(&prev) = by_lsn.get(&cursor) else {
                panic!("chain cursor {cursor} does not point at a record");
            };
            visited += 1;
            if visited > 256 {
                panic!("chain revisits records (fork or cycle)");
            }
            if prev == cursor {
                panic!("self-referential chain link");
            }
            cursor = prev;
            if visited == 256 {
                break;
            }
        }
        assert_eq!(visited, 256, "chain covers every record exactly once");
        assert_eq!(cursor, 0, "chain terminates at the pre-append link value");
    }

    #[test]
    fn transient_full_without_drainer_still_rejects() {
        // No drainer registered → preserve the historical reject-on-full
        // contract for bare buffers so nothing parks forever.
        let buf = WalBuffer::new(64, Lsn::ZERO);
        buf.append(&rec(
            RecordType::HeapInsert,
            b"first-record-payload",
            Lsn::ZERO,
        ))
        .expect("first append fits");
        let err = buf
            .append(&rec(
                RecordType::HeapInsert,
                b"second-record-payload",
                Lsn::ZERO,
            ))
            .expect_err("a transiently full buffer with no drainer rejects");
        assert!(matches!(err, WalBufferError::Full { .. }), "{err:?}");
    }
}
