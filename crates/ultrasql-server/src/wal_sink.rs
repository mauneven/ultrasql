//! [`WalBufferSink`] — bridge between [`ultrasql_wal::WalBuffer`] and the
//! [`ultrasql_storage::WalSink`] trait.
//!
//! The storage crate defines [`WalSink`] as a narrow interface ("here is a
//! fully-formed WAL record") without importing the WAL writer. The WAL crate
//! defines [`WalBuffer`] as a lock-guarded ring buffer that the background
//! flusher thread drains to disk. This module is the only place where both
//! crates are visible, so the glue lives here.
//!
//! # Per-XID LSN tracking
//!
//! [`WalSink::last_lsn_for`] is needed so the heap can fill `prev_lsn` in
//! every WAL record, forming a per-transaction linked list. `WalBuffer`
//! itself does not track this; `WalBufferSink` maintains a `DashMap<Xid,
//! Lsn>` for the purpose.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;
use ultrasql_core::{Lsn, Xid};
use ultrasql_storage::{WalSink, WalSinkError, WalSinkStats};
use ultrasql_wal::{RECORD_HEADER_SIZE, RecordType, WalBuffer, WalRecord};

/// Wraps an [`Arc<WalBuffer>`] and adds per-XID LSN tracking so the
/// storage layer can chain WAL records into a per-transaction log.
pub struct WalBufferSink {
    buffer: Arc<WalBuffer>,
    /// Last LSN assigned to each XID, updated on every successful append.
    last_lsn: DashMap<u64, AtomicU64>,
    /// First LSN ever assigned to each *normal* (user) XID. Used at checkpoint
    /// to bound WAL truncation: a still-in-progress transaction's earliest
    /// record must never be recycled, or recovery would lose the records that
    /// let it resolve the transaction's status (an unknown XID defaults to
    /// `InProgress` forever). Entries for resolved transactions are pruned by
    /// [`Self::prune_terminal_and_oldest_active_first_lsn`] at each checkpoint.
    first_lsn: DashMap<u64, AtomicU64>,
    /// Single-XID hot cache for page-local WAL bursts from one transaction.
    last_lsn_cache: LastLsnCache,
    /// Serializes rare hot-cache XID switches.
    last_lsn_switch: Mutex<()>,
    /// Cumulative records accepted by this sink.
    wal_records: AtomicU64,
    /// Cumulative full-page-write records accepted by this sink.
    wal_fpi: AtomicU64,
    /// Cumulative serialized bytes accepted by this sink.
    wal_bytes: AtomicU64,
}

struct LastLsnCache {
    xid_raw: AtomicU64,
    lsn_raw: AtomicU64,
}

impl LastLsnCache {
    fn new() -> Self {
        Self {
            xid_raw: AtomicU64::new(0),
            lsn_raw: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Debug for WalBufferSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("WalBufferSink")
            .field("buffer", &self.buffer)
            .field("last_lsn_entries", &self.last_lsn.len())
            .field("wal_records", &stats.wal_records)
            .field("wal_fpi", &stats.wal_fpi)
            .field("wal_bytes", &stats.wal_bytes)
            .finish()
    }
}

impl WalBufferSink {
    /// Create a new sink backed by `buffer`.
    pub fn new(buffer: Arc<WalBuffer>) -> Self {
        Self {
            buffer,
            last_lsn: DashMap::new(),
            first_lsn: DashMap::new(),
            last_lsn_cache: LastLsnCache::new(),
            last_lsn_switch: Mutex::new(()),
            wal_records: AtomicU64::new(0),
            wal_fpi: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
        }
    }

    /// Record the earliest LSN seen for a normal (user) transaction. `fetch_min`
    /// keeps the true minimum even if a transaction's records are appended out of
    /// order across threads (the stored value is always a lower bound on the
    /// transaction's WAL footprint). Bootstrap/frozen/INVALID XIDs are skipped:
    /// they never sit in-progress across a checkpoint and must not pin the floor
    /// (the checkpoint barrier/Nop records carry `Xid::INVALID`).
    fn record_first_lsn(&self, xid: Xid, lsn: Lsn) {
        if !xid.is_normal() {
            return;
        }
        self.first_lsn
            .entry(xid.raw())
            .or_insert_with(|| AtomicU64::new(lsn.raw()))
            .fetch_min(lsn.raw(), Ordering::AcqRel);
    }

    /// Prune `first_lsn` entries for transactions that are no longer in progress
    /// and return the oldest first-LSN among those still active.
    ///
    /// `is_active` is the in-progress predicate (the checkpoint passes a CLOG
    /// status check). Pruning here makes correctness independent of any explicit
    /// per-commit cleanup: a resolved transaction's stale entry can never pin the
    /// truncation floor because it is dropped the next time this runs. Returns
    /// `None` when no in-progress transaction has written anything — then only the
    /// redo point bounds the floor.
    pub fn prune_terminal_and_oldest_active_first_lsn(
        &self,
        is_active: impl Fn(Xid) -> bool,
    ) -> Option<Lsn> {
        let mut oldest: Option<u64> = None;
        self.first_lsn.retain(|&xid_raw, lsn| {
            if is_active(Xid::new(xid_raw)) {
                let value = lsn.load(Ordering::Acquire);
                oldest = Some(oldest.map_or(value, |current| current.min(value)));
                true
            } else {
                false
            }
        });
        oldest.map(Lsn::new)
    }

    fn publish_last_lsn(&self, xid: Xid, lsn: Lsn) {
        let xid_raw = xid.raw();
        let lsn_raw = lsn.raw();
        if self.last_lsn_cache.xid_raw.load(Ordering::Acquire) == xid_raw {
            self.last_lsn_cache
                .lsn_raw
                .fetch_max(lsn_raw, Ordering::AcqRel);
            return;
        }

        let _switch = self.last_lsn_switch.lock();
        let current_xid = self.last_lsn_cache.xid_raw.load(Ordering::Acquire);
        if current_xid == xid_raw {
            self.last_lsn_cache
                .lsn_raw
                .fetch_max(lsn_raw, Ordering::AcqRel);
            return;
        }

        if current_xid != 0 {
            let current_lsn = self.last_lsn_cache.lsn_raw.load(Ordering::Acquire);
            self.last_lsn
                .entry(current_xid)
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_max(current_lsn, Ordering::AcqRel);
        }
        let prior_lsn = self
            .last_lsn
            .get(&xid_raw)
            .map(|stored| stored.load(Ordering::Acquire))
            .unwrap_or(0);
        self.last_lsn_cache
            .lsn_raw
            .store(prior_lsn.max(lsn_raw), Ordering::Release);
        self.last_lsn_cache
            .xid_raw
            .store(xid_raw, Ordering::Release);
    }
}

impl WalSink for WalBufferSink {
    fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
        self.append_ref(&record)
    }

    fn append_ref(&self, record: &WalRecord) -> Result<Lsn, WalSinkError> {
        let xid = record.header.xid;
        let total_length = u64::from(record.header.total_length);
        let is_fpi = record.header.record_type == RecordType::FullPageWrite;
        let lsn = self
            .buffer
            .append(record)
            .map_err(|e| WalSinkError::Rejected(format!("WalBuffer rejected record: {e}")))?;
        self.publish_last_lsn(xid, lsn);
        self.record_first_lsn(xid, lsn);
        self.wal_records.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes.fetch_add(total_length, Ordering::Relaxed);
        if is_fpi {
            self.wal_fpi.fetch_add(1, Ordering::Relaxed);
        }
        Ok(lsn)
    }

    fn append_borrowed(
        &self,
        record_type: RecordType,
        xid: Xid,
        prev_lsn: Lsn,
        flags: u8,
        payload: &[u8],
    ) -> Result<Lsn, WalSinkError> {
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            WalSinkError::Rejected("borrowed WAL payload length overflow".to_owned())
        })?;
        let header_len = u64::try_from(RECORD_HEADER_SIZE)
            .map_err(|_| WalSinkError::Rejected("WAL header length overflow".to_owned()))?;
        let total_length = header_len
            .checked_add(payload_len)
            .ok_or_else(|| WalSinkError::Rejected("borrowed WAL length overflow".to_owned()))?;
        let is_fpi = record_type == RecordType::FullPageWrite;
        let lsn = self
            .buffer
            .append_borrowed(record_type, xid, prev_lsn, flags, payload)
            .map_err(|e| WalSinkError::Rejected(format!("WalBuffer rejected record: {e}")))?;
        self.publish_last_lsn(xid, lsn);
        self.record_first_lsn(xid, lsn);
        self.wal_records.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes.fetch_add(total_length, Ordering::Relaxed);
        if is_fpi {
            self.wal_fpi.fetch_add(1, Ordering::Relaxed);
        }
        Ok(lsn)
    }

    fn appends_without_blocking_io(&self) -> bool {
        true
    }

    fn durable_lsn(&self) -> Lsn {
        self.buffer.durable_lsn()
    }

    fn last_lsn_for(&self, xid: Xid) -> Lsn {
        if self.last_lsn_cache.xid_raw.load(Ordering::Acquire) == xid.raw() {
            return Lsn::new(self.last_lsn_cache.lsn_raw.load(Ordering::Acquire));
        }
        self.last_lsn
            .get(&xid.raw())
            .map(|r| Lsn::new(r.load(Ordering::Acquire)))
            .unwrap_or(Lsn::ZERO)
    }

    fn stats(&self) -> WalSinkStats {
        let wal_records = self.wal_records.load(Ordering::Relaxed);
        WalSinkStats {
            wal_records,
            wal_fpi: self.wal_fpi.load(Ordering::Relaxed),
            wal_bytes: self.wal_bytes.load(Ordering::Relaxed),
            wal_write: wal_records,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_wal::WalRecord;

    #[test]
    fn wal_buffer_sink_stats_track_records_bytes_and_fpi() {
        let buffer = Arc::new(WalBuffer::new(4096, Lsn::ZERO));
        let sink = WalBufferSink::new(buffer);
        let heap_record = WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(7),
            Lsn::ZERO,
            0,
            b"row".to_vec(),
        )
        .expect("test WAL record should fit size limits");
        let fpi_record = WalRecord::new(
            RecordType::FullPageWrite,
            Xid::new(7),
            Lsn::ZERO,
            0,
            vec![0; 32],
        )
        .expect("test WAL record should fit size limits");
        let expected_bytes =
            u64::from(heap_record.header.total_length) + u64::from(fpi_record.header.total_length);

        sink.append(heap_record).expect("append heap record");
        sink.append(fpi_record).expect("append fpi record");

        assert_eq!(
            sink.stats(),
            WalSinkStats {
                wal_records: 2,
                wal_fpi: 1,
                wal_bytes: expected_bytes,
                wal_write: 2,
            }
        );
    }

    #[test]
    fn wal_buffer_sink_last_lsn_cache_survives_xid_switches() {
        let buffer = Arc::new(WalBuffer::new(4096, Lsn::ZERO));
        let sink = WalBufferSink::new(buffer);
        let xid7 = Xid::new(7);
        let xid8 = Xid::new(8);

        let first = sink.append(nop_record_for(xid7)).expect("append xid7");
        assert_eq!(sink.last_lsn_for(xid7), first);

        let second = sink.append(nop_record_for(xid8)).expect("append xid8");
        assert_eq!(sink.last_lsn_for(xid7), first);
        assert_eq!(sink.last_lsn_for(xid8), second);

        let third = sink
            .append(nop_record_for(xid7))
            .expect("append xid7 again");
        assert_eq!(sink.last_lsn_for(xid7), third);
        assert_eq!(sink.last_lsn_for(xid8), second);
    }

    fn nop_record_for(xid: Xid) -> WalRecord {
        WalRecord::new(RecordType::Nop, xid, Lsn::ZERO, 0, Vec::new())
            .expect("test WAL record should fit size limits")
    }

    #[test]
    fn first_lsn_keeps_the_earliest_per_normal_xid() {
        let buffer = Arc::new(WalBuffer::new(4096, Lsn::ZERO));
        let sink = WalBufferSink::new(buffer);
        let xid = Xid::new(7);

        let first = sink.append(nop_record_for(xid)).expect("first append");
        let _second = sink.append(nop_record_for(xid)).expect("second append");

        // Everything is active → the oldest first-LSN is this xid's first record.
        let oldest = sink.prune_terminal_and_oldest_active_first_lsn(|_| true);
        assert_eq!(oldest, Some(first));
    }

    #[test]
    fn first_lsn_ignores_non_normal_xids() {
        let buffer = Arc::new(WalBuffer::new(4096, Lsn::ZERO));
        let sink = WalBufferSink::new(buffer);
        // INVALID (checkpoint barrier) and BOOTSTRAP must never pin the floor.
        sink.append(nop_record_for(Xid::INVALID)).expect("invalid");
        sink.append(nop_record_for(Xid::BOOTSTRAP))
            .expect("bootstrap");
        assert_eq!(
            sink.prune_terminal_and_oldest_active_first_lsn(|_| true),
            None
        );
    }

    #[test]
    fn pruning_drops_resolved_transactions_and_returns_oldest_active() {
        let buffer = Arc::new(WalBuffer::new(4096, Lsn::ZERO));
        let sink = WalBufferSink::new(buffer);
        let resolved = Xid::new(7);
        let active = Xid::new(8);

        let resolved_first = sink.append(nop_record_for(resolved)).expect("resolved");
        let active_first = sink.append(nop_record_for(active)).expect("active");
        assert!(resolved_first < active_first);

        // Only xid 8 is still in progress: the resolved (older) xid is pruned and
        // must not pin the floor; the oldest active is xid 8's first record.
        let oldest = sink.prune_terminal_and_oldest_active_first_lsn(|xid| xid == active);
        assert_eq!(oldest, Some(active_first));

        // The resolved entry is gone, so a later pass sees only the active one.
        let oldest_again = sink.prune_terminal_and_oldest_active_first_lsn(|_| true);
        assert_eq!(oldest_again, Some(active_first));
    }
}
