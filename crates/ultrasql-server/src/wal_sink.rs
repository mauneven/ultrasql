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
use ultrasql_core::{Lsn, Xid};
use ultrasql_storage::{WalSink, WalSinkError, WalSinkStats};
use ultrasql_wal::{RecordType, WalBuffer, WalRecord};

/// Wraps an [`Arc<WalBuffer>`] and adds per-XID LSN tracking so the
/// storage layer can chain WAL records into a per-transaction log.
pub struct WalBufferSink {
    buffer: Arc<WalBuffer>,
    /// Last LSN assigned to each XID, updated on every successful append.
    last_lsn: DashMap<u64, Lsn>,
    /// Cumulative records accepted by this sink.
    wal_records: AtomicU64,
    /// Cumulative full-page-write records accepted by this sink.
    wal_fpi: AtomicU64,
    /// Cumulative serialized bytes accepted by this sink.
    wal_bytes: AtomicU64,
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
            wal_records: AtomicU64::new(0),
            wal_fpi: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
        }
    }
}

impl WalSink for WalBufferSink {
    fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
        let xid = record.header.xid;
        let total_length = u64::from(record.header.total_length);
        let is_fpi = record.header.record_type == RecordType::FullPageWrite;
        let lsn = self
            .buffer
            .append(&record)
            .map_err(|e| WalSinkError::Rejected(format!("WalBuffer rejected record: {e}")))?;
        self.last_lsn.insert(xid.raw(), lsn);
        self.wal_records.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes.fetch_add(total_length, Ordering::Relaxed);
        if is_fpi {
            self.wal_fpi.fetch_add(1, Ordering::Relaxed);
        }
        Ok(lsn)
    }

    fn durable_lsn(&self) -> Lsn {
        self.buffer.durable_lsn()
    }

    fn last_lsn_for(&self, xid: Xid) -> Lsn {
        self.last_lsn
            .get(&xid.raw())
            .map(|r| *r.value())
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
}
