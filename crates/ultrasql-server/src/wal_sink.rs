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

use dashmap::DashMap;
use ultrasql_core::{Lsn, Xid};
use ultrasql_storage::{WalSink, WalSinkError};
use ultrasql_wal::{WalBuffer, WalRecord};

/// Wraps an [`Arc<WalBuffer>`] and adds per-XID LSN tracking so the
/// storage layer can chain WAL records into a per-transaction log.
#[allow(missing_debug_implementations)] // DashMap does not impl Debug
pub struct WalBufferSink {
    buffer: Arc<WalBuffer>,
    /// Last LSN assigned to each XID, updated on every successful append.
    last_lsn: DashMap<u64, Lsn>,
}

impl WalBufferSink {
    /// Create a new sink backed by `buffer`.
    pub fn new(buffer: Arc<WalBuffer>) -> Self {
        Self {
            buffer,
            last_lsn: DashMap::new(),
        }
    }
}

impl WalSink for WalBufferSink {
    fn append(&self, record: WalRecord) -> Result<Lsn, WalSinkError> {
        let xid = record.header.xid;
        let lsn = self
            .buffer
            .append(&record)
            .map_err(|e| WalSinkError::Rejected(format!("WalBuffer rejected record: {e}")))?;
        self.last_lsn.insert(xid.raw(), lsn);
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
}
