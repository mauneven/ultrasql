//! Static hash index with fixed primary bucket pages and overflow chains.
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{HashOpKind, HashOpPayload};
use ultrasql_wal::record::RecordType;

use super::{AccessMethod, AccessMethodError};
use crate::wal_sink::WalSink;

// ---------------------------------------------------------------------------
// Hash index (static hashing with overflow pages)
// ---------------------------------------------------------------------------

/// Page-shaped hash index using static buckets and overflow chains.
///
/// Each bucket starts with one primary page. When the primary page is full,
/// insert walks or extends a singly-linked overflow-page chain. The number of
/// primary buckets is fixed at construction; a future dynamic policy can layer
/// extendible or linear hashing on top of the same page shape.
///
/// # Thread safety
///
/// The current implementation uses one lock over the page array so chain
/// updates are atomic. The layout is deliberately page-shaped even though the
/// pages are held in memory by this access-method facade.
#[derive(Debug)]
pub struct HashIndex {
    /// Primary bucket pages plus overflow-page arena.
    storage: Mutex<HashStorage>,
    /// Number of top-level buckets. Power-of-two for cheap masking.
    num_buckets: usize,
    /// Maximum number of entries held by one page.
    page_capacity: usize,
}

#[derive(Debug)]
struct HashStorage {
    buckets: Vec<HashPage>,
    overflow_pages: Vec<HashPage>,
}

#[derive(Debug, Default)]
struct HashPage {
    entries: Vec<(Vec<u8>, TupleId)>,
    next_overflow: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
enum HashPageRef {
    Bucket(usize),
    Overflow(usize),
}

#[derive(Clone, Copy)]
struct HashWalRequest<'a> {
    op: HashOpKind,
    index_rel: RelationId,
    page_ref: HashPageRef,
    key_hash: u64,
    key: &'a [u8],
    tid: TupleId,
    xid: Xid,
    wal: Option<&'a dyn WalSink>,
}

impl HashStorage {
    fn new(num_buckets: usize) -> Self {
        Self {
            buckets: (0..num_buckets).map(|_| HashPage::default()).collect(),
            overflow_pages: Vec::new(),
        }
    }

    fn page(&self, page_ref: HashPageRef) -> &HashPage {
        match page_ref {
            HashPageRef::Bucket(idx) => &self.buckets[idx],
            HashPageRef::Overflow(idx) => &self.overflow_pages[idx],
        }
    }

    fn page_mut(&mut self, page_ref: HashPageRef) -> &mut HashPage {
        match page_ref {
            HashPageRef::Bucket(idx) => &mut self.buckets[idx],
            HashPageRef::Overflow(idx) => &mut self.overflow_pages[idx],
        }
    }
}

impl HashIndex {
    /// Create a hash index with `num_buckets` buckets.
    ///
    /// `num_buckets` is rounded up to the next power of two. A
    /// reasonable starting point for OLTP workloads is 256 or 1 024.
    #[must_use]
    pub fn new(num_buckets: usize) -> Self {
        Self::with_page_capacity(num_buckets, 64)
    }

    /// Create a hash index with a custom page capacity.
    ///
    /// This is mainly used by tests to force overflow chains with small input
    /// sets. Production callers should use [`Self::new`].
    #[must_use]
    pub fn with_page_capacity(num_buckets: usize, page_capacity: usize) -> Self {
        let n = num_buckets.next_power_of_two().max(1);
        Self {
            storage: Mutex::new(HashStorage::new(n)),
            num_buckets: n,
            page_capacity: page_capacity.max(1),
        }
    }

    fn key_hash(key: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    fn bucket_index(&self, key: &[u8]) -> usize {
        let hash = Self::key_hash(key);
        hash_low_bits_usize(hash) & (self.num_buckets - 1)
    }

    /// Number of allocated overflow pages.
    #[must_use]
    pub fn overflow_page_count(&self) -> usize {
        self.storage.lock().overflow_pages.len()
    }

    /// Insert `(key, tid)` and emit a `HashOp` WAL record when `wal` is set.
    ///
    /// The WAL record carries the static bucket number, the page-shaped hash
    /// page touched by the insert, the stable key hash, the encoded key, and
    /// the encoded heap [`TupleId`]. When inserting into a new overflow page,
    /// an `OverflowLink` record is emitted before the `Insert` record.
    pub fn insert_logged(
        &self,
        index_rel: RelationId,
        key: &[u8],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let key_hash = Self::key_hash(key);
        let idx = self.bucket_index(key);
        let mut storage = self.storage.lock();
        let mut current = HashPageRef::Bucket(idx);
        loop {
            let next = {
                let page = storage.page_mut(current);
                if page.entries.len() < self.page_capacity {
                    self.emit_hash_wal(HashWalRequest {
                        op: HashOpKind::Insert,
                        index_rel,
                        page_ref: current,
                        key_hash,
                        key,
                        tid,
                        xid,
                        wal,
                    })?;
                    page.entries.push((key.to_vec(), tid));
                    return Ok(());
                }
                page.next_overflow
            };
            if let Some(next) = next {
                current = HashPageRef::Overflow(next);
                continue;
            }
            let overflow_idx = storage.overflow_pages.len();
            let overflow_ref = HashPageRef::Overflow(overflow_idx);
            self.emit_hash_wal(HashWalRequest {
                op: HashOpKind::OverflowLink,
                index_rel,
                page_ref: current,
                key_hash,
                key,
                tid,
                xid,
                wal,
            })?;
            self.emit_hash_wal(HashWalRequest {
                op: HashOpKind::Insert,
                index_rel,
                page_ref: overflow_ref,
                key_hash,
                key,
                tid,
                xid,
                wal,
            })?;
            storage.overflow_pages.push(HashPage::default());
            storage.page_mut(current).next_overflow = Some(overflow_idx);
            storage.overflow_pages[overflow_idx]
                .entries
                .push((key.to_vec(), tid));
            return Ok(());
        }
    }

    /// Delete `(key, tid)` and emit a `HashOp` WAL record when `wal` is set.
    pub fn delete_logged(
        &self,
        index_rel: RelationId,
        key: &[u8],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let key_hash = Self::key_hash(key);
        let idx = self.bucket_index(key);
        let mut storage = self.storage.lock();
        let mut current = Some(HashPageRef::Bucket(idx));
        while let Some(page_ref) = current {
            let page = storage.page_mut(page_ref);
            if let Some(pos) = page
                .entries
                .iter()
                .position(|(k, t)| k.as_slice() == key && *t == tid)
            {
                self.emit_hash_wal(HashWalRequest {
                    op: HashOpKind::Delete,
                    index_rel,
                    page_ref,
                    key_hash,
                    key,
                    tid,
                    xid,
                    wal,
                })?;
                page.entries.remove(pos);
                return Ok(());
            }
            current = page.next_overflow.map(HashPageRef::Overflow);
        }
        Err(AccessMethodError::NotFound)
    }

    fn emit_hash_wal(&self, request: HashWalRequest<'_>) -> Result<(), AccessMethodError> {
        let Some(sink) = request.wal else {
            return Ok(());
        };
        let page = self.hash_page_id(request.index_rel, request.page_ref)?;
        let payload = HashOpPayload {
            op: request.op,
            index_rel: request.index_rel,
            bucket: u32::try_from(self.bucket_index(request.key)).map_err(|_| {
                AccessMethodError::Storage("hash bucket does not fit in u32".to_owned())
            })?,
            page,
            key_hash: request.key_hash,
            key_bytes: request.key.to_vec(),
            value_bytes: Self::tuple_id_bytes(request.tid),
        }
        .encode()
        .map_err(|e| AccessMethodError::Storage(format!("hash WAL payload encode: {e}")))?;
        let prev_lsn = sink.last_lsn_for(request.xid);
        let record = WalRecord::new(RecordType::HashOp, request.xid, prev_lsn, 0, payload)
            .map_err(|e| AccessMethodError::Storage(format!("hash WAL record encode: {e}")))?;
        sink.append(record)
            .map(|_| ())
            .map_err(|e| AccessMethodError::Storage(format!("hash WAL append: {e}")))
    }

    fn hash_page_id(
        &self,
        index_rel: RelationId,
        page_ref: HashPageRef,
    ) -> Result<PageId, AccessMethodError> {
        let raw_block = match page_ref {
            HashPageRef::Bucket(idx) => idx,
            HashPageRef::Overflow(idx) => self.num_buckets.checked_add(idx).ok_or_else(|| {
                AccessMethodError::Storage("hash overflow page number overflow".to_owned())
            })?,
        };
        let block = u32::try_from(raw_block)
            .map_err(|_| AccessMethodError::Storage("hash page does not fit in u32".to_owned()))?;
        Ok(PageId::new(index_rel, BlockNumber::new(block)))
    }

    pub(crate) fn tuple_id_bytes(tid: TupleId) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
        out.extend_from_slice(&tid.page.relation.oid().raw().to_le_bytes());
        out.extend_from_slice(&tid.page.block.raw().to_le_bytes());
        out.extend_from_slice(&tid.slot.to_le_bytes());
        out
    }
}

fn hash_low_bits_usize(hash: u64) -> usize {
    const BITS_PER_BYTE: usize = 8;
    let mut out = 0_usize;
    for (idx, byte) in hash
        .to_le_bytes()
        .iter()
        .take(std::mem::size_of::<usize>())
        .enumerate()
    {
        out |= usize::from(*byte) << (idx * BITS_PER_BYTE);
    }
    out
}

impl AccessMethod for HashIndex {
    fn name(&self) -> &'static str {
        "hash"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.insert_logged(RelationId::INVALID, key, tid, Xid::INVALID, None)
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let idx = self.bucket_index(key);
        let storage = self.storage.lock();
        let mut current = Some(HashPageRef::Bucket(idx));
        let mut results = Vec::new();
        while let Some(page_ref) = current {
            let page = storage.page(page_ref);
            results.extend(
                page.entries
                    .iter()
                    .filter(|(k, _)| k.as_slice() == key)
                    .map(|(_, tid)| *tid),
            );
            current = page.next_overflow.map(HashPageRef::Overflow);
        }
        Ok(results)
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.delete_logged(RelationId::INVALID, key, tid, Xid::INVALID, None)
    }
}
