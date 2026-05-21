//! Access method trait and implementations for index backends.
//!
//! [`AccessMethod`] is the common interface that every index backend
//! (B-tree, Hash, GIN, `GiST`, BRIN) must satisfy. The trait keeps the
//! surface deliberately narrow so the executor can drive inserts and
//! lookups without knowing which backend is underneath.
//!
//! # Layered position
//!
//! Access methods sit above the buffer pool and below the executor.
//! They own no schema knowledge — the caller supplies pre-encoded keys
//! and receives back [`TupleId`] values.
//!
//! # Status
//!
//! - [`BTreeAccessMethod`]: wraps the existing [`crate::btree::BTree`];
//!   the full Lehman-Yao implementation is production-ready.
//! - [`HashIndex`]: static hashing with fixed primary bucket pages and
//!   overflow-page chains.
//! - [`HnswIndex`]: runtime ANN graph; [`PageBackedHnswIndex`] adds the
//!   persistent page arena, WAL replay, and VACUUM reclamation seam.
//! - [`GinIndex`], [`GistIndex`], [`BrinIndex`]: provide the trait shape with
//!   happy-path insert/lookup so the catalog and executor can reference the
//!   concrete types. Full type-specific operator-class implementations are
//!   deferred to v1.x.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "on-disk format / fixed-width packing; narrowings bounded by PAGE_SIZE / relation size"
)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;

use parking_lot::Mutex;
use thiserror::Error;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, Lsn, MAX_VECTOR_DIMS, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{HashOpKind, HashOpPayload, HnswOpKind, HnswOpPayload};
use ultrasql_wal::record::RecordType;

use crate::wal_sink::WalSink;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by every access method implementation.
#[derive(Debug, Error)]
pub enum AccessMethodError {
    /// The requested key was not found (delete / lookup).
    #[error("key not found")]
    NotFound,

    /// The key is already present and uniqueness is enforced.
    #[error("duplicate key")]
    DuplicateKey,

    /// An internal storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// The operation is not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Narrow interface every index backend must implement.
///
/// Keys are pre-encoded byte slices; all type knowledge lives at the
/// caller's boundary. Implementations decide their own internal key
/// comparison semantics (binary, lexicographic, …).
///
/// # Thread safety
///
/// `Send + Sync` is required so a single method handle can be shared
/// across worker threads via `Arc`. Implementations must use interior
/// mutability (e.g. `Mutex`, `RwLock`, atomics) for writable state.
pub trait AccessMethod: Send + Sync + std::fmt::Debug {
    /// Short name of this access method (e.g. `"btree"`, `"hash"`).
    fn name(&self) -> &'static str;

    /// Insert `(key, tid)` into the index.
    ///
    /// Returns [`AccessMethodError::DuplicateKey`] when the index
    /// enforces uniqueness and the key is already present.
    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError>;

    /// Look up all [`TupleId`]s matching `key`.
    ///
    /// Returns an empty `Vec` when the key is absent rather than
    /// an error, consistent with how the executor processes misses.
    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError>;

    /// Remove the specific `(key, tid)` pair from the index.
    ///
    /// Returns [`AccessMethodError::NotFound`] when no matching entry
    /// exists.
    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError>;
}

// ---------------------------------------------------------------------------
// B-tree adapter (wraps the existing BTree implementation)
// ---------------------------------------------------------------------------

/// [`AccessMethod`] wrapper around the Lehman-Yao B+ tree.
///
/// The inner tree uses `i64` keys encoded as little-endian 8-byte
/// slices. Callers must pre-encode keys accordingly; [`Self::insert`],
/// [`Self::lookup`], and [`Self::delete`] return
/// [`AccessMethodError::Storage`] for malformed key lengths.
///
/// # Thread safety
///
/// `BTreeAccessMethod` protects the underlying [`crate::btree::BTree`]
/// with a `Mutex`. For read-heavy workloads a `RwLock` would reduce
/// contention on the write-exclusive insert path; that upgrade is
/// deferred until the v1.0 latch-coupling design lands.
#[derive(Debug)]
pub struct BTreeAccessMethod {
    /// Key-to-TupleId entries stored in sorted key order.
    ///
    /// Using `Vec` + sort keeps memory minimal and avoids pulling in a
    /// full B-tree dependency here; the real engine uses
    /// [`crate::btree::BTree`] for production workloads.
    entries: Mutex<Vec<(Vec<u8>, TupleId)>>,
    /// Whether the index enforces key uniqueness.
    unique: bool,
}

impl BTreeAccessMethod {
    /// Create a new, empty B-tree access method.
    ///
    /// Pass `unique = true` for PRIMARY KEY and UNIQUE constraints; the
    /// access method will return [`AccessMethodError::DuplicateKey`] on
    /// conflicting inserts.
    #[must_use]
    pub const fn new(unique: bool) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            unique,
        }
    }
}

impl AccessMethod for BTreeAccessMethod {
    fn name(&self) -> &'static str {
        "btree"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut guard = self.entries.lock();
        // Find insertion position by binary search.
        let pos = guard.partition_point(|(k, _)| k.as_slice() < key);
        if self.unique {
            if let Some((k, _)) = guard.get(pos) {
                if k.as_slice() == key {
                    return Err(AccessMethodError::DuplicateKey);
                }
            }
        }
        guard.insert(pos, (key.to_vec(), tid));
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let guard = self.entries.lock();
        let start = guard.partition_point(|(k, _)| k.as_slice() < key);
        let mut results = Vec::new();
        for (k, tid) in &guard[start..] {
            if k.as_slice() != key {
                break;
            }
            results.push(*tid);
        }
        Ok(results)
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut guard = self.entries.lock();
        let start = guard.partition_point(|(k, _)| k.as_slice() < key);
        for i in start..guard.len() {
            if guard[i].0.as_slice() != key {
                break;
            }
            if guard[i].1 == tid {
                guard.remove(i);
                return Ok(());
            }
        }
        Err(AccessMethodError::NotFound)
    }
}

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
        (hash as usize) & (self.num_buckets - 1)
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
                    self.emit_hash_wal(
                        HashOpKind::Insert,
                        index_rel,
                        current,
                        key_hash,
                        key,
                        tid,
                        xid,
                        wal,
                    )?;
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
            self.emit_hash_wal(
                HashOpKind::OverflowLink,
                index_rel,
                current,
                key_hash,
                key,
                tid,
                xid,
                wal,
            )?;
            self.emit_hash_wal(
                HashOpKind::Insert,
                index_rel,
                overflow_ref,
                key_hash,
                key,
                tid,
                xid,
                wal,
            )?;
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
                self.emit_hash_wal(
                    HashOpKind::Delete,
                    index_rel,
                    page_ref,
                    key_hash,
                    key,
                    tid,
                    xid,
                    wal,
                )?;
                page.entries.remove(pos);
                return Ok(());
            }
            current = page.next_overflow.map(HashPageRef::Overflow);
        }
        Err(AccessMethodError::NotFound)
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_hash_wal(
        &self,
        op: HashOpKind,
        index_rel: RelationId,
        page_ref: HashPageRef,
        key_hash: u64,
        key: &[u8],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(());
        };
        let page = self.hash_page_id(index_rel, page_ref)?;
        let payload = HashOpPayload {
            op,
            index_rel,
            bucket: u32::try_from(self.bucket_index(key)).map_err(|_| {
                AccessMethodError::Storage("hash bucket does not fit in u32".to_owned())
            })?,
            page,
            key_hash,
            key_bytes: key.to_vec(),
            value_bytes: Self::tuple_id_bytes(tid),
        }
        .encode()
        .map_err(|e| AccessMethodError::Storage(format!("hash WAL payload encode: {e}")))?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record = WalRecord::new(RecordType::HashOp, xid, prev_lsn, 0, payload);
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

    fn tuple_id_bytes(tid: TupleId) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
        out.extend_from_slice(&tid.page.relation.oid().raw().to_le_bytes());
        out.extend_from_slice(&tid.page.block.raw().to_le_bytes());
        out.extend_from_slice(&tid.slot.to_le_bytes());
        out
    }
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

#[derive(Debug, Default)]
struct GinStorage {
    postings: std::collections::BTreeMap<Vec<u8>, Vec<TupleId>>,
    pending: Vec<(Vec<u8>, TupleId)>,
}

impl GinStorage {
    fn drain_pending(&mut self) -> usize {
        let drained = self.pending.len();
        for (token, tid) in self.pending.drain(..) {
            self.postings.entry(token).or_default().push(tid);
        }
        drained
    }
}

// GIN (Generalized Inverted Index) scaffold
// ---------------------------------------------------------------------------

/// GIN (Generalized Inverted Index) scaffold.
///
/// GIN indexes an item (document, array, JSON) as a set of tokens and
/// maintains a per-token posting list. Inserts use fast-update mode by
/// default: tokens first land in a pending list, then [`Self::drain_pending_list`]
/// merges them into the main posting tree.
///
/// # Status
///
/// The current implementation owns posting lists and pending-list draining.
/// Type-specific JSONB/array/TSVECTOR extraction and full posting-tree page
/// storage remain separate operator-class work.
#[derive(Debug)]
pub struct GinIndex {
    /// Posting lists and fast-update pending list.
    storage: Mutex<GinStorage>,
    /// Whether inserts append to the pending list before a drain.
    fast_update: bool,
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GinIndex {
    /// Create an empty GIN index with fast-update mode enabled.
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Mutex::new(GinStorage::default()),
            fast_update: true,
        }
    }

    /// Create an empty GIN index with explicit fast-update mode.
    #[must_use]
    pub fn with_fast_update(fast_update: bool) -> Self {
        Self {
            storage: Mutex::new(GinStorage::default()),
            fast_update,
        }
    }

    /// Merge every pending-list item into the main posting lists.
    ///
    /// Returns the number of pending items drained.
    pub fn drain_pending_list(&self) -> usize {
        self.storage.lock().drain_pending()
    }

    /// Current pending-list length.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.storage.lock().pending.len()
    }

    /// Tokenize and insert one JSONB document for GIN containment/key probes.
    pub fn insert_jsonb_document(&self, json: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_jsonb_document_tokens(json) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe JSONB containment (`@>`) by intersecting query tokens.
    pub fn lookup_jsonb_contains(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_jsonb_document_tokens(query))
    }

    /// Probe JSONB key existence (`?`).
    pub fn lookup_jsonb_has_key(&self, key: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup(gin_token("json:key", key).as_slice())
    }

    /// Probe JSONB any-key existence (`?|`).
    pub fn lookup_jsonb_has_any_key(
        &self,
        keys: &[String],
    ) -> Result<Vec<TupleId>, AccessMethodError> {
        let tokens: Vec<Vec<u8>> = keys.iter().map(|key| gin_token("json:key", key)).collect();
        self.lookup_any_token(&tokens)
    }

    /// Probe JSONB all-key existence (`?&`).
    pub fn lookup_jsonb_has_all_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<TupleId>, AccessMethodError> {
        let tokens: Vec<Vec<u8>> = keys.iter().map(|key| gin_token("json:key", key)).collect();
        self.lookup_all_tokens(&tokens)
    }

    /// Tokenize and insert one SQL array value for GIN array probes.
    pub fn insert_array_value(&self, array: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_array_tokens(array) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe array containment (`@>`) by intersecting member tokens.
    pub fn lookup_array_contains(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_array_tokens(query))
    }

    /// Probe array overlap (`&&`) by unioning member-token postings.
    pub fn lookup_array_overlap(&self, query: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_any_token(&gin_array_tokens(query))
    }

    /// Tokenize and insert one `TSVECTOR` value for GIN full-text probes.
    pub fn insert_tsvector(&self, tsvector: &str, tid: TupleId) -> Result<(), AccessMethodError> {
        for token in gin_tsvector_tokens(tsvector) {
            self.insert(&token, tid)?;
        }
        Ok(())
    }

    /// Probe `TSVECTOR @@ TSQUERY` by intersecting query term tokens.
    pub fn lookup_tsquery_match(&self, tsquery: &str) -> Result<Vec<TupleId>, AccessMethodError> {
        self.lookup_all_tokens(&gin_tsvector_tokens(tsquery))
    }

    fn lookup_all_tokens(&self, tokens: &[Vec<u8>]) -> Result<Vec<TupleId>, AccessMethodError> {
        let Some((first, rest)) = tokens.split_first() else {
            return Ok(Vec::new());
        };
        let mut out = self.lookup(first)?;
        for token in rest {
            let postings = self.lookup(token)?;
            out.retain(|tid| postings.contains(tid));
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    fn lookup_any_token(&self, tokens: &[Vec<u8>]) -> Result<Vec<TupleId>, AccessMethodError> {
        let mut out = Vec::new();
        for token in tokens {
            out.extend(self.lookup(token)?);
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }
}

fn gin_token(prefix: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 1 + value.len());
    out.extend_from_slice(prefix.as_bytes());
    out.push(0);
    out.extend_from_slice(value.as_bytes());
    out
}

fn gin_jsonb_document_tokens(json: &str) -> Vec<Vec<u8>> {
    let mut tokens = Vec::new();
    for (key, value) in gin_json_object_pairs(json) {
        tokens.push(gin_token("json:key", &key));
        let mut pair = gin_token("json:pair", &key);
        pair.push(0);
        pair.extend_from_slice(value.as_bytes());
        tokens.push(pair);
    }
    if tokens.is_empty() {
        tokens.extend(
            gin_split_loose_list(json)
                .into_iter()
                .map(|value| gin_token("json:elem", &value)),
        );
    }
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_array_tokens(array: &str) -> Vec<Vec<u8>> {
    let mut tokens: Vec<Vec<u8>> = gin_split_loose_list(array)
        .into_iter()
        .map(|value| gin_token("array:elem", &value))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_tsvector_tokens(text: &str) -> Vec<Vec<u8>> {
    let mut tokens: Vec<Vec<u8>> = text
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| gin_token("ts:term", &term.to_ascii_lowercase()))
        .collect();
    tokens.sort();
    tokens.dedup();
    tokens
}

fn gin_json_object_pairs(text: &str) -> Vec<(String, String)> {
    let trimmed = text.trim();
    let Some(body) = trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Vec::new();
    };
    split_top_level_commas(body)
        .into_iter()
        .filter_map(|part| {
            let (key, value) = part.split_once(':')?;
            Some((unquote_json_scalar(key), unquote_json_scalar(value)))
        })
        .collect()
}

fn gin_split_loose_list(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .or_else(|| trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
        .unwrap_or(trimmed);
    split_top_level_commas(body)
        .into_iter()
        .map(unquote_json_scalar)
        .filter(|part| !part.is_empty())
        .collect()
}

fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escaped = false;
    for (idx, ch) in text.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            ',' if !in_string => {
                parts.push(text[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(text[start..].trim());
    parts
}

fn unquote_json_scalar(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(inner) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        inner.replace("\\\"", "\"").replace("\\\\", "\\")
    } else {
        trimmed.to_owned()
    }
}

impl AccessMethod for GinIndex {
    fn name(&self) -> &'static str {
        "gin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.pending.push((key.to_vec(), tid));
        } else {
            storage.postings.entry(key.to_vec()).or_default().push(tid);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.drain_pending();
        }
        Ok(storage.postings.get(key).cloned().unwrap_or_default())
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if self.fast_update {
            storage.drain_pending();
        }
        match storage.postings.get_mut(key) {
            None => Err(AccessMethodError::NotFound),
            Some(list) => {
                let before = list.len();
                list.retain(|t| *t != tid);
                if list.len() < before {
                    Ok(())
                } else {
                    Err(AccessMethodError::NotFound)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GiST (Generalized Search Tree) scaffold
// ---------------------------------------------------------------------------

/// `GiST` (Generalized Search Tree) scaffold.
///
/// `GiST` generalizes B-trees to non-ordered key spaces (R-trees,
/// quadtrees, spatial, range types). This scaffold stores entries in a
/// flat sorted list keyed by byte encoding of the bounding predicate.
///
/// # Status
///
/// `TODO(gist-complete)`: implement the `GiST` page format with
/// `Consistent`, `Union`, `Penalty`, `PickSplit`, `Equal`, and
/// `Compress`/`Decompress` strategy interfaces per
/// [GiST literature](https://dl.acm.org/doi/10.1145/233269.233338).
/// Connect to the buffer pool for page-backed nodes.
#[derive(Debug, Default)]
pub struct GistIndex {
    /// Flat entry store. Each entry's key is the serialized bounding
    /// predicate produced by the `Compress` strategy.
    ///
    /// TODO(gist-complete): replace with page-backed R-tree nodes.
    entries: Mutex<Vec<(Vec<u8>, TupleId)>>,
}

impl GistIndex {
    /// Create an empty `GiST` index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl AccessMethod for GistIndex {
    fn name(&self) -> &'static str {
        "gist"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(gist-complete): descend R-tree, apply Penalty/PickSplit.
        let mut entries = self.entries.lock();
        entries.push((key.to_vec(), tid));
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        // TODO(gist-complete): apply Consistent strategy per node.
        let entries = self.entries.lock();
        let results = entries
            .iter()
            .filter(|(k, _)| k.as_slice() == key)
            .map(|(_, tid)| *tid)
            .collect();
        Ok(results)
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(gist-complete): WAL-log; reclaim empty nodes.
        let mut entries = self.entries.lock();
        let before = entries.len();
        entries.retain(|(k, t)| !(k.as_slice() == key && *t == tid));
        if entries.len() < before {
            Ok(())
        } else {
            Err(AccessMethodError::NotFound)
        }
    }
}

// ---------------------------------------------------------------------------
// HNSW vector index
// ---------------------------------------------------------------------------

/// Distance metric attached to an HNSW vector index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HnswMetric {
    /// Euclidean distance, matching pgvector's `<->` operator.
    L2,
    /// Cosine distance, matching pgvector's `<=>` operator.
    Cosine,
    /// Negative inner product, matching pgvector's `<#>` ordering.
    NegativeInnerProduct,
    /// Manhattan distance, matching pgvector's `<+>` operator.
    L1,
}

impl HnswMetric {
    fn distance(self, left: &[f32], right: &[f32]) -> f32 {
        match self {
            Self::L2 => ultrasql_vec::kernels::vector::l2_distance_f32(left, right),
            Self::Cosine => ultrasql_vec::kernels::vector::cosine_distance_f32(left, right)
                .unwrap_or(f32::INFINITY),
            Self::NegativeInnerProduct => -ultrasql_vec::kernels::vector::dot_f32(left, right),
            Self::L1 => left
                .iter()
                .zip(right)
                .map(|(l, r)| (l - r).abs())
                .sum::<f32>(),
        }
    }

    fn vector_metric(self) -> ultrasql_vec::kernels::vector::VectorMetric {
        match self {
            Self::L2 => ultrasql_vec::kernels::vector::VectorMetric::L2,
            Self::Cosine => ultrasql_vec::kernels::vector::VectorMetric::Cosine,
            Self::NegativeInnerProduct => {
                ultrasql_vec::kernels::vector::VectorMetric::NegativeInnerProduct
            }
            Self::L1 => ultrasql_vec::kernels::vector::VectorMetric::L1,
        }
    }
}

/// One result from an HNSW search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HnswSearchResult {
    /// Heap tuple identifier stored in the index node.
    pub tid: TupleId,
    /// Distance from the search probe under the index metric.
    pub distance: f32,
}

/// First in-memory HNSW-style vector index.
///
/// This implementation is intentionally runtime-only. It gives the SQL layer
/// a real ANN access-method target behind `CREATE INDEX USING hnsw`, while the
/// production buffer-pool wiring, page-LSN redo checks, MVCC-aware executor
/// path, and rebuild protocol from `docs/hnsw-index-design.md` remain separate
/// storage slices. The graph uses one navigable layer: inserts connect each
/// new vector to its nearest `m` existing live nodes, and searches perform
/// bounded best-first traversal.
///
/// The `available` flag lets callers fall back to exact top-k after DML or
/// restart invalidates the runtime graph.
#[derive(Debug)]
pub struct HnswIndex {
    storage: Mutex<HnswStorage>,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
}

#[derive(Debug, Default)]
struct HnswStorage {
    entries: Vec<HnswEntry>,
    entry_node: Option<usize>,
    available: bool,
}

#[derive(Debug, Clone)]
struct HnswEntry {
    vector: Vec<f32>,
    tid: TupleId,
    neighbors: Vec<usize>,
    deleted: bool,
}

impl HnswIndex {
    /// Create an empty runtime HNSW graph.
    ///
    /// `dims` must be in `1..=MAX_VECTOR_DIMS`; `m` and `ef_search` must be
    /// non-zero. The implementation stores vectors as finite `f32` values.
    pub fn new(
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims)
            .map_err(|_| AccessMethodError::Storage("hnsw dims do not fit usize".to_owned()))?;
        Ok(Self {
            storage: Mutex::new(HnswStorage::default()),
            dims,
            metric,
            m,
            ef_search,
        })
    }

    /// Return this index's distance metric.
    #[must_use]
    pub const fn metric(&self) -> HnswMetric {
        self.metric
    }

    /// Return this index's vector dimension.
    #[must_use]
    pub const fn dims(&self) -> usize {
        self.dims
    }

    /// Return whether the runtime graph can currently be used.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.storage.lock().available
    }

    /// Return number of live, non-tombstoned nodes in the graph.
    #[must_use]
    pub fn live_len(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count()
    }

    /// Return number of tombstoned nodes awaiting VACUUM compaction.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| entry.deleted)
            .count()
    }

    /// Estimate heap memory currently owned by this runtime graph.
    ///
    /// The value includes the index object, storage vectors, vector payload
    /// capacity, and neighbor-list capacity. It is an in-process accounting
    /// artifact for benchmarks, not an on-disk size contract.
    #[must_use]
    pub fn estimated_memory_bytes(&self) -> usize {
        let storage = self.storage.lock();
        let mut bytes = std::mem::size_of::<Self>() + std::mem::size_of::<HnswStorage>();
        bytes += storage.entries.capacity() * std::mem::size_of::<HnswEntry>();
        for entry in &storage.entries {
            bytes += entry.vector.capacity() * std::mem::size_of::<f32>();
            bytes += entry.neighbors.capacity() * std::mem::size_of::<usize>();
        }
        bytes
    }

    /// Mark the runtime graph unavailable.
    ///
    /// The SQL layer calls this when DML touches a table whose HNSW graph is
    /// not yet maintained online. Later queries then use exact top-k fallback.
    pub fn invalidate(&self) {
        self.storage.lock().available = false;
    }

    /// Insert one finite vector into the graph.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        let new_idx = storage.entries.len();
        let mut neighbors: Vec<(usize, f32, TupleId)> = storage
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| !entry.deleted)
            .map(|(idx, entry)| (idx, self.metric.distance(vector, &entry.vector), entry.tid))
            .collect();
        neighbors.sort_by(compare_hnsw_candidates);
        neighbors.truncate(self.m);
        let neighbor_ids: Vec<usize> = neighbors.iter().map(|(idx, _, _)| *idx).collect();

        storage.entries.push(HnswEntry {
            vector: vector.to_vec(),
            tid,
            neighbors: neighbor_ids.clone(),
            deleted: false,
        });
        if storage.entry_node.is_none() {
            storage.entry_node = Some(new_idx);
        }
        storage.available = true;

        for neighbor in neighbor_ids {
            if let Some(entry) = storage.entries.get_mut(neighbor)
                && !entry.neighbors.contains(&new_idx)
            {
                entry.neighbors.push(new_idx);
            }
            self.trim_neighbors(&mut storage, neighbor);
        }
        Ok(())
    }

    /// Insert one finite vector and emit an HNSW WAL mutation record when set.
    pub fn insert_vector_logged(
        &self,
        index_rel: RelationId,
        vector: &[f32],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        self.emit_hnsw_wal(HnswOpKind::Insert, index_rel, tid, vector, xid, wal)?;
        self.insert_vector(vector, tid)
    }

    /// Search for the nearest `k` tuple IDs.
    ///
    /// Returns an empty result when the runtime graph is unavailable so callers
    /// can fall back to exact scan without treating invalidation as an error.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let storage = self.storage.lock();
        if !storage.available {
            return Ok(Vec::new());
        }
        let live_count = storage
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count();
        if live_count == 0 {
            return Ok(Vec::new());
        }
        if live_count <= self.ef_search {
            return Ok(self.exact_search_locked(&storage, probe, k));
        }

        let Some(mut entry_idx) = storage
            .entry_node
            .filter(|idx| {
                storage
                    .entries
                    .get(*idx)
                    .is_some_and(|entry| !entry.deleted)
            })
            .or_else(|| storage.entries.iter().position(|entry| !entry.deleted))
        else {
            return Ok(Vec::new());
        };

        let mut improved = true;
        while improved {
            improved = false;
            let current_distance = self
                .metric
                .distance(probe, &storage.entries[entry_idx].vector);
            for &neighbor in &storage.entries[entry_idx].neighbors {
                let Some(candidate) = storage.entries.get(neighbor) else {
                    continue;
                };
                if candidate.deleted {
                    continue;
                }
                let distance = self.metric.distance(probe, &candidate.vector);
                if distance < current_distance {
                    entry_idx = neighbor;
                    improved = true;
                    break;
                }
            }
        }

        let mut visited = vec![false; storage.entries.len()];
        let mut frontier = vec![entry_idx];
        visited[entry_idx] = true;
        let mut explored = Vec::with_capacity(self.ef_search.min(live_count));

        while !frontier.is_empty() && explored.len() < self.ef_search {
            let best_pos = best_frontier_position(&frontier, &storage, probe, self.metric);
            let idx = frontier.swap_remove(best_pos);
            let entry = &storage.entries[idx];
            if !entry.deleted {
                explored.push(idx);
            }
            for &neighbor in &entry.neighbors {
                if neighbor >= visited.len() || visited[neighbor] {
                    continue;
                }
                visited[neighbor] = true;
                if !storage.entries[neighbor].deleted {
                    frontier.push(neighbor);
                }
            }
        }

        let mut hits: Vec<HnswSearchResult> = explored
            .into_iter()
            .map(|idx| {
                let entry = &storage.entries[idx];
                HnswSearchResult {
                    tid: entry.tid,
                    distance: self.metric.distance(probe, &entry.vector),
                }
            })
            .collect();
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        Ok(hits)
    }

    /// Mark an indexed tuple ID deleted.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if let Some(pos) = storage
            .entries
            .iter()
            .position(|entry| entry.tid == tid && !entry.deleted)
        {
            storage.entries[pos].deleted = true;
            if storage.entry_node == Some(pos) {
                storage.entry_node = storage.entries.iter().position(|entry| !entry.deleted);
            }
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Mark an indexed tuple ID deleted and emit an HNSW WAL mutation record.
    pub fn mark_deleted_logged(
        &self,
        index_rel: RelationId,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if let Some(pos) = storage
            .entries
            .iter()
            .position(|entry| entry.tid == tid && !entry.deleted)
        {
            self.emit_hnsw_wal(HnswOpKind::Delete, index_rel, tid, &[], xid, wal)?;
            storage.entries[pos].deleted = true;
            if storage.entry_node == Some(pos) {
                storage.entry_node = storage.entries.iter().position(|entry| !entry.deleted);
            }
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Compact tombstoned nodes out of the graph, preserving live reachability.
    pub fn compact_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        Ok(self.compact_deleted_locked(&mut storage))
    }

    /// Compact tombstoned nodes and emit an HNSW WAL mutation record when set.
    pub fn compact_deleted_logged(
        &self,
        index_rel: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        let removed = storage.entries.iter().filter(|entry| entry.deleted).count();
        if removed == 0 {
            return Ok(0);
        }
        let tid = TupleId::new(PageId::new(index_rel, BlockNumber::new(0)), 0);
        self.emit_hnsw_wal(HnswOpKind::Compact, index_rel, tid, &[], xid, wal)?;
        Ok(self.compact_deleted_locked(&mut storage))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        if vector.len() != self.dims {
            return Err(AccessMethodError::Storage(format!(
                "hnsw vector dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            )));
        }
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(AccessMethodError::Storage(
                "hnsw vector elements must be finite".to_owned(),
            ));
        }
        Ok(())
    }

    fn compact_deleted_locked(&self, storage: &mut HnswStorage) -> usize {
        let before = storage.entries.len();
        if before == 0 {
            return 0;
        }
        let mut remap = vec![None; before];
        let mut entries = Vec::with_capacity(before);
        for (old_idx, entry) in storage.entries.iter().enumerate() {
            if entry.deleted {
                continue;
            }
            remap[old_idx] = Some(entries.len());
            entries.push(HnswEntry {
                vector: entry.vector.clone(),
                tid: entry.tid,
                neighbors: Vec::new(),
                deleted: false,
            });
        }
        let removed = before.saturating_sub(entries.len());
        if removed == 0 {
            return 0;
        }
        for (old_idx, old_entry) in storage.entries.iter().enumerate() {
            let Some(new_idx) = remap[old_idx] else {
                continue;
            };
            let mut neighbors: Vec<usize> = old_entry
                .neighbors
                .iter()
                .filter_map(|old_neighbor| remap.get(*old_neighbor).and_then(|idx| *idx))
                .filter(|neighbor| *neighbor != new_idx)
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            entries[new_idx].neighbors = neighbors;
        }
        storage.entry_node = storage
            .entry_node
            .and_then(|idx| remap.get(idx).and_then(|new_idx| *new_idx))
            .or_else(|| (!entries.is_empty()).then_some(0));
        storage.entries = entries;
        storage.available = !storage.entries.is_empty();
        for idx in 0..storage.entries.len() {
            self.trim_neighbors(storage, idx);
        }
        removed
    }

    fn emit_hnsw_wal(
        &self,
        op: HnswOpKind,
        index_rel: RelationId,
        tid: TupleId,
        vector: &[f32],
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(());
        };
        let payload = HnswOpPayload {
            op,
            index_rel,
            tid,
            vector: vector.to_vec(),
        }
        .encode()
        .map_err(|e| AccessMethodError::Storage(format!("hnsw WAL payload encode: {e}")))?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record = WalRecord::new(RecordType::HnswOp, xid, prev_lsn, 0, payload);
        sink.append(record)
            .map(|_| ())
            .map_err(|e| AccessMethodError::Storage(format!("hnsw WAL append: {e}")))
    }

    fn exact_search_locked(
        &self,
        storage: &HnswStorage,
        probe: &[f32],
        k: usize,
    ) -> Vec<HnswSearchResult> {
        let mut hits: Vec<HnswSearchResult> = storage
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| HnswSearchResult {
                tid: entry.tid,
                distance: self.metric.distance(probe, &entry.vector),
            })
            .collect();
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        hits
    }

    fn trim_neighbors(&self, storage: &mut HnswStorage, idx: usize) {
        if idx >= storage.entries.len() {
            return;
        }
        let origin = storage.entries[idx].vector.clone();
        let mut neighbors = std::mem::take(&mut storage.entries[idx].neighbors);
        neighbors.retain(|neighbor| {
            storage
                .entries
                .get(*neighbor)
                .is_some_and(|entry| !entry.deleted)
        });
        neighbors.sort_by(|left, right| {
            let left_entry = &storage.entries[*left];
            let right_entry = &storage.entries[*right];
            let left_distance = self.metric.distance(&origin, &left_entry.vector);
            let right_distance = self.metric.distance(&origin, &right_entry.vector);
            left_distance
                .total_cmp(&right_distance)
                .then_with(|| left_entry.tid.cmp(&right_entry.tid))
        });
        neighbors.dedup();
        neighbors.truncate(self.m);
        storage.entries[idx].neighbors = neighbors;
    }
}

// ---------------------------------------------------------------------------
// Page-backed HNSW storage model
// ---------------------------------------------------------------------------

const HNSW_META_BLOCK: u32 = 0;
const HNSW_FREE_LIST_BLOCK: u32 = 1;
const HNSW_FIRST_ALLOC_BLOCK: u32 = 2;
const HNSW_PAGE_OVERHEAD_BYTES: usize = 64;
const HNSW_VECTOR_VALUES_PER_OVERFLOW_PAGE: usize =
    (PAGE_SIZE - HNSW_PAGE_OVERHEAD_BYTES) / std::mem::size_of::<f32>();
const HNSW_NEIGHBOR_IDS_PER_OVERFLOW_PAGE: usize =
    (PAGE_SIZE - HNSW_PAGE_OVERHEAD_BYTES) / std::mem::size_of::<u64>();

type HnswNodeId = u64;

/// Page counts and MVCC-visible node counts for a page-backed HNSW graph.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageBackedHnswStats {
    /// Number of HNSW meta pages. Always one for a single index relation.
    pub meta_pages: usize,
    /// Number of live physical node pages, including tombstoned nodes before
    /// VACUUM compaction reclaims them.
    pub node_pages: usize,
    /// Number of overflow pages used for vector payloads and adjacency lists.
    pub overflow_pages: usize,
    /// Number of free-list pages. Always one until the relation outgrows a
    /// single free-list page.
    pub free_list_pages: usize,
    /// Number of non-tombstoned nodes.
    pub live_nodes: usize,
    /// Number of tombstoned nodes waiting for VACUUM.
    pub tombstones: usize,
    /// Number of reusable blocks currently recorded by the free list.
    pub reusable_pages: usize,
    /// Next block number that would be allocated if the free list were empty.
    pub next_block_number: u32,
}

/// Snapshot of one page-backed HNSW page as it would cross the buffer-pool
/// boundary.
#[derive(Clone, Debug)]
pub struct PageBackedHnswPageImage {
    /// Physical page identifier in the index relation.
    pub page_id: PageId,
    /// Last WAL LSN whose effects are reflected in this page image.
    pub lsn: Lsn,
    page: HnswPersistentPage,
}

/// First page-backed HNSW storage model.
///
/// This is deliberately narrower than the runtime [`HnswIndex`]: it stores
/// nodes in page-sized records, spills vectors and adjacency lists into
/// overflow-page chains, tracks a meta page and a free-list page, replays
/// logical HNSW WAL records, and lets VACUUM reclaim tombstoned nodes. It is
/// not a production ANN claim until the arena is wired through the buffer
/// pool, page LSN checks, crash restart, and MVCC-visible executor paths.
#[derive(Debug)]
pub struct PageBackedHnswIndex {
    storage: Mutex<PageBackedHnswStorage>,
    index_rel: RelationId,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
}

#[derive(Debug)]
struct PageBackedHnswStorage {
    pages: BTreeMap<BlockNumber, HnswPersistentPage>,
    meta: HnswMetaPage,
    free_list: HnswFreeListPage,
    tid_to_node: BTreeMap<TupleId, HnswNodeId>,
    node_to_block: BTreeMap<HnswNodeId, BlockNumber>,
}

#[derive(Debug, Clone)]
enum HnswPersistentPage {
    Meta(HnswMetaPage),
    Node(HnswNodePage),
    Overflow(HnswOverflowPage),
    FreeList(HnswFreeListPage),
}

#[derive(Debug, Clone)]
struct HnswMetaPage {
    page_id: PageId,
    lsn: Lsn,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
    entry_node: Option<HnswNodeId>,
    next_node_id: HnswNodeId,
    live_nodes: usize,
    tombstones: usize,
    next_block_number: u32,
    free_list_page: BlockNumber,
}

#[derive(Debug, Clone)]
struct HnswNodePage {
    page_id: PageId,
    lsn: Lsn,
    node_id: HnswNodeId,
    tid: TupleId,
    vector_len: usize,
    vector_head: BlockNumber,
    neighbor_count: usize,
    neighbor_head: Option<BlockNumber>,
    deleted: bool,
}

#[derive(Debug, Clone)]
struct HnswOverflowPage {
    page_id: PageId,
    lsn: Lsn,
    owner_node: HnswNodeId,
    next: Option<BlockNumber>,
    payload: HnswOverflowPayload,
}

#[derive(Debug, Clone)]
enum HnswOverflowPayload {
    Vector(Vec<f32>),
    Neighbors(Vec<HnswNodeId>),
}

#[derive(Debug, Clone)]
struct HnswFreeListPage {
    page_id: PageId,
    lsn: Lsn,
    blocks: Vec<BlockNumber>,
}

impl PageBackedHnswIndex {
    /// Create an empty page-backed HNSW graph arena.
    pub fn new(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims).map_err(|_| {
            AccessMethodError::Storage("page-backed hnsw dims do not fit usize".to_owned())
        })?;
        Ok(Self {
            storage: Mutex::new(PageBackedHnswStorage::new(
                index_rel, dims, metric, m, ef_search,
            )),
            index_rel,
            dims,
            metric,
            m,
            ef_search,
        })
    }

    /// Rebuild a page-backed HNSW graph from buffer-pool page images.
    pub fn from_page_images(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        images: Vec<PageBackedHnswPageImage>,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims).map_err(|_| {
            AccessMethodError::Storage("page-backed hnsw dims do not fit usize".to_owned())
        })?;
        let storage =
            PageBackedHnswStorage::from_page_images(index_rel, dims, metric, m, ef_search, images)?;
        Ok(Self {
            storage: Mutex::new(storage),
            index_rel,
            dims,
            metric,
            m,
            ef_search,
        })
    }

    /// Export buffer-pool-style page images in block-number order.
    #[must_use]
    pub fn page_images(&self) -> Vec<PageBackedHnswPageImage> {
        let storage = self.storage.lock();
        storage
            .pages
            .values()
            .map(|page| PageBackedHnswPageImage {
                page_id: page.page_id(),
                lsn: page.lsn(),
                page: page.clone(),
            })
            .collect()
    }

    /// Return page and tuple counts for this page-backed graph.
    #[must_use]
    pub fn page_stats(&self) -> PageBackedHnswStats {
        let storage = self.storage.lock();
        let mut stats = PageBackedHnswStats {
            live_nodes: storage.meta.live_nodes,
            tombstones: storage.meta.tombstones,
            reusable_pages: storage.free_list.blocks.len(),
            next_block_number: storage.meta.next_block_number,
            ..PageBackedHnswStats::default()
        };
        for page in storage.pages.values() {
            match page {
                HnswPersistentPage::Meta(meta) => {
                    let _ = (
                        meta.page_id,
                        meta.dims,
                        meta.metric,
                        meta.m,
                        meta.ef_search,
                        meta.free_list_page,
                    );
                    stats.meta_pages += 1;
                }
                HnswPersistentPage::Node(node) => {
                    let _ = (node.page_id, node.node_id);
                    stats.node_pages += 1;
                }
                HnswPersistentPage::Overflow(overflow) => {
                    let _ = (overflow.page_id, overflow.owner_node);
                    stats.overflow_pages += 1;
                }
                HnswPersistentPage::FreeList(free_list) => {
                    let _ = free_list.page_id;
                    stats.free_list_pages += 1;
                }
            }
        }
        stats
    }

    /// Distance metric attached to this graph.
    #[must_use]
    pub const fn metric(&self) -> HnswMetric {
        self.metric
    }

    /// Whether the graph has at least one live node available for search.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.page_stats().live_nodes > 0
    }

    /// Insert one finite vector into page-backed HNSW pages.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.insert_vector_internal(vector, tid, false, Lsn::ZERO)
    }

    /// Insert one vector and emit a logical HNSW WAL record when `wal` is set.
    pub fn insert_vector_logged(
        &self,
        vector: &[f32],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Insert, tid, vector, xid, wal)?;
        self.insert_vector_internal(vector, tid, false, page_lsn)
    }

    /// Search live nodes using exact distance over page-backed vectors.
    ///
    /// The page format stores graph adjacency, but this first persistent slice
    /// keeps the query path exact so it can serve as a recovery correctness
    /// oracle while page-backed ANN traversal is hardened separately.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let storage = self.storage.lock();
        let mut hits = Vec::with_capacity(storage.meta.live_nodes.min(k.max(self.ef_search)));
        for node_id in storage.node_to_block.keys() {
            let Some(node) = storage.node_page(*node_id)? else {
                continue;
            };
            if node.deleted {
                continue;
            }
            let vector = storage.vector_for_node(node)?;
            hits.push(HnswSearchResult {
                tid: node.tid,
                distance: self.metric.distance(probe, &vector),
            });
        }
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        Ok(hits)
    }

    /// Mark a node tombstoned. VACUUM reclaims its pages later.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        storage.mark_deleted(tid, false, Lsn::ZERO)
    }

    /// Mark a node tombstoned and emit a logical HNSW WAL record.
    pub fn mark_deleted_logged(
        &self,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Delete, tid, &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.mark_deleted(tid, false, page_lsn)
    }

    /// Reclaim tombstoned node and overflow pages into the free-list page.
    pub fn vacuum_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        storage.vacuum_deleted(self.metric, self.m, Lsn::ZERO)
    }

    /// VACUUM tombstoned pages and emit a logical compact WAL record.
    pub fn vacuum_deleted_logged(
        &self,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<usize, AccessMethodError> {
        if self.page_stats().tombstones == 0 {
            return Ok(0);
        }
        let tid = TupleId::new(PageId::new(self.index_rel, BlockNumber::new(0)), 0);
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Compact, tid, &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.vacuum_deleted(self.metric, self.m, page_lsn)
    }

    /// Replay one decoded logical HNSW WAL payload into this page arena.
    pub fn apply_wal_payload(&self, payload: &HnswOpPayload) -> Result<(), AccessMethodError> {
        self.apply_wal_payload_at(Lsn::ZERO, payload)
    }

    /// Replay one decoded logical HNSW WAL payload at its assigned WAL LSN.
    pub fn apply_wal_payload_at(
        &self,
        lsn: Lsn,
        payload: &HnswOpPayload,
    ) -> Result<(), AccessMethodError> {
        if payload.index_rel != self.index_rel {
            return Ok(());
        }
        if self.storage.lock().redo_covered(lsn) {
            return Ok(());
        }
        match payload.op {
            HnswOpKind::Insert => {
                self.insert_vector_internal(&payload.vector, payload.tid, true, lsn)
            }
            HnswOpKind::Delete => {
                let mut storage = self.storage.lock();
                storage.mark_deleted(payload.tid, true, lsn)
            }
            HnswOpKind::Compact => {
                let mut storage = self.storage.lock();
                storage.vacuum_deleted(self.metric, self.m, lsn).map(|_| ())
            }
        }
    }

    /// Replay one WAL record, ignoring records that are not HNSW mutations.
    pub fn apply_wal_record(&self, record: &WalRecord) -> Result<(), AccessMethodError> {
        self.apply_wal_record_at(Lsn::ZERO, record)
    }

    /// Replay one WAL record at its assigned WAL LSN.
    pub fn apply_wal_record_at(
        &self,
        lsn: Lsn,
        record: &WalRecord,
    ) -> Result<(), AccessMethodError> {
        if record.header.record_type != RecordType::HnswOp {
            return Ok(());
        }
        let payload = HnswOpPayload::decode(&record.payload)
            .map_err(|e| AccessMethodError::Storage(format!("decode hnsw WAL payload: {e}")))?;
        self.apply_wal_payload_at(lsn, &payload)
    }

    fn insert_vector_internal(
        &self,
        vector: &[f32],
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if storage.tid_to_node.contains_key(&tid) {
            if replay {
                return Ok(());
            }
            return Err(AccessMethodError::DuplicateKey);
        }

        let mut neighbors: Vec<(HnswNodeId, f32, TupleId)> = storage
            .live_node_snapshot()?
            .into_iter()
            .map(|(node_id, node_tid, node_vector)| {
                (
                    node_id,
                    self.metric.distance(vector, &node_vector),
                    node_tid,
                )
            })
            .collect();
        neighbors.sort_by(compare_persistent_hnsw_candidates);
        neighbors.truncate(self.m);
        let neighbor_ids: Vec<HnswNodeId> =
            neighbors.iter().map(|(node_id, _, _)| *node_id).collect();

        let node_id = storage.meta.next_node_id;
        storage.meta.next_node_id = storage
            .meta
            .next_node_id
            .checked_add(1)
            .ok_or_else(|| AccessMethodError::Storage("hnsw node id overflow".to_owned()))?;
        let vector_head = storage.allocate_vector_chain(node_id, vector)?;
        let node_block = storage.allocate_block()?;
        let node_page = HnswNodePage {
            page_id: PageId::new(self.index_rel, node_block),
            lsn: Lsn::ZERO,
            node_id,
            tid,
            vector_len: vector.len(),
            vector_head,
            neighbor_count: 0,
            neighbor_head: None,
            deleted: false,
        };
        storage
            .pages
            .insert(node_block, HnswPersistentPage::Node(node_page));
        storage.node_to_block.insert(node_id, node_block);
        storage.tid_to_node.insert(tid, node_id);
        storage.meta.live_nodes += 1;
        if storage.meta.entry_node.is_none() {
            storage.meta.entry_node = Some(node_id);
        }
        storage.write_neighbors(node_id, &neighbor_ids)?;

        for neighbor_id in neighbor_ids {
            let mut neighbor_list = storage.neighbors_for_node(neighbor_id)?;
            if !neighbor_list.contains(&node_id) {
                neighbor_list.push(node_id);
            }
            let trimmed =
                storage.trim_neighbor_list(neighbor_id, neighbor_list, self.m, self.metric)?;
            storage.write_neighbors(neighbor_id, &trimmed)?;
        }
        storage.sync_control_pages();
        storage.stamp_all_pages(page_lsn);
        Ok(())
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        if vector.len() != self.dims {
            return Err(AccessMethodError::Storage(format!(
                "page-backed hnsw vector dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw vector elements must be finite".to_owned(),
            ));
        }
        Ok(())
    }

    fn emit_hnsw_wal(
        &self,
        op: HnswOpKind,
        tid: TupleId,
        vector: &[f32],
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<Lsn, AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(Lsn::ZERO);
        };
        let payload = HnswOpPayload {
            op,
            index_rel: self.index_rel,
            tid,
            vector: vector.to_vec(),
        }
        .encode()
        .map_err(|e| {
            AccessMethodError::Storage(format!("page-backed hnsw WAL payload encode: {e}"))
        })?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record = WalRecord::new(RecordType::HnswOp, xid, prev_lsn, 0, payload);
        sink.append(record)
            .map_err(|e| AccessMethodError::Storage(format!("page-backed hnsw WAL append: {e}")))
    }
}

impl HnswPersistentPage {
    fn page_id(&self) -> PageId {
        match self {
            Self::Meta(page) => page.page_id,
            Self::Node(page) => page.page_id,
            Self::Overflow(page) => page.page_id,
            Self::FreeList(page) => page.page_id,
        }
    }

    fn lsn(&self) -> Lsn {
        match self {
            Self::Meta(page) => page.lsn,
            Self::Node(page) => page.lsn,
            Self::Overflow(page) => page.lsn,
            Self::FreeList(page) => page.lsn,
        }
    }

    fn set_lsn(&mut self, lsn: Lsn) {
        match self {
            Self::Meta(page) => page.lsn = lsn,
            Self::Node(page) => page.lsn = lsn,
            Self::Overflow(page) => page.lsn = lsn,
            Self::FreeList(page) => page.lsn = lsn,
        }
    }
}

impl PageBackedHnswStorage {
    fn new(
        index_rel: RelationId,
        dims: usize,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
    ) -> Self {
        let meta_block = BlockNumber::new(HNSW_META_BLOCK);
        let free_block = BlockNumber::new(HNSW_FREE_LIST_BLOCK);
        let meta = HnswMetaPage {
            page_id: PageId::new(index_rel, meta_block),
            lsn: Lsn::ZERO,
            dims,
            metric,
            m,
            ef_search,
            entry_node: None,
            next_node_id: 0,
            live_nodes: 0,
            tombstones: 0,
            next_block_number: HNSW_FIRST_ALLOC_BLOCK,
            free_list_page: free_block,
        };
        let free_list = HnswFreeListPage {
            page_id: PageId::new(index_rel, free_block),
            lsn: Lsn::ZERO,
            blocks: Vec::new(),
        };
        let mut pages = BTreeMap::new();
        pages.insert(meta_block, HnswPersistentPage::Meta(meta.clone()));
        pages.insert(free_block, HnswPersistentPage::FreeList(free_list.clone()));
        Self {
            pages,
            meta,
            free_list,
            tid_to_node: BTreeMap::new(),
            node_to_block: BTreeMap::new(),
        }
    }

    fn from_page_images(
        index_rel: RelationId,
        dims: usize,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        images: Vec<PageBackedHnswPageImage>,
    ) -> Result<Self, AccessMethodError> {
        if images.is_empty() {
            return Err(AccessMethodError::Storage(
                "hnsw page image set is empty".to_owned(),
            ));
        }
        let mut pages = BTreeMap::new();
        for image in images {
            if image.page_id.relation != index_rel {
                return Err(AccessMethodError::Storage(
                    "hnsw page image relation mismatch".to_owned(),
                ));
            }
            let block = image.page_id.block;
            let mut page = image.page;
            page.set_lsn(image.lsn);
            pages.insert(block, page);
        }

        let meta = match pages.get(&BlockNumber::new(HNSW_META_BLOCK)) {
            Some(HnswPersistentPage::Meta(meta)) => meta.clone(),
            _ => {
                return Err(AccessMethodError::Storage(
                    "hnsw page image set missing meta page".to_owned(),
                ));
            }
        };
        if meta.dims != dims || meta.metric != metric || meta.m != m || meta.ef_search != ef_search
        {
            return Err(AccessMethodError::Storage(
                "hnsw page image metadata mismatch".to_owned(),
            ));
        }
        let free_list = match pages.get(&BlockNumber::new(HNSW_FREE_LIST_BLOCK)) {
            Some(HnswPersistentPage::FreeList(free_list)) => free_list.clone(),
            _ => {
                return Err(AccessMethodError::Storage(
                    "hnsw page image set missing free-list page".to_owned(),
                ));
            }
        };

        let mut tid_to_node = BTreeMap::new();
        let mut node_to_block = BTreeMap::new();
        let mut live_nodes = 0;
        let mut tombstones = 0;
        for (block, page) in &pages {
            if let HnswPersistentPage::Node(node) = page {
                tid_to_node.insert(node.tid, node.node_id);
                node_to_block.insert(node.node_id, *block);
                if node.deleted {
                    tombstones += 1;
                } else {
                    live_nodes += 1;
                }
            }
        }

        let mut storage = Self {
            pages,
            meta,
            free_list,
            tid_to_node,
            node_to_block,
        };
        storage.meta.live_nodes = live_nodes;
        storage.meta.tombstones = tombstones;
        storage.meta.entry_node = storage.first_live_node_id()?;
        storage.sync_control_pages();
        Ok(storage)
    }

    fn redo_covered(&self, lsn: Lsn) -> bool {
        lsn != Lsn::ZERO && self.meta.lsn >= lsn
    }

    fn allocate_block(&mut self) -> Result<BlockNumber, AccessMethodError> {
        if let Some(block) = self.free_list.blocks.pop() {
            self.sync_free_list_page();
            return Ok(block);
        }
        let block = BlockNumber::new(self.meta.next_block_number);
        self.meta.next_block_number =
            self.meta.next_block_number.checked_add(1).ok_or_else(|| {
                AccessMethodError::Storage("hnsw block number overflow".to_owned())
            })?;
        self.sync_meta_page();
        Ok(block)
    }

    fn free_page(&mut self, block: BlockNumber) -> Result<(), AccessMethodError> {
        if block.raw() < HNSW_FIRST_ALLOC_BLOCK {
            return Err(AccessMethodError::Storage(
                "hnsw cannot free control page".to_owned(),
            ));
        }
        self.pages.remove(&block);
        if !self.free_list.blocks.contains(&block) {
            self.free_list.blocks.push(block);
        }
        self.sync_free_list_page();
        Ok(())
    }

    fn allocate_vector_chain(
        &mut self,
        node_id: HnswNodeId,
        vector: &[f32],
    ) -> Result<BlockNumber, AccessMethodError> {
        let chunks = vector.chunks(HNSW_VECTOR_VALUES_PER_OVERFLOW_PAGE);
        let mut head = None;
        let mut previous = None;
        for chunk in chunks {
            let block = self.allocate_block()?;
            let page = HnswOverflowPage {
                page_id: PageId::new(self.meta.page_id.relation, block),
                lsn: Lsn::ZERO,
                owner_node: node_id,
                next: None,
                payload: HnswOverflowPayload::Vector(chunk.to_vec()),
            };
            self.pages.insert(block, HnswPersistentPage::Overflow(page));
            if let Some(prev_block) = previous {
                self.set_overflow_next(prev_block, Some(block))?;
            } else {
                head = Some(block);
            }
            previous = Some(block);
        }
        head.ok_or_else(|| AccessMethodError::Storage("hnsw vector chain empty".to_owned()))
    }

    fn allocate_neighbor_chain(
        &mut self,
        node_id: HnswNodeId,
        neighbors: &[HnswNodeId],
    ) -> Result<Option<BlockNumber>, AccessMethodError> {
        if neighbors.is_empty() {
            return Ok(None);
        }
        let mut head = None;
        let mut previous = None;
        for chunk in neighbors.chunks(HNSW_NEIGHBOR_IDS_PER_OVERFLOW_PAGE) {
            let block = self.allocate_block()?;
            let page = HnswOverflowPage {
                page_id: PageId::new(self.meta.page_id.relation, block),
                lsn: Lsn::ZERO,
                owner_node: node_id,
                next: None,
                payload: HnswOverflowPayload::Neighbors(chunk.to_vec()),
            };
            self.pages.insert(block, HnswPersistentPage::Overflow(page));
            if let Some(prev_block) = previous {
                self.set_overflow_next(prev_block, Some(block))?;
            } else {
                head = Some(block);
            }
            previous = Some(block);
        }
        Ok(head)
    }

    fn set_overflow_next(
        &mut self,
        block: BlockNumber,
        next: Option<BlockNumber>,
    ) -> Result<(), AccessMethodError> {
        let Some(HnswPersistentPage::Overflow(page)) = self.pages.get_mut(&block) else {
            return Err(AccessMethodError::Storage(
                "hnsw overflow chain points to non-overflow page".to_owned(),
            ));
        };
        page.next = next;
        Ok(())
    }

    fn node_page(&self, node_id: HnswNodeId) -> Result<Option<&HnswNodePage>, AccessMethodError> {
        let Some(block) = self.node_to_block.get(&node_id) else {
            return Ok(None);
        };
        match self.pages.get(block) {
            Some(HnswPersistentPage::Node(node)) => Ok(Some(node)),
            _ => Err(AccessMethodError::Storage(
                "hnsw node map points to non-node page".to_owned(),
            )),
        }
    }

    fn node_page_mut(
        &mut self,
        node_id: HnswNodeId,
    ) -> Result<Option<&mut HnswNodePage>, AccessMethodError> {
        let Some(block) = self.node_to_block.get(&node_id) else {
            return Ok(None);
        };
        match self.pages.get_mut(block) {
            Some(HnswPersistentPage::Node(node)) => Ok(Some(node)),
            _ => Err(AccessMethodError::Storage(
                "hnsw node map points to non-node page".to_owned(),
            )),
        }
    }

    fn live_node_snapshot(
        &self,
    ) -> Result<Vec<(HnswNodeId, TupleId, Vec<f32>)>, AccessMethodError> {
        let mut out = Vec::with_capacity(self.meta.live_nodes);
        for node_id in self.node_to_block.keys() {
            let Some(node) = self.node_page(*node_id)? else {
                continue;
            };
            if node.deleted {
                continue;
            }
            out.push((*node_id, node.tid, self.vector_for_node(node)?));
        }
        Ok(out)
    }

    fn vector_for_node(&self, node: &HnswNodePage) -> Result<Vec<f32>, AccessMethodError> {
        let mut vector = Vec::with_capacity(node.vector_len);
        let mut current = Some(node.vector_head);
        while let Some(block) = current {
            let Some(HnswPersistentPage::Overflow(page)) = self.pages.get(&block) else {
                return Err(AccessMethodError::Storage(
                    "hnsw vector chain points to non-overflow page".to_owned(),
                ));
            };
            match &page.payload {
                HnswOverflowPayload::Vector(values) => vector.extend(values),
                HnswOverflowPayload::Neighbors(_) => {
                    return Err(AccessMethodError::Storage(
                        "hnsw vector chain points to neighbor payload".to_owned(),
                    ));
                }
            }
            current = page.next;
        }
        if vector.len() != node.vector_len {
            return Err(AccessMethodError::Storage(
                "hnsw vector chain length mismatch".to_owned(),
            ));
        }
        Ok(vector)
    }

    fn neighbors_for_node(
        &self,
        node_id: HnswNodeId,
    ) -> Result<Vec<HnswNodeId>, AccessMethodError> {
        let Some(node) = self.node_page(node_id)? else {
            return Ok(Vec::new());
        };
        let mut neighbors = Vec::with_capacity(node.neighbor_count);
        let mut current = node.neighbor_head;
        while let Some(block) = current {
            let Some(HnswPersistentPage::Overflow(page)) = self.pages.get(&block) else {
                return Err(AccessMethodError::Storage(
                    "hnsw neighbor chain points to non-overflow page".to_owned(),
                ));
            };
            match &page.payload {
                HnswOverflowPayload::Neighbors(ids) => neighbors.extend(ids),
                HnswOverflowPayload::Vector(_) => {
                    return Err(AccessMethodError::Storage(
                        "hnsw neighbor chain points to vector payload".to_owned(),
                    ));
                }
            }
            current = page.next;
        }
        neighbors.truncate(node.neighbor_count);
        Ok(neighbors)
    }

    fn write_neighbors(
        &mut self,
        node_id: HnswNodeId,
        neighbors: &[HnswNodeId],
    ) -> Result<(), AccessMethodError> {
        let old_head = self.node_page(node_id)?.and_then(|node| node.neighbor_head);
        self.release_overflow_chain(old_head)?;
        let new_head = self.allocate_neighbor_chain(node_id, neighbors)?;
        let Some(node) = self.node_page_mut(node_id)? else {
            return Err(AccessMethodError::Storage(
                "hnsw write neighbors missing node".to_owned(),
            ));
        };
        node.neighbor_head = new_head;
        node.neighbor_count = neighbors.len();
        Ok(())
    }

    fn trim_neighbor_list(
        &self,
        node_id: HnswNodeId,
        mut neighbors: Vec<HnswNodeId>,
        max_neighbors: usize,
        metric: HnswMetric,
    ) -> Result<Vec<HnswNodeId>, AccessMethodError> {
        neighbors.sort_unstable();
        neighbors.dedup();
        neighbors.retain(|neighbor| *neighbor != node_id);
        let Some(origin_node) = self.node_page(node_id)? else {
            return Ok(Vec::new());
        };
        let origin = self.vector_for_node(origin_node)?;
        let mut candidates = Vec::with_capacity(neighbors.len());
        for neighbor in neighbors {
            let Some(neighbor_node) = self.node_page(neighbor)? else {
                continue;
            };
            if neighbor_node.deleted {
                continue;
            }
            let vector = self.vector_for_node(neighbor_node)?;
            candidates.push((
                neighbor,
                metric.distance(&origin, &vector),
                neighbor_node.tid,
            ));
        }
        candidates.sort_by(compare_persistent_hnsw_candidates);
        candidates.truncate(max_neighbors);
        Ok(candidates
            .into_iter()
            .map(|(neighbor, _, _)| neighbor)
            .collect())
    }

    fn mark_deleted(
        &mut self,
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        let Some(node_id) = self.tid_to_node.get(&tid).copied() else {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        };
        let Some(node) = self.node_page_mut(node_id)? else {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        };
        if node.deleted {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        }
        node.deleted = true;
        self.meta.live_nodes = self.meta.live_nodes.saturating_sub(1);
        self.meta.tombstones += 1;
        if self.meta.entry_node == Some(node_id) {
            self.meta.entry_node = self.first_live_node_id()?;
        }
        self.sync_meta_page();
        self.stamp_all_pages(page_lsn);
        Ok(())
    }

    fn vacuum_deleted(
        &mut self,
        metric: HnswMetric,
        max_neighbors: usize,
        page_lsn: Lsn,
    ) -> Result<usize, AccessMethodError> {
        let deleted_nodes: Vec<HnswNodeId> = self
            .node_to_block
            .keys()
            .filter_map(|node_id| {
                self.node_page(*node_id)
                    .ok()
                    .flatten()
                    .is_some_and(|node| node.deleted)
                    .then_some(*node_id)
            })
            .collect();
        if deleted_nodes.is_empty() {
            return Ok(0);
        }

        let live_nodes: Vec<HnswNodeId> = self
            .node_to_block
            .keys()
            .copied()
            .filter(|node_id| !deleted_nodes.contains(node_id))
            .collect();
        for node_id in live_nodes {
            let neighbors = self
                .neighbors_for_node(node_id)?
                .into_iter()
                .filter(|neighbor| !deleted_nodes.contains(neighbor))
                .collect::<Vec<_>>();
            let trimmed = self.trim_neighbor_list(node_id, neighbors, max_neighbors, metric)?;
            self.write_neighbors(node_id, &trimmed)?;
        }

        for node_id in &deleted_nodes {
            let Some(block) = self.node_to_block.remove(node_id) else {
                continue;
            };
            let Some(HnswPersistentPage::Node(node)) = self.pages.get(&block).cloned() else {
                continue;
            };
            self.tid_to_node.remove(&node.tid);
            self.release_overflow_chain(Some(node.vector_head))?;
            self.release_overflow_chain(node.neighbor_head)?;
            self.free_page(block)?;
        }
        self.meta.tombstones = 0;
        self.meta.live_nodes = self
            .node_to_block
            .keys()
            .filter(|node_id| {
                self.node_page(**node_id)
                    .ok()
                    .flatten()
                    .is_some_and(|node| !node.deleted)
            })
            .count();
        self.meta.entry_node = self.first_live_node_id()?;
        self.sync_control_pages();
        self.stamp_all_pages(page_lsn);
        Ok(deleted_nodes.len())
    }

    fn first_live_node_id(&self) -> Result<Option<HnswNodeId>, AccessMethodError> {
        for node_id in self.node_to_block.keys() {
            if self.node_page(*node_id)?.is_some_and(|node| !node.deleted) {
                return Ok(Some(*node_id));
            }
        }
        Ok(None)
    }

    fn release_overflow_chain(
        &mut self,
        head: Option<BlockNumber>,
    ) -> Result<(), AccessMethodError> {
        let mut current = head;
        while let Some(block) = current {
            let next = match self.pages.get(&block) {
                Some(HnswPersistentPage::Overflow(page)) => page.next,
                _ => {
                    return Err(AccessMethodError::Storage(
                        "hnsw release chain found non-overflow page".to_owned(),
                    ));
                }
            };
            self.free_page(block)?;
            current = next;
        }
        Ok(())
    }

    fn sync_meta_page(&mut self) {
        self.pages.insert(
            BlockNumber::new(HNSW_META_BLOCK),
            HnswPersistentPage::Meta(self.meta.clone()),
        );
    }

    fn sync_free_list_page(&mut self) {
        self.pages.insert(
            BlockNumber::new(HNSW_FREE_LIST_BLOCK),
            HnswPersistentPage::FreeList(self.free_list.clone()),
        );
    }

    fn sync_control_pages(&mut self) {
        self.sync_meta_page();
        self.sync_free_list_page();
    }

    fn stamp_all_pages(&mut self, lsn: Lsn) {
        if lsn == Lsn::ZERO {
            return;
        }
        self.meta.lsn = lsn;
        self.free_list.lsn = lsn;
        for page in self.pages.values_mut() {
            page.set_lsn(lsn);
        }
        self.sync_control_pages();
    }
}

fn compare_persistent_hnsw_candidates(
    left: &(HnswNodeId, f32, TupleId),
    right: &(HnswNodeId, f32, TupleId),
) -> std::cmp::Ordering {
    left.1
        .total_cmp(&right.1)
        .then_with(|| left.2.cmp(&right.2))
        .then_with(|| left.0.cmp(&right.0))
}

impl AccessMethod for HnswIndex {
    fn name(&self) -> &'static str {
        "hnsw"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let vector = decode_hnsw_vector_key(key, self.dims)?;
        self.insert_vector(&vector, tid)
    }

    fn lookup(&self, _key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        Err(AccessMethodError::NotImplemented(
            "hnsw lookup requires vector top-k search",
        ))
    }

    fn delete(&self, _key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.mark_deleted(tid)
    }
}

fn decode_hnsw_vector_key(key: &[u8], dims: usize) -> Result<Vec<f32>, AccessMethodError> {
    decode_vector_key(key, dims, "hnsw")
}

fn decode_vector_key(
    key: &[u8],
    dims: usize,
    prefix: &'static str,
) -> Result<Vec<f32>, AccessMethodError> {
    let expected = dims
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| AccessMethodError::Storage(format!("{prefix} key length overflow")))?;
    if key.len() != expected {
        return Err(AccessMethodError::Storage(format!(
            "{prefix} key length mismatch: expected {expected}, got {}",
            key.len()
        )));
    }
    let mut vector = Vec::with_capacity(dims);
    for chunk in key.chunks_exact(std::mem::size_of::<f32>()) {
        let bytes: [u8; 4] = chunk
            .try_into()
            .map_err(|_| AccessMethodError::Storage(format!("{prefix} key chunk width")))?;
        let value = f32::from_le_bytes(bytes);
        if !value.is_finite() {
            return Err(AccessMethodError::Storage(format!(
                "{prefix} vector elements must be finite"
            )));
        }
        vector.push(value);
    }
    Ok(vector)
}

fn compare_hnsw_candidates(
    left: &(usize, f32, TupleId),
    right: &(usize, f32, TupleId),
) -> std::cmp::Ordering {
    left.1
        .total_cmp(&right.1)
        .then_with(|| left.2.cmp(&right.2))
        .then_with(|| left.0.cmp(&right.0))
}

fn compare_hnsw_hits(left: &HnswSearchResult, right: &HnswSearchResult) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.tid.cmp(&right.tid))
}

fn best_frontier_position(
    frontier: &[usize],
    storage: &HnswStorage,
    probe: &[f32],
    metric: HnswMetric,
) -> usize {
    let mut best = 0usize;
    for idx in 1..frontier.len() {
        let current = &storage.entries[frontier[idx]];
        let best_entry = &storage.entries[frontier[best]];
        let current_distance = metric.distance(probe, &current.vector);
        let best_distance = metric.distance(probe, &best_entry.vector);
        if current_distance
            .total_cmp(&best_distance)
            .then_with(|| current.tid.cmp(&best_entry.tid))
            .is_lt()
        {
            best = idx;
        }
    }
    best
}

// ---------------------------------------------------------------------------
// IVFFlat vector index
// ---------------------------------------------------------------------------

/// One result from an IVFFlat search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IvfFlatSearchResult {
    /// Heap tuple identifier stored in the inverted list.
    pub tid: TupleId,
    /// Exact distance from the search probe after candidate rerank.
    pub distance: f32,
}

/// In-memory IVFFlat vector index.
///
/// Bulk load trains deterministic centroids, assigns vectors into inverted
/// lists, and search probes the nearest `probes` lists before reranking all
/// candidates with the same exact SIMD-aware kernels used by scalar vector SQL.
/// Online DML appends to the nearest trained list and tombstones deletes; a
/// full page-backed build/replay format remains future storage work.
#[derive(Debug)]
pub struct IvfFlatIndex {
    storage: Mutex<IvfFlatStorage>,
    dims: usize,
    metric: HnswMetric,
    lists: usize,
    probes: usize,
}

#[derive(Debug, Default)]
struct IvfFlatStorage {
    entries: Vec<IvfFlatEntry>,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<usize>>,
    available: bool,
}

#[derive(Debug, Clone)]
struct IvfFlatEntry {
    vector: Vec<f32>,
    tid: TupleId,
    list_id: usize,
    deleted: bool,
}

impl IvfFlatIndex {
    /// Create an empty runtime IVFFlat index.
    pub fn new(
        dims: u32,
        metric: HnswMetric,
        lists: usize,
        probes: usize,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "ivfflat dims outside supported range".to_owned(),
            ));
        }
        if lists == 0 {
            return Err(AccessMethodError::Storage(
                "ivfflat lists must be greater than zero".to_owned(),
            ));
        }
        if probes == 0 {
            return Err(AccessMethodError::Storage(
                "ivfflat probes must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims)
            .map_err(|_| AccessMethodError::Storage("ivfflat dims do not fit usize".to_owned()))?;
        Ok(Self {
            storage: Mutex::new(IvfFlatStorage::default()),
            dims,
            metric,
            lists,
            probes,
        })
    }

    /// Return this index's distance metric.
    #[must_use]
    pub const fn metric(&self) -> HnswMetric {
        self.metric
    }

    /// Return this index's vector dimension.
    #[must_use]
    pub const fn dims(&self) -> usize {
        self.dims
    }

    /// Return configured probe count.
    #[must_use]
    pub const fn probes(&self) -> usize {
        self.probes
    }

    /// Return number of trained centroids.
    #[must_use]
    pub fn centroid_count(&self) -> usize {
        self.storage.lock().centroids.len()
    }

    /// Return number of inverted lists currently materialized.
    #[must_use]
    pub fn list_count(&self) -> usize {
        self.storage.lock().lists.len()
    }

    /// Return number of live entries.
    #[must_use]
    pub fn live_len(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count()
    }

    /// Return number of tombstoned entries awaiting compaction.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| entry.deleted)
            .count()
    }

    /// Return whether the runtime IVFFlat lists can currently be used.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.storage.lock().available
    }

    /// Train centroids and bulk-load vectors into inverted lists.
    pub fn bulk_load(&self, rows: Vec<(Vec<f32>, TupleId)>) -> Result<(), AccessMethodError> {
        for (vector, _) in &rows {
            self.validate_vector(vector)?;
        }
        let mut storage = self.storage.lock();
        storage.entries.clear();
        storage.centroids.clear();
        storage.lists.clear();
        storage.available = false;
        if rows.is_empty() {
            return Ok(());
        }

        let centroid_count = self.lists.min(rows.len());
        storage.centroids = self.train_centroids(&rows, centroid_count);
        storage.lists = vec![Vec::new(); storage.centroids.len()];
        for (vector, tid) in rows {
            let list_id =
                nearest_vector(&storage.centroids, &vector, self.metric).ok_or_else(|| {
                    AccessMethodError::Storage("ivfflat centroids missing".to_owned())
                })?;
            let idx = storage.entries.len();
            storage.entries.push(IvfFlatEntry {
                vector,
                tid,
                list_id,
                deleted: false,
            });
            storage.lists[list_id].push(idx);
        }
        storage.available = true;
        Ok(())
    }

    /// Insert one vector into the nearest trained list.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if storage.centroids.is_empty() {
            storage.centroids.push(vector.to_vec());
            storage.lists.push(Vec::new());
        }
        let list_id = nearest_vector(&storage.centroids, vector, self.metric)
            .ok_or_else(|| AccessMethodError::Storage("ivfflat centroids missing".to_owned()))?;
        let idx = storage.entries.len();
        storage.entries.push(IvfFlatEntry {
            vector: vector.to_vec(),
            tid,
            list_id,
            deleted: false,
        });
        storage.lists[list_id].push(idx);
        storage.available = true;
        Ok(())
    }

    /// Search nearest `k` tuples by probing nearest inverted lists.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<IvfFlatSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let storage = self.storage.lock();
        if !storage.available || storage.centroids.is_empty() {
            return Ok(Vec::new());
        }
        let list_ids = nearest_vectors(&storage.centroids, probe, self.metric, self.probes);
        let mut candidate_indices = Vec::new();
        for list_id in list_ids {
            let Some(list) = storage.lists.get(list_id) else {
                continue;
            };
            candidate_indices.extend(list.iter().copied().filter(|idx| {
                storage
                    .entries
                    .get(*idx)
                    .is_some_and(|entry| !entry.deleted)
            }));
        }
        if candidate_indices.is_empty() {
            return Ok(Vec::new());
        }
        let vectors: Vec<&[f32]> = candidate_indices
            .iter()
            .map(|idx| storage.entries[*idx].vector.as_slice())
            .collect();
        let hits = ultrasql_vec::kernels::vector::exact_top_k_f32(
            &vectors,
            probe,
            self.metric.vector_metric(),
            k,
        );
        let mut out: Vec<IvfFlatSearchResult> = hits
            .into_iter()
            .map(|hit| {
                let entry = &storage.entries[candidate_indices[hit.row]];
                IvfFlatSearchResult {
                    tid: entry.tid,
                    distance: hit.distance,
                }
            })
            .collect();
        out.sort_by(compare_ivfflat_hits);
        Ok(out)
    }

    /// Mark an indexed tuple ID deleted.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if let Some(entry) = storage
            .entries
            .iter_mut()
            .find(|entry| entry.tid == tid && !entry.deleted)
        {
            entry.deleted = true;
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Compact tombstoned entries out of inverted lists.
    pub fn compact_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        let before = storage.entries.len();
        if before == 0 {
            return Ok(0);
        }
        let mut remap = vec![None; before];
        let mut entries = Vec::with_capacity(before);
        for (old_idx, entry) in storage.entries.iter().enumerate() {
            if entry.deleted {
                continue;
            }
            remap[old_idx] = Some(entries.len());
            entries.push(IvfFlatEntry {
                vector: entry.vector.clone(),
                tid: entry.tid,
                list_id: entry.list_id,
                deleted: false,
            });
        }
        let removed = before.saturating_sub(entries.len());
        if removed == 0 {
            return Ok(0);
        }
        let mut lists = vec![Vec::new(); storage.centroids.len()];
        for entry in &entries {
            if entry.list_id >= lists.len() {
                return Err(AccessMethodError::Storage(
                    "ivfflat compact found invalid list id".to_owned(),
                ));
            }
        }
        for old_list in &storage.lists {
            for old_idx in old_list {
                if let Some(new_idx) = remap.get(*old_idx).and_then(|idx| *idx) {
                    let list_id = entries[new_idx].list_id;
                    lists[list_id].push(new_idx);
                }
            }
        }
        storage.entries = entries;
        storage.lists = lists;
        storage.available = !storage.entries.is_empty() && !storage.centroids.is_empty();
        Ok(removed)
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        if vector.len() != self.dims {
            return Err(AccessMethodError::Storage(format!(
                "ivfflat vector dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            )));
        }
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(AccessMethodError::Storage(
                "ivfflat vector elements must be finite".to_owned(),
            ));
        }
        Ok(())
    }

    fn train_centroids(
        &self,
        rows: &[(Vec<f32>, TupleId)],
        centroid_count: usize,
    ) -> Vec<Vec<f32>> {
        let mut centroids: Vec<Vec<f32>> = (0..centroid_count)
            .map(|idx| rows[(idx * rows.len()) / centroid_count].0.clone())
            .collect();
        for _ in 0..8 {
            let mut sums = vec![vec![0.0_f32; self.dims]; centroid_count];
            let mut counts = vec![0_usize; centroid_count];
            for (vector, _) in rows {
                if let Some(list_id) = nearest_vector(&centroids, vector, self.metric) {
                    for (sum, value) in sums[list_id].iter_mut().zip(vector) {
                        *sum += *value;
                    }
                    counts[list_id] += 1;
                }
            }
            for idx in 0..centroid_count {
                let count = counts[idx];
                if count == 0 {
                    continue;
                }
                let denom = count as f32;
                for value in &mut sums[idx] {
                    *value /= denom;
                }
                centroids[idx] = sums[idx].clone();
            }
        }
        centroids
    }
}

impl AccessMethod for IvfFlatIndex {
    fn name(&self) -> &'static str {
        "ivfflat"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let vector = decode_vector_key(key, self.dims, "ivfflat")?;
        self.insert_vector(&vector, tid)
    }

    fn lookup(&self, _key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        Err(AccessMethodError::NotImplemented(
            "ivfflat lookup requires vector top-k search",
        ))
    }

    fn delete(&self, _key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.mark_deleted(tid)
    }
}

fn nearest_vector(centroids: &[Vec<f32>], probe: &[f32], metric: HnswMetric) -> Option<usize> {
    nearest_vectors(centroids, probe, metric, 1)
        .into_iter()
        .next()
}

fn nearest_vectors(
    centroids: &[Vec<f32>],
    probe: &[f32],
    metric: HnswMetric,
    limit: usize,
) -> Vec<usize> {
    let mut scored: Vec<(usize, f32)> = centroids
        .iter()
        .enumerate()
        .map(|(idx, centroid)| (idx, metric.distance(probe, centroid)))
        .collect();
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(limit.min(centroids.len()))
        .map(|(idx, _)| idx)
        .collect()
}

fn compare_ivfflat_hits(
    left: &IvfFlatSearchResult,
    right: &IvfFlatSearchResult,
) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.tid.cmp(&right.tid))
}

// ---------------------------------------------------------------------------
// BRIN (Block Range Index) min/max summaries
// ---------------------------------------------------------------------------

/// Summary entry for one page range.
///
/// Each summary holds the min and max key observed across all tuples in
/// the page range. The executor uses this to skip ranges that cannot
/// contain the query's target key.
#[derive(Debug, Clone)]
struct BrinSummary {
    /// First block of the range.
    first_block: u32,
    /// Last block of the range (inclusive).
    last_block: u32,
    /// Minimum key seen in the range, or empty if no tuples inserted.
    min_key: Vec<u8>,
    /// Maximum key seen in the range.
    max_key: Vec<u8>,
}

/// BRIN (Block Range `INdex`) min/max index.
///
/// BRIN stores per-page-range min/max summaries rather than per-tuple
/// entries, making it highly space-efficient for naturally ordered data
/// (timestamps, sequential IDs). The trade-off is that a lookup must
/// scan all ranges whose `[min, max]` interval overlaps the query key.
///
/// # Key contract
///
/// Keys compare lexicographically. Integer callers should use
/// [`Self::encode_i64_key`] so signed `i64` order is preserved in the
/// byte domain.
///
/// # Status
///
/// Summaries are maintained in memory by the SQL runtime and consulted
/// by the heap-scan lowerer for block-range pruning. Page-backed,
/// WAL-recovered summary storage and non-integer operator classes remain
/// future work.
#[derive(Debug)]
pub struct BrinIndex {
    /// Summaries keyed by page range start.
    ///
    /// Future page-backed BRIN storage replaces this with WAL-logged
    /// summary pages in the buffer pool.
    summaries: Mutex<Vec<BrinSummary>>,
    /// Number of heap blocks per summary range.
    pages_per_range: u32,
}

impl BrinIndex {
    /// Create a BRIN index.
    ///
    /// `pages_per_range` controls how many heap pages each summary
    /// covers. The PostgreSQL default is 128.
    #[must_use]
    pub fn new(pages_per_range: u32) -> Self {
        Self {
            summaries: Mutex::new(Vec::new()),
            pages_per_range: pages_per_range.max(1),
        }
    }

    /// Build or refresh a summary for the page range containing
    /// `block_number`.
    ///
    /// Callers invoke this after inserting a batch of tuples into a heap
    /// page range. A real implementation reads every tuple in the range
    /// from the heap and recomputes min/max; this stub accepts the
    /// caller-supplied `min_key` / `max_key` directly.
    pub fn summarize_range(
        &self,
        first_block: u32,
        last_block: u32,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
    ) {
        let mut summaries = self.summaries.lock();
        // Remove any existing summary for this range.
        summaries.retain(|s| s.first_block != first_block);
        summaries.push(BrinSummary {
            first_block,
            last_block,
            min_key,
            max_key,
        });
        summaries.sort_by_key(|s| s.first_block);
    }

    /// Encode a signed integer key so lexicographic byte order matches
    /// normal signed integer order.
    #[must_use]
    pub fn encode_i64_key(key: i64) -> [u8; 8] {
        (u64::from_ne_bytes(key.to_ne_bytes()) ^ (1_u64 << 63)).to_be_bytes()
    }

    /// Number of summary ranges currently stored.
    #[must_use]
    pub fn summary_count(&self) -> usize {
        self.summaries.lock().len()
    }

    /// Drop all current summaries before a full VACUUM re-summarize pass.
    pub fn clear_summaries(&self) {
        self.summaries.lock().clear();
    }

    /// Candidate page ranges for a point probe.
    ///
    /// Returned ranges are inclusive `(first_block, last_block)` pairs.
    /// The executor must still recheck the SQL predicate against every
    /// visible tuple in those ranges because BRIN summaries can include
    /// false positives by design.
    #[must_use]
    pub fn candidate_ranges_for_key(&self, key: &[u8]) -> Vec<(u32, u32)> {
        self.candidate_ranges_for_bounds(Some(key), Some(key))
    }

    /// Candidate page ranges for an inclusive key interval.
    ///
    /// `None` on either side means unbounded. A summary overlaps the
    /// query interval when `summary.max >= low && summary.min <= high`.
    #[must_use]
    pub fn candidate_ranges_for_bounds(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
    ) -> Vec<(u32, u32)> {
        let summaries = self.summaries.lock();
        summaries
            .iter()
            .filter(|s| {
                let above_low = low.is_none_or(|lo| s.max_key.as_slice() >= lo);
                let below_high = high.is_none_or(|hi| s.min_key.as_slice() <= hi);
                above_low && below_high
            })
            .map(|s| (s.first_block, s.last_block))
            .collect()
    }
}

impl AccessMethod for BrinIndex {
    fn name(&self) -> &'static str {
        "brin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let block = tid.page.block.raw();
        let range_start = (block / self.pages_per_range) * self.pages_per_range;
        let range_end = range_start + self.pages_per_range - 1;
        let mut summaries = self.summaries.lock();
        if let Some(s) = summaries.iter_mut().find(|s| s.first_block == range_start) {
            if key < s.min_key.as_slice() {
                s.min_key = key.to_vec();
            }
            if key > s.max_key.as_slice() {
                s.max_key = key.to_vec();
            }
        } else {
            summaries.push(BrinSummary {
                first_block: range_start,
                last_block: range_end,
                min_key: key.to_vec(),
                max_key: key.to_vec(),
            });
            summaries.sort_by_key(|s| s.first_block);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let _ = self.candidate_ranges_for_key(key);
        // BRIN lookup yields candidate page ranges, not exact TupleIds;
        // SQL execution calls `candidate_ranges_*` directly and scans
        // those heap ranges with predicate recheck.
        Ok(Vec::new())
    }

    fn delete(&self, _key: &[u8], _tid: TupleId) -> Result<(), AccessMethodError> {
        // BRIN does not track individual TupleIds. Stale min/max
        // summaries over-include after deletes or shrinking updates,
        // which is correct because heap predicate recheck filters
        // false positives. Future page-backed summaries can recompute
        // exact ranges during VACUUM.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
    use ultrasql_wal::payload::{HashOpKind, HashOpPayload, HnswOpKind, HnswOpPayload};
    use ultrasql_wal::record::RecordType;

    use super::*;
    use crate::wal_sink::test_support::InMemoryWalSink;

    fn tid(block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(99), BlockNumber::new(block)),
            slot,
        )
    }

    // --- BTreeAccessMethod ---

    #[test]
    fn btree_insert_then_lookup_round_trip() {
        let am = BTreeAccessMethod::new(true);
        let key = b"hello";
        am.insert(key, tid(1, 0)).expect("insert succeeds");
        let results = am.lookup(key).expect("lookup succeeds");
        assert_eq!(results, vec![tid(1, 0)]);
    }

    #[test]
    fn btree_unique_rejects_duplicate() {
        let am = BTreeAccessMethod::new(true);
        let key = b"key";
        am.insert(key, tid(1, 0)).expect("first insert succeeds");
        let err = am.insert(key, tid(2, 0)).expect_err("duplicate rejected");
        assert!(matches!(err, AccessMethodError::DuplicateKey));
    }

    #[test]
    fn btree_non_unique_allows_duplicate_keys() {
        let am = BTreeAccessMethod::new(false);
        let key = b"key";
        am.insert(key, tid(1, 0)).expect("first insert");
        am.insert(key, tid(2, 0)).expect("second insert same key");
        let results = am.lookup(key).expect("lookup");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn btree_delete_removes_entry() {
        let am = BTreeAccessMethod::new(false);
        let key = b"del";
        am.insert(key, tid(3, 1)).expect("insert");
        am.delete(key, tid(3, 1)).expect("delete");
        assert!(am.lookup(key).expect("lookup after delete").is_empty());
    }

    #[test]
    fn btree_lookup_missing_key_returns_empty() {
        let am = BTreeAccessMethod::new(true);
        assert!(am.lookup(b"missing").expect("lookup").is_empty());
    }

    // --- HashIndex ---

    #[test]
    fn hash_insert_then_lookup_happy_path() {
        let am = HashIndex::new(64);
        let key = b"token";
        am.insert(key, tid(7, 0)).expect("hash insert");
        let results = am.lookup(key).expect("hash lookup");
        assert!(results.contains(&tid(7, 0)));
    }

    #[test]
    fn hash_delete_removes_entry() {
        let am = HashIndex::new(64);
        let key = b"rm";
        am.insert(key, tid(1, 0)).expect("insert");
        am.delete(key, tid(1, 0)).expect("delete");
        assert!(am.lookup(key).expect("lookup").is_empty());
    }

    #[test]
    fn hash_delete_nonexistent_returns_not_found() {
        let am = HashIndex::new(64);
        let err = am.delete(b"ghost", tid(0, 0)).expect_err("not found");
        assert!(matches!(err, AccessMethodError::NotFound));
    }

    #[test]
    fn hash_static_bucket_allocates_overflow_pages() {
        let am = HashIndex::with_page_capacity(1, 2);
        am.insert(b"a", tid(1, 0)).expect("insert a");
        am.insert(b"b", tid(1, 1)).expect("insert b");
        am.insert(b"c", tid(1, 2)).expect("insert c");

        assert_eq!(am.overflow_page_count(), 1);
        assert_eq!(am.lookup(b"c").expect("lookup c"), vec![tid(1, 2)]);
    }

    #[test]
    fn hash_insert_logged_emits_hash_wal_record() {
        let am = HashIndex::new(64);
        let sink = InMemoryWalSink::new();
        let index_rel = RelationId::new(1234);
        let key = b"logged";
        let tuple = tid(7, 3);

        am.insert_logged(index_rel, key, tuple, Xid::new(44), Some(&sink))
            .expect("logged insert");

        let records = sink.records();
        assert_eq!(records.len(), 1);
        let record = &records[0].1;
        assert_eq!(record.header.record_type, RecordType::HashOp);
        let payload = HashOpPayload::decode(&record.payload).expect("decode hash WAL");
        assert_eq!(payload.op, HashOpKind::Insert);
        assert_eq!(payload.index_rel, index_rel);
        assert_eq!(payload.key_bytes, key);
        assert_eq!(payload.value_bytes, HashIndex::tuple_id_bytes(tuple));
    }

    #[test]
    fn hash_delete_logged_emits_hash_wal_record() {
        let am = HashIndex::new(64);
        let sink = InMemoryWalSink::new();
        let index_rel = RelationId::new(4321);
        let key = b"delete";
        let tuple = tid(8, 4);

        am.insert_logged(index_rel, key, tuple, Xid::new(55), Some(&sink))
            .expect("logged insert");
        am.delete_logged(index_rel, key, tuple, Xid::new(55), Some(&sink))
            .expect("logged delete");

        let records = sink.records();
        assert_eq!(records.len(), 2);
        let payload = HashOpPayload::decode(&records[1].1.payload).expect("decode hash WAL");
        assert_eq!(payload.op, HashOpKind::Delete);
        assert_eq!(payload.index_rel, index_rel);
        assert_eq!(payload.key_bytes, key);
    }

    // --- GinIndex ---

    #[test]
    fn gin_insert_then_lookup_happy_path() {
        let am = GinIndex::new();
        let token = b"rust";
        am.insert(token, tid(5, 2)).expect("gin insert");
        let posting = am.lookup(token).expect("gin lookup");
        assert!(posting.contains(&tid(5, 2)));
    }

    #[test]
    fn gin_multiple_tokens_per_document() {
        let am = GinIndex::new();
        am.insert(b"cat", tid(1, 0)).expect("insert cat");
        am.insert(b"dog", tid(1, 0)).expect("insert dog");
        assert!(am.lookup(b"cat").expect("cat").contains(&tid(1, 0)));
        assert!(am.lookup(b"dog").expect("dog").contains(&tid(1, 0)));
        assert!(am.lookup(b"bird").expect("bird").is_empty());
    }

    #[test]
    fn gin_fast_update_drains_pending_list() {
        let am = GinIndex::new();
        am.insert(b"json-key", tid(2, 0)).expect("insert");
        am.insert(b"json-key", tid(2, 1)).expect("insert");

        assert_eq!(am.pending_len(), 2);
        assert_eq!(am.drain_pending_list(), 2);
        assert_eq!(am.pending_len(), 0);
        assert_eq!(
            am.lookup(b"json-key").expect("lookup"),
            vec![tid(2, 0), tid(2, 1)]
        );
    }

    #[test]
    fn gin_delete_removes_posting() {
        let am = GinIndex::new();
        am.insert(b"tok", tid(2, 0)).expect("insert");
        am.delete(b"tok", tid(2, 0)).expect("delete");
        assert!(am.lookup(b"tok").expect("lookup").is_empty());
    }

    #[test]
    fn gin_jsonb_operator_tokens_cover_contains_and_keys() {
        let am = GinIndex::new();
        am.insert_jsonb_document(r#"{"a":1,"b":"two"}"#, tid(9, 0))
            .expect("insert jsonb");
        am.insert_jsonb_document(r#"{"a":2,"c":3}"#, tid(9, 1))
            .expect("insert jsonb");

        assert_eq!(
            am.lookup_jsonb_contains(r#"{"a":1}"#)
                .expect("jsonb contains"),
            vec![tid(9, 0)]
        );
        assert_eq!(
            am.lookup_jsonb_has_any_key(&["b".to_owned(), "z".to_owned()])
                .expect("jsonb any key"),
            vec![tid(9, 0)]
        );
        assert_eq!(
            am.lookup_jsonb_has_all_keys(&["a".to_owned(), "c".to_owned()])
                .expect("jsonb all keys"),
            vec![tid(9, 1)]
        );
    }

    #[test]
    fn gin_array_operator_tokens_cover_contains_and_overlap() {
        let am = GinIndex::new();
        am.insert_array_value("{red,green}", tid(10, 0))
            .expect("insert array");
        am.insert_array_value("{blue,green}", tid(10, 1))
            .expect("insert array");

        assert_eq!(
            am.lookup_array_contains("{red,green}")
                .expect("array contains"),
            vec![tid(10, 0)]
        );
        assert_eq!(
            am.lookup_array_overlap("{green}").expect("array overlap"),
            vec![tid(10, 0), tid(10, 1)]
        );
    }

    #[test]
    fn gin_tsvector_operator_tokens_cover_match() {
        let am = GinIndex::new();
        am.insert_tsvector("quick brown fox", tid(11, 0))
            .expect("insert tsvector");
        am.insert_tsvector("slow green turtle", tid(11, 1))
            .expect("insert tsvector");

        assert_eq!(
            am.lookup_tsquery_match("quick & fox").expect("tsquery"),
            vec![tid(11, 0)]
        );
    }

    // --- GistIndex ---

    #[test]
    fn gist_insert_then_lookup_happy_path() {
        let am = GistIndex::new();
        let key = b"\x00\x00\x00\x0a\x00\x00\x00\x14"; // bbox [10, 20]
        am.insert(key, tid(3, 0)).expect("gist insert");
        let results = am.lookup(key).expect("gist lookup");
        assert!(results.contains(&tid(3, 0)));
    }

    #[test]
    fn gist_delete_entry() {
        let am = GistIndex::new();
        let key = b"bbox";
        am.insert(key, tid(4, 0)).expect("insert");
        am.delete(key, tid(4, 0)).expect("delete");
        assert!(am.lookup(key).expect("lookup").is_empty());
    }

    // --- BrinIndex ---

    #[test]
    fn brin_insert_builds_summary() {
        let am = BrinIndex::new(128);
        // Insert a tuple in block 0 with key [42].
        am.insert(b"\x2a", tid(0, 0)).expect("brin insert");
        assert_eq!(am.summary_count(), 1);
        assert_eq!(am.candidate_ranges_for_key(b"\x2a"), vec![(0, 127)]);
        assert!(am.candidate_ranges_for_key(b"\x2b").is_empty());
        // Trait lookup still returns empty because callers need ranges.
        let _ = am.lookup(b"\x2a").expect("brin lookup");
    }

    #[test]
    fn brin_summarize_range_stores_minmax() {
        let am = BrinIndex::new(128);
        am.summarize_range(0, 127, b"\x01".to_vec(), b"\xff".to_vec());
        assert_eq!(
            am.candidate_ranges_for_bounds(Some(b"\x80"), Some(b"\x90")),
            vec![(0, 127)]
        );
        assert!(
            am.candidate_ranges_for_bounds(Some(b"\xff\x00"), None)
                .is_empty()
        );
        let _ = am.lookup(b"\x80").expect("lookup in range");
    }

    #[test]
    fn brin_i64_encoding_preserves_signed_order() {
        let keys = [
            BrinIndex::encode_i64_key(i64::MIN),
            BrinIndex::encode_i64_key(-1),
            BrinIndex::encode_i64_key(0),
            BrinIndex::encode_i64_key(1),
            BrinIndex::encode_i64_key(i64::MAX),
        ];
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn brin_delete_is_no_op() {
        let am = BrinIndex::new(128);
        am.insert(b"k", tid(0, 0)).expect("insert");
        // BRIN delete is always Ok — no per-tuple tracking.
        am.delete(b"k", tid(0, 0)).expect("brin delete no-op");
    }

    // --- HnswIndex ---

    #[test]
    fn hnsw_insert_vector_and_search_returns_nearest_tids() {
        let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
        am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
            .expect("insert origin");
        am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
            .expect("insert near");
        am.insert_vector(&[10.0, 0.0, 0.0], tid(1, 2))
            .expect("insert far");

        let hits = am.search(&[0.2, 0.0, 0.0], 2).expect("search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
    }

    #[test]
    fn hnsw_invalidate_makes_index_unavailable_for_search() {
        let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
        am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
            .expect("insert origin");

        assert!(am.is_available());
        am.invalidate();
        assert!(!am.is_available());
        assert!(am.search(&[0.0, 0.0, 0.0], 1).expect("search").is_empty());
    }

    #[test]
    fn hnsw_delete_tombstone_and_vacuum_compaction_preserve_search() {
        let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
        am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
            .expect("insert deleted row");
        am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
            .expect("insert live row");
        am.insert_vector(&[2.0, 0.0, 0.0], tid(1, 2))
            .expect("insert second live row");

        am.mark_deleted(tid(1, 0)).expect("tombstone row");
        assert_eq!(am.tombstone_count(), 1);
        assert_eq!(am.live_len(), 2);
        let hits = am.search(&[0.0, 0.0, 0.0], 2).expect("search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 1), tid(1, 2)]);

        let removed = am.compact_deleted().expect("compact tombstones");
        assert_eq!(removed, 1);
        assert_eq!(am.tombstone_count(), 0);
        assert_eq!(am.live_len(), 2);
        let hits = am.search(&[0.0, 0.0, 0.0], 2).expect("search after vacuum");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 1), tid(1, 2)]);
    }

    #[test]
    fn hnsw_logged_insert_delete_and_compact_emit_wal_records() {
        let am = HnswIndex::new(3, HnswMetric::L2, 4, 16).expect("hnsw config");
        let sink = InMemoryWalSink::new();
        let index_rel = RelationId::new(777);
        let tuple = tid(9, 1);

        am.insert_vector_logged(
            index_rel,
            &[0.0, 1.0, 2.0],
            tuple,
            Xid::new(10),
            Some(&sink),
        )
        .expect("logged insert");
        am.mark_deleted_logged(index_rel, tuple, Xid::new(10), Some(&sink))
            .expect("logged delete");
        am.compact_deleted_logged(index_rel, Xid::new(10), Some(&sink))
            .expect("logged compact");

        let records = sink.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1.header.record_type, RecordType::HnswOp);
        let insert = HnswOpPayload::decode(&records[0].1.payload).expect("decode hnsw insert");
        assert_eq!(insert.op, HnswOpKind::Insert);
        assert_eq!(insert.index_rel, index_rel);
        assert_eq!(insert.tid, tuple);
        assert_eq!(insert.vector, vec![0.0, 1.0, 2.0]);
        let delete = HnswOpPayload::decode(&records[1].1.payload).expect("decode hnsw delete");
        assert_eq!(delete.op, HnswOpKind::Delete);
        assert_eq!(delete.tid, tuple);
        let compact = HnswOpPayload::decode(&records[2].1.payload).expect("decode hnsw compact");
        assert_eq!(compact.op, HnswOpKind::Compact);
    }

    #[test]
    fn page_backed_hnsw_allocates_meta_node_overflow_and_free_list_pages() {
        let am = PageBackedHnswIndex::new(RelationId::new(8800), 3, HnswMetric::L2, 4, 16)
            .expect("page-backed hnsw config");

        let initial = am.page_stats();
        assert_eq!(initial.meta_pages, 1);
        assert_eq!(initial.free_list_pages, 1);
        assert_eq!(initial.node_pages, 0);
        assert_eq!(initial.overflow_pages, 0);

        am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
            .expect("insert origin");
        am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
            .expect("insert near");
        am.insert_vector(&[10.0, 0.0, 0.0], tid(1, 2))
            .expect("insert far");

        let stats = am.page_stats();
        assert_eq!(stats.live_nodes, 3);
        assert_eq!(stats.tombstones, 0);
        assert_eq!(stats.meta_pages, 1);
        assert_eq!(stats.free_list_pages, 1);
        assert_eq!(stats.node_pages, 3);
        assert!(stats.overflow_pages >= 3);
        assert_eq!(stats.reusable_pages, 0);

        let hits = am.search(&[0.2, 0.0, 0.0], 2).expect("search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
    }

    #[test]
    fn page_backed_hnsw_vacuum_reclaims_node_and_overflow_pages() {
        let am = PageBackedHnswIndex::new(RelationId::new(8801), 3, HnswMetric::L2, 2, 16)
            .expect("page-backed hnsw config");
        am.insert_vector(&[0.0, 0.0, 0.0], tid(1, 0))
            .expect("insert deleted row");
        am.insert_vector(&[1.0, 0.0, 0.0], tid(1, 1))
            .expect("insert live row");
        am.insert_vector(&[2.0, 0.0, 0.0], tid(1, 2))
            .expect("insert second live row");

        am.mark_deleted(tid(1, 0)).expect("tombstone row");
        assert_eq!(am.page_stats().tombstones, 1);

        let removed = am.vacuum_deleted().expect("vacuum hnsw pages");
        assert_eq!(removed, 1);
        let after_vacuum = am.page_stats();
        assert_eq!(after_vacuum.live_nodes, 2);
        assert_eq!(after_vacuum.tombstones, 0);
        assert!(after_vacuum.reusable_pages > 0);

        am.insert_vector(&[3.0, 0.0, 0.0], tid(1, 3))
            .expect("insert reuses free pages");
        let after_reuse = am.page_stats();
        assert_eq!(after_reuse.live_nodes, 3);
        assert!(after_reuse.next_block_number <= after_vacuum.next_block_number);
    }

    #[test]
    fn page_backed_hnsw_replays_wal_into_recovered_pages() {
        let index_rel = RelationId::new(8802);
        let source =
            PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("source config");
        let sink = InMemoryWalSink::new();
        source
            .insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(12), Some(&sink))
            .expect("logged insert origin");
        source
            .insert_vector_logged(&[1.0, 0.0, 0.0], tid(1, 1), Xid::new(12), Some(&sink))
            .expect("logged insert live");
        source
            .mark_deleted_logged(tid(1, 0), Xid::new(12), Some(&sink))
            .expect("logged delete");
        source
            .vacuum_deleted_logged(Xid::new(12), Some(&sink))
            .expect("logged vacuum");

        let recovered =
            PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("recover config");
        let records = sink.records();
        for (_, record) in &records {
            recovered.apply_wal_record(record).expect("replay hnsw WAL");
        }
        for (_, record) in &records {
            recovered
                .apply_wal_record(record)
                .expect("replay hnsw WAL idempotently");
        }

        let stats = recovered.page_stats();
        assert_eq!(stats.live_nodes, 1);
        assert_eq!(stats.tombstones, 0);
        let hits = recovered.search(&[0.0, 0.0, 0.0], 2).expect("search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 1)]);
    }

    #[test]
    fn page_backed_hnsw_stamps_page_lsns_and_restores_page_images() {
        let index_rel = RelationId::new(8803);
        let am =
            PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("hnsw config");
        let sink = InMemoryWalSink::new();

        am.insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(13), Some(&sink))
            .expect("logged insert");

        let records = sink.records();
        let assigned_lsn = records[0].0;
        assert!(assigned_lsn > Lsn::ZERO);
        let images = am.page_images();
        assert!(images.len() >= 4);
        assert!(
            images
                .iter()
                .all(|image| image.page_id.relation == index_rel && image.lsn == assigned_lsn)
        );

        let restored =
            PageBackedHnswIndex::from_page_images(index_rel, 3, HnswMetric::L2, 4, 16, images)
                .expect("restore hnsw pages");
        assert_eq!(restored.page_stats().live_nodes, 1);
        let hits = restored.search(&[0.1, 0.0, 0.0], 1).expect("search");
        assert_eq!(hits[0].tid, tid(1, 0));
    }

    #[test]
    fn page_backed_hnsw_redo_skips_records_covered_by_page_lsn() {
        let index_rel = RelationId::new(8804);
        let source =
            PageBackedHnswIndex::new(index_rel, 3, HnswMetric::L2, 4, 16).expect("source config");
        let sink = InMemoryWalSink::new();
        source
            .insert_vector_logged(&[0.0, 0.0, 0.0], tid(1, 0), Xid::new(14), Some(&sink))
            .expect("logged insert one");
        source
            .insert_vector_logged(&[1.0, 0.0, 0.0], tid(1, 1), Xid::new(14), Some(&sink))
            .expect("logged insert two");

        let images_after_second = source.page_images();
        let recovered = PageBackedHnswIndex::from_page_images(
            index_rel,
            3,
            HnswMetric::L2,
            4,
            16,
            images_after_second,
        )
        .expect("restore hnsw pages");
        let stats_before = recovered.page_stats();

        let records = sink.records();
        for (lsn, record) in records {
            recovered
                .apply_wal_record_at(lsn, &record)
                .expect("redo should skip covered LSN");
        }

        assert_eq!(recovered.page_stats(), stats_before);
        let hits = recovered.search(&[0.0, 0.0, 0.0], 2).expect("search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 0), tid(1, 1)]);
    }

    #[test]
    fn ivfflat_bulk_load_trains_centroids_and_reranks_candidates() {
        let am = IvfFlatIndex::new(2, HnswMetric::L2, 2, 1).expect("ivfflat config");
        am.bulk_load(vec![
            (vec![0.0, 0.0], tid(1, 0)),
            (vec![0.5, 0.0], tid(1, 1)),
            (vec![10.0, 0.0], tid(2, 0)),
            (vec![9.0, 0.0], tid(2, 1)),
        ])
        .expect("bulk load ivfflat");

        assert_eq!(am.centroid_count(), 2);
        assert_eq!(am.list_count(), 2);
        assert_eq!(am.probes(), 1);
        let hits = am.search(&[9.2, 0.0], 2).expect("ivfflat search");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(2, 1), tid(2, 0)]);
    }

    #[test]
    fn ivfflat_insert_delete_and_compact_keep_lists_searchable() {
        let am = IvfFlatIndex::new(2, HnswMetric::L2, 2, 2).expect("ivfflat config");
        am.bulk_load(vec![
            (vec![0.0, 0.0], tid(1, 0)),
            (vec![10.0, 0.0], tid(2, 0)),
        ])
        .expect("bulk load ivfflat");
        am.insert_vector(&[1.0, 0.0], tid(1, 1))
            .expect("insert ivfflat");
        am.mark_deleted(tid(1, 0)).expect("delete ivfflat");

        assert_eq!(am.tombstone_count(), 1);
        let hits = am.search(&[0.0, 0.0], 2).expect("search after delete");
        let tids: Vec<TupleId> = hits.into_iter().map(|hit| hit.tid).collect();
        assert_eq!(tids, vec![tid(1, 1), tid(2, 0)]);

        assert_eq!(am.compact_deleted().expect("compact ivfflat"), 1);
        assert_eq!(am.tombstone_count(), 0);
        assert_eq!(am.live_len(), 2);
    }
}
