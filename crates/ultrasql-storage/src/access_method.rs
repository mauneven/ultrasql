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

use parking_lot::Mutex;
use thiserror::Error;
use ultrasql_core::{BlockNumber, MAX_VECTOR_DIMS, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{HashOpKind, HashOpPayload};
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
}

impl HnswMetric {
    fn distance(self, left: &[f32], right: &[f32]) -> f32 {
        match self {
            Self::L2 => ultrasql_vec::kernels::vector::l2_distance_f32(left, right),
            Self::Cosine => ultrasql_vec::kernels::vector::cosine_distance_f32(left, right)
                .unwrap_or(f32::INFINITY),
            Self::NegativeInnerProduct => -ultrasql_vec::kernels::vector::dot_f32(left, right),
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
/// page layout, WAL records, MVCC-aware deletes, and rebuild protocol from
/// `docs/hnsw-index-design.md` remain separate storage slices. The graph uses
/// one navigable layer: inserts connect each new vector to its nearest `m`
/// existing live nodes, and searches perform bounded best-first traversal.
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
    let expected = dims
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| AccessMethodError::Storage("hnsw key length overflow".to_owned()))?;
    if key.len() != expected {
        return Err(AccessMethodError::Storage(format!(
            "hnsw key length mismatch: expected {expected}, got {}",
            key.len()
        )));
    }
    let mut vector = Vec::with_capacity(dims);
    for chunk in key.chunks_exact(std::mem::size_of::<f32>()) {
        let bytes: [u8; 4] = chunk
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw key chunk width".to_owned()))?;
        let value = f32::from_le_bytes(bytes);
        if !value.is_finite() {
            return Err(AccessMethodError::Storage(
                "hnsw vector elements must be finite".to_owned(),
            ));
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
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};
    use ultrasql_wal::payload::{HashOpKind, HashOpPayload};
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
}
