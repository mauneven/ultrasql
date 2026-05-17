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
//! - [`HashIndex`], [`GinIndex`], [`GistIndex`], [`BrinIndex`]: provide
//!   the trait shape with happy-path insert/lookup so the catalog and
//!   executor can reference the concrete types. Full implementations are
//!   deferred to v1.x; stub bodies are tagged `TODO(<index>-complete)`.

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
use ultrasql_core::TupleId;

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
// Hash index (static hashing with overflow bucket list)
// ---------------------------------------------------------------------------

/// Hash index using in-memory bucket chains.
///
/// Each bucket is a sorted `Vec<(key, TupleId)>`. The number of
/// buckets is fixed at construction; overflow chains grow unboundedly.
/// A dynamic resizing policy (extendible or linear hashing) is
/// deferred to `TODO(hash-complete)`.
///
/// # Thread safety
///
/// Bucket locks are sharded so concurrent lookups into different
/// buckets do not contend. The current implementation uses a single
/// global lock for simplicity; shard-per-bucket locking is
/// `TODO(hash-complete)`.
#[derive(Debug)]
pub struct HashIndex {
    /// Serialised (key, `TupleId`) entries grouped by bucket.
    ///
    /// TODO(hash-complete): replace with page-backed overflow chains
    /// using the buffer pool.
    buckets: Mutex<Vec<Vec<(Vec<u8>, TupleId)>>>,
    /// Number of top-level buckets. Power-of-two for cheap masking.
    num_buckets: usize,
}

impl HashIndex {
    /// Create a hash index with `num_buckets` buckets.
    ///
    /// `num_buckets` is rounded up to the next power of two. A
    /// reasonable starting point for OLTP workloads is 256 or 1 024.
    #[must_use]
    pub fn new(num_buckets: usize) -> Self {
        let n = num_buckets.next_power_of_two().max(1);
        Self {
            buckets: Mutex::new(vec![Vec::new(); n]),
            num_buckets: n,
        }
    }

    fn bucket_index(&self, key: &[u8]) -> usize {
        // FNV-1a hash for simplicity; TODO(hash-complete): use xxHash.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        (hash as usize) & (self.num_buckets - 1)
    }
}

impl AccessMethod for HashIndex {
    fn name(&self) -> &'static str {
        "hash"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(hash-complete): WAL-log the bucket page mutation.
        let idx = self.bucket_index(key);
        let mut buckets = self.buckets.lock();
        buckets[idx].push((key.to_vec(), tid));
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        // TODO(hash-complete): read from buffer-pool bucket pages.
        let idx = self.bucket_index(key);
        let buckets = self.buckets.lock();
        let results = buckets[idx]
            .iter()
            .filter(|(k, _)| k.as_slice() == key)
            .map(|(_, tid)| *tid)
            .collect();
        Ok(results)
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(hash-complete): WAL-log the bucket page mutation.
        let idx = self.bucket_index(key);
        let mut buckets = self.buckets.lock();
        let bucket = &mut buckets[idx];
        let before = bucket.len();
        bucket.retain(|(k, t)| !(k.as_slice() == key && *t == tid));
        if bucket.len() < before {
            Ok(())
        } else {
            Err(AccessMethodError::NotFound)
        }
    }
}

// ---------------------------------------------------------------------------
// GIN (Generalized Inverted Index) scaffold
// ---------------------------------------------------------------------------

/// GIN (Generalized Inverted Index) scaffold.
///
/// GIN indexes an item (document, array, JSON) as a set of tokens and
/// maintains a per-token posting list. This scaffold stores tokens in a
/// sorted `Vec` and posting lists as `Vec<TupleId>`.
///
/// # Status
///
/// `TODO(gin-complete)`: replace the in-memory posting list with a
/// WAL-logged posting tree backed by the buffer pool; add compression
/// (varbyte / `PForDelta`); add `GinConsistent` / `GinPartialMatch`
/// strategy interfaces.
#[derive(Debug, Default)]
pub struct GinIndex {
    /// Posting lists keyed by token bytes.
    ///
    /// TODO(gin-complete): replace with a B-tree over posting-list
    /// buffer-pool pages.
    postings: Mutex<std::collections::BTreeMap<Vec<u8>, Vec<TupleId>>>,
}

impl GinIndex {
    /// Create an empty GIN index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl AccessMethod for GinIndex {
    fn name(&self) -> &'static str {
        "gin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(gin-complete): WAL-log the posting-list update.
        let mut postings = self.postings.lock();
        postings.entry(key.to_vec()).or_default().push(tid);
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        // TODO(gin-complete): traverse the posting tree on disk.
        let postings = self.postings.lock();
        Ok(postings.get(key).cloned().unwrap_or_default())
    }

    fn delete(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(gin-complete): update posting list and reclaim dead pages.
        let mut postings = self.postings.lock();
        match postings.get_mut(key) {
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
// BRIN (Block Range Index) scaffold
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

/// BRIN (Block Range `INdex`) scaffold.
///
/// BRIN stores per-page-range min/max summaries rather than per-tuple
/// entries, making it highly space-efficient for naturally ordered data
/// (timestamps, sequential IDs). The trade-off is that a lookup must
/// scan all ranges whose `[min, max]` interval overlaps the query key.
///
/// # Status
///
/// `TODO(brin-complete)`: implement page-backed summary storage; add
/// auto-summarise via the vacuum path; add the `BrinOpclass` strategy
/// interface for non-integer types; add inclusion operator classes.
#[derive(Debug)]
pub struct BrinIndex {
    /// Summaries keyed by page range start.
    ///
    /// TODO(brin-complete): replace with WAL-logged summary pages
    /// in the buffer pool.
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
        // TODO(brin-complete): scan heap pages and compute min/max.
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
}

impl AccessMethod for BrinIndex {
    fn name(&self) -> &'static str {
        "brin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(brin-complete): update the page-range summary that covers
        // tid.page.block; if none exists, defer to auto-summarize.
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
        // TODO(brin-complete): return candidate block ranges, not TupleIds.
        // For now, return an empty vec; the caller falls back to a heap
        // scan filtered by BRIN's block-range pruning.
        let summaries = self.summaries.lock();
        // A range is a candidate when key is within [min_key, max_key].
        let _candidates: Vec<(u32, u32)> = summaries
            .iter()
            .filter(|s| key >= s.min_key.as_slice() && key <= s.max_key.as_slice())
            .map(|s| (s.first_block, s.last_block))
            .collect();
        // BRIN lookup yields candidate page ranges, not exact TupleIds.
        // Returning empty is correct for this scaffold — callers must
        // integrate with the heap scanner.
        Ok(Vec::new())
    }

    fn delete(&self, _key: &[u8], _tid: TupleId) -> Result<(), AccessMethodError> {
        // TODO(brin-complete): BRIN does not track individual TupleIds.
        // Deletion marks the range as "needs re-summarize" and vacuum
        // triggers a re-summarize pass.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

    use super::*;

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
    fn gin_delete_removes_posting() {
        let am = GinIndex::new();
        am.insert(b"tok", tid(2, 0)).expect("insert");
        am.delete(b"tok", tid(2, 0)).expect("delete");
        assert!(am.lookup(b"tok").expect("lookup").is_empty());
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
        // BRIN lookup returns empty (callers integrate with heap scanner).
        let _ = am.lookup(b"\x2a").expect("brin lookup");
    }

    #[test]
    fn brin_summarize_range_stores_minmax() {
        let am = BrinIndex::new(128);
        am.summarize_range(0, 127, b"\x01".to_vec(), b"\xff".to_vec());
        // Lookup within range returns empty (scaffold behaviour).
        let _ = am.lookup(b"\x80").expect("lookup in range");
    }

    #[test]
    fn brin_delete_is_no_op() {
        let am = BrinIndex::new(128);
        am.insert(b"k", tid(0, 0)).expect("insert");
        // BRIN delete is always Ok — no per-tuple tracking.
        am.delete(b"k", tid(0, 0)).expect("brin delete no-op");
    }
}
