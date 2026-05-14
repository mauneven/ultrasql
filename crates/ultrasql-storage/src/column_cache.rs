//! Per-relation columnar projection cache.
//!
//! The row-store heap is the authoritative source of truth (MVCC,
//! WAL, durability all live there). For analytical SELECTs over an
//! unchanged relation that source pays per-tuple decode on every
//! `SeqScan` — proportional to row count and dominant in the
//! 1M-row OLAP bench. A column store (DuckDB, ClickHouse) avoids
//! this by physically arranging data column-by-column on disk.
//!
//! [`ColumnCache`] interposes a lazy in-memory columnar projection
//! between the heap and the executor:
//!
//! - First `SeqScan` over a relation walks the heap normally **and**
//!   accumulates the decoded columns into an [`Arc<CachedColumns>`]
//!   entry keyed on the relation's monotonically-increasing
//!   relation version.
//! - Subsequent `SeqScan`s over the **same** relation, **same**
//!   version, skip the heap walk entirely and stream batches
//!   directly from the cached columns.
//! - Any mutation on the relation (`heap.insert` / `heap.update` /
//!   `heap.delete` and their bulk variants) bumps the version,
//!   which makes existing cache entries unreachable; the next read
//!   re-builds from heap.
//!
//! # Correctness under MVCC
//!
//! The cache stores the **set of live tuples observed by the
//! cache-building scan at its snapshot**. A subsequent scan with a
//! newer snapshot must see at least those rows (the older snapshot's
//! visible tuples remain visible to any later snapshot under MVCC
//! semantics) **provided** no mutation has happened since cache
//! build. The version stamp encodes that "no mutation" predicate:
//! we increment it on any insert/update/delete, so cache hits only
//! return data the heap has not modified since the build, and the
//! subsequent snapshot sees the same row set the build snapshot saw.
//!
//! Concurrent writes during a build are handled by the same
//! mechanism: the writer bumps the version, the build's `put` is
//! rejected (or overwrites a stale entry which will be invalidated
//! again on the next read). Worst case the cache is rebuilt; never
//! wrong.

use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::RwLock;
use ultrasql_core::{RelationId, Schema};
use ultrasql_vec::column::Column;

/// Cached column projection for a relation at a specific version.
#[derive(Debug)]
pub struct CachedColumns {
    /// Version of the relation when this entry was built. Bumping
    /// the relation's version through [`ColumnCache::bump_version`]
    /// makes subsequent [`ColumnCache::get`] calls miss.
    pub version: u64,
    /// Logical schema of the cached columns (relation schema, no
    /// TID prefix).
    pub schema: Schema,
    /// Per-column values for **every live tuple** observed by the
    /// cache-building scan. `columns[i]` has the same length for
    /// every `i` and equals the row count of the cached projection.
    pub columns: Vec<Column>,
}

/// Per-`HeapAccess` columnar cache.
///
/// Holds two pieces of state:
///
/// - A monotone per-relation version counter (`AtomicU64` behind a
///   sharded map). Read on cache lookup; bumped on every mutation
///   the heap performs against that relation.
/// - A version-stamped entry per relation. Cache lookups validate
///   the entry's `version` against the relation's current version
///   and miss when they differ.
#[derive(Debug, Default)]
pub struct ColumnCache {
    inner: RwLock<ColumnCacheInner>,
}

#[derive(Debug, Default)]
struct ColumnCacheInner {
    versions: AHashMap<RelationId, u64>,
    entries: AHashMap<RelationId, Arc<CachedColumns>>,
}

impl ColumnCache {
    /// Construct an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the current monotonic version for `rel`. Starts at
    /// zero for a relation that has never been mutated.
    #[must_use]
    pub fn relation_version(&self, rel: RelationId) -> u64 {
        let g = self.inner.read();
        *g.versions.get(&rel).unwrap_or(&0)
    }

    /// Bump the version of `rel` and drop any cached entry for it.
    ///
    /// Called by every heap mutation path
    /// (`insert` / `update` / `delete` / bulk variants).
    pub fn bump_version(&self, rel: RelationId) {
        let mut g = self.inner.write();
        let v = g.versions.entry(rel).or_insert(0);
        *v = v.saturating_add(1);
        g.entries.remove(&rel);
    }

    /// Look up the cached projection for `rel` at the current
    /// version. Returns `None` on cache miss (no entry or stale
    /// version).
    #[must_use]
    pub fn get(&self, rel: RelationId) -> Option<Arc<CachedColumns>> {
        let g = self.inner.read();
        let current = *g.versions.get(&rel).unwrap_or(&0);
        g.entries
            .get(&rel)
            .filter(|e| e.version == current)
            .cloned()
    }

    /// Store a cache entry. The entry's `version` is compared
    /// against the relation's current version on insert: a stale
    /// build is dropped on the floor so a writer-during-build race
    /// cannot resurrect outdated columns.
    pub fn put(&self, rel: RelationId, entry: CachedColumns) {
        let mut g = self.inner.write();
        let current = *g.versions.get(&rel).unwrap_or(&0);
        if entry.version != current {
            // A writer bumped the version while we were building;
            // drop this entry and let the next reader rebuild from
            // the new heap state.
            return;
        }
        g.entries.insert(rel, Arc::new(entry));
    }
}
