//! Inherent and `Debug` implementations for the DML index maintainers
//! ([`InsertIndexMaintainer`], [`VectorIndexMaintainer`]) and their
//! private runtime enum.

use std::sync::Arc;

use ultrasql_core::{TupleId, Value, Xid};
use ultrasql_mvcc::{Snapshot, Visibility, XidStatusOracle, is_visible};
use ultrasql_storage::PageLoader;
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::{BTree, BTreeError};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::wal_sink::WalSink;

use super::{
    InsertIndexEncoder, InsertIndexMaintainer, VectorIndexEncoder, VectorIndexMaintainer,
    VectorIndexRuntime,
};
use crate::ExecError;

/// Outcome of a unique-index conflict recheck against the heap.
///
/// Under the Option-A no-index-undo model a UNIQUE leaf may hold a stale
/// entry whose heap tuple is dead. Classifying the hit lets the caller
/// either reject (live conflict), proceed (no conflict), or proceed *after*
/// physically replacing the specific dead entry (`DeadConflict`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UniqueConflict {
    /// No indexed entry for the key, or the only entry points at a tuple
    /// that is dead to an up-to-date snapshot (rolled-back / aborted /
    /// deleted-and-committed). The insert may proceed.
    None,
    /// The indexed entry points at a **live** tuple (visible, or an
    /// in-progress foreign writer's pending insert). A real conflict — the
    /// caller must reject with `UniqueViolation`.
    Live,
    /// The indexed entry points at a **dead** tuple at this exact TID. The
    /// insert may proceed, but the stale physical leaf entry must be
    /// removed (targeted-dead-replace) before inserting the new one, or the
    /// storage layer's own `DuplicateKey` will reject the reuse.
    Dead(TupleId),
}

impl<L: PageLoader> std::fmt::Debug for InsertIndexMaintainer<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertIndexMaintainer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> InsertIndexMaintainer<L> {
    /// Construct a maintainer for one already-created B-tree index.
    #[must_use]
    pub fn new<N: Into<String>>(
        name: N,
        tree: BTree<L>,
        encode: InsertIndexEncoder,
        unique: bool,
    ) -> Self {
        Self {
            name: name.into(),
            tree,
            encode,
            unique,
            key_columns: Vec::new(),
            brin: None,
        }
    }

    /// Attach target-table column indices covered by this index key.
    #[must_use]
    pub fn with_key_columns(mut self, columns: Vec<usize>) -> Self {
        self.key_columns = columns;
        self
    }

    /// Attach the in-memory BRIN summary maintained beside this index.
    #[must_use]
    pub fn with_brin(mut self, brin: Option<Arc<BrinIndex>>) -> Self {
        self.brin = brin;
        self
    }

    pub(crate) fn encode_key(&self, row: &[Value]) -> Result<Option<i64>, ExecError> {
        (self.encode)(row)
    }

    pub(crate) fn contains_key(&self, key: i64) -> Result<bool, ExecError> {
        self.lookup_tid(key).map(|tid| tid.is_some())
    }

    /// Heap-rechecking unique-conflict test (Option-A, design §1 A3).
    ///
    /// Looks up `key` in the index; if a TID is found, fetches its heap
    /// tuple and classifies the hit:
    ///
    /// - **`Live`** — the tuple is visible to `snapshot`, is our own
    ///   pending write, or was inserted by a *still-in-progress* foreign
    ///   writer (the dirty-snapshot conflict PostgreSQL enforces so two
    ///   concurrent inserters of the same key cannot both win). Reject.
    /// - **`Dead(tid)`** — the tuple is dead to an up-to-date view
    ///   (rolled-back / aborted inserter, or a committed delete). The key
    ///   may be reused, but the stale leaf entry at `tid` must be replaced.
    /// - **`None`** — no indexed entry at all.
    ///
    /// `snapshot` should be reasonably current: a stale snapshot would let
    /// a key deleted-after-the-snapshot still look live, which is *safe*
    /// (over-strict, never under-strict) for the uniqueness guarantee.
    pub(crate) fn classify_unique_conflict<L2, O>(
        &self,
        key: i64,
        heap: &HeapAccess<L2>,
        snapshot: &Snapshot,
        oracle: &O,
    ) -> Result<UniqueConflict, ExecError>
    where
        L2: PageLoader,
        O: XidStatusOracle + ?Sized,
    {
        let Some(tid) = self.lookup_tid(key)? else {
            return Ok(UniqueConflict::None);
        };
        let tuple = heap
            .fetch(tid)
            .map_err(|e| ExecError::TypeMismatch(format!("unique recheck heap fetch: {e}")))?;
        let header = &tuple.header;
        match is_visible(header, snapshot, oracle) {
            // Visible / our own write / pre-image of an in-place update —
            // the row logically exists for this key: a real conflict.
            Visibility::Visible | Visibility::VisiblePreImage | Visibility::DeletedByOwn => {
                Ok(UniqueConflict::Live)
            }
            Visibility::Invisible => {
                // Invisible has two causes that must be told apart:
                //   1. The inserter is still in-progress (a foreign writer's
                //      uncommitted insert, or our own future command). That
                //      is a live conflict — PostgreSQL blocks the second
                //      inserter via a dirty-snapshot probe.
                //   2. The inserter aborted / rolled back, or the row was
                //      deleted-and-committed. The key is free to reuse; the
                //      stale leaf entry must be replaced.
                if Self::tuple_is_pending_live(header, oracle) {
                    Ok(UniqueConflict::Live)
                } else {
                    Ok(UniqueConflict::Dead(tid))
                }
            }
        }
    }

    /// `true` iff an `Invisible` tuple is invisible only because its writer
    /// is still **in progress** (so the key is held by a pending live
    /// version), not because it is aborted/deleted-committed (dead).
    fn tuple_is_pending_live<O: XidStatusOracle + ?Sized>(
        header: &ultrasql_mvcc::TupleHeader,
        oracle: &O,
    ) -> bool {
        use ultrasql_mvcc::status::XidStatus;
        if header.xmin.is_invalid() {
            return false;
        }
        // If the inserter has not yet resolved, the key is held by a
        // pending insert: treat as a live conflict.
        if matches!(oracle.status(header.xmin), XidStatus::InProgress) {
            return true;
        }
        // Inserter committed. If there is no deleter the row is live (the
        // snapshot just doesn't see it yet — e.g. committed-after-snapshot);
        // that still holds the key. A committed deleter makes it dead; an
        // aborted inserter was handled above (status != InProgress only when
        // committed/aborted — an aborted inserter yields a dead row).
        if matches!(oracle.status(header.xmin), XidStatus::Aborted) {
            return false;
        }
        if header.xmax.is_invalid() {
            return true;
        }
        // Has a deleter: live only if the delete is itself unresolved
        // (in-progress) — a committed delete makes the row dead.
        matches!(oracle.status(header.xmax), XidStatus::InProgress)
    }

    /// Insert a key, tolerating a stale duplicate physical entry whose heap
    /// tuple is dead (targeted-dead-replace, Option-A A3).
    ///
    /// For a UNIQUE index the physical tree still holds a dead key's entry,
    /// so a plain [`Self::insert_key`] would hit the storage layer's
    /// `DuplicateKey`. When `dead_tid` is `Some`, the specific stale entry
    /// is physically removed first (a forward, WAL-logged delete — *not* a
    /// rollback-undo), then the new entry is inserted. For a non-unique
    /// index there is nothing to replace and this is a plain insert.
    pub(crate) fn insert_key_replacing_dead(
        &mut self,
        key: i64,
        tid: TupleId,
        dead_tid: Option<TupleId>,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), ExecError> {
        if self.unique {
            if let Some(dead) = dead_tid {
                // Forward, logged removal of the specific dead entry. Safe:
                // this is a normal index mutation, fully recoverable, not an
                // undo of a rollback.
                let _ = self.delete_key(key, dead, xid, wal)?;
            }
        }
        self.insert_key(key, tid, xid, wal)
    }

    pub(crate) fn lookup_tid(&self, key: i64) -> Result<Option<TupleId>, ExecError> {
        self.tree
            .lookup::<i64>(key)
            .map_err(|e| ExecError::TypeMismatch(format!("index lookup {}: {e}", self.name)))
    }

    pub(crate) const fn is_unique(&self) -> bool {
        self.unique
    }

    pub(crate) fn insert_key(
        &mut self,
        key: i64,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), ExecError> {
        let result = if self.unique {
            self.tree.insert(key, tid, xid, wal)
        } else {
            self.tree.insert_non_unique(key, tid, xid, wal)
        };
        result.map_err(|e| match e {
            BTreeError::DuplicateKey => ExecError::UniqueViolation(self.name.clone()),
            other => ExecError::TypeMismatch(format!("index insert {}: {other}", self.name)),
        })?;
        if let Some(brin) = &self.brin {
            let brin_key = BrinIndex::encode_i64_key(key);
            brin.insert(&brin_key, tid).map_err(|e| {
                ExecError::TypeMismatch(format!("brin summary insert {}: {e}", self.name))
            })?;
        }
        Ok(())
    }

    pub(crate) fn delete_key(
        &mut self,
        key: i64,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<bool, ExecError> {
        self.tree
            .delete_logged::<i64>(key, tid, xid, wal)
            .map_err(|e| ExecError::TypeMismatch(format!("index delete {}: {e}", self.name)))
    }
}

impl std::fmt::Debug for VectorIndexRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hnsw(_) => f.write_str("Hnsw"),
            Self::IvfFlat(_) => f.write_str("IvfFlat"),
        }
    }
}

impl std::fmt::Debug for VectorIndexMaintainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorIndexMaintainer")
            .field("name", &self.name)
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

impl VectorIndexMaintainer {
    /// Construct a maintainer for one runtime HNSW graph.
    #[must_use]
    pub fn new_hnsw<N: Into<String>>(
        name: N,
        hnsw: Arc<ultrasql_storage::access_method::PageBackedHnswIndex>,
        encode: VectorIndexEncoder,
        xid: Xid,
        wal: Option<Arc<dyn WalSink>>,
    ) -> Self {
        Self {
            name: name.into(),
            runtime: VectorIndexRuntime::Hnsw(hnsw),
            encode,
            xid,
            wal,
        }
    }

    /// Construct a maintainer for one runtime IVFFlat index.
    #[must_use]
    pub fn new_ivfflat<N: Into<String>>(
        name: N,
        ivfflat: Arc<ultrasql_storage::access_method::PageBackedIvfFlatIndex>,
        encode: VectorIndexEncoder,
        xid: Xid,
        wal: Option<Arc<dyn WalSink>>,
    ) -> Self {
        Self {
            name: name.into(),
            runtime: VectorIndexRuntime::IvfFlat(ivfflat),
            encode,
            xid,
            wal,
        }
    }

    pub(crate) fn encode_key(&self, row: &[Value]) -> Result<Option<Vec<f32>>, ExecError> {
        (self.encode)(row)
    }

    pub(crate) fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), ExecError> {
        match &self.runtime {
            VectorIndexRuntime::Hnsw(hnsw) => hnsw
                .insert_vector_logged(vector, tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("hnsw insert {}: {e}", self.name))),
            VectorIndexRuntime::IvfFlat(ivfflat) => ivfflat
                .insert_vector_logged(vector, tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("ivfflat insert {}: {e}", self.name))),
        }
    }

    pub(crate) fn delete_tid(&self, tid: TupleId) -> Result<(), ExecError> {
        match &self.runtime {
            VectorIndexRuntime::Hnsw(hnsw) => hnsw
                .mark_deleted_logged(tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("hnsw delete {}: {e}", self.name))),
            VectorIndexRuntime::IvfFlat(ivfflat) => ivfflat
                .mark_deleted_logged(tid, self.xid, self.wal.as_deref())
                .map_err(|e| ExecError::TypeMismatch(format!("ivfflat delete {}: {e}", self.name))),
        }
    }
}
