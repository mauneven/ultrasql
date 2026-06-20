//! Inherent and `Debug` implementations for the DML index maintainers
//! ([`InsertIndexMaintainer`], [`VectorIndexMaintainer`]) and their
//! private runtime enum.

use std::sync::Arc;

use ultrasql_core::{TupleId, Value, Xid};
use ultrasql_storage::PageLoader;
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::{BTree, BTreeError};
use ultrasql_storage::wal_sink::WalSink;

use super::{
    InsertIndexEncoder, InsertIndexMaintainer, VectorIndexEncoder, VectorIndexMaintainer,
    VectorIndexRuntime,
};
use crate::ExecError;

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
