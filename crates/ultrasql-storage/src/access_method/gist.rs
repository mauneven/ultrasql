//! GiST (Generalized Search Tree) scaffold.
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::TupleId;

use super::{AccessMethod, AccessMethodError};

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
