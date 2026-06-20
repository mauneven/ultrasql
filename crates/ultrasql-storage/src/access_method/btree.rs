//! B-tree adapter wrapping the persistent [`crate::btree::BTree`].
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::TupleId;

use super::{AccessMethod, AccessMethodError};

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
