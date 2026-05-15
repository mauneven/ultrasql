//! Index-shape adapters that ride on top of an `AccessMethod`:
//! composite keys, expression indexes, partial indexes, covering
//! (`INCLUDE`) indexes, and the two-pass `CREATE INDEX CONCURRENTLY`
//! state machine.
//!
//! Each adapter is independent of the underlying access method; they
//! delegate the actual page I/O to whatever `AccessMethod`
//! implementation the caller supplies.

use ultrasql_core::TupleId;
use ultrasql_core::endian::{read_i64_le, write_i64_le};

// ---------------------------------------------------------------------------
// Multi-column key support
// ---------------------------------------------------------------------------

/// A composite key made of multiple fixed-width components.
///
/// Each component is an `i64` value. The composite key encodes all
/// components concatenated in little-endian order, making the encoding
/// length `N * 8` bytes.
///
/// v0.8 restricts component count to 1–8 (yielding 8–64 bytes). The
/// existing `BTree` `Key` trait requires `SIZE == 8`; composite keys
/// bypass that restriction by using the byte-slice `AccessMethod`
/// interface instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKey<const N: usize> {
    /// Component values in declaration order.
    pub values: [i64; N],
}

impl<const N: usize> CompositeKey<N> {
    /// Construct a composite key from an array of `i64` values.
    #[must_use]
    pub const fn new(values: [i64; N]) -> Self {
        Self { values }
    }

    /// Encode the composite key into a byte buffer.
    ///
    /// The buffer must be exactly `N * 8` bytes long.
    pub fn encode_into(&self, out: &mut [u8]) {
        assert_eq!(out.len(), N * 8, "buffer length must equal N*8");
        for (i, &v) in self.values.iter().enumerate() {
            write_i64_le(&mut out[i * 8..i * 8 + 8], v);
        }
    }

    /// Decode a composite key from a byte buffer.
    pub fn decode_from(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), N * 8, "buffer length must equal N*8");
        let mut values = [0_i64; N];
        for (i, v) in values.iter_mut().enumerate() {
            *v = read_i64_le(&bytes[i * 8..i * 8 + 8]).unwrap_or(0);
        }
        Self { values }
    }
}

// ---------------------------------------------------------------------------
// Expression index helper
// ---------------------------------------------------------------------------

/// An expression index stores keys computed by a caller-supplied
/// function rather than direct column values.
///
/// The `ExprIndexAdapter` wraps a `BTree` (via the `AccessMethod`
/// interface) and a key-extraction function. The caller inserts rows;
/// the adapter extracts the key, encodes it, and forwards to the
/// underlying index.
///
/// # Usage
///
/// ```ignore
/// let idx = ExprIndexAdapter::new(
///     BTreeAccessMethod::new(true),
///     |row| {
///         // Expression: lower(email)
///         if let Some(Value::Text(s)) = row.get(2) {
///             s.to_lowercase().into_bytes()
///         } else {
///             vec![]
///         }
///     },
/// );
/// idx.insert_row(&row, tid).unwrap();
/// ```
pub struct ExprIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
}

impl std::fmt::Debug for ExprIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExprIndexAdapter").finish_non_exhaustive()
    }
}

impl ExprIndexAdapter {
    /// Construct an expression index adapter.
    ///
    /// - `inner` — underlying access method (typically `BTreeAccessMethod`).
    /// - `key_fn` — maps a row to the index key bytes.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
        }
    }

    /// Insert a row into the expression index.
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        let key = (self.key_fn)(row);
        self.inner.insert(&key, tid)
    }

    /// Look up a pre-encoded expression key.
    pub fn lookup_key(
        &self,
        key: &[u8],
    ) -> Result<Vec<TupleId>, crate::access_method::AccessMethodError> {
        self.inner.lookup(key)
    }
}

// ---------------------------------------------------------------------------
// Partial index predicate wrapper
// ---------------------------------------------------------------------------

/// A partial index only indexes rows satisfying a predicate.
///
/// The `PartialIndexAdapter` wraps any `AccessMethod` and filters
/// inserts through a WHERE-clause predicate. Rows that do not satisfy
/// the predicate are silently skipped.
pub struct PartialIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    predicate: Box<dyn Fn(&[ultrasql_core::Value]) -> bool + Send + Sync>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
}

impl std::fmt::Debug for PartialIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialIndexAdapter")
            .finish_non_exhaustive()
    }
}

impl PartialIndexAdapter {
    /// Construct a partial index adapter.
    ///
    /// - `inner` — underlying access method.
    /// - `key_fn` — extracts the key bytes from a row.
    /// - `predicate` — returns `true` when a row should be indexed.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
        predicate: impl Fn(&[ultrasql_core::Value]) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
            predicate: Box::new(predicate),
        }
    }

    /// Insert a row if the predicate passes.
    ///
    /// Returns `Ok(())` silently when the predicate is false (the row
    /// is not indexed).
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        if !(self.predicate)(row) {
            return Ok(()); // Row does not satisfy the partial predicate.
        }
        let key = (self.key_fn)(row);
        self.inner.insert(&key, tid)
    }

    /// Look up a pre-encoded key.
    pub fn lookup_key(
        &self,
        key: &[u8],
    ) -> Result<Vec<TupleId>, crate::access_method::AccessMethodError> {
        self.inner.lookup(key)
    }
}

// ---------------------------------------------------------------------------
// Covering index (INCLUDE columns) wrapper
// ---------------------------------------------------------------------------

/// Leaf payload for a covering index entry.
///
/// In a covering index the leaf stores the primary key columns plus
/// additional INCLUDE columns. This struct holds the INCLUDE payload as
/// raw bytes alongside the `TupleId`; the executor can satisfy a query
/// without visiting the heap.
///
/// TODO(btree-covering-persistent): store the INCLUDE payload on the
/// leaf page in the buffer pool rather than in memory.
#[derive(Clone, Debug)]
pub struct CoveringEntry {
    /// Tuple identifier (used as a fallback when INCLUDE columns
    /// do not satisfy the query).
    pub tid: TupleId,
    /// Additional INCLUDE column bytes, serialized by the caller.
    pub include_payload: Vec<u8>,
}

/// A covering index that stores INCLUDE column payloads alongside
/// the indexed key.
///
/// Keys are managed by the inner `AccessMethod`; INCLUDE payloads are
/// stored in a side-table indexed by `TupleId`.
pub struct CoveringIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
    include_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
    /// INCLUDE payloads keyed by `TupleId`.
    payloads: parking_lot::Mutex<std::collections::HashMap<u64, Vec<u8>>>,
}

impl std::fmt::Debug for CoveringIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoveringIndexAdapter")
            .finish_non_exhaustive()
    }
}

/// Encode a `TupleId` to a `u64` for use as a hash map key.
fn tid_to_u64(tid: TupleId) -> u64 {
    let rel_oid = u64::from(tid.page.relation.0.raw());
    let block = u64::from(tid.page.block.raw());
    let slot = u64::from(tid.slot);
    (rel_oid << 48) | (block << 16) | slot
}

impl CoveringIndexAdapter {
    /// Construct a covering index adapter.
    ///
    /// - `key_fn` — produces the key bytes from a row.
    /// - `include_fn` — produces the INCLUDE column payload bytes.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
        include_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
            include_fn: Box::new(include_fn),
            payloads: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Insert a row, storing the INCLUDE payload alongside the key.
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        let key = (self.key_fn)(row);
        let payload = (self.include_fn)(row);
        self.inner.insert(&key, tid)?;
        self.payloads.lock().insert(tid_to_u64(tid), payload);
        Ok(())
    }

    /// Look up key + INCLUDE payloads for all matching entries.
    pub fn lookup_covering(
        &self,
        key: &[u8],
    ) -> Result<Vec<CoveringEntry>, crate::access_method::AccessMethodError> {
        let tids = self.inner.lookup(key)?;
        let payloads = self.payloads.lock();
        Ok(tids
            .into_iter()
            .map(|tid| {
                let include_payload = payloads.get(&tid_to_u64(tid)).cloned().unwrap_or_default();
                CoveringEntry {
                    tid,
                    include_payload,
                }
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX CONCURRENTLY simulation (2-pass build)
// ---------------------------------------------------------------------------

/// Status of a concurrent index build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrentBuildStatus {
    /// First pass complete; the index covers rows inserted before `snapshot_xid`.
    Pass1Complete {
        /// XID at which the first pass's snapshot was taken.
        snapshot_xid: u64,
    },
    /// Both passes complete; the index is ready for use.
    Ready,
}

/// Simulated CREATE INDEX CONCURRENTLY state machine.
///
/// A concurrent build proceeds in two phases without taking an
/// `AccessExclusive` lock on the table:
///
/// 1. **Pass 1** — build the initial index from a snapshot of the table
///    taken at the caller-supplied `snapshot_xid`. Rows inserted after
///    that XID are not yet indexed.
/// 2. **Pass 2** — index rows that were inserted between the pass-1
///    snapshot and the current time. After pass 2 the index is valid.
///
/// This implementation delegates to the caller-supplied row iterators
/// rather than reading from the buffer pool, keeping the storage crate
/// decoupled from the executor. The actual page I/O (and WAL logging)
/// occurs inside the `AccessMethod::insert` calls.
///
/// `TODO(cic-complete)`: integrate with the MVCC visibility layer and
/// the lock manager to replay missed rows correctly.
#[derive(Debug)]
pub struct ConcurrentIndexBuilder {
    am: Box<dyn crate::access_method::AccessMethod>,
    status: parking_lot::Mutex<Option<ConcurrentBuildStatus>>,
}

impl ConcurrentIndexBuilder {
    /// Create a builder wrapping an already-allocated (empty) index.
    pub fn new(am: impl crate::access_method::AccessMethod + 'static) -> Self {
        Self {
            am: Box::new(am),
            status: parking_lot::Mutex::new(None),
        }
    }

    /// Execute pass 1: index every `(key, tid)` pair supplied by the
    /// iterator.
    ///
    /// `snapshot_xid` is the XID at which the pass-1 heap scan was
    /// taken; rows inserted later are deferred to pass 2.
    pub fn build_pass1(
        &self,
        rows: impl Iterator<Item = (Vec<u8>, TupleId)>,
        snapshot_xid: u64,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        for (key, tid) in rows {
            self.am.insert(&key, tid)?;
        }
        *self.status.lock() = Some(ConcurrentBuildStatus::Pass1Complete { snapshot_xid });
        Ok(())
    }

    /// Execute pass 2: index rows that arrived after the pass-1 snapshot.
    ///
    /// The caller supplies only the delta rows (those with XID >
    /// `snapshot_xid`). After this call the builder reports `Ready`.
    pub fn build_pass2(
        &self,
        delta_rows: impl Iterator<Item = (Vec<u8>, TupleId)>,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        for (key, tid) in delta_rows {
            // Ignore duplicate-key errors: a row may have been indexed
            // during pass 1 if the snapshot window overlapped.
            match self.am.insert(&key, tid) {
                Ok(()) | Err(crate::access_method::AccessMethodError::DuplicateKey) => {}
                Err(e) => return Err(e),
            }
        }
        *self.status.lock() = Some(ConcurrentBuildStatus::Ready);
        Ok(())
    }

    /// Return the current build status.
    pub fn status(&self) -> Option<ConcurrentBuildStatus> {
        self.status.lock().clone()
    }

    /// Consume the builder and return the finished access method.
    ///
    /// Panics if the build is not in the `Ready` state.
    pub fn finish(self) -> Box<dyn crate::access_method::AccessMethod> {
        assert_eq!(
            *self.status.lock(),
            Some(ConcurrentBuildStatus::Ready),
            "build_pass2 must complete before finish()"
        );
        self.am
    }
}
