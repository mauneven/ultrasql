//! Sequential heap scan operator backed by the storage subsystem.
//!
//! Drives [`HeapAccess::scan_visible`] and decodes each tuple's payload
//! through a [`RowCodec`] into batched columns. Batches are capped at
//! 4096 rows per `ARCHITECTURE.md` §9.
//!
//! The scan owns its [`RowCodec`] (schema) and pulls visible tuples
//! lazily — pages are pinned via the buffer pool one at a time by the
//! underlying `VisibleHeapScan` iterator.
//!
//! # v0.5 limitation
//!
//! The first `next_batch` call materialises **all** visible rows into a
//! `Vec` and subsequent calls drain it in 4096-row chunks. This is
//! O(relation size) in memory and acceptable for v0.5 where relations
//! are small. A `TODO(seq-scan-streaming)` below marks the follow-up
//! work to stream rows page-by-page once the iterator's lifetime contract
//! is resolved.

use std::sync::Arc;
use std::vec::IntoIter;

use ultrasql_core::{DataType, RelationId, Schema, Value};
use ultrasql_mvcc::{Snapshot, XidStatusOracle};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::row_codec::RowCodec;
use crate::{ExecError, Operator};

/// Maximum rows per batch, matching the `ARCHITECTURE.md` §9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Sequential heap scan operator.
///
/// Reads every MVCC-visible tuple from `rel` and decodes each payload via
/// the bound [`RowCodec`], then emits 4096-row [`Batch`]es.
///
/// `L` is the [`PageLoader`] implementation (in production: the segment
/// loader; in tests: an in-memory map). `O` is the [`XidStatusOracle`]
/// implementation (in production: the CLOG-backed oracle; in tests:
/// [`MapOracle`](ultrasql_mvcc::status::test_support::MapOracle)).
///
/// # Send bound
///
/// The operator is `Send` because all owned types — `Arc<HeapAccess<L>>`,
/// `Snapshot`, `Arc<O>`, and `RowCodec` — are `Send + Sync`.
pub struct SeqScan<L: PageLoader, O> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    block_count: u32,
    snapshot: Snapshot,
    oracle: Arc<O>,
    codec: RowCodec,
    /// Materialised row buffer, filled on the first `next_batch` call.
    ///
    /// `None` = not yet materialised. `Some(iter)` = currently draining.
    ///
    /// TODO(seq-scan-streaming): Replace with a cursor-based streaming
    /// scan once the `VisibleHeapScan` iterator's lifetime can be tied
    /// to owned `Snapshot` + `Arc<O>` state inside the operator.
    all_rows: Option<IntoIter<Vec<Value>>>,
    /// `true` after the scan has emitted `Ok(None)`.
    eof: bool,
}

impl<L: PageLoader, O> std::fmt::Debug for SeqScan<L, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScan")
            .field("relation", &self.relation)
            .field("block_count", &self.block_count)
            .field("eof", &self.eof)
            .field("schema", self.codec.schema())
            .finish_non_exhaustive()
    }
}

impl<L, O> SeqScan<L, O>
where
    L: PageLoader + Send + Sync + 'static,
    O: XidStatusOracle + Send + Sync + 'static,
{
    /// Construct a `SeqScan`.
    ///
    /// - `heap` — shared reference to the heap access method.
    /// - `relation` — relation id to scan.
    /// - `block_count` — number of allocated blocks in `relation` (from
    ///   the catalog or `HeapAccess::block_count`).
    /// - `snapshot` — MVCC snapshot for visibility filtering.
    /// - `oracle` — transaction-status oracle.
    /// - `codec` — row codec whose schema matches the relation's column layout.
    #[must_use]
    pub const fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        block_count: u32,
        snapshot: Snapshot,
        oracle: Arc<O>,
        codec: RowCodec,
    ) -> Self {
        Self {
            heap,
            relation,
            block_count,
            snapshot,
            oracle,
            codec,
            all_rows: None,
            eof: false,
        }
    }
}

impl<L, O> Operator for SeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Materialise all visible rows on first call.
        //
        // v0.5 limitation: this loads the entire relation into memory before
        // yielding the first batch. For small test relations this is acceptable;
        // the streaming follow-up is tracked as TODO(seq-scan-streaming).
        if self.all_rows.is_none() {
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for result in self.heap.scan_visible(
                self.relation,
                self.block_count,
                &self.snapshot,
                &*self.oracle,
            ) {
                let tup = result.map_err(|e| {
                    tracing::warn!(error = %e, "heap scan error");
                    ExecError::Internal("heap scan failed")
                })?;
                let row = self
                    .codec
                    .decode(&tup.data)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
                rows.push(row);
            }
            self.all_rows = Some(rows.into_iter());
        }

        let rows_iter = self.all_rows.as_mut().expect("just-set above");
        let chunk: Vec<Vec<Value>> = rows_iter.by_ref().take(BATCH_TARGET_ROWS).collect();

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }

        let batch = build_batch(&chunk, self.codec.schema())?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        self.codec.schema()
    }
}

/// Convert a slice of decoded rows into a [`Batch`] matching `schema`.
///
/// Each column in `schema` maps to a [`Column`] variant. Only the
/// types that have a direct [`Column`] counterpart are supported here;
/// other types surface as [`ExecError::TypeMismatch`].
#[allow(clippy::too_many_lines)]
pub fn build_batch(rows: &[Vec<Value>], schema: &Schema) -> Result<Batch, ExecError> {
    if rows.is_empty() {
        return Batch::new(std::iter::empty::<Column>()).map_err(ExecError::from);
    }

    let n_cols = schema.len();
    let n_rows = rows.len();

    let mut columns: Vec<Column> = Vec::with_capacity(n_cols);

    for col_idx in 0..n_cols {
        let field = schema.field_at(col_idx);
        let col = match &field.data_type {
            DataType::Bool => {
                let mut data: Vec<bool> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Bool(v) => data.push(*v),
                        Value::Null => data.push(false), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Bool at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Bool(BoolColumn::from_data(data))
            }
            DataType::Int32 => {
                let mut data: Vec<i32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int32(v) => data.push(*v),
                        Value::Null => data.push(0), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Int32(NumericColumn::from_data(data))
            }
            DataType::Int64 => {
                let mut data: Vec<i64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Int64(v) => data.push(*v),
                        Value::Null => data.push(0), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Int64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Int64(NumericColumn::from_data(data))
            }
            DataType::Float32 => {
                let mut data: Vec<f32> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float32(v) => data.push(*v),
                        Value::Null => data.push(0.0), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float32 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Float32(NumericColumn::from_data(data))
            }
            DataType::Float64 => {
                let mut data: Vec<f64> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Float64(v) => data.push(*v),
                        Value::Null => data.push(0.0), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Float64 at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Float64(NumericColumn::from_data(data))
            }
            DataType::Text { .. } => {
                let mut strings: Vec<String> = Vec::with_capacity(n_rows);
                for (row_idx, row) in rows.iter().enumerate() {
                    match &row[col_idx] {
                        Value::Text(s) => strings.push(s.clone()),
                        Value::Null => strings.push(String::new()), // placeholder for null
                        other => {
                            return Err(ExecError::TypeMismatch(format!(
                                "expected Text at row {row_idx} col {col_idx}, got {:?}",
                                other.data_type()
                            )));
                        }
                    }
                }
                Column::Utf8(StringColumn::from_data(strings))
            }
            other => {
                return Err(ExecError::TypeMismatch(format!(
                    "SeqScan: unsupported column type {other} for batch building"
                )));
            }
        };
        columns.push(col);
    }

    Batch::new(columns).map_err(ExecError::from)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        CommandId, DataType, Field, PageId, RelationId, Result, Schema, Value, Xid,
    };
    use ultrasql_mvcc::Snapshot;
    use ultrasql_mvcc::status::test_support::MapOracle;
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::{HeapAccess, InsertOptions};
    use ultrasql_storage::page::Page;
    use ultrasql_vec::column::Column;

    use super::SeqScan;
    use crate::row_codec::RowCodec;
    use crate::{ExecError, Operator};

    // -----------------------------------------------------------------------
    // Test fixtures — duplicated from ultrasql_storage tests (those are
    // cfg(test)-gated and not reachable from this crate).
    // -----------------------------------------------------------------------

    /// In-memory page loader that materialises blank heap pages on first miss
    /// and persists them across evictions.
    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl MapLoader {
        fn new() -> Self {
            Self::default()
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|b| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**b);
                    copy
                })
            };
            if let Some(bytes) = stored {
                return Page::from_bytes(bytes)
                    .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
            }
            let page = Page::new_heap();
            let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                .into_boxed_slice()
                .try_into()
                .expect("alloc matches PAGE_SIZE");
            copy.copy_from_slice(page.as_bytes());
            self.store.lock().insert(page_id, copy);
            Ok(page)
        }
    }

    fn rel() -> RelationId {
        RelationId::new(1)
    }

    fn make_heap() -> Arc<HeapAccess<MapLoader>> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        Arc::new(HeapAccess::new(pool))
    }

    /// Build a snapshot for an *observer* transaction at `xid + 1` that
    /// sees rows written by the inserter transaction `xid` as committed.
    ///
    /// The inserter `xid` is strictly below `xmin` (`xid + 1`), so the
    /// snapshot's `xid_in_progress` predicate returns `false` for it.
    /// The caller must register `xid` as committed in the oracle so
    /// `is_committed_before_snapshot` returns `true`, making the rows
    /// visible.
    fn snap_for(xid: u64) -> Snapshot {
        Snapshot::new(
            Xid::new(xid + 1),
            Xid::new(xid + 2),
            Xid::new(xid + 1),
            CommandId::FIRST,
            [],
        )
    }

    fn insert_opts(xid: u64) -> InsertOptions<'static> {
        InsertOptions {
            xmin: Xid::new(xid),
            command_id: CommandId::FIRST,
            wal: None,
        }
    }

    fn schema_i32_text() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok")
    }

    // -----------------------------------------------------------------------
    // Helper: drain all batches into a flat vec of rows
    // -----------------------------------------------------------------------

    fn drain_rows(scan: &mut dyn Operator) -> Vec<(i32, String)> {
        let mut out = Vec::new();
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            let cols = batch.columns();
            assert_eq!(cols.len(), 2);
            let ids = match &cols[0] {
                Column::Int32(c) => c.data().to_vec(),
                other => panic!("expected Int32, got {other:?}"),
            };
            let names: Vec<String> = match &cols[1] {
                Column::Utf8(c) => (0..c.len()).map(|i| c.value(i).to_owned()).collect(),
                other => panic!("expected Utf8, got {other:?}"),
            };
            assert_eq!(ids.len(), names.len());
            for (id, name) in ids.into_iter().zip(names) {
                out.push((id, name));
            }
        }
        out
    }

    // -----------------------------------------------------------------------
    // Test 1: scan returns inserted rows in insert order
    // -----------------------------------------------------------------------

    #[test]
    fn scan_returns_inserted_rows_in_insert_order() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 10;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        // Insert 10 rows.
        let expected: Vec<(i32, String)> = (0_i32..10).map(|i| (i, format!("row_{i}"))).collect();
        for (id, name) in &expected {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let rows = drain_rows(&mut scan);
        assert_eq!(rows, expected, "scan returned rows in wrong order");
    }

    // -----------------------------------------------------------------------
    // Test 2: scan filters invisible rows
    // -----------------------------------------------------------------------

    #[test]
    fn scan_filters_invisible_rows() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid_committed: u64 = 20;
        let xid_aborted: u64 = 21;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid_committed));
        oracle.set_aborted(Xid::new(xid_aborted));

        // Insert rows under two different xids.
        let committed_rows: Vec<(i32, String)> =
            (0_i32..5).map(|i| (i, format!("committed_{i}"))).collect();
        let aborted_rows: Vec<(i32, String)> = (100_i32..105)
            .map(|i| (i, format!("aborted_{i}")))
            .collect();

        for (id, name) in &committed_rows {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid_committed))
                .expect("insert");
        }
        for (id, name) in &aborted_rows {
            let row = vec![Value::Int32(*id), Value::Text(name.clone())];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid_aborted))
                .expect("insert");
        }

        // Observer is at xid 22 (above both inserters). xmin=22 places both
        // xid_committed (20) and xid_aborted (21) below xmin, so neither is
        // in-progress. The oracle resolves 20 as committed and 21 as aborted.
        let snapshot = Snapshot::new(
            Xid::new(xid_aborted + 1),
            Xid::new(xid_aborted + 2),
            Xid::new(xid_aborted + 1),
            CommandId::FIRST,
            [],
        );
        let block_count = heap.block_count(rel());
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let rows = drain_rows(&mut scan);
        assert_eq!(
            rows, committed_rows,
            "scan should only return committed rows"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: scan chunks into 4096-row batches
    // -----------------------------------------------------------------------

    #[test]
    fn scan_chunks_into_4096_row_batches() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 30;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        // Insert 4100 rows (> 4096).
        let total = 4100_usize;
        for i in 0_i32..i32::try_from(total).expect("fits i32") {
            let row = vec![Value::Int32(i), Value::Text(format!("r{i}"))];
            let payload = codec.encode(&row).expect("encode");
            heap.insert(rel(), &payload, insert_opts(xid))
                .expect("insert");
        }

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let mut batch_sizes: Vec<usize> = Vec::new();
        while let Some(batch) = scan.next_batch().expect("operator must not error") {
            batch_sizes.push(batch.rows());
        }

        let total_scanned: usize = batch_sizes.iter().sum();
        assert_eq!(total_scanned, total, "total rows mismatch");
        assert!(
            batch_sizes.contains(&4096),
            "expected at least one full 4096-row batch, got {batch_sizes:?}"
        );
        // Last batch is the remainder.
        assert_eq!(
            *batch_sizes.last().expect("at least one batch"),
            total % 4096,
            "remainder batch size mismatch"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: scan empty relation returns None immediately
    // -----------------------------------------------------------------------

    #[test]
    fn scan_empty_relation_returns_none() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let oracle = Arc::new(MapOracle::new());
        // block_count = 0: no blocks allocated.
        let snapshot = snap_for(1);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            0,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let result = scan.next_batch().expect("operator must not error");
        assert!(
            result.is_none(),
            "empty relation must return None immediately"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: corrupt payload returns TypeMismatch
    // -----------------------------------------------------------------------

    #[test]
    fn scan_propagates_codec_errors_as_type_mismatch() {
        let heap = make_heap();
        let codec = RowCodec::new(schema_i32_text());
        let xid: u64 = 40;
        let oracle = Arc::new(MapOracle::new());
        oracle.set_committed(Xid::new(xid));

        // Insert a row with a deliberately corrupt payload (just random bytes
        // that cannot decode against the schema).
        let corrupt_payload = vec![0xDE, 0xAD]; // way too short / invalid
        heap.insert(rel(), &corrupt_payload, insert_opts(xid))
            .expect("insert corrupt payload");

        let block_count = heap.block_count(rel());
        let snapshot = snap_for(xid);
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            block_count,
            snapshot,
            Arc::clone(&oracle),
            codec,
        );

        let err = scan.next_batch().expect_err("corrupt payload must error");
        assert!(
            matches!(err, ExecError::TypeMismatch(_)),
            "expected TypeMismatch, got {err:?}"
        );
    }
}
