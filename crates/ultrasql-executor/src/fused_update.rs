//! [`FusedUpdateInt32Add`] — a single-operator UPDATE for the
//! `UPDATE t SET col_i = col_i ± lit [WHERE col_j cmp lit]` shape
//! over a `(Int32, Int32)` relation.
//!
//! The default UPDATE plan lowers as `ModifyTable(Filter(SeqScan(t)))`.
//! That chain materialises a 4-column batch (`tid_block`, `tid_slot`,
//! `id`, `val`) through `SeqScan`, applies a SIMD-comparator inside
//! `Filter`, then re-decodes the four columns inside
//! `ModifyTable::next_batch` to build per-row `(TupleId,
//! UpdatePayload)` edits before handing them to
//! [`HeapAccess::update_many`]. For the cross_compare_sql
//! `update_throughput_10k` shape that batch round-trip is ~150 µs of
//! pure plumbing — column builders, batch construction, filter
//! materialisation, batch-to-row decode — that the actual update path
//! immediately discards.
//!
//! `FusedUpdateInt32Add` reads the heap walker directly and emits one
//! `(TupleId, UpdatePayload)` per visible tuple that passes the
//! predicate, then makes the same `HeapAccess::update_many` call. The
//! per-row cost is the strict minimum the MVCC contract requires: one
//! tuple visibility check (the walker's existing logic), one 4-byte
//! predicate decode + comparison, and one 9-byte payload assembly.
//!
//! Shape recognised:
//!
//! - Relation has two `Int32` columns.
//! - Single assignment `col_i = col_i ± Int32 literal` (BinaryOp::Add
//!   or BinaryOp::Sub against a literal; subtraction is normalised to
//!   `delta = -lit`).
//! - Optional `WHERE col_j cmp literal` predicate where `cmp` is one
//!   of `=`, `!=`, `<`, `<=`, `>`, `>=`, the column is `Int32`, and
//!   the literal is `Int32`.
//!
//! Any other shape falls back to the default
//! `ModifyTable(Filter(SeqScan))` plan.

use std::sync::Arc;

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, TupleId, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::{HeapAccess, HeapError};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;

use crate::affected_rows::affected_rows_batch;
use crate::{ExecError, Operator};

type TargetTidLock = dyn Fn(TupleId) -> Result<bool, String> + Send + Sync;

/// Predicate descriptor for the optional `WHERE col_j cmp literal`
/// clause. Both `col_index` and `literal` are typed at construction
/// time: only `Int32` columns and `Int32` literals are accepted.
#[derive(Clone, Copy, Debug)]
pub struct FusedPredicate {
    /// 0-based index of the column the comparison reads (0 or 1 over
    /// the (Int32, Int32) relation).
    pub col_index: u8,
    /// Comparison kind.
    pub op: FusedCmp,
    /// Right-hand `Int32` literal.
    pub literal: i32,
}

/// Comparison kinds supported by the fused operator. Mirrors the
/// existing `CmpOp` enum in `filter_op`; duplicated here so the fused
/// path can stay independent of that module's internals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FusedCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl FusedCmp {
    #[inline]
    pub const fn check(self, lhs: i32, rhs: i32) -> bool {
        match self {
            Self::Eq => lhs == rhs,
            Self::Ne => lhs != rhs,
            Self::Lt => lhs < rhs,
            Self::Le => lhs <= rhs,
            Self::Gt => lhs > rhs,
            Self::Ge => lhs >= rhs,
        }
    }
}

/// Operator state. Constructed by `lower_real_update` when the input
/// shape matches; transparently equivalent to
/// `ModifyTable(Filter(SeqScan))` for any caller that asks for the
/// affected-row-count batch this operator emits.
pub struct FusedUpdateInt32Add<L: PageLoader> {
    heap: Arc<HeapAccess<L>>,
    relation: RelationId,
    snapshot: Snapshot,
    oracle: Arc<TransactionManager>,
    block_count: u32,
    predicate: Option<FusedPredicate>,
    /// 0 = assign to `id`, 1 = assign to `val`. Other indices are
    /// rejected at construction time.
    target_col: u8,
    /// `checked_add(delta)` is applied to the target column.
    /// Subtraction is normalised to `delta = -lit` upstream.
    delta: i32,
    xid: Xid,
    command_id: CommandId,
    target_tids: Option<Vec<TupleId>>,
    target_tid_lock: Option<Arc<TargetTidLock>>,
    refresh_snapshot_after_lock: bool,
    vm: Option<Arc<VisibilityMap>>,
    schema: Schema,
    done: bool,
}

/// Construction inputs for [`FusedUpdateInt32Add`].
///
/// The lowering layer fills this only after it validates the relation,
/// assignment, and optional predicate against the fused UPDATE shape.
pub struct FusedUpdateInt32AddConfig<L: PageLoader> {
    /// Shared heap access method for the target relation.
    pub heap: Arc<HeapAccess<L>>,
    /// Target relation identifier.
    pub relation: RelationId,
    /// Statement snapshot used for MVCC visibility.
    pub snapshot: Snapshot,
    /// Transaction manager used as the XID status oracle.
    pub oracle: Arc<TransactionManager>,
    /// Number of heap blocks to scan.
    pub block_count: u32,
    /// Optional Int32 comparison predicate.
    pub predicate: Option<FusedPredicate>,
    /// Target column index inside the `(Int32, Int32)` row shape.
    pub target_col: u8,
    /// Delta applied with checked Int32 addition.
    pub delta: i32,
    /// Transaction ID stamped into updated tuples.
    pub xid: Xid,
    /// Command ID stamped into updated tuples.
    pub command_id: CommandId,
}

impl<L: PageLoader> std::fmt::Debug for FusedUpdateInt32AddConfig<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedUpdateInt32AddConfig")
            .field("relation", &self.relation)
            .field("block_count", &self.block_count)
            .field("predicate", &self.predicate)
            .field("target_col", &self.target_col)
            .field("delta", &self.delta)
            .field("xid", &self.xid)
            .field("command_id", &self.command_id)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> std::fmt::Debug for FusedUpdateInt32Add<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedUpdateInt32Add")
            .field("relation", &self.relation)
            .field("predicate", &self.predicate)
            .field("target_col", &self.target_col)
            .field("delta", &self.delta)
            .field("target_tids", &self.target_tids.as_ref().map(Vec::len))
            .field("block_count", &self.block_count)
            .finish()
    }
}

impl<L: PageLoader> FusedUpdateInt32Add<L> {
    /// Construct the fused operator. Caller guarantees the shape
    /// preconditions documented in the module header.
    #[must_use]
    pub fn new(config: FusedUpdateInt32AddConfig<L>) -> Self {
        let FusedUpdateInt32AddConfig {
            heap,
            relation,
            snapshot,
            oracle,
            block_count,
            predicate,
            target_col,
            delta,
            xid,
            command_id,
        } = config;
        let schema = match Schema::new([Field::required("count", DataType::Int64)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "fused update count schema construction failed");
                Schema::empty()
            }
        };
        Self {
            heap,
            relation,
            snapshot,
            oracle,
            block_count,
            predicate,
            target_col,
            delta,
            xid,
            command_id,
            target_tids: None,
            target_tid_lock: None,
            refresh_snapshot_after_lock: false,
            vm: None,
            schema,
            done: false,
        }
    }

    /// Restrict the fused update to the heap tuple IDs found through an
    /// index point probe.
    #[must_use]
    pub fn with_target_tids(mut self, target_tids: Vec<TupleId>) -> Self {
        self.target_tids = Some(target_tids);
        self
    }

    /// Acquire row locks before indexed point updates.
    ///
    /// The callback returns `true` when it had to wait. When
    /// `refresh_snapshot_after_lock` is true, waits install a fresh
    /// statement snapshot before the heap write.
    /// That is the READ COMMITTED update path: a waiter must operate on
    /// the latest committed row after the prior writer releases the row.
    #[must_use]
    pub fn with_target_tid_lock<F>(mut self, lock: F, refresh_snapshot_after_lock: bool) -> Self
    where
        F: Fn(TupleId) -> Result<bool, String> + Send + Sync + 'static,
    {
        self.target_tid_lock = Some(Arc::new(lock));
        self.refresh_snapshot_after_lock = refresh_snapshot_after_lock;
        self
    }

    #[must_use]
    pub fn with_visibility_map(mut self, vm: Arc<VisibilityMap>) -> Self {
        self.vm = Some(vm);
        self
    }
}

impl<L: PageLoader + Send + Sync + std::fmt::Debug + 'static> Operator for FusedUpdateInt32Add<L> {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Pre-size the edits Vec to the upper bound on visible tuples.
        // A `(Int32, Int32)` relation packs ~166 tuples per 8 KiB page;
        // overshooting is cheap because `Vec::with_capacity` allocates
        // contiguous memory but `SmallVec`'s inline storage means each
        // entry's payload stays on the stack until update_many consumes
        // it.
        let predicate = self.predicate;
        let target_col = self.target_col;
        let delta = self.delta;

        // Single-pass UPDATE: scan, predicate-filter, write new
        // version, and stamp the old slot under one source-page
        // write guard. Bypasses `update_many` / `insert_batch` and
        // the intermediate edits Vec entirely; see
        // `HeapAccess::update_int32_pair_in_place_add` for the
        // page-traversal contract.
        let predicate_fn = |id: i32, val: i32| -> bool {
            match predicate {
                None => true,
                Some(pred) => {
                    let key = if pred.col_index == 0 { id } else { val };
                    pred.op.check(key, pred.literal)
                }
            }
        };
        // Thread the buffer pool's WAL sink when present so per-row
        // `HeapUpdateInPlace` records are emitted alongside the page
        // mutation. The pool decides whether a sink is configured;
        // tests using the no-sink constructor still exercise the
        // mutation path with `None`.
        let wal_sink_arc = self.heap.wal_sink().cloned();
        let wal_sink: Option<&dyn ultrasql_storage::WalSink> = wal_sink_arc.as_deref();
        let n = if let Some(target_tids) = &self.target_tids {
            let mut total = 0_usize;
            let mut update_snapshot = self.snapshot.clone();
            for tid in target_tids {
                let mut refreshed_after_lock = false;
                if let Some(lock) = &self.target_tid_lock {
                    let waited = lock(*tid).map_err(ExecError::TypeMismatch)?;
                    if waited && self.refresh_snapshot_after_lock {
                        update_snapshot = self.oracle.statement_snapshot(self.xid, self.command_id);
                        refreshed_after_lock = true;
                    }
                }
                let update_result = self.heap.update_int32_pair_tid_inplace_undo(
                    *tid,
                    &update_snapshot,
                    &*self.oracle,
                    predicate_fn,
                    target_col,
                    delta,
                    self.xid,
                    self.command_id,
                    wal_sink,
                    self.vm.as_deref(),
                );
                total += match update_result {
                    Ok(updated) => updated,
                    Err(HeapError::WriteConflict(_))
                        if self.target_tid_lock.is_some()
                            && self.refresh_snapshot_after_lock
                            && !refreshed_after_lock =>
                    {
                        update_snapshot = self.oracle.statement_snapshot(self.xid, self.command_id);
                        self.heap
                            .update_int32_pair_tid_inplace_undo(
                                *tid,
                                &update_snapshot,
                                &*self.oracle,
                                predicate_fn,
                                target_col,
                                delta,
                                self.xid,
                                self.command_id,
                                wal_sink,
                                self.vm.as_deref(),
                            )
                            .map_err(heap_update_error_to_exec_error)?
                    }
                    Err(e) => return Err(heap_update_error_to_exec_error(e)),
                };
            }
            total
        } else {
            if wal_sink.is_none() {
                self.heap.update_int32_pair_inplace_undo_parallel_no_wal(
                    self.relation,
                    self.block_count,
                    &self.snapshot,
                    &*self.oracle,
                    predicate_fn,
                    target_col,
                    delta,
                    self.xid,
                    self.command_id,
                    self.vm.as_deref(),
                )
            } else {
                self.heap.update_int32_pair_inplace_undo(
                    self.relation,
                    self.block_count,
                    &self.snapshot,
                    &*self.oracle,
                    predicate_fn,
                    target_col,
                    delta,
                    self.xid,
                    self.command_id,
                    wal_sink,
                    self.vm.as_deref(),
                )
            }
            .map_err(heap_update_error_to_exec_error)?
        };

        Ok(Some(affected_rows_batch(n, "fused UPDATE")?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn heap_update_error_to_exec_error(error: HeapError) -> ExecError {
    match error {
        HeapError::NumericOverflow(detail) => ExecError::NumericFieldOverflow(detail.to_owned()),
        other => ExecError::TypeMismatch(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{
        CommandId, DataType, Field, PageId, RelationId, Result, Schema, TupleId, Value, Xid,
    };
    use ultrasql_mvcc::Snapshot;
    use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
    use ultrasql_storage::heap::{HeapAccess, InsertOptions};
    use ultrasql_storage::page::Page;
    use ultrasql_txn::TransactionManager;
    use ultrasql_vec::column::Column;

    use super::{FusedCmp, FusedPredicate, FusedUpdateInt32Add, FusedUpdateInt32AddConfig};
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::fused_delete::{FusedDeleteInt32Pair, FusedDeleteInt32PairConfig};
    use crate::fused_insert::FusedInsertInt32Pair;
    use crate::row_codec::RowCodec;
    use crate::seq_scan::SeqScan;

    #[derive(Default, Debug)]
    struct MapLoader {
        store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
    }

    impl PageLoader for MapLoader {
        fn load(&self, page_id: PageId) -> Result<Page> {
            let stored = {
                let store = self.store.lock();
                store.get(&page_id).map(|bytes| {
                    let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                        .into_boxed_slice()
                        .try_into()
                        .expect("alloc matches PAGE_SIZE");
                    copy.copy_from_slice(&**bytes);
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
        RelationId::new(9001)
    }

    fn schema_pair() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn heap() -> Arc<HeapAccess<MapLoader>> {
        Arc::new(HeapAccess::new(Arc::new(BufferPool::new(
            32,
            MapLoader::default(),
        ))))
    }

    fn snapshot(current_xid: Xid) -> Snapshot {
        snapshot_at(current_xid, CommandId::FIRST)
    }

    fn snapshot_at(current_xid: Xid, command_id: CommandId) -> Snapshot {
        Snapshot::new(
            Xid::BOOTSTRAP,
            current_xid.next(),
            current_xid,
            command_id,
            [],
        )
    }

    fn insert_pair_rows(heap: &HeapAccess<MapLoader>, rows: &[(i32, i32)]) -> Vec<TupleId> {
        let codec = RowCodec::new(schema_pair());
        rows.iter()
            .map(|(id, val)| {
                let payload = codec
                    .encode(&[Value::Int32(*id), Value::Int32(*val)])
                    .expect("encode pair");
                heap.insert(
                    rel(),
                    &payload,
                    InsertOptions {
                        xmin: Xid::BOOTSTRAP,
                        command_id: CommandId::FIRST,
                        wal: None,
                        fsm: None,
                        vm: None,
                    },
                )
                .expect("insert")
            })
            .collect()
    }

    fn affected_count(batch: ultrasql_vec::Batch) -> i64 {
        let Column::Int64(column) = &batch.columns()[0] else {
            panic!("affected count must be Int64");
        };
        column.data()[0]
    }

    fn scan_pairs(
        heap: Arc<HeapAccess<MapLoader>>,
        oracle: Arc<TransactionManager>,
        current_xid: Xid,
    ) -> Vec<(i32, i32)> {
        let mut scan = SeqScan::new(
            Arc::clone(&heap),
            rel(),
            heap.block_count(rel()),
            snapshot_at(current_xid, CommandId::new(1)),
            oracle,
            RowCodec::new(schema_pair()),
        );
        let schema = scan.schema().clone();
        let mut rows = Vec::new();
        while let Some(batch) = scan.next_batch().expect("scan") {
            for row in batch_to_rows(&batch, &schema).expect("rows") {
                let (Value::Int32(id), Value::Int32(val)) = (&row[0], &row[1]) else {
                    panic!("expected int32 pair");
                };
                rows.push((*id, *val));
            }
        }
        rows.sort_unstable();
        rows
    }

    #[test]
    fn fused_insert_writes_int32_pairs_and_is_single_shot() {
        let heap = heap();
        let oracle = Arc::new(TransactionManager::new());
        let mut op = FusedInsertInt32Pair::new(
            Arc::clone(&heap),
            rel(),
            vec![(1, 10), (2, 20), (3, 30)],
            Xid::BOOTSTRAP,
            CommandId::FIRST,
            None,
            None,
        );

        let batch = op.next_batch().expect("insert").expect("batch");
        assert_eq!(affected_count(batch), 3);
        assert!(op.next_batch().expect("single shot").is_none());
        assert_eq!(
            scan_pairs(heap, oracle, Xid::new(100)),
            vec![(1, 10), (2, 20), (3, 30)]
        );
    }

    #[test]
    fn fused_update_filters_rows_and_updates_target_column() {
        let heap = heap();
        let oracle = Arc::new(TransactionManager::new());
        insert_pair_rows(&heap, &[(1, 10), (2, 20), (3, 30)]);
        let xid = Xid::new(20);
        let predicate = FusedPredicate {
            col_index: 0,
            op: FusedCmp::Ge,
            literal: 2,
        };
        let mut op = FusedUpdateInt32Add::new(FusedUpdateInt32AddConfig {
            heap: Arc::clone(&heap),
            relation: rel(),
            snapshot: snapshot(xid),
            oracle: Arc::clone(&oracle),
            block_count: heap.block_count(rel()),
            predicate: Some(predicate),
            target_col: 1,
            delta: 5,
            xid,
            command_id: CommandId::FIRST,
        });

        let batch = op.next_batch().expect("update").expect("batch");
        assert_eq!(affected_count(batch), 2);
        assert!(op.next_batch().expect("single shot").is_none());
        assert_eq!(
            scan_pairs(heap, oracle, xid),
            vec![(1, 10), (2, 25), (3, 35)]
        );
    }

    #[test]
    fn fused_update_locks_target_tids_and_refreshes_after_wait() {
        let heap = heap();
        let oracle = Arc::new(TransactionManager::new());
        let tids = insert_pair_rows(&heap, &[(1, 10), (2, 20)]);
        let xid = Xid::new(21);
        let locked = Arc::new(Mutex::new(Vec::new()));
        let locked_for_callback = Arc::clone(&locked);
        let mut op = FusedUpdateInt32Add::new(FusedUpdateInt32AddConfig {
            heap: Arc::clone(&heap),
            relation: rel(),
            snapshot: snapshot(xid),
            oracle: Arc::clone(&oracle),
            block_count: heap.block_count(rel()),
            predicate: None,
            target_col: 0,
            delta: 100,
            xid,
            command_id: CommandId::FIRST,
        })
        .with_target_tids(vec![tids[1]])
        .with_target_tid_lock(
            move |tid| {
                locked_for_callback.lock().push(tid);
                Ok(true)
            },
            true,
        );

        let batch = op.next_batch().expect("target update").expect("batch");
        assert_eq!(affected_count(batch), 1);
        assert_eq!(&*locked.lock(), &[tids[1]]);
        assert_eq!(scan_pairs(heap, oracle, xid), vec![(1, 10), (102, 20)]);
    }

    #[test]
    fn fused_delete_filters_visible_rows() {
        let heap = heap();
        let oracle = Arc::new(TransactionManager::new());
        insert_pair_rows(&heap, &[(1, 10), (2, 20), (3, 30)]);
        let xid = Xid::new(22);
        let predicate = FusedPredicate {
            col_index: 1,
            op: FusedCmp::Lt,
            literal: 30,
        };
        let mut op = FusedDeleteInt32Pair::new(FusedDeleteInt32PairConfig {
            heap: Arc::clone(&heap),
            relation: rel(),
            snapshot: snapshot(xid),
            oracle: Arc::clone(&oracle),
            block_count: heap.block_count(rel()),
            predicate: Some(predicate),
            xid,
            command_id: CommandId::FIRST,
        });

        let batch = op.next_batch().expect("delete").expect("batch");
        assert_eq!(affected_count(batch), 2);
        assert!(op.next_batch().expect("single shot").is_none());
        assert_eq!(scan_pairs(heap, oracle, xid), vec![(3, 30)]);
    }

    #[test]
    fn fused_comparison_matrix_matches_int32_predicate_semantics() {
        let cases = [
            (FusedCmp::Eq, 4, 4, true),
            (FusedCmp::Ne, 4, 5, true),
            (FusedCmp::Lt, 4, 5, true),
            (FusedCmp::Le, 4, 4, true),
            (FusedCmp::Gt, 5, 4, true),
            (FusedCmp::Ge, 4, 4, true),
            (FusedCmp::Eq, 4, 5, false),
            (FusedCmp::Ne, 4, 4, false),
            (FusedCmp::Lt, 5, 4, false),
            (FusedCmp::Le, 5, 4, false),
            (FusedCmp::Gt, 4, 5, false),
            (FusedCmp::Ge, 4, 5, false),
        ];
        for (op, lhs, rhs, expected) in cases {
            assert_eq!(op.check(lhs, rhs), expected, "{op:?} {lhs} {rhs}");
        }
    }
}
