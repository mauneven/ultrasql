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

use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::{ExecError, Operator};

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
    /// `wrapping_add(delta)` is applied to the target column.
    /// Subtraction is normalised to `delta = -lit` upstream.
    delta: i32,
    xid: Xid,
    command_id: CommandId,
    vm: Option<Arc<VisibilityMap>>,
    schema: Schema,
    done: bool,
}

impl<L: PageLoader> std::fmt::Debug for FusedUpdateInt32Add<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedUpdateInt32Add")
            .field("relation", &self.relation)
            .field("predicate", &self.predicate)
            .field("target_col", &self.target_col)
            .field("delta", &self.delta)
            .field("block_count", &self.block_count)
            .finish()
    }
}

impl<L: PageLoader> FusedUpdateInt32Add<L> {
    /// Construct the fused operator. Caller guarantees the shape
    /// preconditions documented in the module header.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heap: Arc<HeapAccess<L>>,
        relation: RelationId,
        snapshot: Snapshot,
        oracle: Arc<TransactionManager>,
        block_count: u32,
        predicate: Option<FusedPredicate>,
        target_col: u8,
        delta: i32,
        xid: Xid,
        command_id: CommandId,
    ) -> Self {
        let schema = Schema::new([Field::required("count", DataType::Int64)])
            .expect("affected-count schema is well-formed");
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
            vm: None,
            schema,
            done: false,
        }
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
        let n = self
            .heap
            .update_int32_pair_inplace_undo(
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
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

        let affected_i64 = i64::try_from(n).unwrap_or(i64::MAX);
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![affected_i64]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
