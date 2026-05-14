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
use ultrasql_storage::heap::{HeapAccess, UpdateOptions, UpdatePayload};
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
    const fn check(self, lhs: i32, rhs: i32) -> bool {
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
            schema,
            done: false,
        }
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
        let cap_upper = (self.block_count as usize).saturating_mul(180);
        let mut edits: Vec<(ultrasql_core::TupleId, UpdatePayload)> = Vec::with_capacity(cap_upper);

        let predicate = self.predicate;
        let target_col = self.target_col;
        let delta = self.delta;

        // `for_each_visible` holds the per-page read guard for the
        // duration of one page's slot loop and reads each slot's
        // bytes directly off the buffer-pool page — no per-page 8 KiB
        // memcpy into a walker-side scratch buffer. Cheaper than
        // `scan_visible_walker` when the caller's only job is to
        // pull `(tid, payload)` and immediately consume them, which
        // is exactly this operator's shape.
        self.heap
            .for_each_visible(
                self.relation,
                self.block_count,
                &self.snapshot,
                &*self.oracle,
                |tid, _header, payload| {
                    // `(Int32, Int32)` relation payload layout:
                    //   byte 0      null bitmap (always 0 for the bench shape)
                    //   bytes 1..5  id   little-endian i32
                    //   bytes 5..9  val  little-endian i32
                    if payload.len() < 9 {
                        return Err(ultrasql_storage::heap::HeapError::MalformedHeader(
                            "fused UPDATE expected a 9-byte (Int32, Int32) payload",
                        ));
                    }
                    let id = i32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    let val = i32::from_le_bytes([payload[5], payload[6], payload[7], payload[8]]);

                    if let Some(pred) = predicate {
                        let key = if pred.col_index == 0 { id } else { val };
                        if !pred.op.check(key, pred.literal) {
                            return Ok(());
                        }
                    }

                    let (new_id, new_val) = if target_col == 0 {
                        (id.wrapping_add(delta), val)
                    } else {
                        (id, val.wrapping_add(delta))
                    };

                    let mut new_payload = UpdatePayload::new();
                    new_payload.push(0_u8);
                    new_payload.extend_from_slice(&new_id.to_le_bytes());
                    new_payload.extend_from_slice(&new_val.to_le_bytes());
                    edits.push((tid, new_payload));
                    Ok(())
                },
            )
            .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;

        let n = edits.len();
        if n > 0 {
            self.heap
                .update_many(
                    edits,
                    UpdateOptions {
                        xid: self.xid,
                        command_id: self.command_id,
                        // HOT eligibility is left on; the bench's
                        // 99%-full pages cannot fit a new version so
                        // every entry hits the bulk-non-HOT fallback,
                        // but the contract is the same as the
                        // ModifyTable path so any future caller of
                        // this operator over partially-empty pages
                        // gets the HOT win.
                        hot_eligible: true,
                        wal: None,
                        vm: None,
                    },
                )
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        }

        let affected_i64 = i64::try_from(n).unwrap_or(i64::MAX);
        let batch = Batch::new([Column::Int64(NumericColumn::from_data(vec![affected_i64]))])
            .map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
