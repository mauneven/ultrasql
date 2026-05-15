//! Index-scan lowering: detect `WHERE col op lit` shapes that match an
//! existing B-tree index and lower them to an `IndexScan`.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Value, Xid};
use ultrasql_executor::filter_sum_op::{
    CachedAvgI32Scan, CachedFilterSumI32Scan, CachedSumI32Scan, FilterSumI32Scan,
};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{
    CteScan, Filter, FilterEqI32, HashAggregate, HashJoin, IndexScan, Limit, MemTableScan,
    ModifyKind, ModifyTable, NestedLoopJoin, Operator, Project, ResultOp, RightFactory, RowCodec,
    SeqScan, SetOp, Sort, ValuesScan,
};
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};
use ultrasql_planner::{
    BinaryOp, InMemoryCatalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    TableMeta,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::BlankPageLoader;
use crate::error::ServerError;

use super::LowerCtx;

pub(super) struct IndexKeyRange {
    /// Inclusive lower bound, or `None` for unbounded below.
    pub(super) low: Option<i64>,
    /// Inclusive upper bound, or `None` for unbounded above.
    pub(super) high: Option<i64>,
}

impl IndexKeyRange {
    /// Point probe: `key == k`.
    const fn point(k: i64) -> Self {
        Self {
            low: Some(k),
            high: Some(k),
        }
    }
}

/// Try to lower a `Filter { Scan(table), predicate }` shape into an
/// [`IndexScan`] operator backed by a B-tree probe.
///
/// Returns:
/// - `Ok(Some(op))` when the table is catalog-resolved, has a single-
///   column Int32/Int64 B-tree index covering the predicate's column,
///   and the predicate matches an [indexable shape](#indexable-shapes).
/// - `Ok(None)` for any other case so the caller falls back to the
///   default [`Filter(SeqScan)`] plan. The fallback path is the
///   non-regressing default: a query that does not match an indexable
///   shape, hits an unindexed column, or runs against the sample-table
///   registry continues to use the existing sequential scan + filter
///   path.
/// - `Err(_)` only when the B-tree probe or heap fetch itself fails;
///   those errors are not recoverable by trying a different operator.
///
/// # Indexable shapes
///
/// In this wave the dispatcher recognises:
/// - `col = literal` → point lookup.
/// - `col < literal`, `col <= literal`, `col > literal`, `col >= literal`
///   → one-sided range scan.
/// - `col BETWEEN lo AND hi` (binder-rewritten into
///   `col >= lo AND col <= hi`) → bounded range scan.
/// - `lo <= col AND col <= hi` and equivalent rewrites whose operands
///   commute (the binder may emit any of `>=`, `<=`, `>`, `<` on either
///   side of an AND) → bounded range scan.
///
/// Compound predicates joined by `OR`, `NOT`, or anything beyond a
/// single conjunction of column-vs-literal comparisons fall through to
/// `Ok(None)`. The binder produces precisely these shapes for
/// `BETWEEN` (see `bind_between`); broader rewrites land with the
/// optimizer's predicate canonicaliser in a later wave.
///
/// # Why a single helper instead of a planner emission
///
/// We pattern-match in `lower_query` rather than teaching the planner
/// to emit `LogicalPlan::IndexScan` directly. Two reasons:
/// 1. The planner currently emits `Filter { Scan, predicate }` for
///    every WHERE clause; adding an `IndexScan` node would force every
///    consumer of `LogicalPlan` (binder tests, optimizer rewrites,
///    debug printers, EXPLAIN plumbing) to learn the new variant.
/// 2. The catalog snapshot is materialised in [`LowerCtx`], not in the
///    binder. Doing the dispatch here keeps the catalog-look-up local
///    to one function and the planner stays catalog-snapshot-free,
///    which the optimizer wave (v0.6 P0) needs to remain
///    plan-cache-friendly.
pub(super) fn try_index_scan(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    // Step 1: the input must be a bare base-table scan over a relation
    // the catalog snapshot knows about. Sample-table scans never have
    // an index, so we let them fall back to SeqScan-equivalent shapes.
    let LogicalPlan::Scan { table, .. } = input else {
        return Ok(None);
    };
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };

    // Step 2: extract `(column_index, key_range)` from the predicate.
    // A miss (None) means the shape is not indexable.
    let Some((col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };

    // Step 3: locate an index covering exactly this column. A10's
    // CREATE INDEX path emits `IndexEntry::columns` as a single-element
    // `Vec<u16>` of 0-based attnums; we look up by that exact shape.
    // A composite index that *starts with* this column would also
    // satisfy a point lookup, but the storage layer only supports
    // 8-byte keys today, so we conservatively require a single-column
    // match.
    let Some(index_entry) = find_single_column_index(&ctx.catalog_snapshot, table_entry, col_idx)
    else {
        return Ok(None);
    };

    // Step 4: confirm the indexed column's type is one the B-tree can
    // store. A10 only widens Int32 / Int64 into the i64 key space;
    // other types (text, float, bool) fall back to SeqScan.
    let Some(_widen) = key_type_for_btree(table_entry, col_idx) else {
        return Ok(None);
    };

    // Step 5: probe the B-tree, fetch matching tuples from the heap
    // with MVCC visibility applied, and wrap them in an IndexScan.
    let payloads = probe_index(index_entry, range, ctx)?;
    let codec = RowCodec::new(table_entry.schema.clone());
    Ok(Some(Box::new(IndexScan::new(payloads, codec))))
}

/// Decode a `WHERE` predicate into an `(column_index, IndexKeyRange)`
/// pair when its shape is one the B-tree dispatcher can probe.
///
/// Recognised top-level shapes:
/// - `Binary(op, Column, Literal)` for `op ∈ {Eq, Lt, LtEq, Gt, GtEq}`
///   (or commuted operand order).
/// - `Binary(And, sub_left, sub_right)` where both subterms are
///   single-side comparisons on the same column — produces a bounded
///   range. This is the canonical post-binder shape for `BETWEEN`.
///
/// Returns `None` for anything else; the caller falls back to a
/// general filter.
pub(crate) fn match_indexable_predicate(predicate: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    if let Some((col, range)) = match_simple_comparison(predicate) {
        return Some((col, range));
    }
    // Conjunction of two single-side comparisons on the same column.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    let (left_col, left_range) = match_simple_comparison(left)?;
    let (right_col, right_range) = match_simple_comparison(right)?;
    if left_col != right_col {
        return None;
    }
    let combined = IndexKeyRange {
        low: max_lower_bound(left_range.low, right_range.low),
        high: min_upper_bound(left_range.high, right_range.high),
    };
    Some((left_col, combined))
}

/// Decode a single `Column op Literal` (or commuted) comparison into an
/// `(column_index, IndexKeyRange)`. Returns `None` when the operand
/// types are not Int32 / Int64, the literal cannot be represented as
/// `i64`, or the operator is not a comparison.
///
/// Strict-bound operators are normalised to inclusive bounds via
/// `±1` adjustment (`x > 5` becomes `low = Some(6)`,
/// `x < 5` becomes `high = Some(4)`). Overflowing the adjustment
/// clamps to the sentinel; the resulting range is empty, which is

pub(crate) fn match_simple_comparison(expr: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    // Decompose into (column_idx, literal_as_i64, op_with_col_on_left).
    let (col_idx, raw_lit, op_normalised) = match (left.as_ref(), right.as_ref()) {
        (col @ ScalarExpr::Column { .. }, lit @ ScalarExpr::Literal { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
            (idx, lit_val, *op)
        }
        (lit @ ScalarExpr::Literal { .. }, col @ ScalarExpr::Column { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
            // Flip the operator so `lit op col` reads as `col flipped_op lit`.
            let flipped = match op {
                BinaryOp::Eq => BinaryOp::Eq,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                _ => return None,
            };
            (idx, lit_val, flipped)
        }
        _ => return None,
    };
    let range = match op_normalised {
        BinaryOp::Eq => IndexKeyRange::point(raw_lit),
        BinaryOp::Lt => IndexKeyRange {
            low: None,
            high: raw_lit.checked_sub(1),
        },
        BinaryOp::LtEq => IndexKeyRange {
            low: None,
            high: Some(raw_lit),
        },
        BinaryOp::Gt => IndexKeyRange {
            low: raw_lit.checked_add(1),
            high: None,
        },
        BinaryOp::GtEq => IndexKeyRange {
            low: Some(raw_lit),
            high: None,
        },
        _ => return None,
    };
    Some((col_idx, range))
}

/// Read the column index from a [`ScalarExpr::Column`] whose data type
/// is a `B-tree-supported` integer (`Int32` or `Int64`). Returns
/// `None` for non-column expressions, NULL columns, or non-integer
/// types.
const fn column_idx_for_int_key(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    match data_type {
        DataType::Int32 | DataType::Int64 => Some(*index),
        _ => None,
    }
}

/// Lift an integer-typed literal to `i64`. `Int32` is sign-extended
/// via the lossless `i64::from(i32)` widening conversion. Returns
/// `None` for non-integer literals (text, float, NULL, …).
pub(super) fn literal_as_i64(expr: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Literal { value, .. } = expr else {
        return None;
    };
    match value {
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Pick the tighter (i.e., larger) lower bound from two candidates.
/// `None` means "no constraint"; any concrete bound wins over `None`.
const fn max_lower_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x > y { x } else { y }),
    }
}

/// Pick the tighter (i.e., smaller) upper bound from two candidates.
const fn min_upper_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x < y { x } else { y }),
    }
}

/// Return the [`IndexEntry`] that covers exactly the single column
/// `col_idx` of `table_entry`, if any. Composite indexes whose first
/// key is `col_idx` are *not* returned today: the on-disk B-tree only
/// supports 8-byte keys, so a composite index could not be probed
/// through the existing API.
pub(super) fn find_single_column_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes
        .iter()
        .find(|e| e.columns.len() == 1 && e.columns[0] == attnum)
}

/// Confirm the keyed column has a type the B-tree can store. Returns
/// `Some(widen)` where `widen == true` for Int32 (key is sign-extended
/// to `i64`) and `false` for Int64 (key is stored directly). Returns
/// `None` for any other type so the caller falls back to `SeqScan`.
///
/// Mirrors the check in `Server::execute_create_index` — keep the two
/// in sync, or a `CREATE INDEX` that succeeds will produce an index a

pub(super) fn key_type_for_btree(table_entry: &TableEntry, col_idx: usize) -> Option<bool> {
    let field = table_entry.schema.field(col_idx)?;
    match field.data_type {
        DataType::Int32 => Some(true),
        DataType::Int64 => Some(false),
        _ => None,
    }
}

/// Probe the B-tree for every tuple satisfying `range` and return the
/// (visible) heap payloads in B-tree-order.
///
/// Visibility is enforced inline: a tuple whose MVCC header is not
/// visible to `ctx.snapshot` under `ctx.oracle` is silently dropped.
/// This means the `IndexScan` operator never sees a tuple a `SeqScan`
/// would hide; the user observes the same row set whether or not the
/// index is consulted.
///
/// # Errors
///
/// Returns [`ServerError::Ddl`] when the B-tree probe or heap fetch
/// fails. The `Ddl` variant carries a dynamic message and is the
/// appropriate channel for runtime storage faults; the simpler
/// `Unsupported` channel is reserved for shape-level rejections that
/// the caller can recover from by falling back to `SeqScan`.

pub(super) fn probe_index(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let index_rel = RelationId::new(index_entry.oid.raw());
    let pool = ctx.heap.buffer_pool();
    let btree: BTree<BlankPageLoader> =
        BTree::open(Arc::clone(pool), index_rel, index_entry.root_block);

    // Collect the matching TupleIds. A point lookup uses the cheap
    // `lookup` path; everything else walks the leaf chain via
    // `range_scan` between `[low, high+1)` (half-open). `range_scan`'s
    // upper bound is exclusive, so we add 1 to `high` to keep the
    // inclusive contract — overflowing to `None` (i.e., scan to the
    // end of the leaf chain) when `high == i64::MAX`.
    let mut tids: Vec<ultrasql_core::TupleId> = Vec::new();
    match (range.low, range.high) {
        (Some(lo), Some(hi)) if lo == hi => {
            if let Some(tid) = btree
                .lookup::<i64>(lo)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
            {
                tids.push(tid);
            }
        }
        (low, high) => {
            // Walk the half-open `[start, end_exclusive)`. `start =
            // low.unwrap_or(i64::MIN)` and `end_exclusive =
            // high.map(|h| h.checked_add(1))` — when the +1 overflows we
            // pass `None` to mean "scan to the end of the leaf chain".
            let start = low.unwrap_or(i64::MIN);
            // `i64::MAX + 1` overflows to `None`, which `range_scan`
            // treats as "unbounded above" — exactly the contract we want.
            let end_exclusive: Option<i64> = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (_key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                tids.push(tid);
            }
        }
    }

    // Fetch the heap tuples in B-tree order and apply MVCC visibility
    // inline. An index entry whose heap tuple is invisible to the
    // statement's snapshot is silently dropped — the same outcome a
    // SeqScan would deliver. We use [`HeapAccess::fetch`] (no
    // visibility check) plus an explicit `is_visible` call rather than
    // chaining through `scan_visible` because the latter walks a
    // block-by-block iterator we cannot project onto an arbitrary
    // TupleId list.
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(tids.len());
    for tid in tids {
        let tuple = ctx
            .heap
            .fetch(tid)
            .map_err(|e| ServerError::ddl(format!("IndexScan heap fetch: {e}")))?;
        let visibility = is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref());
        if matches!(visibility, Visibility::Visible) {
            payloads.push(tuple.data);
        }
    }
    Ok(payloads)
}
