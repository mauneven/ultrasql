//! Top-level `Filter(Scan)` dispatch into B-tree, hash, or BRIN index
//! scans.

use ultrasql_catalog::TableEntry;
use ultrasql_executor::{Filter, IndexScan, Operator, RowCodec};
use ultrasql_planner::{LogicalPlan, ScalarExpr};
use ultrasql_storage::access_method::BrinIndex;

use crate::error::ServerError;

use super::LowerCtx;
use super::btree_probe::{probe_index, scan_brin_candidate_ranges, usize_to_u64_saturating};
use super::catalog_lookup::{
    brin_summary, find_single_column_brin_index, find_single_column_hash_index,
    find_single_column_index, key_type_for_btree,
};
use super::predicate::{IndexKeyRange, match_hash_equality_predicate, match_indexable_predicate};

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
pub(crate) fn try_index_scan(
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
    if let Some(index_entry) =
        find_single_column_index(&ctx.catalog_snapshot, table_entry, col_idx, ctx)
    {
        // Step 4: confirm the indexed column's type is one the B-tree
        // can store. A10 only widens Int32 / Int64 into the i64 key
        // space; other types (text, float, bool) fall back to SeqScan.
        let Some(_widen) = key_type_for_btree(table_entry, col_idx) else {
            return Ok(None);
        };

        // Step 5: probe the B-tree, fetch matching tuples from the heap
        // with MVCC visibility applied, and wrap them in an IndexScan.
        let payloads = probe_index(index_entry, range, ctx)?;
        let codec = RowCodec::new(table_entry.schema.clone());
        return Ok(Some(Box::new(IndexScan::new(payloads, codec))));
    }

    if let Some(op) = try_hash_index_scan(table_entry, predicate, ctx)? {
        return Ok(Some(op));
    }

    try_brin_index_scan(table_entry, col_idx, range, predicate, ctx)
}

fn try_hash_index_scan(
    table_entry: &TableEntry,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some((col_idx, value)) = match_hash_equality_predicate(predicate) else {
        return Ok(None);
    };
    let Some(index_entry) =
        find_single_column_hash_index(&ctx.catalog_snapshot, table_entry, col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(hash_key) = crate::hash_index_value(&value) else {
        return Ok(None);
    };
    let payloads = probe_index(index_entry, IndexKeyRange::point(hash_key), ctx)?;
    let codec = RowCodec::new(table_entry.schema.clone());
    let scan = Box::new(IndexScan::new(payloads, codec));
    Ok(Some(Box::new(Filter::new(scan, predicate.clone()))))
}

fn try_brin_index_scan(
    table_entry: &TableEntry,
    col_idx: usize,
    range: IndexKeyRange,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(index_entry) =
        find_single_column_brin_index(&ctx.catalog_snapshot, table_entry, col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(_widen) = key_type_for_btree(table_entry, col_idx) else {
        return Ok(None);
    };
    if range.is_empty() {
        let codec = RowCodec::new(table_entry.schema.clone());
        let scan = Box::new(IndexScan::new(Vec::new(), codec));
        return Ok(Some(Box::new(Filter::new(scan, predicate.clone()))));
    }
    let Some(brin) = brin_summary(ctx, table_entry.oid, index_entry.oid) else {
        return Ok(None);
    };
    let low_key = range.low.map(BrinIndex::encode_i64_key);
    let high_key = range.high.map(BrinIndex::encode_i64_key);
    let candidate_ranges = brin.candidate_ranges_for_bounds(
        low_key.as_ref().map(|k| k.as_slice()),
        high_key.as_ref().map(|k| k.as_slice()),
    );
    let payloads = scan_brin_candidate_ranges(table_entry, &candidate_ranges, ctx)?;
    let visible_rows = usize_to_u64_saturating(payloads.len());
    ctx.workload_recorder
        .record_index_usage(index_entry.oid.raw(), visible_rows, visible_rows);
    let codec = RowCodec::new(table_entry.schema.clone());
    let scan = Box::new(IndexScan::new(payloads, codec));
    Ok(Some(Box::new(Filter::new(scan, predicate.clone()))))
}
