//! Index-scan lowering: detect `WHERE col op lit` shapes that match an
//! existing B-tree index and lower them to an `IndexScan`.

use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{BlockNumber, DataType, Field, RelationId, Schema, TupleId, Value};
use ultrasql_executor::{Filter, IndexOnlyScan, IndexScan, Limit, Operator, RowCodec, TopK};
use ultrasql_mvcc::{InfoMask, TupleHeader, Visibility, is_visible};
use ultrasql_planner::{BinaryOp, LogicalIndexMethod, LogicalPlan, ScalarExpr, SortKey};
use ultrasql_storage::access_method::{
    BrinIndex, HnswMetric, PageBackedHnswIndex, PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;

use crate::BlankPageLoader;
use crate::error::ServerError;

use super::LowerCtx;
use super::modify::lower_project_columns;

const LATE_MATERIALIZATION_MIN_TABLE_WIDTH: usize = 8;
const LATE_MATERIALIZATION_MAX_PROJECTED_COLUMNS: usize = 3;

type LateMaterializationProjectShape<'a> =
    (&'a LogicalPlan, &'a [(ScalarExpr, String)], Option<u64>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexKeyRange {
    /// Inclusive lower bound, or `None` for unbounded below.
    pub(crate) low: Option<i64>,
    /// Inclusive upper bound, or `None` for unbounded above.
    pub(crate) high: Option<i64>,
}

impl IndexKeyRange {
    /// Point probe: `key == k`.
    const fn point(k: i64) -> Self {
        Self {
            low: Some(k),
            high: Some(k),
        }
    }

    /// Empty key range.
    pub(crate) const fn empty() -> Self {
        Self {
            low: Some(1),
            high: Some(0),
        }
    }

    /// Whether this inclusive range cannot contain any key.
    pub(crate) const fn is_empty(self) -> bool {
        matches!((self.low, self.high), (Some(low), Some(high)) if low > high)
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

/// Try to lower `ORDER BY vector_distance LIMIT k` through an available vector
/// ANN runtime index.
///
/// Missing or invalid ANN metadata returns `Ok(None)`, letting the caller use
/// exact `Sort + Limit`. This is the correctness fallback for restarts, DML
/// invalidation, unsupported metrics, and non-top-k shapes.
pub(super) fn try_hnsw_top_k_limit(
    input: &LogicalPlan,
    limit: u64,
    offset: u64,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if offset != 0 || limit == 0 || limit == u64::MAX {
        return Ok(None);
    }
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    match input {
        LogicalPlan::Sort {
            input: sort_input,
            keys,
        } => try_hnsw_sorted_scan(sort_input, keys, limit, ctx),
        LogicalPlan::Project {
            input: project_input,
            exprs,
            ..
        } => {
            let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = project_input.as_ref()
            else {
                return Ok(None);
            };
            let Some(scan) = try_hnsw_sorted_scan(sort_input, keys, limit, ctx)? else {
                return Ok(None);
            };
            lower_project_columns(scan, exprs).map(Some)
        }
        _ => Ok(None),
    }
}

fn try_hnsw_sorted_scan(
    sort_input: &LogicalPlan,
    keys: &[SortKey],
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let [key] = keys else {
        return Ok(None);
    };
    if !key.asc {
        return Ok(None);
    }
    let LogicalPlan::Scan {
        table, projection, ..
    } = sort_input
    else {
        return Ok(None);
    };
    if projection.is_some() {
        return Ok(None);
    }
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let Some((col_idx, metric, probe)) = match_hnsw_sort_key(&key.expr) else {
        return Ok(None);
    };
    let hits = if let Some(hnsw) = find_hnsw_index(ctx, table_entry, col_idx, metric) {
        hnsw.search(&probe, limit)
            .map_err(|e| ServerError::ddl(format!("HNSW search: {e}")))?
            .into_iter()
            .map(|hit| VectorSearchHit { tid: hit.tid })
            .collect::<Vec<_>>()
    } else if let Some(ivfflat) = find_ivfflat_index(ctx, table_entry, col_idx, metric) {
        ivfflat
            .search(&probe, limit)
            .map_err(|e| ServerError::ddl(format!("IVFFlat search: {e}")))?
            .into_iter()
            .map(|hit| VectorSearchHit { tid: hit.tid })
            .collect::<Vec<_>>()
    } else {
        return Ok(None);
    };
    if hits.is_empty() {
        return Ok(None);
    }
    let payloads = fetch_vector_visible_payloads(&hits, table_entry, col_idx, metric, &probe, ctx)?;
    let codec = RowCodec::new(table_entry.schema.clone());
    Ok(Some(Box::new(IndexScan::new(payloads, codec))))
}

fn match_hnsw_sort_key(expr: &ScalarExpr) -> Option<(usize, HnswMetric, Vec<f32>)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let metric = match op {
        BinaryOp::VectorL2Distance => HnswMetric::L2,
        BinaryOp::VectorCosineDistance => HnswMetric::Cosine,
        BinaryOp::VectorNegativeInnerProduct => HnswMetric::NegativeInnerProduct,
        BinaryOp::VectorL1Distance => HnswMetric::L1,
        _ => return None,
    };
    hnsw_column_probe(left, right, metric).or_else(|| hnsw_column_probe(right, left, metric))
}

fn hnsw_column_probe(
    column: &ScalarExpr,
    probe: &ScalarExpr,
    metric: HnswMetric,
) -> Option<(usize, HnswMetric, Vec<f32>)> {
    let ScalarExpr::Column {
        index,
        data_type: DataType::Vector { .. } | DataType::HalfVec { .. },
        ..
    } = column
    else {
        return None;
    };
    let ScalarExpr::Literal {
        value: Value::Vector(values) | Value::HalfVec(values),
        ..
    } = probe
    else {
        return None;
    };
    Some((*index, metric, values.clone()))
}

fn find_hnsw_index(
    ctx: &LowerCtx<'_>,
    table_entry: &TableEntry,
    col_idx: usize,
    metric: HnswMetric,
) -> Option<Arc<PageBackedHnswIndex>> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = ctx
        .catalog_snapshot
        .indexes_by_table
        .get(&table_entry.oid)?;
    let constraints = ctx.table_constraints.get(&table_entry.oid)?;
    indexes.iter().find_map(|index| {
        if index.columns.as_slice() != [attnum] {
            return None;
        }
        let metadata = constraints.indexes.get(&index.oid)?;
        if metadata.method != LogicalIndexMethod::Hnsw {
            return None;
        }
        let hnsw = metadata.hnsw.as_ref()?;
        if hnsw.metric() == metric && hnsw.is_available() {
            Some(Arc::clone(hnsw))
        } else {
            None
        }
    })
}

fn find_ivfflat_index(
    ctx: &LowerCtx<'_>,
    table_entry: &TableEntry,
    col_idx: usize,
    metric: HnswMetric,
) -> Option<Arc<PageBackedIvfFlatIndex>> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = ctx
        .catalog_snapshot
        .indexes_by_table
        .get(&table_entry.oid)?;
    let constraints = ctx.table_constraints.get(&table_entry.oid)?;
    indexes.iter().find_map(|index| {
        if index.columns.as_slice() != [attnum] {
            return None;
        }
        let metadata = constraints.indexes.get(&index.oid)?;
        if metadata.method != LogicalIndexMethod::IvfFlat {
            return None;
        }
        let ivfflat = metadata.ivfflat.as_ref()?;
        if ivfflat.metric() == metric && ivfflat.is_available() {
            Some(Arc::clone(ivfflat))
        } else {
            None
        }
    })
}

#[derive(Clone, Copy, Debug)]
struct VectorSearchHit {
    tid: TupleId,
}

fn fetch_vector_visible_payloads(
    hits: &[VectorSearchHit],
    table_entry: &TableEntry,
    col_idx: usize,
    metric: HnswMetric,
    probe: &[f32],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let codec = RowCodec::new(table_entry.schema.clone());
    let mut rows: Vec<(f32, TupleId, Vec<u8>)> = Vec::with_capacity(hits.len());
    for hit in hits {
        let tuple = ctx
            .heap
            .fetch(hit.tid)
            .map_err(|e| ServerError::ddl(format!("vector ANN heap fetch: {e}")))?;
        let visibility = is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref());
        if !matches!(visibility, Visibility::Visible) {
            continue;
        }
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::ddl(format!("vector ANN heap decode: {e}")))?;
        let Some(Value::Vector(vector) | Value::HalfVec(vector)) = row.get(col_idx) else {
            return Err(ServerError::ddl(
                "vector ANN heap recheck: key column did not decode as vector or halfvec",
            ));
        };
        if vector.len() != probe.len() {
            return Err(ServerError::ddl(
                "vector ANN heap recheck: vector dimension mismatch",
            ));
        }
        let distance = metric_distance(metric, vector, probe);
        rows.push((distance, hit.tid, tuple.data));
    }
    rows.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    Ok(rows.into_iter().map(|(_, _, payload)| payload).collect())
}

fn metric_distance(metric: HnswMetric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        HnswMetric::L2 => ultrasql_vec::kernels::vector::l2_distance_f32(left, right),
        HnswMetric::Cosine => {
            ultrasql_vec::kernels::vector::cosine_distance_f32(left, right).unwrap_or(f32::INFINITY)
        }
        HnswMetric::NegativeInnerProduct => -ultrasql_vec::kernels::vector::dot_f32(left, right),
        HnswMetric::L1 => left.iter().zip(right).map(|(l, r)| (l - r).abs()).sum(),
    }
}

/// Try to lower `ORDER BY indexed_col [ASC|DESC]` over a bare table scan into
/// a directed B-tree scan.
///
/// This is intentionally narrow: one integer sort key, one base table, no
/// scan-level projection. Broader interesting-order planning belongs in the
/// optimizer, but this path makes backward index scan reachable through the
/// real wire lowerer today.
pub(super) fn try_ordered_index_scan(
    input: &LogicalPlan,
    keys: &[SortKey],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    try_ordered_index_scan_with_cap(input, keys, None, ctx)
}

/// Try to lower `LIMIT/OFFSET` over an index-ordered scan without
/// draining the entire index first.
///
/// The B-tree walk and heap fetch stop after enough MVCC-visible rows
/// have been collected to satisfy `offset + limit`. The executor still
/// receives a normal [`Limit`] over a presorted [`TopK`] so the row-cap
/// contract stays centralised in executor code.
pub(super) fn try_ordered_index_scan_limit(
    input: &LogicalPlan,
    limit: u64,
    offset: u64,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if limit == u64::MAX {
        return Ok(None);
    }
    let cap = usize::try_from(limit.saturating_add(offset)).unwrap_or(usize::MAX);
    match input {
        LogicalPlan::Sort {
            input: sort_input,
            keys,
        } => {
            let Some(scan) = try_ordered_index_scan_with_cap(sort_input, keys, Some(cap), ctx)?
            else {
                return Ok(None);
            };
            Ok(Some(limit_presorted_scan(scan, limit, offset, cap)))
        }
        LogicalPlan::Project {
            input: project_input,
            exprs,
            ..
        } => {
            let LogicalPlan::Sort {
                input: sort_input,
                keys,
            } = project_input.as_ref()
            else {
                return Ok(None);
            };
            let Some(scan) = try_ordered_index_scan_with_cap(sort_input, keys, Some(cap), ctx)?
            else {
                return Ok(None);
            };
            let limited = limit_presorted_scan(scan, limit, offset, cap);
            lower_project_columns(limited, exprs).map(Some)
        }
        _ => Ok(None),
    }
}

fn try_ordered_index_scan_with_cap(
    input: &LogicalPlan,
    keys: &[SortKey],
    cap: Option<usize>,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let [key] = keys else {
        return Ok(None);
    };
    let LogicalPlan::Scan {
        table, projection, ..
    } = input
    else {
        return Ok(None);
    };
    if projection.is_some() {
        return Ok(None);
    }
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let Some(col_idx) = column_idx_for_int_key(&key.expr) else {
        return Ok(None);
    };
    let Some(index_entry) =
        find_single_column_index(&ctx.catalog_snapshot, table_entry, col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(_widen) = key_type_for_btree(table_entry, col_idx) else {
        return Ok(None);
    };
    let range = IndexKeyRange {
        low: None,
        high: None,
    };
    let payloads = if let Some(cap) = cap {
        probe_index_ordered_limited(index_entry, range, key.asc, cap, ctx)?
    } else {
        probe_index_ordered(index_entry, range, key.asc, ctx)?
    };
    let codec = RowCodec::new(table_entry.schema.clone());
    Ok(Some(Box::new(IndexScan::new(payloads, codec))))
}

fn limit_presorted_scan(
    scan: Box<dyn Operator>,
    limit: u64,
    offset: u64,
    cap: usize,
) -> Box<dyn Operator> {
    let schema = scan.schema().clone();
    let top_k = Box::new(TopK::new_presorted(scan, schema, cap));
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    Box::new(Limit::with_offset(top_k, limit, offset))
}

/// Try to lower `Project(Filter(Scan), key_column)` into an index-only scan.
///
/// This path is intentionally narrow and correctness-first:
/// - one indexed Int32/Int64 key column,
/// - projected columns must all be that same covered key,
/// - predicate must be a normal indexable B-tree range,
/// - every candidate tuple's heap page must be marked all-visible in VM.
///
/// If any condition misses, caller falls back to the existing
/// `Project(IndexScan)` or `Project(Filter(SeqScan))` path. We do not fetch
/// heap rows inside this operator; VM proof is required before choosing it.
pub(super) fn try_index_only_scan(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if exprs.is_empty() {
        return Ok(None);
    }
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
        return Ok(None);
    };
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let Some((predicate_col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };
    let Some(index_entry) =
        find_single_column_index(&ctx.catalog_snapshot, table_entry, predicate_col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(_widen) = key_type_for_btree(table_entry, predicate_col_idx) else {
        return Ok(None);
    };

    let mut output_fields: Vec<Field> = Vec::with_capacity(exprs.len());
    for (expr, name) in exprs {
        let ScalarExpr::Column {
            index, data_type, ..
        } = expr
        else {
            return Ok(None);
        };
        if *index != predicate_col_idx {
            return Ok(None);
        }
        if !matches!(data_type, DataType::Int32 | DataType::Int64) {
            return Ok(None);
        }
        output_fields.push(Field::nullable(name.clone(), data_type.clone()));
    }
    let output_schema = ultrasql_core::Schema::new(output_fields).map_err(|e| {
        ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
            "index-only projection schema: {e}"
        )))
    })?;

    let entries = probe_index_entries_ordered(index_entry, range, true, ctx)?;
    let tuples_read = usize_to_u64_saturating(entries.len());
    let table_rel = RelationId(table_entry.oid);
    if entries
        .iter()
        .any(|(_, tid)| !ctx.vm.is_all_visible(table_rel, tid.page.block))
    {
        return Ok(None);
    }

    let projected_rows: Option<Vec<Vec<Value>>> = entries
        .into_iter()
        .map(|(key, _tid)| {
            exprs
                .iter()
                .map(|(expr, _)| key_value_for_expr(key, expr))
                .collect()
        })
        .collect();
    let Some(projected_rows) = projected_rows else {
        return Ok(None);
    };
    ctx.workload_recorder
        .record_index_usage(index_entry.oid.raw(), tuples_read, 0);
    let vm = vec![true; projected_rows.len()];
    Ok(Some(Box::new(IndexOnlyScan::new(
        projected_rows,
        vm,
        Vec::new(),
        output_schema,
    ))))
}

/// Counters reported by the late-materialization prototype.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LateMaterializationSummary {
    /// TIDs emitted by the index probe before visibility checks.
    pub(crate) candidate_tids: u64,
    /// MVCC-visible heap rows fetched by the payload phase.
    pub(crate) fetched_rows: u64,
    /// Candidate TIDs skipped because the heap tuple was not visible.
    pub(crate) skipped_invisible: u64,
    /// Human-readable EXPLAIN note.
    pub(crate) note: String,
}

impl LateMaterializationSummary {
    fn not_applicable(reason: impl Into<String>) -> Self {
        Self {
            note: reason.into(),
            ..Self::default()
        }
    }
}

/// Try to lower `Project(Filter(Scan), payload_cols)` into a two-phase
/// B-tree TID probe followed by deferred heap payload fetch.
pub(super) fn try_late_materialization_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    output_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(shape) = late_materialization_shape(input, exprs, ctx)? else {
        return Ok(None);
    };
    let entries =
        probe_index_entries_ordered(shape.index_entry, shape.range, shape.ascending, ctx)?;
    let tids = entries.into_iter().map(|(_, tid)| tid).collect();
    let codec = RowCodec::new(shape.table_entry.schema.clone());
    Ok(Some(Box::new(LateMaterializeScan::new(
        tids,
        codec,
        shape.projected_cols,
        output_schema.clone(),
        Arc::clone(&ctx.heap),
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
    ))))
}

/// Return the same counters printed by `EXPLAIN ANALYZE`.
pub(crate) fn late_materialization_summary_for_plan(
    plan: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<LateMaterializationSummary, ServerError> {
    let Some((input, exprs, visible_cap)) = late_materialization_project_shape(plan) else {
        return Ok(LateMaterializationSummary::not_applicable(
            "not applicable (no Project(Filter(Scan)) shape)",
        ));
    };
    let Some(shape) = late_materialization_shape(input, exprs, ctx)? else {
        return Ok(LateMaterializationSummary::not_applicable(
            "not selected (shape, index, or projection not eligible)",
        ));
    };
    let entries =
        probe_index_entries_ordered(shape.index_entry, shape.range, shape.ascending, ctx)?;
    let mut fetched_rows = 0_u64;
    let mut skipped_invisible = 0_u64;
    let mut candidate_tids = 0_u64;
    for (_, tid) in &entries {
        candidate_tids = candidate_tids.saturating_add(1);
        if fetch_visible_index_payload(*tid, ctx)?.is_some() {
            fetched_rows = fetched_rows.saturating_add(1);
            if visible_cap.is_some_and(|cap| fetched_rows >= cap) {
                break;
            }
        } else {
            skipped_invisible = skipped_invisible.saturating_add(1);
        }
    }
    Ok(LateMaterializationSummary {
        candidate_tids,
        fetched_rows,
        skipped_invisible,
        note: format!(
            "selected {} on {}: candidates={} fetched={} skipped={} via index TID probe then deferred heap payload fetch",
            shape.index_entry.name,
            shape.table_name,
            candidate_tids,
            fetched_rows,
            skipped_invisible
        ),
    })
}

fn late_materialization_project_shape(
    plan: &LogicalPlan,
) -> Option<LateMaterializationProjectShape<'_>> {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => Some((input, exprs, None)),
        LogicalPlan::Limit { input, n, offset } => {
            let LogicalPlan::Project {
                input: project_input,
                exprs,
                ..
            } = input.as_ref()
            else {
                return None;
            };
            let visible_cap = n.checked_add(*offset).or(Some(u64::MAX));
            Some((project_input, exprs, visible_cap))
        }
        _ => None,
    }
}

struct LateMaterializationShape<'a> {
    table_name: &'a str,
    table_entry: &'a TableEntry,
    index_entry: &'a IndexEntry,
    range: IndexKeyRange,
    projected_cols: Vec<usize>,
    ascending: bool,
}

fn late_materialization_shape<'a>(
    input: &'a LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    ctx: &'a LowerCtx<'_>,
) -> Result<Option<LateMaterializationShape<'a>>, ServerError> {
    if exprs.is_empty() {
        return Ok(None);
    }
    let (input, sort_keys) = match input {
        LogicalPlan::Sort { input, keys } => (input.as_ref(), Some(keys.as_slice())),
        other => (other, None),
    };
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan { table, .. } = filter_input.as_ref() else {
        return Ok(None);
    };
    let Some(table_entry) = ctx.catalog_snapshot.tables.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let Some((predicate_col_idx, range)) = match_indexable_predicate(predicate) else {
        return Ok(None);
    };
    let Some(index_entry) =
        find_single_column_index(&ctx.catalog_snapshot, table_entry, predicate_col_idx, ctx)
    else {
        return Ok(None);
    };
    let Some(_widen) = key_type_for_btree(table_entry, predicate_col_idx) else {
        return Ok(None);
    };
    let Some(projected_cols) = simple_projected_columns(exprs, table_entry.schema.len()) else {
        return Ok(None);
    };
    if projected_cols.iter().all(|col| *col == predicate_col_idx) {
        return Ok(None);
    }
    if !late_materialization_is_worthwhile(table_entry.schema.len(), projected_cols.len()) {
        return Ok(None);
    }
    let ascending = if let Some(keys) = sort_keys {
        let Some(ascending) = sort_keys_preserve_index_order(keys, predicate_col_idx) else {
            return Ok(None);
        };
        ascending
    } else {
        true
    };
    Ok(Some(LateMaterializationShape {
        table_name: table.as_str(),
        table_entry,
        index_entry,
        range,
        projected_cols,
        ascending,
    }))
}

fn late_materialization_is_worthwhile(table_width: usize, projected_width: usize) -> bool {
    table_width >= LATE_MATERIALIZATION_MIN_TABLE_WIDTH
        && projected_width <= LATE_MATERIALIZATION_MAX_PROJECTED_COLUMNS
        && projected_width.saturating_mul(4) <= table_width
}

fn sort_keys_preserve_index_order(keys: &[SortKey], predicate_col_idx: usize) -> Option<bool> {
    let [key] = keys else {
        return None;
    };
    let ScalarExpr::Column { index, .. } = &key.expr else {
        return None;
    };
    (*index == predicate_col_idx).then_some(key.asc)
}

fn simple_projected_columns(
    exprs: &[(ScalarExpr, String)],
    table_width: usize,
) -> Option<Vec<usize>> {
    let mut projected_cols = Vec::with_capacity(exprs.len());
    for (expr, _) in exprs {
        let ScalarExpr::Column { index, .. } = expr else {
            return None;
        };
        if *index >= table_width {
            return None;
        }
        projected_cols.push(*index);
    }
    Some(projected_cols)
}

struct LateMaterializeScan {
    tids: std::vec::IntoIter<TupleId>,
    codec: RowCodec,
    projection: Vec<usize>,
    output_schema: Schema,
    heap: Arc<HeapAccess<BlankPageLoader>>,
    snapshot: ultrasql_mvcc::Snapshot,
    oracle: Arc<TransactionManager>,
    eof: bool,
    candidate_tids: u64,
    fetched_rows: u64,
    skipped_invisible: u64,
}

impl std::fmt::Debug for LateMaterializeScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LateMaterializeScan")
            .field("remaining_tids", &self.tids.len())
            .field("projection", &self.projection)
            .field("candidate_tids", &self.candidate_tids)
            .field("fetched_rows", &self.fetched_rows)
            .field("skipped_invisible", &self.skipped_invisible)
            .finish()
    }
}

impl LateMaterializeScan {
    fn new(
        tids: Vec<TupleId>,
        codec: RowCodec,
        projection: Vec<usize>,
        output_schema: Schema,
        heap: Arc<HeapAccess<BlankPageLoader>>,
        snapshot: ultrasql_mvcc::Snapshot,
        oracle: Arc<TransactionManager>,
    ) -> Self {
        let candidate_tids = u64::try_from(tids.len()).unwrap_or(u64::MAX);
        Self {
            tids: tids.into_iter(),
            codec,
            projection,
            output_schema,
            heap,
            snapshot,
            oracle,
            eof: false,
            candidate_tids,
            fetched_rows: 0,
            skipped_invisible: 0,
        }
    }
}

impl Operator for LateMaterializeScan {
    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ultrasql_executor::ExecError> {
        if self.eof {
            return Ok(None);
        }
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(4096);
        while rows.len() < 4096 {
            let Some(tid) = self.tids.next() else {
                self.eof = true;
                break;
            };
            let Some(payload) = self.fetch_visible_payload(tid)? else {
                self.skipped_invisible = self.skipped_invisible.saturating_add(1);
                continue;
            };
            let row = self
                .codec
                .decode_projected(&payload, &self.projection)
                .map_err(|e| ultrasql_executor::ExecError::TypeMismatch(e.to_string()))?;
            self.fetched_rows = self.fetched_rows.saturating_add(1);
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        ultrasql_executor::build_batch(&rows, &self.output_schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        Some(self.tids.len())
    }
}

impl LateMaterializeScan {
    fn fetch_visible_payload(
        &self,
        tid: TupleId,
    ) -> Result<Option<Vec<u8>>, ultrasql_executor::ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = self.heap.fetch(current).map_err(|_| {
                ultrasql_executor::ExecError::Internal("LateMaterializeScan heap fetch failed")
            })?;
            let visibility = is_visible(&tuple.header, &self.snapshot, self.oracle.as_ref());
            match visibility {
                Visibility::Visible => return Ok(Some(tuple.data)),
                Visibility::Invisible | Visibility::DeletedByOwn => {
                    if let Some(next) = updated_ctid_target(&tuple.header, current) {
                        current = next;
                        continue;
                    }
                    return Ok(None);
                }
                Visibility::VisiblePreImage => return Ok(None),
            }
        }
        Err(ultrasql_executor::ExecError::Internal(
            "LateMaterializeScan update ctid chain exceeded 64 hops",
        ))
    }
}

fn updated_ctid_target(header: &TupleHeader, current: TupleId) -> Option<TupleId> {
    if header.ctid == current {
        return None;
    }
    let redirects = header.infomask.contains(InfoMask::UPDATED)
        || header.infomask.contains(InfoMask::HOT_UPDATED);
    redirects.then_some(header.ctid)
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
        BinaryOp::Lt => raw_lit
            .checked_sub(1)
            .map_or_else(IndexKeyRange::empty, |high| IndexKeyRange {
                low: None,
                high: Some(high),
            }),
        BinaryOp::LtEq => IndexKeyRange {
            low: None,
            high: Some(raw_lit),
        },
        BinaryOp::Gt => raw_lit
            .checked_add(1)
            .map_or_else(IndexKeyRange::empty, |low| IndexKeyRange {
                low: Some(low),
                high: None,
            }),
        BinaryOp::GtEq => IndexKeyRange {
            low: Some(raw_lit),
            high: None,
        },
        _ => return None,
    };
    Some((col_idx, range))
}

fn match_hash_equality_predicate(expr: &ScalarExpr) -> Option<(usize, Value)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. })
        | (ScalarExpr::Literal { value, .. }, ScalarExpr::Column { index, .. }) => {
            Some((*index, value.clone()))
        }
        _ => None,
    }
}

/// Read the column index from a [`ScalarExpr::Column`] whose data type
/// is represented directly in the index `i64` key space.
const fn column_idx_for_int_key(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    match data_type {
        DataType::Bool
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp
        | DataType::TimestampTz => Some(*index),
        _ => None,
    }
}

/// Lift an integer-typed literal to `i64`. `Int32` is sign-extended
/// via the lossless `i64::from(i32)` widening conversion. Returns
/// `None` for non-integer literals (text, float, NULL, …).
pub(crate) fn literal_as_i64(expr: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Literal { value, .. } = expr else {
        return None;
    };
    match value {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Int16(v) => Some(i64::from(*v)),
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        Value::Timestamp(v) | Value::TimestampTz(v) => Some(*v),
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
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Btree
    })
}

fn find_single_column_hash_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Hash
    })
}

fn find_single_column_brin_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Brin
    })
}

fn brin_summary(
    ctx: &LowerCtx<'_>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> Option<Arc<BrinIndex>> {
    let constraints = ctx.table_constraints.get(&table_oid)?;
    constraints.indexes.get(&index_oid)?.brin.clone()
}

fn index_method(
    ctx: &LowerCtx<'_>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> LogicalIndexMethod {
    ctx.table_constraints
        .get(&table_oid)
        .map_or(LogicalIndexMethod::Btree, |constraints| {
            constraints
                .indexes
                .get(&index_oid)
                .map_or(LogicalIndexMethod::Btree, |metadata| metadata.method)
        })
}

/// Confirm the keyed column has a type stored directly in the `i64`
/// key space. Returns `None` for types whose index encoding needs a
/// transform not represented by [`literal_as_i64`].
///
/// Mirrors the check in `Server::execute_create_index` — keep the two
/// in sync, or a `CREATE INDEX` that succeeds will produce an index a

pub(super) fn key_type_for_btree(table_entry: &TableEntry, col_idx: usize) -> Option<bool> {
    let field = table_entry.schema.field(col_idx)?;
    match field.data_type {
        DataType::Bool | DataType::Int16 | DataType::Timestamp | DataType::TimestampTz => {
            Some(true)
        }
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
    probe_index_ordered(index_entry, range, true, ctx)
}

fn probe_index_ordered(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    if range.is_empty() {
        ctx.workload_recorder
            .record_index_usage(index_entry.oid.raw(), 0, 0);
        return Ok(Vec::new());
    }
    let entries = probe_index_entries_ordered(index_entry, range, ascending, ctx)?;
    let tuples_read = usize_to_u64_saturating(entries.len());
    let mut payloads = fetch_visible_index_payloads(entries.into_iter().map(|(_, tid)| tid), ctx)?;
    if payloads.is_empty()
        && let (Some(lo), Some(hi)) = (range.low, range.high)
        && lo == hi
    {
        let fallback_limit = if index_entry.is_unique { 1 } else { usize::MAX };
        payloads = fallback_point_payloads(index_entry, lo, fallback_limit, ctx)?;
    }
    ctx.workload_recorder.record_index_usage(
        index_entry.oid.raw(),
        tuples_read,
        usize_to_u64_saturating(payloads.len()),
    );
    Ok(payloads)
}

fn probe_index_ordered_limited(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    if range.is_empty() {
        ctx.workload_recorder
            .record_index_usage(index_entry.oid.raw(), 0, 0);
        return Ok(Vec::new());
    }
    let index_rel = RelationId::new(index_entry.oid.raw());
    let pool = ctx.heap.buffer_pool();
    let btree: BTree<BlankPageLoader> =
        BTree::open(Arc::clone(pool), index_rel, index_entry.root_block);
    let mut payloads = Vec::new();
    let mut tuples_read = 0_u64;

    match (range.low, range.high, ascending) {
        (Some(lo), Some(hi), true) if lo == hi => {
            if index_entry.is_unique {
                if let Some(tid) = btree
                    .lookup::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    tuples_read = tuples_read.saturating_add(1);
                    push_visible_index_payload(&mut payloads, tid, ctx, limit)?;
                }
            } else {
                for tid in btree
                    .lookup_all::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    tuples_read = tuples_read.saturating_add(1);
                    if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                        break;
                    }
                }
            }
            if payloads.is_empty() {
                payloads = fallback_point_payloads(index_entry, lo, limit, ctx)?;
            }
        }
        (low, high, true) => {
            let start = low.unwrap_or(i64::MIN);
            let end_exclusive = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (_key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                tuples_read = tuples_read.saturating_add(1);
                if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                    break;
                }
            }
        }
        (low, high, false) => {
            let start = high.unwrap_or(i64::MAX);
            let end = low;
            for entry in btree
                .backward_scan::<i64>(start, end)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?
            {
                let (_key, tid) = entry
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?;
                tuples_read = tuples_read.saturating_add(1);
                if push_visible_index_payload(&mut payloads, tid, ctx, limit)? {
                    break;
                }
            }
        }
    }
    ctx.workload_recorder.record_index_usage(
        index_entry.oid.raw(),
        tuples_read,
        usize_to_u64_saturating(payloads.len()),
    );
    Ok(payloads)
}

fn fallback_point_payloads(
    index_entry: &IndexEntry,
    key: i64,
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let Some(&attnum) = index_entry.columns.first() else {
        return Ok(Vec::new());
    };
    let Some(table_entry) = ctx
        .catalog_snapshot
        .tables
        .values()
        .find(|entry| entry.oid == index_entry.table_oid)
    else {
        return Ok(Vec::new());
    };
    let col_idx = usize::from(attnum);
    if col_idx >= table_entry.schema.len() {
        return Ok(Vec::new());
    }
    let codec = RowCodec::new(table_entry.schema.clone());
    let rel = RelationId(table_entry.oid);
    let block_count = ctx.heap.block_count(rel).max(table_entry.n_blocks);
    let mut walker =
        ctx.heap
            .scan_visible_walker(rel, block_count, &ctx.snapshot, ctx.oracle.as_ref());
    let mut payloads = Vec::new();
    while let Some((_tid, _header, payload)) = walker
        .try_next()
        .map_err(|e| ServerError::ddl(format!("IndexScan fallback heap scan: {e}")))?
    {
        let row = codec
            .decode(payload)
            .map_err(|e| ServerError::ddl(format!("IndexScan fallback row decode: {e}")))?;
        if row
            .get(col_idx)
            .is_some_and(|value| value_matches_i64(value, key))
        {
            payloads.push(payload.to_vec());
            if payloads.len() >= limit {
                break;
            }
        }
    }
    Ok(payloads)
}

fn value_matches_i64(value: &Value, key: i64) -> bool {
    match value {
        Value::Int16(v) => i64::from(*v) == key,
        Value::Int32(v) => i64::from(*v) == key,
        Value::Int64(v) => *v == key,
        _ => false,
    }
}

pub(super) fn probe_index_entries_ordered(
    index_entry: &IndexEntry,
    range: IndexKeyRange,
    ascending: bool,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<(i64, TupleId)>, ServerError> {
    if range.is_empty() {
        return Ok(Vec::new());
    }
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
    let mut entries_out: Vec<(i64, TupleId)> = Vec::new();
    match (range.low, range.high, ascending) {
        (Some(lo), Some(hi), true) if lo == hi => {
            if index_entry.is_unique {
                if let Some(tid) = btree
                    .lookup::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    entries_out.push((lo, tid));
                }
            } else {
                for tid in btree
                    .lookup_all::<i64>(lo)
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree lookup: {e}")))?
                {
                    entries_out.push((lo, tid));
                }
            }
        }
        (low, high, true) => {
            // Walk the half-open `[start, end_exclusive)`. `start =
            // low.unwrap_or(i64::MIN)` and `end_exclusive =
            // high.map(|h| h.checked_add(1))` — when the +1 overflows we
            // pass `None` to mean "scan to the end of the leaf chain".
            let start = low.unwrap_or(i64::MIN);
            // `i64::MAX + 1` overflows to `None`, which `range_scan`
            // treats as "unbounded above" — exactly the contract we want.
            let end_exclusive: Option<i64> = high.and_then(|h| h.checked_add(1));
            for entry in btree.range_scan::<i64>(start, end_exclusive) {
                let (key, tid) =
                    entry.map_err(|e| ServerError::ddl(format!("IndexScan btree scan: {e}")))?;
                entries_out.push((key, tid));
            }
        }
        (low, high, false) => {
            let start = high.unwrap_or(i64::MAX);
            let end = low;
            for entry in btree
                .backward_scan::<i64>(start, end)
                .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?
            {
                let (key, tid) = entry
                    .map_err(|e| ServerError::ddl(format!("IndexScan btree backward scan: {e}")))?;
                entries_out.push((key, tid));
            }
        }
    }
    Ok(entries_out)
}

fn fetch_visible_index_payloads<I>(tids: I, ctx: &LowerCtx<'_>) -> Result<Vec<Vec<u8>>, ServerError>
where
    I: IntoIterator<Item = TupleId>,
{
    // Fetch the heap tuples in B-tree order and apply MVCC visibility
    // inline. An index entry whose heap tuple is invisible to the
    // statement's snapshot is silently dropped — the same outcome a
    // SeqScan would deliver. We use [`HeapAccess::fetch`] (no
    // visibility check) plus an explicit `is_visible` call rather than
    // chaining through `scan_visible` because the latter walks a
    // block-by-block iterator we cannot project onto an arbitrary
    // TupleId list.
    let iter = tids.into_iter();
    let (lower, _) = iter.size_hint();
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(lower);
    for tid in iter {
        if let Some(payload) = fetch_visible_index_payload(tid, ctx)? {
            payloads.push(payload);
        }
    }
    Ok(payloads)
}

fn push_visible_index_payload(
    payloads: &mut Vec<Vec<u8>>,
    tid: TupleId,
    ctx: &LowerCtx<'_>,
    limit: usize,
) -> Result<bool, ServerError> {
    if let Some(payload) = fetch_visible_index_payload(tid, ctx)? {
        payloads.push(payload);
    }
    Ok(payloads.len() >= limit)
}

fn fetch_visible_index_payload(
    tid: TupleId,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Vec<u8>>, ServerError> {
    let mut current = tid;
    for _ in 0..64 {
        let tuple = ctx
            .heap
            .fetch(current)
            .map_err(|e| ServerError::ddl(format!("IndexScan heap fetch: {e}")))?;
        let visibility = is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref());
        match visibility {
            Visibility::Visible => return Ok(Some(tuple.data)),
            Visibility::Invisible | Visibility::DeletedByOwn => {
                if let Some(next) = updated_ctid_target(&tuple.header, current) {
                    current = next;
                    continue;
                }
                return Ok(None);
            }
            Visibility::VisiblePreImage => return Ok(None),
        }
    }
    Err(ServerError::ddl(
        "IndexScan heap fetch: update ctid chain exceeded 64 hops",
    ))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn scan_brin_candidate_ranges(
    table_entry: &TableEntry,
    ranges: &[(u32, u32)],
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let table_rel = RelationId(table_entry.oid);
    let block_count = ctx.heap.block_count(table_rel).max(table_entry.n_blocks);
    let ranges = normalize_brin_ranges(ranges, block_count);
    let mut payloads = Vec::new();
    for (start_block, end_block_inclusive) in ranges {
        let end_exclusive = end_block_inclusive.saturating_add(1);
        let mut walker = ctx.heap.scan_visible_walker_range_with_vm(
            table_rel,
            start_block,
            end_exclusive,
            &ctx.snapshot,
            ctx.oracle.as_ref(),
            ctx.vm.as_ref(),
        );
        while let Some((_tid, _header, payload)) = walker
            .try_next()
            .map_err(|e| ServerError::ddl(format!("BRIN heap range scan: {e}")))?
        {
            payloads.push(payload.to_vec());
        }
    }
    Ok(payloads)
}

fn normalize_brin_ranges(ranges: &[(u32, u32)], block_count: u32) -> Vec<(u32, u32)> {
    if block_count == 0 {
        return Vec::new();
    }
    let last_block = block_count - 1;
    let mut ranges: Vec<(u32, u32)> = ranges
        .iter()
        .filter_map(|(start, end)| {
            if *start > last_block {
                return None;
            }
            let end = (*end).min(last_block);
            if *start > end {
                return None;
            }
            Some((*start, end))
        })
        .collect();
    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, current_end)) = merged.last_mut()
            && start <= current_end.saturating_add(1)
        {
            *current_end = (*current_end).max(end);
            continue;
        }
        merged.push((start, end));
    }
    merged
}

fn key_value_for_expr(key: i64, expr: &ScalarExpr) -> Option<Value> {
    let ScalarExpr::Column { data_type, .. } = expr else {
        return None;
    };
    match data_type {
        DataType::Int32 => i32::try_from(key).ok().map(Value::Int32),
        DataType::Int64 => Some(Value::Int64(key)),
        _ => None,
    }
}
