//! Vector ANN lowering: top-k and filtered top-k `ORDER BY
//! vector_distance LIMIT k` through HNSW / IVFFlat runtime indexes.

use std::sync::Arc;

use num_traits::ToPrimitive;

use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, TupleId, Value};
use ultrasql_executor::{Eval, IndexScan, Operator, RowCodec};
use ultrasql_mvcc::{Visibility, is_visible};
use ultrasql_planner::{BinaryOp, LogicalIndexMethod, LogicalPlan, ScalarExpr, SortKey};
use ultrasql_storage::access_method::{HnswMetric, PageBackedHnswIndex, PageBackedIvfFlatIndex};

use crate::error::ServerError;

use super::LowerCtx;
use super::modify::lower_project_columns;

/// Try to lower `ORDER BY vector_distance LIMIT k` through an available vector
/// ANN runtime index.
///
/// Missing or invalid ANN metadata returns `Ok(None)`, letting the caller use
/// exact `Sort + Limit`. This is the correctness fallback for restarts, DML
/// invalidation, unsupported metrics, and non-top-k shapes.
pub(crate) fn try_hnsw_top_k_limit(
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
    // Over-fetch candidates beyond `limit`: an MVCC-dead tuple (aborted, or
    // superseded by an UPDATE) can sit nearer the probe than a live row and,
    // taking exactly `limit` candidates, would push a real answer out of the
    // result only to be dropped by the visibility recheck — leaving fewer than
    // `limit` live rows. A wider candidate set absorbs that mortality; the exact
    // fallback below covers the rest.
    let hits = if let Some(hnsw) = find_hnsw_index(ctx, table_entry, col_idx, metric) {
        // A per-session `hnsw.ef_search` (pgvector-compatible) overrides the
        // auto-sized budget so users can trade latency for recall; it must still
        // be at least `limit` to return k results.
        let want = match session_ef_search(ctx) {
            Some(ef) => ef.max(limit),
            None => limit
                .saturating_mul(ANN_TOPK_OVERFETCH)
                .max(ANN_TOPK_MIN_EF)
                .max(hnsw.ef_search()),
        };
        hnsw.search_with_ef(&probe, want, want)
            .map_err(|e| ServerError::ddl(format!("HNSW search: {e}")))?
            .into_iter()
            .map(|hit| VectorSearchHit { tid: hit.tid })
            .collect::<Vec<_>>()
    } else if let Some(ivfflat) = find_ivfflat_index(ctx, table_entry, col_idx, metric) {
        let want = limit
            .saturating_mul(ANN_TOPK_OVERFETCH)
            .max(ANN_TOPK_MIN_EF);
        ivfflat
            .search(&probe, want)
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
    let mut payloads =
        fetch_vector_visible_payloads(&hits, table_entry, col_idx, metric, &probe, ctx)?;
    // If even the over-fetch cannot deliver `limit` live rows, the ANN answer is
    // not trustworthy at this `k` — decline to lower and let the exact sort path
    // (recall 1.0) handle it.
    if payloads.len() < limit {
        return Ok(None);
    }
    payloads.truncate(limit);
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

// Filtered-ANN crossover tuning. The over-fetch budget is sized to the filter:
// to surface `k` survivors when a fraction `s` of rows pass, explore roughly
// `k / s` candidates with a safety multiplier. Below the floor the search is
// effectively exact (small ef); above the ceiling a very selective filter is
// better served by the exact filter+sort path.
const FILTERED_ANN_OVERFETCH: usize = 4;
const FILTERED_ANN_MIN_EF: usize = 64;
const FILTERED_ANN_MAX_EF: usize = 8192;

// Unfiltered top-k ANN over-fetches candidates beyond `k` so MVCC-dead tuples
// (aborted, or superseded by an UPDATE) sitting near the probe cannot occupy a
// result slot only to be dropped by the visibility recheck — which would starve
// the answer below `k` live rows. This mirrors the filtered path's over-fetch,
// but it is sized for tuple mortality rather than filter selectivity.
const ANN_TOPK_OVERFETCH: usize = 4;
const ANN_TOPK_MIN_EF: usize = 64;

/// Read a per-session `hnsw.ef_search` override (pgvector-compatible). Returns
/// `None` when unset or non-positive, in which case the lowering auto-sizes the
/// exploration budget.
fn session_ef_search(ctx: &LowerCtx<'_>) -> Option<usize> {
    ctx.session_settings
        .get("hnsw.ef_search")
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|&ef| ef > 0)
}

/// Try to lower `WHERE <predicate> ORDER BY vector_distance LIMIT k`
/// (`Sort(Filter(Scan))`, optionally under a `Project`) through an HNSW index
/// with a selectivity-aware crossover.
///
/// Loose filters use ANN over-fetch + post-filter (fast, no full scan); very
/// selective filters, or cases where too few ANN candidates survive the filter,
/// return `Ok(None)` so the caller uses the exact filter+sort path (recall 1.0).
/// The fallback is what keeps recall from collapsing at any selectivity.
pub(crate) fn try_hnsw_filtered_top_k_limit(
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
        } => try_hnsw_filtered_sorted(sort_input, keys, limit, ctx),
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
            let Some(scan) = try_hnsw_filtered_sorted(sort_input, keys, limit, ctx)? else {
                return Ok(None);
            };
            lower_project_columns(scan, exprs).map(Some)
        }
        _ => Ok(None),
    }
}

fn try_hnsw_filtered_sorted(
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
    let LogicalPlan::Filter {
        input: filter_input,
        predicate,
    } = sort_input
    else {
        return Ok(None);
    };
    let LogicalPlan::Scan {
        table, projection, ..
    } = filter_input.as_ref()
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
    // Size the exploration budget to the estimated filter selectivity.
    let selectivity = estimate_filter_selectivity(predicate).clamp(0.0001, 1.0);
    let overfetch = limit.saturating_mul(FILTERED_ANN_OVERFETCH);
    let want = overfetch.to_f64().unwrap_or(f64::MAX) / selectivity;
    let max_ef = FILTERED_ANN_MAX_EF.to_f64().unwrap_or(f64::MAX);
    // Very selective filter: ANN would have to explore beyond the ceiling to
    // surface k survivors, so exact filter+sort is both correct and faster.
    if want > max_ef {
        return Ok(None);
    }
    let min_ef = FILTERED_ANN_MIN_EF.to_f64().unwrap_or(0.0);
    // `clamped` is finite within [MIN_EF, MAX_EF] (small positive usizes), so the
    // checked `to_usize` cannot fail; fall back to the ceiling if it ever does.
    let selectivity_ef = want
        .clamp(min_ef, max_ef)
        .ceil()
        .to_usize()
        .unwrap_or(FILTERED_ANN_MAX_EF);
    // A per-session `ef_search` raises the floor so users can boost recall on
    // filtered queries too, but never below what selectivity demands.
    let ef = selectivity_ef.max(session_ef_search(ctx).unwrap_or(0));

    // HNSW scales its `ef` exploration budget; IVFFlat scales the number of
    // inverted lists it probes (both inversely with selectivity). Other index
    // kinds fall back to the exact filter+sort path.
    let hits = if let Some(hnsw) = find_hnsw_index(ctx, table_entry, col_idx, metric) {
        hnsw.search_with_ef(&probe, ef, ef)
            .map_err(|e| ServerError::ddl(format!("HNSW filtered search: {e}")))?
            .into_iter()
            .map(|hit| VectorSearchHit { tid: hit.tid })
            .collect::<Vec<_>>()
    } else if let Some(ivfflat) = find_ivfflat_index(ctx, table_entry, col_idx, metric) {
        // Probe more lists for a more selective filter, capped at the
        // materialized list count (probing every list is exact); `ef`
        // over-fetches the result count. The cap is the only bound that matters,
        // so clamp with `min(lists)` — never `clamp(configured, lists)`, which
        // would panic when the configured probes exceed the materialized lists
        // (e.g. `WITH (lists = 4, probes = 8)`, or fewer rows than lists).
        let configured = ivfflat.probes().max(1);
        let lists = ivfflat.list_count().max(1);
        let probes = (configured.to_f64().unwrap_or(f64::MAX) / selectivity)
            .ceil()
            .to_usize()
            .unwrap_or(lists)
            .min(lists)
            .max(1);
        ivfflat
            .search_with_probes(&probe, ef, probes)
            .map_err(|e| ServerError::ddl(format!("IVFFlat filtered search: {e}")))?
            .into_iter()
            .map(|hit| VectorSearchHit { tid: hit.tid })
            .collect::<Vec<_>>()
    } else {
        return Ok(None);
    };
    if hits.is_empty() {
        return Ok(None);
    }
    let predicate_eval = Eval::new(predicate.clone());
    let payloads = fetch_vector_visible_filtered_payloads(
        &hits,
        table_entry,
        col_idx,
        metric,
        &probe,
        &predicate_eval,
        limit,
        ctx,
    )?;
    // Too few survived the filter (the estimate was optimistic): fall back to
    // exact so recall cannot collapse.
    if payloads.len() < limit {
        return Ok(None);
    }
    let codec = RowCodec::new(table_entry.schema.clone());
    Ok(Some(Box::new(IndexScan::new(payloads, codec))))
}

/// Self-contained predicate selectivity heuristic for the filtered-ANN
/// crossover, mirroring the optimizer's stats-free formulas. The estimate only
/// sizes the over-fetch budget; correctness is guaranteed by the exact
/// fallback when too few candidates survive.
fn estimate_filter_selectivity(pred: &ScalarExpr) -> f64 {
    const EQ_SEL: f64 = 0.1;
    const RANGE_SEL: f64 = 0.33;
    const DEFAULT_SEL: f64 = 0.5;
    match pred {
        ScalarExpr::Binary {
            op, left, right, ..
        } => match op {
            BinaryOp::Eq => EQ_SEL,
            BinaryOp::NotEq => 1.0 - EQ_SEL,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => RANGE_SEL,
            BinaryOp::And => estimate_filter_selectivity(left) * estimate_filter_selectivity(right),
            BinaryOp::Or => {
                let l = estimate_filter_selectivity(left);
                let r = estimate_filter_selectivity(right);
                1.0 - (1.0 - l) * (1.0 - r)
            }
            BinaryOp::JsonContains
            | BinaryOp::JsonContained
            | BinaryOp::JsonHasKey
            | BinaryOp::JsonHasAnyKey
            | BinaryOp::JsonHasAllKeys => EQ_SEL,
            _ => DEFAULT_SEL,
        },
        _ => DEFAULT_SEL,
    }
}

#[allow(clippy::too_many_arguments)]
fn fetch_vector_visible_filtered_payloads(
    hits: &[VectorSearchHit],
    table_entry: &TableEntry,
    col_idx: usize,
    metric: HnswMetric,
    probe: &[f32],
    predicate: &Eval,
    limit: usize,
    ctx: &LowerCtx<'_>,
) -> Result<Vec<Vec<u8>>, ServerError> {
    let codec = RowCodec::new(table_entry.schema.clone());
    let mut rows: Vec<(f32, TupleId, Vec<u8>)> = Vec::new();
    for hit in hits {
        let tuple = ctx
            .heap
            .fetch(hit.tid)
            .map_err(|e| ServerError::ddl(format!("filtered ANN heap fetch: {e}")))?;
        if !matches!(
            is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref()),
            Visibility::Visible
        ) {
            continue;
        }
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::ddl(format!("filtered ANN heap decode: {e}")))?;
        match predicate.eval(&row) {
            Ok(Value::Bool(true)) => {}
            Ok(Value::Bool(false) | Value::Null) => continue,
            Ok(other) => {
                return Err(ServerError::ddl(format!(
                    "filtered ANN predicate must be boolean, got {:?}",
                    other.data_type()
                )));
            }
            Err(e) => {
                return Err(ServerError::ddl(format!(
                    "filtered ANN predicate eval: {e}"
                )));
            }
        }
        let Some(Value::Vector(vector) | Value::HalfVec(vector)) = row.get(col_idx) else {
            return Err(ServerError::ddl(
                "filtered ANN recheck: key column did not decode as vector or halfvec",
            ));
        };
        if vector.len() != probe.len() {
            return Err(ServerError::ddl(
                "filtered ANN recheck: vector dimension mismatch",
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
    rows.truncate(limit);
    Ok(rows.into_iter().map(|(_, _, payload)| payload).collect())
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
