//! Directed (ASC/DESC) B-tree index scans over `ORDER BY indexed_col`,
//! optionally with a `LIMIT/OFFSET` cap.

use ultrasql_executor::{IndexScan, Limit, Operator, RowCodec, TopK};
use ultrasql_planner::{LogicalPlan, SortKey};

use crate::error::ServerError;

use super::LowerCtx;
use super::btree_probe::{probe_index_ordered, probe_index_ordered_limited};
use super::catalog_lookup::{find_single_column_index, key_type_for_btree};
use super::modify::lower_project_columns;
use super::predicate::{IndexKeyRange, column_idx_for_int_key};

/// Try to lower `ORDER BY indexed_col [ASC|DESC]` over a bare table scan into
/// a directed B-tree scan.
///
/// This is intentionally narrow: one integer sort key, one base table, no
/// scan-level projection. Broader interesting-order planning belongs in the
/// optimizer, but this path makes backward index scan reachable through the
/// real wire lowerer today.
pub(crate) fn try_ordered_index_scan(
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
pub(crate) fn try_ordered_index_scan_limit(
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
