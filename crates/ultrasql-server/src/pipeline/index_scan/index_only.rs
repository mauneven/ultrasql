//! Covering index-only scans: serve a projection from B-tree keys when
//! every candidate heap page is proven all-visible.

use ultrasql_core::{DataType, Field, RelationId, Value};
use ultrasql_executor::{IndexOnlyScan, Operator};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::LowerCtx;
use super::btree_probe::{probe_index_entries_ordered, usize_to_u64_saturating};
use super::catalog_lookup::{find_single_column_index, key_type_for_btree};
use super::predicate::{key_value_for_expr, match_indexable_predicate};

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
pub(crate) fn try_index_only_scan(
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
