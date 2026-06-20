//! Shared lowering helpers: the TID-emitting child scan for UPDATE /
//! DELETE and the projection-elision logic for SELECT lists.

use ultrasql_executor::{Filter, Operator, Project};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use ultrasql_catalog::TableEntry;

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::pipeline::agg_fuse::shift_column_indices;

use super::indexes::build_tid_seq_scan;

/// Build the TID-emitting child operator for an UPDATE / DELETE.
///
/// Recognises the binder's `Scan` / nested `Filter` shapes:
///
/// - bare `Scan { table }` → TID-emitting `SeqScan`.
/// - `Filter { input, predicate }` → `Filter`(lowered `input`),
///   with every `Column { index }` in `predicate` shifted by +2 to
///   re-target the TID-prefixed batch.
///
/// Any other input shape — the planner does not produce it for UPDATE

pub(super) fn build_filtered_tid_scan(
    target_table: &str,
    entry: &TableEntry,
    input: &LogicalPlan,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    match input {
        LogicalPlan::Scan { table, .. } => {
            if !table.eq_ignore_ascii_case(target_table) {
                return Err(ServerError::Unsupported(
                    "UPDATE / DELETE child scan references a different table",
                ));
            }
            Ok(build_tid_seq_scan(entry, ctx))
        }
        LogicalPlan::Filter {
            input: filter_input,
            predicate,
        } => {
            let child = build_filtered_tid_scan(target_table, entry, filter_input, ctx)?;
            let shifted = shift_column_indices(predicate, 2);
            Ok(Box::new(Filter::new(child, shifted)))
        }
        _ => Err(ServerError::Unsupported(
            "UPDATE / DELETE input shape; expected Scan or Filter(Scan)",
        )),
    }
}

pub(crate) fn lower_project_columns(
    child: Box<dyn Operator>,
    exprs: &[(ScalarExpr, String)],
) -> Result<Box<dyn Operator>, ServerError> {
    // Fast path: every projection item is a bare column reference.
    // The downstream pipeline can then short-circuit through the
    // index-only `Project` operator and (when the indices match the
    // child schema) skip the projection wrapper entirely.
    //
    // When any item carries an expression (function call, arithmetic,
    // CASE, …) we route through the general `ProjectExprs` operator
    // that evaluates each `ScalarExpr` per row.
    let all_bare_columns = exprs
        .iter()
        .all(|(e, _)| matches!(e, ScalarExpr::Column { .. }));
    if !all_bare_columns {
        // Build the output schema before handing to the operator;
        // each projection's output type is the bound expression's
        // declared type, named after the alias / derived label.
        let mut fields: Vec<ultrasql_core::Field> = Vec::with_capacity(exprs.len());
        for (e, name) in exprs {
            fields.push(ultrasql_core::Field::nullable(name.clone(), e.data_type()));
        }
        let output_schema = ultrasql_core::Schema::new_with_duplicate_names(fields);
        return ultrasql_executor::ProjectExprs::new(child, exprs, output_schema)
            .map(|op| Box::new(op) as Box<dyn Operator>)
            .map_err(|e| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "projection: {e}"
                )))
            });
    }
    let mut indices: Vec<usize> = Vec::with_capacity(exprs.len());
    for (expr, _name) in exprs {
        match expr {
            ScalarExpr::Column { index, .. } => indices.push(*index),
            _ => unreachable!("filtered to bare columns above"),
        }
    }
    // Identity-projection elision: if the requested indices exactly
    // mirror the child's column order (`[0, 1, .., n-1]`) and cover
    // every child column **with the same output names**, the
    // [`Project`] wrapper would just clone each `Column` into a fresh
    // `Vec<Column>` on every batch — a per-batch `Vec<i32>` deep-copy
    // for narrow-int relations. Hand the child back to the caller
    // unchanged so the wire-encoder sees the scan's own batches
    // without an extra layer of clones.
    //
    // `SELECT id, val FROM t` over a two-column `(id INT NOT NULL,
    // val INT)` relation matches this shape — eliminating the
    // wrapper drops ~16 KiB/batch of memcpy on the `select_scan_10k`
    // workload. The name check guards against `SELECT id AS my_id
    // FROM t`, which keeps the same data flow but rebinds the wire
    // `RowDescription` column name and so must build a wrapping
    // projection to carry the alias.
    let child_schema = child.schema();
    let child_width = child_schema.len();
    let is_identity_indices =
        indices.len() == child_width && indices.iter().enumerate().all(|(i, &idx)| i == idx);
    let names_match = is_identity_indices
        && exprs
            .iter()
            .enumerate()
            .all(|(i, (_, name))| child_schema.field_at(i).name == *name);
    if names_match {
        return Ok(child);
    }
    let fields: Vec<ultrasql_core::Field> = exprs
        .iter()
        .map(|(expr, name)| ultrasql_core::Field::nullable(name.clone(), expr.data_type()))
        .collect();
    let output_schema = ultrasql_core::Schema::new_with_duplicate_names(fields);
    Ok(Box::new(Project::with_schema(
        child,
        indices,
        output_schema,
    )?))
}
