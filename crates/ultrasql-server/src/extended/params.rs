//! Parameter-inference helpers.
//!
//! Walk a bound [`LogicalPlan`] (or one of its sub-expressions) and
//! discover the maximum `$N` parameter index plus a best-effort type
//! mapping for each parameter slot. Used by [`super::handlers::handle_parse`]
//! to populate `ParameterDescription` before binding.

use ultrasql_core::DataType;
use ultrasql_planner::{
    BinaryOp, LogicalJoinCondition, LogicalOnConflict, LogicalPlan, ScalarExpr,
};

// ---------------------------------------------------------------------------
// Parameter-counting walker.
// ---------------------------------------------------------------------------

/// Return the highest `$N` placeholder index referenced anywhere in `plan`.
///
/// Returns `0` if the plan contains no `Parameter` nodes. Used by
/// `handle_parse` to validate the parameter count at `Bind` time.
pub(super) fn count_parameters_in_plan(plan: &LogicalPlan) -> u32 {
    let mut max = 0_u32;
    walk_plan_exprs(plan, &mut |e| {
        if let ScalarExpr::Parameter { index, .. } = e {
            max = max.max(*index);
        }
    });
    max
}

/// Infer a concrete [`DataType`] for each `$N` placeholder referenced
/// in `plan`, returning a vector indexed by `index - 1` (1-based on the
/// wire, 0-based in the vector). Unresolved slots default to
/// [`DataType::Null`].
///
/// The inference is local: each `Parameter` in a binary comparison
/// against a column borrows the column's type; each `Values` row inside
/// an `Insert` borrows the target column's type; each `Update`
/// assignment to `Parameter` borrows the target column's type. This
/// covers the v0.5 wire shapes (WHERE col op $1, INSERT $1, UPDATE
/// SET col=$1 WHERE col=$2).
///
/// `catalog` is consulted when the inference encounters an `Insert` or
/// `Update` whose target schema is not visible from the plan alone
/// (PostgreSQL stores the target column types on the table catalog,
/// not on the bound plan). Passing `None` is equivalent to having no
/// catalog: target-driven inference is skipped and only the predicate-
/// shape inference applies.
pub(super) fn infer_parameter_types(
    plan: &LogicalPlan,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
) -> Vec<DataType> {
    let n = usize::try_from(count_parameters_in_plan(plan)).unwrap_or(0);
    let mut out = vec![DataType::Null; n];
    if n > 0 {
        infer_into(plan, catalog, &mut out);
    }
    out
}

/// Recursive driver behind [`infer_parameter_types`].
///
/// The match-on-`LogicalPlan` shape is intentionally exhaustive; per-
/// variant logic doesn't compress into a generic walker without
/// obscuring the type-inference rules. The `#[allow]` mirrors the
/// pattern used in `crates/ultrasql-protocol/src/codec.rs`.
#[allow(clippy::too_many_lines)]
fn infer_into(
    plan: &LogicalPlan,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
    out: &mut [DataType],
) {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => {}
        LogicalPlan::Explain { input, .. } => infer_into(input, catalog, out),
        LogicalPlan::Filter { input, predicate } => {
            infer_into(input, catalog, out);
            infer_expr_types_from_predicate(predicate, out);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            infer_into(input, catalog, out);
            for (e, _) in exprs {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
            infer_into(input, catalog, out);
            if let LogicalPlan::Sort { keys, .. } = plan {
                for k in keys {
                    infer_in_expr(&k.expr, None, out);
                }
            }
        }
        LogicalPlan::Values { rows, schema } => {
            for row in rows {
                for (col_i, cell) in row.iter().enumerate() {
                    let target = schema.fields().get(col_i).map(|f| f.data_type.clone());
                    infer_in_expr(cell, target, out);
                }
            }
        }
        LogicalPlan::Insert {
            table,
            columns,
            source,
            returning,
            ..
        } => {
            // The binder's `Values` schema collapses to `Null` when
            // every cell is a parameter, so we cannot rely on
            // `source.schema()`. Look up the target table in the
            // catalog and infer each `Values` row cell against the
            // *target column* type.
            if let Some(cat) = catalog {
                if let Some(meta) = cat.lookup_table(table) {
                    let target_cols: Vec<DataType> = if columns.is_empty() {
                        meta.schema
                            .fields()
                            .iter()
                            .map(|f| f.data_type.clone())
                            .collect()
                    } else {
                        columns
                            .iter()
                            .map(|i| {
                                meta.schema
                                    .fields()
                                    .get(*i)
                                    .map_or(DataType::Null, |f| f.data_type.clone())
                            })
                            .collect()
                    };
                    if let LogicalPlan::Values { rows, .. } = source.as_ref() {
                        for row in rows {
                            for (i, cell) in row.iter().enumerate() {
                                let target = target_cols.get(i).cloned();
                                infer_in_expr(cell, target, out);
                            }
                        }
                    }
                }
            }
            infer_into(source, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            ..
        } => {
            // Target column types via the catalog when available, then
            // via the underlying scan's schema as a fallback.
            let table_schema_owned: Option<ultrasql_core::Schema> = catalog
                .and_then(|cat| cat.lookup_table(table))
                .map(|m| m.schema);
            let table_schema: Option<&ultrasql_core::Schema> =
                table_schema_owned.as_ref().or_else(|| scan_schema(input));
            for (col_idx, e) in assignments {
                let target = table_schema
                    .and_then(|s| s.fields().get(*col_idx).map(|f| f.data_type.clone()));
                infer_in_expr(e, target, out);
            }
            infer_into(input, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            infer_into(input, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            infer_into(left, catalog, out);
            infer_into(right, catalog, out);
            if let LogicalJoinCondition::On(e) = condition {
                infer_expr_types_from_predicate(e, out);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            infer_into(input, catalog, out);
            for e in group_by {
                infer_in_expr(e, None, out);
            }
            for a in aggregates {
                if let Some(e) = &a.arg {
                    infer_in_expr(e, None, out);
                }
                if let Some(e) = &a.direct_arg {
                    infer_in_expr(e, None, out);
                }
                if let Some(key) = &a.order_by {
                    infer_in_expr(&key.expr, None, out);
                }
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            infer_into(left, catalog, out);
            infer_into(right, catalog, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            infer_into(definition, catalog, out);
            infer_into(body, catalog, out);
        }
        LogicalPlan::LockRows { input, .. } => {
            infer_into(input, catalog, out);
        }
        LogicalPlan::Window { input, .. } => {
            infer_into(input, catalog, out);
        }
        LogicalPlan::CreatePolicy { .. } => {}
    }
}

/// Return the column [`Schema`] of the leftmost base-table `Scan` in `plan`.
///
/// Used to look up assignment-target column types in UPDATE: the
/// `assignments` list addresses the target table's schema directly, but
/// the `Update.input` plan's `Filter { Scan { ... } }` is where that
/// schema lives.
fn scan_schema(plan: &LogicalPlan) -> Option<&ultrasql_core::Schema> {
    match plan {
        LogicalPlan::Scan { schema, .. } => Some(schema),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Limit { input, .. } => scan_schema(input),
        _ => None,
    }
}

fn zero_based_parameter_slot(index: u32) -> Option<usize> {
    usize::try_from(index.checked_sub(1)?).ok()
}

/// Infer parameter types from a boolean predicate at the top of a
/// `Filter` / join `On`.
///
/// Recognises `Column ⟷ Parameter` and `Parameter ⟷ Column` binary
/// shapes (Eq/Lt/Gt/etc.) and assigns the column's type to the
/// parameter slot. Other shapes fall through to the generic walker.
fn infer_expr_types_from_predicate(expr: &ScalarExpr, out: &mut [DataType]) {
    match expr {
        ScalarExpr::Binary {
            left, right, op, ..
        } => {
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                // Column = Parameter, or Parameter = Column.
                match (left.as_ref(), right.as_ref()) {
                    (ScalarExpr::Column { data_type, .. }, ScalarExpr::Parameter { index, .. })
                    | (ScalarExpr::Parameter { index, .. }, ScalarExpr::Column { data_type, .. }) => {
                        if let Some(slot) = zero_based_parameter_slot(*index) {
                            if let Some(s) = out.get_mut(slot) {
                                if matches!(s, DataType::Null) {
                                    *s = data_type.clone();
                                }
                            }
                        }
                    }
                    _ => {
                        infer_in_expr(left, None, out);
                        infer_in_expr(right, None, out);
                    }
                }
                // Recurse into nested binaries (AND/OR conjunctions).
                infer_in_expr(left, None, out);
                infer_in_expr(right, None, out);
            } else {
                infer_expr_types_from_predicate(left, out);
                infer_expr_types_from_predicate(right, out);
            }
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            infer_expr_types_from_predicate(expr, out);
        }
        _ => infer_in_expr(expr, None, out),
    }
}

/// Infer types from a generic expression. `target_type` is the expected
/// result type at this position (e.g. the target column's type in an
/// INSERT cell or an UPDATE assignment); a bare `Parameter` borrows it.
fn infer_in_expr(expr: &ScalarExpr, target_type: Option<DataType>, out: &mut [DataType]) {
    match expr {
        ScalarExpr::Parameter { index, data_type } => {
            let inferred = if matches!(data_type, DataType::Null) {
                target_type
            } else {
                Some(data_type.clone())
            };
            if let Some(t) = inferred {
                if let Some(slot) = zero_based_parameter_slot(*index) {
                    if let Some(s) = out.get_mut(slot) {
                        if matches!(s, DataType::Null) {
                            *s = t;
                        }
                    }
                }
            }
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::OuterColumn { .. } => {
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            infer_in_expr(expr, None, out);
        }
        ScalarExpr::Binary {
            left, right, op, ..
        } => {
            // Comparisons surface column/parameter pairs.
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                infer_expr_types_from_predicate(expr, out);
            }
            let child_target = if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) {
                target_type
            } else {
                None
            };
            infer_in_expr(left, child_target.clone(), out);
            infer_in_expr(right, child_target, out);
        }
        ScalarExpr::ScalarSubquery { subplan, .. } | ScalarExpr::Exists { subplan, .. } => {
            // Type-inference inside subqueries does not have a catalog
            // handle here; subquery shapes are already a v0.5 follow-up
            // for the Extended Query path, so passing `None` is fine.
            infer_into(subplan, None, out);
        }
        ScalarExpr::InSubquery { expr, subplan, .. } => {
            infer_in_expr(expr, None, out);
            infer_into(subplan, None, out);
        }
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            let arg_target = runtime_cast_parameter_target(name, data_type);
            for a in args {
                infer_in_expr(a, arg_target.clone(), out);
            }
        }
    }
}

fn runtime_cast_parameter_target(name: &str, data_type: &DataType) -> Option<DataType> {
    matches!(
        name,
        "__ultrasql_cast_money"
            | "__ultrasql_cast_numeric"
            | "__ultrasql_cast_oid"
            | "__ultrasql_cast_regclass"
            | "__ultrasql_cast_regtype"
            | "__ultrasql_cast_text"
    )
    .then(|| data_type.clone())
}

/// Walk every `ScalarExpr` reachable from `plan`, calling `f` on each.
///
/// Recurses into sub-plans (subqueries, CTE definitions) so $N references
/// in a subquery are visible to the caller. The walker is read-only —
/// see [`map_plan_exprs`] for the mutating sibling.
#[allow(clippy::too_many_lines)]
pub(super) fn walk_plan_exprs<F: FnMut(&ScalarExpr)>(plan: &LogicalPlan, f: &mut F) {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::CreateTypeEnum { .. }
        | LogicalPlan::CreateTypeComposite { .. }
        | LogicalPlan::CreateDomain { .. }
        | LogicalPlan::CreateOperator { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
        | LogicalPlan::CreateSchema { .. }
        | LogicalPlan::DropSchema { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateSequence { .. }
        | LogicalPlan::AlterSequence { .. }
        | LogicalPlan::DropSequence { .. }
        | LogicalPlan::Comment { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. }
        | LogicalPlan::SetVariable { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => {}
        LogicalPlan::Explain { input, .. } => walk_plan_exprs(input, f),
        LogicalPlan::Filter { input, predicate } => {
            walk_plan_exprs(input, f);
            walk_expr(predicate, f);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            walk_plan_exprs(input, f);
            for (e, _) in exprs {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
            walk_plan_exprs(input, f);
            if let LogicalPlan::Sort { keys, .. } = plan {
                for k in keys {
                    walk_expr(&k.expr, f);
                }
            }
        }
        LogicalPlan::Values { rows, .. } => {
            for row in rows {
                for cell in row {
                    walk_expr(cell, f);
                }
            }
        }
        LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            walk_plan_exprs(source, f);
            if let Some(oc) = on_conflict {
                walk_on_conflict(oc, f);
            }
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Update {
            assignments,
            input,
            returning,
            ..
        } => {
            walk_plan_exprs(input, f);
            for (_, e) in assignments {
                walk_expr(e, f);
            }
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            walk_plan_exprs(input, f);
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            walk_plan_exprs(left, f);
            walk_plan_exprs(right, f);
            if let LogicalJoinCondition::On(e) = condition {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            walk_plan_exprs(input, f);
            for e in group_by {
                walk_expr(e, f);
            }
            for a in aggregates {
                if let Some(e) = &a.arg {
                    walk_expr(e, f);
                }
                if let Some(e) = &a.direct_arg {
                    walk_expr(e, f);
                }
                if let Some(key) = &a.order_by {
                    walk_expr(&key.expr, f);
                }
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            walk_plan_exprs(left, f);
            walk_plan_exprs(right, f);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            walk_plan_exprs(definition, f);
            walk_plan_exprs(body, f);
        }
        LogicalPlan::LockRows { input, .. } => {
            walk_plan_exprs(input, f);
        }
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            ..
        } => {
            walk_plan_exprs(input, f);
            for e in partition_by {
                walk_expr(e, f);
            }
            for k in order_by {
                walk_expr(&k.expr, f);
            }
            match func {
                ultrasql_planner::LogicalWindowFunc::Lag { expr, .. }
                | ultrasql_planner::LogicalWindowFunc::Lead { expr, .. }
                | ultrasql_planner::LogicalWindowFunc::FirstValue(expr)
                | ultrasql_planner::LogicalWindowFunc::LastValue(expr)
                | ultrasql_planner::LogicalWindowFunc::NthValue { expr, .. } => walk_expr(expr, f),
                _ => {}
            }
        }
        LogicalPlan::CreatePolicy { .. } => {}
    }
}

/// Recursively visit every node in `expr`, calling `f` on each.
///
/// Recurses into subquery plans via [`walk_plan_exprs`] so a `$N`
/// reference deep inside a `WHERE x IN (SELECT … WHERE y = $1)` is
/// surfaced to the caller.
fn walk_expr<F: FnMut(&ScalarExpr)>(expr: &ScalarExpr, f: &mut F) {
    f(expr);
    match expr {
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. } => {}
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => walk_expr(expr, f),
        ScalarExpr::Binary { left, right, .. } => {
            walk_expr(left, f);
            walk_expr(right, f);
        }
        ScalarExpr::ScalarSubquery { subplan, .. } | ScalarExpr::Exists { subplan, .. } => {
            walk_plan_exprs(subplan, f);
        }
        ScalarExpr::InSubquery { expr, subplan, .. } => {
            walk_expr(expr, f);
            walk_plan_exprs(subplan, f);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for a in args {
                walk_expr(a, f);
            }
        }
    }
}

/// Walk the expressions inside an `ON CONFLICT` clause.
fn walk_on_conflict<F: FnMut(&ScalarExpr)>(oc: &LogicalOnConflict, f: &mut F) {
    if let LogicalOnConflict::DoUpdate {
        assignments,
        r#where,
        ..
    } = oc
    {
        for (_, e) in assignments {
            walk_expr(e, f);
        }
        if let Some(w) = r#where {
            walk_expr(w, f);
        }
    }
}
