//! Parameter substitution and the plan-tree rewriter that backs it.
//!
//! `Bind` decodes each parameter value into a [`Value`] and then walks
//! the prepared statement's bound [`LogicalPlan`], rewriting every
//! [`ScalarExpr::Parameter`] node into a [`ScalarExpr::Literal`]. The
//! substituted plan is what [`super::execute::execute_portal`] runs.

use ultrasql_core::{DataType, Value};
use ultrasql_planner::{
    BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalOnConflict, LogicalPlan,
    LogicalSetOp, LogicalSetQuantifier, ScalarExpr, SortKey,
};

// Parameter substitution: ScalarExpr::Parameter → ScalarExpr::Literal.
// ---------------------------------------------------------------------------

/// Walk `plan` and rewrite every `ScalarExpr::Parameter { index }` into
/// a `ScalarExpr::Literal { value: values[index-1] }`.
///
/// Out-of-range `$N` references are left as `Parameter` nodes; the
/// executor will surface them as `EvalError::ParameterIndex`. That
/// behaviour matches PostgreSQL, which only checks parameter arity
/// during `Bind` (we already check in `handle_bind`).
///
/// The walker constructs a fresh plan; the input is unchanged. This
/// makes the function suitable for use against a `&PreparedStatement`
/// shared across multiple `Bind` calls.
pub(crate) fn substitute_parameters_in_plan(plan: &LogicalPlan, values: &[Value]) -> LogicalPlan {
    map_plan_exprs(plan, &|e| substitute_parameter_in_expr(e, values))
}

/// Recursively rewrite parameters in `expr`.
fn substitute_parameter_in_expr(expr: &ScalarExpr, values: &[Value]) -> ScalarExpr {
    match expr {
        ScalarExpr::Parameter { index, .. } => zero_based_parameter_slot(*index)
            .and_then(|slot| values.get(slot))
            .map_or_else(
                || expr.clone(),
                |v| ScalarExpr::Literal {
                    data_type: v.data_type(),
                    value: v.clone(),
                },
            ),
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::OuterColumn { .. } => {
            expr.clone()
        }
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            let mut new_left = substitute_parameter_in_expr(left, values);
            let mut new_right = substitute_parameter_in_expr(right, values);
            // After substitution we may have a binary operator whose
            // result type was inferred against `Null` (the binder's
            // default for `Parameter`). Re-derive the result type from
            // the now-concrete operand types so the executor's type
            // checks downstream see the right shape (e.g. an `Eq` with
            // an Int32 column on the left should report Bool, not
            // whatever was inferred before).
            let lt = new_left.data_type();
            let rt = new_right.data_type();
            // Best-effort numeric widening: if comparing/arith between
            // Int32-column and Int64-literal, the literal coerces to
            // Int32 when it fits and to Int64 otherwise. We coerce the
            // *literal* side so the Filter operator's SIMD i32/i64
            // dispatch picks the column's type.
            coerce_literal_to_match(&mut new_left, &mut new_right);
            // Recompute data_type if the operator is a comparison;
            // comparisons always return Bool. For arithmetic we keep
            // the binder's original choice (it's a join over the two
            // operand types and that join is still valid).
            let new_dt = match op {
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::Like
                | BinaryOp::NotLike
                | BinaryOp::Ilike
                | BinaryOp::NotIlike => DataType::Bool,
                _ => data_type.clone(),
            };
            let _ = (lt, rt);
            ScalarExpr::Binary {
                op: *op,
                left: Box::new(new_left),
                right: Box::new(new_right),
                data_type: new_dt,
            }
        }
        ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            negated: *negated,
        },
        ScalarExpr::ScalarSubquery {
            subplan,
            correlated,
            data_type,
        } => ScalarExpr::ScalarSubquery {
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            correlated: *correlated,
            data_type: data_type.clone(),
        },
        ScalarExpr::Exists {
            subplan,
            negated,
            correlated,
        } => ScalarExpr::Exists {
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            negated: *negated,
            correlated: *correlated,
        },
        ScalarExpr::InSubquery {
            expr,
            subplan,
            negated,
            correlated,
            data_type,
        } => ScalarExpr::InSubquery {
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            negated: *negated,
            correlated: *correlated,
            data_type: data_type.clone(),
        },
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_parameter_in_expr(a, values))
                .collect(),
            data_type: data_type.clone(),
        },
    }
}

fn zero_based_parameter_slot(index: u32) -> Option<usize> {
    usize::try_from(index.checked_sub(1)?).ok()
}

/// If `left` or `right` is a `Literal` and the other side is a `Column`
/// (or anything with a concrete type), coerce the literal to the
/// column's type when it's a safe numeric narrow/widen. Keeps the
/// Filter SIMD fast-path happy: it dispatches on the column's type and
/// expects both operands to have the same width.
fn coerce_literal_to_match(left: &mut ScalarExpr, right: &mut ScalarExpr) {
    coerce_literal_side(left, right);
    coerce_literal_side(right, left);
}

fn coerce_literal_side(lit_side: &mut ScalarExpr, ref_side: &ScalarExpr) {
    let target = ref_side.data_type();
    coerce_literal_to_type(lit_side, &target);
}

fn coerce_literal_to_type(expr: &mut ScalarExpr, target: &DataType) {
    let ScalarExpr::Literal { value, data_type } = expr else {
        return;
    };
    if matches!(target, DataType::Null) || data_type == target {
        return;
    }
    match (target, &*value) {
        (DataType::Int16, Value::Int32(v)) => {
            if let Ok(narrow) = i16::try_from(*v) {
                *value = Value::Int16(narrow);
                *data_type = DataType::Int16;
            }
        }
        (DataType::Int16, Value::Int64(v)) => {
            if let Ok(narrow) = i16::try_from(*v) {
                *value = Value::Int16(narrow);
                *data_type = DataType::Int16;
            }
        }
        (DataType::Int32, Value::Int16(v)) => {
            *value = Value::Int32(i32::from(*v));
            *data_type = DataType::Int32;
        }
        (DataType::Int32, Value::Int64(v)) => {
            if let Ok(narrow) = i32::try_from(*v) {
                *value = Value::Int32(narrow);
                *data_type = DataType::Int32;
            }
        }
        (DataType::Int64, Value::Int16(v)) => {
            *value = Value::Int64(i64::from(*v));
            *data_type = DataType::Int64;
        }
        (DataType::Int64, Value::Int32(v)) => {
            *value = Value::Int64(i64::from(*v));
            *data_type = DataType::Int64;
        }
        (DataType::Float64, Value::Float32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float32, Value::Float64(v)) => {
            #[allow(clippy::cast_possible_truncation)]
            let narrow = *v as f32;
            *value = Value::Float32(narrow);
            *data_type = DataType::Float32;
        }
        // Int → Float widening (e.g. id (Int32) = 42 (Int32 lit) is fine;
        // this hits when comparing a Float column to an integer literal).
        (DataType::Float64, Value::Int16(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int64(v)) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = *v as f64;
            *value = Value::Float64(widened);
            *data_type = DataType::Float64;
        }
        (DataType::Float32, Value::Int16(v)) => {
            *value = Value::Float32(f32::from(*v));
            *data_type = DataType::Float32;
        }
        _ => {}
    }
}

/// Walk `plan`, rebuilding it with every `ScalarExpr` replaced by `f(e)`.
///
/// The traversal is exhaustive: every place the plan stores a
/// `ScalarExpr` is visited. Sub-plans (subqueries, CTE bodies) are
/// recursed into via `substitute_parameters_in_plan`.
#[allow(clippy::too_many_lines)]
fn map_plan_exprs<F>(plan: &LogicalPlan, f: &F) -> LogicalPlan
where
    F: Fn(&ScalarExpr) -> ScalarExpr,
{
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
        | LogicalPlan::SetRole { .. }
        | LogicalPlan::Listen { .. }
        | LogicalPlan::Notify { .. }
        | LogicalPlan::Unlisten { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::FunctionScan { .. } => plan.clone(),
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(map_plan_exprs(input, f)),
            predicate: f(predicate),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(map_plan_exprs(input, f)),
            exprs: exprs.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Limit { input, n, offset } => LogicalPlan::Limit {
            input: Box::new(map_plan_exprs(input, f)),
            n: *n,
            offset: *offset,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(map_plan_exprs(input, f)),
            keys: keys
                .iter()
                .map(|k| SortKey {
                    expr: f(&k.expr),
                    asc: k.asc,
                    nulls_first: k.nulls_first,
                })
                .collect(),
        },
        LogicalPlan::Values { rows, schema } => {
            // After substitution, parameter cells become concrete-typed
            // literals; the binder built `schema` assuming `Null` for
            // every all-parameter column. Rebuild any column whose
            // schema type is still `Null` from the first concrete-typed
            // cell — the executor's ValuesScan / batch builder rejects
            // `DataType::Null` and we don't want a downstream panic.
            let new_rows: Vec<Vec<ScalarExpr>> =
                rows.iter().map(|row| row.iter().map(f).collect()).collect();
            let mut new_rows = new_rows;
            for row in &mut new_rows {
                for (ci, cell) in row.iter_mut().enumerate() {
                    if let Some(field) = schema.fields().get(ci) {
                        coerce_literal_to_type(cell, &field.data_type);
                    }
                }
            }
            let new_schema = rebuild_values_schema(schema, &new_rows);
            LogicalPlan::Values {
                rows: new_rows,
                schema: new_schema,
            }
        }
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            schema,
        } => LogicalPlan::Insert {
            table: table.clone(),
            columns: columns.clone(),
            source: Box::new(map_plan_exprs(source, f)),
            on_conflict: on_conflict.as_ref().map(|oc| map_on_conflict(oc, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            schema,
        } => LogicalPlan::Update {
            table: table.clone(),
            assignments: assignments.iter().map(|(i, e)| (*i, f(e))).collect(),
            input: Box::new(map_plan_exprs(input, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Delete {
            table,
            input,
            returning,
            schema,
        } => LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(map_plan_exprs(input, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(map_plan_exprs(left, f)),
            right: Box::new(map_plan_exprs(right, f)),
            join_type: *join_type,
            condition: match condition {
                LogicalJoinCondition::On(e) => LogicalJoinCondition::On(f(e)),
                other => other.clone(),
            },
            schema: schema.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(map_plan_exprs(input, f)),
            group_by: group_by.iter().map(f).collect(),
            aggregates: aggregates
                .iter()
                .map(|a| LogicalAggregateExpr {
                    func: a.func,
                    arg: a.arg.as_ref().map(f),
                    direct_arg: a.direct_arg.as_ref().map(f),
                    order_by: a.order_by.as_ref().map(|key| ultrasql_planner::SortKey {
                        expr: f(&key.expr),
                        asc: key.asc,
                        nulls_first: key.nulls_first,
                    }),
                    distinct: a.distinct,
                    output_name: a.output_name.clone(),
                    data_type: a.data_type.clone(),
                })
                .collect(),
            schema: schema.clone(),
        },
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => LogicalPlan::SetOp {
            op: match op {
                LogicalSetOp::Union => LogicalSetOp::Union,
                LogicalSetOp::Intersect => LogicalSetOp::Intersect,
                LogicalSetOp::Except => LogicalSetOp::Except,
            },
            quantifier: match quantifier {
                LogicalSetQuantifier::All => LogicalSetQuantifier::All,
                LogicalSetQuantifier::Distinct => LogicalSetQuantifier::Distinct,
            },
            left: Box::new(map_plan_exprs(left, f)),
            right: Box::new(map_plan_exprs(right, f)),
            schema: schema.clone(),
        },
        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            schema,
        } => LogicalPlan::Cte {
            name: name.clone(),
            recursive: *recursive,
            definition: Box::new(map_plan_exprs(definition, f)),
            body: Box::new(map_plan_exprs(body, f)),
            schema: schema.clone(),
        },
        LogicalPlan::LockRows {
            input,
            strength,
            wait_policy,
            schema,
        } => LogicalPlan::LockRows {
            input: Box::new(map_plan_exprs(input, f)),
            strength: *strength,
            wait_policy: *wait_policy,
            schema: schema.clone(),
        },
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            output_name,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(map_plan_exprs(input, f)),
            partition_by: partition_by.iter().map(f).collect(),
            order_by: order_by
                .iter()
                .map(|k| ultrasql_planner::SortKey {
                    expr: f(&k.expr),
                    asc: k.asc,
                    nulls_first: k.nulls_first,
                })
                .collect(),
            func: match func {
                ultrasql_planner::LogicalWindowFunc::Lag {
                    expr,
                    offset,
                    default,
                } => ultrasql_planner::LogicalWindowFunc::Lag {
                    expr: f(expr),
                    offset: *offset,
                    default: default.clone(),
                },
                ultrasql_planner::LogicalWindowFunc::Lead {
                    expr,
                    offset,
                    default,
                } => ultrasql_planner::LogicalWindowFunc::Lead {
                    expr: f(expr),
                    offset: *offset,
                    default: default.clone(),
                },
                ultrasql_planner::LogicalWindowFunc::FirstValue(e) => {
                    ultrasql_planner::LogicalWindowFunc::FirstValue(f(e))
                }
                ultrasql_planner::LogicalWindowFunc::LastValue(e) => {
                    ultrasql_planner::LogicalWindowFunc::LastValue(f(e))
                }
                ultrasql_planner::LogicalWindowFunc::NthValue { expr, n } => {
                    ultrasql_planner::LogicalWindowFunc::NthValue {
                        expr: f(expr),
                        n: *n,
                    }
                }
                other => other.clone(),
            },
            output_name: output_name.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::CreatePolicy { .. } => plan.clone(),
    }
}

/// Rebuild a `Values` plan's column schema when post-substitution
/// cells reveal concrete types the binder could not see.
///
/// For each column position, keep the existing schema field if its
/// type is already concrete (not `Null`); otherwise take the first
/// concrete-typed cell across all rows. The output schema mirrors the
/// binder's "column1, column2, …" naming convention. Falling back to
/// the input schema on any rebuild failure keeps callers crash-safe.
fn rebuild_values_schema(
    schema: &ultrasql_core::Schema,
    rows: &[Vec<ScalarExpr>],
) -> ultrasql_core::Schema {
    let fields = schema.fields();
    let mut new_types: Vec<DataType> = fields.iter().map(|f| f.data_type.clone()).collect();
    for (ci, ty) in new_types.iter_mut().enumerate() {
        if matches!(ty, DataType::Null) {
            for row in rows {
                if let Some(cell) = row.get(ci) {
                    let cell_ty = cell.data_type();
                    if !matches!(cell_ty, DataType::Null) {
                        *ty = cell_ty;
                        break;
                    }
                }
            }
        }
    }
    let rebuilt: Vec<ultrasql_core::Field> = new_types
        .into_iter()
        .enumerate()
        .map(|(i, ty)| {
            // Mirror the binder's `column{N}` naming.
            let name = fields
                .get(i)
                .map_or_else(|| format!("column{}", i + 1), |f| f.name.clone());
            ultrasql_core::Field::nullable(name, ty)
        })
        .collect();
    ultrasql_core::Schema::new(rebuilt).unwrap_or_else(|_| schema.clone())
}

fn map_on_conflict<F>(oc: &LogicalOnConflict, f: &F) -> LogicalOnConflict
where
    F: Fn(&ScalarExpr) -> ScalarExpr,
{
    match oc {
        LogicalOnConflict::DoNothing { target } => LogicalOnConflict::DoNothing {
            target: target.clone(),
        },
        LogicalOnConflict::DoUpdate {
            target,
            assignments,
            r#where,
        } => LogicalOnConflict::DoUpdate {
            target: target.clone(),
            assignments: assignments.iter().map(|(i, e)| (*i, f(e))).collect(),
            r#where: r#where.as_ref().map(f),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{Field, Schema};

    #[test]
    fn substitute_ignores_zero_parameter_index() {
        let schema = Schema::new([Field::nullable("p", DataType::Null)]).expect("schema");
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Empty {
                schema: Schema::empty(),
            }),
            exprs: vec![(
                ScalarExpr::Parameter {
                    index: 0,
                    data_type: DataType::Null,
                },
                "p".to_owned(),
            )],
            schema,
        };

        let substituted = substitute_parameters_in_plan(&plan, &[Value::Int32(7)]);
        let LogicalPlan::Project { exprs, .. } = substituted else {
            panic!("expected Project");
        };
        assert!(matches!(
            exprs[0].0,
            ScalarExpr::Parameter {
                index: 0,
                data_type: DataType::Null
            }
        ));
    }
}
