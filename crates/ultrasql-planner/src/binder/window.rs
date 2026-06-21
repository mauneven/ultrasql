//! Window-function binder.
//!
//! Splits a SELECT projection's window-function calls out of the
//! per-row projection and wraps the FROM/WHERE plan in one
//! [`LogicalPlan::Window`] node per call. The projection is rewritten
//! so each call site becomes a [`Expr::Column`] reference to the
//! appended `"$wn_N"` column the new operator emits.
//!
//! The transformation runs **once per `SELECT`**, immediately after
//! `WHERE` binding and before the projection/aggregate logic. Because
//! the rewrite preserves left-to-right evaluation order and never
//! shifts pre-existing schema indices, the downstream projection
//! binder sees a schema whose new tail columns name each window
//! result with a unique synthetic identifier.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    Expr, FrameBound, FrameExclusion, FrameUnits, Identifier, NullsOrder, ObjectName,
    SortDirection, UnaryOp, WindowFrame, WindowSpec,
};

use crate::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{
    BoundFrameBound, BoundFrameExclusion, BoundFrameUnits, LogicalPlan, LogicalWindowFrame,
    LogicalWindowFunc, SortKey, WindowAggKind,
};

use super::aggregate::aggregate_return_type;

use super::ScopeStack;
use super::expr_bind::bind_expr_with_ctes;

/// Walk `projection` and pull out every top-level `Expr::Call { over:
/// Some(_), .. }` into a parallel `Vec<WindowExtraction>`; replace the
/// in-place call expression with a synthetic [`Expr::Column`] that
/// references the appended `"$wn_N"` column.
///
/// Returns `(rewritten_projection, extractions)`. The caller is
/// responsible for wrapping the FROM/WHERE plan in one
/// [`LogicalPlan::Window`] per extraction and then binding
/// `rewritten_projection` against the resulting schema.
pub(super) fn extract_window_calls(
    projection: &[ultrasql_parser::ast::SelectItem],
) -> (Vec<ultrasql_parser::ast::SelectItem>, Vec<WindowExtraction>) {
    let mut extractions: Vec<WindowExtraction> = Vec::new();
    let rewritten: Vec<ultrasql_parser::ast::SelectItem> = projection
        .iter()
        .map(|item| match item {
            ultrasql_parser::ast::SelectItem::Expr { expr, alias, span } => {
                let rewritten_expr = rewrite_expr(expr, &mut extractions);
                ultrasql_parser::ast::SelectItem::Expr {
                    expr: rewritten_expr,
                    alias: alias.clone(),
                    span: *span,
                }
            }
            other => other.clone(),
        })
        .collect();
    (rewritten, extractions)
}

/// Recursively walk `expr`. If it is a window call, replace it with a
/// `Column` ref to the synthetic output name and push an extraction;
/// otherwise recurse into children so a nested window call inside a
/// `CASE` arm or a binary op is still discovered.
fn rewrite_expr(expr: &Expr, out: &mut Vec<WindowExtraction>) -> Expr {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            over: Some(spec),
            span,
            ..
        } => {
            let output_name = format!("$wn_{}", out.len());
            out.push(WindowExtraction {
                name: name.clone(),
                args: args.clone(),
                distinct: *distinct,
                spec: spec.clone(),
                output_name: output_name.clone(),
            });
            Expr::Column {
                name: ObjectName {
                    parts: vec![Identifier {
                        value: output_name,
                        quoted: false,
                        span: *span,
                    }],
                    span: *span,
                },
            }
        }
        Expr::Binary {
            op,
            left,
            right,
            span,
        } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite_expr(left, out)),
            right: Box::new(rewrite_expr(right, out)),
            span: *span,
        },
        Expr::Unary { op, expr, span } => Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_expr(expr, out)),
            span: *span,
        },
        Expr::Collate {
            expr,
            collation,
            span,
        } => Expr::Collate {
            expr: Box::new(rewrite_expr(expr, out)),
            collation: collation.clone(),
            span: *span,
        },
        // Other shapes do not contain window calls in practice; leave
        // them untouched. The binder will fail with a useful error if a
        // window call appears in a context this rewriter does not
        // recognise.
        other => other.clone(),
    }
}

/// A single window-function call lifted out of the projection.
pub(super) struct WindowExtraction {
    pub name: ObjectName,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub spec: WindowSpec,
    pub output_name: String,
}

/// Wrap `plan` in one [`LogicalPlan::Window`] per extraction. Each
/// wrapper extends the schema with the synthetic `"$wn_N"` column the
/// extraction reserved. Returns the wrapped plan; on a malformed call
/// (unsupported function, wrong arity) returns a [`PlanError`].
pub(super) fn apply_window_extractions(
    mut plan: LogicalPlan,
    extractions: Vec<WindowExtraction>,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    for ex in extractions {
        let func_name = ex
            .name
            .parts
            .last()
            .map_or(String::new(), |p| p.value.to_ascii_lowercase());

        if ex.distinct {
            return Err(PlanError::not_supported(format!(
                "DISTINCT window function '{func_name}'"
            )));
        }

        let func = resolve_window_func(
            &func_name,
            &ex.args,
            plan.schema(),
            catalog,
            cte_catalog,
            scope,
        )?;

        let partition_by: Vec<ScalarExpr> = ex
            .spec
            .partition_by
            .iter()
            .map(|e| bind_expr_with_ctes(e, plan.schema(), catalog, cte_catalog, scope))
            .collect::<Result<_, _>>()?;
        let order_by: Vec<SortKey> = ex
            .spec
            .order_by
            .iter()
            .map(|item| -> Result<SortKey, PlanError> {
                let bound =
                    bind_expr_with_ctes(&item.expr, plan.schema(), catalog, cte_catalog, scope)?;
                Ok(SortKey {
                    expr: bound,
                    asc: matches!(item.direction, SortDirection::Asc),
                    nulls_first: matches!(item.nulls, NullsOrder::First),
                })
            })
            .collect::<Result<_, _>>()?;

        // Resolve the frame: bind an explicit frame, or apply the SQL
        // default (RANGE running with ORDER BY; whole partition without).
        // Frame-insensitive functions (ranking / LAG / LEAD) always carry
        // the whole-partition frame so the executor never branches on a
        // user-supplied frame for them — matching PostgreSQL.
        let frame = if frame_insensitive(&func) {
            LogicalWindowFrame::whole_partition()
        } else {
            resolve_window_frame(
                ex.spec.frame.as_ref(),
                &order_by,
                plan.schema(),
                catalog,
                cte_catalog,
                scope,
            )?
        };

        let result_type = window_func_result_type(&func);
        let mut new_fields: Vec<Field> = plan.schema().fields().to_vec();
        new_fields.push(Field::nullable(&ex.output_name, result_type));
        let new_schema = Schema::new(new_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("window schema: {e}")))?;

        plan = LogicalPlan::Window {
            input: Box::new(plan),
            partition_by,
            order_by,
            func,
            frame,
            output_name: ex.output_name,
            schema: new_schema,
        };
    }
    Ok(plan)
}

/// `true` for window functions whose result is defined independently of
/// the frame: the ranking functions and the offset functions
/// (`LAG`/`LEAD`). PostgreSQL ignores any frame clause on these.
fn frame_insensitive(func: &LogicalWindowFunc) -> bool {
    matches!(
        func,
        LogicalWindowFunc::RowNumber
            | LogicalWindowFunc::Rank
            | LogicalWindowFunc::DenseRank
            | LogicalWindowFunc::Ntile(_)
            | LogicalWindowFunc::Lag { .. }
            | LogicalWindowFunc::Lead { .. }
    )
}

/// Bind an explicit [`WindowFrame`] (or apply the SQL default frame) to a
/// [`LogicalWindowFrame`], validating it per the SQL/PostgreSQL window
/// semantics. Offset expressions are bound against `schema`.
fn resolve_window_frame(
    frame: Option<&WindowFrame>,
    order_by: &[SortKey],
    schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalWindowFrame, PlanError> {
    let Some(frame) = frame else {
        // No explicit frame: SQL default depends on ORDER BY presence.
        return Ok(if order_by.is_empty() {
            LogicalWindowFrame::whole_partition()
        } else {
            LogicalWindowFrame::default_running()
        });
    };

    let units = match frame.units {
        FrameUnits::Rows => BoundFrameUnits::Rows,
        FrameUnits::Range => BoundFrameUnits::Range,
        FrameUnits::Groups => BoundFrameUnits::Groups,
    };

    let start = bind_frame_bound(&frame.start, schema, catalog, cte_catalog, scope)?;
    let end = bind_frame_bound(&frame.end, schema, catalog, cte_catalog, scope)?;
    let exclude = match frame.exclude {
        FrameExclusion::NoOthers => BoundFrameExclusion::NoOthers,
        FrameExclusion::CurrentRow => BoundFrameExclusion::CurrentRow,
        FrameExclusion::Group => BoundFrameExclusion::Group,
        FrameExclusion::Ties => BoundFrameExclusion::Ties,
    };

    validate_frame(units, &start, &end, order_by, schema)?;

    Ok(LogicalWindowFrame {
        units,
        start,
        end,
        exclude,
    })
}

/// Bind a parser [`FrameBound`] into a [`BoundFrameBound`], lowering any
/// offset expression to a [`ScalarExpr`]. The offset is not range-checked
/// here (negative/NULL checks are execution-time per the SQL spec); but
/// references to output columns / window functions / aggregates would be
/// rejected by `bind_expr_with_ctes`.
fn bind_frame_bound(
    bound: &FrameBound,
    schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<BoundFrameBound, PlanError> {
    Ok(match bound {
        FrameBound::UnboundedPreceding => BoundFrameBound::UnboundedPreceding,
        FrameBound::CurrentRow => BoundFrameBound::CurrentRow,
        FrameBound::UnboundedFollowing => BoundFrameBound::UnboundedFollowing,
        FrameBound::Preceding(expr) => BoundFrameBound::Preceding(bind_expr_with_ctes(
            expr,
            schema,
            catalog,
            cte_catalog,
            scope,
        )?),
        FrameBound::Following(expr) => BoundFrameBound::Following(bind_expr_with_ctes(
            expr,
            schema,
            catalog,
            cte_catalog,
            scope,
        )?),
    })
}

/// Validate a bound frame against the SQL window-frame rules (the §4
/// error table). Negative/NULL offset checks are deferred to execution.
fn validate_frame(
    units: BoundFrameUnits,
    start: &BoundFrameBound,
    end: &BoundFrameBound,
    order_by: &[SortKey],
    schema: &Schema,
) -> Result<(), PlanError> {
    // UNBOUNDED FOLLOWING may not start a frame; UNBOUNDED PRECEDING may
    // not end one.
    if matches!(start, BoundFrameBound::UnboundedFollowing) {
        return Err(PlanError::InvalidWindowFrame(
            "frame start cannot be UNBOUNDED FOLLOWING".to_string(),
        ));
    }
    if matches!(end, BoundFrameBound::UnboundedPreceding) {
        return Err(PlanError::InvalidWindowFrame(
            "frame end cannot be UNBOUNDED PRECEDING".to_string(),
        ));
    }

    // Start-after-end ordering rules, detectable from bound kinds.
    if matches!(start, BoundFrameBound::Following(_))
        && matches!(
            end,
            BoundFrameBound::Preceding(_) | BoundFrameBound::CurrentRow
        )
    {
        return Err(PlanError::InvalidWindowFrame(
            "frame starting from following row cannot have preceding rows".to_string(),
        ));
    }
    if matches!(start, BoundFrameBound::CurrentRow) && matches!(end, BoundFrameBound::Preceding(_)) {
        return Err(PlanError::InvalidWindowFrame(
            "frame starting from current row cannot have preceding rows".to_string(),
        ));
    }

    // RANGE with a value offset requires exactly one ORDER BY column of a
    // numeric type (date/time/interval offsets are deferred).
    let has_offset = matches!(
        start,
        BoundFrameBound::Preceding(_) | BoundFrameBound::Following(_)
    ) || matches!(
        end,
        BoundFrameBound::Preceding(_) | BoundFrameBound::Following(_)
    );
    if matches!(units, BoundFrameUnits::Range) && has_offset {
        if order_by.len() != 1 {
            return Err(PlanError::InvalidWindowFrame(
                "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column"
                    .to_string(),
            ));
        }
        let order_type = order_by[0].expr.data_type();
        let numeric = matches!(
            order_type,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal { .. }
        );
        if !numeric {
            // Date/time/interval RANGE offsets are an explicit deferral.
            let _ = schema;
            return Err(PlanError::not_supported(format!(
                "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {order_type:?}"
            )));
        }
    }

    // GROUPS mode requires an ORDER BY clause.
    if matches!(units, BoundFrameUnits::Groups) && order_by.is_empty() {
        return Err(PlanError::InvalidWindowFrame(
            "GROUPS mode requires an ORDER BY clause".to_string(),
        ));
    }

    Ok(())
}

/// Map a function name + argument list to a [`LogicalWindowFunc`].
fn resolve_window_func(
    func_name: &str,
    args: &[Expr],
    input_schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalWindowFunc, PlanError> {
    match func_name {
        "row_number" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "row_number() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::RowNumber)
        }
        "rank" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "rank() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::Rank)
        }
        "dense_rank" => {
            if !args.is_empty() {
                return Err(PlanError::TypeMismatch(
                    "dense_rank() takes no arguments".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::DenseRank)
        }
        "lag" | "lead" => {
            if args.is_empty() || args.len() > 3 {
                return Err(PlanError::TypeMismatch(format!(
                    "{func_name}: expected 1, 2, or 3 arguments, got {}",
                    args.len()
                )));
            }
            let expr = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
            let offset = if args.len() >= 2 {
                bind_usize_literal(
                    &args[1],
                    func_name,
                    1,
                    input_schema,
                    catalog,
                    cte_catalog,
                    scope,
                )?
            } else {
                1
            };
            let default = if args.len() == 3 {
                let bound =
                    bind_expr_with_ctes(&args[2], input_schema, catalog, cte_catalog, scope)?;
                extract_literal_value(&bound).ok_or_else(|| {
                    PlanError::TypeMismatch(format!(
                        "{func_name}: default argument must be a literal"
                    ))
                })?
            } else {
                Value::Null
            };
            Ok(if func_name == "lag" {
                LogicalWindowFunc::Lag {
                    expr,
                    offset,
                    default,
                }
            } else {
                LogicalWindowFunc::Lead {
                    expr,
                    offset,
                    default,
                }
            })
        }
        "first_value" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "first_value: expected 1 argument, got {}",
                    args.len()
                )));
            }
            Ok(LogicalWindowFunc::FirstValue(bind_expr_with_ctes(
                &args[0],
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?))
        }
        "last_value" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "last_value: expected 1 argument, got {}",
                    args.len()
                )));
            }
            Ok(LogicalWindowFunc::LastValue(bind_expr_with_ctes(
                &args[0],
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?))
        }
        "nth_value" => {
            if args.len() != 2 {
                return Err(PlanError::TypeMismatch(format!(
                    "nth_value: expected 2 arguments, got {}",
                    args.len()
                )));
            }
            let expr = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
            let n = bind_usize_literal(
                &args[1],
                "nth_value",
                1,
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
            if n == 0 {
                return Err(PlanError::TypeMismatch(
                    "nth_value: n must be ≥ 1".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::NthValue { expr, n })
        }
        "ntile" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "ntile: expected 1 argument, got {}",
                    args.len()
                )));
            }
            let n = bind_usize_literal(
                &args[0],
                "ntile",
                0,
                input_schema,
                catalog,
                cte_catalog,
                scope,
            )?;
            if n == 0 {
                return Err(PlanError::TypeMismatch(
                    "ntile: bucket count must be ≥ 1".to_string(),
                ));
            }
            Ok(LogicalWindowFunc::Ntile(n))
        }
        "count" if args.is_empty() || is_star_arg(args) => Ok(LogicalWindowFunc::CountStar),
        "sum" | "avg" | "count" | "min" | "max" => {
            if args.len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "{func_name}: expected 1 argument, got {}",
                    args.len()
                )));
            }
            let expr = bind_expr_with_ctes(&args[0], input_schema, catalog, cte_catalog, scope)?;
            let kind = match func_name {
                "sum" => WindowAggKind::Sum,
                "avg" => WindowAggKind::Avg,
                "count" => WindowAggKind::Count,
                "min" => WindowAggKind::Min,
                "max" => WindowAggKind::Max,
                // Unreachable: the outer match arm gates these five names.
                _ => unreachable!("aggregate window func name already matched"),
            };
            Ok(LogicalWindowFunc::Aggregate { kind, expr })
        }
        other => Err(PlanError::not_supported(format!(
            "window function '{other}'"
        ))),
    }
}

/// `true` when `args` is the single `*` wildcard argument of `count(*)`.
fn is_star_arg(args: &[Expr]) -> bool {
    args.len() == 1
        && matches!(&args[0], Expr::Column { name }
            if name.parts.len() == 1 && name.parts[0].value == "*")
}

/// Fold a bound expression into a `Value` if it represents a constant
/// the binder can evaluate at plan time. Handles bare `Literal` nodes
/// plus `Unary { Neg, Literal(int) }` so SQL like `lag(x, 1, -1)`
/// parses correctly (the parser does not pre-fold the negation).
fn extract_literal_value(expr: &ScalarExpr) -> Option<Value> {
    match expr {
        ScalarExpr::Literal { value, .. } => Some(value.clone()),
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: inner,
            ..
        } => match inner.as_ref() {
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => v.checked_neg().map(Value::Int32),
            ScalarExpr::Literal {
                value: Value::Int64(v),
                ..
            } => v.checked_neg().map(Value::Int64),
            ScalarExpr::Literal {
                value: Value::Float64(v),
                ..
            } => Some(Value::Float64(-v)),
            ScalarExpr::Literal {
                value: Value::Decimal { value, scale },
                ..
            } => value.checked_neg().map(|value| Value::Decimal {
                value,
                scale: *scale,
            }),
            ScalarExpr::Literal {
                value: Value::Money(v),
                ..
            } => v.checked_neg().map(Value::Money),
            _ => None,
        },
        _ => None,
    }
}

fn bind_usize_literal(
    expr: &Expr,
    func_name: &str,
    arg_index: usize,
    schema: &Schema,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<usize, PlanError> {
    match bind_expr_with_ctes(expr, schema, catalog, cte_catalog, scope)? {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            ..
        } if v >= 0 => usize::try_from(v).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{func_name}: argument {arg_index} value {v} exceeds usize range"
            ))
        }),
        ScalarExpr::Literal {
            value: Value::Int64(v),
            ..
        } if v >= 0 => usize::try_from(v).map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{func_name}: argument {arg_index} value {v} exceeds usize range"
            ))
        }),
        _ => Err(PlanError::TypeMismatch(format!(
            "{func_name}: argument {arg_index} must be a non-negative integer literal"
        ))),
    }
}

/// Return the [`DataType`] of the column appended by a window function.
fn window_func_result_type(func: &LogicalWindowFunc) -> DataType {
    use crate::plan::AggregateFunc;
    match func {
        LogicalWindowFunc::RowNumber
        | LogicalWindowFunc::Rank
        | LogicalWindowFunc::DenseRank
        | LogicalWindowFunc::Ntile(_)
        | LogicalWindowFunc::CountStar => DataType::Int64,
        LogicalWindowFunc::Lag { expr, .. }
        | LogicalWindowFunc::Lead { expr, .. }
        | LogicalWindowFunc::FirstValue(expr)
        | LogicalWindowFunc::LastValue(expr)
        | LogicalWindowFunc::NthValue { expr, .. } => expr.data_type(),
        LogicalWindowFunc::Aggregate { kind, expr } => {
            let agg = match kind {
                WindowAggKind::Sum => AggregateFunc::Sum,
                WindowAggKind::Avg => AggregateFunc::Avg,
                WindowAggKind::Count => AggregateFunc::Count,
                WindowAggKind::Min => AggregateFunc::Min,
                WindowAggKind::Max => AggregateFunc::Max,
            };
            aggregate_return_type(agg, expr.data_type())
        }
    }
}
