//! Simple-Query handlers for `PREPARE` / `EXECUTE` / `DEALLOCATE`.
//!
//! These statements manipulate the per-session prepared-statement
//! cache that the Extended Query path also owns
//! ([`crate::extended::ExtendedConnState::statements`]). They are
//! dispatched from [`super::execute::Session::execute_query`]
//! *before* binding — the binder rejects these AST variants because
//! they have no [`LogicalPlan`] of their own.
//!
//! # Semantics
//!
//! - `PREPARE name [(types)] AS stmt` — bind `stmt`, store under
//!   `name`. Re-preparing under a name that already exists is an
//!   error in PostgreSQL (SQLSTATE `42P05`); we follow that.
//! - `EXECUTE name [(args)]` — look up `name`, evaluate each
//!   literal-shaped arg into a [`Value`], substitute the values
//!   into the plan's `Parameter` nodes, and run the substituted
//!   plan through the normal DML/SELECT dispatch. Argument count
//!   must match the prepared plan's `n_params`.
//! - `DEALLOCATE name` — remove `name` from the cache.
//! - `DEALLOCATE ALL` — drain every prepared statement.
//!
//! # Cross-path consistency
//!
//! The cache is the same `HashMap` the Extended Query Parse/Bind
//! pair uses, so a name created via `PREPARE` is visible to a
//! subsequent Extended-Query `Bind` referencing it (and vice
//! versa). This matches PostgreSQL.

use std::sync::Arc;

use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::Value;
use ultrasql_parser::ast::{DeallocateStmt, ExecuteStmt, Expr, Literal, PrepareStmt, Statement};
use ultrasql_planner::{LogicalPlan, ScalarExpr, bind};
use ultrasql_protocol::BackendMessage;

use crate::CombinedCatalog;
use crate::error::ServerError;
use crate::extended::{PreparedStatement, substitute_parameters_in_plan};
use crate::result_encoder::SelectResult;

use super::Session;

impl<RW> Session<RW>
where
    RW: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    /// Inspect `stmt` and dispatch the three meta statements.
    ///
    /// Returns `Ok(Some(result))` when the statement was handled,
    /// `Ok(None)` when it is some other shape that should fall
    /// through to the regular bind+execute path.
    pub(crate) fn try_dispatch_meta_statement(
        &mut self,
        stmt: &Statement,
        catalog_snapshot: Arc<CatalogSnapshot>,
    ) -> Result<Option<SelectResult>, ServerError> {
        match stmt {
            Statement::Prepare(p) => {
                Ok(Some(self.execute_prepare_statement(p, &catalog_snapshot)?))
            }
            Statement::Execute(e) => Ok(Some(self.execute_execute_statement(e)?)),
            Statement::Deallocate(d) => Ok(Some(self.execute_deallocate_statement(d))),
            _ => Ok(None),
        }
    }

    fn execute_prepare_statement(
        &mut self,
        stmt: &PrepareStmt,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        let name = stmt.name.value.clone();
        if self.extended.statements.contains_key(&name) {
            return Err(ServerError::Plan(
                ultrasql_planner::PlanError::TypeMismatch(format!(
                    "prepared statement \"{name}\" already exists"
                )),
            ));
        }
        let combined = CombinedCatalog {
            snapshot: catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let plan = bind(&stmt.statement, &combined)?;
        let plan_hash = crate::workload::plan_hash_for_plan(&plan);
        let n_params = max_param_index(&plan);
        self.extended.statements.insert(
            name,
            PreparedStatement {
                sql: String::new(),
                plan: Some(plan),
                plan_hash,
                param_type_oids: Vec::new(),
                n_params,
                limit_offset_param_indexes: Vec::new(),
            },
        );
        Ok(simple_tag("PREPARE"))
    }

    fn execute_execute_statement(
        &mut self,
        stmt: &ExecuteStmt,
    ) -> Result<SelectResult, ServerError> {
        let name = stmt.name.value.clone();
        let prepared = self
            .extended
            .statements
            .get(&name)
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "prepared statement \"{name}\" does not exist"
                )))
            })?
            .clone();
        let Some(plan) = prepared.plan else {
            return Ok(simple_tag("EXECUTE"));
        };

        let expected = prepared_param_count(prepared.n_params)?;
        if stmt.args.len() != expected {
            return Err(ServerError::Plan(
                ultrasql_planner::PlanError::TypeMismatch(format!(
                    "EXECUTE \"{name}\": wrong number of arguments ({} given, {expected} expected)",
                    stmt.args.len()
                )),
            ));
        }

        let mut values: Vec<Value> = Vec::with_capacity(expected);
        for arg in &stmt.args {
            values.push(eval_literal_arg(arg)?);
        }

        let substituted = substitute_parameters_in_plan(&plan, &values);
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        self.run_dml_or_select(&substituted, &catalog_snapshot)
    }

    fn execute_deallocate_statement(&mut self, stmt: &DeallocateStmt) -> SelectResult {
        if stmt.all {
            self.extended.statements.clear();
        } else if let Some(name) = &stmt.name {
            // PostgreSQL's DEALLOCATE on an unknown name is a no-op
            // with a notice; we silently drop to keep the failure path
            // minimal.
            self.extended.statements.remove(&name.value);
        }
        simple_tag("DEALLOCATE")
    }
}

/// Build a no-op `SelectResult` carrying just a `CommandComplete`
/// tag — used for `PREPARE` and `DEALLOCATE`, which produce no
/// rows.
fn simple_tag(tag: &str) -> SelectResult {
    SelectResult {
        messages: vec![BackendMessage::CommandComplete {
            tag: tag.to_string(),
        }],
        streamed_body: None,
        shared_streamed_body: None,
        rows: 0,
    }
}

/// Best-effort literal evaluation of an `EXECUTE` argument from the
/// parser AST.
///
/// Accepts `Literal::*`, parenthesised literals, and a unary `-`
/// over an integer/float literal so users can pass negative numbers
/// without quoting. PostgreSQL's `EXECUTE` only accepts constant
/// expressions for its arguments too; the matching error is
/// surfaced as a plan-level TypeMismatch.
fn eval_literal_arg(arg: &Expr) -> Result<Value, ServerError> {
    match arg {
        Expr::Literal(lit) => literal_to_value(lit),
        Expr::Paren { expr, .. } => eval_literal_arg(expr),
        Expr::Unary {
            op: ultrasql_parser::ast::UnaryOp::Neg,
            expr,
            ..
        } => match eval_literal_arg(expr)? {
            Value::Int32(v) => Ok(Value::Int32(-v)),
            Value::Int64(v) => Ok(Value::Int64(-v)),
            Value::Float32(v) => Ok(Value::Float32(-v)),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            other => Err(literal_arg_error(format!(
                "cannot negate value of type {:?}",
                other.data_type()
            ))),
        },
        _ => Err(literal_arg_error(
            "EXECUTE arguments must be literal constants".into(),
        )),
    }
}

fn literal_to_value(lit: &Literal) -> Result<Value, ServerError> {
    match lit {
        Literal::Null { .. } => Ok(Value::Null),
        Literal::Bool { value, .. } => Ok(Value::Bool(*value)),
        Literal::Integer { text, .. } => {
            // Match the binder's integer-typing rule: fits in i32 → Int32
            // else Int64.
            if let Ok(v) = text.parse::<i32>() {
                Ok(Value::Int32(v))
            } else if let Ok(v) = text.parse::<i64>() {
                Ok(Value::Int64(v))
            } else {
                Err(literal_arg_error(format!(
                    "integer literal {text} out of range"
                )))
            }
        }
        Literal::Float { text, .. } => text
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| literal_arg_error(format!("float literal {text}: {e}"))),
        Literal::String { value, .. } => Ok(Value::Text(value.clone())),
        // `Literal` is `#[non_exhaustive]`; reject any future variant.
        _ => Err(literal_arg_error(
            "EXECUTE arg: unsupported literal kind".into(),
        )),
    }
}

fn literal_arg_error(msg: String) -> ServerError {
    ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(msg))
}

fn prepared_param_count(n_params: u32) -> Result<usize, ServerError> {
    usize::try_from(n_params).map_err(|_| {
        ServerError::Unsupported("prepared statement parameter count exceeds platform limit")
    })
}

/// Return the highest `$N` index referenced anywhere in `plan`, or
/// `0` if the plan has no parameters.
fn max_param_index(plan: &LogicalPlan) -> u32 {
    let mut max_idx: u32 = 0;
    walk_plan_for_max_param(plan, &mut max_idx);
    max_idx
}

fn walk_plan_for_max_param(plan: &LogicalPlan, max_idx: &mut u32) {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
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
        | LogicalPlan::Truncate { .. }
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
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::FunctionScan { .. } => {}
        LogicalPlan::Filter { input, predicate } => {
            walk_plan_for_max_param(input, max_idx);
            walk_expr_for_max_param(predicate, max_idx);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            walk_plan_for_max_param(input, max_idx);
            for (e, _) in exprs {
                walk_expr_for_max_param(e, max_idx);
            }
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::LockRows { input, .. } => {
            walk_plan_for_max_param(input, max_idx)
        }
        LogicalPlan::Sort { input, keys } => {
            walk_plan_for_max_param(input, max_idx);
            for k in keys {
                walk_expr_for_max_param(&k.expr, max_idx);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            walk_plan_for_max_param(input, max_idx);
            for g in group_by {
                walk_expr_for_max_param(g, max_idx);
            }
            for a in aggregates {
                if let Some(arg) = &a.arg {
                    walk_expr_for_max_param(arg, max_idx);
                }
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            walk_plan_for_max_param(left, max_idx);
            walk_plan_for_max_param(right, max_idx);
            if let ultrasql_planner::LogicalJoinCondition::On(expr) = condition {
                walk_expr_for_max_param(expr, max_idx);
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            walk_plan_for_max_param(left, max_idx);
            walk_plan_for_max_param(right, max_idx);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            walk_plan_for_max_param(definition, max_idx);
            walk_plan_for_max_param(body, max_idx);
        }
        LogicalPlan::Values { rows, .. } => {
            for r in rows {
                for e in r {
                    walk_expr_for_max_param(e, max_idx);
                }
            }
        }
        LogicalPlan::Insert { source, .. } => walk_plan_for_max_param(source, max_idx),
        LogicalPlan::Update {
            input, assignments, ..
        } => {
            walk_plan_for_max_param(input, max_idx);
            for (_idx, expr) in assignments {
                walk_expr_for_max_param(expr, max_idx);
            }
        }
        LogicalPlan::Delete { input, .. } => walk_plan_for_max_param(input, max_idx),
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            func,
            ..
        } => {
            walk_plan_for_max_param(input, max_idx);
            for e in partition_by {
                walk_expr_for_max_param(e, max_idx);
            }
            for k in order_by {
                walk_expr_for_max_param(&k.expr, max_idx);
            }
            match func {
                ultrasql_planner::LogicalWindowFunc::Lag { expr, .. }
                | ultrasql_planner::LogicalWindowFunc::Lead { expr, .. }
                | ultrasql_planner::LogicalWindowFunc::FirstValue(expr)
                | ultrasql_planner::LogicalWindowFunc::LastValue(expr)
                | ultrasql_planner::LogicalWindowFunc::NthValue { expr, .. } => {
                    walk_expr_for_max_param(expr, max_idx);
                }
                _ => {}
            }
        }
        LogicalPlan::CreatePolicy { .. } => {}
    }
}

fn walk_expr_for_max_param(expr: &ScalarExpr, max_idx: &mut u32) {
    match expr {
        ScalarExpr::Parameter { index, .. } => {
            if *index > *max_idx {
                *max_idx = *index;
            }
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::OuterColumn { .. } => {
        }
        ScalarExpr::Unary { expr, .. } => walk_expr_for_max_param(expr, max_idx),
        ScalarExpr::Binary { left, right, .. } => {
            walk_expr_for_max_param(left, max_idx);
            walk_expr_for_max_param(right, max_idx);
        }
        ScalarExpr::IsNull { expr, .. } => walk_expr_for_max_param(expr, max_idx),
        ScalarExpr::ScalarSubquery { subplan, .. } => walk_plan_for_max_param(subplan, max_idx),
        ScalarExpr::Exists { subplan, .. } => walk_plan_for_max_param(subplan, max_idx),
        ScalarExpr::InSubquery { expr, subplan, .. } => {
            walk_expr_for_max_param(expr, max_idx);
            walk_plan_for_max_param(subplan, max_idx);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for a in args {
                walk_expr_for_max_param(a, max_idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_parser::span::Span;
    use ultrasql_planner::{
        AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType,
        LogicalSetOp, LogicalSetQuantifier, LogicalWindowFunc, SortKey,
    };

    fn span() -> Span {
        Span::default()
    }

    fn param(index: u32) -> ScalarExpr {
        ScalarExpr::Parameter {
            index,
            data_type: DataType::Int32,
        }
    }

    fn schema() -> Schema {
        Schema::new([Field::required("x", DataType::Int32)]).expect("schema")
    }

    fn values_with_param(index: u32) -> LogicalPlan {
        LogicalPlan::Values {
            rows: vec![vec![param(index)]],
            schema: schema(),
        }
    }

    #[test]
    fn prepared_param_count_uses_checked_platform_conversion() {
        assert_eq!(prepared_param_count(0).expect("zero"), 0);
        assert_eq!(prepared_param_count(2).expect("two"), 2);
    }

    #[test]
    fn literal_execute_args_cover_scalars_negation_and_errors() {
        assert_eq!(
            literal_to_value(&Literal::Null { span: span() }).expect("null"),
            Value::Null
        );
        assert_eq!(
            literal_to_value(&Literal::Bool {
                value: true,
                span: span()
            })
            .expect("bool"),
            Value::Bool(true)
        );
        assert_eq!(
            literal_to_value(&Literal::Integer {
                text: "42".to_owned(),
                span: span()
            })
            .expect("i32"),
            Value::Int32(42)
        );
        assert!(matches!(
            literal_to_value(&Literal::Integer {
                text: i64::MAX.to_string(),
                span: span()
            })
            .expect("i64"),
            Value::Int64(_)
        ));
        assert!(
            literal_to_value(&Literal::Integer {
                text: "999999999999999999999999999999".to_owned(),
                span: span()
            })
            .is_err()
        );
        assert_eq!(
            eval_literal_arg(&Expr::Paren {
                expr: Box::new(Expr::Unary {
                    op: ultrasql_parser::ast::UnaryOp::Neg,
                    expr: Box::new(Expr::Literal(Literal::Float {
                        text: "2.5".to_owned(),
                        span: span(),
                    })),
                    span: span(),
                }),
                span: span(),
            })
            .expect("negative float"),
            Value::Float64(-2.5)
        );
        assert!(
            eval_literal_arg(&Expr::Unary {
                op: ultrasql_parser::ast::UnaryOp::Neg,
                expr: Box::new(Expr::Literal(Literal::String {
                    value: "x".to_owned(),
                    span: span(),
                })),
                span: span(),
            })
            .is_err()
        );
        assert!(
            eval_literal_arg(&Expr::Parameter {
                index: 1,
                span: span()
            })
            .is_err()
        );
        assert!(
            literal_to_value(&Literal::Float {
                text: "nope".to_owned(),
                span: span()
            })
            .is_err()
        );
    }

    #[test]
    fn max_param_index_walks_nested_plan_and_expression_shapes() {
        let filter = LogicalPlan::Filter {
            input: Box::new(values_with_param(1)),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(param(2)),
                right: Box::new(ScalarExpr::FunctionCall {
                    name: "coalesce".to_owned(),
                    args: vec![param(3)],
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Bool,
            },
        };
        let project = LogicalPlan::Project {
            input: Box::new(filter),
            exprs: vec![(
                ScalarExpr::IsNull {
                    expr: Box::new(param(4)),
                    negated: true,
                },
                "p".to_owned(),
            )],
            schema: schema(),
        };
        let sorted = LogicalPlan::Sort {
            input: Box::new(project),
            keys: vec![SortKey {
                expr: param(5),
                asc: true,
                nulls_first: false,
            }],
        };
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(sorted),
            group_by: vec![param(6)],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::Sum,
                arg: Some(ScalarExpr::ScalarSubquery {
                    subplan: Box::new(values_with_param(7)),
                    correlated: false,
                    data_type: DataType::Int32,
                }),
                direct_arg: None,
                order_by: None,
                distinct: false,
                output_name: "s".to_owned(),
                data_type: DataType::Int64,
            }],
            schema: schema(),
        };
        let joined = LogicalPlan::Join {
            left: Box::new(aggregate),
            right: Box::new(values_with_param(8)),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(param(9)),
            schema: schema(),
        };
        let set_op = LogicalPlan::SetOp {
            op: LogicalSetOp::Union,
            quantifier: LogicalSetQuantifier::All,
            left: Box::new(joined),
            right: Box::new(values_with_param(10)),
            schema: schema(),
        };
        let cte = LogicalPlan::Cte {
            name: "r".to_owned(),
            recursive: false,
            definition: Box::new(values_with_param(11)),
            body: Box::new(set_op),
            schema: schema(),
        };
        let window = LogicalPlan::Window {
            input: Box::new(cte),
            partition_by: vec![param(12)],
            order_by: vec![SortKey {
                expr: param(13),
                asc: true,
                nulls_first: false,
            }],
            func: LogicalWindowFunc::Lag {
                expr: ScalarExpr::InSubquery {
                    expr: Box::new(param(14)),
                    subplan: Box::new(values_with_param(15)),
                    negated: false,
                    correlated: true,
                    data_type: DataType::Int32,
                },
                offset: 1,
                default: Value::Null,
            },
            output_name: "lag".to_owned(),
            schema: schema(),
        };
        let update = LogicalPlan::Update {
            table: "t".to_owned(),
            assignments: vec![(0, param(16))],
            input: Box::new(window),
            returning: vec![],
            schema: Schema::empty(),
        };

        assert_eq!(max_param_index(&update), 16);
        assert_eq!(
            max_param_index(&LogicalPlan::Empty {
                schema: Schema::empty()
            }),
            0
        );
        assert_eq!(simple_tag("DEALLOCATE").rows, 0);
    }
}
