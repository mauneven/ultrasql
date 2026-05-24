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

        let expected = prepared.n_params as usize;
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
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::AlterRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::GrantPrivileges { .. }
        | LogicalPlan::RevokePrivileges { .. }
        | LogicalPlan::AlterDefaultPrivileges { .. }
        | LogicalPlan::GrantRole { .. }
        | LogicalPlan::RevokeRole { .. }
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
