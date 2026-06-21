//! PostgreSQL advisory-lock SQL function surface.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::Value;
use ultrasql_parser::ast::{
    Expr as AstExpr, Literal as AstLiteral, SelectItem, Statement, UnaryOp,
};
use ultrasql_protocol::{BackendMessage, FieldDescription};

use super::Session;
use crate::error::ServerError;
use crate::result_encoder::SelectResult;

const PG_OID_BOOL: u32 = 16;
const PG_OID_VOID: u32 = 2278;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn try_dispatch_advisory_lock_select(
        &mut self,
        stmt: &Statement,
    ) -> Result<Option<SelectResult>, ServerError> {
        let Some((name, args, output_name)) = simple_advisory_call(stmt) else {
            return Ok(None);
        };
        if matches!(self.txn_state, crate::TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }
        let values = advisory_ast_args(args, &name)?;
        let value = self.advisory_state.evaluate_function(
            &name,
            &values,
            &self.state.txn_manager.lock_manager,
        )?;
        let result = match name.as_str() {
            "pg_advisory_lock" | "pg_advisory_unlock_all" => void_select_result(output_name),
            "pg_try_advisory_lock" | "pg_advisory_unlock" => bool_select_result(output_name, value),
            _ => return Ok(None),
        };
        Ok(Some(result))
    }
}

fn simple_advisory_call(stmt: &Statement) -> Option<(String, &[AstExpr], String)> {
    let Statement::Select(select) = stmt else {
        return None;
    };
    if !select.from.is_empty()
        || select.r#where.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || !select.set_ops.is_empty()
        || !select.ctes.is_empty()
        || !select.locking.is_empty()
        || select.projection.len() != 1
    {
        return None;
    }
    let SelectItem::Expr { expr, alias, .. } = &select.projection[0] else {
        return None;
    };
    let AstExpr::Call { name, args, .. } = expr else {
        return None;
    };
    let func = name.parts.last()?.value.to_ascii_lowercase();
    if !matches!(
        func.as_str(),
        "pg_advisory_lock"
            | "pg_try_advisory_lock"
            | "pg_advisory_unlock"
            | "pg_advisory_unlock_all"
    ) {
        return None;
    }
    let output = alias
        .as_ref()
        .map_or_else(|| func.clone(), |a| a.value.clone());
    Some((func, args.as_slice(), output))
}

fn advisory_ast_args(args: &[AstExpr], name: &str) -> Result<Vec<Value>, ServerError> {
    args.iter().map(|arg| advisory_ast_i64(arg, name)).collect()
}

fn advisory_ast_i64(expr: &AstExpr, name: &str) -> Result<Value, ServerError> {
    match expr {
        AstExpr::Literal(AstLiteral::Integer { text, .. }) => text
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| advisory_arg_error(name)),
        AstExpr::Literal(AstLiteral::Null { .. }) => Ok(Value::Null),
        AstExpr::Unary {
            op: UnaryOp::Neg,
            expr,
            ..
        } => {
            let Value::Int64(value) = advisory_ast_i64(expr, name)? else {
                return Ok(Value::Null);
            };
            value
                .checked_neg()
                .map(Value::Int64)
                .ok_or_else(|| advisory_arg_error(name))
        }
        _ => Err(advisory_arg_error(name)),
    }
}

fn advisory_arg_error(name: &str) -> ServerError {
    ServerError::Execute(ultrasql_executor::ExecError::TypeMismatch(format!(
        "{name}: advisory lock arguments must be integer literals on the simple-query fast path",
    )))
}

fn void_select_result(name: String) -> SelectResult {
    SelectResult {
        messages: vec![
            BackendMessage::RowDescription {
                fields: vec![FieldDescription {
                    name,
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid: PG_OID_VOID,
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                }],
            },
            BackendMessage::DataRow {
                columns: vec![Some(Vec::new())],
            },
            BackendMessage::CommandComplete {
                tag: "SELECT 1".to_owned(),
            },
        ],
        streamed_body: None,
        shared_streamed_body: None,
        streaming: None,
        rows: 1,
    }
}

fn bool_select_result(name: String, value: Value) -> SelectResult {
    let column = match value {
        Value::Bool(true) => Some(b"t".to_vec()),
        Value::Bool(false) => Some(b"f".to_vec()),
        Value::Null => None,
        _ => None,
    };
    SelectResult {
        messages: vec![
            BackendMessage::RowDescription {
                fields: vec![FieldDescription {
                    name,
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid: PG_OID_BOOL,
                    type_size: 1,
                    type_modifier: -1,
                    format_code: 0,
                }],
            },
            BackendMessage::DataRow {
                columns: vec![column],
            },
            BackendMessage::CommandComplete {
                tag: "SELECT 1".to_owned(),
            },
        ],
        streamed_body: None,
        shared_streamed_body: None,
        streaming: None,
        rows: 1,
    }
}
