//! Sequence DDL and SQL function surface.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::{RelationId, Xid};
use ultrasql_parser::ast::{
    Expr as AstExpr, Literal as AstLiteral, SelectItem, Statement, UnaryOp,
};
use ultrasql_planner::{LogicalPlan, LogicalSequenceChange, LogicalSequenceOptions};
use ultrasql_protocol::{BackendMessage, FieldDescription};
use ultrasql_storage::sequence::{Sequence, SequenceOptions};
use ultrasql_wal::payload::SequenceOpKind;

use super::Session;
use crate::TxnState;
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};

const PG_OID_INT8: u32 = 20;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_create_sequence(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateSequence {
            sequence_name,
            options,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_sequence called with non-CreateSequence plan",
            ));
        };
        if self.state.sequences.contains_key(sequence_name) {
            if *if_not_exists {
                return Ok(result_encoder::run_ddl_command("CREATE SEQUENCE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(sequence_name.clone()),
            ));
        }
        let seq = Sequence::new(to_storage_options(*options))
            .map_err(|e| ServerError::ddl(format!("CREATE SEQUENCE: {e}")))?;
        let seq_oid = self.state.persistent_catalog.next_oid();
        let seq_rel = RelationId::new(seq_oid.raw());
        let seq_opts = seq.options_snapshot();
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let seq_row = ultrasql_catalog::persistent::SequenceRow {
            seqrelid: seq_oid,
            seqtypid: PG_OID_INT8,
            seqstart: seq_opts.start,
            seqincrement: seq_opts.increment,
            seqmax: seq_opts.max.unwrap_or(i64::MAX),
            seqmin: seq_opts.min.unwrap_or(1),
            seqcache: i64::from(seq_opts.cache),
            seqcycle: seq_opts.cycle,
        };
        if let Err(e) = self.state.persistent_catalog.persist_sequence_rows(
            sequence_name,
            &seq_row,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_sequence_rows error",
                );
            }
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE SEQUENCE catalog transaction")?;
        seq.emit_wal(
            SequenceOpKind::Create,
            sequence_name,
            seq_rel,
            self.sequence_xid(),
            self.sequence_wal_sink(),
        )
        .map_err(|e| ServerError::ddl(format!("CREATE SEQUENCE WAL: {e}")))?;
        self.state
            .sequences
            .insert(sequence_name.clone(), Arc::new(seq));
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("CREATE SEQUENCE"))
    }

    pub(crate) fn execute_alter_sequence(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterSequence {
            sequence_name,
            options,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_sequence called with non-AlterSequence plan",
            ));
        };
        let seq = self
            .state
            .sequences
            .get(sequence_name)
            .ok_or_else(|| {
                ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                    sequence_name.clone(),
                ))
            })?
            .clone();
        let (storage, restart_value) = apply_sequence_change(seq.options_snapshot(), *options);
        seq.alter_options_logged(
            storage,
            restart_value,
            sequence_name,
            RelationId::INVALID,
            self.sequence_xid(),
            self.sequence_wal_sink(),
        )
        .map_err(|e| ServerError::ddl(format!("ALTER SEQUENCE: {e}")))?;
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("ALTER SEQUENCE"))
    }

    pub(crate) fn execute_drop_sequence(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropSequence {
            sequences,
            if_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_sequence called with non-DropSequence plan",
            ));
        };
        for name in sequences {
            let existing = self.state.sequences.get(name).map(|seq| seq.clone());
            if let Some(seq) = existing {
                seq.emit_wal(
                    SequenceOpKind::Drop,
                    name,
                    RelationId::INVALID,
                    self.sequence_xid(),
                    self.sequence_wal_sink(),
                )
                .map_err(|e| ServerError::ddl(format!("DROP SEQUENCE WAL: {e}")))?;
            }
            if self.state.sequences.remove(name).is_none() && !*if_exists {
                return Err(ServerError::Catalog(
                    ultrasql_catalog::CatalogError::not_found(name.clone()),
                ));
            }
            self.sequence_state.forget(name);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP SEQUENCE"))
    }

    pub(crate) fn try_dispatch_sequence_select(
        &mut self,
        stmt: &Statement,
    ) -> Result<Option<SelectResult>, ServerError> {
        let Some((name, args, output_name)) = simple_sequence_call(stmt) else {
            return Ok(None);
        };
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }
        let value = match name.as_str() {
            "nextval" => self.sequence_nextval(args)?,
            "currval" => self.sequence_currval(args)?,
            "lastval" => self.sequence_lastval(args)?,
            "setval" => self.sequence_setval(args)?,
            _ => return Ok(None),
        };
        Ok(Some(int8_select_result(output_name, value)))
    }

    fn sequence_nextval(&mut self, args: &[AstExpr]) -> Result<i64, ServerError> {
        expect_arity(args, 1)?;
        let seq_name = expect_sequence_name(&args[0])?;
        let seq = self.sequence_by_name(&seq_name)?;
        let value = seq
            .nextval_logged(
                &seq_name,
                RelationId::INVALID,
                self.sequence_xid(),
                self.sequence_wal_sink(),
            )
            .map_err(|e| ServerError::ddl(format!("nextval: {e}")))?;
        self.sequence_state.record_nextval(&seq_name, value);
        Ok(value)
    }

    fn sequence_currval(&self, args: &[AstExpr]) -> Result<i64, ServerError> {
        expect_arity(args, 1)?;
        let seq_name = expect_sequence_name(&args[0])?;
        self.sequence_by_name(&seq_name)?;
        self.sequence_state.currval(&seq_name).ok_or_else(|| {
            ServerError::ObjectNotInPrerequisiteState(
                "currval of sequence called before nextval".to_owned(),
            )
        })
    }

    fn sequence_lastval(&self, args: &[AstExpr]) -> Result<i64, ServerError> {
        if !args.is_empty() {
            return Err(ServerError::Unsupported("lastval takes no arguments"));
        }
        let Some((seq_name, value)) = self.sequence_state.lastval() else {
            return Err(ServerError::ObjectNotInPrerequisiteState(
                "lastval is not yet defined in this session".to_owned(),
            ));
        };
        self.sequence_by_name(&seq_name)?;
        Ok(value)
    }

    fn sequence_setval(&mut self, args: &[AstExpr]) -> Result<i64, ServerError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(ServerError::Unsupported("setval expects 2 or 3 arguments"));
        }
        let seq_name = expect_sequence_name(&args[0])?;
        let value = expect_i64(&args[1])?;
        let is_called = if args.len() == 3 {
            expect_bool(&args[2])?
        } else {
            true
        };
        let seq = self.sequence_by_name(&seq_name)?;
        seq.setval_logged(
            value,
            is_called,
            &seq_name,
            RelationId::INVALID,
            self.sequence_xid(),
            self.sequence_wal_sink(),
        )
        .map_err(|e| ServerError::ddl(format!("setval: {e}")))?;
        if is_called {
            self.sequence_state.record_nextval(&seq_name, value);
        }
        Ok(value)
    }

    fn sequence_by_name(&self, name: &str) -> Result<Arc<Sequence>, ServerError> {
        self.state
            .sequences
            .get(name)
            .map(|seq| seq.clone())
            .ok_or_else(|| {
                ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(name.to_owned()))
            })
    }

    fn sequence_xid(&self) -> Xid {
        match &self.txn_state {
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => txn.current_xid(),
            TxnState::Idle => Xid::INVALID,
        }
    }

    fn sequence_wal_sink(&self) -> Option<&dyn ultrasql_storage::WalSink> {
        self.state.heap.wal_sink().map(|sink| sink.as_ref())
    }
}

fn simple_sequence_call(stmt: &Statement) -> Option<(String, &[AstExpr], String)> {
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
    if !matches!(func.as_str(), "nextval" | "currval" | "lastval" | "setval") {
        return None;
    }
    let output = alias
        .as_ref()
        .map_or_else(|| func.clone(), |a| a.value.clone());
    Some((func, args.as_slice(), output))
}

fn expect_arity(args: &[AstExpr], expected_len: usize) -> Result<(), ServerError> {
    if args.len() != expected_len {
        return Err(ServerError::Unsupported(
            "sequence function called with wrong arity",
        ));
    }
    Ok(())
}

fn expect_sequence_name(expr: &AstExpr) -> Result<String, ServerError> {
    let AstExpr::Literal(AstLiteral::String { value, .. }) = expr else {
        return Err(ServerError::Unsupported(
            "sequence name must be a string literal",
        ));
    };
    Ok(value.to_ascii_lowercase())
}

fn expect_i64(expr: &AstExpr) -> Result<i64, ServerError> {
    match expr {
        AstExpr::Literal(AstLiteral::Integer { text, .. }) => text
            .parse::<i64>()
            .map_err(|_| ServerError::Unsupported("integer literal out of range")),
        AstExpr::Unary {
            op: UnaryOp::Neg,
            expr,
            ..
        } => expect_i64(expr).and_then(|v| {
            v.checked_neg()
                .ok_or(ServerError::Unsupported("integer literal out of range"))
        }),
        _ => Err(ServerError::Unsupported(
            "setval value must be an integer literal",
        )),
    }
}

fn expect_bool(expr: &AstExpr) -> Result<bool, ServerError> {
    let AstExpr::Literal(AstLiteral::Bool { value, .. }) = expr else {
        return Err(ServerError::Unsupported(
            "setval is_called must be a boolean literal",
        ));
    };
    Ok(*value)
}

pub(crate) fn to_storage_options(options: LogicalSequenceOptions) -> SequenceOptions {
    SequenceOptions {
        start: options.start,
        increment: options.increment,
        min: options.min,
        max: options.max,
        cache: options.cache,
        cycle: options.cycle,
    }
}

fn apply_sequence_change(
    mut current: SequenceOptions,
    change: LogicalSequenceChange,
) -> (SequenceOptions, Option<i64>) {
    if let Some(start) = change.start {
        current.start = start;
    }
    if let Some(increment) = change.increment {
        current.increment = increment;
    }
    if let Some(min) = change.min {
        current.min = min;
    }
    if let Some(max) = change.max {
        current.max = max;
    }
    if let Some(cache) = change.cache {
        current.cache = cache;
    }
    if let Some(cycle) = change.cycle {
        current.cycle = cycle;
    }
    let restart_value = change.restart.map(|value| value.unwrap_or(current.start));
    (current, restart_value)
}

fn int8_select_result(name: String, value: i64) -> SelectResult {
    SelectResult {
        messages: vec![
            BackendMessage::RowDescription {
                fields: vec![FieldDescription {
                    name,
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid: PG_OID_INT8,
                    type_size: 8,
                    type_modifier: -1,
                    format_code: 0,
                }],
            },
            BackendMessage::DataRow {
                columns: vec![Some(value.to_string().into_bytes())],
            },
            BackendMessage::CommandComplete {
                tag: "SELECT 1".to_owned(),
            },
        ],
        streamed_body: None,
        shared_streamed_body: None,
        rows: 1,
    }
}
