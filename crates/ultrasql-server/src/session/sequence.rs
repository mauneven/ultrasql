//! Sequence DDL and SQL function surface.

use std::collections::HashSet;
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
use crate::auth::{PrivilegeKind, PrivilegeObjectKind};
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};

const PG_OID_INT8: u32 = 20;

fn display_sequence_target(sequence_name: &str, namespace: Option<&str>) -> String {
    namespace.map_or_else(
        || sequence_name.to_owned(),
        |namespace| format!("{namespace}.{sequence_name}"),
    )
}

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
            namespace,
            options,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_sequence called with non-CreateSequence plan",
            ));
        };
        let sequence_key = crate::sequence_lookup_key(namespace, sequence_name);
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        if self.state.sequences.contains_key(&sequence_key) {
            if *if_not_exists {
                return Ok(result_encoder::run_ddl_command("CREATE SEQUENCE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(sequence_key),
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
            namespace,
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
            &sequence_key,
            seq_rel,
            self.sequence_xid(),
            self.sequence_wal_sink(),
        )
        .map_err(|e| ServerError::ddl(format!("CREATE SEQUENCE WAL: {e}")))?;
        self.state
            .sequences
            .insert(sequence_key.clone(), Arc::new(seq));
        self.state
            .sequence_owners
            .insert(sequence_key.clone(), self.current_user.to_ascii_lowercase());
        self.state
            .sequence_namespaces
            .insert(sequence_key.clone(), namespace.to_ascii_lowercase());
        if let Err(err) = self.state.persist_sequence_owner_metadata() {
            self.state.sequence_owners.remove(&sequence_key);
            self.state.sequence_namespaces.remove(&sequence_key);
            self.state.sequences.remove(&sequence_key);
            return Err(err);
        }
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Sequence,
            sequence_name,
        );
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("CREATE SEQUENCE"))
    }

    pub(crate) fn execute_alter_sequence(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterSequence {
            sequence_name,
            namespace,
            options,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_sequence called with non-AlterSequence plan",
            ));
        };
        let sequence_key =
            self.ensure_sequence_namespace_matches(sequence_name, namespace.as_deref())?;
        let seq = self
            .state
            .sequences
            .get(&sequence_key)
            .ok_or_else(|| {
                ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                    sequence_key.clone(),
                ))
            })?
            .clone();
        self.ensure_sequence_owner_or_superuser(&sequence_key)?;
        let (storage, restart_value) = apply_sequence_change(seq.options_snapshot(), *options);
        seq.alter_options_logged(
            storage,
            restart_value,
            &sequence_key,
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
            sequence_namespaces,
            if_exists,
            cascade,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_sequence called with non-DropSequence plan",
            ));
        };
        let mut drop_set = HashSet::with_capacity(sequences.len());
        for (idx, name) in sequences.iter().enumerate() {
            let namespace = sequence_namespaces
                .get(idx)
                .and_then(std::option::Option::as_deref);
            if let Some(sequence_key) = self.sequence_key_for_ddl(name, namespace) {
                drop_set.insert(sequence_key);
            } else if !*if_exists {
                return Err(ServerError::Catalog(
                    ultrasql_catalog::CatalogError::not_found(display_sequence_target(
                        name, namespace,
                    )),
                ));
            }
        }
        if drop_set.is_empty() {
            return Ok(result_encoder::run_ddl_command("DROP SEQUENCE"));
        }
        for name in &drop_set {
            self.ensure_sequence_owner_or_superuser(name)?;
        }
        if *cascade {
            let before_constraints = self.table_runtime_constraint_snapshot();
            if self.detach_sequence_default_dependencies(&drop_set)
                && let Err(err) = self.state.persist_table_runtime_constraints_metadata()
            {
                self.restore_table_runtime_constraints(before_constraints);
                return Err(err);
            }
        } else {
            for name in &drop_set {
                let dependents = self.sequence_default_dependents(name);
                if !dependents.is_empty() {
                    return Err(ServerError::DependentObjectsStillExist(format!(
                        "cannot drop sequence {name} because other objects depend on it: {}",
                        dependents.join(", ")
                    )));
                }
            }
        }
        let mut privilege_grants_removed = false;
        for name in &drop_set {
            if let Some(seq) = self.state.sequences.get(name).map(|seq| seq.clone()) {
                seq.emit_wal(
                    SequenceOpKind::Drop,
                    name,
                    RelationId::INVALID,
                    self.sequence_xid(),
                    self.sequence_wal_sink(),
                )
                .map_err(|e| ServerError::ddl(format!("DROP SEQUENCE WAL: {e}")))?;
            }
            self.state.sequences.remove(name);
            self.state.sequence_owners.remove(name);
            self.state.sequence_namespaces.remove(name);
            self.sequence_state.forget(name);
            privilege_grants_removed |= self
                .state
                .privilege_catalog
                .remove_object_grants(crate::auth::PrivilegeObjectKind::Sequence, name);
        }
        if privilege_grants_removed {
            self.state.persist_privilege_metadata()?;
        }
        self.state.persist_sequence_owner_metadata()?;
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP SEQUENCE"))
    }

    fn ensure_sequence_namespace_matches(
        &self,
        sequence_name: &str,
        namespace: Option<&str>,
    ) -> Result<String, ServerError> {
        self.sequence_key_for_ddl(sequence_name, namespace)
            .ok_or_else(|| {
                ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                    display_sequence_target(sequence_name, namespace),
                ))
            })
    }

    fn sequence_key_for_ddl(&self, sequence_name: &str, namespace: Option<&str>) -> Option<String> {
        if let Some(namespace) = namespace {
            let key = crate::sequence_lookup_key(namespace, sequence_name);
            return self.state.sequences.contains_key(&key).then_some(key);
        }
        self.sequence_key_for_reference(sequence_name)
    }

    fn sequence_key_for_reference(&self, sequence_name: &str) -> Option<String> {
        let folded = sequence_name.to_ascii_lowercase();
        let parts = crate::parse_pg_identifier_path(&folded)?;
        match parts.as_slice() {
            [name] => {
                if self.state.sequences.contains_key(name) {
                    return Some(name.clone());
                }
                crate::search_path_schema_names(
                    self.session_settings.get("search_path").map(String::as_str),
                )
                .into_iter()
                .map(|namespace| crate::sequence_lookup_key(&namespace, name))
                .find(|key| self.state.sequences.contains_key(key))
            }
            [namespace, name] => {
                let key = crate::sequence_lookup_key(namespace, name);
                self.state.sequences.contains_key(&key).then_some(key)
            }
            _ => None,
        }
    }

    fn sequence_default_dependents(&self, sequence_name: &str) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut out = Vec::new();
        for item in self.state.table_constraints.iter() {
            if item
                .value()
                .sequence_defaults
                .iter()
                .flatten()
                .any(|name| name.eq_ignore_ascii_case(sequence_name))
            {
                let table_name = snapshot.tables_by_oid.get(item.key()).map_or_else(
                    || format!("relation {}", item.key().raw()),
                    |table| table.name.clone(),
                );
                out.push(format!("{table_name} column default"));
            }
        }
        out.sort();
        out
    }

    fn detach_sequence_default_dependencies(&self, drop_set: &HashSet<String>) -> bool {
        let mut updates = Vec::new();
        for item in self.state.table_constraints.iter() {
            let mut next = item.value().as_ref().clone();
            let mut changed = false;
            for default in &mut next.sequence_defaults {
                if default
                    .as_ref()
                    .is_some_and(|name| drop_set.contains(&name.to_ascii_lowercase()))
                {
                    *default = None;
                    changed = true;
                }
            }
            if changed {
                updates.push((*item.key(), Arc::new(next)));
            }
        }
        let changed = !updates.is_empty();
        for (oid, constraints) in updates {
            self.state.table_constraints.insert(oid, constraints);
        }
        changed
    }

    fn table_runtime_constraint_snapshot(
        &self,
    ) -> Vec<(ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>)> {
        self.state
            .table_constraints
            .iter()
            .map(|item| (*item.key(), Arc::clone(item.value())))
            .collect()
    }

    fn restore_table_runtime_constraints(
        &self,
        snapshot: Vec<(ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>)>,
    ) {
        for (oid, constraints) in snapshot {
            self.state.table_constraints.insert(oid, constraints);
        }
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
        let seq_key = self.sequence_key_for_reference(&seq_name).ok_or_else(|| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(seq_name.clone()))
        })?;
        let seq = self.sequence_by_name(&seq_key)?;
        self.ensure_sequence_function_privilege(
            &seq_key,
            &[PrivilegeKind::Usage, PrivilegeKind::Update],
        )?;
        let value = seq
            .nextval_logged(
                &seq_key,
                RelationId::INVALID,
                self.sequence_xid(),
                self.sequence_wal_sink(),
            )
            .map_err(|e| ServerError::ddl(format!("nextval: {e}")))?;
        self.sequence_state.record_nextval(&seq_key, value);
        Ok(value)
    }

    fn sequence_currval(&self, args: &[AstExpr]) -> Result<i64, ServerError> {
        expect_arity(args, 1)?;
        let seq_name = expect_sequence_name(&args[0])?;
        let seq_key = self.sequence_key_for_reference(&seq_name).ok_or_else(|| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(seq_name.clone()))
        })?;
        self.sequence_by_name(&seq_key)?;
        self.ensure_sequence_function_privilege(
            &seq_key,
            &[PrivilegeKind::Usage, PrivilegeKind::Select],
        )?;
        self.sequence_state.currval(&seq_key).ok_or_else(|| {
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
        self.ensure_sequence_function_privilege(
            &seq_name,
            &[PrivilegeKind::Usage, PrivilegeKind::Select],
        )?;
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
        let seq_key = self.sequence_key_for_reference(&seq_name).ok_or_else(|| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(seq_name.clone()))
        })?;
        let seq = self.sequence_by_name(&seq_key)?;
        self.ensure_sequence_function_privilege(&seq_key, &[PrivilegeKind::Update])?;
        seq.setval_logged(
            value,
            is_called,
            &seq_key,
            RelationId::INVALID,
            self.sequence_xid(),
            self.sequence_wal_sink(),
        )
        .map_err(|e| ServerError::ddl(format!("setval: {e}")))?;
        if is_called {
            self.sequence_state.record_nextval(&seq_key, value);
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

    fn ensure_sequence_function_privilege(
        &self,
        sequence_name: &str,
        privileges: &[PrivilegeKind],
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let sequence_key = sequence_name.to_ascii_lowercase();
        let schema_name = self
            .state
            .sequence_namespaces
            .get(&sequence_key)
            .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
        self.ensure_schema_usage_privilege(&schema_name)?;
        let owns_sequence = self
            .state
            .sequence_owners
            .get(&sequence_key)
            .is_some_and(|owner| owner.eq_ignore_ascii_case(&current_user));
        if owns_sequence {
            return Ok(());
        }
        let roles = self.state.role_catalog.inherited_role_names(&current_user);
        if privileges.iter().any(|privilege| {
            self.state.privilege_catalog.has_privilege_for_roles(
                &roles,
                PrivilegeObjectKind::Sequence,
                &sequence_key,
                *privilege,
            )
        }) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(format!(
            "permission denied for sequence {sequence_name}"
        )))
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
