//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::collections::HashSet;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    Catalog, CatalogSnapshot, CompositeTypeEntry, DomainTypeEntry, EnumLabelEntry, EnumTypeEntry,
    IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalCommentTarget,
    LogicalIndexMethod, LogicalIndexOption, LogicalPlan, LogicalRlsCommand,
    LogicalRlsPermissiveness, TableMeta, bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::access_method::{
    AccessMethod, AnnPayloadKind, BrinIndex, HnswMetric, PageBackedHnswIndex,
    PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};
use ultrasql_wal::payload::SequenceOpKind;

use super::Session;
use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn,
};

const COLUMN_COLLATION_OPTION_PREFIX: &str = "ultrasql.attcollation.";

const PG_OID_INT8: u32 = 20;

struct CreateIndexProgressGuard<'a> {
    recorder: &'a crate::workload::WorkloadRecorder,
    pid: u32,
}

impl<'a> CreateIndexProgressGuard<'a> {
    fn new(
        recorder: &'a crate::workload::WorkloadRecorder,
        pid: u32,
        relid: u32,
        index_relid: u32,
        blocks_total: u32,
    ) -> Self {
        recorder.begin_create_index(pid, relid, index_relid, blocks_total);
        Self { recorder, pid }
    }

    fn update(&self, phase: &'static str, blocks_done: u32) {
        self.recorder
            .update_create_index(self.pid, phase, blocks_done);
    }
}

impl Drop for CreateIndexProgressGuard<'_> {
    fn drop(&mut self) {
        self.recorder.finish_create_index(self.pid);
    }
}

fn rewrite_domain_value_expr(
    expr: &ultrasql_planner::ScalarExpr,
    column_index: usize,
    column_name: &str,
    base_type: &DataType,
) -> ultrasql_planner::ScalarExpr {
    use ultrasql_planner::ScalarExpr;

    match expr {
        ScalarExpr::Column { index: 0, .. } => ScalarExpr::Column {
            name: column_name.to_owned(),
            index: column_index,
            data_type: base_type.clone(),
        },
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(rewrite_domain_value_expr(
                expr,
                column_index,
                column_name,
                base_type,
            )),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => ScalarExpr::Binary {
            op: *op,
            left: Box::new(rewrite_domain_value_expr(
                left,
                column_index,
                column_name,
                base_type,
            )),
            right: Box::new(rewrite_domain_value_expr(
                right,
                column_index,
                column_name,
                base_type,
            )),
            data_type: data_type.clone(),
        },
        ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
            expr: Box::new(rewrite_domain_value_expr(
                expr,
                column_index,
                column_name,
                base_type,
            )),
            negated: *negated,
        },
        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => ScalarExpr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| rewrite_domain_value_expr(arg, column_index, column_name, base_type))
                .collect(),
            data_type: data_type.clone(),
        },
        other => other.clone(),
    }
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Register a row-level security policy for one table.
    pub(crate) fn execute_create_policy(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreatePolicy { policy, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_create_policy called with non-CreatePolicy plan",
            ));
        };
        let entry = snapshot.tables.get(&policy.table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                policy.table_name.clone(),
            ))
        })?;
        self.ensure_table_owner_or_superuser(entry.oid, &policy.table_name)?;
        let previous = self
            .state
            .row_security
            .get(&entry.oid)
            .map(|guard| guard.clone());
        let mut runtime = previous
            .as_ref()
            .map(|existing| existing.as_ref().clone())
            .unwrap_or_default();
        if runtime.owner_role.is_empty() {
            runtime.owner_role = self.current_user.to_ascii_lowercase();
        }
        if runtime
            .policies
            .iter()
            .any(|existing| existing.name == policy.policy_name)
        {
            return Err(ServerError::ddl(format!(
                "row-level security policy '{}' already exists on {}",
                policy.policy_name, policy.table_name
            )));
        }
        let mut roles = policy.roles.clone();
        roles.sort();
        roles.dedup();
        for role in &roles {
            if role != "public" && self.state.role_catalog.lookup_role(role).is_none() {
                return Err(ServerError::ddl(format!("role \"{role}\" does not exist")));
            }
        }
        runtime.policies.push(crate::RuntimeRlsPolicy {
            name: policy.policy_name.clone(),
            permissiveness: match policy.permissiveness {
                LogicalRlsPermissiveness::Permissive => crate::RuntimeRlsPermissiveness::Permissive,
                LogicalRlsPermissiveness::Restrictive => {
                    crate::RuntimeRlsPermissiveness::Restrictive
                }
            },
            command: match policy.command {
                LogicalRlsCommand::All => crate::RuntimeRlsCommand::All,
                LogicalRlsCommand::Select => crate::RuntimeRlsCommand::Select,
                LogicalRlsCommand::Insert => crate::RuntimeRlsCommand::Insert,
                LogicalRlsCommand::Update => crate::RuntimeRlsCommand::Update,
                LogicalRlsCommand::Delete => crate::RuntimeRlsCommand::Delete,
            },
            roles,
            using: policy
                .using
                .as_ref()
                .map(|expr| crate::RuntimeTenantPolicyExpr {
                    column_index: expr.column_index,
                    column_name: expr.column_name.clone(),
                    setting_name: expr.setting_name.clone(),
                }),
            with_check: policy
                .with_check
                .as_ref()
                .map(|expr| crate::RuntimeTenantPolicyExpr {
                    column_index: expr.column_index,
                    column_name: expr.column_name.clone(),
                    setting_name: expr.setting_name.clone(),
                }),
        });
        self.state.row_security.insert(entry.oid, Arc::new(runtime));
        if let Err(e) = self.state.persist_row_security_metadata() {
            if let Some(previous) = previous {
                self.state.row_security.insert(entry.oid, previous);
            } else {
                self.state.row_security.remove(&entry.oid);
            }
            return Err(e);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE POLICY"))
    }

    /// Persist a `CREATE TYPE name AS ENUM (...)` declaration.
    pub(crate) fn execute_create_type_enum(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTypeEnum {
            type_name,
            namespace,
            labels,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_type_enum called with non-CreateTypeEnum plan",
            ));
        };
        if snapshot.enum_types.contains_key(type_name)
            || snapshot.composite_types.contains_key(type_name)
            || snapshot.tables.contains_key(type_name)
        {
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(type_name.clone()),
            ));
        }
        let type_oid = self.state.persistent_catalog.next_oid();
        let mut label_entries = Vec::with_capacity(labels.len());
        for (idx, label) in labels.iter().enumerate() {
            let sort_order = u32::try_from(idx + 1)
                .map_err(|_| ServerError::ddl("CREATE TYPE enum label count overflow"))?;
            label_entries.push(EnumLabelEntry {
                oid: self.state.persistent_catalog.next_oid(),
                label: label.clone(),
                sort_order,
            });
        }
        let entry = EnumTypeEntry {
            oid: type_oid,
            name: type_name.clone(),
            schema_name: namespace.clone(),
            labels: label_entries,
        };
        self.state
            .persistent_catalog
            .create_enum_type(entry.clone())?;
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let persist_result = self.state.persistent_catalog.persist_enum_type_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        );
        if let Err(e) = persist_result {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_enum_type_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_enum_type(type_name);
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE TYPE catalog-write transaction")?;
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TYPE"))
    }

    /// Persist a `CREATE TYPE name AS (...)` composite declaration.
    pub(crate) fn execute_create_type_composite(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTypeComposite {
            type_name,
            namespace,
            attributes,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_type_composite called with non-CreateTypeComposite plan",
            ));
        };
        if snapshot.enum_types.contains_key(type_name)
            || snapshot.composite_types.contains_key(type_name)
            || snapshot.tables.contains_key(type_name)
        {
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(type_name.clone()),
            ));
        }
        let entry = CompositeTypeEntry {
            oid: self.state.persistent_catalog.next_oid(),
            name: type_name.clone(),
            schema_name: namespace.clone(),
            schema: attributes.clone(),
        };
        self.state
            .persistent_catalog
            .create_composite_type(entry.clone())?;
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let persist_result = self.state.persistent_catalog.persist_composite_type_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        );
        if let Err(e) = persist_result {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_composite_type_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_composite_type(type_name);
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE TYPE catalog-write transaction")?;
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TYPE"))
    }

    /// Persist a `CREATE DOMAIN name AS base_type ...` declaration.
    pub(crate) fn execute_create_domain(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateDomain {
            domain_name,
            namespace,
            base_type,
            not_null,
            checks,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_domain called with non-CreateDomain plan",
            ));
        };
        if snapshot.enum_types.contains_key(domain_name)
            || snapshot.composite_types.contains_key(domain_name)
            || snapshot.domain_types.contains_key(domain_name)
            || snapshot.tables.contains_key(domain_name)
        {
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(domain_name.clone()),
            ));
        }
        if crate::data_type_token(base_type).is_none() {
            return Err(ServerError::ddl(format!(
                "CREATE DOMAIN base type {base_type} is outside restart-persistable subset"
            )));
        }
        let entry = DomainTypeEntry {
            oid: self.state.persistent_catalog.next_oid(),
            name: domain_name.clone(),
            schema_name: namespace.clone(),
            base_type: base_type.clone(),
            not_null: *not_null,
        };
        self.state
            .persistent_catalog
            .create_domain_type(entry.clone())?;
        let runtime = Arc::new(crate::DomainRuntimeConstraints {
            base_type: base_type.clone(),
            not_null: *not_null,
            checks: checks
                .iter()
                .map(|check| crate::RuntimeCheckConstraint {
                    name: check.name.clone(),
                    expr: check.expr.clone(),
                })
                .collect(),
        });
        self.state.domain_constraints.insert(entry.oid, runtime);
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let persist_result = self.state.persistent_catalog.persist_domain_type_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        );
        if let Err(e) = persist_result {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_domain_type_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_domain_type(domain_name);
            self.state.domain_constraints.remove(&entry.oid);
            return Err(e.into());
        }
        if let Err(e) = self.state.persist_domain_runtime_constraints_metadata() {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after domain-runtime metadata error",
                );
            }
            let _ = self.state.persistent_catalog.drop_domain_type(domain_name);
            self.state.domain_constraints.remove(&entry.oid);
            return Err(e);
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE DOMAIN catalog-write transaction")?;
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE DOMAIN"))
    }

    /// Register a user-defined operator in the runtime catalog.
    pub(crate) fn execute_create_operator(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateOperator {
            operator_name,
            namespace,
            left_type,
            right_type,
            procedure,
            result_type,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_operator called with non-CreateOperator plan",
            ));
        };
        let key =
            crate::runtime_operator_signature(namespace, operator_name, left_type, right_type);
        let operator = crate::RuntimeOperator {
            oid: crate::runtime_operator_oid(&key),
            name: operator_name.clone(),
            namespace: namespace.clone(),
            left_type: left_type.clone(),
            right_type: right_type.clone(),
            procedure: procedure.clone(),
            result_type: result_type.clone(),
        };
        match self.state.operators.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err(ServerError::ddl(format!(
                    "operator '{}' already exists for declared argument types",
                    operator_name
                )));
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(Arc::new(operator));
            }
        }
        if let Err(err) = self.state.persist_operator_metadata() {
            self.state.operators.remove(&key);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE OPERATOR"))
    }

    fn domain_checks_for_columns(
        &self,
        columns: &ultrasql_core::Schema,
    ) -> Result<Vec<crate::RuntimeCheckConstraint>, ServerError> {
        let mut out = Vec::new();
        for (idx, field) in columns.fields().iter().enumerate() {
            let DataType::Domain {
                oid,
                name,
                base_type,
                ..
            } = &field.data_type
            else {
                continue;
            };
            let runtime = self.state.domain_constraints.get(oid).ok_or_else(|| {
                ServerError::ddl(format!(
                    "domain metadata for '{}' was not loaded before CREATE TABLE",
                    name
                ))
            })?;
            for check in &runtime.checks {
                out.push(crate::RuntimeCheckConstraint {
                    name: format!("{}_{}", field.name, check.name),
                    expr: rewrite_domain_value_expr(&check.expr, idx, &field.name, base_type),
                });
            }
        }
        Ok(out)
    }

    /// Persist a `CREATE TABLE` into the catalog.
    ///
    /// Honors `IF NOT EXISTS` by short-circuiting when the relation
    /// already exists in either the persistent snapshot or the
    /// in-memory sample catalog. The resolved column [`Schema`] from
    /// the binder is stored verbatim, so a subsequent statement that
    /// captures a fresh snapshot will see the new relation.
    ///
    /// Currently a metadata-only operation: the segment file and the
    /// `pg_class.relfilenode` block are allocated lazily on the first
    /// `INSERT`. This matches PostgreSQL's `RelationSetNewRelfilenode`
    /// timing closely enough that subsequent `INSERT` wiring (in a
    /// follow-up commit) can stamp the right block number then.
    pub(crate) fn execute_create_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            columns,
            column_collations,
            defaults,
            sequence_defaults,
            sequence_options,
            identity_always,
            generated_stored,
            checks,
            unique_constraints,
            foreign_keys,
            exclusion_constraints,
            partition,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_table called with non-CreateTable plan",
            ));
        };
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let oid = self.state.persistent_catalog.next_oid();
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone())
            .with_options(column_collation_options(column_collations));
        for unique in unique_constraints {
            crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &unique.columns)?;
        }
        let serial_sequences: Vec<(String, ultrasql_planner::LogicalSequenceOptions)> =
            sequence_defaults
                .iter()
                .zip(sequence_options)
                .filter_map(|(name, options)| {
                    name.as_ref()
                        .map(|name| (name.clone(), options.unwrap_or_default()))
                })
                .collect();
        for (seq_name, _) in &serial_sequences {
            if self.state.sequences.contains_key(seq_name) {
                return Err(ServerError::Catalog(
                    ultrasql_catalog::CatalogError::already_exists(seq_name.clone()),
                ));
            }
        }
        self.state.persistent_catalog.create_table(entry.clone())?;
        let mut serial_sequence_rows = Vec::with_capacity(serial_sequences.len());
        for (seq_name, options) in &serial_sequences {
            let seq = ultrasql_storage::sequence::Sequence::new(
                super::sequence::to_storage_options(*options),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence: {e}")))?;
            let seq_oid = self.state.persistent_catalog.next_oid();
            let seq_rel = RelationId::new(seq_oid.raw());
            let seq_opts = seq.options_snapshot();
            serial_sequence_rows.push((
                seq_name.clone(),
                ultrasql_catalog::persistent::SequenceRow {
                    seqrelid: seq_oid,
                    seqtypid: PG_OID_INT8,
                    seqstart: seq_opts.start,
                    seqincrement: seq_opts.increment,
                    seqmax: seq_opts.max.unwrap_or(i64::MAX),
                    seqmin: seq_opts.min.unwrap_or(1),
                    seqcache: i64::from(seq_opts.cache),
                    seqcycle: seq_opts.cycle,
                },
            ));
            seq.emit_wal(
                SequenceOpKind::Create,
                seq_name,
                seq_rel,
                ultrasql_core::Xid::INVALID,
                self.state.heap.wal_sink().map(|sink| sink.as_ref()),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence WAL: {e}")))?;
            self.state.sequences.insert(seq_name.clone(), Arc::new(seq));
        }
        let runtime_foreign_keys = foreign_keys
            .iter()
            .map(|fk| {
                let target = snapshot.tables.get(&fk.target_table).ok_or_else(|| {
                    ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                        fk.target_table.clone(),
                    ))
                })?;
                Ok(crate::RuntimeForeignKeyConstraint {
                    name: fk.name.clone(),
                    columns: fk.columns.clone(),
                    target_table: fk.target_table.clone(),
                    target_oid: target.oid,
                    target_columns: fk.target_columns.clone(),
                    on_delete: fk.on_delete,
                    on_update: fk.on_update,
                    deferrable: fk.deferrable,
                    initially_deferred: fk.initially_deferred,
                })
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        let runtime_exclusion_constraints = exclusion_constraints
            .iter()
            .map(|constraint| crate::RuntimeExclusionConstraint {
                name: constraint.name.clone(),
                method: constraint.method,
                elements: constraint
                    .elements
                    .iter()
                    .map(|element| crate::RuntimeExclusionElement {
                        column: element.column,
                        op: element.op,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let mut runtime_checks = checks
            .iter()
            .map(|check| crate::RuntimeCheckConstraint {
                name: check.name.clone(),
                expr: check.expr.clone(),
            })
            .collect::<Vec<_>>();
        runtime_checks.extend(self.domain_checks_for_columns(columns)?);
        let mut persistent_constraint_rows = Vec::with_capacity(
            unique_constraints.len()
                + runtime_checks.len()
                + runtime_foreign_keys.len()
                + runtime_exclusion_constraints.len(),
        );
        for unique in unique_constraints {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: unique.name.clone(),
                conrelid: oid,
                contype: if unique.primary_key {
                    ultrasql_catalog::persistent::ConType::PrimaryKey
                } else {
                    ultrasql_catalog::persistent::ConType::Unique
                },
                condeferrable: false,
                condeferred: false,
                conkey: constraint_attnums(&unique.columns, &unique.name)?,
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        for check in &runtime_checks {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: check.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::Check,
                condeferrable: false,
                condeferred: false,
                conkey: Vec::new(),
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        for fk in &runtime_foreign_keys {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: fk.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::ForeignKey,
                condeferrable: fk.deferrable,
                condeferred: fk.initially_deferred,
                conkey: constraint_attnums(&fk.columns, &fk.name)?,
                confrelid: fk.target_oid,
                confkey: constraint_attnums(&fk.target_columns, &fk.name)?,
            });
        }
        for exclusion in &runtime_exclusion_constraints {
            let columns = exclusion
                .elements
                .iter()
                .map(|element| element.column)
                .collect::<Vec<_>>();
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: exclusion.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::Exclusion,
                condeferrable: false,
                condeferred: false,
                conkey: constraint_attnums(&columns, &exclusion.name)?,
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        if defaults.iter().any(Option::is_some)
            || sequence_defaults.iter().any(Option::is_some)
            || identity_always.iter().any(|v| *v)
            || generated_stored.iter().any(Option::is_some)
            || !runtime_checks.is_empty()
            || !runtime_foreign_keys.is_empty()
            || !runtime_exclusion_constraints.is_empty()
        {
            self.state.table_constraints.insert(
                oid,
                Arc::new(crate::TableRuntimeConstraints {
                    defaults: defaults.clone(),
                    sequence_defaults: sequence_defaults.clone(),
                    identity_always: identity_always.clone(),
                    generated_stored: generated_stored.clone(),
                    checks: runtime_checks.clone(),
                    foreign_keys: runtime_foreign_keys.clone(),
                    exclusion_constraints: runtime_exclusion_constraints.clone(),
                    indexes: std::collections::HashMap::new(),
                }),
            );
        }
        let created_unique_indexes =
            match self.create_table_unique_indexes(&entry, unique_constraints) {
                Ok(indexes) => indexes,
                Err(e) => {
                    let _ = self.state.persistent_catalog.drop_table(table_name);
                    self.state.table_constraints.remove(&oid);
                    for (seq_name, _) in &serial_sequences {
                        self.state.sequences.remove(seq_name);
                    }
                    return Err(e);
                }
            };
        let attr_has_defaults: Vec<bool> = (0..columns.len())
            .map(|idx| {
                defaults.get(idx).is_some_and(Option::is_some)
                    || sequence_defaults.get(idx).is_some_and(Option::is_some)
                    || identity_always.get(idx).copied().unwrap_or(false)
                    || generated_stored.get(idx).is_some_and(Option::is_some)
            })
            .collect();
        // Persist the typed pg_class + pg_attribute rows so a restart
        // can rebuild this `TableEntry` via
        // `PersistentCatalog::bootstrap_from_heap`. The DDL runs in an
        // autocommit transaction allocated on the spot; the rows are
        // stamped with that xid so MVCC visibility lines up with the
        // user-table relations created in the same statement.
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let persist_result = (|| -> Result<(), ultrasql_catalog::CatalogError> {
            self.state
                .persistent_catalog
                .persist_table_rows_with_defaults(
                    &entry,
                    &attr_has_defaults,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            for index in &created_unique_indexes {
                self.state.persistent_catalog.persist_index_rows(
                    index,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            }
            for row in &persistent_constraint_rows {
                self.state.persistent_catalog.persist_constraint_row(
                    row,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            }
            for (seq_name, row) in &serial_sequence_rows {
                self.state.persistent_catalog.persist_sequence_rows(
                    seq_name,
                    row,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            }
            Ok(())
        })();
        if let Err(e) = persist_result {
            // Abort the catalog-write txn before surfacing the error so
            // the CLOG entry is closed and the rollback path cleans
            // any partial in-place undo entries (there are none for
            // pg_class inserts, but symmetry matters for future
            // expansion).
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_table_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_table(table_name);
            self.state.table_constraints.remove(&oid);
            for (seq_name, _) in &serial_sequences {
                self.state.sequences.remove(seq_name);
            }
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE TABLE catalog-write transaction")?;
        self.state
            .persistent_catalog
            .install_constraint_rows(persistent_constraint_rows);
        let mut row_security = self
            .state
            .row_security
            .get(&oid)
            .map(|guard| guard.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        self.state.persist_table_runtime_constraints_metadata()?;
        self.state.persist_row_security_metadata()?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Table,
            table_name,
        );
        for (seq_name, _) in &serial_sequences {
            self.state.privilege_catalog.apply_default_privileges(
                &self.current_user,
                namespace,
                crate::auth::PrivilegeObjectKind::Sequence,
                seq_name,
            );
        }
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        if let Some(partition) = partition {
            self.state.time_partitions.insert(
                table_name.clone(),
                Arc::new(crate::time_partition::TimePartitionRuntime::daily(
                    table_name.clone(),
                    oid,
                    columns.clone(),
                    partition.column.clone(),
                    partition.column_index,
                )),
            );
        }
        // A new relation can shadow names a cached plan rewrote against
        // the previous snapshot; clear the cache so the next statement
        // re-plans.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TABLE"))
    }

    /// Persist and populate an append-only materialized view.
    pub(crate) fn execute_create_materialized_view(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateMaterializedView {
            table_name,
            namespace,
            columns,
            source,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_materialized_view called with wrong plan",
            ));
        };
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE MATERIALIZED VIEW"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let Some(source_table) = crate::append_only_materialized_source_table(source) else {
            return Err(ServerError::Unsupported(
                "CREATE MATERIALIZED VIEW supports append-only SELECT/FILTER/PROJECT over one table",
            ));
        };

        let oid = self.state.persistent_catalog.next_oid();
        let mut entry =
            TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        entry.options.push((
            "ultrasql.relkind".to_owned(),
            "materialized_view".to_owned(),
        ));
        self.state.persistent_catalog.create_table(entry.clone())?;

        let runtime = Arc::new(crate::MaterializedViewRuntime {
            view_table: table_name.clone(),
            source_table: source_table.to_ascii_lowercase(),
            source: source.as_ref().clone(),
            materialized_rows: std::sync::atomic::AtomicU64::new(0),
        });
        let attr_has_defaults = vec![false; columns.len()];
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let materialized_rows = (|| -> Result<u64, ServerError> {
            self.state
                .persistent_catalog
                .persist_relation_rows_with_defaults(
                    &entry,
                    ultrasql_catalog::persistent::RelKind::MaterializedView,
                    &attr_has_defaults,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            self.materialize_view_delta(&runtime, &ddl_txn)
        })();
        let materialized_rows = match materialized_rows {
            Ok(rows) => rows,
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of materialized-view DDL txn failed",
                    );
                }
                let _ = self.state.persistent_catalog.drop_table(table_name);
                return Err(e);
            }
        };
        if let Err(e) = self
            .state
            .persist_materialized_view_runtime_metadata(&runtime, materialized_rows)
        {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of materialized-view metadata txn failed",
                );
            }
            let _ = self.state.persistent_catalog.drop_table(table_name);
            return Err(e);
        }
        self.state.commit_transaction(
            ddl_txn,
            true,
            "CREATE MATERIALIZED VIEW catalog transaction",
        )?;
        let mut row_security = self
            .state
            .row_security
            .get(&oid)
            .map(|guard| guard.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        self.state.persist_row_security_metadata()?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Table,
            table_name,
        );
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        runtime
            .materialized_rows
            .store(materialized_rows, std::sync::atomic::Ordering::Release);
        self.state
            .note_table_modifications(table_name, materialized_rows);
        self.state
            .materialized_views
            .insert(table_name.clone(), runtime);
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE MATERIALIZED VIEW"))
    }

    fn create_table_unique_indexes(
        &self,
        table: &TableEntry,
        unique_constraints: &[ultrasql_planner::LogicalUniqueConstraint],
    ) -> Result<Vec<IndexEntry>, ServerError> {
        let mut created = Vec::with_capacity(unique_constraints.len());
        for unique in unique_constraints {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &unique.columns)?;
            let index_oid = self.state.persistent_catalog.next_oid();
            let index_rel = RelationId::new(index_oid.raw());
            let btree = BTree::create(Arc::clone(self.state.heap.buffer_pool()), index_rel)
                .map_err(|e| {
                    ServerError::ddl(format!("CREATE TABLE constraint index create: {e}"))
                })?;
            let root_block = btree.root_block();
            let mut attnums = Vec::with_capacity(unique.columns.len());
            for &col in &unique.columns {
                let attnum = u16::try_from(col).map_err(|_| {
                    ServerError::Unsupported(
                        "CREATE TABLE: constraint column index does not fit u16",
                    )
                })?;
                attnums.push(attnum);
            }
            let mut entry =
                IndexEntry::new(index_oid, unique.name.clone(), table.oid, attnums, true);
            entry.root_block = root_block;
            // Empty table, so there are no existing heap rows to populate.
            self.state.persistent_catalog.create_index(entry.clone())?;
            created.push(entry);
        }
        Ok(created)
    }

    /// Build a B+ tree index over the supplied table and register it
    /// in `pg_index`.
    ///
    /// The kernel work is split into four steps:
    ///
    /// 1. Validate the request against the current catalog snapshot —
    ///    `IF NOT EXISTS`, presence of the parent table, and key-column
    ///    type compatibility with the B-tree (the v0.5 tree stores
    ///    fixed-size 8-byte keys, so every supported column type is
    ///    mapped into an `i64` by the
    ///    [`crate::index_key::IndexKeyEncoding`] this method picks).
    /// 2. Allocate a fresh OID for the index and instantiate a new
    ///    [`BTree`] over a relation id derived from that OID. The
    ///    buffer pool's blank-page loader hands out empty heap pages
    ///    which `BTree::create` then initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column(s), and call
    ///    [`BTree::insert`] with the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`] carrying the root block plus the
    ///    requested attnums, register it with the persistent catalog,
    ///    and let the catalog's snapshot rotation publish the entry to
    ///    subsequent statements.
    ///
    /// # Supported key shapes
    ///
    /// - Single column of `Int16`, `Int32`, `Int64`, `Bool`,
    ///   `Timestamp`, `TimestampTz`, `Float32`, `Float64`, or `Text`.
    ///   See [`crate::index_key::IndexKeyEncoding`] for the per-type
    ///   mapping. `Text` columns are truncated to their first 8 UTF-8
    ///   bytes; collisions are resolved by a heap-side recheck during
    ///   index probes.
    /// - Two columns of `Bool` / `Int16` / `Int32` packed into a single
    ///   `i64` (`hi << 32 | lo`). Composite probes are recheck-filtered
    ///   to drop bit-pattern collisions.
    /// - Indexes over three or more columns, over wider integer halves,
    ///   and over float / text composites still return
    ///   [`ServerError::Unsupported`] — they require a `Vec<u8>`-keyed
    ///   B-tree, scheduled for the v0.7 wave.
    ///
    /// # Other gaps
    ///
    /// - `UNIQUE` is honoured at the catalog level — the
    ///   [`IndexEntry::is_unique`] flag is propagated — but the
    ///   B-tree's existing duplicate-key rejection is the only
    ///   enforcement. Non-unique indexes that happen to have unique
    ///   data still build correctly; non-unique indexes with
    ///   duplicates would error here, which is a known limitation we
    ///   accept until the B-tree gains a non-unique mode.
    pub(crate) fn execute_create_index(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateIndex {
            index_name,
            table_name,
            columns,
            key_exprs,
            opclasses,
            index_options,
            include_columns,
            predicate,
            method,
            aggregating,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // 1a. IF NOT EXISTS short-circuit.
        if snapshot.indexes.contains_key(index_name) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_name.clone()),
            ));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;
        self.ensure_table_owner_or_superuser(table.oid, table_name)?;

        if *method == LogicalIndexMethod::Aggregating {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE AGGREGATING INDEX is not supported",
                ));
            }
            let Some(spec) = aggregating.clone() else {
                return Err(ServerError::ddl(
                    "CREATE AGGREGATING INDEX missing aggregating metadata",
                ));
            };
            let index_oid = self.state.persistent_catalog.next_oid();
            let block_count = self
                .state
                .heap
                .block_count(RelationId(table.oid))
                .max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("building index", 0);
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let build_result = crate::aggregating_index::build_aggregating_index_rows(
                table,
                &spec,
                self.state.heap.as_ref(),
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            self.state
                .commit_transaction(txn, false, "CREATE AGGREGATING INDEX scan")?;
            let rows = build_result?;
            progress.update("writing catalog", block_count);
            let attnums = columns
                .iter()
                .map(|col| {
                    u16::try_from(*col).map_err(|_| {
                        ServerError::Unsupported(
                            "CREATE AGGREGATING INDEX: column index does not fit in u16 attnum field",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, false)
                .with_access_method("aggregating", vec![None; spec.group_columns.len()])
                .with_options(
                    crate::aggregating_index::catalog_options_for_aggregating_index(
                        &spec, table.oid, index_oid,
                    ),
                );
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE AGGREGATING INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: key_exprs.clone(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: None,
                    aggregating: Some(Arc::new(crate::RuntimeAggregatingIndex::new(spec, rows))),
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::IvfFlat {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING ivfflat: ivfflat indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING ivfflat", &field.data_type)?;
            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let (lists, probes, payload) = ivfflat_options(index_options)?;
            let payload = payload.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            let ivfflat = Arc::new(
                PageBackedIvfFlatIndex::new_with_payload_kind(
                    RelationId::new(index_oid.raw()),
                    dims,
                    metric,
                    lists,
                    probes,
                    payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("scanning table", 0);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                let mut rows = Vec::new();
                let mut last_progress_block = 0;
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat heap scan: {e}"))
                    })?;
                    let blocks_done = tuple
                        .tid
                        .page
                        .block
                        .raw()
                        .saturating_add(1)
                        .min(block_count);
                    if blocks_done != last_progress_block {
                        progress.update("scanning table", blocks_done);
                        last_progress_block = blocks_done;
                    }
                    let row = codec.decode(&tuple.data).map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat decode: {e}"))
                    })?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector.clone(),
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX ivfflat: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    rows.push((vector, tuple.tid));
                }
                progress.update("loading index", block_count);
                ivfflat
                    .bulk_load_logged(rows, txn.xid, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat bulk load: {e}")))
            })();
            self.state
                .commit_transaction(txn, true, "CREATE INDEX ivfflat build")?;
            build_result?;
            progress.update("writing catalog", block_count);
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_access_method("ivfflat", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE IVFFLAT INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: Some(ivfflat),
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::Hnsw {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING hnsw: hnsw indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING hnsw: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING hnsw", &field.data_type)?;

            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let payload = hnsw_payload_option(index_options)?.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            let index_rel = RelationId::new(index_oid.raw());
            let hnsw = Arc::new(
                PageBackedHnswIndex::new_with_payload_kind(
                    index_rel, dims, metric, 16, 64, payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let progress = CreateIndexProgressGuard::new(
                self.state.workload_recorder.as_ref(),
                self.pid,
                table.oid.raw(),
                index_oid.raw(),
                block_count,
            );
            progress.update("building index", 0);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                let mut last_progress_block = 0;
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX hnsw heap scan: {e}"))
                    })?;
                    let blocks_done = tuple
                        .tid
                        .page
                        .block
                        .raw()
                        .saturating_add(1)
                        .min(block_count);
                    if blocks_done != last_progress_block {
                        progress.update("building index", blocks_done);
                        last_progress_block = blocks_done;
                    }
                    let row = codec
                        .decode(&tuple.data)
                        .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw decode: {e}")))?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector,
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX hnsw: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    hnsw.insert_vector_logged(
                        vector,
                        tuple.tid,
                        txn.xid,
                        self.state.heap.wal_sink().map(Arc::as_ref),
                    )
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw insert: {e}")))?;
                }
                Ok(())
            })();
            self.state
                .commit_transaction(txn, true, "CREATE INDEX hnsw build")?;
            build_result?;
            progress.update("writing catalog", block_count);
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_access_method("hnsw", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE HNSW INDEX catalog transaction",
            )?;
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: Some(hnsw),
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let expression_key_exprs = if columns.is_empty() {
            let [expr] = key_exprs.as_slice() else {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: expression indexes support exactly one key in this wave",
                ));
            };
            let _ = expr;
            key_exprs.clone()
        } else {
            Vec::new()
        };
        let encoding = if *method == ultrasql_planner::LogicalIndexMethod::Hash {
            crate::index_key::IndexKeyEncoding::Int64
        } else if expression_key_exprs.is_empty() {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, columns)?
        } else {
            crate::index_key::IndexKeyEncoding::for_data_type(&expression_key_exprs[0].data_type())?
        };
        let key_col_idx = columns.first().copied();

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();
        let brin_summary = if *method == ultrasql_planner::LogicalIndexMethod::Brin {
            Some(Arc::new(BrinIndex::new(128)))
        } else {
            None
        };

        // 3. Scan the heap and populate the tree.
        let mut attnums: Vec<u16> = Vec::with_capacity(columns.len());
        for &col in columns {
            let attnum = u16::try_from(col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            attnums.push(attnum);
        }
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
        let progress = CreateIndexProgressGuard::new(
            self.state.workload_recorder.as_ref(),
            self.pid,
            table.oid.raw(),
            index_oid.raw(),
            block_count,
        );
        progress.update("building index", 0);
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            let mut last_progress_block = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let blocks_done = tup.tid.page.block.raw().saturating_add(1).min(block_count);
                if blocks_done != last_progress_block {
                    progress.update("building index", blocks_done);
                    last_progress_block = blocks_done;
                }
                let row = decode_key_column(
                    &tup.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    *method,
                    &encoding,
                )?;
                if let Some(key) = row {
                    if *unique {
                        btree.insert(key, tup.tid, txn.xid, None).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                        })?;
                    } else {
                        btree
                            .insert_non_unique(key, tup.tid, txn.xid, None)
                            .map_err(|e| {
                                ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                            })?;
                    }
                    if let Some(brin) = &brin_summary {
                        let brin_key = BrinIndex::encode_i64_key(key);
                        brin.insert(&brin_key, tup.tid).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX brin summarize: {e}"))
                        })?;
                    }
                    inserted += 1;
                }
                // NULL key — skip; PostgreSQL's btree omits NULL keys
                // from the index unless `INCLUDE` adds them, and our
                // BTree::insert lacks a NULL marker.
            }
            Ok(inserted)
        })();

        // Commit the txn regardless of build outcome so the XID does
        // not leak as in-progress forever.
        self.state
            .commit_transaction(txn, true, "CREATE INDEX build")?;
        let _ = insert_result?;
        progress.update("writing catalog", block_count);

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let mut entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique)
            .with_access_method(logical_index_method_name(*method), opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
        entry.root_block = root_block;
        self.state.persistent_catalog.create_index(entry.clone())?;
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_index_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_index_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_index(index_name);
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE INDEX catalog transaction")?;
        if !expression_key_exprs.is_empty()
            || predicate.is_some()
            || !include_columns.is_empty()
            || *method != ultrasql_planner::LogicalIndexMethod::Btree
        {
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: expression_key_exprs,
                    predicate: predicate.clone(),
                    include_columns: include_columns.clone(),
                    method: *method,
                    brin: brin_summary.clone(),
                    hnsw: None,
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
        }
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Drop one or more indexes and their dependent runtime metadata.
    pub(crate) fn execute_drop_index(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropIndex {
            indexes, if_exists, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_index called with non-DropIndex plan",
            ));
        };

        let mut entries = Vec::with_capacity(indexes.len());
        for name in indexes {
            if let Some(entry) = self.state.persistent_catalog.lookup_index(name) {
                let table_name = self
                    .state
                    .persistent_catalog
                    .lookup_table_by_oid(entry.table_oid)
                    .map_or_else(
                        || format!("oid {}", entry.table_oid.raw()),
                        |table| table.name,
                    );
                self.ensure_table_owner_or_superuser(entry.table_oid, &table_name)?;
                if entry.is_unique && entry.name.ends_with("_pkey") {
                    return Err(ServerError::DependentObjectsStillExist(format!(
                        "cannot drop index {} because primary key constraint depends on it",
                        entry.name
                    )));
                }
                if let Some(dependency) = self
                    .state
                    .persistent_catalog
                    .constraint_dependency_for_index(entry.table_oid, &entry.name)
                {
                    return Err(ServerError::DependentObjectsStillExist(format!(
                        "cannot drop index {} because constraint {} depends on it",
                        entry.name, dependency.conname
                    )));
                }
                entries.push(entry);
            } else if !*if_exists {
                return Err(ultrasql_catalog::CatalogError::not_found(name.clone()).into());
            }
        }
        if entries.is_empty() {
            return Ok(run_ddl_command("DROP INDEX"));
        }

        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let persist_result = entries.iter().try_for_each(|entry| {
            self.state.persistent_catalog.persist_index_drop_tombstone(
                entry,
                self.state.heap.as_ref(),
                ddl_xid,
                ddl_command_id,
            )
        });
        if let Err(e) = persist_result {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of DROP INDEX catalog txn failed",
                );
            }
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "DROP INDEX catalog transaction")?;

        let mut runtime_metadata_removed = false;
        for entry in entries {
            if let Some(mut constraints) = self
                .state
                .table_constraints
                .get(&entry.table_oid)
                .map(|guard| guard.value().as_ref().clone())
                && constraints.indexes.remove(&entry.oid).is_some()
            {
                self.state
                    .table_constraints
                    .insert(entry.table_oid, Arc::new(constraints));
                runtime_metadata_removed = true;
            }
            self.state
                .persistent_catalog
                .clear_descriptions_for_object(entry.oid);
            self.state.persistent_catalog.drop_index(&entry.name)?;
        }
        if runtime_metadata_removed {
            self.state.persist_table_runtime_constraints_metadata()?;
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP INDEX"))
    }

    /// Drop one or more tables.
    ///
    /// The binder has already filtered names through the catalog —
    /// see [`ultrasql_planner::bind`] — so the only failure surface
    /// here is `CatalogError::NotFound`, which can fire only when a
    /// concurrent DDL deleted the relation between the binder and the
    /// dispatcher. Associated indexes are removed by
    /// [`MutableCatalog::drop_table`] in a single atomic snapshot
    /// rotation.
    ///
    /// Heap pages backing the dropped relation are *not* reclaimed in
    /// this wave: the in-memory buffer pool grows on demand and the
    /// segment manager has not yet landed. The dropped name becomes
    /// available immediately for reuse via `CREATE TABLE` — subsequent
    /// inserts will reuse the relation-id space without colliding
    /// because OIDs are monotonic.
    pub(crate) fn execute_drop_table(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropTable {
            tables, cascade, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_table called with non-DropTable plan",
            ));
        };
        let initial_drop_set: HashSet<String> = tables
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        let mut drop_set = initial_drop_set.clone();
        let mut drop_names = tables.clone();
        if *cascade {
            for name in self.materialized_view_cascade_drop_names(&mut drop_set) {
                if !drop_names
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&name))
                {
                    drop_names.push(name);
                }
            }
        }
        for name in tables {
            let Some(entry) = self.state.persistent_catalog.lookup_table(name) else {
                continue;
            };
            self.ensure_table_owner_or_superuser(entry.oid, name)?;
            let mut dependents = self.foreign_key_dependents(entry.oid, &drop_set);
            dependents.extend(self.materialized_view_dependents(name, &initial_drop_set));
            dependents.sort();
            if !dependents.is_empty() && !*cascade {
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop table {name} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        let mut durable_drop_entries = Vec::new();
        for name in &drop_names {
            let Some(entry) = self.state.persistent_catalog.lookup_table(name) else {
                continue;
            };
            durable_drop_entries.push(entry);
            if let Some(runtime) = self.state.time_partitions.get(name) {
                for chunk in runtime.chunks.iter() {
                    if let Some(chunk_entry) = self
                        .state
                        .persistent_catalog
                        .lookup_table(&chunk.value().table_name)
                    {
                        durable_drop_entries.push(chunk_entry);
                    }
                }
            }
        }
        if !durable_drop_entries.is_empty() {
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            let ddl_xid = ddl_txn.xid;
            let ddl_command_id = ddl_txn.current_command;
            let persist_result = durable_drop_entries.iter().try_for_each(|entry| {
                self.state.persistent_catalog.persist_table_drop_tombstone(
                    entry,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )
            });
            if let Err(e) = persist_result {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of DROP TABLE catalog txn failed",
                    );
                }
                return Err(e.into());
            }
            self.state
                .commit_transaction(ddl_txn, true, "DROP TABLE catalog transaction")?;
        }
        let mut privilege_grants_removed = false;
        for name in &drop_names {
            if let Some(entry) = self.state.persistent_catalog.lookup_table(name) {
                if *cascade {
                    self.drop_foreign_key_dependencies(entry.oid, &drop_set);
                }
                let dependent_index_oids = self
                    .state
                    .persistent_catalog
                    .list_indexes_for_table(entry.oid)
                    .into_iter()
                    .map(|index| index.oid)
                    .collect::<Vec<_>>();
                if let Some((_, runtime)) = self.state.time_partitions.remove(name) {
                    for chunk in runtime.chunks.iter() {
                        let _ = self.state.persistent_catalog.drop_table(&chunk.table_name);
                    }
                }
                self.state.columnar_storage.remove(name);
                let folded_name = name.to_ascii_lowercase();
                self.state.stats_catalog.write().remove(&folded_name);
                self.state
                    .persistent_catalog
                    .replace_statistics(entry.oid, std::iter::empty());
                self.state
                    .persistent_catalog
                    .remove_statistic_ext_for_relation(entry.oid);
                self.state.table_modifications.remove(&folded_name);
                self.state.pending_analyze_tables.remove(&folded_name);
                if let Some((_, constraints)) = self.state.table_constraints.remove(&entry.oid) {
                    for seq_name in constraints.sequence_defaults.iter().flatten() {
                        if let Some(seq) = self.state.sequences.get(seq_name).map(|seq| seq.clone())
                        {
                            seq.emit_wal(
                                SequenceOpKind::Drop,
                                seq_name,
                                RelationId::INVALID,
                                ultrasql_core::Xid::INVALID,
                                self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                            )
                            .map_err(|e| {
                                ServerError::ddl(format!("DROP TABLE owned sequence WAL: {e}"))
                            })?;
                        }
                        self.state.sequences.remove(seq_name);
                        self.sequence_state.forget(seq_name);
                        privilege_grants_removed |=
                            self.state.privilege_catalog.remove_object_grants(
                                crate::auth::PrivilegeObjectKind::Sequence,
                                seq_name,
                            );
                    }
                }
                self.state.row_security.remove(&entry.oid);
                privilege_grants_removed |= self
                    .state
                    .privilege_catalog
                    .remove_object_grants(crate::auth::PrivilegeObjectKind::Table, name);
                self.state
                    .persistent_catalog
                    .clear_descriptions_for_object(entry.oid);
                for index_oid in dependent_index_oids {
                    self.state
                        .persistent_catalog
                        .clear_descriptions_for_object(index_oid);
                }
            }
            self.state.materialized_views.remove(name);
            self.state.persistent_catalog.drop_table(name)?;
        }
        self.state.persist_table_runtime_constraints_metadata()?;
        self.state.persist_row_security_metadata()?;
        if privilege_grants_removed {
            self.state.persist_privilege_metadata()?;
        }
        self.state
            .remove_materialized_view_runtime_metadata(&drop_names)?;
        // Any cached plan that referenced this name is now invalid;
        // clear the cache so subsequent statements re-plan.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP TABLE"))
    }

    fn foreign_key_dependents(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut out = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table.name.to_ascii_lowercase()) {
                continue;
            }
            for fk in &item.value().foreign_keys {
                if fk.target_oid == target_oid {
                    out.push(format!("{}.{}", table.name, fk.name));
                }
            }
        }
        out.sort();
        out
    }

    fn materialized_view_dependents(
        &self,
        target_table: &str,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let mut out = Vec::new();
        for item in self.state.materialized_views.iter() {
            let runtime = item.value();
            if runtime.source_table.eq_ignore_ascii_case(target_table)
                && !drop_set.contains(&runtime.view_table.to_ascii_lowercase())
            {
                out.push(runtime.view_table.clone());
            }
        }
        out.sort();
        out
    }

    fn materialized_view_cascade_drop_names(&self, drop_set: &mut HashSet<String>) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let mut changed = false;
            for item in self.state.materialized_views.iter() {
                let runtime = item.value();
                let source = runtime.source_table.to_ascii_lowercase();
                let view = runtime.view_table.to_ascii_lowercase();
                if drop_set.contains(&source) && drop_set.insert(view) {
                    out.push(runtime.view_table.clone());
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        out.sort();
        out
    }

    fn drop_foreign_key_dependencies(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) {
        let snapshot = self.state.catalog_snapshot();
        let mut updates = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table.name.to_ascii_lowercase()) {
                continue;
            }
            if item
                .value()
                .foreign_keys
                .iter()
                .any(|fk| fk.target_oid == target_oid)
            {
                let mut next = item.value().as_ref().clone();
                next.foreign_keys.retain(|fk| fk.target_oid != target_oid);
                updates.push((table_oid, next));
            }
        }
        for (table_oid, constraints) in updates {
            self.state
                .table_constraints
                .insert(table_oid, Arc::new(constraints));
        }
    }

    pub(crate) fn execute_comment(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Comment {
            target, comment, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_comment called with non-Comment plan",
            ));
        };
        let (objoid, objsubid) = match target {
            LogicalCommentTarget::Table { table } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Index { index } => {
                let entry = snapshot
                    .indexes
                    .get(index)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(index.clone()))?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Column { table, attnum, .. } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                (entry.oid, *attnum)
            }
        };
        let classoid = ultrasql_core::Oid::new(ultrasql_catalog::bootstrap::PG_CLASS_OID);
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let row = ultrasql_catalog::persistent::DescriptionRow {
            objoid,
            classoid,
            objsubid,
            description: comment.clone().unwrap_or_default(),
        };
        if let Err(e) = self.state.persistent_catalog.persist_description_row(
            &row,
            comment.is_none(),
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of COMMENT catalog txn failed",
                );
            }
            return Err(e.into());
        }
        self.state
            .commit_transaction(ddl_txn, true, "COMMENT catalog transaction")?;
        self.state
            .persistent_catalog
            .set_description(objoid, classoid, objsubid, comment.clone());
        self.plan_cache_invalidate();
        Ok(run_ddl_command("COMMENT"))
    }
}

fn hnsw_metric_for_opclass(opclass: Option<&str>) -> Result<HnswMetric, ServerError> {
    match opclass.unwrap_or("vector_l2_ops") {
        "vector_l2_ops" => Ok(HnswMetric::L2),
        "vector_cosine_ops" => Ok(HnswMetric::Cosine),
        "vector_ip_ops" => Ok(HnswMetric::NegativeInnerProduct),
        "vector_l1_ops" => Ok(HnswMetric::L1),
        other => Err(ServerError::ddl(format!(
            "CREATE INDEX USING hnsw: unsupported vector opclass {other}"
        ))),
    }
}

fn logical_index_method_name(method: LogicalIndexMethod) -> &'static str {
    match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    }
}

fn index_options_as_pairs(options: &[LogicalIndexOption]) -> Vec<(String, String)> {
    options
        .iter()
        .map(|option| (option.name.clone(), option.value.clone()))
        .collect()
}

fn column_collation_options(collations: &[Option<u32>]) -> Vec<(String, String)> {
    collations
        .iter()
        .enumerate()
        .filter_map(|(idx, collation)| {
            collation.map(|oid| {
                (
                    format!("{COLUMN_COLLATION_OPTION_PREFIX}{idx}"),
                    oid.to_string(),
                )
            })
        })
        .collect()
}

fn ann_dims_and_default_payload(
    context: &str,
    data_type: &DataType,
) -> Result<(u32, AnnPayloadKind), ServerError> {
    match data_type {
        DataType::Vector { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::F32)),
        DataType::HalfVec { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::Bf16)),
        other => Err(ServerError::ddl(format!(
            "{context} requires vector(n) or halfvec(n), got {other}"
        ))),
    }
}

fn hnsw_payload_option(
    options: &[LogicalIndexOption],
) -> Result<Option<AnnPayloadKind>, ServerError> {
    let mut payload = None;
    for option in options {
        if option.name != "payload" {
            return Err(ServerError::ddl(format!(
                "CREATE INDEX USING hnsw: unsupported option {}",
                option.name
            )));
        }
        payload = Some(ann_payload_kind_from_value(
            "CREATE INDEX USING hnsw",
            &option.value,
        )?);
    }
    Ok(payload)
}

fn ann_payload_kind_from_value(context: &str, value: &str) -> Result<AnnPayloadKind, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" => Ok(AnnPayloadKind::F32),
        "bf16" | "bfloat16" => Ok(AnnPayloadKind::Bf16),
        "int8" | "i8" => Ok(AnnPayloadKind::Int8),
        other => Err(ServerError::ddl(format!(
            "{context}: unsupported payload {other}; expected f32, bf16, or int8"
        ))),
    }
}

fn ivfflat_options(
    options: &[LogicalIndexOption],
) -> Result<(usize, usize, Option<AnnPayloadKind>), ServerError> {
    let mut lists = 100_usize;
    let mut probes = 1_usize;
    let mut payload = None;
    for option in options {
        match option.name.as_str() {
            "lists" => lists = parse_positive_ivfflat_option(option)?,
            "probes" => probes = parse_positive_ivfflat_option(option)?,
            "payload" => {
                payload = Some(ann_payload_kind_from_value(
                    "CREATE INDEX USING ivfflat",
                    &option.value,
                )?);
            }
            other => {
                return Err(ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: unsupported option {other}"
                )));
            }
        }
    }
    Ok((lists, probes, payload))
}

fn parse_positive_ivfflat_option(option: &LogicalIndexOption) -> Result<usize, ServerError> {
    let parsed = option.value.parse::<usize>().map_err(|_| {
        ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be a positive integer",
            option.name
        ))
    })?;
    if parsed == 0 {
        return Err(ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be greater than zero",
            option.name
        )));
    }
    Ok(parsed)
}

fn constraint_attnums(columns: &[usize], name: &str) -> Result<Vec<i16>, ServerError> {
    columns
        .iter()
        .map(|col| {
            let attnum = col.checked_add(1).ok_or(ServerError::Unsupported(
                "CREATE TABLE: constraint attnum overflow",
            ))?;
            i16::try_from(attnum).map_err(|_| {
                ServerError::ddl(format!(
                    "CREATE TABLE: constraint {name} column position {attnum} does not fit i16"
                ))
            })
        })
        .collect()
}
