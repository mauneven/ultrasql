//! `CREATE TYPE` / `CREATE DOMAIN` / `CREATE OPERATOR` / `CREATE POLICY`
//! DDL handlers. Part of the `session::ddl` module split; reopens the
//! `impl<RW> Session<RW>` block defined in `session/mod.rs`.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{
    CatalogSnapshot, CompositeTypeEntry, DomainTypeEntry, EnumLabelEntry, EnumTypeEntry,
};
use ultrasql_core::DataType;
use ultrasql_planner::{LogicalPlan, LogicalRlsCommand, LogicalRlsPermissiveness};

use super::super::Session;
use super::log_failed_ddl_rollback;
use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

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
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let type_key = ultrasql_catalog::type_lookup_key(namespace, type_name);
        let relation_key = ultrasql_catalog::table_lookup_key(namespace, type_name);
        if snapshot.enum_types.contains_key(&type_key)
            || snapshot.composite_types.contains_key(&type_key)
            || snapshot.domain_types.contains_key(&type_key)
            || snapshot.tables.contains_key(&relation_key)
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
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_enum_type(&type_key),
                "drop enum type",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE TYPE enum catalog rollback after persist error",
            ));
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
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let type_key = ultrasql_catalog::type_lookup_key(namespace, type_name);
        let relation_key = ultrasql_catalog::table_lookup_key(namespace, type_name);
        if snapshot.enum_types.contains_key(&type_key)
            || snapshot.composite_types.contains_key(&type_key)
            || snapshot.domain_types.contains_key(&type_key)
            || snapshot.tables.contains_key(&relation_key)
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
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_composite_type(&type_key),
                "drop composite type",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE TYPE composite catalog rollback after persist error",
            ));
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
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let type_key = ultrasql_catalog::type_lookup_key(namespace, domain_name);
        let relation_key = ultrasql_catalog::table_lookup_key(namespace, domain_name);
        if snapshot.enum_types.contains_key(&type_key)
            || snapshot.composite_types.contains_key(&type_key)
            || snapshot.domain_types.contains_key(&type_key)
            || snapshot.tables.contains_key(&relation_key)
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
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_domain_type(&type_key),
                "drop domain type",
            );
            self.state.domain_constraints.remove(&entry.oid);
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE DOMAIN catalog rollback after persist error",
            ));
        }
        if let Err(e) = self.state.persist_domain_runtime_constraints_metadata() {
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_domain_type(&type_key),
                "drop domain type",
            );
            self.state.domain_constraints.remove(&entry.oid);
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e,
                "CREATE DOMAIN catalog rollback after runtime metadata error",
            ));
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
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
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

    pub(super) fn domain_checks_for_columns(
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
