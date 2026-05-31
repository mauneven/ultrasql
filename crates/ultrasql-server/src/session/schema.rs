//! Schema DDL execution.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::RelationId;
use ultrasql_planner::LogicalPlan;
use ultrasql_wal::payload::SequenceOpKind;

use super::Session;
use crate::auth::PrivilegeObjectKind;
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};
use crate::{RuntimeSchema, builtin_schema_name};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn ensure_schema_exists(&self, schema_name: &str) -> Result<(), ServerError> {
        let folded = schema_name.to_ascii_lowercase();
        if builtin_schema_name(&folded) || self.state.schemas.contains_key(&folded) {
            return Ok(());
        }
        Err(ServerError::UndefinedSchema(format!(
            "schema \"{schema_name}\" does not exist"
        )))
    }

    pub(crate) fn execute_create_schema(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_schema called with non-CreateSchema plan",
            ));
        };
        if builtin_schema_name(schema_name) || self.state.schemas.contains_key(schema_name) {
            if *if_not_exists {
                return Ok(result_encoder::run_ddl_command("CREATE SCHEMA"));
            }
            return Err(ServerError::ddl(format!(
                "schema '{schema_name}' already exists"
            )));
        }
        let schema = RuntimeSchema {
            name: schema_name.clone(),
            owner_role: self.current_user.to_ascii_lowercase(),
        };
        self.state
            .schemas
            .insert(schema_name.clone(), Arc::new(schema));
        if let Err(err) = self.state.persist_schema_metadata() {
            self.state.schemas.remove(schema_name);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("CREATE SCHEMA"))
    }

    pub(crate) fn execute_drop_schema(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropSchema {
            schemas,
            if_exists,
            cascade,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_schema called with non-DropSchema plan",
            ));
        };
        let mut drop_set = Vec::new();
        for name in schemas {
            if builtin_schema_name(name) {
                return Err(ServerError::ddl(format!(
                    "cannot drop built-in schema '{name}'"
                )));
            }
            if self.state.schemas.contains_key(name) {
                drop_set.push(name.clone());
            } else if !*if_exists {
                return Err(ServerError::ddl(format!("schema '{name}' does not exist")));
            }
        }
        if drop_set.is_empty() {
            return Ok(result_encoder::run_ddl_command("DROP SCHEMA"));
        }
        for name in &drop_set {
            self.ensure_schema_owner_or_superuser(name)?;
        }
        for name in &drop_set {
            let dependents = self.schema_dependents(name);
            let dependents = if *cascade {
                dependents
                    .into_iter()
                    .filter(|dependent| !dependent.starts_with("sequence "))
                    .collect::<Vec<_>>()
            } else {
                dependents
            };
            if !dependents.is_empty() {
                let action = if *cascade { "cascade" } else { "restrict" };
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop schema {name} with {action} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        let mut privilege_metadata_changed = false;
        let mut sequence_owner_metadata_changed = false;
        if *cascade {
            for name in &drop_set {
                let (sequence_removed, grants_removed) = self.drop_schema_sequences(name)?;
                sequence_owner_metadata_changed |= sequence_removed;
                privilege_metadata_changed |= grants_removed;
            }
        }
        for name in &drop_set {
            self.state.schemas.remove(name);
            privilege_metadata_changed |= self
                .state
                .privilege_catalog
                .remove_object_grants(crate::auth::PrivilegeObjectKind::Schema, name);
            privilege_metadata_changed |= self
                .state
                .privilege_catalog
                .remove_default_grants_for_schema(name);
        }
        if sequence_owner_metadata_changed {
            self.state.persist_sequence_owner_metadata()?;
        }
        self.state.persist_schema_metadata()?;
        if privilege_metadata_changed {
            self.state.persist_privilege_metadata()?;
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP SCHEMA"))
    }

    fn drop_schema_sequences(&mut self, schema_name: &str) -> Result<(bool, bool), ServerError> {
        let mut sequence_removed = false;
        let mut grants_removed = false;
        for sequence_name in self.schema_sequence_names(schema_name) {
            if let Some(seq) = self
                .state
                .sequences
                .get(&sequence_name)
                .map(|seq| seq.clone())
            {
                seq.emit_wal(
                    SequenceOpKind::Drop,
                    &sequence_name,
                    RelationId::INVALID,
                    ultrasql_core::Xid::INVALID,
                    self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                )
                .map_err(|e| ServerError::ddl(format!("DROP SCHEMA sequence WAL: {e}")))?;
            }
            let sequence_key = sequence_name.to_ascii_lowercase();
            self.state.sequences.remove(&sequence_key);
            self.state.sequence_owners.remove(&sequence_key);
            self.state.sequence_namespaces.remove(&sequence_key);
            self.sequence_state.forget(&sequence_key);
            grants_removed |= self
                .state
                .privilege_catalog
                .remove_object_grants(PrivilegeObjectKind::Sequence, &sequence_key);
            sequence_removed = true;
        }
        Ok((sequence_removed, grants_removed))
    }

    fn schema_sequence_names(&self, schema_name: &str) -> Vec<String> {
        let mut names = self
            .state
            .sequence_namespaces
            .iter()
            .filter_map(|item| {
                if item.value().eq_ignore_ascii_case(schema_name) {
                    Some(item.key().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }

    fn schema_dependents(&self, schema_name: &str) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut dependents = Vec::new();
        for table in snapshot.tables_by_oid.values() {
            if table.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("table {}", table.name));
            }
        }
        for item in self.state.sequence_namespaces.iter() {
            if item.value().eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("sequence {}", item.key()));
            }
        }
        for ty in snapshot.enum_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("type {}", ty.name));
            }
        }
        for ty in snapshot.composite_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("type {}", ty.name));
            }
        }
        for ty in snapshot.domain_types_by_oid.values() {
            if ty.schema_name.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("domain {}", ty.name));
            }
        }
        for operator in self.state.operators.iter() {
            if operator.value().namespace.eq_ignore_ascii_case(schema_name) {
                dependents.push(format!("operator {}", operator.value().name));
            }
        }
        dependents.sort();
        dependents.dedup();
        dependents
    }
}
