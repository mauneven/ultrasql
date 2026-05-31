//! Schema DDL execution.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_planner::LogicalPlan;

use super::Session;
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
        &self,
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
            let dependents = self.schema_dependents(name);
            if !dependents.is_empty() {
                let action = if *cascade { "cascade" } else { "restrict" };
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop schema {name} with {action} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        for name in &drop_set {
            self.state.schemas.remove(name);
            self.state
                .privilege_catalog
                .remove_object_grants(crate::auth::PrivilegeObjectKind::Schema, name);
        }
        self.state.persist_schema_metadata()?;
        self.state.persist_privilege_metadata()?;
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("DROP SCHEMA"))
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
