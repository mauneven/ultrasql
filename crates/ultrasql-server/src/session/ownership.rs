//! Shared ownership checks for session-level DDL.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_core::Oid;

use super::Session;
use crate::auth::{PrivilegeKind, PrivilegeObjectKind, pg_authid::AuthCatalog};
use crate::builtin_schema_name;
use crate::error::ServerError;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn ensure_table_owner_or_superuser(
        &self,
        table_oid: Oid,
        table_name: &str,
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let owns_table = self
            .state
            .row_security
            .get(&table_oid)
            .is_some_and(|runtime| runtime.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_table {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(format!(
                "permission denied to manage table {table_name}"
            )))
        }
    }

    pub(super) fn ensure_table_owner_or_privilege_or_superuser(
        &self,
        table_oid: Oid,
        table_name: &str,
        privilege: PrivilegeKind,
        action: &str,
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let owns_table = self
            .state
            .row_security
            .get(&table_oid)
            .is_some_and(|runtime| runtime.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_table {
            return Ok(());
        }
        let roles = self.state.role_catalog.inherited_role_names(&current_user);
        if self.state.privilege_catalog.has_privilege_for_roles(
            &roles,
            PrivilegeObjectKind::Table,
            table_name,
            privilege,
        ) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(format!(
            "permission denied to {action} table {table_name}"
        )))
    }

    pub(super) fn ensure_schema_owner_or_superuser(
        &self,
        schema_name: &str,
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let owns_schema = self
            .state
            .schemas
            .get(schema_name)
            .is_some_and(|schema| schema.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_schema {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(format!(
                "permission denied to drop schema {schema_name}"
            )))
        }
    }

    pub(super) fn ensure_schema_create_privilege(
        &self,
        schema_name: &str,
    ) -> Result<(), ServerError> {
        let schema_name = schema_name.to_ascii_lowercase();
        if builtin_schema_name(&schema_name) {
            return Ok(());
        }
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let owns_schema = self
            .state
            .schemas
            .get(&schema_name)
            .is_some_and(|schema| schema.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_schema {
            return Ok(());
        }
        let roles = self.state.role_catalog.inherited_role_names(&current_user);
        if self.state.privilege_catalog.has_privilege_for_roles(
            &roles,
            PrivilegeObjectKind::Schema,
            &schema_name,
            PrivilegeKind::Create,
        ) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(format!(
            "permission denied to create objects in schema {schema_name}"
        )))
    }

    pub(super) fn ensure_schema_usage_privilege(
        &self,
        schema_name: &str,
    ) -> Result<(), ServerError> {
        let schema_name = schema_name.to_ascii_lowercase();
        if builtin_schema_name(&schema_name) {
            return Ok(());
        }
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let owns_schema = self
            .state
            .schemas
            .get(&schema_name)
            .is_some_and(|schema| schema.owner_role.eq_ignore_ascii_case(&current_user));
        if owns_schema {
            return Ok(());
        }
        let roles = self.state.role_catalog.inherited_role_names(&current_user);
        if self.state.privilege_catalog.has_privilege_for_roles(
            &roles,
            PrivilegeObjectKind::Schema,
            &schema_name,
            PrivilegeKind::Usage,
        ) {
            return Ok(());
        }
        Err(ServerError::InsufficientPrivilege(format!(
            "USAGE privilege on schema {schema_name}"
        )))
    }

    pub(super) fn ensure_sequence_owner_or_superuser(
        &self,
        sequence_name: &str,
    ) -> Result<(), ServerError> {
        let current_user = self.current_user.to_ascii_lowercase();
        if self.current_user_is_superuser(&current_user) {
            return Ok(());
        }
        let sequence_key = sequence_name.to_ascii_lowercase();
        let owns_sequence = self
            .state
            .sequence_owners
            .get(&sequence_key)
            .is_some_and(|owner| owner.eq_ignore_ascii_case(&current_user));
        if owns_sequence {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(format!(
                "permission denied to manage sequence {sequence_name}"
            )))
        }
    }

    pub(super) fn current_user_is_superuser(&self, current_user: &str) -> bool {
        self.state
            .role_catalog
            .lookup_role(current_user)
            .is_some_and(|role| role.is_superuser)
    }
}
