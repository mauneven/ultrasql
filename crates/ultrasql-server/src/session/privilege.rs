//! Privilege-management DDL execution.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_planner::{
    LogicalDefaultPrivilegeOperation, LogicalPlan, LogicalPrivilegeKind,
    LogicalPrivilegeObjectKind, LogicalPrivilegeSpec,
};

use super::Session;
use crate::auth::{
    AuthCatalog, DefaultPrivilegeUpdate, PrivilegeKind, PrivilegeObjectKind, PrivilegeRequest,
};
use crate::error::ServerError;
use crate::result_encoder::{self, SelectResult};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_grant_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::GrantPrivileges {
            privileges,
            object_kind,
            objects,
            grantees,
            grant_option,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_grant_privileges called with non-GrantPrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.grant_many(
            "ultrasql",
            convert_object_kind(*object_kind),
            objects,
            grantees,
            &convert_privileges(privileges),
            *grant_option,
        );
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("GRANT"))
    }

    pub(crate) fn execute_revoke_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::RevokePrivileges {
            privileges,
            object_kind,
            objects,
            grantees,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_revoke_privileges called with non-RevokePrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.revoke_many(
            convert_object_kind(*object_kind),
            objects,
            grantees,
            &convert_privileges(privileges),
        );
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("REVOKE"))
    }

    pub(crate) fn execute_alter_default_privileges(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterDefaultPrivileges {
            target_roles,
            schemas,
            operation,
            privileges,
            object_kind,
            grantees,
            grant_option,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_default_privileges called with non-AlterDefaultPrivileges plan",
            ));
        };
        self.validate_grantees(grantees)?;
        let owner_roles = self.default_privilege_owner_roles(target_roles)?;
        let privilege_requests = convert_privileges(privileges);
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        match operation {
            LogicalDefaultPrivilegeOperation::Grant => {
                self.state
                    .privilege_catalog
                    .grant_default_many(DefaultPrivilegeUpdate {
                        grantor: &self.current_user,
                        owner_roles: &owner_roles,
                        schemas,
                        object_kind: convert_object_kind(*object_kind),
                        grantees,
                        privileges: &privilege_requests,
                        grant_option: *grant_option,
                    });
            }
            LogicalDefaultPrivilegeOperation::Revoke => {
                self.state.privilege_catalog.revoke_default_many(
                    &owner_roles,
                    schemas,
                    convert_object_kind(*object_kind),
                    grantees,
                    &privilege_requests,
                );
            }
        }
        if let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.plan_cache_invalidate();
        Ok(result_encoder::run_ddl_command("ALTER DEFAULT PRIVILEGES"))
    }

    fn validate_grantees(&self, grantees: &[String]) -> Result<(), ServerError> {
        for grantee in grantees {
            if grantee == "public" {
                continue;
            }
            if self.state.role_catalog.lookup_role(grantee).is_none() {
                return Err(ServerError::ddl(format!("role '{grantee}' does not exist")));
            }
        }
        Ok(())
    }

    fn default_privilege_owner_roles(
        &self,
        target_roles: &[String],
    ) -> Result<Vec<String>, ServerError> {
        let owner_roles = if target_roles.is_empty() {
            vec![self.current_user.clone()]
        } else {
            target_roles.to_vec()
        };
        for owner in &owner_roles {
            if self.state.role_catalog.lookup_role(owner).is_none() {
                return Err(ServerError::ddl(format!("role '{owner}' does not exist")));
            }
            if !self.state.role_catalog.can_set_role(&self.auth_user, owner) {
                return Err(ServerError::InsufficientPrivilege(format!(
                    "permission denied to alter default privileges for role {owner}"
                )));
            }
        }
        Ok(owner_roles)
    }
}

fn convert_object_kind(kind: LogicalPrivilegeObjectKind) -> PrivilegeObjectKind {
    match kind {
        LogicalPrivilegeObjectKind::Table => PrivilegeObjectKind::Table,
        LogicalPrivilegeObjectKind::Schema => PrivilegeObjectKind::Schema,
        LogicalPrivilegeObjectKind::Database => PrivilegeObjectKind::Database,
        LogicalPrivilegeObjectKind::Sequence => PrivilegeObjectKind::Sequence,
        LogicalPrivilegeObjectKind::Function => PrivilegeObjectKind::Function,
    }
}

fn convert_privileges(privileges: &[LogicalPrivilegeSpec]) -> Vec<PrivilegeRequest> {
    privileges
        .iter()
        .map(|privilege| PrivilegeRequest {
            privilege: convert_privilege_kind(privilege.kind),
            columns: privilege.columns.clone(),
        })
        .collect()
}

fn convert_privilege_kind(kind: LogicalPrivilegeKind) -> PrivilegeKind {
    match kind {
        LogicalPrivilegeKind::Select => PrivilegeKind::Select,
        LogicalPrivilegeKind::Insert => PrivilegeKind::Insert,
        LogicalPrivilegeKind::Update => PrivilegeKind::Update,
        LogicalPrivilegeKind::Delete => PrivilegeKind::Delete,
        LogicalPrivilegeKind::Truncate => PrivilegeKind::Truncate,
        LogicalPrivilegeKind::References => PrivilegeKind::References,
        LogicalPrivilegeKind::Trigger => PrivilegeKind::Trigger,
        LogicalPrivilegeKind::Usage => PrivilegeKind::Usage,
        LogicalPrivilegeKind::Create => PrivilegeKind::Create,
        LogicalPrivilegeKind::Connect => PrivilegeKind::Connect,
        LogicalPrivilegeKind::Temporary => PrivilegeKind::Temporary,
        LogicalPrivilegeKind::Execute => PrivilegeKind::Execute,
    }
}
